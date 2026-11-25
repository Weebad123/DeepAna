//! The ingestor: the orchestration layer that turns PRISM's components into
//! a running pipeline.
//!
//! Everything else in the crate is a component. [`Ingestor`] builds and owns
//! them (the [`SlotProcessor`], the [`DecoderRegistry`], the
//! [`AccountResolver`] and the [`ParquetWriter`]), wires them together and
//! drives them as two concurrent Tokio tasks joined by a bounded channel:
//!
//! ```text
//!   process ──(processed channel: ProcessedSlot)──▶ writer
//!     │                                               │
//!   SlotProcessor: fetch, normalize,                skipped: cursor.record_skipped
//!     GapHandler for skipped slots                  data:    ParquetWriter::append
//!   DecoderRegistry::decode_block                            cursor.record_processed
//!   AccountResolver::resolve                        flush:   ParquetWriter::flush
//!                                                            ChangeLog: one New per slot
//!                                                            cursor.record_flush
//! ```
//!
//! * **process** follows the chain tip at [`IngestorConfig::commitment`] and
//!   hands every slot from the resume point upwards, in order, to the slot
//!   processor. The processor retries transient fetch failures itself and
//!   publishes the `Skip` event of a skipped slot; the ingestor then decodes
//!   the slot's instructions, resolves its token transfers and sends the
//!   [`ProcessedSlot`] on. A slot the processor gives up on halts the
//!   pipeline, so the cursor never moves past data that was never read.
//! * **writer** appends each slot's rows to the [`ParquetWriter`] and moves
//!   the cursor's processed edge. Every [`IngestorConfig::flush_interval`],
//!   or sooner once [`IngestorConfig::flush_max_slots`] slots are buffered,
//!   it flushes, emits one [`ChangeEvent::New`] per flushed slot carrying the
//!   files written, and then moves the cursor's flush edge, which persists
//!   it. With an enricher attached ([`Ingestor::with_metadata`]) it then
//!   hints the mint of every flushed transfer, without waiting.
//!
//! | File        | Holds                                                    |
//! |-------------|----------------------------------------------------------|
//! | `mod.rs`    | [`Ingestor`], its config and the Helius setup             |
//! | [`cursor`]  | [`IngestorCursor`], the persisted resume point            |
//!
//! A failed flush keeps its rows buffered and is retried at the next
//! interval; [`IngestorConfig::max_flush_failures`] failures in a row halt
//! the pipeline. Files a flush wrote but the cursor never recorded (a crash
//! or halt between the two) are removed by [`Ingestor::run`] before it
//! starts, and their slots are ingested again, so every slot ends up on disk
//! exactly once. Their `New` events may then be emitted twice: delivery to
//! the change log is at least once.
//!
//! Shutdown is graceful and cascades down the pipe: cancelling the token
//! stops process, whose dropped sender lets writer drain the channel, flush
//! whatever is left and persist the cursor. The same cascade runs when
//! process reaches [`IngestorConfig::end_slot`].
//!
//! The ingestor also executes the rewinds the
//! [`Reconciler`](crate::reconcile::Reconciler) decides on, through the
//! [`IngestorRewinder`] from [`Ingestor::rewinder`]. This is the "stop,
//! rewind, restart" loop of [`Ingestor::run`]: one *epoch* runs both tasks
//! until a rewind request arrives, which stops them *without* flushing (the
//! buffered slots are above the rewind point and about to be dropped); then
//!
//! 1. the correction is announced and the change log flushed (nothing of
//!    the stopped epoch can be announced after it),
//! 2. the cursor is lowered,
//! 3. the files at or above the rewind point are deleted,
//!
//! and the next epoch starts from there. Before the first epoch,
//! [`Ingestor::run`] finishes a rewind a crash interrupted
//! ([`Reconciler::recover`](crate::reconcile::Reconciler::recover)).
//!
//! Slots already below the finalized watermark are fetched at `finalized`,
//! whatever [`IngestorConfig::commitment`] says: a slot that is re-ingested
//! after a correction can then never be read from the abandoned fork again.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use helius::HeliusBuilder;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::account_resolver::{AccountResolver, ResolverMetrics, TokenAccountIndex};
use crate::adapter::config::{SolanaAdapterConfig, TransactionEncoding};
use crate::adapter::solana::SolanaAdapter;
use crate::adapter::{ChainAdapter, CommitmentLevel};
use crate::changelog::ChangeLog;
use crate::commitment::CommitmentTracker;
use crate::decoder::DecoderRegistry;
use crate::metadata::MetadataHandle;
use crate::normalizer::{NormalizedBlock, Normalizer};
use crate::reconcile::{InFlight, ReconcileContext, Reconciler, Rewind};
use crate::slot_processor::{GapHandler, ProcessedSlot, SlotFetcher, SlotFetcherConfig, SlotProcessor};
use crate::storage::{ParquetWriter, Storage, WriterMetrics};
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::ChangeEvent;
use crate::types::udm::{DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction};

pub mod cursor;

pub use cursor::{CursorState, IngestorCursor};
/// Re-exported so callers of [`helius_solana_adapter`] need not depend on
/// the Helius SDK themselves.
pub use helius::types::Cluster;

/// Environment variable the Helius API key is read from.
pub const HELIUS_API_KEY_ENV: &str = "HELIUS_API_KEY";

/// Base58 id of the Solana vote program.
pub const VOTE_PROGRAM_ID: &str = "Vote111111111111111111111111111111111111111";

/// Name of the cursor file [`IngestorConfig::new`] puts in the data directory.
pub const CURSOR_FILE_NAME: &str = "cursor.json";

/// Name of the skip log [`IngestorConfig::new`] puts in the data directory.
pub const SKIP_LOG_FILE_NAME: &str = "skipped_slots.log";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How the ingestor runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestorConfig {
    /// Where the Parquet files are written, under `solana/`.
    pub data_dir: PathBuf,
    /// Where the cursor file lives.
    pub cursor_path: PathBuf,
    /// Where the gap handler appends one line per skipped slot.
    pub skip_log_path: PathBuf,
    /// First slot for a fresh cursor. `None` starts at the current tip. A
    /// cursor with history ignores this and resumes from its flush edge.
    pub start_slot: Option<u64>,
    /// Last slot to ingest (inclusive). `None` follows the tip until shut
    /// down.
    pub end_slot: Option<u64>,
    /// Commitment of the tip that process follows.
    pub commitment: CommitmentLevel,
    /// Pause between tip polls once caught up, and before retrying a tip
    /// read that failed transiently.
    pub tip_poll_interval: Duration,
    /// Retry and timeout policy for fetching a slot.
    pub fetcher: SlotFetcherConfig,
    /// Processed slots that may wait between process and writer.
    pub channel_capacity: usize,
    /// Longest a slot waits in the writer's buffer before being flushed.
    /// Also the pause before retrying a failed flush.
    pub flush_interval: Duration,
    /// Buffered slots that trigger a flush before `flush_interval` is up.
    pub flush_max_slots: usize,
    /// Failed flushes in a row after which the pipeline halts.
    pub max_flush_failures: u32,
}

impl IngestorConfig {
    /// The defaults, writing under `data_dir`, with the cursor and the skip
    /// log next to the tables.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        Self {
            cursor_path: data_dir.join(CURSOR_FILE_NAME),
            skip_log_path: data_dir.join(SKIP_LOG_FILE_NAME),
            data_dir,
            start_slot: None,
            end_slot: None,
            commitment: CommitmentLevel::Confirmed,
            tip_poll_interval: Duration::from_millis(400),
            fetcher: SlotFetcherConfig::default(),
            channel_capacity: 64,
            flush_interval: Duration::from_secs(10),
            flush_max_slots: 256,
            max_flush_failures: 3,
        }
    }

    /// Check that the values can actually be used. The first offending field
    /// is reported as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        for (field, path) in [
            ("data_dir", &self.data_dir),
            ("cursor_path", &self.cursor_path),
            ("skip_log_path", &self.skip_log_path),
        ] {
            if path.as_os_str().is_empty() {
                return Err(PrismError::config(field, "must not be empty"));
            }
        }
        if let (Some(start), Some(end)) = (self.start_slot, self.end_slot)
            && end < start
        {
            return Err(PrismError::config(
                "end_slot",
                format!("must not be below start_slot {start}"),
            ));
        }
        if self.tip_poll_interval.is_zero() {
            return Err(PrismError::config("tip_poll_interval", "must be greater than 0"));
        }
        self.fetcher.validate()?;
        if self.channel_capacity == 0 {
            return Err(PrismError::config("channel_capacity", "must be greater than 0"));
        }
        if self.flush_interval.is_zero() {
            return Err(PrismError::config("flush_interval", "must be greater than 0"));
        }
        if self.flush_max_slots == 0 {
            return Err(PrismError::config("flush_max_slots", "must be greater than 0"));
        }
        if self.max_flush_failures == 0 {
            return Err(PrismError::config("max_flush_failures", "must be greater than 0"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helius
// ---------------------------------------------------------------------------

/// The Helius API key from [`HELIUS_API_KEY_ENV`].
pub fn helius_api_key_from_env() -> PrismResult<String> {
    std::env::var(HELIUS_API_KEY_ENV)
        .ok()
        .filter(|key| !key.trim().is_empty())
        .ok_or_else(|| PrismError::config(HELIUS_API_KEY_ENV, "environment variable is not set"))
}

/// PRISM's chain id for a Helius cluster.
pub const fn helius_chain_id(cluster: &Cluster) -> &'static str {
    match cluster {
        Cluster::Devnet => "solana-devnet",
        Cluster::MainnetBeta | Cluster::StakedMainnetBeta => "solana-mainnet",
    }
}

/// A [`SolanaAdapter`] whose RPC client comes from the Helius SDK.
///
/// The SDK builds the endpoint and the async Solana client; blocks are
/// requested as `base64`. The adapter layers PRISM's rate limiting, retries and error mapping on top. Fails
/// with [`PrismError::ConfigError`] when the SDK rejects the key.
pub async fn helius_solana_adapter(api_key: &str, cluster: Cluster) -> PrismResult<SolanaAdapter> {
    let chain_id = helius_chain_id(&cluster);
    let helius = HeliusBuilder::new()
        .with_api_key(api_key)
        .map_err(|err| PrismError::config(HELIUS_API_KEY_ENV, err.to_string()))?
        .with_cluster(cluster)
        .with_async_solana()
        .build()
        .await
        .map_err(|err| PrismError::config("helius", err.to_string()))?;
    let client = helius
        .async_rpc_client
        .clone()
        .ok_or_else(|| PrismError::config("helius", "SDK did not build an async client"))?;
    // `base58`, the adapter default, is refused by the RPC together with the
    // `maxSupportedTransactionVersion` the adapter always sends.
    let config = SolanaAdapterConfig {
        encoding: TransactionEncoding::Base64,
        ..SolanaAdapterConfig::new(chain_id, helius.config.build_rpc_url())
    };
    SolanaAdapter::with_shared_client(config, client)
}

// ---------------------------------------------------------------------------
// Ingestor
// ---------------------------------------------------------------------------

/// Totals of one [`Ingestor::run`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestorReport {
    /// First slot fetched, `None` when the run fetched nothing.
    pub first_slot: Option<u64>,
    /// Slots with data handed to the writer.
    pub slots_processed: u64,
    pub slots_skipped: u64,
    pub transactions_processed: u64,
    /// Successful flushes, including those that only moved the flush edge
    /// past skipped slots.
    pub flushes: u64,
    /// Rewinds executed for the reconciler during the run.
    pub rewinds: u64,
    /// Cursor state after the final flush.
    pub cursor: CursorState,
}

/// The pipeline: owns the components and runs them as one.
pub struct Ingestor {
    adapter: Arc<dyn ChainAdapter>,
    commitment_tracker: Arc<CommitmentTracker>,
    changelog: Arc<dyn ChangeLog>,
    processor: Arc<SlotProcessor>,
    decoders: Arc<DecoderRegistry>,
    resolver: Arc<AsyncMutex<AccountResolver>>,
    resolver_metrics: Arc<ResolverMetrics>,
    writer: Arc<Mutex<ParquetWriter>>,
    cursor: IngestorCursor,
    config: IngestorConfig,
    shutdown: CancellationToken,
    rewind_tx: mpsc::Sender<RewindRequest>,
    /// Held by [`Ingestor::run`] for as long as it runs.
    rewind_rx: AsyncMutex<mpsc::Receiver<RewindRequest>>,
    /// Told about the mints of every flushed transfer, when set.
    metadata: Option<MetadataHandle>,
}

impl std::fmt::Debug for Ingestor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingestor")
            .field("chain_id", &self.adapter.chain_id())
            .field("config", &self.config)
            .field("cursor", &self.cursor.state())
            .finish_non_exhaustive()
    }
}

impl Ingestor {
    /// Wire up a pipeline: builds the slot processor (whose gap handler
    /// publishes to `changelog`), a decoder registry for SPL Token and
    /// Token-2022, the account resolver and a [`ParquetWriter`] under
    /// [`IngestorConfig::data_dir`]. Loads (or initialises) the cursor. No
    /// request is made and nothing is written until [`Self::run`].
    pub fn new(
        adapter: Arc<dyn ChainAdapter>,
        commitment_tracker: Arc<CommitmentTracker>,
        changelog: Arc<dyn ChangeLog>,
        config: IngestorConfig,
    ) -> PrismResult<Self> {
        config.validate()?;
        let cursor = IngestorCursor::load_or_init(&config.cursor_path)?;
        let processor = SlotProcessor::new(
            SlotFetcher::new(Arc::clone(&adapter), config.fetcher)?,
            GapHandler::new(Arc::clone(&changelog), &config.skip_log_path),
            Normalizer::new(),
        );
        let resolver = AccountResolver::new(Arc::clone(&adapter));
        let resolver_metrics = Arc::clone(resolver.metrics());
        let (rewind_tx, rewind_rx) = mpsc::channel(1);
        Ok(Self {
            adapter,
            commitment_tracker,
            changelog,
            processor: Arc::new(processor),
            decoders: Arc::new(DecoderRegistry::with_spl_token()),
            resolver: Arc::new(AsyncMutex::new(resolver)),
            resolver_metrics,
            writer: Arc::new(Mutex::new(ParquetWriter::new(&config.data_dir))),
            cursor,
            config,
            shutdown: CancellationToken::new(),
            rewind_tx,
            rewind_rx: AsyncMutex::new(rewind_rx),
            metadata: None,
        })
    }

    /// Hint `metadata` with the mint of every token transfer, once its
    /// flush has succeeded. [`MetadataHandle::hint`] never waits, so this
    /// cannot slow the pipeline; the caller keeps its own handle to shut
    /// the enricher down.
    #[must_use]
    pub fn with_metadata(mut self, metadata: MetadataHandle) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// The token metadata enricher, when one was attached.
    pub fn metadata(&self) -> Option<&MetadataHandle> {
        self.metadata.as_ref()
    }

    /// Decode with `registry` instead of the default SPL Token registry,
    /// e.g. one with [`AnchorDecoder`](crate::decoder::AnchorDecoder)s added.
    #[must_use]
    pub fn with_decoders(mut self, registry: DecoderRegistry) -> Self {
        self.decoders = Arc::new(registry);
        self
    }

    pub fn config(&self) -> &IngestorConfig {
        &self.config
    }

    pub fn cursor(&self) -> &IngestorCursor {
        &self.cursor
    }

    pub fn commitment_tracker(&self) -> &Arc<CommitmentTracker> {
        &self.commitment_tracker
    }

    /// The slot processor, for its fetcher, gap handler and own metrics.
    pub fn processor(&self) -> &Arc<SlotProcessor> {
        &self.processor
    }

    pub fn decoders(&self) -> &DecoderRegistry {
        &self.decoders
    }

    pub fn resolver_metrics(&self) -> &Arc<ResolverMetrics> {
        &self.resolver_metrics
    }

    /// The Parquet writer's counters.
    pub fn writer_metrics(&self) -> Arc<WriterMetrics> {
        Arc::clone(lock(&self.writer).metrics())
    }

    /// Slots appended to the writer and not yet flushed.
    pub fn buffered_slot_count(&self) -> u64 {
        lock(&self.writer).buffered_slot_count()
    }

    /// Cancel this to shut the pipeline down gracefully.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    /// Request a graceful shutdown. [`Self::run`] returns once the buffer is
    /// flushed and the cursor persisted.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// The [`Rewind`] the reconciler acts through. Requests are executed by
    /// [`Self::run`]; one made while it is not running waits for it.
    pub fn rewinder(&self) -> IngestorRewinder {
        IngestorRewinder {
            tx: self.rewind_tx.clone(),
        }
    }

    /// The [`ReconcileContext`] the reconciler reads this pipeline through.
    /// `genesis_hash` is the cluster's, e.g. from
    /// [`ChainAdapter::genesis_hash`].
    pub fn reconcile_context(&self, genesis_hash: impl Into<String>) -> IngestorReconcileContext {
        IngestorReconcileContext {
            cursor: self.cursor.clone(),
            tracker: Arc::clone(&self.commitment_tracker),
            genesis_hash: genesis_hash.into(),
        }
    }

    /// The first slot this run fetches: the cursor's resume point, else
    /// [`IngestorConfig::start_slot`], else the current tip.
    pub async fn starting_slot(&self) -> PrismResult<u64> {
        if let Some(slot) = self.cursor.resume_slot() {
            return Ok(slot);
        }
        if let Some(slot) = self.config.start_slot {
            return Ok(slot);
        }
        Ok(self.adapter.get_tip(self.config.commitment).await?.slot_or_height)
    }

    /// Run the pipeline until shut down, until [`IngestorConfig::end_slot`]
    /// is ingested, or until a component fails.
    ///
    /// Before anything is fetched, a reconcile rewind a crash interrupted is
    /// finished, then files at or above the first slot are rolled back: they
    /// were written by a flush the cursor never recorded, and their slots are
    /// about to be written again.
    ///
    /// Rewinds requested through [`Self::rewinder`] are executed while the
    /// pipeline runs: it is stopped without a flush, rewound and started
    /// again, all within this call.
    ///
    /// The buffer is flushed and the cursor persisted on the way out in every
    /// case where the writer still works. The first error any task hit is
    /// returned. The commitment tracker's poller counts as one of those
    /// tasks: if the change log fails it, the pipeline halts with its error.
    ///
    /// Fails with [`PrismError::ConfigError`] when the pipeline is already
    /// running.
    pub async fn run(&self) -> PrismResult<IngestorReport> {
        let mut rewinds = self
            .rewind_rx
            .try_lock()
            .map_err(|_| PrismError::config("ingestor", "is already running"))?;
        if let Some(from_slot) = Reconciler::recover(&self.config.data_dir, &self.cursor, self.changelog.as_ref())? {
            info!(from_slot, "finished an interrupted reconcile rewind");
        }

        let mut report = IngestorReport::default();
        let mut totals = WriterTotals::default();
        loop {
            let first = self.starting_slot().await?;
            if self.config.end_slot.is_some_and(|end| end < first) {
                self.cursor.persist()?;
                break;
            }
            report.first_slot.get_or_insert(first);
            lock(&self.writer).rollback(first)?;
            info!(
                first_slot = first,
                chain = self.adapter.chain_id(),
                data_dir = %self.config.data_dir.display(),
                "ingestor starting"
            );
            let (epoch, request) = self.run_epoch(first, &mut rewinds).await?;
            totals.add(&epoch);
            let Some(request) = request else { break };
            let RewindRequest { from_slot, announce, reply } = request;
            match self.apply_rewind(from_slot, announce) {
                Ok(()) => {
                    report.rewinds += 1;
                    let _ = reply.send(Ok(()));
                }
                Err(err) => {
                    let _ = reply.send(Err(PrismError::config(
                        "ingestor",
                        format!("rewind to slot {from_slot} failed: {err}"),
                    )));
                    return Err(err);
                }
            }
            if self.shutdown.is_cancelled() {
                break;
            }
        }

        let cursor = self.cursor.state();
        info!(
            slots_processed = totals.slots_processed,
            slots_skipped = totals.slots_skipped,
            transactions_processed = totals.transactions_processed,
            rewinds = report.rewinds,
            last_flush_slot = cursor.last_flush_slot,
            "ingestor stopped"
        );
        Ok(IngestorReport {
            slots_processed: totals.slots_processed,
            slots_skipped: totals.slots_skipped,
            transactions_processed: totals.transactions_processed,
            flushes: totals.flushes,
            cursor,
            ..report
        })
    }

    /// Run both tasks from `first` until they stop, or until a rewind is
    /// requested, which stops them without a flush. Returns the writer's
    /// totals and the rewind request, if that is what stopped them.
    async fn run_epoch(
        &self,
        first: u64,
        rewinds: &mut mpsc::Receiver<RewindRequest>,
    ) -> PrismResult<(WriterTotals, Option<RewindRequest>)> {
        // A child: a rewind stops this epoch only; shutdown stops them all.
        let epoch = self.shutdown.child_token();
        let discard = CancellationToken::new();
        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let process = spawn_task(&self.shutdown, process_task(
            Arc::clone(&self.adapter),
            Arc::clone(&self.processor),
            Arc::clone(&self.decoders),
            Arc::clone(&self.resolver),
            Arc::clone(&self.commitment_tracker),
            self.config.clone(),
            first,
            tx,
            epoch.clone(),
        ));
        let writer = WriterTask {
            writer: Arc::clone(&self.writer),
            changelog: Arc::clone(&self.changelog),
            cursor: self.cursor.clone(),
            flush_max_slots: self.config.flush_max_slots as u64,
            max_flush_failures: self.config.max_flush_failures,
            pending: Vec::new(),
            metadata: self.metadata.clone(),
            pending_mints: HashMap::new(),
            high_water: None,
            consecutive_failures: 0,
            totals: WriterTotals::default(),
        };
        let writer = spawn_task(
            &self.shutdown,
            writer.run(rx, self.config.flush_interval, discard.clone()),
        );
        let finality = tokio::spawn(halt_with_tracker(
            Arc::clone(&self.commitment_tracker),
            self.shutdown.clone(),
        ));

        let pipeline = async {
            let process = self.join("process", process).await;
            let writer = self.join("writer", writer).await;
            (process, writer)
        };
        tokio::pin!(pipeline);
        let mut request: Option<RewindRequest> = None;
        let (process_result, writer_result) = loop {
            tokio::select! {
                results = &mut pipeline => break results,
                Some(next) = rewinds.recv(), if request.is_none() => {
                    if next.reply.is_closed() {
                        // Whoever asked has given up.
                        continue;
                    }
                    info!(from_slot = next.from_slot, "rewind requested; stopping the pipeline without a flush");
                    discard.cancel();
                    epoch.cancel();
                    request = Some(next);
                }
            }
        };
        finality.abort();
        // Awaited so its tracker reference is gone once `run` returns.
        let _ = finality.await;

        let failed = |request: Option<RewindRequest>| {
            if let Some(request) = request {
                let _ = request.reply.send(Err(PrismError::config(
                    "ingestor",
                    "the pipeline failed before the rewind could run",
                )));
            }
        };
        // The poller's change log failure is what stopped the pipeline, so
        // it comes first. A failed log names its original failure in every
        // later error, whichever producer reports it.
        if let Some(err) = self.commitment_tracker.take_error() {
            failed(request);
            return Err(err);
        }
        let totals = match process_result.and(writer_result) {
            Ok(totals) => totals,
            Err(err) => {
                failed(request);
                return Err(err);
            }
        };
        Ok((totals, request))
    }

    /// With the pipeline stopped: announce `announce` (if any) and flush the
    /// change log, lower the cursor so ingestion resumes at `from_slot`
    /// (never above the flush edge), then delete files and buffered rows
    /// from there. Nothing is changed unless the announcement is durable.
    /// The cursor goes before the files: if the process dies in between, the
    /// next [`Self::run`] rolls back from the lowered resume point anyway.
    fn apply_rewind(&self, from_slot: u64, announce: Option<ChangeEvent>) -> PrismResult<()> {
        if let Some(event) = announce {
            self.changelog.emit(event)?;
            self.changelog.flush()?;
        }
        let point = from_slot.min(self.cursor.state().last_flush_slot + 1);
        self.cursor.rewind(point)?;
        lock(&self.writer).rollback(point)?;
        info!(from_slot = point, "rewound; ingesting again from there");
        Ok(())
    }

    /// Await a task. A panic is reported as an error and cancels the rest
    /// of the pipeline (an error already did, see [`spawn_task`]).
    async fn join<T>(&self, name: &str, handle: JoinHandle<PrismResult<T>>) -> PrismResult<T> {
        let result = match handle.await {
            Ok(result) => result,
            Err(err) => Err(PrismError::config(
                "ingestor",
                format!("{name} task panicked: {err}"),
            )),
        };
        if let Err(err) = &result {
            warn!(task = name, error = %err, "ingestor task failed; shutting down");
            self.shutdown.cancel();
        }
        result
    }
}

fn lock(writer: &Mutex<ParquetWriter>) -> MutexGuard<'_, ParquetWriter> {
    // Appends and flushes leave the buffers whole or untouched, so a panic
    // elsewhere cannot leave the writer half-updated.
    writer.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Spawn a pipeline task that cancels `shutdown` when it fails, so the
/// other tasks wind down right away instead of when the failure is joined.
fn spawn_task<T: Send + 'static>(
    shutdown: &CancellationToken,
    task: impl Future<Output = PrismResult<T>> + Send + 'static,
) -> JoinHandle<PrismResult<T>> {
    let shutdown = shutdown.clone();
    tokio::spawn(async move {
        let result = task.await;
        if result.is_err() {
            shutdown.cancel();
        }
        result
    })
}

/// Shut the pipeline down if the commitment tracker's poller halts.
async fn halt_with_tracker(tracker: Arc<CommitmentTracker>, shutdown: CancellationToken) {
    tokio::select! {
        () = tracker.halted() => {
            warn!("finality poller halted; shutting down");
            shutdown.cancel();
        }
        () = shutdown.cancelled() => {}
    }
}

// ---------------------------------------------------------------------------
// Process task
// ---------------------------------------------------------------------------

/// Follow the tip and send every slot from `next` upwards, processed,
/// decoded and resolved.
#[allow(clippy::too_many_arguments)]
async fn process_task(
    adapter: Arc<dyn ChainAdapter>,
    processor: Arc<SlotProcessor>,
    decoders: Arc<DecoderRegistry>,
    resolver: Arc<AsyncMutex<AccountResolver>>,
    tracker: Arc<CommitmentTracker>,
    config: IngestorConfig,
    mut next: u64,
    tx: mpsc::Sender<ProcessedSlot>,
    shutdown: CancellationToken,
) -> PrismResult<()> {
    loop {
        if config.end_slot.is_some_and(|end| next > end) {
            return Ok(());
        }
        let tip = tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            tip = adapter.get_tip(config.commitment) => tip,
        };
        let tip = match tip {
            Ok(tip) => tip.slot_or_height,
            Err(err) if err.is_retryable() => {
                warn!(error = %err, "tip unavailable; retrying");
                if !pause(&shutdown, config.tip_poll_interval).await {
                    return Ok(());
                }
                continue;
            }
            Err(err) => return Err(err),
        };
        let upto = config.end_slot.map_or(tip, |end| end.min(tip));
        while next <= upto {
            // A finalized slot is read at `finalized`, so a re-ingest never
            // fetches a block from an abandoned fork.
            let commitment = (config.commitment != CommitmentLevel::Finalized
                && tracker.is_finalized(next))
            .then_some(CommitmentLevel::Finalized);
            let processed = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                processed = process_slot(&processor, &decoders, &resolver, next, commitment) => processed?,
            };
            let sent = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                sent = tx.send(processed) => sent,
            };
            if sent.is_err() {
                // The writer is gone; its own error is what gets reported.
                return Ok(());
            }
            next += 1;
        }
        if config.end_slot.is_some_and(|end| next > end) {
            return Ok(());
        }
        if !pause(&shutdown, config.tip_poll_interval).await {
            return Ok(());
        }
    }
}

/// `slot` through the slot processor, then, when it has data, its
/// instructions decoded and its token transfers resolved into
/// [`ProcessedSlot::udm_transfers`].
///
/// Only transfers of transactions that succeeded are kept: a failed
/// transaction is rolled back whole, so its transfer instructions moved
/// nothing on chain (their instruction rows are still written, decoded).
/// The rest are completed from the transactions' own token balances first
/// and the mint accounts second; see
/// [`AccountResolver::resolve_with_metadata`].
async fn process_slot(
    processor: &SlotProcessor,
    decoders: &DecoderRegistry,
    resolver: &AsyncMutex<AccountResolver>,
    slot: u64,
    commitment: Option<CommitmentLevel>,
) -> PrismResult<ProcessedSlot> {
    let mut processed = processor.process_at(slot, commitment).await?;
    let Some(udm_slot) = processed.udm_slot.take() else {
        return Ok(processed);
    };
    let mut block = NormalizedBlock {
        slot: udm_slot,
        transactions: std::mem::take(&mut processed.udm_transactions),
        instructions: std::mem::take(&mut processed.udm_instructions),
        token_balances: Vec::new(),
    };
    let transfers = executed_transfers(decoders.decode_block(&mut block), &block.transactions);
    // Read once here; the writer has no use for them.
    let metadata = TokenAccountIndex::from_balances(&std::mem::take(&mut processed.token_balances));
    processed.udm_transfers = resolver
        .lock()
        .await
        .resolve_with_metadata(transfers, &metadata)
        .await?;
    processed.udm_slot = Some(block.slot);
    processed.udm_transactions = block.transactions;
    processed.udm_instructions = block.instructions;
    Ok(processed)
}

/// `transfers` without those of transactions that failed.
pub fn executed_transfers(
    transfers: Vec<DeepAnaTokenTransfer>,
    transactions: &[DeepAnaTransaction],
) -> Vec<DeepAnaTokenTransfer> {
    let failed: std::collections::HashSet<&str> = transactions
        .iter()
        .filter(|tx| !tx.is_success())
        .map(|tx| tx.tx_signature.as_str())
        .collect();
    if failed.is_empty() {
        return transfers;
    }
    transfers
        .into_iter()
        .filter(|transfer| !failed.contains(transfer.tx_signature.as_str()))
        .collect()
}

/// Sleep for `duration`; `false` when shutdown came first.
async fn pause(shutdown: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

// ---------------------------------------------------------------------------
// Writer task
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct WriterTotals {
    slots_processed: u64,
    slots_skipped: u64,
    transactions_processed: u64,
    flushes: u64,
}

impl WriterTotals {
    fn add(&mut self, other: &Self) {
        self.slots_processed += other.slots_processed;
        self.slots_skipped += other.slots_skipped;
        self.transactions_processed += other.transactions_processed;
        self.flushes += other.flushes;
    }
}

/// The consuming end of the pipeline: buffers processed slots in the
/// [`ParquetWriter`], flushes them, announces them and moves the cursor.
struct WriterTask {
    writer: Arc<Mutex<ParquetWriter>>,
    changelog: Arc<dyn ChangeLog>,
    cursor: IngestorCursor,
    flush_max_slots: u64,
    max_flush_failures: u32,
    /// Slot rows appended since the last successful flush, in slot order:
    /// each gets a `New` event once its files are written.
    pending: Vec<DeepAnaSlot>,
    metadata: Option<MetadataHandle>,
    /// Mints of the transfers appended since the last successful flush,
    /// with the highest slot each was seen in. Hinted after the flush;
    /// dropped with the rest of the epoch on a rewind.
    pending_mints: HashMap<Pubkey, u64>,
    /// Highest slot received since the last successful flush, skipped slots
    /// included. The flush edge moves here.
    high_water: Option<u64>,
    consecutive_failures: u32,
    totals: WriterTotals,
}

impl WriterTask {
    /// Consume the channel until process hangs up, then flush what is left
    /// and persist the cursor.
    ///
    /// Once `discard` is cancelled (a rewind), it stops as soon as it can and
    /// flushes nothing more: the buffered slots are about to be rolled back,
    /// and announcing them would point consumers at files that are deleted
    /// right after.
    async fn run(
        mut self,
        mut rx: mpsc::Receiver<ProcessedSlot>,
        interval: Duration,
        discard: CancellationToken,
    ) -> PrismResult<WriterTotals> {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                () = discard.cancelled() => return Ok(self.totals),
                processed = rx.recv() => {
                    let Some(processed) = processed else { break };
                    if self.receive(processed)? && !discard.is_cancelled() {
                        self.flush(false).await?;
                    }
                }
                _ = ticker.tick() => {
                    if !discard.is_cancelled() {
                        self.flush(false).await?;
                    }
                }
            }
        }
        if discard.is_cancelled() {
            return Ok(self.totals);
        }
        self.flush(true).await?;
        self.cursor.persist()?;
        Ok(self.totals)
    }

    /// Take one processed slot in. Returns whether the buffer is full enough
    /// to flush now; not after a failed flush, which waits for the next
    /// interval instead of retrying on every slot.
    fn receive(&mut self, processed: ProcessedSlot) -> PrismResult<bool> {
        let slot = processed.slot;
        self.high_water = Some(slot);
        if processed.was_skipped {
            // The slot processor's gap handler has already published the
            // `Skip` event and written the skip log.
            self.cursor.record_skipped(slot);
            self.totals.slots_skipped += 1;
            return Ok(false);
        }
        let Some(udm_slot) = processed.udm_slot else {
            return Err(PrismError::deserialize(
                Some(slot),
                "processed slot",
                "not skipped, yet it has no slot row",
            ));
        };
        let buffered = {
            let mut writer = lock(&self.writer);
            writer.append(
                &udm_slot,
                &processed.udm_transactions,
                &processed.udm_instructions,
                &processed.udm_transfers,
            )?;
            writer.buffered_slot_count()
        };
        if self.metadata.is_some() {
            for mint in processed.udm_transfers.iter().filter_map(|t| t.mint_token_address.as_deref()) {
                // A mint that is not a pubkey has no metadata to look up.
                if let Ok(mint) = Pubkey::from_str(mint) {
                    let seen = self.pending_mints.entry(mint).or_insert(slot);
                    *seen = (*seen).max(slot);
                }
            }
        }
        self.cursor.record_processed(slot, &udm_slot.blockhash);
        self.totals.slots_processed += 1;
        self.totals.transactions_processed += processed.udm_transactions.len() as u64;
        self.pending.push(udm_slot);
        Ok(self.consecutive_failures == 0 && buffered >= self.flush_max_slots)
    }

    /// Flush the writer, emit a `New` event per flushed slot, then move the
    /// flush edge. Nothing to do when no slot came in since the last flush.
    ///
    /// A failed flush is logged and left for the next call, unless it is the
    /// `last` one or the `max_flush_failures`th in a row, which is returned.
    /// A change log failure (emit or flush) is returned at once: the cursor
    /// has not moved, so the slots are written and announced again after a
    /// restart.
    async fn flush(&mut self, last: bool) -> PrismResult<()> {
        let Some(high_water) = self.high_water else {
            return Ok(());
        };
        let flushed = lock(&self.writer).flush();
        let result = match flushed {
            Ok(result) => result,
            Err(err) => {
                self.consecutive_failures += 1;
                if last || self.consecutive_failures >= self.max_flush_failures {
                    return Err(err);
                }
                warn!(
                    failures = self.consecutive_failures,
                    max = self.max_flush_failures,
                    error = %err,
                    "flush failed; rows stay buffered until the next interval"
                );
                return Ok(());
            }
        };
        self.consecutive_failures = 0;
        for slot in &self.pending {
            let event = ChangeEvent::new_from_slot(slot, result.parquet_paths.clone());
            self.changelog.emit(event)?;
        }
        // Durable before the cursor moves past them, and with them every
        // `Skip` the gap handler emitted for the slots below the edge.
        self.changelog.flush()?;
        self.pending.clear();
        self.cursor.record_flush(high_water)?;
        // Only now are the transfers on disk and announced. `hint` never
        // waits: a full queue drops the hint, never this task's time.
        if let Some(metadata) = &self.metadata {
            for (mint, slot) in self.pending_mints.drain() {
                metadata.hint(mint, slot);
            }
        }
        self.high_water = None;
        self.totals.flushes += 1;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Reconcile hooks
// ---------------------------------------------------------------------------

/// A rewind asked of [`Ingestor::run`], with where to send the outcome.
struct RewindRequest {
    from_slot: u64,
    /// Emitted once the pipeline has stopped, before anything is deleted.
    announce: Option<ChangeEvent>,
    reply: oneshot::Sender<PrismResult<()>>,
}

/// The ingestor's [`Rewind`]: hands the request to [`Ingestor::run`] and
/// waits until it has lowered the cursor and deleted the files.
#[derive(Debug, Clone)]
pub struct IngestorRewinder {
    tx: mpsc::Sender<RewindRequest>,
}

impl IngestorRewinder {
    async fn request(&self, from_slot: u64, announce: Option<ChangeEvent>) -> PrismResult<()> {
        let (reply, outcome) = oneshot::channel();
        self.tx
            .send(RewindRequest { from_slot, announce, reply })
            .await
            .map_err(|_| PrismError::config("ingestor", "is gone; the rewind was not executed"))?;
        outcome.await.map_err(|_| {
            PrismError::config("ingestor", "stopped before executing the rewind")
        })?
    }
}

#[async_trait]
impl Rewind for IngestorRewinder {
    async fn rewind_to(&self, from_slot: u64) -> PrismResult<()> {
        self.request(from_slot, None).await
    }

    /// Stops the pipeline first, then announces on the ingestor's own change
    /// log (the one its `New` and `Skip` events go to; `_changelog` should be
    /// that same log), then rewinds.
    async fn correct(&self, correction: &InFlight, _changelog: &dyn ChangeLog) -> PrismResult<()> {
        self.request(correction.from_slot, Some(correction.event())).await
    }
}

/// The ingestor's [`ReconcileContext`]: the cursor's durable flush edge and
/// processed edge, the commitment tracker's *durable* finalized watermark,
/// and the genesis hash it was built with.
///
/// The durable watermark (see
/// [`CommitmentTracker::durable_finalized_slot`]) is what a restart starts
/// from, so every slot the reconciler acts on is still known to be finalized
/// after a crash, and its re-ingest is read at `finalized` from the first
/// slot on.
#[derive(Clone)]
pub struct IngestorReconcileContext {
    cursor: IngestorCursor,
    tracker: Arc<CommitmentTracker>,
    genesis_hash: String,
}

impl std::fmt::Debug for IngestorReconcileContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestorReconcileContext")
            .field("cursor", &self.cursor.state())
            .field("finalized_slot", &self.tracker.durable_finalized_slot())
            .field("genesis_hash", &self.genesis_hash)
            .finish()
    }
}

impl ReconcileContext for IngestorReconcileContext {
    fn durable_last_flush_slot(&self) -> u64 {
        self.cursor.durable_flush_slot()
    }

    fn cursor_slot(&self) -> u64 {
        self.cursor.state().last_processed_slot
    }

    fn finalized_slot(&self) -> u64 {
        self.tracker.durable_finalized_slot()
    }

    fn genesis_hash(&self) -> String {
        self.genesis_hash.clone()
    }
}
