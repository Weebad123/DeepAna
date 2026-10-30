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
//!   it.
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

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use helius::HeliusBuilder;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::account_resolver::{AccountResolver, ResolverMetrics};
use crate::adapter::config::{SolanaAdapterConfig, TransactionEncoding};
use crate::adapter::solana::SolanaAdapter;
use crate::adapter::{ChainAdapter, CommitmentLevel};
use crate::changelog::ChangeLog;
use crate::commitment::CommitmentTracker;
use crate::decoder::DecoderRegistry;
use crate::normalizer::{NormalizedBlock, Normalizer};
use crate::slot_processor::{GapHandler, ProcessedSlot, SlotFetcher, SlotFetcherConfig, SlotProcessor};
use crate::storage::{ParquetWriter, Storage, WriterMetrics};
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::ChangeEvent;
use crate::types::udm::DeepAnaSlot;

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
        })
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
    /// Before anything is fetched, files at or above the first slot are
    /// rolled back: they were written by a flush the cursor never recorded,
    /// and their slots are about to be written again.
    ///
    /// The buffer is flushed and the cursor persisted on the way out in every
    /// case where the writer still works. The first error any task hit is
    /// returned.
    pub async fn run(&self) -> PrismResult<IngestorReport> {
        let first = self.starting_slot().await?;
        if self.config.end_slot.is_some_and(|end| end < first) {
            self.cursor.persist()?;
            return Ok(IngestorReport {
                cursor: self.cursor.state(),
                ..IngestorReport::default()
            });
        }
        lock(&self.writer).rollback(first)?;
        info!(
            first_slot = first,
            chain = self.adapter.chain_id(),
            data_dir = %self.config.data_dir.display(),
            "ingestor starting"
        );

        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let process = spawn_task(&self.shutdown, process_task(
            Arc::clone(&self.adapter),
            Arc::clone(&self.processor),
            Arc::clone(&self.decoders),
            Arc::clone(&self.resolver),
            self.config.clone(),
            first,
            tx,
            self.shutdown.clone(),
        ));
        let writer = WriterTask {
            writer: Arc::clone(&self.writer),
            changelog: Arc::clone(&self.changelog),
            cursor: self.cursor.clone(),
            flush_max_slots: self.config.flush_max_slots as u64,
            max_flush_failures: self.config.max_flush_failures,
            pending: Vec::new(),
            high_water: None,
            consecutive_failures: 0,
            totals: WriterTotals::default(),
        };
        let writer = spawn_task(&self.shutdown, writer.run(rx, self.config.flush_interval));

        let process_result = self.join("process", process).await;
        let writer_result = self.join("writer", writer).await;
        process_result?;
        let totals = writer_result?;
        let cursor = self.cursor.state();
        info!(
            slots_processed = totals.slots_processed,
            slots_skipped = totals.slots_skipped,
            transactions_processed = totals.transactions_processed,
            last_flush_slot = cursor.last_flush_slot,
            "ingestor stopped"
        );
        Ok(IngestorReport {
            first_slot: Some(first),
            slots_processed: totals.slots_processed,
            slots_skipped: totals.slots_skipped,
            transactions_processed: totals.transactions_processed,
            flushes: totals.flushes,
            cursor,
        })
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
            let processed = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                processed = process_slot(&processor, &decoders, &resolver, next) => processed?,
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
async fn process_slot(
    processor: &SlotProcessor,
    decoders: &DecoderRegistry,
    resolver: &AsyncMutex<AccountResolver>,
    slot: u64,
) -> PrismResult<ProcessedSlot> {
    let mut processed = processor.process(slot).await?;
    let Some(udm_slot) = processed.udm_slot.take() else {
        return Ok(processed);
    };
    let mut block = NormalizedBlock {
        slot: udm_slot,
        transactions: std::mem::take(&mut processed.udm_transactions),
        instructions: std::mem::take(&mut processed.udm_instructions),
    };
    let transfers = decoders.decode_block(&mut block);
    processed.udm_transfers = resolver.lock().await.resolve(transfers).await?;
    processed.udm_slot = Some(block.slot);
    processed.udm_transactions = block.transactions;
    processed.udm_instructions = block.instructions;
    Ok(processed)
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
    /// Highest slot received since the last successful flush, skipped slots
    /// included. The flush edge moves here.
    high_water: Option<u64>,
    consecutive_failures: u32,
    totals: WriterTotals,
}

impl WriterTask {
    /// Consume the channel until process hangs up, then flush what is left
    /// and persist the cursor.
    async fn run(
        mut self,
        mut rx: mpsc::Receiver<ProcessedSlot>,
        interval: Duration,
    ) -> PrismResult<WriterTotals> {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            tokio::select! {
                processed = rx.recv() => {
                    let Some(processed) = processed else { break };
                    if self.receive(processed)? {
                        self.flush(false).await?;
                    }
                }
                _ = ticker.tick() => self.flush(false).await?,
            }
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
    /// A change log failure is returned at once: the cursor has not moved,
    /// so the slots are written and announced again after a restart.
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
            self.changelog.emit(event).await?;
        }
        self.pending.clear();
        self.cursor.record_flush(high_water)?;
        self.high_water = None;
        self.totals.flushes += 1;
        Ok(())
    }
}
