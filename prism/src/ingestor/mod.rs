//! The ingestor: the orchestration layer that turns PRISM's components into
//! a running pipeline.
//!
//! Everything else in the crate is a component. [`Ingestor`] owns them, wires
//! them together and drives them as three concurrent Tokio tasks:
//!
//! ```text
//!   fetch ──(bounded channel)──▶ process ──(SlotBuffer)──▶ flush ──▶ FlushSink
//!     │                            │                         │
//!   adapter.get_tip / get_block  cursor.record_processed   cursor.record_flush
//! ```
//!
//! * **fetch** follows the chain tip at [`IngestorConfig::commitment`] and
//!   fetches every slot from the resume point upwards, in order. Skipped
//!   slots are passed on as such; transient RPC failures are retried after
//!   [`IngestorConfig::tip_poll_interval`]; anything else halts the pipeline.
//! * **process** decodes each block into a [`BufferedSlot`], stamps it with
//!   the [`CommitmentTracker`]'s finality, appends it to the in-memory
//!   [`SlotBuffer`] and advances the cursor's processed edge.
//! * **flush** drains the buffer into the [`FlushSink`] every
//!   [`IngestorConfig::flush_interval`], or sooner once
//!   [`IngestorConfig::flush_max_slots`] slots are waiting, and then moves
//!   the cursor's flush edge, which persists it.
//!
//! | File        | Holds                                                    |
//! |-------------|----------------------------------------------------------|
//! | `mod.rs`    | [`Ingestor`], its config, the buffer and the Helius setup |
//! | [`cursor`]  | [`IngestorCursor`], the persisted resume point            |
//!
//! Shutdown is graceful and cascades down the pipe: cancelling the token
//! stops fetch, whose dropped sender lets process drain the channel and
//! exit, after which flush writes whatever is left and persists the cursor.
//! The same cascade runs when fetch reaches [`IngestorConfig::end_slot`].
//!
//! Until the Parquet writer exists, [`LogSink`] stands in for it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use helius::HeliusBuilder;
use helius::types::Cluster;
use solana_sdk::message::VersionedMessage;
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, UiMessage, UiTransactionStatusMeta,
};
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::adapter::config::{SolanaAdapterConfig, TransactionEncoding};
use crate::adapter::solana::SolanaAdapter;
use crate::adapter::{ChainAdapter, ChainBlock, ChainBlockData, CommitmentLevel};
use crate::commitment::CommitmentTracker;
use crate::types::error::{PrismError, PrismResult};

pub mod cursor;

pub use cursor::{CursorState, IngestorCursor};

/// Environment variable the Helius API key is read from.
pub const HELIUS_API_KEY_ENV: &str = "HELIUS_API_KEY";

/// Base58 id of the Solana vote program.
pub const VOTE_PROGRAM_ID: &str = "Vote111111111111111111111111111111111111111";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How the ingestor runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestorConfig {
    /// Where the cursor file lives.
    pub cursor_path: PathBuf,
    /// First slot for a fresh cursor. `None` starts at the current tip. A
    /// cursor with history ignores this and resumes from its flush edge.
    pub start_slot: Option<u64>,
    /// Last slot to ingest (inclusive). `None` follows the tip until shut
    /// down.
    pub end_slot: Option<u64>,
    /// Commitment of the tip that fetch follows.
    pub commitment: CommitmentLevel,
    /// Pause between tip polls once caught up, and before retrying a slot
    /// that failed transiently.
    pub tip_poll_interval: Duration,
    /// Blocks that may wait between fetch and process.
    pub channel_capacity: usize,
    /// Longest a processed slot waits in the buffer before being flushed.
    pub flush_interval: Duration,
    /// Buffered slots that trigger a flush before `flush_interval` is up.
    pub flush_max_slots: usize,
}

impl IngestorConfig {
    /// The defaults, with the cursor at `cursor_path`.
    pub fn new(cursor_path: impl Into<PathBuf>) -> Self {
        Self {
            cursor_path: cursor_path.into(),
            start_slot: None,
            end_slot: None,
            commitment: CommitmentLevel::Confirmed,
            tip_poll_interval: Duration::from_millis(400),
            channel_capacity: 64,
            flush_interval: Duration::from_secs(10),
            flush_max_slots: 256,
        }
    }

    /// Check that the values can actually be used. The first offending field
    /// is reported as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        if self.cursor_path.as_os_str().is_empty() {
            return Err(PrismError::config("cursor_path", "must not be empty"));
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
        if self.channel_capacity == 0 {
            return Err(PrismError::config("channel_capacity", "must be greater than 0"));
        }
        if self.flush_interval.is_zero() {
            return Err(PrismError::config("flush_interval", "must be greater than 0"));
        }
        if self.flush_max_slots == 0 {
            return Err(PrismError::config("flush_max_slots", "must be greater than 0"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Buffered data
// ---------------------------------------------------------------------------

/// One transaction as held in the buffer, waiting to be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedTransaction {
    /// Position within the block.
    pub index: usize,
    /// First signature, base58: the transaction id.
    pub signature: String,
    /// `"legacy"`, `"0"` or `"1"`; `"unknown"` for JSON encodings.
    pub version: String,
    /// Whether it executed without error.
    pub success: bool,
    /// The on-chain error, when it failed.
    pub error: Option<String>,
    /// Fee paid, in lamports.
    pub fee: u64,
    /// Compute units consumed, when the node reports it.
    pub compute_units: Option<u64>,
    /// Top-level instructions.
    pub instruction_count: usize,
    /// Static account keys (address lookup table entries not included).
    pub account_count: usize,
    /// Base58 ids of the programs invoked by top-level instructions, in
    /// first-seen order.
    pub program_ids: Vec<String>,
    /// Whether this is a validator vote.
    pub is_vote: bool,
}

/// One slot as held in the buffer, waiting to be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedSlot {
    pub slot: u64,
    pub blockhash: String,
    pub parent_blockhash: String,
    /// Unix timestamp in seconds, when the chain reports one.
    pub block_time: Option<i64>,
    /// Finality at the time the slot was buffered.
    pub is_finalized: bool,
    pub transactions: Vec<BufferedTransaction>,
}

impl BufferedSlot {
    /// Decode `block` into its buffered form.
    ///
    /// Fails with [`PrismError::DeserializeError`] when a transaction cannot
    /// be decoded.
    pub fn from_block(block: &ChainBlock, is_finalized: bool) -> PrismResult<Self> {
        let ChainBlockData::Solana(raw) = &block.raw;
        let slot = block.slot_or_height;
        let transactions = raw
            .transactions
            .iter()
            .enumerate()
            .map(|(index, tx)| decode_transaction(slot, index, tx))
            .collect::<PrismResult<_>>()?;
        Ok(Self {
            slot,
            blockhash: block.block_id.clone(),
            parent_blockhash: block.parent_id.clone(),
            block_time: block.timestamp,
            is_finalized,
            transactions,
        })
    }

    /// Transactions that are not validator votes.
    pub fn non_vote_transactions(&self) -> impl Iterator<Item = &BufferedTransaction> {
        self.transactions.iter().filter(|tx| !tx.is_vote)
    }
}

/// Signature, version and the shape of the message, whatever the encoding.
struct DecodedMessage {
    signature: String,
    version: String,
    account_keys: Vec<String>,
    program_ids: Vec<String>,
    instruction_count: usize,
}

fn decode_transaction(
    slot: u64,
    index: usize,
    tx: &EncodedTransactionWithStatusMeta,
) -> PrismResult<BufferedTransaction> {
    let message = decode_message(&tx.transaction).ok_or_else(|| {
        PrismError::deserialize(
            Some(slot),
            "transaction",
            format!("transaction {index} has an undecodable encoding"),
        )
    })?;
    let meta: Option<&UiTransactionStatusMeta> = tx.meta.as_ref();
    let mut program_ids: Vec<String> = Vec::new();
    for id in message.program_ids {
        if !program_ids.contains(&id) {
            program_ids.push(id);
        }
    }
    Ok(BufferedTransaction {
        index,
        signature: message.signature,
        version: message.version,
        success: meta.is_none_or(|meta| meta.err.is_none()),
        error: meta.and_then(|meta| meta.err.as_ref().map(|err| format!("{err:?}"))),
        fee: meta.map_or(0, |meta| meta.fee),
        compute_units: meta.and_then(|meta| meta.compute_units_consumed.clone().into()),
        instruction_count: message.instruction_count,
        account_count: message.account_keys.len(),
        is_vote: program_ids.iter().any(|id| id == VOTE_PROGRAM_ID),
        program_ids,
    })
}

fn decode_message(tx: &EncodedTransaction) -> Option<DecodedMessage> {
    if let EncodedTransaction::Json(ui) = tx {
        let signature = ui.signatures.first()?.clone();
        return Some(match &ui.message {
            UiMessage::Raw(raw) => DecodedMessage {
                signature,
                version: String::from("unknown"),
                program_ids: raw
                    .instructions
                    .iter()
                    .filter_map(|ix| raw.account_keys.get(usize::from(ix.program_id_index)))
                    .cloned()
                    .collect(),
                instruction_count: raw.instructions.len(),
                account_keys: raw.account_keys.clone(),
            },
            UiMessage::Parsed(parsed) => DecodedMessage {
                signature,
                version: String::from("unknown"),
                // Parsed instructions name their program in differing shapes;
                // the JSON encodings are not what the pipeline asks for.
                program_ids: Vec::new(),
                instruction_count: parsed.instructions.len(),
                account_keys: parsed.account_keys.iter().map(|key| key.pubkey.clone()).collect(),
            },
        });
    }
    let decoded = tx.decode()?;
    let account_keys: Vec<String> = decoded
        .message
        .static_account_keys()
        .iter()
        .map(ToString::to_string)
        .collect();
    let program_ids = decoded
        .message
        .instructions()
        .iter()
        .filter_map(|ix| account_keys.get(usize::from(ix.program_id_index)))
        .cloned()
        .collect();
    Some(DecodedMessage {
        signature: decoded.signatures.first()?.to_string(),
        version: match decoded.message {
            VersionedMessage::Legacy(_) => String::from("legacy"),
            VersionedMessage::V0(_) => String::from("0"),
            VersionedMessage::V1(_) => String::from("1"),
        },
        instruction_count: decoded.message.instructions().len(),
        account_keys,
        program_ids,
    })
}

/// The processed-but-not-flushed window, shared by process and flush.
#[derive(Debug, Default)]
pub struct SlotBuffer {
    inner: Mutex<BufferInner>,
    filled: Notify,
}

#[derive(Debug, Default)]
struct BufferInner {
    slots: Vec<BufferedSlot>,
    /// Highest slot processed into the buffer, skipped slots included.
    high_water: Option<u64>,
}

/// What one drain of the buffer took out.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drained {
    pub slots: Vec<BufferedSlot>,
    /// Highest slot covered, skipped slots included. The flush edge moves
    /// here once `slots` are written.
    pub high_water: u64,
}

impl SlotBuffer {
    /// A copy of what is currently buffered.
    pub fn snapshot(&self) -> Vec<BufferedSlot> {
        self.lock().slots.clone()
    }

    /// Buffered slots (skipped slots are not buffered).
    pub fn len(&self) -> usize {
        self.lock().slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn push(&self, slot: BufferedSlot, flush_at: usize) {
        let len = {
            let mut inner = self.lock();
            inner.high_water = Some(slot.slot);
            inner.slots.push(slot);
            inner.slots.len()
        };
        if len >= flush_at {
            self.filled.notify_one();
        }
    }

    fn mark_skipped(&self, slot: u64) {
        self.lock().high_water = Some(slot);
    }

    /// Take everything out. `None` when nothing was processed since the last
    /// drain.
    fn drain(&self) -> Option<Drained> {
        let mut inner = self.lock();
        let high_water = inner.high_water.take()?;
        Some(Drained {
            slots: std::mem::take(&mut inner.slots),
            high_water,
        })
    }

    /// Put a drain back after a failed flush, ahead of anything newer.
    fn restore(&self, drained: Drained) {
        let mut inner = self.lock();
        let mut slots = drained.slots;
        slots.append(&mut inner.slots);
        inner.slots = slots;
        inner.high_water = Some(inner.high_water.map_or(drained.high_water, |h| h.max(drained.high_water)));
    }

    fn lock(&self) -> MutexGuard<'_, BufferInner> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

// ---------------------------------------------------------------------------
// Sink
// ---------------------------------------------------------------------------

/// Where flushed slots go. The Parquet writer will implement this.
#[async_trait]
pub trait FlushSink: Send + Sync {
    /// Write `slots` durably. Returns once they are on disk; an `Err` means
    /// they are not, and the cursor's flush edge does not move.
    async fn flush(&self, slots: &[BufferedSlot]) -> PrismResult<()>;
}

/// A sink that only logs what it is given: the stand-in until the Parquet
/// writer exists.
#[derive(Debug, Default, Clone, Copy)]
pub struct LogSink;

#[async_trait]
impl FlushSink for LogSink {
    async fn flush(&self, slots: &[BufferedSlot]) -> PrismResult<()> {
        let transactions: usize = slots.iter().map(|slot| slot.transactions.len()).sum();
        info!(
            slots = slots.len(),
            transactions,
            first_slot = slots.first().map(|slot| slot.slot),
            last_slot = slots.last().map(|slot| slot.slot),
            "flushed (log sink)"
        );
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

/// What fetch hands to process.
#[derive(Debug)]
enum Fetched {
    Block(Box<ChainBlock>),
    Skipped(u64),
}

/// Totals of one [`Ingestor::run`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IngestorReport {
    /// First slot fetched, `None` when the run fetched nothing.
    pub first_slot: Option<u64>,
    pub slots_processed: u64,
    pub slots_skipped: u64,
    pub transactions_processed: u64,
    pub flushes: u64,
    /// Cursor state after the final flush.
    pub cursor: CursorState,
}

/// The pipeline: owns the components and runs them as one.
pub struct Ingestor {
    adapter: Arc<dyn ChainAdapter>,
    commitment_tracker: Arc<CommitmentTracker>,
    cursor: IngestorCursor,
    config: IngestorConfig,
    shutdown: CancellationToken,
    buffer: Arc<SlotBuffer>,
    sink: Arc<dyn FlushSink>,
}

impl Ingestor {
    /// Wire up a pipeline. Loads (or initialises) the cursor; no request is
    /// made until [`Self::run`].
    pub fn new(
        adapter: Arc<dyn ChainAdapter>,
        commitment_tracker: Arc<CommitmentTracker>,
        config: IngestorConfig,
        sink: Arc<dyn FlushSink>,
    ) -> PrismResult<Self> {
        config.validate()?;
        let cursor = IngestorCursor::load_or_init(&config.cursor_path)?;
        Ok(Self {
            adapter,
            commitment_tracker,
            cursor,
            config,
            shutdown: CancellationToken::new(),
            buffer: Arc::new(SlotBuffer::default()),
            sink,
        })
    }

    pub fn config(&self) -> &IngestorConfig {
        &self.config
    }

    pub fn cursor(&self) -> &IngestorCursor {
        &self.cursor
    }

    /// The in-memory window of processed, not yet flushed slots.
    pub fn buffer(&self) -> &Arc<SlotBuffer> {
        &self.buffer
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
    /// The buffer is flushed and the cursor persisted on the way out in every
    /// case where the sink still works. The first error any task hit is
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
        info!(first_slot = first, chain = self.adapter.chain_id(), "ingestor starting");

        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        // Cancelled by process on exit: tells flush that nothing more comes.
        let drained = CancellationToken::new();

        let fetch = spawn_task(&self.shutdown, fetch_task(
            Arc::clone(&self.adapter),
            self.config.clone(),
            first,
            tx,
            self.shutdown.clone(),
        ));
        let process = spawn_task(&self.shutdown, process_task(
            Arc::clone(&self.commitment_tracker),
            self.cursor.clone(),
            Arc::clone(&self.buffer),
            self.config.flush_max_slots,
            rx,
            drained.clone(),
        ));
        let flush = spawn_task(&self.shutdown, flush_task(
            Arc::clone(&self.sink),
            self.cursor.clone(),
            Arc::clone(&self.buffer),
            self.config.flush_interval,
            drained,
        ));

        let fetch_result = self.join("fetch", fetch).await;
        let process_result = self.join("process", process).await;
        let flush_result = self.join("flush", flush).await;

        let slots_skipped = fetch_result?;
        let (slots_processed, transactions_processed) = process_result?;
        let flushes = flush_result?;
        let cursor = self.cursor.state();
        info!(
            slots_processed,
            slots_skipped,
            transactions_processed,
            last_flush_slot = cursor.last_flush_slot,
            "ingestor stopped"
        );
        Ok(IngestorReport {
            first_slot: Some(first),
            slots_processed,
            slots_skipped,
            transactions_processed,
            flushes,
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

/// Follow the tip and send every slot from `next` upwards. Returns the
/// number of skipped slots.
async fn fetch_task(
    adapter: Arc<dyn ChainAdapter>,
    config: IngestorConfig,
    mut next: u64,
    tx: mpsc::Sender<Fetched>,
    shutdown: CancellationToken,
) -> PrismResult<u64> {
    let mut skipped = 0;
    loop {
        if config.end_slot.is_some_and(|end| next > end) {
            return Ok(skipped);
        }
        let tip = tokio::select! {
            () = shutdown.cancelled() => return Ok(skipped),
            tip = adapter.get_tip(config.commitment) => tip,
        };
        let tip = match tip {
            Ok(tip) => tip.slot_or_height,
            Err(err) if err.is_retryable() => {
                warn!(error = %err, "tip unavailable; retrying");
                if !pause(&shutdown, config.tip_poll_interval).await {
                    return Ok(skipped);
                }
                continue;
            }
            Err(err) => return Err(err),
        };
        let upto = config.end_slot.map_or(tip, |end| end.min(tip));
        while next <= upto {
            let block = tokio::select! {
                () = shutdown.cancelled() => return Ok(skipped),
                block = adapter.get_block(next) => block,
            };
            let item = match block {
                Ok(block) => Fetched::Block(Box::new(block)),
                Err(PrismError::SlotSkipped { slot }) => {
                    skipped += 1;
                    Fetched::Skipped(slot)
                }
                Err(err) if err.is_retryable() => {
                    warn!(slot = next, error = %err, "block unavailable; retrying");
                    if !pause(&shutdown, config.tip_poll_interval).await {
                        return Ok(skipped);
                    }
                    continue;
                }
                Err(err) => return Err(err),
            };
            let sent = tokio::select! {
                () = shutdown.cancelled() => return Ok(skipped),
                sent = tx.send(item) => sent,
            };
            if sent.is_err() {
                // Process is gone; its own error is what gets reported.
                return Ok(skipped);
            }
            next += 1;
        }
        if config.end_slot.is_some_and(|end| next > end) {
            return Ok(skipped);
        }
        if !pause(&shutdown, config.tip_poll_interval).await {
            return Ok(skipped);
        }
    }
}

/// Sleep for `duration`; `false` when shutdown came first.
async fn pause(shutdown: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        () = shutdown.cancelled() => false,
        () = tokio::time::sleep(duration) => true,
    }
}

/// Decode fetched blocks into the buffer until fetch hangs up. Returns
/// (slots processed, transactions processed).
async fn process_task(
    tracker: Arc<CommitmentTracker>,
    cursor: IngestorCursor,
    buffer: Arc<SlotBuffer>,
    flush_at: usize,
    mut rx: mpsc::Receiver<Fetched>,
    drained: CancellationToken,
) -> PrismResult<(u64, u64)> {
    let _drained = drained.drop_guard();
    let (mut slots, mut transactions) = (0, 0);
    while let Some(item) = rx.recv().await {
        match item {
            Fetched::Block(block) => {
                let buffered =
                    BufferedSlot::from_block(&block, tracker.is_finalized(block.slot_or_height))?;
                slots += 1;
                transactions += buffered.transactions.len() as u64;
                cursor.record_processed(buffered.slot, &buffered.blockhash);
                buffer.push(buffered, flush_at);
            }
            Fetched::Skipped(slot) => {
                cursor.record_skipped(slot);
                buffer.mark_skipped(slot);
            }
        }
    }
    Ok((slots, transactions))
}

/// Flush the buffer on a timer or when it fills, and once more when process
/// is done. Returns the number of flushes that wrote something.
async fn flush_task(
    sink: Arc<dyn FlushSink>,
    cursor: IngestorCursor,
    buffer: Arc<SlotBuffer>,
    interval: Duration,
    drained: CancellationToken,
) -> PrismResult<u64> {
    let mut flushes = 0;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        let done = tokio::select! {
            () = drained.cancelled() => true,
            _ = ticker.tick() => false,
            () = buffer.filled.notified() => false,
        };
        if flush_once(sink.as_ref(), &cursor, &buffer).await? {
            flushes += 1;
        }
        if done {
            cursor.persist()?;
            return Ok(flushes);
        }
    }
}

/// Drain the buffer into the sink and move the flush edge. `false` when
/// there was nothing to flush.
async fn flush_once(
    sink: &dyn FlushSink,
    cursor: &IngestorCursor,
    buffer: &SlotBuffer,
) -> PrismResult<bool> {
    let Some(drained) = buffer.drain() else {
        return Ok(false);
    };
    if let Err(err) = sink.flush(&drained.slots).await {
        buffer.restore(drained);
        return Err(err);
    }
    cursor.record_flush(drained.high_water)?;
    Ok(true)
}
