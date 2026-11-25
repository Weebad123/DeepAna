//! Token metadata: human-readable name, symbol and URI for the mints PRISM's
//! transfers name, read from each mint's Metaplex metadata account.
//!
//! | File          | Holds                                                         |
//! |---------------|---------------------------------------------------------------|
//! | `mod.rs`      | [`MetadataEnricher`], [`MetadataHandle`], config, stats        |
//! | [`pda`]       | [`metadata_pda`]: the metadata account's address              |
//! | [`parse`]     | [`parse_metadata`]: account bytes → [`TokenMetadata`] or [`Reject`] |
//! | [`writer`]    | [`MetadataWriter`]: the `token_metadata` table, write-once     |
//!
//! It runs entirely off the hot path. The ingestor's writer task, once a
//! flush has succeeded, calls [`MetadataHandle::hint`] for each mint of the
//! flushed transfers. A hint is a `try_send` on a bounded queue: it never
//! waits, and when the queue is full the hint is dropped and counted.
//! Ingestion never waits for anything this module does.
//!
//! ```text
//!   hint(mint, slot) ──try_send──▶ queue ──▶ task: drop known / negative-cached
//!                                                  batch (100 mints or 2 s)
//!                                                  ≤ 1 getMultipleAccounts / s
//!                                                  parse ──▶ MetadataWriter (buffer)
//!                                                  flush every 5 min / 1,000 rows
//! ```
//!
//! What happens when something goes wrong:
//!
//! | Failure | What happens |
//! |---------|--------------|
//! | Hint queue full | The hint is dropped (`hints_dropped_queue_full`). The mint is hinted again the next time a flushed transfer names it. |
//! | RPC error, timeout, rate limit | Counted by class in `fetch_errors`. Retryable errors put the batch's mints back after an exponential backoff (2 s doubling to 60 s, or longer if the server asked); after 5 attempts a mint is dropped (`exhausted`) until hinted again. Non-retryable errors drop the batch at once (`exhausted`). |
//! | No account, or an account that fails validation | Counted (`not_found`, `rejected` by reason) and remembered in the negative cache for an hour, in memory only, so a restart asks again. |
//! | Flush fails | Rows stay buffered and the flush is retried one flush interval later. After 10 failures in a row the enricher is [`MetadataState::Disabled`]; the buffered rows' mints are in no file, so the next run fetches them again. |
//! | Unreadable existing file at startup | [`MetadataState::Disabled`] with the file named in the reason: the known set would be incomplete and a mint could be written twice. Ingestion is unaffected. |
//! | Shutdown | Pending hints are dropped and buffered rows flushed once. |
//! | Rewind, reconcile pass, backfill | Nothing: the table is not keyed by slot, rollback never enters its directory, and re-ingested mints are already known. |
//!
//! A row is written once per mint and never updated: it is a snapshot of
//! the metadata as first read. On devnet most mints have no metadata
//! account, so `not_found` far outnumbering `written` is normal.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::adapter::ChainAdapter;
use crate::metrics::{ErrorClass, Metrics, RpcMethod};
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::DeepAnaTokenMetadata;

pub mod parse;
pub mod pda;
pub mod writer;

pub use parse::{Reject, TokenMetadata, parse_metadata};
pub use pda::{METADATA_PROGRAM, METADATA_PROGRAM_ID, metadata_pda};
pub use writer::{BufferOutcome, MetadataWriter, ScanReport, WrittenFile};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How the enricher runs. [`Default`] holds the production values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataConfig {
    /// Hints that may wait in the queue; more are dropped.
    pub queue_capacity: usize,
    /// Mints per `getMultipleAccounts` request.
    pub batch_max_mints: usize,
    /// Longest the first mint of a batch waits for the batch to fill.
    pub batch_window: Duration,
    /// Least time between two requests, on top of the adapter's own limiter.
    pub min_request_interval: Duration,
    /// How long a mint with no or an invalid metadata account is not asked
    /// for again.
    pub negative_ttl: Duration,
    /// Entries the negative cache may hold.
    pub max_negative_entries: usize,
    /// Mints the known set may grow to. Mints already on disk are always
    /// loaded; past the cap new mints are dropped (`known_set_full`).
    pub max_known_mints: usize,
    /// Longest a row waits in the buffer; also the pause before retrying a
    /// failed flush.
    pub flush_interval: Duration,
    /// Buffered rows that trigger a flush before `flush_interval` is up.
    pub flush_max_rows: usize,
    /// Failed flushes in a row after which the enricher disables itself.
    pub max_flush_failures: u32,
    /// Backoff before the first retry of a failed fetch; doubles each time.
    pub retry_base_delay: Duration,
    /// Longest backoff between fetch retries.
    pub retry_max_delay: Duration,
    /// Fetch attempts per mint before it is dropped as `exhausted`.
    pub max_fetch_attempts: u32,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 10_000,
            batch_max_mints: 100,
            batch_window: Duration::from_secs(2),
            min_request_interval: Duration::from_secs(1),
            negative_ttl: Duration::from_secs(60 * 60),
            max_negative_entries: 100_000,
            max_known_mints: 1_000_000,
            flush_interval: Duration::from_secs(5 * 60),
            flush_max_rows: 1_000,
            max_flush_failures: 10,
            retry_base_delay: Duration::from_secs(2),
            retry_max_delay: Duration::from_secs(60),
            max_fetch_attempts: 5,
        }
    }
}

impl MetadataConfig {
    /// The first unusable field, as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        for (field, value) in [
            ("queue_capacity", self.queue_capacity),
            ("batch_max_mints", self.batch_max_mints),
            ("max_negative_entries", self.max_negative_entries),
            ("max_known_mints", self.max_known_mints),
            ("flush_max_rows", self.flush_max_rows),
        ] {
            if value == 0 {
                return Err(PrismError::config(field, "must be greater than 0"));
            }
        }
        for (field, value) in [
            ("batch_window", self.batch_window),
            ("flush_interval", self.flush_interval),
            ("retry_base_delay", self.retry_base_delay),
        ] {
            if value.is_zero() {
                return Err(PrismError::config(field, "must be greater than 0"));
            }
        }
        if self.retry_max_delay < self.retry_base_delay {
            return Err(PrismError::config("retry_max_delay", "must not be below retry_base_delay"));
        }
        if self.max_flush_failures == 0 {
            return Err(PrismError::config("max_flush_failures", "must be greater than 0"));
        }
        if self.max_fetch_attempts == 0 {
            return Err(PrismError::config("max_fetch_attempts", "must be greater than 0"));
        }
        Ok(())
    }

    /// Backoff before retry number `attempt` (1-based).
    fn backoff(&self, attempt: u32) -> Duration {
        let factor = 1u32.checked_shl(attempt.saturating_sub(1)).unwrap_or(u32::MAX);
        self.retry_base_delay
            .checked_mul(factor)
            .unwrap_or(self.retry_max_delay)
            .min(self.retry_max_delay)
    }
}

// ---------------------------------------------------------------------------
// State and stats
// ---------------------------------------------------------------------------

/// Where the enricher is in its life.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum MetadataState {
    /// Accepting hints (including while the startup scan runs).
    Running,
    /// Stopped for good after a failure; hints are dropped. Ingestion is
    /// unaffected. A restart tries again.
    Disabled { reason: String },
    /// Shutting down, or shut down: hints are dropped.
    Draining,
}

impl MetadataState {
    /// `running`, `disabled` or `draining`.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Disabled { .. } => "disabled",
            Self::Draining => "draining",
        }
    }
}

/// What the enricher has done since it started. Counters only go up, except
/// `known_mints` (a gauge) and `state`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetadataStats {
    /// Every [`MetadataHandle::hint`] call.
    pub hints_received: u64,
    /// Hints dropped because the queue was full.
    pub hints_dropped_queue_full: u64,
    /// Hints dropped because the enricher was disabled or shutting down.
    pub hints_dropped_closed: u64,
    /// Hints for a mint already on disk or buffered.
    pub hints_known: u64,
    /// `getMultipleAccounts` requests made.
    pub fetch_batches: u64,
    /// Failed requests, by [`ErrorClass`] label.
    pub fetch_errors: BTreeMap<&'static str, u64>,
    /// Metadata accounts that existed, valid or not.
    pub accounts_fetched: u64,
    /// Mints with no metadata account.
    pub not_found: u64,
    /// Rows durably written.
    pub written: u64,
    /// Accounts that failed validation, by [`Reject`] label.
    pub rejected: BTreeMap<&'static str, u64>,
    /// Hints skipped because the mint was in the negative cache.
    pub negative_cache_hits: u64,
    /// Mints dropped after running out of fetch attempts.
    pub exhausted: u64,
    /// Mints dropped because the known set was at its cap.
    pub known_set_full: u64,
    pub files_written: u64,
    /// Failed flushes.
    pub write_failures: u64,
    /// Mints on disk or buffered.
    pub known_mints: u64,
    pub state: MetadataState,
}

impl Default for MetadataStats {
    /// All zero, every error class and reject reason present, `Running`.
    fn default() -> Self {
        Shared::new(MetadataState::Running).stats()
    }
}

impl MetadataStats {
    /// Every monotonic counter, flattened by name (`fetch_errors_timeout`,
    /// `rejected_bad_utf8`, ...): what [`Sources`](crate::metrics::Sources)
    /// exposes. `known_mints` and `state` are not counters and are left out.
    pub fn counters(&self) -> BTreeMap<&'static str, u64> {
        let mut out = BTreeMap::from([
            ("hints_received", self.hints_received),
            ("hints_dropped_queue_full", self.hints_dropped_queue_full),
            ("hints_dropped_closed", self.hints_dropped_closed),
            ("hints_known", self.hints_known),
            ("fetch_batches", self.fetch_batches),
            ("accounts_fetched", self.accounts_fetched),
            ("not_found", self.not_found),
            ("written", self.written),
            ("negative_cache_hits", self.negative_cache_hits),
            ("exhausted", self.exhausted),
            ("known_set_full", self.known_set_full),
            ("files_written", self.files_written),
            ("write_failures", self.write_failures),
        ]);
        for class in ErrorClass::ALL {
            let n = self.fetch_errors.get(class.as_str()).copied().unwrap_or(0);
            out.insert(fetch_error_counter(*class), n);
        }
        for reason in Reject::ALL {
            let n = self.rejected.get(reason.as_str()).copied().unwrap_or(0);
            out.insert(reject_counter(reason), n);
        }
        out
    }
}

const fn fetch_error_counter(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::RateLimited => "fetch_errors_rate_limited",
        ErrorClass::Timeout => "fetch_errors_timeout",
        ErrorClass::Transport => "fetch_errors_transport",
        ErrorClass::Http => "fetch_errors_http",
        ErrorClass::Rpc => "fetch_errors_rpc",
        ErrorClass::InvalidResponse => "fetch_errors_invalid_response",
        ErrorClass::Other => "fetch_errors_other",
    }
}

const fn reject_counter(reason: Reject) -> &'static str {
    match reason {
        Reject::WrongOwner => "rejected_wrong_owner",
        Reject::Truncated => "rejected_truncated",
        Reject::WrongKey => "rejected_wrong_key",
        Reject::MintMismatch => "rejected_mint_mismatch",
        Reject::TooLong => "rejected_too_long",
        Reject::BadUtf8 => "rejected_bad_utf8",
    }
}

#[derive(Debug, Default)]
struct Counters {
    hints_received: AtomicU64,
    hints_dropped_queue_full: AtomicU64,
    hints_dropped_closed: AtomicU64,
    hints_known: AtomicU64,
    fetch_batches: AtomicU64,
    fetch_errors: [AtomicU64; ErrorClass::COUNT],
    accounts_fetched: AtomicU64,
    not_found: AtomicU64,
    written: AtomicU64,
    rejected: [AtomicU64; Reject::COUNT],
    negative_cache_hits: AtomicU64,
    exhausted: AtomicU64,
    known_set_full: AtomicU64,
    files_written: AtomicU64,
    write_failures: AtomicU64,
    known_mints: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn class_index(class: ErrorClass) -> usize {
    ErrorClass::ALL.iter().position(|c| *c == class).unwrap_or(ErrorClass::COUNT - 1)
}

/// What the task and every handle share.
#[derive(Debug)]
struct Shared {
    counters: Counters,
    state: Mutex<MetadataState>,
}

impl Shared {
    fn new(state: MetadataState) -> Arc<Self> {
        Arc::new(Self {
            counters: Counters::default(),
            state: Mutex::new(state),
        })
    }

    fn state(&self) -> MetadataState {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    fn set_state(&self, state: MetadataState) {
        *self.state.lock().unwrap_or_else(|p| p.into_inner()) = state;
    }

    fn stats(&self) -> MetadataStats {
        let c = &self.counters;
        MetadataStats {
            hints_received: load(&c.hints_received),
            hints_dropped_queue_full: load(&c.hints_dropped_queue_full),
            hints_dropped_closed: load(&c.hints_dropped_closed),
            hints_known: load(&c.hints_known),
            fetch_batches: load(&c.fetch_batches),
            fetch_errors: ErrorClass::ALL
                .iter()
                .map(|class| (class.as_str(), load(&c.fetch_errors[class_index(*class)])))
                .collect(),
            accounts_fetched: load(&c.accounts_fetched),
            not_found: load(&c.not_found),
            written: load(&c.written),
            rejected: Reject::ALL
                .iter()
                .map(|reason| (reason.as_str(), load(&c.rejected[reason.idx()])))
                .collect(),
            negative_cache_hits: load(&c.negative_cache_hits),
            exhausted: load(&c.exhausted),
            known_set_full: load(&c.known_set_full),
            files_written: load(&c.files_written),
            write_failures: load(&c.write_failures),
            known_mints: load(&c.known_mints),
            state: self.state(),
        }
    }
}

// ---------------------------------------------------------------------------
// Handle
// ---------------------------------------------------------------------------

/// One mint seen in a flushed transfer.
#[derive(Debug, Clone, Copy)]
struct Hint {
    mint: Pubkey,
    observed_slot: u64,
}

/// The enricher's public face. Cheap to clone; every clone talks to the same
/// task. When the last clone is dropped the task drains and stops.
#[derive(Debug, Clone)]
pub struct MetadataHandle {
    inner: Arc<HandleInner>,
}

#[derive(Debug)]
struct HandleInner {
    /// `None` when the enricher was disabled before it started.
    tx: Option<mpsc::Sender<Hint>>,
    shared: Arc<Shared>,
    cancel: CancellationToken,
    /// Cancelled when the task has finished, however it ended.
    done: CancellationToken,
}

impl MetadataHandle {
    fn disabled(reason: String) -> Self {
        warn!(reason = %reason, "token metadata enricher disabled");
        let done = CancellationToken::new();
        done.cancel();
        Self {
            inner: Arc::new(HandleInner {
                tx: None,
                shared: Shared::new(MetadataState::Disabled { reason }),
                cancel: CancellationToken::new(),
                done,
            }),
        }
    }

    /// Ask for `mint`'s metadata; `observed_slot` is the slot of the
    /// transfer that named it. Never waits: the hint is queued if there is
    /// room and dropped (and counted) if not.
    pub fn hint(&self, mint: Pubkey, observed_slot: u64) {
        let counters = &self.inner.shared.counters;
        bump(&counters.hints_received);
        let Some(tx) = &self.inner.tx else {
            bump(&counters.hints_dropped_closed);
            return;
        };
        match tx.try_send(Hint { mint, observed_slot }) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => bump(&counters.hints_dropped_queue_full),
            Err(TrySendError::Closed(_)) => bump(&counters.hints_dropped_closed),
        }
    }

    pub fn stats(&self) -> MetadataStats {
        self.inner.shared.stats()
    }

    pub fn state(&self) -> MetadataState {
        self.inner.shared.state()
    }

    /// Stop: drop pending hints, flush buffered rows once, and return when
    /// the task has finished. Safe to call more than once and from several
    /// clones; every call waits for the same end.
    pub async fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.done.cancelled().await;
    }
}

// ---------------------------------------------------------------------------
// Enricher
// ---------------------------------------------------------------------------

/// Starts the enrichment task. See the module docs.
#[derive(Debug)]
pub struct MetadataEnricher;

impl MetadataEnricher {
    /// Start the task writing under `data_dir` and return its handle at
    /// once: the startup scan runs inside the task. Never fails: an invalid
    /// `config`, or no Tokio runtime to run on, yields a handle that is
    /// [`MetadataState::Disabled`] and drops every hint. Cancelling
    /// `shutdown` stops the task like [`MetadataHandle::shutdown`].
    ///
    /// `metrics` receives the `getMultipleAccounts` requests, errors and
    /// latencies; the enricher's own counters are in
    /// [`MetadataHandle::stats`].
    pub fn start(
        config: MetadataConfig,
        adapter: Arc<dyn ChainAdapter>,
        data_dir: impl Into<PathBuf>,
        metrics: Arc<Metrics>,
        shutdown: CancellationToken,
    ) -> MetadataHandle {
        if let Err(err) = config.validate() {
            return MetadataHandle::disabled(format!("invalid config: {err}"));
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return MetadataHandle::disabled("no Tokio runtime to run on".to_owned());
        };
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let shared = Shared::new(MetadataState::Running);
        let cancel = shutdown.child_token();
        let done = CancellationToken::new();
        let task = Task {
            data_dir: data_dir.into(),
            adapter,
            metrics,
            shared: Arc::clone(&shared),
            rx,
            cancel: cancel.clone(),
            writer: None,
            negative: HashMap::new(),
            negative_sweep_after: None,
            tracked: HashMap::new(),
            ready: VecDeque::new(),
            waiting: BinaryHeap::new(),
            batch_opened: None,
            last_request: None,
            buffered_since: None,
            flush_retry_at: None,
            flush_failures: 0,
            config,
        };
        let guard = done.clone().drop_guard();
        runtime.spawn(async move {
            let _guard = guard;
            task.run().await;
        });
        MetadataHandle {
            inner: Arc::new(HandleInner {
                tx: Some(tx),
                shared,
                cancel,
                done,
            }),
        }
    }
}

/// A mint waiting to be fetched, now or after a backoff.
#[derive(Debug, Clone, Copy)]
struct Tracked {
    /// Highest slot it was hinted with.
    observed_slot: u64,
    /// Fetches that failed so far.
    attempts: u32,
}

struct Task {
    config: MetadataConfig,
    data_dir: PathBuf,
    adapter: Arc<dyn ChainAdapter>,
    metrics: Arc<Metrics>,
    shared: Arc<Shared>,
    rx: mpsc::Receiver<Hint>,
    cancel: CancellationToken,
    /// `None` until the startup scan is done, and while a flush runs.
    writer: Option<MetadataWriter>,
    /// Mints with no or an invalid metadata account, until when.
    negative: HashMap<Pubkey, Instant>,
    /// When the negative cache was full of live entries: no sweep before.
    negative_sweep_after: Option<Instant>,
    /// Every mint queued or backing off.
    tracked: HashMap<Pubkey, Tracked>,
    /// Mints to fetch, in arrival order.
    ready: VecDeque<Pubkey>,
    /// Mints backing off, by when they are ready again.
    waiting: BinaryHeap<Reverse<(Instant, Pubkey)>>,
    /// When `ready` last went from empty to non-empty.
    batch_opened: Option<Instant>,
    last_request: Option<Instant>,
    /// When the oldest unflushed row was buffered.
    buffered_since: Option<Instant>,
    /// After a failed flush: not before then.
    flush_retry_at: Option<Instant>,
    flush_failures: u32,
}

impl Task {
    fn counters(&self) -> &Counters {
        &self.shared.counters
    }

    fn writer(&self) -> &MetadataWriter {
        self.writer.as_ref().expect("the writer is open outside a flush")
    }

    async fn run(mut self) {
        if !self.open().await {
            return;
        }
        loop {
            let wake = self.next_wake();
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                hint = self.rx.recv() => match hint {
                    Some(hint) => {
                        self.accept(hint);
                        // Take what else is queued without another wake-up,
                        // up to a full batch.
                        while self.ready.len() < self.config.batch_max_mints {
                            let Ok(hint) = self.rx.try_recv() else { break };
                            self.accept(hint);
                        }
                    }
                    // Every handle is gone.
                    None => break,
                },
                () = sleep_until_or_never(wake) => {}
            }
            let now = Instant::now();
            self.promote_due_retries(now);
            if self.batch_due(now) && !self.fetch_batch().await {
                break;
            }
            if self.flush_due(Instant::now()) && !self.flush().await {
                return;
            }
        }
        self.drain().await;
    }

    /// Run the startup scan. `false` (and disabled, or draining when
    /// cancelled first) when the task should stop.
    async fn open(&mut self) -> bool {
        let data_dir = self.data_dir.clone();
        let max_known = self.config.max_known_mints;
        let scan = tokio::task::spawn_blocking(move || MetadataWriter::open(data_dir, max_known));
        let opened = tokio::select! {
            biased;
            () = self.cancel.cancelled() => {
                self.shared.set_state(MetadataState::Draining);
                return false;
            }
            opened = scan => opened,
        };
        match opened {
            Ok(Ok((writer, report))) => {
                self.counters().known_mints.store(writer.known_len() as u64, Ordering::Relaxed);
                debug!(?report, "token metadata enricher started");
                self.writer = Some(writer);
                true
            }
            Ok(Err(err)) => {
                self.disable(format!("startup scan failed: {err}"));
                false
            }
            Err(err) => {
                self.disable(format!("startup scan panicked: {err}"));
                false
            }
        }
    }

    fn disable(&mut self, reason: String) {
        error!(reason = %reason, "token metadata enricher disabled; ingestion continues without it");
        // Closed first: whoever sees the state finds hints refused.
        self.rx.close();
        self.shared.set_state(MetadataState::Disabled { reason });
    }

    /// Queue `hint` unless its mint is known, negative-cached or already
    /// queued (then only its slot moves up).
    fn accept(&mut self, Hint { mint, observed_slot }: Hint) {
        if self.writer().is_known(&mint) {
            bump(&self.counters().hints_known);
            return;
        }
        if let Some(&until) = self.negative.get(&mint) {
            if Instant::now() < until {
                bump(&self.counters().negative_cache_hits);
                return;
            }
            self.negative.remove(&mint);
        }
        if let Some(tracked) = self.tracked.get_mut(&mint) {
            tracked.observed_slot = tracked.observed_slot.max(observed_slot);
            return;
        }
        if self.writer().is_full() {
            bump(&self.counters().known_set_full);
            return;
        }
        self.tracked.insert(mint, Tracked { observed_slot, attempts: 0 });
        self.push_ready(mint);
    }

    fn push_ready(&mut self, mint: Pubkey) {
        if self.ready.is_empty() {
            self.batch_opened = Some(Instant::now());
        }
        self.ready.push_back(mint);
    }

    fn promote_due_retries(&mut self, now: Instant) {
        while let Some(Reverse((at, mint))) = self.waiting.peek().copied() {
            if at > now {
                break;
            }
            self.waiting.pop();
            self.push_ready(mint);
        }
    }

    fn batch_due(&self, now: Instant) -> bool {
        self.ready.len() >= self.config.batch_max_mints
            || self
                .batch_opened
                .is_some_and(|opened| !self.ready.is_empty() && now >= opened + self.config.batch_window)
    }

    fn flush_due(&self, now: Instant) -> bool {
        let buffered = self.writer().buffered_len();
        if buffered == 0 {
            return false;
        }
        match self.flush_retry_at {
            Some(at) => now >= at,
            None => {
                buffered >= self.config.flush_max_rows
                    || self.buffered_since.is_some_and(|since| now >= since + self.config.flush_interval)
            }
        }
    }

    /// The earliest moment something becomes due.
    fn next_wake(&self) -> Option<Instant> {
        let batch = self
            .batch_opened
            .filter(|_| !self.ready.is_empty())
            .map(|opened| opened + self.config.batch_window);
        let retry = self.waiting.peek().map(|Reverse((at, _))| *at);
        let flush = if self.writer().buffered_len() == 0 {
            None
        } else {
            self.flush_retry_at
                .or_else(|| self.buffered_since.map(|since| since + self.config.flush_interval))
        };
        [batch, retry, flush].into_iter().flatten().min()
    }

    /// Fetch up to one batch of ready mints and act on the answers. `false`
    /// when shutdown came first (the batch is then simply dropped).
    async fn fetch_batch(&mut self) -> bool {
        if let Some(last) = self.last_request {
            let at = last + self.config.min_request_interval;
            if Instant::now() < at {
                tokio::select! {
                    biased;
                    () = self.cancel.cancelled() => return false,
                    () = sleep_until(at) => {}
                }
            }
        }
        let take = self.ready.len().min(self.config.batch_max_mints);
        let mints: Vec<Pubkey> = self.ready.drain(..take).collect();
        if self.ready.is_empty() {
            self.batch_opened = None;
        }
        if mints.is_empty() {
            return true;
        }
        let pdas: Vec<Pubkey> = mints.iter().map(metadata_pda).collect();
        let addresses: Vec<String> = pdas.iter().map(Pubkey::to_string).collect();

        self.last_request = Some(Instant::now());
        bump(&self.counters().fetch_batches);
        self.metrics.rpc_request(RpcMethod::GetMultipleAccounts);
        let started = std::time::Instant::now();
        let adapter = Arc::clone(&self.adapter);
        let result = tokio::select! {
            biased;
            () = self.cancel.cancelled() => return false,
            result = adapter.get_accounts(&addresses) => result,
        };
        self.metrics.rpc_latency(RpcMethod::GetMultipleAccounts, started.elapsed());

        let accounts = match result {
            Ok(accounts) if accounts.len() == mints.len() => accounts,
            Ok(accounts) => {
                let err = PrismError::deserialize(
                    None,
                    "getMultipleAccounts response",
                    format!("asked for {} accounts, got {}", mints.len(), accounts.len()),
                );
                self.fetch_failed(&mints, &err);
                return true;
            }
            Err(err) => {
                self.fetch_failed(&mints, &err);
                return true;
            }
        };

        let now = Instant::now();
        for ((mint, pda), account) in mints.into_iter().zip(pdas).zip(accounts) {
            let Some(tracked) = self.tracked.remove(&mint) else { continue };
            let Some(account) = account else {
                bump(&self.counters().not_found);
                self.remember_negative(mint, now);
                continue;
            };
            bump(&self.counters().accounts_fetched);
            match parse_metadata(&account, &mint) {
                Ok(meta) => self.buffer(mint, pda, meta, tracked.observed_slot),
                Err(reason) => {
                    debug!(%mint, %reason, "metadata account rejected");
                    bump(&self.counters().rejected[reason.idx()]);
                    self.remember_negative(mint, now);
                }
            }
        }
        true
    }

    fn buffer(&mut self, mint: Pubkey, pda: Pubkey, meta: TokenMetadata, observed_slot: u64) {
        let row = DeepAnaTokenMetadata {
            mint_address: mint.to_string(),
            metadata_account: pda.to_string(),
            name: meta.name,
            symbol: meta.symbol,
            uri: meta.uri,
            observed_slot,
        };
        let writer = self.writer.as_mut().expect("the writer is open outside a flush");
        match writer.buffer(mint, row) {
            BufferOutcome::Buffered => {
                let known = writer.known_len() as u64;
                self.counters().known_mints.store(known, Ordering::Relaxed);
                self.buffered_since.get_or_insert_with(Instant::now);
            }
            BufferOutcome::Known => bump(&self.counters().hints_known),
            BufferOutcome::Full => bump(&self.counters().known_set_full),
        }
    }

    fn fetch_failed(&mut self, mints: &[Pubkey], err: &PrismError) {
        let class = ErrorClass::of(err);
        bump(&self.counters().fetch_errors[class_index(class)]);
        self.metrics.rpc_error(RpcMethod::GetMultipleAccounts, class);
        let retryable = err.is_retryable();
        warn!(error = %err, mints = mints.len(), retryable, "token metadata fetch failed");
        let now = Instant::now();
        for mint in mints {
            let Some(tracked) = self.tracked.get_mut(mint) else { continue };
            tracked.attempts += 1;
            if !retryable || tracked.attempts >= self.config.max_fetch_attempts {
                self.tracked.remove(mint);
                bump(&self.counters().exhausted);
                continue;
            }
            let mut delay = self.config.backoff(tracked.attempts);
            if let Some(asked) = err.retry_after() {
                delay = delay.max(asked);
            }
            self.waiting.push(Reverse((now + delay, *mint)));
        }
    }

    fn remember_negative(&mut self, mint: Pubkey, now: Instant) {
        let cap = self.config.max_negative_entries;
        if self.negative.len() >= cap && self.negative_sweep_after.is_none_or(|after| now >= after) {
            self.negative.retain(|_, until| *until > now);
            // Still full of live entries: do not sweep again for a while.
            self.negative_sweep_after = (self.negative.len() >= cap).then(|| now + Duration::from_secs(60));
        }
        if self.negative.len() < cap {
            self.negative.insert(mint, now + self.config.negative_ttl);
        }
    }

    /// Flush the writer off the async threads. `false` when the enricher
    /// has just disabled itself.
    async fn flush(&mut self) -> bool {
        let mut writer = self.writer.take().expect("the writer is open outside a flush");
        let joined = tokio::task::spawn_blocking(move || {
            let result = writer.flush();
            (writer, result)
        })
        .await;
        let (writer, result) = match joined {
            Ok(done) => done,
            Err(err) => {
                self.disable(format!("token metadata flush panicked: {err}"));
                return false;
            }
        };
        self.writer = Some(writer);
        match result {
            Ok(Some(file)) => {
                bump(&self.counters().files_written);
                add(&self.counters().written, file.rows as u64);
                info!(path = %file.path.display(), rows = file.rows, "token metadata written");
                self.flush_failures = 0;
                self.flush_retry_at = None;
                self.buffered_since = None;
                true
            }
            Ok(None) => true,
            Err(err) => {
                bump(&self.counters().write_failures);
                self.flush_failures += 1;
                if self.flush_failures >= self.config.max_flush_failures {
                    self.disable(format!(
                        "{} token metadata flushes failed in a row, last: {err}",
                        self.flush_failures
                    ));
                    return false;
                }
                warn!(
                    failures = self.flush_failures,
                    max = self.config.max_flush_failures,
                    error = %err,
                    "token metadata flush failed; rows stay buffered"
                );
                self.flush_retry_at = Some(Instant::now() + self.config.flush_interval);
                true
            }
        }
    }

    /// Drop pending hints, flush what is buffered once.
    async fn drain(&mut self) {
        self.rx.close();
        self.shared.set_state(MetadataState::Draining);
        self.tracked.clear();
        self.ready.clear();
        self.waiting.clear();
        if self.writer.as_ref().is_some_and(|w| w.buffered_len() > 0) {
            self.flush_retry_at = None;
            if self.flush().await && self.writer().buffered_len() > 0 {
                warn!(
                    rows = self.writer().buffered_len(),
                    "final token metadata flush failed; those mints are fetched again on the next run"
                );
            }
        }
        // `flush` may have disabled it; that state is the more useful one.
        if !matches!(self.shared.state(), MetadataState::Disabled { .. }) {
            self.shared.set_state(MetadataState::Draining);
        }
        info!("token metadata enricher stopped");
    }
}

async fn sleep_until_or_never(at: Option<Instant>) {
    match at {
        Some(at) => sleep_until(at).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_the_documented_one_and_valid() {
        let c = MetadataConfig::default();
        assert_eq!(c.batch_max_mints, 100);
        assert_eq!(c.batch_window, Duration::from_secs(2));
        assert_eq!(c.min_request_interval, Duration::from_secs(1));
        assert_eq!(c.negative_ttl, Duration::from_secs(3_600));
        assert_eq!(c.flush_interval, Duration::from_secs(300));
        assert_eq!(c.flush_max_rows, 1_000);
        assert_eq!(c.max_flush_failures, 10);
        c.validate().unwrap();
    }

    #[test]
    fn backoff_doubles_and_caps() {
        let c = MetadataConfig::default();
        let secs: Vec<u64> = (1..=8).map(|n| c.backoff(n).as_secs()).collect();
        assert_eq!(secs, [2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(c.backoff(u32::MAX), c.retry_max_delay);
    }

    #[test]
    fn invalid_configs_name_the_field() {
        for (field, config) in [
            ("queue_capacity", MetadataConfig { queue_capacity: 0, ..Default::default() }),
            ("batch_window", MetadataConfig { batch_window: Duration::ZERO, ..Default::default() }),
            ("retry_max_delay", MetadataConfig { retry_max_delay: Duration::from_millis(1), ..Default::default() }),
            ("max_flush_failures", MetadataConfig { max_flush_failures: 0, ..Default::default() }),
        ] {
            match config.validate() {
                Err(PrismError::ConfigError { field: f, .. }) => assert_eq!(f, field),
                other => panic!("{field}: {other:?}"),
            }
        }
    }

    #[test]
    fn counters_flatten_by_label() {
        let shared = Shared::new(MetadataState::Running);
        bump(&shared.counters.rejected[Reject::BadUtf8.idx()]);
        bump(&shared.counters.fetch_errors[class_index(ErrorClass::Timeout)]);
        let counters = shared.stats().counters();
        assert_eq!(counters["rejected_bad_utf8"], 1);
        assert_eq!(counters["fetch_errors_timeout"], 1);
        assert!(!counters.contains_key("known_mints"));
        assert_eq!(counters.len(), 13 + ErrorClass::COUNT + Reject::COUNT);
        for class in ErrorClass::ALL {
            assert_eq!(fetch_error_counter(*class), format!("fetch_errors_{}", class.as_str()));
        }
        for reason in Reject::ALL {
            assert_eq!(reject_counter(reason), format!("rejected_{}", reason.as_str()));
        }
    }
}
