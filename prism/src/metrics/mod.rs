//! PRISM's own instrument panel.
//!
//! [`Metrics`] counts what the pipeline actually does (RPC calls, rows and
//! files written, events emitted, slots processed, instructions decoded) and
//! exposes it as a JSON-ready [`MetricsSnapshot`] and, through
//! [`render_prometheus`], as Prometheus text. HTTP is not this crate's job:
//! `lens` serves both from `snapshot()` and `render_prometheus()`.
//!
//! | File              | Holds                                                    |
//! |-------------------|----------------------------------------------------------|
//! | `mod.rs`          | [`Metrics`], label enums, [`MetricsSnapshot`], [`Sources`] |
//! | [`histogram`]     | [`Histogram`]: fixed buckets, integer values             |
//! | [`scrub`]         | secret registry and error-text scrubbing                 |
//! | [`prometheus`]    | text exposition, version 0.0.4                           |
//!
//! Rules this module keeps:
//!
//! - **Recording never blocks and never allocates.** Every recording method
//!   is a handful of relaxed atomic operations on fixed arrays indexed by a
//!   label enum. The two text fields (halt reason, last error) sit behind
//!   mutexes no hot path touches.
//! - **Labels are fixed enums, never text**, so the series count is bounded
//!   and known at compile time.
//! - **Counters only increase** and start from zero on restart; Prometheus'
//!   `rate()` handles the reset.
//! - **Unknown is not zero.** Slot gauges start unknown and stay `None` in the
//!   snapshot (and absent from Prometheus) until first set; lag is derived at
//!   snapshot time and is `None` unless both ends are known.
//! - **A snapshot is not an atomic cut.** Each value is read independently;
//!   two related counters may be one increment apart.
//! - **Expose, don't duplicate.** Components that already keep stats (the
//!   account resolver, the reconciler) hand them in through [`Sources`] at
//!   snapshot time instead of reporting into `Metrics` twice.
//! - **Free text only in JSON.** Halt reason and last error are scrubbed with
//!   [`scrub::clean`] and never appear in the Prometheus output.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use solana_client::client_error::ClientErrorKind;

use crate::account_resolver::ResolverMetricsSnapshot;
use crate::reconcile::ReconcileStats;
use crate::storage::partition::TableKind;
use crate::types::error::PrismError;
use crate::types::event::SkipReason;

pub mod histogram;
pub mod prometheus;
pub mod scrub;

pub use histogram::{Histogram, HistogramSnapshot};
pub use prometheus::render_prometheus;

/// The tables the writer produces, as a metric label.
pub type Table = TableKind;

// ---------------------------------------------------------------------------
// Label enums
// ---------------------------------------------------------------------------

/// Defines a fieldless label enum with `ALL`, `COUNT`, `as_str` and an array
/// index.
macro_rules! label_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident { $( $(#[$vmeta:meta])* $variant:ident => $label:literal ),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name { $( $(#[$vmeta])* $variant ),+ }

        /// Serializes as its label value, so JSON and Prometheus spell every
        /// label the same way (`getBlock`, not `get_block`).
        impl Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant),+];
            pub const COUNT: usize = Self::ALL.len();

            /// Label value.
            pub const fn as_str(self) -> &'static str {
                match self { $( Self::$variant => $label ),+ }
            }

            #[allow(dead_code)]
            const fn idx(self) -> usize {
                self as usize
            }
        }
    };
}

label_enum! {
    /// RPC methods PRISM calls, by their JSON-RPC name.
    pub enum RpcMethod {
        GetBlock => "getBlock",
        GetBlocks => "getBlocks",
        GetLatestBlockhash => "getLatestBlockhash",
        GetGenesisHash => "getGenesisHash",
        GetMultipleAccounts => "getMultipleAccounts",
        GetHealth => "getHealth",
        /// Any method not listed above.
        Other => "other",
    }
}

impl RpcMethod {
    /// Map a JSON-RPC method name (as the adapter passes it around) to its
    /// label. Unknown names become [`RpcMethod::Other`].
    pub fn from_name(name: &str) -> Self {
        Self::ALL
            .iter()
            .copied()
            .find(|m| m.as_str() == name)
            .unwrap_or(Self::Other)
    }
}

label_enum! {
    /// Why an RPC attempt failed.
    pub enum ErrorClass {
        /// `429 Too Many Requests`.
        RateLimited => "rate_limited",
        /// The request timed out.
        Timeout => "timeout",
        /// No answer: connect failure, broken connection, I/O error.
        Transport => "transport",
        /// An HTTP error status other than 429.
        Http => "http",
        /// A JSON-RPC error response from the node.
        Rpc => "rpc",
        /// The answer was not a valid JSON-RPC response or did not parse.
        InvalidResponse => "invalid_response",
        Other => "other",
    }
}

impl ErrorClass {
    /// Classify an error returned by the adapter.
    pub fn of(err: &PrismError) -> Self {
        match err {
            PrismError::RateLimitError { .. } => Self::RateLimited,
            PrismError::DeserializeError { .. } => Self::InvalidResponse,
            PrismError::RpcError { source, .. } => match source.kind() {
                ClientErrorKind::Io(_) => Self::Transport,
                ClientErrorKind::Reqwest(http) if http.is_timeout() => Self::Timeout,
                ClientErrorKind::Reqwest(http) => match http.status() {
                    Some(s) if s.as_u16() == 429 => Self::RateLimited,
                    Some(_) => Self::Http,
                    None => Self::Transport,
                },
                ClientErrorKind::RpcError(_) => Self::Rpc,
                ClientErrorKind::SerdeJson(_) => Self::InvalidResponse,
                _ => Self::Other,
            },
            _ => Self::Other,
        }
    }
}

label_enum! {
    /// Change log event kinds.
    pub enum EventType {
        New => "new",
        Skip => "skip",
        Final => "final",
        Correction => "correction",
    }
}

impl EventType {
    pub const fn of(event: &crate::types::event::ChangeEvent) -> Self {
        use crate::types::event::ChangeEvent;
        match event {
            ChangeEvent::New { .. } => Self::New,
            ChangeEvent::Skip { .. } => Self::Skip,
            ChangeEvent::Final { .. } => Self::Final,
            ChangeEvent::Correction { .. } => Self::Correction,
        }
    }
}

label_enum! {
    /// Coarse class of the program an instruction belongs to. Program ids
    /// are unbounded and never used as labels; anything not listed is
    /// [`ProgramClass::Other`].
    pub enum ProgramClass {
        System => "system",
        Vote => "vote",
        ComputeBudget => "compute_budget",
        SplToken => "spl_token",
        Token2022 => "token_2022",
        AssociatedToken => "associated_token",
        Memo => "memo",
        Other => "other",
    }
}

impl ProgramClass {
    /// Classify a base58 program id.
    pub fn of(program_id: &str) -> Self {
        match program_id {
            "11111111111111111111111111111111" => Self::System,
            "Vote111111111111111111111111111111111111111" => Self::Vote,
            "ComputeBudget111111111111111111111111111111" => Self::ComputeBudget,
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => Self::SplToken,
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => Self::Token2022,
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL" => Self::AssociatedToken,
            "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
            | "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo" => Self::Memo,
            _ => Self::Other,
        }
    }
}

label_enum! {
    /// What happened to one instruction in the decoder. Mirrors the three
    /// exclusive buckets of the decoder registry.
    pub enum DecodeOutcome {
        Decoded => "decoded",
        NoDecoder => "no_decoder",
        Error => "error",
    }
}

label_enum! {
    /// Coarse pipeline state.
    pub enum PipelineState {
        /// Created, not yet running.
        Starting => "starting",
        Running => "running",
        Halted => "halted",
    }
}

impl PipelineState {
    const fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Running,
            2 => Self::Halted,
            _ => Self::Starting,
        }
    }
}

const fn table_idx(t: TableKind) -> usize {
    match t {
        TableKind::Slots => 0,
        TableKind::Transactions => 1,
        TableKind::Instructions => 2,
        TableKind::TokenTransfers => 3,
    }
}

const fn skip_idx(r: SkipReason) -> usize {
    match r {
        SkipReason::ValidatorSkipped => 0,
        SkipReason::RpcUnavailable => 1,
        SkipReason::BelowCursor => 2,
    }
}

const TABLES: usize = TableKind::ALL.len();
const SKIP_REASONS: usize = SkipReason::ALL.len();

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Sentinel for a gauge that has never been set.
const UNKNOWN: u64 = u64::MAX;

type Counters<const N: usize> = [AtomicU64; N];

fn counters<const N: usize>() -> Counters<N> {
    std::array::from_fn(|_| AtomicU64::new(0))
}

fn bump(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

fn load(c: &AtomicU64) -> u64 {
    c.load(Ordering::Relaxed)
}

fn gauge(c: &AtomicU64) -> Option<u64> {
    match c.load(Ordering::Relaxed) {
        UNKNOWN => None,
        v => Some(v),
    }
}

/// PRISM's counters, gauges and histograms. Share it as `Arc<Metrics>`.
#[derive(Debug)]
pub struct Metrics {
    state: AtomicU8,

    rpc_requests: Counters<{ RpcMethod::COUNT }>,
    rpc_retries: Counters<{ RpcMethod::COUNT }>,
    rpc_errors: [Counters<{ ErrorClass::COUNT }>; RpcMethod::COUNT],
    rpc_latency: [Histogram; RpcMethod::COUNT],
    rate_limit_wait: Histogram,

    rows_written: Counters<TABLES>,
    bytes_written: Counters<TABLES>,
    files_written: Counters<TABLES>,
    flushes_ok: AtomicU64,
    flushes_failed: AtomicU64,
    flush_duration: Histogram,

    events: Counters<{ EventType::COUNT }>,
    slots_processed: AtomicU64,
    slots_skipped: Counters<SKIP_REASONS>,
    instructions: [Counters<{ DecodeOutcome::COUNT }>; ProgramClass::COUNT],

    tip_slot: AtomicU64,
    processed_slot: AtomicU64,
    flushed_slot: AtomicU64,
    finalized_slot: AtomicU64,
    changelog_bytes: AtomicU64,
    changelog_segments: AtomicU64,

    halt_reason: Mutex<Option<String>>,
    last_error: Mutex<Option<String>>,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicU8::new(PipelineState::Starting as u8),
            rpc_requests: counters(),
            rpc_retries: counters(),
            rpc_errors: std::array::from_fn(|_| counters()),
            rpc_latency: std::array::from_fn(|_| {
                Histogram::new(histogram::RPC_LATENCY_BUCKETS_US)
            }),
            rate_limit_wait: Histogram::new(histogram::RATE_LIMIT_WAIT_BUCKETS_US),
            rows_written: counters(),
            bytes_written: counters(),
            files_written: counters(),
            flushes_ok: AtomicU64::new(0),
            flushes_failed: AtomicU64::new(0),
            flush_duration: Histogram::new(histogram::FLUSH_BUCKETS_US),
            events: counters(),
            slots_processed: AtomicU64::new(0),
            slots_skipped: counters(),
            instructions: std::array::from_fn(|_| counters()),
            tip_slot: AtomicU64::new(UNKNOWN),
            processed_slot: AtomicU64::new(UNKNOWN),
            flushed_slot: AtomicU64::new(UNKNOWN),
            finalized_slot: AtomicU64::new(UNKNOWN),
            changelog_bytes: AtomicU64::new(UNKNOWN),
            changelog_segments: AtomicU64::new(UNKNOWN),
            halt_reason: Mutex::new(None),
            last_error: Mutex::new(None),
        })
    }

    // -- RPC ---------------------------------------------------------------

    /// One RPC attempt was made (retries included).
    pub fn rpc_request(&self, method: RpcMethod) {
        bump(&self.rpc_requests[method.idx()], 1);
    }

    /// One attempt was a retry of an earlier failed attempt.
    pub fn rpc_retry(&self, method: RpcMethod) {
        bump(&self.rpc_retries[method.idx()], 1);
    }

    /// One attempt failed.
    pub fn rpc_error(&self, method: RpcMethod, class: ErrorClass) {
        bump(&self.rpc_errors[method.idx()][class.idx()], 1);
    }

    /// Wall time of one attempt, success or failure.
    pub fn rpc_latency(&self, method: RpcMethod, elapsed: Duration) {
        self.rpc_latency[method.idx()].record_duration(elapsed);
    }

    /// Time spent waiting on the client-side rate limiter or a `Retry-After`.
    pub fn rpc_rate_limit_wait(&self, waited: Duration) {
        self.rate_limit_wait.record_duration(waited);
    }

    // -- Storage -----------------------------------------------------------

    pub fn rows_written(&self, table: Table, n: u64) {
        bump(&self.rows_written[table_idx(table)], n);
    }

    pub fn bytes_written(&self, table: Table, n: u64) {
        bump(&self.bytes_written[table_idx(table)], n);
    }

    pub fn files_written(&self, table: Table, n: u64) {
        bump(&self.files_written[table_idx(table)], n);
    }

    /// One flush finished, successfully or not, after `elapsed`.
    pub fn flush_done(&self, ok: bool, elapsed: Duration) {
        bump(if ok { &self.flushes_ok } else { &self.flushes_failed }, 1);
        self.flush_duration.record_duration(elapsed);
    }

    // -- Pipeline ----------------------------------------------------------

    pub fn event_emitted(&self, event: EventType) {
        bump(&self.events[event.idx()], 1);
    }

    pub fn slot_processed(&self) {
        bump(&self.slots_processed, 1);
    }

    pub fn slot_skipped(&self, reason: SkipReason) {
        bump(&self.slots_skipped[skip_idx(reason)], 1);
    }

    pub fn instruction(&self, program_class: ProgramClass, outcome: DecodeOutcome) {
        bump(&self.instructions[program_class.idx()][outcome.idx()], 1);
    }

    // -- Gauges ------------------------------------------------------------

    /// Latest chain tip seen. `u64::MAX` is reserved for "unknown" and is
    /// ignored.
    pub fn set_tip(&self, slot: u64) {
        set_gauge(&self.tip_slot, slot);
    }

    /// Highest slot processed (rows built, not necessarily flushed).
    pub fn set_processed(&self, slot: u64) {
        set_gauge(&self.processed_slot, slot);
    }

    /// Highest slot whose rows are durable on disk.
    pub fn set_flushed(&self, slot: u64) {
        set_gauge(&self.flushed_slot, slot);
    }

    /// Finalized watermark.
    pub fn set_finalized(&self, slot: u64) {
        set_gauge(&self.finalized_slot, slot);
    }

    /// Change log size on disk and its number of segments.
    pub fn set_changelog(&self, bytes: u64, segments: u64) {
        set_gauge(&self.changelog_bytes, bytes);
        set_gauge(&self.changelog_segments, segments);
    }

    // -- State -------------------------------------------------------------

    /// The pipeline is running. Clears a previous halt reason.
    pub fn set_running(&self) {
        self.state.store(PipelineState::Running as u8, Ordering::Relaxed);
        *lock(&self.halt_reason) = None;
    }

    /// The pipeline halted. `reason` is scrubbed and capped before it is kept.
    /// Rare (once per halt), so it may allocate and lock.
    pub fn set_halted(&self, reason: &str) {
        let cleaned = scrub::clean(reason);
        *lock(&self.halt_reason) = Some(cleaned);
        self.state.store(PipelineState::Halted as u8, Ordering::Relaxed);
    }

    /// Keep the text of the most recent error, scrubbed and capped.
    ///
    /// Not a hot-path call: it scrubs and allocates. Call it where an error is
    /// handled (logged, retried, given up on), not per attempt in a tight
    /// loop. If another thread is updating it at the same moment this call
    /// is dropped rather than waiting.
    pub fn set_last_error(&self, text: &str) {
        let cleaned = scrub::clean(text);
        if let Ok(mut slot) = self.last_error.try_lock() {
            *slot = Some(cleaned);
        }
    }

    // -- Snapshot ----------------------------------------------------------

    /// Copy everything into a serializable snapshot. Not an atomic cut.
    pub fn snapshot(&self, sources: &Sources) -> MetricsSnapshot {
        let tip_slot = gauge(&self.tip_slot);
        let processed_slot = gauge(&self.processed_slot);
        let lag_slots = match (tip_slot, processed_slot) {
            (Some(tip), Some(done)) => Some(tip.saturating_sub(done)),
            _ => None,
        };

        let rpc = RpcMethod::ALL
            .iter()
            .map(|&m| {
                let i = m.idx();
                let errors = ErrorClass::ALL
                    .iter()
                    .map(|&c| (c.as_str(), load(&self.rpc_errors[i][c.idx()])))
                    .collect();
                let snap = RpcSnapshot {
                    requests: load(&self.rpc_requests[i]),
                    retries: load(&self.rpc_retries[i]),
                    errors,
                    latency_us: self.rpc_latency[i].snapshot(),
                };
                (m.as_str(), snap)
            })
            .collect();

        let tables = TableKind::ALL
            .iter()
            .map(|&t| {
                let i = table_idx(t);
                let snap = TableSnapshot {
                    rows: load(&self.rows_written[i]),
                    bytes: load(&self.bytes_written[i]),
                    files: load(&self.files_written[i]),
                };
                (t.as_str(), snap)
            })
            .collect();

        let events = EventType::ALL
            .iter()
            .map(|&e| (e.as_str(), load(&self.events[e.idx()])))
            .collect();

        let slots_skipped = SkipReason::ALL
            .iter()
            .map(|&r| (r.as_str(), load(&self.slots_skipped[skip_idx(r)])))
            .collect();

        let instructions = ProgramClass::ALL
            .iter()
            .map(|&p| {
                let by_outcome = DecodeOutcome::ALL
                    .iter()
                    .map(|&o| (o.as_str(), load(&self.instructions[p.idx()][o.idx()])))
                    .collect();
                (p.as_str(), by_outcome)
            })
            .collect();

        MetricsSnapshot {
            state: PipelineState::from_u8(self.state.load(Ordering::Relaxed)),
            halt_reason: lock(&self.halt_reason).clone(),
            last_error: lock(&self.last_error).clone(),
            tip_slot,
            processed_slot,
            flushed_slot: gauge(&self.flushed_slot),
            finalized_slot: gauge(&self.finalized_slot),
            lag_slots,
            changelog_bytes: gauge(&self.changelog_bytes),
            changelog_segments: gauge(&self.changelog_segments),
            rpc,
            rate_limit_wait_us: self.rate_limit_wait.snapshot(),
            tables,
            flushes_ok: load(&self.flushes_ok),
            flushes_failed: load(&self.flushes_failed),
            flush_duration_us: self.flush_duration.snapshot(),
            events,
            slots_processed: load(&self.slots_processed),
            slots_skipped,
            instructions,
            sources: SourcesSnapshot {
                resolver: sources.resolver.as_ref().map(resolver_counters),
                reconcile: sources.reconcile.as_ref().map(reconcile_counters),
            },
        }
    }
}

fn set_gauge(g: &AtomicU64, v: u64) {
    if v != UNKNOWN {
        g.store(v, Ordering::Relaxed);
    }
}

/// Lock, recovering from poisoning: a panic elsewhere must not take the
/// metrics down with it.
fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

/// Stats other components already keep, passed in at snapshot time. `None`
/// for a component that is not running.
#[derive(Debug, Clone, Default)]
pub struct Sources {
    pub resolver: Option<ResolverMetricsSnapshot>,
    pub reconcile: Option<ReconcileStats>,
}

/// A source's counters by name. Every value is a monotonic counter.
pub type SourceCounters = BTreeMap<&'static str, u64>;

/// Counters from [`Sources`], flattened for exposure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SourcesSnapshot {
    pub resolver: Option<SourceCounters>,
    pub reconcile: Option<SourceCounters>,
}

fn resolver_counters(s: &ResolverMetricsSnapshot) -> SourceCounters {
    BTreeMap::from([
        ("transfers_processed", s.total_transfers_processed),
        ("fully_enriched", s.fully_enriched),
        ("partially_enriched", s.partially_enriched),
        ("mint_fetches_attempted", s.mint_fetches_attempted),
        ("mint_fetches_succeeded", s.mint_fetches_succeeded),
        ("mint_fetches_failed", s.mint_fetches_failed),
        ("cache_hits", s.cache_hits),
        ("filled_from_metadata", s.filled_from_metadata),
    ])
}

fn reconcile_counters(s: &ReconcileStats) -> SourceCounters {
    BTreeMap::from([
        ("passes", s.passes),
        ("passes_not_ready", s.passes_not_ready),
        ("passes_inconclusive", s.passes_inconclusive),
        ("slots_examined", s.slots_examined),
        ("slots_reconciled", s.slots_reconciled),
        ("unlogged_skips", s.unlogged_skips),
        ("rewinds", s.rewinds),
        ("rate_limited", s.rate_limited),
        ("unresolved", s.unresolved),
        ("skip_log_malformed_lines", s.skip_log_malformed_lines),
    ])
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Per-method RPC counters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RpcSnapshot {
    pub requests: u64,
    pub retries: u64,
    /// By [`ErrorClass`] label.
    pub errors: BTreeMap<&'static str, u64>,
    pub latency_us: HistogramSnapshot,
}

/// Per-table storage counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TableSnapshot {
    pub rows: u64,
    pub bytes: u64,
    pub files: u64,
}

/// Everything [`Metrics`] knows, at one (non-atomic) moment. Maps are keyed by
/// label value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricsSnapshot {
    pub state: PipelineState,
    /// Scrubbed. JSON only.
    pub halt_reason: Option<String>,
    /// Scrubbed. JSON only.
    pub last_error: Option<String>,

    pub tip_slot: Option<u64>,
    pub processed_slot: Option<u64>,
    pub flushed_slot: Option<u64>,
    pub finalized_slot: Option<u64>,
    /// `tip_slot - processed_slot`; `None` unless both are known.
    pub lag_slots: Option<u64>,
    pub changelog_bytes: Option<u64>,
    pub changelog_segments: Option<u64>,

    /// By [`RpcMethod`] label.
    pub rpc: BTreeMap<&'static str, RpcSnapshot>,
    pub rate_limit_wait_us: HistogramSnapshot,

    /// By [`Table`] label.
    pub tables: BTreeMap<&'static str, TableSnapshot>,
    pub flushes_ok: u64,
    pub flushes_failed: u64,
    pub flush_duration_us: HistogramSnapshot,

    /// By [`EventType`] label.
    pub events: BTreeMap<&'static str, u64>,
    pub slots_processed: u64,
    /// By [`SkipReason`] label.
    pub slots_skipped: BTreeMap<&'static str, u64>,
    /// By [`ProgramClass`] then [`DecodeOutcome`] label.
    pub instructions: BTreeMap<&'static str, BTreeMap<&'static str, u64>>,

    pub sources: SourcesSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_metrics_have_unknown_gauges_and_zero_counters() {
        let m = Metrics::new();
        let s = m.snapshot(&Sources::default());
        assert_eq!(s.state, PipelineState::Starting);
        assert_eq!(s.tip_slot, None);
        assert_eq!(s.lag_slots, None);
        assert_eq!(s.rpc["getBlock"].requests, 0);
        assert_eq!(s.rpc.len(), RpcMethod::COUNT);
        assert_eq!(s.sources, SourcesSnapshot::default());
    }

    #[test]
    fn lag_needs_both_ends() {
        let m = Metrics::new();
        m.set_tip(100);
        assert_eq!(m.snapshot(&Sources::default()).lag_slots, None);
        m.set_processed(90);
        assert_eq!(m.snapshot(&Sources::default()).lag_slots, Some(10));
        m.set_processed(0);
        assert_eq!(m.snapshot(&Sources::default()).lag_slots, Some(100));
        m.set_processed(150);
        assert_eq!(m.snapshot(&Sources::default()).lag_slots, Some(0));
    }

    #[test]
    fn recording_lands_in_labelled_slots() {
        let m = Metrics::new();
        m.rpc_request(RpcMethod::GetBlock);
        m.rpc_request(RpcMethod::GetBlock);
        m.rpc_retry(RpcMethod::GetBlock);
        m.rpc_error(RpcMethod::GetBlock, ErrorClass::Timeout);
        m.rpc_latency(RpcMethod::GetBlock, Duration::from_millis(20));
        m.rows_written(Table::Transactions, 7);
        m.files_written(Table::Transactions, 1);
        m.flush_done(true, Duration::from_millis(5));
        m.flush_done(false, Duration::from_millis(5));
        m.event_emitted(EventType::New);
        m.slot_processed();
        m.slot_skipped(SkipReason::RpcUnavailable);
        m.instruction(ProgramClass::SplToken, DecodeOutcome::Decoded);

        let s = m.snapshot(&Sources::default());
        let rpc = &s.rpc["getBlock"];
        assert_eq!((rpc.requests, rpc.retries), (2, 1));
        assert_eq!(rpc.errors["timeout"], 1);
        assert_eq!(rpc.latency_us.count, 1);
        assert_eq!(s.tables["transactions"].rows, 7);
        assert_eq!((s.flushes_ok, s.flushes_failed), (1, 1));
        assert_eq!(s.flush_duration_us.count, 2);
        assert_eq!(s.events["new"], 1);
        assert_eq!(s.slots_processed, 1);
        assert_eq!(s.slots_skipped["rpc_unavailable"], 1);
        assert_eq!(s.instructions["spl_token"]["decoded"], 1);
    }

    #[test]
    fn halt_reason_is_scrubbed_and_cleared_on_running() {
        let m = Metrics::new();
        m.set_halted("rpc https://x.io/?api-key=abc failed\n");
        let s = m.snapshot(&Sources::default());
        assert_eq!(s.state, PipelineState::Halted);
        assert_eq!(s.halt_reason.as_deref(), Some("rpc <url> failed "));
        m.set_running();
        let s = m.snapshot(&Sources::default());
        assert_eq!(s.state, PipelineState::Running);
        assert_eq!(s.halt_reason, None);
    }

    #[test]
    fn sources_are_exposed_when_present() {
        let m = Metrics::new();
        let sources = Sources {
            resolver: Some(ResolverMetricsSnapshot {
                cache_hits: 3,
                ..Default::default()
            }),
            reconcile: Some(ReconcileStats {
                rewinds: 2,
                ..Default::default()
            }),
        };
        let s = m.snapshot(&sources);
        assert_eq!(s.sources.resolver.unwrap()["cache_hits"], 3);
        assert_eq!(s.sources.reconcile.unwrap()["rewinds"], 2);
    }

    #[test]
    fn label_lookups() {
        assert_eq!(RpcMethod::from_name("getBlocks"), RpcMethod::GetBlocks);
        assert_eq!(RpcMethod::from_name("getFoo"), RpcMethod::Other);
        assert_eq!(ErrorClass::of(&PrismError::rate_limited(None)), ErrorClass::RateLimited);
        assert_eq!(
            ProgramClass::of("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"),
            ProgramClass::SplToken
        );
    }

    #[test]
    fn snapshot_serializes() {
        let m = Metrics::new();
        m.set_tip(5);
        let json = serde_json::to_value(m.snapshot(&Sources::default())).unwrap();
        assert_eq!(json["state"], "starting");
        assert_eq!(json["tip_slot"], 5);
        assert!(json["processed_slot"].is_null());
        assert_eq!(json["rpc"]["getBlock"]["requests"], 0);
    }
}
