//! Periodic digests: a fixed, plain-language summary of one window of slots.
//!
//! A [`Digester`] wakes every [`DIGEST_TICK_SECS`], works out which windows
//! are due, builds each one from LENS, the store's anomalies and the
//! auditor's latest report, and stores it with [`Store::insert_digest`].
//! [`DigestHandle`] is the API's view: status, stored digests, and a
//! provisional [`DigestHandle::preview`] of a window that is not done yet.
//!
//! # Windows
//!
//! A window is [`DIGEST_BUCKETS`] detector buckets, aligned: window `k`
//! covers buckets `k * 50 ..= k * 50 + 49`, so slots `k * window_slots ..=
//! (k + 1) * window_slots - 1` with `window_slots = 50 * bucket_slots`. The
//! bounds are a pure function of integers, so a window has one identity
//! however late it is built.
//!
//! A window is built **once**: `insert_digest` keeps the first body stored
//! for a range. So a digest is built only when it is **complete**:
//!
//! 1. **The data is in**: `end_slot <= W`, where `W` is the smaller of
//!    `MAX(slot)` of `solana.slots` and of `solana.transactions`, the newest
//!    slot both tables cover.
//! 2. **Every enabled detector has scored it**: each enabled detector's
//!    `evaluated_through >= last_bucket` (see [`Coverage`]). A detector that
//!    has not blocks the build, and the status says
//!    [`DigestState::Waiting`] with its id. A disabled detector does not
//!    block, and the body lists it as `enabled: false`.
//!
//! # Which window is next
//!
//! * After the latest stored digest: the first aligned window starting
//!   after its `end_slot`.
//! * With none stored, **from now**: the smallest window whose
//!   `first_bucket > complete_max`, where `complete_max = (W + 1) /
//!   bucket_slots - 1`. Detectors baseline at their first `complete_max` and
//!   report nothing before it, so earlier windows would claim "no anomalies"
//!   without having looked. [`DigestHandle::preview`] still shows them.
//! * The latest digest ends above `W`: PRISM probably restarted from a low
//!   slot after a ledger reset. Nothing is built, the state is
//!   [`DigestState::AheadOfData`], and a warning is logged on entry and then
//!   at most every [`AHEAD_WARN_EVERY_SECS`].
//!
//! # One tick
//!
//! Facts, then the next window, then up to [`MAX_WINDOWS_PER_TICK`] windows
//! in order. A window that is not data-complete stops the loop (no later one
//! can be). One that is not covered stops it with `Waiting`. A build error
//! stores nothing and stops; the next tick retries the same window. A
//! catch-up after downtime therefore stores digests oldest first, one after
//! another.
//!
//! # Building one window
//!
//! Every LENS query is bounded by `slot >= start_slot AND slot <= end_slot`,
//! so data arriving mid-build (or a catalog refresh) cannot change a number:
//! the window is already complete. Totals use `COUNT(*)`, as the detectors
//! do, so the digest's numbers and the anomaly baselines agree. Every literal
//! in the SQL is an integer computed here; an id read from LENS is validated
//! before it is shown, and is never put into another query. A `truncated`
//! result, or any LENS error, aborts the build.
//!
//! Anomalies come from the store, one bounded query per detector metric
//! (`since_bucket = first_bucket`, `until_bucket = last_bucket`), so one
//! metric's many rows cannot crowd out another's. A metric that hits
//! [`MAX_LIST_LIMIT`] sets `anomalies_truncated`.
//!
//! Quality is optional: [`Auditor::report`] with a [`QUALITY_MAX_AGE_SECS`]
//! cache, waited for at most [`QUALITY_WAIT_SECS`]. A failure makes the
//! section `unavailable`; the digest is still built.
//!
//! # Ranking and text
//!
//! [`assemble`] is a pure function of its inputs, and the order of the
//! anomaly rows does not matter: they are sorted before they are merged.
//! See its docs for the episode, ranking and first-seen rules, and
//! [`render`] for the sentence templates.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use forge::lens_client::{LensApi, LensCallError, LensResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL, RunnerHandle};
use crate::quality::{Auditor, Clock, Overall, QualityReport, Status};
use crate::store::{Anomaly, AnomalyFilter, AnomalyKind, MAX_DIGEST_BYTES, MAX_LIST_LIMIT, Store};

/// A stored digest, as the store returns it. `created_at` is when it was
/// built; the body has no time of its own.
pub type StoredDigest = crate::store::Digest;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Detector buckets per window.
pub const DIGEST_BUCKETS: u64 = 50;
/// Seconds between ticks.
pub const DIGEST_TICK_SECS: u64 = 60;
/// Most windows one tick builds, so a long catch-up yields between ticks.
pub const MAX_WINDOWS_PER_TICK: usize = 8;
/// Episodes kept in a digest; the rest are counted in `omitted.anomalies`.
pub const TOP_ANOMALIES: usize = 10;
/// First-seen entities listed per group; the rest are "and N more".
pub const FIRST_SEEN_SHOWN: usize = 5;
/// Busiest programs and mints listed.
pub const BUSIEST_SHOWN: u64 = 5;
/// Oldest cached quality report a digest accepts.
pub const QUALITY_MAX_AGE_SECS: u64 = 15 * 60;
/// Longest a build waits for the quality report before calling it
/// unavailable.
pub const QUALITY_WAIT_SECS: u64 = 60;
/// Deadline of one window's LENS and store reads.
pub const BUILD_DEADLINE_SECS: u64 = 120;
/// Body size the design allows.
pub const DIGEST_BODY_LIMIT: usize = 256 * 1024;
/// The size actually enforced: the store refuses anything above
/// [`MAX_DIGEST_BYTES`], and an over-size body must be trimmed here, not
/// lost there.
pub const EFFECTIVE_BODY_LIMIT: usize = if DIGEST_BODY_LIMIT < MAX_DIGEST_BYTES {
    DIGEST_BODY_LIMIT
} else {
    MAX_DIGEST_BYTES
};
/// Body format version.
pub const DIGEST_VERSION: u32 = 1;
/// `AheadOfData` is logged on entry, then at most this often.
pub const AHEAD_WARN_EVERY_SECS: u64 = 3600;

/// Rendered in place of an id that is not base58 of exactly 32 bytes.
pub const INVALID_ID: &str = "[invalid id]";
/// Rendered in place of a metric id that is not `[a-z0-9_]{1,64}`.
pub const UNKNOWN_METRIC: &str = "[unknown metric]";

pub const FIRST_SEEN_PROGRAM: &str = "first_seen_program";
pub const FIRST_SEEN_MINT: &str = "first_seen_mint";

/// Labels of the eight detector ids.
pub const METRIC_LABELS: [(&str, &str); 8] = [
    ("tx_count", "Transactions per bucket"),
    ("tx_failure_rate", "Transaction failure rate"),
    ("fee_per_tx", "Mean fee per transaction"),
    ("transfer_count", "Token transfers per bucket"),
    ("top_mint_share", "Busiest mint's share of transfers"),
    (FIRST_SEEN_MINT, "New mints"),
    (FIRST_SEEN_PROGRAM, "New programs"),
    ("program_failure_rate", "Program failure rate"),
];

/// Metrics whose value is a fraction in `0..=1`, rendered as a percentage.
pub const RATIO_METRICS: [&str; 3] = ["tx_failure_rate", "top_mint_share", "program_failure_rate"];

/// Well-known program names, until `catalog/entities` replaces this.
pub const PROGRAM_NAMES: [(&str, &str); 7] = [
    ("11111111111111111111111111111111", "System Program"),
    ("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "SPL Token"),
    ("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", "SPL Token-2022"),
    ("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL", "Associated Token Account"),
    ("ComputeBudget111111111111111111111111111111", "Compute Budget"),
    ("Vote111111111111111111111111111111111111111", "Vote"),
    ("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", "Memo"),
];

// ---------------------------------------------------------------------------
// Coverage
// ---------------------------------------------------------------------------

/// One detector's coverage: how far it has scored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DetectorCoverage {
    pub id: String,
    pub enabled: bool,
    /// The last bucket it has fully evaluated; `None` before its first run.
    pub evaluated_through: Option<u64>,
}

/// Where the digester learns how far the detectors have got. The runner's
/// handle implements it; tests fake it.
pub trait Coverage: Send + Sync {
    fn detectors(&self) -> Vec<DetectorCoverage>;
}

impl Coverage for RunnerHandle {
    fn detectors(&self) -> Vec<DetectorCoverage> {
        self.status()
            .detectors
            .into_iter()
            .map(|d| DetectorCoverage { id: d.id, enabled: d.enabled, evaluated_through: d.progress })
            .collect()
    }
}

/// Enabled detectors that have not evaluated `last_bucket`, by id.
pub fn blocking(detectors: &[DetectorCoverage], last_bucket: u64) -> Vec<String> {
    let mut on: Vec<String> = detectors
        .iter()
        .filter(|d| d.enabled && d.evaluated_through.is_none_or(|e| e < last_bucket))
        .map(|d| d.id.clone())
        .collect();
    on.sort();
    on
}

// ---------------------------------------------------------------------------
// Errors and status
// ---------------------------------------------------------------------------

/// Why no digest was produced.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DigestError {
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },
    /// LENS has no data, or no table, for it yet.
    #[error("not ready: {0}")]
    NotReady(String),
    /// LENS failed, or answered in a shape that cannot be used.
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("timed out: {0}")]
    Timeout(String),
    /// Another preview is running.
    #[error("another preview is running")]
    Busy,
    #[error("the digester is shut down")]
    Cancelled,
    #[error("store: {0}")]
    Store(String),
}

impl DigestError {
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid { field: field.into(), reason: reason.into() }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "invalid",
            Self::NotReady(_) => "not_ready",
            Self::Unavailable(_) => "unavailable",
            Self::Timeout(_) => "timeout",
            Self::Busy => "busy",
            Self::Cancelled => "cancelled",
            Self::Store(_) => "store",
        }
    }

    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::Busy => 429,
            Self::NotReady(_) | Self::Unavailable(_) | Self::Cancelled => 503,
            Self::Timeout(_) => 504,
            Self::Store(_) => 500,
        }
    }
}

fn lens_error(err: LensCallError) -> DigestError {
    match &err {
        LensCallError::BadQuery { message } if message.contains("not found") => DigestError::NotReady(err.to_string()),
        LensCallError::Timeout { .. } => DigestError::Timeout(err.to_string()),
        LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } | LensCallError::Unavailable { .. } => {
            DigestError::Unavailable(err.to_string())
        }
    }
}

/// What the digester is doing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum DigestState {
    /// Nothing due, or every due window is built.
    Idle,
    /// A window is being built.
    Building,
    /// The next window's data is in, but these enabled detectors have not
    /// scored it.
    Waiting { on: Vec<String> },
    /// LENS has no data, or no table, yet. Not a fault.
    NotReady,
    /// The latest digest ends above LENS's data: a ledger reset?
    AheadOfData,
    /// The last build or store call failed; retried next tick.
    Failing,
}

/// A window's bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestWindow {
    pub start_slot: u64,
    pub end_slot: u64,
    pub slots: u64,
    pub bucket_slots: u64,
    pub first_bucket: u64,
    pub last_bucket: u64,
}

impl DigestWindow {
    /// Window `index`; `None` if its slots overflow `i64`.
    pub fn at(index: u64, bucket_slots: u64) -> Option<Self> {
        let slots = window_slots(bucket_slots)?;
        let first_bucket = index.checked_mul(DIGEST_BUCKETS)?;
        let last_bucket = first_bucket.checked_add(DIGEST_BUCKETS - 1)?;
        let start_slot = index.checked_mul(slots)?;
        let end_slot = start_slot.checked_add(slots - 1)?;
        i64::try_from(end_slot).ok()?;
        Some(Self { start_slot, end_slot, slots, bucket_slots, first_bucket, last_bucket })
    }

    /// The window holding `slot`.
    pub fn containing(slot: u64, bucket_slots: u64) -> Option<Self> {
        Self::at(slot / window_slots(bucket_slots)?, bucket_slots)
    }

    /// The first window starting at or after `slot`.
    pub fn first_from(slot: u64, bucket_slots: u64) -> Option<Self> {
        Self::at(slot.div_ceil(window_slots(bucket_slots)?), bucket_slots)
    }

    pub const fn index(&self) -> u64 {
        self.first_bucket / DIGEST_BUCKETS
    }

    pub fn next(&self) -> Option<Self> {
        Self::at(self.index().checked_add(1)?, self.bucket_slots)
    }
}

/// `DIGEST_BUCKETS * bucket_slots`; `None` for 0 or an overflow.
pub fn window_slots(bucket_slots: u64) -> Option<u64> {
    if bucket_slots == 0 { None } else { DIGEST_BUCKETS.checked_mul(bucket_slots) }
}

/// The first window a store with no digests builds: the smallest whose
/// `first_bucket > complete_max`.
pub fn first_window(complete_max: u64, bucket_slots: u64) -> Option<DigestWindow> {
    DigestWindow::at(complete_max / DIGEST_BUCKETS + 1, bucket_slots)
}

/// Data-complete windows from `next` on that are not built yet.
pub fn lag_windows(next: &DigestWindow, w: u64) -> u64 {
    let ws = next.slots;
    let complete = (w.saturating_add(1)) / ws; // windows 0..complete have end <= w
    complete.saturating_sub(next.index())
}

/// The digester, for `/digests` and `/health`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DigestStatus {
    #[serde(flatten)]
    pub state: DigestState,
    /// The last window stored by this process.
    pub last_window: Option<DigestWindow>,
    /// Digests stored by this process.
    pub windows_built: u64,
    /// Data-complete windows still to build after the last tick.
    pub lag_windows: u64,
    pub last_error: Option<String>,
    /// Unix seconds, by the injected clock.
    pub last_tick_at: Option<u64>,
}

// ---------------------------------------------------------------------------
// The body
// ---------------------------------------------------------------------------

/// The stored body. `headline` is every sentence, in section order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Digest {
    pub version: u32,
    pub provisional: bool,
    pub window: DigestWindow,
    pub coverage: CoverageSection,
    pub overview: Overview,
    pub anomalies: Vec<Episode>,
    pub new_entities: NewEntities,
    pub quality: QualitySection,
    pub busiest: Busiest,
    pub omitted: Omitted,
    pub anomalies_truncated: bool,
    pub warnings: Vec<String>,
    pub headline: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoverageSection {
    /// Data in and every enabled detector through `last_bucket`. Always
    /// true for a stored digest.
    pub complete: bool,
    pub detectors: Vec<DetectorCoverage>,
    /// Provisional only: the newest slot the numbers include.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_through_slot: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Overview {
    pub transactions: u64,
    pub failed_transactions: u64,
    pub instructions: u64,
    pub token_transfers: u64,
}

/// Anomalies of one `(metric, subject, direction)` in consecutive buckets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Episode {
    pub metric: String,
    /// `""` for a global series; otherwise the entity, or [`INVALID_ID`].
    pub subject: String,
    pub direction: String,
    pub first_bucket: u64,
    pub last_bucket: u64,
    pub buckets: u64,
    pub peak_bucket: u64,
    /// The largest `|score|` of the episode.
    pub peak_score: f64,
    pub peak_value: f64,
    pub baseline: Option<f64>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEntity {
    pub id: String,
    pub first_slot: u64,
    pub text: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewEntities {
    pub programs: Vec<NewEntity>,
    pub mints: Vec<NewEntity>,
    /// Entities found but not listed, both groups together.
    pub omitted: u64,
    /// "And N more" and cap-reached sentences: programs', then mints'.
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualityIssue {
    pub id: String,
    pub status: String,
    pub violations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QualitySection {
    /// `available` or `unavailable`.
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overall: Option<Overall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of_slot: Option<u64>,
    /// Failing checks first, then warnings, each in the auditor's order.
    pub issues: Vec<QualityIssue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusyProgram {
    pub id: String,
    pub instructions: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BusyMint {
    pub id: String,
    pub transfers: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Busiest {
    pub programs: Vec<BusyProgram>,
    pub mints: Vec<BusyMint>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Omitted {
    /// Episodes not shown: below [`TOP_ANOMALIES`], or trimmed for size.
    pub anomalies: u64,
    /// Of those, the ones trimmed to fit [`EFFECTIVE_BODY_LIMIT`].
    #[serde(default)]
    pub trimmed_for_size: u64,
}

// ---------------------------------------------------------------------------
// Ids and numbers
// ---------------------------------------------------------------------------

/// Base58 of exactly 32 bytes (32 to 44 characters).
pub fn is_valid_id(id: &str) -> bool {
    if !(32..=44).contains(&id.len()) {
        return false;
    }
    let mut out = [0u8; 32];
    matches!(bs58::decode(id).onto(&mut out[..]), Ok(32))
}

/// `[a-z0-9_]{1,64}`.
pub fn is_plain_metric(id: &str) -> bool {
    (1..=64).contains(&id.len()) && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// A metric's label: the fixed map, else the id if plain, else
/// [`UNKNOWN_METRIC`].
pub fn metric_label(metric: &str) -> String {
    match METRIC_LABELS.iter().find(|(id, _)| *id == metric) {
        Some((_, label)) => (*label).to_owned(),
        None if is_plain_metric(metric) => metric.to_owned(),
        None => UNKNOWN_METRIC.to_owned(),
    }
}

/// `"{name} ({id})"` for a known program, the id otherwise. `id` must
/// already be valid or [`INVALID_ID`].
pub fn name_or_id(id: &str) -> String {
    match PROGRAM_NAMES.iter().find(|(known, _)| *known == id) {
        Some((_, name)) => format!("{name} ({id})"),
        None => id.to_owned(),
    }
}

/// A finite value: integral ones plain, others with one decimal.
pub fn fmt_num(v: f64) -> Option<String> {
    if !v.is_finite() {
        return None;
    }
    if v.fract() == 0.0 && v.abs() < 1e15 {
        Some(format!("{}", v as i64))
    } else {
        Some(format!("{v:.1}"))
    }
}

/// A fraction as a percentage with one decimal: `0.125` is `12.5%`.
pub fn fmt_rate(v: f64) -> Option<String> {
    let p = v * 100.0;
    p.is_finite().then(|| format!("{p:.1}%"))
}

/// `n` of `d` as a percentage with one decimal, without the sign.
pub fn fmt_share(n: u64, d: u64) -> Option<String> {
    (d > 0).then(|| format!("{:.1}", n as f64 * 100.0 / d as f64))
}

/// A multiple: one decimal from 1 up, two below.
pub fn fmt_factor(f: f64) -> Option<String> {
    if !f.is_finite() {
        return None;
    }
    Some(if f >= 1.0 { format!("{f:.1}×") } else { format!("{f:.2}×") })
}

fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Counts invalid ids so the body can report them once.
#[derive(Default)]
struct Ids {
    invalid: u64,
}

impl Ids {
    fn check(&mut self, id: &str) -> String {
        if is_valid_id(id) {
            id.to_owned()
        } else {
            self.invalid += 1;
            INVALID_ID.to_owned()
        }
    }

    /// A subject: `""` stays `""`.
    fn subject(&mut self, subject: &str) -> String {
        if subject.is_empty() { String::new() } else { self.check(subject) }
    }
}

// ---------------------------------------------------------------------------
// Sentences
// ---------------------------------------------------------------------------

/// The sentence templates. Every number goes through the `fmt_*` rules; a
/// non-finite number is left out of its sentence.
///
/// | Case                          | Template |
/// |-------------------------------|----------|
/// | Overview                      | `Slots {s}–{e}: {tx} transactions, {failed} failed ({pct}%), {ix} instructions, {xfers} token transfers.` (`({pct}%)` only with transactions; `Slots {s}–{e} (provisional): …` for a preview) |
/// | Count, baseline ≥ 1           | `{Label}{ for X} rose to {v} in slots {a}–{b}, {f}× its usual {base}.` (`fell to` going down) |
/// | Count, baseline < 1           | `{Label}{ for X} rose to {v} in slots {a}–{b}; it is usually {base:.2}.` |
/// | Ratio metric                  | `{Label}{ for X} rose to {v%} in slots {a}–{b}, from a usual {base%}.` |
/// | Episode of several buckets    | `in slots` becomes `at its peak in slots`, plus ` The episode spanned {n} buckets, slots {x}–{y}.` |
/// | Per-subject metric            | ` for {name_or_id}` after the label |
/// | First seen                    | `New program {name_or_id} first seen at slot {slot}.` / `New mint {id} first seen at slot {slot}.` |
/// | More                          | `And {n} more new programs.` |
/// | Cap reached                   | `New programs in slots {a}–{b} reached the detector's cap; the list may be incomplete.` |
/// | Quality                       | `Pipeline quality is {overall} as of slot {slot}.` |
/// | Quality fail / warn           | `Quality check {id} failed: {n} violations.` / `Quality check {id} warned: {n} violations.` |
/// | Quality unavailable           | `Pipeline quality is unavailable for this digest.` |
/// | Busiest                       | `Busiest program: {name_or_id} with {n} instructions ({share}% of all instructions).` / `Busiest mint: {id} with {n} token transfers ({share}% of all token transfers).` |
pub mod render {
    use super::*;

    pub fn overview(window: &DigestWindow, through: u64, provisional: bool, o: &Overview) -> String {
        let tag = if provisional { " (provisional)" } else { "" };
        let failed = match fmt_share(o.failed_transactions, o.transactions) {
            Some(pct) => format!("{} failed ({pct}%)", o.failed_transactions),
            None => format!("{} failed", o.failed_transactions),
        };
        format!(
            "Slots {}–{}{tag}: {}, {failed}, {}, {}.",
            window.start_slot,
            through,
            plural(o.transactions, "transaction", "transactions"),
            plural(o.instructions, "instruction", "instructions"),
            plural(o.token_transfers, "token transfer", "token transfers"),
        )
    }

    /// The episode's sentence. `peak_slots` are the peak bucket's slots,
    /// `span_slots` the whole episode's.
    pub fn episode(e: &Episode, bucket_slots: u64) -> String {
        let label = metric_label(&e.metric);
        let subject = if e.subject.is_empty() { String::new() } else { format!(" for {}", name_or_id(&e.subject)) };
        let verb = if e.direction == "down" { "fell to" } else { "rose to" };
        let (a, b) = bucket_slots_of(e.peak_bucket, bucket_slots);
        let place = if e.buckets > 1 { format!("at its peak in slots {a}–{b}") } else { format!("in slots {a}–{b}") };
        let ratio = RATIO_METRICS.contains(&e.metric.as_str());

        let value = if ratio { fmt_rate(e.peak_value) } else { fmt_num(e.peak_value) };
        let mut s = match value {
            Some(v) => format!("{label}{subject} {verb} {v} {place}"),
            None => format!("{label}{subject} was unusual {place}"),
        };
        let base = e.baseline.filter(|b| b.is_finite());
        match base {
            Some(base) if ratio => {
                if let Some(b) = fmt_rate(base) {
                    s.push_str(&format!(", from a usual {b}"));
                }
            }
            Some(base) if base.abs() >= 1.0 => {
                let factor = fmt_factor(e.peak_value / base);
                match (factor, fmt_num(base)) {
                    (Some(f), Some(b)) => s.push_str(&format!(", {f} its usual {b}")),
                    (None, Some(b)) => s.push_str(&format!(", against a usual {b}")),
                    _ => {}
                }
            }
            Some(base) => s.push_str(&format!("; it is usually {base:.2}")),
            None => {}
        }
        s.push('.');
        if e.buckets > 1 {
            let (x, _) = bucket_slots_of(e.first_bucket, bucket_slots);
            let (_, y) = bucket_slots_of(e.last_bucket, bucket_slots);
            s.push_str(&format!(" The episode spanned {} buckets, slots {x}–{y}.", e.buckets));
        }
        s
    }

    pub fn new_program(id: &str, first_slot: u64) -> String {
        format!("New program {} first seen at slot {first_slot}.", name_or_id(id))
    }

    pub fn new_mint(id: &str, first_slot: u64) -> String {
        format!("New mint {id} first seen at slot {first_slot}.")
    }

    pub fn more(n: u64, noun_one: &str, noun_many: &str) -> String {
        format!("And {n} more {}.", if n == 1 { noun_one } else { noun_many })
    }

    pub fn cap(group: &str, a: u64, b: u64) -> String {
        format!("{group} in slots {a}–{b} reached the detector's cap; the list may be incomplete.")
    }

    pub fn quality(q: &QualitySection) -> Vec<String> {
        let (Some(overall), Some(slot)) = (q.overall, q.as_of_slot) else {
            return vec!["Pipeline quality is unavailable for this digest.".to_owned()];
        };
        let overall = match overall {
            Overall::Pass => "pass",
            Overall::Warn => "warn",
            Overall::Incomplete => "incomplete",
            Overall::Fail => "fail",
        };
        let mut out = vec![format!("Pipeline quality is {overall} as of slot {slot}.")];
        for i in &q.issues {
            let verb = if i.status == "fail" { "failed" } else { "warned" };
            out.push(format!("Quality check {} {verb}: {}.", i.id, plural(i.violations, "violation", "violations")));
        }
        out
    }

    pub fn busiest_program(id: &str, n: u64, total: u64) -> String {
        let share = fmt_share(n, total).map(|s| format!(" ({s}% of all instructions)")).unwrap_or_default();
        format!("Busiest program: {} with {}{share}.", name_or_id(id), plural(n, "instruction", "instructions"))
    }

    pub fn busiest_mint(id: &str, n: u64, total: u64) -> String {
        let share = fmt_share(n, total).map(|s| format!(" ({s}% of all token transfers)")).unwrap_or_default();
        format!("Busiest mint: {id} with {}{share}.", plural(n, "token transfer", "token transfers"))
    }
}

fn bucket_slots_of(bucket: u64, bucket_slots: u64) -> (u64, u64) {
    let a = bucket.saturating_mul(bucket_slots);
    (a, a.saturating_add(bucket_slots - 1))
}

/// Every sentence of `d`, in section order.
pub fn headline(d: &Digest) -> Vec<String> {
    let through = d.coverage.data_through_slot.unwrap_or(d.window.end_slot);
    let mut out = vec![render::overview(&d.window, through, d.provisional, &d.overview)];
    out.extend(d.anomalies.iter().map(|e| e.text.clone()));
    out.extend(d.new_entities.programs.iter().map(|e| e.text.clone()));
    out.extend(d.new_entities.mints.iter().map(|e| e.text.clone()));
    out.extend(d.new_entities.notes.iter().cloned());
    out.extend(render::quality(&d.quality));
    if let Some(p) = d.busiest.programs.first() {
        out.push(render::busiest_program(&p.id, p.instructions, d.overview.instructions));
    }
    if let Some(m) = d.busiest.mints.first() {
        out.push(render::busiest_mint(&m.id, m.transfers, d.overview.token_transfers));
    }
    out
}

// ---------------------------------------------------------------------------
// Assembly
// ---------------------------------------------------------------------------

/// What LENS said about a window.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WindowData {
    pub overview: Overview,
    /// `(program, instructions)`, busiest first, as LENS returned them.
    pub programs: Vec<(String, u64)>,
    /// `(mint, transfers)`, busiest first.
    pub mints: Vec<(String, u64)>,
}

/// Everything one digest is built from.
#[derive(Debug, Clone)]
pub struct AssembleInput<'a> {
    pub window: DigestWindow,
    pub provisional: bool,
    pub coverage: CoverageSection,
    pub data: &'a WindowData,
    /// The store's rows for the window, in any order.
    pub anomalies: &'a [Anomaly],
    pub anomalies_truncated: bool,
    /// The auditor's report, or why there is none.
    pub quality: Result<&'a QualityReport, String>,
}

/// The direction of one row: `detail.direction` if it says `up` or `down`,
/// else the sign of `value - baseline` (`up` without a baseline).
pub fn direction_of(a: &Anomaly) -> &'static str {
    match a.detail.as_ref().and_then(|d| d.get("direction")).and_then(Value::as_str) {
        Some("up") => "up",
        Some("down") => "down",
        _ => match a.baseline {
            Some(b) if a.value < b => "down",
            _ => "up",
        },
    }
}

/// Merge rows into episodes, unranked and without text.
///
/// Only series, concentration and program-failure rows take part. Rows are
/// sorted by `(metric, subject, direction, bucket, id)` first, so the input
/// order never matters; a row whose bucket follows the previous one's
/// (`bucket <= last + 1`) of the same key extends its episode. The peak is
/// the row with the largest `|score|`, the earliest on a tie.
pub fn episodes(rows: &[Anomaly]) -> Vec<Episode> {
    let mut scored: Vec<(&Anomaly, &'static str)> = rows
        .iter()
        .filter(|a| matches!(a.kind, AnomalyKind::Series | AnomalyKind::Concentration | AnomalyKind::ProgramFailure))
        .map(|a| (a, direction_of(a)))
        .collect();
    scored.sort_by(|(a, da), (b, db)| {
        (&a.metric, &a.subject, *da, a.bucket, a.id).cmp(&(&b.metric, &b.subject, *db, b.bucket, b.id))
    });

    let mut out: Vec<Episode> = Vec::new();
    let mut prev: Option<(&str, &str, &str)> = None;
    for (a, dir) in scored {
        let key = (a.metric.as_str(), a.subject.as_str(), dir);
        let extends = prev == Some(key) && out.last().is_some_and(|e| a.bucket <= e.last_bucket + 1);
        let magnitude = a.score.abs();
        if extends {
            let e = out.last_mut().unwrap();
            if a.bucket > e.last_bucket {
                e.last_bucket = a.bucket;
                e.buckets += 1;
            }
            if magnitude > e.peak_score {
                e.peak_score = magnitude;
                e.peak_bucket = a.bucket;
                e.peak_value = a.value;
                e.baseline = a.baseline;
            }
        } else {
            out.push(Episode {
                metric: a.metric.clone(),
                subject: a.subject.clone(),
                direction: dir.to_owned(),
                first_bucket: a.bucket,
                last_bucket: a.bucket,
                buckets: 1,
                peak_bucket: a.bucket,
                peak_score: magnitude,
                peak_value: a.value,
                baseline: a.baseline,
                text: String::new(),
            });
        }
        prev = Some(key);
    }
    out
}

/// Rank: peak score descending, then `(metric, subject, first_bucket,
/// direction)` ascending.
pub fn rank(episodes: &mut [Episode]) {
    episodes.sort_by(|a, b| {
        b.peak_score
            .total_cmp(&a.peak_score)
            .then_with(|| (&a.metric, &a.subject, a.first_bucket, &a.direction).cmp(&(&b.metric, &b.subject, b.first_bucket, &b.direction)))
    });
}

fn first_slot_of(a: &Anomaly) -> u64 {
    a.detail
        .as_ref()
        .and_then(|d| d.get("first_slot"))
        .and_then(Value::as_u64)
        .unwrap_or(a.start_slot)
}

/// Build a digest. Pure: the same input gives the same body, whatever the
/// order of `input.anomalies`.
///
/// * **Episodes**: see [`episodes`] and [`rank`]; the top [`TOP_ANOMALIES`]
///   are kept and the rest counted in `omitted.anomalies`.
/// * **First seen**: never scored. `first_seen_program` and
///   `first_seen_mint` rows, each group ordered by `(bucket, subject)`; the
///   first [`FIRST_SEEN_SHOWN`] are listed and the rest are "and N more". A
///   row with an empty subject is the detector's cap marker and becomes a
///   note, not an entity.
/// * **Ids**: any subject, program or mint that is not base58 of 32 bytes
///   is shown as [`INVALID_ID`] and counted in one warning.
/// * **Size**: if the body is over [`EFFECTIVE_BODY_LIMIT`], the
///   lowest-ranked episodes go first, then listed entities, recorded in
///   `omitted` and `warnings`.
pub fn assemble(input: AssembleInput<'_>) -> Digest {
    let AssembleInput { window, provisional, coverage, data, anomalies, anomalies_truncated, quality } = input;
    let bs = window.bucket_slots;
    let mut ids = Ids::default();
    let mut warnings = Vec::new();

    let in_window: Vec<Anomaly> = anomalies
        .iter()
        .filter(|a| (window.first_bucket..=window.last_bucket).contains(&a.bucket))
        .cloned()
        .collect();

    // Episodes.
    let mut eps = episodes(&in_window);
    rank(&mut eps);
    let omitted_eps = eps.len().saturating_sub(TOP_ANOMALIES) as u64;
    eps.truncate(TOP_ANOMALIES);
    for e in &mut eps {
        e.subject = ids.subject(&e.subject);
        e.text = render::episode(e, bs);
    }

    // First seen.
    let mut new_entities = NewEntities::default();
    let mut unknown_first_seen = 0u64;
    let mut groups: BTreeMap<&str, Vec<&Anomaly>> = BTreeMap::new();
    for a in in_window.iter().filter(|a| a.kind == AnomalyKind::FirstSeen) {
        if a.metric == FIRST_SEEN_PROGRAM || a.metric == FIRST_SEEN_MINT {
            groups.entry(a.metric.as_str()).or_default().push(a);
        } else {
            unknown_first_seen += 1;
        }
    }
    for metric in [FIRST_SEEN_PROGRAM, FIRST_SEEN_MINT] {
        let mut rows = groups.remove(metric).unwrap_or_default();
        rows.sort_by(|a, b| (a.bucket, &a.subject, a.id).cmp(&(b.bucket, &b.subject, b.id)));
        let (caps, entities): (Vec<&Anomaly>, Vec<&Anomaly>) = rows.into_iter().partition(|a| a.subject.is_empty());
        let programs = metric == FIRST_SEEN_PROGRAM;
        let mut listed = Vec::new();
        for a in entities.iter().take(FIRST_SEEN_SHOWN) {
            let id = ids.check(&a.subject);
            let slot = first_slot_of(a);
            let text = if programs { render::new_program(&id, slot) } else { render::new_mint(&id, slot) };
            listed.push(NewEntity { id, first_slot: slot, text });
        }
        let more = entities.len().saturating_sub(FIRST_SEEN_SHOWN) as u64;
        if more > 0 {
            new_entities.omitted += more;
            new_entities.notes.push(if programs {
                render::more(more, "new program", "new programs")
            } else {
                render::more(more, "new mint", "new mints")
            });
        }
        if let (Some(lo), Some(hi)) = (caps.iter().map(|a| a.bucket).min(), caps.iter().map(|a| a.bucket).max()) {
            let (a, _) = bucket_slots_of(lo, bs);
            let (_, b) = bucket_slots_of(hi, bs);
            new_entities.notes.push(render::cap(if programs { "New programs" } else { "New mints" }, a, b));
        }
        if programs { new_entities.programs = listed } else { new_entities.mints = listed }
    }
    if unknown_first_seen > 0 {
        warnings.push(format!("{} first-seen rows of an unknown metric were skipped", unknown_first_seen));
    }

    // Quality.
    let quality = match quality {
        Ok(r) => {
            let mut issues = Vec::new();
            for wanted in [Status::Fail, Status::Warn] {
                for c in r.checks.iter().filter(|c| c.status == wanted) {
                    issues.push(QualityIssue { id: c.id.to_owned(), status: wanted.as_str().to_owned(), violations: c.violations });
                }
            }
            QualitySection {
                status: "available".into(),
                overall: Some(r.overall),
                as_of_slot: Some(r.as_of_slot),
                issues,
                reason: None,
            }
        }
        Err(reason) => QualitySection {
            status: "unavailable".into(),
            overall: None,
            as_of_slot: None,
            issues: Vec::new(),
            reason: Some(reason),
        },
    };

    // Busiest.
    let busiest = Busiest {
        programs: data.programs.iter().map(|(id, n)| BusyProgram { id: ids.check(id), instructions: *n }).collect(),
        mints: data.mints.iter().map(|(id, n)| BusyMint { id: ids.check(id), transfers: *n }).collect(),
    };

    if ids.invalid > 0 {
        warnings.push(format!("{} rendered as {INVALID_ID}: not base58 of 32 bytes", plural(ids.invalid, "id", "ids")));
    }
    if anomalies_truncated {
        warnings.push(format!("a metric had at least {MAX_LIST_LIMIT} anomalies in this window; some were not read"));
    }

    let mut d = Digest {
        version: DIGEST_VERSION,
        provisional,
        window,
        coverage,
        overview: data.overview,
        anomalies: eps,
        new_entities,
        quality,
        busiest,
        omitted: Omitted { anomalies: omitted_eps, trimmed_for_size: 0 },
        anomalies_truncated,
        warnings,
        headline: Vec::new(),
    };
    d.headline = headline(&d);
    fit(&mut d, EFFECTIVE_BODY_LIMIT);
    d
}

/// Serialized size of `d`.
pub fn body_size(d: &Digest) -> usize {
    serde_json::to_vec(d).map_or(usize::MAX, |v| v.len())
}

/// Trim `d` to at most `limit` bytes: lowest-ranked episodes first, then
/// listed entities. The overview is never dropped.
pub fn fit(d: &mut Digest, limit: usize) {
    let mut trimmed = 0u64;
    let mut dropped_entities = 0u64;
    while body_size(d) > limit {
        if d.anomalies.pop().is_some() {
            trimmed += 1;
            d.omitted.anomalies += 1;
            d.omitted.trimmed_for_size += 1;
        } else if d.new_entities.mints.pop().is_some() || d.new_entities.programs.pop().is_some() {
            dropped_entities += 1;
            d.new_entities.omitted += 1;
        } else if !d.new_entities.notes.is_empty() {
            d.new_entities.notes.clear();
        } else if d.quality.issues.pop().is_some() {
        } else {
            break;
        }
        d.headline = headline(d);
    }
    if trimmed > 0 || dropped_entities > 0 {
        d.warnings.push(format!(
            "trimmed to fit {limit} bytes: {trimmed} anomalies and {dropped_entities} new entities left out"
        ));
        d.headline = headline(d);
    }
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// The window's statements. Each takes two slots and splices in nothing but
/// integers computed here and constants written here.
pub mod sql {
    use super::BUSIEST_SHOWN;

    pub fn transactions(from: u64, to: u64) -> String {
        format!(
            "SELECT COUNT(*) AS transactions, COUNT(CASE WHEN status = 'failed' THEN 1 END) AS failed \
             FROM solana.transactions WHERE slot >= {from} AND slot <= {to}"
        )
    }

    pub fn instructions(from: u64, to: u64) -> String {
        format!("SELECT COUNT(*) AS n FROM solana.instructions WHERE slot >= {from} AND slot <= {to}")
    }

    pub fn token_transfers(from: u64, to: u64) -> String {
        format!("SELECT COUNT(*) AS n FROM solana.token_transfers WHERE slot >= {from} AND slot <= {to}")
    }

    pub fn busiest_programs(from: u64, to: u64) -> String {
        format!(
            "SELECT program_id AS subject, COUNT(*) AS n FROM solana.instructions \
             WHERE slot >= {from} AND slot <= {to} AND program_id IS NOT NULL \
             GROUP BY program_id ORDER BY n DESC, subject ASC LIMIT {BUSIEST_SHOWN}"
        )
    }

    pub fn busiest_mints(from: u64, to: u64) -> String {
        format!(
            "SELECT mint_token_address AS subject, COUNT(*) AS n FROM solana.token_transfers \
             WHERE slot >= {from} AND slot <= {to} AND mint_token_address IS NOT NULL \
             GROUP BY mint_token_address ORDER BY n DESC, subject ASC LIMIT {BUSIEST_SHOWN}"
        )
    }

    /// Every statement of one window, in the order they run.
    pub fn all(from: u64, to: u64) -> [String; 5] {
        [
            transactions(from, to),
            instructions(from, to),
            token_transfers(from, to),
            busiest_programs(from, to),
            busiest_mints(from, to),
        ]
    }
}

fn malformed(what: impl std::fmt::Display) -> DigestError {
    DigestError::Unavailable(format!("unexpected LENS result: {what}"))
}

fn one_row(r: &LensResponse) -> Result<&Map<String, Value>, DigestError> {
    match r.rows.as_slice() {
        [row] => Ok(row),
        rows => Err(malformed(format!("{} rows, expected 1", rows.len()))),
    }
}

fn get_u64(row: &Map<String, Value>, column: &str) -> Result<u64, DigestError> {
    row.get(column).and_then(Value::as_u64).ok_or_else(|| malformed(format!("column {column} is not a count")))
}

fn top_rows(r: &LensResponse, total: u64) -> Result<Vec<(String, u64)>, DigestError> {
    if r.rows.len() as u64 > BUSIEST_SHOWN {
        return Err(malformed("more rows than the limit"));
    }
    r.rows
        .iter()
        .map(|row| {
            let n = get_u64(row, "n")?;
            let id = match row.get("subject") {
                Some(Value::String(s)) => s.clone(),
                _ => return Err(malformed("column subject is not a string")),
            };
            if n > total {
                return Err(DigestError::Unavailable("a busiest count exceeds its total; data changed mid-build".into()));
            }
            Ok((id, n))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The digester
// ---------------------------------------------------------------------------

/// The facts of one tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Facts {
    /// The newest slot both tables cover.
    pub w: u64,
    /// The newest complete bucket, `None` before the first.
    pub complete_max: Option<u64>,
}

impl Facts {
    pub fn new(max_slot: u64, max_tx_slot: u64, bucket_slots: u64) -> Self {
        let w = max_slot.min(max_tx_slot);
        let complete_max = (w.saturating_add(1) / bucket_slots.max(1)).checked_sub(1);
        Self { w, complete_max }
    }
}

struct Inner {
    status: DigestStatus,
    /// Clock seconds of the last `AheadOfData` warning.
    ahead_warned_at: Option<u64>,
    /// The first window of an empty store, fixed on the first tick that sees
    /// a complete bucket so "from now" does not slide as data arrives.
    anchor: Option<DigestWindow>,
}

struct Shared {
    store: Store,
    lens: Arc<dyn LensApi>,
    auditor: Arc<Auditor>,
    coverage: Arc<dyn Coverage>,
    clock: Arc<dyn Clock>,
    bucket_slots: u64,
    inner: Mutex<Inner>,
    /// Held by a preview: one at a time.
    preview: tokio::sync::Mutex<()>,
    cancel: CancellationToken,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Builds and stores digests every [`DIGEST_TICK_SECS`].
pub struct Digester {
    shared: Arc<Shared>,
}

/// A cloneable view of a [`Digester`] for the API.
#[derive(Clone)]
pub struct DigestHandle {
    shared: Arc<Shared>,
}

impl Digester {
    pub fn new(
        store: Store,
        lens: Arc<dyn LensApi>,
        auditor: Arc<Auditor>,
        coverage: Arc<dyn Coverage>,
        clock: Arc<dyn Clock>,
        bucket_slots: u64,
    ) -> Self {
        assert!(window_slots(bucket_slots).is_some(), "bucket_slots must be positive");
        Self {
            shared: Arc::new(Shared {
                store,
                lens,
                auditor,
                coverage,
                clock,
                bucket_slots,
                inner: Mutex::new(Inner {
                    status: DigestStatus {
                        state: DigestState::Idle,
                        last_window: None,
                        windows_built: 0,
                        lag_windows: 0,
                        last_error: None,
                        last_tick_at: None,
                    },
                    ahead_warned_at: None,
                    anchor: None,
                }),
                preview: tokio::sync::Mutex::new(()),
                cancel: CancellationToken::new(),
            }),
        }
    }

    pub fn handle(&self) -> DigestHandle {
        DigestHandle { shared: Arc::clone(&self.shared) }
    }

    /// Tick now, then every [`DIGEST_TICK_SECS`], until `shutdown`.
    pub fn spawn(self, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(bucket_slots = self.shared.bucket_slots, "digests started");
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = self.run_once() => {}
                }
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    () = tokio::time::sleep(Duration::from_secs(DIGEST_TICK_SECS)) => {}
                }
            }
            self.shared.cancel.cancel();
            info!("digests stopped");
        })
    }

    /// One tick (see the module docs). Tests call it directly.
    pub async fn run_once(&self) {
        let shared = &self.shared;
        let now = shared.clock.now_unix_secs();
        lock(&shared.inner).status.last_tick_at = Some(now);

        let facts = match facts(shared).await {
            Ok(f) => f,
            Err(DigestError::NotReady(msg)) => return self.set(DigestState::NotReady, Some(msg)),
            Err(e @ (DigestError::Timeout(_) | DigestError::Unavailable(_))) => {
                // Transient: skip the tick, keep the state.
                debug!(error = %e, "digest tick skipped");
                lock(&shared.inner).status.last_error = Some(e.to_string());
                return;
            }
            Err(e) => return self.set(DigestState::Failing, Some(e.to_string())),
        };

        let latest = match store_call(shared, |s| s.latest_digest()).await {
            Ok(latest) => latest,
            Err(e) => return self.set(DigestState::Failing, Some(e.to_string())),
        };
        let next = match &latest {
            Some(d) if d.end_slot > facts.w => {
                self.ahead(d.end_slot, facts.w, now);
                return;
            }
            Some(d) => DigestWindow::first_from(d.end_slot + 1, shared.bucket_slots),
            None => match facts.complete_max {
                None => return self.set(DigestState::NotReady, Some("no complete bucket yet".into())),
                Some(cm) => {
                    let mut inner = lock(&shared.inner);
                    match inner.anchor {
                        Some(a) => Some(a),
                        None => {
                            inner.anchor = first_window(cm, shared.bucket_slots);
                            inner.anchor
                        }
                    }
                }
            },
        };
        let Some(mut window) = next else {
            return self.set(DigestState::Failing, Some("the next window's slots overflow".into()));
        };
        lock(&shared.inner).ahead_warned_at = None;

        let mut outcome = (DigestState::Idle, None);
        for _ in 0..MAX_WINDOWS_PER_TICK {
            if shared.cancel.is_cancelled() {
                return;
            }
            if window.end_slot > facts.w {
                break; // Not data-complete: nothing later is either.
            }
            let detectors = shared.coverage.detectors();
            let on = blocking(&detectors, window.last_bucket);
            if !on.is_empty() {
                outcome = (DigestState::Waiting { on }, None);
                break;
            }
            lock(&shared.inner).status.state = DigestState::Building;
            let coverage = CoverageSection { complete: true, detectors, data_through_slot: None };
            let built = tokio::select! {
                () = shared.cancel.cancelled() => return,
                built = build(shared, window, window.end_slot, false, coverage) => built,
            };
            let digest = match built {
                Ok(d) => d,
                Err(DigestError::NotReady(msg)) => {
                    outcome = (DigestState::NotReady, Some(msg));
                    break;
                }
                Err(e) => {
                    warn!(start_slot = window.start_slot, error = %e, "digest build failed; retried next tick");
                    outcome = (DigestState::Failing, Some(e.to_string()));
                    break;
                }
            };
            let body = match serde_json::to_value(&digest) {
                Ok(b) => b,
                Err(e) => {
                    outcome = (DigestState::Failing, Some(format!("serialize digest: {e}")));
                    break;
                }
            };
            let (start, end) = (window.start_slot, window.end_slot);
            match store_call(shared, move |s| s.insert_digest(start, end, &body)).await {
                Ok(inserted) => {
                    let mut inner = lock(&shared.inner);
                    if inserted {
                        inner.status.windows_built += 1;
                        info!(start_slot = start, end_slot = end, "digest stored");
                    }
                    inner.status.last_window = Some(window);
                }
                Err(e) => {
                    outcome = (DigestState::Failing, Some(e.to_string()));
                    break;
                }
            }
            match window.next() {
                Some(n) => window = n,
                None => break,
            }
        }
        lock(&shared.inner).status.lag_windows = lag_windows(&window, facts.w);
        self.set(outcome.0, outcome.1);
    }

    fn set(&self, state: DigestState, error: Option<String>) {
        let mut inner = lock(&self.shared.inner);
        inner.status.state = state;
        inner.status.last_error = error;
    }

    fn ahead(&self, end_slot: u64, w: u64, now: u64) {
        let msg = format!(
            "the latest digest ends at slot {end_slot}, above LENS's newest covered slot {w}; if the ledger was reset, \
             prune the old digests"
        );
        let mut inner = lock(&self.shared.inner);
        let due = inner.status.state != DigestState::AheadOfData
            || inner.ahead_warned_at.is_none_or(|t| now.saturating_sub(t) >= AHEAD_WARN_EVERY_SECS);
        if due {
            warn!(end_slot, w, "digests are ahead of LENS's data");
            inner.ahead_warned_at = Some(now);
        }
        inner.status.state = DigestState::AheadOfData;
        inner.status.last_error = Some(msg);
        inner.status.lag_windows = 0;
    }
}

async fn store_call<T: Send + 'static>(
    shared: &Shared,
    f: impl FnOnce(&Store) -> crate::OracleResult<T> + Send + 'static,
) -> Result<T, DigestError> {
    let store = shared.store.clone();
    match tokio::task::spawn_blocking(move || f(&store)).await {
        Ok(r) => r.map_err(|e| DigestError::Store(e.to_string())),
        Err(e) => Err(DigestError::Store(format!("store task failed: {e}"))),
    }
}

async fn query(shared: &Shared, sql: &str) -> Result<LensResponse, DigestError> {
    let r = shared.lens.query(sql).await.map_err(lens_error)?;
    if r.truncated {
        return Err(DigestError::Unavailable("LENS truncated a digest query".into()));
    }
    Ok(r)
}

async fn max_slot(shared: &Shared, sql: &str, table: &str) -> Result<u64, DigestError> {
    let r = query(shared, sql).await?;
    match one_row(&r)?.get("max_slot") {
        Some(Value::Null) => Err(DigestError::NotReady(format!("{table} has no rows yet"))),
        Some(v) => v.as_u64().ok_or_else(|| malformed("max_slot is not a slot")),
        None => Err(malformed("no max_slot column")),
    }
}

async fn facts(shared: &Shared) -> Result<Facts, DigestError> {
    let slots = max_slot(shared, MAX_SLOTS_SQL, "solana.slots").await?;
    let txs = max_slot(shared, MAX_TX_SLOT_SQL, "solana.transactions").await?;
    Ok(Facts::new(slots, txs, shared.bucket_slots))
}

/// Read and assemble one window, its numbers bounded to `start_slot ..=
/// through`.
async fn build(
    shared: &Shared,
    window: DigestWindow,
    through: u64,
    provisional: bool,
    coverage: CoverageSection,
) -> Result<Digest, DigestError> {
    let read = async {
        let (from, to) = (window.start_slot, through);
        let tx = query(shared, &sql::transactions(from, to)).await?;
        let row = one_row(&tx)?;
        let (transactions, failed) = (get_u64(row, "transactions")?, get_u64(row, "failed")?);
        if failed > transactions {
            return Err(malformed("more failed transactions than transactions"));
        }
        let instructions = get_u64(one_row(&query(shared, &sql::instructions(from, to)).await?)?, "n")?;
        let token_transfers = get_u64(one_row(&query(shared, &sql::token_transfers(from, to)).await?)?, "n")?;
        let programs = top_rows(&query(shared, &sql::busiest_programs(from, to)).await?, instructions)?;
        let mints = top_rows(&query(shared, &sql::busiest_mints(from, to)).await?, token_transfers)?;
        let data = WindowData {
            overview: Overview { transactions, failed_transactions: failed, instructions, token_transfers },
            programs,
            mints,
        };

        let mut metrics: Vec<String> = coverage.detectors.iter().map(|d| d.id.clone()).collect();
        metrics.sort();
        metrics.dedup();
        let (first, last) = (window.first_bucket, window.last_bucket);
        let (rows, truncated) = store_call(shared, move |s| {
            let mut rows = Vec::new();
            let mut truncated = false;
            for metric in metrics {
                let got = s.list_anomalies(&AnomalyFilter {
                    metric: Some(metric),
                    subject: None,
                    since_bucket: Some(first),
                    until_bucket: Some(last),
                    limit: MAX_LIST_LIMIT,
                })?;
                truncated |= got.len() >= MAX_LIST_LIMIT;
                rows.extend(got);
            }
            Ok((rows, truncated))
        })
        .await?;
        Ok::<_, DigestError>((data, rows, truncated))
    };
    let (data, rows, truncated) = tokio::time::timeout(Duration::from_secs(BUILD_DEADLINE_SECS), read)
        .await
        .map_err(|_| DigestError::Timeout(format!("the window's reads took over {BUILD_DEADLINE_SECS} s")))??;

    let report = tokio::time::timeout(
        Duration::from_secs(QUALITY_WAIT_SECS),
        shared.auditor.report(Duration::from_secs(QUALITY_MAX_AGE_SECS)),
    )
    .await;
    let quality = match &report {
        Ok(Ok(r)) => Ok(r.as_ref()),
        Ok(Err(e)) => Err(e.kind().to_owned()),
        Err(_) => Err("timeout".to_owned()),
    };

    Ok(assemble(AssembleInput {
        window,
        provisional,
        coverage,
        data: &data,
        anomalies: &rows,
        anomalies_truncated: truncated,
        quality,
    }))
}

impl DigestHandle {
    pub fn status(&self) -> DigestStatus {
        lock(&self.shared.inner).status.clone()
    }

    /// The most recently stored digest.
    pub fn latest(&self) -> Result<Option<StoredDigest>, DigestError> {
        self.shared.store.latest_digest().map_err(|e| DigestError::Store(e.to_string()))
    }

    /// Up to `limit` stored digests, newest first.
    pub fn list(&self, limit: usize) -> Result<Vec<StoredDigest>, DigestError> {
        self.shared.store.list_digests(limit).map_err(|e| DigestError::Store(e.to_string()))
    }

    /// Build a window now, without storing it. `None` is the window holding
    /// LENS's newest covered slot. `Some(start)` must be a window's first
    /// slot. A window that is not complete is built from the data so far
    /// (`provisional: true`, `coverage.data_through_slot` set).
    pub async fn preview(&self, window_start: Option<u64>) -> Result<Digest, DigestError> {
        let shared = &self.shared;
        if shared.cancel.is_cancelled() {
            return Err(DigestError::Cancelled);
        }
        let _one = shared.preview.try_lock().map_err(|_| DigestError::Busy)?;
        let ws = window_slots(shared.bucket_slots).expect("checked in new");
        if let Some(start) = window_start
            && start % ws != 0
        {
            return Err(DigestError::invalid("window_start", format!("must be a multiple of {ws}")));
        }
        let work = async {
            let facts = facts(shared).await?;
            let window = match window_start {
                Some(start) => DigestWindow::at(start / ws, shared.bucket_slots),
                None => DigestWindow::containing(facts.w, shared.bucket_slots),
            }
            .ok_or_else(|| DigestError::invalid("window_start", "out of range"))?;
            if window.start_slot > facts.w {
                return Err(DigestError::NotReady(format!(
                    "the window starts at slot {}, after LENS's newest covered slot {}",
                    window.start_slot, facts.w
                )));
            }
            let detectors = shared.coverage.detectors();
            let through = window.end_slot.min(facts.w);
            let complete = window.end_slot <= facts.w && blocking(&detectors, window.last_bucket).is_empty();
            let coverage = CoverageSection { complete, detectors, data_through_slot: (!complete).then_some(through) };
            build(shared, window, through, !complete, coverage).await
        };
        tokio::select! {
            () = shared.cancel.cancelled() => Err(DigestError::Cancelled),
            r = work => r,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_are_aligned() {
        let w = DigestWindow::at(3, 200).unwrap();
        assert_eq!((w.start_slot, w.end_slot, w.slots), (30_000, 39_999, 10_000));
        assert_eq!((w.first_bucket, w.last_bucket), (150, 199));
        assert_eq!(DigestWindow::containing(39_999, 200).unwrap(), w);
        assert_eq!(DigestWindow::first_from(30_000, 200).unwrap(), w);
        assert_eq!(DigestWindow::first_from(30_001, 200).unwrap().index(), 4);
        assert_eq!(w.next().unwrap().start_slot, 40_000);
        assert!(DigestWindow::at(u64::MAX / 2, 200).is_none());
    }

    #[test]
    fn first_window_is_after_complete_max() {
        assert_eq!(first_window(0, 200).unwrap().first_bucket, 50);
        assert_eq!(first_window(49, 200).unwrap().first_bucket, 50);
        assert_eq!(first_window(50, 200).unwrap().first_bucket, 100);
    }

    #[test]
    fn facts_and_lag() {
        let f = Facts::new(10_500, 9_999, 200);
        assert_eq!((f.w, f.complete_max), (9_999, Some(49)));
        assert_eq!(Facts::new(198, 300, 200).complete_max, None);
        let w1 = DigestWindow::at(1, 200).unwrap();
        assert_eq!(lag_windows(&w1, 19_998), 0);
        assert_eq!(lag_windows(&w1, 19_999), 1);
        assert_eq!(lag_windows(&w1, 39_999), 3);
    }

    #[test]
    fn ids_and_known_program_names_are_valid() {
        for (id, _) in PROGRAM_NAMES {
            assert!(is_valid_id(id), "{id}");
        }
        assert!(!is_valid_id(""));
        assert!(!is_valid_id("0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl"));
        assert!(!is_valid_id(&bs58::encode([1u8; 31]).into_string()));
        assert!(!is_valid_id(&bs58::encode([1u8; 33]).into_string()));
    }

    #[test]
    fn number_formats() {
        assert_eq!(fmt_num(910.0).as_deref(), Some("910"));
        assert_eq!(fmt_num(280.5).as_deref(), Some("280.5"));
        assert_eq!(fmt_num(f64::NAN), None);
        assert_eq!(fmt_rate(0.125).as_deref(), Some("12.5%"));
        assert_eq!(fmt_factor(3.244).as_deref(), Some("3.2×"));
        assert_eq!(fmt_factor(0.256).as_deref(), Some("0.26×"));
        assert_eq!(fmt_factor(f64::INFINITY), None);
        assert_eq!(fmt_share(1, 3).as_deref(), Some("33.3"));
        assert_eq!(fmt_share(1, 0), None);
    }

    #[test]
    fn metric_labels() {
        assert_eq!(metric_label("tx_count"), "Transactions per bucket");
        assert_eq!(metric_label("new_metric_2"), "new_metric_2");
        assert_eq!(metric_label("Bad-Metric"), UNKNOWN_METRIC);
        assert_eq!(metric_label(&"a".repeat(65)), UNKNOWN_METRIC);
    }

    #[test]
    fn sql_is_bounded_and_literal_free() {
        for s in sql::all(100, 199) {
            assert!(s.contains("slot >= 100 AND slot <= 199"), "{s}");
            let quoted: Vec<_> = s.split('\'').skip(1).step_by(2).collect();
            assert!(quoted.iter().all(|q| *q == "failed"), "{s}");
        }
    }
}
