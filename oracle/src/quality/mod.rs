//! Pipeline health checks: does what PRISM wrote, as LENS serves it, look
//! like a chain?
//!
//! An [`Auditor`] holds no database state and schedules nothing. It depends
//! on [`LensApi`] and a [`Clock`], and nothing else.
//!
//! # The flow of an audit
//!
//! 1. **Validate** the [`AuditRequest`]. Nothing reaches LENS before that.
//! 2. **Snapshot**, once for the whole audit: [`MAX_SLOTS_SQL`] then
//!    [`MAX_TX_SLOT_SQL`]. The snapshot slot `W` is the smaller of the two,
//!    or `as_of_slot` if given (rejected if it is above that). The window is
//!    the `window_slots` slots ending at `W`, both ends inclusive. Only the
//!    snapshot can fail the audit, with a [`QualityError`].
//! 3. **Checks**, in the fixed order of [`CHECKS`], one after another and
//!    never concurrently. Each one is independent: a LENS failure inside a
//!    check becomes that check's own status (`error` or `not_ready`), and
//!    the other checks still run. A partial report is a meaningful report.
//! 4. **Assemble**: the summary counts statuses; `overall` is the worst
//!    status, see [`Overall`].
//!
//! # Query rules
//!
//! Every check obeys these; the unit tests enforce the mechanical ones.
//!
//! * Every table read is bounded by `slot >= from AND slot <= to`. The one
//!   exemption is [`data_freshness`](CHECKS), which reads the newest data.
//! * Every check dedupes on the table's natural key before counting:
//!   `slot` for slots, `(slot, tx_signature)` for transactions, and
//!   `(slot, tx_signature, instruction_index, inner_instruction_index)` for
//!   instructions and token transfers. PRISM re-writes already flushed slots
//!   after a crash, so duplicate rows are expected; only the `duplicate_*`
//!   checks count them, every other check is blind to them.
//! * `inner_instruction_index` is `NULL` for a top-level instruction, and
//!   `NULL = NULL` is not true, so a join on it silently drops every
//!   top-level row. Joins use `COALESCE(CAST(.. AS BIGINT), -1)` on both
//!   sides instead. `GROUP BY` and `DISTINCT` treat `NULL`s as equal and
//!   need no help.
//! * No arithmetic on `slot` in SQL. Neighbours are found through
//!   `parent_slot`, never `slot - 1`: skipped slots are normal on Solana.
//! * No multi-column `COUNT(DISTINCT a, b)`; a `SELECT DISTINCT` subquery
//!   and `COUNT(*)` instead.
//! * Every literal is an integer computed here or a constant written here.
//! * A count first; a sample query only when the count found violations, so
//!   a healthy window costs one query per check.
//! * A `truncated` result is an error, never a number.
//!
//! # Statuses
//!
//! A check that finds violations reports its [`Severity`]: `fail`, `warn`
//! or `info`. An `info` check always reports `info`: it measures a share
//! that is expected to be non-zero, and is never a fault.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use forge::lens_client::{LensApi, LensCallError, LensResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio_util::sync::CancellationToken;

pub use crate::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL};
pub use crate::profile::Window;

/// `window_slots` when the request has none: about 2.2 hours.
pub const DEFAULT_WINDOW_SLOTS: u64 = 20_000;
pub const MIN_WINDOW_SLOTS: u64 = 1;
/// An audit scans every row in its window; about 11 hours.
pub const MAX_WINDOW_SLOTS: u64 = 100_000;
/// Most samples one check reports.
pub const SAMPLE_LIMIT: u64 = 5;
/// Deadline of the checks of one audit, the snapshot included. Waiting for
/// another audit to finish does not count.
pub const AUDIT_DEADLINE_SECS: u64 = 300;
/// `data_freshness` warns when the newest `block_time` is older than this.
/// PRISM flushes in batches and LENS refreshes its catalog every 30 s, so a
/// healthy pipeline is normally a minute or so behind.
pub const FRESHNESS_WARN_SECS: u64 = 300;
/// Queries every audit runs to fix its snapshot.
pub const SNAPSHOT_QUERIES: u32 = 2;

/// SPL Token.
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
/// SPL Token-2022.
pub const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
/// The `instruction_type`s PRISM's SPL decoder writes a token transfer row
/// for.
pub const TRANSFER_TYPES: [&str; 3] = ["Transfer", "TransferChecked", "TransferCheckedWithFee"];
/// The subset that names its mint and decimals inline.
pub const CHECKED_TRANSFER_TYPES: [&str; 2] = ["TransferChecked", "TransferCheckedWithFee"];

/// Message of every check that could not run because of a LENS failure;
/// `error_kind` says which.
pub const MSG_ERROR: &str = "the check could not run; error_kind says why, and the other checks are unaffected";
/// Message of every check whose table or column is not in LENS yet.
pub const MSG_NOT_READY: &str = "a table or column this check reads is not in LENS yet";

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// Where an [`Auditor`] reads the time: for `generated_at`, the age of the
/// cached report, and `data_freshness`.
pub trait Clock: Send + Sync {
    /// Seconds since the Unix epoch.
    fn now_unix_secs(&self) -> u64;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> u64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
    }
}

// ---------------------------------------------------------------------------
// Request and errors
// ---------------------------------------------------------------------------

/// One audit request. The default is the one [`Auditor::report`] runs.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRequest {
    /// Slots in the window, [`DEFAULT_WINDOW_SLOTS`] if `None`.
    #[serde(default)]
    pub window_slots: Option<u64>,
    /// Pin the snapshot to this slot for a reproducible report.
    #[serde(default)]
    pub as_of_slot: Option<u64>,
}

/// Why no report was produced. Only the request and the snapshot can cause
/// one; a failure inside a check is that check's status.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum QualityError {
    /// The request is wrong. Nothing was sent to LENS, except for an
    /// `as_of_slot` above the snapshot, which needs the snapshot to judge.
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },
    /// LENS has no data, or not the table, yet.
    #[error("LENS is not ready: {0}")]
    NotReady(String),
    /// A transient LENS failure; the same call may succeed later.
    #[error("LENS is unavailable: {0}")]
    Unavailable(String),
    /// LENS, or the audit deadline, ran out of time.
    #[error("audit timed out: {0}")]
    Timeout(String),
    /// [`Auditor::shutdown`] was called.
    #[error("the auditor is shut down")]
    Cancelled,
    /// The snapshot query is wrong, or LENS answered it in a shape we do not
    /// expect: a bug. Not in the original design: without it, a bug would
    /// have to pose as one of the retryable variants.
    #[error("{0}")]
    Permanent(String),
}

impl QualityError {
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid { field: field.into(), reason: reason.into() }
    }

    /// Stable `snake_case` name of the variant.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "invalid",
            Self::NotReady(_) => "not_ready",
            Self::Unavailable(_) => "unavailable",
            Self::Timeout(_) => "timeout",
            Self::Cancelled => "cancelled",
            Self::Permanent(_) => "permanent",
        }
    }

    /// The HTTP status the API answers with.
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::NotReady(_) | Self::Unavailable(_) | Self::Cancelled => 503,
            Self::Timeout(_) => 504,
            Self::Permanent(_) => 500,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::NotReady(_) | Self::Unavailable(_) | Self::Timeout(_))
    }
}

/// A snapshot query's failure, as the profile module maps it.
fn snapshot_error(err: LensCallError) -> QualityError {
    match &err {
        LensCallError::BadQuery { message } if message.contains("not found") => QualityError::NotReady(err.to_string()),
        LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } => QualityError::Permanent(err.to_string()),
        LensCallError::Timeout { .. } => QualityError::Timeout(err.to_string()),
        LensCallError::Unavailable { .. } => QualityError::Unavailable(err.to_string()),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Valid {
    window_slots: u64,
    as_of_slot: Option<u64>,
}

fn validate(req: &AuditRequest) -> Result<Valid, QualityError> {
    let window_slots = req.window_slots.unwrap_or(DEFAULT_WINDOW_SLOTS);
    if !(MIN_WINDOW_SLOTS..=MAX_WINDOW_SLOTS).contains(&window_slots) {
        return Err(QualityError::invalid(
            "window_slots",
            format!("{window_slots} is outside {MIN_WINDOW_SLOTS}..={MAX_WINDOW_SLOTS}"),
        ));
    }
    if let Some(slot) = req.as_of_slot
        && i64::try_from(slot).is_err()
    {
        return Err(QualityError::invalid("as_of_slot", format!("{slot} does not fit in i64")));
    }
    Ok(Valid { window_slots, as_of_slot: req.as_of_slot })
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// What a check is about. Reports list checks by group, in this order, then
/// by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Group {
    /// The rows form a chain: no duplicates, no gaps, nothing orphaned.
    Integrity,
    /// The SPL decoder did its job.
    Decoding,
    /// The pipeline is keeping up.
    Freshness,
    /// Shares the other modules' numbers depend on. Never a fault.
    Coverage,
}

impl Group {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Integrity => "integrity",
            Self::Decoding => "decoding",
            Self::Freshness => "freshness",
            Self::Coverage => "coverage",
        }
    }
}

/// What a check reports when it finds violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Fail,
    Warn,
    /// Always reported, whatever the count; never affects `overall`.
    Info,
}

/// One check's outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Pass,
    Warn,
    Fail,
    Info,
    /// A LENS failure stopped the check; `error_kind` says which.
    Error,
    /// A table or column the check reads does not exist yet, or (for
    /// `data_freshness`) no slot has a `block_time`.
    NotReady,
}

impl Status {
    pub const ALL: [Status; 6] =
        [Status::Pass, Status::Warn, Status::Fail, Status::Info, Status::Error, Status::NotReady];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Info => "info",
            Self::Error => "error",
            Self::NotReady => "not_ready",
        }
    }
}

/// The audit's verdict: the worst check status, ranked `fail`, then
/// `incomplete` (an `error` or `not_ready`: an unknown can hide a failure,
/// so it outranks a warning), then `warn`, then `pass`.
///
/// `info` checks never affect it, not even when they cannot run: an info
/// check never fails, so not knowing its result cannot hide a failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Overall {
    Pass,
    Warn,
    Incomplete,
    Fail,
}

impl Overall {
    /// The worst over `checks`; see the type docs.
    pub fn of(checks: &[CheckResult]) -> Self {
        checks
            .iter()
            .filter(|c| def(c.id).is_none_or(|d| d.severity != Severity::Info))
            .map(|c| match c.status {
                Status::Pass | Status::Info => Self::Pass,
                Status::Warn => Self::Warn,
                Status::Error | Status::NotReady => Self::Incomplete,
                Status::Fail => Self::Fail,
            })
            .max()
            .unwrap_or(Self::Pass)
    }
}

/// How a check's SQL is built from the window.
#[derive(Debug, Clone, Copy)]
enum Probe {
    /// A count query returning `examined` and `violations`, and a sample
    /// query, run only when there are violations, whose columns are joined
    /// with `:` into one sample each.
    Count { count: fn(&Window) -> String, sample: Option<fn(&Window) -> String> },
    /// `data_freshness`: one unbounded query and the clock.
    Freshness,
}

/// A check: its identity and SQL.
#[derive(Debug, Clone, Copy)]
pub struct CheckDef {
    pub id: &'static str,
    pub group: Group,
    pub severity: Severity,
    /// The message when nothing is found. For `info` checks, the only one.
    pub pass_message: &'static str,
    /// The message when violations are found.
    pub found_message: &'static str,
    probe: Probe,
}

impl CheckDef {
    /// The fixed message for `status`. Never contains data read from LENS.
    pub fn message(&self, status: Status) -> &'static str {
        match status {
            Status::Pass => self.pass_message,
            Status::Warn | Status::Fail => self.found_message,
            Status::Info => self.pass_message,
            Status::Error => MSG_ERROR,
            Status::NotReady if self.id == "data_freshness" => "no slot has a block_time yet",
            Status::NotReady => MSG_NOT_READY,
        }
    }
}

const fn count(count: fn(&Window) -> String, sample: fn(&Window) -> String) -> Probe {
    Probe::Count { count, sample: Some(sample) }
}

const fn count_only(count: fn(&Window) -> String) -> Probe {
    Probe::Count { count, sample: None }
}

/// Every check, in report order (group, then id).
pub const CHECKS: &[CheckDef] = &[
    // --- integrity ---
    CheckDef {
        id: "blockhash_mismatch",
        group: Group::Integrity,
        severity: Severity::Warn,
        pass_message: "every slot's rows and its transactions' rows agree on one blockhash",
        found_message: "a slot has rows with more than one blockhash; PRISM stored a block from a fork that was later abandoned, \
             possible at confirmed commitment but rare",
        probe: count(sql::blockhash_mismatch, sql::blockhash_mismatch_sample),
    },
    CheckDef {
        id: "duplicate_instructions",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "no instruction row is duplicated",
        found_message: "instruction rows duplicated; a restart after a crash re-writes already flushed slots",
        probe: count(sql::duplicate_instructions, sql::duplicate_instructions_sample),
    },
    CheckDef {
        id: "duplicate_transactions",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "no transaction row is duplicated",
        found_message: "transaction rows duplicated; a restart after a crash re-writes already flushed slots",
        probe: count(sql::duplicate_transactions, sql::duplicate_transactions_sample),
    },
    CheckDef {
        id: "duplicate_transfers",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "no token transfer row is duplicated",
        found_message: "token transfer rows duplicated; a restart after a crash re-writes already flushed slots",
        probe: count(sql::duplicate_transfers, sql::duplicate_transfers_sample),
    },
    CheckDef {
        id: "orphan_instructions",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "every instruction has its transaction",
        found_message: "instructions without their transaction row",
        probe: count(sql::orphan_instructions, sql::orphan_instructions_sample),
    },
    CheckDef {
        id: "orphan_transactions",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "every transaction has its slot",
        found_message: "transactions without their slot row",
        probe: count(sql::orphan_transactions, sql::orphan_transactions_sample),
    },
    CheckDef {
        id: "orphan_transfers",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "every token transfer has its transaction",
        found_message: "token transfers without their transaction row",
        probe: count(sql::orphan_transfers, sql::orphan_transfers_sample),
    },
    CheckDef {
        id: "parent_link_missing",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "every slot's parent is stored",
        found_message: "slots whose parent slot is not stored; a block was dropped between them",
        probe: count(sql::parent_link_missing, sql::parent_link_missing_sample),
    },
    CheckDef {
        id: "slot_duplicates",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "no slot row is duplicated",
        found_message: "slot rows duplicated; a restart after a crash re-writes already flushed slots",
        probe: count(sql::slot_duplicates, sql::slot_duplicates_sample),
    },
    CheckDef {
        id: "slot_tx_count_mismatch",
        group: Group::Integrity,
        severity: Severity::Fail,
        pass_message: "every slot holds as many distinct transactions as its transaction_count",
        found_message: "slots whose distinct transaction rows disagree with their transaction_count",
        probe: count(sql::slot_tx_count_mismatch, sql::slot_tx_count_mismatch_sample),
    },
    CheckDef {
        id: "tx_without_instructions",
        group: Group::Integrity,
        severity: Severity::Warn,
        pass_message: "every transaction has at least one instruction",
        found_message: "transactions without any instruction row; valid on chain but rare, so likely dropped rows",
        probe: count(sql::tx_without_instructions, sql::tx_without_instructions_sample),
    },
    // --- decoding ---
    CheckDef {
        id: "mint_decimals_conflict",
        group: Group::Decoding,
        severity: Severity::Fail,
        pass_message: "every mint has one decimals value",
        found_message: "mints seen with more than one decimals value; a mint's decimals never change, so a decoder is wrong",
        probe: count(sql::mint_decimals_conflict, sql::mint_decimals_conflict_sample),
    },
    CheckDef {
        id: "spl_decode_rate",
        group: Group::Decoding,
        severity: Severity::Warn,
        pass_message: "every SPL Token and Token-2022 instruction was decoded",
        found_message: "SPL Token or Token-2022 instructions left undecoded",
        probe: count(sql::spl_decode_rate, sql::spl_decode_rate_sample),
    },
    CheckDef {
        id: "transfer_checked_completeness",
        group: Group::Decoding,
        severity: Severity::Fail,
        pass_message: "every checked transfer has its mint and decimals",
        found_message: "checked transfers without a mint or decimals; both are inline in the instruction, so no lookup can explain it",
        probe: count(sql::transfer_checked_completeness, sql::transfer_checked_completeness_sample),
    },
    CheckDef {
        id: "transfer_row_coverage",
        group: Group::Decoding,
        severity: Severity::Fail,
        pass_message: "every decoded transfer instruction has its token transfer row",
        found_message: "decoded transfer instructions without a token transfer row",
        probe: count(sql::transfer_row_coverage, sql::transfer_row_coverage_sample),
    },
    // --- freshness ---
    CheckDef {
        id: "data_freshness",
        group: Group::Freshness,
        severity: Severity::Warn,
        pass_message: "the newest block_time is recent",
        found_message: "the newest block_time is older than the freshness threshold; the pipeline is behind or stopped",
        probe: Probe::Freshness,
    },
    // --- coverage ---
    CheckDef {
        id: "finalized_flag",
        group: Group::Coverage,
        severity: Severity::Info,
        pass_message: "slots with an is_finalized = false row. PRISM stamps is_finalized at write time and never rewrites it, \
             so false does not mean unfinalized: the share depends on the flush size, and rows written in the last ~32 slots \
             of every flush batch are false forever. Do not build anything on this column",
        found_message: "",
        probe: count_only(sql::finalized_flag),
    },
    CheckDef {
        id: "owner_resolution",
        group: Group::Coverage,
        severity: Severity::Info,
        pass_message: "token transfers with a resolved source or destination owner; 0 until PRISM resolves owners",
        found_message: "",
        probe: count_only(sql::owner_resolution),
    },
    CheckDef {
        id: "transfers_in_failed_tx",
        group: Group::Coverage,
        severity: Severity::Info,
        pass_message: "token transfers in failed transactions; PRISM decodes without looking at the outcome, \
             so non-zero means the failed-transaction exclusion profiles apply matters",
        found_message: "",
        probe: count_only(sql::transfers_in_failed_tx),
    },
    CheckDef {
        id: "unattributed_transfers",
        group: Group::Coverage,
        severity: Severity::Info,
        pass_message: "token transfers without a mint; expected for plain Transfer instructions, \
             and the share by which every mint-level number is a lower bound",
        found_message: "",
        probe: count_only(sql::unattributed_transfers),
    },
];

/// The definition of check `id`.
pub fn def(id: &str) -> Option<&'static CheckDef> {
    CHECKS.iter().find(|c| c.id == id)
}

/// The count query check `id` runs over `w`, for tests and debugging.
pub fn count_sql(id: &str, w: &Window) -> Option<String> {
    match def(id)?.probe {
        Probe::Count { count, .. } => Some(count(w)),
        Probe::Freshness => Some(sql::DATA_FRESHNESS.to_owned()),
    }
}

/// The sample query check `id` runs over `w`, if it has one.
pub fn sample_sql(id: &str, w: &Window) -> Option<String> {
    match def(id)?.probe {
        Probe::Count { sample, .. } => sample.map(|s| s(w)),
        Probe::Freshness => None,
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// One check's result.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CheckResult {
    pub id: &'static str,
    pub group: Group,
    pub status: Status,
    /// Rows or keys the check looked at, so a rate is meaningful and an
    /// empty window shows as `0 of 0`.
    pub examined: u64,
    /// For `fail` and `warn` checks, the faults found. For `info` checks,
    /// the rows with the measured property (see the message).
    pub violations: u64,
    /// Up to [`SAMPLE_LIMIT`] keys of violating rows, smallest first, the
    /// key's columns joined with `:` (`-` for a `NULL`).
    pub samples: Vec<String>,
    /// Fixed per check and status; never contains data read from LENS.
    pub message: &'static str,
    /// Why the check, or its sample query, failed: a [`LensCallError`] kind,
    /// `truncated`, or `malformed` for a result of an unexpected shape.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<&'static str>,
    /// `data_freshness` only: seconds between the clock and the newest
    /// `block_time`, 0 if `block_time` is ahead of the clock.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lag_secs: Option<u64>,
    pub query_count: u32,
    pub duration_ms: u64,
}

/// Checks per status.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct StatusSummary {
    pub pass: u32,
    pub warn: u32,
    pub fail: u32,
    pub info: u32,
    pub error: u32,
    pub not_ready: u32,
}

impl StatusSummary {
    pub fn of(checks: &[CheckResult]) -> Self {
        let mut s = Self::default();
        for c in checks {
            *match c.status {
                Status::Pass => &mut s.pass,
                Status::Warn => &mut s.warn,
                Status::Fail => &mut s.fail,
                Status::Info => &mut s.info,
                Status::Error => &mut s.error,
                Status::NotReady => &mut s.not_ready,
            } += 1;
        }
        s
    }

    pub const fn total(&self) -> u32 {
        self.pass + self.warn + self.fail + self.info + self.error + self.not_ready
    }
}

/// The report. Every field is a function of the request, the data at or
/// below `as_of_slot` and the clock, except the `duration_ms` fields and
/// `data_freshness`, which reads the newest data.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QualityReport {
    /// The snapshot slot; pass it back as `as_of_slot` to reproduce.
    pub as_of_slot: u64,
    pub window: Window,
    /// Unix seconds when the audit started.
    pub generated_at: u64,
    pub overall: Overall,
    pub summary: StatusSummary,
    /// In [`CHECKS`] order.
    pub checks: Vec<CheckResult>,
    pub queries_run: u32,
    pub duration_ms: u64,
}

impl QualityReport {
    pub fn check(&self, id: &str) -> Option<&CheckResult> {
        self.checks.iter().find(|c| c.id == id)
    }
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// Statement builders. Each takes the window only, and splices in nothing
/// but its two integers and the constants above.
pub mod sql {
    use super::{CHECKED_TRANSFER_TYPES, SAMPLE_LIMIT, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID, TRANSFER_TYPES, Window};

    /// Natural key of an instruction or token transfer row.
    const IX_KEY: &str = "slot, tx_signature, instruction_index, inner_instruction_index";

    /// `data_freshness`: unbounded on purpose. `block_time` is a
    /// `Timestamp(Second)`, so the cast gives Unix seconds.
    pub const DATA_FRESHNESS: &str = "SELECT CAST(MAX(block_time) AS BIGINT) AS newest FROM solana.slots";

    fn bound(w: &Window) -> String {
        format!("slot >= {} AND slot <= {}", w.from_slot, w.to_slot)
    }

    fn list(items: &[&str]) -> String {
        items.iter().map(|s| format!("'{s}'")).collect::<Vec<_>>().join(", ")
    }

    fn token_programs() -> String {
        list(&[TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID])
    }

    /// `inner_instruction_index` made joinable: `NULL` becomes -1.
    fn inner(column: &str) -> String {
        format!("COALESCE(CAST({column} AS BIGINT), -1)")
    }

    fn slot_keys(w: &Window) -> String {
        format!("SELECT DISTINCT slot FROM solana.slots WHERE {}", bound(w))
    }

    fn tx_keys(w: &Window) -> String {
        format!("SELECT DISTINCT slot, tx_signature FROM solana.transactions WHERE {}", bound(w))
    }

    fn row_keys(table: &str, w: &Window) -> String {
        format!("SELECT DISTINCT {IX_KEY} FROM solana.{table} WHERE {}", bound(w))
    }

    /// `(slot, tx_signature, instruction_index, inner)` of decoded token
    /// program instructions of `types`, one row per key.
    fn typed_ix_keys(w: &Window, types: &[&str]) -> String {
        format!(
            "SELECT DISTINCT slot, tx_signature, instruction_index, inner_instruction_index, {} AS inner_key \
             FROM solana.instructions \
             WHERE {} AND program_id IN ({}) AND instruction_type IN ({})",
            inner("inner_instruction_index"),
            bound(w),
            token_programs(),
            list(types)
        )
    }

    /// Up to [`SAMPLE_LIMIT`] violating keys, smallest first. The columns
    /// are aliased `c0`, `c1`, .. so their order survives a JSON object.
    fn sample(columns: &str, from: &str, filter: &str) -> String {
        let aliased: Vec<String> =
            columns.split(", ").enumerate().map(|(i, c)| format!("{c} AS c{i}")).collect();
        let order: Vec<String> = (0..aliased.len()).map(|i| format!("c{i}")).collect();
        format!(
            "SELECT {} FROM {from} WHERE {filter} ORDER BY {} LIMIT {SAMPLE_LIMIT}",
            aliased.join(", "),
            order.join(", ")
        )
    }

    // --- duplicates ---

    fn dup_groups(table: &str, keys: &str, w: &Window) -> String {
        format!("(SELECT {keys}, COUNT(*) AS n FROM solana.{table} WHERE {} GROUP BY {keys}) d", bound(w))
    }

    fn dup_count(table: &str, keys: &str, w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN n > 1 THEN 1 END) AS violations FROM {}",
            dup_groups(table, keys, w)
        )
    }

    fn dup_sample(table: &str, keys: &str, w: &Window) -> String {
        sample(keys, &dup_groups(table, keys, w), "n > 1")
    }

    pub fn slot_duplicates(w: &Window) -> String {
        dup_count("slots", "slot", w)
    }
    pub fn slot_duplicates_sample(w: &Window) -> String {
        dup_sample("slots", "slot", w)
    }
    pub fn duplicate_transactions(w: &Window) -> String {
        dup_count("transactions", "slot, tx_signature", w)
    }
    pub fn duplicate_transactions_sample(w: &Window) -> String {
        dup_sample("transactions", "slot, tx_signature", w)
    }
    pub fn duplicate_instructions(w: &Window) -> String {
        dup_count("instructions", IX_KEY, w)
    }
    pub fn duplicate_instructions_sample(w: &Window) -> String {
        dup_sample("instructions", IX_KEY, w)
    }
    pub fn duplicate_transfers(w: &Window) -> String {
        dup_count("token_transfers", IX_KEY, w)
    }
    pub fn duplicate_transfers_sample(w: &Window) -> String {
        dup_sample("token_transfers", IX_KEY, w)
    }

    // --- chain ---

    /// One row per child slot whose parent should be stored: the parent is
    /// at or above the oldest slot stored in the window. Below it, the
    /// parent is outside what this window can judge (or before PRISM
    /// started), so it is not examined.
    fn parent_links(w: &Window) -> String {
        let b = bound(w);
        format!(
            "(SELECT c.slot AS slot, MAX(CASE WHEN p.slot IS NULL THEN 1 ELSE 0 END) AS missing \
             FROM (SELECT DISTINCT slot, parent_slot FROM solana.slots WHERE {b} \
                   AND parent_slot >= (SELECT MIN(slot) FROM solana.slots WHERE {b})) c \
             LEFT JOIN ({}) p ON p.slot = c.parent_slot \
             GROUP BY c.slot) l",
            slot_keys(w)
        )
    }

    pub fn parent_link_missing(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN missing = 1 THEN 1 END) AS violations FROM {}",
            parent_links(w)
        )
    }
    pub fn parent_link_missing_sample(w: &Window) -> String {
        sample("slot", &parent_links(w), "missing = 1")
    }

    fn hashes(w: &Window) -> String {
        let b = bound(w);
        format!(
            "(SELECT slot, COUNT(DISTINCT blockhash) AS h FROM \
               (SELECT slot, blockhash FROM solana.slots WHERE {b} \
                UNION ALL SELECT slot, blockhash FROM solana.transactions WHERE {b}) u \
             GROUP BY slot) g"
        )
    }

    pub fn blockhash_mismatch(w: &Window) -> String {
        format!("SELECT COUNT(*) AS examined, COUNT(CASE WHEN h > 1 THEN 1 END) AS violations FROM {}", hashes(w))
    }
    pub fn blockhash_mismatch_sample(w: &Window) -> String {
        sample("slot", &hashes(w), "h > 1")
    }

    /// Per slot: whether its rows agree on `transaction_count` and that
    /// matches its distinct transaction rows.
    fn tx_counts(w: &Window) -> String {
        format!(
            "(SELECT s.slot AS slot, \
               CASE WHEN s.lo <> s.hi OR CAST(s.hi AS BIGINT) <> COALESCE(t.n, 0) THEN 1 ELSE 0 END AS bad \
             FROM (SELECT slot, MIN(transaction_count) AS lo, MAX(transaction_count) AS hi \
                   FROM solana.slots WHERE {} GROUP BY slot) s \
             LEFT JOIN (SELECT slot, COUNT(*) AS n FROM ({}) k GROUP BY slot) t ON t.slot = s.slot) c",
            bound(w),
            tx_keys(w)
        )
    }

    pub fn slot_tx_count_mismatch(w: &Window) -> String {
        format!("SELECT COUNT(*) AS examined, COUNT(CASE WHEN bad = 1 THEN 1 END) AS violations FROM {}", tx_counts(w))
    }
    pub fn slot_tx_count_mismatch_sample(w: &Window) -> String {
        sample("slot", &tx_counts(w), "bad = 1")
    }

    // --- orphans ---

    fn orphan_txs(w: &Window) -> String {
        format!("({}) k LEFT JOIN ({}) s ON s.slot = k.slot", tx_keys(w), slot_keys(w))
    }

    pub fn orphan_transactions(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN s.slot IS NULL THEN 1 END) AS violations FROM {}",
            orphan_txs(w)
        )
    }
    pub fn orphan_transactions_sample(w: &Window) -> String {
        sample("k.slot, k.tx_signature", &orphan_txs(w), "s.slot IS NULL")
    }

    fn orphan_rows(table: &str, w: &Window) -> String {
        format!(
            "({}) k LEFT JOIN ({}) t ON t.slot = k.slot AND t.tx_signature = k.tx_signature",
            row_keys(table, w),
            tx_keys(w)
        )
    }

    fn orphan_count(table: &str, w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN t.slot IS NULL THEN 1 END) AS violations FROM {}",
            orphan_rows(table, w)
        )
    }

    fn orphan_sample(table: &str, w: &Window) -> String {
        sample(
            "k.slot, k.tx_signature, k.instruction_index, k.inner_instruction_index",
            &orphan_rows(table, w),
            "t.slot IS NULL",
        )
    }

    pub fn orphan_instructions(w: &Window) -> String {
        orphan_count("instructions", w)
    }
    pub fn orphan_instructions_sample(w: &Window) -> String {
        orphan_sample("instructions", w)
    }
    pub fn orphan_transfers(w: &Window) -> String {
        orphan_count("token_transfers", w)
    }
    pub fn orphan_transfers_sample(w: &Window) -> String {
        orphan_sample("token_transfers", w)
    }

    fn bare_txs(w: &Window) -> String {
        format!(
            "({}) k LEFT JOIN (SELECT DISTINCT slot, tx_signature FROM solana.instructions WHERE {}) i \
             ON i.slot = k.slot AND i.tx_signature = k.tx_signature",
            tx_keys(w),
            bound(w)
        )
    }

    pub fn tx_without_instructions(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN i.slot IS NULL THEN 1 END) AS violations FROM {}",
            bare_txs(w)
        )
    }
    pub fn tx_without_instructions_sample(w: &Window) -> String {
        sample("k.slot, k.tx_signature", &bare_txs(w), "i.slot IS NULL")
    }

    // --- decoding ---

    fn token_ix(w: &Window) -> String {
        format!(
            "(SELECT {IX_KEY}, MAX(CASE WHEN instruction_type IS NULL THEN 0 ELSE 1 END) AS decoded \
             FROM solana.instructions WHERE {} AND program_id IN ({}) GROUP BY {IX_KEY}) d",
            bound(w),
            token_programs()
        )
    }

    pub fn spl_decode_rate(w: &Window) -> String {
        format!("SELECT COUNT(*) AS examined, COUNT(CASE WHEN decoded = 0 THEN 1 END) AS violations FROM {}", token_ix(w))
    }
    pub fn spl_decode_rate_sample(w: &Window) -> String {
        sample(IX_KEY, &token_ix(w), "decoded = 0")
    }

    fn transfer_rows(w: &Window) -> String {
        format!(
            "({}) i LEFT JOIN (SELECT DISTINCT slot, tx_signature, instruction_index, {} AS inner_key \
                               FROM solana.token_transfers WHERE {}) x \
             ON x.slot = i.slot AND x.tx_signature = i.tx_signature \
                AND x.instruction_index = i.instruction_index AND x.inner_key = i.inner_key",
            typed_ix_keys(w, &TRANSFER_TYPES),
            inner("inner_instruction_index"),
            bound(w)
        )
    }

    pub fn transfer_row_coverage(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN x.slot IS NULL THEN 1 END) AS violations FROM {}",
            transfer_rows(w)
        )
    }
    pub fn transfer_row_coverage_sample(w: &Window) -> String {
        sample("i.slot, i.tx_signature, i.instruction_index, i.inner_instruction_index", &transfer_rows(w), "x.slot IS NULL")
    }

    fn checked_rows(w: &Window) -> String {
        format!(
            "(SELECT slot, tx_signature, instruction_index, inner_instruction_index, {} AS inner_key, \
               MAX(CASE WHEN mint_token_address IS NULL OR decimals IS NULL THEN 1 ELSE 0 END) AS incomplete \
             FROM solana.token_transfers WHERE {} GROUP BY {IX_KEY}) x \
             JOIN ({}) i ON i.slot = x.slot AND i.tx_signature = x.tx_signature \
               AND i.instruction_index = x.instruction_index AND i.inner_key = x.inner_key",
            inner("inner_instruction_index"),
            bound(w),
            typed_ix_keys(w, &CHECKED_TRANSFER_TYPES)
        )
    }

    pub fn transfer_checked_completeness(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN x.incomplete = 1 THEN 1 END) AS violations FROM {}",
            checked_rows(w)
        )
    }
    pub fn transfer_checked_completeness_sample(w: &Window) -> String {
        sample("x.slot, x.tx_signature, x.instruction_index, x.inner_instruction_index", &checked_rows(w), "x.incomplete = 1")
    }

    fn mint_decimals(w: &Window) -> String {
        format!(
            "(SELECT mint_token_address AS mint, COUNT(DISTINCT decimals) AS d FROM solana.token_transfers \
             WHERE {} AND mint_token_address IS NOT NULL AND decimals IS NOT NULL \
             GROUP BY mint_token_address) g",
            bound(w)
        )
    }

    pub fn mint_decimals_conflict(w: &Window) -> String {
        format!("SELECT COUNT(*) AS examined, COUNT(CASE WHEN d > 1 THEN 1 END) AS violations FROM {}", mint_decimals(w))
    }
    pub fn mint_decimals_conflict_sample(w: &Window) -> String {
        sample("mint", &mint_decimals(w), "d > 1")
    }

    // --- coverage ---

    /// Token transfer keys with `flag`, an expression true for a row with
    /// the property; a key has it if any of its rows does.
    fn transfer_share(w: &Window, flag: &str) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN f = 1 THEN 1 END) AS violations FROM \
             (SELECT {IX_KEY}, MAX(CASE WHEN {flag} THEN 1 ELSE 0 END) AS f \
              FROM solana.token_transfers WHERE {} GROUP BY {IX_KEY}) g",
            bound(w)
        )
    }

    pub fn unattributed_transfers(w: &Window) -> String {
        transfer_share(w, "mint_token_address IS NULL")
    }

    pub fn owner_resolution(w: &Window) -> String {
        transfer_share(w, "source_owner IS NOT NULL OR destination_owner IS NOT NULL")
    }

    pub fn transfers_in_failed_tx(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN t.failed = 1 THEN 1 END) AS violations \
             FROM ({}) k JOIN (SELECT slot, tx_signature, MAX(CASE WHEN status <> 'success' THEN 1 ELSE 0 END) AS failed \
                               FROM solana.transactions WHERE {} GROUP BY slot, tx_signature) t \
             ON t.slot = k.slot AND t.tx_signature = k.tx_signature",
            row_keys("token_transfers", w),
            bound(w)
        )
    }

    pub fn finalized_flag(w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS examined, COUNT(CASE WHEN f = 1 THEN 1 END) AS violations FROM \
             (SELECT slot, MAX(CASE WHEN is_finalized THEN 0 ELSE 1 END) AS f \
              FROM solana.slots WHERE {} GROUP BY slot) g",
            bound(w)
        )
    }
}

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

/// A result of an unexpected shape.
#[derive(Debug)]
struct Malformed;

fn single_row(response: &LensResponse) -> Result<&Map<String, Value>, Malformed> {
    match response.rows.as_slice() {
        [row] => Ok(row),
        _ => Err(Malformed),
    }
}

fn opt_u64(row: &Map<String, Value>, column: &str) -> Result<Option<u64>, Malformed> {
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or(Malformed),
        None => Err(Malformed),
    }
}

fn get_u64(row: &Map<String, Value>, column: &str) -> Result<u64, Malformed> {
    opt_u64(row, column)?.ok_or(Malformed)
}

/// A sample row's values `c0`, `c1`, .. joined with `:`.
fn sample_key(row: &Map<String, Value>) -> String {
    (0..row.len())
        .map(|i| match row.get(&format!("c{i}")).unwrap_or(&Value::Null) {
            Value::Null => "-".to_owned(),
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(":")
}

// ---------------------------------------------------------------------------
// Auditor
// ---------------------------------------------------------------------------

/// Runs audits, at most one at a time, and caches the default one.
pub struct Auditor {
    lens: Arc<dyn LensApi>,
    clock: Arc<dyn Clock>,
    deadline: Duration,
    /// Held for the whole of an audit: at most one runs at a time, and a
    /// [`Self::report`] caller that waits on it finds the report the audit
    /// it waited for produced.
    gate: tokio::sync::Mutex<()>,
    /// The newest report of a default request [`Self::report`] produced.
    cache: Mutex<Option<Arc<QualityReport>>>,
    shutdown: CancellationToken,
}

impl fmt::Debug for Auditor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auditor")
            .field("deadline", &self.deadline)
            .field("shut_down", &self.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

/// The queries of one audit, counted.
struct Session<'a> {
    lens: &'a dyn LensApi,
    queries: u32,
}

/// Why a check's query gave no usable result.
enum QueryFailure {
    NotReady,
    Error(&'static str),
}

impl Session<'_> {
    async fn raw(&mut self, sql: &str) -> Result<LensResponse, LensCallError> {
        self.queries += 1;
        self.lens.query(sql).await
    }

    async fn snapshot_max(&mut self, sql: &str, table: &str) -> Result<u64, QualityError> {
        let response = self.raw(sql).await.map_err(snapshot_error)?;
        if response.truncated {
            return Err(QualityError::Permanent("LENS truncated a snapshot query".to_owned()));
        }
        let malformed = |_| QualityError::Permanent("unexpected snapshot result".to_owned());
        opt_u64(single_row(&response).map_err(malformed)?, "max_slot")
            .map_err(malformed)?
            .ok_or_else(|| QualityError::NotReady(format!("{table} has no rows yet")))
    }

    /// One check query. `missing_column` is a column whose absence means
    /// the check is not ready rather than broken.
    async fn check(&mut self, sql: &str, missing_column: Option<&str>) -> Result<LensResponse, QueryFailure> {
        match self.raw(sql).await {
            Ok(r) if r.truncated => Err(QueryFailure::Error("truncated")),
            Ok(r) => Ok(r),
            Err(LensCallError::BadQuery { message })
                if message.contains("not found") || missing_column.is_some_and(|c| message.contains(c)) =>
            {
                Err(QueryFailure::NotReady)
            }
            Err(err) => Err(QueryFailure::Error(err.kind())),
        }
    }
}

impl Auditor {
    pub fn new(lens: Arc<dyn LensApi>, clock: Arc<dyn Clock>) -> Self {
        Self::with_deadline(lens, clock, Duration::from_secs(AUDIT_DEADLINE_SECS))
    }

    /// For tests: a different deadline.
    pub fn with_deadline(lens: Arc<dyn LensApi>, clock: Arc<dyn Clock>, deadline: Duration) -> Self {
        Self {
            lens,
            clock,
            deadline,
            gate: tokio::sync::Mutex::new(()),
            cache: Mutex::new(None),
            shutdown: CancellationToken::new(),
        }
    }

    /// Run an audit for `req`, always: never served from or stored in the
    /// cache. Waits for a running audit to finish first.
    pub async fn run(&self, req: AuditRequest) -> Result<QualityReport, QualityError> {
        let valid = validate(&req)?;
        let _gate = self.enter().await?;
        self.audit(valid).await
    }

    /// The report for the default request: the cached one if it is at most
    /// `max_age` old by the clock, else a new one.
    ///
    /// Single-flight: callers that find the cache stale while an audit runs
    /// wait for it and share its report, instead of starting their own. A
    /// failed audit is not cached; the next caller runs a new one.
    pub async fn report(&self, max_age: Duration) -> Result<Arc<QualityReport>, QualityError> {
        if let Some(fresh) = self.cached(max_age) {
            return Ok(fresh);
        }
        let _gate = self.enter().await?;
        // Whoever held the gate may just have stored a report.
        if let Some(fresh) = self.cached(max_age) {
            return Ok(fresh);
        }
        let report = Arc::new(self.audit(validate(&AuditRequest::default())?).await?);
        *self.cache.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(&report));
        Ok(report)
    }

    /// Stop: running and future audits return [`QualityError::Cancelled`].
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    fn cached(&self, max_age: Duration) -> Option<Arc<QualityReport>> {
        let cache = self.cache.lock().unwrap_or_else(PoisonError::into_inner);
        let report = cache.as_ref()?;
        let age = self.clock.now_unix_secs().saturating_sub(report.generated_at);
        (age <= max_age.as_secs()).then(|| Arc::clone(report))
    }

    async fn enter(&self) -> Result<tokio::sync::MutexGuard<'_, ()>, QualityError> {
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => Err(QualityError::Cancelled),
            guard = self.gate.lock() => Ok(guard),
        }
    }

    /// The audit itself, under the gate, the deadline and the shutdown token.
    async fn audit(&self, valid: Valid) -> Result<QualityReport, QualityError> {
        let deadline = self.deadline;
        tokio::select! {
            biased;
            () = self.shutdown.cancelled() => Err(QualityError::Cancelled),
            result = tokio::time::timeout(deadline, self.audit_inner(valid)) => result.unwrap_or_else(|_| {
                Err(QualityError::Timeout(format!(
                    "the audit did not finish within {} s; narrow window_slots",
                    deadline.as_secs()
                )))
            }),
        }
    }

    async fn audit_inner(&self, valid: Valid) -> Result<QualityReport, QualityError> {
        let started = Instant::now();
        let generated_at = self.clock.now_unix_secs();
        let mut s = Session { lens: self.lens.as_ref(), queries: 0 };

        let slots = s.snapshot_max(MAX_SLOTS_SQL, "solana.slots").await?;
        let txs = s.snapshot_max(MAX_TX_SLOT_SQL, "solana.transactions").await?;
        let visible = slots.min(txs);
        let w = match valid.as_of_slot {
            Some(pin) if pin > visible => {
                return Err(QualityError::invalid(
                    "as_of_slot",
                    format!("{pin} is above {visible}, the newest fully visible slot"),
                ));
            }
            Some(pin) => pin,
            None => visible,
        };
        if i64::try_from(w).is_err() {
            return Err(QualityError::Permanent(format!("snapshot slot {w} does not fit in i64")));
        }
        let window = Window::ending_at(w, valid.window_slots);

        let mut checks = Vec::with_capacity(CHECKS.len());
        for def in CHECKS {
            checks.push(self.run_check(&mut s, def, &window).await);
        }
        Ok(QualityReport {
            as_of_slot: w,
            window,
            generated_at,
            overall: Overall::of(&checks),
            summary: StatusSummary::of(&checks),
            checks,
            queries_run: s.queries,
            duration_ms: millis(started),
        })
    }

    async fn run_check(&self, s: &mut Session<'_>, def: &'static CheckDef, w: &Window) -> CheckResult {
        let started = Instant::now();
        let before = s.queries;
        let mut result = CheckResult {
            id: def.id,
            group: def.group,
            status: Status::Pass,
            examined: 0,
            violations: 0,
            samples: Vec::new(),
            message: "",
            error_kind: None,
            lag_secs: None,
            query_count: 0,
            duration_ms: 0,
        };
        match def.probe {
            Probe::Freshness => self.freshness(s, def, &mut result).await,
            Probe::Count { count, sample } => {
                let missing_column = (def.id == "finalized_flag").then_some("is_finalized");
                Self::counted(s, def, &mut result, &count(w), sample.map(|f| f(w)), missing_column).await;
            }
        }
        result.message = def.message(result.status);
        result.query_count = s.queries - before;
        result.duration_ms = millis(started);
        result
    }

    async fn counted(
        s: &mut Session<'_>,
        def: &CheckDef,
        result: &mut CheckResult,
        count_sql: &str,
        sample_sql: Option<String>,
        missing_column: Option<&str>,
    ) {
        let counts = match s.check(count_sql, missing_column).await {
            Ok(response) => single_row(&response).and_then(|row| {
                let (examined, violations) = (get_u64(row, "examined")?, get_u64(row, "violations")?);
                if violations > examined { Err(Malformed) } else { Ok((examined, violations)) }
            }),
            Err(failure) => {
                fail_with(result, failure);
                return;
            }
        };
        let Ok((examined, violations)) = counts else {
            fail_with(result, QueryFailure::Error("malformed"));
            return;
        };
        result.examined = examined;
        result.violations = violations;
        result.status = match def.severity {
            Severity::Info => Status::Info,
            _ if violations == 0 => Status::Pass,
            Severity::Fail => Status::Fail,
            Severity::Warn => Status::Warn,
        };
        let Some(sample_sql) = sample_sql.filter(|_| violations > 0 && def.severity != Severity::Info) else {
            return;
        };
        // The count stands even if the sample fails; error_kind says so.
        match s.check(&sample_sql, missing_column).await {
            Ok(response) if response.rows.len() as u64 <= SAMPLE_LIMIT => {
                result.samples = response.rows.iter().map(sample_key).collect();
            }
            Ok(_) => result.error_kind = Some("malformed"),
            Err(QueryFailure::NotReady) => result.error_kind = Some("not_ready"),
            Err(QueryFailure::Error(kind)) => result.error_kind = Some(kind),
        }
    }

    async fn freshness(&self, s: &mut Session<'_>, def: &CheckDef, result: &mut CheckResult) {
        let response = match s.check(sql::DATA_FRESHNESS, None).await {
            Ok(r) => r,
            Err(failure) => return fail_with(result, failure),
        };
        let newest = single_row(&response).and_then(|row| match row.get("newest") {
            Some(Value::Null) => Ok(None),
            Some(v) => v.as_i64().map(Some).ok_or(Malformed),
            None => Err(Malformed),
        });
        match newest {
            Err(Malformed) => fail_with(result, QueryFailure::Error("malformed")),
            Ok(None) => result.status = Status::NotReady,
            Ok(Some(newest)) => {
                let now = i64::try_from(self.clock.now_unix_secs()).unwrap_or(i64::MAX);
                let lag = u64::try_from(now.saturating_sub(newest)).unwrap_or(0);
                result.examined = 1;
                result.lag_secs = Some(lag);
                result.violations = u64::from(lag > FRESHNESS_WARN_SECS);
                result.status = if result.violations == 0 {
                    Status::Pass
                } else if def.severity == Severity::Fail {
                    Status::Fail
                } else {
                    Status::Warn
                };
            }
        }
    }
}

fn fail_with(result: &mut CheckResult, failure: QueryFailure) {
    match failure {
        QueryFailure::NotReady => result.status = Status::NotReady,
        QueryFailure::Error(kind) => {
            result.status = Status::Error;
            result.error_kind = Some(kind);
        }
    }
}

fn millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn w() -> Window {
        Window::ending_at(1_000, 100)
    }

    fn all_sql() -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        for c in CHECKS {
            out.push((c.id, count_sql(c.id, &w()).unwrap()));
            if let Some(s) = sample_sql(c.id, &w()) {
                out.push((c.id, s));
            }
        }
        out
    }

    #[test]
    fn checks_are_in_group_then_id_order_and_unique() {
        for pair in CHECKS.windows(2) {
            assert!((pair[0].group, pair[0].id) < (pair[1].group, pair[1].id), "{} / {}", pair[0].id, pair[1].id);
        }
        assert_eq!(CHECKS.len(), 20);
    }

    #[test]
    fn every_table_read_is_bounded_by_the_window() {
        for (id, sql) in all_sql() {
            if id == "data_freshness" {
                continue;
            }
            let reads = sql.matches("FROM solana.").count();
            let bounds = sql.matches("slot >= 901 AND slot <= 1000").count();
            assert!(reads > 0, "{id}");
            assert_eq!(reads, bounds, "{id}: every table read needs its own bound: {sql}");
        }
    }

    #[test]
    fn no_slot_arithmetic_no_multi_column_count_distinct() {
        for (id, sql) in all_sql() {
            for bad in ["slot -", "slot +", "slot - 1", "slot + 1"] {
                assert!(!sql.contains(bad), "{id}: {bad} in {sql}");
            }
            for part in sql.split("COUNT(DISTINCT ").skip(1) {
                let arg = &part[..part.find(')').unwrap()];
                assert!(!arg.contains(','), "{id}: COUNT(DISTINCT {arg})");
            }
        }
    }

    #[test]
    fn no_join_on_a_raw_nullable_inner_index() {
        for (id, sql) in all_sql() {
            assert!(!sql.contains(".inner_instruction_index = "), "{id}: {sql}");
            assert!(!sql.contains("= x.inner_instruction_index"), "{id}");
        }
    }

    #[test]
    fn samples_are_limited_and_ordered() {
        for c in CHECKS {
            if let Some(s) = sample_sql(c.id, &w()) {
                assert!(s.contains(&format!("LIMIT {SAMPLE_LIMIT}")), "{}", c.id);
                assert!(s.contains("ORDER BY"), "{}", c.id);
            }
            // Info checks report shares, not faults, so they never sample.
            assert_eq!(c.severity == Severity::Info, sample_sql(c.id, &w()).is_none() && c.id != "data_freshness");
        }
    }

    #[test]
    fn messages_are_fixed_and_non_empty_for_every_reachable_status() {
        for c in CHECKS {
            let reachable: &[Status] = match c.severity {
                Severity::Info => &[Status::Info, Status::Error, Status::NotReady],
                Severity::Warn => &[Status::Pass, Status::Warn, Status::Error, Status::NotReady],
                Severity::Fail => &[Status::Pass, Status::Fail, Status::Error, Status::NotReady],
            };
            for s in reachable {
                assert!(!c.message(*s).is_empty(), "{} {:?}", c.id, s);
            }
        }
        assert!(def("finalized_flag").unwrap().pass_message.contains("never rewrites"));
    }

    #[test]
    fn overall_ranks_fail_incomplete_warn_pass_and_ignores_info() {
        let r = |id: &'static str, status| CheckResult {
            id,
            group: def(id).unwrap().group,
            status,
            examined: 0,
            violations: 0,
            samples: vec![],
            message: "",
            error_kind: None,
            lag_secs: None,
            query_count: 1,
            duration_ms: 0,
        };
        use Status::*;
        assert_eq!(Overall::of(&[]), Overall::Pass);
        assert_eq!(Overall::of(&[r("slot_duplicates", Pass), r("unattributed_transfers", Info)]), Overall::Pass);
        assert_eq!(Overall::of(&[r("spl_decode_rate", Warn), r("slot_duplicates", Pass)]), Overall::Warn);
        assert_eq!(Overall::of(&[r("spl_decode_rate", Warn), r("slot_duplicates", Error)]), Overall::Incomplete);
        assert_eq!(Overall::of(&[r("spl_decode_rate", NotReady)]), Overall::Incomplete);
        assert_eq!(Overall::of(&[r("slot_duplicates", Fail), r("spl_decode_rate", Error)]), Overall::Fail);
        // An info check that cannot run hides nothing.
        assert_eq!(Overall::of(&[r("finalized_flag", NotReady), r("owner_resolution", Error)]), Overall::Pass);
        assert_eq!(serde_json::to_value(Overall::Incomplete).unwrap(), "incomplete");
    }

    #[test]
    fn requests_are_validated() {
        assert_eq!(validate(&AuditRequest::default()).unwrap().window_slots, DEFAULT_WINDOW_SLOTS);
        for bad in [0, MAX_WINDOW_SLOTS + 1] {
            let err = validate(&AuditRequest { window_slots: Some(bad), as_of_slot: None }).unwrap_err();
            assert!(matches!(err, QualityError::Invalid { ref field, .. } if field == "window_slots"), "{err:?}");
        }
        let err = validate(&AuditRequest { window_slots: None, as_of_slot: Some(u64::MAX) }).unwrap_err();
        assert_eq!(err.kind(), "invalid");
    }

    #[test]
    fn sample_keys_join_columns_in_order() {
        let row: Map<String, Value> =
            serde_json::from_str(r#"{"c2": 0, "c0": 7, "c3": null, "c1": "sig"}"#)
                .unwrap();
        assert_eq!(sample_key(&row), "7:sig:0:-");
    }
}
