//! The store: one embedded SQLite file (`oracle.db`) holding all of ORACLE's
//! own state. Everything else ORACLE knows comes from LENS on demand.
//!
//! | Table               | Holds                                                      |
//! |---------------------|------------------------------------------------------------|
//! | `anomalies`         | every finding a detector committed, one row per key        |
//! | `detector_progress` | per metric, the last bucket a detector fully evaluated     |
//! | `digests`           | rendered "what changed" summaries, one per slot range      |
//!
//! # Scan commits
//!
//! A detector scans a range of buckets and calls [`Store::commit_scan`] with
//! its findings and the last bucket it evaluated. The anomalies and the
//! progress commit in **one transaction**, as FORGE's alert progress and
//! outbox row do. A crash can therefore never leave progress recorded without
//! its anomalies (findings lost for good, since the buckets are never scanned
//! again) or anomalies without progress (the buckets are rescanned, which is
//! harmless but wasted work).
//!
//! `(metric, subject, bucket)` is unique. `subject` is `''` for a global
//! series (transactions per bucket) and an entity for per-entity findings (a
//! program's failure rate, a first-seen mint). Each commit is judged
//! against the stored progress `P` for its metric:
//!
//! | `progress_bucket` | Meaning                          | Outcome                                                         |
//! |-------------------|----------------------------------|-----------------------------------------------------------------|
//! | none stored, or > `P` | a new scan                   | insert every anomaly, advance progress; every anomaly's bucket must be after `P` |
//! | `= P`             | a replay (e.g. a retried commit) | accepted only if every anomaly is already stored; nothing changes |
//! | `< P`             | a stale scan, or a ledger reset  | [`StoreError::Conflict`]; nothing changes                       |
//!
//! So a commit either moves progress forward with exactly its findings, or
//! repeats one that already did, or writes nothing at all. Each bucket is
//! judged by exactly one scan. A row that fails validation anywhere in the
//! batch rejects the whole batch before the database is touched.
//!
//! Anomalies are inserted with `ON CONFLICT (metric, subject, bucket) DO
//! NOTHING`, not `INSERT OR IGNORE`: `OR IGNORE` also silently skips rows
//! that break a `NOT NULL` or `CHECK` constraint, so a corrupt row would be
//! counted as a duplicate instead of failing the commit.
//!
//! # Ledger resets
//!
//! Devnet is reset from time to time. When PRISM restarts from a low slot,
//! bucket numbers go backwards while the stored progress stays high, so
//! every new scan is "stale" and is refused. The refusal names
//! [`Store::reset_progress`], the operator remedy: it deletes the metric's
//! progress row, so the next scan baselines again. Stored anomalies are kept.
//! If old-ledger anomalies are still present when the new ledger reaches the
//! same bucket numbers, the new findings at those keys are reported as
//! duplicates in [`ScanCommit::duplicates`]; prune them first with
//! [`Store::prune_anomalies_before`] if the history is not wanted.
//!
//! # Storage rules
//!
//! * Slots and buckets are `u64` in Rust and `INTEGER` (`i64`) in SQLite,
//!   converted with checked casts. A value above `i64::MAX` is rejected,
//!   never wrapped.
//! * SQLite stores a NaN `REAL` as NULL, silently. Every float is checked
//!   finite before it is bound. The schema also refuses infinities, as a
//!   second line of defence.
//! * Times are unix milliseconds (`i64`) throughout.
//!
//! # Opening
//!
//! [`Store::open`]:
//!
//! 1. takes an exclusive advisory lock on the sidecar `{db_path}.lock`, so
//!    two ORACLE processes never run detectors against one database,
//! 2. opens (creating if missing) `config.db_path()`, whose parent directory
//!    `config.rs` has checked,
//! 3. sets the connection pragmas (WAL, `synchronous = FULL`, foreign keys,
//!    a busy timeout),
//! 4. refuses a file that is not ORACLE's: a SQLite database that already
//!    holds other tables, or carries another `application_id` (FORGE's
//!    `forge.db`, say),
//! 5. runs migrations.
//!
//! `synchronous = FULL` costs one fsync per commit. Detectors commit about
//! once per metric per minute, so that is negligible, and in exchange a scan
//! that [`Store::commit_scan`] reported as committed survives power loss.
//!
//! # Migrations
//!
//! `PRAGMA user_version` is the schema version. [`MIGRATIONS`]`[n]` takes the
//! schema from version `n` to `n + 1`. Each migration runs in its own
//! transaction together with the version bump, so a crash mid-migration
//! leaves the previous version intact. A database newer than this build is
//! refused.
//!
//! # Errors
//!
//! Every public operation returns an [`OracleError::StoreError`] whose source
//! is a [`StoreError`]. [`OracleError::as_store_error`] recovers it, and
//! [`StoreError::http_status`] gives `api/` its status code without string
//! matching. Failing to open the lock file is an [`OracleError::IoError`].

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::types::Value as SqlValue;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params, params_from_iter};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::{BoxError, OracleConfig, OracleError, OracleResult};

/// Suffix of the sidecar file [`Store::open`] locks.
pub const LOCK_FILE_SUFFIX: &str = ".lock";

/// How long a statement waits on a lock held by another connection.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// `PRAGMA application_id` of an ORACLE database: `"ORCL"` in ASCII.
pub const APPLICATION_ID: i32 = 0x4F52_434C;

/// Longest `metric`, in characters.
pub const MAX_METRIC_CHARS: usize = 64;
/// Longest `subject`, in characters. A base58 pubkey is at most 44.
pub const MAX_SUBJECT_CHARS: usize = 128;
/// Largest serialised anomaly `detail`, in bytes.
pub const MAX_DETAIL_BYTES: usize = 4 * 1024;
/// Largest serialised digest body, in bytes.
pub const MAX_DIGEST_BYTES: usize = 64 * 1024;
/// Most anomalies one scan may commit. Far above anything a sane detector
/// produces; it bounds how long one commit holds the write lock.
pub const MAX_ANOMALIES_PER_SCAN: usize = 10_000;
/// Most rows any list operation returns; larger limits are capped to it.
pub const MAX_LIST_LIMIT: usize = 1_000;
/// [`AnomalyFilter::default`]'s limit.
pub const DEFAULT_LIST_LIMIT: usize = 100;

/// Schema migrations, oldest first. Never edit a released entry; append one.
pub const MIGRATIONS: &[&str] = &[
    // 1: anomalies, detector progress and digests.
    //
    // The CHECKs repeat the validation `commit_scan` and `insert_digest`
    // already do, so a bug there fails loudly instead of storing bad rows.
    // `9e999` is infinity to SQLite. A NaN is bound as NULL, which the NOT
    // NULL columns refuse; the nullable ones rely on validation.
    r#"
    CREATE TABLE anomalies (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        metric      TEXT    NOT NULL CHECK (length(metric) BETWEEN 1 AND 64),
        subject     TEXT    NOT NULL CHECK (length(subject) <= 128),
        bucket      INTEGER NOT NULL CHECK (bucket >= 0),
        start_slot  INTEGER NOT NULL CHECK (start_slot >= 0),
        end_slot    INTEGER NOT NULL CHECK (end_slot >= start_slot),
        kind        TEXT    NOT NULL
                    CHECK (kind IN ('series', 'first_seen', 'concentration', 'program_failure')),
        value       REAL    NOT NULL CHECK (value > -9e999 AND value < 9e999),
        baseline    REAL             CHECK (baseline > -9e999 AND baseline < 9e999),
        scale       REAL             CHECK (scale >= 0 AND scale < 9e999),
        score       REAL    NOT NULL CHECK (score > -9e999 AND score < 9e999),
        detail      TEXT             CHECK (json_valid(detail) AND length(CAST(detail AS BLOB)) <= 4096),
        detected_at INTEGER NOT NULL,
        -- What makes replays idempotent.
        UNIQUE (metric, subject, bucket),
        -- A MAD without the median it was measured around means nothing.
        CHECK (scale IS NULL OR baseline IS NOT NULL)
    ) STRICT;

    -- Listing newest-first overall and per metric, and pruning by age.
    -- Per (metric, subject) is served by the UNIQUE index.
    CREATE INDEX anomalies_by_bucket ON anomalies (bucket, id);
    CREATE INDEX anomalies_by_metric_bucket ON anomalies (metric, bucket, id);
    CREATE INDEX anomalies_by_detected_at ON anomalies (detected_at);

    CREATE TABLE detector_progress (
        metric     TEXT    PRIMARY KEY NOT NULL CHECK (length(metric) BETWEEN 1 AND 64),
        bucket     INTEGER NOT NULL CHECK (bucket >= 0),
        updated_at INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE digests (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        start_slot INTEGER NOT NULL CHECK (start_slot >= 0),
        end_slot   INTEGER NOT NULL CHECK (end_slot >= start_slot),
        body       TEXT    NOT NULL CHECK (json_valid(body) AND length(CAST(body AS BLOB)) <= 65536),
        created_at INTEGER NOT NULL,
        UNIQUE (start_slot, end_slot)
    ) STRICT;

    CREATE INDEX digests_by_created_at ON digests (created_at);
    "#,
];

// ---------------------------------------------------------------------------
// StoreError
// ---------------------------------------------------------------------------

/// Why a store operation failed. Always carried as the source of an
/// [`OracleError::StoreError`].
#[derive(Debug, Error)]
pub enum StoreError {
    /// Input failed validation; nothing was written. HTTP 400.
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },

    /// The write clashes with stored state: stale progress, a replay that
    /// would add rows, a second ORACLE on the same file. Nothing was
    /// written. HTTP 409.
    #[error("conflict: {reason}")]
    Conflict { reason: String },

    /// SQLite failed, or stored data is unreadable. HTTP 500.
    #[error("storage failure: {0}")]
    Storage(BoxError),
}

impl StoreError {
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub fn conflict(reason: impl Into<String>) -> Self {
        Self::Conflict { reason: reason.into() }
    }

    pub fn storage(source: impl Into<BoxError>) -> Self {
        Self::Storage(source.into())
    }

    /// The HTTP status `api/` answers with.
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::Conflict { .. } => 409,
            Self::Storage(_) => 500,
        }
    }
}

// As in FORGE's registry: a storage failure needs no context of its own (the
// enclosing `OracleError::StoreError` names the operation), so `?` on a
// rusqlite call inside a store closure is fine.
impl From<rusqlite::Error> for StoreError {
    fn from(err: rusqlite::Error) -> Self {
        Self::storage(err)
    }
}

impl OracleError {
    /// The [`StoreError`] behind an [`OracleError::StoreError`].
    pub fn as_store_error(&self) -> Option<&StoreError> {
        match self {
            Self::StoreError { source, .. } => source.downcast_ref(),
            _ => None,
        }
    }
}

/// Slots and buckets are `u64`; SQLite integers are `i64`. No real value
/// comes near the boundary, but one must never wrap silently.
fn to_sql(field: &str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::invalid(field, format!("{value} exceeds {}", i64::MAX)))
}

/// The reverse of [`to_sql`], for values read back. The schema's CHECKs keep
/// them non-negative, so a failure means the file was edited by hand.
fn from_sql(what: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::storage(format!("stored {what} {value} is negative")))
}

/// Now, in unix milliseconds. A clock before 1970 reads as 0.
pub fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
}

/// `limit` capped at [`MAX_LIST_LIMIT`]. Zero is a caller bug, not a request
/// for nothing.
fn clamp_limit(limit: usize) -> Result<i64, StoreError> {
    if limit == 0 {
        return Err(StoreError::invalid("limit", "must be at least 1"));
    }
    Ok(limit.min(MAX_LIST_LIMIT) as i64)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// What kind of detector produced an anomaly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnomalyKind {
    /// A value far from its rolling median/MAD baseline.
    Series,
    /// A mint or program never seen before.
    FirstSeen,
    /// One entity taking an unusual share of activity.
    Concentration,
    /// A shift in a program's failure rate.
    ProgramFailure,
}

impl AnomalyKind {
    pub const ALL: [Self; 4] = [Self::Series, Self::FirstSeen, Self::Concentration, Self::ProgramFailure];

    /// The stored and serialised name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Series => "series",
            Self::FirstSeen => "first_seen",
            Self::Concentration => "concentration",
            Self::ProgramFailure => "program_failure",
        }
    }

    /// The kind named `name` (as [`Self::as_str`] spells it).
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == name)
    }
}

impl std::fmt::Display for AnomalyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One finding, as a detector hands it to [`Store::commit_scan`]. The metric
/// is the scan's, so it is not repeated here.
#[derive(Debug, Clone, PartialEq)]
pub struct NewAnomaly {
    /// `""` for a global series; otherwise the entity (mint, program,
    /// address). At most [`MAX_SUBJECT_CHARS`] characters, no control
    /// characters, no surrounding whitespace.
    pub subject: String,
    pub bucket: u64,
    /// First slot of the bucket's data. At most `end_slot`.
    pub start_slot: u64,
    pub end_slot: u64,
    pub kind: AnomalyKind,
    /// The observed value. Finite.
    pub value: f64,
    /// The median at scoring time, if the kind has one. Finite.
    pub baseline: Option<f64>,
    /// The MAD at scoring time. Finite, non-negative, and only with a
    /// `baseline`.
    pub scale: Option<f64>,
    /// How anomalous: a modified z-score for series. Finite, any sign.
    pub score: f64,
    /// Extra context for the API. A JSON object of at most
    /// [`MAX_DETAIL_BYTES`] serialised.
    pub detail: Option<serde_json::Value>,
}

impl NewAnomaly {
    /// Check every field rule without touching the database.
    pub fn validate(&self) -> Result<(), StoreError> {
        self.prepare("anomaly").map(drop)
    }

    /// Validate and convert to bindable values. `at` prefixes field names in
    /// errors: `anomalies[3].score`.
    fn prepare(&self, at: &str) -> Result<PreparedAnomaly, StoreError> {
        let field = |name: &str| format!("{at}.{name}");

        validate_subject(&field("subject"), &self.subject)?;
        let bucket = to_sql(&field("bucket"), self.bucket)?;
        let start_slot = to_sql(&field("start_slot"), self.start_slot)?;
        let end_slot = to_sql(&field("end_slot"), self.end_slot)?;
        if self.start_slot > self.end_slot {
            return Err(StoreError::invalid(
                field("end_slot"),
                format!("{} is before start_slot {}", self.end_slot, self.start_slot),
            ));
        }

        check_finite(&field("value"), self.value)?;
        check_finite(&field("score"), self.score)?;
        if let Some(baseline) = self.baseline {
            check_finite(&field("baseline"), baseline)?;
        }
        if let Some(scale) = self.scale {
            check_finite(&field("scale"), scale)?;
            if scale < 0.0 {
                return Err(StoreError::invalid(field("scale"), format!("{scale} is negative; a MAD is at least 0")));
            }
            if self.baseline.is_none() {
                return Err(StoreError::invalid(field("scale"), "is set without a baseline"));
            }
        }

        let detail = match &self.detail {
            None => None,
            Some(value) => Some(serialize_object(&field("detail"), value, MAX_DETAIL_BYTES)?),
        };

        Ok(PreparedAnomaly {
            subject: self.subject.clone(),
            bucket: self.bucket,
            sql_bucket: bucket,
            start_slot,
            end_slot,
            kind: self.kind.as_str(),
            value: self.value,
            baseline: self.baseline,
            scale: self.scale,
            score: self.score,
            detail,
        })
    }
}

/// One stored anomaly.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Anomaly {
    pub id: i64,
    pub metric: String,
    pub subject: String,
    pub bucket: u64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub kind: AnomalyKind,
    pub value: f64,
    pub baseline: Option<f64>,
    pub scale: Option<f64>,
    pub score: f64,
    pub detail: Option<serde_json::Value>,
    /// When the scan that found it committed, unix ms.
    pub detected_at: i64,
}

/// Which anomalies [`Store::list_anomalies`] returns. Unset filters match
/// everything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnomalyFilter {
    pub metric: Option<String>,
    /// `Some("")` selects global series only.
    pub subject: Option<String>,
    /// Only buckets at or after this one.
    pub since_bucket: Option<u64>,
    /// At least 1; capped at [`MAX_LIST_LIMIT`].
    pub limit: usize,
}

impl Default for AnomalyFilter {
    fn default() -> Self {
        Self {
            metric: None,
            subject: None,
            since_bucket: None,
            limit: DEFAULT_LIST_LIMIT,
        }
    }
}

/// What one [`Store::commit_scan`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ScanCommit {
    /// Anomalies newly stored.
    pub inserted: usize,
    /// Anomalies whose `(metric, subject, bucket)` was already stored. The
    /// stored row is kept as it was.
    pub duplicates: usize,
    /// Whether progress moved. `false` only for a replay.
    pub progress_advanced: bool,
}

/// One metric's progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DetectorProgress {
    pub metric: String,
    /// The last fully evaluated bucket.
    pub bucket: u64,
    /// When it last advanced, unix ms.
    pub updated_at: i64,
}

/// One stored digest.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Digest {
    pub id: i64,
    pub start_slot: u64,
    pub end_slot: u64,
    pub body: serde_json::Value,
    /// Unix ms.
    pub created_at: i64,
}

/// What [`Store::stats`] reports, for `/health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct StoreStats {
    pub schema_version: u32,
    pub anomalies: u64,
    /// Metrics with stored progress.
    pub detectors: u64,
    pub digests: u64,
    /// `detected_at` of the oldest and newest anomaly, unix ms.
    pub oldest_anomaly_at: Option<i64>,
    pub newest_anomaly_at: Option<i64>,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// `a-z`, `0-9`, `_`, `.`, `-`; 1 to [`MAX_METRIC_CHARS`] characters. Metric
/// names appear in URLs and metric labels, so they stay plain.
fn validate_metric(field: &str, metric: &str) -> Result<(), StoreError> {
    if metric.is_empty() {
        return Err(StoreError::invalid(field, "must not be empty"));
    }
    let chars = metric.chars().count();
    if chars > MAX_METRIC_CHARS {
        return Err(StoreError::invalid(
            field,
            format!("is {chars} characters; at most {MAX_METRIC_CHARS} are allowed"),
        ));
    }
    if let Some(bad) = metric
        .chars()
        .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '.' | '-')))
    {
        return Err(StoreError::invalid(
            field,
            format!("{metric:?} contains {bad:?}; only a-z, 0-9, '_', '.' and '-' are allowed"),
        ));
    }
    Ok(())
}

/// Empty, or up to [`MAX_SUBJECT_CHARS`] characters with no control
/// characters and no surrounding whitespace: `" mint"` and `"mint"` would
/// otherwise be two keys for one entity.
fn validate_subject(field: &str, subject: &str) -> Result<(), StoreError> {
    let chars = subject.chars().count();
    if chars > MAX_SUBJECT_CHARS {
        return Err(StoreError::invalid(
            field,
            format!("is {chars} characters; at most {MAX_SUBJECT_CHARS} are allowed"),
        ));
    }
    if subject.chars().any(char::is_control) {
        return Err(StoreError::invalid(field, "contains a control character"));
    }
    if subject.trim() != subject {
        return Err(StoreError::invalid(field, format!("{subject:?} has surrounding whitespace")));
    }
    Ok(())
}

fn check_finite(field: &str, value: f64) -> Result<(), StoreError> {
    if value.is_finite() {
        Ok(())
    } else {
        // SQLite would store NaN as NULL without complaint.
        Err(StoreError::invalid(field, format!("{value} is not finite")))
    }
}

/// `value` as compact JSON, if it is an object of at most `max_bytes`.
fn serialize_object(field: &str, value: &serde_json::Value, max_bytes: usize) -> Result<String, StoreError> {
    if !value.is_object() {
        return Err(StoreError::invalid(field, "must be a JSON object"));
    }
    let json = serde_json::to_string(value).map_err(StoreError::storage)?;
    if json.len() > max_bytes {
        return Err(StoreError::invalid(
            field,
            format!("is {} bytes serialised; at most {max_bytes} are allowed", json.len()),
        ));
    }
    Ok(json)
}

/// An anomaly that passed validation, with its values ready to bind.
#[derive(Debug, Clone)]
struct PreparedAnomaly {
    subject: String,
    bucket: u64,
    sql_bucket: i64,
    start_slot: i64,
    end_slot: i64,
    kind: &'static str,
    value: f64,
    baseline: Option<f64>,
    scale: Option<f64>,
    score: f64,
    detail: Option<String>,
}

/// A whole scan that passed validation.
#[derive(Debug, Clone)]
struct PreparedScan {
    metric: String,
    progress_bucket: u64,
    sql_progress: i64,
    rows: Vec<PreparedAnomaly>,
}

impl PreparedScan {
    /// Every check that needs no database, so an invalid row anywhere in the
    /// batch rejects the batch before the write lock is taken.
    fn new(metric: &str, anomalies: &[NewAnomaly], progress_bucket: u64) -> Result<Self, StoreError> {
        validate_metric("metric", metric)?;
        let sql_progress = to_sql("progress_bucket", progress_bucket)?;
        if anomalies.len() > MAX_ANOMALIES_PER_SCAN {
            return Err(StoreError::invalid(
                "anomalies",
                format!("{} in one scan; at most {MAX_ANOMALIES_PER_SCAN} are allowed", anomalies.len()),
            ));
        }

        let mut rows = Vec::with_capacity(anomalies.len());
        let mut seen: HashMap<(&str, u64), usize> = HashMap::with_capacity(anomalies.len());
        for (index, anomaly) in anomalies.iter().enumerate() {
            let at = format!("anomalies[{index}]");
            let row = anomaly.prepare(&at)?;
            if anomaly.bucket > progress_bucket {
                return Err(StoreError::invalid(
                    format!("{at}.bucket"),
                    format!(
                        "{} is after progress_bucket {progress_bucket}; a scan can only report buckets it evaluated",
                        anomaly.bucket
                    ),
                ));
            }
            // Within one batch a repeated key is a detector bug, not a
            // replay; storing the first and counting the second as a
            // duplicate would hide it.
            if let Some(first) = seen.insert((anomaly.subject.as_str(), anomaly.bucket), index) {
                return Err(StoreError::invalid(
                    at.clone(),
                    format!(
                        "repeats anomalies[{first}]: subject {:?}, bucket {}",
                        anomaly.subject, anomaly.bucket
                    ),
                ));
            }
            rows.push(row);
        }

        Ok(Self {
            metric: metric.to_owned(),
            progress_bucket,
            sql_progress,
            rows,
        })
    }

    fn earliest_bucket(&self) -> Option<u64> {
        self.rows.iter().map(|row| row.bucket).min()
    }
}

// ---------------------------------------------------------------------------
// Scan commit, in stages
// ---------------------------------------------------------------------------

/// How a scan relates to the stored progress; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanMode {
    Advance,
    Replay,
}

/// Judge `scan` against the stored progress, inside `tx`.
fn check_progress(tx: &Transaction<'_>, scan: &PreparedScan) -> Result<ScanMode, StoreError> {
    let Some(stored) = read_progress(tx, &scan.metric)? else {
        return Ok(ScanMode::Advance);
    };
    let (metric, requested) = (&scan.metric, scan.progress_bucket);
    if requested < stored {
        return Err(StoreError::conflict(format!(
            "metric {metric:?} is already evaluated through bucket {stored}; a scan through {requested} is stale. \
             If the ledger was reset, call reset_progress({metric:?})"
        )));
    }
    if requested == stored {
        return Ok(ScanMode::Replay);
    }
    if let Some(earliest) = scan.earliest_bucket()
        && earliest <= stored
    {
        return Err(StoreError::conflict(format!(
            "metric {metric:?} is already evaluated through bucket {stored}; an anomaly for bucket {earliest} \
             belongs to an earlier scan"
        )));
    }
    Ok(ScanMode::Advance)
}

/// Insert every row of `scan`, skipping keys already stored. Returns
/// `(inserted, duplicates)`. Any other constraint violation is an error.
fn insert_anomalies(tx: &Transaction<'_>, scan: &PreparedScan, now: i64) -> Result<(usize, usize), StoreError> {
    let mut stmt = tx.prepare_cached(
        "INSERT INTO anomalies (metric, subject, bucket, start_slot, end_slot, kind, value, baseline, scale, score,
                                detail, detected_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
         ON CONFLICT (metric, subject, bucket) DO NOTHING",
    )?;
    let mut inserted = 0;
    for row in &scan.rows {
        inserted += stmt.execute(params![
            scan.metric,
            row.subject,
            row.sql_bucket,
            row.start_slot,
            row.end_slot,
            row.kind,
            row.value,
            row.baseline,
            row.scale,
            row.score,
            row.detail,
            now
        ])?;
    }
    Ok((inserted, scan.rows.len() - inserted))
}

/// Finish the scan after its inserts: advance progress, or confirm that a
/// replay added nothing.
fn finish_scan(
    tx: &Transaction<'_>,
    scan: &PreparedScan,
    mode: ScanMode,
    (inserted, duplicates): (usize, usize),
    now: i64,
) -> Result<ScanCommit, StoreError> {
    match mode {
        ScanMode::Replay => {
            if inserted > 0 {
                // Rolls back the rows just inserted.
                return Err(StoreError::conflict(format!(
                    "metric {:?} is already evaluated through bucket {}; a replay of that scan would add {inserted} \
                     new anomalies, but a scan that does not advance progress may only repeat stored ones",
                    scan.metric, scan.progress_bucket
                )));
            }
            Ok(ScanCommit { inserted, duplicates, progress_advanced: false })
        }
        ScanMode::Advance => {
            // The WHERE repeats `check_progress`'s rule in SQL, so progress
            // cannot move backwards even if the two ever disagree.
            let changed = tx.execute(
                "INSERT INTO detector_progress (metric, bucket, updated_at) VALUES (?1, ?2, ?3)
                 ON CONFLICT (metric) DO UPDATE SET bucket = excluded.bucket, updated_at = excluded.updated_at
                 WHERE excluded.bucket > detector_progress.bucket",
                params![scan.metric, scan.sql_progress, now],
            )?;
            if changed != 1 {
                return Err(StoreError::storage(format!(
                    "progress for metric {:?} did not advance to {}",
                    scan.metric, scan.progress_bucket
                )));
            }
            Ok(ScanCommit { inserted, duplicates, progress_advanced: true })
        }
    }
}

/// The whole scan commit, inside `tx`.
fn write_scan(tx: &Transaction<'_>, scan: &PreparedScan, now: i64) -> Result<ScanCommit, StoreError> {
    let mode = check_progress(tx, scan)?;
    let counts = insert_anomalies(tx, scan, now)?;
    finish_scan(tx, scan, mode, counts, now)
}

fn read_progress(conn: &Connection, metric: &str) -> Result<Option<u64>, StoreError> {
    let bucket: Option<i64> = conn
        .query_row("SELECT bucket FROM detector_progress WHERE metric = ?1", [metric], |row| row.get(0))
        .optional()?;
    bucket.map(|bucket| from_sql("progress bucket", bucket)).transpose()
}

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// ORACLE's open database. Cheap to clone; every clone shares one
/// connection and the file lock, which is released when the last clone is
/// dropped.
#[derive(Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    path: Arc<PathBuf>,
    /// Exclusively locked until the last clone is dropped.
    _lock: Arc<File>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("path", &self.path).finish_non_exhaustive()
    }
}

impl Store {
    /// Open (or create) `config.db_path()`; see the module docs.
    pub fn open(config: &OracleConfig) -> OracleResult<Self> {
        Self::open_path(config.db_path())
    }

    /// [`Store::open`] on an explicit path. `path`'s parent must exist.
    pub fn open_path(path: impl AsRef<Path>) -> OracleResult<Self> {
        let path = path.as_ref().to_path_buf();
        let fail = |operation: &str, err: StoreError| OracleError::store(format!("{operation} {}", path.display()), err);

        let lock_path = sidecar_lock_path(&path);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| OracleError::io(lock_path.display().to_string(), err))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(fail(
                    "lock",
                    StoreError::conflict(format!(
                        "another process holds {} (is a second ORACLE running on this database?)",
                        lock_path.display()
                    )),
                ));
            }
            Err(TryLockError::Error(err)) => return Err(OracleError::io(lock_path.display().to_string(), err)),
        }

        let mut conn = Connection::open(&path).map_err(|err| fail("open", err.into()))?;
        configure(&conn).map_err(|err| fail("configure", err))?;
        check_identity(&conn).map_err(|err| fail("open", err))?;
        let version = migrate(&mut conn).map_err(|err| fail("migrate", err))?;
        info!(path = %path.display(), schema_version = version, "store open");

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            path: Arc::new(path),
            _lock: Arc::new(lock),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic mid-transaction drops its `Transaction`, which rolls back,
        // so the connection is still consistent.
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `f` on the connection outside any explicit transaction.
    fn with_conn<T>(&self, operation: &str, f: impl FnOnce(&Connection) -> Result<T, StoreError>) -> OracleResult<T> {
        f(&self.lock()).map_err(|err| OracleError::store(operation, err))
    }

    /// Run `f` in one `BEGIN IMMEDIATE` transaction: commit if it returns
    /// `Ok`, roll back if it returns `Err` or panics. `IMMEDIATE` takes the
    /// write lock up front, so the transaction cannot fail half-way with
    /// `SQLITE_BUSY` when it first writes.
    fn transaction<T>(
        &self,
        operation: &str,
        f: impl FnOnce(&Transaction<'_>) -> Result<T, StoreError>,
    ) -> OracleResult<T> {
        let mut conn = self.lock();
        let run = |conn: &mut Connection| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let value = f(&tx)?;
            tx.commit()?;
            Ok(value)
        };
        run(&mut conn).map_err(|err: StoreError| OracleError::store(operation, err))
    }

    // -- scans --------------------------------------------------------------

    /// Store a scan's anomalies and advance `metric`'s progress to
    /// `progress_bucket`, all in one transaction. See the module docs for how
    /// the commit is judged against the stored progress.
    ///
    /// Every anomaly is validated first, and every anomaly's bucket must be
    /// at most `progress_bucket`. Any failure, at any stage, writes nothing.
    pub fn commit_scan(
        &self,
        metric: &str,
        anomalies: &[NewAnomaly],
        progress_bucket: u64,
    ) -> OracleResult<ScanCommit> {
        let operation = format!("commit scan {metric}");
        let scan =
            PreparedScan::new(metric, anomalies, progress_bucket).map_err(|err| OracleError::store(&operation, err))?;
        let commit = self.transaction(&operation, |tx| write_scan(tx, &scan, unix_now_ms()))?;
        debug!(
            metric,
            progress_bucket,
            inserted = commit.inserted,
            duplicates = commit.duplicates,
            progress_advanced = commit.progress_advanced,
            "scan committed"
        );
        Ok(commit)
    }

    // -- progress -----------------------------------------------------------

    /// The last bucket `metric` has fully evaluated; `None` before its first
    /// scan, or after [`Store::reset_progress`].
    pub fn progress(&self, metric: &str) -> OracleResult<Option<u64>> {
        self.with_conn("read progress", |conn| read_progress(conn, metric))
    }

    /// Every metric's progress, by metric name.
    pub fn list_progress(&self) -> OracleResult<Vec<DetectorProgress>> {
        self.with_conn("list progress", |conn| {
            let mut stmt = conn.prepare("SELECT metric, bucket, updated_at FROM detector_progress ORDER BY metric")?;
            let rows = stmt.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
            })?;
            rows.map(|row| {
                let (metric, bucket, updated_at) = row?;
                Ok(DetectorProgress {
                    metric,
                    bucket: from_sql("progress bucket", bucket)?,
                    updated_at,
                })
            })
            .collect()
        })
    }

    /// Forget `metric`'s progress, so its next scan baselines again. The
    /// remedy for a ledger reset (see the module docs). Anomalies are kept.
    /// Returns the progress that was cleared, `None` if there was none.
    pub fn reset_progress(&self, metric: &str) -> OracleResult<Option<u64>> {
        let cleared = self.transaction("reset progress", |tx| {
            let previous = read_progress(tx, metric)?;
            tx.execute("DELETE FROM detector_progress WHERE metric = ?1", [metric])?;
            Ok(previous)
        })?;
        if let Some(bucket) = cleared {
            warn!(metric, bucket, "detector progress reset");
        }
        Ok(cleared)
    }

    // -- anomalies ----------------------------------------------------------

    /// Anomalies matching `filter`, newest bucket first, then newest id
    /// first. At most `filter.limit` rows, capped at [`MAX_LIST_LIMIT`].
    pub fn list_anomalies(&self, filter: &AnomalyFilter) -> OracleResult<Vec<Anomaly>> {
        self.with_conn("list anomalies", |conn| {
            let limit = clamp_limit(filter.limit)?;
            let mut clauses = Vec::new();
            let mut args = Vec::new();
            if let Some(metric) = &filter.metric {
                args.push(SqlValue::Text(metric.clone()));
                clauses.push(format!("metric = ?{}", args.len()));
            }
            if let Some(subject) = &filter.subject {
                args.push(SqlValue::Text(subject.clone()));
                clauses.push(format!("subject = ?{}", args.len()));
            }
            if let Some(since) = filter.since_bucket {
                args.push(SqlValue::Integer(to_sql("since_bucket", since)?));
                clauses.push(format!("bucket >= ?{}", args.len()));
            }
            let mut sql = format!("SELECT {ANOMALY_COLUMNS} FROM anomalies");
            if !clauses.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&clauses.join(" AND "));
            }
            args.push(SqlValue::Integer(limit));
            sql.push_str(&format!(" ORDER BY bucket DESC, id DESC LIMIT ?{}", args.len()));

            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(args), read_anomaly)?;
            rows.map(|row| row?).collect()
        })
    }

    /// Delete anomalies detected before `unix_ms` (strictly). Returns how many
    /// were removed. Progress is untouched.
    pub fn prune_anomalies_before(&self, unix_ms: i64) -> OracleResult<u64> {
        self.with_conn("prune anomalies", |conn| {
            Ok(conn.execute("DELETE FROM anomalies WHERE detected_at < ?1", [unix_ms])? as u64)
        })
    }

    // -- digests ------------------------------------------------------------

    /// Store a digest for `start_slot..=end_slot`. `body` must be a JSON
    /// object of at most [`MAX_DIGEST_BYTES`] serialised. Returns whether a
    /// new row was created; a range already stored keeps its first body.
    pub fn insert_digest(&self, start_slot: u64, end_slot: u64, body: &serde_json::Value) -> OracleResult<bool> {
        self.with_conn("insert digest", |conn| {
            let start = to_sql("start_slot", start_slot)?;
            let end = to_sql("end_slot", end_slot)?;
            if start_slot > end_slot {
                return Err(StoreError::invalid(
                    "end_slot",
                    format!("{end_slot} is before start_slot {start_slot}"),
                ));
            }
            let body = serialize_object("body", body, MAX_DIGEST_BYTES)?;
            let inserted = conn.execute(
                "INSERT INTO digests (start_slot, end_slot, body, created_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (start_slot, end_slot) DO NOTHING",
                params![start, end, body, unix_now_ms()],
            )?;
            Ok(inserted == 1)
        })
    }

    /// The most recently stored digest.
    pub fn latest_digest(&self) -> OracleResult<Option<Digest>> {
        Ok(self.list_digests(1)?.pop())
    }

    /// Up to `limit` digests (capped at [`MAX_LIST_LIMIT`]), most recently
    /// stored first.
    ///
    /// Ordered by insertion, not by slot: after a ledger reset the newest
    /// digest covers the lowest slots.
    pub fn list_digests(&self, limit: usize) -> OracleResult<Vec<Digest>> {
        self.with_conn("list digests", |conn| {
            let limit = clamp_limit(limit)?;
            let mut stmt = conn.prepare(
                "SELECT id, start_slot, end_slot, body, created_at FROM digests ORDER BY id DESC LIMIT ?1",
            )?;
            let rows = stmt.query_map([limit], read_digest)?;
            rows.map(|row| row?).collect()
        })
    }

    /// Delete digests stored before `unix_ms` (strictly). Returns how many
    /// were removed.
    pub fn prune_digests_before(&self, unix_ms: i64) -> OracleResult<u64> {
        self.with_conn("prune digests", |conn| {
            Ok(conn.execute("DELETE FROM digests WHERE created_at < ?1", [unix_ms])? as u64)
        })
    }

    // -- maintenance --------------------------------------------------------

    /// The schema version (`PRAGMA user_version`).
    pub fn schema_version(&self) -> OracleResult<u32> {
        self.with_conn("read schema version", |conn| Ok(user_version(conn)?))
    }

    /// Row counts and the anomaly time span, for `/health`.
    pub fn stats(&self) -> OracleResult<StoreStats> {
        self.with_conn("read store stats", |conn| {
            let count = |table: &str| -> Result<u64, StoreError> {
                let n: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))?;
                from_sql("row count", n)
            };
            let (oldest_anomaly_at, newest_anomaly_at) =
                conn.query_row("SELECT MIN(detected_at), MAX(detected_at) FROM anomalies", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
            Ok(StoreStats {
                schema_version: user_version(conn)?,
                anomalies: count("anomalies")?,
                detectors: count("detector_progress")?,
                digests: count("digests")?,
                oldest_anomaly_at,
                newest_anomaly_at,
            })
        })
    }
}

/// `{db_path}.lock`.
pub fn sidecar_lock_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(LOCK_FILE_SUFFIX);
    PathBuf::from(name)
}

fn configure(conn: &Connection) -> Result<(), StoreError> {
    // WAL lets the API read while a detector commits.
    let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::storage(format!("journal_mode is {mode}, expected wal")));
    }
    conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    Ok(())
}

fn user_version(conn: &Connection) -> rusqlite::Result<u32> {
    conn.query_row("PRAGMA user_version", [], |row| row.get(0))
}

fn application_id(conn: &Connection) -> rusqlite::Result<i32> {
    conn.query_row("PRAGMA application_id", [], |row| row.get(0))
}

/// Refuse a database that belongs to something else, before migrations add
/// ORACLE's tables to it.
fn check_identity(conn: &Connection) -> Result<(), StoreError> {
    let app_id = application_id(conn)?;
    let version = user_version(conn)?;
    let not_ours = |what: String| {
        StoreError::conflict(format!(
            "not an ORACLE database ({what}); is ORACLE_DB_PATH pointing at another service's file?"
        ))
    };
    if version > 0 || app_id != 0 {
        if app_id != APPLICATION_ID {
            return Err(not_ours(format!("application_id {app_id:#x}, schema version {version}")));
        }
        return Ok(());
    }
    // A brand-new file, or an unversioned database with tables of its own.
    let tables: i64 =
        conn.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'", [], |row| row.get(0))?;
    if tables > 0 {
        return Err(not_ours(format!("{tables} unversioned tables")));
    }
    Ok(())
}

/// Apply every migration past the stored version; return the new version.
fn migrate(conn: &mut Connection) -> Result<u32, StoreError> {
    let target = MIGRATIONS.len() as u32;
    let current = user_version(conn)?;
    if current > target {
        return Err(StoreError::conflict(format!(
            "schema version {current} is newer than this ORACLE supports ({target})"
        )));
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        let version = index as u32 + 1;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(sql)?;
        // Both live in the database header, so they commit with the
        // migration.
        tx.pragma_update(None, "application_id", APPLICATION_ID)?;
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
        info!(version, "applied store migration");
    }
    Ok(target)
}

const ANOMALY_COLUMNS: &str =
    "id, metric, subject, bucket, start_slot, end_slot, kind, value, baseline, scale, score, detail, detected_at";

fn read_anomaly(row: &Row<'_>) -> rusqlite::Result<Result<Anomaly, StoreError>> {
    let id: i64 = row.get(0)?;
    let kind: String = row.get(6)?;
    let detail: Option<String> = row.get(11)?;
    let (bucket, start_slot, end_slot): (i64, i64, i64) = (row.get(3)?, row.get(4)?, row.get(5)?);
    let build = || -> Result<Anomaly, StoreError> {
        let kind = AnomalyKind::from_name(&kind)
            .ok_or_else(|| StoreError::storage(format!("anomaly {id} has unknown kind {kind:?}")))?;
        let detail = detail
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|err| StoreError::storage(format!("anomaly {id} has unreadable detail: {err}")))?;
        Ok(Anomaly {
            id,
            metric: row.get(1)?,
            subject: row.get(2)?,
            bucket: from_sql("bucket", bucket)?,
            start_slot: from_sql("start_slot", start_slot)?,
            end_slot: from_sql("end_slot", end_slot)?,
            kind,
            value: row.get(7)?,
            baseline: row.get(8)?,
            scale: row.get(9)?,
            score: row.get(10)?,
            detail,
            detected_at: row.get(12)?,
        })
    };
    Ok(build())
}

fn read_digest(row: &Row<'_>) -> rusqlite::Result<Result<Digest, StoreError>> {
    let id: i64 = row.get(0)?;
    let (start_slot, end_slot): (i64, i64) = (row.get(1)?, row.get(2)?);
    let body: String = row.get(3)?;
    let created_at: i64 = row.get(4)?;
    let build = || -> Result<Digest, StoreError> {
        Ok(Digest {
            id,
            start_slot: from_sql("start_slot", start_slot)?,
            end_slot: from_sql("end_slot", end_slot)?,
            body: serde_json::from_str(&body)
                .map_err(|err| StoreError::storage(format!("digest {id} has an unreadable body: {err}")))?,
            created_at,
        })
    };
    Ok(build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A scratch directory removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "oracle-store-unit-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn db(&self) -> PathBuf {
            self.0.join("oracle.db")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn anomaly(subject: &str, bucket: u64) -> NewAnomaly {
        NewAnomaly {
            subject: subject.into(),
            bucket,
            start_slot: bucket * 10,
            end_slot: bucket * 10 + 9,
            kind: AnomalyKind::Series,
            value: 42.0,
            baseline: Some(10.0),
            scale: Some(2.0),
            score: 10.8,
            detail: None,
        }
    }

    /// `(progress, anomaly count)` for `metric`, read on a fresh connection.
    fn state(path: &Path, metric: &str) -> (Option<i64>, i64) {
        let conn = Connection::open(path).unwrap();
        let progress = conn
            .query_row("SELECT bucket FROM detector_progress WHERE metric = ?1", [metric], |r| r.get(0))
            .optional()
            .unwrap();
        let count = conn
            .query_row("SELECT COUNT(*) FROM anomalies WHERE metric = ?1", [metric], |r| r.get(0))
            .unwrap();
        (progress, count)
    }

    #[test]
    fn http_status_per_variant() {
        assert_eq!(StoreError::invalid("f", "r").http_status(), 400);
        assert_eq!(StoreError::conflict("c").http_status(), 409);
        assert_eq!(StoreError::storage("s").http_status(), 500);
    }

    #[test]
    fn store_error_is_recoverable_from_oracle_error() {
        let err = OracleError::store("commit scan m", StoreError::conflict("stale"));
        assert!(matches!(err.as_store_error(), Some(StoreError::Conflict { .. })));
        assert!(OracleError::config("f", "r").as_store_error().is_none());
        // A StoreError variant whose source is something else.
        assert!(OracleError::store("op", "plain string").as_store_error().is_none());
        assert!(err.to_string().contains("commit scan m"), "{err}");
    }

    #[test]
    fn u64_conversions_are_checked_at_the_i64_boundary() {
        assert_eq!(to_sql("s", 0).unwrap(), 0);
        assert_eq!(to_sql("s", i64::MAX as u64).unwrap(), i64::MAX);
        assert!(to_sql("s", i64::MAX as u64 + 1).is_err());
        assert!(to_sql("s", u64::MAX).is_err());
        assert_eq!(from_sql("s", 0).unwrap(), 0);
        assert_eq!(from_sql("s", i64::MAX).unwrap(), i64::MAX as u64);
        assert!(from_sql("s", -1).is_err());
    }

    #[test]
    fn limits_are_capped_and_zero_is_rejected() {
        assert!(clamp_limit(0).is_err());
        assert_eq!(clamp_limit(1).unwrap(), 1);
        assert_eq!(clamp_limit(MAX_LIST_LIMIT).unwrap(), MAX_LIST_LIMIT as i64);
        assert_eq!(clamp_limit(MAX_LIST_LIMIT + 1).unwrap(), MAX_LIST_LIMIT as i64);
        assert_eq!(clamp_limit(usize::MAX).unwrap(), MAX_LIST_LIMIT as i64);
    }

    #[test]
    fn kind_names_round_trip_and_match_serde_and_the_schema() {
        for kind in AnomalyKind::ALL {
            assert_eq!(AnomalyKind::from_name(kind.as_str()), Some(kind));
            assert_eq!(serde_json::to_value(kind).unwrap(), kind.as_str());
            assert!(MIGRATIONS[0].contains(&format!("'{}'", kind.as_str())), "{kind} missing from the CHECK");
        }
        assert_eq!(AnomalyKind::from_name("Series"), None);
        assert_eq!(AnomalyKind::from_name(""), None);
    }

    #[test]
    fn the_application_id_spells_orcl() {
        assert_eq!(APPLICATION_ID.to_be_bytes(), *b"ORCL");
    }

    #[test]
    fn migrations_are_idempotent_and_stamp_the_application_id() {
        let dir = TempDir::new();
        let mut conn = Connection::open(dir.db()).unwrap();
        assert_eq!(migrate(&mut conn).unwrap(), MIGRATIONS.len() as u32);
        assert_eq!(migrate(&mut conn).unwrap(), MIGRATIONS.len() as u32);
        assert_eq!(application_id(&conn).unwrap(), APPLICATION_ID);
        check_identity(&conn).unwrap();
    }

    #[test]
    fn the_schema_limits_match_the_constants() {
        let schema = MIGRATIONS[0];
        assert!(schema.contains(&format!("length(metric) BETWEEN 1 AND {MAX_METRIC_CHARS}")));
        assert!(schema.contains(&format!("length(subject) <= {MAX_SUBJECT_CHARS}")));
        assert!(schema.contains(&format!("length(CAST(detail AS BLOB)) <= {MAX_DETAIL_BYTES}")));
        assert!(schema.contains(&format!("length(CAST(body AS BLOB)) <= {MAX_DIGEST_BYTES}")));
    }

    /// Why anomalies are inserted with `ON CONFLICT ... DO NOTHING`: a row
    /// that somehow got past validation must fail the commit, not be counted
    /// as a duplicate. `INSERT OR IGNORE` would swallow it.
    #[test]
    fn a_constraint_violation_is_an_error_not_a_duplicate() {
        let dir = TempDir::new();
        let store = Store::open_path(dir.db()).unwrap();
        let mut scan = PreparedScan::new("m", &[anomaly("", 1)], 1).unwrap();
        // Bypass validation: NaN is bound as NULL into a NOT NULL column.
        scan.rows[0].score = f64::NAN;

        let err = store.transaction("t", |tx| insert_anomalies(tx, &scan, 0)).unwrap_err();
        assert_eq!(err.as_store_error().unwrap().http_status(), 500, "{err}");
        assert_eq!(state(&dir.db(), "m"), (None, 0));

        // The same row through OR IGNORE vanishes without an error.
        let ignored = store
            .with_conn("t", |conn| {
                Ok(conn.execute(
                    "INSERT OR IGNORE INTO anomalies (metric, subject, bucket, start_slot, end_slot, kind, value, score,
                                                      detected_at)
                     VALUES ('m', '', 1, 10, 19, 'series', 1.0, ?1, 0)",
                    [f64::NAN],
                )?)
            })
            .unwrap();
        assert_eq!(ignored, 0, "OR IGNORE swallowed the bad row");
    }

    #[test]
    fn the_schema_refuses_what_validation_refuses() {
        let dir = TempDir::new();
        let store = Store::open_path(dir.db()).unwrap();
        let insert = |columns: &str, values: &str| {
            store.with_conn("t", |conn| {
                Ok(conn.execute(
                    &format!(
                        "INSERT INTO anomalies (metric, subject, bucket, start_slot, end_slot, kind, value, score,
                                                detected_at {columns})
                         VALUES ('m', '', 1, 10, 19, 'series', 1.0, 1.0, 0 {values})"
                    ),
                    [],
                )?)
            })
        };
        assert!(insert("", "").is_ok());
        for (columns, values) in [
            (", baseline", ", 9e999"),
            (", baseline, scale", ", 1.0, -1.0"),
            (", scale", ", 1.0"),
            (", detail", ", 'not json'"),
            (", detail", &format!(", '{{\"k\":\"{}\"}}'", "x".repeat(MAX_DETAIL_BYTES))),
        ] {
            let err = insert(columns, values).unwrap_err();
            assert!(err.to_string().contains("CHECK"), "{columns} {}: {err}", &values[..values.len().min(30)]);
        }
    }

    // -- crash at each stage -----------------------------------------------

    const CRASH_DIR_VAR: &str = "ORACLE_STORE_CRASH_DIR";
    const CRASH_STAGE_VAR: &str = "ORACLE_STORE_CRASH_STAGE";

    /// Child half of the crash tests: runs only when re-invoked with
    /// [`CRASH_DIR_VAR`] set. It runs a scan commit's stages in order and
    /// aborts the process at the named stage. No destructor runs, so nothing
    /// rolls back; only SQLite's recovery on the next open decides.
    #[test]
    fn crash_child() {
        let (Some(dir), Ok(stage)) = (std::env::var_os(CRASH_DIR_VAR), std::env::var(CRASH_STAGE_VAR)) else {
            return;
        };
        let store = Store::open_path(Path::new(&dir).join("oracle.db")).unwrap();
        let scan = PreparedScan::new("m", &[anomaly("a", 2), anomaly("b", 2), anomaly("", 2)], 2).unwrap();
        let _ = store.transaction("crash", |tx| {
            let now = unix_now_ms();
            let mode = check_progress(tx, &scan)?;
            let counts = insert_anomalies(tx, &scan, now)?;
            if stage == "after_inserts" {
                std::process::abort();
            }
            let commit = finish_scan(tx, &scan, mode, counts, now)?;
            if stage == "before_commit" {
                std::process::abort();
            }
            Ok(commit)
        });
        if stage == "after_commit" {
            std::process::abort();
        }
    }

    /// Commit bucket 1 (one anomaly), then crash the child at `stage` while
    /// it commits bucket 2 (three anomalies). Returns the state on reopen.
    fn crash_at(stage: &str) -> (Option<i64>, i64) {
        let dir = TempDir::new();
        {
            let store = Store::open_path(dir.db()).unwrap();
            store.commit_scan("m", &[anomaly("", 1)], 1).unwrap();
        }
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "store::tests::crash_child", "--test-threads=1", "--nocapture"])
            .env(CRASH_DIR_VAR, &dir.0)
            .env(CRASH_STAGE_VAR, stage)
            .output()
            .unwrap();
        assert!(!output.status.success(), "the child must have aborted at {stage}: {output:?}");

        // The child's lock died with it; reopening runs WAL recovery.
        let store = Store::open_path(dir.db()).unwrap();
        let reopened = state(&dir.db(), "m");
        // Whatever survived, the store is consistent and usable.
        let integrity: String = store
            .with_conn("t", |conn| Ok(conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?))
            .unwrap();
        assert_eq!(integrity, "ok");
        reopened
    }

    #[test]
    fn a_crash_after_the_inserts_leaves_neither_half() {
        assert_eq!(crash_at("after_inserts"), (Some(1), 1));
    }

    #[test]
    fn a_crash_after_advancing_but_before_commit_leaves_neither_half() {
        assert_eq!(crash_at("before_commit"), (Some(1), 1));
    }

    #[test]
    fn a_crash_right_after_commit_keeps_both_halves() {
        assert_eq!(crash_at("after_commit"), (Some(2), 4));
    }

    #[test]
    fn a_panic_mid_scan_rolls_back_and_the_store_stays_usable() {
        let dir = TempDir::new();
        let store = Store::open_path(dir.db()).unwrap();
        store.commit_scan("m", &[anomaly("", 1)], 1).unwrap();
        let scan = PreparedScan::new("m", &[anomaly("x", 2)], 2).unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = store.transaction("t", |tx| {
                let mode = check_progress(tx, &scan)?;
                let counts = insert_anomalies(tx, &scan, 0)?;
                finish_scan(tx, &scan, mode, counts, 0)?;
                panic!("detector bug");
                #[allow(unreachable_code)]
                Ok(())
            });
        }));
        assert!(result.is_err());
        assert_eq!(state(&dir.db(), "m"), (Some(1), 1));

        // The poisoned mutex is recovered and no transaction is left open.
        let commit = store.commit_scan("m", &[anomaly("x", 2)], 2).unwrap();
        assert_eq!(commit, ScanCommit { inserted: 1, duplicates: 0, progress_advanced: true });
        assert_eq!(state(&dir.db(), "m"), (Some(2), 2));
    }
}
