//! ORACLE configuration and the crate-wide error type.
//!
//! [`OracleConfig`] is read once, when the host starts ORACLE. Every field
//! comes from an `ORACLE_*` environment variable. All of them have a default
//! except `ORACLE_LENS_URL`: LENS is ORACLE's only data source, and there is
//! no address it could safely guess for it.
//!
//! | Variable                         | Field                    | Default     | Valid values                                      |
//! |----------------------------------|--------------------------|-------------|---------------------------------------------------|
//! | `ORACLE_LENS_URL`                | `lens_url`               | required    | `http(s)://host[:port][/path]`, see below         |
//! | `ORACLE_DB_PATH`                 | `db_path`                | `oracle.db` | regular file, or a missing file in an existing dir |
//! | `ORACLE_LENS_TIMEOUT_SECS`       | `lens_timeout_secs`      | `35`        | `1..=330`                                         |
//! | `ORACLE_MAX_ROWS`                | `max_rows`               | `1000`      | `1..=10000`                                       |
//! | `ORACLE_MAX_SLOT_SPAN`           | `max_slot_span`          | `216000`    | `150..=6480000`                                   |
//! | `ORACLE_DETECT_INTERVAL_SECS`    | `detect_interval_secs`   | `60`        | `10..=3600`, and at most `bucket_secs`            |
//! | `ORACLE_BUCKET_SECS`             | `bucket_secs`            | `300`       | `60..=86400`                                      |
//! | `ORACLE_BASELINE_BUCKETS`        | `baseline_buckets`       | `288`       | `5..=2016`                                        |
//! | `ORACLE_ANOMALY_THRESHOLD`       | `anomaly_threshold`      | `3.5`       | finite decimal, `1.0..=20.0`                      |
//!
//! `lens_url` must use `http` or `https`, name a host, and carry no
//! credentials, query string or fragment; port 0 is rejected. A trailing `/`
//! is dropped, so request paths can be appended directly.
//!
//! `lens_timeout_secs` bounds one HTTP request to LENS. Its default sits just
//! above LENS's default query timeout (30 s) and its maximum just above
//! LENS's maximum (300 s), so a slow query is reported by LENS's own 504,
//! which says what happened, rather than by ORACLE giving up first.
//!
//! `max_rows` caps the rows a compiled spec asks for. Its maximum is LENS's
//! default `max_query_rows`, so on a default LENS a spec is never silently
//! truncated. `max_slot_span` caps how many slots a spec's window may cover;
//! the default is about one day at 400 ms per slot, the bounds about one
//! minute and thirty days.
//!
//! Anomaly detection runs every `detect_interval_secs`, sorts activity into
//! buckets of `bucket_secs`, and scores the newest bucket against the median
//! and MAD of the `baseline_buckets` before it. A score above
//! `anomaly_threshold` (a modified z-score; 3.5 is the usual cut-off) is
//! flagged. The interval may not exceed the bucket size, or whole buckets
//! would close between two runs without ever being scored. At least five
//! baseline buckets are required for the median and MAD to mean anything.
//!
//! `db_path` may name a file that does not exist yet (ORACLE creates its
//! database), but the parent directory must exist. It is stored
//! canonicalized, so the host changing its working directory later does not
//! move the database.
//!
//! A variable that is unset takes its default. A variable that is set but
//! empty (or only whitespace) is rejected rather than treated as unset, so a
//! broken deployment template fails loudly instead of silently falling back
//! to a default.
//!
//! [`OracleConfig`] has no setters and no public fields: once
//! [`OracleConfig::load`] returns, the config is fixed for the life of the
//! process. Share it behind an `Arc`.
//!
//! # Errors
//!
//! Every fallible function in ORACLE returns [`OracleResult`]. Each variant
//! carries enough context (config field, URL, operation, metric, ...) for a
//! single log line to be self-contained.
//!
//! | Stage                 | Variants          |
//! |-----------------------|-------------------|
//! | `config.rs`           | [`ConfigError`]   |
//! | LENS requests         | [`LensError`]     |
//! | `store/`              | [`StoreError`]    |
//! | `spec/`, `intent/`    | [`SpecError`]     |
//! | `detect/`             | [`DetectError`]   |
//! | filesystem access     | [`IoError`]       |
//!
//! Every config failure is a [`ConfigError`], including a `db_path` whose
//! directory cannot be inspected (e.g. permission denied): the operator's
//! fix is the same, change the variable or the directory.
//!
//! As in PRISM, LENS and FORGE, there are deliberately no blanket `From`
//! conversions: a bare `?` could not supply the context each variant needs.
//! Use the constructors with `map_err` instead:
//!
//! ```ignore
//! conn.execute(sql, params).map_err(|e| OracleError::store("insert baseline", e))?;
//! ```
//!
//! The `Display` output of a variant includes its underlying cause. The cause
//! is also exposed through [`std::error::Error::source`] for callers that need
//! to downcast it.
//!
//! [`ConfigError`]: OracleError::ConfigError
//! [`LensError`]: OracleError::LensError
//! [`StoreError`]: OracleError::StoreError
//! [`SpecError`]: OracleError::SpecError
//! [`DetectError`]: OracleError::DetectError
//! [`IoError`]: OracleError::IoError

use std::ffi::OsString;
use std::fmt::Display;
use std::fs;
use std::io;
use std::path::{Path, PathBuf, is_separator};
use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;
use url::Url;

pub const ENV_LENS_URL: &str = "ORACLE_LENS_URL";
pub const ENV_DB_PATH: &str = "ORACLE_DB_PATH";
pub const ENV_LENS_TIMEOUT_SECS: &str = "ORACLE_LENS_TIMEOUT_SECS";
pub const ENV_MAX_ROWS: &str = "ORACLE_MAX_ROWS";
pub const ENV_MAX_SLOT_SPAN: &str = "ORACLE_MAX_SLOT_SPAN";
pub const ENV_DETECT_INTERVAL_SECS: &str = "ORACLE_DETECT_INTERVAL_SECS";
pub const ENV_BUCKET_SECS: &str = "ORACLE_BUCKET_SECS";
pub const ENV_BASELINE_BUCKETS: &str = "ORACLE_BASELINE_BUCKETS";
pub const ENV_ANOMALY_THRESHOLD: &str = "ORACLE_ANOMALY_THRESHOLD";

/// Every variable [`OracleConfig::load`] reads.
pub const ENV_VARS: [&str; 9] = [
    ENV_LENS_URL,
    ENV_DB_PATH,
    ENV_LENS_TIMEOUT_SECS,
    ENV_MAX_ROWS,
    ENV_MAX_SLOT_SPAN,
    ENV_DETECT_INTERVAL_SECS,
    ENV_BUCKET_SECS,
    ENV_BASELINE_BUCKETS,
    ENV_ANOMALY_THRESHOLD,
];

/// URL schemes `lens_url` may use.
pub const LENS_URL_SCHEMES: [&str; 2] = ["http", "https"];

pub const DEFAULT_DB_PATH: &str = "oracle.db";

/// Above LENS's default `query_timeout_secs` (30).
pub const DEFAULT_LENS_TIMEOUT_SECS: u64 = 35;
pub const MIN_LENS_TIMEOUT_SECS: u64 = 1;
/// Above LENS's maximum `query_timeout_secs` (300).
pub const MAX_LENS_TIMEOUT_SECS: u64 = 330;

pub const DEFAULT_MAX_ROWS: u32 = 1_000;
pub const MIN_MAX_ROWS: u32 = 1;
/// LENS's default `max_query_rows`.
pub const MAX_MAX_ROWS: u32 = 10_000;

/// About one day of slots at 400 ms each.
pub const DEFAULT_MAX_SLOT_SPAN: u64 = 216_000;
/// About one minute of slots.
pub const MIN_MAX_SLOT_SPAN: u64 = 150;
/// About thirty days of slots.
pub const MAX_MAX_SLOT_SPAN: u64 = 6_480_000;

pub const DEFAULT_DETECT_INTERVAL_SECS: u64 = 60;
/// Below this, detection passes would spend most of their time querying LENS.
pub const MIN_DETECT_INTERVAL_SECS: u64 = 10;
pub const MAX_DETECT_INTERVAL_SECS: u64 = 3_600;

pub const DEFAULT_BUCKET_SECS: u64 = 300;
/// At least the minimum detection interval, so every valid bucket size has a
/// valid interval that scores each bucket.
pub const MIN_BUCKET_SECS: u64 = 60;
pub const MAX_BUCKET_SECS: u64 = 86_400;

/// One day of default-sized (5 minute) buckets.
pub const DEFAULT_BASELINE_BUCKETS: u32 = 288;
/// Fewer points than this give a median/MAD dominated by noise.
pub const MIN_BASELINE_BUCKETS: u32 = 5;
/// One week of default-sized buckets.
pub const MAX_BASELINE_BUCKETS: u32 = 2_016;

/// The usual modified z-score cut-off (Iglewicz and Hoaglin).
pub const DEFAULT_ANOMALY_THRESHOLD: f64 = 3.5;
/// Below this, ordinary variation is flagged.
pub const MIN_ANOMALY_THRESHOLD: f64 = 1.0;
pub const MAX_ANOMALY_THRESHOLD: f64 = 20.0;

// ---------------------------------------------------------------------------
// OracleConfig
// ---------------------------------------------------------------------------

/// How ORACLE runs. Built only by [`OracleConfig::load`] (or
/// [`OracleConfig::from_lookup`]), so every instance has passed validation.
///
/// Not `Eq`: `anomaly_threshold` is an `f64`. It is always finite, so
/// `PartialEq` behaves as a full equivalence here.
#[derive(Debug, Clone, PartialEq)]
pub struct OracleConfig {
    /// Base URL of the LENS HTTP API, without a trailing `/`.
    lens_url: String,
    /// ORACLE's own database file, canonicalized.
    db_path: PathBuf,
    /// How long one request to LENS may take.
    lens_timeout_secs: u64,
    /// Most rows a compiled spec may ask LENS for.
    max_rows: u32,
    /// Most slots a spec's window may cover.
    max_slot_span: u64,
    /// How often the detectors run.
    detect_interval_secs: u64,
    /// Width of one activity bucket.
    bucket_secs: u64,
    /// Buckets in the rolling baseline.
    baseline_buckets: u32,
    /// Modified z-score above which a bucket is anomalous.
    anomaly_threshold: f64,
}

impl OracleConfig {
    /// Read and validate the config from the process environment.
    ///
    /// The first offending field is reported as an
    /// [`OracleError::ConfigError`].
    pub fn load() -> OracleResult<Self> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    /// [`Self::load`] with the variables supplied by `lookup` instead of the
    /// process environment. `lookup` returns `None` for an unset variable.
    ///
    /// Lets tests and hosts build a config without mutating global process
    /// state.
    pub fn from_lookup<F>(lookup: F) -> OracleResult<Self>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        // Cheap, pure checks first, so a typo in a number is reported without
        // touching the filesystem. The database path is checked on disk last.
        let lens_url = read_string(&lookup, ENV_LENS_URL, "lens_url")?.ok_or_else(|| {
            OracleError::config("lens_url", format!("{ENV_LENS_URL} must be set to the base URL of LENS"))
        })?;
        let lens_url = validate_lens_url(&lens_url)?;

        let raw_db_path =
            read_path(&lookup, ENV_DB_PATH, "db_path")?.unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let lens_timeout_secs = read_number(&lookup, ENV_LENS_TIMEOUT_SECS, "lens_timeout_secs")?
            .unwrap_or(DEFAULT_LENS_TIMEOUT_SECS);
        check_range("lens_timeout_secs", lens_timeout_secs, MIN_LENS_TIMEOUT_SECS, MAX_LENS_TIMEOUT_SECS)?;

        let max_rows = read_number(&lookup, ENV_MAX_ROWS, "max_rows")?.unwrap_or(DEFAULT_MAX_ROWS);
        check_range("max_rows", max_rows, MIN_MAX_ROWS, MAX_MAX_ROWS)?;

        let max_slot_span =
            read_number(&lookup, ENV_MAX_SLOT_SPAN, "max_slot_span")?.unwrap_or(DEFAULT_MAX_SLOT_SPAN);
        check_range("max_slot_span", max_slot_span, MIN_MAX_SLOT_SPAN, MAX_MAX_SLOT_SPAN)?;

        let detect_interval_secs = read_number(&lookup, ENV_DETECT_INTERVAL_SECS, "detect_interval_secs")?
            .unwrap_or(DEFAULT_DETECT_INTERVAL_SECS);
        check_range(
            "detect_interval_secs",
            detect_interval_secs,
            MIN_DETECT_INTERVAL_SECS,
            MAX_DETECT_INTERVAL_SECS,
        )?;

        let bucket_secs = read_number(&lookup, ENV_BUCKET_SECS, "bucket_secs")?.unwrap_or(DEFAULT_BUCKET_SECS);
        check_range("bucket_secs", bucket_secs, MIN_BUCKET_SECS, MAX_BUCKET_SECS)?;

        // A bucket that opens and closes between two runs is never scored.
        if detect_interval_secs > bucket_secs {
            return Err(OracleError::config(
                "detect_interval_secs",
                format!(
                    "{detect_interval_secs} must not exceed bucket_secs ({bucket_secs}), \
                     or buckets would close between runs without being scored"
                ),
            ));
        }

        let baseline_buckets = read_number(&lookup, ENV_BASELINE_BUCKETS, "baseline_buckets")?
            .unwrap_or(DEFAULT_BASELINE_BUCKETS);
        check_range("baseline_buckets", baseline_buckets, MIN_BASELINE_BUCKETS, MAX_BASELINE_BUCKETS)?;

        let anomaly_threshold = read_float(&lookup, ENV_ANOMALY_THRESHOLD, "anomaly_threshold")?
            .unwrap_or(DEFAULT_ANOMALY_THRESHOLD);
        check_range("anomaly_threshold", anomaly_threshold, MIN_ANOMALY_THRESHOLD, MAX_ANOMALY_THRESHOLD)?;

        let db_path = validate_file_path("db_path", &raw_db_path)?;

        Ok(Self {
            lens_url,
            db_path,
            lens_timeout_secs,
            max_rows,
            max_slot_span,
            detect_interval_secs,
            bucket_secs,
            baseline_buckets,
            anomaly_threshold,
        })
    }

    /// Base URL of the LENS API, without a trailing `/`.
    pub fn lens_url(&self) -> &str {
        &self.lens_url
    }

    /// ORACLE's database file, absolute and canonical. May not exist yet.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub const fn lens_timeout_secs(&self) -> u64 {
        self.lens_timeout_secs
    }

    pub const fn lens_timeout(&self) -> Duration {
        Duration::from_secs(self.lens_timeout_secs)
    }

    pub const fn max_rows(&self) -> u32 {
        self.max_rows
    }

    pub const fn max_slot_span(&self) -> u64 {
        self.max_slot_span
    }

    pub const fn detect_interval_secs(&self) -> u64 {
        self.detect_interval_secs
    }

    pub const fn detect_interval(&self) -> Duration {
        Duration::from_secs(self.detect_interval_secs)
    }

    pub const fn bucket_secs(&self) -> u64 {
        self.bucket_secs
    }

    pub const fn bucket(&self) -> Duration {
        Duration::from_secs(self.bucket_secs)
    }

    pub const fn baseline_buckets(&self) -> u32 {
        self.baseline_buckets
    }

    /// How far back the rolling baseline reaches: `baseline_buckets` buckets.
    pub const fn baseline_span(&self) -> Duration {
        // At most 2016 * 86400 seconds; cannot overflow.
        Duration::from_secs(self.bucket_secs * self.baseline_buckets as u64)
    }

    /// Always finite and within its bounds.
    pub const fn anomaly_threshold(&self) -> f64 {
        self.anomaly_threshold
    }

    /// Log the effective configuration as one structured `info` event. Meant
    /// to be called once, when the host starts ORACLE.
    ///
    /// Every field is safe to log: `lens_url` is rejected at load time if it
    /// embeds credentials.
    pub fn display(&self) {
        tracing::info!(
            lens_url = %self.lens_url,
            db_path = %self.db_path.display(),
            lens_timeout_secs = self.lens_timeout_secs,
            max_rows = self.max_rows,
            max_slot_span = self.max_slot_span,
            detect_interval_secs = self.detect_interval_secs,
            bucket_secs = self.bucket_secs,
            baseline_buckets = self.baseline_buckets,
            anomaly_threshold = self.anomaly_threshold,
            "ORACLE configuration loaded"
        );
    }
}

// ---------------------------------------------------------------------------
// Reading variables
// ---------------------------------------------------------------------------

/// A non-blank path, or `None` when `var` is unset. Not checked on disk.
fn read_path<F>(lookup: &F, var: &str, field: &str) -> OracleResult<Option<PathBuf>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw) = lookup(var) else {
        return Ok(None);
    };
    // A path need not be UTF-8, so only reject it as blank if it is.
    if raw.is_empty() || raw.to_str().is_some_and(|s| s.trim().is_empty()) {
        return Err(OracleError::config(field, format!("{var} is set but empty")));
    }
    Ok(Some(PathBuf::from(raw)))
}

/// A trimmed, non-empty UTF-8 value, or `None` when `var` is unset.
fn read_string<F>(lookup: &F, var: &str, field: &str) -> OracleResult<Option<String>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw) = lookup(var) else {
        return Ok(None);
    };
    let value = raw
        .into_string()
        .map_err(|_| OracleError::config(field, format!("{var} is not valid UTF-8")))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(OracleError::config(field, format!("{var} is set but empty")));
    }
    Ok(Some(trimmed.to_owned()))
}

/// A value parsed as the unsigned integer `T`, or `None` when `var` is unset.
fn read_number<F, T>(lookup: &F, var: &str, field: &str) -> OracleResult<Option<T>>
where
    F: Fn(&str) -> Option<OsString>,
    T: FromStr,
    T::Err: Display,
{
    let Some(value) = read_string(lookup, var, field)? else {
        return Ok(None);
    };
    value.parse().map(Some).map_err(|err| {
        OracleError::config(field, format!("{var}={value:?} is not a valid unsigned integer: {err}"))
    })
}

/// A finite `f64`, or `None` when `var` is unset.
///
/// `NaN` and the infinities parse as `f64` but are rejected: `NaN` compares
/// false against both bounds, so a range check alone would let it through.
fn read_float<F>(lookup: &F, var: &str, field: &str) -> OracleResult<Option<f64>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(value) = read_string(lookup, var, field)? else {
        return Ok(None);
    };
    let parsed: f64 = value
        .parse()
        .map_err(|err| OracleError::config(field, format!("{var}={value:?} is not a valid number: {err}")))?;
    if !parsed.is_finite() {
        return Err(OracleError::config(field, format!("{var}={value:?} is not a finite number")));
    }
    Ok(Some(parsed))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Inclusive on both ends. Callers must reject `NaN` first.
fn check_range<T>(field: &str, value: T, min: T, max: T) -> OracleResult<()>
where
    T: PartialOrd + Display,
{
    if value < min || value > max {
        return Err(OracleError::config(
            field,
            format!("{value} is out of range; must be between {min} and {max}"),
        ));
    }
    Ok(())
}

/// Check that `raw` is a usable LENS base URL and return it without a
/// trailing `/`.
fn validate_lens_url(raw: &str) -> OracleResult<String> {
    let invalid = |reason: String| OracleError::config("lens_url", reason);

    let url = Url::parse(raw).map_err(|err| invalid(format!("{raw:?} is not a valid URL: {err}")))?;
    // Checked first, and without echoing the value, so the password never
    // reaches a log line.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(invalid("must not embed credentials".to_owned()));
    }
    if !LENS_URL_SCHEMES.contains(&url.scheme()) {
        return Err(invalid(format!(
            "{raw:?} has scheme {:?}; expected one of {}",
            url.scheme(),
            LENS_URL_SCHEMES.join(", ")
        )));
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err(invalid(format!("{raw:?} has no host")));
    }
    if url.port() == Some(0) {
        return Err(invalid(format!("{raw:?} has port 0")));
    }
    if url.query().is_some() {
        return Err(invalid(format!("{raw:?} must not have a query string")));
    }
    if url.fragment().is_some() {
        return Err(invalid(format!("{raw:?} must not have a fragment")));
    }
    Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// Check that `raw` names a regular file, or a file that does not exist yet
/// in an existing directory, and return it canonicalized.
///
/// A filesystem failure other than "not found" (e.g. permission denied) is
/// also a [`OracleError::ConfigError`], with the I/O error in its reason.
fn validate_file_path(field: &str, raw: &Path) -> OracleResult<PathBuf> {
    let shown = raw.display();
    let inaccessible =
        |path: &dyn Display, err: io::Error| OracleError::config(field, format!("cannot inspect {path}: {err}"));

    // `Path` normalizes a trailing separator away (`data/` has file name
    // `data`), so look at the raw bytes: a trailing separator means a
    // directory was meant.
    let ends_with_separator = raw
        .as_os_str()
        .as_encoded_bytes()
        .last()
        .is_some_and(|&byte| byte.is_ascii() && is_separator(byte as char));
    let Some(file_name) = raw.file_name().filter(|_| !ends_with_separator) else {
        return Err(OracleError::config(field, format!("{shown} does not name a file")));
    };

    match fs::metadata(raw) {
        Ok(metadata) if metadata.is_dir() => {
            Err(OracleError::config(field, format!("{shown} is a directory, not a file")))
        }
        Ok(metadata) if !metadata.is_file() => {
            Err(OracleError::config(field, format!("{shown} is not a regular file")))
        }
        Ok(_) => fs::canonicalize(raw).map_err(|err| inaccessible(&shown, err)),
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
            // `oracle.db` has an empty parent: the working directory.
            let parent = match raw.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent,
                _ => Path::new("."),
            };
            let parent_shown = parent.display();
            match fs::metadata(parent) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(OracleError::config(
                        field,
                        format!("parent of {shown} ({parent_shown}) is not a directory"),
                    ));
                }
                Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
                    return Err(OracleError::config(
                        field,
                        format!("parent directory of {shown} ({parent_shown}) does not exist"),
                    ));
                }
                Err(err) => return Err(inaccessible(&parent_shown, err)),
            }
            let parent = fs::canonicalize(parent).map_err(|err| inaccessible(&parent_shown, err))?;
            Ok(parent.join(file_name))
        }
        Err(err) => Err(inaccessible(&shown, err)),
    }
}

// ---------------------------------------------------------------------------
// OracleError
// ---------------------------------------------------------------------------

/// Boxed cause for variants whose underlying error type is not fixed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Result alias used across ORACLE.
pub type OracleResult<T> = Result<T, OracleError>;

/// Every way ORACLE can fail.
#[derive(Debug, Error)]
pub enum OracleError {
    /// The configuration is invalid.
    #[error("invalid config field {field}: {reason}")]
    ConfigError {
        /// Name of the offending config field.
        field: String,
        /// Why it is invalid.
        reason: String,
    },

    /// A request to LENS failed (transport error, non-success status,
    /// unreadable response).
    #[error("LENS request to {url} failed: {source}")]
    LensError {
        /// The URL that was requested.
        url: String,
        /// The lens-client error. Boxed so this type does not pin the
        /// client's version; downcast through [`std::error::Error::source`].
        source: BoxError,
    },

    /// An `oracle.db` operation failed.
    #[error("store operation {operation} failed: {source}")]
    StoreError {
        /// What was being done: `"insert baseline"`, `"load first-seen mints"`, ...
        operation: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A query spec (or a question matched to one) was rejected.
    #[error("invalid spec field {field}: {reason}")]
    SpecError {
        /// The offending part of the spec: `"measure"`, `"window"`, ...
        field: String,
        /// Why it was rejected.
        reason: String,
    },

    /// A detector could not compute its metric.
    #[error("detector for {metric} failed: {source}")]
    DetectError {
        /// The metric being scored: `"transfers_per_mint"`, `"failure_rate"`, ...
        metric: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A filesystem operation failed.
    #[error("I/O failure on {path}: {source}")]
    IoError {
        /// Filesystem path the operation was about.
        path: String,
        /// The underlying I/O error.
        source: io::Error,
    },
}

impl OracleError {
    /// Build an [`OracleError::ConfigError`].
    pub fn config(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConfigError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Build an [`OracleError::LensError`].
    pub fn lens(url: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::LensError {
            url: url.into(),
            source: source.into(),
        }
    }

    /// Build an [`OracleError::StoreError`].
    pub fn store(operation: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::StoreError {
            operation: operation.into(),
            source: source.into(),
        }
    }

    /// Build an [`OracleError::SpecError`].
    pub fn spec(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::SpecError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Build an [`OracleError::DetectError`].
    pub fn detect(metric: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::DetectError {
            metric: metric.into(),
            source: source.into(),
        }
    }

    /// Build an [`OracleError::IoError`].
    pub fn io(path: impl Into<String>, source: io::Error) -> Self {
        Self::IoError {
            path: path.into(),
            source,
        }
    }

    /// Stable `snake_case` name of the variant, for logs and metric labels.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ConfigError { .. } => "config_error",
            Self::LensError { .. } => "lens_error",
            Self::StoreError { .. } => "store_error",
            Self::SpecError { .. } => "spec_error",
            Self::DetectError { .. } => "detect_error",
            Self::IoError { .. } => "io_error",
        }
    }

    /// The config field this error is about, for [`OracleError::ConfigError`].
    pub fn config_field(&self) -> Option<&str> {
        match self {
            Self::ConfigError { field, .. } => Some(field),
            _ => None,
        }
    }
}

#[cfg(test)]
// Some tests pin invariants between constants; asserting on constants is the point.
#[allow(clippy::assertions_on_constants)]
mod tests {
    use super::*;

    fn lookup<'a>(vars: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    #[test]
    fn read_string_trims_and_treats_unset_as_none() {
        let vars = [("A", "  value  ")];
        assert_eq!(read_string(&lookup(&vars), "A", "a").unwrap().as_deref(), Some("value"));
        assert_eq!(read_string(&lookup(&vars), "B", "b").unwrap(), None);
    }

    #[test]
    fn read_string_rejects_blank_values() {
        for blank in ["", " ", "\t\n"] {
            let vars = [("A", blank)];
            let err = read_string(&lookup(&vars), "A", "a").unwrap_err();
            assert_eq!(err.config_field(), Some("a"), "{blank:?}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn read_string_rejects_non_utf8() {
        use std::os::unix::ffi::OsStringExt;
        let bad = |_: &str| Some(OsString::from_vec(vec![0x66, 0xff, 0x6f]));
        let err = read_string(&bad, "A", "a").unwrap_err();
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");
    }

    #[test]
    fn read_path_rejects_blank_but_keeps_inner_whitespace() {
        for blank in ["", " ", "\t"] {
            let vars = [("P", blank)];
            assert!(read_path(&lookup(&vars), "P", "p").is_err(), "{blank:?}");
        }
        let vars = [("P", "a dir/file")];
        assert_eq!(read_path(&lookup(&vars), "P", "p").unwrap(), Some(PathBuf::from("a dir/file")));
        assert_eq!(read_path(&lookup(&vars), "Q", "q").unwrap(), None);
    }

    #[cfg(unix)]
    #[test]
    fn read_path_accepts_non_utf8() {
        use std::os::unix::ffi::OsStringExt;
        let raw = vec![b'/', b't', 0xff];
        let bad = |_: &str| Some(OsString::from_vec(raw.clone()));
        assert_eq!(read_path(&bad, "P", "p").unwrap(), Some(PathBuf::from(OsString::from_vec(raw.clone()))));
    }

    #[test]
    fn read_number_reports_the_variable_and_value() {
        let vars = [("N", "12abc")];
        let err = read_number::<_, u64>(&lookup(&vars), "N", "n").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("N=\"12abc\""), "{message}");
        assert_eq!(err.config_field(), Some("n"));
    }

    #[test]
    fn read_number_rejects_negative_fractional_and_overflowing_values() {
        for raw in ["-1", "18446744073709551616", "1.5", "1e3", "0x10", "+-1"] {
            let vars = [("N", raw)];
            assert!(read_number::<_, u64>(&lookup(&vars), "N", "n").is_err(), "{raw}");
        }
        let vars = [("N", "4294967296")];
        assert!(read_number::<_, u32>(&lookup(&vars), "N", "n").is_err());
        let vars = [("N", "4294967295")];
        assert_eq!(read_number::<_, u32>(&lookup(&vars), "N", "n").unwrap(), Some(u32::MAX));
    }

    #[test]
    fn read_float_accepts_decimal_forms() {
        for (raw, expected) in [("3.5", 3.5), ("3", 3.0), ("+3.5", 3.5), (".5", 0.5), ("5.", 5.0), ("1e1", 10.0)] {
            let vars = [("F", raw)];
            assert_eq!(read_float(&lookup(&vars), "F", "f").unwrap(), Some(expected), "{raw}");
        }
        assert_eq!(read_float(&lookup(&[]), "F", "f").unwrap(), None);
    }

    #[test]
    fn read_float_rejects_non_finite_and_malformed_values() {
        for raw in ["NaN", "nan", "inf", "-inf", "+infinity", "Infinity", "1e309"] {
            let vars = [("F", raw)];
            let err = read_float(&lookup(&vars), "F", "f").unwrap_err();
            assert!(err.to_string().contains("not a finite number"), "{raw}: {err}");
        }
        for raw in ["abc", "3,5", "1_0", "0x1", "3.5.1", "--1"] {
            let vars = [("F", raw)];
            let err = read_float(&lookup(&vars), "F", "f").unwrap_err();
            assert!(err.to_string().contains("not a valid number"), "{raw}: {err}");
        }
    }

    #[test]
    fn check_range_is_inclusive_on_both_ends() {
        assert!(check_range("f", 1, 1, 10).is_ok());
        assert!(check_range("f", 10, 1, 10).is_ok());
        assert!(check_range("f", 0, 1, 10).is_err());
        assert!(check_range("f", 11, 1, 10).is_err());
        assert!(check_range("f", 1.0, 1.0, 20.0).is_ok());
        assert!(check_range("f", 20.0, 1.0, 20.0).is_ok());
        assert!(check_range("f", 0.999, 1.0, 20.0).is_err());
        assert!(check_range("f", 20.001, 1.0, 20.0).is_err());
    }

    #[test]
    fn check_range_names_the_bounds() {
        let err = check_range("f", 11u64, 1, 10).unwrap_err();
        assert_eq!(err.to_string(), "invalid config field f: 11 is out of range; must be between 1 and 10");
    }

    #[test]
    fn lens_url_is_normalized_without_a_trailing_slash() {
        for (raw, expected) in [
            ("http://127.0.0.1:8080", "http://127.0.0.1:8080"),
            ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
            ("HTTPS://Lens.Example.com/api/", "https://lens.example.com/api"),
            ("http://[::1]:8080", "http://[::1]:8080"),
            // The scheme's default port is dropped.
            ("http://lens:80/", "http://lens"),
        ] {
            assert_eq!(validate_lens_url(raw).unwrap(), expected, "{raw}");
        }
    }

    #[test]
    fn lens_url_rejects_unusable_values() {
        for bad in [
            "127.0.0.1:8080",
            "lens",
            "ftp://lens",
            "ws://lens:8080",
            "file:///tmp/lens",
            "http://",
            "http://lens:8080/?a=1",
            "http://lens:8080/?",
            "http://lens:8080/#top",
            "http://lens:8080#",
            "http://user@lens",
            "http://user:pw@lens",
            "http://:pw@lens",
        ] {
            let err = validate_lens_url(bad).unwrap_err();
            assert_eq!(err.config_field(), Some("lens_url"), "{bad}");
        }
    }

    #[test]
    fn lens_url_errors_never_echo_credentials() {
        let err = validate_lens_url("ftp://admin:hunter2@lens").unwrap_err();
        assert!(!err.to_string().contains("hunter2"), "{err}");
    }

    #[test]
    fn file_path_with_a_trailing_separator_does_not_name_a_file() {
        for bad in ["/", "data/", "./", ".."] {
            let err = validate_file_path("f", Path::new(bad)).unwrap_err();
            assert!(err.to_string().contains("does not name a file"), "{bad}: {err}");
        }
    }

    #[test]
    fn defaults_are_inside_their_own_bounds() {
        assert!(check_range("", DEFAULT_LENS_TIMEOUT_SECS, MIN_LENS_TIMEOUT_SECS, MAX_LENS_TIMEOUT_SECS).is_ok());
        assert!(check_range("", DEFAULT_MAX_ROWS, MIN_MAX_ROWS, MAX_MAX_ROWS).is_ok());
        assert!(check_range("", DEFAULT_MAX_SLOT_SPAN, MIN_MAX_SLOT_SPAN, MAX_MAX_SLOT_SPAN).is_ok());
        assert!(
            check_range("", DEFAULT_DETECT_INTERVAL_SECS, MIN_DETECT_INTERVAL_SECS, MAX_DETECT_INTERVAL_SECS).is_ok()
        );
        assert!(check_range("", DEFAULT_BUCKET_SECS, MIN_BUCKET_SECS, MAX_BUCKET_SECS).is_ok());
        assert!(check_range("", DEFAULT_BASELINE_BUCKETS, MIN_BASELINE_BUCKETS, MAX_BASELINE_BUCKETS).is_ok());
        assert!(check_range("", DEFAULT_ANOMALY_THRESHOLD, MIN_ANOMALY_THRESHOLD, MAX_ANOMALY_THRESHOLD).is_ok());
        assert!(DEFAULT_DETECT_INTERVAL_SECS <= DEFAULT_BUCKET_SECS);
    }

    #[test]
    fn every_bucket_size_in_range_has_a_valid_detect_interval() {
        // Otherwise the bottom of the bucket range would be unreachable.
        assert!(MIN_DETECT_INTERVAL_SECS <= MIN_BUCKET_SECS);
    }

    #[test]
    fn the_longest_baseline_span_does_not_overflow() {
        assert!(MAX_BUCKET_SECS.checked_mul(u64::from(MAX_BASELINE_BUCKETS)).is_some());
    }
}
