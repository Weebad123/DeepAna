//! FORGE configuration and the crate-wide error type.
//!
//! [`ForgeConfig`] is read once from the environment at startup. Every field
//! comes from a `FORGE_*` environment variable. All of them have a default
//! except `FORGE_CHANGELOG_PATH`, which must point at the same file PRISM's
//! change log appends to.
//!
//! | Variable                        | Field                     | Default                 | Valid values                                         |
//! |---------------------------------|---------------------------|-------------------------|------------------------------------------------------|
//! | `FORGE_CHANGELOG_PATH`          | `changelog_path`          | required                | regular file, or a missing file in an existing dir   |
//! | `FORGE_LENS_URL`                | `lens_url`                | `http://127.0.0.1:8080` | `http(s)://host[:port][/path]`, see below            |
//! | `FORGE_DB_PATH`                 | `db_path`                 | `forge.db`              | as `changelog_path`, and not the same file           |
//! | `FORGE_BIND_ADDRESS`            | `bind_address`            | `127.0.0.1:8081`        | `ip:port`, port not 0                                |
//! | `FORGE_POLL_INTERVAL_MS`        | `poll_interval_ms`        | `500`                   | `50..=60000`                                         |
//! | `FORGE_VISIBILITY_TIMEOUT_SECS` | `visibility_timeout_secs` | `120`                   | `5..=3600`, and greater than `webhook_timeout_secs`  |
//! | `FORGE_WEBHOOK_MAX_ATTEMPTS`    | `webhook_max_attempts`    | `5`                     | `1..=20`                                             |
//! | `FORGE_WEBHOOK_TIMEOUT_SECS`    | `webhook_timeout_secs`    | `10`                    | `1..=60`                                             |
//! | `FORGE_LOG_LEVEL`               | `log_level`               | `info`                  | `trace` `debug` `info` `warn` `error`                |
//!
//! `lens_url` must use `http` or `https`, name a host, and carry no
//! credentials, query string or fragment; port 0 is rejected. A trailing `/`
//! is dropped, so request paths can be appended directly. Its default is
//! LENS's own default bind address.
//!
//! `changelog_path` and `db_path` may name files that do not exist yet (PRISM
//! may not have written its first event, and FORGE creates its database), but
//! the parent directory must exist. Both are stored canonicalized.
//!
//! A variable that is set but empty (or only whitespace) is rejected rather
//! than treated as unset, so a broken deployment template fails loudly
//! instead of silently falling back to a default.
//!
//! [`ForgeConfig`] has no setters and no public fields: once
//! [`ForgeConfig::load`] returns, the config is fixed for the life of the
//! process. Share it behind an `Arc`.
//!
//! # Errors
//!
//! Every fallible function in FORGE returns [`ForgeResult`]. Each variant
//! carries enough context (config field, path, URL, delivery id, ...) for a
//! single log line to be self-contained.
//!
//! | Stage               | Variants                         |
//! |---------------------|----------------------------------|
//! | `config.rs`         | [`ConfigError`], [`IoError`]     |
//! | `changelog_reader/` | [`ChangelogError`], [`IoError`]  |
//! | `lens_client/`      | [`LensClientError`]              |
//! | `registry/`         | [`RegistryError`]                |
//! | `webhook/`          | [`WebhookError`]                 |
//!
//! As in PRISM and LENS, there are deliberately no blanket `From`
//! conversions: a bare `?` could not supply the context each variant needs.
//! Use the constructors with `map_err` instead:
//!
//! ```ignore
//! fs::File::open(&path).map_err(|e| ForgeError::io(path.display().to_string(), e))?;
//! ```
//!
//! The `Display` output of a variant includes its underlying cause. The cause
//! is also exposed through [`std::error::Error::source`] for callers that need
//! to downcast it.
//!
//! [`ConfigError`]: ForgeError::ConfigError
//! [`ChangelogError`]: ForgeError::ChangelogError
//! [`LensClientError`]: ForgeError::LensClientError
//! [`RegistryError`]: ForgeError::RegistryError
//! [`WebhookError`]: ForgeError::WebhookError
//! [`IoError`]: ForgeError::IoError

use std::ffi::OsString;
use std::fmt::Display;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf, is_separator};
use std::str::FromStr;
use std::time::Duration;

use thiserror::Error;
use url::Url;

pub const ENV_CHANGELOG_PATH: &str = "FORGE_CHANGELOG_PATH";
pub const ENV_LENS_URL: &str = "FORGE_LENS_URL";
pub const ENV_DB_PATH: &str = "FORGE_DB_PATH";
pub const ENV_BIND_ADDRESS: &str = "FORGE_BIND_ADDRESS";
pub const ENV_POLL_INTERVAL_MS: &str = "FORGE_POLL_INTERVAL_MS";
pub const ENV_VISIBILITY_TIMEOUT_SECS: &str = "FORGE_VISIBILITY_TIMEOUT_SECS";
pub const ENV_WEBHOOK_MAX_ATTEMPTS: &str = "FORGE_WEBHOOK_MAX_ATTEMPTS";
pub const ENV_WEBHOOK_TIMEOUT_SECS: &str = "FORGE_WEBHOOK_TIMEOUT_SECS";
pub const ENV_LOG_LEVEL: &str = "FORGE_LOG_LEVEL";

/// Every variable [`ForgeConfig::load`] reads.
pub const ENV_VARS: [&str; 9] = [
    ENV_CHANGELOG_PATH,
    ENV_LENS_URL,
    ENV_DB_PATH,
    ENV_BIND_ADDRESS,
    ENV_POLL_INTERVAL_MS,
    ENV_VISIBILITY_TIMEOUT_SECS,
    ENV_WEBHOOK_MAX_ATTEMPTS,
    ENV_WEBHOOK_TIMEOUT_SECS,
    ENV_LOG_LEVEL,
];

/// LENS's default bind address (`lens::config::DEFAULT_BIND_ADDRESS`).
pub const DEFAULT_LENS_URL: &str = "http://127.0.0.1:8080";
/// URL schemes `lens_url` may use.
pub const LENS_URL_SCHEMES: [&str; 2] = ["http", "https"];

pub const DEFAULT_DB_PATH: &str = "forge.db";

/// One port above LENS's default, so both run side by side out of the box.
pub const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8081";

pub const DEFAULT_POLL_INTERVAL_MS: u64 = 500;
/// Below this, tailing the change log degenerates into a busy loop.
pub const MIN_POLL_INTERVAL_MS: u64 = 50;
pub const MAX_POLL_INTERVAL_MS: u64 = 60_000;

/// Above [`MAX_WEBHOOK_TIMEOUT_SECS`], so any valid webhook timeout works
/// without also raising this.
pub const DEFAULT_VISIBILITY_TIMEOUT_SECS: u64 = 120;
/// Below this, ordinary scheduling jitter can let a second worker re-claim an
/// outbox row its first claimant is still delivering.
pub const MIN_VISIBILITY_TIMEOUT_SECS: u64 = 5;
pub const MAX_VISIBILITY_TIMEOUT_SECS: u64 = 3_600;

pub const DEFAULT_WEBHOOK_MAX_ATTEMPTS: u32 = 5;
pub const MAX_WEBHOOK_MAX_ATTEMPTS: u32 = 20;

pub const DEFAULT_WEBHOOK_TIMEOUT_SECS: u64 = 10;
pub const MAX_WEBHOOK_TIMEOUT_SECS: u64 = 60;

pub const DEFAULT_LOG_LEVEL: &str = "info";
/// Accepted values of `log_level`, matched case-insensitively.
pub const LOG_LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];

// ---------------------------------------------------------------------------
// ForgeConfig
// ---------------------------------------------------------------------------

/// How FORGE runs. Built only by [`ForgeConfig::load`] (or
/// [`ForgeConfig::from_lookup`]), so every instance has passed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgeConfig {
    /// PRISM's change log file, canonicalized to an absolute path.
    changelog_path: PathBuf,
    /// Base URL of the LENS HTTP API, without a trailing `/`.
    lens_url: String,
    /// FORGE's registry / outbox database file, canonicalized.
    db_path: PathBuf,
    /// Address the HTTP API listens on, as `ip:port`.
    bind_address: String,
    /// How often the change log is polled for new events.
    poll_interval_ms: u64,
    /// How long a claimed outbox row stays invisible to other workers.
    visibility_timeout_secs: u64,
    /// Delivery attempts per webhook, including the first.
    webhook_max_attempts: u32,
    /// How long a single webhook request may take.
    webhook_timeout_secs: u64,
    /// Lowercased tracing level.
    log_level: String,
}

impl ForgeConfig {
    /// Read and validate the config from the process environment.
    ///
    /// The first offending field is reported as a [`ForgeError::ConfigError`].
    /// A filesystem failure other than "not found" while inspecting
    /// `changelog_path` or `db_path` (e.g. permission denied) is reported as
    /// a [`ForgeError::IoError`].
    pub fn load() -> ForgeResult<Self> {
        Self::from_lookup(|name| std::env::var_os(name))
    }

    /// [`Self::load`] with the variables supplied by `lookup` instead of the
    /// process environment. `lookup` returns `None` for an unset variable.
    ///
    /// Lets tests and embedders build a config without mutating global
    /// process state.
    pub fn from_lookup<F>(lookup: F) -> ForgeResult<Self>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        // Cheap, pure checks first, so a typo in a number is reported without
        // touching the filesystem. The two paths are checked on disk last.
        let raw_changelog_path = read_path(&lookup, ENV_CHANGELOG_PATH, "changelog_path")?.ok_or_else(|| {
            ForgeError::config(
                "changelog_path",
                format!("{ENV_CHANGELOG_PATH} must be set to the change log file PRISM appends to"),
            )
        })?;

        let lens_url = match read_string(&lookup, ENV_LENS_URL, "lens_url")? {
            Some(raw) => validate_lens_url(&raw)?,
            None => validate_lens_url(DEFAULT_LENS_URL)?,
        };

        let raw_db_path = read_path(&lookup, ENV_DB_PATH, "db_path")?
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DB_PATH));

        let bind_address = read_string(&lookup, ENV_BIND_ADDRESS, "bind_address")?
            .unwrap_or_else(|| DEFAULT_BIND_ADDRESS.to_owned());
        validate_bind_address(&bind_address)?;

        let poll_interval_ms = read_number(&lookup, ENV_POLL_INTERVAL_MS, "poll_interval_ms")?
            .unwrap_or(DEFAULT_POLL_INTERVAL_MS);
        check_range("poll_interval_ms", poll_interval_ms, MIN_POLL_INTERVAL_MS, MAX_POLL_INTERVAL_MS)?;

        let visibility_timeout_secs =
            read_number(&lookup, ENV_VISIBILITY_TIMEOUT_SECS, "visibility_timeout_secs")?
                .unwrap_or(DEFAULT_VISIBILITY_TIMEOUT_SECS);
        check_range(
            "visibility_timeout_secs",
            visibility_timeout_secs,
            MIN_VISIBILITY_TIMEOUT_SECS,
            MAX_VISIBILITY_TIMEOUT_SECS,
        )?;

        let webhook_max_attempts =
            read_number(&lookup, ENV_WEBHOOK_MAX_ATTEMPTS, "webhook_max_attempts")?
                .unwrap_or(DEFAULT_WEBHOOK_MAX_ATTEMPTS);
        check_range("webhook_max_attempts", webhook_max_attempts, 1, MAX_WEBHOOK_MAX_ATTEMPTS)?;

        let webhook_timeout_secs =
            read_number(&lookup, ENV_WEBHOOK_TIMEOUT_SECS, "webhook_timeout_secs")?
                .unwrap_or(DEFAULT_WEBHOOK_TIMEOUT_SECS);
        check_range("webhook_timeout_secs", webhook_timeout_secs, 1, MAX_WEBHOOK_TIMEOUT_SECS)?;

        // A claim that expires while its delivery is still in flight lets a
        // second worker send the same webhook again.
        if visibility_timeout_secs <= webhook_timeout_secs {
            return Err(ForgeError::config(
                "visibility_timeout_secs",
                format!(
                    "{visibility_timeout_secs} must be greater than webhook_timeout_secs \
                     ({webhook_timeout_secs}), or a delivery still in flight can be claimed again"
                ),
            ));
        }

        let log_level = read_string(&lookup, ENV_LOG_LEVEL, "log_level")?
            .map_or_else(|| DEFAULT_LOG_LEVEL.to_owned(), |level| level.to_ascii_lowercase());
        if !LOG_LEVELS.contains(&log_level.as_str()) {
            return Err(ForgeError::config(
                "log_level",
                format!("{log_level:?} is not one of {}", LOG_LEVELS.join(", ")),
            ));
        }

        let changelog_path = validate_file_path("changelog_path", &raw_changelog_path)?;
        let db_path = validate_file_path("db_path", &raw_db_path)?;
        if db_path == changelog_path {
            return Err(ForgeError::config(
                "db_path",
                format!("{} is the same file as changelog_path", db_path.display()),
            ));
        }

        Ok(Self {
            changelog_path,
            lens_url,
            db_path,
            bind_address,
            poll_interval_ms,
            visibility_timeout_secs,
            webhook_max_attempts,
            webhook_timeout_secs,
            log_level,
        })
    }

    /// PRISM's change log file, absolute and canonical. May not exist yet.
    pub fn changelog_path(&self) -> &Path {
        &self.changelog_path
    }

    /// Base URL of the LENS API, without a trailing `/`.
    pub fn lens_url(&self) -> &str {
        &self.lens_url
    }

    /// FORGE's database file, absolute and canonical. May not exist yet.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// The listen address as configured.
    pub fn bind_address(&self) -> &str {
        &self.bind_address
    }

    /// The listen address, parsed.
    pub fn socket_addr(&self) -> SocketAddr {
        self.bind_address
            .parse()
            .expect("bind_address is validated when the config is loaded")
    }

    pub const fn poll_interval_ms(&self) -> u64 {
        self.poll_interval_ms
    }

    pub const fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub const fn visibility_timeout_secs(&self) -> u64 {
        self.visibility_timeout_secs
    }

    pub const fn visibility_timeout(&self) -> Duration {
        Duration::from_secs(self.visibility_timeout_secs)
    }

    pub const fn webhook_max_attempts(&self) -> u32 {
        self.webhook_max_attempts
    }

    pub const fn webhook_timeout_secs(&self) -> u64 {
        self.webhook_timeout_secs
    }

    pub const fn webhook_timeout(&self) -> Duration {
        Duration::from_secs(self.webhook_timeout_secs)
    }

    /// One of [`LOG_LEVELS`].
    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    /// Log the effective configuration as one structured `info` event. Meant
    /// to be called once, right after the tracing subscriber is installed.
    ///
    /// Every field is safe to log: `lens_url` is rejected at load time if it
    /// embeds credentials.
    pub fn display(&self) {
        tracing::info!(
            changelog_path = %self.changelog_path.display(),
            lens_url = %self.lens_url,
            db_path = %self.db_path.display(),
            bind_address = %self.bind_address,
            poll_interval_ms = self.poll_interval_ms,
            visibility_timeout_secs = self.visibility_timeout_secs,
            webhook_max_attempts = self.webhook_max_attempts,
            webhook_timeout_secs = self.webhook_timeout_secs,
            log_level = %self.log_level,
            "FORGE configuration loaded"
        );
    }
}

// ---------------------------------------------------------------------------
// Reading variables
// ---------------------------------------------------------------------------

/// A non-blank path, or `None` when `var` is unset. Not checked on disk.
fn read_path<F>(lookup: &F, var: &str, field: &str) -> ForgeResult<Option<PathBuf>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw) = lookup(var) else {
        return Ok(None);
    };
    // A path need not be UTF-8, so only reject it as blank if it is.
    if raw.is_empty() || raw.to_str().is_some_and(|s| s.trim().is_empty()) {
        return Err(ForgeError::config(field, format!("{var} is set but empty")));
    }
    Ok(Some(PathBuf::from(raw)))
}

/// A trimmed, non-empty UTF-8 value, or `None` when `var` is unset.
fn read_string<F>(lookup: &F, var: &str, field: &str) -> ForgeResult<Option<String>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw) = lookup(var) else {
        return Ok(None);
    };
    let value = raw
        .into_string()
        .map_err(|_| ForgeError::config(field, format!("{var} is not valid UTF-8")))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ForgeError::config(field, format!("{var} is set but empty")));
    }
    Ok(Some(trimmed.to_owned()))
}

/// A value parsed as `T`, or `None` when `var` is unset.
fn read_number<F, T>(lookup: &F, var: &str, field: &str) -> ForgeResult<Option<T>>
where
    F: Fn(&str) -> Option<OsString>,
    T: FromStr,
    T::Err: Display,
{
    let Some(value) = read_string(lookup, var, field)? else {
        return Ok(None);
    };
    value.parse().map(Some).map_err(|err| {
        ForgeError::config(field, format!("{var}={value:?} is not a valid unsigned integer: {err}"))
    })
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn check_range<T>(field: &str, value: T, min: T, max: T) -> ForgeResult<()>
where
    T: PartialOrd + Display,
{
    if value < min || value > max {
        return Err(ForgeError::config(
            field,
            format!("{value} is out of range; must be between {min} and {max}"),
        ));
    }
    Ok(())
}

fn validate_bind_address(bind_address: &str) -> ForgeResult<()> {
    let addr: SocketAddr = bind_address.parse().map_err(|err| {
        ForgeError::config(
            "bind_address",
            format!("{bind_address:?} is not a valid socket address (expected ip:port): {err}"),
        )
    })?;
    if addr.port() == 0 {
        return Err(ForgeError::config(
            "bind_address",
            format!("{bind_address:?} has port 0; a fixed port is required"),
        ));
    }
    Ok(())
}

/// Check that `raw` is a usable LENS base URL and return it without a
/// trailing `/`.
fn validate_lens_url(raw: &str) -> ForgeResult<String> {
    let invalid = |reason: String| ForgeError::config("lens_url", reason);

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
fn validate_file_path(field: &str, raw: &Path) -> ForgeResult<PathBuf> {
    let shown = raw.display();
    // `Path` normalizes a trailing separator away (`logs/` has file name
    // `logs`), so look at the raw bytes: a trailing separator means a
    // directory was meant.
    let ends_with_separator = raw
        .as_os_str()
        .as_encoded_bytes()
        .last()
        .is_some_and(|&byte| byte.is_ascii() && is_separator(byte as char));
    let Some(file_name) = raw.file_name().filter(|_| !ends_with_separator) else {
        return Err(ForgeError::config(field, format!("{shown} does not name a file")));
    };

    match fs::metadata(raw) {
        Ok(metadata) if metadata.is_dir() => {
            Err(ForgeError::config(field, format!("{shown} is a directory, not a file")))
        }
        Ok(metadata) if !metadata.is_file() => {
            Err(ForgeError::config(field, format!("{shown} is not a regular file")))
        }
        Ok(_) => fs::canonicalize(raw).map_err(|err| ForgeError::io(shown.to_string(), err)),
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
            // `forge.db` has an empty parent: the working directory.
            let parent = match raw.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => parent,
                _ => Path::new("."),
            };
            let parent_shown = parent.display();
            match fs::metadata(parent) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => {
                    return Err(ForgeError::config(
                        field,
                        format!("parent of {shown} ({parent_shown}) is not a directory"),
                    ));
                }
                Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
                    return Err(ForgeError::config(
                        field,
                        format!("parent directory of {shown} ({parent_shown}) does not exist"),
                    ));
                }
                Err(err) => return Err(ForgeError::io(parent_shown.to_string(), err)),
            }
            let parent = fs::canonicalize(parent).map_err(|err| ForgeError::io(parent_shown.to_string(), err))?;
            Ok(parent.join(file_name))
        }
        Err(err) => Err(ForgeError::io(shown.to_string(), err)),
    }
}

// ---------------------------------------------------------------------------
// ForgeError
// ---------------------------------------------------------------------------

/// Boxed cause for variants whose underlying error type is not fixed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Result alias used across FORGE.
pub type ForgeResult<T> = Result<T, ForgeError>;

/// Every way FORGE can fail.
#[derive(Debug, Error)]
pub enum ForgeError {
    /// The configuration is invalid.
    #[error("invalid config field {field}: {reason}")]
    ConfigError {
        /// Name of the offending config field.
        field: String,
        /// Why it is invalid.
        reason: String,
    },

    /// The change log could not be read or held a malformed entry.
    #[error("change log {path} is unusable: {reason}")]
    ChangelogError {
        /// Filesystem path of the change log.
        path: String,
        /// Why: `"malformed event at line 12"`, `"truncated record"`, ...
        reason: String,
    },

    /// A request to LENS failed (transport error, non-success status,
    /// unreadable response).
    #[error("LENS request to {url} failed: {source}")]
    LensClientError {
        /// The URL that was requested.
        url: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A registry (saved queries, alerts, outbox) operation failed.
    #[error("registry operation {operation} failed: {source}")]
    RegistryError {
        /// What was being done: `"insert alert"`, `"claim outbox row"`, ...
        operation: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A webhook delivery attempt failed.
    #[error("webhook delivery {delivery_id} failed: {source}")]
    WebhookError {
        /// Id of the outbox delivery.
        delivery_id: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A filesystem operation failed.
    #[error("I/O failure on {path}: {source}")]
    IoError {
        /// Filesystem path the operation was about.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
}

impl ForgeError {
    /// Build a [`ForgeError::ConfigError`].
    pub fn config(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConfigError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Build a [`ForgeError::ChangelogError`].
    pub fn changelog(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ChangelogError {
            path: path.into(),
            reason: reason.into(),
        }
    }

    /// Build a [`ForgeError::LensClientError`].
    pub fn lens_client(url: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::LensClientError {
            url: url.into(),
            source: source.into(),
        }
    }

    /// Build a [`ForgeError::RegistryError`].
    pub fn registry(operation: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::RegistryError {
            operation: operation.into(),
            source: source.into(),
        }
    }

    /// Build a [`ForgeError::WebhookError`].
    pub fn webhook(delivery_id: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::WebhookError {
            delivery_id: delivery_id.into(),
            source: source.into(),
        }
    }

    /// Build a [`ForgeError::IoError`].
    pub fn io(path: impl Into<String>, source: std::io::Error) -> Self {
        Self::IoError {
            path: path.into(),
            source,
        }
    }

    /// Stable `snake_case` name of the variant, for logs and metric labels.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ConfigError { .. } => "config_error",
            Self::ChangelogError { .. } => "changelog_error",
            Self::LensClientError { .. } => "lens_client_error",
            Self::RegistryError { .. } => "registry_error",
            Self::WebhookError { .. } => "webhook_error",
            Self::IoError { .. } => "io_error",
        }
    }

    /// The config field this error is about, for [`ForgeError::ConfigError`].
    pub fn config_field(&self) -> Option<&str> {
        match self {
            Self::ConfigError { field, .. } => Some(field),
            _ => None,
        }
    }
}

#[cfg(test)]
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
        // u32 overflows one past its own maximum, not u64's.
        let vars = [("N", "4294967296")];
        assert!(read_number::<_, u32>(&lookup(&vars), "N", "n").is_err());
        let vars = [("N", "4294967295")];
        assert_eq!(read_number::<_, u32>(&lookup(&vars), "N", "n").unwrap(), Some(u32::MAX));
    }

    #[test]
    fn check_range_is_inclusive_on_both_ends() {
        assert!(check_range("f", 1, 1, 10).is_ok());
        assert!(check_range("f", 10, 1, 10).is_ok());
        assert!(check_range("f", 0, 1, 10).is_err());
        assert!(check_range("f", 11, 1, 10).is_err());
    }

    #[test]
    fn check_range_names_the_bounds() {
        let err = check_range("f", 11u64, 1, 10).unwrap_err();
        assert_eq!(err.to_string(), "invalid config field f: 11 is out of range; must be between 1 and 10");
    }

    #[test]
    fn bind_address_accepts_ipv4_and_ipv6() {
        for ok in ["127.0.0.1:8081", "0.0.0.0:1", "[::1]:8081", "[::]:65535"] {
            assert!(validate_bind_address(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn bind_address_rejects_port_zero_and_unparseable_values() {
        for bad in ["127.0.0.1:0", "[::]:0", "localhost:8081", "127.0.0.1", ":8081", "127.0.0.1:65536", "::1:8081"] {
            let err = validate_bind_address(bad).unwrap_err();
            assert_eq!(err.config_field(), Some("bind_address"), "{bad}");
        }
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
    fn lens_url_port_bounds() {
        assert!(validate_lens_url("http://lens:0").is_err());
        assert_eq!(validate_lens_url("http://lens:1").unwrap(), "http://lens:1");
        assert_eq!(validate_lens_url("http://lens:65535").unwrap(), "http://lens:65535");
        assert!(validate_lens_url("http://lens:65536").is_err());
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
        for bad in ["/", "logs/", "./", ".."] {
            let err = validate_file_path("f", Path::new(bad)).unwrap_err();
            assert!(err.to_string().contains("does not name a file"), "{bad}: {err}");
        }
    }

    #[test]
    fn defaults_are_inside_their_own_bounds() {
        assert!(validate_lens_url(DEFAULT_LENS_URL).is_ok());
        assert!(validate_bind_address(DEFAULT_BIND_ADDRESS).is_ok());
        assert!(check_range("", DEFAULT_POLL_INTERVAL_MS, MIN_POLL_INTERVAL_MS, MAX_POLL_INTERVAL_MS).is_ok());
        assert!(
            check_range(
                "",
                DEFAULT_VISIBILITY_TIMEOUT_SECS,
                MIN_VISIBILITY_TIMEOUT_SECS,
                MAX_VISIBILITY_TIMEOUT_SECS
            )
            .is_ok()
        );
        assert!(check_range("", DEFAULT_WEBHOOK_MAX_ATTEMPTS, 1, MAX_WEBHOOK_MAX_ATTEMPTS).is_ok());
        assert!(check_range("", DEFAULT_WEBHOOK_TIMEOUT_SECS, 1, MAX_WEBHOOK_TIMEOUT_SECS).is_ok());
        assert!(DEFAULT_VISIBILITY_TIMEOUT_SECS > MAX_WEBHOOK_TIMEOUT_SECS);
        assert!(LOG_LEVELS.contains(&DEFAULT_LOG_LEVEL));
    }

    #[test]
    fn every_visibility_timeout_in_range_has_a_valid_webhook_timeout() {
        // Otherwise the bottom of the visibility range would be unreachable.
        assert!(MIN_VISIBILITY_TIMEOUT_SECS > 1);
        // And the top of the webhook range must stay reachable too.
        assert!(MAX_VISIBILITY_TIMEOUT_SECS > MAX_WEBHOOK_TIMEOUT_SECS);
    }
}
