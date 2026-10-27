//! LENS configuration, read once from the environment at startup.
//!
//! Every field comes from a `LENS_*` environment variable. All of them have a
//! default except `LENS_DATA_DIR`, which must point at the same directory
//! PRISM's `ParquetWriter` was given. PRISM lays its tables out under a
//! per-chain directory, so with the default chain LENS reads:
//!
//! ```text
//! {LENS_DATA_DIR}/solana/
//!   slots/  transactions/  instructions/  token_transfers/
//! ```
//!
//! | Variable                             | Field                           | Default          | Valid values                          |
//! |--------------------------------------|---------------------------------|------------------|---------------------------------------|
//! | `LENS_DATA_DIR`                      | `data_dir`                      | required         | existing directory holding PRISM data |
//! | `LENS_BIND_ADDRESS`                  | `bind_address`                  | `127.0.0.1:8080` | `ip:port`, port not 0                 |
//! | `LENS_MAX_QUERY_ROWS`                | `max_query_rows`                | `10000`          | `1..=100000`                          |
//! | `LENS_QUERY_TIMEOUT_SECS`            | `query_timeout_secs`            | `30`             | `1..=300`                             |
//! | `LENS_CATALOG_REFRESH_INTERVAL_SECS` | `catalog_refresh_interval_secs` | `30`             | `1..=3600`                            |
//! | `LENS_LOG_LEVEL`                     | `log_level`                     | `info`           | `trace` `debug` `info` `warn` `error` |
//! | `LENS_CHAIN`                         | `chain`                         | `solana`         | `solana`                              |
//!
//! A variable that is set but empty (or only whitespace) is rejected rather
//! than treated as unset, so a broken deployment template fails loudly
//! instead of silently falling back to a default.
//!
//! [`LensConfig`] has no setters and no public fields: once
//! [`LensConfig::load`] returns, the config is fixed for the life of the
//! process. Share it behind an `Arc`.

use std::ffi::OsString;
use std::fmt::Display;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use crate::error::{LensError, LensResult};

pub const ENV_DATA_DIR: &str = "LENS_DATA_DIR";
pub const ENV_BIND_ADDRESS: &str = "LENS_BIND_ADDRESS";
pub const ENV_MAX_QUERY_ROWS: &str = "LENS_MAX_QUERY_ROWS";
pub const ENV_QUERY_TIMEOUT_SECS: &str = "LENS_QUERY_TIMEOUT_SECS";
pub const ENV_CATALOG_REFRESH_INTERVAL_SECS: &str = "LENS_CATALOG_REFRESH_INTERVAL_SECS";
pub const ENV_LOG_LEVEL: &str = "LENS_LOG_LEVEL";
pub const ENV_CHAIN: &str = "LENS_CHAIN";

/// Every variable [`LensConfig::load`] reads.
pub const ENV_VARS: [&str; 7] = [
    ENV_DATA_DIR,
    ENV_BIND_ADDRESS,
    ENV_MAX_QUERY_ROWS,
    ENV_QUERY_TIMEOUT_SECS,
    ENV_CATALOG_REFRESH_INTERVAL_SECS,
    ENV_LOG_LEVEL,
    ENV_CHAIN,
];

pub const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8080";

pub const DEFAULT_MAX_QUERY_ROWS: usize = 10_000;
/// Upper bound on `max_query_rows`. Results are materialized in memory before
/// they are serialized, so this caps the memory a single query can pin.
pub const MAX_QUERY_ROWS_LIMIT: usize = 100_000;

pub const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 30;
pub const MAX_QUERY_TIMEOUT_SECS: u64 = 300;

pub const DEFAULT_CATALOG_REFRESH_INTERVAL_SECS: u64 = 30;
pub const MAX_CATALOG_REFRESH_INTERVAL_SECS: u64 = 3_600;

pub const DEFAULT_LOG_LEVEL: &str = "info";
/// Accepted values of `log_level`, matched case-insensitively.
pub const LOG_LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];

pub const DEFAULT_CHAIN: &str = "solana";
/// Chains PRISM writes data for. The chain name is also the directory under
/// `data_dir` that holds its tables (PRISM's `CHAIN_DIR`).
pub const SUPPORTED_CHAINS: [&str; 1] = ["solana"];

/// Table directories PRISM's `ParquetWriter` creates under
/// `{data_dir}/{chain}/`. Mirrors `prism::storage::partition::TableKind`.
pub const TABLE_DIRS: [&str; 4] = ["slots", "transactions", "instructions", "token_transfers"];

/// How LENS runs. Built only by [`LensConfig::load`] (or
/// [`LensConfig::load_with`]), so every instance has passed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LensConfig {
    /// PRISM's data directory, canonicalized to an absolute path.
    data_dir: PathBuf,
    /// Address the HTTP API listens on, as `ip:port`.
    bind_address: String,
    /// Most rows LENS returns for any single query.
    max_query_rows: usize,
    /// How long a query may run before it is cancelled.
    query_timeout_secs: u64,
    /// How often the background task re-scans the Parquet directory to pick
    /// up new files written by PRISM.
    catalog_refresh_interval_secs: u64,
    /// Lowercased tracing level.
    log_level: String,
    /// Lowercased chain name; also the table directory under `data_dir`.
    chain: String,
}

impl LensConfig {
    /// Read and validate the config from the process environment.
    ///
    /// The first offending field is reported as a [`LensError::ConfigError`].
    /// A filesystem failure other than "not found" while inspecting
    /// `data_dir` (e.g. permission denied) is reported as a
    /// [`LensError::IoError`].
    pub fn load() -> LensResult<Self> {
        Self::load_with(|name| std::env::var_os(name))
    }

    /// [`Self::load`] with the variables supplied by `lookup` instead of the
    /// process environment. `lookup` returns `None` for an unset variable.
    ///
    /// Lets tests and embedders build a config without mutating global
    /// process state.
    pub fn load_with<F>(lookup: F) -> LensResult<Self>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        // Cheap, pure checks first, so a typo in a number is reported without
        // touching the filesystem. `data_dir` is checked on disk last.
        let raw_data_dir = read_data_dir(&lookup)?;

        let bind_address = read_string(&lookup, ENV_BIND_ADDRESS, "bind_address")?
            .unwrap_or_else(|| DEFAULT_BIND_ADDRESS.to_owned());
        validate_bind_address(&bind_address)?;

        let max_query_rows = read_number(&lookup, ENV_MAX_QUERY_ROWS, "max_query_rows")?
            .unwrap_or(DEFAULT_MAX_QUERY_ROWS);
        check_range("max_query_rows", max_query_rows, 1, MAX_QUERY_ROWS_LIMIT)?;

        let query_timeout_secs =
            read_number(&lookup, ENV_QUERY_TIMEOUT_SECS, "query_timeout_secs")?
                .unwrap_or(DEFAULT_QUERY_TIMEOUT_SECS);
        check_range("query_timeout_secs", query_timeout_secs, 1, MAX_QUERY_TIMEOUT_SECS)?;

        let catalog_refresh_interval_secs = read_number(
            &lookup,
            ENV_CATALOG_REFRESH_INTERVAL_SECS,
            "catalog_refresh_interval_secs",
        )?
        .unwrap_or(DEFAULT_CATALOG_REFRESH_INTERVAL_SECS);
        check_range(
            "catalog_refresh_interval_secs",
            catalog_refresh_interval_secs,
            1,
            MAX_CATALOG_REFRESH_INTERVAL_SECS,
        )?;

        let log_level = read_string(&lookup, ENV_LOG_LEVEL, "log_level")?
            .map_or_else(|| DEFAULT_LOG_LEVEL.to_owned(), |level| level.to_ascii_lowercase());
        if !LOG_LEVELS.contains(&log_level.as_str()) {
            return Err(LensError::config(
                "log_level",
                format!("{log_level:?} is not one of {}", LOG_LEVELS.join(", ")),
            ));
        }

        let chain = read_string(&lookup, ENV_CHAIN, "chain")?
            .map_or_else(|| DEFAULT_CHAIN.to_owned(), |chain| chain.to_ascii_lowercase());
        if !SUPPORTED_CHAINS.contains(&chain.as_str()) {
            return Err(LensError::config(
                "chain",
                format!("{chain:?} is not supported; expected one of {}", SUPPORTED_CHAINS.join(", ")),
            ));
        }

        let data_dir = validate_data_dir(&raw_data_dir, &chain)?;

        Ok(Self {
            data_dir,
            bind_address,
            max_query_rows,
            query_timeout_secs,
            catalog_refresh_interval_secs,
            log_level,
            chain,
        })
    }

    /// PRISM's data directory, absolute and canonical.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// `{data_dir}/{chain}`: the directory holding the table directories.
    pub fn chain_root(&self) -> PathBuf {
        self.data_dir.join(&self.chain)
    }

    /// `{data_dir}/{chain}/{table}`.
    pub fn table_dir(&self, table: &str) -> PathBuf {
        self.chain_root().join(table)
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

    pub const fn max_query_rows(&self) -> usize {
        self.max_query_rows
    }

    pub const fn query_timeout_secs(&self) -> u64 {
        self.query_timeout_secs
    }

    pub const fn query_timeout(&self) -> Duration {
        Duration::from_secs(self.query_timeout_secs)
    }

    pub const fn catalog_refresh_interval_secs(&self) -> u64 {
        self.catalog_refresh_interval_secs
    }

    pub const fn catalog_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.catalog_refresh_interval_secs)
    }

    /// One of [`LOG_LEVELS`].
    pub fn log_level(&self) -> &str {
        &self.log_level
    }

    /// One of [`SUPPORTED_CHAINS`].
    pub fn chain(&self) -> &str {
        &self.chain
    }

    /// Log the effective configuration as one structured `info` event. Meant
    /// to be called once, right after the tracing subscriber is installed.
    pub fn log_startup(&self) {
        tracing::info!(
            data_dir = %self.data_dir.display(),
            chain_root = %self.chain_root().display(),
            bind_address = %self.bind_address,
            max_query_rows = self.max_query_rows,
            query_timeout_secs = self.query_timeout_secs,
            catalog_refresh_interval_secs = self.catalog_refresh_interval_secs,
            log_level = %self.log_level,
            chain = %self.chain,
            "LENS configuration loaded"
        );
    }
}

// ---------------------------------------------------------------------------
// Reading variables
// ---------------------------------------------------------------------------

/// `LENS_DATA_DIR`, which must be set and non-blank. Not checked on disk.
fn read_data_dir<F>(lookup: &F) -> LensResult<PathBuf>
where
    F: Fn(&str) -> Option<OsString>,
{
    let raw = lookup(ENV_DATA_DIR).ok_or_else(|| {
        LensError::config(
            "data_dir",
            format!("{ENV_DATA_DIR} must be set to the directory PRISM writes Parquet files to"),
        )
    })?;
    // A path need not be UTF-8, so only reject it as blank if it is.
    if raw.is_empty() || raw.to_str().is_some_and(|s| s.trim().is_empty()) {
        return Err(LensError::config("data_dir", format!("{ENV_DATA_DIR} is set but empty")));
    }
    Ok(PathBuf::from(raw))
}

/// A trimmed, non-empty UTF-8 value, or `None` when `var` is unset.
fn read_string<F>(lookup: &F, var: &str, field: &str) -> LensResult<Option<String>>
where
    F: Fn(&str) -> Option<OsString>,
{
    let Some(raw) = lookup(var) else {
        return Ok(None);
    };
    let value = raw
        .into_string()
        .map_err(|_| LensError::config(field, format!("{var} is not valid UTF-8")))?;
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(LensError::config(field, format!("{var} is set but empty")));
    }
    Ok(Some(trimmed.to_owned()))
}

/// A value parsed as `T`, or `None` when `var` is unset.
fn read_number<F, T>(lookup: &F, var: &str, field: &str) -> LensResult<Option<T>>
where
    F: Fn(&str) -> Option<OsString>,
    T: FromStr,
    T::Err: Display,
{
    let Some(value) = read_string(lookup, var, field)? else {
        return Ok(None);
    };
    value.parse().map(Some).map_err(|err| {
        LensError::config(field, format!("{var}={value:?} is not a valid unsigned integer: {err}"))
    })
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

fn check_range<T>(field: &str, value: T, min: T, max: T) -> LensResult<()>
where
    T: PartialOrd + Display,
{
    if value < min || value > max {
        return Err(LensError::config(
            field,
            format!("{value} is out of range; must be between {min} and {max}"),
        ));
    }
    Ok(())
}

/// `ip:port` with a non-zero port. Hostnames are not resolved.
fn validate_bind_address(bind_address: &str) -> LensResult<()> {
    let addr: SocketAddr = bind_address.parse().map_err(|err| {
        LensError::config(
            "bind_address",
            format!("{bind_address:?} is not a valid socket address (expected ip:port): {err}"),
        )
    })?;
    if addr.port() == 0 {
        return Err(LensError::config(
            "bind_address",
            format!("{bind_address:?} has port 0; a fixed port is required"),
        ));
    }
    Ok(())
}

/// Check that `raw` is an existing directory with at least one PRISM table
/// directory under `{raw}/{chain}/`, and return it canonicalized.
fn validate_data_dir(raw: &Path, chain: &str) -> LensResult<PathBuf> {
    let shown = raw.display();
    let metadata = match fs::metadata(raw) {
        Ok(metadata) => metadata,
        Err(err) if matches!(err.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
            return Err(LensError::config("data_dir", format!("{shown} does not exist")));
        }
        Err(err) => return Err(LensError::io(shown.to_string(), err)),
    };
    if !metadata.is_dir() {
        return Err(LensError::config("data_dir", format!("{shown} is not a directory")));
    }
    let data_dir = fs::canonicalize(raw).map_err(|err| LensError::io(shown.to_string(), err))?;

    let chain_root = data_dir.join(chain);
    if !TABLE_DIRS.iter().any(|table| chain_root.join(table).is_dir()) {
        return Err(LensError::config(
            "data_dir",
            format!(
                "no PRISM table directory found under {}; expected at least one of {}/ \
                 (is {ENV_DATA_DIR} the same directory PRISM's ParquetWriter writes to?)",
                chain_root.display(),
                TABLE_DIRS.join("/, "),
            ),
        ));
    }
    Ok(data_dir)
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
    fn read_number_reports_the_variable_and_value() {
        let vars = [("N", "12abc")];
        let err = read_number::<_, u64>(&lookup(&vars), "N", "n").unwrap_err();
        let message = err.to_string();
        assert!(message.contains("N=\"12abc\""), "{message}");
        assert_eq!(err.config_field(), Some("n"));
    }

    #[test]
    fn read_number_rejects_negative_and_overflowing_values() {
        for raw in ["-1", "18446744073709551616", "1.5", "1e3", "0x10"] {
            let vars = [("N", raw)];
            assert!(read_number::<_, u64>(&lookup(&vars), "N", "n").is_err(), "{raw}");
        }
    }

    #[test]
    fn check_range_is_inclusive_on_both_ends() {
        assert!(check_range("f", 1, 1, 10).is_ok());
        assert!(check_range("f", 10, 1, 10).is_ok());
        assert!(check_range("f", 0, 1, 10).is_err());
        assert!(check_range("f", 11, 1, 10).is_err());
    }

    #[test]
    fn bind_address_accepts_ipv4_and_ipv6() {
        for ok in ["127.0.0.1:8080", "0.0.0.0:1", "[::1]:8080", "[::]:65535"] {
            assert!(validate_bind_address(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn bind_address_rejects_port_zero_and_unparseable_values() {
        for bad in ["127.0.0.1:0", "localhost:8080", "127.0.0.1", ":8080", "127.0.0.1:65536", "::1:8080"] {
            let err = validate_bind_address(bad).unwrap_err();
            assert_eq!(err.config_field(), Some("bind_address"), "{bad}");
        }
    }

    #[test]
    fn defaults_are_inside_their_own_bounds() {
        assert!(check_range("", DEFAULT_MAX_QUERY_ROWS, 1, MAX_QUERY_ROWS_LIMIT).is_ok());
        assert!(check_range("", DEFAULT_QUERY_TIMEOUT_SECS, 1, MAX_QUERY_TIMEOUT_SECS).is_ok());
        assert!(
            check_range("", DEFAULT_CATALOG_REFRESH_INTERVAL_SECS, 1, MAX_CATALOG_REFRESH_INTERVAL_SECS)
                .is_ok()
        );
        assert!(validate_bind_address(DEFAULT_BIND_ADDRESS).is_ok());
        assert!(LOG_LEVELS.contains(&DEFAULT_LOG_LEVEL));
        assert!(SUPPORTED_CHAINS.contains(&DEFAULT_CHAIN));
    }
}
