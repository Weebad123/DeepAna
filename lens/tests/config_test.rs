//! `LensConfig::load_with` against an in-memory environment and a scratch
//! data directory, plus `LensError` formatting.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use lens::config::*;
use lens::{LensConfig, LensError};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A scratch directory removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-config-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    /// A data dir laid out like PRISM's, with the given table directories.
    fn with_tables(tables: &[&str]) -> Self {
        let dir = Self::new();
        for table in tables {
            fs::create_dir_all(dir.path().join(DEFAULT_CHAIN).join(table)).unwrap();
        }
        dir
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Build a config from exactly the given variables.
fn load(vars: &[(&str, &str)]) -> Result<LensConfig, LensError> {
    let env: HashMap<String, OsString> = vars
        .iter()
        .map(|(key, value)| (key.to_string(), OsString::from(value)))
        .collect();
    LensConfig::load_with(|name| env.get(name).cloned())
}

/// The field of a `ConfigError`, panicking on any other outcome.
fn rejected_field(result: Result<LensConfig, LensError>) -> String {
    match result {
        Err(LensError::ConfigError { field, .. }) => field,
        Err(other) => panic!("expected ConfigError, got {other:?}"),
        Ok(config) => panic!("expected ConfigError, got {config:?}"),
    }
}

fn data_dir_str(dir: &TempDir) -> &str {
    dir.path().to_str().unwrap()
}

// ---------------------------------------------------------------------------
// Defaults and overrides
// ---------------------------------------------------------------------------

#[test]
fn only_data_dir_set_yields_the_defaults() {
    let dir = TempDir::with_tables(&["slots"]);
    let config = load(&[(ENV_DATA_DIR, data_dir_str(&dir))]).unwrap();

    assert_eq!(config.data_dir(), fs::canonicalize(dir.path()).unwrap());
    assert_eq!(config.bind_address(), DEFAULT_BIND_ADDRESS);
    assert_eq!(config.max_query_rows(), 10_000);
    assert_eq!(config.query_timeout_secs(), DEFAULT_QUERY_TIMEOUT_SECS);
    assert_eq!(config.catalog_refresh_interval_secs(), DEFAULT_CATALOG_REFRESH_INTERVAL_SECS);
    assert_eq!(config.log_level(), "info");
    assert_eq!(config.chain(), "solana");
}

#[test]
fn every_variable_overrides_its_default() {
    let dir = TempDir::with_tables(&["transactions"]);
    let config = load(&[
        (ENV_DATA_DIR, data_dir_str(&dir)),
        (ENV_BIND_ADDRESS, "0.0.0.0:9000"),
        (ENV_MAX_QUERY_ROWS, "500"),
        (ENV_QUERY_TIMEOUT_SECS, "5"),
        (ENV_CATALOG_REFRESH_INTERVAL_SECS, "120"),
        (ENV_LOG_LEVEL, "debug"),
        (ENV_CHAIN, "solana"),
    ])
    .unwrap();

    assert_eq!(config.bind_address(), "0.0.0.0:9000");
    assert_eq!(config.socket_addr(), "0.0.0.0:9000".parse::<SocketAddr>().unwrap());
    assert_eq!(config.max_query_rows(), 500);
    assert_eq!(config.query_timeout(), Duration::from_secs(5));
    assert_eq!(config.catalog_refresh_interval(), Duration::from_secs(120));
    assert_eq!(config.log_level(), "debug");
}

#[test]
fn values_are_trimmed_and_case_normalized() {
    let dir = TempDir::with_tables(&["slots"]);
    let config = load(&[
        (ENV_DATA_DIR, data_dir_str(&dir)),
        (ENV_BIND_ADDRESS, " [::1]:8080 "),
        (ENV_MAX_QUERY_ROWS, " 42\n"),
        (ENV_LOG_LEVEL, "WARN"),
        (ENV_CHAIN, "Solana"),
    ])
    .unwrap();

    assert_eq!(config.bind_address(), "[::1]:8080");
    assert_eq!(config.max_query_rows(), 42);
    assert_eq!(config.log_level(), "warn");
    assert_eq!(config.chain(), "solana");
}

#[test]
fn every_log_level_is_accepted() {
    let dir = TempDir::with_tables(&["slots"]);
    for level in LOG_LEVELS {
        let config = load(&[(ENV_DATA_DIR, data_dir_str(&dir)), (ENV_LOG_LEVEL, level)]).unwrap();
        assert_eq!(config.log_level(), level);
    }
}

#[test]
fn numeric_bounds_are_inclusive() {
    let dir = TempDir::with_tables(&["slots"]);
    let max_rows = MAX_QUERY_ROWS_LIMIT.to_string();
    let max_timeout = MAX_QUERY_TIMEOUT_SECS.to_string();
    let max_refresh = MAX_CATALOG_REFRESH_INTERVAL_SECS.to_string();
    for (rows, timeout, refresh) in [("1", "1", "1"), (&*max_rows, &*max_timeout, &*max_refresh)] {
        let config = load(&[
            (ENV_DATA_DIR, data_dir_str(&dir)),
            (ENV_MAX_QUERY_ROWS, rows),
            (ENV_QUERY_TIMEOUT_SECS, timeout),
            (ENV_CATALOG_REFRESH_INTERVAL_SECS, refresh),
        ])
        .unwrap();
        assert_eq!(config.max_query_rows().to_string(), rows);
        assert_eq!(config.query_timeout_secs().to_string(), timeout);
        assert_eq!(config.catalog_refresh_interval_secs().to_string(), refresh);
    }
}

// ---------------------------------------------------------------------------
// data_dir
// ---------------------------------------------------------------------------

#[test]
fn missing_data_dir_is_rejected_and_names_the_variable() {
    let err = load(&[]).unwrap_err();
    assert_eq!(err.config_field(), Some("data_dir"));
    assert!(err.to_string().contains(ENV_DATA_DIR), "{err}");
}

#[test]
fn nonexistent_data_dir_is_rejected() {
    let dir = TempDir::new();
    let missing = dir.path().join("nope");
    let err = load(&[(ENV_DATA_DIR, missing.to_str().unwrap())]).unwrap_err();
    assert_eq!(err.config_field(), Some("data_dir"));
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[test]
fn data_dir_pointing_at_a_file_is_rejected() {
    let dir = TempDir::new();
    let file = dir.path().join("file.parquet");
    fs::write(&file, b"not a dir").unwrap();

    let err = load(&[(ENV_DATA_DIR, file.to_str().unwrap())]).unwrap_err();
    assert_eq!(err.config_field(), Some("data_dir"));
    assert!(err.to_string().contains("not a directory"), "{err}");

    // A path that goes *through* the file is reported as missing, not as I/O.
    let through = file.join("child");
    let err = load(&[(ENV_DATA_DIR, through.to_str().unwrap())]).unwrap_err();
    assert_eq!(err.config_field(), Some("data_dir"), "{err:?}");
}

#[test]
fn data_dir_without_any_table_directory_is_rejected() {
    let dir = TempDir::new();
    let err = load(&[(ENV_DATA_DIR, data_dir_str(&dir))]).unwrap_err();
    assert_eq!(err.config_field(), Some("data_dir"));
    let message = err.to_string();
    for table in TABLE_DIRS {
        assert!(message.contains(table), "{message} should list {table}");
    }
}

#[test]
fn table_directories_must_sit_under_the_chain_directory() {
    // `{data_dir}/slots` is not PRISM's layout; `{data_dir}/solana/slots` is.
    let dir = TempDir::new();
    fs::create_dir_all(dir.path().join("slots")).unwrap();
    assert_eq!(rejected_field(load(&[(ENV_DATA_DIR, data_dir_str(&dir))])), "data_dir");
}

#[test]
fn a_table_entry_that_is_a_file_does_not_count() {
    let dir = TempDir::new();
    fs::create_dir_all(dir.path().join(DEFAULT_CHAIN)).unwrap();
    fs::write(dir.path().join(DEFAULT_CHAIN).join("slots"), b"").unwrap();
    assert_eq!(rejected_field(load(&[(ENV_DATA_DIR, data_dir_str(&dir))])), "data_dir");
}

#[test]
fn any_single_table_directory_is_enough() {
    for table in TABLE_DIRS {
        let dir = TempDir::with_tables(&[table]);
        let config = load(&[(ENV_DATA_DIR, data_dir_str(&dir))])
            .unwrap_or_else(|err| panic!("{table}: {err}"));
        assert_eq!(config.table_dir(table), config.data_dir().join("solana").join(table));
        assert!(config.table_dir(table).is_dir());
    }
}

#[test]
fn non_canonical_data_dir_is_canonicalized() {
    let dir = TempDir::with_tables(&["slots"]);
    // `<dir>/solana/..` is a roundabout spelling of `<dir>`.
    let roundabout = dir.path().join(DEFAULT_CHAIN).join("..");
    let config = load(&[(ENV_DATA_DIR, roundabout.to_str().unwrap())]).unwrap();
    assert_eq!(config.data_dir(), fs::canonicalize(dir.path()).unwrap());
    assert!(config.data_dir().is_absolute());
}

#[cfg(unix)]
#[test]
fn unreadable_data_dir_parent_is_an_io_error_not_a_config_error() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new();
    let locked = dir.path().join("locked");
    let inner = locked.join("data");
    fs::create_dir_all(inner.join(DEFAULT_CHAIN).join("slots")).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();

    let result = load(&[(ENV_DATA_DIR, inner.to_str().unwrap())]);
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

    // Root ignores permission bits, in which case the load simply succeeds.
    if let Err(err) = result {
        match err {
            LensError::IoError { path, source } => {
                assert_eq!(path, inner.display().to_string());
                assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
            }
            other => panic!("expected IoError, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Validation order
// ---------------------------------------------------------------------------

#[test]
fn scalar_fields_are_validated_before_the_filesystem() {
    // data_dir does not exist, but the bad number is what gets reported.
    let err = load(&[
        (ENV_DATA_DIR, "/definitely/not/a/real/lens/dir"),
        (ENV_MAX_QUERY_ROWS, "lots"),
    ])
    .unwrap_err();
    assert_eq!(err.config_field(), Some("max_query_rows"));
}

#[test]
fn unrelated_variables_are_ignored() {
    let dir = TempDir::with_tables(&["slots"]);
    let config = load(&[(ENV_DATA_DIR, data_dir_str(&dir)), ("LENS_UNKNOWN", "x"), ("PATH", "")]);
    assert!(config.is_ok(), "{config:?}");
}

// ---------------------------------------------------------------------------
// Startup logging
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn log_startup_emits_one_info_event_with_every_field() {
    let dir = TempDir::with_tables(&["slots"]);
    let config = load(&[
        (ENV_DATA_DIR, data_dir_str(&dir)),
        (ENV_BIND_ADDRESS, "127.0.0.1:7777"),
        (ENV_MAX_QUERY_ROWS, "1234"),
        (ENV_QUERY_TIMEOUT_SECS, "17"),
        (ENV_CATALOG_REFRESH_INTERVAL_SECS, "45"),
        (ENV_LOG_LEVEL, "trace"),
    ])
    .unwrap();

    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .finish();
    tracing::subscriber::with_default(subscriber, || config.log_startup());

    let output = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.lines().count(), 1, "{output}");
    assert!(output.contains("INFO"), "{output}");
    assert!(output.contains("LENS configuration loaded"), "{output}");
    for expected in [
        format!("data_dir={}", config.data_dir().display()),
        format!("chain_root={}", config.chain_root().display()),
        "bind_address=127.0.0.1:7777".to_owned(),
        "max_query_rows=1234".to_owned(),
        "query_timeout_secs=17".to_owned(),
        "catalog_refresh_interval_secs=45".to_owned(),
        "log_level=trace".to_owned(),
        "chain=solana".to_owned(),
    ] {
        assert!(output.contains(&expected), "missing {expected:?} in {output}");
    }
}

// ---------------------------------------------------------------------------
// LensError
// ---------------------------------------------------------------------------

#[test]
fn error_display_includes_context_and_cause() {
    let cause = || io::Error::other("boom");
    let cases: Vec<(LensError, &str, &str)> = vec![
        (LensError::config("bind_address", "bad"), "config_error", "invalid config field bind_address: bad"),
        (LensError::catalog("slots", cause()), "catalog_error", "catalog failure on table slots: boom"),
        (LensError::query_plan("SELECT", cause()), "query_plan_error", "failed to plan query `SELECT`: boom"),
        (LensError::query_timeout("SELECT 1", 30), "query_timeout", "query `SELECT 1` timed out after 30s"),
        (
            LensError::query_execution("SELECT 1", cause()),
            "query_execution_error",
            "failed to execute query `SELECT 1`: boom",
        ),
        (LensError::serialization("json", cause()), "serialization_error", "failed to serialize result as json: boom"),
        (LensError::io("/data", cause()), "io_error", "I/O failure on /data: boom"),
    ];
    for (err, kind, display) in cases {
        assert_eq!(err.kind(), kind);
        assert_eq!(err.to_string(), display);
    }
}

#[test]
fn error_source_exposes_the_cause() {
    use std::error::Error;

    let err = LensError::io("/data", io::Error::new(io::ErrorKind::NotFound, "gone"));
    let source = err.source().expect("IoError has a source");
    assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::NotFound);

    assert!(LensError::config("f", "r").source().is_none());
    assert!(LensError::query_timeout("q", 1).source().is_none());
}

#[test]
fn lens_error_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<LensError>();
}
