//! Helpers shared by the integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use oracle::config::*;
use oracle::{OracleConfig, OracleError};

/// A LENS URL that passes validation, for tests about other fields.
pub const LENS_URL: &str = "http://127.0.0.1:8080";

/// A scratch directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "oracle-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    /// `name` under this directory, as a `&str` for an env value.
    pub fn file(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_owned()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Build a config from exactly the given variables.
pub fn load(vars: &[(&str, &str)]) -> Result<OracleConfig, OracleError> {
    let env: HashMap<String, OsString> = vars
        .iter()
        .map(|(key, value)| (key.to_string(), OsString::from(value)))
        .collect();
    OracleConfig::from_lookup(|name| env.get(name).cloned())
}

/// A scratch directory, a database path inside it and a valid LENS URL.
/// Every other variable is left to its default unless the test sets it.
pub struct Fixture {
    pub dir: TempDir,
    pub db: String,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = TempDir::new();
        let db = dir.file("oracle.db");
        Self { dir, db }
    }

    /// Load with the fixture's LENS URL and database path plus `vars`; later
    /// entries win.
    pub fn load(&self, vars: &[(&str, &str)]) -> Result<OracleConfig, OracleError> {
        let mut all = vec![(ENV_LENS_URL, LENS_URL), (ENV_DB_PATH, self.db.as_str())];
        all.extend_from_slice(vars);
        let env: HashMap<&str, &str> = all.into_iter().collect();
        let env: Vec<(&str, &str)> = env.into_iter().collect();
        load(&env)
    }
}

/// The field of a `ConfigError`, panicking on any other outcome.
pub fn rejected_field(result: Result<OracleConfig, OracleError>) -> String {
    match result {
        Err(OracleError::ConfigError { field, .. }) => field,
        Err(other) => panic!("expected ConfigError, got {other:?}"),
        Ok(config) => panic!("expected ConfigError, got {config:?}"),
    }
}

/// The reason of a `ConfigError` for `field`, panicking on any other outcome.
pub fn rejection_reason(result: Result<OracleConfig, OracleError>, field: &str) -> String {
    match result {
        Err(OracleError::ConfigError { field: got, reason }) if got == field => reason,
        Err(other) => panic!("expected ConfigError on {field}, got {other:?}"),
        Ok(config) => panic!("expected ConfigError on {field}, got {config:?}"),
    }
}
