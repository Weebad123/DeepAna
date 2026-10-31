//! Helpers shared by the config tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use forge::config::*;
use forge::{ForgeConfig, ForgeError};

/// A scratch directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "forge-config-test-{}-{}",
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
pub fn load(vars: &[(&str, &str)]) -> Result<ForgeConfig, ForgeError> {
    let env: HashMap<String, OsString> = vars
        .iter()
        .map(|(key, value)| (key.to_string(), OsString::from(value)))
        .collect();
    ForgeConfig::from_lookup(|name| env.get(name).cloned())
}

/// A scratch directory and the two path variables pointing into it. Every
/// other variable is left to its default unless the test sets it.
pub struct Fixture {
    pub dir: TempDir,
    pub changelog: String,
    pub db: String,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = TempDir::new();
        let changelog = dir.file("changelog.jsonl");
        let db = dir.file("forge.db");
        Self { dir, changelog, db }
    }

    /// Load with the fixture's paths plus `vars`; later entries win.
    pub fn load(&self, vars: &[(&str, &str)]) -> Result<ForgeConfig, ForgeError> {
        let mut all = vec![(ENV_CHANGELOG_PATH, self.changelog.as_str()), (ENV_DB_PATH, self.db.as_str())];
        all.extend_from_slice(vars);
        let env: HashMap<&str, &str> = all.into_iter().collect();
        let env: Vec<(&str, &str)> = env.into_iter().collect();
        load(&env)
    }
}

/// The field of a `ConfigError`, panicking on any other outcome.
pub fn rejected_field(result: Result<ForgeConfig, ForgeError>) -> String {
    match result {
        Err(ForgeError::ConfigError { field, .. }) => field,
        Err(other) => panic!("expected ConfigError, got {other:?}"),
        Ok(config) => panic!("expected ConfigError, got {config:?}"),
    }
}

/// The reason of a `ConfigError` for `field`, panicking on any other outcome.
pub fn rejection_reason(result: Result<ForgeConfig, ForgeError>, field: &str) -> String {
    match result {
        Err(ForgeError::ConfigError { field: got, reason }) if got == field => reason,
        Err(other) => panic!("expected ConfigError on {field}, got {other:?}"),
        Ok(config) => panic!("expected ConfigError on {field}, got {config:?}"),
    }
}
