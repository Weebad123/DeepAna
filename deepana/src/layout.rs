//! Where everything lives: every file any service reads or writes is under
//! one data directory, `--parquet-directory <DIR>`.
//!
//! ```text
//! <DIR>/
//!   solana/              PRISM's Parquet tables (PRISM writes, LENS scans).
//!                        Nothing else is ever written here.
//!   cursor.json          PRISM's cursor
//!   changelog.jsonl      PRISM's change log (FORGE and the dashboard tail it)
//!   skipped_slots.log    PRISM's skip audit log
//!   forge.db             FORGE's registry, plus forge.db.lock, forge.db-wal,
//!                        forge.db-shm and forge.checkpoint.json
//!   oracle.db            ORACLE's store, plus oracle.db.lock, oracle.db-wal,
//!                        oracle.db-shm
//!   commons/             optional, read only: a COMMONS repository
//!                        (artifacts/ + fixtures/) for the verify panel
//! ```
//!
//! Every path is built from the canonical `<DIR>`, so nothing depends on the
//! working directory.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// The per-chain directory PRISM writes its tables under and LENS scans.
pub const CHAIN_DIR: &str = "solana";
pub const CURSOR_FILE: &str = "cursor.json";
pub const CHANGELOG_FILE: &str = "changelog.jsonl";
pub const SKIP_LOG_FILE: &str = "skipped_slots.log";
pub const FORGE_DB_FILE: &str = "forge.db";
pub const ORACLE_DB_FILE: &str = "oracle.db";
pub const COMMONS_DIR: &str = "commons";

/// The data directory and every path under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    root: PathBuf,
}

impl Layout {
    /// Create `dir` if it does not exist and canonicalize it.
    pub fn create(dir: &Path) -> io::Result<Self> {
        if dir.as_os_str().is_empty() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "the data directory must not be empty"));
        }
        fs::create_dir_all(dir)?;
        let root = fs::canonicalize(dir)?;
        if !root.is_dir() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{} is not a directory", root.display())));
        }
        Ok(Self { root })
    }

    /// `<DIR>`, absolute and canonical.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// `<DIR>/solana`.
    pub fn chain_root(&self) -> PathBuf {
        self.root.join(CHAIN_DIR)
    }

    /// `<DIR>/cursor.json`, written by PRISM.
    pub fn cursor(&self) -> PathBuf {
        self.root.join(CURSOR_FILE)
    }

    /// `<DIR>/changelog.jsonl`, written by PRISM.
    pub fn changelog(&self) -> PathBuf {
        self.root.join(CHANGELOG_FILE)
    }

    /// `<DIR>/skipped_slots.log`, written by PRISM.
    pub fn skip_log(&self) -> PathBuf {
        self.root.join(SKIP_LOG_FILE)
    }

    /// `<DIR>/forge.db`.
    pub fn forge_db(&self) -> PathBuf {
        self.root.join(FORGE_DB_FILE)
    }

    /// `<DIR>/oracle.db`.
    pub fn oracle_db(&self) -> PathBuf {
        self.root.join(ORACLE_DB_FILE)
    }

    /// `<DIR>/commons`, the default COMMONS repository. Only ever read.
    pub fn commons_root(&self) -> PathBuf {
        self.root.join(COMMONS_DIR)
    }
}
