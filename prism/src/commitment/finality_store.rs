//! `finality.json`: the finalized watermark, kept across restarts.
//!
//! ```text
//! { "version": 1, "chain_id": "solana-mainnet",
//!   "finalized_slot": 384000123, "finalized_blockhash": "…",
//!   "announced_slot": 384000123, "updated_at": 1761040000 }
//! ```
//!
//! Without it a restarted PRISM knows nothing about finality until its first
//! poll: every slot reads as not finalized, so a slot re-ingested right after
//! a restart (to finish an interrupted correction) would be fetched at
//! `confirmed`, and the poller would announce the whole finalized range again
//! from 0. With it, the watermark starts where it left off.
//!
//! * **Monotonic and conservative.** The file only ever lags the chain: a
//!   finalized slot never stops being finalized, so seeding the watermark
//!   from an old file under-reports finality, never over-reports it.
//! * **Atomic.** Replaced like every PRISM state file (temp file, fsync,
//!   rename, directory fsync); a crash leaves the old record or the new one.
//! * **Announced separately.** `announced_slot` is the watermark of the last
//!   `Final` event that was emitted *and* flushed. It may lag
//!   `finalized_slot`; a restarted poller announces from there, so no `Final`
//!   is lost and none is repeated from 0.
//! * **Pinned to a chain.** A record for another `chain_id` is ignored (with
//!   a warning), not trusted.
//!
//! A file that cannot be parsed is quarantined (renamed to
//! `finality.json.corrupt-{unix_time}`) and PRISM starts as if there were
//! none: the safe direction, since an unknown watermark under-reports.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::types::error::{PrismError, PrismResult};

/// Name of the file in the data directory.
pub const FINALITY_FILE_NAME: &str = "finality.json";

/// The format version this build reads and writes.
pub const FINALITY_VERSION: u32 = 1;

/// What `finality.json` holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FinalityRecord {
    pub version: u32,
    /// The chain the watermark belongs to.
    pub chain_id: String,
    /// Highest slot observed at `finalized`.
    pub finalized_slot: u64,
    /// Blockhash of `finalized_slot`.
    pub finalized_blockhash: String,
    /// Watermark of the last `Final` event emitted and flushed. Never above
    /// `finalized_slot`.
    pub announced_slot: u64,
    /// Unix seconds of the save.
    pub updated_at: i64,
}

impl FinalityRecord {
    pub fn new(
        chain_id: impl Into<String>,
        finalized_slot: u64,
        finalized_blockhash: impl Into<String>,
        announced_slot: u64,
    ) -> Self {
        Self {
            version: FINALITY_VERSION,
            chain_id: chain_id.into(),
            finalized_slot,
            finalized_blockhash: finalized_blockhash.into(),
            announced_slot: announced_slot.min(finalized_slot),
            updated_at: chrono::Utc::now().timestamp(),
        }
    }
}

/// Reads and writes `finality.json`.
#[derive(Debug, Clone)]
pub struct FinalityStore {
    path: PathBuf,
}

impl FinalityStore {
    /// The store for the file at `path`.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The store for `{data_dir}/finality.json`.
    pub fn in_dir(data_dir: impl AsRef<Path>) -> Self {
        Self::new(data_dir.as_ref().join(FINALITY_FILE_NAME))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The saved record for `chain_id`. `None` when there is no file, when
    /// it belongs to another chain, or when it was corrupt and has just been
    /// quarantined.
    ///
    /// Fails with [`PrismError::CursorError`] when the file cannot be read or
    /// quarantined, or was written by a newer format version.
    pub fn load(&self, chain_id: &str) -> PrismResult<Option<FinalityRecord>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(self.error(err)),
        };
        let record = match serde_json::from_slice::<FinalityRecord>(&bytes) {
            Ok(record) if record.version > FINALITY_VERSION => {
                return Err(self.error(invalid_data(format!(
                    "written by format version {}; this build reads version {FINALITY_VERSION}",
                    record.version
                ))));
            }
            Ok(record) if record.version == FINALITY_VERSION && record.announced_slot <= record.finalized_slot => {
                record
            }
            Ok(_) | Err(_) => {
                self.quarantine()?;
                return Ok(None);
            }
        };
        if record.chain_id != chain_id {
            warn!(
                path = %self.path.display(),
                recorded = %record.chain_id,
                expected = chain_id,
                "finality record belongs to another chain; ignored"
            );
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// Replace the file with `record`, atomically.
    pub fn save(&self, record: &FinalityRecord) -> PrismResult<()> {
        let json = serde_json::to_vec_pretty(record).map_err(|err| self.error(invalid_data(err)))?;
        crate::storage::write_file_atomically(&self.path, &json).map_err(|err| self.error(err))
    }

    fn quarantine(&self) -> PrismResult<()> {
        let mut name = self.path.as_os_str().to_owned();
        name.push(format!(".corrupt-{}", chrono::Utc::now().timestamp()));
        let target = PathBuf::from(name);
        fs::rename(&self.path, &target).map_err(|err| self.error(err))?;
        warn!(
            path = %self.path.display(),
            quarantined = %target.display(),
            "finality record is corrupt; quarantined, starting without one"
        );
        Ok(())
    }

    fn error(&self, source: io::Error) -> PrismError {
        PrismError::cursor(self.path.display().to_string(), source)
    }
}

fn invalid_data(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
