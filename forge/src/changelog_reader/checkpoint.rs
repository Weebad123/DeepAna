//! [`Checkpoint`] and [`CheckpointStore`]: where FORGE resumes reading PRISM's
//! change log after a restart or a crash.
//!
//! A checkpoint names the last envelope FORGE has fully processed, by id and
//! by the byte offset just after its line, together with a fingerprint of
//! the file's first line. On startup the reader checks all three against the
//! file before trusting them (see [`super::ChangelogReader::open`]).
//!
//! [`CheckpointStore::save`] replaces the file atomically:
//!
//! 1. serialize the checkpoint to JSON,
//! 2. write it to `forge.checkpoint.json.tmp`,
//! 3. `sync_all` the temp file,
//! 4. rename it over `forge.checkpoint.json`,
//! 5. `sync_all` the parent directory, so the rename itself is durable.
//!
//! A crash at any step leaves either the old checkpoint or the new one,
//! never a mix. A temp file left behind by a crash is ignored by
//! [`CheckpointStore::load`] and overwritten by the next save.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::config::{ForgeConfig, ForgeError, ForgeResult};

/// Name of the checkpoint file [`CheckpointStore::for_config`] uses, next to
/// FORGE's database.
pub const CHECKPOINT_FILE_NAME: &str = "forge.checkpoint.json";

/// Suffix of the temp file a save writes before renaming it into place.
pub const CHECKPOINT_TEMP_SUFFIX: &str = ".tmp";

/// How far FORGE has durably processed the change log.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Id of the last fully processed envelope; 0 before the first.
    pub last_id: u64,
    /// Offset just after that envelope's trailing `'\n'`; 0 before the first.
    pub byte_offset: u64,
    /// SHA-256 (lowercase hex) of the change log's first line, without its
    /// `'\n'`. Empty while `byte_offset` is 0.
    pub first_line_fingerprint: String,
    /// Highest slot of a `Final` event at or before `byte_offset`; 0 if none.
    /// Kept across a change log reset: finality never moves backwards.
    pub highest_final_slot: u64,
    /// Unix seconds of the save.
    pub updated_at: i64,
}

impl Checkpoint {
    /// Whether this checkpoint points anywhere into a file.
    pub const fn is_at_start(&self) -> bool {
        self.byte_offset == 0
    }

    /// Why this checkpoint cannot be right, if it cannot.
    fn validate(&self) -> Result<(), String> {
        if (self.last_id == 0) != (self.byte_offset == 0) {
            return Err(format!(
                "last_id {} and byte_offset {} must both be 0 or both be non-zero",
                self.last_id, self.byte_offset
            ));
        }
        if self.byte_offset > 0 && !is_sha256_hex(&self.first_line_fingerprint) {
            return Err(format!(
                "first_line_fingerprint {:?} is not a SHA-256 hex digest",
                self.first_line_fingerprint
            ));
        }
        Ok(())
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Unix seconds now.
pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// Loads and atomically saves a [`Checkpoint`] at one path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointStore {
    path: PathBuf,
}

impl CheckpointStore {
    /// A store for the checkpoint file at `path`. Its directory must exist.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// `forge.checkpoint.json` in the directory of [`ForgeConfig::db_path`].
    pub fn for_config(config: &ForgeConfig) -> Self {
        let dir = config.db_path().parent().unwrap_or(Path::new("."));
        Self::new(dir.join(CHECKPOINT_FILE_NAME))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Where [`Self::save`] writes before renaming.
    pub fn temp_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(CHECKPOINT_TEMP_SUFFIX);
        PathBuf::from(name)
    }

    /// The saved checkpoint.
    ///
    /// * no file: `Ok(None)`, FORGE has never committed anything;
    /// * a valid file: `Ok(Some(..))`;
    /// * a file that does not parse, or names an impossible position:
    ///   [`ForgeError::ChangelogError`] on the checkpoint path. FORGE does
    ///   not guess where to resume; an operator must look.
    ///
    /// Any other read failure is a [`ForgeError::IoError`].
    pub fn load(&self) -> ForgeResult<Option<Checkpoint>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(ForgeError::io(self.shown(), err)),
        };
        let checkpoint: Checkpoint = serde_json::from_slice(&bytes)
            .map_err(|err| ForgeError::changelog(self.shown(), format!("malformed checkpoint: {err}")))?;
        checkpoint
            .validate()
            .map_err(|reason| ForgeError::changelog(self.shown(), format!("invalid checkpoint: {reason}")))?;
        Ok(Some(checkpoint))
    }

    /// Durably replace the saved checkpoint with `checkpoint`. On error the
    /// previous checkpoint is still the one [`Self::load`] returns.
    pub fn save(&self, checkpoint: &Checkpoint) -> ForgeResult<()> {
        let json = serde_json::to_vec_pretty(checkpoint).expect("Checkpoint is always JSON-serialisable");
        let temp = self.temp_path();
        let temp_shown = temp.display().to_string();

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&temp)
            .map_err(|err| ForgeError::io(&temp_shown, err))?;
        file.write_all(&json).map_err(|err| ForgeError::io(&temp_shown, err))?;
        file.sync_all().map_err(|err| ForgeError::io(&temp_shown, err))?;
        drop(file);

        fs::rename(&temp, &self.path).map_err(|err| ForgeError::io(self.shown(), err))?;
        sync_dir(self.path.parent()).map_err(|err| ForgeError::io(self.shown(), err))
    }

    fn shown(&self) -> String {
        self.path.display().to_string()
    }
}

/// `fsync` the directory holding the checkpoint, making the rename durable.
#[cfg(unix)]
fn sync_dir(dir: Option<&Path>) -> io::Result<()> {
    let dir = match dir {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    File::open(dir)?.sync_all()
}

/// Directories cannot be opened for syncing here; the rename is as durable
/// as the platform makes it.
#[cfg(not(unix))]
fn sync_dir(_dir: Option<&Path>) -> io::Result<()> {
    Ok(())
}
