//! The ingestion cursor: where PRISM resumes from after a crash or restart.
//!
//! The ingestor buffers decoded rows in memory and flushes them to Parquet in
//! batches, so at any moment there is a window of slots that have been
//! processed but are not yet on disk:
//!
//! ```text
//!   ... ──────────────┬───────────────────────────┬──────────▶ slots
//!                last_flush_slot          last_processed_slot
//!          on disk    │   in memory (lost on crash)  │
//! ```
//!
//! [`IngestorCursor`] tracks both edges. Only `last_flush_slot` is a safe
//! resume point: everything above it lives in a buffer that a crash throws
//! away. [`IngestorCursor::resume_slot`] therefore restarts one slot past the
//! last flush, and the slots between the two edges are fetched again.
//!
//! The state is a small JSON file, replaced atomically on every
//! [`IngestorCursor::persist`]: written to a sibling temp file, fsynced, then
//! renamed over the old one. A crash mid-write leaves the previous state in
//! place, never a torn file.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};

use crate::types::error::{PrismError, PrismResult};

/// What the cursor file holds.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CursorState {
    /// Last slot handed to the writer's in-memory buffer.
    pub last_processed_slot: u64,
    /// Blockhash of `last_processed_slot`. Empty before the first slot.
    pub last_processed_blockhash: String,
    /// Last slot whose data landed on disk. Never above
    /// `last_processed_slot`.
    pub last_flush_slot: u64,
    /// Unix timestamp (seconds) of the last state change.
    pub updated_at: i64,
}

impl CursorState {
    /// Whether no slot has been processed yet.
    pub fn is_fresh(&self) -> bool {
        self.last_processed_slot == 0 && self.last_flush_slot == 0
    }

    /// Slots processed but not yet flushed: the window a crash would lose.
    pub fn unflushed_slots(&self) -> u64 {
        self.last_processed_slot.saturating_sub(self.last_flush_slot)
    }

    /// Move the processed edge to `slot` if it is ahead of it.
    ///
    /// The first slot of a fresh cursor also sets the flush edge to the slot
    /// before it: nothing below the starting point is wanted, so a crash
    /// before the first flush resumes at the start instead of at slot 1.
    fn advance_processed(&mut self, slot: u64) -> bool {
        if self.is_fresh() {
            self.last_flush_slot = slot.saturating_sub(1);
        } else if slot <= self.last_processed_slot {
            return false;
        }
        self.last_processed_slot = slot;
        true
    }

    fn validate(&self) -> Result<(), String> {
        if self.last_flush_slot > self.last_processed_slot {
            return Err(format!(
                "last_flush_slot {} is above last_processed_slot {}",
                self.last_flush_slot, self.last_processed_slot
            ));
        }
        Ok(())
    }
}

/// The persisted resume point of the ingestor.
///
/// Cheap to clone: clones share one state, so the fetch, process and flush
/// tasks all see the same cursor. Updates are in memory until
/// [`Self::persist`] (which [`Self::record_flush`] calls for you).
#[derive(Debug, Clone)]
pub struct IngestorCursor {
    path: Arc<PathBuf>,
    state: Arc<Mutex<CursorState>>,
}

impl IngestorCursor {
    /// Load the cursor at `path`, or start a fresh one when the file does not
    /// exist. A fresh cursor is not written until the first persist.
    ///
    /// Fails with [`PrismError::CursorError`] when the file exists but cannot
    /// be read, is not valid cursor JSON, or breaks the
    /// `last_flush_slot <= last_processed_slot` invariant.
    pub fn load_or_init(path: impl Into<PathBuf>) -> PrismResult<Self> {
        let path = path.into();
        let state = match fs::read(&path) {
            Ok(bytes) => {
                let state: CursorState = serde_json::from_slice(&bytes)
                    .map_err(|err| cursor_error(&path, invalid_data(err)))?;
                state
                    .validate()
                    .map_err(|reason| cursor_error(&path, invalid_data(reason)))?;
                state
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => CursorState::default(),
            Err(err) => return Err(cursor_error(&path, err)),
        };
        Ok(Self {
            path: Arc::new(path),
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A copy of the current state.
    pub fn state(&self) -> CursorState {
        self.lock().clone()
    }

    /// The first slot to fetch after a restart: one past the last flush, or
    /// `None` for a fresh cursor (the caller picks the starting point).
    pub fn resume_slot(&self) -> Option<u64> {
        let state = self.lock();
        (!state.is_fresh()).then(|| state.last_flush_slot + 1)
    }

    /// Record that `slot` was handed to the writer buffer. Not persisted.
    ///
    /// Slots must arrive in increasing order; a slot at or below the current
    /// `last_processed_slot` is ignored and `false` is returned.
    pub fn record_processed(&self, slot: u64, blockhash: &str) -> bool {
        let mut state = self.lock();
        if !state.advance_processed(slot) {
            return false;
        }
        state.last_processed_blockhash = blockhash.to_owned();
        state.updated_at = now();
        true
    }

    /// Record that `slot` was skipped by the chain: the processed edge moves
    /// past it and the blockhash stays that of the last real block. Same
    /// ordering rule as [`Self::record_processed`].
    pub fn record_skipped(&self, slot: u64) -> bool {
        let mut state = self.lock();
        if !state.advance_processed(slot) {
            return false;
        }
        state.updated_at = now();
        true
    }

    /// Record that every slot up to `slot` is on disk, and persist.
    ///
    /// `slot` is clamped to `last_processed_slot` (nothing unprocessed can
    /// have been flushed) and never moves the flush edge backwards. Returns
    /// the flush slot now in effect.
    pub fn record_flush(&self, slot: u64) -> PrismResult<u64> {
        let flushed = {
            let mut state = self.lock();
            let slot = slot.min(state.last_processed_slot);
            if slot > state.last_flush_slot {
                state.last_flush_slot = slot;
                state.updated_at = now();
            }
            state.last_flush_slot
        };
        self.persist()?;
        Ok(flushed)
    }

    /// Write the current state to disk atomically.
    pub fn persist(&self) -> PrismResult<()> {
        let state = self.state();
        let json = serde_json::to_vec_pretty(&state)
            .map_err(|err| cursor_error(&self.path, invalid_data(err)))?;
        write_atomically(&self.path, &json).map_err(|err| cursor_error(&self.path, err))
    }

    fn lock(&self) -> MutexGuard<'_, CursorState> {
        // The state is plain data and every update is a few assignments, so a
        // panic elsewhere cannot leave it half-written.
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn write_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp = PathBuf::from(tmp_name);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)
}

fn cursor_error(path: &Path, source: io::Error) -> PrismError {
    PrismError::cursor(path.display().to_string(), source)
}

fn invalid_data(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
