//! `reconcile.json`: what the reconciler remembers across passes and
//! restarts.
//!
//! ```text
//! { "version": 1, "genesis_hash": "…", "reconciled_through": 524894000,
//!   "strikes":    [{"slot": …, "kind": "orphaned", "seen": 1}],
//!   "attempts":   [{"slot": …, "count": 1}],
//!   "unresolved": [{"slot": …, "kind": "…", "reason": "too_far_below_cursor"}],
//!   "rewinds": [unix_time, …], "in_flight": null, "halted": null, "updated_at": … }
//! ```
//!
//! The file is replaced atomically on every save: written to a sibling
//! `.tmp`, fsynced, renamed over the old one, and the directory fsynced. A
//! crash leaves either the old state or the new one, never a torn file.
//!
//! A file that cannot be parsed, or that breaks the state's invariants, is
//! **quarantined**: renamed to `reconcile.json.corrupt-{unix_time}` and
//! replaced by a fresh state, which re-pins the cluster and reconciles again
//! from the first stored slot. A file written by a newer version is refused
//! instead, so a downgrade cannot throw away state it does not understand.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::classify::FindingKind;
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::{ChangeEvent, CorrectionReason};

/// Name of the state file in the data directory.
pub const RECONCILE_FILE_NAME: &str = "reconcile.json";

/// The format version this build writes and reads.
pub const STATE_VERSION: u32 = 1;

/// A finding seen in a pass, waiting for the next pass to confirm it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strike {
    pub slot: u64,
    pub kind: FindingKind,
    /// Passes in a row that reported this slot with this kind: 1 for a first
    /// strike, 2 once confirmed (kept at 2 while a rewind waits).
    pub seen: u32,
}

/// How many times a slot has been corrected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub slot: u64,
    pub count: u32,
}

/// Why a confirmed finding is no longer acted on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnresolvedReason {
    /// The slot was already corrected the maximum number of times and is
    /// still wrong: another rewind would loop.
    TooManyCorrections,
    /// The slot is so far below the ingestion cursor that rewinding to it
    /// would re-ingest too much.
    TooFarBelowCursor,
}

impl UnresolvedReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TooManyCorrections => "too_many_corrections",
            Self::TooFarBelowCursor => "too_far_below_cursor",
        }
    }
}

/// A confirmed finding the reconciler gave up acting on. It still blocks
/// `reconciled_through` until the slot is found ok again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Unresolved {
    pub slot: u64,
    pub kind: FindingKind,
    pub reason: UnresolvedReason,
}

/// A rewind that was started and not yet recorded as done. Set before the
/// correction is announced; cleared once the ingestor has lowered its cursor
/// and deleted the files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlight {
    /// The rewind point: every file at or above it is deleted.
    pub from_slot: u64,
    pub to_slot: u64,
    pub reason: CorrectionReason,
    pub affected: u64,
    /// The slots being corrected, ascending.
    pub slots: Vec<u64>,
    /// Unix seconds.
    pub started_at: i64,
}

impl InFlight {
    /// The [`ChangeEvent::Correction`] that announces this rewind.
    pub fn event(&self) -> ChangeEvent {
        ChangeEvent::correction(self.from_slot, self.to_slot, self.reason, self.affected)
    }

    /// Whether `other` is the same rewind (same range, same start time):
    /// how a completion recorded by someone else is told apart.
    pub fn is_same_rewind(&self, other: &Self) -> bool {
        self.from_slot == other.from_slot && self.to_slot == other.to_slot && self.started_at == other.started_at
    }
}

/// Why the reconciler stopped for good. Cleared only by an operator
/// (removing the `halted` value, or the file).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltReason {
    /// Too many slots of one window disagreed with the chain at once: more
    /// likely a wrong RPC or a broken data directory than real forks.
    MassMismatch,
    /// The pipeline's genesis hash is not the one the data was pinned to.
    ClusterMismatch,
}

impl HaltReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MassMismatch => "mass_mismatch",
            Self::ClusterMismatch => "cluster_mismatch",
        }
    }
}

/// Everything `reconcile.json` holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileState {
    pub version: u32,
    /// The cluster the data directory is pinned to; `None` until the first
    /// pass, which also sets `reconciled_through`.
    pub genesis_hash: Option<String>,
    /// Every slot at or below this was found ok (or confirmed ok after a
    /// correction). Only moves forward.
    pub reconciled_through: u64,
    #[serde(default)]
    pub strikes: Vec<Strike>,
    #[serde(default)]
    pub attempts: Vec<Attempt>,
    #[serde(default)]
    pub unresolved: Vec<Unresolved>,
    /// Unix seconds of recent rewinds, for the hourly cap.
    #[serde(default)]
    pub rewinds: Vec<i64>,
    #[serde(default)]
    pub in_flight: Option<InFlight>,
    #[serde(default)]
    pub halted: Option<HaltReason>,
    pub updated_at: i64,
}

impl Default for ReconcileState {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            genesis_hash: None,
            reconciled_through: 0,
            strikes: Vec::new(),
            attempts: Vec::new(),
            unresolved: Vec::new(),
            rewinds: Vec::new(),
            in_flight: None,
            halted: None,
            updated_at: 0,
        }
    }
}

impl ReconcileState {
    /// Whether the first pass has pinned the cluster and set the start.
    pub const fn is_initialized(&self) -> bool {
        self.genesis_hash.is_some()
    }

    /// Times `slot` has been corrected.
    pub fn attempts_for(&self, slot: u64) -> u32 {
        self.attempts
            .iter()
            .find(|a| a.slot == slot)
            .map_or(0, |a| a.count)
    }

    /// The lowest slot with a strike or an unresolved finding.
    pub fn lowest_pending(&self) -> Option<u64> {
        self.strikes
            .iter()
            .map(|s| s.slot)
            .chain(self.unresolved.iter().map(|u| u.slot))
            .min()
    }

    /// Step 4 of a rewind: count the attempt for every corrected slot, drop
    /// their strikes (the next pass looks at them afresh), record the rewind
    /// time and clear `in_flight`. A no-op without a rewind in flight.
    pub fn complete_in_flight(&mut self, now: i64) {
        let Some(in_flight) = self.in_flight.take() else {
            return;
        };
        let corrected: BTreeSet<u64> = in_flight.slots.iter().copied().collect();
        for &slot in &corrected {
            match self.attempts.iter_mut().find(|a| a.slot == slot) {
                Some(attempt) => attempt.count += 1,
                None => self.attempts.push(Attempt { slot, count: 1 }),
            }
        }
        self.attempts.sort_by_key(|a| a.slot);
        self.strikes.retain(|s| !corrected.contains(&s.slot));
        self.rewinds.push(now);
        self.updated_at = now;
    }

    /// Check what a hand-edited or damaged file could get wrong.
    fn validate(&self) -> Result<(), String> {
        let unique = |slots: Vec<u64>, what: &str| {
            let set: BTreeSet<u64> = slots.iter().copied().collect();
            if set.len() == slots.len() {
                Ok(())
            } else {
                Err(format!("{what} lists a slot twice"))
            }
        };
        unique(self.strikes.iter().map(|s| s.slot).collect(), "strikes")?;
        unique(self.attempts.iter().map(|a| a.slot).collect(), "attempts")?;
        unique(self.unresolved.iter().map(|u| u.slot).collect(), "unresolved")?;
        if let Some(slot) = self.lowest_pending()
            && slot <= self.reconciled_through
        {
            return Err(format!(
                "pending slot {slot} is at or below reconciled_through {}",
                self.reconciled_through
            ));
        }
        if let Some(in_flight) = &self.in_flight {
            if in_flight.from_slot == 0 || in_flight.to_slot < in_flight.from_slot {
                return Err(format!(
                    "in_flight range {}..={} is invalid",
                    in_flight.from_slot, in_flight.to_slot
                ));
            }
            if in_flight.slots.first() != Some(&in_flight.from_slot) {
                return Err("in_flight slots do not start at from_slot".to_owned());
            }
        }
        Ok(())
    }
}

/// Reads and writes `reconcile.json`.
#[derive(Debug, Clone)]
pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    /// The store for `{data_dir}/reconcile.json`.
    pub fn new(data_dir: impl AsRef<Path>) -> Self {
        Self::at(data_dir.as_ref().join(RECONCILE_FILE_NAME))
    }

    /// The store for the file at `path`.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The saved state; `None` when there is no file, or when there was one
    /// that was corrupt and has just been quarantined.
    ///
    /// Fails with [`PrismError::CursorError`] when the file cannot be read,
    /// cannot be quarantined, or was written by a newer format version.
    pub fn load(&self) -> PrismResult<Option<ReconcileState>> {
        let bytes = match fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(self.error(err)),
        };
        let parsed = parse(&bytes);
        match parsed {
            Ok(state) => Ok(Some(state)),
            Err(Corrupt::Version(version)) => Err(self.error(invalid_data(format!(
                "written by format version {version}; this build reads version {STATE_VERSION}"
            )))),
            Err(Corrupt::Invalid(reason)) => {
                let quarantined = self.quarantine()?;
                warn!(
                    path = %self.path.display(),
                    quarantined = %quarantined.display(),
                    reason,
                    "reconcile state is corrupt; quarantined, starting afresh"
                );
                Ok(None)
            }
        }
    }

    /// Replace the file with `state`, atomically.
    pub fn save(&self, state: &ReconcileState) -> PrismResult<()> {
        let json = serde_json::to_vec_pretty(state).map_err(|err| self.error(invalid_data(err)))?;
        crate::storage::write_file_atomically(&self.path, &json).map_err(|err| self.error(err))
    }

    /// Move the file aside to `{path}.corrupt-{unix_time}`; returns where.
    fn quarantine(&self) -> PrismResult<PathBuf> {
        let mut name = self.path.as_os_str().to_owned();
        name.push(format!(".corrupt-{}", chrono::Utc::now().timestamp()));
        let target = PathBuf::from(name);
        fs::rename(&self.path, &target).map_err(|err| self.error(err))?;
        info!(from = %self.path.display(), to = %target.display(), "quarantined reconcile state");
        Ok(target)
    }

    fn error(&self, source: io::Error) -> PrismError {
        PrismError::cursor(self.path.display().to_string(), source)
    }
}

enum Corrupt {
    /// Parsed, but written by another format version.
    Version(u64),
    /// Not a usable state.
    Invalid(String),
}

fn parse(bytes: &[u8]) -> Result<ReconcileState, Corrupt> {
    // Version first, so a newer layout is refused rather than quarantined.
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|err| Corrupt::Invalid(err.to_string()))?;
    match value.get("version").and_then(serde_json::Value::as_u64) {
        Some(version) if version == u64::from(STATE_VERSION) => {}
        Some(version) if version > u64::from(STATE_VERSION) => return Err(Corrupt::Version(version)),
        _ => return Err(Corrupt::Invalid("missing or unknown version".to_owned())),
    }
    let state: ReconcileState =
        serde_json::from_value(value).map_err(|err| Corrupt::Invalid(err.to_string()))?;
    state.validate().map_err(Corrupt::Invalid)?;
    Ok(state)
}

fn invalid_data(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}
