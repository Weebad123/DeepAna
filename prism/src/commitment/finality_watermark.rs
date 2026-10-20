//! [`FinalityWatermark`]: the single source of truth for where the finality
//! boundary sits.
//!
//! Two slots are tracked, each with the blockhash it was observed with:
//!
//! * **confirmed**: the highest slot seen at `confirmed` commitment. Data up
//!   to here has been written, but could in theory still be rolled back.
//! * **finalized**: the highest slot seen at `finalized` commitment. Data up
//!   to here is permanent.
//!
//! Guarantees, relied on by everything that reads the watermark:
//!
//! * **Monotonic.** Neither slot ever moves backwards. An observation below
//!   the current value (a lagging RPC node, a retried response) is rejected
//!   and reported as [`Advance::Stale`].
//! * **`finalized <= confirmed`.** A finalized slot is confirmed by
//!   definition, so advancing finalized past confirmed raises confirmed with
//!   it.
//! * **Slot and blockhash are consistent.** Each pair is written together
//!   under its blockhash's write lock, so [`FinalityWatermark::confirmed_tip`]
//!   / [`FinalityWatermark::finalized_tip`] never pair a slot with another
//!   slot's hash. The slot-only readers ([`FinalityWatermark::is_finalized`]
//!   and friends) take no lock: they are on the per-row hot path.
//! * **No claim without evidence.** Slot `0` means "nothing observed yet";
//!   until the first finalized observation, no slot is reported finalized.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use arrow::array::BooleanArray;
use chrono::Utc;

/// The result of offering a new observation to the watermark.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Advance {
    /// The watermark moved forward from `previous`.
    Advanced { previous: u64 },
    /// The observation matches what is already recorded.
    Unchanged,
    /// The observation is below the watermark and was rejected.
    Stale { current: u64 },
    /// The observation is for the recorded slot but carries a different
    /// blockhash. Rejected: the recorded pair is kept.
    Conflict { recorded: String },
}

impl Advance {
    pub const fn is_advanced(&self) -> bool {
        matches!(self, Self::Advanced { .. })
    }
}

/// A consistent, point-in-time copy of a [`FinalityWatermark`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WatermarkSnapshot {
    pub confirmed_slot: u64,
    pub confirmed_blockhash: String,
    pub finalized_slot: u64,
    pub finalized_blockhash: String,
    /// Unix seconds of the last advance; `None` if there has been none.
    pub last_updated: Option<i64>,
}

impl WatermarkSnapshot {
    /// Slots between the confirmed and the finalized tip.
    pub const fn lag(&self) -> u64 {
        self.confirmed_slot.saturating_sub(self.finalized_slot)
    }
}

/// The finality boundary, shared across tasks behind an `Arc`.
///
/// Lock order, when both are held: `finalized_blockhash`, then
/// `confirmed_blockhash`.
#[derive(Debug, Default)]
pub struct FinalityWatermark {
    confirmed_slot: AtomicU64,
    finalized_slot: AtomicU64,
    confirmed_blockhash: RwLock<String>,
    finalized_blockhash: RwLock<String>,
    last_updated: AtomicI64,
}

impl FinalityWatermark {
    /// A watermark with nothing observed yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Highest slot seen at confirmed commitment; `0` if none yet.
    pub fn confirmed_slot(&self) -> u64 {
        self.confirmed_slot.load(Ordering::Acquire)
    }

    /// Highest slot seen at finalized commitment; `0` if none yet.
    pub fn finalized_slot(&self) -> u64 {
        self.finalized_slot.load(Ordering::Acquire)
    }

    /// The confirmed slot together with its blockhash.
    pub fn confirmed_tip(&self) -> (u64, String) {
        let hash = read(&self.confirmed_blockhash);
        (self.confirmed_slot(), hash.clone())
    }

    /// The finalized slot together with its blockhash.
    pub fn finalized_tip(&self) -> (u64, String) {
        let hash = read(&self.finalized_blockhash);
        (self.finalized_slot(), hash.clone())
    }

    /// Whether `slot` is at or below the finalized watermark, i.e. its data
    /// can never be rolled back. Always `false` before the first finalized
    /// observation.
    pub fn is_finalized(&self, slot: u64) -> bool {
        is_finalized_at(self.finalized_slot(), slot)
    }

    /// The `is_finalized` column for a batch of rows, one value per slot.
    ///
    /// The watermark is read **once** for the whole batch, so the column can
    /// never be torn by an advance landing mid-batch (a row marked final
    /// while a lower slot in the same batch is not).
    pub fn is_finalized_column(&self, slots: impl IntoIterator<Item = u64>) -> BooleanArray {
        let finalized = self.finalized_slot();
        slots
            .into_iter()
            .map(|slot| Some(is_finalized_at(finalized, slot)))
            .collect()
    }

    /// Slots between the confirmed and the finalized tip.
    pub fn lag(&self) -> u64 {
        // Finalized first: confirmed is raised before finalized is stored, so
        // once this load sees a finalized value, confirmed is at least that.
        let finalized = self.finalized_slot();
        let confirmed = self.confirmed_slot();
        confirmed.saturating_sub(finalized)
    }

    /// Unix seconds of the last time either slot advanced; `None` if neither
    /// has yet. For health checks: on a live chain this should never be more
    /// than a few poll intervals old.
    pub fn last_updated(&self) -> Option<i64> {
        match self.last_updated.load(Ordering::Relaxed) {
            0 => None,
            ts => Some(ts),
        }
    }

    /// A consistent copy of the whole watermark.
    pub fn snapshot(&self) -> WatermarkSnapshot {
        let finalized_hash = read(&self.finalized_blockhash);
        let confirmed_hash = read(&self.confirmed_blockhash);
        WatermarkSnapshot {
            confirmed_slot: self.confirmed_slot(),
            confirmed_blockhash: confirmed_hash.clone(),
            finalized_slot: self.finalized_slot(),
            finalized_blockhash: finalized_hash.clone(),
            last_updated: self.last_updated(),
        }
    }

    /// Offer a slot observed at confirmed commitment. Only a slot above the
    /// current confirmed watermark is accepted.
    pub fn advance_confirmed(&self, slot: u64, blockhash: &str) -> Advance {
        let mut hash = write(&self.confirmed_blockhash);
        let outcome = raise(&self.confirmed_slot, &mut hash, slot, blockhash);
        if outcome.is_advanced() {
            self.touch();
        }
        outcome
    }

    /// Offer a slot observed at finalized commitment. Only a slot above the
    /// current finalized watermark is accepted; the confirmed watermark is
    /// raised to it if it is behind.
    pub fn advance_finalized(&self, slot: u64, blockhash: &str) -> Advance {
        let mut finalized_hash = write(&self.finalized_blockhash);
        if slot > self.finalized_slot.load(Ordering::Relaxed) {
            let mut confirmed_hash = write(&self.confirmed_blockhash);
            // Stale / conflict here just means confirmed is already ahead.
            let _ = raise(&self.confirmed_slot, &mut confirmed_hash, slot, blockhash);
        }
        let outcome = raise(&self.finalized_slot, &mut finalized_hash, slot, blockhash);
        if outcome.is_advanced() {
            self.touch();
        }
        outcome
    }

    fn touch(&self) {
        self.last_updated
            .store(Utc::now().timestamp(), Ordering::Relaxed);
    }
}

fn is_finalized_at(finalized: u64, slot: u64) -> bool {
    finalized != 0 && slot <= finalized
}

/// Move `cell` / `hash` to `slot` / `blockhash` if `slot` is higher. The
/// caller holds the write guard `hash`, which is what serialises writers to
/// `cell`.
fn raise(cell: &AtomicU64, hash: &mut String, slot: u64, blockhash: &str) -> Advance {
    let current = cell.load(Ordering::Relaxed);
    if slot > current {
        blockhash.clone_into(hash);
        cell.store(slot, Ordering::Release);
        Advance::Advanced { previous: current }
    } else if slot < current {
        Advance::Stale { current }
    } else if current == 0 || *hash == blockhash {
        // Slot 0 is the "nothing observed" sentinel, never a recorded pair.
        Advance::Unchanged
    } else {
        Advance::Conflict {
            recorded: hash.clone(),
        }
    }
}

// The guarded sections only copy strings and cannot leave the data half
// written, so a poisoned lock is still safe to use.

fn read(lock: &RwLock<String>) -> RwLockReadGuard<'_, String> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write(lock: &RwLock<String>) -> RwLockWriteGuard<'_, String> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}
