//! Classification: what is wrong with a window of stored slots.
//!
//! A pure function of four inputs, all restricted to one [`SlotWindow`]:
//!
//! * **stored**: slots that have a row in the `slots` Parquet table;
//! * **produced**: slots the chain has a block for at `finalized`;
//! * **logged**: slots the skip log says the leader skipped
//!   (`validator_skipped` lines only: an `rpc_unavailable` line says the
//!   block could not be read, not that there is none).
//!
//! [`classify`] compares only the *sets* of slots; [`classify_blocks`] adds
//! the block-level checks below. Every slot in the window falls in exactly
//! one row of this table:
//!
//! | stored | produced | logged | class            | actionable |
//! |--------|----------|--------|------------------|------------|
//! | yes    | yes      | any    | ok               |            |
//! | yes    | no       | any    | `orphaned`       | yes        |
//! | no     | yes      | no     | `missing`        | yes        |
//! | no     | yes      | yes    | `false_skip`     | yes        |
//! | no     | no       | yes    | ok               |            |
//! | no     | no       | no     | `unlogged_skip`  | no         |
//!
//! The skip log is append-only, so "stored and produced" is ok whatever it
//! says: a false skip that was since repaired would otherwise be flagged
//! forever. `unlogged_skip` is informational: the chain skipped the slot and
//! PRISM stored nothing, which is right, but no `Skip` was ever recorded.
//!
//! # Is it the same block?
//!
//! A slot that is both stored and produced may still hold the wrong block:
//! one from a fork that also had a block at that slot. [`classify_blocks`]
//! adds two checks on top of the table, for those slots only:
//!
//! * **lineage** (free): on the finalized chain the parent of a block is the
//!   previous slot that has a block, which the block list already gives. A
//!   stored row whose `parent_slot` is another slot is `mismatched`. When
//!   the stored parent is itself an orphan of the same window the child's
//!   mismatch is a consequence, not a second discrepancy: the rewind to the
//!   orphan re-ingests the child anyway, so it is not reported. When the
//!   finalized parent is unknown (no block found in the lookback before the
//!   window) the check is skipped: no answer is not evidence.
//! * **blockhash** (one header request per slot, opt-in): a stored
//!   `blockhash` that differs from the finalized block's is `mismatched`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::event::CorrectionReason;

/// An inclusive range of slots examined in one pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SlotWindow {
    pub start: u64,
    pub end: u64,
}

impl SlotWindow {
    /// `None` when `end < start`.
    pub const fn new(start: u64, end: u64) -> Option<Self> {
        if end < start {
            None
        } else {
            Some(Self { start, end })
        }
    }

    /// Slots in the window, both ends included.
    pub const fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    /// Never true: a window holds at least one slot.
    pub const fn is_empty(&self) -> bool {
        false
    }

    pub const fn contains(&self, slot: u64) -> bool {
        self.start <= slot && slot <= self.end
    }
}

impl fmt::Display for SlotWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..={}", self.start, self.end)
    }
}

/// What is wrong with one slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// Stored, but the finalized chain has no block there.
    Orphaned,
    /// Stored and on the finalized chain, but the stored block is not the
    /// finalized one: its parent (or blockhash) differs.
    Mismatched,
    /// The finalized chain has a block, nothing is stored, no skip logged.
    Missing,
    /// The finalized chain has a block, nothing is stored, a skip logged.
    FalseSkip,
    /// No block, nothing stored, but no skip logged either. Not acted on.
    UnloggedSkip,
}

impl FindingKind {
    pub const ALL: [FindingKind; 5] = [
        FindingKind::Orphaned,
        FindingKind::Mismatched,
        FindingKind::Missing,
        FindingKind::FalseSkip,
        FindingKind::UnloggedSkip,
    ];

    /// Log / file representation (matches the serde encoding).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Orphaned => "orphaned",
            Self::Mismatched => "mismatched",
            Self::Missing => "missing",
            Self::FalseSkip => "false_skip",
            Self::UnloggedSkip => "unlogged_skip",
        }
    }

    /// Whether the stored data is wrong and must be re-ingested.
    pub const fn is_actionable(self) -> bool {
        !matches!(self, Self::UnloggedSkip)
    }

    /// The reason a correction for this kind carries; `None` for
    /// [`Self::UnloggedSkip`], which is never corrected.
    pub const fn correction_reason(self) -> Option<CorrectionReason> {
        match self {
            Self::Orphaned => Some(CorrectionReason::Orphaned),
            Self::Mismatched => Some(CorrectionReason::Mismatched),
            Self::Missing => Some(CorrectionReason::Missing),
            Self::FalseSkip => Some(CorrectionReason::FalseSkip),
            Self::UnloggedSkip => None,
        }
    }
}

impl fmt::Display for FindingKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One slot that is not ok.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Finding {
    pub slot: u64,
    pub kind: FindingKind,
}

/// The findings of one window, sorted by slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classification {
    pub window: SlotWindow,
    /// Slots looked at: the whole window.
    pub examined: u64,
    /// Every slot that is not ok, ascending, one entry per slot.
    pub findings: Vec<Finding>,
}

impl Classification {
    /// Findings that call for a correction (all but `unlogged_skip`).
    pub fn actionable(&self) -> impl Iterator<Item = &Finding> {
        self.findings.iter().filter(|f| f.kind.is_actionable())
    }

    pub fn actionable_count(&self) -> u64 {
        self.actionable().count() as u64
    }

    /// Findings of `kind`.
    pub fn count(&self, kind: FindingKind) -> u64 {
        self.findings.iter().filter(|f| f.kind == kind).count() as u64
    }

    /// Whether every slot in the window is ok.
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Classify every slot of `window`. Members of the sets outside the window
/// are ignored. Deterministic: the same inputs give the same findings in
/// the same order.
pub fn classify(
    window: SlotWindow,
    stored: &BTreeSet<u64>,
    produced: &BTreeSet<u64>,
    logged: &BTreeSet<u64>,
) -> Classification {
    let findings = (window.start..=window.end)
        .filter_map(|slot| {
            classify_slot(stored.contains(&slot), produced.contains(&slot), logged.contains(&slot))
                .map(|kind| Finding { slot, kind })
        })
        .collect();
    Classification {
        window,
        examined: window.len(),
        findings,
    }
}

/// The table in the module docs. `None` is ok.
pub const fn classify_slot(stored: bool, produced: bool, logged: bool) -> Option<FindingKind> {
    match (stored, produced, logged) {
        (true, true, _) | (false, false, true) => None,
        (true, false, _) => Some(FindingKind::Orphaned),
        (false, true, false) => Some(FindingKind::Missing),
        (false, true, true) => Some(FindingKind::FalseSkip),
        (false, false, false) => Some(FindingKind::UnloggedSkip),
    }
}

/// What the `slots` table holds for one stored block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBlock {
    pub blockhash: String,
    pub parent_slot: u64,
}

/// The finalized chain's blocks around a window.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FinalizedChain {
    /// Slots of the window that hold a finalized block.
    pub produced: BTreeSet<u64>,
    /// The last slot below the window that holds a finalized block, when
    /// the lookback found one.
    pub predecessor: Option<u64>,
}

/// [`classify`], plus the "same block?" checks of the module docs.
/// `finalized_hashes` holds the finalized blockhash of the slots that were
/// looked up (empty when blockhashes are not verified). Deterministic.
pub fn classify_blocks(
    window: SlotWindow,
    stored: &BTreeMap<u64, StoredBlock>,
    chain: &FinalizedChain,
    logged: &BTreeSet<u64>,
    finalized_hashes: &BTreeMap<u64, String>,
) -> Classification {
    let stored_slots: BTreeSet<u64> = stored.keys().copied().collect();
    let mut classification = classify(window, &stored_slots, &chain.produced, logged);
    let orphaned = |slot: u64| window.contains(slot) && stored.contains_key(&slot) && !chain.produced.contains(&slot);

    let mut previous = chain.predecessor;
    let mut mismatched = Vec::new();
    for slot in window.start..=window.end {
        if !chain.produced.contains(&slot) {
            continue;
        }
        let finalized_parent = previous.replace(slot);
        let Some(row) = stored.get(&slot) else { continue };
        let wrong_parent = finalized_parent
            .is_some_and(|parent| row.parent_slot != parent && !orphaned(row.parent_slot));
        let wrong_hash = finalized_hashes
            .get(&slot)
            .is_some_and(|hash| *hash != row.blockhash);
        if wrong_parent || wrong_hash {
            mismatched.push(Finding { slot, kind: FindingKind::Mismatched });
        }
    }
    if !mismatched.is_empty() {
        // Produced and stored slots had no finding, so no slot appears twice.
        classification.findings.extend(mismatched);
        classification.findings.sort_by_key(|f| f.slot);
    }
    classification
}
