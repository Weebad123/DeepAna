//! Decisions: what one pass does with its findings.
//!
//! Pure functions of the findings, the persisted state and the [`Limits`]:
//!
//! 1. [`plan_window`]: if fewer than `min_window` safe slots lie above
//!    `reconciled_through`, no pass runs. Otherwise the window is the next
//!    `max_chunk` of them.
//! 2. [`decide`], mass mismatch: if at least `mass_mismatch_min_examined`
//!    slots were examined and the actionable findings exceed
//!    `mass_mismatch_percent` of them, the pass **refuses**: `halted` is set
//!    and nothing else changes.
//! 3. Two passes must agree: a finding seen in the previous pass with the
//!    same kind is **confirmed**; a new or changed one gets a first strike;
//!    strikes (and unresolved findings) for slots that are ok again are
//!    dropped.
//! 4. A confirmed finding on a slot already corrected
//!    `max_corrections_per_slot` times, or more than
//!    `max_depth_below_cursor` slots below the cursor, becomes
//!    **unresolved**.
//! 5. If confirmed findings remain and `max_rewinds_per_window` rewinds
//!    already happened in the last `rewind_window_secs`, the pass **waits**.
//! 6. `reconciled_through` moves to `min(window.end, lowest pending - 1)`,
//!    pending meaning any strike or unresolved finding: it never passes a
//!    discrepancy that is not resolved.
//! 7. Otherwise the action is a **rewind** to the lowest confirmed slot.

use std::collections::BTreeMap;

use super::classify::{Classification, Finding, SlotWindow};
use super::state::{HaltReason, ReconcileState, Strike, Unresolved, UnresolvedReason};
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::CorrectionReason;

/// The thresholds every decision is taken against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Fewest new safe slots worth a pass.
    pub min_window: u64,
    /// Most slots examined in one pass.
    pub max_chunk: u64,
    /// Fewest examined slots for the mass-mismatch rule to apply.
    pub mass_mismatch_min_examined: u64,
    /// Share of examined slots (in percent) that actionable findings must
    /// exceed for the pass to refuse.
    pub mass_mismatch_percent: u64,
    /// Corrections of one slot after which a confirmed finding there is
    /// unresolved.
    pub max_corrections_per_slot: u32,
    /// Distance below the ingestion cursor past which a confirmed finding is
    /// unresolved.
    pub max_depth_below_cursor: u64,
    /// Rewinds allowed within `rewind_window_secs`.
    pub max_rewinds_per_window: usize,
    /// The sliding window for `max_rewinds_per_window`, in seconds.
    pub rewind_window_secs: i64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            min_window: 20,
            max_chunk: 2_000,
            mass_mismatch_min_examined: 20,
            mass_mismatch_percent: 50,
            max_corrections_per_slot: 2,
            max_depth_below_cursor: 5_000,
            max_rewinds_per_window: 3,
            rewind_window_secs: 3_600,
        }
    }
}

impl Limits {
    /// The first offending field, as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        if self.min_window == 0 {
            return Err(PrismError::config("min_window", "must be greater than 0"));
        }
        if self.max_chunk < self.min_window {
            return Err(PrismError::config(
                "max_chunk",
                format!("must be at least min_window ({})", self.min_window),
            ));
        }
        if !(1..=100).contains(&self.mass_mismatch_percent) {
            return Err(PrismError::config("mass_mismatch_percent", "must be within 1..=100"));
        }
        if self.max_corrections_per_slot == 0 {
            return Err(PrismError::config("max_corrections_per_slot", "must be greater than 0"));
        }
        if self.max_rewinds_per_window == 0 {
            return Err(PrismError::config("max_rewinds_per_window", "must be greater than 0"));
        }
        if self.rewind_window_secs <= 0 {
            return Err(PrismError::config("rewind_window_secs", "must be greater than 0"));
        }
        Ok(())
    }
}

/// Rule 1: the window the next pass examines, or `None` when fewer than
/// `min_window` slots in `reconciled_through + 1 ..= safe_upper` are waiting.
pub fn plan_window(reconciled_through: u64, safe_upper: u64, limits: &Limits) -> Option<SlotWindow> {
    if safe_upper.saturating_sub(reconciled_through) < limits.min_window {
        return None;
    }
    let start = reconciled_through + 1;
    let end = safe_upper.min(reconciled_through.saturating_add(limits.max_chunk));
    SlotWindow::new(start, end)
}

/// A rewind the reconciler should trigger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RewindPlan {
    /// The lowest confirmed slot: the rewind point.
    pub from_slot: u64,
    /// The highest confirmed slot.
    pub to_slot: u64,
    /// The most severe kind among them.
    pub reason: CorrectionReason,
    /// The confirmed slots, ascending.
    pub slots: Vec<u64>,
}

impl RewindPlan {
    pub fn affected(&self) -> u64 {
        self.slots.len() as u64
    }
}

/// What a pass does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Nothing is confirmed: the window is clean, or only first strikes and
    /// unresolved findings remain.
    Nothing,
    /// Rule 2: too much disagrees at once. The reconciler stops.
    Refuse { actionable: u64, examined: u64 },
    /// Rule 5: confirmed findings wait for the rewind cap to free up. The
    /// earliest a rewind is allowed again (Unix seconds).
    Wait { confirmed: u64, until: i64 },
    /// Rule 7.
    Rewind(RewindPlan),
}

/// The outcome of [`decide`]: the action and the state to persist before
/// taking it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub action: Action,
    pub state: ReconcileState,
    /// Findings that became unresolved in this pass, for the log.
    pub newly_unresolved: Vec<Unresolved>,
}

/// Rules 2 to 7 for one classified window. `cursor_slot` is the ingestion
/// cursor and `now` the time in Unix seconds.
pub fn decide(
    classification: &Classification,
    state: &ReconcileState,
    cursor_slot: u64,
    now: i64,
    limits: &Limits,
) -> Decision {
    let window = classification.window;
    let mut next = state.clone();
    next.updated_at = now;

    // Rule 2.
    let actionable: Vec<&Finding> = classification.actionable().collect();
    let examined = classification.examined;
    let count = actionable.len() as u64;
    if examined >= limits.mass_mismatch_min_examined
        && count.saturating_mul(100) > examined.saturating_mul(limits.mass_mismatch_percent)
    {
        next.halted = Some(HaltReason::MassMismatch);
        return Decision {
            action: Action::Refuse {
                actionable: count,
                examined,
            },
            state: next,
            newly_unresolved: Vec::new(),
        };
    }

    // Rule 3. What lies outside the window was not looked at and is kept.
    let previous: BTreeMap<u64, Strike> = state.strikes.iter().map(|s| (s.slot, *s)).collect();
    let was_unresolved: BTreeMap<u64, Unresolved> =
        state.unresolved.iter().map(|u| (u.slot, *u)).collect();
    let mut strikes: BTreeMap<u64, Strike> = previous
        .iter()
        .filter(|(slot, _)| !window.contains(**slot))
        .map(|(slot, s)| (*slot, *s))
        .collect();
    let mut unresolved: BTreeMap<u64, Unresolved> = was_unresolved
        .iter()
        .filter(|(slot, _)| !window.contains(**slot))
        .map(|(slot, u)| (*slot, *u))
        .collect();
    let mut confirmed = Vec::new();
    for finding in actionable {
        let slot = finding.slot;
        if let Some(u) = was_unresolved.get(&slot) {
            // Still wrong: stays unresolved, with what it looks like now.
            unresolved.insert(slot, Unresolved { kind: finding.kind, ..*u });
            continue;
        }
        match previous.get(&slot) {
            Some(strike) if strike.kind == finding.kind => confirmed.push(*finding),
            _ => {
                strikes.insert(slot, Strike { slot, kind: finding.kind, seen: 1 });
            }
        }
    }

    // Rule 4.
    let mut newly_unresolved = Vec::new();
    let mut actionable_confirmed = Vec::new();
    for finding in confirmed {
        let reason = if state.attempts_for(finding.slot) >= limits.max_corrections_per_slot {
            Some(UnresolvedReason::TooManyCorrections)
        } else if cursor_slot.saturating_sub(finding.slot) > limits.max_depth_below_cursor {
            Some(UnresolvedReason::TooFarBelowCursor)
        } else {
            None
        };
        match reason {
            Some(reason) => {
                let entry = Unresolved {
                    slot: finding.slot,
                    kind: finding.kind,
                    reason,
                };
                unresolved.insert(finding.slot, entry);
                newly_unresolved.push(entry);
            }
            None => {
                strikes.insert(
                    finding.slot,
                    Strike {
                        slot: finding.slot,
                        kind: finding.kind,
                        seen: 2,
                    },
                );
                actionable_confirmed.push(finding);
            }
        }
    }

    next.strikes = strikes.into_values().collect();
    next.unresolved = unresolved.into_values().collect();

    // Rule 6.
    let advanced_to = match next.lowest_pending() {
        Some(lowest) => window.end.min(lowest.saturating_sub(1)),
        None => window.end,
    };
    next.reconciled_through = state.reconciled_through.max(advanced_to);
    // A slot at or below reconciled_through is never examined again.
    let through = next.reconciled_through;
    next.attempts.retain(|a| a.slot > through);

    // Rule 5.
    let horizon = now.saturating_sub(limits.rewind_window_secs);
    next.rewinds.retain(|&at| at > horizon);
    next.rewinds.sort_unstable();

    let action = if actionable_confirmed.is_empty() {
        Action::Nothing
    } else if next.rewinds.len() >= limits.max_rewinds_per_window {
        let oldest_counted = next.rewinds[next.rewinds.len() - limits.max_rewinds_per_window];
        Action::Wait {
            confirmed: actionable_confirmed.len() as u64,
            until: oldest_counted.saturating_add(limits.rewind_window_secs),
        }
    } else {
        // Rule 7.
        let slots: Vec<u64> = actionable_confirmed.iter().map(|f| f.slot).collect();
        let reason = actionable_confirmed
            .iter()
            .filter_map(|f| f.kind.correction_reason())
            .max()
            .expect("actionable findings always carry a correction reason");
        Action::Rewind(RewindPlan {
            from_slot: slots[0],
            to_slot: slots[slots.len() - 1],
            reason,
            slots,
        })
    };

    Decision {
        action,
        state: next,
        newly_unresolved,
    }
}
