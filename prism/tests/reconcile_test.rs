//! Tests for `prism::reconcile`.
//!
//! The pure parts (`classify`, `decide`) are tested on hand-built sets and
//! states. `state` and `sources` run against real files in a fresh temp
//! directory. The [`Reconciler`] runs against a [`FakeChain`] (which serves a
//! block list and, for the ingestor test, blocks), a [`FakeContext`] and a
//! [`RecordingRewinder`]; the last test runs it against a real [`Ingestor`].

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::commitment::{CommitmentTracker, FinalityWatermark};
use prism::ingestor::{CURSOR_FILE_NAME, Ingestor, IngestorConfig, IngestorCursor, SKIP_LOG_FILE_NAME};
use prism::reconcile::sources::{
    SkipLogReader, lowest_stored_slot, parse_skip_line, produced_slots, stored_slots,
};
use prism::reconcile::{
    Action, Attempt, Finding, FindingKind, HaltReason, InFlight, Limits, PassOutcome, RECONCILE_FILE_NAME,
    ReconcileConfig, ReconcileContext, ReconcileState, Reconciler, Rewind, SlotWindow, StateStore, Strike,
    Unresolved, UnresolvedReason, classify, decide, plan_window,
};
use prism::slot_processor::SlotFetcherConfig;
use prism::storage::{ParquetWriter, Storage};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::{ChangeEvent, CorrectionReason, SkipReason};
use prism::types::udm::DeepAnaSlot;
use serde_json::json;
use solana_client::client_error::ClientError;
use solana_transaction_status::EncodedConfirmedBlock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, empty directory under the system temp dir.
fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-reconcile-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn set(slots: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
    slots.into_iter().collect()
}

fn window(start: u64, end: u64) -> SlotWindow {
    SlotWindow::new(start, end).unwrap()
}

fn rpc_failure() -> PrismError {
    PrismError::rpc(
        None,
        "getBlocks",
        ClientError::from(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out")),
    )
}

fn slot_row(slot: u64) -> DeepAnaSlot {
    DeepAnaSlot {
        slot,
        blockhash: format!("hash-{slot}"),
        parent_slot: slot.saturating_sub(1),
        block_time: Some(1_700_000_000 + slot as i64),
        block_height: Some(slot),
        transaction_count: 0,
        leader: None,
    }
}

/// Write `slots` to the `slots` table under `dir`, one flush per batch.
fn store(dir: &Path, batches: &[&[u64]]) {
    let mut writer = ParquetWriter::new(dir);
    for batch in batches {
        for &slot in *batch {
            writer.append(&slot_row(slot), &[], &[], &[]).unwrap();
        }
        writer.flush().unwrap();
    }
}

fn skip_line(slot: u64, reason: SkipReason) -> String {
    format!("1700000000,{slot},{reason}\n")
}

fn append(path: &Path, text: &str) {
    use std::io::Write;
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .unwrap()
        .write_all(text.as_bytes())
        .unwrap();
}

/// A state as the first pass leaves it, reconciled through `through`.
fn state_at(through: u64) -> ReconcileState {
    ReconcileState {
        genesis_hash: Some("genesis".to_owned()),
        reconciled_through: through,
        ..ReconcileState::default()
    }
}

const NOW: i64 = 1_800_000_000;

// ---------------------------------------------------------------------------
// classify
// ---------------------------------------------------------------------------

#[test]
fn every_row_of_the_classification_table() {
    use prism::reconcile::classify::classify_slot;
    assert_eq!(classify_slot(true, true, false), None);
    assert_eq!(classify_slot(true, true, true), None, "a repaired false skip is ok");
    assert_eq!(classify_slot(true, false, false), Some(FindingKind::Orphaned));
    assert_eq!(classify_slot(true, false, true), Some(FindingKind::Orphaned));
    assert_eq!(classify_slot(false, true, false), Some(FindingKind::Missing));
    assert_eq!(classify_slot(false, true, true), Some(FindingKind::FalseSkip));
    assert_eq!(classify_slot(false, false, true), None);
    assert_eq!(classify_slot(false, false, false), Some(FindingKind::UnloggedSkip));
}

#[test]
fn classification_is_sorted_windowed_and_counts_every_slot() {
    // 10..=19: 12 orphaned, 14 missing, 16 false skip, 18 unlogged skip.
    let produced = set((10..=19).filter(|s| ![12, 17, 18].contains(s)).chain([5, 25]));
    let stored = set((10..=19).filter(|s| ![14, 16, 17, 18].contains(s)).chain([5]));
    let logged = set([16, 17, 30]);
    let c = classify(window(10, 19), &stored, &produced, &logged);
    assert_eq!(c.examined, 10);
    assert_eq!(
        c.findings,
        vec![
            Finding { slot: 12, kind: FindingKind::Orphaned },
            Finding { slot: 14, kind: FindingKind::Missing },
            Finding { slot: 16, kind: FindingKind::FalseSkip },
            Finding { slot: 18, kind: FindingKind::UnloggedSkip },
        ]
    );
    assert_eq!(c.actionable_count(), 3);
    assert_eq!(c.count(FindingKind::UnloggedSkip), 1);
    assert_eq!(c, classify(window(10, 19), &stored, &produced, &logged), "deterministic");
}

#[test]
fn finding_kinds_map_to_correction_reasons_by_severity() {
    assert!(CorrectionReason::Orphaned > CorrectionReason::Missing);
    assert!(CorrectionReason::Missing > CorrectionReason::FalseSkip);
    assert_eq!(FindingKind::UnloggedSkip.correction_reason(), None);
    assert!(!FindingKind::UnloggedSkip.is_actionable());
    assert_eq!(FindingKind::FalseSkip.correction_reason(), Some(CorrectionReason::FalseSkip));
}

// ---------------------------------------------------------------------------
// decide
// ---------------------------------------------------------------------------

#[test]
fn no_pass_below_twenty_new_safe_slots_and_windows_are_chunked() {
    let limits = Limits { max_chunk: 100, ..Limits::default() };
    assert_eq!(plan_window(100, 119, &limits), None);
    assert_eq!(plan_window(100, 120, &limits), Some(window(101, 120)));
    assert_eq!(plan_window(100, 5_000, &limits), Some(window(101, 200)));
    assert_eq!(plan_window(100, 50, &limits), None, "safe edge below the cursor");
}

/// A window 101..=200 where every slot is ok except `findings`.
fn classified(findings: &[(u64, FindingKind)]) -> prism::reconcile::Classification {
    let mut stored = set(101..=200);
    let mut produced = set(101..=200);
    let mut logged = BTreeSet::new();
    for &(slot, kind) in findings {
        match kind {
            FindingKind::Orphaned => {
                produced.remove(&slot);
            }
            FindingKind::Missing => {
                stored.remove(&slot);
            }
            FindingKind::FalseSkip => {
                stored.remove(&slot);
                logged.insert(slot);
            }
            FindingKind::UnloggedSkip => {
                stored.remove(&slot);
                produced.remove(&slot);
            }
            FindingKind::Mismatched => unreachable!("a block-level finding: see classify_blocks"),
        }
    }
    classify(window(101, 200), &stored, &produced, &logged)
}

#[test]
fn a_clean_window_moves_reconciled_through_to_its_end() {
    let d = decide(&classified(&[]), &state_at(100), 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Nothing);
    assert_eq!(d.state.reconciled_through, 200);
    assert!(d.state.strikes.is_empty());
}

#[test]
fn a_first_finding_is_a_strike_and_holds_reconciled_through_below_it() {
    let d = decide(&classified(&[(150, FindingKind::Orphaned)]), &state_at(100), 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Nothing);
    assert_eq!(d.state.strikes, vec![Strike { slot: 150, kind: FindingKind::Orphaned, seen: 1 }]);
    assert_eq!(d.state.reconciled_through, 149);
}

#[test]
fn a_second_agreeing_pass_confirms_and_rewinds_to_the_lowest_slot() {
    let findings = [
        (150, FindingKind::FalseSkip),
        (160, FindingKind::Orphaned),
        (170, FindingKind::Missing),
    ];
    let first = decide(&classified(&findings), &state_at(100), 300, NOW, &Limits::default());
    let second = decide(&classified(&findings), &first.state, 300, NOW, &Limits::default());
    let Action::Rewind(plan) = second.action else { panic!("{:?}", second.action) };
    assert_eq!(plan.from_slot, 150);
    assert_eq!(plan.to_slot, 170);
    assert_eq!(plan.slots, vec![150, 160, 170]);
    assert_eq!(plan.reason, CorrectionReason::Orphaned, "the most severe kind");
    assert_eq!(plan.affected(), 3);
    assert!(second.state.strikes.iter().all(|s| s.seen == 2));
    assert_eq!(second.state.reconciled_through, 149);
}

#[test]
fn a_changed_kind_starts_over_and_an_ok_slot_drops_its_strike() {
    let first = decide(
        &classified(&[(150, FindingKind::Orphaned), (160, FindingKind::Missing)]),
        &state_at(100),
        300,
        NOW,
        &Limits::default(),
    );
    let second = decide(&classified(&[(150, FindingKind::Missing)]), &first.state, 300, NOW, &Limits::default());
    assert_eq!(second.action, Action::Nothing);
    assert_eq!(second.state.strikes, vec![Strike { slot: 150, kind: FindingKind::Missing, seen: 1 }]);
    assert_eq!(second.state.reconciled_through, 149);
}

#[test]
fn unlogged_skips_are_never_struck_nor_block_progress() {
    let c = classified(&[(150, FindingKind::UnloggedSkip)]);
    let first = decide(&c, &state_at(100), 300, NOW, &Limits::default());
    let second = decide(&c, &first.state, 300, NOW, &Limits::default());
    assert_eq!(second.action, Action::Nothing);
    assert!(second.state.strikes.is_empty());
    assert_eq!(first.state.reconciled_through, 200);
}

#[test]
fn more_than_half_disagreeing_refuses_and_changes_nothing_else() {
    let findings: Vec<_> = (101..=151).map(|s| (s, FindingKind::Orphaned)).collect();
    let state = state_at(100);
    let d = decide(&classified(&findings), &state, 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Refuse { actionable: 51, examined: 100 });
    assert_eq!(d.state.halted, Some(HaltReason::MassMismatch));
    assert_eq!(ReconcileState { halted: None, updated_at: state.updated_at, ..d.state }, state);
}

#[test]
fn exactly_half_or_unlogged_skips_do_not_refuse() {
    let half: Vec<_> = (101..=150).map(|s| (s, FindingKind::Missing)).collect();
    let d = decide(&classified(&half), &state_at(100), 300, NOW, &Limits::default());
    assert_ne!(d.state.halted, Some(HaltReason::MassMismatch));

    let unlogged: Vec<_> = (101..=190).map(|s| (s, FindingKind::UnloggedSkip)).collect();
    let d = decide(&classified(&unlogged), &state_at(100), 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Nothing);
}

#[test]
fn a_slot_corrected_twice_becomes_unresolved_and_blocks_progress() {
    let mut state = state_at(100);
    state.strikes = vec![Strike { slot: 150, kind: FindingKind::Orphaned, seen: 1 }];
    state.attempts = vec![Attempt { slot: 150, count: 2 }];
    let c = classified(&[(150, FindingKind::Orphaned), (180, FindingKind::Missing)]);
    let d = decide(&c, &state, 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Nothing);
    let expected = Unresolved {
        slot: 150,
        kind: FindingKind::Orphaned,
        reason: UnresolvedReason::TooManyCorrections,
    };
    assert_eq!(d.state.unresolved, vec![expected]);
    assert_eq!(d.newly_unresolved, vec![expected]);
    assert_eq!(d.state.reconciled_through, 149);

    // Still unresolved while wrong; dropped once ok, and progress resumes.
    let again = decide(&c, &d.state, 300, NOW, &Limits::default());
    assert_eq!(again.state.unresolved.len(), 1);
    assert!(again.newly_unresolved.is_empty());
    let fixed = decide(&classified(&[]), &again.state, 300, NOW, &Limits::default());
    assert!(fixed.state.unresolved.is_empty());
    assert_eq!(fixed.state.reconciled_through, 200);
}

#[test]
fn a_slot_too_far_below_the_cursor_becomes_unresolved() {
    let mut state = state_at(100);
    state.strikes = vec![Strike { slot: 150, kind: FindingKind::Missing, seen: 1 }];
    let d = decide(&classified(&[(150, FindingKind::Missing)]), &state, 5_151, NOW, &Limits::default());
    assert_eq!(d.action, Action::Nothing);
    assert_eq!(d.state.unresolved[0].reason, UnresolvedReason::TooFarBelowCursor);

    let d = decide(&classified(&[(150, FindingKind::Missing)]), &state, 5_150, NOW, &Limits::default());
    assert!(matches!(d.action, Action::Rewind(_)), "exactly 5,000 below is still acted on");
}

#[test]
fn three_rewinds_in_the_last_hour_make_a_confirmed_finding_wait() {
    let mut state = state_at(100);
    state.strikes = vec![Strike { slot: 150, kind: FindingKind::Missing, seen: 1 }];
    state.rewinds = vec![NOW - 7_200, NOW - 3_000, NOW - 2_000, NOW - 1_000];
    let d = decide(&classified(&[(150, FindingKind::Missing)]), &state, 300, NOW, &Limits::default());
    assert_eq!(d.action, Action::Wait { confirmed: 1, until: NOW - 3_000 + 3_600 });
    assert_eq!(d.state.rewinds, vec![NOW - 3_000, NOW - 2_000, NOW - 1_000], "the old one is pruned");
    assert_eq!(d.state.strikes[0].seen, 2, "stays confirmed for the next pass");

    let later = decide(&classified(&[(150, FindingKind::Missing)]), &d.state, 300, NOW + 601, &Limits::default());
    assert!(matches!(later.action, Action::Rewind(_)));
}

#[test]
fn reconciled_through_never_moves_back_and_attempts_below_it_are_dropped() {
    let mut state = state_at(100);
    state.attempts = vec![Attempt { slot: 120, count: 1 }, Attempt { slot: 250, count: 1 }];
    let d = decide(&classified(&[]), &state, 300, NOW, &Limits::default());
    assert_eq!(d.state.reconciled_through, 200);
    assert_eq!(d.state.attempts, vec![Attempt { slot: 250, count: 1 }]);
}

#[test]
fn invalid_limits_name_the_field() {
    let cases = [
        (Limits { min_window: 0, ..Limits::default() }, "min_window"),
        (Limits { max_chunk: 10, ..Limits::default() }, "max_chunk"),
        (Limits { mass_mismatch_percent: 0, ..Limits::default() }, "mass_mismatch_percent"),
        (Limits { max_rewinds_per_window: 0, ..Limits::default() }, "max_rewinds_per_window"),
    ];
    for (limits, field) in cases {
        let Err(PrismError::ConfigError { field: got, .. }) = limits.validate() else { panic!("{field}") };
        assert_eq!(got, field);
    }
    assert!(Limits::default().validate().is_ok());
}

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

#[test]
fn state_round_trips_and_a_missing_file_is_none() {
    let dir = temp_dir();
    let store = StateStore::new(&dir);
    assert_eq!(store.load().unwrap(), None);
    let mut state = state_at(500);
    state.strikes = vec![Strike { slot: 510, kind: FindingKind::FalseSkip, seen: 1 }];
    state.rewinds = vec![NOW];
    store.save(&state).unwrap();
    assert_eq!(store.load().unwrap(), Some(state));
    let json: serde_json::Value = serde_json::from_slice(&fs::read(dir.join(RECONCILE_FILE_NAME)).unwrap()).unwrap();
    assert_eq!(json["version"], 1);
    assert_eq!(json["strikes"][0]["kind"], "false_skip");
    assert_eq!(json["in_flight"], serde_json::Value::Null);
    assert!(!dir.join("reconcile.json.tmp").exists());
}

fn quarantined_files(dir: &Path) -> Vec<String> {
    fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.starts_with("reconcile.json.corrupt-"))
        .collect()
}

#[test]
fn a_corrupt_file_is_quarantined_and_the_state_starts_afresh() {
    let dir = temp_dir();
    fs::write(dir.join(RECONCILE_FILE_NAME), "{ not json").unwrap();
    assert_eq!(StateStore::new(&dir).load().unwrap(), None);
    assert!(!dir.join(RECONCILE_FILE_NAME).exists());
    assert_eq!(quarantined_files(&dir).len(), 1);
}

#[test]
fn a_file_breaking_the_invariants_is_quarantined() {
    let dir = temp_dir();
    let mut state = state_at(500);
    state.strikes = vec![Strike { slot: 400, kind: FindingKind::Missing, seen: 1 }];
    fs::write(dir.join(RECONCILE_FILE_NAME), serde_json::to_vec(&state).unwrap()).unwrap();
    assert_eq!(StateStore::new(&dir).load().unwrap(), None);
    assert_eq!(quarantined_files(&dir).len(), 1);
}

#[test]
fn a_newer_version_is_refused_and_left_in_place() {
    let dir = temp_dir();
    let mut json = serde_json::to_value(state_at(5)).unwrap();
    json["version"] = json!(2);
    fs::write(dir.join(RECONCILE_FILE_NAME), json.to_string()).unwrap();
    assert!(matches!(StateStore::new(&dir).load(), Err(PrismError::CursorError { .. })));
    assert!(dir.join(RECONCILE_FILE_NAME).exists());
    assert!(quarantined_files(&dir).is_empty());
}

#[test]
fn completing_a_rewind_counts_attempts_drops_strikes_and_records_the_time() {
    let mut state = state_at(100);
    state.strikes = vec![
        Strike { slot: 150, kind: FindingKind::Orphaned, seen: 2 },
        Strike { slot: 190, kind: FindingKind::Missing, seen: 1 },
    ];
    state.attempts = vec![Attempt { slot: 150, count: 1 }];
    state.in_flight = Some(InFlight {
        from_slot: 150,
        to_slot: 160,
        reason: CorrectionReason::Orphaned,
        affected: 2,
        slots: vec![150, 160],
        started_at: NOW,
    });
    state.complete_in_flight(NOW + 5);
    assert_eq!(state.in_flight, None);
    assert_eq!(state.attempts, vec![Attempt { slot: 150, count: 2 }, Attempt { slot: 160, count: 1 }]);
    assert_eq!(state.strikes, vec![Strike { slot: 190, kind: FindingKind::Missing, seen: 1 }]);
    assert_eq!(state.rewinds, vec![NOW + 5]);
}

// ---------------------------------------------------------------------------
// sources
// ---------------------------------------------------------------------------

#[test]
fn skip_lines_parse_or_are_rejected() {
    let line = parse_skip_line("1761040000,350123457,validator_skipped").unwrap();
    assert_eq!((line.at, line.slot, line.reason), (1_761_040_000, 350_123_457, SkipReason::ValidatorSkipped));
    assert!(parse_skip_line("1761040000,350123457,rpc_unavailable\r").is_some());
    for bad in ["", "1,2", "x,2,validator_skipped", "1,y,validator_skipped", "1,2,bogus"] {
        assert_eq!(parse_skip_line(bad), None, "{bad:?}");
    }
}

#[test]
fn the_skip_log_is_followed_incrementally() {
    let dir = temp_dir();
    let path = dir.join(SKIP_LOG_FILE_NAME);
    let mut reader = SkipLogReader::new(&path);
    reader.refresh().unwrap();
    assert!(reader.logged_in(window(0, 1_000)).is_empty(), "no file yet");

    append(&path, &skip_line(10, SkipReason::ValidatorSkipped));
    append(&path, &skip_line(11, SkipReason::RpcUnavailable));
    append(&path, "garbage\n");
    append(&path, "1700000000,12,valid"); // being written
    reader.refresh().unwrap();
    assert_eq!(reader.logged_in(window(0, 1_000)), set([10]));
    assert_eq!(reader.malformed_lines(), 1);

    append(&path, "ator_skipped\n");
    reader.refresh().unwrap();
    assert_eq!(reader.logged_in(window(0, 1_000)), set([10, 12]));
    assert_eq!(reader.logged_in(window(11, 20)), set([12]));

    reader.prune_through(10);
    assert_eq!(reader.logged_in(window(0, 1_000)), set([12]));

    // Replaced by a shorter file: read again from the start, still pruned.
    fs::write(&path, skip_line(10, SkipReason::ValidatorSkipped)).unwrap();
    reader.refresh().unwrap();
    assert!(reader.logged_in(window(0, 1_000)).is_empty());
}

#[test]
fn stored_slots_come_from_the_slots_files_overlapping_the_window() {
    let dir = temp_dir();
    assert_eq!(lowest_stored_slot(&dir).unwrap(), None);
    assert!(stored_slots(&dir, window(0, 100)).unwrap().is_empty());
    store(&dir, &[&[10, 11, 13], &[20, 22], &[40]]);
    assert_eq!(lowest_stored_slot(&dir).unwrap(), Some(10));
    assert_eq!(stored_slots(&dir, window(11, 21)).unwrap(), set([11, 13, 20]));
    assert_eq!(stored_slots(&dir, window(0, 100)).unwrap(), set([10, 11, 13, 20, 22, 40]));
}

#[test]
fn an_unreadable_slots_file_is_an_error_not_an_empty_set() {
    let dir = temp_dir();
    store(&dir, &[&[10, 11]]);
    let file = fs::read_dir(dir.join("solana/slots"))
        .unwrap()
        .flat_map(|d| fs::read_dir(d.unwrap().path()).unwrap())
        .map(|e| e.unwrap().path())
        .next()
        .unwrap();
    fs::write(&file, b"not parquet").unwrap();
    assert!(matches!(stored_slots(&dir, window(0, 100)), Err(PrismError::StorageError { .. })));
}

#[tokio::test]
async fn produced_slots_use_the_finalized_block_list() {
    let chain = FakeChain::new(1..=50, &[7]);
    assert_eq!(produced_slots(chain.as_ref(), window(5, 9)).await.unwrap(), set([5, 6, 8, 9]));
    assert_eq!(chain.range_commitments(), vec![CommitmentLevel::Finalized]);
}

// ---------------------------------------------------------------------------
// The Correction event
// ---------------------------------------------------------------------------

#[test]
fn correction_events_have_a_flat_wire_format() {
    let event = ChangeEvent::correction(100, 140, CorrectionReason::FalseSkip, 3);
    assert_eq!(
        serde_json::to_value(&event).unwrap(),
        json!({"event": "correction", "from_slot": 100, "to_slot": 140, "reason": "false_skip", "affected": 3})
    );
    assert_eq!(ChangeEvent::from_json(&event.to_json()).unwrap(), event);
    assert_eq!(event.kind(), "correction");
    assert_eq!(event.slot(), 100);
    assert!(event.is_correction());
    assert!(event.parquet_paths().is_empty());
    assert_eq!(event.to_string(), "correction from=100 to=140 reason=false_skip affected=3");
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

fn hash(slot: u64) -> String {
    format!("hash-{slot}")
}

fn block(slot: u64) -> ChainBlock {
    block_with_parent(slot, slot.saturating_sub(1))
}

/// A block at `slot` built on `parent`.
fn block_with_parent(slot: u64, parent: u64) -> ChainBlock {
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": hash(parent),
        "blockhash": hash(slot),
        "parentSlot": parent,
        "transactions": [],
        "rewards": [],
        "blockTime": 1_700_000_000 + slot as i64,
        "blockHeight": slot,
    }))
    .unwrap();
    ChainBlock {
        slot_or_height: slot,
        block_id: hash(slot),
        parent_id: hash(parent),
        timestamp: raw.block_time,
        raw: ChainBlockData::Solana(Box::new(raw)),
    }
}

/// A chain whose *finalized* history has a block at every slot of `range`
/// except `skipped`. `orphans` are served at `confirmed` (the fork PRISM
/// first saw) but are skipped at `finalized`.
struct FakeChain {
    tip: u64,
    skipped: HashSet<u64>,
    orphans: Mutex<HashSet<u64>>,
    fail_range: AtomicBool,
    range_commitments: Mutex<Vec<CommitmentLevel>>,
    finalized_reads: Mutex<Vec<u64>>,
}

impl FakeChain {
    fn new(range: std::ops::RangeInclusive<u64>, skipped: &[u64]) -> Arc<Self> {
        Arc::new(Self {
            tip: *range.end(),
            skipped: skipped.iter().copied().collect(),
            orphans: Mutex::default(),
            fail_range: AtomicBool::new(false),
            range_commitments: Mutex::default(),
            finalized_reads: Mutex::default(),
        })
    }

    fn orphan(&self, slot: u64) {
        self.orphans.lock().unwrap().insert(slot);
    }

    fn range_commitments(&self) -> Vec<CommitmentLevel> {
        self.range_commitments.lock().unwrap().clone()
    }

    fn has_block(&self, slot: u64, commitment: CommitmentLevel) -> bool {
        if self.skipped.contains(&slot) {
            return false;
        }
        commitment != CommitmentLevel::Finalized || !self.orphans.lock().unwrap().contains(&slot)
    }
}

#[async_trait]
impl ChainAdapter for FakeChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-fake"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        Ok(ChainTip { slot_or_height: self.tip, block_id: hash(self.tip), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        self.get_block_at(slot, CommitmentLevel::Confirmed).await
    }

    async fn get_block_at(&self, slot: u64, commitment: CommitmentLevel) -> PrismResult<ChainBlock> {
        if commitment == CommitmentLevel::Finalized {
            self.finalized_reads.lock().unwrap().push(slot);
        }
        if self.has_block(slot, commitment) {
            // A block's parent is the previous block of the same chain: on
            // the fork PRISM first saw (confirmed) that may be an orphan, on
            // the finalized chain it never is.
            let parent = (0..slot).rev().find(|s| self.has_block(*s, commitment)).unwrap_or(0);
            Ok(block_with_parent(slot, parent))
        } else {
            Err(PrismError::slot_skipped(slot))
        }
    }

    async fn get_blocks_in_range(&self, from: u64, to: u64, commitment: CommitmentLevel) -> PrismResult<Vec<u64>> {
        self.range_commitments.lock().unwrap().push(commitment);
        if self.fail_range.load(Ordering::SeqCst) {
            return Err(rpc_failure());
        }
        Ok((from..=to.min(self.tip)).filter(|s| self.has_block(*s, commitment)).collect())
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

struct FakeContext {
    flushed: AtomicU64,
    cursor: AtomicU64,
    finalized: AtomicU64,
    genesis: Mutex<String>,
}

impl FakeContext {
    fn at(slot: u64) -> Arc<Self> {
        Arc::new(Self {
            flushed: AtomicU64::new(slot),
            cursor: AtomicU64::new(slot),
            finalized: AtomicU64::new(slot),
            genesis: Mutex::new("genesis".to_owned()),
        })
    }
}

impl ReconcileContext for FakeContext {
    fn durable_last_flush_slot(&self) -> u64 {
        self.flushed.load(Ordering::SeqCst)
    }
    fn cursor_slot(&self) -> u64 {
        self.cursor.load(Ordering::SeqCst)
    }
    fn finalized_slot(&self) -> u64 {
        self.finalized.load(Ordering::SeqCst)
    }
    fn genesis_hash(&self) -> String {
        self.genesis.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct RecordingRewinder {
    calls: Mutex<Vec<u64>>,
    fail: AtomicBool,
}

#[async_trait]
impl Rewind for RecordingRewinder {
    async fn rewind_to(&self, from_slot: u64) -> PrismResult<()> {
        self.calls.lock().unwrap().push(from_slot);
        if self.fail.load(Ordering::SeqCst) {
            return Err(PrismError::config("ingestor", "rewind refused"));
        }
        Ok(())
    }
}

#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
    refuse: AtomicBool,
}

impl RecordingChangeLog {
    fn events(&self) -> Vec<ChangeEvent> {
        self.events.lock().unwrap().clone()
    }

    fn corrections(&self) -> Vec<ChangeEvent> {
        self.events().into_iter().filter(ChangeEvent::is_correction).collect()
    }
}

impl ChangeLog for RecordingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(PrismError::changelog("changelog.jsonl", "append event", "disk full"));
        }
        let mut events = self.events.lock().unwrap();
        events.push(event);
        Ok(events.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

struct Rig {
    dir: PathBuf,
    chain: Arc<FakeChain>,
    ctx: Arc<FakeContext>,
    rewinder: Arc<RecordingRewinder>,
    changelog: Arc<RecordingChangeLog>,
}

impl Rig {
    /// Slots 100..=150 stored and finalized, the chain agreeing on all.
    fn new() -> Self {
        let dir = temp_dir();
        store(&dir, &[&(100..=125).collect::<Vec<_>>(), &(126..=150).collect::<Vec<_>>()]);
        Self {
            dir,
            chain: FakeChain::new(1..=10_000, &[]),
            ctx: FakeContext::at(150),
            rewinder: Arc::default(),
            changelog: Arc::default(),
        }
    }

    fn reconciler(&self) -> Reconciler {
        Reconciler::new(
            ReconcileConfig { interval: Duration::from_millis(5), ..ReconcileConfig::default() },
            Arc::clone(&self.chain) as Arc<dyn ChainAdapter>,
            Arc::clone(&self.ctx) as Arc<dyn ReconcileContext>,
            Arc::clone(&self.rewinder) as Arc<dyn Rewind>,
            Arc::clone(&self.changelog) as Arc<dyn ChangeLog>,
            &self.dir,
        )
        .unwrap()
    }

    fn saved(&self) -> ReconcileState {
        StateStore::new(&self.dir).load().unwrap().unwrap()
    }
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_first_pass_pins_the_cluster_and_starts_at_the_lowest_stored_slot() {
    let rig = Rig::new();
    let mut reconciler = rig.reconciler();
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    let state = rig.saved();
    assert_eq!(state.genesis_hash.as_deref(), Some("genesis"));
    assert_eq!(state.reconciled_through, 150);
    let stats = reconciler.handle().stats();
    assert_eq!(stats.slots_examined, 51);
    assert_eq!(stats.slots_reconciled, 51);
    assert_eq!(reconciler.handle().state(), state);
}

#[tokio::test]
async fn nothing_stored_yet_is_not_ready_and_writes_nothing() {
    let rig = Rig::new();
    fs::remove_dir_all(rig.dir.join("solana")).unwrap();
    let mut reconciler = rig.reconciler();
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::NotReady);
    assert!(!rig.dir.join(RECONCILE_FILE_NAME).exists());
}

#[tokio::test]
async fn only_slots_below_both_the_flush_edge_and_finality_are_examined() {
    let rig = Rig::new();
    rig.ctx.finalized.store(130, Ordering::SeqCst);
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    assert_eq!(rig.saved().reconciled_through, 130);
    // 130..=149 would be 20 new slots, but nothing new is flushed beyond 150.
    rig.ctx.finalized.store(200, Ordering::SeqCst);
    rig.ctx.flushed.store(149, Ordering::SeqCst);
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::NotReady);
}

#[tokio::test]
async fn an_orphan_is_struck_then_corrected_on_the_next_pass() {
    let rig = Rig::new();
    rig.chain.orphan(120);
    let mut reconciler = rig.reconciler();

    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    assert_eq!(rig.saved().strikes, vec![Strike { slot: 120, kind: FindingKind::Orphaned, seen: 1 }]);
    assert_eq!(rig.saved().reconciled_through, 119);
    assert!(rig.rewinder.calls.lock().unwrap().is_empty());

    let outcome = reconciler.run_pass().await.unwrap();
    assert_eq!(outcome, PassOutcome::Rewound { from_slot: 120, to_slot: 120 });
    assert_eq!(*rig.rewinder.calls.lock().unwrap(), vec![120]);
    assert_eq!(
        rig.changelog.corrections(),
        vec![ChangeEvent::correction(120, 120, CorrectionReason::Orphaned, 1)]
    );
    let state = rig.saved();
    assert_eq!(state.in_flight, None);
    assert!(state.strikes.is_empty());
    assert_eq!(state.attempts, vec![Attempt { slot: 120, count: 1 }]);
    assert_eq!(state.rewinds.len(), 1);
    assert_eq!(reconciler.handle().stats().rewinds, 1);
}

#[tokio::test]
async fn a_failed_source_concludes_nothing() {
    let rig = Rig::new();
    rig.chain.orphan(120);
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    let before = rig.saved();

    rig.chain.fail_range.store(true, Ordering::SeqCst);
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Inconclusive);
    assert_eq!(rig.saved(), before);
    let stats = reconciler.handle().stats();
    assert_eq!(stats.passes_inconclusive, 1);
    assert!(stats.last_error.unwrap().contains("getBlocks"));

    // The strike survives the failure: the next good pass confirms it.
    rig.chain.fail_range.store(false, Ordering::SeqCst);
    assert!(matches!(reconciler.run_pass().await.unwrap(), PassOutcome::Rewound { .. }));
}

#[tokio::test]
async fn a_false_skip_is_found_from_the_skip_log() {
    let rig = Rig::new();
    // Slot 130 is not stored, the chain has it, a skip was logged.
    fs::remove_dir_all(rig.dir.join("solana")).unwrap();
    store(&rig.dir, &[&(100..=150).filter(|s| *s != 130).collect::<Vec<_>>()]);
    append(&rig.dir.join(SKIP_LOG_FILE_NAME), &skip_line(130, SkipReason::ValidatorSkipped));
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    assert_eq!(rig.saved().strikes[0].kind, FindingKind::FalseSkip);
}

#[tokio::test]
async fn a_different_cluster_halts_for_good() {
    let rig = Rig::new();
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    *rig.ctx.genesis.lock().unwrap() = "other".to_owned();
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Halted(HaltReason::ClusterMismatch));
    assert_eq!(rig.saved().halted, Some(HaltReason::ClusterMismatch));

    // A restarted reconciler stays halted, and `run` returns at once.
    let reconciler = rig.reconciler();
    tokio::time::timeout(Duration::from_secs(5), reconciler.run(Default::default()))
        .await
        .expect("a halted reconciler stops")
        .unwrap();
}

#[tokio::test]
async fn mass_mismatch_halts_the_loop_without_rewinding() {
    let rig = Rig::new();
    for slot in 100..=140 {
        rig.chain.orphan(slot);
    }
    let reconciler = rig.reconciler();
    let handle = reconciler.handle();
    tokio::time::timeout(Duration::from_secs(5), reconciler.run(Default::default()))
        .await
        .expect("the loop stops")
        .unwrap();
    assert_eq!(handle.state().halted, Some(HaltReason::MassMismatch));
    assert_eq!(rig.saved().reconciled_through, 99, "nothing else changed");
    assert!(rig.rewinder.calls.lock().unwrap().is_empty());
    assert!(rig.changelog.events().is_empty());
}

#[tokio::test]
async fn a_change_log_failure_aborts_the_rewind_and_leaves_it_in_flight() {
    let rig = Rig::new();
    rig.chain.orphan(120);
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    rig.changelog.refuse.store(true, Ordering::SeqCst);
    assert!(matches!(reconciler.run_pass().await, Err(PrismError::ChangeLogError { .. })));
    assert!(rig.rewinder.calls.lock().unwrap().is_empty(), "no rewind without its announcement");
    let in_flight = rig.saved().in_flight.expect("left for recovery");
    assert_eq!((in_flight.from_slot, in_flight.slots), (120, vec![120]));
}

#[tokio::test]
async fn a_rewind_left_in_flight_is_finished_by_the_next_pass() {
    let rig = Rig::new();
    rig.chain.orphan(120);
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    rig.rewinder.fail.store(true, Ordering::SeqCst);
    assert!(reconciler.run_pass().await.is_err());
    assert!(rig.saved().in_flight.is_some());

    rig.rewinder.fail.store(false, Ordering::SeqCst);
    let mut reconciler = rig.reconciler();
    reconciler.run_pass().await.unwrap();
    assert_eq!(*rig.rewinder.calls.lock().unwrap(), vec![120, 120]);
    let state = rig.saved();
    assert_eq!(state.in_flight, None);
    assert_eq!(state.attempts, vec![Attempt { slot: 120, count: 1 }]);
}

#[test]
fn recovery_lowers_the_cursor_deletes_the_files_and_completes_the_rewind() {
    let dir = temp_dir();
    store(&dir, &[&[100, 101, 102], &[103, 104, 105]]);
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    cursor.record_processed(100, "h");
    cursor.record_processed(105, "h");
    cursor.record_flush(105).unwrap();

    let changelog = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &changelog).unwrap(), None, "no state file");

    let mut state = state_at(99);
    state.strikes = vec![Strike { slot: 102, kind: FindingKind::Orphaned, seen: 2 }];
    state.in_flight = Some(InFlight {
        from_slot: 102,
        to_slot: 102,
        reason: CorrectionReason::Orphaned,
        affected: 1,
        slots: vec![102],
        started_at: NOW,
    });
    StateStore::new(&dir).save(&state).unwrap();

    assert_eq!(Reconciler::recover(&dir, &cursor, &changelog).unwrap(), Some(102));
    // The correction is announced again: the crash may have come before it.
    assert_eq!(changelog.corrections(), vec![ChangeEvent::correction(102, 102, CorrectionReason::Orphaned, 1)]);
    assert_eq!(cursor.state().last_flush_slot, 101);
    assert_eq!(cursor.durable_flush_slot(), 101);
    assert_eq!(IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap().state().last_flush_slot, 101);
    assert_eq!(stored_slots(&dir, window(0, 1_000)).unwrap(), set([100, 101]));
    let state = StateStore::new(&dir).load().unwrap().unwrap();
    assert_eq!(state.in_flight, None);
    assert_eq!(state.attempts, vec![Attempt { slot: 102, count: 1 }]);
    assert!(state.strikes.is_empty());

    // Idempotent: nothing left to do, nothing announced again.
    assert_eq!(Reconciler::recover(&dir, &cursor, &changelog).unwrap(), None);
    assert_eq!(changelog.corrections().len(), 1);
}

#[test]
fn rewinding_the_cursor_never_raises_it_and_refuses_the_start_of_the_chain() {
    let dir = temp_dir();
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    cursor.record_processed(100, "h");
    cursor.record_processed(110, "h");
    cursor.record_flush(105).unwrap();
    cursor.rewind(200).unwrap();
    let state = cursor.state();
    assert_eq!((state.last_flush_slot, state.last_processed_slot), (105, 105));
    cursor.rewind(103).unwrap();
    assert_eq!(cursor.resume_slot(), Some(103));
    assert!(matches!(cursor.rewind(1), Err(PrismError::ConfigError { .. })));
}

#[test]
fn invalid_reconcile_config_is_rejected() {
    let rig = Rig::new();
    let config = ReconcileConfig { interval: Duration::ZERO, ..ReconcileConfig::default() };
    let err = Reconciler::new(
        config,
        Arc::clone(&rig.chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&rig.ctx) as Arc<dyn ReconcileContext>,
        Arc::clone(&rig.rewinder) as Arc<dyn Rewind>,
        Arc::clone(&rig.changelog) as Arc<dyn ChangeLog>,
        &rig.dir,
    )
    .unwrap_err();
    assert!(matches!(err, PrismError::ConfigError { field, .. } if field == "interval"));
}

// ---------------------------------------------------------------------------
// With a real ingestor
// ---------------------------------------------------------------------------

/// Waits up to 10 s for `done`.
async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn new_slots(events: &[ChangeEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::New { slot, .. } => Some(*slot),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_orphan_written_by_the_ingestor_is_rewound_and_re_ingested_at_finalized() {
    let dir = temp_dir();
    // The ingestor first sees slot 120 on a fork; the finalized chain skips it.
    let chain = FakeChain::new(1..=150, &[]);
    chain.orphan(120);
    let watermark = Arc::new(FinalityWatermark::new());
    let tracker = Arc::new(CommitmentTracker::from_watermark(Arc::clone(&watermark)));
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig {
        start_slot: Some(100),
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_millis(20),
        fetcher: SlotFetcherConfig { max_retries: 1, base_delay_ms: 1, max_delay_ms: 2, request_timeout_ms: 1_000 },
        ..IngestorConfig::new(&dir)
    };
    let ingestor = Arc::new(
        Ingestor::new(
            Arc::clone(&chain) as Arc<dyn ChainAdapter>,
            tracker,
            Arc::clone(&changelog) as Arc<dyn ChangeLog>,
            config,
        )
        .unwrap(),
    );
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    let ctx = Arc::new(ingestor.reconcile_context("genesis"));
    wait_for("the first ingest", || ctx.durable_last_flush_slot() >= 150).await;
    assert!(stored_slots(&dir, window(120, 120)).unwrap().contains(&120), "the orphan is stored");
    let _ = watermark.advance_finalized(150, &hash(150));

    let mut reconciler = Reconciler::new(
        ReconcileConfig::default(),
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&ctx) as Arc<dyn ReconcileContext>,
        Arc::new(ingestor.rewinder()),
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        &dir,
    )
    .unwrap();
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    assert_eq!(
        reconciler.run_pass().await.unwrap(),
        PassOutcome::Rewound { from_slot: 120, to_slot: 120 }
    );

    wait_for("the re-ingest", || ctx.durable_last_flush_slot() >= 150).await;
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    assert_eq!(reconciler.state().reconciled_through, 150);
    assert!(reconciler.state().strikes.is_empty());

    // On disk: every slot once, the orphan gone.
    let expected: BTreeSet<u64> = (100..=150).filter(|s| *s != 120).collect();
    assert_eq!(stored_slots(&dir, window(0, 1_000)).unwrap(), expected);
    // The re-ingest read the orphan at finalized, and logged it as skipped.
    assert!(chain.finalized_reads.lock().unwrap().contains(&120));
    let skip_log = fs::read_to_string(dir.join(SKIP_LOG_FILE_NAME)).unwrap();
    assert!(skip_log.contains(",120,validator_skipped"));

    // In the change log, the correction precedes the re-announcement.
    let events = changelog.events();
    let at = events.iter().position(ChangeEvent::is_correction).unwrap();
    assert_eq!(events[at], ChangeEvent::correction(120, 120, CorrectionReason::Orphaned, 1));
    let after = new_slots(&events[at..]);
    assert_eq!(after, (121..=150).collect::<Vec<_>>());
    assert!(new_slots(&events[..at]).contains(&120));

    ingestor.shutdown();
    let report = tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
    assert_eq!(report.rewinds, 1);
    assert_eq!(report.first_slot, Some(100));
    assert_eq!(report.cursor.last_flush_slot, 150);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_run_while_running_is_refused() {
    let dir = temp_dir();
    let chain = FakeChain::new(1..=1_000_000, &[]);
    let changelog = Arc::new(RecordingChangeLog::default());
    let ingestor = Arc::new(
        Ingestor::new(
            chain as Arc<dyn ChainAdapter>,
            Arc::new(CommitmentTracker::from_watermark(Arc::new(FinalityWatermark::new()))),
            changelog as Arc<dyn ChangeLog>,
            IngestorConfig { start_slot: Some(10), ..IngestorConfig::new(&dir) },
        )
        .unwrap(),
    );
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    wait_for("the pipeline to start", || ingestor.cursor().state().last_processed_slot >= 10).await;
    assert!(matches!(ingestor.run().await, Err(PrismError::ConfigError { .. })));
    ingestor.shutdown();
    tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
}
