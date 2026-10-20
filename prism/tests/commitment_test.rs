//! Tests for `prism::commitment`.
//!
//! The chain is a [`ScriptedAdapter`] whose tips are scripted per commitment,
//! and the change log a [`RecordingChangeLog`] that keeps what it was given.

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, BooleanArray, UInt64Array};
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::commitment::{
    Advance, CommitmentTracker, DEFAULT_POLL_INTERVAL, FinalityPoller, FinalityWatermark,
    IS_FINALIZED_COLUMN, WatermarkSnapshot, is_finalized_field,
};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::ChangeEvent;
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, UdmRow,
};
use tokio_util::sync::CancellationToken;

use CommitmentLevel::{Confirmed, Finalized};

// ---------------------------------------------------------------------------
// Fakes and helpers
// ---------------------------------------------------------------------------

fn hash(slot: u64) -> String {
    format!("hash-{slot}")
}

fn final_event(slot: u64, previous_slot: u64) -> ChangeEvent {
    ChangeEvent::finalized(slot, hash(slot), previous_slot)
}

/// One scripted answer to `get_tip`.
#[derive(Debug, Clone)]
enum Reply {
    /// A tip at `slot` with blockhash [`hash`]`(slot)`.
    Tip(u64),
    /// A tip at `slot` with the given blockhash.
    TipWithHash(u64, &'static str),
    /// A tip labelled with the other commitment than the one asked for.
    Mislabeled(u64),
    /// An RPC failure.
    Fail,
    /// Never answers.
    Hang,
}

/// A chain adapter whose tips are scripted per commitment. When a script is
/// down to its last reply, that reply repeats.
#[derive(Default)]
struct ScriptedAdapter {
    confirmed: Mutex<VecDeque<Reply>>,
    finalized: Mutex<VecDeque<Reply>>,
    calls: Mutex<Vec<CommitmentLevel>>,
}

impl ScriptedAdapter {
    fn new(
        confirmed: impl IntoIterator<Item = Reply>,
        finalized: impl IntoIterator<Item = Reply>,
    ) -> Arc<Self> {
        Arc::new(Self {
            confirmed: Mutex::new(confirmed.into_iter().collect()),
            finalized: Mutex::new(finalized.into_iter().collect()),
            calls: Mutex::default(),
        })
    }

    fn calls(&self) -> Vec<CommitmentLevel> {
        self.calls.lock().unwrap().clone()
    }

    fn next_reply(&self, commitment: CommitmentLevel) -> Reply {
        let script = match commitment {
            Finalized => &self.finalized,
            _ => &self.confirmed,
        };
        let mut script = script.lock().unwrap();
        if script.len() > 1 {
            script.pop_front().unwrap()
        } else {
            script.front().cloned().unwrap_or(Reply::Fail)
        }
    }
}

fn chain_tip(slot: u64, blockhash: &str, commitment: CommitmentLevel) -> ChainTip {
    ChainTip {
        slot_or_height: slot,
        block_id: blockhash.to_owned(),
        commitment,
    }
}

#[async_trait]
impl ChainAdapter for ScriptedAdapter {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-test"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        self.calls.lock().unwrap().push(commitment);
        match self.next_reply(commitment) {
            Reply::Tip(slot) => Ok(chain_tip(slot, &hash(slot), commitment)),
            Reply::TipWithHash(slot, blockhash) => Ok(chain_tip(slot, blockhash, commitment)),
            Reply::Mislabeled(slot) => {
                let other = if commitment == Finalized { Confirmed } else { Finalized };
                Ok(chain_tip(slot, &hash(slot), other))
            }
            Reply::Fail => Err(PrismError::rate_limited(None)),
            Reply::Hang => std::future::pending().await,
        }
    }

    async fn get_block(&self, _slot_or_height: u64) -> PrismResult<ChainBlock> {
        unimplemented!("not used by the commitment module")
    }

    async fn get_accounts(&self, _addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        unimplemented!("not used by the commitment module")
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

/// A change log that records every event, after failing the first
/// `failures_left` emits.
#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
    failures_left: AtomicUsize,
}

impl RecordingChangeLog {
    fn new() -> Arc<Self> {
        Arc::default()
    }

    fn failing(times: usize) -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::default(),
            failures_left: AtomicUsize::new(times),
        })
    }

    fn events(&self) -> Vec<ChangeEvent> {
        self.events.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChangeLog for RecordingChangeLog {
    async fn emit(&self, event: ChangeEvent) -> PrismResult<()> {
        let fail = self
            .failures_left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if fail {
            return Err(PrismError::cursor(
                "changelog.jsonl",
                io::Error::other("disk full"),
            ));
        }
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

/// A poller over a fresh watermark.
fn poller(
    adapter: &Arc<ScriptedAdapter>,
    changelog: &Arc<RecordingChangeLog>,
) -> (FinalityPoller, Arc<FinalityWatermark>) {
    let watermark = Arc::new(FinalityWatermark::new());
    let poller = FinalityPoller::new(
        adapter.clone(),
        watermark.clone(),
        changelog.clone(),
        DEFAULT_POLL_INTERVAL,
        CancellationToken::new(),
    )
    .unwrap();
    (poller, watermark)
}

/// xorshift64*: deterministic pseudo-random numbers without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// ---------------------------------------------------------------------------
// FinalityWatermark
// ---------------------------------------------------------------------------

#[test]
fn a_new_watermark_has_observed_nothing() {
    let wm = FinalityWatermark::new();
    assert_eq!(wm.confirmed_slot(), 0);
    assert_eq!(wm.finalized_slot(), 0);
    assert_eq!(wm.lag(), 0);
    assert_eq!(wm.last_updated(), None);
    assert_eq!(wm.confirmed_tip(), (0, String::new()));
    assert_eq!(wm.finalized_tip(), (0, String::new()));
    assert_eq!(wm.snapshot(), WatermarkSnapshot::default());
    // No evidence yet, so not even slot 0 is claimed final.
    assert!(!wm.is_finalized(0));
    assert!(!wm.is_finalized(1));
}

#[test]
fn advance_confirmed_records_slot_and_blockhash() {
    let wm = FinalityWatermark::new();
    assert_eq!(
        wm.advance_confirmed(100, "h100"),
        Advance::Advanced { previous: 0 }
    );
    assert_eq!(
        wm.advance_confirmed(105, "h105"),
        Advance::Advanced { previous: 100 }
    );
    assert_eq!(wm.confirmed_tip(), (105, "h105".to_owned()));
    assert_eq!(wm.finalized_slot(), 0, "confirmed never moves finalized");
    assert!(wm.last_updated().is_some());
}

#[test]
fn advance_reports_stale_unchanged_and_conflicting_observations() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_confirmed(100, "h100");
    let _ = wm.advance_finalized(60, "h60");

    assert_eq!(wm.advance_confirmed(99, "h99"), Advance::Stale { current: 100 });
    assert_eq!(wm.advance_confirmed(100, "h100"), Advance::Unchanged);
    assert_eq!(
        wm.advance_confirmed(100, "fork"),
        Advance::Conflict {
            recorded: "h100".into()
        }
    );
    assert_eq!(wm.advance_finalized(59, "h59"), Advance::Stale { current: 60 });
    assert_eq!(wm.advance_finalized(60, "h60"), Advance::Unchanged);
    assert_eq!(
        wm.advance_finalized(60, "fork"),
        Advance::Conflict {
            recorded: "h60".into()
        }
    );

    // Nothing that was rejected changed the recorded pairs.
    assert_eq!(wm.confirmed_tip(), (100, "h100".to_owned()));
    assert_eq!(wm.finalized_tip(), (60, "h60".to_owned()));
}

#[test]
fn slot_zero_is_never_recorded() {
    let wm = FinalityWatermark::new();
    assert_eq!(wm.advance_confirmed(0, "genesis"), Advance::Unchanged);
    assert_eq!(wm.advance_finalized(0, "genesis"), Advance::Unchanged);
    assert_eq!(wm.snapshot(), WatermarkSnapshot::default());
    assert!(!wm.is_finalized(0));
}

#[test]
fn advance_finalized_raises_confirmed_when_it_is_behind() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_confirmed(100, "h100");
    assert_eq!(
        wm.advance_finalized(120, "h120"),
        Advance::Advanced { previous: 0 }
    );
    assert_eq!(wm.finalized_tip(), (120, "h120".to_owned()));
    assert_eq!(wm.confirmed_tip(), (120, "h120".to_owned()));
    assert_eq!(wm.lag(), 0);
}

#[test]
fn advance_finalized_leaves_a_confirmed_tip_that_is_ahead_alone() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_confirmed(150, "h150");
    let _ = wm.advance_finalized(118, "h118");
    assert_eq!(wm.confirmed_tip(), (150, "h150".to_owned()));
    assert_eq!(wm.finalized_tip(), (118, "h118".to_owned()));
    assert_eq!(wm.lag(), 32);
}

#[test]
fn is_finalized_is_inclusive_of_the_watermark() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_confirmed(150, "h150");
    let _ = wm.advance_finalized(118, "h118");
    assert!(wm.is_finalized(0));
    assert!(wm.is_finalized(117));
    assert!(wm.is_finalized(118));
    assert!(!wm.is_finalized(119));
    assert!(!wm.is_finalized(150), "confirmed is not finalized");
}

#[test]
fn is_finalized_column_has_one_value_per_slot_in_input_order() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_finalized(10, "h10");
    let column = wm.is_finalized_column([12, 3, 10, 11, 10]);
    assert_eq!(
        column,
        BooleanArray::from(vec![false, true, true, false, true])
    );
    assert_eq!(column.null_count(), 0);
    assert!(wm.is_finalized_column([]).is_empty());
}

#[test]
fn snapshot_copies_every_field() {
    let wm = FinalityWatermark::new();
    let _ = wm.advance_confirmed(150, "h150");
    let _ = wm.advance_finalized(118, "h118");
    let snap = wm.snapshot();
    assert_eq!(snap.confirmed_slot, 150);
    assert_eq!(snap.confirmed_blockhash, "h150");
    assert_eq!(snap.finalized_slot, 118);
    assert_eq!(snap.finalized_blockhash, "h118");
    assert_eq!(snap.last_updated, wm.last_updated());
    assert_eq!(snap.lag(), 32);
}

#[test]
fn is_finalized_field_is_a_non_nullable_boolean() {
    let field = is_finalized_field();
    assert_eq!(field.name(), IS_FINALIZED_COLUMN);
    assert_eq!(field.name(), "is_finalized");
    assert_eq!(field.data_type(), &DataType::Boolean);
    assert!(!field.is_nullable());
}

// ---------------------------------------------------------------------------
// FinalityPoller
// ---------------------------------------------------------------------------

#[test]
fn poller_rejects_a_zero_interval() {
    let adapter = ScriptedAdapter::new([], []);
    let changelog = RecordingChangeLog::new();
    let err = FinalityPoller::new(
        adapter,
        Arc::new(FinalityWatermark::new()),
        changelog,
        Duration::ZERO,
        CancellationToken::new(),
    )
    .err()
    .expect("zero interval is rejected");
    assert!(matches!(&err, PrismError::ConfigError { field, .. } if field == "poll_interval"));
}

#[test]
fn default_poll_interval_is_five_seconds() {
    assert_eq!(DEFAULT_POLL_INTERVAL, Duration::from_secs(5));
}

#[tokio::test]
async fn poll_once_fetches_confirmed_then_finalized() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118)]);
    let changelog = RecordingChangeLog::new();
    let (mut poller, _) = poller(&adapter, &changelog);
    poller.poll_once().await;
    assert_eq!(adapter.calls(), vec![Confirmed, Finalized]);
}

#[tokio::test]
async fn poll_once_advances_both_watermarks_and_emits_final() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118)]);
    let changelog = RecordingChangeLog::new();
    let (mut poller, wm) = poller(&adapter, &changelog);
    poller.poll_once().await;

    assert_eq!(wm.confirmed_tip(), (150, hash(150)));
    assert_eq!(wm.finalized_tip(), (118, hash(118)));
    assert_eq!(changelog.events(), vec![final_event(118, 0)]);
}

#[tokio::test]
async fn final_is_emitted_once_per_advance_with_the_previous_watermark() {
    let adapter = ScriptedAdapter::new(
        [Reply::Tip(150), Reply::Tip(155), Reply::Tip(160)],
        [Reply::Tip(118), Reply::Tip(118), Reply::Tip(128)],
    );
    let changelog = RecordingChangeLog::new();
    let (mut poller, _) = poller(&adapter, &changelog);
    for _ in 0..4 {
        poller.poll_once().await;
    }
    assert_eq!(
        changelog.events(),
        vec![final_event(118, 0), final_event(128, 118)],
        "no event for a poll where finalized did not move"
    );
}

#[tokio::test]
async fn a_failed_confirmed_fetch_does_not_block_finalized() {
    let adapter = ScriptedAdapter::new([Reply::Fail], [Reply::Tip(118)]);
    let changelog = RecordingChangeLog::new();
    let (mut poller, wm) = poller(&adapter, &changelog);
    poller.poll_once().await;
    assert_eq!(wm.finalized_slot(), 118);
    assert_eq!(wm.confirmed_slot(), 118, "finalized implies confirmed");
    assert_eq!(changelog.events(), vec![final_event(118, 0)]);
}

#[tokio::test]
async fn a_failed_finalized_fetch_still_advances_confirmed() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Fail]);
    let changelog = RecordingChangeLog::new();
    let (mut poller, wm) = poller(&adapter, &changelog);
    poller.poll_once().await;
    assert_eq!(wm.confirmed_slot(), 150);
    assert_eq!(wm.finalized_slot(), 0);
    assert!(changelog.events().is_empty());
}

#[tokio::test]
async fn a_tip_at_the_wrong_commitment_is_ignored() {
    // Asked for confirmed, told finalized (and the other way round). Taking
    // either at face value would misreport finality.
    let adapter = ScriptedAdapter::new([Reply::Mislabeled(150)], [Reply::Mislabeled(118)]);
    let changelog = RecordingChangeLog::new();
    let (mut poller, wm) = poller(&adapter, &changelog);
    poller.poll_once().await;
    assert_eq!(wm.snapshot(), WatermarkSnapshot::default());
    assert!(changelog.events().is_empty());
}

#[tokio::test]
async fn a_failed_emit_is_retried_and_covers_the_whole_gap() {
    let adapter = ScriptedAdapter::new(
        [Reply::Tip(150), Reply::Tip(160)],
        [Reply::Tip(118), Reply::Tip(128)],
    );
    let changelog = RecordingChangeLog::failing(1);
    let (mut poller, wm) = poller(&adapter, &changelog);

    poller.poll_once().await;
    assert_eq!(wm.finalized_slot(), 118, "the watermark is not held back");
    assert!(changelog.events().is_empty());

    poller.poll_once().await;
    assert_eq!(changelog.events(), vec![final_event(128, 0)]);
}

#[tokio::test]
async fn a_failed_emit_is_retried_even_if_finalized_does_not_move() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118)]);
    let changelog = RecordingChangeLog::failing(1);
    let (mut poller, _) = poller(&adapter, &changelog);
    poller.poll_once().await;
    poller.poll_once().await;
    assert_eq!(changelog.events(), vec![final_event(118, 0)]);
}

#[tokio::test]
async fn a_poller_over_a_seeded_watermark_does_not_reannounce_it() {
    let watermark = Arc::new(FinalityWatermark::new());
    let _ = watermark.advance_finalized(118, &hash(118));
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118), Reply::Tip(128)]);
    let changelog = RecordingChangeLog::new();
    let mut poller = FinalityPoller::new(
        adapter,
        watermark,
        changelog.clone(),
        DEFAULT_POLL_INTERVAL,
        CancellationToken::new(),
    )
    .unwrap();

    poller.poll_once().await;
    assert!(changelog.events().is_empty());
    poller.poll_once().await;
    assert_eq!(changelog.events(), vec![final_event(128, 118)]);
}

// ---------------------------------------------------------------------------
// CommitmentTracker
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn tracker_polls_immediately_then_every_interval() {
    let adapter = ScriptedAdapter::new(
        [Reply::Tip(132), Reply::Tip(140), Reply::Tip(150)],
        [Reply::Tip(100), Reply::Tip(108), Reply::Tip(118)],
    );
    let changelog = RecordingChangeLog::new();
    let mut tracker = CommitmentTracker::spawn_with_interval(
        adapter.clone(),
        changelog.clone(),
        Duration::from_secs(5),
    )
    .unwrap();
    assert!(tracker.is_running());

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(adapter.calls().len(), 2, "first poll at t=0");
    assert_eq!(tracker.confirmed_slot(), 132);
    assert_eq!(tracker.finalized_slot(), 100);

    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(adapter.calls().len(), 6, "polls at t=5 and t=10");
    assert_eq!(tracker.confirmed_slot(), 150);
    assert_eq!(tracker.finalized_slot(), 118);
    assert_eq!(tracker.lag(), 32);
    assert!(tracker.is_finalized(118));
    assert!(!tracker.is_finalized(119));
    assert_eq!(
        changelog.events(),
        vec![
            final_event(100, 0),
            final_event(108, 100),
            final_event(118, 108)
        ]
    );

    tracker.shutdown().await;
    assert!(!tracker.is_running());
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(adapter.calls().len(), 6, "no polling after shutdown");
    assert_eq!(tracker.finalized_slot(), 118, "still readable after shutdown");
    tracker.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn spawn_uses_the_default_interval() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118)]);
    let mut tracker = CommitmentTracker::spawn(adapter.clone(), RecordingChangeLog::new());
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert_eq!(adapter.calls().len(), 2);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(adapter.calls().len(), 4);
    tracker.shutdown().await;
}

#[tokio::test]
async fn spawn_with_interval_rejects_zero() {
    let adapter = ScriptedAdapter::new([], []);
    let result =
        CommitmentTracker::spawn_with_interval(adapter, RecordingChangeLog::new(), Duration::ZERO);
    assert!(matches!(result, Err(PrismError::ConfigError { .. })));
}

#[tokio::test(start_paused = true)]
async fn shutdown_does_not_wait_for_a_hanging_fetch() {
    let adapter = ScriptedAdapter::new([Reply::Hang], [Reply::Tip(118)]);
    let mut tracker = CommitmentTracker::spawn(adapter.clone(), RecordingChangeLog::new());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(adapter.calls(), vec![Confirmed], "stuck in the first fetch");

    tokio::time::timeout(Duration::from_secs(1), tracker.shutdown())
        .await
        .expect("shutdown returns while the fetch hangs");
    assert!(!tracker.is_running());
}

#[tokio::test(start_paused = true)]
async fn dropping_the_tracker_stops_the_poller() {
    let adapter = ScriptedAdapter::new([Reply::Tip(150)], [Reply::Tip(118)]);
    let tracker = CommitmentTracker::spawn(adapter.clone(), RecordingChangeLog::new());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(adapter.calls().len(), 2);

    drop(tracker);
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert_eq!(adapter.calls().len(), 2);
}

#[tokio::test]
async fn from_watermark_wraps_an_externally_driven_watermark() {
    let watermark = Arc::new(FinalityWatermark::new());
    let mut tracker = CommitmentTracker::from_watermark(watermark.clone());
    assert!(!tracker.is_running());
    assert!(Arc::ptr_eq(tracker.watermark(), &watermark));

    let _ = watermark.advance_confirmed(150, "h150");
    let _ = watermark.advance_finalized(118, "h118");
    assert_eq!(tracker.confirmed_slot(), 150);
    assert_eq!(tracker.finalized_slot(), 118);
    assert_eq!(tracker.lag(), 32);
    assert!(tracker.is_finalized(118));
    assert_eq!(
        tracker.is_finalized_column([118, 119]),
        BooleanArray::from(vec![true, false])
    );
    assert_eq!(tracker.snapshot(), watermark.snapshot());

    tracker.shutdown().await;
}

// ---------------------------------------------------------------------------
// Required check 1: watermark monotonicity
// ---------------------------------------------------------------------------

/// Neither watermark ever moves backwards, and finalized never overtakes
/// confirmed, under every condition the watermark can meet:
///
/// 1. an adversarial sequence of offers (lower, equal, higher, zero,
///    conflicting blockhashes), checked step by step against a reference
///    model;
/// 2. the `u64` extremes;
/// 3. many threads advancing it at once while others read it;
/// 4. the poller fed by a misbehaving RPC: lagging nodes, failures,
///    mislabeled commitments, conflicting blockhashes and a failing change
///    log.
#[tokio::test]
async fn watermark_never_moves_backwards_under_any_condition() {
    // 1. Adversarial sequence against a reference model.
    {
        let wm = FinalityWatermark::new();
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        let mut model_confirmed = (0_u64, String::new());
        let mut model_finalized = (0_u64, String::new());

        for step in 0..50_000 {
            let before = wm.snapshot();
            // Offers cluster around one of the tips so that stale, equal and
            // higher slots are all common.
            let base = if rng.below(2) == 0 {
                before.confirmed_slot
            } else {
                before.finalized_slot
            };
            let slot = match rng.below(5) {
                0 => base.saturating_sub(rng.below(64)),
                1 => base,
                2 | 3 => base + rng.below(64),
                _ => rng.below(base + 1),
            };
            // Few distinct hashes per slot, so equal-slot conflicts happen.
            let blockhash = format!("{slot}-{}", rng.below(3));
            let finalized_offer = rng.below(3) == 0;

            let outcome = if finalized_offer {
                wm.advance_finalized(slot, &blockhash)
            } else {
                wm.advance_confirmed(slot, &blockhash)
            };
            let after = wm.snapshot();

            assert!(
                after.confirmed_slot >= before.confirmed_slot,
                "step {step}: confirmed moved backwards {} -> {}",
                before.confirmed_slot,
                after.confirmed_slot
            );
            assert!(
                after.finalized_slot >= before.finalized_slot,
                "step {step}: finalized moved backwards {} -> {}",
                before.finalized_slot,
                after.finalized_slot
            );
            assert!(
                after.finalized_slot <= after.confirmed_slot,
                "step {step}: finalized {} overtook confirmed {}",
                after.finalized_slot,
                after.confirmed_slot
            );

            // Reference model: a pair is replaced only by a strictly higher
            // slot; a finalized offer also raises confirmed.
            let target = if finalized_offer { model_finalized.0 } else { model_confirmed.0 };
            if finalized_offer && slot > model_finalized.0 {
                if slot > model_confirmed.0 {
                    model_confirmed = (slot, blockhash.clone());
                }
                model_finalized = (slot, blockhash.clone());
            } else if !finalized_offer && slot > model_confirmed.0 {
                model_confirmed = (slot, blockhash.clone());
            }
            assert_eq!(
                (after.confirmed_slot, after.confirmed_blockhash.clone()),
                model_confirmed,
                "step {step}: confirmed"
            );
            assert_eq!(
                (after.finalized_slot, after.finalized_blockhash.clone()),
                model_finalized,
                "step {step}: finalized"
            );
            assert_eq!(
                outcome.is_advanced(),
                slot > target,
                "step {step}: {outcome:?} for slot {slot} over {target}"
            );
            if slot < target {
                assert_eq!(outcome, Advance::Stale { current: target }, "step {step}");
            }
        }
        assert!(model_finalized.0 > 1_000, "the sequence made real progress");
    }

    // 2. The u64 extremes: no overflow, no wrap-around to 0.
    {
        let wm = FinalityWatermark::new();
        let _ = wm.advance_finalized(u64::MAX, "max");
        for slot in [0, 1, u64::MAX / 2, u64::MAX - 1] {
            assert!(matches!(wm.advance_confirmed(slot, "x"), Advance::Stale { .. }));
            assert!(matches!(wm.advance_finalized(slot, "x"), Advance::Stale { .. }));
        }
        assert!(matches!(
            wm.advance_finalized(u64::MAX, "other"),
            Advance::Conflict { .. }
        ));
        assert_eq!(wm.confirmed_tip(), (u64::MAX, "max".to_owned()));
        assert_eq!(wm.finalized_tip(), (u64::MAX, "max".to_owned()));
        assert!(wm.is_finalized(u64::MAX));
    }

    // 3. Concurrent writers racing upward with heavy jitter (several RPC
    //    nodes at different heights), concurrent readers checking every
    //    observation against their previous one.
    {
        const WRITERS: u64 = 4;
        const READERS: usize = 4;
        const OPS: u64 = 25_000;

        let wm = FinalityWatermark::new();
        let done = AtomicBool::new(false);
        let (wm, done) = (&wm, &done);

        let (max_confirmed, max_finalized) = std::thread::scope(|s| {
            let readers: Vec<_> = (0..READERS)
                .map(|_| {
                    s.spawn(move || {
                        let (mut last_c, mut last_f, mut rounds) = (0, 0, 0_u64);
                        loop {
                            let finished = done.load(Ordering::Acquire);
                            // Finalized first: see `FinalityWatermark::lag`.
                            let f = wm.finalized_slot();
                            let c = wm.confirmed_slot();
                            assert!(f >= last_f, "finalized moved backwards {last_f} -> {f}");
                            assert!(c >= last_c, "confirmed moved backwards {last_c} -> {c}");
                            assert!(f <= c, "finalized {f} overtook confirmed {c}");
                            if last_f != 0 {
                                assert!(wm.is_finalized(last_f), "slot {last_f} un-finalized");
                            }
                            let (tip_f, tip_hash) = wm.finalized_tip();
                            assert!(tip_f >= f);
                            if tip_f != 0 {
                                assert_eq!(tip_hash, hash(tip_f), "torn finalized pair");
                            }
                            let (tip_c, tip_hash) = wm.confirmed_tip();
                            assert!(tip_c >= c);
                            if tip_c != 0 {
                                assert_eq!(tip_hash, hash(tip_c), "torn confirmed pair");
                            }
                            (last_c, last_f) = (c, f);
                            rounds += 1;
                            if finished {
                                return rounds;
                            }
                        }
                    })
                })
                .collect();

            let writers: Vec<_> = (0..WRITERS)
                .map(|w| {
                    s.spawn(move || {
                        let mut rng = Rng(0xD1B5_4A32_D192_ED03 ^ (w + 1));
                        let (mut max_c, mut max_f) = (0, 0);
                        for i in 0..OPS {
                            let slot = (i * 4 + 200).saturating_sub(rng.below(400));
                            if rng.below(3) == 0 {
                                let _ = wm.advance_finalized(slot, &hash(slot));
                                max_f = max_f.max(slot);
                            } else {
                                let _ = wm.advance_confirmed(slot, &hash(slot));
                                max_c = max_c.max(slot);
                            }
                        }
                        (max_c, max_f)
                    })
                })
                .collect();

            let maxima: Vec<(u64, u64)> =
                writers.into_iter().map(|h| h.join().unwrap()).collect();
            done.store(true, Ordering::Release);
            for reader in readers {
                assert!(reader.join().unwrap() > 0);
            }
            let max_c = maxima.iter().map(|m| m.0).max().unwrap();
            let max_f = maxima.iter().map(|m| m.1).max().unwrap();
            (max_c, max_f)
        });

        // The highest offer always wins, however the race was interleaved.
        assert_eq!(wm.finalized_slot(), max_finalized);
        assert_eq!(wm.confirmed_slot(), max_confirmed.max(max_finalized));
    }

    // 4. Through the poller, fed by a misbehaving RPC.
    {
        use Reply::*;
        let adapter = ScriptedAdapter::new(
            [
                Tip(200),
                Tip(150), // lagging node
                Fail,
                Tip(200),
                TipWithHash(210, "fork"),
                Tip(210), // same slot, different hash
                Mislabeled(500),
                Tip(205), // lagging node
                Tip(260),
            ],
            [
                Tip(168),
                Tip(100), // lagging node
                Tip(168),
                Fail,
                Mislabeled(400),
                Tip(170),
                TipWithHash(170, "other"), // same slot, different hash
                Tip(120),                  // lagging node
                Tip(228),
            ],
        );
        let changelog = RecordingChangeLog::failing(1);
        let (mut poller, wm) = poller(&adapter, &changelog);

        let expected = [
            (200, 168),
            (200, 168),
            (200, 168),
            (200, 168),
            (210, 168),
            (210, 170),
            (210, 170),
            (210, 170),
            (260, 228),
        ];
        let mut previous = wm.snapshot();
        for (poll, want) in expected.into_iter().enumerate() {
            poller.poll_once().await;
            let now = wm.snapshot();
            assert!(now.confirmed_slot >= previous.confirmed_slot, "poll {poll}: confirmed");
            assert!(now.finalized_slot >= previous.finalized_slot, "poll {poll}: finalized");
            assert!(now.finalized_slot <= now.confirmed_slot, "poll {poll}: overtaken");
            assert_eq!((now.confirmed_slot, now.finalized_slot), want, "poll {poll}");
            previous = now;
        }
        assert_eq!(wm.confirmed_tip(), (260, hash(260)));
        assert_eq!(wm.finalized_tip(), (228, hash(228)));

        // The Final stream is monotonic too, and gap-free: each event picks
        // up exactly where the previous one ended.
        let events = changelog.events();
        assert_eq!(
            events,
            vec![
                final_event(168, 0),
                final_event(170, 168),
                final_event(228, 170)
            ]
        );
        let mut announced = 0;
        for event in &events {
            let ChangeEvent::Final {
                slot,
                previous_slot,
                ..
            } = event
            else {
                panic!("unexpected event {event}");
            };
            assert_eq!(*previous_slot, announced);
            assert!(*slot > announced);
            announced = *slot;
        }
        assert_eq!(announced, wm.finalized_slot());
    }
}

// ---------------------------------------------------------------------------
// Required check 2: is_finalized column integrity in Parquet
// ---------------------------------------------------------------------------

/// Append the `is_finalized` column to a UDM batch, the way the storage
/// writer is meant to: the table's own columns, plus the column from a single
/// watermark read.
fn stamp<R: UdmRow>(rows: &[R], tracker: &CommitmentTracker) -> RecordBatch {
    let batch = R::to_record_batch(rows).unwrap();
    let mut fields: Vec<_> = batch.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(is_finalized_field()));
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(
        tracker.is_finalized_column(rows.iter().map(UdmRow::slot)),
    ));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

/// Write `batch` to an in-memory Parquet file and read it back.
fn parquet_round_trip(batch: &RecordBatch) -> RecordBatch {
    let mut file = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut file, batch.schema(), None).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file)).unwrap();
    let schema = builder.schema().clone();
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    arrow::compute::concat_batches(&schema, &batches).unwrap()
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> &'a T {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<T>()
        .unwrap_or_else(|| panic!("column {name} has the wrong type"))
}

/// Check the `is_finalized` column of a batch read back from Parquet against
/// the finalized watermark at the time the batch was written.
fn assert_is_finalized_integrity(table: &str, batch: &RecordBatch, finalized_at_write: u64) {
    let schema = batch.schema();
    let field = schema
        .field_with_name(IS_FINALIZED_COLUMN)
        .unwrap_or_else(|_| panic!("{table}: no {IS_FINALIZED_COLUMN} column"));
    assert_eq!(field.data_type(), &DataType::Boolean, "{table}");
    assert!(!field.is_nullable(), "{table}");

    let flags: &BooleanArray = column(batch, IS_FINALIZED_COLUMN);
    let slots: &UInt64Array = column(batch, "slot");
    assert_eq!(flags.len(), batch.num_rows(), "{table}");
    assert_eq!(flags.null_count(), 0, "{table}: every row has a value");

    // Every row agrees with the watermark it was written under.
    for row in 0..batch.num_rows() {
        let slot = slots.value(row);
        let expected = finalized_at_write != 0 && slot <= finalized_at_write;
        assert_eq!(
            flags.value(row),
            expected,
            "{table}: row {row} (slot {slot}) with finalized watermark {finalized_at_write}"
        );
    }

    // Not torn: every finalized row is below every non-finalized row.
    let max_final = (0..batch.num_rows())
        .filter(|&r| flags.value(r))
        .map(|r| slots.value(r))
        .max();
    let min_open = (0..batch.num_rows())
        .filter(|&r| !flags.value(r))
        .map(|r| slots.value(r))
        .min();
    if let (Some(max_final), Some(min_open)) = (max_final, min_open) {
        assert!(
            max_final < min_open,
            "{table}: slot {max_final} final but slot {min_open} not"
        );
    }
}

/// Every UDM table, written with `slots` and read back from Parquet.
fn write_every_table(slots: &[u64], tracker: &CommitmentTracker) -> Vec<(&'static str, RecordBatch)> {
    let slot_rows: Vec<_> = slots
        .iter()
        .map(|&slot| DeepAnaSlot {
            slot,
            blockhash: hash(slot),
            parent_slot: slot - 1,
            block_time: Some(1_700_000_000 + slot as i64),
            ..Default::default()
        })
        .collect();
    let transactions: Vec<_> = slots
        .iter()
        .flat_map(|&slot| {
            (0..3).map(move |i| DeepAnaTransaction {
                slot,
                blockhash: hash(slot),
                transaction_index: i,
                tx_signature: format!("sig-{slot}-{i}"),
                ..Default::default()
            })
        })
        .collect();
    let instructions: Vec<_> = slots
        .iter()
        .flat_map(|&slot| {
            (0..2).map(move |i| DeepAnaInstruction {
                slot,
                tx_signature: format!("sig-{slot}-0"),
                instruction_index: i,
                ..Default::default()
            })
        })
        .collect();
    let transfers: Vec<_> = slots
        .iter()
        .map(|&slot| DeepAnaTokenTransfer {
            slot,
            tx_signature: format!("sig-{slot}-0"),
            amount_raw: slot,
            ..Default::default()
        })
        .collect();

    fn write<R: UdmRow>(rows: &[R], tracker: &CommitmentTracker) -> (&'static str, RecordBatch) {
        let stamped = stamp(rows, tracker);
        let read_back = parquet_round_trip(&stamped);
        assert_eq!(read_back, stamped, "{}: Parquet changed the batch", R::TABLE_NAME);
        (R::TABLE_NAME, read_back)
    }
    vec![
        write(&slot_rows, tracker),
        write(&transactions, tracker),
        write(&instructions, tracker),
        write(&transfers, tracker),
    ]
}

/// The `is_finalized` column survives a Parquet round trip intact for every
/// UDM table:
///
/// 1. before anything is finalized, every row is `false`;
/// 2. every row matches the watermark it was written under, the boundary
///    slot itself included, regardless of row order or skipped slots;
/// 3. rows written as `false` are exactly the ones later `Final` events
///    cover, so consumers can always reconstruct current finality;
/// 4. a batch stamped while the watermark is advancing is never torn.
///
/// The storage writer is not implemented yet; [`stamp`] does what it will
/// do with the commitment module's API, and the Parquet round trip uses the
/// `parquet` crate directly.
#[tokio::test]
async fn is_finalized_column_round_trips_through_parquet_with_integrity() {
    let adapter = ScriptedAdapter::new(
        [Reply::Fail, Reply::Tip(1_000_040), Reply::Tip(1_000_062)],
        [Reply::Fail, Reply::Tip(1_000_008), Reply::Tip(1_000_030)],
    );
    let changelog = RecordingChangeLog::new();
    let (mut poller, watermark) = poller(&adapter, &changelog);
    let tracker = CommitmentTracker::from_watermark(watermark);

    // Slots 999_990..=1_000_040 in a scrambled order, with every 7th slot
    // skipped by its leader. The finalized boundary and the slot just past
    // it are both present.
    let mut slots: Vec<u64> = (999_990..=1_000_040).filter(|s| s % 7 != 0).collect();
    slots.reverse();
    slots.rotate_left(17);
    assert!(slots.contains(&1_000_008) && slots.contains(&1_000_009));

    // 1. Nothing finalized yet (both fetches failed).
    poller.poll_once().await;
    assert_eq!(tracker.finalized_slot(), 0);
    for (table, batch) in write_every_table(&slots, &tracker) {
        assert_is_finalized_integrity(table, &batch, 0);
        let flags: &BooleanArray = column(&batch, IS_FINALIZED_COLUMN);
        assert_eq!(flags.true_count(), 0, "{table}");
    }

    // 2. Finalized up to 1_000_008, confirmed up to 1_000_040.
    poller.poll_once().await;
    let finalized_at_write = tracker.finalized_slot();
    assert_eq!(finalized_at_write, 1_000_008);
    let written = write_every_table(&slots, &tracker);
    for (table, batch) in &written {
        assert_is_finalized_integrity(table, batch, finalized_at_write);
        let flags: &BooleanArray = column(batch, IS_FINALIZED_COLUMN);
        let slot_col: &UInt64Array = column(batch, "slot");
        assert!(flags.true_count() > 0 && flags.false_count() > 0, "{table}");
        for row in 0..batch.num_rows() {
            match slot_col.value(row) {
                1_000_008 => assert!(flags.value(row), "{table}: the boundary is final"),
                1_000_009 => assert!(!flags.value(row), "{table}: past the boundary"),
                _ => {}
            }
        }
    }

    // 3. Finality moves on to 1_000_030 after the files were written. A
    //    row's current finality is its stored flag, or a later Final event
    //    covering its slot; that must equal what the watermark says now.
    poller.poll_once().await;
    assert_eq!(tracker.finalized_slot(), 1_000_030);
    let events = changelog.events();
    assert_eq!(
        events,
        vec![
            final_event(1_000_008, 0),
            final_event(1_000_030, 1_000_008)
        ]
    );
    let covered_by_later_final = |slot: u64| {
        events.iter().any(|event| match event {
            ChangeEvent::Final {
                slot: to,
                previous_slot: from,
                ..
            } => *from >= finalized_at_write && *from < slot && slot <= *to,
            _ => false,
        })
    };
    for (table, batch) in &written {
        let flags: &BooleanArray = column(batch, IS_FINALIZED_COLUMN);
        let slot_col: &UInt64Array = column(batch, "slot");
        for row in 0..batch.num_rows() {
            let slot = slot_col.value(row);
            let stored = flags.value(row);
            assert!(
                !(stored && covered_by_later_final(slot)),
                "{table}: slot {slot} announced twice"
            );
            assert_eq!(
                stored || covered_by_later_final(slot),
                tracker.is_finalized(slot),
                "{table}: slot {slot}"
            );
        }
    }

    // 4. Stamp batches while another thread advances the watermark one slot
    //    at a time. Each batch must show one clean boundary that lies within
    //    the watermark values seen just before and just after stamping.
    let contiguous: Vec<u64> = (1_000_000..=1_000_600).collect();
    let rows: Vec<DeepAnaSlot> = contiguous
        .iter()
        .map(|&slot| DeepAnaSlot {
            slot,
            blockhash: hash(slot),
            parent_slot: slot - 1,
            ..Default::default()
        })
        .collect();
    let watermark = Arc::clone(tracker.watermark());
    std::thread::scope(|s| {
        let advancer = s.spawn(|| {
            for slot in 1_000_031..=1_000_530 {
                let _ = watermark.advance_finalized(slot, &hash(slot));
                std::thread::yield_now();
            }
        });
        let mut batches = 0;
        while !advancer.is_finished() || batches == 0 {
            let before = tracker.finalized_slot();
            let stamped = stamp(&rows, &tracker);
            let after = tracker.finalized_slot();

            let flags: &BooleanArray = column(&stamped, IS_FINALIZED_COLUMN);
            let boundary = contiguous[0] + flags.true_count() as u64 - 1;
            assert!(
                (before..=after).contains(&boundary),
                "boundary {boundary} outside [{before}, {after}]"
            );
            // Read back a sample through Parquet; check the rest in memory.
            let batch = if batches % 25 == 0 {
                parquet_round_trip(&stamped)
            } else {
                stamped
            };
            assert_is_finalized_integrity("solana_slots", &batch, boundary);
            batches += 1;
        }
        advancer.join().unwrap();
    });
    assert_eq!(tracker.finalized_slot(), 1_000_530);
}
