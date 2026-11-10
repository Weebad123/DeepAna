//! The runner's state machine, against a scripted LENS and scripted
//! detectors: the watermark, baselining from now, chunking, truncation,
//! error isolation, exactly-once commits, resets, enable state and
//! cancellation. Whether the real detectors' SQL finds real anomalies is
//! `detect_lens_test`'s job.

mod common;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::TempDir;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensResponse, Visibility};
use oracle::detect::{
    DEFAULT_CHUNK_BUCKETS, Detector, DetectorState, MAX_CHUNKS_PER_TURN, Runner, ScanContext, ScanError, ScanOutput,
};
use oracle::store::{AnomalyFilter, AnomalyKind, NewAnomaly, Store};
use oracle::OracleConfig;
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;

/// 60 s buckets: 150 slots each.
const BS: u64 = 150;

// ---------------------------------------------------------------------------
// Scripted LENS: answers only the four watermark queries.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct FakeLens {
    max_slot: Mutex<Option<u64>>,
    /// The newest slot `solana.slots` says carries transactions.
    last_tx_slot: Mutex<Option<u64>>,
    max_tx_slot: Mutex<Option<u64>>,
    min_slot: Mutex<Option<u64>>,
    fail_with: Mutex<Option<LensCallError>>,
    queries: AtomicUsize,
}

impl FakeLens {
    /// Data covers slots `min ..= max` in both tables, and every slot
    /// carries transactions.
    fn covering(min: u64, max: u64) -> Arc<Self> {
        let lens = Arc::new(Self::default());
        lens.set(min, max);
        lens
    }

    fn set(&self, min: u64, max: u64) {
        *self.min_slot.lock().unwrap() = Some(min);
        *self.max_slot.lock().unwrap() = Some(max);
        *self.last_tx_slot.lock().unwrap() = Some(max);
        *self.max_tx_slot.lock().unwrap() = Some(max);
    }

    /// Complete through bucket `b`.
    fn complete_through(&self, b: u64) {
        let min = self.min_slot.lock().unwrap().unwrap_or(0);
        self.set(min, (b + 1) * BS - 1);
    }
}

fn one_row(sql: &str, column: &str, value: Option<u64>) -> LensResponse {
    let mut row = Map::new();
    row.insert(column.into(), value.map_or(Value::Null, Value::from));
    LensResponse { rows: vec![row], row_count: 1, duration_ms: 0, truncated: false, sql: sql.into(), round_trip_ms: 0 }
}

#[async_trait]
impl LensApi for FakeLens {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        if let Some(err) = self.fail_with.lock().unwrap().clone() {
            return Err(err);
        }
        Ok(match sql {
            oracle::detect::MAX_SLOTS_SQL => one_row(sql, "max_slot", *self.max_slot.lock().unwrap()),
            oracle::detect::LAST_TX_SLOT_SQL => one_row(sql, "max_slot", *self.last_tx_slot.lock().unwrap()),
            oracle::detect::MAX_TX_SLOT_SQL => one_row(sql, "max_slot", *self.max_tx_slot.lock().unwrap()),
            oracle::detect::MIN_SLOTS_SQL => one_row(sql, "min_slot", *self.min_slot.lock().unwrap()),
            other => panic!("the scripted LENS only answers watermark queries, got {other}"),
        })
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unreachable!()
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        unreachable!()
    }
}

// ---------------------------------------------------------------------------
// Scripted detector
// ---------------------------------------------------------------------------

/// What a scripted detector does for one scan.
#[derive(Clone)]
enum Step {
    /// Flag these buckets (each one that lies in the range).
    Flag(Vec<u64>),
    Fail(ScanError),
    /// Never finish (until dropped).
    Hang,
}

struct Scripted {
    id: &'static str,
    /// Steps consumed one per scan; when empty, `default` applies.
    steps: Mutex<VecDeque<Step>>,
    default: Step,
    /// `(from_bucket, to_bucket, first_complete_bucket)` of every scan.
    seen: Mutex<Vec<(u64, u64, u64)>>,
    /// Fails with Truncated whenever a chunk is wider than this.
    max_width: AtomicU64,
}

impl Scripted {
    fn new(id: &'static str) -> Arc<Self> {
        Self::with(id, Step::Flag(vec![]))
    }

    fn with(id: &'static str, default: Step) -> Arc<Self> {
        Arc::new(Self {
            id,
            steps: Mutex::new(VecDeque::new()),
            default,
            seen: Mutex::new(Vec::new()),
            max_width: AtomicU64::new(u64::MAX),
        })
    }

    fn push(&self, step: Step) {
        self.steps.lock().unwrap().push_back(step);
    }

    fn seen(&self) -> Vec<(u64, u64, u64)> {
        self.seen.lock().unwrap().clone()
    }
}

fn anomaly(bucket: u64) -> NewAnomaly {
    NewAnomaly {
        subject: String::new(),
        bucket,
        start_slot: bucket * BS,
        end_slot: bucket * BS + BS - 1,
        kind: AnomalyKind::Series,
        value: bucket as f64,
        baseline: Some(1.0),
        scale: Some(1.0),
        score: 9.0,
        detail: Some(json!({ "scripted": true })),
    }
}

#[async_trait]
impl Detector for Scripted {
    fn id(&self) -> &str {
        self.id
    }

    fn kind(&self) -> AnomalyKind {
        AnomalyKind::Series
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        self.seen.lock().unwrap().push((ctx.from_bucket, ctx.to_bucket, ctx.first_complete_bucket));
        if ctx.to_bucket - ctx.from_bucket + 1 > self.max_width.load(Ordering::SeqCst) {
            return Err(ScanError::Truncated);
        }
        let step = self.steps.lock().unwrap().pop_front().unwrap_or_else(|| self.default.clone());
        match step {
            Step::Flag(buckets) => Ok(ScanOutput {
                anomalies: buckets
                    .into_iter()
                    .filter(|b| (ctx.from_bucket..=ctx.to_bucket).contains(b))
                    .map(anomaly)
                    .collect(),
            }),
            Step::Fail(err) => Err(err),
            Step::Hang => std::future::pending().await,
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

struct Harness {
    dir: TempDir,
    config: OracleConfig,
    store: Store,
    lens: Arc<FakeLens>,
}

impl Harness {
    fn new(lens: Arc<FakeLens>) -> Self {
        let dir = TempDir::new();
        let config = config_in(&dir);
        let store = Store::open(&config).unwrap();
        Self { dir, config, store, lens }
    }

    fn runner(&self, detectors: Vec<Arc<dyn Detector>>) -> Runner {
        self.runner_with(detectors, CancellationToken::new())
    }

    fn runner_with(&self, detectors: Vec<Arc<dyn Detector>>, cancel: CancellationToken) -> Runner {
        Runner::with_detectors(self.store.clone(), self.lens.clone(), &self.config, cancel, detectors)
    }

    fn buckets(&self, metric: &str) -> Vec<u64> {
        let filter = AnomalyFilter { metric: Some(metric.into()), limit: 1_000, ..Default::default() };
        let mut buckets: Vec<u64> = self.store.list_anomalies(&filter).unwrap().iter().map(|a| a.bucket).collect();
        buckets.sort_unstable();
        buckets
    }
}

fn config_in(dir: &TempDir) -> OracleConfig {
    common::load(&[
        ("ORACLE_LENS_URL", common::LENS_URL),
        ("ORACLE_DB_PATH", &dir.file("oracle.db")),
        ("ORACLE_BUCKET_SECS", "60"),
        ("ORACLE_DETECT_INTERVAL_SECS", "10"),
        ("ORACLE_BASELINE_BUCKETS", "8"),
    ])
    .unwrap()
}

fn dets(list: &[&Arc<Scripted>]) -> Vec<Arc<dyn Detector>> {
    list.iter().map(|d| Arc::clone(*d) as Arc<dyn Detector>).collect()
}

fn state_of(runner: &Runner, id: &str) -> DetectorState {
    runner.handle().detector(id).unwrap().state
}

// ---------------------------------------------------------------------------
// Watermark and first run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_first_run_baselines_from_now_and_reports_nothing() {
    // Ingestion started mid-bucket 10 and is complete through bucket 40.
    let h = Harness::new(FakeLens::covering(10 * BS + 7, 41 * BS - 1));
    let det = Scripted::with("a", Step::Flag((0..100).collect()));
    let runner = h.runner(dets(&[&det]));

    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), Some(40), "progress jumps to the newest complete bucket");
    assert!(det.seen().is_empty(), "nothing is scanned on the first run");
    assert!(h.buckets("a").is_empty(), "no historical wobble is reported");
    assert_eq!(state_of(&runner, "a"), DetectorState::Baselined);

    let status = runner.handle().status();
    let wm = status.watermark.unwrap();
    assert_eq!((wm.first_complete_bucket, wm.complete_max), (11, 40));
}

#[tokio::test]
async fn a_bucket_is_scored_only_once_complete() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await; // baseline at 9

    // Bucket 10 is one slot short of complete.
    lens.set(0, 11 * BS - 2);
    runner.run_once().await;
    assert!(det.seen().is_empty());
    assert_eq!(state_of(&runner, "a"), DetectorState::UpToDate);

    lens.set(0, 11 * BS - 1);
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(10, 10, 0)]);
    assert_eq!(h.store.progress("a").unwrap(), Some(10));
}

#[tokio::test]
async fn the_lagging_table_bounds_completeness() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await; // baseline at 9

    *lens.max_slot.lock().unwrap() = Some(20 * BS);
    *lens.last_tx_slot.lock().unwrap() = Some(20 * BS);
    // transactions only reach the end of bucket 12.
    *lens.max_tx_slot.lock().unwrap() = Some(13 * BS - 1);
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(10, 12, 0)]);
}

#[tokio::test]
async fn trailing_slots_without_transactions_still_complete_their_buckets() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await; // baseline at 9

    // The chain went quiet after the first slot of bucket 11: buckets 11 and
    // 12 are complete, with nothing in them. The transactions table holds
    // the newest slot with transactions, so it is caught up.
    *lens.max_slot.lock().unwrap() = Some(13 * BS - 1);
    *lens.last_tx_slot.lock().unwrap() = Some(11 * BS);
    *lens.max_tx_slot.lock().unwrap() = Some(11 * BS);
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(10, 12, 0)], "a quiet bucket is scored, not waited on");
    assert_eq!(h.store.progress("a").unwrap(), Some(12));
}

#[tokio::test]
async fn an_empty_or_missing_dataset_ends_the_tick_quietly() {
    let lens = Arc::new(FakeLens::default());
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), None);
    assert!(runner.handle().status().tick_error.is_some());

    *lens.fail_with.lock().unwrap() = Some(LensCallError::BadQuery {
        message: "query_plan_error: table 'datafusion.solana.slots' not found".into(),
    });
    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), None);
    assert!(runner.handle().status().tick_error.unwrap().contains("not ready"));
}

// ---------------------------------------------------------------------------
// Chunking and progress
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_chunks_still_advance_progress_in_bounded_turns() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await; // baseline at 9

    // A long outage: far more than one turn's budget behind.
    let budget = DEFAULT_CHUNK_BUCKETS * u64::from(MAX_CHUNKS_PER_TURN);
    lens.complete_through(9 + budget + 5);
    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), Some(9 + budget));
    assert_eq!(state_of(&runner, "a"), DetectorState::CatchingUp);
    let seen = det.seen();
    assert_eq!(seen.len(), MAX_CHUNKS_PER_TURN as usize);
    // Chunks tile the range with no gap and no overlap.
    for pair in seen.windows(2) {
        assert_eq!(pair[0].1 + 1, pair[1].0);
    }
    assert_eq!(seen[0].0, 10);

    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), Some(9 + budget + 5));
    assert_eq!(state_of(&runner, "a"), DetectorState::UpToDate);
}

#[tokio::test]
async fn buckets_before_the_data_are_skipped_not_scanned() {
    // Progress stored at 3, but LENS's data now starts at bucket 8 (pruned).
    let lens = FakeLens::covering(8 * BS, 12 * BS - 1);
    let h = Harness::new(lens);
    h.store.commit_scan("a", &[], 3).unwrap();
    let det = Scripted::new("a");
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(8, 11, 8)], "the scan starts at the first complete bucket");
    assert_eq!(h.store.progress("a").unwrap(), Some(11));
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn truncation_halves_the_chunk_and_never_scores_the_truncated_result() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::with("a", Step::Flag(vec![12, 25]));
    det.max_width.store(5, Ordering::SeqCst);
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await; // baseline at 9

    lens.complete_through(29);
    runner.run_once().await;
    // 32 -> 16 -> 8 -> 4, then 4-bucket chunks to the end.
    let widths: Vec<u64> = det.seen().iter().map(|(f, t, _)| t - f + 1).collect();
    assert_eq!(&widths[..4], &[20, 16, 8, 4], "{widths:?}");
    assert!(widths[3..].iter().all(|w| *w <= 4));
    assert_eq!(runner.handle().detector("a").unwrap().chunk_buckets, 4);
    assert_eq!(h.store.progress("a").unwrap(), Some(29));
    assert_eq!(h.buckets("a"), vec![12, 25], "each flagged once, from the accepted chunks only");
}

#[tokio::test]
async fn truncation_of_a_single_bucket_marks_the_detector_failing() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    det.max_width.store(0, Ordering::SeqCst);
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    lens.complete_through(12);
    runner.run_once().await;

    match state_of(&runner, "a") {
        DetectorState::Failing(message) => {
            assert!(message.contains("ORACLE_BASELINE_BUCKETS"), "{message}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(h.store.progress("a").unwrap(), Some(9), "progress does not advance");
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_detectors_failure_never_stops_another() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let failing = Scripted::with("a_failing", Step::Fail(ScanError::Permanent("boom".into())));
    let flaky = Scripted::with("b_flaky", Step::Fail(ScanError::Transient("timeout".into())));
    let missing = Scripted::with("c_missing", Step::Fail(ScanError::NotReady("no table".into())));
    let healthy = Scripted::with("d_healthy", Step::Flag(vec![11]));
    let runner = h.runner(dets(&[&failing, &flaky, &missing, &healthy]));
    runner.run_once().await;
    lens.complete_through(12);
    runner.run_once().await;

    assert_eq!(state_of(&runner, "a_failing"), DetectorState::Failing("boom".into()));
    assert_eq!(state_of(&runner, "b_flaky"), DetectorState::Retrying("timeout".into()));
    assert_eq!(state_of(&runner, "c_missing"), DetectorState::NotReady("no table".into()));
    assert_eq!(state_of(&runner, "d_healthy"), DetectorState::UpToDate);
    for id in ["a_failing", "b_flaky", "c_missing"] {
        assert_eq!(h.store.progress(id).unwrap(), Some(9), "{id}");
    }
    assert_eq!(h.store.progress("d_healthy").unwrap(), Some(12));
    assert_eq!(h.buckets("d_healthy"), vec![11]);
}

#[tokio::test]
async fn a_transient_failure_retries_the_same_buckets_next_tick() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::with("a", Step::Flag(vec![11]));
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    lens.complete_through(12);
    det.push(Step::Fail(ScanError::Transient("timeout".into())));
    runner.run_once().await;
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(10, 12, 0), (10, 12, 0)]);
    assert_eq!(h.buckets("a"), vec![11]);
}

#[tokio::test]
async fn an_invalid_finding_fails_the_detector_without_writing_anything() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    struct Bad;
    #[async_trait]
    impl Detector for Bad {
        fn id(&self) -> &str {
            "bad"
        }
        fn kind(&self) -> AnomalyKind {
            AnomalyKind::Series
        }
        async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
            let mut a = anomaly(ctx.from_bucket);
            a.score = f64::NAN;
            Ok(ScanOutput { anomalies: vec![anomaly(ctx.to_bucket), a] })
        }
    }
    let runner = h.runner(vec![Arc::new(Bad)]);
    runner.run_once().await;
    lens.complete_through(12);
    runner.run_once().await;
    assert!(matches!(state_of(&runner, "bad"), DetectorState::Failing(_)));
    assert_eq!(h.store.progress("bad").unwrap(), Some(9));
    assert!(h.buckets("bad").is_empty());
}

// ---------------------------------------------------------------------------
// Exactly once
// ---------------------------------------------------------------------------

#[tokio::test]
async fn repeated_ticks_and_restarts_store_each_finding_exactly_once() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let flagged = vec![10, 11, 15, 20, 33];
    let det = Scripted::with("a", Step::Flag(flagged.clone()));
    {
        let runner = h.runner(dets(&[&det]));
        runner.run_once().await;
        lens.complete_through(20);
        runner.run_once().await;
        runner.run_once().await; // nothing new
    }
    // A "restart": a fresh runner over the same store.
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    lens.complete_through(40);
    runner.run_once().await;
    runner.run_once().await;

    assert_eq!(h.buckets("a"), flagged);
    // Every bucket 10..=40 was scanned exactly once across all runs.
    let mut scanned: Vec<u64> = det.seen().iter().flat_map(|(f, t, _)| *f..=*t).collect();
    scanned.sort_unstable();
    assert_eq!(scanned, (10..=40).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_crash_between_scan_and_commit_rescans_without_duplicates() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::with("a", Step::Flag(vec![12]));
    let runner = h.runner(dets(&[&det]));
    runner.run_once().await;
    lens.complete_through(14);

    // The chunk's findings were computed but the process died before the
    // commit: nothing in the store, progress unchanged. Simulated by a scan
    // that is cancelled mid-flight.
    let cancel = CancellationToken::new();
    det.push(Step::Hang);
    let dying = h.runner_with(dets(&[&det]), cancel.clone());
    let tick = tokio::spawn(async move { dying.run_once().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), tick).await.expect("cancellation stops the tick").unwrap();
    assert_eq!(h.store.progress("a").unwrap(), Some(9));

    // And a crash *after* a commit: the commit replays as a no-op.
    runner.run_once().await;
    let replay = h.store.commit_scan("a", &[anomaly(12)], 14).unwrap();
    assert!(!replay.progress_advanced);
    assert_eq!(replay.inserted, 0);
    runner.run_once().await;
    assert_eq!(h.buckets("a"), vec![12]);
}

#[tokio::test]
async fn cancellation_is_honoured_between_detectors() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let cancel = CancellationToken::new();
    let first = Scripted::new("a");
    let second = Scripted::new("b");
    let runner = h.runner_with(dets(&[&first, &second]), cancel.clone());
    cancel.cancel();
    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), None);
    assert_eq!(lens.queries.load(Ordering::SeqCst), 0, "no LENS call after shutdown");
}

#[tokio::test(start_paused = true)]
async fn spawn_ticks_on_schedule_and_stops_on_shutdown() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let cancel = CancellationToken::new();
    let det = Scripted::new("a");
    let task = h.runner_with(dets(&[&det]), cancel.clone()).spawn();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let after_first = lens.queries.load(Ordering::SeqCst);
    assert_eq!(after_first, 4, "one tick at start: four watermark queries");
    tokio::time::sleep(Duration::from_secs(10)).await;
    assert_eq!(lens.queries.load(Ordering::SeqCst), 8, "a second tick after detect_interval_secs");
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task).await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Handle: enable state and reset
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_disabled_detector_is_skipped_and_catches_up_when_enabled() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::with("a", Step::Flag(vec![11]));
    let runner = h.runner(dets(&[&det]));
    let handle = runner.handle();
    runner.run_once().await;

    assert_eq!(handle.set_enabled("a", false).unwrap().state, DetectorState::Disabled);
    assert!(handle.set_enabled("nope", false).is_none());
    lens.complete_through(12);
    runner.run_once().await;
    assert!(det.seen().is_empty());
    assert_eq!(h.store.progress("a").unwrap(), Some(9));

    handle.set_enabled("a", true).unwrap();
    runner.run_once().await;
    assert_eq!(det.seen(), vec![(10, 12, 0)]);
    assert_eq!(h.buckets("a"), vec![11]);
}

#[tokio::test]
async fn a_ledger_reset_waits_for_the_operator_then_baselines_again() {
    let lens = FakeLens::covering(0, 50 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::with("a", Step::Flag((0..100).collect()));
    let runner = h.runner(dets(&[&det]));
    let handle = runner.handle();
    runner.run_once().await; // baseline at 49

    // Devnet reset: the ledger restarts from slot 0.
    lens.set(0, 5 * BS - 1);
    runner.run_once().await;
    assert!(matches!(state_of(&runner, "a"), DetectorState::AheadOfData(_)));
    assert!(det.seen().is_empty());

    assert_eq!(handle.reset_progress("a").await.unwrap(), Some(49));
    assert!(handle.reset_progress("nope").await.is_err());
    runner.run_once().await;
    assert_eq!(h.store.progress("a").unwrap(), Some(4), "baselined again from the new now");
    assert!(det.seen().is_empty(), "and still reported nothing");
    assert!(h.buckets("a").is_empty());
}

#[tokio::test]
async fn reset_waits_for_a_turn_in_flight() {
    let lens = FakeLens::covering(0, 10 * BS - 1);
    let h = Harness::new(lens.clone());
    let det = Scripted::new("a");
    let runner = Arc::new(h.runner(dets(&[&det])));
    runner.run_once().await;
    lens.complete_through(12);

    // A scan that takes a while; reset arrives during it.
    det.push(Step::Hang);
    let handle = runner.handle();
    let r = Arc::clone(&runner);
    let tick = tokio::spawn(async move { r.run_once().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let reset = tokio::spawn(async move { handle.reset_progress("a").await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!reset.is_finished(), "reset waits for the turn");
    tick.abort();
    let _ = tick.await;
    assert_eq!(reset.await.unwrap().unwrap(), Some(9));
    assert_eq!(h.store.progress("a").unwrap(), None);
}
