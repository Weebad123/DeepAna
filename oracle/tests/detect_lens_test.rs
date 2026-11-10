//! The detector set against the real LENS.
//!
//! PRISM's `ParquetWriter` writes a synthetic chain with steady activity
//! and one planted anomaly per detector; `lens::serve_on` serves it over a
//! real socket; the runner reaches it through FORGE's `lens_client`, as
//! in production. So every detector's SQL is run by DataFusion, not by a
//! fake.
//!
//! * every planted anomaly is found, by the right detector, in the right
//!   bucket, with the right subject, and nothing else is;
//! * every stored score is reproduced from the stored value, baseline and
//!   scale, and one is checked against an independent hand computation;
//! * live reconciliation: ticking while data arrives bucket by bucket
//!   (half-written buckets included) stores exactly what one backfill over
//!   the finished data stores, and re-ticking adds nothing;
//! * a LENS row limit small enough to truncate the scans changes nothing
//!   either, except that the detector whose history alone is too big says
//!   so.

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::TempDir;
use lens::{LensConfig, QueryEngine};
use forge::lens_client::{LensApi, LensClient};
use oracle::OracleConfig;
use oracle::detect::baseline::{median, modified_z};
use oracle::detect::{DetectorState, Runner, default_detectors};
use oracle::store::{Anomaly, AnomalyFilter, AnomalyKind, Store};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// `ORACLE_BUCKET_SECS=60`: 150 slots a bucket.
const BS: u64 = 150;
/// `ORACLE_BASELINE_BUCKETS`.
const HISTORY: u64 = 8;
/// Ingestion starts at the first slot of this bucket.
const FIRST: u64 = 10;
/// The first run sees data complete through this bucket and baselines here.
const BASELINE_AT: u64 = 19;
const LAST: u64 = 39;

const P1: &str = "Prog1111111111111111111111111111111111111111";
const P2: &str = "Prog2222222222222222222222222222222222222222";
const P_NEW: &str = "ProgNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEWNEW";
const MINTS: [&str; 5] = ["MintAAAA", "MintBBBB", "MintCCCC", "MintDDDD", "MintEEEE"];
const MINT_NEW: &str = "MintNEWNEW";

const TX_SPIKE: u64 = 24;
const FAILURE_SPIKE: u64 = 27;
const FEE_SPIKE: u64 = 30;
const TRANSFER_SPIKE: u64 = 33;
const CONCENTRATION: u64 = 36;
const NEWCOMERS: u64 = 38;

/// What the planted chain must produce: `(metric, subject, bucket)`.
fn planted() -> BTreeSet<(String, String, u64)> {
    [
        ("tx_count", "", TX_SPIKE),
        ("tx_failure_rate", "", FAILURE_SPIKE),
        ("program_failure_rate", P1, FAILURE_SPIKE),
        ("fee_per_tx", "", FEE_SPIKE),
        ("transfer_count", "", TRANSFER_SPIKE),
        ("top_mint_share", MINTS[0], CONCENTRATION),
        ("first_seen_mint", MINT_NEW, NEWCOMERS),
        ("first_seen_program", P_NEW, NEWCOMERS),
    ]
    .into_iter()
    .map(|(m, s, b)| (m.to_owned(), s.to_owned(), b))
    .collect()
}

// ---------------------------------------------------------------------------
// The synthetic chain
// ---------------------------------------------------------------------------

/// One bucket's activity.
struct Plan {
    txs: u64,
    failed: u64,
    fee: u64,
    /// One mint per transfer.
    transfers: Vec<&'static str>,
    new_program: bool,
}

/// Calm activity with a small deterministic wobble, plus the plants.
fn plan(b: u64) -> Plan {
    let mut p = Plan {
        txs: 40 + b % 3,
        failed: 2 + b % 2,
        fee: 5_000,
        transfers: (0..30 + b % 3).map(|j| MINTS[(j % 5) as usize]).collect(),
        new_program: false,
    };
    match b {
        TX_SPIKE => p.txs = 200,
        FAILURE_SPIKE => p.failed = 30,
        FEE_SPIKE => p.fee = 50_000,
        TRANSFER_SPIKE => p.transfers = (0..150).map(|j| MINTS[j % 5]).collect(),
        CONCENTRATION => {
            p.transfers = vec![MINTS[0]; 27];
            p.transfers.extend(&MINTS[1..4]);
        }
        NEWCOMERS => {
            p.transfers.push(MINT_NEW);
            p.new_program = true;
        }
        _ => {}
    }
    p
}

/// The rows of slots `lo ..= hi` (all within one bucket's plan or several).
fn write_slots(dir: &Path, lo: u64, hi: u64) {
    let mut writer = ParquetWriter::new(dir);
    for slot in lo..=hi {
        let b = slot / BS;
        let offset = slot % BS;
        let p = plan(b);

        let mut txs = Vec::new();
        let mut ixs = Vec::new();
        // Transaction i of the bucket lives in slot offset i % BS.
        for i in (offset..p.txs).step_by(BS as usize) {
            let sig = format!("sig-{b}-{i}");
            let failed = i < p.failed;
            txs.push(DeepAnaTransaction {
                slot,
                blockhash: format!("hash{slot}"),
                transaction_index: txs.len() as u32,
                tx_signature: sig.clone(),
                fee: p.fee,
                status: if failed { TransactionStatus::Failed } else { TransactionStatus::Success },
                err: failed.then(|| "custom program error".to_owned()),
                ..Default::default()
            });
            let mut programs = vec![P1];
            if i < 10 {
                programs.push(P2);
            }
            if p.new_program && i == 0 {
                programs.push(P_NEW);
            }
            for (k, program) in programs.into_iter().enumerate() {
                ixs.push(DeepAnaInstruction {
                    slot,
                    tx_signature: sig.clone(),
                    instruction_index: k as u32,
                    program_id: program.to_owned(),
                    ..Default::default()
                });
            }
        }
        let transfers: Vec<DeepAnaTokenTransfer> = p
            .transfers
            .iter()
            .enumerate()
            .filter(|(j, _)| *j as u64 % BS == offset)
            .map(|(j, mint)| DeepAnaTokenTransfer {
                slot,
                tx_signature: format!("xfer-{b}-{j}"),
                instruction_index: 0,
                spl_token_program_id: "TokenProgram".into(),
                source_token_account: "src".into(),
                destination_token_account: "dst".into(),
                mint_token_address: Some((*mint).to_owned()),
                amount_raw: 1,
                decimals: Some(6),
                block_time: Some(block_time(slot)),
                ..Default::default()
            })
            .collect();
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot.saturating_sub(1),
            block_time: Some(block_time(slot)),
            block_height: Some(slot),
            transaction_count: txs.len() as u32,
            leader: None,
        };
        writer.append(&row, &txs, &ixs, &transfers).unwrap();
    }
    writer.flush().unwrap();
}

/// Within one UTC day, so every row lands in one partition.
fn block_time(slot: u64) -> i64 {
    1_793_102_400 + (slot / 3) as i64
}

fn first_slot(b: u64) -> u64 {
    b * BS
}

fn last_slot(b: u64) -> u64 {
    b * BS + BS - 1
}

// ---------------------------------------------------------------------------
// A real LENS
// ---------------------------------------------------------------------------

struct RealLens {
    url: String,
    engine: Arc<QueryEngine>,
    server: Option<JoinHandle<lens::LensResult<()>>>,
}

impl RealLens {
    async fn start(data_dir: &Path, vars: &[(&str, &str)]) -> Self {
        let data_dir = data_dir.to_str().unwrap().to_owned();
        let config = LensConfig::load_with(|name| {
            if name == "LENS_DATA_DIR" {
                return Some(data_dir.clone().into());
            }
            vars.iter().find(|(key, _)| *key == name).map(|(_, value)| (*value).into())
        })
        .unwrap();
        let engine = Arc::new(QueryEngine::new(Arc::new(config)).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));
        Self { url, engine, server: Some(server) }
    }

    /// Built the way ORACLE builds its client from `lens_url` and
    /// `lens_timeout_secs`.
    fn client(&self) -> Arc<dyn LensApi> {
        Arc::new(LensClient::from_parts(&self.url, Duration::from_secs(120)).unwrap())
    }

    /// Stop LENS and wait until it is gone: `serve_on` stops accepting,
    /// drops its listener, and closes every connection (idle keep-alive
    /// ones in a client's pool included) before it returns. Aborting the
    /// task instead would leave all three to happen in the background, so a
    /// request sent right after could still be answered.
    async fn stop(mut self) {
        self.engine.shutdown();
        let server = self.server.take().unwrap();
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("LENS did not stop within 10 s")
            .unwrap()
            .unwrap();
    }

    /// Pick up files PRISM wrote since the last refresh.
    fn refresh(&self) {
        let report = self.engine.refresh_catalog();
        assert!(report.is_success(), "{report:?}");
    }
}

impl Drop for RealLens {
    fn drop(&mut self) {
        self.engine.shutdown();
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn oracle_config(dir: &TempDir) -> OracleConfig {
    common::load(&[
        ("ORACLE_LENS_URL", common::LENS_URL),
        ("ORACLE_DB_PATH", &dir.file("oracle.db")),
        ("ORACLE_BUCKET_SECS", "60"),
        ("ORACLE_DETECT_INTERVAL_SECS", "10"),
        ("ORACLE_BASELINE_BUCKETS", &HISTORY.to_string()),
        ("ORACLE_ANOMALY_THRESHOLD", "3.5"),
    ])
    .unwrap()
}

struct Oracle {
    _dir: TempDir,
    store: Store,
    runner: Runner,
}

impl Oracle {
    fn new(lens: Arc<dyn LensApi>) -> Self {
        let dir = TempDir::new();
        let config = Arc::new(oracle_config(&dir));
        let store = Store::open(&config).unwrap();
        let runner = Runner::new(store.clone(), lens, config, CancellationToken::new());
        Self { _dir: dir, store, runner }
    }

    /// Every stored anomaly, without the fields that depend on when it was
    /// committed, in a stable order.
    fn anomalies(&self) -> Vec<Anomaly> {
        let mut all = self.store.list_anomalies(&AnomalyFilter { limit: 1_000, ..Default::default() }).unwrap();
        for a in &mut all {
            a.id = 0;
            a.detected_at = 0;
        }
        all.sort_by(|a, b| (&a.metric, &a.subject, a.bucket).cmp(&(&b.metric, &b.subject, b.bucket)));
        all
    }

    fn keys(&self) -> BTreeSet<(String, String, u64)> {
        self.anomalies().into_iter().map(|a| (a.metric, a.subject, a.bucket)).collect()
    }

    fn states(&self) -> Vec<(String, DetectorState)> {
        self.runner.handle().status().detectors.into_iter().map(|d| (d.id, d.state)).collect()
    }

    fn assert_all(&self, expected: &DetectorState) {
        for (id, state) in self.states() {
            assert_eq!(&state, expected, "{id}");
        }
    }
}

fn data_through(b: u64) -> TempDir {
    let dir = TempDir::new();
    write_slots(dir.path(), first_slot(FIRST), last_slot(b));
    dir
}

/// A backfill: a fresh ORACLE whose detectors all baselined at
/// `BASELINE_AT`, run until caught up over everything in `lens`.
async fn backfill(lens: Arc<dyn LensApi>) -> Oracle {
    let oracle = Oracle::new(lens);
    for detector in default_detectors() {
        oracle.store.commit_scan(detector.id(), &[], BASELINE_AT).unwrap();
    }
    oracle.runner.run_once().await;
    oracle
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_planted_anomaly_is_found_and_nothing_else() {
    let data = data_through(LAST);
    let lens = RealLens::start(data.path(), &[]).await;
    let oracle = backfill(lens.client()).await;
    oracle.assert_all(&DetectorState::UpToDate);
    assert_eq!(oracle.keys(), planted());

    for a in oracle.anomalies() {
        let expected_kind = match a.metric.as_str() {
            "top_mint_share" => AnomalyKind::Concentration,
            "first_seen_mint" | "first_seen_program" => AnomalyKind::FirstSeen,
            "program_failure_rate" => AnomalyKind::ProgramFailure,
            _ => AnomalyKind::Series,
        };
        assert_eq!(a.kind, expected_kind, "{a:?}");
        assert_eq!((a.start_slot, a.end_slot), (first_slot(a.bucket), last_slot(a.bucket)), "{a:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stored_numbers_reproduce_the_score_and_match_a_hand_computation() {
    let data = data_through(LAST);
    let lens = RealLens::start(data.path(), &[]).await;
    let oracle = backfill(lens.client()).await;
    let anomalies = oracle.anomalies();

    for a in &anomalies {
        match a.kind {
            AnomalyKind::FirstSeen => {
                assert_eq!((a.value, a.score, a.baseline, a.scale), (1.0, 1.0, None, None));
                let first = a.detail.as_ref().unwrap()["first_slot"].as_u64().unwrap();
                assert_eq!(first / BS, a.bucket);
            }
            _ => {
                let (baseline, scale) = (a.baseline.unwrap(), a.scale.unwrap());
                assert_eq!(
                    modified_z(a.value, baseline, scale).to_bits(),
                    a.score.to_bits(),
                    "stored row does not reproduce its score: {a:?}"
                );
                assert!(a.score.abs() > 3.5, "{a:?}");
            }
        }
    }

    // tx_count at the spike, independently: history = buckets 16..=23.
    let a = anomalies.iter().find(|a| a.metric == "tx_count").unwrap();
    let history: Vec<f64> = (TX_SPIKE - HISTORY..TX_SPIKE).map(|b| plan(b).txs as f64).collect();
    let m = median(&history).unwrap();
    let mut devs: Vec<f64> = history.iter().map(|x| (x - m).abs()).collect();
    devs.sort_by(f64::total_cmp);
    let mad = (devs[3] + devs[4]) / 2.0;
    let scale = mad.max(1.0).max(0.05 * m);
    assert_eq!(a.value, 200.0);
    assert_eq!(a.baseline, Some(m));
    assert_eq!(a.scale, Some(scale));
    assert_eq!(a.score, 0.6745 * (200.0 - m) / scale);
    assert_eq!(a.detail.as_ref().unwrap()["mad"], serde_json::json!(mad));

    // The ratio detectors divide what LENS counted.
    let fr = anomalies.iter().find(|a| a.metric == "tx_failure_rate").unwrap();
    let p = plan(FAILURE_SPIKE);
    assert_eq!(fr.value, p.failed as f64 / p.txs as f64);
    let share = anomalies.iter().find(|a| a.metric == "top_mint_share").unwrap();
    assert_eq!(share.value, 27.0 / 30.0);
    assert_eq!(share.detail.as_ref().unwrap()["total"], 30);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_ticks_reconcile_exactly_with_a_backfill() {
    // Live: data arrives half a bucket at a time; a tick after each write.
    let live_dir = data_through(BASELINE_AT);
    let lens = RealLens::start(live_dir.path(), &[]).await;
    let live = Oracle::new(lens.client());

    live.runner.run_once().await;
    live.assert_all(&DetectorState::Baselined);
    for detector in default_detectors() {
        assert_eq!(live.store.progress(detector.id()).unwrap(), Some(BASELINE_AT));
    }
    assert!(live.anomalies().is_empty(), "the first run reports nothing");

    for b in BASELINE_AT + 1..=LAST {
        let mid = first_slot(b) + BS / 2;
        write_slots(live_dir.path(), first_slot(b), mid);
        lens.refresh();
        live.runner.run_once().await;
        // A half-written bucket is never scored.
        for detector in default_detectors() {
            assert_eq!(live.store.progress(detector.id()).unwrap(), Some(b - 1), "{} at {b}", detector.id());
        }
        write_slots(live_dir.path(), mid + 1, last_slot(b));
        lens.refresh();
        live.runner.run_once().await;
        live.assert_all(&DetectorState::UpToDate);
    }
    // Nothing new arrives: ticks are no-ops.
    let before = live.anomalies();
    live.runner.run_once().await;
    live.runner.run_once().await;
    assert_eq!(live.anomalies(), before, "re-ticking stores nothing twice");

    // Backfill over identical data.
    let full_dir = data_through(LAST);
    let full_lens = RealLens::start(full_dir.path(), &[]).await;
    let full = backfill(full_lens.client()).await;

    assert_eq!(live.keys(), planted());
    assert_eq!(live.anomalies(), full.anomalies(), "live and backfill disagree");
    let inserted: u64 = live.runner.handle().status().detectors.iter().map(|d| d.anomalies_inserted).sum();
    assert_eq!(inserted as usize, planted().len(), "each finding inserted exactly once");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncation_shrinks_chunks_without_changing_any_finding() {
    let data = data_through(LAST);
    let roomy = RealLens::start(data.path(), &[]).await;
    let expected = backfill(roomy.client()).await.anomalies();

    // 12 rows: a series scan fits 8 history buckets + 4 scored ones, so
    // chunks must shrink from 32 to 4. Two programs over 9 buckets need 18
    // rows even for one scored bucket.
    let tight = RealLens::start(data.path(), &[("LENS_MAX_QUERY_ROWS", "12")]).await;
    let oracle = backfill(tight.client()).await;
    for _ in 0..4 {
        oracle.runner.run_once().await;
    }

    let status = oracle.runner.handle();
    let tx = status.detector("tx_count").unwrap();
    assert_eq!(tx.state, DetectorState::UpToDate);
    assert_eq!(tx.chunk_buckets, 4);
    match status.detector("program_failure_rate").unwrap().state {
        DetectorState::Failing(message) => assert!(message.contains("ORACLE_BASELINE_BUCKETS"), "{message}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(oracle.store.progress("program_failure_rate").unwrap(), Some(BASELINE_AT));

    let without_programs: Vec<Anomaly> =
        expected.into_iter().filter(|a| a.metric != "program_failure_rate").collect();
    assert_eq!(oracle.anomalies(), without_programs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lens_that_cannot_answer_leaves_every_detector_where_it_was() {
    let data = data_through(LAST);
    let lens = RealLens::start(data.path(), &[]).await;
    let oracle = backfill(lens.client()).await;
    let before = oracle.anomalies();
    assert_eq!(oracle.keys(), planted());
    lens.stop().await; // LENS goes away.

    // Rewind nothing; just ask again with LENS down.
    oracle.runner.run_once().await;
    let tick_error = oracle.runner.handle().status().tick_error.expect("a tick with LENS down must say why");
    assert!(tick_error.starts_with("transient LENS failure"), "an unreachable LENS is transient: {tick_error}");
    oracle.assert_all(&DetectorState::UpToDate);
    assert_eq!(oracle.anomalies(), before);
    for detector in default_detectors() {
        assert_eq!(oracle.store.progress(detector.id()).unwrap(), Some(LAST));
    }
}
