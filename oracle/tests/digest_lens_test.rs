//! `digest/` against the real LENS: PRISM's `ParquetWriter` writes a small
//! chain, `lens::serve_on` serves it, and every digest query runs in
//! DataFusion. The numbers are counted by hand, and rows just outside the
//! window prove the bounds.

mod common;

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::TempDir;
use forge::lens_client::{LensApi, LensClient};
use lens::{LensConfig, QueryEngine};
use oracle::digest::*;
use oracle::quality::{Auditor, Clock, TOKEN_PROGRAM_ID};
use oracle::store::Store;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Two slots per bucket: a window is 100 slots.
const BS: u64 = 2;
const FIRST: u64 = 1_000;
const LAST: u64 = 1_105;

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}
fn system() -> String {
    "11111111111111111111111111111111".to_owned()
}
fn sig(slot: u64, i: u32) -> String {
    format!("sig-{slot}-{i}")
}

fn tx(slot: u64, i: u32, ok: bool) -> DeepAnaTransaction {
    DeepAnaTransaction {
        slot,
        blockhash: format!("hash{slot}"),
        transaction_index: i,
        tx_signature: sig(slot, i),
        fee: 5_000,
        status: if ok { TransactionStatus::Success } else { TransactionStatus::Failed },
        err: (!ok).then(|| "custom program error".to_owned()),
        ..Default::default()
    }
}

fn ix(slot: u64, i: u32, index: u32, program: &str) -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot,
        tx_signature: sig(slot, i),
        instruction_index: index,
        inner_instruction_index: None,
        program_id: program.to_owned(),
        instruction_type: None,
        ..Default::default()
    }
}

fn xfer(slot: u64, i: u32, index: u32, mint: Option<String>) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        slot,
        tx_signature: sig(slot, i),
        instruction_index: index,
        inner_instruction_index: None,
        spl_token_program_id: TOKEN_PROGRAM_ID.to_owned(),
        source_token_account: key(10),
        destination_token_account: key(11),
        source_owner: None,
        destination_owner: None,
        mint_token_address: mint,
        amount_raw: 100,
        decimals: Some(6),
        block_time: Some(1_793_102_400),
    }
}

/// | slot | tx | status  | instructions               | transfers          |
/// |------|----|---------|----------------------------|--------------------|
/// | 1001 | 0  | success | Token, System              | mint 1             |
/// | 1001 | 1  | failed  | Token                      |                    |
/// | 1050 | 0  | success | other                      | mint 1             |
/// | 1099 | 0  | success | Token                      | mint 2; no mint    |
/// | 1100 | 0  | success | Token ×3 (next window)     | mint 2 ×3          |
/// | 1105 | 0  | success |                            |                    |
///
/// Window 10 (1000..=1099): 4 transactions, 1 failed, 5 instructions
/// (Token 3), 4 transfers (mint 1: 2, mint 2: 1, one without a mint).
fn write_chain(dir: &Path) {
    let txs = vec![tx(1_001, 0, true), tx(1_001, 1, false), tx(1_050, 0, true), tx(1_099, 0, true), tx(1_100, 0, true), tx(1_105, 0, true)];
    let ixs = vec![
        ix(1_001, 0, 0, TOKEN_PROGRAM_ID),
        ix(1_001, 0, 1, &system()),
        ix(1_001, 1, 0, TOKEN_PROGRAM_ID),
        ix(1_050, 0, 0, &key(20)),
        ix(1_099, 0, 0, TOKEN_PROGRAM_ID),
        ix(1_100, 0, 0, TOKEN_PROGRAM_ID),
        ix(1_100, 0, 1, TOKEN_PROGRAM_ID),
        ix(1_100, 0, 2, TOKEN_PROGRAM_ID),
    ];
    let xfers = vec![
        xfer(1_001, 0, 0, Some(key(1))),
        xfer(1_050, 0, 0, Some(key(1))),
        xfer(1_099, 0, 0, Some(key(2))),
        xfer(1_099, 0, 1, None),
        xfer(1_100, 0, 0, Some(key(2))),
        xfer(1_100, 0, 1, Some(key(2))),
        xfer(1_100, 0, 2, Some(key(2))),
    ];
    let mut writer = ParquetWriter::new(dir);
    for slot in FIRST..=LAST {
        let count = txs.iter().filter(|t| t.slot == slot).count() as u32;
        let s = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_793_102_400),
            block_height: Some(slot),
            transaction_count: count,
            leader: None,
        };
        if slot == FIRST {
            writer.append(&s, &txs, &ixs, &xfers).unwrap();
        } else {
            writer.append(&s, &[], &[], &[]).unwrap();
        }
    }
    writer.flush().unwrap();
}

struct RealLens {
    url: String,
    engine: Arc<QueryEngine>,
    server: Option<JoinHandle<lens::LensResult<()>>>,
}

impl RealLens {
    async fn start(data_dir: &Path) -> Self {
        let data_dir = data_dir.to_str().unwrap().to_owned();
        let config = LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data_dir.clone().into())).unwrap();
        let engine = Arc::new(QueryEngine::new(Arc::new(config)).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));
        Self { url, engine, server: Some(server) }
    }

    fn client(&self) -> Arc<dyn LensApi> {
        Arc::new(LensClient::from_parts(&self.url, Duration::from_secs(60)).unwrap())
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

struct FixedClock(AtomicU64);
impl Clock for FixedClock {
    fn now_unix_secs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

struct AllThrough(Mutex<u64>);
impl Coverage for AllThrough {
    fn detectors(&self) -> Vec<DetectorCoverage> {
        let b = *self.0.lock().unwrap();
        vec![DetectorCoverage { id: "tx_count".into(), enabled: true, evaluated_through: Some(b) }]
    }
}

struct Rig {
    _data: TempDir,
    _db: TempDir,
    _lens: RealLens,
    store: Store,
    digester: Digester,
}

async fn rig() -> Rig {
    let data = TempDir::new();
    write_chain(data.path());
    let lens = RealLens::start(data.path()).await;
    let db = TempDir::new();
    let store = Store::open_path(db.path().join("oracle.db")).unwrap();
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(AtomicU64::new(1_793_102_410)));
    let auditor = Arc::new(Auditor::new(lens.client(), Arc::clone(&clock)));
    let digester = Digester::new(store.clone(), lens.client(), auditor, Arc::new(AllThrough(Mutex::new(600))), clock, BS);
    Rig { _data: data, _db: db, _lens: lens, store, digester }
}

fn expected_programs() -> Vec<BusyProgram> {
    let mut ones = vec![system(), key(20)];
    ones.sort();
    let mut v = vec![BusyProgram { id: TOKEN_PROGRAM_ID.into(), instructions: 3 }];
    v.extend(ones.into_iter().map(|id| BusyProgram { id, instructions: 1 }));
    v
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_window_is_counted_exactly_and_bounded_to_its_slots() {
    let r = rig().await;
    let d = r.digester.handle().preview(Some(1_000)).await.unwrap();
    assert!(!d.provisional, "data runs to 1105 and the detector to bucket 600");
    assert_eq!((d.window.start_slot, d.window.end_slot, d.window.first_bucket, d.window.last_bucket), (1_000, 1_099, 500, 549));
    assert_eq!(d.overview, Overview { transactions: 4, failed_transactions: 1, instructions: 5, token_transfers: 4 });
    assert_eq!(d.busiest.programs, expected_programs());
    assert_eq!(
        d.busiest.mints,
        vec![BusyMint { id: key(1), transfers: 2 }, BusyMint { id: key(2), transfers: 1 }]
    );
    assert_eq!(d.headline[0], "Slots 1000–1099: 4 transactions, 1 failed (25.0%), 5 instructions, 4 token transfers.");
    assert!(d.headline.contains(&format!(
        "Busiest program: SPL Token ({TOKEN_PROGRAM_ID}) with 3 instructions (60.0% of all instructions)."
    )));
    assert!(d.headline.contains(&format!("Busiest mint: {} with 2 token transfers (50.0% of all token transfers).", key(1))));
    assert!(d.warnings.is_empty(), "{:?}", d.warnings);

    // The current window, 1100..=1199, holds data only through 1105.
    let p = r.digester.handle().preview(None).await.unwrap();
    assert!(p.provisional);
    assert_eq!(p.coverage.data_through_slot, Some(1_105));
    assert_eq!(p.overview, Overview { transactions: 2, failed_transactions: 0, instructions: 3, token_transfers: 3 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tick_stores_the_next_complete_window_and_nothing_before_now() {
    let r = rig().await;
    // No digests: "from now" is window 12 (complete_max = 552); nothing is due.
    r.digester.run_once().await;
    assert!(r.store.list_digests(10).unwrap().is_empty());

    // After window 9, window 10 is next and complete; window 11 is not.
    r.store.insert_digest(900, 999, &json!({"version": 1})).unwrap();
    r.digester.run_once().await;
    let latest = r.store.latest_digest().unwrap().unwrap();
    assert_eq!((latest.start_slot, latest.end_slot), (1_000, 1_099));
    let body: Digest = serde_json::from_value(latest.body).unwrap();
    assert_eq!(body.overview, Overview { transactions: 4, failed_transactions: 1, instructions: 5, token_transfers: 4 });
    assert_eq!(body.busiest.programs, expected_programs());
    assert!(body.coverage.complete && !body.provisional);
    assert_eq!(r.digester.handle().status().state, DigestState::Idle);
    assert_eq!(r.store.list_digests(10).unwrap().len(), 2);
}
