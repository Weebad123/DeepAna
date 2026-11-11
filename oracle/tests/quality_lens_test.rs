//! `quality/` against the real LENS: every check finds exactly its own fault
//! and never a false one.
//!
//! PRISM's `ParquetWriter` writes a small hand-built chain, `lens::serve_on`
//! serves it, and the auditor reaches it through FORGE's `lens_client`, so
//! every check's SQL is run by DataFusion.
//!
//! * **Clean chain**: every `fail` and `warn` check passes, and every number
//!   (examined, the info shares) matches a value counted by hand. The chain
//!   holds the traps a naive query falls into: skipped slots, top-level and
//!   inner instructions side by side, two transactions in one slot, a
//!   failed transaction.
//! * **One fault per test**: the clean chain plus one injected fault. The
//!   set of checks that do not pass is exactly the one that should fire,
//!   with the exact violation count and samples.
//! * **Duplicates are tolerated**: re-writing a range of slots, as PRISM
//!   does after a crash, fires the four duplicate checks and nothing else,
//!   and leaves every other number unchanged.

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use common::TempDir;
use forge::lens_client::{LensApi, LensClient};
use lens::{LensConfig, QueryEngine};
use oracle::quality::*;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

const FIRST: u64 = 1_000;
const LAST: u64 = 1_099;
/// Skipped by the leader: no row, and 1012's parent is 1009.
const SKIPPED: [u64; 2] = [1_010, 1_011];
/// Wider than the chain, so the window reaches before the first slot.
const WINDOW: u64 = 200;

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}
fn mint() -> String {
    key(1)
}
fn mint2() -> String {
    key(2)
}
fn other_program() -> String {
    key(20)
}
fn system() -> String {
    "11111111111111111111111111111111".to_owned()
}

fn block_time(slot: u64) -> i64 {
    1_793_102_400 + (slot as i64 - FIRST as i64) / 2
}
const NEWEST_BLOCK_TIME: i64 = 1_793_102_400 + (LAST - FIRST) as i64 / 2;

fn sig(slot: u64, i: u32) -> String {
    format!("sig-{slot}-{i}")
}

/// Every row of the chain, as PRISM would write them.
#[derive(Clone)]
struct Chain {
    slots: Vec<DeepAnaSlot>,
    txs: Vec<DeepAnaTransaction>,
    ixs: Vec<DeepAnaInstruction>,
    xfers: Vec<DeepAnaTokenTransfer>,
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

fn ix(slot: u64, i: u32, index: u32, inner: Option<u32>, program: &str, ty: Option<&str>) -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot,
        tx_signature: sig(slot, i),
        instruction_index: index,
        inner_instruction_index: inner,
        program_id: program.to_owned(),
        instruction_type: ty.map(str::to_owned),
        ..Default::default()
    }
}

fn xfer(
    slot: u64,
    i: u32,
    inner: Option<u32>,
    program: &str,
    mint: Option<String>,
    decimals: Option<u8>,
    owner: bool,
) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        slot,
        tx_signature: sig(slot, i),
        instruction_index: 0,
        inner_instruction_index: inner,
        spl_token_program_id: program.to_owned(),
        source_token_account: key(10),
        destination_token_account: key(11),
        source_owner: owner.then(|| key(30)),
        destination_owner: None,
        mint_token_address: mint,
        amount_raw: 100,
        decimals,
        block_time: Some(block_time(slot)),
    }
}

/// The clean chain.
///
/// | slot | tx | status  | instructions                                        | transfer row                      |
/// |------|----|---------|-----------------------------------------------------|-----------------------------------|
/// | 1020 | 0  | success | 0: Token `Transfer`                                 | (0, -) no mint, no decimals       |
/// | 1030 | 0  | success | 0: other program, undecoded; (0, 0): Token `TransferChecked` | (0, 0) mint, 6             |
/// | 1040 | 0  | failed  | 0: Token-2022 `TransferCheckedWithFee`              | (0, -) mint2, 9                   |
/// | 1050 | 0  | success | 0: Token `TransferChecked`                          | (0, -) mint, 6, owner resolved    |
/// | 1050 | 1  | success | 0: Token `MintTo`; 1: other program                 | none                              |
/// | 1060 | 0  | success | 0: system program, undecoded                        | none                              |
fn clean() -> Chain {
    let mut c = Chain { slots: vec![], txs: vec![], ixs: vec![], xfers: vec![] };
    c.txs = vec![tx(1_020, 0, true), tx(1_030, 0, true), tx(1_040, 0, false), tx(1_050, 0, true), tx(1_050, 1, true), tx(1_060, 0, true)];
    c.ixs = vec![
        ix(1_020, 0, 0, None, TOKEN_PROGRAM_ID, Some("Transfer")),
        ix(1_030, 0, 0, None, &other_program(), None),
        ix(1_030, 0, 0, Some(0), TOKEN_PROGRAM_ID, Some("TransferChecked")),
        ix(1_040, 0, 0, None, TOKEN_2022_PROGRAM_ID, Some("TransferCheckedWithFee")),
        ix(1_050, 0, 0, None, TOKEN_PROGRAM_ID, Some("TransferChecked")),
        ix(1_050, 1, 0, None, TOKEN_PROGRAM_ID, Some("MintTo")),
        ix(1_050, 1, 1, None, &other_program(), None),
        ix(1_060, 0, 0, None, &system(), None),
    ];
    c.xfers = vec![
        xfer(1_020, 0, None, TOKEN_PROGRAM_ID, None, None, false),
        xfer(1_030, 0, Some(0), TOKEN_PROGRAM_ID, Some(mint()), Some(6), false),
        xfer(1_040, 0, None, TOKEN_2022_PROGRAM_ID, Some(mint2()), Some(9), false),
        xfer(1_050, 0, None, TOKEN_PROGRAM_ID, Some(mint()), Some(6), true),
    ];
    let mut parent = FIRST - 1;
    for slot in FIRST..=LAST {
        if SKIPPED.contains(&slot) {
            continue;
        }
        c.slots.push(DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: parent,
            block_time: Some(block_time(slot)),
            block_height: Some(slot),
            transaction_count: 0,
            leader: None,
        });
        parent = slot;
    }
    c.recount();
    c
}

impl Chain {
    /// Set every `transaction_count` from the distinct transactions.
    fn recount(&mut self) {
        for s in &mut self.slots {
            let sigs: BTreeSet<_> = self.txs.iter().filter(|t| t.slot == s.slot).map(|t| &t.tx_signature).collect();
            s.transaction_count = sigs.len() as u32;
        }
    }

    fn slot_mut(&mut self, slot: u64) -> &mut DeepAnaSlot {
        self.slots.iter_mut().find(|s| s.slot == slot).unwrap()
    }
}

/// One flush of everything: one file per table.
fn write(dir: &Path, c: &Chain) {
    write_flush(dir, &c.slots, &c.txs, &c.ixs, &c.xfers);
}

/// One flush, as one PRISM flush writes it: the slots' range names the files.
fn write_flush(
    dir: &Path,
    slots: &[DeepAnaSlot],
    txs: &[DeepAnaTransaction],
    ixs: &[DeepAnaInstruction],
    xfers: &[DeepAnaTokenTransfer],
) {
    let mut writer = ParquetWriter::new(dir);
    for (n, slot) in slots.iter().enumerate() {
        if n == 0 {
            writer.append(slot, txs, ixs, xfers).unwrap();
        } else {
            writer.append(slot, &[], &[], &[]).unwrap();
        }
    }
    writer.flush().unwrap();
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

/// Ten seconds after the newest block: fresh.
const NOW: u64 = NEWEST_BLOCK_TIME as u64 + 10;

async fn audit_with(dir: &TempDir, now: u64) -> QualityReport {
    let lens = RealLens::start(dir.path()).await;
    let auditor = Auditor::new(lens.client(), Arc::new(FixedClock(AtomicU64::new(now))));
    auditor.run(AuditRequest { window_slots: Some(WINDOW), as_of_slot: None }).await.unwrap()
}

/// Write `c` alone and audit it.
async fn audit(c: &Chain) -> QualityReport {
    let dir = TempDir::new();
    write(dir.path(), c);
    audit_with(&dir, NOW).await
}

fn is_fault_check(id: &str) -> bool {
    def(id).unwrap().severity != Severity::Info
}

/// The fail/warn checks that did not pass.
fn fired(r: &QualityReport) -> BTreeSet<&'static str> {
    r.checks.iter().filter(|c| is_fault_check(c.id) && c.status != Status::Pass).map(|c| c.id).collect()
}

/// Assert exactly `id` fired, with `violations` and `samples`.
#[track_caller]
fn only(r: &QualityReport, id: &str, status: Status, violations: u64, samples: &[String]) {
    assert_eq!(fired(r), BTreeSet::from([def(id).unwrap().id]), "{:#?}", r.checks);
    let c = r.check(id).unwrap();
    assert_eq!(c.status, status, "{c:#?}");
    assert_eq!(c.violations, violations, "{c:#?}");
    assert_eq!(c.samples, samples, "{c:#?}");
    assert_eq!(c.message, def(id).unwrap().message(status));
    assert_eq!(c.query_count, 2, "a count, then a sample: {c:#?}");
    assert!(c.error_kind.is_none(), "{c:#?}");
}

fn examined(r: &QualityReport, id: &str) -> u64 {
    r.check(id).unwrap().examined
}

fn info(r: &QualityReport, id: &str) -> (u64, u64) {
    let c = r.check(id).unwrap();
    assert_eq!(c.status, Status::Info, "{c:#?}");
    (c.violations, c.examined)
}

// ---------------------------------------------------------------------------
// The clean chain: no false positives, every number by hand
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_clean_chain_passes_every_fault_check_with_hand_counted_numbers() {
    let r = audit(&clean()).await;

    assert_eq!(r.as_of_slot, 1_060, "W: slots run to 1099, transactions only to 1060");
    assert_eq!(r.window, Window { from_slot: 861, to_slot: 1_060, slots: WINDOW });
    assert_eq!(r.generated_at, NOW);
    assert_eq!(fired(&r), BTreeSet::new(), "{:#?}", r.checks);
    let ids: Vec<_> = r.checks.iter().map(|c| c.id).collect();
    assert_eq!(ids, CHECKS.iter().map(|c| c.id).collect::<Vec<_>>());

    // Slots 1000..=1060 minus the two skipped: 59 slots.
    assert_eq!(examined(&r, "slot_duplicates"), 59);
    assert_eq!(examined(&r, "slot_tx_count_mismatch"), 59);
    // 1000's parent is before the oldest stored slot, so it is not judged;
    // 1012's parent is 1009, not 1011: a skip is not a gap.
    assert_eq!(examined(&r, "parent_link_missing"), 58);
    // Slots with a slot row or transaction row.
    assert_eq!(examined(&r, "blockhash_mismatch"), 59);
    assert_eq!(examined(&r, "duplicate_transactions"), 6);
    assert_eq!(examined(&r, "orphan_transactions"), 6);
    assert_eq!(examined(&r, "tx_without_instructions"), 6);
    assert_eq!(examined(&r, "duplicate_instructions"), 8);
    assert_eq!(examined(&r, "orphan_instructions"), 8);
    assert_eq!(examined(&r, "duplicate_transfers"), 4);
    assert_eq!(examined(&r, "orphan_transfers"), 4);
    // Token and Token-2022 instructions: 1020, 1030 inner, 1040, 1050 x2.
    assert_eq!(examined(&r, "spl_decode_rate"), 5);
    // The inner transfer at 1030 is matched despite the NULL trap, and so
    // are the three top-level ones.
    assert_eq!(examined(&r, "transfer_row_coverage"), 4);
    assert_eq!(examined(&r, "transfer_checked_completeness"), 3);
    assert_eq!(examined(&r, "mint_decimals_conflict"), 2);

    let fresh = r.check("data_freshness").unwrap();
    assert_eq!((fresh.status, fresh.lag_secs, fresh.examined), (Status::Pass, Some(10), 1));

    assert_eq!(info(&r, "transfers_in_failed_tx"), (1, 4));
    assert_eq!(info(&r, "unattributed_transfers"), (1, 4));
    assert_eq!(info(&r, "owner_resolution"), (1, 4));
    // PRISM's writer does not write is_finalized yet.
    let fin = r.check("finalized_flag").unwrap();
    assert_eq!(fin.status, Status::NotReady, "{fin:#?}");
    assert_eq!(fin.message, MSG_NOT_READY);

    // ...and an info check that cannot run does not make the audit incomplete.
    assert_eq!(r.overall, Overall::Pass);
    assert_eq!(r.summary, StatusSummary { pass: 16, warn: 0, fail: 0, info: 3, error: 0, not_ready: 1 });
    // Two snapshot queries and one count per check: nothing to sample.
    assert_eq!(r.queries_run, SNAPSHOT_QUERIES + CHECKS.len() as u32);
    for c in &r.checks {
        assert_eq!(c.query_count, 1, "{c:#?}");
        assert!(c.samples.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_audit_is_reproducible_and_bounded() {
    let dir = TempDir::new();
    write(dir.path(), &clean());
    let lens = RealLens::start(dir.path()).await;
    let auditor = Auditor::new(lens.client(), Arc::new(FixedClock(AtomicU64::new(NOW))));
    // Pinned before 1030: only 1020's transaction is in the window.
    let req = AuditRequest { window_slots: Some(40), as_of_slot: Some(1_025) };
    let strip = |mut r: QualityReport| {
        r.duration_ms = 0;
        r.checks.iter_mut().for_each(|c| c.duration_ms = 0);
        r
    };
    let a = strip(auditor.run(req).await.unwrap());
    let b = strip(auditor.run(req).await.unwrap());
    assert_eq!(a, b);
    assert_eq!(serde_json::to_string(&a).unwrap(), serde_json::to_string(&b).unwrap());
    assert_eq!(a.window, Window { from_slot: 986, to_slot: 1_025, slots: 40 });
    assert_eq!(examined(&a, "duplicate_transactions"), 1);
    assert_eq!(examined(&a, "slot_duplicates"), 24); // 1000..=1025 minus 2
    assert_eq!(fired(&a), BTreeSet::new());

    let err = auditor.run(AuditRequest { window_slots: None, as_of_slot: Some(1_061) }).await.unwrap_err();
    assert!(matches!(err, QualityError::Invalid { ref field, .. } if field == "as_of_slot"), "{err:?}");
}

// ---------------------------------------------------------------------------
// One fault each: exactly its own check fires
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slot_duplicates_finds_a_re_written_slot_row() {
    let dir = TempDir::new();
    let c = clean();
    write(dir.path(), &c);
    // A second flush of slot 1033 alone, as after a crash.
    write_flush(dir.path(), &[c.slots.iter().find(|s| s.slot == 1_033).unwrap().clone()], &[], &[], &[]);
    let r = audit_with(&dir, NOW).await;
    only(&r, "slot_duplicates", Status::Fail, 1, &["1033".into()]);
    assert_eq!(examined(&r, "slot_duplicates"), 59, "keys, not rows");
    assert_eq!(r.overall, Overall::Fail);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_link_missing_finds_a_dropped_block_and_not_a_skipped_one() {
    let mut c = clean();
    c.slots.retain(|s| s.slot != 1_044);
    let r = audit(&c).await;
    only(&r, "parent_link_missing", Status::Fail, 1, &["1045".into()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blockhash_mismatch_finds_a_transaction_from_another_fork() {
    let mut c = clean();
    c.txs.iter_mut().find(|t| t.tx_signature == sig(1_050, 1)).unwrap().blockhash = "forked".into();
    let r = audit(&c).await;
    only(&r, "blockhash_mismatch", Status::Warn, 1, &["1050".into()]);
    assert_eq!(r.overall, Overall::Warn);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_transactions_finds_a_doubled_row_and_the_count_check_is_not_fooled() {
    let mut c = clean();
    let dup = c.txs.iter().find(|t| t.slot == 1_060).unwrap().clone();
    c.txs.push(dup);
    let r = audit(&c).await;
    only(&r, "duplicate_transactions", Status::Fail, 1, &[format!("1060:{}", sig(1_060, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_instructions_finds_a_doubled_top_level_row() {
    let mut c = clean();
    let dup = c.ixs.iter().find(|i| i.slot == 1_060).unwrap().clone();
    c.ixs.push(dup);
    let r = audit(&c).await;
    // NULL inner indexes group together: the top-level row is caught.
    only(&r, "duplicate_instructions", Status::Fail, 1, &[format!("1060:{}:0:-", sig(1_060, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_transfers_finds_a_doubled_row_and_the_shares_ignore_it() {
    let mut c = clean();
    let dup = c.xfers.iter().find(|x| x.slot == 1_050).unwrap().clone();
    c.xfers.push(dup);
    let r = audit(&c).await;
    only(&r, "duplicate_transfers", Status::Fail, 1, &[format!("1050:{}:0:-", sig(1_050, 0))]);
    assert_eq!(info(&r, "owner_resolution"), (1, 4));
    assert_eq!(examined(&r, "transfer_checked_completeness"), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slot_tx_count_mismatch_finds_a_missing_transaction() {
    let mut c = clean();
    c.slot_mut(1_060).transaction_count = 2;
    let r = audit(&c).await;
    only(&r, "slot_tx_count_mismatch", Status::Fail, 1, &["1060".into()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_duplicate_does_not_hide_a_dropped_transaction() {
    let mut c = clean();
    // 1050 says 2 transactions. Drop tx 1 (and its instructions) and double
    // tx 0: two rows, as many as transaction_count, but one transaction.
    c.txs.retain(|t| t.tx_signature != sig(1_050, 1));
    c.ixs.retain(|i| i.tx_signature != sig(1_050, 1));
    let dup = c.txs.iter().find(|t| t.slot == 1_050).unwrap().clone();
    c.txs.push(dup);
    let r = audit(&c).await;
    assert_eq!(fired(&r), BTreeSet::from(["duplicate_transactions", "slot_tx_count_mismatch"]), "{:#?}", r.checks);
    assert_eq!(r.check("slot_tx_count_mismatch").unwrap().samples, ["1050"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_transactions_finds_a_transaction_in_a_slot_without_a_row() {
    let mut c = clean();
    // In a skipped slot: no slot row will ever exist for it.
    c.txs.push(tx(1_010, 0, true));
    c.ixs.push(ix(1_010, 0, 0, None, &system(), None));
    let r = audit(&c).await;
    only(&r, "orphan_transactions", Status::Fail, 1, &[format!("1010:{}", sig(1_010, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_instructions_finds_an_instruction_without_its_transaction() {
    let mut c = clean();
    c.ixs.push(ix(1_060, 7, 0, Some(3), &system(), None));
    let r = audit(&c).await;
    only(&r, "orphan_instructions", Status::Fail, 1, &[format!("1060:{}:0:3", sig(1_060, 7))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn orphan_transfers_finds_a_transfer_without_its_transaction() {
    let mut c = clean();
    c.xfers.push(xfer(1_060, 7, None, TOKEN_PROGRAM_ID, Some(mint()), Some(6), false));
    let r = audit(&c).await;
    only(&r, "orphan_transfers", Status::Fail, 1, &[format!("1060:{}:0:-", sig(1_060, 7))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tx_without_instructions_finds_a_bare_transaction() {
    let mut c = clean();
    c.ixs.retain(|i| i.slot != 1_060);
    let r = audit(&c).await;
    only(&r, "tx_without_instructions", Status::Warn, 1, &[format!("1060:{}", sig(1_060, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spl_decode_rate_finds_an_undecoded_token_instruction_only() {
    let mut c = clean();
    // Undecoded Token-2022 instruction: counted. The undecoded system and
    // other-program instructions in the clean chain never were.
    c.ixs.push(ix(1_060, 0, 1, None, TOKEN_2022_PROGRAM_ID, None));
    let r = audit(&c).await;
    only(&r, "spl_decode_rate", Status::Warn, 1, &[format!("1060:{}:1:-", sig(1_060, 0))]);
    assert_eq!(examined(&r, "spl_decode_rate"), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_row_coverage_finds_a_decoded_transfer_without_its_row() {
    let mut c = clean();
    c.xfers.retain(|x| x.slot != 1_020);
    let r = audit(&c).await;
    only(&r, "transfer_row_coverage", Status::Fail, 1, &[format!("1020:{}:0:-", sig(1_020, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_row_coverage_matches_an_inner_transfer_by_its_inner_index() {
    let mut c = clean();
    // The inner TransferChecked's row now claims another inner index.
    c.xfers.iter_mut().find(|x| x.slot == 1_030).unwrap().inner_instruction_index = Some(1);
    let r = audit(&c).await;
    assert_eq!(fired(&r), BTreeSet::from(["transfer_row_coverage"]), "{:#?}", r.checks);
    assert_eq!(r.check("transfer_row_coverage").unwrap().samples, [format!("1030:{}:0:0", sig(1_030, 0))]);
    // The row is no longer joined to a checked instruction.
    assert_eq!(examined(&r, "transfer_checked_completeness"), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_checked_completeness_finds_a_checked_transfer_without_a_mint() {
    let mut c = clean();
    let x = c.xfers.iter_mut().find(|x| x.slot == 1_050).unwrap();
    x.mint_token_address = None;
    let r = audit(&c).await;
    only(&r, "transfer_checked_completeness", Status::Fail, 1, &[format!("1050:{}:0:-", sig(1_050, 0))]);
    // The plain Transfer at 1020 also lacks both, and is not a fault.
    assert_eq!(info(&r, "unattributed_transfers"), (2, 4));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_checked_completeness_finds_an_inner_checked_transfer_without_decimals() {
    let mut c = clean();
    c.xfers.iter_mut().find(|x| x.slot == 1_030).unwrap().decimals = None;
    let r = audit(&c).await;
    only(&r, "transfer_checked_completeness", Status::Fail, 1, &[format!("1030:{}:0:0", sig(1_030, 0))]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mint_decimals_conflict_finds_a_mint_with_two_decimals() {
    let mut c = clean();
    c.xfers.iter_mut().find(|x| x.slot == 1_030).unwrap().decimals = Some(9);
    let r = audit(&c).await;
    only(&r, "mint_decimals_conflict", Status::Fail, 1, &[mint()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_freshness_warns_when_the_newest_block_is_old() {
    let dir = TempDir::new();
    write(dir.path(), &clean());
    let r = audit_with(&dir, NEWEST_BLOCK_TIME as u64 + FRESHNESS_WARN_SECS + 1).await;
    assert_eq!(fired(&r), BTreeSet::from(["data_freshness"]), "{:#?}", r.checks);
    let c = r.check("data_freshness").unwrap();
    assert_eq!((c.status, c.violations, c.lag_secs), (Status::Warn, 1, Some(FRESHNESS_WARN_SECS + 1)));

    // Exactly at the threshold is still fresh, and a clock behind the
    // newest block_time is a lag of 0, not a wrap-around.
    for (now, lag) in [(NEWEST_BLOCK_TIME as u64 + FRESHNESS_WARN_SECS, FRESHNESS_WARN_SECS), (NEWEST_BLOCK_TIME as u64 - 5, 0)] {
        let r = audit_with(&dir, now).await;
        let c = r.check("data_freshness").unwrap();
        assert_eq!((c.status, c.lag_secs), (Status::Pass, Some(lag)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_freshness_is_not_ready_without_any_block_time() {
    let mut c = clean();
    c.slots.iter_mut().for_each(|s| s.block_time = None);
    let r = audit(&c).await;
    assert_eq!(fired(&r), BTreeSet::from(["data_freshness"]));
    let f = r.check("data_freshness").unwrap();
    assert_eq!((f.status, f.lag_secs), (Status::NotReady, None));
    assert_eq!(f.message, "no slot has a block_time yet");
    assert_eq!(r.overall, Overall::Incomplete, "an unknown can hide a failure");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_freshness_reads_past_a_pinned_snapshot() {
    let dir = TempDir::new();
    write(dir.path(), &clean());
    let lens = RealLens::start(dir.path()).await;
    let auditor = Auditor::new(lens.client(), Arc::new(FixedClock(AtomicU64::new(NOW))));
    let r = auditor.run(AuditRequest { window_slots: Some(10), as_of_slot: Some(1_020) }).await.unwrap();
    assert_eq!(r.check("data_freshness").unwrap().lag_secs, Some(10), "the newest block, not the pin's");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_info_shares_count_their_own_property() {
    let mut c = clean();
    // 1060 fails, and gains a transfer with a mint and both owners.
    c.txs.iter_mut().find(|t| t.slot == 1_060).unwrap().status = TransactionStatus::Failed;
    c.ixs.push(ix(1_060, 0, 1, None, TOKEN_PROGRAM_ID, Some("TransferChecked")));
    let mut x = xfer(1_060, 0, None, TOKEN_PROGRAM_ID, Some(mint()), Some(6), true);
    x.instruction_index = 1;
    x.destination_owner = Some(key(31));
    c.xfers.push(x);
    let r = audit(&c).await;
    assert_eq!(fired(&r), BTreeSet::new(), "{:#?}", r.checks);
    assert_eq!(info(&r, "transfers_in_failed_tx"), (2, 5));
    assert_eq!(info(&r, "unattributed_transfers"), (1, 5));
    assert_eq!(info(&r, "owner_resolution"), (2, 5));
    assert_eq!(r.overall, Overall::Pass);
}

// ---------------------------------------------------------------------------
// Duplicates are tolerated
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_re_written_range_fires_the_duplicate_checks_and_nothing_else() {
    let dir = TempDir::new();
    let c = clean();
    write(dir.path(), &c);
    // After a crash PRISM re-writes 1020..=1050 in a new flush.
    let in_range = |slot: u64| (1_020..=1_050).contains(&slot);
    write_flush(
        dir.path(),
        &c.slots.iter().filter(|s| in_range(s.slot)).cloned().collect::<Vec<_>>(),
        &c.txs.iter().filter(|t| in_range(t.slot)).cloned().collect::<Vec<_>>(),
        &c.ixs.iter().filter(|i| in_range(i.slot)).cloned().collect::<Vec<_>>(),
        &c.xfers.iter().filter(|x| in_range(x.slot)).cloned().collect::<Vec<_>>(),
    );
    let r = audit_with(&dir, NOW).await;
    let base = audit(&c).await;

    assert_eq!(
        fired(&r),
        BTreeSet::from(["duplicate_instructions", "duplicate_transactions", "duplicate_transfers", "slot_duplicates"]),
        "{:#?}",
        r.checks
    );
    assert_eq!(r.check("slot_duplicates").unwrap().violations, 31);
    assert_eq!(r.check("duplicate_transactions").unwrap().violations, 5);
    assert_eq!(r.check("duplicate_instructions").unwrap().violations, 7);
    assert_eq!(r.check("duplicate_transfers").unwrap().violations, 4);
    assert_eq!(r.check("slot_duplicates").unwrap().samples, ["1020", "1021", "1022", "1023", "1024"]);
    // Every other check sees the same keys and the same numbers.
    for (a, b) in r.checks.iter().zip(&base.checks) {
        if !a.id.starts_with("duplicate_") && a.id != "slot_duplicates" {
            assert_eq!((a.id, a.status, a.examined, a.violations), (b.id, b.status, b.examined, b.violations));
        }
    }
}
