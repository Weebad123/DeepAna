//! `spec/` against the real LENS.
//!
//! PRISM's `ParquetWriter` writes a small hand-built chain; `lens::serve_on`
//! serves it; the runner reaches it through FORGE's `lens_client`. So every
//! compiled statement is planned and run by DataFusion, and every number is
//! checked against a value worked out by hand from the chain below:
//!
//! * execution: counts, exact decimal volumes above `u64::MAX`, per-mint
//!   null exclusion, raw-column filters, ratios and averages, grouping;
//! * ordering: ties broken by the group-by dimensions, decimal sums sorted
//!   as numbers, nulls last in both directions;
//! * windows: `last_slots` ends at `W`, ranges, the inward bucket snap, the
//!   `as_of_slot` pin reproducing an answer after more data arrives;
//! * `limit + 1`, LENS's own truncation flagged, a missing table not ready;
//! * every spec the generator accepts runs on DataFusion and shapes.

mod common;
mod spec_support;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::TempDir;
use forge::lens_client::{LensApi, LensClient};
use lens::{LensConfig, QueryEngine};
use oracle::catalog::Catalog;
use oracle::spec::*;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::{Value, json};
use spec_support::{KEY_A, KEY_B, filter, group, order};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

/// SPL Token and Token-2022: what the transfers are filtered to.
const SPL: &str = KEY_A;
const T22: &str = KEY_B;
fn other_program() -> String {
    key(9)
}
fn m1() -> String {
    key(1)
}
fn m2() -> String {
    key(2)
}
fn m3() -> String {
    key(3)
}
fn owner(n: u8) -> String {
    key(30 + n)
}
fn leader() -> String {
    key(40)
}

/// Slots 850..=1199 are written; transactions only up to 1190, so `W` is 1190.
const FIRST_SLOT: u64 = 850;
const LAST_SLOT: u64 = 1_199;
const W: u64 = 1_190;
const MAX: u64 = u64::MAX;

struct Xfer {
    slot: u64,
    mint: Option<String>,
    amount: u64,
    program: &'static str,
    source_owner: Option<String>,
}

fn xfer(slot: u64, mint: Option<String>, amount: u64, program: &'static str, source_owner: Option<String>) -> Xfer {
    Xfer { slot, mint, amount, program, source_owner }
}

/// One transfer per listed slot, each in its own transaction.
fn transfers() -> Vec<Xfer> {
    let o1 = || Some(owner(1));
    vec![
        xfer(900, Some(m1()), 1_000, SPL, o1()),
        xfer(1_010, Some(m1()), MAX, SPL, o1()),
        xfer(1_020, Some(m1()), MAX, SPL, o1()),
        xfer(1_030, Some(m2()), 9, SPL, Some(owner(2))),
        xfer(1_040, Some(m2()), 1, T22, Some(owner(2))),
        xfer(1_050, Some(m3()), 100, SPL, None),
        xfer(1_060, None, 5, SPL, o1()),
        xfer(1_070, None, 7, T22, o1()),
        xfer(1_080, Some(m3()), 5, "OTHER", o1()),
        xfer(1_100, Some(m2()), 9, SPL, Some(owner(2))),
        xfer(1_150, Some(m1()), 1, T22, o1()),
    ]
}

/// Transactions with no transfer: `(slot, ok, fee)`.
fn plain_txs() -> Vec<(u64, bool, u64)> {
    vec![(1_000, true, 5_000), (1_000, false, 7_000), (1_120, false, 10_000), (W, true, 5_000)]
}

/// Transfer transactions: all succeed, fee 5000, except 1050 which fails.
fn transfer_tx_ok(slot: u64) -> bool {
    slot != 1_050
}

fn write(dir: &Path, xfers: &[Xfer], plain: &[(u64, bool, u64)], tx_until: u64, lo: u64, hi: u64) {
    let mut writer = ParquetWriter::new(dir);
    for slot in lo..=hi {
        let mut txs = Vec::new();
        let mut ixs = Vec::new();
        let mut rows = Vec::new();
        let mut add_tx = |ok: bool, fee: u64| -> String {
            let sig = format!("sig-{slot}-{}", txs.len());
            txs.push(DeepAnaTransaction {
                slot,
                blockhash: format!("hash{slot}"),
                transaction_index: txs.len() as u32,
                tx_signature: sig.clone(),
                fee,
                status: if ok { TransactionStatus::Success } else { TransactionStatus::Failed },
                err: (!ok).then(|| "custom program error".to_owned()),
                compute_units_consumed: Some(1_000),
                ..Default::default()
            });
            sig
        };
        if slot <= tx_until {
            for &(_, ok, fee) in plain.iter().filter(|t| t.0 == slot) {
                add_tx(ok, fee);
            }
            for (k, x) in xfers.iter().filter(|x| x.slot == slot).enumerate() {
                let sig = add_tx(transfer_tx_ok(slot), 5_000);
                ixs.push(DeepAnaInstruction {
                    slot,
                    tx_signature: sig.clone(),
                    instruction_index: 0,
                    inner_instruction_index: (k % 2 == 1).then_some(0),
                    program_id: if x.program == "OTHER" { other_program() } else { x.program.to_owned() },
                    instruction_type: x.mint.as_ref().map(|_| "transfer".to_owned()),
                    ..Default::default()
                });
                rows.push(DeepAnaTokenTransfer {
                    slot,
                    tx_signature: sig,
                    instruction_index: 0,
                    spl_token_program_id: if x.program == "OTHER" { other_program() } else { x.program.to_owned() },
                    source_token_account: key(10),
                    destination_token_account: key(11),
                    source_owner: x.source_owner.clone(),
                    destination_owner: Some(owner(9)),
                    mint_token_address: x.mint.clone(),
                    amount_raw: x.amount,
                    decimals: x.mint.as_ref().map(|_| 6),
                    block_time: Some(1_793_102_400 + slot as i64),
                    ..Default::default()
                });
            }
        }
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot.saturating_sub(1),
            block_time: Some(1_793_102_400 + slot as i64),
            block_height: Some(slot),
            transaction_count: txs.len() as u32,
            leader: (slot % 2 == 0).then(leader),
        };
        writer.append(&row, &txs, &ixs, &rows).unwrap();
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

    fn client(&self) -> Arc<dyn LensApi> {
        Arc::new(LensClient::from_parts(&self.url, Duration::from_secs(60)).unwrap())
    }

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

fn runner_on(lens: &RealLens) -> SpecRunner {
    SpecRunner::with_limits(Arc::new(Catalog::new(vec![]).unwrap()), lens.client(), spec_support::limits())
}

async fn setup(vars: &[(&str, &str)]) -> (TempDir, RealLens, SpecRunner) {
    let dir = TempDir::new();
    write(dir.path(), &transfers(), &plain_txs(), W, FIRST_SLOT, LAST_SLOT);
    let lens = RealLens::start(dir.path(), vars).await;
    let runner = runner_on(&lens);
    (dir, lens, runner)
}

async fn ask(r: &SpecRunner, s: &QuerySpec) -> SpecAnswer {
    r.run(s, &CancellationToken::new()).await.unwrap_or_else(|e| panic!("{e}\n{s:#?}"))
}

fn base(measures: &[&str], last_slots: u64) -> QuerySpec {
    QuerySpec { window: Window::LastSlots(last_slots), limit: Some(10), ..spec_support::spec(measures) }
}

/// The by-mint question of the design, with catalog names: transfers and
/// raw volume per mint, SPL Token and Token-2022 only, last 250 slots.
fn by_mint(order_by: Vec<Order>) -> QuerySpec {
    QuerySpec {
        group_by: vec![group("mint")],
        filters: vec![filter("token_program", FilterOp::In, &[SPL, T22])],
        order_by,
        ..base(&["transfer_count", "transfer_volume_raw"], 250)
    }
}

fn row(mint: &str, n: u64, volume: Option<&str>) -> Vec<Value> {
    vec![json!(mint), json!(n), volume.map_or(Value::Null, |v| json!(v))]
}

/// `(a, b)` sorted by key ascending.
fn asc(a: String, b: String) -> (String, String) {
    if a < b { (a, b) } else { (b, a) }
}

/// The answer with its only non-deterministic field cleared.
fn stable(mut a: SpecAnswer) -> SpecAnswer {
    a.duration_ms = 0;
    a
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_by_mint_question_matches_the_chain() {
    let (_dir, _lens, r) = setup(&[]).await;
    let a = ask(&r, &by_mint(vec![order("transfer_count", Direction::Desc)])).await;

    // W: solana.slots runs to 1199, solana.transactions only to 1190.
    assert_eq!(a.as_of_slot, W);
    assert_eq!(a.window, SlotWindow::new(941, W));
    assert_eq!(a.queries_run, 3);
    assert!(!a.has_more && !a.truncated);

    // In 941..=1190, SPL or Token-2022 (1080's program is neither; 900 is
    // before the window):
    //   m1: 1010, 1020, 1150  → 3, MAX + MAX + 1
    //   m2: 1030, 1040, 1100  → 3, 9 + 1 + 9
    //   no mint: 1060, 1070   → 2, not summed (the volume is per mint)
    //   m3: 1050              → 1, 100
    let m1_volume = (2 * u128::from(MAX) + 1).to_string();
    assert_eq!(m1_volume, "36893488147419103231");
    let (first, second) = asc(m1(), m2());
    let volume_of = |m: &String| if *m == m1() { m1_volume.as_str() } else { "19" };
    assert_eq!(
        a.rows,
        vec![
            // m1 and m2 tie on 3: the mint breaks it, ascending.
            row(&first, 3, Some(volume_of(&first))),
            row(&second, 3, Some(volume_of(&second))),
            row("unattributed", 2, None),
            row(&m3(), 1, Some("100")),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_mint_volume_alone_excludes_rows_with_no_mint() {
    let (_dir, _lens, r) = setup(&[]).await;
    let s = QuerySpec { measures: vec!["transfer_volume_raw".into()], ..by_mint(vec![]) };
    let a = ask(&r, &s).await;
    // No `unattributed` group: the exclusion is the WHERE clause here.
    let mints: Vec<&Value> = a.rows.iter().map(|r| &r[0]).collect();
    assert!(!mints.contains(&&json!("unattributed")), "{mints:?}");
    assert_eq!(a.rows.len(), 3);

    // Pinned to one mint by a filter instead of grouped.
    let s = QuerySpec {
        group_by: vec![],
        filters: vec![filter("mint", FilterOp::Eq, &[&m2()])],
        ..base(&["transfer_count", "transfer_volume_raw"], 250)
    };
    assert_eq!(ask(&r, &s).await.rows, vec![vec![json!(3), json!("19")]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decimal_sums_sort_as_numbers_and_nulls_sort_last_both_ways() {
    let (_dir, _lens, r) = setup(&[]).await;
    let volumes = |a: &SpecAnswer| a.rows.iter().map(|r| r[2].clone()).collect::<Vec<_>>();
    let big = json!((2 * u128::from(MAX) + 1).to_string());

    // As text, "19" > "100"; as numbers it is the other way round.
    let desc = ask(&r, &by_mint(vec![order("transfer_volume_raw", Direction::Desc)])).await;
    assert_eq!(volumes(&desc), vec![big.clone(), json!("100"), json!("19"), Value::Null]);
    let up = ask(&r, &by_mint(vec![order("transfer_volume_raw", Direction::Asc)])).await;
    assert_eq!(volumes(&up), vec![json!("19"), json!("100"), big, Value::Null]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn filters_test_the_raw_column() {
    let (_dir, _lens, r) = setup(&[]).await;
    // source_owner: 1050 has none, so it is grouped as `unknown` but never
    // matches a filter.
    let s = QuerySpec { group_by: vec![group("source_owner")], ..base(&["transfer_count"], 250) };
    let a = ask(&r, &s).await;
    let mut expected = vec![row(&owner(1), 6, None), row(&owner(2), 3, None), row("unknown", 1, None)];
    expected.iter_mut().for_each(|r| r.truncate(2));
    expected.sort_by(|a, b| a[0].as_str().cmp(&b[0].as_str()));
    assert_eq!(a.rows, expected);

    let only = |owner_key: String| QuerySpec {
        filters: vec![filter("source_owner", FilterOp::Eq, &[&owner_key])],
        ..base(&["transfer_count"], 250)
    };
    assert_eq!(ask(&r, &only(owner(2))).await.rows, vec![vec![json!(3)]]);
    assert_eq!(ask(&r, &only(owner(7))).await.rows, vec![vec![json!(0)]]);

    // A slot filter intersects the window.
    let s = QuerySpec { filters: vec![filter("slot", FilterOp::In, &["1010..=1030", "1150", "900"])], ..base(&["transfer_count"], 250) };
    assert_eq!(ask(&r, &s).await.rows, vec![vec![json!(4)]]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_slot_and_instruction_measures() {
    let (_dir, _lens, r) = setup(&[]).await;
    // Window 1000..=1190: plain txs at 1000 (ok 5000, failed 7000), 1120
    // (failed 10000), 1190 (ok 5000); transfer txs at 1010..=1150 (ten),
    // each 5000, failed only at 1050.
    let s = QuerySpec {
        window: Window::Range { from_slot: 1_000, to_slot: W },
        group_by: vec![group("tx_status")],
        ..base(&["tx_count", "fee_total_lamports", "tx_failure_ratio", "fee_avg_lamports"], 1)
    };
    let a = ask(&r, &s).await;
    assert_eq!(
        a.rows,
        vec![
            vec![json!("failed"), json!(3), json!("22000"), json!(1.0), json!(22_000.0 / 3.0)],
            vec![json!("success"), json!(11), json!("55000"), json!(0.0), json!(5_000.0)],
        ]
    );
    let total = QuerySpec { group_by: vec![], ..s.clone() };
    assert_eq!(ask(&r, &total).await.rows, vec![vec![json!(14), json!("77000"), json!(3.0 / 14.0), json!(77_000.0 / 14.0)]]);

    // Folded: tx_failed_count beside tx_count counts only failures, without
    // dropping the rows tx_count needs.
    let both = QuerySpec { measures: vec!["tx_count".into(), "tx_failed_count".into()], group_by: vec![], ..s.clone() };
    assert_eq!(ask(&r, &both).await.rows, vec![vec![json!(14), json!(3)]]);

    // Leaders: even slots have one, odd ones are `unknown`. 1000..=1190 is
    // 96 even and 95 odd slots.
    let s = QuerySpec {
        window: Window::Range { from_slot: 1_000, to_slot: W },
        group_by: vec![group("leader")],
        ..base(&["slot_count"], 1)
    };
    let mut expected = vec![vec![json!(leader()), json!(96)], vec![json!("unknown"), json!(95)]];
    expected.sort_by(|a, b| a[0].as_str().cmp(&b[0].as_str()));
    assert_eq!(ask(&r, &s).await.rows, expected);

    // Instructions: one per transfer tx; odd-indexed ones would be inner,
    // but each tx has one, so all are top level. Unattributed ones are
    // undecoded.
    let s = QuerySpec {
        window: Window::Range { from_slot: 1_000, to_slot: W },
        group_by: vec![group("ix_decoded")],
        ..base(&["program_count", "ix_undecoded_count", "ix_cpi_count"], 1)
    };
    assert_eq!(
        ask(&r, &s).await.rows,
        vec![vec![json!("decoded"), json!(3), json!(0), json!(0)], vec![json!("undecoded"), json!(2), json!(2), json!(0)]]
    );
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn last_slots_ends_at_w_and_a_range_is_used_as_given() {
    let (_dir, _lens, r) = setup(&[]).await;
    let count = |window| QuerySpec { window, ..base(&["transfer_count"], 1) };
    let n = |a: SpecAnswer| a.rows[0][0].as_u64().unwrap();

    // 1150 is the newest transfer: in the last 41 slots (1150..=1190), not the last 40.
    assert_eq!(n(ask(&r, &count(Window::LastSlots(41))).await), 1);
    assert_eq!(n(ask(&r, &count(Window::LastSlots(40))).await), 0);
    // Everything, clipped at 0.
    assert_eq!(n(ask(&r, &count(Window::LastSlots(200_000))).await), 11);
    let a = ask(&r, &count(Window::LastSlots(200_000))).await;
    assert_eq!(a.window, SlotWindow::new(0, W));
    // Inclusive at both ends.
    assert_eq!(n(ask(&r, &count(Window::Range { from_slot: 1_010, to_slot: 1_040 })).await), 4);
    assert_eq!(n(ask(&r, &count(Window::Range { from_slot: 1_011, to_slot: 1_039 })).await), 2);
    // Up to W, not past it: slots exist to 1199 but transactions only to 1190.
    let past = r.run(&count(Window::Range { from_slot: 1_000, to_slot: W + 1 }), &CancellationToken::new()).await;
    assert!(matches!(past, Err(SpecError::Invalid { ref field, .. }) if field == "window.to_slot"), "{past:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bucket_grouping_counts_whole_buckets_only() {
    let (_dir, _lens, r) = setup(&[]).await;
    // The last 299 slots are 892..=1190; whole buckets of 100 in them are
    // 9 (900..=999) and 10 (1000..=1099). 1100..=1190 is a partial bucket
    // and is left out, so 1100 and 1150 are not counted.
    let by_bucket = |m: &str| QuerySpec {
        group_by: vec![GroupBy { dimension: "slot_bucket".into(), bucket_width: Some(100) }],
        ..base(&[m], 299)
    };
    let a = ask(&r, &by_bucket("transfer_count")).await;
    assert_eq!(a.requested_window, SlotWindow::new(892, W));
    assert_eq!(a.window, SlotWindow::new(900, 1_099));
    assert_eq!(a.rows, vec![vec![json!(9), json!(1)], vec![json!(10), json!(8)]]);
    assert!(a.warnings.iter().any(|w| w.contains("snapped inward")), "{:?}", a.warnings);
    // Every bucket is whole: 100 slots each.
    let a = ask(&r, &by_bucket("slot_count")).await;
    assert_eq!(a.rows, vec![vec![json!(9), json!(100)], vec![json!(10), json!(100)]]);

    // Ordered by count descending, the bucket still breaks ties ascending.
    let s = QuerySpec {
        group_by: vec![GroupBy { dimension: "slot_bucket".into(), bucket_width: Some(10) }],
        order_by: vec![order("slot_count", Direction::Desc)],
        limit: Some(3),
        ..base(&["slot_count"], 299)
    };
    let a = ask(&r, &s).await;
    assert_eq!(a.window, SlotWindow::new(900, 1_189));
    assert_eq!(a.rows, vec![vec![json!(90), json!(10)], vec![json!(91), json!(10)], vec![json!(92), json!(10)]]);
    assert!(a.has_more);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_answer_is_reproduced_after_more_data_arrives() {
    let dir = TempDir::new();
    write(dir.path(), &transfers(), &plain_txs(), W, FIRST_SLOT, LAST_SLOT);
    let lens = RealLens::start(dir.path(), &[]).await;
    let r = runner_on(&lens);
    let first = ask(&r, &by_mint(vec![order("transfer_count", Direction::Desc)])).await;
    assert_eq!(first.as_of_slot, W);

    // More transfers of m3, past W, and transactions up to 1290.
    let later: Vec<Xfer> = (0..20).map(|i| xfer(1_200 + i * 4, Some(m3()), 1, SPL, Some(owner(1)))).collect();
    write(dir.path(), &later, &[(1_290, true, 5_000)], 1_290, LAST_SLOT + 1, 1_300);
    lens.refresh();

    let moved = ask(&r, &by_mint(vec![order("transfer_count", Direction::Desc)])).await;
    assert_eq!(moved.as_of_slot, 1_290);
    // 1041..=1290 now: m3's 1050 transfer (100) and the twenty new ones (1 each).
    assert_eq!(moved.rows[0], row(&m3(), 21, Some("120")), "m3 now leads the last 250 slots");

    let pinned = QuerySpec { as_of_slot: Some(first.as_of_slot), ..by_mint(vec![order("transfer_count", Direction::Desc)]) };
    assert_eq!(stable(ask(&r, &pinned).await), stable(first));
}

// ---------------------------------------------------------------------------
// Limits and refusals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn limit_plus_one_sets_has_more_exactly() {
    let (_dir, _lens, r) = setup(&[]).await;
    let limited = |limit| QuerySpec { limit: Some(limit), ..by_mint(vec![order("transfer_count", Direction::Desc)]) };
    let a = ask(&r, &limited(4)).await;
    assert_eq!((a.rows.len(), a.has_more), (4, false), "exactly four groups");
    let a = ask(&r, &limited(3)).await;
    assert_eq!((a.rows.len(), a.has_more, a.truncated), (3, true, false));
    let full = ask(&r, &limited(10)).await;
    assert_eq!(a.rows[..], full.rows[..3], "a smaller limit is a prefix");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_result_lens_truncated_is_flagged() {
    // LENS keeps two rows of any result; the four groups are cut.
    let (_dir, _lens, r) = setup(&[("LENS_MAX_QUERY_ROWS", "2")]).await;
    let a = ask(&r, &by_mint(vec![order("transfer_count", Direction::Desc)])).await;
    assert!(a.truncated && a.has_more);
    assert_eq!(a.rows.len(), 2);
    let (first, _) = asc(m1(), m2());
    assert_eq!(a.rows[0][0], json!(first), "what is kept is the head of the order");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_prism_has_not_written_is_not_ready() {
    // Slots and transactions, but no transfer yet: no token_transfers table.
    let dir = TempDir::new();
    write(dir.path(), &[], &plain_txs(), W, FIRST_SLOT, LAST_SLOT);
    let lens = RealLens::start(dir.path(), &[]).await;
    let r = runner_on(&lens);
    let err = r.run(&base(&["transfer_count"], 250), &CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.kind(), "not_ready", "{err}");
    // The other tables answer: the plain transactions at 1000 (two), 1120 and 1190.
    assert_eq!(ask(&r, &base(&["tx_count"], 250)).await.rows, vec![vec![json!(4)]]);

    // Nothing at all yet.
    let empty = TempDir::new();
    std::fs::create_dir_all(empty.path().join("solana").join("slots")).unwrap();
    let lens = RealLens::start(empty.path(), &[]).await;
    let err = runner_on(&lens).run(&base(&["tx_count"], 250), &CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.kind(), "not_ready", "{err}");
}

// ---------------------------------------------------------------------------
// Every accepted spec runs
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_generated_spec_runs_on_datafusion_and_shapes() {
    let (_dir, _lens, r) = setup(&[]).await;
    let c = Catalog::new(vec![]).unwrap();
    let specs = spec_support::accepted_specs(&c);
    let mut tasks = Vec::with_capacity(specs.len());
    for (label, mut s) in specs {
        // The generator's range runs past this chain's W.
        if let Window::Range { .. } = s.window {
            s.window = Window::Range { from_slot: 1_000, to_slot: 1_189 };
        }
        let r = r.clone();
        tasks.push(tokio::spawn(async move {
            let result = r.run(&s, &CancellationToken::new()).await;
            (label, result)
        }));
    }
    let mut ran = 0;
    for t in tasks {
        let (label, result) = t.await.unwrap();
        let a = result.unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(a.queries_run, 3, "{label}");
        assert!(a.rows.iter().all(|row| row.len() == a.columns.len()), "{label}");
        ran += 1;
    }
    assert!(ran > 300, "{ran}");
}
