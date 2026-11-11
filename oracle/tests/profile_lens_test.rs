//! `profile/` against the real LENS.
//!
//! PRISM's `ParquetWriter` writes a small hand-built chain; `lens::serve_on`
//! serves it; the profiler reaches it through FORGE's `lens_client`. So the
//! SQL is run by DataFusion, and every number is checked against a value
//! computed by hand from the chain below:
//!
//! * query correctness: counts, first/last slot, distinct counts, volumes
//!   above `u64::MAX`, top-N with ties, the series, coverage, failure share
//!   and decode rate, for each kind; the window and failed-tx exclusions;
//! * input safety: a hostile literal, quoted by FORGE's routine, matches
//!   nothing, while the same text spliced in unquoted would match everything;
//! * snapshot consistency: a pinned profile is byte-identical before and
//!   after more data arrives; W is the newest slot both tables cover;
//! * truncation and a missing table are reported, not served.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::TempDir;
use forge::lens_client::{LensApi, LensClient};
use lens::{LensConfig, QueryEngine};
use oracle::profile::*;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

fn mint() -> String {
    key(1)
}
fn other_mint() -> String {
    key(2)
}
fn acct(c: char) -> String {
    key(10 + (c as u8 - b'A'))
}
fn owner(c: char) -> String {
    key(30 + (c as u8 - b'A'))
}
fn prog() -> String {
    key(20)
}
fn prog2() -> String {
    key(21)
}

const FIRST_SLOT: u64 = 900;
/// Slots run to here; the last transaction is at [`LAST_TX_SLOT`].
const LAST_SLOT: u64 = 1_199;
const LAST_TX_SLOT: u64 = 1_190;
const WINDOW: u64 = 250;
/// `W - (WINDOW - 1)` with `W = LAST_TX_SLOT`.
const FROM: u64 = LAST_TX_SLOT - (WINDOW - 1);

struct Xfer {
    src: char,
    dst: char,
    mint: Option<String>,
    amount: u64,
    decimals: Option<u8>,
}

struct Ix {
    program: String,
    inner: bool,
    ty: Option<&'static str>,
}

struct Tx {
    slot: u64,
    ok: bool,
    xfers: Vec<Xfer>,
    ixs: Vec<Ix>,
}

fn x(src: char, dst: char, mint: Option<String>, amount: u64, decimals: Option<u8>) -> Xfer {
    Xfer { src, dst, mint, amount, decimals }
}

fn ix(program: String, inner: bool, ty: Option<&'static str>) -> Ix {
    Ix { program, inner, ty }
}

/// The hand-built chain. See the expectations in each test.
fn chain() -> Vec<Tx> {
    let m = || Some(mint());
    vec![
        // Before the window: counted nowhere.
        Tx { slot: 920, ok: true, xfers: vec![x('A', 'B', m(), 1_000, Some(6))], ixs: vec![ix(prog(), false, Some("transfer"))] },
        Tx { slot: 1_010, ok: true, xfers: vec![x('A', 'B', m(), u64::MAX, Some(6))], ixs: vec![ix(prog(), false, Some("transfer"))] },
        Tx {
            slot: 1_020,
            ok: true,
            xfers: vec![x('A', 'C', m(), u64::MAX, Some(6))],
            ixs: vec![ix(prog(), false, Some("transfer")), ix(prog(), true, None)],
        },
        Tx { slot: 1_050, ok: true, xfers: vec![x('B', 'C', m(), 10, Some(6))], ixs: vec![ix(prog2(), false, None)] },
        // Failed: excluded, but counted in coverage and failure share.
        Tx { slot: 1_100, ok: false, xfers: vec![x('A', 'D', m(), 999, Some(6))], ixs: vec![ix(prog(), false, Some("transfer"))] },
        // No mint: unattributable.
        Tx { slot: 1_120, ok: true, xfers: vec![x('C', 'D', None, 5, None)], ixs: vec![ix(prog(), false, Some("mint_to"))] },
        Tx { slot: 1_150, ok: true, xfers: vec![x('D', 'A', Some(other_mint()), 7, Some(9))], ixs: vec![] },
        Tx { slot: LAST_TX_SLOT, ok: true, xfers: vec![x('B', 'A', m(), 1, Some(6))], ixs: vec![] },
    ]
}

/// More activity after the first snapshot.
fn later() -> Vec<Tx> {
    (0..20)
        .map(|i| Tx {
            slot: 1_210 + i * 5,
            ok: true,
            xfers: vec![x('A', 'B', Some(mint()), 1, Some(6))],
            ixs: vec![ix(prog(), false, Some("transfer"))],
        })
        .collect()
}

fn write(dir: &Path, txs: &[Tx], lo: u64, hi: u64) {
    let mut writer = ParquetWriter::new(dir);
    for slot in lo..=hi {
        let mut rows = Vec::new();
        let mut ixs = Vec::new();
        let mut xfers = Vec::new();
        for (i, tx) in txs.iter().filter(|t| t.slot == slot).enumerate() {
            let sig = format!("sig-{slot}-{i}");
            rows.push(DeepAnaTransaction {
                slot,
                blockhash: format!("hash{slot}"),
                transaction_index: i as u32,
                tx_signature: sig.clone(),
                fee: 5_000,
                status: if tx.ok { TransactionStatus::Success } else { TransactionStatus::Failed },
                err: (!tx.ok).then(|| "custom program error".to_owned()),
                ..Default::default()
            });
            for (k, ix) in tx.ixs.iter().enumerate() {
                ixs.push(DeepAnaInstruction {
                    slot,
                    tx_signature: sig.clone(),
                    instruction_index: 0,
                    inner_instruction_index: ix.inner.then_some(k as u32),
                    program_id: ix.program.clone(),
                    instruction_type: ix.ty.map(str::to_owned),
                    ..Default::default()
                });
            }
            for (k, t) in tx.xfers.iter().enumerate() {
                xfers.push(DeepAnaTokenTransfer {
                    slot,
                    tx_signature: sig.clone(),
                    instruction_index: k as u32,
                    spl_token_program_id: "TokenProgram".into(),
                    source_token_account: acct(t.src),
                    destination_token_account: acct(t.dst),
                    source_owner: Some(owner(t.src)),
                    destination_owner: Some(owner(t.dst)),
                    mint_token_address: t.mint.clone(),
                    amount_raw: t.amount,
                    decimals: t.decimals,
                    block_time: Some(block_time(slot)),
                    ..Default::default()
                });
            }
        }
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot.saturating_sub(1),
            block_time: Some(block_time(slot)),
            block_height: Some(slot),
            transaction_count: rows.len() as u32,
            leader: None,
        };
        writer.append(&row, &rows, &ixs, &xfers).unwrap();
    }
    writer.flush().unwrap();
}

fn block_time(slot: u64) -> i64 {
    1_793_102_400 + (slot / 3) as i64
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

async fn setup(vars: &[(&str, &str)]) -> (TempDir, RealLens, Profiler) {
    let dir = TempDir::new();
    write(dir.path(), &chain(), FIRST_SLOT, LAST_SLOT);
    let lens = RealLens::start(dir.path(), vars).await;
    let profiler = Profiler::new(lens.client());
    (dir, lens, profiler)
}

fn request(kind: Kind, id: String) -> ProfileRequest {
    ProfileRequest { kind, id, window_slots: Some(WINDOW), as_of_slot: None }
}

/// The report with its only non-deterministic field cleared.
fn stable(mut p: Profile) -> Profile {
    p.duration_ms = 0;
    p
}

/// Expected series: `(slot, count)` events into the window's buckets.
fn expected_series(w: &Window, events: &[(u64, u64)]) -> Vec<(u64, u64, u64)> {
    let width = w.bucket_width();
    let mut out = Vec::new();
    let mut lo = w.from_slot;
    while lo <= w.to_slot {
        let hi = (lo + width - 1).min(w.to_slot);
        let n = events.iter().filter(|(s, _)| (lo..=hi).contains(s)).map(|(_, n)| n).sum();
        out.push((lo, hi, n));
        lo += width;
    }
    out
}

fn series(p: &Profile) -> Vec<(u64, u64, u64)> {
    p.series.iter().map(|s| (s.from_slot, s.to_slot, s.count)).collect()
}

fn tops(p: &Profile, name: &str) -> Vec<(Option<String>, u64, Option<String>)> {
    p.top[name].iter().map(|t| (t.key.clone(), t.count, t.volume_raw.clone())).collect()
}

/// `(a, n_a)` and `(b, n_b)` ordered as the report must: count desc, key asc.
fn ordered<T: Clone>(mut entries: Vec<(String, u64, T)>) -> Vec<(Option<String>, u64, T)> {
    entries.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    entries.into_iter().map(|(k, n, v)| (Some(k), n, v)).collect()
}

// ---------------------------------------------------------------------------
// Query correctness
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mint_profile_matches_the_chain() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    let p = profiler.profile(request(Kind::Mint, mint())).await.unwrap();

    // W: solana.slots runs to 1199, solana.transactions only to 1190.
    assert_eq!(p.as_of_slot, LAST_TX_SLOT);
    assert_eq!(p.window, Window { from_slot: FROM, to_slot: LAST_TX_SLOT, slots: WINDOW });
    assert!(p.found);
    assert_eq!(p.queries_run, Kind::Mint.queries());

    // In the window, successful, this mint: slots 1010, 1020, 1050, 1190.
    // Not 920 (before the window), 1100 (failed), 1120 (no mint), 1150 (other mint).
    let volume = 2 * u128::from(u64::MAX) + 10 + 1;
    let Summary::Mint(s) = &p.summary else { panic!("{:?}", p.summary) };
    assert_eq!(s.transfers, 4);
    assert_eq!((s.first_seen_slot, s.last_seen_slot), (Some(1_010), Some(LAST_TX_SLOT)));
    assert_eq!(s.distinct_sources, 2); // A, B
    assert_eq!(s.distinct_destinations, 3); // B, C, A
    assert_eq!(s.volume_raw, volume.to_string());
    assert_eq!(s.volume_raw, "36893488147419103241");
    assert_eq!(s.volume_ui.as_deref(), Some("36893488147419.103241"));
    assert_eq!(s.decimals, Some(6));

    let max = u128::from(u64::MAX);
    // A and B both send twice: the tie breaks on the key.
    assert_eq!(
        tops(&p, "top_sources"),
        ordered(vec![
            (acct('A'), 2, Some((2 * max).to_string())),
            (acct('B'), 2, Some("11".to_owned())),
        ])
    );
    assert_eq!(
        tops(&p, "top_destinations"),
        ordered(vec![
            (acct('C'), 2, Some((max + 10).to_string())),
            (acct('B'), 1, Some(max.to_string())),
            (acct('A'), 1, Some("1".to_owned())),
        ])
    );
    assert_eq!(series(&p), expected_series(&p.window, &[(1_010, 1), (1_020, 1), (1_050, 1), (LAST_TX_SLOT, 1)]));
    assert_eq!(p.series.len(), 23); // 250 slots in buckets of ceil(250/24) = 11
    assert_eq!(p.coverage["transfers_in_failed_tx_excluded"], 1);
    assert_eq!(p.coverage["unattributed_transfers"], 1);
    assert_eq!(p.warnings, vec![WARN_MINT_LOWER_BOUND.to_owned()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_account_profile_matches_the_chain() {
    let (_dir, _lens, profiler) = setup(&[]).await;

    // B: in at 1010 (from A), out at 1050 (to C) and 1190 (to A).
    let p = profiler.profile(request(Kind::TokenAccount, acct('B'))).await.unwrap();
    assert_eq!(p.queries_run, Kind::TokenAccount.queries());
    let Summary::TokenAccount(s) = &p.summary else { panic!() };
    assert_eq!((s.transfers, s.transfers_out, s.transfers_in), (3, 2, 1));
    assert_eq!((s.first_seen_slot, s.last_seen_slot), (Some(1_010), Some(LAST_TX_SLOT)));
    assert_eq!((s.mint.clone(), s.distinct_mints), (Some(mint()), 1));
    assert_eq!((s.owner.clone(), s.distinct_owners), (Some(owner('B')), 1));
    assert_eq!(s.volume_out_raw, "11");
    assert_eq!(s.volume_in_raw, u64::MAX.to_string());
    assert_eq!(s.volume_out_ui.as_deref(), Some("0.000011"));
    assert_eq!(s.volume_in_ui.as_deref(), Some("18446744073709.551615"));
    assert_eq!(
        tops(&p, "top_counterparties"),
        ordered(vec![
            (acct('A'), 2, Some((u128::from(u64::MAX) + 1).to_string())),
            (acct('C'), 1, Some("10".to_owned())),
        ])
    );
    assert_eq!(series(&p), expected_series(&p.window, &[(1_010, 1), (1_050, 1), (LAST_TX_SLOT, 1)]));
    assert_eq!(p.coverage["transfers_in_failed_tx_excluded"], 0);
    assert!(p.warnings.is_empty(), "{:?}", p.warnings);

    // A: out at 1010 and 1020, in at 1150 (other mint) and 1190; the failed
    // transfer out at 1100 is excluded. Two mints touch it.
    let p = profiler.profile(request(Kind::TokenAccount, acct('A'))).await.unwrap();
    let Summary::TokenAccount(s) = &p.summary else { panic!() };
    assert_eq!((s.transfers, s.transfers_out, s.transfers_in), (4, 2, 2));
    assert_eq!((s.mint.clone(), s.distinct_mints, s.decimals), (None, 2, None));
    assert_eq!(p.coverage["transfers_in_failed_tx_excluded"], 1);
    assert!(p.warnings.contains(&WARN_MULTIPLE_MINTS.to_owned()), "{:?}", p.warnings);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_program_profile_matches_the_chain() {
    let (_dir, _lens, profiler) = setup(&[]).await;

    // prog, successful, in the window: 1010 transfer; 1020 transfer + inner
    // undecoded; 1120 mint_to. Failed: 1100. Before the window: 920.
    let p = profiler.profile(request(Kind::Program, prog())).await.unwrap();
    assert_eq!(p.queries_run, Kind::Program.queries());
    let Summary::Program(s) = &p.summary else { panic!() };
    assert_eq!(s.invocations, 4);
    assert_eq!(s.inner_invocations, 1);
    assert_eq!(s.transactions, 3);
    assert_eq!((s.first_seen_slot, s.last_seen_slot), (Some(1_010), Some(1_120)));
    assert_eq!(s.decoded_invocations, 3);
    assert_eq!(s.decode_rate, Some(0.75));
    assert_eq!((s.transactions_including_failed, s.failed_transactions), (4, 1));
    assert_eq!(s.failure_share, Some(0.25));
    assert_eq!(p.coverage["instructions_in_failed_tx_excluded"], 1);
    // "transfer" 2; then the two 1s tie and sort by key: '' (undecoded) < "mint_to".
    assert_eq!(
        tops(&p, "top_instruction_types"),
        vec![(Some("transfer".to_owned()), 2, None), (None, 1, None), (Some("mint_to".to_owned()), 1, None)]
    );
    assert_eq!(series(&p), expected_series(&p.window, &[(1_010, 1), (1_020, 2), (1_120, 1)]));
    assert_eq!(p.warnings, vec![WARN_FAILURE_SHARE.to_owned(), WARN_DECODE_RATE.to_owned()]);

    // prog2: one undecoded instruction.
    let p = profiler.profile(request(Kind::Program, prog2())).await.unwrap();
    let Summary::Program(s) = &p.summary else { panic!() };
    assert_eq!((s.invocations, s.decoded_invocations, s.decode_rate), (1, 0, Some(0.0)));
    assert!(p.warnings.contains(&WARN_NOTHING_DECODED.to_owned()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unknown_entity_is_an_empty_profile_not_an_error() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    for kind in Kind::ALL {
        let p = profiler.profile(request(kind, key(99))).await.unwrap();
        assert!(!p.found, "{kind}");
        assert!(p.series.iter().all(|s| s.count == 0));
        assert!(p.top.values().all(Vec::is_empty));
        assert!(p.warnings.contains(&WARN_NOT_FOUND.to_owned()));
        assert_eq!(p.queries_run, kind.queries());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_window_edges_are_inclusive() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    let count = |p: &Profile| match &p.summary {
        Summary::Mint(s) => s.transfers,
        _ => unreachable!(),
    };
    // A one-slot window on exactly 1010 sees it; one slot later does not.
    let at = |slot: u64| ProfileRequest { window_slots: Some(1), as_of_slot: Some(slot), ..request(Kind::Mint, mint()) };
    assert_eq!(count(&profiler.profile(at(1_010)).await.unwrap()), 1);
    assert_eq!(count(&profiler.profile(at(1_011)).await.unwrap()), 0);
    assert_eq!(count(&profiler.profile(at(1_009)).await.unwrap()), 0);
    // from = 1010 exactly: includes 1010.
    let from_1010 = ProfileRequest { window_slots: Some(1_020 - 1_010 + 1), as_of_slot: Some(1_020), ..request(Kind::Mint, mint()) };
    assert_eq!(count(&profiler.profile(from_1010).await.unwrap()), 2);
    // A window reaching 920 adds the transfer before the default window.
    let wide = ProfileRequest { window_slots: Some(1_000), ..request(Kind::Mint, mint()) };
    assert_eq!(count(&profiler.profile(wide).await.unwrap()), 5);
}

// ---------------------------------------------------------------------------
// Input safety
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quoted_hostile_literal_matches_nothing() {
    let (_dir, lens, _profiler) = setup(&[]).await;
    let client = lens.client();
    let w = Window::ending_at(LAST_TX_SLOT, WINDOW);
    let transfers = |r: forge::lens_client::LensResponse| r.rows[0]["transfers"].as_u64().unwrap();

    let evil = "x' OR '1'='1";
    // Through FORGE's quoting: a literal string that is no mint.
    let safe = sql::mint_summary(&forge::quote_sql_string(evil), &w);
    assert_eq!(transfers(client.query(&safe).await.unwrap()), 0);

    // Control: spliced in raw, the same text binds `OR '1'='1'` around the
    // whole WHERE clause and matches all 8 transfers in the chain, failed
    // and out-of-window ones included. So the check above means something.
    let spliced = sql::mint_summary(&format!("'{evil}'"), &w);
    assert_eq!(transfers(client.query(&spliced).await.unwrap()), 8);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostile_ids_are_rejected_before_lens() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    for id in ["x' OR '1'='1", "' UNION SELECT 1 --", &format!("{}'", &mint()[..43])] {
        let err = profiler.profile(request(Kind::Mint, id.to_owned())).await.unwrap_err();
        assert!(matches!(&err, ProfileError::Invalid { field, .. } if field == "id"), "{id}: {err:?}");
    }
}

// ---------------------------------------------------------------------------
// Snapshot consistency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_profile_does_not_change_when_data_arrives() {
    let (dir, lens, profiler) = setup(&[]).await;
    let mut before = Vec::new();
    for (kind, id) in [(Kind::Mint, mint()), (Kind::TokenAccount, acct('A')), (Kind::Program, prog())] {
        let live = stable(profiler.profile(request(kind, id.clone())).await.unwrap());
        // Re-running with the snapshot pinned gives the same report.
        let pinned = ProfileRequest { as_of_slot: Some(live.as_of_slot), ..request(kind, id) };
        assert_eq!(stable(profiler.profile(pinned.clone()).await.unwrap()), live, "{kind}");
        before.push((pinned, live));
    }

    write(dir.path(), &later(), LAST_SLOT + 1, 1_310);
    lens.refresh();

    for (pinned, live) in before {
        let again = stable(profiler.profile(pinned.clone()).await.unwrap());
        assert_eq!(again, live, "{}", pinned.kind);
        // Same bytes, not only equal values.
        assert_eq!(serde_json::to_vec(&again).unwrap(), serde_json::to_vec(&live).unwrap());

        // Unpinned, the snapshot moved and new activity shows up.
        let now = profiler.profile(ProfileRequest { as_of_slot: None, ..pinned.clone() }).await.unwrap();
        assert_eq!(now.as_of_slot, 1_305, "{}", pinned.kind);
        assert!(now.series.iter().map(|s| s.count).sum::<u64>() > 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pin_above_the_visible_slot_is_refused() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    let req = ProfileRequest { as_of_slot: Some(LAST_TX_SLOT + 1), ..request(Kind::Mint, mint()) };
    let err = profiler.profile(req).await.unwrap_err();
    assert!(matches!(&err, ProfileError::Invalid { field, .. } if field == "as_of_slot"), "{err:?}");
    // Slots above 1190 exist in solana.slots, but transactions do not reach
    // them yet, so they are not fully visible.
    assert!(err.to_string().contains(&LAST_TX_SLOT.to_string()), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sections_agree_with_each_other() {
    let (_dir, _lens, profiler) = setup(&[]).await;
    for (kind, id) in [(Kind::Mint, mint()), (Kind::TokenAccount, acct('C')), (Kind::Program, prog())] {
        let p = profiler.profile(ProfileRequest { window_slots: Some(400), ..request(kind, id) }).await.unwrap();
        let total = match &p.summary {
            Summary::Mint(s) => s.transfers,
            Summary::TokenAccount(s) => s.transfers,
            Summary::Program(s) => s.invocations,
        };
        let (first, last) = match &p.summary {
            Summary::Mint(s) => (s.first_seen_slot, s.last_seen_slot),
            Summary::TokenAccount(s) => (s.first_seen_slot, s.last_seen_slot),
            Summary::Program(s) => (s.first_seen_slot, s.last_seen_slot),
        };
        assert_eq!(p.series.iter().map(|s| s.count).sum::<u64>(), total, "{kind}");
        for top in p.top.values() {
            assert!(top.iter().all(|t| t.count <= total));
            assert!(top.windows(2).all(|w| (w[0].count, &w[1].key) >= (w[1].count, &w[0].key)), "{kind}: {top:?}");
        }
        let (first, last) = (first.unwrap(), last.unwrap());
        assert!(p.window.from_slot <= first && last <= p.window.to_slot);
        // The buckets holding the first and last activity are non-empty.
        let holding = |slot: u64| p.series.iter().find(|s| (s.from_slot..=s.to_slot).contains(&slot)).unwrap().count;
        assert!(holding(first) > 0 && holding(last) > 0, "{kind}");
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncated_result_is_refused() {
    // One row per result: the two-entry top lists are cut off.
    let (_dir, _lens, profiler) = setup(&[("LENS_MAX_QUERY_ROWS", "1")]).await;
    let err = profiler.profile(request(Kind::Mint, mint())).await.unwrap_err();
    assert_eq!(err, ProfileError::Truncated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_data_yet_is_not_ready() {
    // PRISM has created a table directory but written nothing yet.
    let dir = TempDir::new();
    std::fs::create_dir_all(dir.path().join("solana").join("slots")).unwrap();
    let lens = RealLens::start(dir.path(), &[]).await;
    let err = Profiler::new(lens.client()).profile(request(Kind::Mint, mint())).await.unwrap_err();
    assert_eq!(err.kind(), "not_ready", "{err}");
}
