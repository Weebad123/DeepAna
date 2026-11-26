//! Drill-through and detail reports against the real LENS, through the real
//! router.
//!
//! PRISM's `ParquetWriter` writes a small hand-built chain; `lens::serve_on`
//! serves it; a whole ORACLE (`Oracle::start_with`) runs over it and is
//! called through its own router, exactly as the dashboard calls it.
//!
//! The first group is the drill-down guarantee, measured and asserted:
//!
//! * `every_row_of_every_builtin_template_opens_a_detail_report`: every
//!   example of every built-in question template is asked; **every row** of
//!   every answer is drilled; **every** drilled row is opened as a
//!   transaction or slot report, which must be found and must contain that
//!   very transaction, instruction or token transfer;
//! * `every_row_of_custom_queries_opens_a_detail_report`: the same for
//!   custom specs covering every measure with every dimension it supports,
//!   the placeholders (`unattributed`, `unknown`) and buckets included;
//! * `every_row_of_a_raw_sql_query_opens_a_detail_report`: the same for the
//!   rows of custom SQL run on LENS directly (the dashboard's SQL tab).
//!
//! The second group checks the reports against the chain: the instruction
//! tree, logs, memos, read-time decoding, a failed transaction, rows
//! repeated by a replay, paging, and that a pinned request is byte-identical
//! after more data arrives.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::Fixture;
use forge::lens_client::{LensApi, LensClient};
use http_body_util::BodyExt;
use lens::{LensConfig, QueryEngine};
use oracle::config::ENV_DETECT_ENABLED;
use oracle::{Oracle, OracleParts};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SYSTEM: &str = "11111111111111111111111111111111";
const BUDGET: &str = "ComputeBudget111111111111111111111111111111";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const VOTE: &str = "Vote111111111111111111111111111111111111111";
const WSOL: &str = "So11111111111111111111111111111111111111112";

/// The chain's first slot; every window in the tests starts at or after it.
const FIRST_SLOT: u64 = 500_000;
/// The newest slot in both tables: the snapshot `W`.
const LAST_SLOT: u64 = 501_999;
const SWAP_SLOT: u64 = 501_010;
const FAILED_SLOT: u64 = 501_020;

/// A 32-byte address.
fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

/// A 64-byte signature.
fn sig(n: u8) -> String {
    let mut bytes = [7u8; 64];
    bytes[0] = n;
    bs58::encode(bytes).into_string()
}

fn swap_program() -> String {
    key(40)
}
fn mint1() -> String {
    key(1)
}
fn mint2() -> String {
    key(2)
}
fn leader() -> String {
    key(60)
}
fn alice() -> String {
    key(30)
}
fn bob() -> String {
    key(31)
}

const SWAP_SIG: u8 = 1;
const SOL_SIG: u8 = 2;
const FAILED_SIG: u8 = 3;
const UNATTRIBUTED_SIG: u8 = 4;
const WSOL_SIG: u8 = 5;
const VOTE_SIG: u8 = 6;
const OLD_SIG: u8 = 7;
const LAST_SIG: u8 = 8;

struct Tx {
    slot: u64,
    index: u32,
    sig: u8,
    err: Option<&'static str>,
    accounts: Vec<String>,
    logs: Vec<String>,
    ixs: Vec<DeepAnaInstruction>,
    xfers: Vec<DeepAnaTokenTransfer>,
}

fn ix(index: u32, inner: Option<u32>, program: &str, data: &[u8], decoded: Option<(&str, Value)>) -> DeepAnaInstruction {
    DeepAnaInstruction {
        instruction_index: index,
        inner_instruction_index: inner,
        program_id: program.to_owned(),
        accounts: vec![alice(), bob()],
        data_base58: bs58::encode(data).into_string(),
        instruction_type: decoded.as_ref().map(|(ty, _)| (*ty).to_owned()),
        decoded_data: decoded.map(|(_, data)| data),
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn xfer(
    index: u32,
    inner: Option<u32>,
    program: &str,
    mint: Option<String>,
    owners: Option<(String, String)>,
    amount: u64,
    decimals: Option<u8>,
) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        instruction_index: index,
        inner_instruction_index: inner,
        spl_token_program_id: program.to_owned(),
        source_token_account: key(10),
        destination_token_account: key(11),
        source_owner: owners.as_ref().map(|o| o.0.clone()),
        destination_owner: owners.map(|o| o.1),
        mint_token_address: mint,
        amount_raw: amount,
        decimals,
        ..Default::default()
    }
}

fn invoke(program: &str, depth: u32) -> String {
    format!("Program {program} invoke [{depth}]")
}
fn success(program: &str) -> String {
    format!("Program {program} success")
}

fn system_transfer(lamports: u64) -> Vec<u8> {
    [2u32.to_le_bytes().as_slice(), &lamports.to_le_bytes()].concat()
}

fn unit_limit() -> Vec<u8> {
    [[2u8].as_slice(), &200_000u32.to_le_bytes()].concat()
}

fn plain_sol_tx(slot: u64, index: u32, sig: u8) -> Tx {
    Tx {
        slot,
        index,
        sig,
        err: None,
        accounts: vec![alice(), bob(), SYSTEM.to_owned()],
        logs: vec![invoke(SYSTEM, 1), success(SYSTEM)],
        ixs: vec![ix(0, None, SYSTEM, &system_transfer(5_000), None)],
        xfers: vec![],
    }
}

/// The hand-built chain. Every test's expectations come from here.
fn chain() -> Vec<Tx> {
    let swap = swap_program();
    vec![
        // An older transfer of a second mint, outside the last 1000 slots.
        Tx {
            slot: 500_100,
            index: 0,
            sig: OLD_SIG,
            err: None,
            accounts: vec![alice()],
            logs: vec![invoke(TOKEN, 1), success(TOKEN)],
            ixs: vec![ix(0, None, TOKEN, &[3], Some(("Transfer", json!({ "amount": 7 }))))],
            xfers: vec![xfer(0, None, TOKEN, Some(mint2()), Some((alice(), bob())), 7, Some(0))],
        },
        // A swap: budget, a program that calls SPL Token, a memo.
        Tx {
            slot: SWAP_SLOT,
            index: 0,
            sig: SWAP_SIG,
            err: None,
            accounts: vec![alice(), bob(), swap.clone(), TOKEN.to_owned()],
            logs: vec![
                invoke(BUDGET, 1),
                success(BUDGET),
                invoke(&swap, 1),
                "Program log: Instruction: Swap".to_owned(),
                invoke(TOKEN, 2),
                "Program log: Instruction: Transfer".to_owned(),
                format!("Program {TOKEN} consumed 4645 of 180000 compute units"),
                success(TOKEN),
                format!("Program {swap} consumed 30000 of 200000 compute units"),
                success(&swap),
                invoke(MEMO, 1),
                "Program log: Memo (len 10): \"gm deepana\"".to_owned(),
                success(MEMO),
            ],
            ixs: vec![
                ix(0, None, BUDGET, &unit_limit(), None),
                ix(1, None, &swap, &[9, 9], None),
                ix(1, Some(0), TOKEN, &[3], Some(("Transfer", json!({ "amount": 1500 })))),
                ix(2, None, MEMO, b"gm deepana", None),
            ],
            xfers: vec![xfer(1, Some(0), TOKEN, Some(mint1()), Some((alice(), bob())), 1_500, Some(6))],
        },
        plain_sol_tx(SWAP_SLOT, 1, SOL_SIG),
        // Fails in its second instruction; the memo after it never runs.
        Tx {
            slot: FAILED_SLOT,
            index: 0,
            sig: FAILED_SIG,
            err: Some(r#"{"InstructionError":[1,{"Custom":1}]}"#),
            accounts: vec![alice(), bob()],
            logs: vec![
                invoke(BUDGET, 1),
                success(BUDGET),
                invoke(TOKEN, 1),
                "Program log: Error: insufficient funds".to_owned(),
                format!("Program {TOKEN} failed: custom program error: 0x1"),
            ],
            ixs: vec![
                ix(0, None, BUDGET, &unit_limit(), None),
                ix(1, None, TOKEN, &[3], Some(("Transfer", json!({ "amount": 999 })))),
                ix(2, None, MEMO, b"never ran", None),
            ],
            xfers: vec![xfer(1, None, TOKEN, Some(mint1()), Some((alice(), bob())), 999, Some(6))],
        },
        // A Token-2022 transfer PRISM could not resolve: no mint, no owners.
        Tx {
            slot: 501_030,
            index: 0,
            sig: UNATTRIBUTED_SIG,
            err: None,
            accounts: vec![bob()],
            logs: vec![invoke(TOKEN_2022, 1), success(TOKEN_2022)],
            ixs: vec![ix(0, None, TOKEN_2022, &[3], Some(("Transfer", json!({ "amount": 5 }))))],
            xfers: vec![xfer(0, None, TOKEN_2022, None, None, 5, None)],
        },
        // Wrapped SOL, an amount no JavaScript number holds.
        Tx {
            slot: 501_040,
            index: 0,
            sig: WSOL_SIG,
            err: None,
            accounts: vec![alice()],
            logs: vec![invoke(TOKEN, 1), success(TOKEN)],
            ixs: vec![ix(0, None, TOKEN, &[12], Some(("TransferChecked", json!({ "amount": u64::MAX, "decimals": 9 }))))],
            xfers: vec![xfer(0, None, TOKEN, Some(WSOL.to_owned()), Some((alice(), bob())), u64::MAX, Some(9))],
        },
        Tx {
            slot: 501_050,
            index: 0,
            sig: VOTE_SIG,
            err: None,
            accounts: vec![leader()],
            logs: vec![invoke(VOTE, 1), success(VOTE)],
            ixs: vec![ix(0, None, VOTE, &[12, 0, 0, 0], None)],
            xfers: vec![],
        },
        plain_sol_tx(LAST_SLOT, 0, LAST_SIG),
    ]
}

/// More activity after the first snapshot.
fn later() -> Vec<Tx> {
    (0..5).map(|i| plain_sol_tx(LAST_SLOT + 10 + i, 0, 100 + i as u8)).collect()
}

fn block_time(slot: u64) -> i64 {
    1_793_102_400 + (slot - FIRST_SLOT) as i64 / 2
}

/// Write `txs` with PRISM's writer: one flush, so one file per table.
fn write(dir: &Path, txs: &[Tx]) {
    let mut writer = ParquetWriter::new(dir);
    let slots: BTreeSet<u64> = txs.iter().map(|t| t.slot).collect();
    for slot in slots {
        let mut rows = Vec::new();
        let mut ixs = Vec::new();
        let mut xfers = Vec::new();
        for tx in txs.iter().filter(|t| t.slot == slot) {
            let signature = sig(tx.sig);
            rows.push(DeepAnaTransaction {
                slot,
                blockhash: format!("hash{slot}"),
                transaction_index: tx.index,
                tx_signature: signature.clone(),
                fee: 5_000 + u64::from(tx.sig),
                status: TransactionStatus::from_err(tx.err),
                err: tx.err.map(str::to_owned),
                accounts: tx.accounts.clone(),
                log_messages: tx.logs.clone(),
                compute_units_consumed: Some(1_000 * u64::from(tx.sig)),
            });
            ixs.extend(tx.ixs.iter().cloned().map(|i| DeepAnaInstruction { slot, tx_signature: signature.clone(), ..i }));
            xfers.extend(tx.xfers.iter().cloned().map(|x| DeepAnaTokenTransfer {
                slot,
                tx_signature: signature.clone(),
                block_time: Some(block_time(slot)),
                ..x
            }));
        }
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(block_time(slot)),
            block_height: Some(slot - 1_000),
            transaction_count: rows.len() as u32,
            leader: (slot == SWAP_SLOT || slot == FAILED_SLOT).then(leader),
        };
        writer.append(&row, &rows, &ixs, &xfers).unwrap();
    }
    writer.flush().unwrap();
}

// ---------------------------------------------------------------------------
// A real LENS and a whole ORACLE over it
// ---------------------------------------------------------------------------

struct RealLens {
    url: String,
    engine: Arc<QueryEngine>,
    server: Option<JoinHandle<lens::LensResult<()>>>,
}

impl RealLens {
    async fn start(data_dir: &Path) -> Self {
        let data_dir = data_dir.to_str().unwrap().to_owned();
        let config =
            LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data_dir.clone().into())).unwrap();
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

struct Rig {
    fixture: Fixture,
    lens: RealLens,
    app: Router,
    _oracle: Oracle,
    _token: CancellationToken,
}

impl Rig {
    fn data_dir(&self) -> std::path::PathBuf {
        self.fixture.dir.path().join("data")
    }
}

async fn rig() -> Rig {
    let fixture = Fixture::new();
    let data = fixture.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    write(&data, &chain());
    let lens = RealLens::start(&data).await;
    let config = fixture.load(&[(ENV_DETECT_ENABLED, "false")]).unwrap();
    let token = CancellationToken::new();
    let parts = OracleParts { lens: Some(lens.client()), ..OracleParts::default() };
    let oracle = Oracle::start_with(config, parts, token.clone()).await.unwrap();
    let app = oracle.router();
    Rig { fixture, lens, app, _oracle: oracle, _token: token }
}

async fn call(app: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    (status, response.into_body().collect().await.unwrap().to_bytes().to_vec())
}

#[track_caller]
fn parsed(what: &str, status: StatusCode, body: &[u8]) -> Value {
    assert_eq!(status, StatusCode::OK, "{what}: {}", String::from_utf8_lossy(body));
    serde_json::from_slice(body).unwrap()
}

async fn get(app: &Router, uri: &str) -> Value {
    let (status, body) = call(app, Request::get(uri).body(Body::empty()).unwrap()).await;
    parsed(uri, status, &body)
}

async fn get_bytes(app: &Router, uri: &str) -> Vec<u8> {
    let (status, body) = call(app, Request::get(uri).body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK, "{uri}: {}", String::from_utf8_lossy(&body));
    body
}

async fn post(app: &Router, uri: &str, body: &Value) -> Value {
    let req = Request::post(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let (status, bytes) = call(app, req).await;
    parsed(&format!("{uri} {body}"), status, &bytes)
}

async fn transaction(app: &Router, sig_n: u8, slot: u64) -> Value {
    get(app, &format!("/profile/transaction/{}?slot={slot}", sig(sig_n))).await
}

// ---------------------------------------------------------------------------
// The drill-down guarantee
// ---------------------------------------------------------------------------

/// What one walk from answers to detail reports covered.
#[derive(Debug, Default)]
struct Coverage {
    answer_rows: usize,
    drilled_rows: usize,
    /// Detail reports opened, repeats of the same one included.
    details_opened: usize,
    entity_profiles_opened: usize,
    /// Distinct `(slot, signature)` and slots opened.
    transactions: BTreeSet<(u64, String)>,
    slots: BTreeSet<u64>,
}

/// Fetches each detail report once, however many rows lead to it.
#[derive(Default)]
struct Reports {
    transactions: BTreeMap<(u64, String), Value>,
    slots: BTreeMap<u64, Value>,
    entities: BTreeSet<(String, String)>,
}

impl Reports {
    async fn transaction(&mut self, app: &Router, slot: u64, signature: &str) -> &Value {
        let key = (slot, signature.to_owned());
        if !self.transactions.contains_key(&key) {
            let report = get(app, &format!("/profile/transaction/{signature}?slot={slot}")).await;
            self.transactions.insert(key.clone(), report);
        }
        &self.transactions[&key]
    }

    async fn slot(&mut self, app: &Router, slot: u64) -> &Value {
        if !self.slots.contains_key(&slot) {
            let report = get(app, &format!("/profile/slot/{slot}")).await;
            self.slots.insert(slot, report);
        }
        &self.slots[&slot]
    }

    /// An entity profile must answer; it is fetched once.
    async fn entity(&mut self, app: &Router, kind: &str, id: &str) {
        if self.entities.insert((kind.to_owned(), id.to_owned())) {
            let profile = get(app, &format!("/profile/{kind}/{id}?window_slots=2000")).await;
            assert_eq!(profile["id"], id, "{profile}");
        }
    }
}

fn column(columns: &Value, name: &str) -> Option<usize> {
    columns.as_array().unwrap().iter().position(|c| c["name"] == name)
}

/// Every instruction of a transaction report, by path.
fn instructions_by_path(report: &Value) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for top in report["transaction"]["instructions"].as_array().unwrap() {
        out.insert(top["path"].as_str().unwrap().to_owned(), top.clone());
        for inner in top["inner"].as_array().unwrap() {
            out.insert(inner["path"].as_str().unwrap().to_owned(), inner.clone());
        }
    }
    out
}

fn path_of(index: &Value, inner: &Value) -> String {
    match inner.as_u64() {
        None => index.as_u64().unwrap().to_string(),
        Some(inner) => format!("{}.{inner}", index.as_u64().unwrap()),
    }
}

/// Drill **every row** of `answer` (the answer to `spec`), open **every**
/// drilled row as its detail report, and assert the report is found and
/// contains exactly the thing the row names.
async fn drill_every_row(app: &Router, reports: &mut Reports, what: &str, spec: &Value, answer: &Value, cover: &mut Coverage) {
    let columns = answer["columns"].as_array().unwrap();
    let dimensions: Vec<(usize, &str)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c["role"] == "dimension")
        .map(|(i, c)| (i, c["name"].as_str().unwrap()))
        .collect();
    let measures: Vec<(usize, &str)> = columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c["role"] == "measure")
        .map(|(i, c)| (i, c["name"].as_str().unwrap()))
        .collect();
    // The drill reads the very window the answer read.
    let mut pinned = spec.clone();
    pinned["as_of_slot"] = answer["as_of_slot"].clone();

    for row in answer["rows"].as_array().unwrap() {
        cover.answer_rows += 1;
        let row_values: serde_json::Map<String, Value> =
            dimensions.iter().map(|(i, name)| ((*name).to_owned(), row[*i].clone())).collect();
        let request = json!({ "spec": pinned, "row": row_values, "limit": 200 });
        let drill = post(app, "/drill", &request).await;
        let ctx = format!("{what}: row {row} -> {}", drill["sql"]);

        assert_eq!(drill["as_of_slot"], answer["as_of_slot"], "{ctx}");
        assert_eq!(drill["window"], answer["window"], "{ctx}");
        let drilled = drill["rows"].as_array().unwrap();
        // A group exists only because rows are in it.
        if !dimensions.is_empty() {
            assert!(!drilled.is_empty(), "a grouped row drilled to nothing: {ctx}");
        }
        // A plain count is the number of rows behind it.
        if let [(i, name)] = measures.as_slice()
            && name.ends_with("_count")
            && !["mint_count", "program_count"].contains(name)
        {
            assert_eq!(drill["has_more"], false, "{ctx}");
            assert_eq!(row[*i].as_u64().unwrap() as usize, drilled.len(), "{name} disagrees with its rows: {ctx}");
        }

        let cols = &drill["columns"];
        let slot_at = column(cols, "slot").expect("every drill row carries its slot");
        for d in drilled {
            cover.drilled_rows += 1;
            let slot = d[slot_at].as_u64().unwrap();
            assert!((FIRST_SLOT..=LAST_SLOT).contains(&slot), "{ctx}");

            if drill["detail"] == "slot" {
                let report = reports.slot(app, slot).await;
                assert_eq!(report["found"], true, "slot {slot} has no report: {ctx}");
                assert_eq!(report["block"]["blockhash"], d[column(cols, "blockhash").unwrap()], "{ctx}");
                cover.slots.insert(slot);
                cover.details_opened += 1;
                continue;
            }

            assert_eq!(drill["detail"], "transaction", "{ctx}");
            let signature = d[column(cols, "tx_signature").unwrap()].as_str().unwrap();
            let report = reports.transaction(app, slot, signature).await;
            assert_eq!(report["found"], true, "{signature} has no report: {ctx}");
            assert_eq!(report["signature"], signature, "{ctx}");
            let tx = &report["transaction"];
            assert_eq!(tx["slot"], slot, "{ctx}");
            match drill["table"].as_str().unwrap() {
                "transactions" => {
                    assert_eq!(tx["status"], d[column(cols, "status").unwrap()], "{ctx}");
                    assert_eq!(tx["fee_lamports"], d[column(cols, "fee").unwrap()], "{ctx}");
                }
                "instructions" => {
                    let path = path_of(&d[column(cols, "instruction_index").unwrap()], &d[column(cols, "inner_instruction_index").unwrap()]);
                    let tree = instructions_by_path(report);
                    let node = tree.get(&path).unwrap_or_else(|| panic!("instruction {path} is not in the report: {ctx}"));
                    assert_eq!(node["program_id"], d[column(cols, "program_id").unwrap()], "{ctx}");
                }
                "token_transfers" => {
                    let path = path_of(&d[column(cols, "instruction_index").unwrap()], &d[column(cols, "inner_instruction_index").unwrap()]);
                    let transfer = tx["token_transfers"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|t| t["path"] == path.as_str())
                        .unwrap_or_else(|| panic!("transfer {path} is not in the report: {ctx}"));
                    assert_eq!(transfer["amount_raw"], d[column(cols, "amount_raw").unwrap()], "{ctx}");
                    assert_eq!(transfer["mint"], d[column(cols, "mint_token_address").unwrap()], "{ctx}");
                    // The instruction that made it is in the tree too.
                    assert!(instructions_by_path(report).contains_key(&path), "{ctx}");
                }
                other => panic!("unexpected table {other}: {ctx}"),
            }
            cover.transactions.insert((slot, signature.to_owned()));
            cover.details_opened += 1;
        }

        // Values that are entities open as entity profiles as well.
        for (i, name) in &dimensions {
            let Some(value) = row[*i].as_str() else { continue };
            let kind = match *name {
                "mint" if value != "unattributed" => "mint",
                "program" | "token_program" => "program",
                _ => continue,
            };
            reports.entity(app, kind, value).await;
            cover.entity_profiles_opened += 1;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_of_every_builtin_template_opens_a_detail_report() {
    let r = rig().await;
    let mut reports = Reports::default();
    let templates = get(&r.app, "/templates").await;
    let templates = templates["templates"].as_array().unwrap();
    assert!(templates.len() >= 4, "{templates:?}");

    let mut total = Coverage::default();
    for template in templates {
        let id = template["id"].as_str().unwrap();
        let examples = template["examples"].as_array().unwrap();
        assert!(!examples.is_empty(), "{id}");
        let mut cover = Coverage::default();
        for example in examples {
            let question = example.as_str().unwrap();
            let asked = post(&r.app, "/ask", &json!({ "question": question })).await;
            assert_eq!(asked["outcome"], "matched", "{question}: {asked}");
            assert_eq!(asked["executed"], true, "{question}");
            assert_eq!(asked["template_id"], id, "{question}");
            let answer = &asked["answer"];
            assert_eq!(answer["as_of_slot"], LAST_SLOT, "{question}");
            assert!(!answer["rows"].as_array().unwrap().is_empty(), "{question} returned no rows over the chain");
            let before = cover.details_opened;
            drill_every_row(&r.app, &mut reports, question, &asked["spec"], answer, &mut cover).await;
            // Measured per question: each one led to at least one report.
            assert!(cover.details_opened > before, "{question} opened no detail report");
        }
        assert!(cover.answer_rows >= examples.len(), "{id}: {cover:?}");
        assert_eq!(cover.details_opened, cover.drilled_rows, "{id}: a drilled row was not opened: {cover:?}");
        total.answer_rows += cover.answer_rows;
        total.drilled_rows += cover.drilled_rows;
        total.details_opened += cover.details_opened;
        total.entity_profiles_opened += cover.entity_profiles_opened;
        total.transactions.extend(cover.transactions);
        total.slots.extend(cover.slots);
    }

    // The measure: the templates alone reach every transaction of the chain
    // and slots with and without a known leader.
    let every_tx: BTreeSet<(u64, String)> = chain().iter().map(|t| (t.slot, sig(t.sig))).collect();
    assert_eq!(total.transactions, every_tx, "{total:?}");
    assert!(total.slots.contains(&SWAP_SLOT) && total.slots.contains(&LAST_SLOT), "{total:?}");
    assert_eq!(total.details_opened, total.drilled_rows);
    assert!(total.entity_profiles_opened > 0, "{total:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_of_custom_queries_opens_a_detail_report() {
    let r = rig().await;
    let mut reports = Reports::default();
    let catalog = get(&r.app, "/catalog").await;
    let window = json!({ "from_slot": FIRST_SLOT, "to_slot": LAST_SLOT });
    let group = |d: &str| if d == "slot_bucket" { json!({ "dimension": d, "bucket_width": 100 }) } else { json!({ "dimension": d }) };

    // Every measure, grouped by every dimension it supports.
    let mut specs: Vec<(String, Value)> = Vec::new();
    let mut dimensions_used = BTreeSet::new();
    for m in catalog["measures"].as_array().unwrap() {
        let measure = m["id"].as_str().unwrap();
        for d in m["dimensions"].as_array().unwrap() {
            let dimension = d.as_str().unwrap();
            dimensions_used.insert(dimension.to_owned());
            let mut spec = json!({ "measures": [measure], "group_by": [group(dimension)], "window": window, "limit": 100 });
            // A measure that needs a dimension it is not grouped by gets it pinned.
            if let Some(required) = m["requires_dimension"].as_str().filter(|r| *r != dimension) {
                assert_eq!(required, "mint", "the test only knows how to pin a mint");
                spec["filters"] = json!([{ "dimension": "mint", "op": "eq", "values": [mint1()] }]);
            }
            specs.push((format!("{measure} by {dimension}"), spec));
        }
        // And ungrouped, where it can be.
        if m["requires_dimension"].is_null() {
            specs.push((format!("{measure} alone"), json!({ "measures": [measure], "window": window })));
        }
    }
    let every_dimension: BTreeSet<String> =
        catalog["dimensions"].as_array().unwrap().iter().map(|d| d["id"].as_str().unwrap().to_owned()).collect();
    assert_eq!(dimensions_used, every_dimension, "a dimension no measure supports is not covered");

    // Several dimensions at once, filters, several measures, a pinned snapshot.
    specs.push((
        "transfers by mint and source owner".into(),
        json!({ "measures": ["transfer_count"], "group_by": [group("mint"), group("source_owner")], "window": window }),
    ));
    specs.push((
        "instructions by program, level and bucket".into(),
        json!({ "measures": ["ix_count", "ix_cpi_count"], "group_by": [group("program"), group("ix_level"), group("slot_bucket")],
                "window": { "last_slots": 1_500 }, "as_of_slot": LAST_SLOT - 7 }),
    ));
    specs.push((
        "failed transactions per slot".into(),
        json!({ "measures": ["tx_failed_count"], "group_by": [group("slot")], "window": window,
                "filters": [{ "dimension": "tx_status", "op": "in", "values": ["failed", "success"] }] }),
    ));
    specs.push((
        "SPL transfers by destination owner".into(),
        json!({ "measures": ["transfer_count"], "group_by": [group("destination_owner")], "window": window,
                "filters": [{ "dimension": "token_program", "op": "in", "values": [TOKEN, TOKEN_2022] }] }),
    ));

    let mut cover = Coverage::default();
    let mut values_seen: BTreeSet<String> = BTreeSet::new();
    for (label, spec) in &specs {
        let answer = post(&r.app, "/query", spec).await;
        for row in answer["rows"].as_array().unwrap() {
            values_seen.extend(row.as_array().unwrap().iter().filter_map(Value::as_str).map(str::to_owned));
        }
        drill_every_row(&r.app, &mut reports, label, spec, &answer, &mut cover).await;
    }

    assert!(specs.len() > 60, "{}", specs.len());
    assert_eq!(cover.details_opened, cover.drilled_rows, "a drilled row was not opened: {cover:?}");
    assert!(cover.answer_rows > 100 && cover.drilled_rows > 200, "{cover:?}");
    let every_tx: BTreeSet<(u64, String)> = chain().iter().map(|t| (t.slot, sig(t.sig))).collect();
    assert_eq!(cover.transactions, every_tx);
    let every_slot: BTreeSet<u64> = chain().iter().map(|t| t.slot).collect();
    assert_eq!(cover.slots, every_slot);
    // The values a filter cannot express were drilled like any other.
    for placeholder in ["unattributed", "unknown", "failed", "inner", "undecoded"] {
        assert!(values_seen.contains(placeholder), "{placeholder} never appeared in an answer");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_of_a_raw_sql_query_opens_a_detail_report() {
    let r = rig().await;
    let lens = r.lens.client();
    let mut opened = 0;
    // What the dashboard's SQL tab links: a signature with its slot, and a slot.
    for sql in [
        "SELECT slot, tx_signature, status FROM solana.transactions ORDER BY slot DESC",
        "SELECT tx_signature, slot, program_id, instruction_index, inner_instruction_index FROM solana.instructions",
        "SELECT mint_token_address AS mint, slot, tx_signature FROM solana.token_transfers WHERE amount_raw > 1",
    ] {
        let rows = lens.query(sql).await.unwrap().rows;
        assert!(!rows.is_empty(), "{sql}");
        for row in rows {
            let (slot, signature) = (row["slot"].as_u64().unwrap(), row["tx_signature"].as_str().unwrap());
            let report = get(&r.app, &format!("/profile/transaction/{signature}?slot={slot}")).await;
            assert_eq!(report["found"], true, "{sql}: {row:?}");
            if let Some(program) = row.get("program_id") {
                let path = path_of(&row["instruction_index"], &row["inner_instruction_index"]);
                assert_eq!(&instructions_by_path(&report)[&path]["program_id"], program, "{sql}");
            }
            // Without the slot the signature is still found, by searching the window.
            let searched = get(&r.app, &format!("/profile/transaction/{signature}?window_slots=2000")).await;
            assert_eq!(searched["transaction"], report["transaction"], "{signature}");
            opened += 1;
        }
    }
    let slots = lens.query("SELECT slot, transaction_count FROM solana.slots").await.unwrap().rows;
    for row in &slots {
        let report = get(&r.app, &format!("/profile/slot/{}", row["slot"])).await;
        assert_eq!(report["found"], true, "{row:?}");
        assert_eq!(report["counts"]["transactions"], row["transaction_count"], "{row:?}");
        // And every transaction a slot lists opens in turn.
        for tx in report["transactions"].as_array().unwrap() {
            let found = get(&r.app, &format!("/profile/transaction/{}?slot={}", tx["signature"].as_str().unwrap(), row["slot"])).await;
            assert_eq!(found["found"], true);
            assert_eq!(found["transaction"]["transaction_index"], tx["transaction_index"]);
            opened += 1;
        }
    }
    assert_eq!(slots.len(), 7);
    assert!(opened >= 8 + 11 + 4 + 8, "{opened}");
}

// ---------------------------------------------------------------------------
// The reports against the chain
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_report_matches_the_chain() {
    let r = rig().await;
    let report = transaction(&r.app, SWAP_SIG, SWAP_SLOT).await;
    assert_eq!(report["kind"], "transaction");
    assert_eq!(report["found"], true);
    assert_eq!(report["as_of_slot"], LAST_SLOT);
    assert_eq!(report["searched"], json!({ "from_slot": SWAP_SLOT, "to_slot": SWAP_SLOT, "slots": 1 }));
    assert_eq!(report["queries_run"], oracle::profile::TRANSACTION_QUERIES);
    assert_eq!(report["warnings"], json!([]));

    let tx = &report["transaction"];
    assert_eq!(tx["slot"], SWAP_SLOT);
    assert_eq!(tx["transaction_index"], 0);
    assert_eq!(tx["status"], "success");
    assert_eq!(tx["error"], Value::Null);
    assert_eq!(tx["fee_lamports"], 5_001);
    assert_eq!(tx["fee_payer"], alice());
    assert_eq!(tx["compute_units_consumed"], 1_000);
    assert_eq!(
        tx["block"],
        json!({
            "blockhash": format!("hash{SWAP_SLOT}"), "parent_slot": SWAP_SLOT - 1, "block_time": block_time(SWAP_SLOT),
            "block_height": SWAP_SLOT - 1_000, "transaction_count": 2, "leader": leader(), "leader_name": null,
        })
    );
    // Accounts in message order, named from the catalog where it knows them.
    let accounts = tx["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 4);
    assert_eq!(accounts[3], json!({ "index": 3, "address": TOKEN, "name": "SPL Token" }));
    assert_eq!(accounts[0]["name"], Value::Null);

    // The tree: top-level by index, inner under its parent.
    let tops = tx["instructions"].as_array().unwrap();
    assert_eq!(tops.iter().map(|i| i["path"].as_str().unwrap()).collect::<Vec<_>>(), vec!["0", "1", "2"]);
    assert_eq!(tops[0]["inner"], json!([]));
    let inner = &tops[1]["inner"];
    assert_eq!(inner.as_array().unwrap().len(), 1);
    assert_eq!(inner[0]["path"], "1.0");
    assert_eq!(inner[0]["program_name"], "SPL Token");

    // Decoding: PRISM's where it decoded, ORACLE's for the programs it knows.
    assert_eq!(tops[0]["instruction_type"], "SetComputeUnitLimit");
    assert_eq!(tops[0]["decoded"], json!({ "units": 200_000 }));
    assert_eq!(tops[0]["decoded_by"], "oracle");
    assert_eq!(tops[0]["program_name"], "Compute Budget");
    assert_eq!((&inner[0]["instruction_type"], &inner[0]["decoded_by"]), (&json!("Transfer"), &json!("prism")));
    assert_eq!(inner[0]["decoded"], json!({ "amount": 1500 }));
    assert_eq!((&tops[1]["instruction_type"], &tops[1]["decoded_by"]), (&Value::Null, &Value::Null));
    assert_eq!(tops[2]["decoded"], json!({ "memo": "gm deepana" }));

    // Logs: grouped by invocation, and each instruction points at its group.
    let groups = tx["logs"]["groups"].as_array().unwrap();
    assert_eq!(
        groups.iter().map(|g| (g["program_id"].as_str().unwrap(), g["depth"].as_u64().unwrap())).collect::<Vec<_>>(),
        vec![(BUDGET, 1), (swap_program().as_str(), 1), (TOKEN, 2), (MEMO, 1)]
    );
    assert_eq!(tx["logs"]["total_lines"], 13);
    assert_eq!(tx["logs"]["shown_lines"], 13);
    assert_eq!(tops.iter().map(|i| i["log_group"].as_u64().unwrap()).collect::<Vec<_>>(), vec![0, 1, 3]);
    assert_eq!(inner[0]["log_group"], 2);
    assert_eq!(inner[0]["stack_height"], 2);
    assert_eq!(inner[0]["compute_units_consumed"], 4_645);
    assert_eq!(tops[1]["compute_units_consumed"], 30_000);
    assert_eq!(groups[1]["lines"].as_array().unwrap().len(), 4);

    assert_eq!(tx["memos"], json!([{ "path": "2", "text": "gm deepana", "source": "instruction_data" }]));
    assert_eq!(
        tx["token_transfers"],
        json!([{
            "path": "1.0", "instruction_index": 1, "inner_instruction_index": 0,
            "token_program": TOKEN, "token_program_name": "SPL Token",
            "source_token_account": key(10), "destination_token_account": key(11),
            "source_owner": alice(), "destination_owner": bob(),
            "mint": mint1(), "mint_name": null,
            "amount_raw": "1500", "amount_ui": "0.001500", "decimals": 6,
        }])
    );
    assert_eq!(inner[0]["token_transfer"], 0);
    assert_eq!(
        tx["counts"],
        json!({ "instructions": 4, "inner_instructions": 1, "decoded_instructions": 3, "token_transfers": 1, "programs": 4 })
    );
    assert_eq!(
        tx["duplicates"],
        json!({ "transaction_rows": 0, "slot_rows": 0, "instruction_rows": 0, "token_transfer_rows": 0, "conflicting": 0 })
    );

    // An amount beyond 2^53 survives exactly, raw and in UI units.
    let wsol = transaction(&r.app, WSOL_SIG, 501_040).await;
    let t = &wsol["transaction"]["token_transfers"][0];
    assert_eq!(t["amount_raw"], "18446744073709551615");
    assert_eq!(t["amount_ui"], "18446744073.709551615");
    assert_eq!(t["mint_name"], "Wrapped SOL");

    // A SOL transfer PRISM stored as opaque bytes is readable.
    let sol = transaction(&r.app, SOL_SIG, SWAP_SLOT).await;
    let i = &sol["transaction"]["instructions"][0];
    assert_eq!(i["instruction_type"], "Transfer");
    assert_eq!(i["decoded"], json!({ "lamports": "5000", "source": alice(), "destination": bob() }));
    assert_eq!(sol["transaction"]["transaction_index"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_transaction_is_reported_in_full_with_where_and_why() {
    let r = rig().await;
    let report = transaction(&r.app, FAILED_SIG, FAILED_SLOT).await;
    assert_eq!(report["found"], true);
    let tx = &report["transaction"];
    assert_eq!(tx["status"], "failed");
    assert_eq!(
        tx["error"],
        json!({
            "message": r#"{"InstructionError":[1,{"Custom":1}]}"#,
            "instruction_index": 1,
            "program_id": TOKEN, "program_name": "SPL Token",
            "reason": "custom program error: 0x1",
            "path": "1", "log_group": 1,
        })
    );
    let tops = tx["instructions"].as_array().unwrap();
    assert_eq!(tops.iter().map(|i| i["ran"].as_bool().unwrap()).collect::<Vec<_>>(), vec![true, true, false]);
    assert_eq!(tops[1]["outcome"], "failed");
    assert_eq!(tops[2]["log_group"], Value::Null);
    // What it attempted is shown, and the fee it paid.
    assert_eq!(tx["token_transfers"].as_array().unwrap().len(), 1);
    assert_eq!(tx["fee_lamports"], 5_003);
    assert_eq!(report["warnings"], json!([oracle::profile::transaction::WARN_FAILED]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signature_or_slot_that_is_not_there_is_found_false_not_an_error() {
    let r = rig().await;
    // A well-formed signature nobody sent.
    let report = get(&r.app, &format!("/profile/transaction/{}", sig(200))).await;
    assert_eq!(report["found"], false);
    assert_eq!(report["transaction"], Value::Null);
    assert_eq!(report["queries_run"], oracle::profile::TRANSACTION_QUERIES_NOT_FOUND);
    assert_eq!(report["warnings"], json!([oracle::profile::transaction::WARN_NOT_FOUND]));
    assert_eq!(report["searched"]["to_slot"], LAST_SLOT);
    // The right signature in the wrong slot.
    let wrong = transaction(&r.app, SWAP_SIG, FAILED_SLOT).await;
    assert_eq!(wrong["found"], false);
    // Outside the window searched.
    let narrow = get(&r.app, &format!("/profile/transaction/{}?window_slots=100", sig(SWAP_SIG))).await;
    assert_eq!(narrow["found"], false);
    assert_eq!(narrow["searched"], json!({ "from_slot": LAST_SLOT - 99, "to_slot": LAST_SLOT, "slots": 100 }));

    // A slot PRISM never wrote.
    let slot = get(&r.app, "/profile/slot/501011").await;
    assert_eq!(slot["found"], false);
    assert_eq!(slot["block"], Value::Null);
    assert_eq!(slot["transactions"], json!([]));

    // Malformed input is refused before anything reaches LENS.
    for (uri, field) in [
        ("/profile/transaction/not-a-signature".to_owned(), "signature"),
        (format!("/profile/transaction/{}", key(1)), "signature"),
        (format!("/profile/transaction/{}?slot=1&window_slots=5", sig(1)), "window_slots"),
        (format!("/profile/transaction/{}?slot={}", sig(1), LAST_SLOT + 1), "slot"),
        (format!("/profile/transaction/{}?as_of_slot={}", sig(1), LAST_SLOT + 1), "as_of_slot"),
        ("/profile/slot/abc".to_owned(), "slot"),
        (format!("/profile/slot/{}", LAST_SLOT + 1), "slot"),
        (format!("/profile/slot/{SWAP_SLOT}?limit=0"), "limit"),
        (format!("/profile/slot/{SWAP_SLOT}?limit=501"), "limit"),
    ] {
        let (status, body) = call(&r.app, Request::get(&uri).body(Body::empty()).unwrap()).await;
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!((status, &body["field"]), (StatusCode::BAD_REQUEST, &json!(field)), "{uri}: {body}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_report_lists_its_transactions_in_pages() {
    let r = rig().await;
    let report = get(&r.app, &format!("/profile/slot/{SWAP_SLOT}")).await;
    assert_eq!(report["kind"], "slot");
    assert_eq!(report["found"], true);
    assert_eq!(report["queries_run"], oracle::profile::SLOT_QUERIES);
    assert_eq!(report["counts"], json!({ "transactions": 2, "failed_transactions": 0 }));
    assert_eq!(report["block"]["leader"], leader());
    let txs = report["transactions"].as_array().unwrap();
    assert_eq!(
        txs.iter().map(|t| (t["transaction_index"].as_u64().unwrap(), t["signature"].as_str().unwrap().to_owned())).collect::<Vec<_>>(),
        vec![(0, sig(SWAP_SIG)), (1, sig(SOL_SIG))]
    );
    assert_eq!(report["page"], json!({ "after_index": null, "limit": 100, "has_more": false, "next_after_index": null }));
    assert_eq!(report["warnings"], json!([]));

    // One per page: the second page starts where the first says.
    let first = get(&r.app, &format!("/profile/slot/{SWAP_SLOT}?limit=1")).await;
    assert_eq!(first["transactions"].as_array().unwrap().len(), 1);
    assert_eq!(first["page"], json!({ "after_index": null, "limit": 1, "has_more": true, "next_after_index": 0 }));
    assert_eq!(first["counts"]["transactions"], 2, "the counts cover the slot, not the page");
    let second = get(&r.app, &format!("/profile/slot/{SWAP_SLOT}?limit=1&after_index=0")).await;
    assert_eq!(second["transactions"][0]["signature"], sig(SOL_SIG));
    assert_eq!(second["page"]["has_more"], false);

    let failed = get(&r.app, &format!("/profile/slot/{FAILED_SLOT}")).await;
    assert_eq!(failed["counts"], json!({ "transactions": 1, "failed_transactions": 1 }));
    assert_eq!(failed["transactions"][0]["status"], "failed");
}

/// The body without its last field, `duration_ms`.
fn without_duration(body: &[u8]) -> String {
    let text = String::from_utf8(body.to_vec()).unwrap();
    let at = text.rfind(",\"duration_ms\":").expect("duration_ms is the last field");
    assert!(text[at..].trim_end_matches('}').len() < 40, "duration_ms is not last: {}", &text[at..]);
    text[..at].to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_request_gives_identical_bytes_after_more_data_arrives() {
    let r = rig().await;
    let spec = json!({ "measures": ["transfer_count"], "group_by": [{ "dimension": "mint" }],
                       "window": { "last_slots": 2_000 }, "as_of_slot": LAST_SLOT });
    let drill = json!({ "spec": spec, "row": { "mint": mint1() } });
    let uris = [
        format!("/profile/transaction/{}?slot={SWAP_SLOT}&as_of_slot={LAST_SLOT}", sig(SWAP_SIG)),
        format!("/profile/transaction/{}?window_slots=2000&as_of_slot={LAST_SLOT}", sig(FAILED_SIG)),
        format!("/profile/transaction/{}?window_slots=50&as_of_slot={LAST_SLOT}", sig(200)),
        format!("/profile/slot/{SWAP_SLOT}?as_of_slot={LAST_SLOT}"),
    ];
    let post_bytes = |body: Value| {
        let app = r.app.clone();
        async move {
            let req = Request::post("/drill").header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
            let (status, bytes) = call(&app, req).await;
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&bytes));
            bytes
        }
    };

    let mut before = Vec::new();
    for uri in &uris {
        // Twice in a row first: nothing but the duration differs.
        let (a, b) = (get_bytes(&r.app, uri).await, get_bytes(&r.app, uri).await);
        assert_eq!(without_duration(&a), without_duration(&b), "{uri}");
        before.push(without_duration(&a));
    }
    let drill_before = without_duration(&post_bytes(drill.clone()).await);

    // More slots arrive; the snapshot moves on.
    write(&r.data_dir(), &later());
    r.lens.refresh();
    let moved = get(&r.app, &format!("/profile/slot/{SWAP_SLOT}")).await;
    assert_eq!(moved["as_of_slot"], LAST_SLOT + 14);

    for (uri, expected) in uris.iter().zip(&before) {
        assert_eq!(&without_duration(&get_bytes(&r.app, uri).await), expected, "{uri}");
    }
    assert_eq!(without_duration(&post_bytes(drill).await), drill_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_repeated_by_a_replay_are_dropped_and_counted() {
    let r = rig().await;
    let clean_tx = transaction(&r.app, FAILED_SIG, FAILED_SLOT).await;
    let clean_slot = get(&r.app, &format!("/profile/slot/{FAILED_SLOT}")).await;
    let spec = json!({ "measures": ["tx_count"], "group_by": [{ "dimension": "tx_status" }],
                       "window": { "from_slot": FIRST_SLOT, "to_slot": LAST_SLOT } });
    let drill_request = json!({ "spec": spec, "row": { "tx_status": "failed" } });
    let clean_drill = post(&r.app, "/drill", &drill_request).await;

    // PRISM crashes and replays the slot: every row of it is written again.
    let replayed: Vec<Tx> = chain().into_iter().filter(|t| t.slot == FAILED_SLOT).collect();
    write(&r.data_dir(), &replayed);
    r.lens.refresh();

    // The aggregate now counts the transaction twice...
    let answer = post(&r.app, "/query", &spec).await;
    let failed_row = answer["rows"].as_array().unwrap().iter().find(|row| row[0] == "failed").unwrap();
    assert_eq!(failed_row[1], 2, "LENS sees the repeated row: {answer}");

    // ...the transaction report shows it once, and says what it dropped.
    let mut tx = transaction(&r.app, FAILED_SIG, FAILED_SLOT).await;
    assert_eq!(
        tx["transaction"]["duplicates"],
        json!({ "transaction_rows": 1, "slot_rows": 1, "instruction_rows": 3, "token_transfer_rows": 1, "conflicting": 0 })
    );
    assert_eq!(
        tx["warnings"],
        json!([oracle::profile::transaction::WARN_FAILED, oracle::profile::transaction::WARN_DUPLICATES])
    );
    // Nothing else in the report changed.
    tx["transaction"]["duplicates"] = clean_tx["transaction"]["duplicates"].clone();
    tx["warnings"] = clean_tx["warnings"].clone();
    tx["duration_ms"] = clean_tx["duration_ms"].clone();
    assert_eq!(tx, clean_tx);

    // The slot report lists and counts it once.
    let mut slot = get(&r.app, &format!("/profile/slot/{FAILED_SLOT}")).await;
    assert_eq!(slot["duplicates"], json!({ "transaction_rows": 1, "slot_rows": 1, "conflicting": 0 }));
    assert_eq!(slot["warnings"], json!([oracle::profile::slot::WARN_SLOT_DUPLICATES]));
    slot["duplicates"] = clean_slot["duplicates"].clone();
    slot["warnings"] = clean_slot["warnings"].clone();
    slot["duration_ms"] = clean_slot["duration_ms"].clone();
    assert_eq!(slot, clean_slot);

    // And the drill lists it once.
    let drill = post(&r.app, "/drill", &drill_request).await;
    assert_eq!(drill["rows"], clean_drill["rows"]);
    assert_eq!(drill["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_drill_reads_the_answers_window_and_reports_what_it_ran() {
    let r = rig().await;
    let spec = json!({ "measures": ["transfer_count"], "group_by": [{ "dimension": "mint" }],
                       "window": { "last_slots": 1_000 }, "as_of_slot": LAST_SLOT });
    let answer = post(&r.app, "/query", &spec).await;
    let drill = post(&r.app, "/drill", &json!({ "spec": spec, "row": { "mint": mint1() }, "limit": 1 })).await;
    assert_eq!(drill["table"], "token_transfers");
    assert_eq!(drill["detail"], "transaction");
    assert_eq!(drill["window"], answer["window"]);
    assert_eq!(drill["queries_run"], oracle::spec::SPEC_QUERIES);
    assert_eq!(drill["limit"], 1);
    // Two transfers of the mint (one in the failed transaction); newest first.
    assert_eq!(drill["has_more"], true);
    let cols = &drill["columns"];
    assert_eq!(cols[0], json!({ "name": "slot", "entity": "slot" }));
    assert_eq!(drill["rows"], json!([[FAILED_SLOT, sig(FAILED_SIG), 1, null, mint1(), key(10), key(11), "999", 6]]));
    assert!(drill["sql"].as_str().unwrap().contains(&format!("mint_token_address = '{}'", mint1())));

    // Errors are the query's: an unknown measure, a value that is not one.
    for (body, field) in [
        (json!({ "spec": { "measures": ["nope"], "window": { "last_slots": 10 } } }), "measures[0]"),
        (json!({ "spec": spec, "row": { "mint": "x' OR '1'='1" } }), "row.mint"),
        (json!({ "spec": spec, "row": {} }), "row.mint"),
        (json!({ "spec": spec, "row": { "mint": mint1() }, "limit": 201 }), "limit"),
        (json!({ "spec": spec, "row": { "mint": mint1() }, "rows": 5 }), "rows"),
    ] {
        let req = Request::post("/drill").header("content-type", "application/json").body(Body::from(body.to_string())).unwrap();
        let (status, bytes) = call(&r.app, req).await;
        let reply: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!((status, &reply["field"]), (StatusCode::BAD_REQUEST, &json!(field)), "{body}: {reply}");
    }
}

// ---------------------------------------------------------------------------
// Entity hints: every hinted value of every answer opens its report
// ---------------------------------------------------------------------------

/// The ORACLE route a client opens for a value of a column with `hint`, as
/// documented on `EntityHint`: `None` if the value is not one.
fn route_for(hint: &str, value: &Value, row_slot: Option<u64>) -> Option<String> {
    let text = value.as_str();
    let is = |bytes: usize| text.is_some_and(|t| bs58::decode(t).into_vec().is_ok_and(|b| b.len() == bytes));
    match hint {
        "slot" => value.as_u64().map(|n| format!("/profile/slot/{n}")),
        "tx_signature" if is(64) => Some(format!(
            "/profile/transaction/{}{}",
            text.unwrap(),
            row_slot.map(|n| format!("?slot={n}")).unwrap_or_default()
        )),
        "mint" | "program" | "token_account" if is(32) => Some(format!("/profile/{hint}/{}", text.unwrap())),
        _ => None,
    }
}

/// Open every hinted value of `columns`/`rows` and check the report is about
/// it. Returns how many were opened.
async fn open_every_hinted_value(app: &Router, what: &str, columns: &Value, rows: &Value, opened: &mut BTreeSet<String>) -> usize {
    let columns = columns.as_array().unwrap();
    let hints: Vec<Option<&str>> = columns
        .iter()
        .map(|c| c.as_object().unwrap().get("entity").expect("every column has an entity key").as_str())
        .collect();
    let slot_at = hints.iter().position(|h| *h == Some("slot"));
    let mut n = 0;
    for row in rows.as_array().unwrap() {
        let row_slot = slot_at.and_then(|i| row[i].as_u64());
        for (j, hint) in hints.iter().enumerate() {
            let Some(hint) = hint else { continue };
            let value = &row[j];
            let Some(route) = route_for(hint, value, row_slot) else {
                // Only placeholders and nulls may fail to open.
                assert!(
                    value.is_null() || ["unattributed", "unknown"].contains(&value.as_str().unwrap_or_default()),
                    "{what}: {hint} value {value} opens as nothing"
                );
                continue;
            };
            n += 1;
            if !opened.insert(route.clone()) {
                continue;
            }
            let report = get(app, &route).await;
            assert_eq!(report["found"], true, "{what}: {route}: {report}");
            match *hint {
                "slot" => assert_eq!(&report["slot"], value, "{what}"),
                "tx_signature" => {
                    assert_eq!(&report["signature"], value, "{what}");
                    assert_eq!(report["transaction"]["slot"], row_slot.unwrap(), "{what}");
                }
                kind => assert_eq!((report["kind"].as_str().unwrap(), &report["id"]), (kind, value), "{what}"),
            }
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_hinted_value_of_every_answer_and_drill_opens_its_report() {
    let r = rig().await;
    let templates = get(&r.app, "/templates").await;
    let mut opened = BTreeSet::new();
    let mut values = 0;
    let mut hinted_columns = BTreeSet::new();
    for example in templates["templates"].as_array().unwrap().iter().flat_map(|t| t["examples"].as_array().unwrap().clone()) {
        let question = example.as_str().unwrap().to_owned();
        let asked = post(&r.app, "/ask", &json!({ "question": question })).await;
        let answer = &asked["answer"];
        for c in answer["columns"].as_array().unwrap() {
            if let Some(e) = c["entity"].as_str() {
                hinted_columns.insert((c["name"].as_str().unwrap().to_owned(), e.to_owned()));
            }
        }
        values += open_every_hinted_value(&r.app, &question, &answer["columns"], &answer["rows"], &mut opened).await;

        // And the rows behind each answer row.
        let dims: Vec<(usize, String)> = answer["columns"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .filter(|(_, c)| c["role"] == "dimension")
            .map(|(i, c)| (i, c["name"].as_str().unwrap().to_owned()))
            .collect();
        let mut spec = asked["spec"].clone();
        spec["as_of_slot"] = answer["as_of_slot"].clone();
        for row in answer["rows"].as_array().unwrap() {
            let row: serde_json::Map<String, Value> = dims.iter().map(|(i, n)| (n.clone(), row[*i].clone())).collect();
            let drill = post(&r.app, "/drill", &json!({ "spec": spec, "row": row, "limit": 200 })).await;
            values += open_every_hinted_value(&r.app, &format!("{question} / drill"), &drill["columns"], &drill["rows"], &mut opened).await;
        }
    }
    // The answers' hinted columns are exactly the entity dimensions that appeared.
    assert!(hinted_columns.contains(&("mint".to_owned(), "mint".to_owned())), "{hinted_columns:?}");
    assert!(hinted_columns.contains(&("program".to_owned(), "program".to_owned())), "{hinted_columns:?}");
    // Every kind of report was opened from a hinted value.
    for prefix in ["/profile/transaction/", "/profile/slot/", "/profile/mint/", "/profile/program/", "/profile/token_account/"] {
        assert!(opened.iter().any(|r| r.starts_with(prefix)), "nothing opened {prefix}: {opened:?}");
    }
    let txs = opened.iter().filter(|r| r.starts_with("/profile/transaction/")).count();
    assert_eq!(txs, chain().len(), "every transaction of the chain was reached through a hint");
    assert!(values > opened.len(), "{values} values, {} distinct reports", opened.len());
}
