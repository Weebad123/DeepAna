//! DeepAna end to end: every service on one port, under one directory.
//!
//! PRISM is not started (it needs Helius). Instead the test plays PRISM's
//! part: it writes Parquet with PRISM's own writer and appends change log
//! lines in PRISM's format, exactly where PRISM would.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use deepana::app;
use deepana::commons_panel::CommonsPanel;
use deepana::config::{self, ServiceConfigs};
use deepana::layout::Layout;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn no_env(_: &str) -> Option<OsString> {
    None
}

/// Write `slots` with PRISM's writer: each slot has one transaction, one
/// instruction, of program `prog{slot % 2}`, and one token transfer, of mint
/// `mint{slot % 3}`.
fn write_parquet(dir: &Path, slots: std::ops::Range<u64>) {
    let mut writer = ParquetWriter::new(dir);
    for slot in slots {
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_793_102_400 + slot as i64),
            block_height: Some(slot),
            transaction_count: 1,
            leader: Some("leader".to_owned()),
        };
        let tx = DeepAnaTransaction {
            slot,
            blockhash: format!("hash{slot}"),
            transaction_index: 0,
            tx_signature: format!("sig{slot}"),
            fee: 5_000,
            status: TransactionStatus::Success,
            err: None,
            accounts: vec!["payer".to_owned()],
            log_messages: vec![],
            compute_units_consumed: Some(1_000),
        };
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: format!("sig{slot}"),
            instruction_index: 0,
            inner_instruction_index: None,
            spl_token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            source_token_account: format!("src{slot}"),
            destination_token_account: format!("dst{slot}"),
            source_owner: None,
            destination_owner: None,
            mint_token_address: Some(format!("mint{}", slot % 3)),
            amount_raw: 10,
            decimals: Some(6),
            block_time: Some(1_793_102_400 + slot as i64),
        };
        let instruction = DeepAnaInstruction {
            slot,
            tx_signature: format!("sig{slot}"),
            instruction_index: 0,
            inner_instruction_index: None,
            program_id: format!("prog{}", slot % 2),
            accounts: vec!["payer".to_owned()],
            data_base58: String::new(),
            instruction_type: None,
            decoded_data: None,
        };
        writer.append(&row, &[tx], &[instruction], &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const COMPUTE_BUDGET: &str = "ComputeBudget111111111111111111111111111111";

/// A 32-byte address.
fn address(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

/// The 64-byte signature of the one transaction in `slot`.
fn signature_of(slot: u64) -> String {
    let mut bytes = [9u8; 64];
    bytes[..8].copy_from_slice(&slot.to_be_bytes());
    bytes[0] = 1;
    bs58::encode(bytes).into_string()
}

/// Like [`write_parquet`], with what a real chain has: base58 addresses and
/// signatures, logs, a compute budget instruction PRISM leaves undecoded
/// and an SPL transfer it decodes. Mint `address(1 + slot % 3)`; every
/// fifth slot's transaction fails.
fn write_real_parquet(dir: &Path, slots: std::ops::Range<u64>) {
    let mut writer = ParquetWriter::new(dir);
    for slot in slots {
        let sig = signature_of(slot);
        let failed = slot % 5 == 0;
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_793_102_400 + slot as i64),
            block_height: Some(slot),
            transaction_count: 1,
            leader: Some(address(50)),
        };
        let mut logs = vec![
            format!("Program {COMPUTE_BUDGET} invoke [1]"),
            format!("Program {COMPUTE_BUDGET} success"),
            format!("Program {TOKEN_PROGRAM} invoke [1]"),
            "Program log: Instruction: Transfer".to_owned(),
        ];
        logs.push(if failed {
            format!("Program {TOKEN_PROGRAM} failed: custom program error: 0x1")
        } else {
            format!("Program {TOKEN_PROGRAM} success")
        });
        let tx = DeepAnaTransaction {
            slot,
            blockhash: format!("hash{slot}"),
            transaction_index: 0,
            tx_signature: sig.clone(),
            fee: 5_000,
            status: if failed { TransactionStatus::Failed } else { TransactionStatus::Success },
            err: failed.then(|| r#"{"InstructionError":[1,{"Custom":1}]}"#.to_owned()),
            accounts: vec![address(30), address(31), TOKEN_PROGRAM.to_owned()],
            log_messages: logs,
            compute_units_consumed: Some(1_000),
        };
        let budget = DeepAnaInstruction {
            slot,
            tx_signature: sig.clone(),
            instruction_index: 0,
            inner_instruction_index: None,
            program_id: COMPUTE_BUDGET.to_owned(),
            accounts: vec![],
            data_base58: bs58::encode([[2u8].as_slice(), &200_000u32.to_le_bytes()].concat()).into_string(),
            instruction_type: None,
            decoded_data: None,
        };
        let transfer_ix = DeepAnaInstruction {
            instruction_index: 1,
            program_id: TOKEN_PROGRAM.to_owned(),
            accounts: vec![address(10), address(11), address(30)],
            data_base58: bs58::encode([3u8, 10, 0, 0, 0, 0, 0, 0, 0]).into_string(),
            instruction_type: Some("Transfer".to_owned()),
            decoded_data: Some(json!({ "amount": 10 })),
            ..budget.clone()
        };
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: sig,
            instruction_index: 1,
            inner_instruction_index: None,
            spl_token_program_id: TOKEN_PROGRAM.to_owned(),
            source_token_account: address(10),
            destination_token_account: address(11),
            source_owner: Some(address(30)),
            destination_owner: Some(address(31)),
            mint_token_address: Some(address(1 + (slot % 3) as u8)),
            amount_raw: 10,
            decimals: Some(6),
            block_time: Some(1_793_102_400 + slot as i64),
        };
        writer.append(&row, &[tx], &[budget, transfer_ix], &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

/// Append change log lines in PRISM's envelope format.
fn append_changelog(dir: &Path, lines: &[Value]) {
    let mut file = fs::OpenOptions::new().create(true).append(true).open(dir.join("changelog.jsonl")).unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
}

fn skip_event(id: u64, slot: u64) -> Value {
    json!({ "id": id, "emitted_at": 1_793_102_400u64 + id, "chain": "solana-devnet", "event": "skip", "slot": slot, "reason": "validator_skipped" })
}

/// Everything under `dir`, relative, sorted.
fn tree(dir: &Path) -> BTreeSet<String> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            out.insert(path.strip_prefix(root).unwrap().display().to_string());
            if path.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

struct Running {
    addr: SocketAddr,
    root: CancellationToken,
    task: tokio::task::JoinHandle<Result<(), app::RunError>>,
}

/// Bind port 0, build every config for that address, and run DeepAna
/// without PRISM.
async fn start(layout: &Layout, commons_root: &Path) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut configs = ServiceConfigs::build(layout, addr, no_env).unwrap();
    configs.lens.run_prism = false;
    let root = CancellationToken::new();
    let task = tokio::spawn(app::serve_on(
        listener,
        layout.clone(),
        configs,
        CommonsPanel::new(commons_root.to_path_buf()),
        false,
        root.clone(),
    ));
    Running { addr, root, task }
}

/// A plain HTTP/1.1 request; returns the status and the body.
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = body.map(Value::to_string).unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(60), stream.read_to_end(&mut raw)).await.unwrap().unwrap();
    let raw = String::from_utf8_lossy(&raw).into_owned();
    let status = raw.split(' ').nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
    let (head, body) = raw.split_once("\r\n\r\n").unwrap_or((&raw, ""));
    let body = if head.to_ascii_lowercase().contains("transfer-encoding: chunked") { dechunk(body) } else { body.to_owned() };
    (status, body)
}

fn dechunk(mut body: &str) -> String {
    let mut out = String::new();
    while let Some((size, rest)) = body.split_once("\r\n") {
        let Ok(size) = usize::from_str_radix(size.trim(), 16) else { break };
        if size == 0 || rest.len() < size {
            break;
        }
        out.push_str(&rest[..size]);
        body = rest[size..].trim_start_matches("\r\n");
    }
    out
}

async fn json_of(addr: SocketAddr, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
    let (status, body) = http(addr, method, path, body).await;
    (status, serde_json::from_str(&body).unwrap_or(Value::Null))
}

/// Read SSE from `/api/stream` until `done` says the text so far is enough.
async fn read_sse_until(addr: SocketAddr, within: Duration, done: impl Fn(&str) -> bool) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!("GET /api/stream HTTP/1.1\r\nHost: {addr}\r\nAccept: text/event-stream\r\n\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut seen = String::new();
    let deadline = tokio::time::Instant::now() + within;
    let mut buf = [0u8; 16 * 1024];
    while !done(&seen) {
        let n = tokio::time::timeout_at(deadline, stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("SSE never satisfied the condition; got:\n{seen}"))
            .unwrap();
        assert!(n > 0, "SSE stream closed early; got:\n{seen}");
        seen.push_str(&String::from_utf8_lossy(&buf[..n]));
    }
    seen
}

// ---------------------------------------------------------------------------
// Config and layout
// ---------------------------------------------------------------------------

#[test]
fn the_layout_matches_prisms_own_file_names() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    let root = layout.root().to_path_buf();
    assert_eq!(layout.chain_root(), root.join(prism::storage::partition::CHAIN_DIR));
    assert_eq!(layout.cursor(), root.join(prism::ingestor::CURSOR_FILE_NAME));
    assert_eq!(layout.skip_log(), root.join(prism::ingestor::SKIP_LOG_FILE_NAME));
    assert_eq!(layout.changelog(), root.join(prism::changelog::local::CHANGELOG_FILE_NAME));
    let prism = prism::ingestor::IngestorConfig::new(&root);
    assert_eq!(prism.cursor_path, layout.cursor());
    assert_eq!(prism.skip_log_path, layout.skip_log());
}

#[test]
fn every_service_path_is_under_the_data_directory() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    let listen: SocketAddr = "127.0.0.1:8099".parse().unwrap();
    // The environment tries to move every file elsewhere; it must lose.
    let env = |name: &str| -> Option<OsString> {
        match name {
            "LENS_DATA_DIR" | "FORGE_DB_PATH" | "ORACLE_DB_PATH" | "FORGE_CHANGELOG_PATH" => Some("/tmp/elsewhere".into()),
            "FORGE_LENS_URL" | "ORACLE_LENS_URL" => Some("http://10.0.0.1:1".into()),
            "LENS_BIND_ADDRESS" => Some("0.0.0.0:1".into()),
            // Tuning still comes from the environment.
            "FORGE_POLL_INTERVAL_MS" => Some("250".into()),
            "ORACLE_MAX_ROWS" => Some("77".into()),
            _ => None,
        }
    };
    let configs = ServiceConfigs::build(&layout, listen, env).unwrap();
    let root = layout.root();
    assert_eq!(configs.lens.config.data_dir(), root);
    assert!(configs.lens.run_prism);
    assert_eq!(configs.forge.changelog_path(), root.join("changelog.jsonl"));
    assert_eq!(configs.forge.db_path(), root.join("forge.db"));
    assert_eq!(configs.oracle.db_path(), root.join("oracle.db"));
    assert_eq!(configs.forge.lens_url(), "http://127.0.0.1:8099/api/lens");
    assert_eq!(configs.oracle.lens_url(), "http://127.0.0.1:8099/api/lens");
    assert_eq!(configs.forge.poll_interval_ms(), 250);
    assert_eq!(configs.oracle.max_rows(), 77);
    assert_ne!(configs.forge.socket_addr().port(), listen.port(), "FORGE's unused bind address must not clash");
}

#[test]
fn the_port_comes_from_deepana_port_or_defaults_to_8099() {
    assert_eq!(config::listen_address(no_env).unwrap(), "127.0.0.1:8099".parse().unwrap());
    let env = |name: &str| (name == config::ENV_PORT).then(|| OsString::from("9123"));
    assert_eq!(config::listen_address(env).unwrap(), "127.0.0.1:9123".parse().unwrap());
    for bad in ["", " ", "0", "65536", "http", "-1"] {
        let env = |name: &str| (name == config::ENV_PORT).then(|| OsString::from(bad));
        assert!(config::listen_address(env).is_err(), "{bad:?}");
    }
}

#[test]
fn a_relative_data_directory_is_made_absolute() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("a/b");
    let layout = Layout::create(&nested).unwrap();
    assert!(layout.root().is_absolute());
    assert!(layout.forge_db().starts_with(layout.root()));
}

// ---------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_port_serves_every_service_the_dashboard_and_the_stream() {
    let cwd_before = tree(&std::env::current_dir().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    write_parquet(layout.root(), 1_000..1_030);
    append_changelog(layout.root(), &[skip_event(1, 999)]);
    let commons_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../commons/tests/data/verify");
    let running = start(&layout, &commons_root).await;
    let addr = running.addr;

    // The dashboard.
    let (status, page) = http(addr, "GET", "/", None).await;
    assert_eq!(status, 200);
    assert!(page.contains("<title>DeepAna</title>"));

    // Each service through its prefix.
    let (status, lens) = json_of(addr, "GET", "/api/lens/health", None).await;
    assert_eq!(status, 200, "{lens}");
    assert_eq!(lens["status"], "healthy", "{lens}");
    let (status, forge) = json_of(addr, "GET", "/api/forge/health", None).await;
    assert_eq!(status, 200, "{forge}");
    // FORGE reaches LENS through this same port.
    assert_eq!(forge["lens"]["reachable"], true, "{forge}");
    let (status, oracle) = json_of(addr, "GET", "/api/oracle/health", None).await;
    assert_eq!(status, 200, "{oracle}");
    assert_eq!(oracle["lens"]["reachable"], true, "{oracle}");
    assert_eq!(oracle["lens"]["max_slot"], 1_029, "{oracle}");

    // Top 10 mints, straight from LENS.
    let sql = json!({ "sql": "SELECT mint_token_address AS mint, COUNT(*) AS transfers FROM solana.token_transfers GROUP BY mint_token_address ORDER BY transfers DESC, mint LIMIT 10" });
    let (status, top) = json_of(addr, "POST", "/api/lens/query", Some(&sql)).await;
    assert_eq!(status, 200, "{top}");
    assert_eq!(top["rows"].as_array().unwrap().len(), 3);
    assert_eq!(top["rows"][0], json!({ "mint": "mint0", "transfers": 10 }));

    // ...and through ORACLE's question box.
    let ask = json!({ "question": "top 10 mints by transfers in the last hour" });
    let (status, answer) = json_of(addr, "POST", "/api/oracle/ask", Some(&ask)).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["outcome"], "matched", "{answer}");
    let rows = answer["answer"]["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3, "{answer}");
    // The dashboard renders a matched answer as a table of these columns.
    let ask = json!({ "question": "busiest programs in the last 1000 slots" });
    let (status, answer) = json_of(addr, "POST", "/api/oracle/ask", Some(&ask)).await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["outcome"], "matched", "{answer}");
    let columns: Vec<&str> = answer["answer"]["columns"].as_array().unwrap().iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert!(columns.contains(&"program"), "{answer}");
    assert_eq!(answer["answer"]["rows"].as_array().unwrap().len(), 2, "{answer}");

    // The stream: a snapshot on connect holding the existing change log,
    // then a status poll with the newest slot, then new change log lines.
    let first = read_sse_until(addr, Duration::from_secs(20), |s| {
        s.contains("event: snapshot") && s.contains("event: status") && s.contains("\"newest_slot\":1029")
    })
    .await;
    assert!(first.contains("\"reason\":\"validator_skipped\""), "the snapshot carries the change log so far");
    assert!(first.contains("\"ms_per_slot\":400"), "the snapshot carries the dashboard settings");
    assert!(first.contains("\"tick_error\""), "the status carries ORACLE's /detectors");

    let reader = tokio::spawn(read_sse_until(addr, Duration::from_secs(20), |s| s.contains("event: changelog") && s.contains("\"slot\":4242")));
    tokio::time::sleep(Duration::from_millis(500)).await;
    append_changelog(layout.root(), &[skip_event(2, 4242)]);
    let seen = reader.await.unwrap();
    assert!(seen.contains("\"skip_events\":2"), "{seen}");

    // The FORGE demo: created once, adopted after; the secret never leaves.
    let (status, created) = http(addr, "POST", "/api/demo/alert", Some(&json!({}))).await;
    assert_eq!(status, 201, "{created}");
    assert!(!created.contains("secret"), "{created}");
    let created: Value = serde_json::from_str(&created).unwrap();
    assert_eq!(created["alert"]["name"], "deepana-demo");
    assert_eq!(created["alert"]["webhook_url"], format!("http://{addr}/demo/hook"));
    let (status, again) = http(addr, "POST", "/api/demo/alert", Some(&json!({}))).await;
    assert_eq!(status, 200, "{again}");
    assert!(!again.contains("secret"), "{again}");
    // An unsigned delivery is refused, and shown on the stream as refused.
    let reader = tokio::spawn(read_sse_until(addr, Duration::from_secs(20), |s| s.contains("event: demo_delivery")));
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (status, _) = json_of(addr, "POST", "/demo/hook", Some(&json!({ "rows": [] }))).await;
    assert_eq!(status, 401);
    let seen = reader.await.unwrap();
    assert!(seen.contains("\"verified\":false"), "{seen}");

    // COMMONS: verify the sample repository on demand.
    let (status, report) = json_of(addr, "POST", "/api/commons/verify", Some(&json!({}))).await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["ok"], true, "{report}");
    assert_eq!(report["configured"], true, "{report}");
    assert!(report["report"]["artifacts"].as_array().is_some_and(|a| !a.is_empty()), "{report}");
    let (status, _) = json_of(addr, "GET", "/api/commons/report", None).await;
    assert_eq!(status, 200);

    // Shut down cleanly, with a stream still open.
    let open_stream = tokio::spawn(read_sse_until(addr, Duration::from_secs(30), |_| false));
    tokio::time::sleep(Duration::from_millis(200)).await;
    running.root.cancel();
    tokio::time::timeout(Duration::from_secs(30), running.task).await.unwrap().unwrap().unwrap();
    open_stream.abort();

    // Only the documented files, all under <DIR>.
    let allowed: BTreeSet<&str> = [
        "solana", "changelog.jsonl",
        "forge.db", "forge.db.lock", "forge.db-wal", "forge.db-shm", "forge.checkpoint.json",
        "oracle.db", "oracle.db.lock", "oracle.db-wal", "oracle.db-shm",
    ]
    .into_iter()
    .collect();
    for entry in tree(layout.root()) {
        let top = entry.split('/').next().unwrap();
        assert!(allowed.contains(top), "unexpected {entry} in <DIR>");
        if top == "solana" && entry != "solana" {
            let table = entry.split('/').nth(1).unwrap();
            assert!(lens::config::TABLE_DIRS.contains(&table), "unexpected {entry} in solana/");
            assert!(!entry.ends_with(".tmp"), "leftover temp file {entry}");
        }
    }
    assert!(layout.forge_db().is_file());
    assert!(layout.oracle_db().is_file());
    // Nothing new in the working directory.
    assert_eq!(tree(&std::env::current_dir().unwrap()), cwd_before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_service_that_cannot_start_stops_the_ones_already_running() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    write_parquet(layout.root(), 10..12);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut configs = ServiceConfigs::build(&layout, addr, no_env).unwrap();
    configs.lens.run_prism = false;
    // Another FORGE already holds the database.
    let _held = forge::Registry::open(&configs.forge).unwrap();

    let root = CancellationToken::new();
    let result = tokio::time::timeout(
        Duration::from_secs(20),
        app::serve_on(listener, layout.clone(), configs, CommonsPanel::new(layout.commons_root()), false, root.clone()),
    )
    .await
    .unwrap();
    let err = result.unwrap_err();
    assert!(err.failures[0].starts_with("FORGE could not start"), "{err}");
    assert!(root.is_cancelled(), "LENS is told to stop");
    // ORACLE never started, so never created its database.
    assert!(!layout.oracle_db().exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_a_commons_repository_the_report_says_so() {
    let dir = tempfile::tempdir().unwrap();
    let panel = CommonsPanel::new(dir.path().join("commons"));
    let report = panel.verify().await;
    assert_eq!(report["ok"], false);
    assert_eq!(report["configured"], false, "not configured is not a verification failure");
    assert!(report["error"].as_str().unwrap().contains("no COMMONS repository"), "{report}");
    // Only read: nothing created.
    assert!(!dir.path().join("commons").exists());
}

// ---------------------------------------------------------------------------
// Drill-down: from an answer on the dashboard to a transaction
// ---------------------------------------------------------------------------

/// The path the dashboard walks when a row is clicked, over DeepAna's one
/// port: an answer row, the rows behind it (`POST /api/oracle/drill`), and
/// each of those as a transaction report. Every row of every answer, and
/// every row behind it, must open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_row_the_dashboard_shows_drills_through_to_its_details() {
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    write_real_parquet(layout.root(), 2_000..2_030);
    let commons_root = dir.path().join("no-commons");
    let running = start(&layout, &commons_root).await;
    let addr = running.addr;

    // The page ships the drawer and wires every kind of link to ORACLE's routes.
    let (status, page) = http(addr, "GET", "/", None).await;
    assert_eq!(status, 200);
    for needle in [
        "id=\"detail\"",
        "id=\"detail-back\"",
        "/* <link-rules>",
        "answerTable(d.spec, a",
        "sqlTable(data.rows",
        "/profile/transaction/",
        "/profile/slot/",
        "path: \"/drill\"",
        "route(false);",
    ] {
        assert!(page.contains(needle), "the dashboard lost {needle}");
    }

    let mut transactions_opened = 0;
    let mut slots_opened = 0;
    for question in [
        "top 10 mints by transfers in the last hour",
        "busiest programs in the last 1000 slots",
        "transactions by status",
        "top 5 leaders by slots in the last 2 hours",
        "how many failed transactions in the last hour",
    ] {
        let (status, asked) = json_of(addr, "POST", "/api/oracle/ask", Some(&json!({ "question": question }))).await;
        assert_eq!(status, 200, "{asked}");
        assert_eq!(asked["outcome"], "matched", "{question}: {asked}");
        let answer = &asked["answer"];
        let columns = answer["columns"].as_array().unwrap();
        let rows = answer["rows"].as_array().unwrap();
        assert!(!rows.is_empty(), "{question}");

        // What `answerTable` builds for a row's "details" link.
        let mut spec = asked["spec"].clone();
        spec["as_of_slot"] = answer["as_of_slot"].clone();
        for row in rows {
            let values: serde_json::Map<String, Value> = columns
                .iter()
                .zip(row.as_array().unwrap())
                .filter(|(c, _)| c["role"] == "dimension")
                .map(|(c, v)| (c["name"].as_str().unwrap().to_owned(), v.clone()))
                .collect();
            let (status, drill) =
                json_of(addr, "POST", "/api/oracle/drill", Some(&json!({ "spec": spec, "row": values, "limit": 50 }))).await;
            assert_eq!(status, 200, "{question}: {drill}");
            let cols: Vec<&str> =
                drill["columns"].as_array().unwrap().iter().map(|c| c["name"].as_str().unwrap()).collect();
            let at = |name: &str| cols.iter().position(|c| *c == name);
            let behind = drill["rows"].as_array().unwrap();
            assert!(!behind.is_empty(), "{question}: row {row} has nothing behind it");

            // What a click on each of those rows opens.
            for b in behind {
                let slot = b[at("slot").unwrap()].as_u64().unwrap();
                if drill["detail"] == "slot" {
                    let (status, report) = json_of(addr, "GET", &format!("/api/oracle/profile/slot/{slot}"), None).await;
                    assert_eq!((status, &report["found"]), (200, &json!(true)), "{report}");
                    assert_eq!(report["transactions"][0]["signature"], signature_of(slot));
                    slots_opened += 1;
                    continue;
                }
                let sig = b[at("tx_signature").unwrap()].as_str().unwrap();
                assert_eq!(sig, signature_of(slot));
                let (status, report) =
                    json_of(addr, "GET", &format!("/api/oracle/profile/transaction/{sig}?slot={slot}"), None).await;
                assert_eq!((status, &report["found"]), (200, &json!(true)), "{report}");
                let tx = &report["transaction"];
                assert_eq!(tx["status"], if slot % 5 == 0 { "failed" } else { "success" });
                assert_eq!(tx["instructions"].as_array().unwrap().len(), 2);
                assert_eq!(tx["instructions"][0]["instruction_type"], "SetComputeUnitLimit");
                assert_eq!(tx["token_transfers"][0]["mint"], address(1 + (slot % 3) as u8));
                transactions_opened += 1;
            }
        }

        // Mints and programs in an answer open as entity profiles too.
        for (column, row) in columns.iter().enumerate().flat_map(|(j, c)| rows.iter().map(move |r| (c, &r[j]))) {
            let kind = match column["name"].as_str().unwrap() {
                "mint" => "mint",
                "program" => "program",
                _ => continue,
            };
            let (status, profile) =
                json_of(addr, "GET", &format!("/api/oracle/profile/{kind}/{}", row.as_str().unwrap()), None).await;
            assert_eq!((status, &profile["found"]), (200, &json!(true)), "{profile}");
        }
    }
    // 30 transfers + 60 instructions + 30 transactions + 6 failed, and 30 slots.
    assert_eq!(transactions_opened, 30 + 60 + 30 + 6);
    assert_eq!(slots_opened, 30);

    // The SQL tab: a result with `slot` and `tx_signature` columns links the same way.
    let sql = json!({ "sql": "SELECT slot, tx_signature, instruction_index FROM solana.instructions ORDER BY slot DESC LIMIT 5" });
    let (status, result) = json_of(addr, "POST", "/api/lens/query", Some(&sql)).await;
    assert_eq!(status, 200, "{result}");
    for row in result["rows"].as_array().unwrap() {
        let uri = format!("/api/oracle/profile/transaction/{}?slot={}", row["tx_signature"].as_str().unwrap(), row["slot"]);
        let (status, report) = json_of(addr, "GET", &uri, None).await;
        assert_eq!((status, &report["found"]), (200, &json!(true)), "{report}");
    }

    running.root.cancel();
    tokio::time::timeout(Duration::from_secs(30), running.task).await.unwrap().unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Click-through: every link on every table is present, correct, and lands
// ---------------------------------------------------------------------------
//
// The page's link rules live in one pure block of the page itself
// (`/* <link-rules> */`). These tests take that block out of the very page
// DeepAna serves, run it under Node over real answers from a running
// DeepAna, and check every cell of every table it renders:
//
// * **present**: a cell whose column says what its values are (ORACLE's
//   `entity` hint, or a PRISM column name for raw SQL) and whose value is
//   one (a signature, an address, a slot) is a link; nothing else is;
// * **correct**: the link's target is the one worked out here, in Rust,
//   independently of the page; its text is the value;
// * **lands**: the page's own router turns the target into an ORACLE
//   request, DeepAna answers it, and the report is about that very value.
//
// They need `node` on PATH (the page is JavaScript). Without it they say so
// and pass; `DEEPANA_REQUIRE_NODE=1` makes a missing `node` a failure.

/// Whether `program` runs; if not, either skip (returning false) or fail.
fn have(program: &str, arg: &str, require_env: &str) -> bool {
    let ok = std::process::Command::new(program).arg(arg).output().is_ok_and(|o| o.status.success());
    if !ok {
        assert!(std::env::var_os(require_env).is_none(), "{program} is required ({require_env} is set) but does not run");
        eprintln!("SKIPPED: `{program}` is not available; set {require_env}=1 to make this a failure");
    }
    ok
}

/// The page's link rules, run under Node. One process per batch.
struct PageRules {
    dir: tempfile::TempDir,
}

const RULES_HARNESS: &str = r#"
const fs = require("fs");
const html = fs.readFileSync(process.argv[2], "utf8");
const from = html.indexOf("/* <link-rules>"), to = html.indexOf("/* </link-rules>");
if (from < 0 || to < 0) throw new Error("the page has no link-rules block");
const page = new Function(html.slice(from, to) +
  "; return { linkify, hrefFor, parseRoute, answerTable, rowsTable, sqlTable, rowsHref };")();
const calls = JSON.parse(fs.readFileSync(0, "utf8"));
process.stdout.write(JSON.stringify(calls.map(([fn, ...args]) => page[fn](...args) ?? null)));
"#;

impl PageRules {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("page.html"), app::DASHBOARD_HTML).unwrap();
        fs::write(dir.path().join("harness.js"), RULES_HARNESS).unwrap();
        Self { dir }
    }

    /// Call each `[function, args...]`; their results, in order.
    fn call(&self, calls: &[Value]) -> Vec<Value> {
        use std::process::{Command, Stdio};
        let mut child = Command::new("node")
            .arg(self.dir.path().join("harness.js"))
            .arg(self.dir.path().join("page.html"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(Value::from(calls.to_vec()).to_string().as_bytes()).unwrap();
        let out = child.wait_with_output().unwrap();
        assert!(out.status.success(), "node: {}", String::from_utf8_lossy(&out.stderr));
        serde_json::from_slice(&out.stdout).unwrap()
    }

    fn one(&self, call: Value) -> Value {
        self.call(&[call]).remove(0)
    }
}

fn html_unescape(s: &str) -> String {
    s.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", "\"").replace("&#39;", "'").replace("&amp;", "&")
}

/// One `<a>` the page rendered.
#[derive(Debug, Clone, PartialEq)]
struct Link {
    href: String,
    entity: String,
    text: String,
}

fn links_in(html: &str) -> Vec<Link> {
    let attr = |tag: &str, name: &str| {
        let key = format!("{name}=\"");
        tag.find(&key).map(|at| html_unescape(&tag[at + key.len()..][..tag[at + key.len()..].find('"').unwrap()]))
    };
    html.split("<a ")
        .skip(1)
        .map(|rest| {
            let (tag, after) = rest.split_once('>').unwrap();
            let text = html_unescape(&after[..after.find("</a>").expect("an unclosed <a>")]);
            Link {
                href: attr(tag, "href").expect("a link without an href"),
                entity: attr(tag, "data-entity").expect("a link without data-entity"),
                text,
            }
        })
        .collect()
}

/// The cells of each body row of a rendered table, as HTML.
fn table_cells(html: &str) -> Vec<Vec<String>> {
    let Some((_, body)) = html.split_once("<tbody>") else { return Vec::new() };
    let body = &body[..body.find("</tbody>").unwrap()];
    body.split("<tr>")
        .skip(1)
        .map(|row| {
            row.split("<td")
                .skip(1)
                .map(|cell| {
                    let inner = &cell[cell.find('>').unwrap() + 1..];
                    inner[..inner.rfind("</td>").unwrap()].to_owned()
                })
                .collect()
        })
        .collect()
}

fn decodes_to(value: &str, bytes: usize) -> bool {
    bs58::decode(value).into_vec().is_ok_and(|b| b.len() == bytes)
}

/// Where a value of a column with `hint` must link, worked out here and
/// not by the page: `None` if it must not be a link.
fn expected_href(hint: Option<&str>, value: &Value, row_slot: Option<u64>, focus: Option<&str>) -> Option<String> {
    match (hint?, value) {
        ("slot", v) => v.as_u64().map(|n| format!("#/slot/{n}")),
        ("tx_signature", Value::String(s)) if decodes_to(s, 64) => {
            let params: Vec<String> =
                [row_slot.map(|n| format!("slot={n}")), focus.map(|f| format!("ix={f}"))].into_iter().flatten().collect();
            Some(format!("#/tx/{s}{}", if params.is_empty() { String::new() } else { format!("?{}", params.join("&")) }))
        }
        ("mint", Value::String(s)) if decodes_to(s, 32) => Some(format!("#/mint/{s}")),
        ("program", Value::String(s)) if decodes_to(s, 32) => Some(format!("#/program/{s}")),
        ("token_account", Value::String(s)) if decodes_to(s, 32) => Some(format!("#/account/{s}")),
        _ => None,
    }
}

/// What a link's text must be: the value, a number in the en-US grouping.
fn expected_text(value: &Value) -> String {
    match value {
        Value::Number(n) => {
            let digits = n.to_string();
            let mut out = String::new();
            for (i, c) in digits.chars().enumerate() {
                if i > 0 && (digits.len() - i) % 3 == 0 {
                    out.push(',');
                }
                out.push(c);
            }
            out
        }
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Check one rendered table cell by cell; returns its links.
#[track_caller]
fn check_cells(what: &str, html: &str, columns: &[(String, Option<String>)], rows: &[Vec<Value>], lead: usize) -> Vec<Link> {
    let cells = table_cells(html);
    assert_eq!(cells.len(), rows.len(), "{what}: rendered {} rows of {}", cells.len(), rows.len());
    let slot_at = columns.iter().position(|(_, h)| h.as_deref() == Some("slot"));
    let ix_at = columns.iter().position(|(n, _)| n == "instruction_index");
    let inner_at = columns.iter().position(|(n, _)| n == "inner_instruction_index");
    let mut links = Vec::new();
    for (row, cells) in rows.iter().zip(&cells) {
        assert_eq!(cells.len(), 1 + lead + columns.len(), "{what}: {cells:?}");
        let row_slot = slot_at.and_then(|i| row[i].as_u64());
        let focus = ix_at.map(|i| match inner_at.map(|j| &row[j]) {
            Some(Value::Number(inner)) => format!("{}.{inner}", row[i]),
            _ => row[i].to_string(),
        });
        for (j, (name, hint)) in columns.iter().enumerate() {
            let cell = &cells[1 + lead + j];
            let found = links_in(cell);
            match expected_href(hint.as_deref(), &row[j], row_slot, focus.as_deref()) {
                None => assert!(found.is_empty(), "{what}: column {name} = {} must not be a link: {cell}", row[j]),
                Some(href) => {
                    assert_eq!(found.len(), 1, "{what}: column {name} = {} must be one link: {cell}", row[j]);
                    let link = &found[0];
                    assert_eq!(link.href, href, "{what}: column {name}");
                    assert_eq!(link.entity, hint.clone().unwrap(), "{what}: column {name}");
                    assert_eq!(link.text, expected_text(&row[j]), "{what}: column {name}");
                    links.push(link.clone());
                }
            }
        }
        if lead == 1 {
            let details = links_in(&cells[1]);
            assert_eq!(details.len(), 1, "{what}: a row without its details link");
            assert!(details[0].href.starts_with("#/rows?q="), "{what}: {details:?}");
            assert_eq!((details[0].entity.as_str(), details[0].text.as_str()), ("rows", "details ›"));
            links.push(details[0].clone());
        }
    }
    links
}

/// The columns of an ORACLE answer or drill: name and hint.
fn hinted_columns(columns: &Value) -> Vec<(String, Option<String>)> {
    columns
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            let entity = c.as_object().unwrap().get("entity").expect("every column says what its values are");
            (c["name"].as_str().unwrap().to_owned(), entity.as_str().map(str::to_owned))
        })
        .collect()
}

/// What following links covered.
#[derive(Debug, Default)]
struct Walk {
    links: usize,
    landed: BTreeSet<String>,
    by_kind: std::collections::BTreeMap<String, usize>,
}

/// Follow `link` exactly as the page would: its router makes the request,
/// DeepAna answers, and the report must be about the linked value. A rows
/// link is rendered in turn and its own links checked and followed.
async fn land(addr: SocketAddr, rules: &PageRules, link: &Link, walk: &mut Walk, depth: u8) {
    walk.links += 1;
    *walk.by_kind.entry(link.entity.clone()).or_default() += 1;
    if !walk.landed.insert(link.href.clone()) {
        return;
    }
    let route = rules.one(json!(["parseRoute", link.href]));
    assert_ne!(route["view"], "invalid", "{link:?}: {route}");
    let api = &route["api"];
    let path = format!("/api/oracle{}", api["path"].as_str().unwrap());
    let (status, report) = match api.get("body") {
        Some(body) => json_of(addr, "POST", &path, Some(body)).await,
        None => json_of(addr, "GET", &path, None).await,
    };
    assert_eq!(status, 200, "{link:?} -> {path}: {report}");
    match route["view"].as_str().unwrap() {
        "tx" => {
            assert_eq!(report["found"], true, "{link:?}: {report}");
            assert_eq!(report["signature"], route["id"]);
            if let Some(slot) = route["slot"].as_u64() {
                assert_eq!(report["transaction"]["slot"], slot, "{link:?}");
            }
            if let Some(focus) = route["focus"].as_str() {
                let tree = report["transaction"]["instructions"].as_array().unwrap();
                let paths: Vec<&str> = tree
                    .iter()
                    .flat_map(|t| std::iter::once(t).chain(t["inner"].as_array().unwrap()))
                    .map(|i| i["path"].as_str().unwrap())
                    .collect();
                assert!(paths.contains(&focus), "{link:?}: instruction {focus} is not in {paths:?}");
            }
        }
        "profile" => {
            assert_eq!(report["found"], true, "{link:?}: {report}");
            assert_eq!((&report["kind"], &report["id"]), (&route["kind"], &route["id"]), "{link:?}");
        }
        "slot" => {
            assert_eq!(report["found"], true, "{link:?}: {report}");
            assert_eq!(report["slot"], route["slot"], "{link:?}");
            assert_eq!(report["transactions"][0]["signature"], signature_of(route["slot"].as_u64().unwrap()));
        }
        "rows" => {
            let rows: Vec<Vec<Value>> = serde_json::from_value(report["rows"].clone()).unwrap();
            assert!(!rows.is_empty(), "{link:?}: nothing behind the row");
            let html = rules.one(json!(["rowsTable", report]));
            let links = check_cells(&format!("rows behind {link:?}"), html.as_str().unwrap(), &hinted_columns(&report["columns"]), &rows, 0);
            assert!(links.iter().any(|l| l.entity == "tx_signature" || l.entity == "slot"), "{link:?}");
            if depth == 0 {
                for l in links {
                    Box::pin(land(addr, rules, &l, walk, depth + 1)).await;
                }
            }
        }
        other => panic!("{link:?}: view {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_link_on_every_table_is_present_correct_and_lands_on_its_data() {
    if !have("node", "--version", "DEEPANA_REQUIRE_NODE") {
        return;
    }
    let rules = PageRules::new();
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    write_real_parquet(layout.root(), 2_000..2_030);
    let running = start(&layout, &dir.path().join("no-commons")).await;
    let addr = running.addr;
    let mut walk = Walk::default();

    // Every example of every built-in template, as the Ask box shows it.
    let (_, templates) = json_of(addr, "GET", "/api/oracle/templates", None).await;
    let mut answers = 0;
    for example in templates["templates"].as_array().unwrap().iter().flat_map(|t| t["examples"].as_array().unwrap()) {
        let question = example.as_str().unwrap();
        let (status, asked) = json_of(addr, "POST", "/api/oracle/ask", Some(&json!({ "question": question }))).await;
        assert_eq!(status, 200, "{question}: {asked}");
        if asked["outcome"] != "matched" {
            continue;
        }
        let answer = &asked["answer"];
        let columns = hinted_columns(&answer["columns"]);
        // ORACLE says it, the page does not guess: the columns whose values
        // open are hinted, the rest (measures, owners, statuses) are not.
        for ((name, hint), c) in columns.iter().zip(answer["columns"].as_array().unwrap()) {
            let expected = match name.as_str() {
                "mint" => Some("mint"),
                "program" | "token_program" => Some("program"),
                "slot" => Some("slot"),
                _ => None,
            };
            assert_eq!(hint.as_deref(), expected, "{question}: column {name} ({c})");
        }
        let rows: Vec<Vec<Value>> = serde_json::from_value(answer["rows"].clone()).unwrap();
        let html = rules.one(json!(["answerTable", asked["spec"], answer, asked["understood_as"]]));
        let links = check_cells(question, html.as_str().unwrap(), &columns, &rows, 1);
        assert!(links.len() >= rows.len(), "{question}: fewer links than rows");
        for link in &links {
            land(addr, &rules, link, &mut walk, 0).await;
        }
        answers += 1;
    }
    assert!(answers >= 12, "only {answers} template examples matched over the chain");

    // Custom specs: every dimension that is an entity, alone.
    for (dimension, measure) in [("mint", "transfer_count"), ("program", "ix_count"), ("token_program", "transfer_count"), ("slot", "tx_count")] {
        let spec = json!({ "measures": [measure], "group_by": [{ "dimension": dimension }], "window": { "last_slots": 1000 } });
        let (status, answer) = json_of(addr, "POST", "/api/oracle/query", Some(&spec)).await;
        assert_eq!(status, 200, "{answer}");
        let rows: Vec<Vec<Value>> = serde_json::from_value(answer["rows"].clone()).unwrap();
        let html = rules.one(json!(["answerTable", spec, answer, dimension]));
        for link in check_cells(dimension, html.as_str().unwrap(), &hinted_columns(&answer["columns"]), &rows, 1) {
            land(addr, &rules, &link, &mut walk, 0).await;
        }
    }

    // The SQL tab: no metadata, so PRISM's column names decide.
    for sql in [
        "SELECT slot, tx_signature, status FROM solana.transactions ORDER BY slot DESC LIMIT 8",
        "SELECT slot, tx_signature, instruction_index, inner_instruction_index, program_id FROM solana.instructions ORDER BY slot DESC, instruction_index LIMIT 8",
        "SELECT slot, tx_signature, mint_token_address, source_token_account, destination_token_account, amount_raw FROM solana.token_transfers ORDER BY slot LIMIT 8",
        "SELECT slot, parent_slot, leader FROM solana.slots ORDER BY slot LIMIT 3",
    ] {
        let (status, result) = json_of(addr, "POST", "/api/lens/query", Some(&json!({ "sql": sql }))).await;
        assert_eq!(status, 200, "{result}");
        let objects = result["rows"].as_array().unwrap();
        let names: Vec<String> = objects[0].as_object().unwrap().keys().cloned().collect();
        let sql_hint = |n: &str| match n {
            "slot" | "parent_slot" => Some("slot"),
            "tx_signature" => Some("tx_signature"),
            "mint_token_address" => Some("mint"),
            "program_id" => Some("program"),
            "source_token_account" | "destination_token_account" => Some("token_account"),
            _ => None, // leader, status, amounts: nothing to open
        };
        let columns: Vec<(String, Option<String>)> = names.iter().map(|n| (n.clone(), sql_hint(n).map(str::to_owned))).collect();
        let rows: Vec<Vec<Value>> = objects.iter().map(|o| names.iter().map(|n| o[n].clone()).collect()).collect();
        let html = rules.one(json!(["sqlTable", objects]));
        for link in check_cells(sql, html.as_str().unwrap(), &columns, &rows, 0) {
            land(addr, &rules, &link, &mut walk, 0).await;
        }
    }

    // The measure: every kind of link appeared and landed, and every
    // transaction, slot, mint, program and token account of the chain was
    // reached from a table.
    for kind in ["tx_signature", "mint", "program", "token_account", "slot", "rows"] {
        assert!(walk.by_kind.get(kind).copied().unwrap_or(0) > 0, "no {kind} link was checked: {walk:?}");
    }
    let reached = |prefix: &str| walk.landed.iter().filter(|h| h.starts_with(prefix)).count();
    let txs: BTreeSet<String> =
        walk.landed.iter().filter_map(|h| h.strip_prefix("#/tx/")).map(|h| h.split('?').next().unwrap().to_owned()).collect();
    assert_eq!(txs, (2_000..2_030).map(signature_of).collect::<BTreeSet<_>>(), "every transaction is reachable");
    assert_eq!(reached("#/slot/"), 30, "every slot is reachable");
    assert_eq!(reached("#/mint/"), 3);
    assert_eq!(reached("#/program/"), 2, "compute budget and SPL Token");
    assert_eq!(reached("#/account/"), 2, "the source and destination token accounts");
    eprintln!("checked {} links, {} distinct targets landed: {:?}", walk.links, walk.landed.len(), walk.by_kind);

    running.root.cancel();
    tokio::time::timeout(Duration::from_secs(30), running.task).await.unwrap().unwrap().unwrap();
}

#[test]
fn the_link_rules_decide_links_by_hint_and_value_shape() {
    if !have("node", "--version", "DEEPANA_REQUIRE_NODE") {
        return;
    }
    let rules = PageRules::new();
    let sig = signature_of(2_000);
    let key = address(1);
    let link = |col: &str, hint: Value, value: Value, slot: Value| json!(["linkify", col, hint, value, slot]);
    let out = rules.call(&[
        // ORACLE's hint decides, whatever the column is called.
        link("anything", json!("tx_signature"), json!(sig), json!(2_000)),
        link("anything", json!("mint"), json!(key), Value::Null),
        link("anything", json!("program"), json!(key), Value::Null),
        link("anything", json!("token_account"), json!(key), Value::Null),
        link("anything", json!("slot"), json!(2_000), Value::Null),
        // A null hint is "nothing to open", even for a column named mint.
        link("mint", Value::Null, json!(key), Value::Null),
        // Values that are not what the hint says stay text.
        link("mint", json!("mint"), json!("unattributed"), Value::Null),
        link("mint", json!("mint"), Value::Null, Value::Null),
        link("tx_signature", json!("tx_signature"), json!(key), Value::Null),
        link("program", json!("program"), json!(sig), Value::Null),
        link("slot", json!("slot"), json!(-1), Value::Null),
        link("slot", json!("slot"), json!("12x"), Value::Null),
        link("x", json!("wallet"), json!(key), Value::Null),
        // Hostile text is escaped, never markup.
        link("note", Value::Null, json!("<img src=x onerror=alert(1)>"), Value::Null),
    ]);
    let hrefs: Vec<Option<String>> = out.iter().map(|h| links_in(h.as_str().unwrap()).first().map(|l| l.href.clone())).collect();
    assert_eq!(hrefs[0].as_deref(), Some(format!("#/tx/{sig}?slot=2000").as_str()));
    assert_eq!(hrefs[1].as_deref(), Some(format!("#/mint/{key}").as_str()));
    assert_eq!(hrefs[2].as_deref(), Some(format!("#/program/{key}").as_str()));
    assert_eq!(hrefs[3].as_deref(), Some(format!("#/account/{key}").as_str()));
    assert_eq!(hrefs[4].as_deref(), Some("#/slot/2000"));
    assert_eq!(links_in(out[4].as_str().unwrap())[0].text, "2,000");
    for (i, h) in hrefs.iter().enumerate().take(13).skip(5) {
        assert_eq!(h, &None, "case {i}: {}", out[i]);
    }
    assert_eq!(out[6], "unattributed");
    assert_eq!(out[7], "∅");
    // Raw SQL has no metadata: the SQL tab passes `undefined`, which JSON
    // cannot carry, so the name lookup is checked through sqlTable itself.
    let sql = rules.one(json!(["sqlTable", [{ "mint_token_address": key, "spl_token_program_id": key, "leader": key }]]));
    let links = links_in(sql.as_str().unwrap());
    assert_eq!(links.iter().map(|l| l.href.as_str()).collect::<Vec<_>>(), vec![format!("#/mint/{key}"), format!("#/program/{key}")]);
    assert!(!out[13].as_str().unwrap().contains('<'), "{}", out[13]);

    // Every route the page links to parses back to the right ORACLE request.
    let routes = rules.call(&[
        json!(["parseRoute", format!("#/tx/{sig}?slot=2000")]),
        json!(["parseRoute", format!("#/tx/{sig}?slot=2000&ix=1.0")]),
        json!(["parseRoute", format!("#/tx/{sig}")]),
        json!(["parseRoute", format!("#/mint/{key}")]),
        json!(["parseRoute", format!("#/program/{key}")]),
        json!(["parseRoute", format!("#/account/{key}")]),
        json!(["parseRoute", "#/slot/2000?after=4"]),
        json!(["parseRoute", ""]),
        json!(["parseRoute", "#/"]),
        json!(["parseRoute", "#/tx/not-a-signature"]),
        json!(["parseRoute", format!("#/tx/{sig}?slot=abc")]),
        json!(["parseRoute", "#/mint/abc"]),
        json!(["parseRoute", format!("#/wallet/{key}")]),
        json!(["parseRoute", format!("#/mint/{key}/extra")]),
        json!(["parseRoute", "#/rows?q=%%%"]),
    ]);
    let path = |i: usize| routes[i]["api"]["path"].as_str().unwrap().to_owned();
    assert_eq!(path(0), format!("/profile/transaction/{sig}?slot=2000"));
    assert_eq!((path(1), &routes[1]["focus"]), (format!("/profile/transaction/{sig}?slot=2000"), &json!("1.0")));
    assert_eq!(path(2), format!("/profile/transaction/{sig}"));
    assert_eq!(path(3), format!("/profile/mint/{key}"));
    assert_eq!(path(4), format!("/profile/program/{key}"));
    assert_eq!(path(5), format!("/profile/token_account/{key}"));
    assert_eq!(path(6), "/profile/slot/2000?limit=100&after_index=4");
    assert_eq!((&routes[7], &routes[8]), (&Value::Null, &Value::Null), "no hash is the results");
    for (i, r) in routes.iter().enumerate().skip(9) {
        assert_eq!(r["view"], "invalid", "case {i}: {r}");
        assert_eq!(r["api"], Value::Null);
    }

    // A rows link carries the spec and the row through the URL, unicode and all.
    let spec = json!({ "measures": ["transfer_count"], "group_by": [{ "dimension": "mint" }], "window": { "last_slots": 9 }, "as_of_slot": 7 });
    let href = rules.one(json!(["rowsHref", spec, { "mint": key }, "mint é ☀"]));
    let route = rules.one(json!(["parseRoute", href]));
    assert_eq!(route["api"], json!({ "path": "/drill", "body": { "spec": spec, "row": { "mint": key }, "limit": 50 } }));
    assert_eq!(route["label"], "mint é ☀");
}

/// The first Chromium-family browser that runs, or `CHROME` if set.
fn chromium() -> Option<String> {
    let candidates: Vec<String> = std::env::var("CHROME")
        .ok()
        .into_iter()
        .chain(["chromium", "chromium-browser", "google-chrome", "google-chrome-stable"].map(str::to_owned))
        .collect();
    candidates
        .into_iter()
        .find(|c| std::process::Command::new(c).arg("--version").output().is_ok_and(|o| o.status.success()))
}

/// Click-through in a real browser: links in answers, the panel in place of
/// the results, links inside it, browser Back and Forward, reload, a pasted
/// URL, the panel's Back, and the loading / not found / bad link / failure
/// states, with the detail requests counted. The checks are in
/// `tests/browser/navigate.mjs`, one line each in the output.
///
/// Needs `node` and Chromium. Without them it says so and passes;
/// `DEEPANA_REQUIRE_BROWSER=1` makes their absence a failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_dashboard_navigates_links_back_forward_reload_and_pasted_urls_in_a_browser() {
    let require = std::env::var_os("DEEPANA_REQUIRE_BROWSER").is_some();
    let browser = chromium();
    if browser.is_none() || !have("node", "--version", "DEEPANA_REQUIRE_BROWSER") {
        assert!(!require, "DEEPANA_REQUIRE_BROWSER is set but Chromium or node is missing");
        eprintln!("SKIPPED: needs node and Chromium (or CHROME=<path>)");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let layout = Layout::create(dir.path()).unwrap();
    write_real_parquet(layout.root(), 2_000..2_030);
    let running = start(&layout, &dir.path().join("no-commons")).await;

    let fixtures = json!({
        "mint": address(1),
        "account": address(10),
        "signature": signature_of(2_011),
        "failed_signature": signature_of(2_005),
        "unknown_signature": signature_of(9_999),
        "slot": 2_000,
    });
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/browser/navigate.mjs");
    let profile = dir.path().join("chromium-profile");
    let output = tokio::time::timeout(
        Duration::from_secs(180),
        tokio::process::Command::new("node")
            .arg(&script)
            .arg(format!("http://{}", running.addr))
            .arg(browser.unwrap())
            .arg(&profile)
            .arg(fixtures.to_string())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("the browser run did not finish within 180 s")
    .unwrap();
    let log = String::from_utf8_lossy(&output.stdout);
    eprintln!("{log}{}", String::from_utf8_lossy(&output.stderr));
    let checks = log.lines().filter(|l| l.starts_with("ok  ")).count();
    assert!(output.status.success() && !log.contains("FAIL"), "browser checks failed:\n{log}");
    assert!(checks >= 35, "only {checks} browser checks ran:\n{log}");

    running.root.cancel();
    tokio::time::timeout(Duration::from_secs(30), running.task).await.unwrap().unwrap().unwrap();
}
