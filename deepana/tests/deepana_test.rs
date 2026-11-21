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
use prism::types::udm::{DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
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

/// Write `slots` with PRISM's writer: each slot has one transaction and one
/// token transfer, of mint `mint{slot % 3}`.
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
        writer.append(&row, &[tx], &[], &[transfer]).unwrap();
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

    // The stream: a snapshot on connect holding the existing change log,
    // then a status poll with the newest slot, then new change log lines.
    let first = read_sse_until(addr, Duration::from_secs(20), |s| {
        s.contains("event: snapshot") && s.contains("event: status") && s.contains("\"newest_slot\":1029")
    })
    .await;
    assert!(first.contains("\"reason\":\"validator_skipped\""), "the snapshot carries the change log so far");

    let reader = tokio::spawn(read_sse_until(addr, Duration::from_secs(20), |s| s.contains("event: changelog") && s.contains("\"slot\":4242")));
    tokio::time::sleep(Duration::from_millis(500)).await;
    append_changelog(layout.root(), &[skip_event(2, 4242)]);
    let seen = reader.await.unwrap();
    assert!(seen.contains("\"skip_events\":2"), "{seen}");

    // COMMONS: verify the sample repository on demand.
    let (status, report) = json_of(addr, "POST", "/api/commons/verify", Some(&json!({}))).await;
    assert_eq!(status, 200, "{report}");
    assert_eq!(report["ok"], true, "{report}");
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
    assert!(report["error"].as_str().unwrap().contains("no COMMONS repository"), "{report}");
    // Only read: nothing created.
    assert!(!dir.path().join("commons").exists());
}
