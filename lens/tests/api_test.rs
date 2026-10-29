//! Tests for `lens::api`, end to end: PRISM's `ParquetWriter` writes the
//! tables, `QueryEngine::new` registers them, and requests go through the
//! axum router, in-process (`tower::ServiceExt::oneshot`) or over a real TCP
//! socket (`serve_on`).
//!
//! The `status_*` tests pin the HTTP status of every outcome a query can
//! have: success (200), bad SQL or bad request (400 / 415 / 422), timeout
//! (504) and server-side failure (500). `status_agrees_with_engine_outcome`
//! checks each status against what the engine itself recorded.

use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use lens::{AppState, EngineMetricsSnapshot, ErrorBody, LensConfig, MAX_SQL_BYTES, QueryEngine, router};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A scratch data directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-api-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The slots [`dataset`] writes: 10 slots, 20 transactions, 20 instructions,
/// 10 token transfers.
const SLOTS: std::ops::Range<u64> = 1_000..1_010;

/// A query that cannot finish within any test timeout.
const ENDLESS_SQL: &str = "SELECT SUM(value) FROM generate_series(1, 1000000000000)";

fn slot_row(slot: u64) -> DeepAnaSlot {
    DeepAnaSlot {
        slot,
        blockhash: format!("hash{slot}"),
        parent_slot: slot - 1,
        block_time: Some(1_793_102_400 + slot as i64),
        block_height: Some(slot - 100),
        transaction_count: 2,
        leader: Some(format!("leader{slot}")),
    }
}

/// Have PRISM write every table for `slots`.
fn prism_write(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        let transactions: Vec<DeepAnaTransaction> = (0..2u32)
            .map(|index| DeepAnaTransaction {
                slot,
                blockhash: format!("hash{slot}"),
                transaction_index: index,
                tx_signature: format!("sig{slot}_{index}"),
                fee: 5_000,
                status: TransactionStatus::Success,
                err: None,
                accounts: vec![format!("payer{slot}")],
                log_messages: vec![],
                compute_units_consumed: Some(1_000),
            })
            .collect();
        let instructions: Vec<DeepAnaInstruction> = transactions
            .iter()
            .map(|tx| DeepAnaInstruction {
                slot,
                tx_signature: tx.tx_signature.clone(),
                instruction_index: 0,
                inner_instruction_index: None,
                program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
                accounts: vec![],
                data_base58: "3Bxs4h24hBtQy9rw".to_owned(),
                instruction_type: None,
                decoded_data: None,
            })
            .collect();
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: transactions[0].tx_signature.clone(),
            instruction_index: 0,
            inner_instruction_index: None,
            spl_token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            source_token_account: format!("src{slot}"),
            destination_token_account: format!("dst{slot}"),
            source_owner: None,
            destination_owner: None,
            mint_token_address: None,
            amount_raw: slot * 10,
            decimals: Some(9),
            block_time: None,
        };
        writer.append(&slot_row(slot), &transactions, &instructions, &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

/// Have PRISM write only slot rows, so only the `slots` table exists.
fn prism_write_slots_only(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        writer.append(&slot_row(slot), &[], &[], &[]).unwrap();
    }
    writer.flush().unwrap();
}

fn dataset() -> TempDir {
    let dir = TempDir::new();
    prism_write(dir.path(), SLOTS);
    dir
}

fn config(data_dir: &Path, vars: &[(&str, &str)]) -> Arc<LensConfig> {
    let data_dir = data_dir.to_str().unwrap().to_owned();
    let config = LensConfig::load_with(|name| {
        if name == "LENS_DATA_DIR" {
            return Some(data_dir.clone().into());
        }
        vars.iter().find(|(key, _)| *key == name).map(|(_, value)| (*value).into())
    })
    .unwrap();
    Arc::new(config)
}

fn engine(data_dir: &Path, vars: &[(&str, &str)]) -> Arc<QueryEngine> {
    Arc::new(QueryEngine::new(config(data_dir, vars)).unwrap())
}

fn app(engine: &Arc<QueryEngine>) -> Router {
    router(AppState::new(Arc::clone(engine)))
}

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|err| panic!("body is not JSON ({err}): {}", String::from_utf8_lossy(&self.body)))
    }

    fn error(&self) -> ErrorBody {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|err| panic!("body is not an ErrorBody ({err}): {}", String::from_utf8_lossy(&self.body)))
    }

    fn text(&self) -> String {
        String::from_utf8(self.body.clone()).unwrap()
    }

    fn header(&self, name: &str) -> &str {
        self.headers.get(name).unwrap_or_else(|| panic!("no {name} header")).to_str().unwrap()
    }
}

async fn send(app: &Router, request: Request<Body>) -> Reply {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    Reply { status, headers, body }
}

async fn post_query(app: &Router, body: Value) -> Reply {
    let request = Request::post("/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    send(app, request).await
}

async fn sql(app: &Router, sql: &str) -> Reply {
    post_query(app, json!({ "sql": sql })).await
}

async fn get_health(app: &Router) -> Reply {
    send(app, Request::get("/health").body(Body::empty()).unwrap()).await
}

/// Assert `reply` failed with `status` and `kind`, echoing `sql`.
fn assert_error(reply: &Reply, status: StatusCode, kind: &str, sql: Option<&str>) -> ErrorBody {
    assert_eq!(reply.status, status, "body: {}", String::from_utf8_lossy(&reply.body));
    assert_eq!(reply.header("content-type"), "application/json");
    let error = reply.error();
    assert_eq!(error.error_kind, kind, "{error:?}");
    assert_eq!(error.sql.as_deref(), sql, "{error:?}");
    assert!(!error.error.is_empty());
    error
}

/// Write a file with a `.parquet` name that is not Parquet into the slots
/// table, as a crashed or foreign writer might.
fn corrupt_slots_table(data_dir: &Path) {
    let dir = data_dir.join("solana/slots/date=2099-01-01");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("slots_9_9.parquet"), b"this is not a parquet file, not even close to one").unwrap();
}

// ---------------------------------------------------------------------------
// Status: 200
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_200_for_a_json_query() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let reply = sql(&app(&engine), "SELECT slot, blockhash FROM solana.slots ORDER BY slot LIMIT 3").await;

    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    assert_eq!(reply.header("content-type"), "application/json");
    assert_eq!(reply.header("x-lens-row-count"), "3");
    assert_eq!(reply.header("x-lens-truncated"), "false");
    reply.header("x-lens-duration-ms").parse::<u64>().unwrap();

    let body = reply.json();
    assert_eq!(body["row_count"], 3);
    assert_eq!(body["truncated"], false);
    assert_eq!(body["rows"][0], json!({ "slot": 1000, "blockhash": "hash1000" }));
    assert_eq!(body["rows"][2]["slot"], 1002);
}

#[tokio::test]
async fn status_200_for_a_csv_query_in_any_case() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);
    for format in ["csv", "CSV", " Csv "] {
        let reply = post_query(
            &app,
            json!({ "sql": "SELECT slot, leader FROM solana.slots ORDER BY slot LIMIT 2", "format": format }),
        )
        .await;
        assert_eq!(reply.status, StatusCode::OK, "{format:?}: {}", reply.text());
        assert_eq!(reply.header("content-type"), "text/csv; charset=utf-8");
        assert_eq!(reply.text(), "slot,leader\n1000,leader1000\n1001,leader1001\n");
        assert_eq!(reply.header("x-lens-row-count"), "2");
    }
}

#[tokio::test]
async fn status_200_with_explicit_json_format_and_no_rows() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let reply =
        post_query(&app(&engine), json!({ "sql": "SELECT slot FROM solana.slots WHERE slot < 0", "format": "json" }))
            .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    assert_eq!(reply.json()["rows"], json!([]));
    assert_eq!(reply.header("x-lens-row-count"), "0");
}

#[tokio::test]
async fn status_200_for_a_truncated_result() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_MAX_QUERY_ROWS", "4")]);
    let reply = sql(&app(&engine), "SELECT slot FROM solana.transactions").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    assert_eq!(reply.header("x-lens-truncated"), "true");
    assert_eq!(reply.header("x-lens-row-count"), "4");
    assert_eq!(reply.json()["rows"].as_array().unwrap().len(), 4);
}

// ---------------------------------------------------------------------------
// Status: 4xx
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_400_for_sql_that_does_not_plan() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);
    let oversized = format!("SELECT '{}'", "x".repeat(MAX_SQL_BYTES));
    let cases: &[(&str, &str)] = &[
        ("SELEC slot FROM solana.slots", "syntax error"),
        ("SELECT * FROM solana.nonexistent", "unknown table"),
        ("SELECT no_such_column FROM solana.slots", "unknown column"),
        ("SELECT slot FROM solana.slots WHERE", "truncated statement"),
        ("", "empty"),
        ("   \n\t ", "blank"),
        ("CREATE TABLE t AS SELECT 1", "DDL"),
        ("CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/etc/'", "external table"),
        ("INSERT INTO solana.slots VALUES (1)", "DML"),
        ("DROP TABLE solana.slots", "drop"),
        ("SET datafusion.execution.batch_size = 1", "SET"),
        ("COPY (SELECT 1) TO '/tmp/lens-api-test-copy.csv'", "COPY"),
        (&oversized, "oversized"),
    ];
    for (statement, what) in cases {
        let reply = sql(&app, statement).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{what}: {}", reply.text());
        let error = assert_error(&reply, StatusCode::BAD_REQUEST, "query_plan_error", Some(statement));
        assert!(!error.error.contains("Error during planning:"), "{what}: prefixes are stripped: {error:?}");
    }
    assert_eq!(engine.metrics_snapshot().successful_queries, 0);
}

#[tokio::test]
async fn status_400_body_names_the_missing_table() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let statement = "SELECT * FROM solana.nonexistent";
    let reply = sql(&app(&engine), statement).await;
    let error = assert_error(&reply, StatusCode::BAD_REQUEST, "query_plan_error", Some(statement));
    assert!(error.error.contains("nonexistent") && error.error.contains("not found"), "{error:?}");
    assert!(!error.error.contains(statement), "the SQL is in `sql`, not repeated in `error`: {error:?}");
}

#[tokio::test]
async fn status_400_for_sql_that_fails_on_the_data() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);
    for statement in [
        "SELECT CAST(blockhash AS INT) FROM solana.slots",
        "SELECT slot / (slot - slot) FROM solana.slots",
    ] {
        let reply = sql(&app, statement).await;
        assert_error(&reply, StatusCode::BAD_REQUEST, "query_execution_error", Some(statement));
    }
}

#[tokio::test]
async fn status_400_for_an_unknown_format_without_running_the_query() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let statement = "SELECT slot FROM solana.slots";
    for format in ["parquet", "xml", ""] {
        let reply = post_query(&app(&engine), json!({ "sql": statement, "format": format })).await;
        let error = assert_error(&reply, StatusCode::BAD_REQUEST, "invalid_format", Some(statement));
        assert!(error.error.contains("json") && error.error.contains("csv"), "{error:?}");
    }
    assert_eq!(engine.metrics_snapshot().total_queries, 0, "the query never reached the engine");
}

#[tokio::test]
async fn status_4xx_for_a_body_that_is_not_a_query_request() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);

    let raw = |body: &'static str, content_type: Option<&str>| {
        let mut request = Request::post("/query");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        request.body(Body::from(body)).unwrap()
    };
    let json = Some("application/json");

    let cases = [
        (raw("{\"sql\": ", json), StatusCode::BAD_REQUEST, "malformed JSON"),
        (raw("not json", json), StatusCode::BAD_REQUEST, "not JSON"),
        (raw("{\"query\": \"SELECT 1\"}", json), StatusCode::UNPROCESSABLE_ENTITY, "missing sql"),
        (raw("{\"sql\": 42}", json), StatusCode::UNPROCESSABLE_ENTITY, "sql not a string"),
        (raw("{\"sql\": \"SELECT 1\"}", None), StatusCode::UNSUPPORTED_MEDIA_TYPE, "no content type"),
        (raw("{\"sql\": \"SELECT 1\"}", Some("text/plain")), StatusCode::UNSUPPORTED_MEDIA_TYPE, "text/plain"),
    ];
    for (request, status, what) in cases {
        let reply = send(&app, request).await;
        assert_eq!(reply.status, status, "{what}: {}", reply.text());
        assert_error(&reply, status, "invalid_request", None);
    }
    assert_eq!(engine.metrics_snapshot().total_queries, 0);
}

#[tokio::test]
async fn unknown_routes_and_methods_are_rejected() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);
    let request = |method: Method, uri: &str| Request::builder().method(method).uri(uri).body(Body::empty()).unwrap();
    assert_eq!(send(&app, request(Method::GET, "/query")).await.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(send(&app, request(Method::POST, "/health")).await.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(send(&app, request(Method::GET, "/nope")).await.status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Status: 504
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_504_for_a_query_over_the_timeout() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);
    let app = app(&engine);

    let reply = tokio::time::timeout(Duration::from_secs(10), sql(&app, ENDLESS_SQL))
        .await
        .expect("the timeout cuts the query off; the request does not hang");
    let error = assert_error(&reply, StatusCode::GATEWAY_TIMEOUT, "query_timeout", Some(ENDLESS_SQL));
    assert!(error.error.contains("1s"), "{error:?}");

    // The engine is still usable afterwards.
    assert_eq!(sql(&app, "SELECT COUNT(*) AS n FROM solana.slots").await.status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Status: 500
// ---------------------------------------------------------------------------

#[tokio::test]
async fn status_500_for_an_unreadable_parquet_file() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    corrupt_slots_table(data.path());
    engine.refresh_catalog();

    let statement = "SELECT COUNT(*) FROM solana.slots WHERE leader IS NOT NULL";
    let reply = sql(&app(&engine), statement).await;
    assert_eq!(reply.status, StatusCode::INTERNAL_SERVER_ERROR, "{}", reply.text());
    let error = reply.error();
    assert!(error.error_kind.starts_with("query_"), "{error:?}");
    assert!(error.error.to_lowercase().contains("parquet"), "the cause is the bad file: {error:?}");
    assert_eq!(error.sql.as_deref(), Some(statement));

    // Other tables are unaffected.
    assert_eq!(sql(&app(&engine), "SELECT COUNT(*) FROM solana.transactions").await.status, StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Status against the engine's own record
// ---------------------------------------------------------------------------

/// Which engine counter a request moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Succeeded,
    Failed,
    TimedOut,
    /// Turned away before it counted (validation, request errors).
    NotCounted,
}

fn outcome(before: &EngineMetricsSnapshot, after: &EngineMetricsSnapshot) -> Outcome {
    let delta = (
        after.total_queries - before.total_queries,
        after.successful_queries - before.successful_queries,
        after.failed_queries - before.failed_queries,
        after.timed_out_queries - before.timed_out_queries,
    );
    match delta {
        (1, 1, 0, 0) => Outcome::Succeeded,
        (1, 0, 1, 0) => Outcome::Failed,
        (1, 0, 0, 1) => Outcome::TimedOut,
        (0, 0, 0, 0) => Outcome::NotCounted,
        other => panic!("one request moved the metrics by {other:?}"),
    }
}

#[tokio::test]
async fn status_agrees_with_engine_outcome() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);
    let app = app(&engine);
    corrupt_slots_table(data.path());
    engine.refresh_catalog();

    let cases: Vec<(Value, StatusCode, Outcome)> = vec![
        (json!({ "sql": "SELECT COUNT(*) FROM solana.transactions" }), StatusCode::OK, Outcome::Succeeded),
        (json!({ "sql": "SELECT 1", "format": "csv" }), StatusCode::OK, Outcome::Succeeded),
        (json!({ "sql": "SELECT * FROM solana.nonexistent" }), StatusCode::BAD_REQUEST, Outcome::Failed),
        (json!({ "sql": "SELEC 1" }), StatusCode::BAD_REQUEST, Outcome::Failed),
        (json!({ "sql": "SELECT CAST(blockhash AS INT) FROM solana.transactions" }), StatusCode::BAD_REQUEST, Outcome::Failed),
        (json!({ "sql": "" }), StatusCode::BAD_REQUEST, Outcome::NotCounted),
        (json!({ "sql": "SELECT 1", "format": "xml" }), StatusCode::BAD_REQUEST, Outcome::NotCounted),
        (json!({ "query": "SELECT 1" }), StatusCode::UNPROCESSABLE_ENTITY, Outcome::NotCounted),
        (json!({ "sql": ENDLESS_SQL }), StatusCode::GATEWAY_TIMEOUT, Outcome::TimedOut),
        (json!({ "sql": "SELECT COUNT(*) FROM solana.slots WHERE leader IS NOT NULL" }), StatusCode::INTERNAL_SERVER_ERROR, Outcome::Failed),
    ];

    println!("{:<75} {:>6} {:>12}", "request", "status", "outcome");
    for (body, status, expected) in cases {
        let before = engine.metrics_snapshot();
        let reply = post_query(&app, body.clone()).await;
        let after = engine.metrics_snapshot();
        let got = outcome(&before, &after);
        println!("{:<75} {:>6} {:>12?}", body.to_string(), reply.status.as_u16(), got);

        assert_eq!(reply.status, status, "{body}: {}", reply.text());
        assert_eq!(got, expected, "{body}: status {} but engine outcome {got:?}", reply.status);
        // 2xx exactly when the engine recorded a success, 504 exactly when it
        // recorded a timeout.
        assert_eq!(reply.status.is_success(), got == Outcome::Succeeded, "{body}");
        assert_eq!(reply.status == StatusCode::GATEWAY_TIMEOUT, got == Outcome::TimedOut, "{body}");
    }
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn health_reports_tables_engine_and_uptime() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let app = app(&engine);
    assert_eq!(sql(&app, "SELECT COUNT(*) FROM solana.slots").await.status, StatusCode::OK);
    assert_eq!(sql(&app, "SELECT * FROM solana.nonexistent").await.status, StatusCode::BAD_REQUEST);

    let reply = get_health(&app).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.header("content-type"), "application/json");
    let body = reply.json();

    assert_eq!(body["status"], "healthy");
    assert_eq!(body["chain"], "solana");
    assert_eq!(body["data_dir"], engine.config().data_dir().display().to_string());
    assert!(body.get("error").is_none());
    let tables = body["tables"].as_object().unwrap();
    assert_eq!(tables.len(), 4);
    for name in ["solana.slots", "solana.transactions", "solana.instructions", "solana.token_transfers"] {
        assert_eq!(tables[name], json!({ "registered": true, "file_count": 1, "dir_exists": true }), "{name}");
    }

    let engine_json = &body["engine"];
    assert_eq!(engine_json["total_queries"], 2);
    assert_eq!(engine_json["successful_queries"], 1);
    assert_eq!(engine_json["failed_queries"], 1);
    assert_eq!(engine_json["timed_out_queries"], 0);
    assert_eq!(engine_json["success_rate"], 0.5);
    for key in ["last_query_duration_ms", "p50_duration_ms", "p99_duration_ms"] {
        assert!(engine_json[key].is_u64(), "{key}");
    }
    assert_eq!(body["catalog"]["total_refreshes"], 1);
    assert_eq!(body["catalog"]["tables_failed"], 0);
    assert!(body["uptime_secs"].is_u64());
}

#[tokio::test]
async fn health_is_healthy_when_a_table_has_not_been_written_yet() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), SLOTS);
    let engine = engine(data.path(), &[]);
    let body = get_health(&app(&engine)).await.json();

    assert_eq!(body["status"], "healthy");
    assert_eq!(body["tables"]["solana.slots"], json!({ "registered": true, "file_count": 1, "dir_exists": true }));
    assert_eq!(
        body["tables"]["solana.token_transfers"],
        json!({ "registered": false, "file_count": 0, "dir_exists": false })
    );
}

#[tokio::test]
async fn health_is_degraded_until_a_new_table_is_registered() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), SLOTS);
    let engine = engine(data.path(), &[]);
    let app = app(&engine);

    // PRISM starts writing the other tables after LENS registered.
    prism_write(data.path(), 2_000..2_002);
    let body = get_health(&app).await.json();
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["tables"]["solana.transactions"]["dir_exists"], true);
    assert_eq!(body["tables"]["solana.transactions"]["registered"], false);

    engine.refresh_catalog();
    let body = get_health(&app).await.json();
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["tables"]["solana.slots"]["file_count"], 2);
    assert_eq!(body["catalog"]["total_refreshes"], 2);
}

#[tokio::test]
async fn health_is_unhealthy_but_still_200_when_no_table_is_left() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), SLOTS);
    let engine = engine(data.path(), &[]);
    let app = app(&engine);

    fs::remove_dir_all(data.path().join("solana/slots")).unwrap();
    engine.refresh_catalog();

    let reply = get_health(&app).await;
    assert_eq!(reply.status, StatusCode::OK);
    let body = reply.json();
    assert_eq!(body["status"], "unhealthy");
    assert_eq!(body["tables"]["solana.slots"], json!({ "registered": false, "file_count": 0, "dir_exists": false }));
}

#[tokio::test]
async fn health_counts_timeouts() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);
    let app = app(&engine);
    assert_eq!(sql(&app, ENDLESS_SQL).await.status, StatusCode::GATEWAY_TIMEOUT);

    let body = get_health(&app).await.json();
    assert_eq!(body["engine"]["timed_out_queries"], 1);
    assert_eq!(body["engine"]["success_rate"], 0.0);
    assert!(body["engine"]["last_query_duration_ms"].as_u64().unwrap() >= 1_000);
    assert_eq!(body["status"], "healthy", "timeouts do not change the status");
}

// ---------------------------------------------------------------------------
// serve_on / serve, over a real socket
// ---------------------------------------------------------------------------

/// One HTTP/1.1 request over a fresh connection; returns the status and body.
async fn http(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    let status = response.split(' ').nth(1).unwrap().parse().unwrap();
    let body = response.split_once("\r\n\r\n").map(|(_, body)| body.to_owned()).unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_answers_over_tcp_and_stops_on_the_shutdown_signal() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), async move {
        let _ = stopped.await;
    }));

    let (status, body) = http(addr, "GET", "/health", "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["status"], "healthy");

    let (status, body) = http(addr, "POST", "/query", r#"{"sql":"SELECT COUNT(*) AS n FROM solana.slots"}"#).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["rows"], json!([{ "n": 10 }]));

    let (status, _) = http(addr, "POST", "/query", r#"{"sql":"SELEC"}"#).await;
    assert_eq!(status, 400);
    let (status, _) = http(addr, "POST", "/query", &json!({ "sql": ENDLESS_SQL }).to_string()).await;
    assert_eq!(status, 504);

    stop.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), server).await.expect("server stops").unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert!(engine.shutdown_token().is_cancelled(), "the refresh task was told to stop");
    assert!(TcpStream::connect(addr).await.is_err(), "the listener is closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_stops_when_the_engine_shuts_down() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));

    engine.shutdown();
    let result = tokio::time::timeout(Duration::from_secs(5), server).await.expect("server stops").unwrap();
    assert!(result.is_ok(), "{result:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_on_refreshes_the_catalog_in_the_background() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), SLOTS);
    let engine = engine(data.path(), &[("LENS_CATALOG_REFRESH_INTERVAL_SECS", "1")]);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));

    prism_write(data.path(), 2_000..2_003);
    let query = r#"{"sql":"SELECT COUNT(*) AS n FROM solana.transactions"}"#;
    let mut last = (0, String::new());
    for _ in 0..50 {
        last = http(addr, "POST", "/query", query).await;
        if last.0 == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(last.0, 200, "the new table became queryable: {}", last.1);
    assert_eq!(serde_json::from_str::<Value>(&last.1).unwrap()["rows"], json!([{ "n": 6 }]));

    engine.shutdown();
    tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn serve_fails_when_the_bind_address_is_taken() {
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = taken.local_addr().unwrap().to_string();
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_BIND_ADDRESS", &addr)]);

    let err = lens::serve(engine, std::future::pending()).await.unwrap_err();
    assert_eq!(err.kind(), "io_error", "{err}");
    assert!(err.to_string().contains(&addr), "{err}");
}
