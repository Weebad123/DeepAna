//! `api/` end to end: the real router, layers and handlers over real modules
//! (store, catalog, interpreter, spec runner, profiler, auditor, digester,
//! detector runner), with a scripted LENS underneath.
//!
//! Three groups, as the module's contract asks:
//!
//! * **Every route returns its documented status and body** (`route_*`,
//!   `every_route_*`), including the strict-input 400s, 404, 405, 413, 415.
//! * **`/ask` composes with `/query` exactly** (`ask_*`): the answer `/ask`
//!   returns is the answer `/query` returns for the spec `/ask` reports.
//! * **Failures reach the caller through HTTP** (`failure_*`): every row of
//!   the error table, as a status, an `error_kind`, a `Retry-After` where
//!   documented, and a body naming the request id, including over a real
//!   TCP socket.
//!
//! Every response is checked for the headers every response carries.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use common::TempDir;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensHealthStatus, LensResponse, Visibility};
use http_body_util::BodyExt;
use oracle::api::{Budgets, ErrorBody, INTERNAL_MESSAGE, MAX_BODY_BYTES, router, router_with};
use oracle::catalog::Catalog;
use oracle::config::{ENV_DB_PATH, ENV_LENS_URL};
use oracle::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL, Runner, detectors::bucket_slots};
use oracle::digest::Digester;
use oracle::intent::Interpreter;
use oracle::intent::templates::{DimensionPhrase, MeasurePhrase, PhraseTables};
use oracle::spec::guard::SpecLimits;
use oracle::profile::Profiler;
use oracle::quality::{Auditor, Clock, SystemClock, sql::DATA_FRESHNESS};
use oracle::spec::SpecRunner;
use oracle::store::{AnomalyFilter, AnomalyKind, NewAnomaly, Store};
use oracle::{AppState, OracleConfig};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// LENS's newest slot in both tables.
const W: u64 = 300_000;
const KEY: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const FEE_TOTAL: &str = "123456789012345678901234567890";
const TOTAL_FEES_QUESTION: &str = "total fees in the last 1000 slots";

// ---------------------------------------------------------------------------
// A scripted LENS
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    Unreachable,
    Timeout,
    MissingTable,
    /// LENS answers with something unreadable: a bug of ours, or of LENS.
    Protocol,
    /// Never answers.
    Hang,
    Panic,
}

type Answer = dyn Fn(&str) -> Option<LensResponse> + Send + Sync;

struct Lens {
    fault: Mutex<Fault>,
    /// Tried before the defaults.
    answer: Mutex<Option<Box<Answer>>>,
    log: Mutex<Vec<String>>,
}

impl Lens {
    fn new() -> Arc<Self> {
        Arc::new(Self { fault: Mutex::new(Fault::None), answer: Mutex::new(None), log: Mutex::new(Vec::new()) })
    }

    fn fault(&self, fault: Fault) {
        *self.fault.lock().unwrap() = fault;
    }

    fn answer(&self, f: impl Fn(&str) -> Option<LensResponse> + Send + Sync + 'static) {
        *self.answer.lock().unwrap() = Some(Box::new(f));
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// The default answers: the snapshot, the digest's window reads, one
    /// spec measure, and healthy quality checks.
    fn default_answer(sql: &str) -> LensResponse {
        if sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL {
            return response(sql, vec![json!({ "max_slot": W })]);
        }
        if sql == DATA_FRESHNESS {
            return response(sql, vec![json!({ "newest": SystemClock.now_unix_secs() })]);
        }
        if sql.contains("AS \"fee_total_lamports\"") {
            return response(sql, vec![json!({ "fee_total_lamports": FEE_TOTAL })]);
        }
        if sql.contains("AS transactions") {
            return response(sql, vec![json!({ "transactions": 100, "failed": 5 })]);
        }
        if sql.contains("AS subject") {
            return response(sql, vec![]);
        }
        if sql.starts_with("SELECT COUNT(*) AS n FROM solana.instructions") {
            return response(sql, vec![json!({ "n": 300 })]);
        }
        if sql.starts_with("SELECT COUNT(*) AS n FROM solana.token_transfers") {
            return response(sql, vec![json!({ "n": 20 })]);
        }
        if sql.contains("AS violations") || sql.contains("AS examined") {
            return response(sql, vec![json!({ "examined": 10, "violations": 0 })]);
        }
        // A grouped spec over no data.
        if sql.contains("GROUP BY") {
            return response(sql, vec![]);
        }
        response(sql, vec![json!({ "examined": 10, "violations": 0 })])
    }
}

#[async_trait]
impl LensApi for Lens {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        self.log.lock().unwrap().push(sql.to_owned());
        let fault = *self.fault.lock().unwrap();
        match fault {
            Fault::None => {}
            Fault::Unreachable => return Err(LensCallError::Unavailable { message: "connection refused".into() }),
            Fault::Timeout => return Err(LensCallError::Timeout { message: "no response within 35 s".into() }),
            Fault::MissingTable => {
                return Err(LensCallError::BadQuery { message: "table 'solana.slots' not found".into() });
            }
            Fault::Protocol => return Err(LensCallError::Protocol { message: "secret-protocol-detail".into() }),
            Fault::Hang => std::future::pending::<()>().await,
            Fault::Panic => panic!("secret-panic-detail"),
        }
        if let Some(answer) = self.answer.lock().unwrap().as_ref()
            && let Some(r) = answer(sql)
        {
            return Ok(r);
        }
        Ok(Self::default_answer(sql))
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unimplemented!()
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 1 })
    }
}

fn response(sql: &str, rows: Vec<Value>) -> LensResponse {
    let rows: Vec<Map<String, Value>> = rows.into_iter().map(|r| r.as_object().unwrap().clone()).collect();
    LensResponse { row_count: rows.len() as u64, rows, duration_ms: 0, truncated: false, sql: sql.to_owned(), round_trip_ms: 3 }
}

/// A profile over no activity: aggregates are one zero row, lists empty.
fn empty_profile(sql: &str) -> Option<LensResponse> {
    if sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL {
        return None;
    }
    if sql.contains("GROUP BY") {
        return Some(response(sql, vec![]));
    }
    Some(response(
        sql,
        vec![json!({
            "transfers": 0, "first_slot": null, "last_slot": null, "sources": 0, "destinations": 0,
            "volume_raw": null, "min_decimals": null, "max_decimals": null,
            "failed_excluded": 0, "unattributed": 0,
            "sent": 0, "received": 0, "volume_out_raw": null, "volume_in_raw": null,
            "mints": 0, "mint": null, "owners": 0, "owner": null,
            "invocations": 0, "inner_invocations": 0, "transactions": 0, "decoded": 0,
            "transactions_all": 0, "failed_transactions": 0,
        })],
    ))
}

// ---------------------------------------------------------------------------
// The rig
// ---------------------------------------------------------------------------

struct Rig {
    _dir: TempDir,
    db_path: std::path::PathBuf,
    lens: Arc<Lens>,
    state: Arc<AppState>,
    app: Router,
}

fn rig() -> Rig {
    rig_with(None)
}

fn rig_with(budgets: Option<Budgets>) -> Rig {
    rig_full(budgets, None)
}

/// The built-in phrase tables are built to be unambiguous, so the
/// `ambiguous` outcome needs one extra phrase: `levels` as both a measure
/// (`ix_cpi_count`) and a dimension (`ix_level`), as in `intent_test`.
/// "programs by levels" then has two valid, different readings.
fn ambiguous_rig() -> Rig {
    let catalog = Arc::new(Catalog::new(vec![]).unwrap());
    let mut tables = PhraseTables::builtin();
    tables.measures.push(MeasurePhrase { phrase: "levels", measure: "ix_cpi_count", implied: None });
    tables.dimensions.push(DimensionPhrase { phrase: "levels", dimension: "ix_level" });
    let limits = SpecLimits { max_rows: 1_000, max_slot_span: 216_000 };
    rig_full(None, Some(Interpreter::from_tables(catalog, 400, limits, tables).unwrap()))
}

fn rig_full(budgets: Option<Budgets>, interpreter: Option<Interpreter>) -> Rig {
    let dir = TempDir::new();
    let db_path = dir.path().join("oracle.db");
    let db = db_path.to_str().unwrap().to_owned();
    let config = Arc::new(
        OracleConfig::from_lookup(|name| match name {
            n if n == ENV_LENS_URL => Some(common::LENS_URL.into()),
            n if n == ENV_DB_PATH => Some(db.clone().into()),
            _ => None,
        })
        .unwrap(),
    );
    let store = Store::open_path(&db_path).unwrap();
    let lens = Lens::new();
    let api_lens: Arc<dyn LensApi> = lens.clone();
    let shutdown = CancellationToken::new();
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let catalog = Arc::new(Catalog::new(vec![]).unwrap());
    let interpreter =
        Arc::new(interpreter.unwrap_or_else(|| Interpreter::new(Arc::clone(&catalog), 400, config.max_rows()).unwrap()));
    let specs = Arc::new(SpecRunner::new(Arc::clone(&catalog), Arc::clone(&api_lens), &config));
    let profiler = Arc::new(Profiler::new(Arc::clone(&api_lens)));
    let auditor = Arc::new(Auditor::new(Arc::clone(&api_lens), Arc::clone(&clock)));
    let runner = Runner::new(store.clone(), Arc::clone(&api_lens), Arc::clone(&config), shutdown.clone());
    let detectors = runner.handle();
    let digester = Digester::new(
        store.clone(),
        Arc::clone(&api_lens),
        Arc::clone(&auditor),
        Arc::new(detectors.clone()),
        clock,
        bucket_slots(config.bucket_secs()),
    );
    let state = Arc::new(AppState {
        store,
        catalog,
        interpreter,
        specs,
        profiler,
        auditor,
        digests: digester.handle(),
        detectors,
        lens: api_lens,
        config,
        shutdown,
        start_time: Instant::now(),
    });
    let app = match budgets {
        Some(b) => router_with(Arc::clone(&state), b),
        None => router(Arc::clone(&state)),
    };
    Rig { _dir: dir, db_path, lens, state, app }
}

// ---------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Reply {
    fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("not JSON ({e}): {:?}", self.body))
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(|v| v.to_str().unwrap())
    }

    /// The error body, checked against the documented shape.
    fn error(&self) -> ErrorBody {
        let v = self.json();
        let keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
        for key in &keys {
            assert!(["error", "error_kind", "field", "request_id"].contains(key), "unexpected key {key} in {v}");
        }
        let body: ErrorBody = serde_json::from_value(v).unwrap();
        assert_eq!(Some(body.request_id.as_str()), self.header("x-request-id"), "the body names the request");
        body
    }

    /// Assert a failure: status, `error_kind`, and `field`.
    #[track_caller]
    fn expect_error(&self, status: u16, kind: &str, field: Option<&str>) -> ErrorBody {
        assert_eq!(self.status.as_u16(), status, "{:?}", String::from_utf8_lossy(&self.body));
        let body = self.error();
        assert_eq!(body.error_kind, kind, "{body:?}");
        assert_eq!(body.field.as_deref(), field, "{body:?}");
        body
    }

    #[track_caller]
    fn ok(&self) -> Value {
        assert_eq!(self.status, StatusCode::OK, "{:?}", String::from_utf8_lossy(&self.body));
        self.json()
    }
}

/// The headers every response carries, success or failure.
#[track_caller]
fn check_common_headers(headers: &HeaderMap) {
    assert_eq!(headers.get(header::CONTENT_TYPE).unwrap(), "application/json");
    assert_eq!(headers.get(header::CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(headers.get(header::X_CONTENT_TYPE_OPTIONS).unwrap(), "nosniff");
    let id = headers.get("x-request-id").expect("X-Request-Id").to_str().unwrap();
    assert!(uuid::Uuid::parse_str(id).is_ok(), "{id}");
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    check_common_headers(&headers);
    Reply { status, headers, body }
}

async fn get(app: &Router, uri: &str) -> Reply {
    send(app, Request::get(uri).body(Body::empty()).unwrap()).await
}

async fn json_call(app: &Router, method: Method, uri: &str, body: &Value) -> Reply {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    send(app, req).await
}

async fn post(app: &Router, uri: &str, body: &Value) -> Reply {
    json_call(app, Method::POST, uri, body).await
}

async fn raw(app: &Router, method: Method, uri: &str, content_type: Option<&str>, body: impl Into<Body>) -> Reply {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(ct) = content_type {
        req = req.header(header::CONTENT_TYPE, ct);
    }
    send(app, req.body(body.into()).unwrap()).await
}

fn fee_spec() -> Value {
    json!({ "measures": ["fee_total_lamports"], "window": { "last_slots": 1000 } })
}

/// `v` without `duration_ms`, the one field of an answer that is not a
/// function of the spec and the data.
fn timeless(mut v: Value) -> Value {
    v.as_object_mut().unwrap().remove("duration_ms");
    v
}

fn anomaly(bucket: u64) -> NewAnomaly {
    NewAnomaly {
        subject: String::new(),
        bucket,
        start_slot: bucket * 750,
        end_slot: bucket * 750 + 749,
        kind: AnomalyKind::Series,
        value: 42.5,
        baseline: Some(10.0),
        scale: Some(2.25),
        score: 9.75,
        detail: Some(json!({ "note": "spike", "ratio": 0.1 })),
    }
}

// ===========================================================================
// Every route: documented status and body
// ===========================================================================

#[tokio::test]
async fn every_route_returns_its_documented_success_status() {
    let r = rig();
    r.lens.answer(|sql| detail_answer(sql).or_else(|| empty_profile_or_none(sql)));
    r.state.store.commit_scan("tx_count", &[anomaly(5)], 5).unwrap();

    let cases: Vec<(Method, String, Option<Value>, u16)> = vec![
        (Method::POST, "/query".into(), Some(fee_spec()), 200),
        (Method::POST, "/ask".into(), Some(json!({ "question": TOTAL_FEES_QUESTION })), 200),
        (Method::GET, "/suggest?prefix=top&limit=3".into(), None, 200),
        (Method::GET, "/templates".into(), None, 200),
        (Method::GET, "/catalog".into(), None, 200),
        (Method::GET, "/anomalies?metric=tx_count&limit=10".into(), None, 200),
        (Method::GET, "/detectors".into(), None, 200),
        (Method::PATCH, "/detectors/tx_count".into(), Some(json!({ "enabled": false })), 200),
        (Method::POST, "/detectors/tx_count/reset-progress".into(), None, 200),
        (Method::GET, format!("/profile/mint/{KEY}?window_slots=1000"), None, 200),
        (Method::GET, format!("/profile/transaction/{}?slot=1000", signature()), None, 200),
        (Method::GET, "/profile/slot/1000?limit=10".into(), None, 200),
        (Method::POST, "/drill".into(), Some(json!({ "spec": fee_spec(), "row": {} })), 200),
        (Method::GET, "/quality".into(), None, 200),
        // No digest is stored yet.
        (Method::GET, "/digest".into(), None, 404),
        (Method::GET, "/digest/preview".into(), None, 200),
        (Method::GET, "/digests?limit=10".into(), None, 200),
        (Method::GET, "/health".into(), None, 200),
    ];
    for (method, uri, body, status) in cases {
        let reply = match body {
            Some(b) => json_call(&r.app, method.clone(), &uri, &b).await,
            None => raw(&r.app, method.clone(), &uri, None, Body::empty()).await,
        };
        assert_eq!(reply.status.as_u16(), status, "{method} {uri}: {:?}", String::from_utf8_lossy(&reply.body));
    }

    r.state.store.insert_digest(0, 37_499, &json!({ "headline": ["x"] })).unwrap();
    get(&r.app, "/digest").await.ok();
}

/// A well-formed 64-byte signature.
fn signature() -> String {
    bs58::encode([7u8; 64]).into_string()
}

/// The detail reports over no data: the signature is not found, the slot
/// has no row and no transactions. `None` for any other statement.
fn detail_answer(sql: &str) -> Option<LensResponse> {
    if sql.contains("log_messages")
        || sql.starts_with("SELECT slot, blockhash, parent_slot")
        || sql.starts_with("SELECT transaction_index, tx_signature")
    {
        return Some(response(sql, vec![]));
    }
    if sql.contains("AS stored_rows") {
        return Some(response(sql, vec![json!({ "transactions": 0, "failed": 0, "stored_rows": 0 })]));
    }
    None
}

/// Profiles answer from [`empty_profile`]; everything else from the defaults.
fn empty_profile_or_none(sql: &str) -> Option<LensResponse> {
    let profile_query = sql.contains("first_slot") || sql.contains("AS bucket") || sql.contains("failed_excluded")
        || sql.contains("AS invocations") || sql.contains("AS sent") || sql.contains("AS mints");
    if profile_query { empty_profile(sql) } else { None }
}

#[tokio::test]
async fn route_query_returns_the_spec_answer_unchanged() {
    let r = rig();
    let v = post(&r.app, "/query", &fee_spec()).await.ok();
    assert_eq!(v["rows"], json!([[FEE_TOTAL]]));
    assert_eq!(v["columns"][0]["name"], "fee_total_lamports");
    assert_eq!(v["as_of_slot"], W);
    assert_eq!(v["window"], json!({ "from_slot": W - 999, "to_slot": W, "slots": 1000 }));
    assert_eq!((v["has_more"].clone(), v["truncated"].clone()), (json!(false), json!(false)));
    assert_eq!(v["queries_run"], 3);
    // The same fields SpecRunner::run returns, and only those.
    let direct = r.state.specs.run(&serde_json::from_value(fee_spec()).unwrap(), &CancellationToken::new()).await.unwrap();
    assert_eq!(timeless(v), timeless(serde_json::to_value(direct).unwrap()));
}

#[tokio::test]
async fn route_query_rejects_bad_specs_with_the_field() {
    let r = rig();
    // A module's Invalid keeps its field.
    let mut spec = fee_spec();
    spec["measures"] = json!(["no_such_measure"]);
    post(&r.app, "/query", &spec).await.expect_error(400, "InvalidRequest", Some("measures[0]"));
    // Unknown field.
    let mut spec = fee_spec();
    spec["sql"] = json!("SELECT 1");
    post(&r.app, "/query", &spec).await.expect_error(400, "InvalidRequest", Some("sql"));
    // Missing field.
    post(&r.app, "/query", &json!({ "window": { "last_slots": 10 } })).await.expect_error(400, "InvalidRequest", Some("measures"));
    // Wrong type, deep.
    let mut spec = fee_spec();
    spec["window"] = json!({ "last_slots": "1000" });
    let e = post(&r.app, "/query", &spec).await.expect_error(400, "InvalidRequest", Some("window.last_slots"));
    assert!(e.error.contains("invalid type"), "{e:?}");
    // Malformed JSON.
    let reply = raw(&r.app, Method::POST, "/query", Some("application/json"), "{\"measures\": [").await;
    reply.expect_error(400, "InvalidRequest", None);
    // Nothing reached LENS beyond snapshot-free validation.
    assert!(r.lens.log().is_empty(), "{:?}", r.lens.log());
}

#[tokio::test]
async fn route_ask_reports_every_outcome_with_200() {
    let r = rig();
    // matched
    let v = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await.ok();
    assert_eq!((v["outcome"].as_str(), v["executed"].as_bool()), (Some("matched"), Some(true)));
    assert_eq!(v["question"], TOTAL_FEES_QUESTION);
    assert_eq!(v["template_id"], "total");
    assert_eq!(v["spec"]["measures"], json!(["fee_total_lamports"]));
    assert!(v["understood_as"].is_string());
    assert!(v["assumptions"].as_array().unwrap().iter().all(Value::is_string), "{v}");
    assert_eq!(v["answer"]["rows"], json!([[FEE_TOTAL]]));
    for key in ["candidates", "suggestions", "entities"] {
        assert!(v[key].is_array(), "{key}");
    }
    for key in ["message", "reason", "code", "hint"] {
        assert!(v.get(key).unwrap().is_null(), "{key}");
    }

    // ambiguous: candidates, each with a spec, nothing run.
    let amb = ambiguous_rig();
    let v = post(&amb.app, "/ask", &json!({ "question": "programs by levels" })).await.ok();
    assert_eq!((v["outcome"].as_str(), v["executed"].as_bool()), (Some("ambiguous"), Some(false)));
    let candidates = v["candidates"].as_array().unwrap();
    assert!(candidates.len() >= 2, "{v}");
    assert!(candidates.iter().all(|c| c["spec"].is_object() && c["template_id"].is_string()));
    assert!(v["answer"].is_null() && v["spec"].is_null());
    assert!(amb.lens.log().is_empty(), "ORACLE never runs a guess: {:?}", amb.lens.log());
    // The same question, with execute: true, still runs nothing.
    let v = post(&amb.app, "/ask", &json!({ "question": "programs by levels", "execute": true })).await.ok();
    assert_eq!((v["outcome"].as_str(), v["executed"].as_bool()), (Some("ambiguous"), Some(false)));
    assert!(amb.lens.log().is_empty());

    // no_match
    let v = post(&r.app, "/ask", &json!({ "question": "purple elephants" })).await.ok();
    assert_eq!(v["outcome"], "no_match");
    assert_eq!(v["reason"]["type"], "unknown_word");
    assert!(v["message"].is_string());
    assert!(!v["suggestions"].as_array().unwrap().is_empty());

    // rejected
    let v = post(&r.app, "/ask", &json!({ "question": "transactions for wsol" })).await.ok();
    assert_eq!(v["outcome"], "rejected");
    assert!(v["code"].is_string() && v["message"].is_string(), "{v}");
}

#[tokio::test]
async fn route_ask_strict_input() {
    let r = rig();
    for (body, field) in [
        (json!({ "question": "" }), Some("question")),
        (json!({ "question": "a".repeat(300) }), Some("question")),
        (json!({ "question": "top\u{0007}mints" }), Some("question")),
        (json!({ "question": "total fees", "execute": "yes" }), Some("execute")),
        (json!({ "question": "total fees", "run": true }), Some("run")),
        (json!({ "execute": true }), Some("question")),
    ] {
        post(&r.app, "/ask", &body).await.expect_error(400, "InvalidRequest", field);
    }
}

#[tokio::test]
async fn route_suggest_and_templates() {
    let r = rig();
    let v = get(&r.app, "/suggest?prefix=top&limit=2").await.ok();
    let s = v["suggestions"].as_array().unwrap();
    assert!(!s.is_empty() && s.len() <= 2, "{v}");
    assert!(s.iter().all(|q| q.as_str().unwrap().starts_with("top")), "{v}");
    assert_eq!(v.as_object().unwrap().len(), 1);
    // The prefix is optional.
    assert!(!get(&r.app, "/suggest").await.ok()["suggestions"].as_array().unwrap().is_empty());
    // Rejected, never clamped.
    get(&r.app, "/suggest?limit=0").await.expect_error(400, "InvalidRequest", Some("limit"));
    get(&r.app, "/suggest?limit=11").await.expect_error(400, "InvalidRequest", Some("limit"));
    get(&r.app, "/suggest?limit=%2B5").await.expect_error(400, "InvalidRequest", Some("limit"));
    get(&r.app, &format!("/suggest?prefix={}", "a".repeat(101))).await.expect_error(400, "InvalidRequest", Some("prefix"));
    get(&r.app, &format!("/suggest?prefix={}", "a".repeat(100))).await.ok();
    get(&r.app, "/suggest?prefix=a&prefix=b").await.expect_error(400, "InvalidRequest", Some("prefix"));
    get(&r.app, "/suggest?limt=3").await.expect_error(400, "InvalidRequest", Some("limt"));

    let v = get(&r.app, "/templates").await.ok();
    let templates = v["templates"].as_array().unwrap();
    let ids: Vec<&str> = templates.iter().map(|t| t["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["top_n", "by_dimension", "over_time", "total"]);
    for t in templates {
        assert_eq!(t.as_object().unwrap().len(), 3, "{t}");
        assert!(t["description"].is_string() && !t["examples"].as_array().unwrap().is_empty());
    }
    assert_eq!(v, serde_json::to_value(json!({ "templates": r.state.interpreter.templates() })).unwrap());
}

#[tokio::test]
async fn route_catalog_is_sorted_and_deterministic() {
    let r = rig();
    let first = get(&r.app, "/catalog").await;
    let second = get(&r.app, "/catalog").await;
    assert_eq!(first.body, second.body, "byte-identical");
    let v = first.ok();
    let ids = |key: &str, by: &str| -> Vec<String> {
        v[key].as_array().unwrap().iter().map(|m| m[by].as_str().unwrap().to_owned()).collect()
    };
    for (key, by) in [("measures", "id"), ("dimensions", "id"), ("entities", "name")] {
        let got = ids(key, by);
        let mut sorted = got.clone();
        sorted.sort();
        assert_eq!(got, sorted, "{key}");
        assert!(!got.is_empty(), "{key}");
    }
    assert_eq!(ids("measures", "id").len(), r.state.catalog.measures().len());
    assert!(ids("measures", "id").contains(&"fee_total_lamports".to_owned()));
    assert_eq!(v["limits"]["max_rows"], r.state.config.max_rows());
    assert_eq!(v["limits"]["max_slot_span"], r.state.config.max_slot_span());
    assert!(v["limits"]["max_bucket_width_slots"].is_u64());
    // Every id listed is one a spec may use.
    let m = &v["measures"].as_array().unwrap()[0];
    for key in ["label", "description", "table", "unit", "dimensions", "warnings"] {
        assert!(m.get(key).is_some(), "{key}");
    }
}

#[tokio::test]
async fn route_anomalies_lists_the_store_unchanged_and_validates() {
    let r = rig();
    r.state.store.commit_scan("tx_count", &[anomaly(3), anomaly(5)], 5).unwrap();
    let reply = get(&r.app, "/anomalies").await;
    let stored = r.state.store.list_anomalies(&AnomalyFilter::default()).unwrap();
    let expected = oracle::api::anomalies::AnomalyList { anomalies: stored };
    assert_eq!(reply.body, serde_json::to_vec(&expected).unwrap(), "byte-exact");
    assert_eq!(reply.ok()["anomalies"].as_array().unwrap().len(), 2);

    let v = get(&r.app, "/anomalies?metric=tx_count&subject=&since_bucket=4&until_bucket=9&limit=5").await.ok();
    let buckets: Vec<u64> = v["anomalies"].as_array().unwrap().iter().map(|a| a["bucket"].as_u64().unwrap()).collect();
    assert_eq!(buckets, [5]);
    assert_eq!(get(&r.app, "/anomalies?limit=1").await.ok()["anomalies"].as_array().unwrap().len(), 1);

    for (q, field) in [
        ("metric=nope", "metric"),
        ("since_bucket=9&until_bucket=4", "until_bucket"),
        ("limit=0", "limit"),
        ("limit=1001", "limit"),
        ("since_bucket=-1", "since_bucket"),
        ("until_bucket=1.5", "until_bucket"),
        ("bucket=1", "bucket"),
    ] {
        get(&r.app, &format!("/anomalies?{q}")).await.expect_error(400, "InvalidRequest", Some(field));
    }
    get(&r.app, &format!("/anomalies?subject={}", "s".repeat(129))).await.expect_error(400, "InvalidRequest", Some("subject"));
}

#[tokio::test]
async fn route_detectors_status_toggle_and_reset() {
    let r = rig();
    let reply = get(&r.app, "/detectors").await;
    assert_eq!(reply.body, serde_json::to_vec(&r.state.detectors.status()).unwrap(), "unchanged");
    let v = reply.ok();
    for key in ["last_tick_at", "watermark", "tick_error", "detectors"] {
        assert!(v.get(key).is_some(), "{key}");
    }
    let ids = r.state.detectors.ids();
    assert_eq!(v["detectors"].as_array().unwrap().len(), ids.len());

    // PATCH
    let v = json_call(&r.app, Method::PATCH, "/detectors/tx_count", &json!({ "enabled": false })).await.ok();
    assert_eq!((v["id"].as_str(), v["enabled"].as_bool(), v["state"].as_str()), (Some("tx_count"), Some(false), Some("disabled")));
    assert!(!r.state.detectors.detector("tx_count").unwrap().enabled);
    let v = json_call(&r.app, Method::PATCH, "/detectors/tx_count", &json!({ "enabled": true })).await.ok();
    assert_eq!(v["enabled"], true);
    for (body, field) in [
        (json!({ "enabled": "true" }), Some("enabled")),
        (json!({ "enabled": 1 }), Some("enabled")),
        (json!({}), Some("enabled")),
        (json!({ "enabled": true, "x": 1 }), Some("x")),
    ] {
        json_call(&r.app, Method::PATCH, "/detectors/tx_count", &body).await.expect_error(400, "InvalidRequest", field);
    }
    json_call(&r.app, Method::PATCH, "/detectors/nope", &json!({ "enabled": true }))
        .await
        .expect_error(404, "NotFound", None);
    raw(&r.app, Method::PATCH, "/detectors/tx_count", None, r#"{"enabled":true}"#)
        .await
        .expect_error(415, "UnsupportedMediaType", None);

    // reset-progress
    r.state.store.commit_scan("tx_count", &[anomaly(7)], 9).unwrap();
    let v = raw(&r.app, Method::POST, "/detectors/tx_count/reset-progress", None, Body::empty()).await.ok();
    assert_eq!((v["id"].as_str(), v["cleared_progress"].as_u64()), (Some("tx_count"), Some(9)));
    assert_eq!(v["detector"]["id"], "tx_count");
    assert_eq!(r.state.store.progress("tx_count").unwrap(), None);
    // Again: nothing left to clear.
    let v = raw(&r.app, Method::POST, "/detectors/tx_count/reset-progress", None, Body::empty()).await.ok();
    assert!(v["cleared_progress"].is_null());
    // The stored anomalies are kept.
    assert_eq!(r.state.store.list_anomalies(&AnomalyFilter::default()).unwrap().len(), 1);
    raw(&r.app, Method::POST, "/detectors/nope/reset-progress", None, Body::empty())
        .await
        .expect_error(404, "NotFound", None);
    raw(&r.app, Method::POST, "/detectors/tx_count/reset-progress", Some("application/json"), "{}")
        .await
        .expect_error(400, "InvalidRequest", None);
}

#[tokio::test]
async fn route_profile_returns_the_profile_and_validates() {
    let r = rig();
    r.lens.answer(empty_profile);
    let v = get(&r.app, &format!("/profile/mint/{KEY}?window_slots=1000&as_of_slot={}", W - 10)).await.ok();
    assert_eq!((v["kind"].as_str(), v["id"].as_str(), v["found"].as_bool()), (Some("mint"), Some(KEY), Some(false)));
    assert_eq!(v["as_of_slot"], W - 10);
    assert_eq!(v["window"]["slots"], 1000);
    get(&r.app, &format!("/profile/program/{KEY}")).await.ok();
    get(&r.app, &format!("/profile/token_account/{KEY}")).await.ok();

    let before = r.lens.log().len();
    get(&r.app, &format!("/profile/wallet/{KEY}")).await.expect_error(400, "InvalidRequest", Some("kind"));
    get(&r.app, "/profile/mint/not-a-key").await.expect_error(400, "InvalidRequest", Some("id"));
    get(&r.app, &format!("/profile/mint/{KEY}?window_slots=0")).await.expect_error(400, "InvalidRequest", Some("window_slots"));
    get(&r.app, &format!("/profile/mint/{KEY}?window_slots=1e3")).await.expect_error(400, "InvalidRequest", Some("window_slots"));
    get(&r.app, &format!("/profile/mint/{KEY}?as_of_slot=%201")).await.expect_error(400, "InvalidRequest", Some("as_of_slot"));
    get(&r.app, &format!("/profile/mint/{KEY}?window=1")).await.expect_error(400, "InvalidRequest", Some("window"));
    assert_eq!(r.lens.log().len(), before, "nothing invalid reaches LENS");
    // as_of_slot above the snapshot needs the snapshot to judge.
    get(&r.app, &format!("/profile/mint/{KEY}?as_of_slot={}", W + 1)).await.expect_error(400, "InvalidRequest", Some("as_of_slot"));
}

#[tokio::test]
async fn route_query_columns_say_what_their_values_are() {
    let r = rig();
    // Every column carries `entity`, null for measures and for dimensions
    // whose values open as nothing; the other fields are as they were.
    let v = post(&r.app, "/query", &fee_spec()).await.ok();
    assert_eq!(
        v["columns"][0],
        json!({ "name": "fee_total_lamports", "role": "measure", "type": "decimal", "nullable": true, "unit": "lamports", "entity": null })
    );
    for (dimension, measure, entity) in [
        ("mint", "transfer_count", json!("mint")),
        ("program", "ix_count", json!("program")),
        ("token_program", "transfer_count", json!("program")),
        ("slot", "tx_count", json!("slot")),
        ("slot_bucket", "tx_count", Value::Null),
        ("source_owner", "transfer_count", Value::Null),
        ("leader", "slot_count", Value::Null),
        ("tx_status", "tx_count", Value::Null),
    ] {
        let mut group = json!({ "dimension": dimension });
        if dimension == "slot_bucket" {
            group["bucket_width"] = json!(10);
        }
        let spec = json!({ "measures": [measure], "group_by": [group], "window": { "last_slots": 100 } });
        let v = post(&r.app, "/query", &spec).await.ok();
        let columns = v["columns"].as_array().unwrap();
        assert_eq!((&columns[0]["name"], &columns[0]["entity"]), (&json!(dimension), &entity), "{dimension}");
        assert_eq!(columns[1]["entity"], Value::Null, "a measure is never an entity");
        assert!(columns.iter().all(|c| c.as_object().unwrap().contains_key("entity")), "{columns:?}");
    }
    // /ask returns the same columns, hints included.
    let asked = post(&r.app, "/ask", &json!({ "question": "transfers by mint in the last hour" })).await.ok();
    assert_eq!(asked["answer"]["columns"][0]["entity"], "mint");
    // /catalog says it per dimension.
    let catalog = get(&r.app, "/catalog").await.ok();
    let hints: Vec<(String, Value)> = catalog["dimensions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| (d["id"].as_str().unwrap().to_owned(), d["entity"].clone()))
        .filter(|(_, e)| !e.is_null())
        .collect();
    assert_eq!(
        hints,
        vec![
            ("mint".to_owned(), json!("mint")),
            ("program".to_owned(), json!("program")),
            ("slot".to_owned(), json!("slot")),
            ("token_program".to_owned(), json!("program")),
        ]
    );
}

#[tokio::test]
async fn route_detail_reports_and_drill_answer_and_validate() {
    let r = rig();
    r.lens.answer(detail_answer);
    let sig = signature();

    // A signature that is not there is a 200 saying so, as an entity with no activity is.
    let v = get(&r.app, &format!("/profile/transaction/{sig}?slot={}&as_of_slot={}", W - 5, W - 1)).await.ok();
    assert_eq!((&v["kind"], &v["found"], &v["transaction"]), (&json!("transaction"), &json!(false), &Value::Null));
    assert_eq!(v["as_of_slot"], W - 1);
    assert_eq!(v["searched"], json!({ "from_slot": W - 5, "to_slot": W - 5, "slots": 1 }));
    let v = get(&r.app, &format!("/profile/transaction/{sig}?window_slots=100")).await.ok();
    assert_eq!(v["searched"], json!({ "from_slot": W - 99, "to_slot": W, "slots": 100 }));

    let v = get(&r.app, &format!("/profile/slot/{}?limit=7&after_index=3", W - 5)).await.ok();
    assert_eq!((&v["kind"], &v["found"], &v["slot"]), (&json!("slot"), &json!(false), &json!(W - 5)));
    assert_eq!(v["page"], json!({ "after_index": 3, "limit": 7, "has_more": false, "next_after_index": null }));

    let v = post(&r.app, "/drill", &json!({ "spec": fee_spec(), "row": {}, "limit": 5 })).await.ok();
    assert_eq!((&v["table"], &v["detail"], &v["limit"]), (&json!("transactions"), &json!("transaction"), &json!(5)));
    assert_eq!(
        (&v["columns"][0], &v["columns"][1]),
        (&json!({ "name": "slot", "entity": "slot" }), &json!({ "name": "tx_signature", "entity": "tx_signature" }))
    );
    assert_eq!(v["rows"], json!([]));

    // Strict input: a 400 naming the field, and nothing sent to LENS for it.
    let before = r.lens.log().len();
    let key_as_sig = format!("/profile/transaction/{KEY}");
    for (uri, field) in [
        ("/profile/transaction/not-a-signature", "signature"),
        (key_as_sig.as_str(), "signature"),
        (&format!("/profile/transaction/{sig}?slot=1e3"), "slot"),
        (&format!("/profile/transaction/{sig}?slot=1&window_slots=10"), "window_slots"),
        (&format!("/profile/transaction/{sig}?window_slots=0"), "window_slots"),
        (&format!("/profile/transaction/{sig}?window=1"), "window"),
        ("/profile/slot/abc", "slot"),
        ("/profile/slot/-1", "slot"),
        ("/profile/slot/5?limit=0", "limit"),
        ("/profile/slot/5?after_index=4294967296", "after_index"),
        ("/profile/slot/5?page=2", "page"),
    ] {
        get(&r.app, uri).await.expect_error(400, "InvalidRequest", Some(field));
    }
    for (body, field) in [
        (json!({ "spec": fee_spec(), "row": { "mint": KEY } }), "row"),
        (json!({ "spec": fee_spec(), "limit": 0 }), "limit"),
        (json!({ "spec": fee_spec(), "extra": 1 }), "extra"),
        (json!({ "row": {} }), "spec"),
        (json!({ "spec": { "measures": ["nope"], "window": { "last_slots": 5 } } }), "measures[0]"),
    ] {
        post(&r.app, "/drill", &body).await.expect_error(400, "InvalidRequest", Some(field));
    }
    assert_eq!(r.lens.log().len(), before, "an invalid request reached LENS");

    // The snapshot is needed to judge these, so LENS is asked, and they are still a 400.
    get(&r.app, &format!("/profile/transaction/{sig}?slot={}", W + 1)).await.expect_error(400, "InvalidRequest", Some("slot"));
    get(&r.app, &format!("/profile/slot/{}", W + 1)).await.expect_error(400, "InvalidRequest", Some("slot"));
    get(&r.app, &format!("/profile/slot/5?as_of_slot={}", W + 1)).await.expect_error(400, "InvalidRequest", Some("as_of_slot"));

    // Wrong methods and media types are the router's, as everywhere.
    raw(&r.app, Method::POST, &format!("/profile/transaction/{sig}"), None, Body::empty())
        .await
        .expect_error(405, "MethodNotAllowed", None);
    raw(&r.app, Method::GET, "/drill", None, Body::empty()).await.expect_error(405, "MethodNotAllowed", None);
    raw(&r.app, Method::POST, "/drill", Some("text/plain"), "{}").await.expect_error(415, "UnsupportedMediaType", None);
}

#[tokio::test]
async fn failure_detail_reports_and_drill_map_lens_failures_like_profiles_and_queries() {
    let sig = signature();
    let calls = |r: &Rig| {
        let app = r.app.clone();
        let sig = sig.clone();
        async move {
            [
                get(&app, &format!("/profile/transaction/{sig}")).await,
                get(&app, "/profile/slot/5").await,
                post(&app, "/drill", &json!({ "spec": fee_spec(), "row": {} })).await,
            ]
        }
    };
    for (fault, status, kind) in [
        (Fault::Unreachable, 503, "UpstreamUnavailable"),
        (Fault::MissingTable, 503, "NotReady"),
        (Fault::Timeout, 504, "UpstreamTimeout"),
        (Fault::Protocol, 500, "Internal"),
    ] {
        let r = rig();
        r.lens.fault(fault);
        for reply in calls(&r).await {
            let body = reply.expect_error(status, kind, None);
            assert!(!body.error.contains("secret"), "{body:?}");
        }
    }

    // A result LENS cut short cannot be assembled into a report: 502.
    let r = rig();
    r.lens.answer(|sql| {
        let mut response = detail_answer(sql)?;
        response.truncated = true;
        Some(response)
    });
    get(&r.app, &format!("/profile/transaction/{sig}")).await.expect_error(502, "UpstreamError", None);
    get(&r.app, "/profile/slot/5").await.expect_error(502, "UpstreamError", None);

    // Once ORACLE is stopping, all three say so.
    let r = rig();
    r.lens.fault(Fault::Hang);
    r.state.shutdown.cancel();
    for reply in calls(&r).await {
        reply.expect_error(503, "ShuttingDown", None);
    }
}

#[tokio::test]
async fn route_quality_default_report_and_explicit_audit() {
    let r = rig();
    let reply = get(&r.app, "/quality").await;
    let cached = r.state.auditor.last().expect("the default report is cached");
    assert_eq!(reply.body, serde_json::to_vec(&*cached).unwrap(), "byte-exact");
    let v = reply.ok();
    assert_eq!(v["as_of_slot"], W);
    for key in ["window", "generated_at", "overall", "summary", "checks", "queries_run"] {
        assert!(v.get(key).is_some(), "{key}");
    }
    // Served from the cache: no new LENS queries.
    let before = r.lens.log().len();
    let again = get(&r.app, "/quality?max_age_secs=3600").await;
    assert_eq!(again.body, reply.body);
    assert_eq!(r.lens.log().len(), before);

    // Explicit: a new audit, never cached.
    let v = get(&r.app, &format!("/quality?window_slots=100&as_of_slot={}", W - 5)).await.ok();
    assert_eq!(v["as_of_slot"], W - 5);
    assert!(Arc::ptr_eq(&cached, &r.state.auditor.last().unwrap()));

    get(&r.app, "/quality?window_slots=100&max_age_secs=5").await.expect_error(400, "InvalidRequest", Some("max_age_secs"));
    get(&r.app, "/quality?window_slots=0").await.expect_error(400, "InvalidRequest", Some("window_slots"));
    get(&r.app, "/quality?max_age_secs=86401").await.expect_error(400, "InvalidRequest", Some("max_age_secs"));
    get(&r.app, "/quality?as_of_slot=abc").await.expect_error(400, "InvalidRequest", Some("as_of_slot"));
}

#[tokio::test]
async fn route_digests_pass_stored_bodies_through_byte_exact() {
    let r = rig();
    get(&r.app, "/digest").await.expect_error(404, "NotFound", None);
    assert_eq!(get(&r.app, "/digests").await.ok(), json!({ "digests": [] }));

    // Floats, nesting, unicode and key order the store chose.
    let body = json!({ "version": 1, "zeta": [1.5, 0.1, 1e-7], "alpha": { "nested": "é ✓", "n": 18446744073709551615u64 } });
    r.state.store.insert_digest(0, 37_499, &body).unwrap();
    r.state.store.insert_digest(37_500, 74_999, &json!({ "version": 1, "second": true })).unwrap();

    let conn = rusqlite::Connection::open(&r.db_path).unwrap();
    let raw_body = |id: i64| -> String {
        conn.query_row("SELECT body FROM digests WHERE id = ?1", [id], |row| row.get(0)).unwrap()
    };

    let reply = get(&r.app, "/digest").await;
    let text = String::from_utf8(reply.body.to_vec()).unwrap();
    assert!(text.contains(&format!("\"body\":{}", raw_body(2))), "{text}");
    let v = reply.ok();
    assert_eq!((v["id"].as_i64(), v["start_slot"].as_u64(), v["end_slot"].as_u64()), (Some(2), Some(37_500), Some(74_999)));
    let mut keys: Vec<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, ["body", "created_at", "end_slot", "id", "start_slot"]);

    let reply = get(&r.app, "/digests").await;
    let text = String::from_utf8(reply.body.to_vec()).unwrap();
    assert!(text.contains(&format!("\"body\":{}", raw_body(1))), "{text}");
    let v = reply.ok();
    let ids: Vec<i64> = v["digests"].as_array().unwrap().iter().map(|d| d["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, [2, 1], "newest first");
    assert_eq!(get(&r.app, "/digests?limit=1").await.ok()["digests"].as_array().unwrap().len(), 1);
    get(&r.app, "/digests?limit=11").await.expect_error(400, "InvalidRequest", Some("limit"));
    get(&r.app, "/digests?limit=0").await.expect_error(400, "InvalidRequest", Some("limit"));
}

#[tokio::test]
async fn route_digest_preview_builds_without_storing() {
    let r = rig();
    let v = get(&r.app, "/digest/preview").await.ok();
    assert_eq!(v["provisional"], true);
    assert!(v["window"]["start_slot"].as_u64().unwrap() <= W);
    assert_eq!(v["overview"]["transactions"], 100);
    assert!(r.state.store.list_digests(10).unwrap().is_empty(), "never stored");
    let ws = 50 * bucket_slots(r.state.config.bucket_secs());
    let v = get(&r.app, &format!("/digest/preview?window_start={ws}")).await.ok();
    assert_eq!(v["window"]["start_slot"], ws);
    get(&r.app, "/digest/preview?window_start=1").await.expect_error(400, "InvalidRequest", Some("window_start"));
    get(&r.app, "/digest/preview?window_start=x").await.expect_error(400, "InvalidRequest", Some("window_start"));
}

#[tokio::test]
async fn route_health_has_the_documented_shape() {
    let r = rig();
    r.state.store.commit_scan("tx_count", &[anomaly(5)], 5).unwrap();
    let v = get(&r.app, "/health").await.ok();
    assert_eq!(v["status"], "healthy", "{v}");
    assert_eq!(v["reasons"], json!([]));
    assert!(v["uptime_secs"].is_u64());
    assert_eq!(v["lens"], json!({ "reachable": true, "max_slot": W, "round_trip_ms": 3 }));
    let d = &v["detectors"];
    for key in ["watermark", "complete_max", "lens_ok", "items"] {
        assert!(d.get(key).is_some(), "detectors.{key}");
    }
    let item = &d["items"][0];
    for key in ["id", "enabled", "state", "lag_buckets", "anomalies_total", "last_error"] {
        assert!(item.get(key).is_some(), "detectors.items.{key}");
    }
    assert_eq!(d["items"].as_array().unwrap().len(), r.state.detectors.ids().len());
    assert_eq!(v["digest"]["state"], "Idle");
    for key in ["last_window", "windows_built", "lag_windows", "last_error"] {
        assert!(v["digest"].get(key).is_some(), "digest.{key}");
    }
    assert_eq!(v["quality"], json!({ "available": false, "overall": null, "as_of_slot": null, "age_secs": null }));
    assert_eq!(v["store"], json!({ "available": true, "schema_version": 1, "anomalies": 1, "digests": 0 }));

    // Health never runs an audit, but shows the last one.
    get(&r.app, "/quality").await.ok();
    let v = get(&r.app, "/health").await.ok();
    assert_eq!(v["quality"]["available"], true);
    assert_eq!(v["quality"]["as_of_slot"], W);
    assert!(v["quality"]["overall"].is_string());
}

#[tokio::test]
async fn route_unknown_and_wrong_method_and_media_type() {
    let r = rig();
    get(&r.app, "/nope").await.expect_error(404, "NotFound", None);
    get(&r.app, "/").await.expect_error(404, "NotFound", None);

    let reply = get(&r.app, "/query").await;
    reply.expect_error(405, "MethodNotAllowed", None);
    let allow = reply.header("allow").expect("Allow header");
    assert!(allow.contains("POST"), "{allow}");
    let reply = raw(&r.app, Method::DELETE, "/health", None, Body::empty()).await;
    reply.expect_error(405, "MethodNotAllowed", None);
    assert!(reply.header("allow").unwrap().contains("GET"));
    let reply = get(&r.app, "/detectors/tx_count").await;
    reply.expect_error(405, "MethodNotAllowed", None);
    assert!(reply.header("allow").unwrap().contains("PATCH"));

    let spec = serde_json::to_vec(&fee_spec()).unwrap();
    raw(&r.app, Method::POST, "/query", None, spec.clone()).await.expect_error(415, "UnsupportedMediaType", None);
    raw(&r.app, Method::POST, "/query", Some("text/plain"), spec.clone()).await.expect_error(415, "UnsupportedMediaType", None);
    raw(&r.app, Method::POST, "/query", Some("application/json; charset=utf-8"), spec).await.ok();

    // 64 KiB is the limit.
    let big = format!("{{\"question\": \"{}\"}}", "a".repeat(MAX_BODY_BYTES));
    raw(&r.app, Method::POST, "/ask", Some("application/json"), big).await.expect_error(413, "PayloadTooLarge", None);
    raw(&r.app, Method::POST, "/detectors/tx_count/reset-progress", None, vec![b'x'; MAX_BODY_BYTES + 1])
        .await
        .expect_error(413, "PayloadTooLarge", None);
}

#[tokio::test]
async fn request_ids_are_unique_per_request() {
    let r = rig();
    let a = get(&r.app, "/templates").await;
    let b = get(&r.app, "/templates").await;
    assert_ne!(a.header("x-request-id"), b.header("x-request-id"));
}

// ===========================================================================
// /ask composes with /query exactly
// ===========================================================================

#[tokio::test]
async fn ask_answer_is_exactly_the_query_answer_for_its_spec() {
    let r = rig();
    let asked = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await.ok();
    let ask_log = r.lens.log();
    let spec = asked["spec"].clone();
    let queried = post(&r.app, "/query", &spec).await.ok();
    let query_log = r.lens.log()[ask_log.len()..].to_vec();

    assert_eq!(timeless(asked["answer"].clone()), timeless(queried), "same answer");
    assert_eq!(ask_log, query_log, "same LENS statements, in the same order");
    assert_eq!(asked["answer"]["sql"], ask_log.last().unwrap().as_str());
}

#[tokio::test]
async fn ask_without_execute_returns_the_same_spec_and_runs_nothing() {
    let r = rig();
    let executed = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION, "execute": true })).await.ok();
    let before = r.lens.log().len();
    let dry = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION, "execute": false })).await.ok();
    assert_eq!(r.lens.log().len(), before, "execute: false sends nothing to LENS");
    assert_eq!((dry["executed"].as_bool(), dry["answer"].is_null()), (Some(false), true));
    // Everything but the execution is identical.
    for key in ["question", "outcome", "template_id", "understood_as", "spec", "assumptions", "entities"] {
        assert_eq!(dry[key], executed[key], "{key}");
    }
    // `execute` defaults to true.
    let default = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await.ok();
    assert_eq!(default["executed"], true);
}

#[tokio::test]
async fn ask_assumptions_are_the_fixed_sentences() {
    let r = rig();
    let v = post(&r.app, "/ask", &json!({ "question": "total fees", "execute": false })).await.ok();
    let oracle::intent::Interpretation::Matched(m) = r.state.interpreter.interpret("total fees") else { panic!() };
    let expected: Vec<String> = m.assumptions.iter().map(|a| a.text()).collect();
    assert!(!expected.is_empty(), "no window given: the default window is assumed");
    assert_eq!(v["assumptions"], json!(expected));
}

#[tokio::test]
async fn ask_ambiguous_candidates_each_run_through_query() {
    let r = ambiguous_rig();
    let v = post(&r.app, "/ask", &json!({ "question": "programs by levels" })).await.ok();
    let candidates = v["candidates"].as_array().unwrap().clone();
    assert_eq!(candidates.len(), 2, "{v}");
    let mut specs = Vec::new();
    for c in &candidates {
        let answer = post(&r.app, "/query", &c["spec"]).await.ok();
        let names: Vec<&str> = answer["columns"].as_array().unwrap().iter().map(|c| c["name"].as_str().unwrap()).collect();
        let mut expected: Vec<&str> = c["spec"]["group_by"].as_array().unwrap().iter().map(|g| g["dimension"].as_str().unwrap()).collect();
        expected.extend(c["spec"]["measures"].as_array().unwrap().iter().map(|m| m.as_str().unwrap()));
        assert_eq!(names, expected, "the answer is to the candidate's own spec");
        specs.push(c["spec"].clone());
    }
    specs.dedup();
    assert_eq!(specs.len(), candidates.len(), "candidates have different specs");
}

#[tokio::test]
async fn ask_execution_failures_are_query_failures() {
    let r = rig();
    for (fault, status, kind) in [
        (Fault::Unreachable, 503, "UpstreamUnavailable"),
        (Fault::Timeout, 504, "UpstreamTimeout"),
        (Fault::MissingTable, 503, "NotReady"),
        (Fault::Protocol, 500, "Internal"),
    ] {
        r.lens.fault(fault);
        let asked = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await;
        let queried = post(&r.app, "/query", &fee_spec()).await;
        let a = asked.expect_error(status, kind, None);
        let q = queried.expect_error(status, kind, None);
        assert_eq!(a.error, q.error, "{fault:?}");
        assert_eq!(asked.header("retry-after"), queried.header("retry-after"), "{fault:?}");
        // Interpretation alone needs no LENS.
        let dry = post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION, "execute": false })).await;
        assert_eq!(dry.ok()["outcome"], "matched", "{fault:?}");
    }
}

// ===========================================================================
// Failures reach the caller through HTTP
// ===========================================================================

#[tokio::test]
async fn failure_lens_unreachable_is_503_upstream_unavailable() {
    let r = rig();
    r.lens.fault(Fault::Unreachable);
    r.lens.answer(empty_profile);
    for reply in [
        post(&r.app, "/query", &fee_spec()).await,
        get(&r.app, &format!("/profile/mint/{KEY}")).await,
        get(&r.app, "/quality").await,
        get(&r.app, "/digest/preview").await,
    ] {
        reply.expect_error(503, "UpstreamUnavailable", None);
        assert_eq!(reply.header("retry-after"), None);
    }
}

#[tokio::test]
async fn failure_lens_not_ready_is_503_with_retry_after_30() {
    let r = rig();
    r.lens.fault(Fault::MissingTable);
    for reply in [
        post(&r.app, "/query", &fee_spec()).await,
        get(&r.app, &format!("/profile/program/{KEY}")).await,
        get(&r.app, "/quality").await,
        get(&r.app, "/digest/preview").await,
    ] {
        reply.expect_error(503, "NotReady", None);
        assert_eq!(reply.header("retry-after"), Some("30"));
    }
}

#[tokio::test]
async fn failure_lens_timeout_is_504_upstream_timeout() {
    let r = rig();
    r.lens.fault(Fault::Timeout);
    for reply in [
        post(&r.app, "/query", &fee_spec()).await,
        get(&r.app, &format!("/profile/mint/{KEY}")).await,
        get(&r.app, "/quality").await,
        get(&r.app, "/digest/preview").await,
    ] {
        reply.expect_error(504, "UpstreamTimeout", None);
    }
}

#[tokio::test]
async fn failure_route_budget_is_504_timeout() {
    let r = rig_with(Some(Budgets::uniform(Duration::from_millis(150))));
    r.lens.fault(Fault::Hang);
    let started = Instant::now();
    for reply in [
        post(&r.app, "/query", &fee_spec()).await,
        post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await,
        get(&r.app, &format!("/profile/mint/{KEY}")).await,
        get(&r.app, "/quality?window_slots=10").await,
        get(&r.app, "/digest/preview").await,
    ] {
        let e = reply.expect_error(504, "Timeout", None);
        assert!(e.error.contains("0.15"), "{e:?}");
    }
    assert!(started.elapsed() < Duration::from_secs(10));
    // A cheap route is unaffected.
    get(&r.app, "/templates").await.ok();
}

#[tokio::test]
async fn failure_budgets_sit_above_module_deadlines() {
    let r = rig();
    let b = Budgets::for_config(&r.state.config);
    let lens = r.state.config.lens_timeout();
    assert!(b.query > lens * 3, "{b:?}");
    assert!(b.profile > Duration::from_secs(oracle::profile::PROFILE_DEADLINE_SECS));
    assert!(b.quality > Duration::from_secs(oracle::quality::AUDIT_DEADLINE_SECS));
    assert!(b.preview > Duration::from_secs(oracle::digest::BUILD_DEADLINE_SECS + oracle::digest::QUALITY_WAIT_SECS));
    for budget in [b.query, b.profile, b.quality, b.preview] {
        assert!(budget <= Duration::from_secs(400), "just above, not far above: {budget:?}");
    }
}

#[tokio::test]
async fn failure_shutdown_is_503_shutting_down() {
    let r = rig();
    r.state.shutdown.cancel();
    r.state.auditor.shutdown();
    for reply in [
        post(&r.app, "/query", &fee_spec()).await,
        post(&r.app, "/ask", &json!({ "question": TOTAL_FEES_QUESTION })).await,
        get(&r.app, &format!("/profile/mint/{KEY}")).await,
        get(&r.app, "/quality").await,
        get(&r.app, "/digest/preview").await,
        raw(&r.app, Method::POST, "/detectors/tx_count/reset-progress", None, Body::empty()).await,
    ] {
        reply.expect_error(503, "ShuttingDown", None);
    }
    // Health still answers.
    get(&r.app, "/health").await.ok();
}

#[tokio::test]
async fn failure_preview_already_running_is_429_busy() {
    let r = rig();
    r.lens.fault(Fault::Hang);
    let app = r.app.clone();
    let first = tokio::spawn(async move { get(&app, "/digest/preview").await });
    let deadline = Instant::now() + Duration::from_secs(5);
    while r.lens.log().is_empty() {
        assert!(Instant::now() < deadline, "the first preview never reached LENS");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let reply = get(&r.app, "/digest/preview").await;
    reply.expect_error(429, "Busy", None);
    assert_eq!(reply.header("retry-after"), Some("5"));
    first.abort();
}

#[tokio::test]
async fn failure_truncated_profile_is_502_upstream_error() {
    let r = rig();
    r.lens.answer(|sql| {
        (sql != MAX_SLOTS_SQL && sql != MAX_TX_SLOT_SQL).then(|| LensResponse { truncated: true, ..response(sql, vec![]) })
    });
    get(&r.app, &format!("/profile/mint/{KEY}")).await.expect_error(502, "UpstreamError", None);
}

#[tokio::test]
async fn failure_internal_errors_hide_their_detail() {
    let r = rig();
    // A module's Internal: LENS answered something unreadable.
    r.lens.fault(Fault::Protocol);
    r.lens.answer(empty_profile);
    for reply in [post(&r.app, "/query", &fee_spec()).await, get(&r.app, &format!("/profile/mint/{KEY}")).await] {
        let e = reply.expect_error(500, "Internal", None);
        assert_eq!(e.error, INTERNAL_MESSAGE);
        assert!(!String::from_utf8_lossy(&reply.body).contains("secret-protocol-detail"));
    }
}

#[tokio::test]
async fn failure_handler_panic_is_500_internal() {
    let r = rig();
    r.lens.fault(Fault::Panic);
    let reply = post(&r.app, "/query", &fee_spec()).await;
    let e = reply.expect_error(500, "Internal", None);
    assert_eq!(e.error, INTERNAL_MESSAGE);
    assert!(!String::from_utf8_lossy(&reply.body).contains("secret-panic-detail"));
    // The router survives the panic.
    r.lens.fault(Fault::None);
    post(&r.app, "/query", &fee_spec()).await.ok();
}

#[tokio::test]
async fn failure_unreadable_stored_body_is_500_internal() {
    let r = rig();
    r.state.store.insert_digest(0, 37_499, &json!({ "version": 1 })).unwrap();
    let conn = rusqlite::Connection::open(&r.db_path).unwrap();
    conn.execute_batch("PRAGMA ignore_check_constraints = ON; UPDATE digests SET body = 'not json';").unwrap();
    for uri in ["/digest", "/digests"] {
        let reply = get(&r.app, uri).await;
        let e = reply.expect_error(500, "Internal", None);
        assert_eq!(e.error, INTERNAL_MESSAGE, "{uri}");
        assert!(!String::from_utf8_lossy(&reply.body).contains("unreadable"), "{uri}");
    }
}

#[tokio::test]
async fn failure_health_is_200_even_when_lens_is_down() {
    let r = rig();
    r.lens.fault(Fault::Unreachable);
    let v = get(&r.app, "/health").await.ok();
    assert_eq!(v["status"], "unhealthy");
    assert_eq!(v["lens"], json!({ "reachable": false, "max_slot": null, "round_trip_ms": null }));
    assert!(v["reasons"].as_array().unwrap().iter().any(|r| r.as_str().unwrap().contains("LENS")));

    r.lens.fault(Fault::MissingTable);
    let v = get(&r.app, "/health").await.ok();
    assert_eq!(v["status"], "degraded");
    assert_eq!(v["lens"]["reachable"], true);

    // A hung LENS is bounded by the probe timeout.
    r.lens.fault(Fault::Hang);
    let started = Instant::now();
    let v = get(&r.app, "/health").await.ok();
    assert_eq!(v["status"], "unhealthy");
    assert!(started.elapsed() < Duration::from_secs(5));
}

/// Through a real socket: what a `curl` against ORACLE would see.
#[tokio::test]
async fn failure_and_success_over_a_real_tcp_socket() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let r = rig();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = r.app.clone();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    async fn http(addr: std::net::SocketAddr, request: String) -> (u16, String, String) {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut out = String::new();
        stream.read_to_string(&mut out).await.unwrap();
        let (head, body) = out.split_once("\r\n\r\n").unwrap();
        let status = head.split(' ').nth(1).unwrap().parse().unwrap();
        (status, head.to_ascii_lowercase(), body.to_owned())
    }

    let (status, head, body) =
        http(addr, "GET /health HTTP/1.1\r\nHost: oracle\r\nConnection: close\r\n\r\n".into()).await;
    assert_eq!(status, 200);
    assert!(head.contains("content-type: application/json") && head.contains("x-request-id: "), "{head}");
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["status"], "healthy");

    r.lens.fault(Fault::MissingTable);
    let spec = serde_json::to_string(&fee_spec()).unwrap();
    let request = format!(
        "POST /query HTTP/1.1\r\nHost: oracle\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{spec}",
        spec.len()
    );
    let (status, head, body) = http(addr, request).await;
    assert_eq!(status, 503);
    assert!(head.contains("retry-after: 30"), "{head}");
    assert!(head.contains("cache-control: no-store") && head.contains("x-content-type-options: nosniff"), "{head}");
    let e: ErrorBody = serde_json::from_str(&body).unwrap();
    assert_eq!(e.error_kind, "NotReady");
    assert!(head.contains(&format!("x-request-id: {}", e.request_id)), "{head}");

    server.abort();
}
