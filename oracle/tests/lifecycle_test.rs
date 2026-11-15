//! `lib.rs`: assembly and lifecycle. `Oracle::start`/`start_with` over a real
//! store and every real module, with a scripted LENS (or a real client
//! pointed at nothing): ordered startup, no LENS request at start, the
//! child token, fail-stop supervision, bounded shutdown, `detect_enabled`,
//! the window-size warning, and the store lock's lifetime.

mod common;

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::Fixture;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensHealthStatus, LensResponse, Visibility};
use http_body_util::BodyExt;
use oracle::catalog::{EntityDef, EntityKind};
use oracle::config::{ENV_DETECT_ENABLED, ENV_LENS_URL};
use oracle::detect::{Detector, LAST_TX_SLOT_SQL, MAX_SLOTS_SQL, MAX_TX_SLOT_SQL, MIN_SLOTS_SQL, ScanContext, ScanError, ScanOutput};
use oracle::store::{AnomalyKind, Store};
use oracle::{DETECT_TASK, Oracle, OracleConfig, OracleError, OracleParts};
use serde_json::{Map, Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const W: u64 = 300_000;

// ---------------------------------------------------------------------------
// A scripted LENS
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Lens {
    calls: AtomicUsize,
}

fn response(sql: &str, rows: Vec<Value>) -> LensResponse {
    let rows: Vec<Map<String, Value>> = rows.into_iter().map(|r| r.as_object().unwrap().clone()).collect();
    LensResponse { row_count: rows.len() as u64, rows, duration_ms: 0, truncated: false, sql: sql.to_owned(), round_trip_ms: 1 }
}

#[async_trait]
impl LensApi for Lens {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(match sql {
            s if s == MAX_SLOTS_SQL || s == MAX_TX_SLOT_SQL || s == LAST_TX_SLOT_SQL => {
                response(sql, vec![json!({ "max_slot": W })])
            }
            s if s == MIN_SLOTS_SQL => response(sql, vec![json!({ "min_slot": 0 })]),
            _ => response(sql, vec![]),
        })
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unimplemented!()
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 1 })
    }
}

/// A detector whose scan panics: the detector loop's task dies with it.
struct Panicky;

#[async_trait]
impl Detector for Panicky {
    fn id(&self) -> &str {
        "panicky"
    }
    fn kind(&self) -> AnomalyKind {
        AnomalyKind::Series
    }
    async fn scan(&self, _ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        panic!("detector exploded")
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parts(lens: &Arc<Lens>) -> OracleParts {
    OracleParts { lens: Some(lens.clone()), ..OracleParts::default() }
}

fn config(fixture: &Fixture, vars: &[(&str, &str)]) -> OracleConfig {
    fixture.load(vars).unwrap()
}

async fn get(app: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = app.clone().oneshot(Request::get(uri).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

async fn post_json(app: &axum::Router, uri: &str, body: &Value) -> (StatusCode, Value) {
    let req = Request::post(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap();
    let response = app.clone().oneshot(req).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

fn task_failure(r: Result<(), OracleError>) -> (String, String) {
    match r {
        Err(OracleError::TaskFailed { task, reason }) => (task, reason),
        other => panic!("expected TaskFailed, got {other:?}"),
    }
}

/// Wait until the store at `path` can be opened again, i.e. its lock is free.
async fn lock_released(path: &std::path::Path) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if Store::open_path(path).is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

#[tokio::test]
async fn start_serves_the_api_and_stops_cleanly_on_the_host_token() {
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    let oracle = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();
    let app = oracle.router();

    let (status, health) = get(&app, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["lens"]["max_slot"], W);
    assert_eq!(health["store"]["available"], true);
    let (status, detectors) = get(&app, "/detectors").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detectors["detectors"].as_array().unwrap().len(), 8, "the default detectors");
    assert!(detectors["detectors"].as_array().unwrap().iter().all(|d| d["enabled"] == true));

    host.cancel();
    tokio::time::timeout(Duration::from_secs(2), oracle.stopped()).await.expect("stopped() resolves");
    let started = Instant::now();
    oracle.wait().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(oracle::SHUTDOWN_GRACE_SECS), "the tasks stopped on their own");
    drop(app);
    assert!(lock_released(fixture.dir.path().join("oracle.db").as_path()).await);
}

#[tokio::test]
async fn start_needs_no_lens_and_makes_no_request() {
    // A real client, pointed at a port nothing listens on.
    let fixture = Fixture::new();
    let host = CancellationToken::new();
    host.cancel();
    let oracle = Oracle::start(config(&fixture, &[(ENV_LENS_URL, "http://127.0.0.1:9")]), host).await.unwrap();
    oracle.wait().await.unwrap();

    // The same with a scripted LENS: an already-cancelled token is not an
    // error, and nothing (start included) asks LENS anything.
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    host.cancel();
    let oracle = Oracle::start_with(config(&fixture, &[]), parts(&lens), host).await.unwrap();
    oracle.wait().await.unwrap();
    assert_eq!(lens.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn lens_down_is_reported_not_fatal() {
    let fixture = Fixture::new();
    let host = CancellationToken::new();
    let oracle = Oracle::start(config(&fixture, &[(ENV_LENS_URL, "http://127.0.0.1:9")]), host.clone()).await.unwrap();
    let (status, health) = get(&oracle.router(), "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(health["status"], "unhealthy");
    assert_eq!(health["lens"]["reachable"], false);
    host.cancel();
    oracle.wait().await.unwrap();
}

#[tokio::test]
async fn a_failure_before_the_store_touches_nothing_on_disk() {
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let bad = EntityDef { name: "x".into(), aliases: vec![], address: "not-base58!".into(), kind: EntityKind::Mint };
    let parts = OracleParts { extra_entities: vec![bad], ..parts(&lens) };
    let err = Oracle::start_with(config(&fixture, &[]), parts, CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.config_field(), Some("extra_entities"), "{err}");
    assert!(!fixture.dir.path().join("oracle.db").exists(), "the catalog is built before the store is opened");
    assert!(!fixture.dir.path().join("oracle.db.lock").exists());
    assert_eq!(lens.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn a_second_oracle_on_the_same_database_is_refused_until_the_first_is_gone() {
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    let first = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();

    let err = Oracle::start_with(config(&fixture, &[]), parts(&lens), CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.kind(), "store_error", "{err}");
    assert!(!host.is_cancelled(), "a failed start does not touch the running one");

    // The lock lives as long as any router does, even after wait().
    let router = first.router();
    host.cancel();
    first.wait().await.unwrap();
    let err = Oracle::start_with(config(&fixture, &[]), parts(&lens), CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.kind(), "store_error", "the router still holds the state: {err}");

    drop(router);
    let host = CancellationToken::new();
    let again = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();
    host.cancel();
    again.wait().await.unwrap();
}

#[tokio::test]
async fn detect_disabled_starts_every_detector_off_but_runs_both_tasks() {
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    let oracle =
        Oracle::start_with(config(&fixture, &[(ENV_DETECT_ENABLED, "false")]), parts(&lens), host.clone()).await.unwrap();
    let app = oracle.router();
    let (_, detectors) = get(&app, "/detectors").await;
    let items = detectors["detectors"].as_array().unwrap();
    assert!(!items.is_empty());
    assert!(items.iter().all(|d| d["enabled"] == false && d["state"] == "disabled"), "{detectors}");

    // Both loops run: the detector loop still reads the watermark.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, d) = get(&app, "/detectors").await;
        if d["watermark"].is_object() {
            break;
        }
        assert!(Instant::now() < deadline, "the detector loop never ticked");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // And a detector can still be turned on over the API.
    let req = Request::patch("/detectors/tx_count")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"enabled":true}"#))
        .unwrap();
    assert_eq!(app.clone().oneshot(req).await.unwrap().status(), StatusCode::OK);

    host.cancel();
    oracle.wait().await.unwrap();
}

// ---------------------------------------------------------------------------
// Supervision
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_task_failure_stops_oracle_but_not_the_host() {
    let fixture = Fixture::new();
    // Seed progress so the first tick scans (and panics) instead of baselining.
    {
        let store = Store::open_path(fixture.dir.path().join("oracle.db")).unwrap();
        store.commit_scan("panicky", &[], 100).unwrap();
    }
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    let parts = OracleParts { detectors: Some(vec![Arc::new(Panicky) as Arc<dyn Detector>]), ..parts(&lens) };
    let oracle = Oracle::start_with(config(&fixture, &[]), parts, host.clone()).await.unwrap();
    let app = oracle.router();

    tokio::time::timeout(Duration::from_secs(5), oracle.stopped()).await.expect("the failure stops ORACLE");
    assert!(!host.is_cancelled(), "ORACLE cancels its child token, never the host's");

    // Long requests now end as ShuttingDown; health still answers.
    let spec = json!({ "measures": ["tx_count"], "window": { "last_slots": 100 } });
    let (status, body) = post_json(&app, "/query", &spec).await;
    assert_eq!((status, body["error_kind"].as_str()), (StatusCode::SERVICE_UNAVAILABLE, Some("ShuttingDown")));
    assert_eq!(get(&app, "/health").await.0, StatusCode::OK);

    let (task, reason) = task_failure(oracle.wait().await);
    assert_eq!(task, DETECT_TASK);
    assert_eq!(reason, "panicked: detector exploded");
}

#[tokio::test]
async fn dropping_the_handle_without_wait_stops_the_tasks() {
    let fixture = Fixture::new();
    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    let oracle = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();
    let token = oracle.state().shutdown.clone();
    drop(oracle);
    assert!(token.is_cancelled(), "the drop guard cancels ORACLE's token");
    assert!(!host.is_cancelled());
    // The tasks stop and release their clones of the store.
    assert!(lock_released(fixture.dir.path().join("oracle.db").as_path()).await);
}

// ---------------------------------------------------------------------------
// The startup warning
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_changed_digest_window_is_warned_about_at_startup() {
    let fixture = Fixture::new();
    // A digest built with 60 s buckets (150 slots): a 7_500-slot window.
    {
        let store = Store::open_path(fixture.dir.path().join("oracle.db")).unwrap();
        store.insert_digest(0, 7_499, &json!({ "version": 1 })).unwrap();
    }
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    let _default = tracing::subscriber::set_default(subscriber);

    let lens = Arc::new(Lens::default());
    let host = CancellationToken::new();
    // Default 300 s buckets: 750 slots, a 37_500-slot window.
    let oracle = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();
    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("WARN") && logs.contains("different number of slots"), "{logs}");
    assert!(logs.contains("stored_window_slots=7500") && logs.contains("window_slots=37500"), "{logs}");
    // The stored digest is kept as it is.
    let (_, latest) = get(&oracle.router(), "/digest").await;
    assert_eq!(latest["end_slot"], 7_499);
    host.cancel();
    oracle.wait().await.unwrap();

    // The same window size again: no warning.
    let fixture = Fixture::new();
    {
        let store = Store::open_path(fixture.dir.path().join("oracle.db")).unwrap();
        store.insert_digest(0, 37_499, &json!({ "version": 1 })).unwrap();
    }
    captured.0.lock().unwrap().clear();
    let host = CancellationToken::new();
    let oracle = Oracle::start_with(config(&fixture, &[]), parts(&lens), host.clone()).await.unwrap();
    host.cancel();
    oracle.wait().await.unwrap();
    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(!logs.contains("different number of slots"), "{logs}");
}
