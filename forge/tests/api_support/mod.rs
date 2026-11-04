//! Helpers for the API tests: a scripted in-process LENS, and a harness
//! wiring the router to a real registry file, a real change log reader, a
//! running webhook service and an alert engine (whose handle the router
//! reads), all in one scratch directory.
//!
//! Requests go through the router in-process with
//! `tower::ServiceExt::oneshot`, so each test sees exactly the status,
//! headers and body a client would.

#![allow(dead_code)]

use std::collections::HashMap;
use std::ffi::OsString;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use forge::config::{ENV_CHANGELOG_PATH, ENV_DB_PATH};
use forge::{
    AlertEngine, AppState, ChangelogReader, CheckpointStore, Delivery, EngineSettings, ErrorBody, ForgeConfig,
    LensApi, LensCallError, LensHealth, LensHealthStatus, LensResponse, NewDelivery, Registry, RetryPolicy,
    SystemClock, VISIBILITY_SQL, Visibility, WebhookService, WebhookSettings, router,
};
use http_body_util::BodyExt;
use serde_json::{Map, Value, json};
use tower::ServiceExt;

use crate::common::TempDir;

pub type Row = Map<String, Value>;

// ---------------------------------------------------------------------------
// ScriptedLens
// ---------------------------------------------------------------------------

/// A LENS that answers from what the test set, and records every query.
///
/// * [`VISIBILITY_SQL`] answers `max_slot` ([`Self::set_max_slot`]).
/// * Any other SQL answers [`Self::set_rows`], unless a failure set with
///   [`Self::fail_when`] matches it first.
/// * `/health` answers [`Self::set_health`], after [`Self::set_health_delay`].
pub struct ScriptedLens {
    state: Mutex<Script>,
}

struct Script {
    queries: Vec<String>,
    max_slot: Option<u64>,
    rows: Vec<Row>,
    failures: Vec<(String, LensCallError)>,
    health: Result<LensHealth, LensCallError>,
    health_delay: Duration,
}

impl ScriptedLens {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Script {
                queries: Vec::new(),
                max_slot: Some(295_000_999),
                rows: Vec::new(),
                failures: Vec::new(),
                health: Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 4 }),
                health_delay: Duration::ZERO,
            }),
        })
    }

    pub fn set_max_slot(&self, max_slot: Option<u64>) {
        self.state.lock().unwrap().max_slot = max_slot;
    }

    pub fn set_rows(&self, rows: Value) {
        self.state.lock().unwrap().rows = serde_json::from_value(rows).unwrap();
    }

    /// Fail every query whose SQL contains `needle` with `err`.
    pub fn fail_when(&self, needle: &str, err: LensCallError) {
        self.state.lock().unwrap().failures.push((needle.to_owned(), err));
    }

    pub fn clear_failures(&self) {
        self.state.lock().unwrap().failures.clear();
    }

    pub fn set_health(&self, health: Result<LensHealth, LensCallError>) {
        self.state.lock().unwrap().health = health;
    }

    pub fn set_health_delay(&self, delay: Duration) {
        self.state.lock().unwrap().health_delay = delay;
    }

    /// Every SQL received, in order.
    pub fn queries(&self) -> Vec<String> {
        self.state.lock().unwrap().queries.clone()
    }
}

#[async_trait]
impl LensApi for ScriptedLens {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        let mut state = self.state.lock().unwrap();
        state.queries.push(sql.to_owned());
        if let Some((_, err)) = state.failures.iter().find(|(needle, _)| sql.contains(needle.as_str())) {
            return Err(err.clone());
        }
        let rows = if sql == VISIBILITY_SQL {
            vec![serde_json::from_value(json!({ "max_slot": state.max_slot })).unwrap()]
        } else {
            state.rows.clone()
        };
        Ok(LensResponse {
            row_count: rows.len() as u64,
            rows,
            duration_ms: 12,
            truncated: false,
            sql: sql.trim().to_owned(),
            round_trip_ms: 15,
        })
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        let max_slot = self.state.lock().unwrap().max_slot;
        match max_slot {
            Some(max_slot) => Visibility::Visible { max_slot },
            None => Visibility::TimedOut { last_seen: None, last_error: None },
        }
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        let (health, delay) = {
            let state = self.state.lock().unwrap();
            (state.health.clone(), state.health_delay)
        };
        tokio::time::sleep(delay).await;
        health
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Fast engine settings: the tests drive [`AlertEngine::run_once`] by hand.
pub fn engine_settings() -> EngineSettings {
    EngineSettings {
        poll_interval: Duration::from_millis(5),
        batch_size: 1_000,
        gate_timeout: Duration::from_millis(100),
        max_range_slots: 1_000,
        payload_budget_bytes: 256 * 1024,
        retry_min: Duration::from_millis(5),
        retry_max: Duration::from_millis(20),
        permanent_retry: Duration::from_millis(40),
        missing_table_retries: 3,
    }
}

/// One attempt per delivery, so a failing receiver fails a row at once.
pub fn webhook_settings() -> WebhookSettings {
    WebhookSettings {
        max_attempts: 1,
        lease: Duration::from_secs(30),
        request_timeout: Duration::from_secs(2),
        retry: RetryPolicy { delays: vec![Duration::from_millis(20)], jitter_pct: 0 },
        max_in_flight: 4,
        tick: Duration::from_millis(20),
        shutdown_grace: Duration::from_secs(3),
    }
}

/// The API over a scratch directory.
pub struct Api {
    pub dir: TempDir,
    pub lens: Arc<ScriptedLens>,
    pub registry: Arc<Registry>,
    pub webhook: Arc<WebhookService>,
    pub engine: AlertEngine,
    pub config: Arc<ForgeConfig>,
    pub router: Router,
}

impl Api {
    /// Must run inside a Tokio runtime (the webhook service starts here).
    pub fn new() -> Self {
        let dir = TempDir::new();
        let vars: HashMap<&str, OsString> = HashMap::from([
            (ENV_CHANGELOG_PATH, dir.path().join("changelog.jsonl").into_os_string()),
            (ENV_DB_PATH, dir.path().join("forge.db").into_os_string()),
        ]);
        let config = Arc::new(ForgeConfig::from_lookup(|name| vars.get(name).cloned()).unwrap());
        let registry = Arc::new(Registry::open(&config).unwrap());
        let webhook = Arc::new(
            WebhookService::start_with(registry.db().clone(), webhook_settings(), Arc::new(SystemClock)).unwrap(),
        );
        let reader = ChangelogReader::open(&config, CheckpointStore::for_config(&config)).unwrap();
        let lens = ScriptedLens::new();
        let engine =
            AlertEngine::with_settings(registry.clone(), reader, lens.clone(), webhook.clone(), engine_settings())
                .unwrap();
        let state = AppState::new(registry.clone(), lens.clone(), webhook.clone(), engine.handle(), config.clone());
        Self { dir, lens, registry, webhook, engine, config, router: router(state) }
    }

    // -- requests -----------------------------------------------------------

    pub async fn send(&self, request: Request<Body>) -> Reply {
        let response = self.router.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = response.into_body().collect().await.unwrap().to_bytes().to_vec();
        Reply { status, headers, body }
    }

    pub async fn get(&self, uri: &str) -> Reply {
        self.send(Request::get(uri).body(Body::empty()).unwrap()).await
    }

    pub async fn delete(&self, uri: &str) -> Reply {
        self.send(Request::delete(uri).body(Body::empty()).unwrap()).await
    }

    pub async fn post_empty(&self, uri: &str) -> Reply {
        self.send(Request::post(uri).body(Body::empty()).unwrap()).await
    }

    /// `method uri` with `body` as JSON.
    pub async fn json(&self, method: Method, uri: &str, body: &Value) -> Reply {
        self.raw(method, uri, Some("application/json"), body.to_string()).await
    }

    pub async fn post(&self, uri: &str, body: &Value) -> Reply {
        self.json(Method::POST, uri, body).await
    }

    pub async fn patch(&self, uri: &str, body: &Value) -> Reply {
        self.json(Method::PATCH, uri, body).await
    }

    /// `method uri` with a raw body and an optional `Content-Type`.
    pub async fn raw(&self, method: Method, uri: &str, content_type: Option<&str>, body: impl Into<Body>) -> Reply {
        let mut request = Request::builder().method(method).uri(uri);
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        self.send(request.body(body.into()).unwrap()).await
    }

    // -- fixtures -----------------------------------------------------------

    /// Create an alert through the API; its id. Panics unless 201.
    pub async fn alert(&self, name: &str, webhook_url: &str, secret: &str) -> String {
        let reply = self.post("/alerts", &alert_body(name, webhook_url, Some(secret))).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text());
        reply.json()["id"].as_str().unwrap().to_owned()
    }

    /// Enqueue a delivery for `alert_id` directly, as the engine does; its id.
    pub fn enqueue(&self, alert_id: &str, to_slot: u64, hold: bool) -> String {
        let delivery = NewDelivery {
            alert_id: alert_id.to_owned(),
            to_slot,
            payload: json!({ "to_slot": to_slot, "rows": [{ "note": "payload body" }] }).to_string(),
            release_after_slot: hold.then_some(to_slot),
        };
        let enqueued = self
            .registry
            .db()
            .transaction("test enqueue", |tx| self.webhook.enqueue(tx, &delivery))
            .unwrap();
        self.webhook.wake();
        enqueued.delivery_id().to_owned()
    }

    pub fn delivery(&self, delivery_id: &str) -> Delivery {
        self.webhook.outbox().get(delivery_id).unwrap()
    }

    pub fn changelog_path(&self) -> std::path::PathBuf {
        self.config.changelog_path().to_path_buf()
    }
}

/// A `POST /alerts` body.
pub fn alert_body(name: &str, webhook_url: &str, secret: Option<&str>) -> Value {
    let mut body = json!({
        "name": name,
        "sql": ALERT_SQL,
        "webhook_url": webhook_url,
    });
    if let Some(secret) = secret {
        body["secret"] = json!(secret);
    }
    body
}

pub const ALERT_SQL: &str =
    "SELECT slot, amount_raw FROM solana.token_transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";

/// What the dry run of [`ALERT_SQL`] sends to LENS.
pub const ALERT_DRY_RUN: &str = "SELECT slot, amount_raw FROM solana.token_transfers WHERE slot BETWEEN 0 AND 0";

/// One response.
pub struct Reply {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|err| panic!("body is not JSON ({err}): {}", self.text()))
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    pub fn header(&self, name: &str) -> &str {
        self.headers.get(name).unwrap_or_else(|| panic!("no {name} header")).to_str().unwrap()
    }

    /// The body as LENS's error shape: exactly `error`, `error_kind` and
    /// `sql`, served as JSON. Panics otherwise.
    pub fn error(&self) -> ErrorBody {
        assert!(!self.status.is_success(), "expected an error, got {}: {}", self.status, self.text());
        assert_eq!(self.header("content-type"), "application/json", "{}", self.text());
        let value = self.json();
        let mut keys: Vec<&str> = value.as_object().unwrap().keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(keys, ["error", "error_kind", "sql"], "{value}");
        serde_json::from_value(value).unwrap()
    }

    /// The status and `error_kind` of an error reply.
    pub fn kind(&self) -> (StatusCode, String) {
        (self.status, self.error().error_kind)
    }
}
