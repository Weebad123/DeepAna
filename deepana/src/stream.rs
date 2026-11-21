//! `GET /api/stream`: one background poller, many browser tabs.
//!
//! ```text
//!   Hub::run (ONE task)
//!     every 1 s   tail <DIR>/changelog.jsonl  ──► "changelog" event (new lines)
//!     every 3 s   LENS /health + MAX(slot)    ──► "status" event
//!                 FORGE /health, /alerts, deliveries
//!                 ORACLE /health, /digest     ──► "digest" event when a new one lands
//!                 ORACLE /anomalies           ──► "anomalies" event (new ids only)
//!                        │
//!                        ▼
//!               broadcast::Sender<Message>  ──► each SSE client: "snapshot" first, then the events
//! ```
//!
//! The poller calls the services' routers **in-process** (`oneshot`), not
//! over TCP, so it costs one set of requests per tick however many tabs
//! are open. The [`Snapshot`] it keeps is what a new client gets on
//! connect, and what a client that fell behind the channel gets again.
//!
//! Every SSE stream ends when the shutdown token is cancelled, so the HTTP
//! server's graceful shutdown is not held open by browser tabs.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures_util::Stream;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::config::{FORGE_PREFIX, LENS_PREFIX, ORACLE_PREFIX};

/// How often the change log is tailed.
pub const TAIL_INTERVAL: Duration = Duration::from_secs(1);
/// How often the services are polled.
pub const POLL_INTERVAL: Duration = Duration::from_secs(3);
/// A single in-process request gives up after this long.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Change log events the snapshot keeps, newest last.
pub const FEED_LEN: usize = 200;
/// Anomalies the snapshot keeps, newest first.
pub const ANOMALIES_LEN: usize = 50;
/// How much of an existing change log is read at startup, from its end.
pub const BACKFILL_BYTES: u64 = 256 * 1024;
/// Alerts whose recent deliveries are fetched per poll.
pub const ALERTS_WITH_DELIVERIES: usize = 10;
/// Deliveries fetched per alert.
pub const DELIVERIES_PER_ALERT: usize = 5;
/// Messages a slow client may fall behind before it is resynced.
pub const CHANNEL_CAPACITY: usize = 256;
/// Largest response body the poller reads.
const MAX_BODY: usize = 8 * 1024 * 1024;
/// The SQL behind the "newest slot" number.
pub const NEWEST_SLOT_SQL: &str = "SELECT MAX(slot) AS newest_slot FROM solana.slots";

/// One SSE event: its name and its JSON data.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub event: &'static str,
    pub data: Arc<str>,
}

impl Message {
    pub fn new(event: &'static str, data: &Value) -> Self {
        Self { event, data: Arc::from(data.to_string()) }
    }
}

/// PRISM's activity, as seen in the change log.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct PrismActivity {
    pub new_events: u64,
    pub skip_events: u64,
    pub final_events: u64,
    /// Highest slot of a `New` event.
    pub last_new_slot: Option<u64>,
    /// Highest `Final` watermark.
    pub last_final_slot: Option<u64>,
    /// `emitted_at` of the newest event, unix seconds.
    pub last_event_at: Option<i64>,
    /// Id of the newest event.
    pub last_event_id: Option<u64>,
    /// Bytes of the change log read so far.
    pub changelog_bytes: u64,
}

/// What a client gets on connect.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct Snapshot {
    pub data_dir: String,
    pub prism: PrismActivity,
    /// Newest last.
    pub changelog: VecDeque<Value>,
    /// The last poll; `null` before the first.
    pub status: Option<Value>,
    /// Newest first.
    pub anomalies: VecDeque<Value>,
    pub digest: Option<Value>,
    pub commons: Option<Value>,
}

/// The poller's shared state and the channel it feeds.
#[derive(Debug, Clone)]
pub struct Hub {
    inner: Arc<HubInner>,
}

#[derive(Debug)]
struct HubInner {
    tx: broadcast::Sender<Message>,
    snapshot: Mutex<Snapshot>,
    changelog_path: PathBuf,
    shutdown: CancellationToken,
}

impl Hub {
    pub fn new(changelog_path: PathBuf, data_dir: String, shutdown: CancellationToken) -> Self {
        let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
        let snapshot = Snapshot { data_dir, ..Snapshot::default() };
        Self { inner: Arc::new(HubInner { tx, snapshot: Mutex::new(snapshot), changelog_path, shutdown }) }
    }

    /// A new receiver; messages sent from now on reach it.
    pub fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.inner.tx.subscribe()
    }

    /// The current snapshot as a `snapshot` message.
    pub fn snapshot_message(&self) -> Message {
        let snapshot = self.lock().clone();
        Message::new("snapshot", &serde_json::to_value(snapshot).unwrap_or(Value::Null))
    }

    /// The current snapshot.
    pub fn snapshot(&self) -> Snapshot {
        self.lock().clone()
    }

    /// Number of connected receivers.
    pub fn receivers(&self) -> usize {
        self.inner.tx.receiver_count()
    }

    /// Record a COMMONS report and tell every client.
    pub fn publish_commons(&self, report: Value) {
        self.lock().commons = Some(report.clone());
        self.send(Message::new("commons", &report));
    }

    fn send(&self, message: Message) {
        // No receivers is not an error: nobody is watching.
        let _ = self.inner.tx.send(message);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Snapshot> {
        self.inner.snapshot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The poller. Runs until the shutdown token is cancelled. `api` holds
    /// the nested service routers (`/api/lens`, `/api/forge`, `/api/oracle`).
    pub async fn run(self, api: Router) {
        let mut tail = Tail::new(self.inner.changelog_path.clone());
        let lines = tail.backfill().await;
        self.ingest_lines(lines, false);

        let mut tail_tick = tokio::time::interval(TAIL_INTERVAL);
        let mut poll_tick = tokio::time::interval(POLL_INTERVAL);
        tail_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        poll_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let shutdown = self.inner.shutdown.clone();
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _ = tail_tick.tick() => {
                    let lines = tail.poll().await;
                    self.ingest_lines(lines, true);
                    self.lock().prism.changelog_bytes = tail.offset;
                }
                _ = poll_tick.tick() => {
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = self.poll(&api) => {}
                    }
                }
            }
        }
        tracing::debug!("dashboard poller stopped");
    }

    /// Fold change log lines into the snapshot; broadcast them if `send`.
    pub fn ingest_lines(&self, lines: Vec<String>, send: bool) {
        let events: Vec<Value> = lines.iter().filter_map(|line| slim_event(line)).collect();
        if events.is_empty() {
            return;
        }
        {
            let mut snapshot = self.lock();
            for event in &events {
                apply_event(&mut snapshot.prism, event);
                snapshot.changelog.push_back(event.clone());
            }
            while snapshot.changelog.len() > FEED_LEN {
                snapshot.changelog.pop_front();
            }
        }
        if send {
            let prism = serde_json::to_value(self.lock().prism.clone()).unwrap_or(Value::Null);
            self.send(Message::new("changelog", &json!({ "events": events, "prism": prism })));
        }
    }

    /// One poll of every service; see the [module docs](self).
    async fn poll(&self, api: &Router) {
        let lens_health = get(api, format!("{LENS_PREFIX}/health"));
        let newest = post(api, format!("{LENS_PREFIX}/query"), json!({ "sql": NEWEST_SLOT_SQL }));
        let forge_health = get(api, format!("{FORGE_PREFIX}/health"));
        let alerts = get(api, format!("{FORGE_PREFIX}/alerts"));
        let oracle_health = get(api, format!("{ORACLE_PREFIX}/health"));
        let anomalies = get(api, format!("{ORACLE_PREFIX}/anomalies?limit={ANOMALIES_LEN}"));
        let digest = get(api, format!("{ORACLE_PREFIX}/digest"));
        let (lens_health, newest, forge_health, alerts, oracle_health, anomalies, digest) =
            tokio::join!(lens_health, newest, forge_health, alerts, oracle_health, anomalies, digest);

        let deliveries = match &alerts {
            Reply::Ok(body) => self.deliveries(api, body).await,
            _ => Vec::new(),
        };
        let newest_slot = match &newest {
            Reply::Ok(body) => body.pointer("/rows/0/newest_slot").and_then(Value::as_u64),
            _ => None,
        };

        let status = json!({
            "polled_at": unix_secs(),
            "lens": {
                "health": lens_health.to_json(),
                "newest_slot": newest_slot,
                "newest_slot_error": newest.error(),
            },
            "forge": {
                "health": forge_health.to_json(),
                "alerts": alerts.to_json(),
                "deliveries": deliveries,
            },
            "oracle": {
                "health": oracle_health.to_json(),
            },
        });
        self.lock().status = Some(status.clone());
        self.send(Message::new("status", &status));

        if let Reply::Ok(body) = anomalies {
            let fresh = self.merge_anomalies(&body);
            if !fresh.is_empty() {
                self.send(Message::new("anomalies", &json!({ "anomalies": fresh })));
            }
        }
        if let Reply::Ok(body) = digest {
            let changed = {
                let mut snapshot = self.lock();
                let changed = snapshot.digest.as_ref().and_then(|d| d.get("id")) != body.get("id");
                if changed {
                    snapshot.digest = Some(body.clone());
                }
                changed
            };
            if changed {
                self.send(Message::new("digest", &body));
            }
        }
    }

    /// Recent deliveries of the first [`ALERTS_WITH_DELIVERIES`] alerts,
    /// newest first.
    async fn deliveries(&self, api: &Router, alerts: &Value) -> Vec<Value> {
        let ids: Vec<String> = alerts
            .get("alerts")
            .and_then(Value::as_array)
            .map(|alerts| {
                alerts
                    .iter()
                    .filter_map(|a| Some(a.get("id")?.as_str()?.to_owned()))
                    .take(ALERTS_WITH_DELIVERIES)
                    .collect()
            })
            .unwrap_or_default();
        let calls = ids
            .iter()
            .map(|id| get(api, format!("{FORGE_PREFIX}/alerts/{id}/deliveries?limit={DELIVERIES_PER_ALERT}")));
        let mut out: Vec<Value> = futures_util::future::join_all(calls)
            .await
            .into_iter()
            .filter_map(|reply| match reply {
                Reply::Ok(body) => body.get("deliveries").and_then(Value::as_array).cloned(),
                _ => None,
            })
            .flatten()
            .collect();
        out.sort_by_key(|d| std::cmp::Reverse(d.get("created_at").and_then(Value::as_i64).unwrap_or(0)));
        out
    }

    /// Add anomalies not seen yet (by id); return them, newest first.
    pub fn merge_anomalies(&self, body: &Value) -> Vec<Value> {
        let listed = body.get("anomalies").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut snapshot = self.lock();
        let newest_known = snapshot.anomalies.front().and_then(|a| a.get("id")).and_then(Value::as_i64);
        let mut fresh: Vec<Value> = listed
            .into_iter()
            .filter(|a| {
                let id = a.get("id").and_then(Value::as_i64);
                match (id, newest_known) {
                    (Some(id), Some(known)) => id > known,
                    (Some(_), None) => true,
                    (None, _) => false,
                }
            })
            .collect();
        fresh.sort_by_key(|a| std::cmp::Reverse(a.get("id").and_then(Value::as_i64).unwrap_or(0)));
        for anomaly in fresh.iter().rev() {
            snapshot.anomalies.push_front(anomaly.clone());
        }
        snapshot.anomalies.truncate(ANOMALIES_LEN);
        fresh
    }
}

// ---------------------------------------------------------------------------
// The SSE handler
// ---------------------------------------------------------------------------

/// `GET /api/stream`: a `snapshot` event, then every event the poller sends.
/// A client that falls more than [`CHANNEL_CAPACITY`] messages behind gets a
/// fresh `snapshot` instead of the messages it missed.
pub fn sse(hub: &Hub) -> Sse<impl Stream<Item = Result<Event, Infallible>> + use<>> {
    // Subscribe before taking the snapshot, so nothing falls in between.
    let rx = hub.subscribe();
    let first = hub.snapshot_message();
    let state = (Some(first), rx, hub.clone());
    let stream = futures_util::stream::unfold(state, |(first, mut rx, hub)| async move {
        if let Some(message) = first {
            return Some((Ok(to_event(&message)), (None, rx, hub)));
        }
        let shutdown = hub.inner.shutdown.clone();
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return None,
                received = rx.recv() => match received {
                    Ok(message) => return Some((Ok(to_event(&message)), (None, rx, hub))),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let message = hub.snapshot_message();
                        return Some((Ok(to_event(&message)), (None, rx, hub)));
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
            }
        }
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

fn to_event(message: &Message) -> Event {
    Event::default().event(message.event).data(&*message.data)
}

// ---------------------------------------------------------------------------
// Change log
// ---------------------------------------------------------------------------

/// Follows a growing JSONL file by byte offset.
#[derive(Debug)]
pub struct Tail {
    path: PathBuf,
    /// Bytes consumed, up to the end of the last complete line.
    pub offset: u64,
}

impl Tail {
    pub fn new(path: PathBuf) -> Self {
        Self { path, offset: 0 }
    }

    /// The complete lines in the last [`BACKFILL_BYTES`] of the file, and
    /// move to its end. Nothing if it does not exist yet.
    pub async fn backfill(&mut self) -> Vec<String> {
        let Ok(len) = tokio::fs::metadata(&self.path).await.map(|m| m.len()) else {
            return Vec::new();
        };
        let start = len.saturating_sub(BACKFILL_BYTES);
        self.offset = start;
        let mut lines = self.poll().await;
        // Starting mid-file, the first line is probably cut.
        if start > 0 && !lines.is_empty() {
            lines.remove(0);
        }
        lines
    }

    /// The complete lines appended since the last call. A file that shrank
    /// (replaced or truncated) is read again from the start.
    pub async fn poll(&mut self) -> Vec<String> {
        let Ok(mut file) = tokio::fs::File::open(&self.path).await else {
            return Vec::new();
        };
        let Ok(len) = file.metadata().await.map(|m| m.len()) else {
            return Vec::new();
        };
        if len < self.offset {
            self.offset = 0;
        }
        if len == self.offset || file.seek(SeekFrom::Start(self.offset)).await.is_err() {
            return Vec::new();
        }
        let mut buf = Vec::new();
        if (&mut file).take(len - self.offset).read_to_end(&mut buf).await.is_err() {
            return Vec::new();
        }
        // Only complete lines; a partial last line is read next time.
        let Some(end) = buf.iter().rposition(|&b| b == b'\n') else {
            return Vec::new();
        };
        self.offset += end as u64 + 1;
        String::from_utf8_lossy(&buf[..end])
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect()
    }
}

/// One change log line as the dashboard shows it: the envelope without the
/// Parquet paths (only their count). `None` for a line that is not a PRISM
/// event.
pub fn slim_event(line: &str) -> Option<Value> {
    let mut value: Value = serde_json::from_str(line).ok()?;
    let object = value.as_object_mut()?;
    object.get("event")?.as_str()?;
    if let Some(paths) = object.remove("parquet_paths") {
        let files = paths.as_array().map_or(0, Vec::len);
        object.insert("files".to_owned(), json!(files));
    }
    Some(value)
}

fn apply_event(prism: &mut PrismActivity, event: &Value) {
    let slot = event.get("slot").and_then(Value::as_u64);
    match event.get("event").and_then(Value::as_str) {
        Some("new") => {
            prism.new_events += 1;
            prism.last_new_slot = prism.last_new_slot.max(slot);
        }
        Some("skip") => prism.skip_events += 1,
        Some("final") => {
            prism.final_events += 1;
            prism.last_final_slot = prism.last_final_slot.max(slot);
        }
        _ => {}
    }
    if let Some(at) = event.get("emitted_at").and_then(Value::as_i64) {
        prism.last_event_at = Some(prism.last_event_at.map_or(at, |t| t.max(at)));
    }
    if let Some(id) = event.get("id").and_then(Value::as_u64) {
        prism.last_event_id = Some(prism.last_event_id.map_or(id, |t| t.max(id)));
    }
}

// ---------------------------------------------------------------------------
// In-process requests
// ---------------------------------------------------------------------------

/// What an in-process request came back with.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// 2xx with a JSON body.
    Ok(Value),
    /// Any other status; the body if it was JSON.
    Status(u16, Value),
    /// No answer: timeout, unreadable body.
    Failed(String),
}

impl Reply {
    /// For the `status` event: the body, or `{"error": ...}`.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Ok(body) => body.clone(),
            Self::Status(code, body) => json!({ "error": error_text(body).unwrap_or_else(|| format!("HTTP {code}")), "http_status": code }),
            Self::Failed(reason) => json!({ "error": reason }),
        }
    }

    pub fn error(&self) -> Option<String> {
        match self {
            Self::Ok(_) => None,
            Self::Status(code, body) => Some(error_text(body).unwrap_or_else(|| format!("HTTP {code}"))),
            Self::Failed(reason) => Some(reason.clone()),
        }
    }
}

fn error_text(body: &Value) -> Option<String> {
    body.get("error").and_then(Value::as_str).map(str::to_owned)
}

async fn get(api: &Router, uri: String) -> Reply {
    let request = Request::builder().method(Method::GET).uri(uri).body(Body::empty());
    send(api, request).await
}

async fn post(api: &Router, uri: String, body: Value) -> Reply {
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()));
    send(api, request).await
}

async fn send(api: &Router, request: Result<Request<Body>, axum::http::Error>) -> Reply {
    let request = match request {
        Ok(request) => request,
        Err(err) => return Reply::Failed(format!("bad request: {err}")),
    };
    let call = api.clone().oneshot(request);
    let response = match tokio::time::timeout(REQUEST_TIMEOUT, call).await {
        Ok(Ok(response)) => response,
        Ok(Err(never)) => match never {},
        Err(_) => return Reply::Failed(format!("no answer within {}s", REQUEST_TIMEOUT.as_secs())),
    };
    let status = response.status();
    let bytes = match axum::body::to_bytes(response.into_body(), MAX_BODY).await {
        Ok(bytes) => bytes,
        Err(err) => return Reply::Failed(format!("unreadable body: {err}")),
    };
    let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    if status.is_success() {
        Reply::Ok(body)
    } else if status == StatusCode::NOT_FOUND && body.is_null() {
        Reply::Status(404, Value::Null)
    } else {
        Reply::Status(status.as_u16(), body)
    }
}

fn unix_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slim_event_drops_paths_and_keeps_their_count() {
        let line = r#"{"id":3,"emitted_at":10,"chain":"solana-devnet","event":"new","slot":5,"blockhash":"h","parent_slot":4,"block_time":null,"transaction_count":2,"parquet_paths":["a","b","c"]}"#;
        let event = slim_event(line).unwrap();
        assert_eq!(event["files"], 3);
        assert!(event.get("parquet_paths").is_none());
        assert_eq!(event["slot"], 5);
    }

    #[test]
    fn slim_event_rejects_non_events() {
        assert!(slim_event("not json").is_none());
        assert!(slim_event("{\"id\":1}").is_none());
        assert!(slim_event("[1,2]").is_none());
    }

    #[test]
    fn activity_counts_each_kind_and_keeps_the_highest_slots() {
        let mut prism = PrismActivity::default();
        for line in [
            r#"{"id":1,"emitted_at":100,"event":"new","slot":7}"#,
            r#"{"id":2,"emitted_at":101,"event":"skip","slot":8,"reason":"validator_skipped"}"#,
            r#"{"id":3,"emitted_at":99,"event":"new","slot":6}"#,
            r#"{"id":4,"emitted_at":102,"event":"final","slot":5,"previous_slot":0}"#,
        ] {
            apply_event(&mut prism, &slim_event(line).unwrap());
        }
        assert_eq!((prism.new_events, prism.skip_events, prism.final_events), (2, 1, 1));
        assert_eq!(prism.last_new_slot, Some(7));
        assert_eq!(prism.last_final_slot, Some(5));
        assert_eq!(prism.last_event_at, Some(102));
        assert_eq!(prism.last_event_id, Some(4));
    }

    #[test]
    fn anomalies_are_merged_by_id_newest_first() {
        let hub = Hub::new(PathBuf::from("/nonexistent"), String::new(), CancellationToken::new());
        let fresh = hub.merge_anomalies(&json!({ "anomalies": [{ "id": 2 }, { "id": 1 }] }));
        assert_eq!(fresh, vec![json!({ "id": 2 }), json!({ "id": 1 })]);
        let fresh = hub.merge_anomalies(&json!({ "anomalies": [{ "id": 3 }, { "id": 2 }, { "id": 1 }] }));
        assert_eq!(fresh, vec![json!({ "id": 3 })]);
        let ids: Vec<_> = hub.snapshot().anomalies.iter().map(|a| a["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![3, 2, 1]);
    }
}
