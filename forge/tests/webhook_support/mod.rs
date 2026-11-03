//! Helpers for the webhook tests: a scripted receiver on a real socket, a
//! manual clock, an HMAC written from scratch, and a harness wiring them to
//! a real registry file and a running `WebhookService`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forge::{
    Clock, Delivery, DeliveryStatus, Enqueued, NewAlert, NewDelivery, Registry, RetryPolicy, WebhookService,
    WebhookSettings,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::common::TempDir;

pub const SECRET: &str = "whsec-0123456789abcdef";
pub const ALERT_SQL: &str = "SELECT * FROM t WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";
/// The claim lease [`settings`] uses.
pub const LEASE_MS: i64 = 30_000;
/// 2024-10-27T03:33:20Z, in ms.
pub const T0: i64 = 1_730_000_000_000;

// ---------------------------------------------------------------------------
// Independent HMAC-SHA256 (RFC 2104), sharing no code with forge::signer
// ---------------------------------------------------------------------------

pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK: usize = 64;
    let mut block_key = [0u8; BLOCK];
    if key.len() > BLOCK {
        block_key[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block_key[..key.len()].copy_from_slice(key);
    }
    let pad = |byte: u8| block_key.map(|k| k ^ byte);
    let inner = Sha256::new().chain_update(pad(0x36)).chain_update(message).finalize();
    Sha256::new().chain_update(pad(0x5c)).chain_update(inner).finalize().into()
}

pub fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// What a receiver following the documented contract computes.
pub fn expected_signature(secret: &str, timestamp: &str, body: &[u8]) -> String {
    let mut message = Vec::with_capacity(timestamp.len() + 1 + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    format!("v1={}", to_hex(&hmac_sha256(secret.as_bytes(), &message)))
}

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ManualClock(AtomicI64);

impl ManualClock {
    pub fn new(start_ms: i64) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(start_ms)))
    }

    pub fn set(&self, ms: i64) {
        self.0.store(ms, Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_millis() as i64, Ordering::SeqCst);
    }

    pub fn get(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.get()
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// One request as it arrived on the wire.
#[derive(Debug, Clone)]
pub struct Received {
    pub method: String,
    pub path: String,
    /// Names lowercased, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// Index of this request among those with the same delivery id.
    pub nth: usize,
}

impl Received {
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers.iter().find(|(n, _)| *n == name).map(|(_, v)| v.as_str())
    }

    pub fn header_count(&self, name: &str) -> usize {
        let name = name.to_ascii_lowercase();
        self.headers.iter().filter(|(n, _)| *n == name).count()
    }

    pub fn delivery_id(&self) -> &str {
        self.header("x-forge-delivery-id").unwrap_or_default()
    }

    pub fn attempt(&self) -> u32 {
        self.header("x-forge-attempt").and_then(|a| a.parse().ok()).unwrap_or(0)
    }

    pub fn timestamp(&self) -> i64 {
        self.header("x-forge-timestamp").and_then(|t| t.parse().ok()).unwrap_or(i64::MIN)
    }

    /// Whether the signature is what an independent implementation computes
    /// for this exact timestamp header and body.
    pub fn signature_is_valid(&self, secret: &str) -> bool {
        let (Some(ts), Some(sig)) = (self.header("x-forge-timestamp"), self.header("x-forge-signature")) else {
            return false;
        };
        sig == expected_signature(secret, ts, &self.body)
    }
}

/// How the receiver answers one request.
#[derive(Debug, Clone)]
pub enum Reply {
    Status(u16),
    WithBody(u16, String),
    /// Answer `status` after a pause.
    After(Duration, u16),
    /// A 302 to `location`.
    Redirect(String),
    /// Read the request, never answer, keep the socket open.
    Hang,
    /// Read the request, then close without answering.
    Close,
}

type Script = Arc<dyn Fn(&Received) -> Reply + Send + Sync>;

#[derive(Default)]
struct Log {
    received: Vec<Received>,
    /// (delivery id, status) for every response fully written.
    answered: Vec<(String, u16)>,
    active: HashMap<String, usize>,
    max_active_per_path: HashMap<String, usize>,
    active_total: usize,
    max_active_total: usize,
}

/// A webhook receiver on 127.0.0.1 that answers per a swappable script.
#[derive(Clone)]
pub struct Receiver {
    addr: std::net::SocketAddr,
    log: Arc<Mutex<Log>>,
    script: Arc<Mutex<Script>>,
}

impl Receiver {
    pub async fn start(script: impl Fn(&Received) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let receiver = Self {
            addr: listener.local_addr().unwrap(),
            log: Arc::default(),
            script: Arc::new(Mutex::new(Arc::new(script))),
        };
        let accept = receiver.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(accept.clone().serve(stream));
            }
        });
        receiver
    }

    pub fn set_script(&self, script: impl Fn(&Received) -> Reply + Send + Sync + 'static) {
        *self.script.lock().unwrap() = Arc::new(script);
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    pub fn received(&self) -> Vec<Received> {
        self.log.lock().unwrap().received.clone()
    }

    pub fn count(&self) -> usize {
        self.log.lock().unwrap().received.len()
    }

    pub fn for_delivery(&self, delivery_id: &str) -> Vec<Received> {
        self.received().into_iter().filter(|r| r.delivery_id() == delivery_id).collect()
    }

    pub fn answered(&self) -> Vec<(String, u16)> {
        self.log.lock().unwrap().answered.clone()
    }

    pub fn max_active_per_path(&self) -> HashMap<String, usize> {
        self.log.lock().unwrap().max_active_per_path.clone()
    }

    pub fn max_active_total(&self) -> usize {
        self.log.lock().unwrap().max_active_total
    }

    async fn serve(self, mut stream: TcpStream) {
        let Some(mut request) = read_request(&mut stream).await else {
            return;
        };
        let reply = {
            let mut log = self.log.lock().unwrap();
            request.nth = log.received.iter().filter(|r| r.delivery_id() == request.delivery_id()).count();
            log.received.push(request.clone());
            let active = log.active.entry(request.path.clone()).or_default();
            *active += 1;
            let now = *active;
            let max = log.max_active_per_path.entry(request.path.clone()).or_default();
            *max = (*max).max(now);
            log.active_total += 1;
            log.max_active_total = log.max_active_total.max(log.active_total);
            let script = self.script.lock().unwrap().clone();
            drop(log);
            script(&request)
        };

        let (status, body, location) = match reply {
            Reply::Status(status) => (status, String::new(), None),
            Reply::WithBody(status, body) => (status, body, None),
            Reply::After(pause, status) => {
                tokio::time::sleep(pause).await;
                (status, String::new(), None)
            }
            Reply::Redirect(location) => (302, String::new(), Some(location)),
            Reply::Hang => {
                tokio::time::sleep(Duration::from_secs(120)).await;
                return;
            }
            Reply::Close => {
                self.done(&request, None);
                return;
            }
        };
        let mut head = format!("HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n", body.len());
        if let Some(location) = location {
            head.push_str(&format!("Location: {location}\r\n"));
        }
        head.push_str("\r\n");
        let written = stream.write_all(head.as_bytes()).await.is_ok() && stream.write_all(body.as_bytes()).await.is_ok();
        let _ = stream.flush().await;
        self.done(&request, written.then_some(status));
    }

    fn done(&self, request: &Received, status: Option<u16>) {
        let mut log = self.log.lock().unwrap();
        *log.active.get_mut(&request.path).unwrap() -= 1;
        log.active_total -= 1;
        if let Some(status) = status {
            log.answered.push((request.delivery_id().to_owned(), status));
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> Option<Received> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8(buf[..head_end].to_vec()).ok()?;
    let mut lines = head.split("\r\n");
    let mut start = lines.next()?.split(' ');
    let (method, path) = (start.next()?.to_owned(), start.next()?.to_owned());
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let length: usize = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    Some(Received { method, path, headers, body, nth: 0 })
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// Small, deterministic settings: no jitter, a fast tick, short timeouts.
pub fn settings(max_attempts: u32, delays_secs: &[u64]) -> WebhookSettings {
    WebhookSettings {
        max_attempts,
        lease: Duration::from_millis(LEASE_MS as u64),
        request_timeout: Duration::from_secs(2),
        retry: RetryPolicy {
            delays: delays_secs.iter().map(|s| Duration::from_secs(*s)).collect(),
            jitter_pct: 0,
        },
        max_in_flight: 4,
        tick: Duration::from_millis(25),
        shutdown_grace: Duration::from_secs(3),
    }
}

pub struct Harness {
    pub dir: TempDir,
    pub registry: Registry,
    pub clock: Arc<ManualClock>,
    pub service: WebhookService,
}

impl Harness {
    pub fn new(settings: WebhookSettings) -> Self {
        let dir = TempDir::new();
        let registry = Registry::open_path(dir.path().join("forge.db")).unwrap();
        Self::on(dir, registry, settings, ManualClock::new(T0))
    }

    /// A service on an existing database, as after a restart.
    pub fn on(dir: TempDir, registry: Registry, settings: WebhookSettings, clock: Arc<ManualClock>) -> Self {
        let service = WebhookService::start_with(registry.db().clone(), settings, clock.clone()).unwrap();
        Self { dir, registry, clock, service }
    }

    pub fn alert(&self, name: &str, url: &str) -> String {
        self.alert_with_secret(name, url, SECRET)
    }

    pub fn alert_with_secret(&self, name: &str, url: &str, secret: &str) -> String {
        create_alert(&self.registry, name, url, secret)
    }

    /// Advance the alert and enqueue in one transaction, then wake: the
    /// documented calling pattern.
    pub fn fire(&self, alert_id: &str, to_slot: u64, payload: &str, hold: bool) -> String {
        let delivery = NewDelivery {
            alert_id: alert_id.into(),
            to_slot,
            payload: payload.into(),
            release_after_slot: hold.then_some(to_slot),
        };
        let enqueued = self
            .registry
            .db()
            .transaction("fire", |tx| {
                Registry::advance_progress(tx, alert_id, to_slot)?;
                self.service.enqueue(tx, &delivery)
            })
            .unwrap();
        self.service.wake();
        match enqueued {
            Enqueued::Inserted { delivery_id } => delivery_id,
            other => panic!("expected a new row, got {other:?}"),
        }
    }

    pub fn get(&self, delivery_id: &str) -> Delivery {
        self.service.outbox().get(delivery_id).unwrap()
    }

    pub fn status(&self, delivery_id: &str) -> DeliveryStatus {
        self.get(delivery_id).status
    }

    /// The stored payload as raw bytes, straight from SQLite.
    pub fn stored_payload_bytes(&self, delivery_id: &str) -> Vec<u8> {
        stored_payload_bytes(&self.registry, delivery_id)
    }

    /// Wait until `delivery_id` reaches a terminal state.
    pub async fn settled(&self, delivery_id: &str) -> Delivery {
        eventually(&format!("{delivery_id} to settle"), || self.status(delivery_id).is_terminal()).await;
        self.get(delivery_id)
    }

    /// Wait until `delivery_id` is `pending` again after `attempts` attempts.
    pub async fn retrying_after(&self, delivery_id: &str, attempts: u32) -> Delivery {
        // While the attempt is in flight, `next_attempt_at` is the claim
        // lease; once the failure is recorded it is the retry time. Tests
        // never use a retry delay equal to the lease.
        eventually(&format!("{delivery_id} to fail attempt {attempts}"), || {
            let d = self.get(delivery_id);
            d.attempts == attempts
                && d.status == DeliveryStatus::Pending
                && d.last_attempt_at.is_some_and(|at| d.next_attempt_at != at + LEASE_MS)
        })
        .await;
        self.get(delivery_id)
    }
}

pub fn create_alert(registry: &Registry, name: &str, url: &str, secret: &str) -> String {
    registry
        .create_alert(NewAlert {
            name: name.into(),
            sql: ALERT_SQL.into(),
            webhook_url: url.into(),
            secret: secret.into(),
            wait_for_final: false,
        })
        .unwrap()
        .id
}

pub fn stored_payload_bytes(registry: &Registry, delivery_id: &str) -> Vec<u8> {
    registry
        .db()
        .with_conn("payload", |conn| {
            Ok(conn.query_row(
                "SELECT CAST(payload AS BLOB) FROM deliveries WHERE delivery_id = ?1",
                [delivery_id],
                |r| r.get(0),
            )?)
        })
        .unwrap()
}

/// Poll `condition` every 10 ms for up to 10 s.
pub async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Let the dispatcher run for `ms` of real time.
pub async fn idle(ms: u64) {
    tokio::time::sleep(Duration::from_millis(ms)).await;
}
