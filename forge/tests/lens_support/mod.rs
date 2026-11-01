//! Helpers shared by the LENS client tests: a scripted fake LENS on a raw
//! TCP socket, and a [`ForgeConfig`] pointing a client at it.
//!
//! The fake reads each HTTP/1.1 request itself, so a test sees exactly the
//! bytes FORGE sent, and answers with whatever [`Reply`] the script picks,
//! valid HTTP or not. Every answer closes the connection, so each request
//! arrives on a connection of its own and none is retried on a stale one.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use forge::config::ENV_LENS_URL;
use forge::{ForgeConfig, LensClient, LensClientSettings};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::common::Fixture;

/// Short timeouts and a fast poll, so failure paths run in milliseconds.
pub fn fast_settings() -> LensClientSettings {
    LensClientSettings {
        request_timeout: Duration::from_secs(3),
        connect_timeout: Duration::from_secs(1),
        visibility_poll_interval: Duration::from_millis(20),
    }
}

/// A config whose `lens_url` is `url`.
pub fn config_for(url: &str) -> ForgeConfig {
    Fixture::new().load(&[(ENV_LENS_URL, url)]).unwrap()
}

/// A client for `url` with `settings`.
pub fn client_for(url: &str, settings: LensClientSettings) -> LensClient {
    LensClient::with_settings(&config_for(url), settings).unwrap()
}

/// A URL nothing listens on: a port that was just free.
pub fn refused_url() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}")
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// One request as it arrived.
#[derive(Debug, Clone)]
pub struct Captured {
    pub method: String,
    /// The request target: path and query, as sent.
    pub target: String,
    pub version: String,
    /// In arrival order, names as sent.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// When the fake finished reading it.
    pub at: Instant,
}

impl Captured {
    /// Every value of header `name`, case-insensitively.
    pub fn header_values(&self, name: &str) -> Vec<&str> {
        self.headers
            .iter()
            .filter(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .collect()
    }

    /// The single value of header `name`; panics if it is absent or repeated.
    pub fn header(&self, name: &str) -> &str {
        match self.header_values(name).as_slice() {
            [value] => value,
            values => panic!("expected one {name} header, got {values:?} in {:?}", self.headers),
        }
    }

    pub fn has_header(&self, name: &str) -> bool {
        !self.header_values(name).is_empty()
    }

    pub fn body_text(&self) -> &str {
        std::str::from_utf8(&self.body).expect("request body is UTF-8")
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).expect("request body is JSON")
    }

    /// The `sql` of a `/query` request.
    pub fn sql(&self) -> String {
        self.json()["sql"].as_str().expect("sql is a string").to_owned()
    }
}

// ---------------------------------------------------------------------------
// Replies
// ---------------------------------------------------------------------------

/// What the fake does with a request.
#[derive(Debug, Clone)]
pub enum Reply {
    /// A well-formed HTTP/1.1 response, after `delay`.
    Http {
        status: u16,
        content_type: Option<String>,
        body: Vec<u8>,
        delay: Duration,
    },
    /// These bytes, verbatim, then close.
    Raw(Vec<u8>),
    /// Close without a byte.
    Close,
    /// Reset the connection (RST) without a byte.
    Reset,
    /// Hold the connection open and never answer.
    Hang,
    /// A status line and headers announcing `content_length` bytes, `sent`
    /// of the body, then hang.
    StallBody { content_length: usize, sent: Vec<u8> },
    /// As [`Reply::StallBody`], but close instead of hanging.
    TruncateBody { content_length: usize, sent: Vec<u8> },
}

impl Reply {
    pub fn json(status: u16, body: &Value) -> Self {
        Self::Http {
            status,
            content_type: Some("application/json".into()),
            body: body.to_string().into_bytes(),
            delay: Duration::ZERO,
        }
    }

    pub fn text(status: u16, content_type: &str, body: &str) -> Self {
        Self::Http {
            status,
            content_type: Some(content_type.into()),
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
        }
    }

    pub fn empty(status: u16) -> Self {
        Self::Http { status, content_type: None, body: Vec::new(), delay: Duration::ZERO }
    }

    /// A 200 `/query` body exactly as LENS's serializer lays it out.
    pub fn rows(rows: Value, sql: &str) -> Self {
        let count = rows.as_array().expect("rows is an array").len();
        let body = format!(
            "{{\"rows\":{rows},\"row_count\":{count},\"duration_ms\":3,\"truncated\":false,\"sql\":{}}}",
            Value::from(sql)
        );
        Self::text(200, "application/json", &body)
    }

    /// The answer to the visibility query: `max_slot`, or `NULL`.
    pub fn max_slot(max_slot: Option<u64>) -> Self {
        Self::rows(json!([{ "max_slot": max_slot }]), forge::VISIBILITY_SQL)
    }

    /// A failed `/query`, as LENS's `ErrorBody`.
    pub fn lens_error(status: u16, error_kind: &str, error: &str, sql: &str) -> Self {
        Self::json(status, &json!({ "error": error, "error_kind": error_kind, "sql": sql }))
    }

    /// LENS's 400 for a table its catalog has not registered (yet).
    pub fn table_not_found() -> Self {
        Self::lens_error(
            400,
            "query_plan_error",
            "table 'datafusion.solana.slots' not found",
            forge::VISIBILITY_SQL,
        )
    }

    pub fn after(self, wait: Duration) -> Self {
        match self {
            Self::Http { status, content_type, body, .. } => Self::Http { status, content_type, body, delay: wait },
            other => panic!("only an Http reply can be delayed, not {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// The fake
// ---------------------------------------------------------------------------

type Script = dyn Fn(&Captured, usize) -> Reply + Send + Sync;

/// A scripted LENS. Lives until the test's runtime shuts down.
pub struct FakeLens {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<Captured>>>,
    connections: Arc<AtomicUsize>,
}

impl FakeLens {
    /// Answer request `n` (0-based) with `script(request, n)`.
    pub async fn start(script: impl Fn(&Captured, usize) -> Reply + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let connections = Arc::new(AtomicUsize::new(0));
        let script: Arc<Script> = Arc::new(script);
        let (seen, accepted) = (Arc::clone(&requests), Arc::clone(&connections));
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(stream, Arc::clone(&script), Arc::clone(&seen)));
            }
        });
        Self { addr, requests, connections }
    }

    /// Answer every request with `reply`.
    pub async fn always(reply: Reply) -> Self {
        Self::start(move |_, _| reply.clone()).await
    }

    /// Answer request `n` with `replies[n]`, and every later one with the last.
    pub async fn sequence(replies: Vec<Reply>) -> Self {
        assert!(!replies.is_empty());
        Self::start(move |_, n| replies[n.min(replies.len() - 1)].clone()).await
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn client(&self) -> LensClient {
        client_for(&self.url(), fast_settings())
    }

    pub fn client_with(&self, settings: LensClientSettings) -> LensClient {
        client_for(&self.url(), settings)
    }

    /// Every request read so far, in arrival order.
    pub fn requests(&self) -> Vec<Captured> {
        self.requests.lock().unwrap().clone()
    }

    /// The only request read so far.
    pub fn only_request(&self) -> Captured {
        let requests = self.requests();
        assert_eq!(requests.len(), 1, "expected exactly one request, got {requests:#?}");
        requests.into_iter().next().unwrap()
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

async fn serve(mut stream: TcpStream, script: Arc<Script>, seen: Arc<Mutex<Vec<Captured>>>) {
    let Some(request) = read_request(&mut stream).await else {
        return;
    };
    let reply = {
        let mut seen = seen.lock().unwrap();
        let n = seen.len();
        seen.push(request.clone());
        script(&request, n)
    };
    match reply {
        Reply::Http { status, content_type, body, delay } => {
            tokio::time::sleep(delay).await;
            let mut head = status_line(status);
            if let Some(content_type) = content_type {
                head.push_str(&format!("Content-Type: {content_type}\r\n"));
            }
            head.push_str(&format!("Content-Length: {}\r\nConnection: close\r\n\r\n", body.len()));
            let _ = stream.write_all(head.as_bytes()).await;
            let _ = stream.write_all(&body).await;
            let _ = stream.shutdown().await;
        }
        Reply::Raw(bytes) => {
            let _ = stream.write_all(&bytes).await;
            let _ = stream.shutdown().await;
        }
        Reply::Close => drop(stream),
        Reply::Reset => {
            let _ = stream.set_zero_linger();
            drop(stream);
        }
        Reply::Hang => hang(stream).await,
        Reply::StallBody { content_length, sent } => {
            write_partial(&mut stream, content_length, &sent).await;
            hang(stream).await;
        }
        Reply::TruncateBody { content_length, sent } => {
            write_partial(&mut stream, content_length, &sent).await;
            let _ = stream.shutdown().await;
        }
    }
}

fn status_line(status: u16) -> String {
    let reason = StatusCode::from_u16(status).ok().and_then(|code| code.canonical_reason()).unwrap_or("Whatever");
    format!("HTTP/1.1 {status} {reason}\r\n")
}

async fn write_partial(stream: &mut TcpStream, content_length: usize, sent: &[u8]) {
    let head = format!(
        "{}Content-Type: application/json\r\nContent-Length: {content_length}\r\nConnection: close\r\n\r\n",
        status_line(200)
    );
    let _ = stream.write_all(head.as_bytes()).await;
    let _ = stream.write_all(sent).await;
    let _ = stream.flush().await;
}

/// Keep `stream` open, unanswered, until the runtime shuts down.
async fn hang(stream: TcpStream) {
    tokio::time::sleep(Duration::from_secs(3_600)).await;
    drop(stream);
}

/// Read one HTTP/1.1 request; `None` if the peer goes away first.
async fn read_request(stream: &mut TcpStream) -> Option<Captured> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|window| window == b"\r\n\r\n") {
            break pos;
        }
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8(buf[..head_end].to_vec()).expect("request head is UTF-8");
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next()?.split(' ');
    let (method, target, version) = (request_line.next()?, request_line.next()?, request_line.next()?);
    let headers: Vec<(String, String)> = lines
        .map(|line| {
            let (name, value) = line.split_once(':').expect("header has a colon");
            (name.to_owned(), value.trim().to_owned())
        })
        .collect();
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| value.parse::<usize>().expect("numeric content-length"));
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Some(Captured {
        method: method.to_owned(),
        target: target.to_owned(),
        version: version.to_owned(),
        headers,
        body,
        at: Instant::now(),
    })
}
