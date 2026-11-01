//! The LENS client: FORGE's only route to LENS.
//!
//! [`LensClient`] does three jobs:
//!
//! 1. [`LensClient::query`] sends SQL and returns typed rows.
//! 2. Every failure becomes a [`LensCallError`] whose variant tells the
//!    caller what to do: fix the SQL, retry, or report a bug.
//! 3. [`LensClient::wait_until_visible`] answers the visibility question:
//!    has LENS seen the slot PRISM just announced?
//!
//! It does not retry, bind SQL parameters, or cache. Retrying is the caller's
//! call, made with [`LensCallError::is_retryable`].
//!
//! # Wire format
//!
//! `POST {lens_url}/query`, `Content-Type: application/json`:
//!
//! ```json
//! {"sql":"SELECT slot FROM solana.slots LIMIT 5","format":"json"}
//! ```
//!
//! `format` is sent even though JSON is LENS's default, so a change of
//! default on the LENS side cannot hand FORGE CSV. A 200 carries a
//! [`LensResponse`] body:
//!
//! ```json
//! {"rows":[{"slot":295000000}],"row_count":1,"duration_ms":47,"truncated":false,"sql":"SELECT ..."}
//! ```
//!
//! `GET {lens_url}/health` always answers 200 when LENS is up; its verdict is
//! in the body's `status` field ([`LensHealthStatus`]).
//!
//! # Error classification
//!
//! | Outcome                                                    | Variant                         | Retryable |
//! |------------------------------------------------------------|---------------------------------|-----------|
//! | HTTP 400 from `/query`                                     | [`LensCallError::BadQuery`]     | no        |
//! | HTTP 408 or 504, or no full response within the timeout    | [`LensCallError::Timeout`]      | yes       |
//! | Connect failure, reset or dropped connection, other 5xx    | [`LensCallError::Unavailable`]  | yes       |
//! | Any other status, a reply that is not HTTP, an unparsable body | [`LensCallError::Protocol`] | no        |
//!
//! LENS reports its own query timeout as 504, not 408 (see `lens::api::query`),
//! so both are a [`LensCallError::Timeout`]. A connect timeout is
//! [`LensCallError::Unavailable`]: LENS could not be reached at all.
//!
//! A 200 body must parse completely: every row a JSON object, every required
//! field present with its type, and `row_count` equal to the number of rows.
//!
//! # Visibility
//!
//! PRISM announces a slot in the change log once its Parquet files are
//! written, but LENS only sees new files after its next catalog refresh.
//! [`LensClient::wait_until_visible`] polls [`VISIBILITY_SQL`] until LENS
//! reports a `max_slot` at or above the slot, or the timeout runs out. Every
//! failure along the way is recorded and polled through: until LENS's first
//! refresh after PRISM's first write, `solana.slots` does not exist and the
//! query is a [`LensCallError::BadQuery`].

use std::error::Error as StdError;
use std::time::{Duration, Instant};

use reqwest::header::{ACCEPT, CONTENT_TYPE};
use reqwest::{Client, RequestBuilder, StatusCode, redirect};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tracing::{debug, warn};

use crate::config::{ForgeConfig, ForgeError, ForgeResult};

/// Route of LENS's query endpoint, below `lens_url`.
pub const QUERY_PATH: &str = "/query";
/// Route of LENS's health endpoint, below `lens_url`.
pub const HEALTH_PATH: &str = "/health";
/// The `format` every query asks for.
pub const QUERY_FORMAT: &str = "json";
/// Media type of the request body, and the only one FORGE accepts back.
pub const JSON_CONTENT_TYPE: &str = "application/json";

/// What [`LensClient::wait_until_visible`] asks LENS. `NULL` while the table
/// is empty.
pub const VISIBILITY_SQL: &str = "SELECT MAX(slot) AS max_slot FROM solana.slots";
/// The column [`VISIBILITY_SQL`] returns.
pub const VISIBILITY_COLUMN: &str = "max_slot";

/// Above LENS's default `query_timeout_secs` (30), so LENS's own 504, which
/// says why, normally arrives before FORGE gives up.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(35);
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// LENS refreshes its catalog every 30 s by default; polling once a second
/// notices a refresh soon after it happens without loading LENS.
pub const DEFAULT_VISIBILITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Longest part of a response body quoted in an error message, in chars.
pub const MAX_BODY_EXCERPT_CHARS: usize = 512;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The result of a successful [`LensClient::query`]: LENS's 200 body plus
/// the round trip FORGE measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LensResponse {
    /// One object per row, column name to value. LENS writes every column
    /// of every row, `null`s included. Integers keep full `u64` precision.
    pub rows: Vec<Map<String, Value>>,
    /// Rows in `rows`, after truncation.
    pub row_count: u64,
    /// Time LENS spent planning and executing, in milliseconds.
    pub duration_ms: u64,
    /// Whether LENS dropped rows past its `max_query_rows`.
    #[serde(default)]
    pub truncated: bool,
    /// The SQL as LENS ran it (trimmed).
    pub sql: String,
    /// From just before the request was sent until the whole body was read,
    /// measured by FORGE. Never read from the body.
    #[serde(skip_deserializing)]
    pub round_trip_ms: u64,
}

/// Why a call to LENS failed, by what the caller can do about it. See the
/// [module docs](self) for the full table.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LensCallError {
    /// HTTP 400: the SQL or a table it names is wrong. Not retryable.
    #[error("LENS rejected the query: {message}")]
    BadQuery { message: String },
    /// HTTP 408 or 504, or no full response within the client timeout.
    /// Retryable.
    #[error("LENS timed out: {message}")]
    Timeout { message: String },
    /// LENS could not be reached, dropped the connection, or answered with a
    /// 5xx other than 504. Retryable.
    #[error("LENS is unavailable: {message}")]
    Unavailable { message: String },
    /// LENS answered with an unexpected status, or with something FORGE
    /// cannot parse. Not retryable: one side does not speak the other's
    /// protocol.
    #[error("unexpected response from LENS: {message}")]
    Protocol { message: String },
}

impl LensCallError {
    /// Whether the same call may succeed if made again later.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout { .. } | Self::Unavailable { .. })
    }

    /// Stable `snake_case` name of the variant, for logs and metric labels.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::BadQuery { .. } => "bad_query",
            Self::Timeout { .. } => "timeout",
            Self::Unavailable { .. } => "unavailable",
            Self::Protocol { .. } => "protocol",
        }
    }

    /// The detail, without the variant's prefix.
    pub fn message(&self) -> &str {
        match self {
            Self::BadQuery { message }
            | Self::Timeout { message }
            | Self::Unavailable { message }
            | Self::Protocol { message } => message,
        }
    }
}

/// LENS's verdict on itself, the `status` field of `GET /health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LensHealthStatus {
    Healthy,
    /// Serving, but a table is stranded or a catalog refresh failed.
    Degraded,
    /// Up, but no table is registered: every query will fail.
    Unhealthy,
}

impl LensHealthStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    /// Whether LENS can answer queries: healthy or degraded.
    pub const fn is_serving(self) -> bool {
        !matches!(self, Self::Unhealthy)
    }
}

/// The result of a successful [`LensClient::health`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct LensHealth {
    pub status: LensHealthStatus,
    /// Measured by FORGE, like [`LensResponse::round_trip_ms`].
    pub round_trip_ms: u64,
}

/// The answer of [`LensClient::wait_until_visible`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Visibility {
    /// LENS reported `max_slot`, at or above the slot waited for.
    Visible { max_slot: u64 },
    /// The timeout ran out first.
    TimedOut {
        /// `max_slot` from the last poll LENS answered; `None` when it never
        /// answered, or its last answer was an empty table.
        last_seen: Option<u64>,
        /// The error of the last poll, if that poll failed.
        last_error: Option<String>,
    },
}

impl Visibility {
    pub const fn is_visible(&self) -> bool {
        matches!(self, Self::Visible { .. })
    }
}

/// Timeouts and pacing of a [`LensClient`]. Every duration must be non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LensClientSettings {
    /// Longest a call may take, from connecting to the last body byte.
    pub request_timeout: Duration,
    /// Longest connecting may take.
    pub connect_timeout: Duration,
    /// Pause between two polls of [`LensClient::wait_until_visible`].
    pub visibility_poll_interval: Duration,
}

impl Default for LensClientSettings {
    fn default() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            visibility_poll_interval: DEFAULT_VISIBILITY_POLL_INTERVAL,
        }
    }
}

/// The body of `POST /query`, as `lens::api::QueryRequest` reads it.
#[derive(Serialize)]
struct QueryRequestBody<'a> {
    sql: &'a str,
    format: &'static str,
}

/// The fields FORGE reads from a failed `/query`'s body
/// (`lens::api::ErrorBody`).
#[derive(Deserialize)]
struct ErrorBody {
    error: String,
    error_kind: String,
}

/// The field FORGE reads from `GET /health`'s body.
#[derive(Deserialize)]
struct HealthBody {
    status: LensHealthStatus,
}

/// Which endpoint a response came from: a 400 only means bad SQL on `/query`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Endpoint {
    Query,
    Health,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// FORGE's handle on LENS. Cheap to clone; clones share one connection pool.
#[derive(Debug, Clone)]
pub struct LensClient {
    http: Client,
    query_url: String,
    health_url: String,
    settings: LensClientSettings,
}

impl LensClient {
    /// A client for [`ForgeConfig::lens_url`] with the default settings.
    pub fn new(config: &ForgeConfig) -> ForgeResult<Self> {
        Self::with_settings(config, LensClientSettings::default())
    }

    /// A client for [`ForgeConfig::lens_url`] with `settings`.
    ///
    /// Proxy environment variables are ignored: FORGE talks to the LENS it
    /// was configured with, directly. Redirects are not followed, since LENS
    /// never sends one. Fails with a [`ForgeError::LensClientError`] when a
    /// duration is zero or the HTTP client cannot be built.
    pub fn with_settings(config: &ForgeConfig, settings: LensClientSettings) -> ForgeResult<Self> {
        let base = config.lens_url();
        for (name, value) in [
            ("request_timeout", settings.request_timeout),
            ("connect_timeout", settings.connect_timeout),
            ("visibility_poll_interval", settings.visibility_poll_interval),
        ] {
            if value.is_zero() {
                return Err(ForgeError::lens_client(base, format!("{name} must be greater than zero")));
            }
        }
        let http = Client::builder()
            .timeout(settings.request_timeout)
            .connect_timeout(settings.connect_timeout)
            .redirect(redirect::Policy::none())
            .no_proxy()
            // Other crates in the workspace enable reqwest's decompression
            // features; keep FORGE's requests the same either way.
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .build()
            .map_err(|err| ForgeError::lens_client(base, err))?;
        Ok(Self {
            http,
            query_url: format!("{base}{QUERY_PATH}"),
            health_url: format!("{base}{HEALTH_PATH}"),
            settings,
        })
    }

    /// Run `sql` on LENS and return its rows. Never retries.
    pub async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        self.query_within(sql, self.settings.request_timeout).await
    }

    /// `GET /health`: LENS's verdict on itself and the round trip. Any
    /// answer means LENS is reachable, an `unhealthy` one included.
    pub async fn health(&self) -> Result<LensHealth, LensCallError> {
        let what = format!("GET {}", self.health_url);
        let started = Instant::now();
        let request = self.http.get(&self.health_url).header(ACCEPT, JSON_CONTENT_TYPE);
        let (status, body) = exchange(request, &what).await?;
        let round_trip_ms = millis(started.elapsed());
        if status != StatusCode::OK {
            return Err(classify_status(status, &body, Endpoint::Health, &what));
        }
        let HealthBody { status } = serde_json::from_slice(&body).map_err(|err| LensCallError::Protocol {
            message: format!("{what} answered 200 with a body that is not a LENS health report: {err}; body: {}", excerpt(&body)),
        })?;
        Ok(LensHealth { status, round_trip_ms })
    }

    /// Poll LENS until it reports a `max_slot` at or above `slot`, or
    /// `timeout` runs out. See the [module docs](self#visibility).
    ///
    /// Returns within `timeout`, plus scheduling slack: each poll may take
    /// only the time left. A zero `timeout` returns
    /// [`Visibility::TimedOut`] without asking LENS. Polls are
    /// [`LensClientSettings::visibility_poll_interval`] apart; every failure
    /// is polled through, whatever its kind.
    ///
    /// A last poll that the deadline cuts short is not reported: it says
    /// nothing about LENS, and would hide what the polls before it found.
    /// When it was the only poll, its timeout is all there is to report.
    pub async fn wait_until_visible(&self, slot: u64, timeout: Duration) -> Visibility {
        let started = Instant::now();
        let mut last_seen = None;
        let mut last_error = None;
        let mut polls = 0u32;
        loop {
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            polls += 1;
            let budget = remaining.min(self.settings.request_timeout);
            match self.query_within(VISIBILITY_SQL, budget).await.and_then(|response| max_slot_of(&response)) {
                Ok(Some(max_slot)) if max_slot >= slot => {
                    debug!(slot, max_slot, polls, waited_ms = millis(started.elapsed()), "slot visible in LENS");
                    return Visibility::Visible { max_slot };
                }
                Ok(seen) => {
                    debug!(slot, max_slot = seen, polls, "slot not visible in LENS yet");
                    last_seen = seen;
                    last_error = None;
                }
                Err(err) if polls > 1 && cut_short(&err, budget, self.settings.request_timeout, timeout, started) => {
                    // Says only that time ran out; what earlier polls found
                    // describes LENS better.
                    debug!(slot, polls, "last visibility poll cut short by the timeout");
                }
                Err(err) => {
                    debug!(slot, polls, kind = err.kind(), error = %err, "visibility poll failed");
                    last_error = Some(err.to_string());
                }
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                break;
            }
            tokio::time::sleep(remaining.min(self.settings.visibility_poll_interval)).await;
        }
        warn!(
            slot,
            last_seen,
            last_error = last_error.as_deref(),
            polls,
            timeout_ms = millis(timeout),
            "slot not visible in LENS before the timeout"
        );
        Visibility::TimedOut { last_seen, last_error }
    }

    /// `{lens_url}/query`.
    pub fn query_url(&self) -> &str {
        &self.query_url
    }

    /// `{lens_url}/health`.
    pub fn health_url(&self) -> &str {
        &self.health_url
    }

    pub const fn settings(&self) -> &LensClientSettings {
        &self.settings
    }

    /// [`Self::query`], giving up after `timeout`.
    async fn query_within(&self, sql: &str, timeout: Duration) -> Result<LensResponse, LensCallError> {
        let what = format!("POST {}", self.query_url);
        let body = serde_json::to_vec(&QueryRequestBody { sql, format: QUERY_FORMAT })
            .expect("a struct of two strings always serializes");
        let started = Instant::now();
        let request = self
            .http
            .post(&self.query_url)
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE)
            .header(ACCEPT, JSON_CONTENT_TYPE)
            .timeout(timeout)
            .body(body);
        let (status, body) = exchange(request, &what).await?;
        let round_trip_ms = millis(started.elapsed());
        if status != StatusCode::OK {
            let err = classify_status(status, &body, Endpoint::Query, &what);
            debug!(sql, kind = err.kind(), error = %err, round_trip_ms, "LENS query failed");
            return Err(err);
        }
        let response = parse_query_body(&body, round_trip_ms, &what)?;
        debug!(
            sql,
            rows = response.row_count,
            truncated = response.truncated,
            duration_ms = response.duration_ms,
            round_trip_ms,
            "LENS query succeeded"
        );
        Ok(response)
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// Send `request` and read the whole body.
async fn exchange(request: RequestBuilder, what: &str) -> Result<(StatusCode, Vec<u8>), LensCallError> {
    let response = request.send().await.map_err(|err| classify_transport(&err, what))?;
    let status = response.status();
    let body = response.bytes().await.map_err(|err| classify_transport(&err, what))?;
    Ok((status, body.into()))
}

/// A 200 body from `/query`, checked to be a complete [`LensResponse`].
fn parse_query_body(body: &[u8], round_trip_ms: u64, what: &str) -> Result<LensResponse, LensCallError> {
    let mut response: LensResponse = serde_json::from_slice(body).map_err(|err| LensCallError::Protocol {
        message: format!("{what} answered 200 with a body that is not a LENS query result: {err}; body: {}", excerpt(body)),
    })?;
    if response.rows.len() as u64 != response.row_count {
        return Err(LensCallError::Protocol {
            message: format!(
                "{what} answered 200 with row_count {} but {} rows",
                response.row_count,
                response.rows.len()
            ),
        });
    }
    response.round_trip_ms = round_trip_ms;
    Ok(response)
}

/// Whether a visibility poll failed only because the gate's own deadline,
/// rather than the request timeout, stopped it.
fn cut_short(err: &LensCallError, budget: Duration, request_timeout: Duration, timeout: Duration, started: Instant) -> bool {
    matches!(err, LensCallError::Timeout { .. })
        && budget < request_timeout
        && timeout.saturating_sub(started.elapsed()).is_zero()
}

/// The `max_slot` of a [`VISIBILITY_SQL`] result; `None` for an empty table.
fn max_slot_of(response: &LensResponse) -> Result<Option<u64>, LensCallError> {
    let protocol = |message: String| LensCallError::Protocol { message };
    let [row] = response.rows.as_slice() else {
        return Err(protocol(format!("visibility query returned {} rows, expected 1", response.rows.len())));
    };
    match row.get(VISIBILITY_COLUMN) {
        Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| protocol(format!("visibility query returned {VISIBILITY_COLUMN} = {value}, not a slot"))),
        None => Err(protocol(format!("visibility query returned no {VISIBILITY_COLUMN} column"))),
    }
}

// ---------------------------------------------------------------------------
// Classification
// ---------------------------------------------------------------------------

/// The [`LensCallError`] for a non-200 `status`. See the [module docs](self).
fn classify_status(status: StatusCode, body: &[u8], endpoint: Endpoint, what: &str) -> LensCallError {
    let detail = match serde_json::from_slice::<ErrorBody>(body) {
        Ok(ErrorBody { error, error_kind }) => format!("{error_kind}: {error}"),
        Err(_) if body.iter().all(u8::is_ascii_whitespace) => "empty body".to_owned(),
        Err(_) => excerpt(body),
    };
    match status.as_u16() {
        // What whoever wrote the SQL needs to read, without the plumbing.
        400 if endpoint == Endpoint::Query => LensCallError::BadQuery { message: detail },
        408 | 504 => LensCallError::Timeout {
            message: format!("{what} answered {status}: {detail}"),
        },
        500..=599 => LensCallError::Unavailable {
            message: format!("{what} answered {status}: {detail}"),
        },
        _ => LensCallError::Protocol {
            message: format!("{what} answered with unexpected status {status}: {detail}"),
        },
    }
}

/// The [`LensCallError`] for a request that got no complete HTTP response.
fn classify_transport(err: &reqwest::Error, what: &str) -> LensCallError {
    let detail = error_chain(err);
    // Before the timeout check: a connect timeout means LENS is unreachable,
    // not slow.
    if err.is_connect() {
        return LensCallError::Unavailable {
            message: format!("{what}: cannot connect to LENS: {detail}"),
        };
    }
    if err.is_timeout() {
        return LensCallError::Timeout {
            message: format!("{what}: no complete response within the timeout: {detail}"),
        };
    }
    let malformed = find_source::<hyper::Error>(err)
        .is_some_and(|hyper| hyper.is_parse() || hyper.is_parse_status() || hyper.is_user());
    if malformed || err.is_builder() || err.is_redirect() {
        return LensCallError::Protocol {
            message: format!("{what}: the response is not valid HTTP: {detail}"),
        };
    }
    LensCallError::Unavailable {
        message: format!("{what}: the connection to LENS failed: {detail}"),
    }
}

/// The first error of type `T` in `err`'s source chain, `err` included.
fn find_source<'a, T: StdError + 'static>(err: &'a (dyn StdError + 'static)) -> Option<&'a T> {
    let mut current = Some(err);
    while let Some(err) = current {
        if let Some(found) = err.downcast_ref::<T>() {
            return Some(found);
        }
        current = err.source();
    }
    None
}

/// `err` and its causes, `": "`-separated, skipping a cause whose text the
/// line already holds.
fn error_chain(err: &(dyn StdError + 'static)) -> String {
    let mut line = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !line.contains(&text) {
            line.push_str(": ");
            line.push_str(&text);
        }
        source = cause.source();
    }
    line
}

/// Up to [`MAX_BODY_EXCERPT_CHARS`] of `body`, trimmed, lossily decoded.
fn excerpt(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    match text.char_indices().nth(MAX_BODY_EXCERPT_CHARS) {
        Some((cut, _)) => format!("{}... ({} bytes)", &text[..cut], body.len()),
        None => text.to_owned(),
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WHAT: &str = "POST http://lens/query";

    fn status(code: u16) -> StatusCode {
        StatusCode::from_u16(code).unwrap()
    }

    fn response(rows: Value) -> LensResponse {
        let rows: Vec<Map<String, Value>> = serde_json::from_value(rows).unwrap();
        LensResponse {
            row_count: rows.len() as u64,
            rows,
            duration_ms: 1,
            truncated: false,
            sql: VISIBILITY_SQL.to_owned(),
            round_trip_ms: 2,
        }
    }

    // -- classify_status -----------------------------------------------------

    #[test]
    fn every_non_200_status_lands_in_exactly_the_documented_variant() {
        for code in 100..=599u16 {
            if code == 200 {
                continue;
            }
            let query = classify_status(status(code), b"", Endpoint::Query, WHAT);
            let expected = match code {
                400 => "bad_query",
                408 | 504 => "timeout",
                500..=599 => "unavailable",
                _ => "protocol",
            };
            assert_eq!(query.kind(), expected, "HTTP {code} from /query");

            // A 400 from /health is not about any SQL.
            let health = classify_status(status(code), b"", Endpoint::Health, WHAT);
            let expected = if code == 400 { "protocol" } else { expected };
            assert_eq!(health.kind(), expected, "HTTP {code} from /health");
        }
    }

    #[test]
    fn only_timeouts_and_unavailability_are_retryable() {
        let retryable = |code| classify_status(status(code), b"", Endpoint::Query, WHAT).is_retryable();
        for code in [408, 500, 502, 503, 504, 599] {
            assert!(retryable(code), "{code}");
        }
        for code in [201, 204, 301, 400, 401, 403, 404, 405, 413, 415, 422, 429] {
            assert!(!retryable(code), "{code}");
        }
    }

    #[test]
    fn a_lens_error_body_becomes_kind_and_message() {
        let body = br#"{"error":"table 'datafusion.solana.nope' not found","error_kind":"query_plan_error","sql":"SELECT * FROM solana.nope"}"#;
        let err = classify_status(status(400), body, Endpoint::Query, WHAT);
        assert_eq!(
            err,
            LensCallError::BadQuery { message: "query_plan_error: table 'datafusion.solana.nope' not found".into() }
        );

        let body = br#"{"error":"query exceeded the 30s timeout and was cancelled","error_kind":"query_timeout","sql":"SELECT 1"}"#;
        let err = classify_status(status(504), body, Endpoint::Query, WHAT);
        assert_eq!(
            err.message(),
            "POST http://lens/query answered 504 Gateway Timeout: query_timeout: query exceeded the 30s timeout and was cancelled"
        );
    }

    #[test]
    fn a_foreign_error_body_is_quoted_and_an_empty_one_named() {
        let err = classify_status(status(502), b"<html>Bad Gateway</html>\n", Endpoint::Query, WHAT);
        assert_eq!(err.message(), "POST http://lens/query answered 502 Bad Gateway: <html>Bad Gateway</html>");
        let err = classify_status(status(404), b"  \n", Endpoint::Query, WHAT);
        assert_eq!(err.message(), "POST http://lens/query answered with unexpected status 404 Not Found: empty body");
        // A JSON body without LENS's fields is quoted, not half-parsed.
        let err = classify_status(status(400), br#"{"error":"x"}"#, Endpoint::Query, WHAT);
        assert_eq!(err.message(), r#"{"error":"x"}"#);
    }

    #[test]
    fn excerpts_are_bounded_and_char_safe() {
        let long = "é".repeat(MAX_BODY_EXCERPT_CHARS + 10);
        let quoted = excerpt(long.as_bytes());
        assert!(quoted.starts_with(&"é".repeat(MAX_BODY_EXCERPT_CHARS)), "{quoted}");
        assert!(quoted.ends_with(&format!("... ({} bytes)", long.len())), "{quoted}");
        assert_eq!(excerpt("é".repeat(MAX_BODY_EXCERPT_CHARS).as_bytes()), "é".repeat(MAX_BODY_EXCERPT_CHARS));
        assert_eq!(excerpt(b"\xff\xfe ok"), "\u{fffd}\u{fffd} ok");
    }

    // -- parse_query_body ----------------------------------------------------

    #[test]
    fn a_lens_body_parses_field_for_field() {
        let body = br#"{"rows":[{"slot":295000000,"blockhash":"4vJ9Sh","leader":null}],"row_count":1,"duration_ms":47,"truncated":false,"sql":"SELECT slot FROM solana.slots"}"#;
        let parsed = parse_query_body(body, 9, WHAT).unwrap();
        let expected: Map<String, Value> =
            serde_json::from_str(r#"{"slot":295000000,"blockhash":"4vJ9Sh","leader":null}"#).unwrap();
        assert_eq!(
            parsed,
            LensResponse {
                rows: vec![expected],
                row_count: 1,
                duration_ms: 47,
                truncated: false,
                sql: "SELECT slot FROM solana.slots".into(),
                round_trip_ms: 9,
            }
        );
    }

    #[test]
    fn round_trip_comes_from_forge_even_if_the_body_has_one() {
        let body = br#"{"rows":[],"row_count":0,"duration_ms":3,"sql":"S","round_trip_ms":99999}"#;
        let parsed = parse_query_body(body, 4, WHAT).unwrap();
        assert_eq!(parsed.round_trip_ms, 4);
        assert!(!parsed.truncated, "truncated defaults to false");
    }

    #[test]
    fn bodies_that_are_not_a_complete_lens_result_are_protocol_errors() {
        for body in [
            "",
            "not json",
            "[]",
            "null",
            r#"{"rows":[],"row_count":0,"duration_ms":0}"#,
            r#"{"rows":[],"duration_ms":0,"sql":"S"}"#,
            r#"{"row_count":0,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[[1,2]],"row_count":1,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[1],"row_count":1,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":{},"row_count":0,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[],"row_count":-1,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[],"row_count":"0","duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[],"row_count":0,"duration_ms":1.5,"sql":"S"}"#,
            r#"{"rows":[],"row_count":0,"duration_ms":0,"sql":"S","truncated":"no"}"#,
            r#"{"rows":[],"row_count":0,"duration_ms":0,"sql":null}"#,
            r#"{"rows":[],"rows":[],"row_count":0,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[],"row_count":0,"duration_ms":0,"sql":"S"} trailing"#,
            r#"{"rows":[{}],"row_count":0,"duration_ms":0,"sql":"S"}"#,
            r#"{"rows":[],"row_count":1,"duration_ms":0,"sql":"S"}"#,
        ] {
            let err = parse_query_body(body.as_bytes(), 0, WHAT).unwrap_err();
            assert_eq!(err.kind(), "protocol", "{body}: {err}");
            assert!(!err.is_retryable());
        }
    }

    // -- max_slot_of ---------------------------------------------------------

    #[test]
    fn max_slot_reads_a_slot_or_an_empty_table() {
        assert_eq!(max_slot_of(&response(serde_json::json!([{ "max_slot": 350123457u64 }]))), Ok(Some(350123457)));
        assert_eq!(max_slot_of(&response(serde_json::json!([{ "max_slot": u64::MAX }]))), Ok(Some(u64::MAX)));
        assert_eq!(max_slot_of(&response(serde_json::json!([{ "max_slot": 0 }]))), Ok(Some(0)));
        assert_eq!(max_slot_of(&response(serde_json::json!([{ "max_slot": null }]))), Ok(None));
    }

    #[test]
    fn max_slot_rejects_any_other_shape() {
        for rows in [
            serde_json::json!([]),
            serde_json::json!([{ "max_slot": 1 }, { "max_slot": 2 }]),
            serde_json::json!([{ "slot": 1 }]),
            serde_json::json!([{ "max_slot": -1 }]),
            serde_json::json!([{ "max_slot": 1.5 }]),
            serde_json::json!([{ "max_slot": "1" }]),
        ] {
            let err = max_slot_of(&response(rows.clone())).unwrap_err();
            assert_eq!(err.kind(), "protocol", "{rows}");
        }
    }

    // -- the rest ------------------------------------------------------------

    #[test]
    fn the_query_body_is_exactly_what_lens_reads() {
        let body = serde_json::to_string(&QueryRequestBody { sql: "SELECT \"a\"\n", format: QUERY_FORMAT }).unwrap();
        assert_eq!(body, r#"{"sql":"SELECT \"a\"\n","format":"json"}"#);
    }

    #[test]
    fn health_status_parses_every_lens_verdict_and_nothing_else() {
        for (raw, status) in [
            ("healthy", LensHealthStatus::Healthy),
            ("degraded", LensHealthStatus::Degraded),
            ("unhealthy", LensHealthStatus::Unhealthy),
        ] {
            let body: HealthBody = serde_json::from_str(&format!(r#"{{"status":"{raw}"}}"#)).unwrap();
            assert_eq!(body.status, status);
            assert_eq!(status.as_str(), raw);
        }
        for bad in ["Healthy", "ok", ""] {
            assert!(serde_json::from_str::<HealthBody>(&format!(r#"{{"status":"{bad}"}}"#)).is_err(), "{bad}");
        }
        assert!(LensHealthStatus::Degraded.is_serving());
        assert!(!LensHealthStatus::Unhealthy.is_serving());
    }

    #[test]
    fn error_chain_joins_causes_without_repeating_them() {
        #[derive(Debug, Error)]
        #[error("outer")]
        struct Outer(#[source] Middle);
        #[derive(Debug, Error)]
        #[error("middle: inner")]
        struct Middle(#[source] std::io::Error);

        let err = Outer(Middle(std::io::Error::other("inner")));
        assert_eq!(error_chain(&err), "outer: middle: inner");
        assert!(find_source::<std::io::Error>(&err).is_some());
        assert!(find_source::<std::fmt::Error>(&err).is_none());
    }

    #[test]
    fn error_display_names_the_category() {
        let err = LensCallError::Timeout { message: "m".into() };
        assert_eq!(err.to_string(), "LENS timed out: m");
        assert_eq!(err.message(), "m");
        assert_eq!(LensCallError::BadQuery { message: "m".into() }.to_string(), "LENS rejected the query: m");
    }

    #[test]
    fn visibility_serializes_with_its_outcome() {
        let visible = serde_json::to_value(Visibility::Visible { max_slot: 7 }).unwrap();
        assert_eq!(visible, serde_json::json!({ "outcome": "visible", "max_slot": 7 }));
        let timed_out = serde_json::to_value(Visibility::TimedOut { last_seen: None, last_error: Some("e".into()) }).unwrap();
        assert_eq!(timed_out, serde_json::json!({ "outcome": "timed_out", "last_seen": null, "last_error": "e" }));
    }
}
