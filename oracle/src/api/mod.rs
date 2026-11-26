//! API: ORACLE over HTTP.
//!
//! | Route                                              | Success | File          |
//! |----------------------------------------------------|---------|---------------|
//! | `POST /query`                                      | 200     | [`query`]     |
//! | `POST /ask`                                        | 200     | [`query`]     |
//! | `POST /drill`                                      | 200     | [`query`]     |
//! | `GET /suggest?prefix=&limit=`                      | 200     | [`query`]     |
//! | `GET /templates`                                   | 200     | [`query`]     |
//! | `GET /catalog`                                     | 200     | [`query`]     |
//! | `GET /anomalies?metric=&subject=&since_bucket=&until_bucket=&limit=` | 200 | [`anomalies`] |
//! | `GET /detectors`                                   | 200     | [`anomalies`] |
//! | `PATCH /detectors/{id}`                            | 200     | [`anomalies`] |
//! | `POST /detectors/{id}/reset-progress`              | 200     | [`anomalies`] |
//! | `GET /profile/{kind}/{id}?window_slots=&as_of_slot=` | 200   | [`profile`]   |
//! | `GET /profile/transaction/{signature}?slot=&window_slots=&as_of_slot=` | 200 | [`profile`] |
//! | `GET /profile/slot/{slot}?after_index=&limit=&as_of_slot=` | 200 | [`profile`] |
//! | `GET /quality?window_slots=&as_of_slot=&max_age_secs=` | 200 | [`quality`]   |
//! | `GET /digest`                                      | 200, 404 if none yet | [`digest`] |
//! | `GET /digest/preview?window_start=`                | 200     | [`digest`]    |
//! | `GET /digests?limit=`                              | 200     | [`digest`]    |
//! | `GET /health`                                      | 200 always | [`health`] |
//!
//! # Errors
//!
//! Every failure, from any route, an unknown route, a panic and a timed-out
//! request included, has one body, FORGE's shape plus the request id:
//!
//! ```json
//! {"error": "invalid limit: must be 1..=10, got 11", "error_kind": "InvalidRequest", "field": "limit", "request_id": "…"}
//! ```
//!
//! `field` is present only when the failure names one. A handler never
//! builds that body: it returns an [`ApiError`], whose `IntoResponse` only
//! records it on the response, and the error-renderer layer turns it into
//! JSON ([`finish`]), the one place that knows the request id. The
//! conversions from every module's error type are here too, so no handler
//! repeats a mapping.
//!
//! | Source | HTTP | `error_kind` |
//! |---|---|---|
//! | a module's `Invalid`, a malformed JSON body, an unknown field, a bad query parameter | 400 | `InvalidRequest` |
//! | unknown route, unknown detector id, no digest yet | 404 | `NotFound` |
//! | wrong method (with an `Allow` header) | 405 | `MethodNotAllowed` |
//! | body over [`MAX_BODY_BYTES`] | 413 | `PayloadTooLarge` |
//! | wrong or missing `Content-Type` on a JSON route | 415 | `UnsupportedMediaType` |
//! | digest preview already running | 429, `Retry-After: 5` | `Busy` |
//! | LENS has no data or table yet | 503, `Retry-After: 30` | `NotReady` |
//! | LENS unreachable | 503 | `UpstreamUnavailable` |
//! | shutting down | 503 | `ShuttingDown` |
//! | LENS or a module deadline exceeded | 504 | `UpstreamTimeout` |
//! | the route's own time budget exceeded | 504 | `Timeout` |
//! | a profile whose result LENS truncated | 502 | `UpstreamError` |
//! | a module's `Internal`/`Permanent`, a store error, a panic, an unreadable stored body | 500 | `Internal`, generic message |
//!
//! An `Internal` error's detail is logged with the request id and never
//! sent: the client gets a fixed sentence naming the id.
//!
//! # Strict input
//!
//! JSON bodies need `Content-Type: application/json` (or `*+json`), at most
//! [`MAX_BODY_BYTES`], and must match the route's type exactly: unknown
//! fields and wrong types are a 400, never a 422 and never ignored. Query
//! strings are decoded strictly ([`StrictQuery`]): a malformed escape, a key
//! given twice or a key the route does not take is a 400, and numbers are
//! ASCII digits only. A value out of range is rejected, never clamped.
//!
//! # Routes that cost real work
//!
//! `/profile` (the detail reports included), `/quality`, `/digest/preview`,
//! `/query`, `/drill` and `/ask` (when it executes) run LENS queries. Each is bounded by the module it calls, never by the
//! API: the spec runner and the profiler hold a small permit pool, the
//! auditor runs one audit at a time and shares a fresh default report
//! between callers, and only one preview runs at a time (a second is a 429).
//! Each also has its own time budget (below) and ends with `ShuttingDown`
//! once ORACLE stops. `/health` costs one bounded `MAX(slot)` query and a
//! store stats read; it never runs an audit.
//!
//! # Routes that change state
//!
//! `PATCH /detectors/{id}` and `POST /detectors/{id}/reset-progress` are the
//! only writes. Both log at `info` with the request id, the detector and
//! what changed; both answer 404 for an unknown id before touching anything.
//!
//! # Layers
//!
//! [`router`] adds, outermost first:
//!
//! 1. **Request id and trace span** ([`request_id`]): a UUID v4 in the
//!    request's extensions and the `X-Request-Id` response header; a span
//!    with the method, route pattern and id; one `info` line per request
//!    with status and latency. Bodies and query strings are never logged.
//! 2. **Panic catcher**: `CatchPanicLayer` turns a panic into an
//!    [`ApiError::Internal`].
//! 3. **Error renderer** ([`render`]): the JSON body of every error, and
//!    `Content-Type: application/json`, `Cache-Control: no-store` and
//!    `X-Content-Type-Options: nosniff` on every response.
//! 4. **Body limit**: `DefaultBodyLimit` of [`MAX_BODY_BYTES`].
//! 5. **Per-route-group time budgets** ([`Budgets`]), each just above the
//!    deadline of the module the group calls, so a module's own timeout
//!    (which says why) normally wins.
//!
//! The request-id layer runs [`finish`] once more as a backstop, so the
//! panic catcher's error, which is produced outside the renderer, is
//! rendered by the same function.

pub mod anomalies;
pub mod digest;
pub mod health;
pub mod profile;
pub mod quality;
pub mod query;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{DefaultBodyLimit, FromRequest, FromRequestParts, MatchedPath, Path, Request, State};
use axum::http::request::Parts;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tower_http::catch_panic::CatchPanicLayer;
use tracing::Instrument;

use crate::AppState;
use crate::digest::{BUILD_DEADLINE_SECS, DigestError, QUALITY_WAIT_SECS};
use crate::profile::{PROFILE_DEADLINE_SECS, ProfileError};
use crate::quality::{AUDIT_DEADLINE_SECS, QualityError};
use crate::spec::{SPEC_QUERIES, SpecError};
use crate::store::StoreError;
use crate::{OracleConfig, OracleError};

/// Largest request body accepted. A `QuerySpec` is far smaller.
pub const MAX_BODY_BYTES: usize = 64 * 1024;
/// The response header carrying the request id.
pub const REQUEST_ID_HEADER: &str = "x-request-id";
/// `Retry-After` of a [`ApiError::Busy`].
pub const BUSY_RETRY_AFTER_SECS: u64 = 5;
/// `Retry-After` of a [`ApiError::NotReady`].
pub const NOT_READY_RETRY_AFTER_SECS: u64 = 30;
/// Budget of the routes that do no LENS work beyond a health probe.
pub const CHEAP_BUDGET: Duration = Duration::from_secs(10);
/// Margin each budget leaves above the deadline it covers.
pub const BUDGET_MARGIN: Duration = Duration::from_secs(5);
/// The message of every [`ApiError::Internal`]; the detail goes to the log.
pub const INTERNAL_MESSAGE: &str = "internal error; the server log has the details under this request_id";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Every way a request fails. See the [module docs](self#errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    InvalidRequest { message: String, field: Option<String> },
    NotFound(String),
    MethodNotAllowed,
    PayloadTooLarge,
    UnsupportedMediaType,
    Busy,
    NotReady(String),
    UpstreamUnavailable(String),
    ShuttingDown,
    UpstreamTimeout(String),
    /// The route's own budget ran out.
    Timeout(Duration),
    UpstreamError(String),
    /// The detail is logged, never sent.
    Internal(String),
}

impl ApiError {
    /// A 400 on `field`, worded as the modules word theirs.
    pub fn invalid(field: impl Into<String>, reason: impl std::fmt::Display) -> Self {
        let field = field.into();
        Self::InvalidRequest { message: format!("invalid {field}: {reason}"), field: Some(field) }
    }

    /// A 400 naming no field.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest { message: message.into(), field: None }
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(detail.into())
    }

    pub const fn status(&self) -> StatusCode {
        match self {
            Self::InvalidRequest { .. } => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::Busy => StatusCode::TOO_MANY_REQUESTS,
            Self::NotReady(_) | Self::UpstreamUnavailable(_) | Self::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
            Self::UpstreamTimeout(_) | Self::Timeout(_) => StatusCode::GATEWAY_TIMEOUT,
            Self::UpstreamError(_) => StatusCode::BAD_GATEWAY,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The `error_kind` of the body.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InvalidRequest { .. } => "InvalidRequest",
            Self::NotFound(_) => "NotFound",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::PayloadTooLarge => "PayloadTooLarge",
            Self::UnsupportedMediaType => "UnsupportedMediaType",
            Self::Busy => "Busy",
            Self::NotReady(_) => "NotReady",
            Self::UpstreamUnavailable(_) => "UpstreamUnavailable",
            Self::ShuttingDown => "ShuttingDown",
            Self::UpstreamTimeout(_) => "UpstreamTimeout",
            Self::Timeout(_) => "Timeout",
            Self::UpstreamError(_) => "UpstreamError",
            Self::Internal(_) => "Internal",
        }
    }

    pub fn field(&self) -> Option<&str> {
        match self {
            Self::InvalidRequest { field, .. } => field.as_deref(),
            _ => None,
        }
    }

    /// The `error` of the body: what the client may see.
    pub fn message(&self) -> String {
        match self {
            Self::InvalidRequest { message, .. } => message.clone(),
            Self::NotFound(what) => what.clone(),
            Self::MethodNotAllowed => "method not allowed on this route".to_owned(),
            Self::PayloadTooLarge => format!("request body is larger than {MAX_BODY_BYTES} bytes"),
            Self::UnsupportedMediaType => "expected a request body with `Content-Type: application/json`".to_owned(),
            Self::Busy => "a digest preview is already running; retry shortly".to_owned(),
            Self::NotReady(m) => format!("LENS is not ready: {m}"),
            Self::UpstreamUnavailable(m) => format!("LENS is unavailable: {m}"),
            Self::ShuttingDown => "ORACLE is shutting down".to_owned(),
            Self::UpstreamTimeout(m) => format!("timed out: {m}"),
            Self::Timeout(budget) => format!("the request did not finish within {} s", budget.as_secs_f64()),
            Self::UpstreamError(m) => m.clone(),
            Self::Internal(_) => INTERNAL_MESSAGE.to_owned(),
        }
    }

    const fn retry_after(&self) -> Option<u64> {
        match self {
            Self::Busy => Some(BUSY_RETRY_AFTER_SECS),
            Self::NotReady(_) => Some(NOT_READY_RETRY_AFTER_SECS),
            _ => None,
        }
    }

    /// A stand-in for an error response something other than an
    /// [`ApiError`] produced (axum itself), by its status.
    fn from_status(status: StatusCode) -> Self {
        match status {
            StatusCode::NOT_FOUND => Self::NotFound("no such route".to_owned()),
            StatusCode::METHOD_NOT_ALLOWED => Self::MethodNotAllowed,
            StatusCode::PAYLOAD_TOO_LARGE => Self::PayloadTooLarge,
            StatusCode::UNSUPPORTED_MEDIA_TYPE => Self::UnsupportedMediaType,
            s if s.is_client_error() => Self::bad_request(s.canonical_reason().unwrap_or("bad request")),
            s => Self::internal(format!("unrendered {s} response")),
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Internal(detail) => write!(f, "internal: {detail}"),
            other => f.write_str(&other.message()),
        }
    }
}

/// An error waiting for the renderer, in the response's extensions.
#[derive(Debug, Clone)]
struct PendingError(ApiError);

/// Set on a response whose error body has been rendered.
#[derive(Debug, Clone, Copy)]
struct Rendered;

impl IntoResponse for ApiError {
    /// Records the error; [`finish`] writes the body.
    fn into_response(self) -> Response {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = self.status();
        if let Some(secs) = self.retry_after() {
            response.headers_mut().insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
        response.extensions_mut().insert(PendingError(self));
        response
    }
}

/// The body of every failure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct ErrorBody {
    pub error: String,
    pub error_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub request_id: String,
}

// -- conversions, the one place each module's errors are mapped -------------

impl From<SpecError> for ApiError {
    fn from(err: SpecError) -> Self {
        match err {
            SpecError::Invalid { field, reason } => Self::invalid(field, reason),
            SpecError::NotReady(m) => Self::NotReady(m),
            SpecError::Unavailable(m) => Self::UpstreamUnavailable(m),
            SpecError::Timeout(m) => Self::UpstreamTimeout(m),
            SpecError::Cancelled => Self::ShuttingDown,
            SpecError::Internal(m) => Self::Internal(format!("spec: {m}")),
        }
    }
}

impl From<ProfileError> for ApiError {
    fn from(err: ProfileError) -> Self {
        match err {
            ProfileError::Invalid { field, reason } => Self::invalid(field, reason),
            ProfileError::NotReady(m) => Self::NotReady(m),
            ProfileError::Unavailable(m) => Self::UpstreamUnavailable(m),
            ProfileError::Timeout(m) => Self::UpstreamTimeout(m),
            ProfileError::Truncated => Self::UpstreamError(
                "LENS truncated a result of this profile, so it cannot be trusted; narrow window_slots".to_owned(),
            ),
            ProfileError::Permanent(m) => Self::Internal(format!("profile: {m}")),
        }
    }
}

impl From<QualityError> for ApiError {
    fn from(err: QualityError) -> Self {
        match err {
            QualityError::Invalid { field, reason } => Self::invalid(field, reason),
            QualityError::NotReady(m) => Self::NotReady(m),
            QualityError::Unavailable(m) => Self::UpstreamUnavailable(m),
            QualityError::Timeout(m) => Self::UpstreamTimeout(m),
            QualityError::Cancelled => Self::ShuttingDown,
            QualityError::Permanent(m) => Self::Internal(format!("quality: {m}")),
        }
    }
}

impl From<DigestError> for ApiError {
    fn from(err: DigestError) -> Self {
        match err {
            DigestError::Invalid { field, reason } => Self::invalid(field, reason),
            DigestError::NotReady(m) => Self::NotReady(m),
            DigestError::Unavailable(m) => Self::UpstreamUnavailable(m),
            DigestError::Timeout(m) => Self::UpstreamTimeout(m),
            DigestError::Busy => Self::Busy,
            DigestError::Cancelled => Self::ShuttingDown,
            DigestError::Store(m) => Self::Internal(format!("digest store: {m}")),
        }
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Invalid { field, reason } => Self::invalid(field, reason),
            other => Self::Internal(format!("store: {other}")),
        }
    }
}

impl From<OracleError> for ApiError {
    fn from(err: OracleError) -> Self {
        match err.as_store_error() {
            Some(StoreError::Invalid { field, reason }) => Self::invalid(field.clone(), reason),
            _ => Self::Internal(err.to_string()),
        }
    }
}

/// Run blocking `f` (SQLite) on the blocking pool.
pub(crate) async fn blocking<T, E>(f: impl FnOnce() -> Result<T, E> + Send + 'static) -> Result<T, ApiError>
where
    T: Send + 'static,
    E: Into<ApiError> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(Into::into),
        Err(err) => Err(ApiError::internal(format!("store task failed: {err}"))),
    }
}

/// `work`, or [`ApiError::ShuttingDown`] once `state.shutdown` fires.
pub(crate) async fn unless_shutdown<T>(
    state: &AppState,
    work: impl Future<Output = Result<T, ApiError>>,
) -> Result<T, ApiError> {
    tokio::select! {
        biased;
        () = state.shutdown.cancelled() => Err(ApiError::ShuttingDown),
        result = work => result,
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// A 200 JSON body, serialised once with `serde_json`, so a stored or
/// computed value reaches the client exactly as the module produced it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApiJson<T>(pub T);

impl<T: Serialize> IntoResponse for ApiJson<T> {
    fn into_response(self) -> Response {
        match serde_json::to_vec(&self.0) {
            Ok(bytes) => (StatusCode::OK, bytes).into_response(),
            Err(err) => ApiError::internal(format!("serialising the response: {err}")).into_response(),
        }
    }
}

/// A JSON request body of type `T`; every rejection is an [`ApiError`].
impl<S, T> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !is_json(req.headers()) {
            return Err(ApiError::UnsupportedMediaType);
        }
        let bytes = Bytes::from_request(req, state).await.map_err(body_rejection)?;
        parse_json(&bytes).map(Self)
    }
}

fn body_rejection(rejection: BytesRejection) -> ApiError {
    if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::PayloadTooLarge
    } else {
        ApiError::bad_request(format!("cannot read the request body: {}", rejection.body_text()))
    }
}

/// Parse `bytes` as `T`, naming the offending field when there is one.
///
/// axum's `Json::from_bytes` runs serde with the path tracked, so a wrong
/// type deep in a spec says where (`window.last_slots: invalid type ...`).
pub fn parse_json<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    use axum::extract::rejection::JsonRejection;
    axum::Json::<T>::from_bytes(bytes).map(|axum::Json(v)| v).map_err(|rejection| match rejection {
        JsonRejection::JsonDataError(e) => {
            let text = e.body_text();
            let detail = text.split_once("target type: ").map_or(text.as_str(), |(_, d)| d).to_owned();
            // The path ends at the offending value: an unknown key is part
            // of it, a missing one is not.
            let path = path_prefix(&detail);
            let field = match quoted_after(&detail, "missing field `") {
                Some(name) => Some(path.map_or_else(|| name.clone(), |p| format!("{p}.{name}"))),
                None => path.or_else(|| quoted_after(&detail, "unknown field `")),
            };
            ApiError::InvalidRequest { message: format!("invalid request body: {detail}"), field }
        }
        other => ApiError::bad_request(format!("request body is not valid JSON: {}", other.body_text())),
    })
}

/// The serde path before the first `": "` of `detail`, if it is one.
fn path_prefix(detail: &str) -> Option<String> {
    let (path, _) = detail.split_once(": ")?;
    let is_path = !path.is_empty()
        && path != "."
        && path.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '[' | ']'));
    is_path.then(|| path.to_owned())
}

fn quoted_after(message: &str, prefix: &str) -> Option<String> {
    let rest = &message[message.find(prefix)? + prefix.len()..];
    Some(rest[..rest.find('`')?].to_owned())
}

/// `application/json`, or any `application/*+json`, parameters allowed.
fn is_json(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let essence = value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    essence == "application/json" || (essence.starts_with("application/") && essence.ends_with("+json"))
}

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// A path parameter; a rejection is a 400.
#[derive(Debug, Clone)]
pub struct ApiPath<T>(pub T);

impl<S, T> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Path::<T>::from_request_parts(parts, state)
            .await
            .map(|Path(value)| Self(value))
            .map_err(|rejection| ApiError::bad_request(rejection.body_text()))
    }
}

/// A strictly decoded query string: `+` is a space, `%XX` a byte, the result
/// must be UTF-8, and a malformed escape, a pair without `=`, an empty key
/// or a key given twice is a 400. Handlers take the keys they accept with
/// the `take_*` methods and end with [`StrictQuery::finish`], which rejects
/// any key left over.
#[derive(Debug, Clone, Default)]
pub struct StrictQuery {
    pairs: BTreeMap<String, String>,
}

impl<S: Send + Sync> FromRequestParts<S> for StrictQuery {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Self::parse(parts.uri.query())
    }
}

impl StrictQuery {
    pub fn parse(raw: Option<&str>) -> Result<Self, ApiError> {
        let mut pairs = BTreeMap::new();
        for segment in raw.unwrap_or_default().split('&').filter(|s| !s.is_empty()) {
            let Some((raw_key, raw_value)) = segment.split_once('=') else {
                return Err(ApiError::bad_request(format!("query parameter {segment:?} has no '='; write key=value")));
            };
            let key = percent_decode(raw_key)?;
            let value = percent_decode(raw_value)?;
            if key.is_empty() {
                return Err(ApiError::bad_request("query string has a parameter with an empty name"));
            }
            if pairs.insert(key.clone(), value).is_some() {
                return Err(ApiError::invalid(key, "is given more than once"));
            }
        }
        Ok(Self { pairs })
    }

    /// A text parameter, if given.
    pub fn take_str(&mut self, key: &str) -> Option<String> {
        self.pairs.remove(key)
    }

    /// An unsigned integer parameter in `range`, if given: ASCII digits only,
    /// no sign, no space, never clamped.
    pub fn take_u64(&mut self, key: &str, range: std::ops::RangeInclusive<u64>) -> Result<Option<u64>, ApiError> {
        let Some(text) = self.pairs.remove(key) else { return Ok(None) };
        let value = parse_digits(&text).ok_or_else(|| ApiError::invalid(key, "must be an unsigned integer"))?;
        if !range.contains(&value) {
            return Err(ApiError::invalid(key, format!("must be {}..={}, got {value}", range.start(), range.end())));
        }
        Ok(Some(value))
    }

    /// Reject any parameter not taken.
    pub fn finish(self) -> Result<(), ApiError> {
        match self.pairs.into_keys().next() {
            None => Ok(()),
            Some(key) => Err(ApiError::invalid(key, "is not a parameter of this route")),
        }
    }
}

/// `text` as a `u64` if it is 1 to 20 ASCII digits that fit.
pub fn parse_digits(text: &str) -> Option<u64> {
    if text.is_empty() || text.len() > 20 || !text.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn percent_decode(raw: &str) -> Result<String, ApiError> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let byte = bytes
                    .get(i + 1..i + 3)
                    .and_then(|hex| std::str::from_utf8(hex).ok())
                    .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                    .ok_or_else(|| ApiError::bad_request("query string has a malformed '%' escape"))?;
                out.push(byte);
                i += 2;
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| ApiError::bad_request("query string is not UTF-8 once decoded"))
}

// ---------------------------------------------------------------------------
// Layers
// ---------------------------------------------------------------------------

/// The id of one request, in the request's extensions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestId(pub String);

/// Layer 1: the request id, the span, the access log line, and the backstop
/// [`finish`].
async fn request_id(mut req: Request, next: Next) -> Response {
    let id = uuid::Uuid::new_v4().to_string();
    req.extensions_mut().insert(RequestId(id.clone()));
    let method = req.method().clone();
    let route = req.extensions().get::<MatchedPath>().map_or("unmatched", |p| p.as_str()).to_owned();
    let span = tracing::info_span!("request", %method, route = %route, request_id = %id);
    let started = Instant::now();
    let response = next.run(req).instrument(span.clone()).await;
    let mut response = finish(response, &id);
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(HeaderName::from_static(REQUEST_ID_HEADER), value);
    }
    span.in_scope(|| {
        tracing::info!(
            status = response.status().as_u16(),
            latency_ms = started.elapsed().as_millis() as u64,
            "request"
        );
    });
    response
}

/// Layer 3: [`finish`] with the request's id.
async fn render(req: Request, next: Next) -> Response {
    let id = req.extensions().get::<RequestId>().map(|r| r.0.clone()).unwrap_or_default();
    finish(next.run(req).await, &id)
}

/// Render a pending [`ApiError`] (or any error response that is not one)
/// into the JSON body, and set the headers every response carries.
/// Idempotent.
pub fn finish(mut response: Response, request_id: &str) -> Response {
    let pending = response.extensions_mut().remove::<PendingError>().map(|p| p.0);
    let rendered = response.extensions().get::<Rendered>().is_some();
    let error = match pending {
        Some(err) => Some(err),
        None if !rendered && (response.status().is_client_error() || response.status().is_server_error()) => {
            Some(ApiError::from_status(response.status()))
        }
        None => None,
    };
    if let Some(err) = error {
        match &err {
            ApiError::Internal(detail) => tracing::error!(request_id, error = %detail, "internal error"),
            e if e.status().is_server_error() => {
                tracing::warn!(request_id, error_kind = e.kind(), error = %e.message(), "request failed");
            }
            _ => {}
        }
        let body = ErrorBody {
            error: err.message(),
            error_kind: err.kind().to_owned(),
            field: err.field().map(str::to_owned),
            request_id: request_id.to_owned(),
        };
        let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
        let (mut parts, _) = response.into_parts();
        parts.status = err.status();
        parts.headers.remove(header::CONTENT_LENGTH);
        if let Some(secs) = err.retry_after() {
            parts.headers.insert(header::RETRY_AFTER, HeaderValue::from(secs));
        }
        parts.extensions.insert(Rendered);
        response = Response::from_parts(parts, Body::from(bytes));
    }
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

/// Layer 2's handler: a panic is an internal error. The payload is logged,
/// never sent.
fn panic_response(payload: Box<dyn std::any::Any + Send + 'static>) -> Response {
    let detail = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    ApiError::internal(format!("handler panicked: {detail}")).into_response()
}

/// Layer 5: one route group's budget.
async fn budget(State(limit): State<Duration>, req: Request, next: Next) -> Response {
    match tokio::time::timeout(limit, next.run(req)).await {
        Ok(response) => response,
        Err(_) => ApiError::Timeout(limit).into_response(),
    }
}

/// Each route group's time budget. Every one sits just above the deadline
/// of what the group calls, so the module's own timeout normally fires
/// first and says why.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Budgets {
    /// `/catalog`, `/templates`, `/suggest`, `/anomalies`, `/detectors`,
    /// `PATCH /detectors/{id}`, `/digest`, `/digests`, `/health`.
    pub cheap: Duration,
    /// `/query`, `/ask`, `/drill`, and `reset-progress` (which waits for a detector's
    /// turn in flight): [`SPEC_QUERIES`] LENS calls at the client timeout.
    pub query: Duration,
    /// `/profile`, its detail reports included: [`PROFILE_DEADLINE_SECS`].
    pub profile: Duration,
    /// `/quality`: [`AUDIT_DEADLINE_SECS`].
    pub quality: Duration,
    /// `/digest/preview`: [`BUILD_DEADLINE_SECS`] plus [`QUALITY_WAIT_SECS`].
    pub preview: Duration,
}

impl Budgets {
    pub fn for_config(config: &OracleConfig) -> Self {
        let secs = Duration::from_secs;
        Self {
            cheap: CHEAP_BUDGET,
            query: config.lens_timeout() * SPEC_QUERIES + BUDGET_MARGIN,
            profile: secs(PROFILE_DEADLINE_SECS) + BUDGET_MARGIN,
            quality: secs(AUDIT_DEADLINE_SECS) + BUDGET_MARGIN,
            preview: secs(BUILD_DEADLINE_SECS + QUALITY_WAIT_SECS) + BUDGET_MARGIN,
        }
    }

    /// Every budget `d`, for tests.
    pub const fn uniform(d: Duration) -> Self {
        Self { cheap: d, query: d, profile: d, quality: d, preview: d }
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

type ApiRouter = Router<Arc<AppState>>;

fn group(routes: ApiRouter, limit: Duration) -> ApiRouter {
    routes.route_layer(middleware::from_fn_with_state(limit, budget))
}

/// Every route, with `state` and the layers in the [module docs](self#layers),
/// budgets from [`Budgets::for_config`].
pub fn router(state: Arc<AppState>) -> Router {
    let budgets = Budgets::for_config(&state.config);
    router_with(state, budgets)
}

/// [`router`] with explicit budgets.
pub fn router_with(state: Arc<AppState>, budgets: Budgets) -> Router {
    let cheap = Router::new()
        .route("/suggest", get(query::suggest))
        .route("/templates", get(query::templates))
        .route("/catalog", get(query::catalog))
        .route("/anomalies", get(anomalies::list_anomalies))
        .route("/detectors", get(anomalies::list_detectors))
        .route("/detectors/{id}", patch(anomalies::set_enabled))
        .route("/digest", get(digest::latest))
        .route("/digests", get(digest::list))
        .route("/health", get(health::health));
    let query = Router::new()
        .route("/query", post(query::run_query))
        .route("/ask", post(query::ask))
        .route("/drill", post(query::drill))
        .route("/detectors/{id}/reset-progress", post(anomalies::reset_progress));
    // The two literal segments win over `{kind}`: a transaction and a slot
    // are not entity kinds, and their reports have their own shape.
    let profile = Router::new()
        .route("/profile/transaction/{signature}", get(profile::transaction))
        .route("/profile/slot/{slot}", get(profile::slot))
        .route("/profile/{kind}/{id}", get(profile::profile));
    let quality = Router::new().route("/quality", get(quality::quality));
    let preview = Router::new().route("/digest/preview", get(digest::preview));

    Router::new()
        .merge(group(cheap, budgets.cheap))
        .merge(group(query, budgets.query))
        .merge(group(profile, budgets.profile))
        .merge(group(quality, budgets.quality))
        .merge(group(preview, budgets.preview))
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(middleware::from_fn(render))
        .layer(CatchPanicLayer::custom(panic_response))
        .layer(middleware::from_fn(request_id))
        .with_state(state)
}

async fn route_not_found() -> ApiError {
    ApiError::NotFound("no such route".to_owned())
}

async fn method_not_allowed() -> ApiError {
    ApiError::MethodNotAllowed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_strings_decode_strictly() {
        let mut q = StrictQuery::parse(Some("prefix=top+10%21&limit=5")).unwrap();
        assert_eq!(q.take_str("prefix").as_deref(), Some("top 10!"));
        assert_eq!(q.take_u64("limit", 1..=10).unwrap(), Some(5));
        q.finish().unwrap();
        for bad in ["a=1&a=2", "a=1&%61=2", "flag", "=1", "a=%zz", "a=%4", "a=%FF"] {
            let err = StrictQuery::parse(Some(bad)).unwrap_err();
            assert_eq!(err.status(), StatusCode::BAD_REQUEST, "{bad}");
        }
    }

    #[test]
    fn numbers_are_digits_only_and_never_clamped() {
        for bad in ["", "+5", "-1", " 5", "5 ", "0x10", "1e3", "5.0", "99999999999999999999999"] {
            assert_eq!(parse_digits(bad), None, "{bad:?}");
        }
        assert_eq!(parse_digits("007"), Some(7));
        let mut q = StrictQuery::parse(Some("limit=11")).unwrap();
        let err = q.take_u64("limit", 1..=10).unwrap_err();
        assert_eq!(err.field(), Some("limit"));
        let q = StrictQuery::parse(Some("limt=1")).unwrap();
        assert_eq!(q.finish().unwrap_err().field(), Some("limt"));
    }

    #[test]
    fn module_errors_map_to_the_table() {
        let cases: Vec<(ApiError, u16, &str)> = vec![
            (SpecError::invalid("window", "x").into(), 400, "InvalidRequest"),
            (SpecError::NotReady("x".into()).into(), 503, "NotReady"),
            (SpecError::Unavailable("x".into()).into(), 503, "UpstreamUnavailable"),
            (SpecError::Timeout("x".into()).into(), 504, "UpstreamTimeout"),
            (SpecError::Cancelled.into(), 503, "ShuttingDown"),
            (SpecError::Internal("x".into()).into(), 500, "Internal"),
            (ProfileError::Truncated.into(), 502, "UpstreamError"),
            (ProfileError::Permanent("x".into()).into(), 500, "Internal"),
            (ProfileError::Timeout("x".into()).into(), 504, "UpstreamTimeout"),
            (QualityError::Cancelled.into(), 503, "ShuttingDown"),
            (QualityError::Permanent("x".into()).into(), 500, "Internal"),
            (DigestError::Busy.into(), 429, "Busy"),
            (DigestError::Store("x".into()).into(), 500, "Internal"),
            (DigestError::Cancelled.into(), 503, "ShuttingDown"),
            (StoreError::storage("disk").into(), 500, "Internal"),
            (StoreError::invalid("limit", "x").into(), 400, "InvalidRequest"),
        ];
        for (err, status, kind) in cases {
            assert_eq!((err.status().as_u16(), err.kind()), (status, kind), "{err:?}");
        }
        assert_eq!(ApiError::from(SpecError::invalid("window", "x")).field(), Some("window"));
        // The detail of an internal error is never in the message.
        assert_eq!(ApiError::from(SpecError::Internal("secret detail".into())).message(), INTERNAL_MESSAGE);
    }

    #[test]
    fn json_bodies_name_the_bad_field() {
        #[derive(Debug, serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        #[allow(dead_code)]
        struct Body {
            enabled: bool,
        }
        assert_eq!(parse_json::<Body>(br#"{"enabled": true, "x": 1}"#).unwrap_err().field(), Some("x"));
        assert_eq!(parse_json::<Body>(br#"{}"#).unwrap_err().field(), Some("enabled"));
        let err = parse_json::<Body>(br#"{"enabled": "true"}"#).unwrap_err();
        assert_eq!((err.status(), err.field()), (StatusCode::BAD_REQUEST, Some("enabled")));
        assert_eq!(parse_json::<Body>(b"{").unwrap_err().status(), StatusCode::BAD_REQUEST);
    }
}
