//! API: FORGE over HTTP.
//!
//! | Route                                   | Success | Handler                          | File        |
//! |-----------------------------------------|---------|----------------------------------|-------------|
//! | `POST /queries`                         | 201     | [`queries::create_query`]        | [`queries`] |
//! | `GET /queries`                          | 200     | [`queries::list_queries`]        | [`queries`] |
//! | `GET /queries/{name}`                   | 200     | [`queries::get_query`]           | [`queries`] |
//! | `DELETE /queries/{name}`                | 204     | [`queries::delete_query`]        | [`queries`] |
//! | `GET /q/{name}?param=value`             | 200     | [`queries::run_query`]           | [`queries`] |
//! | `POST /alerts`                          | 201     | [`alerts::create_alert`]         | [`alerts`]  |
//! | `GET /alerts`                           | 200     | [`alerts::list_alerts`]          | [`alerts`]  |
//! | `GET /alerts/{id}`                      | 200     | [`alerts::get_alert`]            | [`alerts`]  |
//! | `PATCH /alerts/{id}`                    | 200     | [`alerts::update_alert`]         | [`alerts`]  |
//! | `DELETE /alerts/{id}`                   | 204     | [`alerts::delete_alert`]         | [`alerts`]  |
//! | `GET /alerts/{id}/deliveries?limit=&status=` | 200 | [`alerts::list_deliveries`]     | [`alerts`]  |
//! | `POST /deliveries/{id}/replay`          | 202     | [`alerts::replay_delivery`]      | [`alerts`]  |
//! | `GET /health`                           | 200     | [`health::health_handler`]       | [`health`]  |
//!
//! # Errors
//!
//! Every failure, from any route, an unknown route and a timed-out request
//! included, has the body LENS uses, [`ErrorBody`]:
//!
//! ```json
//! {"error": "invalid secret: must be 16 to 512 bytes, got 5", "error_kind": "validation_error", "sql": null}
//! ```
//!
//! `sql` is the SQL FORGE sent to LENS when LENS is what failed, and `null`
//! otherwise.
//!
//! | Outcome                                                        | Status          | `error_kind`         |
//! |----------------------------------------------------------------|-----------------|----------------------|
//! | Body is not JSON, or not the route's request type; bad query string or path | 400 / 413 / 415 / 422 | `invalid_request` |
//! | A field fails the registry's validation, or a bound value its kind | 400         | `validation_error`   |
//! | No saved query, alert, delivery or route by that key           | 404             | `not_found`          |
//! | Method not allowed on the route                                | 405             | `method_not_allowed` |
//! | Name taken, or a delivery that is not `failed` replayed        | 409             | `conflict`           |
//! | LENS rejected the SQL (HTTP 400 from LENS)                     | 400             | `lens_bad_query`     |
//! | LENS answered with something FORGE cannot read                 | 502             | `lens_protocol`      |
//! | LENS unreachable or failing                                    | 503             | `lens_unavailable`   |
//! | LENS timed out                                                 | 504             | `lens_timeout`       |
//! | The request took longer than [`API_REQUEST_TIMEOUT`]           | 504             | `request_timeout`    |
//! | SQLite failed                                                  | 500             | `storage_error`      |
//! | Anything else                                                  | 500             | `internal_error`     |
//!
//! # Strict input
//!
//! Request bodies must be `Content-Type: application/json`, at most
//! [`MAX_BODY_BYTES`], and match the route's request type exactly: an
//! unknown field is rejected, not ignored, so a typo such as `wait_for_fnal`
//! cannot silently fall back to a default. Query strings are decoded
//! strictly too: malformed `%` escapes, invalid UTF-8 and repeated keys are
//! rejected (see [`parse_query_string`]).
//!
//! # Secrets
//!
//! An alert's signing secret leaves FORGE once, in the `201` body of
//! `POST /alerts`. Responses are built from view types that have no secret
//! field ([`AlertView`]), never from [`crate::Alert`]; error messages
//! never quote a submitted `secret` ([`ApiJson`] redacts it from parse
//! errors, and the registry's validation does not echo it); bodies are not
//! logged.
//!
//! # Serving
//!
//! [`router`] adds, outermost first: request tracing ([`TraceLayer`], at
//! `info`), the [`API_REQUEST_TIMEOUT`], and the [`MAX_BODY_BYTES`] limit.
//! [`serve`] binds `bind_address`, which `config.rs` guarantees is not
//! LENS's own address, and serves until the shutdown future resolves,
//! letting requests in flight finish.

mod alerts;
mod health;
mod queries;

pub use alerts::{
    AlertList, AlertView, CreateAlertRequest, CreatedAlert, DEFAULT_HISTORY_LIMIT, DeliveryHistory, DeliveryView,
    UpdateAlertRequest,
};
pub use health::{
    ChangelogSection, EngineSection, FailingAlertView, HealthResponse, HealthStatus, LENS_HEALTH_TIMEOUT, LensSection,
    OutboxSection, STALE_PENDING_SECS, compute_status,
};
pub use queries::{CreateQueryRequest, ParamRequest, QueryList, QueryResultEnvelope, QueryView};

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::error_handling::HandleErrorLayer;
use axum::extract::{DefaultBodyLimit, FromRequest, FromRequestParts, Path, Request};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::alerts::EngineHandle;
use crate::config::{ForgeConfig, ForgeError, ForgeResult};
use crate::lens_client::{DEFAULT_REQUEST_TIMEOUT, LensApi, LensCallError};
use crate::registry::{Registry, RegistryError};
use crate::webhook::WebhookService;
use crate::webhook::outbox::REDACTED;

/// Largest request body accepted. A saved query's SQL is capped at 64 KiB
/// and its parameters add little, so this leaves ample room.
pub const MAX_BODY_BYTES: usize = 256 * 1024;

/// Longest a request may take. Above two LENS calls at the client's own
/// timeout (`POST /alerts` makes a dry run and a seed query), so a slow
/// LENS is reported as `lens_timeout`, which says why, rather than cut off.
pub const API_REQUEST_TIMEOUT: Duration = Duration::from_secs(DEFAULT_REQUEST_TIMEOUT.as_secs() * 2 + 5);

pub const INVALID_REQUEST_KIND: &str = "invalid_request";
pub const VALIDATION_ERROR_KIND: &str = "validation_error";
pub const NOT_FOUND_KIND: &str = "not_found";
pub const METHOD_NOT_ALLOWED_KIND: &str = "method_not_allowed";
pub const CONFLICT_KIND: &str = "conflict";
pub const STORAGE_ERROR_KIND: &str = "storage_error";
pub const INTERNAL_ERROR_KIND: &str = "internal_error";
pub const REQUEST_TIMEOUT_KIND: &str = "request_timeout";

/// The request field whose value is never quoted back.
const SECRET_FIELD: &str = "secret";

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// What every handler gets. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<Registry>,
    pub lens: Arc<dyn LensApi>,
    pub webhook: Arc<WebhookService>,
    pub engine: EngineHandle,
    pub config: Arc<ForgeConfig>,
    /// When the server started, for `uptime_secs`.
    pub start_time: Instant,
}

impl AppState {
    /// State for these parts, started now.
    pub fn new(
        registry: Arc<Registry>,
        lens: Arc<dyn LensApi>,
        webhook: Arc<WebhookService>,
        engine: EngineHandle,
        config: Arc<ForgeConfig>,
    ) -> Self {
        Self { registry, lens, webhook, engine, config, start_time: Instant::now() }
    }
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("registry", &self.registry)
            .field("webhook", &self.webhook)
            .field("engine", &self.engine)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The body of every failed request; the same shape as LENS's.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// What went wrong.
    pub error: String,
    /// One of the kinds in the [module docs](self#errors).
    pub error_kind: String,
    /// The SQL sent to LENS when LENS failed; `null` otherwise.
    pub sql: Option<String>,
}

/// A failed request: the status and the body that go back.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
}

impl ApiError {
    pub fn new(status: StatusCode, kind: &str, error: impl Into<String>) -> Self {
        Self { status, body: ErrorBody { error: error.into(), error_kind: kind.to_owned(), sql: None } }
    }

    /// A 400 `invalid_request`.
    pub fn invalid_request(error: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, INVALID_REQUEST_KIND, error)
    }

    /// A 400 `validation_error` on `field`, worded as the registry words its
    /// own.
    pub fn invalid(field: &str, reason: impl std::fmt::Display) -> Self {
        Self::from_registry(&RegistryError::invalid(field, reason.to_string()))
    }

    /// A 500 `internal_error`.
    pub fn internal(error: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR_KIND, error)
    }

    /// The status and kind for `err`. A [`RegistryError`] keeps its own
    /// status ([`RegistryError::http_status`]); anything else is a 500.
    pub fn from_forge(err: &ForgeError) -> Self {
        match err.as_registry_error() {
            // The registry's message is meant for the client; the operation
            // in `err` is plumbing.
            Some(registry @ (RegistryError::Invalid { .. } | RegistryError::NotFound { .. } | RegistryError::Conflict { .. })) => {
                Self::from_registry(registry)
            }
            Some(RegistryError::Storage(_)) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, STORAGE_ERROR_KIND, err.to_string())
            }
            None => Self::internal(err.to_string()),
        }
    }

    pub fn from_registry(err: &RegistryError) -> Self {
        let kind = match err {
            RegistryError::Invalid { .. } => VALIDATION_ERROR_KIND,
            RegistryError::NotFound { .. } => NOT_FOUND_KIND,
            RegistryError::Conflict { .. } => CONFLICT_KIND,
            RegistryError::Storage(_) => STORAGE_ERROR_KIND,
        };
        let status = StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        Self::new(status, kind, err.to_string())
    }

    /// `err`, raised by LENS while running `sql`. See the
    /// [module docs](self#errors) for the statuses.
    pub fn from_lens(err: &LensCallError, sql: &str) -> Self {
        let status = match err {
            LensCallError::BadQuery { .. } => StatusCode::BAD_REQUEST,
            LensCallError::Protocol { .. } => StatusCode::BAD_GATEWAY,
            LensCallError::Unavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            LensCallError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        };
        let mut api = Self::new(status, &format!("lens_{}", err.kind()), err.to_string());
        api.body.sql = Some(sql.to_owned());
        api
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if self.status.is_server_error() {
            tracing::warn!(
                status = self.status.as_u16(),
                error_kind = %self.body.error_kind,
                error = %self.body.error,
                "request failed"
            );
        }
        (self.status, Json(self.body)).into_response()
    }
}

/// Run blocking `f` (SQLite) on the blocking pool.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> ForgeResult<T> + Send + 'static,
) -> Result<T, ApiError> {
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result.map_err(|err| ApiError::from_forge(&err)),
        Err(join_err) => Err(ApiError::internal(format!("registry task failed: {join_err}"))),
    }
}

// ---------------------------------------------------------------------------
// Extractors
// ---------------------------------------------------------------------------

/// A JSON body of type `T`, rejected as an [`ApiError`].
///
/// Like `axum::Json`, it needs `Content-Type: application/json` (415
/// otherwise), turns a syntax error into a 400 and a body of the wrong shape
/// into a 422, and is bounded by [`MAX_BODY_BYTES`] (413). Unlike it, the
/// rejection has FORGE's error body, and any value submitted as `secret` is
/// cut out of the message: serde quotes a value of the wrong type
/// (`invalid type: integer `1234`, expected a string`).
#[derive(Debug, Clone, Copy, Default)]
pub struct ApiJson<T>(pub T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        if !is_json(req.headers()) {
            return Err(ApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                INVALID_REQUEST_KIND,
                "expected a request body with `Content-Type: application/json`",
            ));
        }
        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(|rejection| ApiError::new(rejection.status(), INVALID_REQUEST_KIND, rejection.body_text()))?;
        Json::<T>::from_bytes(&bytes).map(|Json(value)| Self(value)).map_err(|rejection| {
            let message = redact_secret(&rejection.body_text(), &bytes);
            ApiError::new(rejection.status(), INVALID_REQUEST_KIND, message)
        })
    }
}

/// `application/json`, or any `application/*+json`, parameters allowed.
fn is_json(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(header::CONTENT_TYPE).and_then(|value| value.to_str().ok()) else {
        return false;
    };
    let essence = value.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
    essence == "application/json" || (essence.starts_with("application/") && essence.ends_with("+json"))
}

/// `message` with the body's top-level `secret` value, whatever its type,
/// replaced by [`REDACTED`].
fn redact_secret(message: &str, body: &[u8]) -> String {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_slice::<serde_json::Value>(body) else {
        // Syntax errors quote positions, not values.
        return message.to_owned();
    };
    let Some(secret) = object.get(SECRET_FIELD) else {
        return message.to_owned();
    };
    let mut needles = vec![secret.to_string()];
    if let Some(text) = secret.as_str() {
        needles.push(text.to_owned());
    }
    needles.retain(|needle| !needle.is_empty());
    needles.sort_by_key(|needle| std::cmp::Reverse(needle.len()));
    needles.iter().fold(message.to_owned(), |message, needle| message.replace(needle.as_str(), REDACTED))
}

/// A path parameter, rejected as an [`ApiError`].
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
            .map_err(|rejection| ApiError::new(rejection.status(), INVALID_REQUEST_KIND, rejection.body_text()))
    }
}

/// Decode an `application/x-www-form-urlencoded` query string strictly:
/// `+` is a space, `%XX` a byte, and the result must be UTF-8. A malformed
/// escape, invalid UTF-8, a pair without `=`, an empty key or a key given
/// twice is a 400 `invalid_request`; empty segments (`a=1&&b=2`, a
/// trailing `&`) are skipped. Pairs are returned in order.
pub fn parse_query_string(raw: Option<&str>) -> Result<Vec<(String, String)>, ApiError> {
    let mut pairs = Vec::new();
    let mut seen = HashSet::new();
    for segment in raw.unwrap_or_default().split('&').filter(|segment| !segment.is_empty()) {
        let Some((raw_key, raw_value)) = segment.split_once('=') else {
            return Err(ApiError::invalid_request(format!(
                "query string segment {segment:?} has no '='; write key=value"
            )));
        };
        let key = percent_decode(raw_key)?;
        let value = percent_decode(raw_value)?;
        if key.is_empty() {
            return Err(ApiError::invalid_request("query string has a parameter with an empty name"));
        }
        if !seen.insert(key.clone()) {
            return Err(ApiError::invalid_request(format!("query string parameter {key:?} is given more than once")));
        }
        pairs.push((key, value));
    }
    Ok(pairs)
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
                    .ok_or_else(|| ApiError::invalid_request(format!("query string {raw:?} has a malformed '%' escape")))?;
                out.push(byte);
                i += 2;
            }
            byte => out.push(byte),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| ApiError::invalid_request(format!("query string {raw:?} is not UTF-8 once decoded")))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Every route, with `state` and the layers in the [module docs](self#serving).
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/queries", post(queries::create_query).get(queries::list_queries))
        .route("/queries/{name}", get(queries::get_query).delete(queries::delete_query))
        .route("/q/{name}", get(queries::run_query))
        .route("/alerts", post(alerts::create_alert).get(alerts::list_alerts))
        .route(
            "/alerts/{id}",
            get(alerts::get_alert).patch(alerts::update_alert).delete(alerts::delete_alert),
        )
        .route("/alerts/{id}/deliveries", get(alerts::list_deliveries))
        .route("/deliveries/{id}/replay", post(alerts::replay_delivery))
        .route("/health", get(health::health_handler))
        .fallback(route_not_found)
        .method_not_allowed_fallback(method_not_allowed)
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(
            ServiceBuilder::new()
                .layer(
                    TraceLayer::new_for_http()
                        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                        .on_response(DefaultOnResponse::new().level(Level::INFO)),
                )
                .layer(HandleErrorLayer::new(request_failed))
                .timeout(API_REQUEST_TIMEOUT),
        )
        .with_state(state)
}

async fn route_not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, NOT_FOUND_KIND, "no such route")
}

async fn method_not_allowed() -> ApiError {
    ApiError::new(StatusCode::METHOD_NOT_ALLOWED, METHOD_NOT_ALLOWED_KIND, "method not allowed on this route")
}

/// What the timeout layer's error becomes.
async fn request_failed(err: tower::BoxError) -> ApiError {
    if err.is::<tower::timeout::error::Elapsed>() {
        ApiError::new(
            StatusCode::GATEWAY_TIMEOUT,
            REQUEST_TIMEOUT_KIND,
            format!("the request did not finish within {}s", API_REQUEST_TIMEOUT.as_secs()),
        )
    } else {
        ApiError::internal(format!("request failed: {err}"))
    }
}

// ---------------------------------------------------------------------------
// Serving
// ---------------------------------------------------------------------------

/// Serve the API on the configured `bind_address` until `shutdown`
/// resolves. See [`serve_on`].
pub async fn serve<F>(state: AppState, shutdown: F) -> ForgeResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let addr = state.config.socket_addr();
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| ForgeError::io(format!("bind address {addr}"), err))?;
    serve_on(listener, state, shutdown).await
}

/// Serve the API on `listener` until `shutdown` resolves. Requests in flight
/// are allowed to finish; returns once they have.
pub async fn serve_on<F>(listener: TcpListener, state: AppState, shutdown: F) -> ForgeResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let local_addr = listener.local_addr().map_err(|err| ForgeError::io("listener address", err))?;
    tracing::info!(
        address = %local_addr,
        lens_url = state.config.lens_url(),
        max_body_bytes = MAX_BODY_BYTES,
        request_timeout_secs = API_REQUEST_TIMEOUT.as_secs(),
        "FORGE listening"
    );
    let app = router(state);
    let stop = async move {
        shutdown.await;
        tracing::info!("shutdown signal received; draining requests");
    };
    axum::serve(listener, app)
        .with_graceful_shutdown(stop)
        .await
        .map_err(|err| ForgeError::io(format!("serving on {local_addr}"), err))
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(raw: &str) -> Vec<(String, String)> {
        parse_query_string(Some(raw)).unwrap()
    }

    #[test]
    fn query_strings_decode_plus_and_percent_escapes() {
        assert_eq!(
            pairs("mint=So1%27%3B+DROP&n=5&empty="),
            [("mint".into(), "So1'; DROP".into()), ("n".into(), "5".into()), ("empty".into(), String::new())]
        );
        assert_eq!(pairs("a=%C3%A9"), [("a".into(), "é".into())]);
        assert_eq!(pairs("a=1&&b=2&"), [("a".into(), "1".into()), ("b".into(), "2".into())]);
        assert_eq!(pairs("a=x=y"), [("a".into(), "x=y".into())]);
        assert!(parse_query_string(None).unwrap().is_empty());
        assert!(pairs("").is_empty());
    }

    #[test]
    fn query_strings_reject_what_a_lenient_decoder_would_guess_at() {
        for bad in ["a=1&a=2", "a=1&%61=2", "flag", "=1", "a=%zz", "a=%4", "a=%", "a=%FF", "a=%C3"] {
            let err = parse_query_string(Some(bad)).unwrap_err();
            assert_eq!((err.status, err.body.error_kind.as_str()), (StatusCode::BAD_REQUEST, INVALID_REQUEST_KIND), "{bad}");
        }
    }

    #[test]
    fn json_content_types() {
        let with = |value: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, value.parse().unwrap());
            is_json(&headers)
        };
        for ok in ["application/json", "application/json; charset=utf-8", "Application/JSON", "application/merge-patch+json"] {
            assert!(with(ok), "{ok}");
        }
        for bad in ["text/plain", "application/jsonx", "text/json", "application/x-www-form-urlencoded"] {
            assert!(!with(bad), "{bad}");
        }
        assert!(!is_json(&HeaderMap::new()));
    }

    #[test]
    fn redaction_covers_every_json_type_of_secret() {
        let message = "secret: invalid type: integer `12345678901234567890`, expected a string";
        assert_eq!(
            redact_secret(message, br#"{"secret": 12345678901234567890}"#),
            "secret: invalid type: integer `<redacted>`, expected a string"
        );
        let message = r#"invalid type: sequence, expected a string: ["hunter2hunter2hunter2"]"#;
        let redacted = redact_secret(message, br#"{"secret": ["hunter2hunter2hunter2"]}"#);
        assert!(!redacted.contains("hunter2"), "{redacted}");
        assert_eq!(redact_secret("unknown field `x`", br#"{"x": 1}"#), "unknown field `x`");
        assert_eq!(redact_secret("EOF", b"{\"secret\": "), "EOF");
    }

    #[test]
    fn lens_errors_map_to_gateway_statuses_and_carry_the_sql() {
        let cases = [
            (LensCallError::BadQuery { message: "m".into() }, StatusCode::BAD_REQUEST, "lens_bad_query"),
            (LensCallError::Protocol { message: "m".into() }, StatusCode::BAD_GATEWAY, "lens_protocol"),
            (LensCallError::Unavailable { message: "m".into() }, StatusCode::SERVICE_UNAVAILABLE, "lens_unavailable"),
            (LensCallError::Timeout { message: "m".into() }, StatusCode::GATEWAY_TIMEOUT, "lens_timeout"),
        ];
        for (err, status, kind) in cases {
            let api = ApiError::from_lens(&err, "SELECT 1");
            assert_eq!((api.status, api.body.error_kind.as_str()), (status, kind));
            assert_eq!(api.body.sql.as_deref(), Some("SELECT 1"));
        }
    }

    #[test]
    fn registry_errors_keep_their_status_and_drop_the_plumbing() {
        let wrapped = |err| ForgeError::registry("create alert", err);
        let api = ApiError::from_forge(&wrapped(RegistryError::invalid("secret", "too short")));
        assert_eq!((api.status, api.body.error.as_str()), (StatusCode::BAD_REQUEST, "invalid secret: too short"));
        assert_eq!(ApiError::from_forge(&wrapped(RegistryError::not_found("alert", "x"))).status, StatusCode::NOT_FOUND);
        assert_eq!(ApiError::from_forge(&wrapped(RegistryError::conflict("taken"))).status, StatusCode::CONFLICT);
        let api = ApiError::from_forge(&wrapped(RegistryError::storage("disk I/O error")));
        assert_eq!((api.status, api.body.error_kind.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, STORAGE_ERROR_KIND));
        let api = ApiError::from_forge(&ForgeError::config("f", "r"));
        assert_eq!((api.status, api.body.error_kind.as_str()), (StatusCode::INTERNAL_SERVER_ERROR, INTERNAL_ERROR_KIND));
    }
}
