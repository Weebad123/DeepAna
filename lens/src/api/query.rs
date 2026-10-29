//! `POST /query`: run one SQL statement and return its rows.
//!
//! ```text
//!   QueryRequest ─► parse format ─► QueryEngine::execute ─► serialize ─► 200 + body
//!        │               │                  │                   │
//!        └───────────────┴──────────────────┴───────────────────┴─► ErrorBody + status
//! ```
//!
//! # Request
//!
//! ```json
//! {"sql": "SELECT slot FROM solana.slots LIMIT 5", "format": "csv"}
//! ```
//!
//! `format` is `"json"` (the default when absent) or `"csv"`, in any case.
//! Anything else is rejected before the query runs.
//!
//! # Response
//!
//! On success, the body [`serialize`] produced, with its `Content-Type` and
//! the result metadata in headers too (CSV has nowhere else to put it):
//! [`ROW_COUNT_HEADER`], [`TRUNCATED_HEADER`], [`DURATION_HEADER`].
//!
//! On failure, an [`ErrorBody`]:
//!
//! ```json
//! {"error": "table 'datafusion.solana.nonexistent' not found",
//!  "error_kind": "query_plan_error",
//!  "sql": "SELECT * FROM solana.nonexistent"}
//! ```
//!
//! # Status codes
//!
//! The status says whose fault a failure is, judged by the root cause rather
//! than by the stage it surfaced in: a corrupt Parquet file can fail a query
//! while it is being planned (statistics are read then), and that is not the
//! client's fault.
//!
//! | Outcome                                                         | Status | `error_kind`              |
//! |-----------------------------------------------------------------|--------|---------------------------|
//! | Rows returned                                                   | 200    |                           |
//! | Body is not a JSON `QueryRequest`                               | 400 / 415 / 422 | `invalid_request` |
//! | `format` is neither `json` nor `csv`                            | 400    | `invalid_format`          |
//! | SQL is empty, does not parse or plan, names an unknown table, or is not read-only | 400 | `query_plan_error` |
//! | SQL fails on the data: bad cast, division by zero, overflow     | 400    | `query_execution_error`   |
//! | Query exceeded `query_timeout_secs`                             | 504    | `query_timeout`           |
//! | Query ran out of memory or spill space                          | 503    | `query_*_error`           |
//! | Anything else: I/O, unreadable Parquet, a panic, serialization  | 500    | [`LensError::kind`]       |
//!
//! A timeout is 504 rather than 408: 408 means the *client* was too slow to
//! send its request, and some clients retry it automatically, which would
//! run the expensive query again.

use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::header::{CONTENT_TYPE, HeaderValue};
use axum::http::{HeaderName, StatusCode};
use axum::response::{IntoResponse, Response};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use serde::{Deserialize, Serialize};

use super::AppState;
use crate::error::{BoxError, LensError};
use crate::result::{QueryFormat, SerializedResult, serialize};

/// Rows in the body, after truncation.
pub const ROW_COUNT_HEADER: HeaderName = HeaderName::from_static("x-lens-row-count");
/// `true` when rows past `max_query_rows` were dropped.
pub const TRUNCATED_HEADER: HeaderName = HeaderName::from_static("x-lens-truncated");
/// Time the query took in the engine, in milliseconds.
pub const DURATION_HEADER: HeaderName = HeaderName::from_static("x-lens-duration-ms");

/// `error_kind` of a body that is not a valid [`QueryRequest`].
pub const INVALID_REQUEST_KIND: &str = "invalid_request";
/// `error_kind` of a `format` LENS does not serve.
pub const INVALID_FORMAT_KIND: &str = "invalid_format";

/// The body of `POST /query`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct QueryRequest {
    pub sql: String,
    /// `"json"` or `"csv"`; JSON when absent.
    #[serde(default)]
    pub format: Option<String>,
}

/// The body of every failed `POST /query`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ErrorBody {
    /// What went wrong, without the SQL (that is in `sql`).
    pub error: String,
    /// [`LensError::kind`], [`INVALID_REQUEST_KIND`] or
    /// [`INVALID_FORMAT_KIND`].
    pub error_kind: String,
    /// The SQL as submitted; `null` when the body could not be read.
    pub sql: Option<String>,
}

/// A failed request: the status and the body that go back.
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub body: ErrorBody,
}

impl ApiError {
    fn new(status: StatusCode, kind: &str, error: impl Into<String>, sql: Option<String>) -> Self {
        Self { status, body: ErrorBody { error: error.into(), error_kind: kind.to_owned(), sql } }
    }

    /// `err`, raised while running `sql`.
    pub fn from_lens(err: &LensError, sql: &str) -> Self {
        Self::new(status_for(err), err.kind(), error_message(err), Some(sql.to_owned()))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

/// Handler for `POST /query`.
pub async fn query_handler(
    State(state): State<AppState>,
    request: Result<Json<QueryRequest>, JsonRejection>,
) -> Response {
    let started = Instant::now();
    let (outcome, sql, format) = match request {
        Ok(Json(request)) => {
            let format = request.format.clone();
            (run(&state, &request).await, Some(request.sql), format)
        }
        Err(rejection) => {
            let err = ApiError::new(rejection.status(), INVALID_REQUEST_KIND, rejection.body_text(), None);
            (Err(err), None, None)
        }
    };
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

    match outcome {
        Ok(result) => {
            tracing::info!(
                status = StatusCode::OK.as_u16(),
                sql = sql.as_deref(),
                format = result.format.as_str(),
                rows = result.row_count,
                truncated = result.truncated,
                query_ms = result.duration_ms,
                elapsed_ms,
                "POST /query"
            );
            success_response(result)
        }
        Err(err) => {
            tracing::info!(
                status = err.status.as_u16(),
                sql = sql.as_deref(),
                format = format.as_deref(),
                error_kind = %err.body.error_kind,
                error = %err.body.error,
                elapsed_ms,
                "POST /query"
            );
            err.into_response()
        }
    }
}

/// Parse the format, execute, serialize.
async fn run(state: &AppState, request: &QueryRequest) -> Result<SerializedResult, ApiError> {
    let format = parse_format(request.format.as_deref())
        .map_err(|err| ApiError::new(StatusCode::BAD_REQUEST, INVALID_FORMAT_KIND, err, Some(request.sql.clone())))?;

    let result = state.engine.execute(&request.sql).await.map_err(|err| ApiError::from_lens(&err, &request.sql))?;

    // Up to `max_query_rows` rows of encoding is CPU work; keep it off the
    // async workers.
    tokio::task::spawn_blocking(move || serialize(result, format))
        .await
        .map_err(|join_err| {
            let err = LensError::serialization(format.as_str(), format!("serializer task failed: {join_err}"));
            ApiError::from_lens(&err, &request.sql)
        })?
        .map_err(|err| ApiError::from_lens(&err, &request.sql))
}

fn success_response(result: SerializedResult) -> Response {
    let mut response = (StatusCode::OK, result.bytes).into_response();
    let headers = response.headers_mut();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type(result.format)));
    headers.insert(ROW_COUNT_HEADER, HeaderValue::from(result.row_count));
    headers.insert(TRUNCATED_HEADER, HeaderValue::from_static(if result.truncated { "true" } else { "false" }));
    headers.insert(DURATION_HEADER, HeaderValue::from(result.duration_ms));
    response
}

/// [`QueryFormat::content_type`], as a `'static` str for a [`HeaderValue`].
const fn content_type(format: QueryFormat) -> &'static str {
    match format {
        QueryFormat::Json => "application/json",
        QueryFormat::Csv => "text/csv; charset=utf-8",
    }
}

/// The requested format: JSON when absent, otherwise `json` or `csv` in any
/// case. Unlike [`QueryFormat::from_str`], which falls back to JSON, an
/// unknown name is an error: a client asking for `"parquet"` should learn it
/// will not get it.
pub fn parse_format(format: Option<&str>) -> Result<QueryFormat, String> {
    let Some(raw) = format else {
        return Ok(QueryFormat::Json);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "json" => Ok(QueryFormat::Json),
        "csv" => Ok(QueryFormat::Csv),
        _ => Err(format!("unsupported format {raw:?}; expected \"json\" or \"csv\"")),
    }
}

// ---------------------------------------------------------------------------
// Status codes
// ---------------------------------------------------------------------------

/// Who is to blame for a failure, which decides its status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Blame {
    /// The SQL: 400.
    Query,
    /// Not enough memory or spill space right now: 503.
    Capacity,
    /// LENS or its data: 500.
    Server,
    /// Nothing specific is known.
    Unknown,
}

/// The HTTP status a [`LensError`] is reported with. See the
/// [module docs](self) for the table.
pub fn status_for(err: &LensError) -> StatusCode {
    match err {
        LensError::QueryTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        LensError::QueryPlanError { source, .. } => match blame(source) {
            Blame::Query | Blame::Unknown => StatusCode::BAD_REQUEST,
            Blame::Capacity => StatusCode::SERVICE_UNAVAILABLE,
            Blame::Server => StatusCode::INTERNAL_SERVER_ERROR,
        },
        LensError::QueryExecutionError { source, .. } => match blame(source) {
            Blame::Query => StatusCode::BAD_REQUEST,
            Blame::Capacity => StatusCode::SERVICE_UNAVAILABLE,
            Blame::Server | Blame::Unknown => StatusCode::INTERNAL_SERVER_ERROR,
        },
        LensError::SerializationError { .. }
        | LensError::CatalogError { .. }
        | LensError::IoError { .. }
        | LensError::ConfigError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Blame for the deepest [`DataFusionError`] behind `source`. Sources that
/// are not DataFusion errors (LENS's own validation messages, a panic) are
/// [`Blame::Unknown`]; the stage decides.
fn blame(source: &BoxError) -> Blame {
    let Some(err) = source.downcast_ref::<DataFusionError>() else {
        return Blame::Unknown;
    };
    match err.find_root() {
        DataFusionError::SQL(..)
        | DataFusionError::Plan(_)
        | DataFusionError::SchemaError(..)
        | DataFusionError::NotImplemented(_) => Blame::Query,
        DataFusionError::ResourcesExhausted(_) => Blame::Capacity,
        DataFusionError::IoError(_)
        | DataFusionError::ParquetError(_)
        | DataFusionError::ObjectStore(_)
        | DataFusionError::Internal(_)
        | DataFusionError::ExecutionJoin(_) => Blame::Server,
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::CastError(_)
            | ArrowError::DivideByZero
            | ArrowError::ArithmeticOverflow(_)
            | ArrowError::ParseError(_)
            | ArrowError::InvalidArgumentError(_) => Blame::Query,
            ArrowError::MemoryError(_) => Blame::Capacity,
            _ => Blame::Server,
        },
        _ => Blame::Unknown,
    }
}

/// The message of `err` for [`ErrorBody::error`]: the underlying cause,
/// without the SQL text the [`LensError`] `Display` repeats.
pub fn error_message(err: &LensError) -> String {
    match err {
        LensError::QueryPlanError { source, .. } | LensError::QueryExecutionError { source, .. } => {
            match source.downcast_ref::<DataFusionError>() {
                // Without the "Error during planning: " style prefixes.
                Some(df) => df.find_root().message().into_owned(),
                None => source.to_string(),
            }
        }
        LensError::QueryTimeout { timeout_secs, .. } => {
            format!("query exceeded the {timeout_secs}s timeout and was cancelled")
        }
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn df(err: DataFusionError) -> BoxError {
        Box::new(err)
    }

    fn arrow(err: ArrowError) -> BoxError {
        df(DataFusionError::ArrowError(Box::new(err), None))
    }

    #[test]
    fn parse_format_defaults_to_json_and_rejects_unknown_names() {
        assert_eq!(parse_format(None), Ok(QueryFormat::Json));
        for (raw, format) in [("json", QueryFormat::Json), (" CSV ", QueryFormat::Csv), ("Json", QueryFormat::Json)] {
            assert_eq!(parse_format(Some(raw)), Ok(format), "{raw:?}");
        }
        for bad in ["", "xml", "parquet", "jsonl", "csv2"] {
            assert!(parse_format(Some(bad)).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn content_types_match_the_result_module() {
        for format in [QueryFormat::Json, QueryFormat::Csv] {
            assert_eq!(content_type(format), format.content_type());
        }
    }

    #[test]
    fn timeouts_are_504() {
        assert_eq!(status_for(&LensError::query_timeout("SELECT 1", 30)), StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn plan_errors_are_400_unless_the_cause_is_on_the_server() {
        let cases: Vec<(BoxError, StatusCode)> = vec![
            ("SQL is empty".into(), StatusCode::BAD_REQUEST),
            (df(DataFusionError::Plan("table 'x' not found".into())), StatusCode::BAD_REQUEST),
            (df(DataFusionError::NotImplemented("LATERAL".into())), StatusCode::BAD_REQUEST),
            (
                df(DataFusionError::Context("ctx".into(), Box::new(DataFusionError::Plan("p".into())))),
                StatusCode::BAD_REQUEST,
            ),
            (df(DataFusionError::Execution("unclassified".into())), StatusCode::BAD_REQUEST),
            (df(DataFusionError::IoError(io::Error::other("disk"))), StatusCode::INTERNAL_SERVER_ERROR),
            (df(DataFusionError::Internal("bug".into())), StatusCode::INTERNAL_SERVER_ERROR),
            (df(DataFusionError::ResourcesExhausted("pool".into())), StatusCode::SERVICE_UNAVAILABLE),
        ];
        for (source, status) in cases {
            let shown = source.to_string();
            assert_eq!(status_for(&LensError::query_plan("SQL", source)), status, "{shown}");
        }
    }

    #[test]
    fn execution_errors_are_500_unless_the_sql_or_capacity_is_to_blame() {
        let cases: Vec<(BoxError, StatusCode)> = vec![
            (arrow(ArrowError::CastError("'a' to Int32".into())), StatusCode::BAD_REQUEST),
            (arrow(ArrowError::DivideByZero), StatusCode::BAD_REQUEST),
            (arrow(ArrowError::ArithmeticOverflow("u64".into())), StatusCode::BAD_REQUEST),
            (df(DataFusionError::ResourcesExhausted("pool".into())), StatusCode::SERVICE_UNAVAILABLE),
            (arrow(ArrowError::MemoryError("oom".into())), StatusCode::SERVICE_UNAVAILABLE),
            (arrow(ArrowError::IoError("read".into(), io::Error::other("eof"))), StatusCode::INTERNAL_SERVER_ERROR),
            (df(DataFusionError::Execution("unclassified".into())), StatusCode::INTERNAL_SERVER_ERROR),
            ("query task panicked: boom".into(), StatusCode::INTERNAL_SERVER_ERROR),
        ];
        for (source, status) in cases {
            let shown = source.to_string();
            assert_eq!(status_for(&LensError::query_execution("SQL", source)), status, "{shown}");
        }
    }

    #[test]
    fn non_query_errors_are_500() {
        for err in [
            LensError::serialization("json", "broken"),
            LensError::catalog("slots", "broken"),
            LensError::io("/data", io::Error::other("broken")),
            LensError::config("data_dir", "broken"),
        ] {
            assert_eq!(status_for(&err), StatusCode::INTERNAL_SERVER_ERROR, "{err}");
        }
    }

    #[test]
    fn error_messages_leave_out_the_sql_and_datafusion_prefixes() {
        let err = LensError::query_plan(
            "SELECT * FROM solana.nope",
            df(DataFusionError::Plan("table 'solana.nope' not found".into())),
        );
        assert_eq!(error_message(&err), "table 'solana.nope' not found");

        let err = LensError::query_plan("", "SQL is empty");
        assert_eq!(error_message(&err), "SQL is empty");

        let err = LensError::query_timeout("SELECT 1", 7);
        assert_eq!(error_message(&err), "query exceeded the 7s timeout and was cancelled");
    }

    #[test]
    fn api_errors_carry_the_submitted_sql_and_the_error_kind() {
        let err = ApiError::from_lens(&LensError::query_timeout("SELECT 1", 1), "  SELECT 1 ");
        assert_eq!(err.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(err.body.error_kind, "query_timeout");
        assert_eq!(err.body.sql.as_deref(), Some("  SELECT 1 "));
    }
}
