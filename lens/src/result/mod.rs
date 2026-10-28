//! Result: what a query hands to the API, and the formats it goes out in.
//!
//! [`QueryEngine::execute`](crate::QueryEngine::execute) returns a
//! [`QueryResult`] holding Arrow [`RecordBatch`]es. The API layer never
//! touches those: it calls [`serialize`] and sends the bytes of the
//! [`SerializedResult`] it gets back.
//!
//! ```text
//!   QueryEngine::execute ──► QueryResult ──► serialize(result, format) ──► SerializedResult ──► HTTP body
//!                            (Arrow)                                      (bytes + content type)
//! ```
//!
//! | File           | Holds                                                     |
//! |----------------|-----------------------------------------------------------|
//! | `mod.rs`       | [`QueryResult`], [`QueryFormat`], [`SerializedResult`]    |
//! | [`serializer`] | [`serialize`], [`serialize_json`], [`serialize_csv`]      |

pub mod serializer;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;

pub use serializer::{serialize, serialize_csv, serialize_json};

// ---------------------------------------------------------------------------
// QueryResult
// ---------------------------------------------------------------------------

/// The rows a query returned, with what the API needs to describe them.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The SQL text that was run, trimmed.
    pub sql: String,
    /// The rows, at most `max_query_rows` of them in total. Holds no empty
    /// batch.
    pub batches: Vec<RecordBatch>,
    /// Rows across `batches`.
    pub row_count: usize,
    /// Column names and types. Set even when no row came back.
    pub schema: SchemaRef,
    /// Wall time from the start of planning to the last row, in milliseconds.
    pub duration_ms: u64,
    /// Whether the query produced more than `max_query_rows` rows and the
    /// rest were dropped.
    pub truncated: bool,
    /// How many rows the query produced before the row limit was applied,
    /// when known. Equal to `row_count` when nothing was dropped; `None`
    /// when [`Self::truncated`], since the engine stops reading one row past
    /// the limit rather than counting the rest.
    pub total_rows_before_limit: Option<usize>,
}

impl QueryResult {
    pub fn column_count(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema.fields().iter().map(|field| field.name().as_str()).collect()
    }
}

// ---------------------------------------------------------------------------
// QueryFormat
// ---------------------------------------------------------------------------

/// The formats a result can be serialized to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QueryFormat {
    /// An object with the rows as an array of `{column: value}` objects, plus
    /// the query metadata. See [`serialize_json`].
    #[default]
    Json,
    /// RFC 4180 CSV with a header line. See [`serialize_csv`].
    Csv,
}

impl QueryFormat {
    /// The format named by `s` (e.g. a `?format=` query parameter), ignoring
    /// case and surrounding whitespace. Anything other than `"csv"` is
    /// [`QueryFormat::Json`], the default.
    #[allow(clippy::should_implement_trait)] // infallible, unlike `FromStr`
    pub fn from_str(s: &str) -> QueryFormat {
        if s.trim().eq_ignore_ascii_case("csv") { QueryFormat::Csv } else { QueryFormat::Json }
    }

    /// The HTTP `Content-Type` of a body in this format.
    pub fn content_type(&self) -> &str {
        match self {
            QueryFormat::Json => "application/json",
            QueryFormat::Csv => "text/csv; charset=utf-8",
        }
    }

    /// Lowercase name of the format, as used in errors: `"json"`, `"csv"`.
    pub const fn as_str(&self) -> &'static str {
        match self {
            QueryFormat::Json => "json",
            QueryFormat::Csv => "csv",
        }
    }
}

// ---------------------------------------------------------------------------
// SerializedResult
// ---------------------------------------------------------------------------

/// A [`QueryResult`] turned into a response body, with the metadata the API
/// puts in headers or logs.
#[derive(Debug, Clone)]
pub struct SerializedResult {
    /// The response body.
    pub bytes: Vec<u8>,
    /// The format `bytes` are in; gives the `Content-Type`.
    pub format: QueryFormat,
    pub row_count: usize,
    pub duration_ms: u64,
    pub truncated: bool,
    pub sql: String,
}
