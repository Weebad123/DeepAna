//! Errors: every way LENS can fail, as one typed enum.
//!
//! Every fallible function in LENS returns [`LensResult`]. Each variant
//! carries enough context (config field, table, SQL text, path, ...) for a
//! single log line or an API error response to be self-contained.
//!
//! | Stage             | Variants                                                         |
//! |-------------------|------------------------------------------------------------------|
//! | `config.rs`       | [`ConfigError`], [`IoError`]                                     |
//! | `catalog/`        | [`CatalogError`], [`IoError`]                                    |
//! | `engine/`         | [`QueryPlanError`], [`QueryExecutionError`], [`QueryTimeout`]    |
//! | `result/`         | [`SerializationError`]                                           |
//!
//! As in PRISM, there are deliberately no blanket `From` conversions: a bare
//! `?` could not supply the context each variant needs. Use the constructors
//! with `map_err` instead:
//!
//! ```ignore
//! fs::read_dir(&dir).map_err(|e| LensError::io(dir.display().to_string(), e))?;
//! ```
//!
//! The `Display` output of a variant includes its underlying cause. The cause
//! is also exposed through [`std::error::Error::source`] for callers that need
//! to downcast it.
//!
//! [`ConfigError`]: LensError::ConfigError
//! [`CatalogError`]: LensError::CatalogError
//! [`QueryPlanError`]: LensError::QueryPlanError
//! [`QueryTimeout`]: LensError::QueryTimeout
//! [`QueryExecutionError`]: LensError::QueryExecutionError
//! [`SerializationError`]: LensError::SerializationError
//! [`IoError`]: LensError::IoError

use thiserror::Error;

/// Boxed cause for variants whose underlying error type is not fixed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Result alias used across LENS.
pub type LensResult<T> = Result<T, LensError>;

/// Every way LENS can fail.
#[derive(Debug, Error)]
pub enum LensError {
    /// The configuration is invalid.
    #[error("invalid config field {field}: {reason}")]
    ConfigError {
        /// Name of the offending config field.
        field: String,
        /// Why it is invalid.
        reason: String,
    },

    /// A table could not be registered in or refreshed from the catalog.
    #[error("catalog failure on table {table}: {source}")]
    CatalogError {
        /// Table name: `"slots"`, `"transactions"`, `"instructions"`,
        /// `"token_transfers"`.
        table: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A SQL statement could not be parsed or planned.
    #[error("failed to plan query `{sql}`: {source}")]
    QueryPlanError {
        /// The SQL text as submitted.
        sql: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A query ran longer than the configured timeout and was cancelled.
    #[error("query `{sql}` timed out after {timeout_secs}s")]
    QueryTimeout {
        /// The SQL text as submitted.
        sql: String,
        /// The timeout that was exceeded, in seconds.
        timeout_secs: u64,
    },

    /// A planned query failed while executing.
    #[error("failed to execute query `{sql}`: {source}")]
    QueryExecutionError {
        /// The SQL text as submitted.
        sql: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A query result could not be serialized to the requested format.
    #[error("failed to serialize result as {format}: {source}")]
    SerializationError {
        /// Output format: `"json"`, `"csv"`, `"arrow"`, ...
        format: String,
        /// The underlying error.
        source: BoxError,
    },

    /// A filesystem operation failed.
    #[error("I/O failure on {path}: {source}")]
    IoError {
        /// Filesystem path the operation was about.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
}

impl LensError {
    /// Build a [`LensError::ConfigError`].
    pub fn config(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConfigError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Build a [`LensError::CatalogError`].
    pub fn catalog(table: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::CatalogError {
            table: table.into(),
            source: source.into(),
        }
    }

    /// Build a [`LensError::QueryPlanError`].
    pub fn query_plan(sql: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::QueryPlanError {
            sql: sql.into(),
            source: source.into(),
        }
    }

    /// Build a [`LensError::QueryTimeout`].
    pub fn query_timeout(sql: impl Into<String>, timeout_secs: u64) -> Self {
        Self::QueryTimeout {
            sql: sql.into(),
            timeout_secs,
        }
    }

    /// Build a [`LensError::QueryExecutionError`].
    pub fn query_execution(sql: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::QueryExecutionError {
            sql: sql.into(),
            source: source.into(),
        }
    }

    /// Build a [`LensError::SerializationError`].
    pub fn serialization(format: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::SerializationError {
            format: format.into(),
            source: source.into(),
        }
    }

    /// Build a [`LensError::IoError`].
    pub fn io(path: impl Into<String>, source: std::io::Error) -> Self {
        Self::IoError {
            path: path.into(),
            source,
        }
    }

    /// Stable `snake_case` name of the variant, for logs and metric labels.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::ConfigError { .. } => "config_error",
            Self::CatalogError { .. } => "catalog_error",
            Self::QueryPlanError { .. } => "query_plan_error",
            Self::QueryTimeout { .. } => "query_timeout",
            Self::QueryExecutionError { .. } => "query_execution_error",
            Self::SerializationError { .. } => "serialization_error",
            Self::IoError { .. } => "io_error",
        }
    }

    /// The config field this error is about, for [`LensError::ConfigError`].
    pub fn config_field(&self) -> Option<&str> {
        match self {
            Self::ConfigError { field, .. } => Some(field),
            _ => None,
        }
    }
}
