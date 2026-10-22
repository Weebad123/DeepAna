//! Errors: every way PRISM can fail, as one typed enum.
//!
//! Every fallible function in every module returns [`PrismResult`]. A failure
//! anywhere in the pipeline propagates up the call stack as a [`PrismError`]
//! variant that carries enough context (slot, RPC method, table, path, ...)
//! for the top level to decide what to do with it: retry, skip, halt or alert.
//!
//! Each stage produces its own variants:
//!
//! | Stage                       | Variants                                                               |
//! |-----------------------------|------------------------------------------------------------------------|
//! | `adapter/solana.rs`         | [`RpcError`], [`RateLimitError`], [`SlotSkipped`]                      |
//! | `slot_processor/fetcher.rs` | [`RpcError`], [`RateLimitError`], [`SlotSkipped`]                      |
//! | `normalizer/`               | [`DeserializeError`]                                                   |
//! | `decoder/spl_token.rs`      | [`DecodeError`]                                                        |
//! | `account_resolver/`         | [`AccountResolveError`]                                                |
//! | `storage/writer.rs`         | [`StorageError`]                                                       |
//! | `ingestor/cursor.rs`        | [`CursorError`]                                                        |
//! | `lib.rs` / `main`           | receives all of them (plus [`ConfigError`]) and picks the action       |
//!
//! There are deliberately no blanket `From<ClientError>` / `From<io::Error>`
//! conversions: a bare `?` could not supply the context each variant needs.
//! Use the constructors with `map_err` instead:
//!
//! ```ignore
//! client.get_block(slot).map_err(|e| PrismError::rpc(Some(slot), "getBlock", e))?;
//! ```
//!
//! The `Display` output of a variant includes its underlying cause, so a
//! single log line is self-contained. The cause is also exposed through
//! [`std::error::Error::source`] for callers that need to downcast it.
//!
//! [`RpcError`]: PrismError::RpcError
//! [`RateLimitError`]: PrismError::RateLimitError
//! [`SlotSkipped`]: PrismError::SlotSkipped
//! [`DeserializeError`]: PrismError::DeserializeError
//! [`DecodeError`]: PrismError::DecodeError
//! [`AccountResolveError`]: PrismError::AccountResolveError
//! [`StorageError`]: PrismError::StorageError
//! [`CursorError`]: PrismError::CursorError
//! [`ConfigError`]: PrismError::ConfigError

use std::time::Duration;

use parquet::errors::ParquetError;
use solana_client::client_error::ClientError;
use thiserror::Error;

/// Boxed cause for variants whose underlying error type is not fixed.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Result alias used across PRISM.
pub type PrismResult<T> = Result<T, PrismError>;

/// Every way PRISM can fail.
#[derive(Debug, Error)]
pub enum PrismError {
    /// An RPC call failed (timeout, transport error, server-side error).
    #[error("RPC {method} failed{}: {source}", at_slot(.slot))]
    RpcError {
        /// Slot the call was about, when it was slot-scoped.
        slot: Option<u64>,
        /// RPC method that failed: `"getBlock"`, `"getSlot"`, ...
        method: String,
        /// The original `solana-client` error.
        source: ClientError,
    },

    /// The leader did not produce a block for this slot. Not a fault: the
    /// slot will never contain data.
    #[error("slot {slot} was skipped (no block produced)")]
    SlotSkipped {
        /// Slot number.
        slot: u64,
    },

    /// The RPC answered `429 Too Many Requests`.
    #[error("RPC rate limit exceeded{}", retry_hint(.retry_after_ms))]
    RateLimitError {
        /// How long the server asked us to wait, in milliseconds. Parsed from
        /// the `Retry-After` response header when present.
        retry_after_ms: Option<u64>,
    },

    /// Raw chain data could not be deserialized into its typed form.
    #[error("failed to deserialize {context}{}: {source}", at_slot(.slot))]
    DeserializeError {
        /// Slot the data belongs to, when known.
        slot: Option<u64>,
        /// What was being deserialized: `"transaction meta"`,
        /// `"mint account"`, `"token instruction"`, ...
        context: String,
        /// The underlying deserialization error.
        source: BoxError,
    },

    /// A program instruction could not be decoded.
    #[error("failed to decode instruction {instruction_index} of program {program_id}: {reason}")]
    DecodeError {
        /// Base58 id of the program whose instruction failed to decode.
        program_id: String,
        /// Index of the instruction within its transaction.
        instruction_index: u8,
        /// Why: `"unknown discriminant"`, `"data too short"`, `"borsh error"`, ...
        reason: String,
    },

    /// A Parquet table could not be written.
    #[error(
        "storage failure on table {table} for slots {}..={}: {source}",
        .slot_range.0,
        .slot_range.1
    )]
    StorageError {
        /// Table name: `"slots"`, `"transactions"`, `"instructions"`,
        /// `"token_transfers"`.
        table: String,
        /// First and last slot (both inclusive) of the affected batch.
        slot_range: (u64, u64),
        /// The underlying Parquet error.
        source: ParquetError,
    },

    /// The ingestion cursor file could not be read or written.
    #[error("cursor file {path} is unusable: {source}")]
    CursorError {
        /// Filesystem path of the cursor file.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// An account referenced by an instruction could not be resolved.
    #[error("failed to resolve account {pubkey}: {source}")]
    AccountResolveError {
        /// Base58 pubkey of the account.
        pubkey: String,
        /// The underlying error (RPC, deserialization, ...).
        source: BoxError,
    },

    /// The configuration is invalid.
    #[error("invalid config field {field}: {reason}")]
    ConfigError {
        /// Name of the offending config field.
        field: String,
        /// Why it is invalid.
        reason: String,
    },
}

fn at_slot(slot: &Option<u64>) -> String {
    slot.map(|s| format!(" at slot {s}")).unwrap_or_default()
}

fn retry_hint(retry_after_ms: &Option<u64>) -> String {
    retry_after_ms
        .map(|ms| format!(", retry after {ms} ms"))
        .unwrap_or_default()
}

impl PrismError {
    /// Build a [`PrismError::RpcError`].
    pub fn rpc(slot: Option<u64>, method: impl Into<String>, source: ClientError) -> Self {
        Self::RpcError {
            slot,
            method: method.into(),
            source,
        }
    }

    /// Build a [`PrismError::SlotSkipped`].
    pub const fn slot_skipped(slot: u64) -> Self {
        Self::SlotSkipped { slot }
    }

    /// Build a [`PrismError::RateLimitError`].
    pub const fn rate_limited(retry_after_ms: Option<u64>) -> Self {
        Self::RateLimitError { retry_after_ms }
    }

    /// Build a [`PrismError::DeserializeError`].
    pub fn deserialize(
        slot: Option<u64>,
        context: impl Into<String>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self::DeserializeError {
            slot,
            context: context.into(),
            source: source.into(),
        }
    }

    /// Build a [`PrismError::DecodeError`].
    pub fn decode(
        program_id: impl Into<String>,
        instruction_index: u8,
        reason: impl Into<String>,
    ) -> Self {
        Self::DecodeError {
            program_id: program_id.into(),
            instruction_index,
            reason: reason.into(),
        }
    }

    /// Build a [`PrismError::StorageError`]. `slot_range` is inclusive on both
    /// ends.
    ///
    /// Accepts anything that converts into a [`ParquetError`], which covers
    /// the `ArrowError` and `std::io::Error` the writer also runs into.
    pub fn storage(
        table: impl Into<String>,
        slot_range: (u64, u64),
        source: impl Into<ParquetError>,
    ) -> Self {
        Self::StorageError {
            table: table.into(),
            slot_range,
            source: source.into(),
        }
    }

    /// Build a [`PrismError::CursorError`].
    pub fn cursor(path: impl Into<String>, source: std::io::Error) -> Self {
        Self::CursorError {
            path: path.into(),
            source,
        }
    }

    /// Build a [`PrismError::AccountResolveError`].
    pub fn account_resolve(pubkey: impl Into<String>, source: impl Into<BoxError>) -> Self {
        Self::AccountResolveError {
            pubkey: pubkey.into(),
            source: source.into(),
        }
    }

    /// Build a [`PrismError::ConfigError`].
    pub fn config(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConfigError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Stable `snake_case` name of the variant, for logs and metric labels.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::RpcError { .. } => "rpc_error",
            Self::SlotSkipped { .. } => "slot_skipped",
            Self::RateLimitError { .. } => "rate_limit_error",
            Self::DeserializeError { .. } => "deserialize_error",
            Self::DecodeError { .. } => "decode_error",
            Self::StorageError { .. } => "storage_error",
            Self::CursorError { .. } => "cursor_error",
            Self::AccountResolveError { .. } => "account_resolve_error",
            Self::ConfigError { .. } => "config_error",
        }
    }

    /// The single slot this error is about, when it has one.
    ///
    /// [`PrismError::StorageError`] spans a range and returns `None`; read its
    /// `slot_range` instead.
    pub const fn slot(&self) -> Option<u64> {
        match self {
            Self::RpcError { slot, .. } | Self::DeserializeError { slot, .. } => *slot,
            Self::SlotSkipped { slot } => Some(*slot),
            Self::RateLimitError { .. }
            | Self::DecodeError { .. }
            | Self::StorageError { .. }
            | Self::CursorError { .. }
            | Self::AccountResolveError { .. }
            | Self::ConfigError { .. } => None,
        }
    }

    /// Whether repeating the same operation could succeed.
    ///
    /// Only the RPC-facing variants are transient. Everything else is either
    /// deterministic (the same bytes fail to decode the same way) or a local
    /// fault that needs an operator. The caller owns backoff and the attempt
    /// budget; see [`Self::retry_after`] for the server's hint.
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::RpcError { .. } | Self::RateLimitError { .. })
    }

    /// How long the server asked us to wait before retrying, if it said so.
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimitError {
                retry_after_ms: Some(ms),
            } => Some(Duration::from_millis(*ms)),
            _ => None,
        }
    }
}
