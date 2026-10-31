//! Change log events: the stream PRISM emits as it processes the chain.
//!
//! This is PRISM's **output contract** toward every downstream system.
//! Consumers (query engines, cache invalidators, indexers, alerting) subscribe
//! to this stream instead of scanning storage. Each [`ChangeEvent::New`]
//! carries the exact Parquet paths that were written for a slot, so a
//! subscriber knows which files to open without listing a directory.
//!
//! `New` and `Skip` events are emitted strictly once per slot, in slot order,
//! after the slot's Parquet files are durably written. [`ChangeEvent::Final`]
//! is a separate, coarser signal emitted by the commitment tracker whenever the
//! finalized watermark moves: it says which slots, and so which already
//! written files, can no longer be invalidated. Wire format is JSON (see
//! [`ChangeEvent::to_json`]), with a top-level `"event"` discriminator so the
//! stream stays forward-compatible when new variants are added.
//!
//! On disk every event is wrapped in an [`EventEnvelope`] that the change log
//! stamps with a gap-free, strictly increasing `id`. The envelope's fields sit
//! next to the event's own, so a line still has the `"event"` discriminator at
//! the top level:
//!
//! ```text
//! {"id":7,"emitted_at":1761040000,"chain":"solana-devnet","event":"skip","slot":350123457,"reason":"validator_skipped"}
//! ```

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::udm::{DeepAnaSlot, unix_to_datetime};

/// Why a slot produced no data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// The leader did not produce a block for this slot. Permanent: the slot
    /// will never contain data.
    ValidatorSkipped,
    /// The RPC could not serve the block (timeout, node behind, pruned).
    /// Transient: the slot may be back-filled by the gap handler later.
    RpcUnavailable,
    /// The slot is older than the ingestion cursor and was intentionally not
    /// processed. Permanent for this run.
    BelowCursor,
}

impl SkipReason {
    pub const ALL: [SkipReason; 3] = [
        SkipReason::ValidatorSkipped,
        SkipReason::RpcUnavailable,
        SkipReason::BelowCursor,
    ];

    /// Wire / log representation (`snake_case`, matches the serde encoding).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidatorSkipped => "validator_skipped",
            Self::RpcUnavailable => "rpc_unavailable",
            Self::BelowCursor => "below_cursor",
        }
    }

    /// Whether a later attempt could turn this skip into a [`ChangeEvent::New`].
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::RpcUnavailable)
    }
}

impl fmt::Display for SkipReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SkipReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "validator_skipped" => Ok(Self::ValidatorSkipped),
            "rpc_unavailable" => Ok(Self::RpcUnavailable),
            "below_cursor" => Ok(Self::BelowCursor),
            other => Err(format!("unknown skip reason: {other:?}")),
        }
    }
}

/// One entry in PRISM's change log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ChangeEvent {
    /// A slot was processed and its rows were written. Emitted once per
    /// successfully processed slot; the most common event.
    New {
        /// Slot number.
        slot: u64,
        /// The slot's blockhash, Solana's block identifier.
        blockhash: String,
        /// Parent slot. Lets downstream systems detect reorgs if needed.
        parent_slot: u64,
        /// Unix timestamp (seconds, UTC). `None` for slots without one.
        block_time: Option<i64>,
        /// Number of transactions in the slot.
        transaction_count: u32,
        /// Parquet files written for this slot batch. The actionable part of
        /// the event: consumers open exactly these files.
        parquet_paths: Vec<String>,
    },
    /// A slot produced no data and will not be written (at least this pass).
    Skip {
        /// Slot number.
        slot: u64,
        /// Why it was skipped.
        reason: SkipReason,
    },
    /// The finalized watermark advanced: every slot at or below `slot` is now
    /// finalized and can never be rolled back. Rows written with
    /// `is_finalized = false` for slots in `previous_slot + 1..=slot` are now
    /// permanent.
    Final {
        /// The new finalized watermark.
        slot: u64,
        /// Blockhash of `slot`.
        blockhash: String,
        /// The watermark announced by the previous `Final` event (0 for the
        /// first one of a run).
        previous_slot: u64,
    },
}

impl ChangeEvent {
    /// Build a [`ChangeEvent::New`] from the UDM slot row and the files
    /// written for it. This is the one place the two contracts meet, so the
    /// header fields can never drift from what was stored.
    pub fn new_from_slot(slot: &DeepAnaSlot, parquet_paths: Vec<String>) -> Self {
        Self::New {
            slot: slot.slot,
            blockhash: slot.blockhash.clone(),
            parent_slot: slot.parent_slot,
            block_time: slot.block_time,
            transaction_count: slot.transaction_count,
            parquet_paths,
        }
    }

    /// Build a [`ChangeEvent::Skip`].
    pub const fn skip(slot: u64, reason: SkipReason) -> Self {
        Self::Skip { slot, reason }
    }

    /// Build a [`ChangeEvent::Final`].
    pub fn finalized(slot: u64, blockhash: impl Into<String>, previous_slot: u64) -> Self {
        Self::Final {
            slot,
            blockhash: blockhash.into(),
            previous_slot,
        }
    }

    /// The slot this event is about. Every variant has one.
    pub const fn slot(&self) -> u64 {
        match self {
            Self::New { slot, .. } | Self::Skip { slot, .. } | Self::Final { slot, .. } => *slot,
        }
    }

    /// Variant name as it appears in the `"event"` discriminator.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::New { .. } => "new",
            Self::Skip { .. } => "skip",
            Self::Final { .. } => "final",
        }
    }

    pub const fn is_new(&self) -> bool {
        matches!(self, Self::New { .. })
    }

    pub const fn is_skip(&self) -> bool {
        matches!(self, Self::Skip { .. })
    }

    pub const fn is_final(&self) -> bool {
        matches!(self, Self::Final { .. })
    }

    /// Parquet paths written for this event. Empty for skips and finals.
    pub fn parquet_paths(&self) -> &[String] {
        match self {
            Self::New { parquet_paths, .. } => parquet_paths,
            Self::Skip { .. } | Self::Final { .. } => &[],
        }
    }

    /// Skip reason, if this is a skip.
    pub const fn skip_reason(&self) -> Option<SkipReason> {
        match self {
            Self::Skip { reason, .. } => Some(*reason),
            Self::New { .. } | Self::Final { .. } => None,
        }
    }

    /// Block time as a `chrono` UTC datetime, when the event carries one.
    pub fn block_datetime(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::New { block_time, .. } => block_time.and_then(unix_to_datetime),
            Self::Skip { .. } | Self::Final { .. } => None,
        }
    }

    /// Serialise to a single JSON line (no trailing newline), the change
    /// log's wire format.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ChangeEvent is always JSON-serialisable")
    }

    /// Parse one JSON line produced by [`Self::to_json`].
    pub fn from_json(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}

impl fmt::Display for ChangeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::New {
                slot,
                blockhash,
                transaction_count,
                parquet_paths,
                ..
            } => write!(
                f,
                "new slot={slot} blockhash={blockhash} txs={transaction_count} files={}",
                parquet_paths.len()
            ),
            Self::Skip { slot, reason } => write!(f, "skip slot={slot} reason={reason}"),
            Self::Final {
                slot,
                blockhash,
                previous_slot,
            } => write!(
                f,
                "final slot={slot} blockhash={blockhash} previous={previous_slot}"
            ),
        }
    }
}

impl From<&DeepAnaSlot> for ChangeEvent {
    /// A `New` event with no Parquet paths. Use [`ChangeEvent::new_from_slot`]
    /// when the written files are known.
    fn from(slot: &DeepAnaSlot) -> Self {
        Self::new_from_slot(slot, Vec::new())
    }
}

/// A [`ChangeEvent`] as the change log records it: one line of the log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Position in the log. The first event is `1`; every later one is the
    /// previous `id + 1`, with no gaps, across restarts.
    pub id: u64,
    /// Unix timestamp (seconds, UTC) at which the event was recorded.
    pub emitted_at: i64,
    /// Chain id the event is about: `"solana-mainnet"`, `"solana-devnet"`, ...
    pub chain: String,
    /// The event itself, flattened into the envelope.
    #[serde(flatten)]
    pub event: ChangeEvent,
}

impl EventEnvelope {
    /// Serialise to a single JSON line (no trailing newline), the change
    /// log's wire format.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("EventEnvelope is always JSON-serialisable")
    }

    /// Parse one JSON line produced by [`Self::to_json`].
    pub fn from_json(line: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(line)
    }
}
