//! Chain adapters: the boundary between PRISM and a blockchain.
//!
//! [`ChainAdapter`] is a contract, not an implementation. It states what PRISM
//! needs from *any* chain in order to do its job (where is the head, give me
//! this block, give me these accounts) and nothing about how a chain provides
//! it. Everything downstream of the adapter (slot processor, normalizer,
//! account resolver) talks to this trait and to the chain-agnostic types in
//! this file, never to an SDK.
//!
//! | File          | Holds                                                        |
//! |---------------|--------------------------------------------------------------|
//! | `mod.rs`      | [`ChainAdapter`] and the types its signatures use            |
//! | [`config`]    | [`AdapterConfig`](config::AdapterConfig) and per-chain config |
//! | [`solana`]    | [`SolanaAdapter`](solana::SolanaAdapter), the first adapter   |
//!
//! The one place chain-specific data crosses the boundary is
//! [`ChainBlock::raw`]: the header fields are normalised, the payload is
//! handed over untouched for the chain's own normalizer to take apart.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use solana_transaction_status::EncodedConfirmedBlock;

use crate::types::error::PrismResult;

pub mod config;
pub mod solana;

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// The family of chains an adapter speaks to. Chains in one family share a
/// data model, so they share a normalizer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainFamily {
    Solana,
    Evm,
}

impl ChainFamily {
    /// Log / metric-label representation (matches the serde encoding).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Solana => "solana",
            Self::Evm => "evm",
        }
    }
}

impl fmt::Display for ChainFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How settled a piece of chain state must be before PRISM reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommitmentLevel {
    /// Supermajority vote on Solana; ~12 confirmations on EVM.
    Confirmed,
    /// Locked in on Solana; checkpoint finality on EVM.
    Finalized,
    /// Latest seen, not yet voted on. Solana only, and risky: the state may
    /// be on a fork that is later abandoned.
    Processed,
}

impl CommitmentLevel {
    /// Config / log representation (matches the serde encoding).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
            Self::Processed => "processed",
        }
    }
}

impl fmt::Display for CommitmentLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The chain-specific payload of a block, exactly as the chain's RPC returned
/// it.
///
/// An `Evm` variant is added when the EVM adapter is built.
#[derive(Debug, Clone, PartialEq)]
pub enum ChainBlockData {
    Solana(Box<EncodedConfirmedBlock>),
}

/// One block, with a chain-agnostic header around the raw payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ChainBlock {
    /// Slot number on Solana, block height on EVM.
    pub slot_or_height: u64,
    /// Blockhash on Solana, block hash on EVM.
    pub block_id: String,
    /// Parent blockhash / parent hash.
    pub parent_id: String,
    /// Unix timestamp in seconds. `None` when the chain omits it, which
    /// Solana does for old slots.
    pub timestamp: Option<i64>,
    /// The chain-specific payload.
    pub raw: ChainBlockData,
}

/// Raw on-chain state of one account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AccountData {
    /// The program that owns this account.
    pub owner_program: String,
    /// Balance in lamports on Solana; 0 on EVM.
    pub lamports: u64,
    /// Raw account data bytes.
    pub data: Vec<u8>,
    /// Whether this account is a program.
    pub executable: bool,
    /// Solana-specific; 0 on EVM.
    pub rent_epoch: u64,
}

/// The current head of the chain at some commitment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTip {
    /// Slot number on Solana, block height on EVM.
    pub slot_or_height: u64,
    /// Blockhash / block hash of the head block.
    pub block_id: String,
    /// The commitment this tip was read at.
    pub commitment: CommitmentLevel,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters an adapter keeps about its RPC traffic. Shared across tasks behind
/// an `Arc`; every counter only ever goes up.
#[derive(Debug, Default)]
pub struct AdapterMetrics {
    requests: AtomicU64,
    errors: AtomicU64,
    retries: AtomicU64,
    rate_limited: AtomicU64,
    slots_skipped: AtomicU64,
}

/// A point-in-time copy of [`AdapterMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AdapterMetricsSnapshot {
    /// RPC attempts made, retries included.
    pub requests: u64,
    /// Attempts that failed. A skipped slot is an answer, not a failure, and
    /// is not counted here.
    pub errors: u64,
    /// Attempts that were a retry of an earlier failed attempt.
    pub retries: u64,
    /// Attempts the RPC rejected with `429 Too Many Requests`. A subset of
    /// `errors`.
    pub rate_limited: u64,
    /// Block requests answered with "this slot has no block".
    pub slots_skipped: u64,
}

impl AdapterMetrics {
    pub fn snapshot(&self) -> AdapterMetricsSnapshot {
        AdapterMetricsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
            rate_limited: self.rate_limited.load(Ordering::Relaxed),
            slots_skipped: self.slots_skipped.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn record_request(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rate_limited(&self) {
        self.rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_slot_skipped(&self) {
        self.slots_skipped.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// What PRISM needs from a blockchain.
///
/// Implementations own everything chain-specific behind these methods: the
/// SDK, the wire format, rate limiting, retries and the mapping of the chain's
/// failure modes onto [`PrismError`](crate::types::error::PrismError). A
/// method that returns `Err` has already retried what was worth retrying.
#[async_trait]
pub trait ChainAdapter: Send + Sync {
    /// The family of chains this adapter speaks to.
    fn chain_family(&self) -> ChainFamily;

    /// The specific network: `"solana-mainnet"`, `"solana-devnet"`,
    /// `"eth-mainnet"`, ...
    fn chain_id(&self) -> &str;

    /// The current head of the chain at `commitment`.
    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip>;

    /// The block at `slot_or_height`, read at the adapter's configured
    /// commitment.
    ///
    /// Returns [`PrismError::SlotSkipped`] when the chain produced no block
    /// there and never will. Any other error means the block could not be
    /// fetched *this time*: asking for a block above the current tip is such
    /// an error, so callers should stay at or below [`Self::get_tip`].
    ///
    /// [`PrismError::SlotSkipped`]: crate::types::error::PrismError::SlotSkipped
    async fn get_block(&self, slot_or_height: u64) -> PrismResult<ChainBlock>;

    /// Raw state of `addresses`, read at the adapter's configured commitment.
    ///
    /// The result has the same length and order as `addresses`; an account
    /// that does not exist is `None`. Any number of addresses may be passed:
    /// the adapter splits them into batches the RPC accepts, so entries from
    /// different batches may have been read at different blocks.
    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>>;

    /// A single, un-retried probe: `Ok` when the RPC is reachable and
    /// considers itself caught up with the chain.
    async fn health_check(&self) -> PrismResult<()>;
}
