//! Typed configuration for chain adapters.
//!
//! [`AdapterConfig`] is the chain-agnostic base: the knobs every adapter has
//! regardless of chain (identity, retry policy, timeouts). Each chain wraps it
//! in its own config that adds what only that chain needs;
//! [`SolanaAdapterConfig`] is the first.
//!
//! Both deserialize with every field optional except the ones that have no
//! sensible default (`chain_id`, `rpc_url`). Those default to empty and are
//! rejected by `validate`, which adapters call on construction, so a config
//! that built an adapter is known to be usable.

use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::CommitmentLevel;
use crate::types::error::{PrismError, PrismResult};

/// Most accounts a single `getMultipleAccounts` call may ask for. Enforced by
/// the RPC; a larger batch is rejected outright.
pub const MAX_ACCOUNTS_PER_BATCH: usize = 100;

// ---------------------------------------------------------------------------
// AdapterConfig
// ---------------------------------------------------------------------------

/// Configuration shared by every chain adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AdapterConfig {
    /// Which network this adapter is pointed at: `"solana-mainnet"`,
    /// `"solana-devnet"`, `"eth-mainnet"`, ... Required.
    pub chain_id: String,
    /// How many times a failed request is retried before giving up. `0`
    /// disables retries.
    pub max_retries: u32,
    /// Delay before the first retry, in milliseconds. Doubles on each further
    /// retry; adapters add jitter on top.
    pub retry_base_delay_ms: u64,
    /// Upper bound on the delay between two retries, in milliseconds.
    pub retry_max_delay_ms: u64,
    /// How long a single request may take, in milliseconds.
    pub request_timeout_ms: u64,
    /// How often the adapter's health should be probed, in seconds.
    pub health_check_interval_s: u64,
}

impl Default for AdapterConfig {
    fn default() -> Self {
        Self {
            chain_id: String::new(),
            max_retries: 5,
            retry_base_delay_ms: 500,
            retry_max_delay_ms: 30_000,
            request_timeout_ms: 10_000,
            health_check_interval_s: 30,
        }
    }
}

impl AdapterConfig {
    /// The defaults, for the given network.
    pub fn new(chain_id: impl Into<String>) -> Self {
        Self {
            chain_id: chain_id.into(),
            ..Self::default()
        }
    }

    pub const fn retry_base_delay(&self) -> Duration {
        Duration::from_millis(self.retry_base_delay_ms)
    }

    pub const fn retry_max_delay(&self) -> Duration {
        Duration::from_millis(self.retry_max_delay_ms)
    }

    pub const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    pub const fn health_check_interval(&self) -> Duration {
        Duration::from_secs(self.health_check_interval_s)
    }

    /// Check that the values can actually be used. The first offending field
    /// is reported as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        if self.chain_id.trim().is_empty() {
            return Err(PrismError::config("chain_id", "must not be empty"));
        }
        if self.retry_base_delay_ms == 0 {
            return Err(PrismError::config(
                "retry_base_delay_ms",
                "must be greater than 0",
            ));
        }
        if self.retry_max_delay_ms < self.retry_base_delay_ms {
            return Err(PrismError::config(
                "retry_max_delay_ms",
                format!(
                    "must be at least retry_base_delay_ms ({})",
                    self.retry_base_delay_ms
                ),
            ));
        }
        if self.request_timeout_ms == 0 {
            return Err(PrismError::config(
                "request_timeout_ms",
                "must be greater than 0",
            ));
        }
        if self.health_check_interval_s == 0 {
            return Err(PrismError::config(
                "health_check_interval_s",
                "must be greater than 0",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TransactionEncoding
// ---------------------------------------------------------------------------

/// How the RPC should encode the transactions of a block.
///
/// PRISM's own mirror of the Solana SDK's `UiTransactionEncoding`, so that
/// the SDK type does not leak into configuration. The SDK's legacy `Binary`
/// variant (an alias for base58) is deliberately not mirrored.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionEncoding {
    /// Raw transaction bytes as base58. Slow to encode and decode.
    #[default]
    Base58,
    /// Raw transaction bytes as base64. The fast binary encoding.
    Base64,
    /// The transaction as a JSON object, instructions left compiled.
    Json,
    /// Like [`Self::Json`], with instructions of well-known programs parsed
    /// by the RPC node.
    JsonParsed,
}

impl TransactionEncoding {
    pub const ALL: [TransactionEncoding; 4] = [
        TransactionEncoding::Base58,
        TransactionEncoding::Base64,
        TransactionEncoding::Json,
        TransactionEncoding::JsonParsed,
    ];

    /// Config / log representation (matches the serde encoding).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base58 => "base58",
            Self::Base64 => "base64",
            Self::Json => "json",
            Self::JsonParsed => "json_parsed",
        }
    }
}

impl fmt::Display for TransactionEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// SolanaAdapterConfig
// ---------------------------------------------------------------------------

/// Configuration of the Solana adapter: the shared base plus what only
/// Solana needs.
///
/// `Debug` redacts `rpc_url` and `ws_url` down to scheme and host, because
/// providers put the API key in the path or query string and configs end up
/// in logs.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct SolanaAdapterConfig {
    /// The chain-agnostic settings.
    pub base: AdapterConfig,
    /// HTTP(S) JSON-RPC endpoint, e.g.
    /// `"https://mainnet.helius-rpc.com/?api-key=..."`. Required.
    pub rpc_url: String,
    /// WebSocket endpoint, for future subscriptions. Not used yet.
    pub ws_url: Option<String>,
    /// Commitment blocks and accounts are read at.
    pub commitment: CommitmentLevel,
    /// Client-side rate limit. The default of 50 is the Helius free tier.
    pub requests_per_second: u32,
    /// Accounts per `getMultipleAccounts` call, at most
    /// [`MAX_ACCOUNTS_PER_BATCH`].
    pub max_accounts_per_batch: usize,
    /// Whether transactions that failed on chain are left out of PRISM's
    /// output.
    ///
    /// The adapter itself always returns blocks complete: dropping
    /// transactions there would shift the block position of every later
    /// transaction. The flag is carried here for the normalizer to apply.
    pub skip_failed_transactions: bool,
    /// Encoding requested for the transactions of a block.
    pub encoding: TransactionEncoding,
}

impl Default for SolanaAdapterConfig {
    fn default() -> Self {
        Self {
            base: AdapterConfig::default(),
            rpc_url: String::new(),
            ws_url: None,
            commitment: CommitmentLevel::Confirmed,
            requests_per_second: 50,
            max_accounts_per_batch: MAX_ACCOUNTS_PER_BATCH,
            skip_failed_transactions: true,
            encoding: TransactionEncoding::default(),
        }
    }
}

impl SolanaAdapterConfig {
    /// The defaults, for the given network and RPC endpoint.
    pub fn new(chain_id: impl Into<String>, rpc_url: impl Into<String>) -> Self {
        Self {
            base: AdapterConfig::new(chain_id),
            rpc_url: rpc_url.into(),
            ..Self::default()
        }
    }

    /// Check that the values can actually be used. The first offending field
    /// is reported as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        self.base.validate()?;
        if !has_scheme(&self.rpc_url, &["http://", "https://"]) {
            return Err(PrismError::config(
                "rpc_url",
                "must be an http:// or https:// URL",
            ));
        }
        if let Some(ws_url) = &self.ws_url
            && !has_scheme(ws_url, &["ws://", "wss://"])
        {
            return Err(PrismError::config(
                "ws_url",
                "must be a ws:// or wss:// URL",
            ));
        }
        if self.requests_per_second == 0 {
            return Err(PrismError::config(
                "requests_per_second",
                "must be greater than 0",
            ));
        }
        if !(1..=MAX_ACCOUNTS_PER_BATCH).contains(&self.max_accounts_per_batch) {
            return Err(PrismError::config(
                "max_accounts_per_batch",
                format!("must be between 1 and {MAX_ACCOUNTS_PER_BATCH}"),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for SolanaAdapterConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SolanaAdapterConfig")
            .field("base", &self.base)
            .field("rpc_url", &redact_url(&self.rpc_url))
            .field("ws_url", &self.ws_url.as_deref().map(redact_url))
            .field("commitment", &self.commitment)
            .field("requests_per_second", &self.requests_per_second)
            .field("max_accounts_per_batch", &self.max_accounts_per_batch)
            .field("skip_failed_transactions", &self.skip_failed_transactions)
            .field("encoding", &self.encoding)
            .finish()
    }
}

/// Whether `url` starts with one of `schemes` and has something after it.
fn has_scheme(url: &str, schemes: &[&str]) -> bool {
    schemes.iter().any(|scheme| {
        url.strip_prefix(scheme)
            .is_some_and(|rest| !rest.is_empty())
    })
}

/// `url` with everything that could hold a credential removed: user info,
/// path, query and fragment. Scheme and host are kept so the output still
/// says which provider is in use.
pub fn redact_url(url: &str) -> String {
    let (scheme, rest) = match url.split_once("://") {
        Some((scheme, rest)) => (Some(scheme), rest),
        None => (None, url),
    };
    let (authority, tail) = match rest.find(['/', '?', '#']) {
        Some(end) => rest.split_at(end),
        None => (rest, ""),
    };
    let host = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);

    let mut redacted = String::with_capacity(url.len());
    if let Some(scheme) = scheme {
        redacted.push_str(scheme);
        redacted.push_str("://");
    }
    redacted.push_str(host);
    if !matches!(tail, "" | "/") {
        redacted.push_str("/<redacted>");
    }
    redacted
}
