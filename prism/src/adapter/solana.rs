//! The Solana adapter: [`ChainAdapter`] fulfilled over Solana JSON-RPC.
//!
//! This is the only file in the adapter module that knows the Solana SDK: its
//! types, its RPC methods and its quirks. Every request goes through one
//! path: wait for the rate limiter, send, classify the outcome, and retry
//! with exponential backoff when the failure is worth retrying.
//!
//! Quirks handled here so that nothing downstream has to know them:
//!
//! * **The tip is read with `getLatestBlockhash`, not `getSlot`.** Its
//!   response carries the slot and that slot's blockhash from one bank, so
//!   [`ChainTip`] is consistent without a second, racy call.
//! * **`getBlock` rejects blocks containing versioned transactions** unless
//!   the request states the highest version it understands. It always does.
//! * **`getBlock` does not accept `processed`.** With that commitment
//!   configured, blocks are fetched at `confirmed`, the lowest it supports.
//! * **A skipped slot is an RPC error**, not an empty result. The skip codes
//!   are mapped to [`PrismError::SlotSkipped`] and never retried. Above the
//!   finalized root a node cannot yet tell "skipped" from "not here yet" and
//!   answers `BLOCK_NOT_AVAILABLE` for both; that is retried, and if the
//!   retries run out the slot surfaces as a retryable
//!   [`PrismError::RpcError`] for the gap handler to revisit.
//! * **Not every RPC error is worth retrying**, even though
//!   [`PrismError::is_retryable`] is true for all of them. Only failures that
//!   another attempt can plausibly fix are retried here (transport errors,
//!   5xx, 429, a node that is behind); a malformed request or a pruned block
//!   fails on the first attempt.
//! * **The SDK's HTTP transport retries `429` on its own**: up to five more
//!   sends, 500 ms apart (or the server's `Retry-After`), before the adapter
//!   sees the error. Those sends bypass this adapter's rate limiter and
//!   counters, and the `Retry-After` value is consumed there, so
//!   [`PrismError::RateLimitError`] is always produced without a hint.
//! * **Transport errors embed the request URL**, which for most providers
//!   contains the API key. The URL is stripped before the error is wrapped.

use std::fmt;
use std::num::NonZeroU32;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use backoff::backoff::Backoff;
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use governor::Quota;
use solana_client::client_error::{ClientError, ClientErrorKind, Result as ClientResult};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{
    CommitmentConfig, RpcAccountInfoConfig, RpcBlockConfig, UiAccountEncoding,
};
use solana_client::rpc_custom_error::{
    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
    JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE,
    JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED, JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
    JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
};
use solana_client::rpc_request::RpcError;
use solana_client::rpc_response::UiAccount;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};
use tracing::warn;

use super::config::{SolanaAdapterConfig, TransactionEncoding};
use super::{
    AccountData, AdapterMetrics, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip,
    CommitmentLevel,
};
use crate::types::error::{PrismError, PrismResult};

/// The token-bucket limiter every request waits on.
pub type RateLimiter = governor::DefaultDirectRateLimiter;

/// Highest transaction version requested from `getBlock`: legacy, v0 and v1
/// are what the pinned SDK can decode. A block holding anything newer fails
/// with `UNSUPPORTED_TRANSACTION_VERSION`, which is the cue to upgrade the
/// SDK and raise this.
const MAX_SUPPORTED_TRANSACTION_VERSION: u8 = 1;

/// JSON-RPC 2.0 "Internal error": the node failed while serving the request.
const JSON_RPC_INTERNAL_ERROR: i64 = -32603;

const HTTP_REQUEST_TIMEOUT: u16 = 408;
const HTTP_TOO_MANY_REQUESTS: u16 = 429;

/// RPC error codes that describe the node's state at the moment of the call
/// rather than a problem with the request.
const TRANSIENT_RPC_CODES: [i64; 6] = [
    JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
    JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY,
    JSON_RPC_SERVER_ERROR_BLOCK_STATUS_NOT_AVAILABLE_YET,
    JSON_RPC_SERVER_ERROR_MIN_CONTEXT_SLOT_NOT_REACHED,
    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_UNREACHABLE,
    JSON_RPC_INTERNAL_ERROR,
];

/// RPC error codes meaning "no block was produced for this slot".
const SLOT_SKIPPED_RPC_CODES: [i64; 2] = [
    JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
    JSON_RPC_SERVER_ERROR_LONG_TERM_STORAGE_SLOT_SKIPPED,
];

/// [`ChainAdapter`] for Solana clusters.
///
/// Cheap to share: wrap it in an `Arc` and call it from as many tasks as
/// needed. All of them draw from the same rate limit.
pub struct SolanaAdapter {
    config: SolanaAdapterConfig,
    client: Arc<RpcClient>,
    rate_limiter: Arc<RateLimiter>,
    metrics: Arc<AdapterMetrics>,
}

/// One failed attempt: the error to report and whether trying again could
/// change the outcome.
struct Failure {
    error: PrismError,
    transient: bool,
}

impl SolanaAdapter {
    /// Build an adapter that talks to `config.rpc_url` over HTTP.
    ///
    /// Fails with [`PrismError::ConfigError`] when the config is unusable. No
    /// request is made; use [`ChainAdapter::health_check`] to find out
    /// whether the endpoint answers.
    pub fn new(config: SolanaAdapterConfig) -> PrismResult<Self> {
        let client = RpcClient::new_with_timeout_and_commitment(
            config.rpc_url.clone(),
            config.base.request_timeout(),
            commitment_config(config.commitment),
        );
        Self::with_client(config, client)
    }

    /// Build an adapter around an existing client, for callers that need a
    /// custom transport. `config.rpc_url` and `request_timeout_ms` are then
    /// the client's business and are not applied.
    pub fn with_client(config: SolanaAdapterConfig, client: RpcClient) -> PrismResult<Self> {
        Self::with_shared_client(config, Arc::new(client))
    }

    /// Like [`Self::with_client`], for a client that is already shared, such
    /// as the one an SDK hands out (the Helius SDK keeps its async client
    /// behind an `Arc`).
    pub fn with_shared_client(
        config: SolanaAdapterConfig,
        client: Arc<RpcClient>,
    ) -> PrismResult<Self> {
        config.validate()?;
        let requests_per_second = NonZeroU32::new(config.requests_per_second)
            .ok_or_else(|| PrismError::config("requests_per_second", "must be greater than 0"))?;
        // A burst of one spaces requests evenly. The default burst of a whole
        // second's quota would let twice the limit through in the first
        // second after an idle period.
        let quota = Quota::per_second(requests_per_second).allow_burst(NonZeroU32::MIN);
        Ok(Self {
            config,
            client,
            rate_limiter: Arc::new(RateLimiter::direct(quota)),
            metrics: Arc::new(AdapterMetrics::default()),
        })
    }

    pub fn config(&self) -> &SolanaAdapterConfig {
        &self.config
    }

    /// The adapter's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<AdapterMetrics> {
        &self.metrics
    }

    /// Run `call` until it succeeds, fails in a way that retrying cannot fix,
    /// or has been retried `max_retries` times. `call` is invoked once per
    /// attempt. The error of the last attempt is the one returned.
    ///
    /// Delays start at `retry_base_delay_ms`, double each time and are capped
    /// at `retry_max_delay_ms`; each is jittered by ±50 % so that tasks which
    /// failed together do not retry together.
    async fn with_retry<T, F, Fut>(
        &self,
        method: &'static str,
        slot: Option<u64>,
        mut call: F,
    ) -> PrismResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = ClientResult<T>>,
    {
        let max_retries = self.config.base.max_retries;
        let mut backoff = self.backoff();
        let mut retries = 0;
        loop {
            let failure = match self.attempt(method, slot, call()).await {
                Ok(value) => return Ok(value),
                Err(failure) => failure,
            };
            if !failure.transient || retries == max_retries {
                return Err(failure.error);
            }
            retries += 1;
            let delay = self.next_delay(&mut backoff);
            warn!(
                chain_id = %self.config.base.chain_id,
                method,
                slot,
                retry = retries,
                max_retries,
                delay_ms = delay.as_millis() as u64,
                error = %failure.error,
                "RPC call failed, retrying"
            );
            tokio::time::sleep(delay).await;
            self.metrics.record_retry();
        }
    }

    /// One rate-limited, counted attempt.
    async fn attempt<T>(
        &self,
        method: &'static str,
        slot: Option<u64>,
        call: impl Future<Output = ClientResult<T>>,
    ) -> Result<T, Failure> {
        self.rate_limiter.until_ready().await;
        self.metrics.record_request();
        let failure = match call.await {
            Ok(value) => return Ok(value),
            Err(err) => classify(method, slot, err),
        };
        match &failure.error {
            PrismError::SlotSkipped { .. } => self.metrics.record_slot_skipped(),
            PrismError::RateLimitError { .. } => {
                self.metrics.record_error();
                self.metrics.record_rate_limited();
            }
            _ => self.metrics.record_error(),
        }
        Err(failure)
    }

    fn backoff(&self) -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(self.config.base.retry_base_delay())
            .with_max_interval(self.config.base.retry_max_delay())
            .with_multiplier(2.0)
            .with_randomization_factor(0.5)
            .with_max_elapsed_time(None)
            .build()
    }

    fn next_delay(&self, backoff: &mut ExponentialBackoff) -> Duration {
        let max = self.config.base.retry_max_delay();
        // The jitter is applied after the interval is capped, so cap again.
        backoff.next_backoff().map_or(max, |delay| delay.min(max))
    }
}

#[async_trait]
impl ChainAdapter for SolanaAdapter {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        &self.config.base.chain_id
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        let commitment_config = commitment_config(commitment);
        let response = self
            .with_retry("getLatestBlockhash", None, || {
                self.client
                    .get_latest_blockhash_with_commitment_and_context(commitment_config)
            })
            .await?;
        let (blockhash, _last_valid_block_height) = response.value;
        Ok(ChainTip {
            slot_or_height: response.context.slot,
            block_id: blockhash.to_string(),
            commitment,
        })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        let config = RpcBlockConfig {
            encoding: Some(self.config.encoding.into()),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: Some(block_commitment_config(self.config.commitment)),
            max_supported_transaction_version: Some(MAX_SUPPORTED_TRANSACTION_VERSION),
        };
        let block = self
            .with_retry("getBlock", Some(slot), || {
                self.client.get_block_with_config(slot, config)
            })
            .await?;
        Ok(ChainBlock {
            slot_or_height: slot,
            block_id: block.blockhash.clone(),
            parent_id: block.previous_blockhash.clone(),
            timestamp: block.block_time,
            raw: ChainBlockData::Solana(Box::new(block.into())),
        })
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        // Reject a bad address before spending requests on the good ones.
        let pubkeys = addresses
            .iter()
            .map(|address| {
                Pubkey::from_str(address).map_err(|err| PrismError::account_resolve(address, err))
            })
            .collect::<PrismResult<Vec<_>>>()?;
        let config = RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64Zstd),
            data_slice: None,
            commitment: Some(commitment_config(self.config.commitment)),
            min_context_slot: None,
        };

        let batch_size = self.config.max_accounts_per_batch;
        let mut accounts = Vec::with_capacity(addresses.len());
        for (batch_addresses, batch_pubkeys) in
            addresses.chunks(batch_size).zip(pubkeys.chunks(batch_size))
        {
            let response = self
                .with_retry("getMultipleAccounts", None, || {
                    self.client
                        .get_multiple_ui_accounts_with_config(batch_pubkeys, config.clone())
                })
                .await?;
            // Results are matched to addresses by position, so a short or
            // long answer would silently mis-attribute accounts.
            if response.value.len() != batch_addresses.len() {
                return Err(PrismError::deserialize(
                    None,
                    "getMultipleAccounts response",
                    format!(
                        "asked for {} accounts, got {}",
                        batch_addresses.len(),
                        response.value.len()
                    ),
                ));
            }
            for (address, account) in batch_addresses.iter().zip(response.value) {
                accounts.push(account.map(|a| account_data(address, a)).transpose()?);
            }
        }
        Ok(accounts)
    }

    async fn health_check(&self) -> PrismResult<()> {
        self.attempt("getHealth", None, self.client.get_health())
            .await
            .map_err(|failure| failure.error)
    }
}

impl fmt::Debug for SolanaAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SolanaAdapter")
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .finish_non_exhaustive()
    }
}

impl From<TransactionEncoding> for UiTransactionEncoding {
    fn from(encoding: TransactionEncoding) -> Self {
        match encoding {
            TransactionEncoding::Base58 => Self::Base58,
            TransactionEncoding::Base64 => Self::Base64,
            TransactionEncoding::Json => Self::Json,
            TransactionEncoding::JsonParsed => Self::JsonParsed,
        }
    }
}

fn commitment_config(level: CommitmentLevel) -> CommitmentConfig {
    match level {
        CommitmentLevel::Confirmed => CommitmentConfig::confirmed(),
        CommitmentLevel::Finalized => CommitmentConfig::finalized(),
        CommitmentLevel::Processed => CommitmentConfig::processed(),
    }
}

/// Commitment for `getBlock`, which does not serve `processed`.
fn block_commitment_config(level: CommitmentLevel) -> CommitmentConfig {
    match level {
        CommitmentLevel::Processed => CommitmentConfig::confirmed(),
        level => commitment_config(level),
    }
}

fn account_data(address: &str, account: UiAccount) -> PrismResult<AccountData> {
    let data = account.data.decode().ok_or_else(|| {
        PrismError::deserialize(
            None,
            format!("data of account {address}"),
            "the RPC returned the data in an encoding that is not binary",
        )
    })?;
    Ok(AccountData {
        owner_program: account.owner,
        lamports: account.lamports,
        data,
        executable: account.executable,
        rent_epoch: account.rent_epoch,
    })
}

/// Map a failed RPC call onto PRISM's error model and decide whether another
/// attempt could succeed.
fn classify(method: &'static str, slot: Option<u64>, err: ClientError) -> Failure {
    if let ClientErrorKind::Reqwest(http) = err.kind()
        && http.status().map(|status| status.as_u16()) == Some(HTTP_TOO_MANY_REQUESTS)
    {
        return Failure {
            error: PrismError::rate_limited(None),
            transient: true,
        };
    }
    if let Some(slot) = slot
        && let ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) = err.kind()
        && SLOT_SKIPPED_RPC_CODES.contains(code)
    {
        return Failure {
            error: PrismError::slot_skipped(slot),
            transient: false,
        };
    }
    Failure {
        transient: is_transient(err.kind()),
        error: PrismError::rpc(slot, method, without_url(err)),
    }
}

fn is_transient(kind: &ClientErrorKind) -> bool {
    match kind {
        ClientErrorKind::Io(_) => true,
        ClientErrorKind::Reqwest(http) => match http.status() {
            Some(status) => status.is_server_error() || status.as_u16() == HTTP_REQUEST_TIMEOUT,
            // No status: the request never got an answer (connect, timeout,
            // truncated body). Only a request that could not even be built
            // is hopeless.
            None => !http.is_builder(),
        },
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, .. }) => {
            TRANSIENT_RPC_CODES.contains(code)
        }
        // The answer was not a JSON-RPC response at all, typically an error
        // page from a proxy in front of the node.
        ClientErrorKind::RpcError(RpcError::RpcRequestError(_)) => true,
        _ => false,
    }
}

/// Drop the request URL from a transport error so that the API key in it
/// cannot reach a log line.
fn without_url(err: ClientError) -> ClientError {
    let ClientError { request, kind } = err;
    let kind = match *kind {
        ClientErrorKind::Reqwest(http) => ClientErrorKind::Reqwest(http.without_url()),
        other => other,
    };
    ClientError {
        request,
        kind: Box::new(kind),
    }
}
