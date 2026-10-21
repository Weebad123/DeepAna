//! The slot fetcher: one slot number in, one block (or "skipped") out.
//!
//! [`SlotFetcher::fetch`] folds every way a `getBlock` can end into three
//! outcomes the caller handles differently:
//!
//! | Outcome           | Meaning                                                  |
//! |-------------------|----------------------------------------------------------|
//! | `Ok(Some(block))` | the slot has a block                                     |
//! | `Ok(None)`        | the leader skipped the slot: normal on Solana, not a fault |
//! | `Err(e)`          | a genuine failure, already retried as far as the config allows |
//!
//! Retries are decided on the [`PrismError`] variant alone, through
//! [`PrismError::is_retryable`]: RPC and rate-limit errors are retried with
//! jittered exponential backoff, anything else is returned on the spot. Each
//! attempt is bounded by [`SlotFetcherConfig::request_timeout_ms`]; an
//! attempt that runs over counts as a retryable RPC error.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use backoff::backoff::Backoff;
use backoff::{ExponentialBackoff, ExponentialBackoffBuilder};
use solana_client::client_error::ClientError;
use tokio::time::Instant;
use tracing::warn;

use crate::adapter::{ChainAdapter, ChainBlock};
use crate::types::error::{PrismError, PrismResult};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Retry and timeout policy of a [`SlotFetcher`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotFetcherConfig {
    /// How many times a retryable failure is retried before giving up. `0`
    /// disables retries.
    pub max_retries: u32,
    /// Delay before the first retry, in milliseconds. Doubles on each further
    /// retry and is jittered by ±50 %.
    pub base_delay_ms: u64,
    /// Upper bound on the delay between two attempts, in milliseconds. Also
    /// caps a server's `Retry-After` hint.
    pub max_delay_ms: u64,
    /// How long one attempt may take, in milliseconds.
    pub request_timeout_ms: u64,
}

impl Default for SlotFetcherConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay_ms: 500,
            max_delay_ms: 15_000,
            request_timeout_ms: 10_000,
        }
    }
}

impl SlotFetcherConfig {
    pub const fn base_delay(&self) -> Duration {
        Duration::from_millis(self.base_delay_ms)
    }

    pub const fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }

    pub const fn request_timeout(&self) -> Duration {
        Duration::from_millis(self.request_timeout_ms)
    }

    /// Check that the values can actually be used. The first offending field
    /// is reported as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        if self.base_delay_ms == 0 {
            return Err(PrismError::config("base_delay_ms", "must be greater than 0"));
        }
        if self.max_delay_ms < self.base_delay_ms {
            return Err(PrismError::config(
                "max_delay_ms",
                format!("must be at least base_delay_ms ({})", self.base_delay_ms),
            ));
        }
        if self.request_timeout_ms == 0 {
            return Err(PrismError::config("request_timeout_ms", "must be greater than 0"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters a [`SlotFetcher`] keeps. Shared behind an `Arc`; every counter
/// except `last_fetch_latency_ms` only ever goes up.
#[derive(Debug, Default)]
pub struct FetcherMetrics {
    total_fetches: AtomicU64,
    successful_fetches: AtomicU64,
    skipped_slots: AtomicU64,
    failed_fetches: AtomicU64,
    total_retries: AtomicU64,
    last_fetch_latency_ms: AtomicU64,
}

/// A point-in-time copy of [`FetcherMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FetcherMetricsSnapshot {
    /// Calls to [`SlotFetcher::fetch`] that have finished.
    pub total_fetches: u64,
    /// Fetches that returned a block.
    pub successful_fetches: u64,
    /// Fetches answered with "this slot was skipped". An answer, not a
    /// failure.
    pub skipped_slots: u64,
    /// Fetches that returned an error.
    pub failed_fetches: u64,
    /// Attempts that were a retry of an earlier failed attempt.
    pub total_retries: u64,
    /// Wall time of the most recent fetch, retries and backoff included.
    pub last_fetch_latency_ms: u64,
}

impl FetcherMetricsSnapshot {
    /// Share of fetches that got an answer from the chain (a block or a
    /// skip). `1.0` before the first fetch.
    pub fn success_rate(&self) -> f64 {
        if self.total_fetches == 0 {
            return 1.0;
        }
        (self.successful_fetches + self.skipped_slots) as f64 / self.total_fetches as f64
    }
}

impl FetcherMetrics {
    pub fn snapshot(&self) -> FetcherMetricsSnapshot {
        FetcherMetricsSnapshot {
            total_fetches: self.total_fetches.load(Ordering::Relaxed),
            successful_fetches: self.successful_fetches.load(Ordering::Relaxed),
            skipped_slots: self.skipped_slots.load(Ordering::Relaxed),
            failed_fetches: self.failed_fetches.load(Ordering::Relaxed),
            total_retries: self.total_retries.load(Ordering::Relaxed),
            last_fetch_latency_ms: self.last_fetch_latency_ms.load(Ordering::Relaxed),
        }
    }

    fn record_outcome(&self, outcome: &AtomicU64, latency: Duration) {
        self.last_fetch_latency_ms
            .store(latency.as_millis() as u64, Ordering::Relaxed);
        outcome.fetch_add(1, Ordering::Relaxed);
        self.total_fetches.fetch_add(1, Ordering::Relaxed);
    }

    fn record_retry(&self) {
        self.total_retries.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Fetcher
// ---------------------------------------------------------------------------

/// Fetches single slots from a [`ChainAdapter`], with retries.
pub struct SlotFetcher {
    adapter: Arc<dyn ChainAdapter>,
    config: SlotFetcherConfig,
    metrics: Arc<FetcherMetrics>,
}

impl SlotFetcher {
    /// Fails with [`PrismError::ConfigError`] when `config` is unusable.
    pub fn new(adapter: Arc<dyn ChainAdapter>, config: SlotFetcherConfig) -> PrismResult<Self> {
        config.validate()?;
        Ok(Self {
            adapter,
            config,
            metrics: Arc::new(FetcherMetrics::default()),
        })
    }

    pub fn config(&self) -> &SlotFetcherConfig {
        &self.config
    }

    /// The fetcher's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<FetcherMetrics> {
        &self.metrics
    }

    /// The block at `slot`, `None` when the slot was skipped.
    ///
    /// An `Err` is the last attempt's error: either one that is not
    /// retryable, or a retryable one after `max_retries` retries.
    pub async fn fetch(&self, slot: u64) -> PrismResult<Option<ChainBlock>> {
        let started = Instant::now();
        let result = self.fetch_with_retry(slot).await;
        let latency = started.elapsed();
        match result {
            Ok(block) => {
                self.metrics.record_outcome(&self.metrics.successful_fetches, latency);
                Ok(Some(block))
            }
            Err(PrismError::SlotSkipped { .. }) => {
                self.metrics.record_outcome(&self.metrics.skipped_slots, latency);
                Ok(None)
            }
            Err(err) => {
                self.metrics.record_outcome(&self.metrics.failed_fetches, latency);
                Err(err)
            }
        }
    }

    async fn fetch_with_retry(&self, slot: u64) -> PrismResult<ChainBlock> {
        let max_retries = self.config.max_retries;
        let mut backoff = self.backoff();
        let mut retries = 0;
        loop {
            let error = match self.attempt(slot).await {
                Ok(block) => return Ok(block),
                Err(err) => err,
            };
            if !error.is_retryable() || retries == max_retries {
                return Err(error);
            }
            retries += 1;
            let delay = self.next_delay(&mut backoff, &error);
            warn!(
                slot,
                retry = retries,
                max_retries,
                delay_ms = delay.as_millis() as u64,
                error = %error,
                "slot fetch failed, retrying"
            );
            tokio::time::sleep(delay).await;
            self.metrics.record_retry();
        }
    }

    /// One `getBlock`, bounded by the request timeout.
    async fn attempt(&self, slot: u64) -> PrismResult<ChainBlock> {
        let timeout = self.config.request_timeout();
        tokio::time::timeout(timeout, self.adapter.get_block(slot))
            .await
            .unwrap_or_else(|_elapsed| Err(timed_out(slot, timeout)))
    }

    fn backoff(&self) -> ExponentialBackoff {
        ExponentialBackoffBuilder::new()
            .with_initial_interval(self.config.base_delay())
            .with_max_interval(self.config.max_delay())
            .with_multiplier(2.0)
            .with_randomization_factor(0.5)
            .with_max_elapsed_time(None)
            .build()
    }

    /// The backoff's next delay, stretched to the server's `Retry-After` when
    /// that is longer. Capped at `max_delay_ms` either way (the jitter is
    /// applied after the backoff's own cap).
    fn next_delay(&self, backoff: &mut ExponentialBackoff, error: &PrismError) -> Duration {
        let max = self.config.max_delay();
        let delay = backoff.next_backoff().unwrap_or(max);
        delay.max(error.retry_after().unwrap_or_default()).min(max)
    }
}

impl fmt::Debug for SlotFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotFetcher")
            .field("chain_id", &self.adapter.chain_id())
            .field("config", &self.config)
            .field("metrics", &self.metrics.snapshot())
            .finish()
    }
}

/// The error for an attempt that ran past the request timeout.
fn timed_out(slot: u64, timeout: Duration) -> PrismError {
    let io = std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        format!("no answer within {} ms", timeout.as_millis()),
    );
    PrismError::rpc(Some(slot), "getBlock", ClientError::from(io))
}
