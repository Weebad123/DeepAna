//! The slot processor: one slot number in, one [`ProcessedSlot`] out.
//!
//! [`SlotProcessor`] owns the pieces that turn a slot into UDM rows and runs
//! them in order:
//!
//! ```text
//!   slot ──▶ SlotFetcher ──Some(block)──▶ Normalizer ──▶ ProcessedSlot
//!                │
//!                ├──None──────▶ GapHandler(ValidatorSkipped) ──▶ ProcessedSlot (skipped)
//!                │
//!                └──Err────▶ GapHandler(RpcUnavailable), if retries ran out ──▶ Err
//! ```
//!
//! | File              | Holds                                             |
//! |-------------------|---------------------------------------------------|
//! | `mod.rs`          | [`SlotProcessor`] and [`ProcessedSlot`]           |
//! | [`fetcher`]       | [`SlotFetcher`]: one slot from the adapter, retried |
//! | [`gap_handler`]   | [`GapHandler`]: skipped slots as events           |
//!
//! A slot the RPC could not serve after every retry is recorded as an
//! `RpcUnavailable` skip, so the gap shows up in the change log and the skip
//! log, and the error is still returned: the ingestor halts on it rather
//! than move the cursor past data it never read. On restart the slot is
//! fetched again, which `RpcUnavailable` (a retryable skip reason) allows.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;
use tracing::warn;

use crate::normalizer::{NormalizedBlock, Normalizer};
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::SkipReason;
use crate::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction};

pub mod fetcher;
pub mod gap_handler;

pub use fetcher::{FetcherMetrics, FetcherMetricsSnapshot, SlotFetcher, SlotFetcherConfig};
pub use gap_handler::{GapHandler, GapHandlerMetrics, GapHandlerMetricsSnapshot};

// ---------------------------------------------------------------------------
// ProcessedSlot
// ---------------------------------------------------------------------------

/// Everything the processor produced for one slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessedSlot {
    pub slot: u64,
    /// The leader skipped the slot; every row field is empty.
    pub was_skipped: bool,
    pub udm_slot: Option<DeepAnaSlot>,
    pub udm_transactions: Vec<DeepAnaTransaction>,
    pub udm_instructions: Vec<DeepAnaInstruction>,
    /// Filled once the decoder and account resolver exist; empty until then.
    pub udm_transfers: Vec<DeepAnaTokenTransfer>,
    /// Wall time of the fetch, retries and backoff included.
    pub fetch_duration_ms: u64,
}

impl ProcessedSlot {
    fn with_data(slot: u64, block: NormalizedBlock, fetch_duration_ms: u64) -> Self {
        Self {
            slot,
            was_skipped: false,
            udm_slot: Some(block.slot),
            udm_transactions: block.transactions,
            udm_instructions: block.instructions,
            udm_transfers: Vec::new(),
            fetch_duration_ms,
        }
    }

    fn skipped(slot: u64, fetch_duration_ms: u64) -> Self {
        Self {
            slot,
            was_skipped: true,
            udm_slot: None,
            udm_transactions: Vec::new(),
            udm_instructions: Vec::new(),
            udm_transfers: Vec::new(),
            fetch_duration_ms,
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters a [`SlotProcessor`] keeps. Only slots that produced a
/// [`ProcessedSlot`] are counted; failures show up in the fetcher's and gap
/// handler's metrics.
#[derive(Debug, Default)]
pub struct ProcessorMetrics {
    total_slots_processed: AtomicU64,
    slots_with_data: AtomicU64,
    slots_skipped: AtomicU64,
    avg_processing_time_ms: AtomicU64,
    last_processed_slot: AtomicU64,
    /// Running total the average is derived from, in microseconds so that
    /// sub-millisecond slots still add up.
    total_processing_time_us: AtomicU64,
}

/// A point-in-time copy of [`ProcessorMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProcessorMetricsSnapshot {
    pub total_slots_processed: u64,
    pub slots_with_data: u64,
    pub slots_skipped: u64,
    /// Mean wall time of [`SlotProcessor::process`] over the processed slots.
    pub avg_processing_time_ms: u64,
    /// Most recently processed slot; 0 before the first.
    pub last_processed_slot: u64,
}

impl ProcessorMetrics {
    pub fn snapshot(&self) -> ProcessorMetricsSnapshot {
        ProcessorMetricsSnapshot {
            total_slots_processed: self.total_slots_processed.load(Ordering::Relaxed),
            slots_with_data: self.slots_with_data.load(Ordering::Relaxed),
            slots_skipped: self.slots_skipped.load(Ordering::Relaxed),
            avg_processing_time_ms: self.avg_processing_time_ms.load(Ordering::Relaxed),
            last_processed_slot: self.last_processed_slot.load(Ordering::Relaxed),
        }
    }

    fn record(&self, processed: &ProcessedSlot, elapsed: Duration) {
        if processed.was_skipped {
            self.slots_skipped.fetch_add(1, Ordering::Relaxed);
        } else {
            self.slots_with_data.fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_us = elapsed.as_micros() as u64;
        let total_us = self.total_processing_time_us.fetch_add(elapsed_us, Ordering::Relaxed) + elapsed_us;
        let count = self.total_slots_processed.fetch_add(1, Ordering::Relaxed) + 1;
        self.avg_processing_time_ms.store(total_us / count / 1_000, Ordering::Relaxed);
        self.last_processed_slot.store(processed.slot, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Processor
// ---------------------------------------------------------------------------

/// Turns slot numbers into [`ProcessedSlot`]s. The ingestor calls
/// [`Self::process`] once per slot, in order.
#[derive(Debug)]
pub struct SlotProcessor {
    fetcher: SlotFetcher,
    gap_handler: GapHandler,
    normalizer: Normalizer,
    metrics: Arc<ProcessorMetrics>,
}

impl SlotProcessor {
    pub fn new(fetcher: SlotFetcher, gap_handler: GapHandler, normalizer: Normalizer) -> Self {
        Self {
            fetcher,
            gap_handler,
            normalizer,
            metrics: Arc::new(ProcessorMetrics::default()),
        }
    }

    pub fn fetcher(&self) -> &SlotFetcher {
        &self.fetcher
    }

    pub fn gap_handler(&self) -> &GapHandler {
        &self.gap_handler
    }

    /// The processor's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<ProcessorMetrics> {
        &self.metrics
    }

    /// Fetch, normalize and, for a skipped slot, record the gap.
    ///
    /// Errors:
    /// * the fetcher's error, once its retries are exhausted (a retryable
    ///   error is first recorded as an `RpcUnavailable` skip) or at once
    ///   when it is not retryable;
    /// * the normalizer's error for a block it cannot read;
    /// * the change log's error when a skip could not be published.
    pub async fn process(&self, slot: u64) -> PrismResult<ProcessedSlot> {
        let started = Instant::now();
        let fetched = self.fetcher.fetch(slot).await;
        let fetch_duration_ms = started.elapsed().as_millis() as u64;
        let processed = match fetched {
            Ok(Some(block)) => {
                let normalized = self.normalizer.normalize(&block)?;
                ProcessedSlot::with_data(slot, normalized, fetch_duration_ms)
            }
            Ok(None) => {
                self.gap_handler.handle(slot, SkipReason::ValidatorSkipped).await?;
                ProcessedSlot::skipped(slot, fetch_duration_ms)
            }
            Err(err) => return Err(self.record_data_gap(slot, err).await),
        };
        self.metrics.record(&processed, started.elapsed());
        Ok(processed)
    }

    /// Record a slot the RPC could not serve, then hand back its error. A
    /// non-retryable error is not a data gap and is passed through as is.
    async fn record_data_gap(&self, slot: u64, err: PrismError) -> PrismError {
        if err.is_retryable()
            && let Err(gap_err) = self.gap_handler.handle(slot, SkipReason::RpcUnavailable).await
        {
            // The fetch error is the one the caller acts on.
            warn!(slot, error = %gap_err, "could not record the data gap");
        }
        err
    }
}
