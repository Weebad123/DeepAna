//! Commitment tracking: which written data is permanent and which could still
//! be invalidated.
//!
//! PRISM writes slots at `confirmed` commitment, which in theory can still be
//! rolled back; only `finalized` slots are permanent. This module tracks the
//! boundary between the two and broadcasts it: as the `is_finalized` column
//! stamped onto every Parquet row at write time, and as
//! [`ChangeEvent::Final`](crate::types::event::ChangeEvent::Final) events
//! when the boundary moves past rows that were written before it.
//!
//! | File                     | Holds                                                         |
//! |--------------------------|---------------------------------------------------------------|
//! | `mod.rs`                 | [`CommitmentTracker`], the module's public interface           |
//! | [`finality_watermark`]   | [`FinalityWatermark`], the atomic boundary state               |
//! | [`poller`]               | [`FinalityPoller`], the background task advancing the boundary |
//!
//! The `is_finalized` value in a Parquet file is a snapshot taken when the
//! file was written. Files are immutable, so a row written as `false` stays
//! `false` on disk; the `Final` event is how consumers learn that it has
//! since become permanent.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::adapter::ChainAdapter;
use crate::changelog::ChangeLog;
use crate::types::error::PrismResult;

pub mod finality_watermark;
pub mod poller;

pub use finality_watermark::{Advance, FinalityWatermark, WatermarkSnapshot};
pub use poller::{DEFAULT_POLL_INTERVAL, FinalityPoller};

/// Name of the column every UDM table carries for finality.
pub const IS_FINALIZED_COLUMN: &str = "is_finalized";

/// The Arrow field for [`IS_FINALIZED_COLUMN`]: a non-nullable boolean.
pub fn is_finalized_field() -> Field {
    Field::new(IS_FINALIZED_COLUMN, DataType::Boolean, false)
}

/// Owns the finality watermark and the task that keeps it current.
///
/// Dropping the tracker stops the poller; call [`Self::shutdown`] to also
/// wait for it to finish.
pub struct CommitmentTracker {
    watermark: Arc<FinalityWatermark>,
    poller_handle: Option<JoinHandle<()>>,
    shutdown: CancellationToken,
}

impl CommitmentTracker {
    /// Start tracking, polling every [`DEFAULT_POLL_INTERVAL`].
    ///
    /// Must be called from within a Tokio runtime.
    pub fn spawn(adapter: Arc<dyn ChainAdapter>, changelog: Arc<dyn ChangeLog>) -> Self {
        Self::spawn_with_interval(adapter, changelog, DEFAULT_POLL_INTERVAL)
            .expect("DEFAULT_POLL_INTERVAL is non-zero")
    }

    /// Start tracking, polling every `poll_interval`.
    ///
    /// Fails with [`PrismError::ConfigError`] when `poll_interval` is zero.
    /// Must be called from within a Tokio runtime.
    ///
    /// [`PrismError::ConfigError`]: crate::types::error::PrismError::ConfigError
    pub fn spawn_with_interval(
        adapter: Arc<dyn ChainAdapter>,
        changelog: Arc<dyn ChangeLog>,
        poll_interval: Duration,
    ) -> PrismResult<Self> {
        let watermark = Arc::new(FinalityWatermark::new());
        let shutdown = CancellationToken::new();
        let poller = FinalityPoller::new(
            adapter,
            Arc::clone(&watermark),
            changelog,
            poll_interval,
            shutdown.clone(),
        )?;
        Ok(Self {
            watermark,
            poller_handle: Some(tokio::spawn(poller.run())),
            shutdown,
        })
    }

    /// A tracker around an existing watermark, with no poller: whoever owns
    /// the watermark advances it.
    pub fn from_watermark(watermark: Arc<FinalityWatermark>) -> Self {
        Self {
            watermark,
            poller_handle: None,
            shutdown: CancellationToken::new(),
        }
    }

    /// The shared watermark, for components that need to hold on to it.
    pub fn watermark(&self) -> &Arc<FinalityWatermark> {
        &self.watermark
    }

    pub fn confirmed_slot(&self) -> u64 {
        self.watermark.confirmed_slot()
    }

    pub fn finalized_slot(&self) -> u64 {
        self.watermark.finalized_slot()
    }

    /// See [`FinalityWatermark::is_finalized`].
    pub fn is_finalized(&self, slot: u64) -> bool {
        self.watermark.is_finalized(slot)
    }

    /// See [`FinalityWatermark::is_finalized_column`].
    pub fn is_finalized_column(&self, slots: impl IntoIterator<Item = u64>) -> BooleanArray {
        self.watermark.is_finalized_column(slots)
    }

    pub fn lag(&self) -> u64 {
        self.watermark.lag()
    }

    pub fn snapshot(&self) -> WatermarkSnapshot {
        self.watermark.snapshot()
    }

    /// Whether the poller is still running. `false` after shutdown, for a
    /// tracker built without one, and if the poller panicked.
    pub fn is_running(&self) -> bool {
        self.poller_handle
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Stop the poller and wait for it to finish. The watermark keeps its
    /// last values and stays readable. Calling this again is a no-op.
    pub async fn shutdown(&mut self) {
        self.shutdown.cancel();
        if let Some(handle) = self.poller_handle.take()
            && let Err(err) = handle.await
            && err.is_panic()
        {
            error!(error = %err, "finality poller panicked");
        }
    }
}

impl Drop for CommitmentTracker {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}
