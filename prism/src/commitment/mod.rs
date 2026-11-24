//! Commitment tracking: which written data is permanent and which could still
//! be invalidated.
//!
//! PRISM writes slots at `confirmed` commitment, which in theory can still be
//! rolled back; only `finalized` slots are permanent. This module tracks the
//! boundary between the two and broadcasts it as
//! [`ChangeEvent::Final`](crate::types::event::ChangeEvent::Final) events
//! whenever it moves past rows that were already written.
//!
//! | File                     | Holds                                                         |
//! |--------------------------|---------------------------------------------------------------|
//! | `mod.rs`                 | [`CommitmentTracker`], the module's public interface           |
//! | [`finality_watermark`]   | [`FinalityWatermark`], the atomic boundary state               |
//! | [`poller`]               | [`FinalityPoller`], the background task advancing the boundary |
//! | [`finality_store`]       | [`FinalityStore`], `finality.json`: the boundary across restarts |
//!
//! The Parquet schema has **no** `is_finalized` column, and PRISM does not
//! add one: the tables are a fixed contract, and a flag frozen into an
//! immutable file at write time would only ever say "not yet". Finality is
//! a property of the slot number instead, answered at read time:
//!
//! * a row is final when its `slot` is at or below the watermark of the
//!   latest `Final` event (or [`CommitmentTracker::is_finalized`]);
//! * [`CommitmentTracker::is_finalized_column`] and [`is_finalized_field`]
//!   build that answer as an Arrow column for a query layer that wants to
//!   expose it next to the stored ones.
//!
//! Inside PRISM the watermark decides two things: the ingestor reads slots
//! already below it at `finalized` (so a re-ingested slot can never come from
//! an abandoned fork), and the [reconciler](crate::reconcile) only examines
//! slots below it.
//!
//! A tracker built with [`CommitmentTracker::spawn_persistent`] saves the
//! watermark to `finality.json` after every poll that moves it and starts
//! from the saved value, so neither of those decisions is made blind after a
//! restart, and announcing resumes where it stopped instead of from 0. Its
//! [`CommitmentTracker::durable_finalized_slot`] is the watermark as saved:
//! what a crash cannot take back.
//!
//! The poller stops if the change log (or the finality file) fails.
//! [`CommitmentTracker::halted`] resolves when it does, and
//! [`CommitmentTracker::take_error`] hands over the error: the ingestor
//! watches the first and returns the second, so a change log failure halts
//! PRISM wherever it happens.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use arrow::array::BooleanArray;
use arrow::datatypes::{DataType, Field};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::adapter::ChainAdapter;
use crate::changelog::ChangeLog;
use crate::types::error::{PrismError, PrismResult};

pub mod finality_store;
pub mod finality_watermark;
pub mod poller;

pub use finality_store::{FINALITY_FILE_NAME, FinalityRecord, FinalityStore};
pub use finality_watermark::{Advance, FinalityWatermark, WatermarkSnapshot};
pub use poller::{DEFAULT_POLL_INTERVAL, FinalityPoller};

/// Name of the derived finality column a query layer may expose next to the
/// stored ones. Not a column of any Parquet table PRISM writes.
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
    /// Cancelled when the poller stops on an error.
    halted: CancellationToken,
    /// That error, until [`Self::take_error`] takes it.
    error: Arc<Mutex<Option<PrismError>>>,
    /// The finalized slot as last saved to `finality.json`; `None` for a
    /// tracker that does not persist.
    durable_finalized: Option<Arc<AtomicU64>>,
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
        Ok(Self::start(watermark, poller, shutdown, None))
    }

    /// Start tracking like [`Self::spawn_with_interval`], keeping the
    /// watermark in the [`FinalityStore`] file at `path` (usually
    /// `{data_dir}/finality.json`, see [`FINALITY_FILE_NAME`]).
    ///
    /// A saved record for the adapter's chain seeds the watermark before the
    /// poller starts, and announcing resumes after its `announced_slot`. A
    /// record for another chain is ignored; a corrupt one is quarantined.
    ///
    /// Fails with [`PrismError::ConfigError`] when `poll_interval` is zero,
    /// and with [`PrismError::CursorError`] when the file exists but cannot
    /// be read. Must be called from within a Tokio runtime.
    pub fn spawn_persistent(
        adapter: Arc<dyn ChainAdapter>,
        changelog: Arc<dyn ChangeLog>,
        poll_interval: Duration,
        path: impl Into<PathBuf>,
    ) -> PrismResult<Self> {
        let store = FinalityStore::new(path);
        let record = store.load(adapter.chain_id())?;
        let watermark = Arc::new(FinalityWatermark::new());
        let durable = Arc::new(AtomicU64::new(0));
        let mut announced = 0;
        if let Some(record) = record.filter(|r| r.finalized_slot > 0) {
            let _ = watermark.advance_finalized(record.finalized_slot, &record.finalized_blockhash);
            durable.store(record.finalized_slot, Ordering::Release);
            announced = record.announced_slot;
        }
        let shutdown = CancellationToken::new();
        let poller = FinalityPoller::new(
            adapter,
            Arc::clone(&watermark),
            changelog,
            poll_interval,
            shutdown.clone(),
        )?
        .announcing_from(announced)
        .persist_to(store, Arc::clone(&durable));
        Ok(Self::start(watermark, poller, shutdown, Some(durable)))
    }

    fn start(
        watermark: Arc<FinalityWatermark>,
        poller: FinalityPoller,
        shutdown: CancellationToken,
        durable_finalized: Option<Arc<AtomicU64>>,
    ) -> Self {
        let halted = CancellationToken::new();
        let error = Arc::new(Mutex::new(None));
        let poller_handle = tokio::spawn({
            let halted = halted.clone();
            let error = Arc::clone(&error);
            async move {
                if let Err(err) = poller.run().await {
                    *error.lock().unwrap_or_else(PoisonError::into_inner) = Some(err);
                    halted.cancel();
                }
            }
        });
        Self {
            watermark,
            poller_handle: Some(poller_handle),
            shutdown,
            halted,
            error,
            durable_finalized,
        }
    }

    /// A tracker around an existing watermark, with no poller: whoever owns
    /// the watermark advances it.
    pub fn from_watermark(watermark: Arc<FinalityWatermark>) -> Self {
        Self {
            watermark,
            poller_handle: None,
            shutdown: CancellationToken::new(),
            halted: CancellationToken::new(),
            error: Arc::default(),
            durable_finalized: None,
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

    /// The finalized slot a crash cannot take back: as last saved to
    /// `finality.json` for a [persistent](Self::spawn_persistent) tracker,
    /// [`Self::finalized_slot`] for any other (it has nothing to lose).
    /// Never above [`Self::finalized_slot`].
    pub fn durable_finalized_slot(&self) -> u64 {
        self.durable_finalized
            .as_ref()
            .map_or_else(|| self.finalized_slot(), |durable| durable.load(Ordering::Acquire))
    }

    /// Whether this tracker saves its watermark.
    pub fn is_persistent(&self) -> bool {
        self.durable_finalized.is_some()
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

    /// Resolves once the poller has stopped on a change log error. Never
    /// resolves for a poller that is running, was shut down, or for a
    /// tracker without one.
    pub async fn halted(&self) {
        self.halted.cancelled().await;
    }

    /// The error the poller stopped on, the first time it is asked for.
    pub fn take_error(&self) -> Option<PrismError> {
        self.error.lock().unwrap_or_else(PoisonError::into_inner).take()
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
