//! The gap handler: skipped slots as first-class events, not errors.
//!
//! [`GapHandler::handle`] does three things for every slot that will not be
//! written:
//!
//! 1. emits a [`ChangeEvent::Skip`] to the change log, so downstream systems
//!    see the hole explicitly instead of inferring it from a missing slot;
//! 2. appends one line to the skip log, a human-readable, append-only audit
//!    trail for operators:
//!
//!    ```text
//!    {unix_timestamp},{slot},{reason}
//!    1761040000,350123457,validator_skipped
//!    ```
//!
//! 3. counts the skip in [`GapHandlerMetrics`].
//!
//! The change log is the contract and its failure is returned. The skip log
//! is only a debugging aid: failing to write it is logged and otherwise
//! ignored, so a full disk cannot halt ingestion. The line is written after
//! the event, so the file lists exactly the skips that were published.
//!
//! Moving the ingestion cursor past the slot is the caller's job: the slot
//! processor reports the skip in [`ProcessedSlot::was_skipped`] and the
//! ingestor records it.
//!
//! [`ProcessedSlot::was_skipped`]: super::ProcessedSlot::was_skipped

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::warn;

use crate::changelog::ChangeLog;
use crate::types::error::PrismResult;
use crate::types::event::{ChangeEvent, SkipReason};

/// Counters a [`GapHandler`] keeps. Shared behind an `Arc`; every counter
/// only ever goes up.
#[derive(Debug, Default)]
pub struct GapHandlerMetrics {
    total_skips_recorded: AtomicU64,
    validator_skips: AtomicU64,
    rpc_unavailable_skips: AtomicU64,
}

/// A point-in-time copy of [`GapHandlerMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct GapHandlerMetricsSnapshot {
    /// Skips published to the change log, whatever the reason.
    pub total_skips_recorded: u64,
    /// Slots the leader did not produce a block for.
    pub validator_skips: u64,
    /// Slots the RPC could not serve even after retrying: data gaps to
    /// back-fill.
    pub rpc_unavailable_skips: u64,
}

impl GapHandlerMetrics {
    pub fn snapshot(&self) -> GapHandlerMetricsSnapshot {
        GapHandlerMetricsSnapshot {
            total_skips_recorded: self.total_skips_recorded.load(Ordering::Relaxed),
            validator_skips: self.validator_skips.load(Ordering::Relaxed),
            rpc_unavailable_skips: self.rpc_unavailable_skips.load(Ordering::Relaxed),
        }
    }

    fn record(&self, reason: SkipReason) {
        match reason {
            SkipReason::ValidatorSkipped => {
                self.validator_skips.fetch_add(1, Ordering::Relaxed);
            }
            SkipReason::RpcUnavailable => {
                self.rpc_unavailable_skips.fetch_add(1, Ordering::Relaxed);
            }
            SkipReason::BelowCursor => {}
        }
        self.total_skips_recorded.fetch_add(1, Ordering::Relaxed);
    }
}

/// Records skipped slots in the change log and the skip log.
pub struct GapHandler {
    changelog: Arc<dyn ChangeLog>,
    skip_log_path: PathBuf,
    metrics: Arc<GapHandlerMetrics>,
}

impl GapHandler {
    /// A handler that publishes to `changelog` and appends to the file at
    /// `skip_log_path`, which is created (with its parent directories) on the
    /// first skip.
    pub fn new(changelog: Arc<dyn ChangeLog>, skip_log_path: impl Into<PathBuf>) -> Self {
        Self {
            changelog,
            skip_log_path: skip_log_path.into(),
            metrics: Arc::new(GapHandlerMetrics::default()),
        }
    }

    pub fn skip_log_path(&self) -> &Path {
        &self.skip_log_path
    }

    /// The handler's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<GapHandlerMetrics> {
        &self.metrics
    }

    /// Record that `slot` was skipped for `reason`.
    ///
    /// Returns the change log's error when the event could not be published;
    /// nothing is written to the skip log or counted in that case.
    pub async fn handle(&self, slot: u64, reason: SkipReason) -> PrismResult<()> {
        self.changelog.emit(ChangeEvent::skip(slot, reason))?;
        let line = skip_log_line(chrono::Utc::now().timestamp(), slot, reason);
        if let Err(err) = append_line(&self.skip_log_path, &line) {
            warn!(
                slot,
                %reason,
                path = %self.skip_log_path.display(),
                error = %err,
                "could not append to the skip log"
            );
        }
        self.metrics.record(reason);
        Ok(())
    }
}

impl std::fmt::Debug for GapHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GapHandler")
            .field("skip_log_path", &self.skip_log_path)
            .field("metrics", &self.metrics.snapshot())
            .finish_non_exhaustive()
    }
}

/// One skip log line, newline included.
fn skip_log_line(unix_timestamp: i64, slot: u64, reason: SkipReason) -> String {
    format!("{unix_timestamp},{slot},{reason}\n")
}

/// Append `line` with a single write, so that lines from concurrent
/// handlers do not interleave.
fn append_line(path: &Path, line: &str) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?
        .write_all(line.as_bytes())
}
