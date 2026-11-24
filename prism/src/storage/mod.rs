//! Storage: where UDM rows go once a block is normalized, decoded and
//! resolved.
//!
//! [`Storage`] is the contract every backend implements. Rows are
//! [appended](Storage::append) to an in-memory buffer one slot at a time,
//! with no I/O, and [flushed](Storage::flush) to durable storage in batches:
//!
//! ```text
//!   append(slot, txs, ixs, transfers) ──▶ buffers ──flush──▶ one file per table
//!                                                              │
//!                                                              ▼
//!                                                         FlushResult { paths, counts }
//! ```
//!
//! | File              | Holds                                                     |
//! |-------------------|-----------------------------------------------------------|
//! | `mod.rs`          | [`Storage`], [`FlushResult`]                               |
//! | [`partition`]     | [`PartitionKey`], [`TableKind`]: the on-disk layout        |
//! | [`writer`]        | [`ParquetWriter`]: buffers → `.parquet` files              |
//!
//! A flush is all or nothing: either every table's file is written, or none
//! is and the buffers are kept as they were, so the next flush writes the
//! same rows again.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::types::error::PrismResult;
use crate::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction};

pub mod partition;
pub mod writer;

pub use partition::{ParsedFileName, PartitionKey, TableKind};
pub use writer::{ParquetWriter, WriterMetrics, WriterMetricsSnapshot};

/// What one [`Storage::flush`] wrote.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlushResult {
    pub slots_written: u64,
    pub transactions_written: u64,
    pub instructions_written: u64,
    pub transfers_written: u64,
    /// Every file written, one per non-empty table, in
    /// [`TableKind::ALL`] order. What
    /// [`ChangeEvent::New`](crate::types::event::ChangeEvent::New) carries.
    pub parquet_paths: Vec<String>,
    /// First slot of the batch. 0 when nothing was flushed.
    pub from_slot: u64,
    /// Last slot of the batch (inclusive). 0 when nothing was flushed.
    pub to_slot: u64,
    pub duration_ms: u64,
}

impl FlushResult {
    /// Whether the flush found nothing to write.
    pub fn is_empty(&self) -> bool {
        self.parquet_paths.is_empty()
    }

    /// Rows written across all tables.
    pub const fn total_rows(&self) -> u64 {
        self.slots_written
            + self.transactions_written
            + self.instructions_written
            + self.transfers_written
    }
}

/// A storage backend for UDM rows.
pub trait Storage: Send + Sync {
    /// Buffer the rows of one slot. Does no I/O. Any of `transactions`,
    /// `instructions` and `transfers` may be empty.
    fn append(
        &mut self,
        slot: &DeepAnaSlot,
        transactions: &[DeepAnaTransaction],
        instructions: &[DeepAnaInstruction],
        transfers: &[DeepAnaTokenTransfer],
    ) -> PrismResult<()>;

    /// Write everything buffered and empty the buffers. A flush with nothing
    /// buffered writes nothing and returns an [empty](FlushResult::is_empty)
    /// result.
    ///
    /// On `Err` nothing from this flush is left on disk and the buffers are
    /// untouched, so calling `flush` again retries the same rows.
    fn flush(&mut self) -> PrismResult<FlushResult>;

    /// Forget every slot at or above `from_slot`, on disk and in the
    /// buffers, e.g. after a fork.
    fn rollback(&mut self, from_slot: u64) -> PrismResult<()>;

    /// Slots currently buffered.
    fn buffered_slot_count(&self) -> u64;

    /// Last slot of the most recent successful flush, `None` before the
    /// first.
    fn last_flushed_slot(&self) -> Option<u64>;
}

/// Replace the file at `path` with `bytes` so that a crash leaves either the
/// old contents or the new ones, never a torn file: written to a sibling
/// `.tmp`, fsynced, renamed over `path`, then the directory fsynced so the
/// rename itself survives a power failure. Parent directories are created.
///
/// The small state files next to the tables (`reconcile.json`,
/// `finality.json`, `cursor.json`) are all written through this.
pub(crate) fn write_file_atomically(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let mut tmp_name = path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp = PathBuf::from(tmp_name);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    // Directories cannot be opened for syncing everywhere; where they
    // cannot, the rename is as durable as the platform makes it.
    #[cfg(unix)]
    if let Some(parent) = parent {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}
