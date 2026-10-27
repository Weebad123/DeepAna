//! [`ParquetWriter`]: the [`Storage`] backend that writes Parquet files.
//!
//! The writer keeps one in-memory row buffer per table. [`append`] only
//! pushes rows; [`flush`] turns each non-empty buffer into one Arrow
//! [`RecordBatch`] and writes it to the file its [`PartitionKey`] names:
//!
//! ```text
//!   append ──▶ slot / transaction / instruction / transfer buffers
//!                │ flush
//!                ▼
//!   UdmRow::to_record_batch ──▶ ArrowWriter ──▶ {key}.parquet.tmp ──rename──▶ {key}.parquet
//! ```
//!
//! The Arrow schemas are the UDM's own ([`UdmRow::schema`]), re-exported
//! here as [`SLOT_SCHEMA`], [`TRANSACTION_SCHEMA`], [`INSTRUCTION_SCHEMA`]
//! and [`TOKEN_TRANSFER_SCHEMA`], so what lands on disk is exactly what the
//! UDM describes.
//!
//! Each file is written under a temporary name and renamed into place once
//! its footer is on disk, so a reader never opens a half-written file. A
//! flush that fails on any table removes the files it already wrote and
//! keeps the buffers, so the next flush writes the same rows again rather
//! than leaving the tables out of step with each other.
//!
//! [`append`]: Storage::append
//! [`flush`]: Storage::flush

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use arrow::array::{RecordBatch, Scalar, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cmp::lt;
use arrow::datatypes::SchemaRef;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use tracing::{debug, info, warn};

use super::partition::{PARQUET_EXTENSION, PartitionKey, TableKind};
use super::{FlushResult, Storage};
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, UdmRow,
};

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

/// Schema of the `slots` table: [`DeepAnaSlot::schema`].
pub static SLOT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(DeepAnaSlot::schema);

/// Schema of the `transactions` table: [`DeepAnaTransaction::schema`].
pub static TRANSACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(DeepAnaTransaction::schema);

/// Schema of the `instructions` table: [`DeepAnaInstruction::schema`].
pub static INSTRUCTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(DeepAnaInstruction::schema);

/// Schema of the `token_transfers` table: [`DeepAnaTokenTransfer::schema`].
pub static TOKEN_TRANSFER_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(DeepAnaTokenTransfer::schema);

/// The Arrow schema `table`'s files are written with.
pub fn schema_for(table: TableKind) -> SchemaRef {
    match table {
        TableKind::Slots => SLOT_SCHEMA.clone(),
        TableKind::Transactions => TRANSACTION_SCHEMA.clone(),
        TableKind::Instructions => INSTRUCTION_SCHEMA.clone(),
        TableKind::TokenTransfers => TOKEN_TRANSFER_SCHEMA.clone(),
    }
}

/// Name of the column every table is partitioned and rolled back by.
const SLOT_COLUMN: &str = "slot";

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters a [`ParquetWriter`] keeps. Every counter except
/// `last_flush_duration_ms` only ever goes up.
#[derive(Debug, Default)]
pub struct WriterMetrics {
    total_flushes: AtomicU64,
    total_rows_written: AtomicU64,
    total_bytes_written: AtomicU64,
    last_flush_duration_ms: AtomicU64,
    flush_errors: AtomicU64,
}

/// A point-in-time copy of [`WriterMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WriterMetricsSnapshot {
    /// Flushes that wrote something. Empty flushes are not counted.
    pub total_flushes: u64,
    /// Rows written across all tables.
    pub total_rows_written: u64,
    /// Size on disk of every file written.
    pub total_bytes_written: u64,
    /// Duration of the most recent successful flush; 0 before the first.
    pub last_flush_duration_ms: u64,
    /// Flushes that failed and left their rows buffered.
    pub flush_errors: u64,
}

impl WriterMetrics {
    pub fn snapshot(&self) -> WriterMetricsSnapshot {
        WriterMetricsSnapshot {
            total_flushes: self.total_flushes.load(Ordering::Relaxed),
            total_rows_written: self.total_rows_written.load(Ordering::Relaxed),
            total_bytes_written: self.total_bytes_written.load(Ordering::Relaxed),
            last_flush_duration_ms: self.last_flush_duration_ms.load(Ordering::Relaxed),
            flush_errors: self.flush_errors.load(Ordering::Relaxed),
        }
    }

    fn record_flush(&self, rows: u64, bytes: u64, duration_ms: u64) {
        self.total_flushes.fetch_add(1, Ordering::Relaxed);
        self.total_rows_written.fetch_add(rows, Ordering::Relaxed);
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
        self.last_flush_duration_ms.store(duration_ms, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.flush_errors.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// One file a flush put on disk.
#[derive(Debug)]
struct WrittenFile {
    path: PathBuf,
    bytes: u64,
}

/// Buffers UDM rows and writes them as one Parquet file per table per flush.
#[derive(Debug)]
pub struct ParquetWriter {
    data_dir: PathBuf,
    slot_buffer: Vec<DeepAnaSlot>,
    transaction_buffer: Vec<DeepAnaTransaction>,
    instruction_buffer: Vec<DeepAnaInstruction>,
    transfer_buffer: Vec<DeepAnaTokenTransfer>,
    /// Lowest slot buffered since the last flush.
    first_buffered_slot: Option<u64>,
    /// Highest slot buffered since the last flush.
    last_buffered_slot: Option<u64>,
    last_flushed_slot: Option<u64>,
    metrics: Arc<WriterMetrics>,
}

impl ParquetWriter {
    /// A writer with empty buffers that writes under `data_dir`. Nothing is
    /// created on disk until the first flush.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            slot_buffer: Vec::new(),
            transaction_buffer: Vec::new(),
            instruction_buffer: Vec::new(),
            transfer_buffer: Vec::new(),
            first_buffered_slot: None,
            last_buffered_slot: None,
            last_flushed_slot: None,
            metrics: Arc::default(),
        }
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// The writer's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<WriterMetrics> {
        &self.metrics
    }

    pub fn first_buffered_slot(&self) -> Option<u64> {
        self.first_buffered_slot
    }

    pub fn last_buffered_slot(&self) -> Option<u64> {
        self.last_buffered_slot
    }

    /// Rows buffered per table, in [`TableKind::ALL`] order.
    pub fn buffered_row_counts(&self) -> [usize; 4] {
        [
            self.slot_buffer.len(),
            self.transaction_buffer.len(),
            self.instruction_buffer.len(),
            self.transfer_buffer.len(),
        ]
    }

    /// The partition keys the next flush would write to, `None` when nothing
    /// is buffered. Filed under the day of the first buffered slot that has
    /// a block time.
    pub fn pending_partition_keys(&self) -> Option<[PartitionKey; 4]> {
        let (from, to) = self.first_buffered_slot.zip(self.last_buffered_slot)?;
        let block_time = self.slot_buffer.iter().find_map(|slot| slot.block_time);
        let date = PartitionKey::date_for_block_time(block_time);
        Some(PartitionKey::for_all_tables(&self.data_dir, date, from, to))
    }

    /// Writes every non-empty buffer to its key. On the first failure, the
    /// files already written are removed again and the error returned.
    fn write_tables(&self, keys: &[PartitionKey; 4]) -> PrismResult<Vec<WrittenFile>> {
        let mut written = Vec::with_capacity(keys.len());
        for key in keys {
            let result = match key.table() {
                TableKind::Slots => write_table(key, &self.slot_buffer),
                TableKind::Transactions => write_table(key, &self.transaction_buffer),
                TableKind::Instructions => write_table(key, &self.instruction_buffer),
                TableKind::TokenTransfers => write_table(key, &self.transfer_buffer),
            };
            match result {
                Ok(Some(file)) => written.push(file),
                Ok(None) => debug!(table = %key.table(), "nothing buffered; table skipped"),
                Err(err) => {
                    for file in &written {
                        if let Err(cleanup) = fs::remove_file(&file.path) {
                            warn!(path = %file.path.display(), "could not remove file of failed flush: {cleanup}");
                        }
                    }
                    return Err(err);
                }
            }
        }
        Ok(written)
    }

    fn clear_buffers(&mut self) {
        self.slot_buffer.clear();
        self.transaction_buffer.clear();
        self.instruction_buffer.clear();
        self.transfer_buffer.clear();
        self.first_buffered_slot = None;
        self.last_buffered_slot = None;
    }

    /// Drops buffered rows at or above `from_slot` and recomputes the
    /// buffered range from what is left.
    fn rollback_buffers(&mut self, from_slot: u64) {
        self.slot_buffer.retain(|row| row.slot < from_slot);
        self.transaction_buffer.retain(|row| row.slot < from_slot);
        self.instruction_buffer.retain(|row| row.slot < from_slot);
        self.transfer_buffer.retain(|row| row.slot < from_slot);
        self.first_buffered_slot = self.slot_buffer.iter().map(|row| row.slot).min();
        self.last_buffered_slot = self.slot_buffer.iter().map(|row| row.slot).max();
    }
}

impl Storage for ParquetWriter {
    /// Fails with [`PrismError::StorageError`] when `slot` is already
    /// buffered: appending it again would write its rows twice.
    fn append(
        &mut self,
        slot: &DeepAnaSlot,
        transactions: &[DeepAnaTransaction],
        instructions: &[DeepAnaInstruction],
        transfers: &[DeepAnaTokenTransfer],
    ) -> PrismResult<()> {
        let number = slot.slot;
        // Slots arrive in order, so the scan only runs for a backfill.
        if self.last_buffered_slot.is_some_and(|last| number <= last)
            && self.slot_buffer.iter().any(|row| row.slot == number)
        {
            return Err(PrismError::storage(
                TableKind::Slots.as_str(),
                (number, number),
                ParquetError::General(format!("slot {number} is already buffered")),
            ));
        }
        self.slot_buffer.push(slot.clone());
        self.transaction_buffer.extend_from_slice(transactions);
        self.instruction_buffer.extend_from_slice(instructions);
        self.transfer_buffer.extend_from_slice(transfers);
        self.first_buffered_slot = Some(self.first_buffered_slot.map_or(number, |f| f.min(number)));
        self.last_buffered_slot = Some(self.last_buffered_slot.map_or(number, |l| l.max(number)));
        Ok(())
    }

    fn flush(&mut self) -> PrismResult<FlushResult> {
        let Some(keys) = self.pending_partition_keys() else {
            return Ok(FlushResult::default());
        };
        let started = Instant::now();
        let written = self.write_tables(&keys).inspect_err(|err| {
            self.metrics.record_error();
            warn!(error = %err, "flush failed; rows stay buffered for the next attempt");
        })?;

        let (from_slot, to_slot) = (keys[0].from_slot(), keys[0].to_slot());
        let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let result = FlushResult {
            slots_written: self.slot_buffer.len() as u64,
            transactions_written: self.transaction_buffer.len() as u64,
            instructions_written: self.instruction_buffer.len() as u64,
            transfers_written: self.transfer_buffer.len() as u64,
            parquet_paths: written.iter().map(|f| f.path.display().to_string()).collect(),
            from_slot,
            to_slot,
            duration_ms,
        };
        let bytes = written.iter().map(|f| f.bytes).sum();
        self.metrics.record_flush(result.total_rows(), bytes, duration_ms);
        self.clear_buffers();
        self.last_flushed_slot = Some(self.last_flushed_slot.map_or(to_slot, |l| l.max(to_slot)));
        info!(
            from_slot,
            to_slot,
            slots = result.slots_written,
            transactions = result.transactions_written,
            instructions = result.instructions_written,
            transfers = result.transfers_written,
            bytes,
            duration_ms,
            "flushed to parquet"
        );
        Ok(result)
    }

    /// Every file under `{data_dir}/solana` whose filename starts at or
    /// above `from_slot` is deleted. A file whose range straddles `from_slot`
    /// is rewritten with only its rows below `from_slot`, under a filename
    /// ending at `from_slot - 1`, so no rolled-back row survives on disk.
    /// Buffered rows at or above `from_slot` are dropped.
    ///
    /// Idempotent: on `Err`, calling it again finishes the job.
    fn rollback(&mut self, from_slot: u64) -> PrismResult<()> {
        let root = PartitionKey::chain_root(&self.data_dir);
        let files = list_parquet_files(&root).map_err(|err| {
            PrismError::storage("all", (from_slot, u64::MAX), err)
        })?;
        let (mut deleted, mut truncated) = (0, 0);
        for path in files {
            let Some(parsed) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(PartitionKey::parse_file_name)
            else {
                continue;
            };
            let range = (parsed.from_slot, parsed.to_slot);
            if parsed.from_slot >= from_slot {
                fs::remove_file(&path)
                    .map_err(|err| PrismError::storage(parsed.table.as_str(), range, err))?;
                deleted += 1;
            } else if parsed.to_slot >= from_slot {
                let Some(key) = PartitionKey::from_path(&path) else {
                    warn!(path = %path.display(), "file straddles the rollback point but is outside the layout; left alone");
                    continue;
                };
                truncate_file(&key, from_slot)?;
                truncated += 1;
            }
        }

        self.rollback_buffers(from_slot);
        if self.last_flushed_slot.is_some_and(|last| last >= from_slot) {
            self.last_flushed_slot = from_slot.checked_sub(1);
        }
        info!(from_slot, deleted, truncated, "rolled back");
        Ok(())
    }

    fn buffered_slot_count(&self) -> u64 {
        self.slot_buffer.len() as u64
    }

    fn last_flushed_slot(&self) -> Option<u64> {
        self.last_flushed_slot
    }
}

// ---------------------------------------------------------------------------
// File I/O
// ---------------------------------------------------------------------------

/// Writes `rows` to `key`'s file. `None` when there are no rows: an empty
/// table gets no file.
fn write_table<R: UdmRow>(key: &PartitionKey, rows: &[R]) -> PrismResult<Option<WrittenFile>> {
    if rows.is_empty() {
        return Ok(None);
    }
    let batch = R::to_record_batch(rows).map_err(|err| storage_error(key, err))?;
    write_batch(key, &batch).map(Some)
}

/// Writes `batch` to `key.temp_path()`, syncs it and renames it to
/// `key.path()`. The temporary file is removed on failure.
fn write_batch(key: &PartitionKey, batch: &RecordBatch) -> PrismResult<WrittenFile> {
    fs::create_dir_all(key.dir()).map_err(|err| storage_error(key, err))?;
    let (temp, path) = (key.temp_path(), key.path());
    let result = write_parquet(&temp, batch).and_then(|()| {
        fs::rename(&temp, &path)?;
        Ok(fs::metadata(&path)?.len())
    });
    match result {
        Ok(bytes) => Ok(WrittenFile { path, bytes }),
        Err(err) => {
            let _ = fs::remove_file(&temp);
            Err(storage_error(key, err))
        }
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<(), ParquetError> {
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(&file, batch.schema(), Some(writer_properties()))?;
    writer.write(batch)?;
    // Writes the footer; the file is unreadable without it.
    writer.close()?;
    file.sync_all()?;
    Ok(())
}

/// Rewrites `key`'s file with only its rows below `from_slot`, under the
/// key cut to end at `from_slot - 1`, then removes the original. A file
/// with no rows left is just removed. `from_slot` must be above
/// `key.from_slot()`.
fn truncate_file(key: &PartitionKey, from_slot: u64) -> PrismResult<()> {
    let kept = read_rows_below(&key.path(), from_slot).map_err(|err| storage_error(key, err))?;
    if kept.num_rows() > 0 {
        write_batch(&key.with_to_slot(from_slot - 1), &kept)?;
    }
    fs::remove_file(key.path()).map_err(|err| storage_error(key, err))
}

/// The rows of the Parquet file at `path` whose `slot` is below `from_slot`,
/// as one batch.
fn read_rows_below(path: &Path, from_slot: u64) -> Result<RecordBatch, ParquetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = builder.schema().clone();
    let bound = Scalar::new(UInt64Array::from(vec![from_slot]));
    let mut kept = Vec::new();
    for batch in builder.build()? {
        let batch = batch?;
        let slots = batch.column_by_name(SLOT_COLUMN).ok_or_else(|| {
            ParquetError::General(format!("{} has no {SLOT_COLUMN} column", path.display()))
        })?;
        kept.push(filter_record_batch(&batch, &lt(slots, &bound)?)?);
    }
    Ok(arrow::compute::concat_batches(&schema, &kept)?)
}

/// Every `.parquet` file under `root`, recursively, sorted. A missing root
/// has none.
fn list_parquet_files(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == PARQUET_EXTENSION) {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn storage_error(key: &PartitionKey, source: impl Into<ParquetError>) -> PrismError {
    PrismError::storage(key.table().as_str(), (key.from_slot(), key.to_slot()), source)
}
