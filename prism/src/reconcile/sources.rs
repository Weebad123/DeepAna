//! Where the three inputs of [`classify`](super::classify::classify) come
//! from. Everything here only reads.
//!
//! | Input      | Source                                                        |
//! |------------|---------------------------------------------------------------|
//! | stored     | the `slot` (and `parent_slot`, `blockhash`) columns of the `slots` files |
//! | logged     | `validator_skipped` lines of the skip log                     |
//! | produced   | the adapter's block list for the window (plus a lookback), at `finalized` |
//! | hashes     | the adapter's block headers at `finalized`, when verifying     |
//!
//! A failure to read any of them is returned as an error and is never
//! evidence: a file that cannot be opened is not "nothing stored", an RPC
//! error is not "no blocks".

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use arrow::array::{Array, AsArray};
use arrow::datatypes::UInt64Type;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::errors::ParquetError;

use super::classify::{FinalizedChain, SlotWindow, StoredBlock};
use crate::adapter::{ChainAdapter, CommitmentLevel};
use crate::storage::partition::PARQUET_EXTENSION;
use crate::storage::{PartitionKey, TableKind};
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::SkipReason;

/// Name of the column the slots table is keyed by.
const SLOT_COLUMN: &str = "slot";
const PARENT_SLOT_COLUMN: &str = "parent_slot";
const BLOCKHASH_COLUMN: &str = "blockhash";

// ---------------------------------------------------------------------------
// Stored slots
// ---------------------------------------------------------------------------

/// Every `slots` table file under `data_dir`, with the range its name gives.
fn slot_files(data_dir: &Path) -> io::Result<Vec<(PathBuf, u64, u64)>> {
    let mut files = Vec::new();
    let mut pending = vec![PartitionKey::table_root(data_dir, TableKind::Slots)];
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
                continue;
            }
            if path.extension().is_none_or(|ext| ext != PARQUET_EXTENSION) {
                continue;
            }
            let parsed = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(PartitionKey::parse_file_name);
            if let Some(parsed) = parsed.filter(|p| p.table == TableKind::Slots) {
                files.push((path, parsed.from_slot, parsed.to_slot));
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The lowest slot any `slots` file covers, from file names alone. `None`
/// when nothing has been stored yet.
pub fn lowest_stored_slot(data_dir: &Path) -> PrismResult<Option<u64>> {
    let files = slot_files(data_dir)
        .map_err(|err| PrismError::storage(TableKind::Slots.as_str(), (0, u64::MAX), err))?;
    Ok(files.iter().map(|(_, from, _)| *from).min())
}

/// The slots of `window` that have a row in the `slots` table. Only files
/// whose name overlaps the window are opened, and only their `slot` column
/// is read.
pub fn stored_slots(data_dir: &Path, window: SlotWindow) -> PrismResult<BTreeSet<u64>> {
    let range = (window.start, window.end);
    let fail = |err: ParquetError| PrismError::storage(TableKind::Slots.as_str(), range, err);
    let files = slot_files(data_dir).map_err(|err| fail(err.into()))?;
    let mut stored = BTreeSet::new();
    for (path, from, to) in files {
        if to < window.start || from > window.end {
            continue;
        }
        read_slot_column(&path, window, &mut stored).map_err(fail)?;
    }
    Ok(stored)
}

fn read_slot_column(path: &Path, window: SlotWindow, into: &mut BTreeSet<u64>) -> Result<(), ParquetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let index = builder
        .schema()
        .index_of(SLOT_COLUMN)
        .map_err(|_| ParquetError::General(format!("{} has no {SLOT_COLUMN} column", path.display())))?;
    let mask = ProjectionMask::roots(builder.parquet_schema(), [index]);
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let column = batch
            .column(0)
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| {
                ParquetError::General(format!("{SLOT_COLUMN} column of {} is not u64", path.display()))
            })?;
        for i in 0..column.len() {
            if column.is_valid(i) {
                let slot = column.value(i);
                if window.contains(slot) {
                    into.insert(slot);
                }
            }
        }
    }
    Ok(())
}

/// The stored blocks of `window`: slot → blockhash and parent slot. Only
/// files whose name overlaps the window are opened, and only those three
/// columns are read.
pub fn stored_blocks(data_dir: &Path, window: SlotWindow) -> PrismResult<BTreeMap<u64, StoredBlock>> {
    let range = (window.start, window.end);
    let fail = |err: ParquetError| PrismError::storage(TableKind::Slots.as_str(), range, err);
    let files = slot_files(data_dir).map_err(|err| fail(err.into()))?;
    let mut stored = BTreeMap::new();
    for (path, from, to) in files {
        if to < window.start || from > window.end {
            continue;
        }
        read_block_columns(&path, window, &mut stored).map_err(fail)?;
    }
    Ok(stored)
}

fn read_block_columns(
    path: &Path,
    window: SlotWindow,
    into: &mut BTreeMap<u64, StoredBlock>,
) -> Result<(), ParquetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let missing = |column: &str| ParquetError::General(format!("{} has no {column} column", path.display()));
    let schema = builder.schema().clone();
    let index = |column: &str| schema.index_of(column).map_err(|_| missing(column));
    let indexes = [index(SLOT_COLUMN)?, index(PARENT_SLOT_COLUMN)?, index(BLOCKHASH_COLUMN)?];
    let mask = ProjectionMask::roots(builder.parquet_schema(), indexes);
    for batch in builder.with_projection(mask).build()? {
        let batch = batch?;
        let wrong = |column: &str| ParquetError::General(format!("{column} column of {} has the wrong type", path.display()));
        let u64_column = |batch: &RecordBatch, column: &str| {
            batch
                .column_by_name(column)
                .and_then(|c| c.as_primitive_opt::<UInt64Type>().cloned())
                .ok_or_else(|| wrong(column))
        };
        let slots = u64_column(&batch, SLOT_COLUMN)?;
        let parents = u64_column(&batch, PARENT_SLOT_COLUMN)?;
        let hashes = batch
            .column_by_name(BLOCKHASH_COLUMN)
            .and_then(|c| c.as_string_opt::<i32>().cloned())
            .ok_or_else(|| wrong(BLOCKHASH_COLUMN))?;
        for i in 0..slots.len() {
            if slots.is_valid(i) && window.contains(slots.value(i)) {
                into.insert(
                    slots.value(i),
                    StoredBlock {
                        blockhash: hashes.value(i).to_owned(),
                        parent_slot: parents.value(i),
                    },
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Skip log
// ---------------------------------------------------------------------------

/// One parsed skip log line: `{unix_time},{slot},{reason}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkipLine {
    pub at: i64,
    pub slot: u64,
    pub reason: SkipReason,
}

/// Parse one line (without its newline). `None` for anything malformed.
pub fn parse_skip_line(line: &str) -> Option<SkipLine> {
    let mut parts = line.trim_end_matches('\r').splitn(3, ',');
    let at = parts.next()?.trim().parse().ok()?;
    let slot = parts.next()?.trim().parse().ok()?;
    let reason = parts.next()?.trim().parse().ok()?;
    Some(SkipLine { at, slot, reason })
}

/// Follows the append-only skip log, remembering the slots it says the
/// leader skipped.
///
/// Each [`Self::refresh`] reads only what was appended since the last one,
/// up to the last complete line. A file that got shorter was replaced, and is
/// read again from the start.
#[derive(Debug)]
pub struct SkipLogReader {
    path: PathBuf,
    /// Bytes consumed: always just after a newline.
    offset: u64,
    /// `validator_skipped` slots above the prune floor.
    skipped: BTreeSet<u64>,
    /// Slots at or below this are forgotten and not recorded again.
    floor: u64,
    malformed_lines: u64,
}

impl SkipLogReader {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            offset: 0,
            skipped: BTreeSet::new(),
            floor: 0,
            malformed_lines: 0,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Lines that did not parse, since the reader was created.
    pub const fn malformed_lines(&self) -> u64 {
        self.malformed_lines
    }

    /// Read what was appended since the last call. A missing file has no
    /// lines. Fails with [`PrismError::StorageError`] on a read error, and
    /// then remembers nothing new.
    pub fn refresh(&mut self) -> PrismResult<()> {
        let fail = |err: io::Error| PrismError::storage("skip_log", (0, u64::MAX), err);
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.reset();
                return Ok(());
            }
            Err(err) => return Err(fail(err)),
        };
        let len = file.metadata().map_err(fail)?.len();
        if len < self.offset {
            self.reset();
        }
        if len == self.offset {
            return Ok(());
        }
        file.seek(SeekFrom::Start(self.offset)).map_err(fail)?;
        let mut appended = Vec::new();
        file.take(len - self.offset)
            .read_to_end(&mut appended)
            .map_err(fail)?;
        // A line being appended right now waits for the next refresh.
        let Some(last_newline) = appended.iter().rposition(|&b| b == b'\n') else {
            return Ok(());
        };
        let complete = &appended[..=last_newline];
        for line in complete.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            match std::str::from_utf8(line).ok().and_then(parse_skip_line) {
                Some(skip) if skip.reason == SkipReason::ValidatorSkipped && skip.slot > self.floor => {
                    self.skipped.insert(skip.slot);
                }
                Some(_) => {}
                None => self.malformed_lines += 1,
            }
        }
        self.offset += complete.len() as u64;
        Ok(())
    }

    /// The logged slots inside `window`.
    pub fn logged_in(&self, window: SlotWindow) -> BTreeSet<u64> {
        self.skipped.range(window.start..=window.end).copied().collect()
    }

    /// Forget slots at or below `slot`: they are never examined again.
    pub fn prune_through(&mut self, slot: u64) {
        self.floor = self.floor.max(slot);
        self.skipped = self.skipped.split_off(&(slot.saturating_add(1)));
    }

    fn reset(&mut self) {
        self.offset = 0;
        self.skipped.clear();
    }
}

// ---------------------------------------------------------------------------
// Chain truth
// ---------------------------------------------------------------------------

/// The slots of `window` that hold a block on the finalized chain.
pub async fn produced_slots(adapter: &dyn ChainAdapter, window: SlotWindow) -> PrismResult<BTreeSet<u64>> {
    let slots = adapter
        .get_blocks_in_range(window.start, window.end, CommitmentLevel::Finalized)
        .await?;
    Ok(slots.into_iter().filter(|slot| window.contains(*slot)).collect())
}

/// The finalized blocks of `window`, plus the last finalized block in the
/// `lookback` slots below it (the parent of the window's first block). One
/// block list request.
pub async fn finalized_chain(
    adapter: &dyn ChainAdapter,
    window: SlotWindow,
    lookback: u64,
) -> PrismResult<FinalizedChain> {
    let from = window.start.saturating_sub(lookback);
    let slots = adapter
        .get_blocks_in_range(from, window.end, CommitmentLevel::Finalized)
        .await?;
    let mut chain = FinalizedChain::default();
    for slot in slots {
        if window.contains(slot) {
            chain.produced.insert(slot);
        } else if (from..window.start).contains(&slot) {
            chain.predecessor = chain.predecessor.max(Some(slot));
        }
    }
    Ok(chain)
}

/// The finalized blockhash of each of `slots`. Any failure, a slot the
/// chain now calls skipped included, is an error: the block list said it
/// has a block, so the two answers disagree and nothing can be concluded.
pub async fn finalized_blockhashes(
    adapter: &dyn ChainAdapter,
    slots: impl IntoIterator<Item = u64>,
) -> PrismResult<BTreeMap<u64, String>> {
    let mut hashes = BTreeMap::new();
    for slot in slots {
        let header = adapter.get_block_header(slot, CommitmentLevel::Finalized).await?;
        hashes.insert(slot, header.block_id);
    }
    Ok(hashes)
}
