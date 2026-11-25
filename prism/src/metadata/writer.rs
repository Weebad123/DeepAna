//! [`MetadataWriter`]: the `token_metadata` table on disk, and the set of
//! mints it already holds.
//!
//! ```text
//! {data_dir}/solana/token_metadata/date=YYYY-MM-DD/metadata_{unix_ms}_{seq}.parquet
//! ```
//!
//! The table is not slot-range named: the date is the UTC day of the write,
//! `unix_ms` its wall-clock time and `seq` a counter that keeps two files of
//! the same millisecond apart. The ingestion tables' rollback only touches
//! files named `{slots|txs|ixs|transfers}_{from}_{to}.parquet` and skips this
//! directory outright, so a rewind, a reconcile pass or a backfill never
//! deletes metadata.
//!
//! Write-once is the writer's job, not its callers':
//!
//! * [`MetadataWriter::open`] scans every existing file and loads each
//!   `mint_address` into the **known set**. That scan, not any state file,
//!   is what makes a restart safe. A file it cannot read fails the open:
//!   with part of the table unreadable, the known set would be incomplete
//!   and a mint could be written twice. Leftover `.tmp` files from a crash
//!   mid-write are incomplete by construction and are removed.
//! * A mint joins the known set when it is **buffered**
//!   ([`MetadataWriter::buffer`]), so a second hint for it is refused before
//!   the first row has even reached disk.
//! * Each file is written as `{name}.tmp`, fsynced, renamed into place and
//!   its directory fsynced: a reader (or the next scan) sees a whole file or
//!   none.
//!
//! A failed flush keeps the rows buffered for the next attempt. If the
//! process dies with rows still buffered, their mints are in no file, so the
//! next run fetches them again: nothing is lost and nothing is duplicated.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{AsArray, RecordBatch};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use solana_sdk::pubkey::Pubkey;
use tracing::{info, warn};

use crate::storage::partition::{PARQUET_EXTENSION, PartitionKey, TOKEN_METADATA_DIR};
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::{DeepAnaTokenMetadata, UdmRow};

/// Directory of the table under `{data_dir}/solana/`. Also its name in
/// errors.
pub const TABLE_DIR: &str = TOKEN_METADATA_DIR;

/// Filename prefix: `metadata_{unix_ms}_{seq}.parquet`.
pub const FILE_PREFIX: &str = "metadata";

/// Suffix of a file still being written.
const TEMP_SUFFIX: &str = ".tmp";

/// Column the startup scan reads.
const MINT_COLUMN: &str = "mint_address";

/// `{data_dir}/solana/token_metadata`.
pub fn table_root(data_dir: impl AsRef<Path>) -> PathBuf {
    PartitionKey::chain_root(data_dir).join(TABLE_DIR)
}

/// `metadata_{unix_ms}_{seq}.parquet`.
pub fn file_name(unix_ms: i64, seq: u64) -> String {
    format!("{FILE_PREFIX}_{unix_ms}_{seq}.{PARQUET_EXTENSION}")
}

/// `(unix_ms, seq)` of a name [`file_name`] produced. `None` otherwise.
pub fn parse_file_name(name: &str) -> Option<(i64, u64)> {
    let stem = name
        .strip_suffix(PARQUET_EXTENSION)?
        .strip_suffix('.')?
        .strip_prefix(FILE_PREFIX)?
        .strip_prefix('_')?;
    let (ms, seq) = stem.split_once('_')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

/// What [`MetadataWriter::open`] found on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanReport {
    /// Parquet files read.
    pub files: usize,
    /// Rows across those files.
    pub rows: usize,
    /// Rows whose mint was already in an earlier file. Always 0 unless a
    /// file was copied in from elsewhere.
    pub duplicate_rows: usize,
    /// Leftover `.tmp` files removed.
    pub temp_files_removed: usize,
}

/// What [`MetadataWriter::buffer`] did with a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufferOutcome {
    /// Buffered; the mint is now known.
    Buffered,
    /// The mint is already known (on disk or buffered); the row was dropped.
    Known,
    /// The known set is at its cap; the row was dropped and the mint stays
    /// unknown.
    Full,
}

/// One file a flush wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenFile {
    pub path: PathBuf,
    pub rows: usize,
    pub bytes: u64,
}

/// Buffers `token_metadata` rows and writes them to atomic Parquet files.
#[derive(Debug)]
pub struct MetadataWriter {
    root: PathBuf,
    known: HashSet<Pubkey>,
    max_known: usize,
    buffer: Vec<DeepAnaTokenMetadata>,
    seq: u64,
}

impl MetadataWriter {
    /// Open the table under `data_dir`: remove leftover temp files and load
    /// the mint of every row on disk into the known set. A missing table is
    /// an empty one.
    ///
    /// `max_known` caps how many mints may be *added*; the mints already on
    /// disk are all loaded whatever their number, since forgetting one could
    /// write it twice.
    ///
    /// Fails with [`PrismError::StorageError`] naming the file when any file
    /// or directory of the table cannot be read.
    pub fn open(data_dir: impl AsRef<Path>, max_known: usize) -> PrismResult<(Self, ScanReport)> {
        let root = table_root(data_dir);
        let mut known = HashSet::new();
        let report = scan(&root, &mut known)?;
        if report.files > 0 || report.temp_files_removed > 0 {
            info!(
                files = report.files,
                rows = report.rows,
                known_mints = known.len(),
                temp_files_removed = report.temp_files_removed,
                "token metadata table scanned"
            );
        }
        Ok((
            Self {
                root,
                known,
                max_known,
                buffer: Vec::new(),
                seq: 0,
            },
            report,
        ))
    }

    /// `{data_dir}/solana/token_metadata`.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether `mint` is on disk or buffered.
    pub fn is_known(&self, mint: &Pubkey) -> bool {
        self.known.contains(mint)
    }

    /// Mints on disk or buffered.
    pub fn known_len(&self) -> usize {
        self.known.len()
    }

    /// Whether no further mint can be added.
    pub fn is_full(&self) -> bool {
        self.known.len() >= self.max_known
    }

    /// Rows waiting for the next flush.
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Buffer `row` for `mint`, unless the mint is already known or the
    /// known set is full. `row.mint_address` must be `mint` in base58.
    pub fn buffer(&mut self, mint: Pubkey, row: DeepAnaTokenMetadata) -> BufferOutcome {
        debug_assert_eq!(row.mint_address, mint.to_string());
        if self.known.contains(&mint) {
            return BufferOutcome::Known;
        }
        if self.is_full() {
            return BufferOutcome::Full;
        }
        self.known.insert(mint);
        self.buffer.push(row);
        BufferOutcome::Buffered
    }

    /// Write every buffered row to one new file, at the current time.
    /// `Ok(None)` when nothing is buffered. On `Err` nothing is left on disk
    /// and the rows stay buffered.
    pub fn flush(&mut self) -> PrismResult<Option<WrittenFile>> {
        self.flush_at(Utc::now())
    }

    /// [`Self::flush`] as if it were `now`: the file is named and dated
    /// from it.
    pub fn flush_at(&mut self, now: DateTime<Utc>) -> PrismResult<Option<WrittenFile>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }
        let range = observed_range(&self.buffer);
        let error = |err: ParquetError| PrismError::storage(TABLE_DIR, range, err);
        let batch = DeepAnaTokenMetadata::to_record_batch(&self.buffer).map_err(|e| error(e.into()))?;
        let dir = self.root.join(format!("date={}", now.format("%Y-%m-%d")));
        fs::create_dir_all(&dir).map_err(|e| error(e.into()))?;
        let unix_ms = now.timestamp_millis();
        // A name never reused: rename would replace the earlier file.
        let path = loop {
            let candidate = dir.join(file_name(unix_ms, self.seq));
            self.seq += 1;
            if !candidate.exists() && !temp_path(&candidate).exists() {
                break candidate;
            }
        };
        let bytes = write_atomically(&path, &batch).map_err(error)?;
        let rows = self.buffer.len();
        self.buffer.clear();
        Ok(Some(WrittenFile { path, rows, bytes }))
    }
}

/// Lowest and highest `observed_slot` of `rows`, for error context.
fn observed_range(rows: &[DeepAnaTokenMetadata]) -> (u64, u64) {
    let slots = rows.iter().map(|r| r.observed_slot);
    (slots.clone().min().unwrap_or(0), slots.max().unwrap_or(0))
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(TEMP_SUFFIX);
    PathBuf::from(name)
}

fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build()
}

/// `batch` to `{path}.tmp`, fsynced, renamed to `path`, directory fsynced.
/// The temp file is removed on failure. Returns the file's size.
fn write_atomically(path: &Path, batch: &RecordBatch) -> Result<u64, ParquetError> {
    let temp = temp_path(path);
    let result = (|| {
        let file = File::create(&temp)?;
        let mut writer = ArrowWriter::try_new(&file, batch.schema(), Some(writer_properties()))?;
        writer.write(batch)?;
        writer.close()?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(fs::metadata(path)?.len())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Walk `root`: remove our leftover temp files, read the mints of every
/// Parquet file into `known`.
fn scan(root: &Path, known: &mut HashSet<Pubkey>) -> PrismResult<ScanReport> {
    let mut report = ScanReport::default();
    let fail = |path: &Path, err: ParquetError| {
        PrismError::storage(
            TABLE_DIR,
            (0, u64::MAX),
            ParquetError::General(format!("{}: {err}", path.display())),
        )
    };
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
            Err(err) => return Err(fail(&dir, err.into())),
        };
        for entry in entries {
            let entry = entry.map_err(|e| fail(&dir, e.into()))?;
            let path = entry.path();
            let file_type = entry.file_type().map_err(|e| fail(&path, e.into()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if file_type.is_dir() {
                pending.push(path);
            } else if name.starts_with(FILE_PREFIX) && name.ends_with(TEMP_SUFFIX) {
                match fs::remove_file(&path) {
                    Ok(()) => report.temp_files_removed += 1,
                    Err(err) => warn!(path = %path.display(), error = %err, "could not remove a leftover temp file"),
                }
            } else if path.extension().is_some_and(|ext| ext == PARQUET_EXTENSION) {
                files.push(path);
            }
        }
    }
    files.sort();
    for path in files {
        let (rows, duplicates) = read_mints(&path, known).map_err(|err| fail(&path, err))?;
        report.files += 1;
        report.rows += rows;
        report.duplicate_rows += duplicates;
    }
    if report.duplicate_rows > 0 {
        warn!(duplicate_rows = report.duplicate_rows, "token metadata table holds mints more than once");
    }
    Ok(report)
}

/// Add the `mint_address` of every row of `path` to `known`. Returns the
/// rows read and how many named a mint already known. A row whose mint is
/// not a valid pubkey makes the file unreadable: it was not written here.
fn read_mints(path: &Path, known: &mut HashSet<Pubkey>) -> Result<(usize, usize), ParquetError> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let expected: Arc<_> = DeepAnaTokenMetadata::schema();
    if builder.schema().fields() != expected.fields() {
        return Err(ParquetError::General(format!(
            "schema is not the token_metadata schema: {:?}",
            builder.schema().fields()
        )));
    }
    let (mut rows, mut duplicates) = (0, 0);
    for batch in builder.build()? {
        let batch = batch?;
        let column = batch
            .column_by_name(MINT_COLUMN)
            .ok_or_else(|| ParquetError::General(format!("no {MINT_COLUMN} column")))?;
        for value in column.as_string::<i32>().iter() {
            let value = value.ok_or_else(|| ParquetError::General(format!("null {MINT_COLUMN}")))?;
            let mint = Pubkey::from_str(value)
                .map_err(|err| ParquetError::General(format!("bad {MINT_COLUMN} {value:?}: {err}")))?;
            rows += 1;
            if !known.insert(mint) {
                duplicates += 1;
            }
        }
    }
    Ok((rows, duplicates))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_names_round_trip_and_are_not_slot_ranges() {
        let name = file_name(1_764_000_000_123, 7);
        assert_eq!(name, "metadata_1764000000123_7.parquet");
        assert_eq!(parse_file_name(&name), Some((1_764_000_000_123, 7)));
        assert_eq!(PartitionKey::parse_file_name(&name), None);
        assert_eq!(parse_file_name("transfers_1_2.parquet"), None);
        assert_eq!(parse_file_name("metadata_1_2.parquet.tmp"), None);
    }

    #[test]
    fn observed_range_spans_the_rows() {
        let row = |s| DeepAnaTokenMetadata { observed_slot: s, ..Default::default() };
        assert_eq!(observed_range(&[row(9), row(3), row(5)]), (3, 9));
        assert_eq!(observed_range(&[]), (0, 0));
    }
}
