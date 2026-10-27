//! Where on disk a Parquet file for a slot range lives.
//!
//! Every file the writer produces sits at a path computed by a
//! [`PartitionKey`]: one directory per table, one Hive-style `date=`
//! directory per UTC day, one file per flushed slot range:
//!
//! ```text
//! {data_dir}/solana/
//!   slots/date=2026-10-27/slots_{from}_{to}.parquet
//!   transactions/date=2026-10-27/txs_{from}_{to}.parquet
//!   instructions/date=2026-10-27/ixs_{from}_{to}.parquet
//!   token_transfers/date=2026-10-27/transfers_{from}_{to}.parquet
//! ```
//!
//! Both ends of the slot range are inclusive. The date is the UTC day of the
//! first slot's `block_time`, so a batch that crosses midnight is filed
//! under the day it started. The filename alone is enough to recover the
//! table and the slot range ([`PartitionKey::parse_file_name`]), which is
//! what rollback relies on.

use std::fmt;
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate};

/// Top-level directory under `data_dir` that holds every Solana table.
pub const CHAIN_DIR: &str = "solana";

/// Extension of every finished data file.
pub const PARQUET_EXTENSION: &str = "parquet";

/// Prefix of the Hive-style date directory: `date=YYYY-MM-DD`.
const DATE_DIR_PREFIX: &str = "date=";

/// Format of the date in the date directory.
const DATE_FORMAT: &str = "%Y-%m-%d";

/// The four tables the writer produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TableKind {
    Slots,
    Transactions,
    Instructions,
    TokenTransfers,
}

impl TableKind {
    pub const ALL: [TableKind; 4] = [
        TableKind::Slots,
        TableKind::Transactions,
        TableKind::Instructions,
        TableKind::TokenTransfers,
    ];

    /// Directory name under `{data_dir}/solana/`. Also the table name used in
    /// [`PrismError::StorageError`](crate::types::error::PrismError::StorageError).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Slots => "slots",
            Self::Transactions => "transactions",
            Self::Instructions => "instructions",
            Self::TokenTransfers => "token_transfers",
        }
    }

    /// Filename prefix, before `_{from}_{to}.parquet`.
    pub const fn file_prefix(self) -> &'static str {
        match self {
            Self::Slots => "slots",
            Self::Transactions => "txs",
            Self::Instructions => "ixs",
            Self::TokenTransfers => "transfers",
        }
    }

    /// The table whose files start with `prefix`.
    pub fn from_file_prefix(prefix: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|table| table.file_prefix() == prefix)
    }
}

impl fmt::Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a data filename says about its contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedFileName {
    pub table: TableKind,
    pub from_slot: u64,
    pub to_slot: u64,
}

/// The location of one Parquet file: a table, a day and a slot range.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionKey {
    data_dir: PathBuf,
    table: TableKind,
    date: NaiveDate,
    from_slot: u64,
    to_slot: u64,
}

impl PartitionKey {
    /// The key for `table`'s file covering `from_slot..=to_slot`, filed under
    /// `date`.
    pub fn new(
        data_dir: impl Into<PathBuf>,
        table: TableKind,
        date: NaiveDate,
        from_slot: u64,
        to_slot: u64,
    ) -> Self {
        debug_assert!(from_slot <= to_slot, "slot range {from_slot}..={to_slot} is reversed");
        Self {
            data_dir: data_dir.into(),
            table,
            date,
            from_slot,
            to_slot,
        }
    }

    /// One key per table, in [`TableKind::ALL`] order, all for the same day
    /// and slot range: the files of one flush.
    pub fn for_all_tables(
        data_dir: impl AsRef<Path>,
        date: NaiveDate,
        from_slot: u64,
        to_slot: u64,
    ) -> [Self; 4] {
        let data_dir = data_dir.as_ref();
        TableKind::ALL.map(|table| Self::new(data_dir, table, date, from_slot, to_slot))
    }

    /// Recover the key from a path this module produced:
    /// `{data_dir}/solana/{table}/date=YYYY-MM-DD/{prefix}_{from}_{to}.parquet`.
    /// `None` for anything that does not have that shape.
    pub fn from_path(path: &Path) -> Option<Self> {
        let parsed = Self::parse_file_name(path.file_name()?.to_str()?)?;
        let date_dir = path.parent()?;
        let date = Self::parse_date_dir(date_dir.file_name()?.to_str()?)?;
        let table_dir = date_dir.parent()?;
        if table_dir.file_name()?.to_str()? != parsed.table.as_str() {
            return None;
        }
        let chain_dir = table_dir.parent()?;
        if chain_dir.file_name()?.to_str()? != CHAIN_DIR {
            return None;
        }
        let data_dir = chain_dir.parent()?;
        Some(Self::new(data_dir, parsed.table, date, parsed.from_slot, parsed.to_slot))
    }

    /// Parse `{prefix}_{from}_{to}.parquet`. `None` for any other name,
    /// temporary files included.
    pub fn parse_file_name(name: &str) -> Option<ParsedFileName> {
        let stem = name.strip_suffix(PARQUET_EXTENSION)?.strip_suffix('.')?;
        let mut parts = stem.rsplitn(3, '_');
        let to_slot = parts.next()?.parse().ok()?;
        let from_slot = parts.next()?.parse().ok()?;
        let table = TableKind::from_file_prefix(parts.next()?)?;
        (from_slot <= to_slot).then_some(ParsedFileName {
            table,
            from_slot,
            to_slot,
        })
    }

    /// Parse a `date=YYYY-MM-DD` directory name.
    pub fn parse_date_dir(name: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(name.strip_prefix(DATE_DIR_PREFIX)?, DATE_FORMAT).ok()
    }

    /// The UTC day a slot with `block_time` (Unix seconds) is filed under.
    /// Slots without a block time, or with one out of range, go to
    /// 1970-01-01 so the path stays deterministic across retries.
    pub fn date_for_block_time(block_time: Option<i64>) -> NaiveDate {
        block_time
            .and_then(|secs| DateTime::from_timestamp(secs, 0))
            .unwrap_or(DateTime::UNIX_EPOCH)
            .date_naive()
    }

    /// `{data_dir}/solana`: the root that rollback scans.
    pub fn chain_root(data_dir: impl AsRef<Path>) -> PathBuf {
        data_dir.as_ref().join(CHAIN_DIR)
    }

    /// `{data_dir}/solana/{table}`.
    pub fn table_root(data_dir: impl AsRef<Path>, table: TableKind) -> PathBuf {
        Self::chain_root(data_dir).join(table.as_str())
    }

    /// The same key with a different upper bound, e.g. for a file cut short
    /// by a rollback.
    pub fn with_to_slot(&self, to_slot: u64) -> Self {
        Self::new(self.data_dir.clone(), self.table, self.date, self.from_slot, to_slot)
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub const fn table(&self) -> TableKind {
        self.table
    }

    pub const fn date(&self) -> NaiveDate {
        self.date
    }

    pub const fn from_slot(&self) -> u64 {
        self.from_slot
    }

    pub const fn to_slot(&self) -> u64 {
        self.to_slot
    }

    /// Number of slots in the range, both ends included.
    pub const fn slot_span(&self) -> u64 {
        self.to_slot - self.from_slot + 1
    }

    /// Whether `slot` falls inside the key's range.
    pub const fn contains_slot(&self, slot: u64) -> bool {
        self.from_slot <= slot && slot <= self.to_slot
    }

    /// `date=YYYY-MM-DD`.
    pub fn date_dir_name(&self) -> String {
        format!("{DATE_DIR_PREFIX}{}", self.date.format(DATE_FORMAT))
    }

    /// `{prefix}_{from}_{to}.parquet`.
    pub fn file_name(&self) -> String {
        format!(
            "{}_{}_{}.{PARQUET_EXTENSION}",
            self.table.file_prefix(),
            self.from_slot,
            self.to_slot
        )
    }

    /// The directory the file goes in, without the filename.
    pub fn dir(&self) -> PathBuf {
        Self::table_root(&self.data_dir, self.table).join(self.date_dir_name())
    }

    /// The full path of the file.
    pub fn path(&self) -> PathBuf {
        self.dir().join(self.file_name())
    }

    /// Where the file is written before being renamed to [`Self::path`], so
    /// that a reader never sees a half-written Parquet file. Does not end in
    /// `.parquet`, so scans skip it.
    pub fn temp_path(&self) -> PathBuf {
        self.dir().join(format!("{}.tmp", self.file_name()))
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path().display())
    }
}
