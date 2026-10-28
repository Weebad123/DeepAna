//! [`PrismTable`]: one UDM dataset registered with DataFusion as a
//! [`ListingTable`].
//!
//! A table knows three things: the directory PRISM writes its Parquet files
//! to, the Arrow schema those files have, and how to describe both to
//! DataFusion:
//!
//! ```text
//! {data_dir}/solana/slots/date=YYYY-MM-DD/slots_{from}_{to}.parquet
//!          │                                            │
//!          └── PrismTable { kind: Slots, dir, schema } ─┘
//!                     │ register_with(ctx)
//!                     ▼
//!          datafusion.solana.slots   (ListingTable over every .parquet below dir)
//! ```
//!
//! # Schemas
//!
//! The schemas here are a copy of PRISM's (`prism::types::udm`, re-exported
//! by `prism::storage::writer`): same column names, order, types and
//! nullability, and no schema metadata. LENS does not depend on PRISM at
//! runtime, so the copy is kept honest by `tests/catalog_test.rs`, which
//! compares the two encoded as Arrow IPC flatbuffers, byte for byte, and
//! against the schema stored in files PRISM actually wrote.
//!
//! The schema is given to DataFusion rather than inferred from the files, so
//! a table whose directory holds no file yet still has the right columns,
//! and a file that does not match fails the query that reads it instead of
//! silently changing the table.
//!
//! The `date=YYYY-MM-DD` directories are not declared as a partition column:
//! the table's columns are exactly PRISM's, and every file is still found,
//! since listing is recursive.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use datafusion::common::TableReference;
use url::Url;

use crate::error::{LensError, LensResult};

/// SQL schema every table is registered under: `solana.slots`, .... Also
/// the directory under `data_dir` that holds them (PRISM's `CHAIN_DIR`).
pub const SCHEMA_NAME: &str = "solana";

/// Extension of the files a table reads. PRISM's in-progress files end in
/// `.parquet.tmp`, so they never match.
pub const PARQUET_FILE_EXTENSION: &str = ".parquet";

/// Time zone of every `block_time` column (PRISM's `UDM_TIMEZONE`).
pub const BLOCK_TIME_TIMEZONE: &str = "UTC";

// ---------------------------------------------------------------------------
// TableKind
// ---------------------------------------------------------------------------

/// The four UDM tables PRISM writes. Mirrors `prism::storage::TableKind`.
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

    /// Unqualified SQL table name.
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::Slots => "slots",
            Self::Transactions => "transactions",
            Self::Instructions => "instructions",
            Self::TokenTransfers => "token_transfers",
        }
    }

    /// Directory under `{data_dir}/solana/` PRISM writes the table to. The
    /// same as [`Self::table_name`], so the SQL name and the path agree.
    pub const fn dir_name(self) -> &'static str {
        self.table_name()
    }

    /// Fully qualified name used in SQL: `solana.{table_name}`.
    pub fn full_name(self) -> String {
        format!("{SCHEMA_NAME}.{}", self.table_name())
    }

    /// `solana.{table_name}` as a DataFusion reference. Registering,
    /// deregistering and listing-cache invalidation all key on it, so they
    /// must all use this one.
    pub fn table_reference(self) -> TableReference {
        TableReference::partial(SCHEMA_NAME, self.table_name())
    }

    /// The Arrow schema PRISM writes this table's files with.
    pub fn schema(self) -> SchemaRef {
        match self {
            Self::Slots => slots_schema(),
            Self::Transactions => transactions_schema(),
            Self::Instructions => instructions_schema(),
            Self::TokenTransfers => token_transfers_schema(),
        }
    }

    /// The table named `name`, qualified (`solana.slots`) or not (`slots`).
    pub fn from_name(name: &str) -> Option<Self> {
        let unqualified = match name.split_once('.') {
            Some((SCHEMA_NAME, table)) => table,
            Some(_) => return None,
            None => name,
        };
        Self::ALL.into_iter().find(|kind| kind.table_name() == unqualified)
    }
}

impl fmt::Display for TableKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.table_name())
    }
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

fn block_time_type() -> DataType {
    DataType::Timestamp(TimeUnit::Second, Some(BLOCK_TIME_TIMEZONE.into()))
}

static SLOTS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("parent_slot", DataType::UInt64, false),
        Field::new("block_time", block_time_type(), true),
        Field::new("block_height", DataType::UInt64, true),
        Field::new("transaction_count", DataType::UInt32, false),
        Field::new("leader", DataType::Utf8, true),
    ]))
});

static TRANSACTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("transaction_index", DataType::UInt32, false),
        Field::new("tx_signature", DataType::Utf8, false),
        Field::new("fee", DataType::UInt64, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("err", DataType::Utf8, true),
        Field::new("accounts", DataType::Utf8, false),
        Field::new("log_messages", DataType::Utf8, false),
        Field::new("compute_units_consumed", DataType::UInt64, true),
    ]))
});

static INSTRUCTIONS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("tx_signature", DataType::Utf8, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("inner_instruction_index", DataType::UInt32, true),
        Field::new("program_id", DataType::Utf8, false),
        Field::new("accounts", DataType::Utf8, false),
        Field::new("data_base58", DataType::Utf8, false),
        Field::new("instruction_type", DataType::Utf8, true),
        Field::new("decoded_data", DataType::Utf8, true),
    ]))
});

static TOKEN_TRANSFERS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("tx_signature", DataType::Utf8, false),
        Field::new("instruction_index", DataType::UInt32, false),
        Field::new("inner_instruction_index", DataType::UInt32, true),
        Field::new("spl_token_program_id", DataType::Utf8, false),
        Field::new("source_token_account", DataType::Utf8, false),
        Field::new("destination_token_account", DataType::Utf8, false),
        Field::new("source_owner", DataType::Utf8, true),
        Field::new("destination_owner", DataType::Utf8, true),
        Field::new("mint_token_address", DataType::Utf8, true),
        Field::new("amount_raw", DataType::UInt64, false),
        Field::new("decimals", DataType::UInt8, true),
        Field::new("block_time", block_time_type(), true),
    ]))
});

/// Schema of `solana.slots`: one row per slot.
pub fn slots_schema() -> Arc<Schema> {
    SLOTS_SCHEMA.clone()
}

/// Schema of `solana.transactions`: one row per transaction. `accounts` and
/// `log_messages` are JSON arrays of strings.
pub fn transactions_schema() -> Arc<Schema> {
    TRANSACTIONS_SCHEMA.clone()
}

/// Schema of `solana.instructions`: one row per top-level or inner
/// instruction. `accounts` is a JSON array, `decoded_data` a JSON value.
pub fn instructions_schema() -> Arc<Schema> {
    INSTRUCTIONS_SCHEMA.clone()
}

/// Schema of `solana.token_transfers`: one row per SPL Token transfer.
pub fn token_transfers_schema() -> Arc<Schema> {
    TOKEN_TRANSFERS_SCHEMA.clone()
}

// ---------------------------------------------------------------------------
// PrismTable
// ---------------------------------------------------------------------------

/// What [`PrismTable::register_with`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterOutcome {
    /// The table is registered, replacing any earlier registration.
    Registered,
    /// The directory does not exist (PRISM has not written the table yet),
    /// so nothing was registered. Not an error.
    SkippedMissingDir,
}

/// One UDM table: its kind, where its files are, and their schema.
#[derive(Debug, Clone)]
pub struct PrismTable {
    kind: TableKind,
    /// `{data_dir}/solana/{dir_name}`.
    dir: PathBuf,
    schema: SchemaRef,
}

impl PrismTable {
    /// The `kind` table under `chain_root` (`{data_dir}/solana`). Touches no
    /// file: whether the directory exists is checked at registration.
    pub fn new(kind: TableKind, chain_root: impl AsRef<Path>) -> Self {
        Self {
            kind,
            dir: chain_root.as_ref().join(kind.dir_name()),
            schema: kind.schema(),
        }
    }

    pub const fn kind(&self) -> TableKind {
        self.kind
    }

    /// The directory the table's Parquet files are under.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// `solana.{table_name}`.
    pub fn full_name(&self) -> String {
        self.kind.full_name()
    }

    /// Whether the table's directory exists. PRISM creates it on its first
    /// flush of the table.
    pub fn dir_exists(&self) -> bool {
        self.dir.is_dir()
    }

    /// How DataFusion should read the files: Parquet, `.parquet` files only,
    /// no partition columns. Parquet reader settings are taken from `ctx`, so
    /// the engine's `datafusion.execution.parquet.*` options apply.
    pub fn listing_options(&self, ctx: &SessionContext) -> ListingOptions {
        let parquet_options = ctx.state().table_options().parquet.clone();
        ListingOptions::new(Arc::new(ParquetFormat::default().with_options(parquet_options)))
            .with_file_extension(PARQUET_FILE_EXTENSION)
    }

    /// `file:///.../{dir_name}/`, tagged with the table's reference so
    /// [`SessionContext::deregister_table`] drops its cached file listing.
    ///
    /// Built from the path with [`Url::from_directory_path`] rather than
    /// [`ListingTableUrl::parse`], so characters like `*` or `[` in the
    /// data directory are taken literally instead of as a glob.
    pub fn listing_table_url(&self) -> LensResult<ListingTableUrl> {
        let dir = std::path::absolute(&self.dir)
            .map_err(|err| LensError::io(self.dir.display().to_string(), err))?;
        let url = Url::from_directory_path(&dir).map_err(|()| {
            LensError::catalog(
                self.kind.table_name(),
                format!("{} cannot be expressed as a file:// URL", dir.display()),
            )
        })?;
        let table_url = ListingTableUrl::try_new(url, None)
            .map_err(|err| LensError::catalog(self.kind.table_name(), err))?;
        Ok(table_url.with_table_ref(self.kind.table_reference()))
    }

    /// The [`ListingTable`] for the table's current directory, with the
    /// fixed schema. Lists no files: DataFusion does that at query time.
    pub fn listing_table(&self, ctx: &SessionContext) -> LensResult<ListingTable> {
        let config = ListingTableConfig::new(self.listing_table_url()?)
            .with_listing_options(self.listing_options(ctx))
            .with_schema(self.schema());
        ListingTable::try_new(config).map_err(|err| LensError::catalog(self.kind.table_name(), err))
    }

    /// Register the table with `ctx` as `solana.{table_name}`, replacing an
    /// earlier registration. Creates the `solana` schema in `ctx`'s default
    /// catalog if it is not there yet.
    ///
    /// When the directory does not exist nothing is registered and
    /// [`RegisterOutcome::SkippedMissingDir`] returned; an existing
    /// registration is left as it is.
    ///
    /// The replacement is atomic: a query planned concurrently sees either
    /// the old table or the new one, never no table. A failure leaves the
    /// old one in place. Afterwards DataFusion's cached file listing for the
    /// table is dropped: that cache never expires by default, so this is what
    /// makes files PRISM wrote since the last registration visible.
    pub fn register_with(&self, ctx: &SessionContext) -> LensResult<RegisterOutcome> {
        // 1. Nothing to read until PRISM's first flush of the table.
        if !self.dir_exists() {
            tracing::debug!(
                table = %self.full_name(),
                dir = %self.dir.display(),
                "table directory does not exist yet; not registered"
            );
            return Ok(RegisterOutcome::SkippedMissingDir);
        }

        // 2-4. URL, config and table. Nothing in `ctx` has changed yet.
        let table = Arc::new(self.listing_table(ctx)?);

        // 5. Swap it in under `solana.{table_name}`.
        let schema = super::ensure_schema(ctx)?;
        schema.replace_table(self.kind.table_name(), table);
        super::invalidate_file_listing(ctx, &self.kind.table_reference())
            .map_err(|err| LensError::catalog(self.kind.table_name(), err))?;

        // 6.
        tracing::info!(
            table = %self.full_name(),
            dir = %self.dir.display(),
            "registered table"
        );
        Ok(RegisterOutcome::Registered)
    }

    /// Remove the table from `ctx`, along with its cached file listing.
    /// Returns whether it was registered.
    pub fn deregister_from(&self, ctx: &SessionContext) -> LensResult<bool> {
        ctx.deregister_table(self.kind.table_reference())
            .map(|old| old.is_some())
            .map_err(|err| LensError::catalog(self.kind.table_name(), err))
    }

    /// Whether `ctx` has a table registered as `solana.{table_name}`. False
    /// when the `solana` schema itself does not exist.
    pub fn is_registered(&self, ctx: &SessionContext) -> bool {
        ctx.table_exist(self.kind.table_reference()).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_kinds_carry_their_names() {
        let names: Vec<_> = TableKind::ALL.iter().map(|kind| kind.table_name()).collect();
        assert_eq!(names, ["slots", "transactions", "instructions", "token_transfers"]);
        for kind in TableKind::ALL {
            assert_eq!(kind.dir_name(), kind.table_name());
            assert_eq!(kind.full_name(), format!("solana.{}", kind.table_name()));
            assert_eq!(kind.to_string(), kind.table_name());
            assert_eq!(kind.table_reference().to_string(), kind.full_name());
        }
    }

    #[test]
    fn from_name_accepts_qualified_and_bare_names_only_under_solana() {
        for kind in TableKind::ALL {
            assert_eq!(TableKind::from_name(kind.table_name()), Some(kind));
            assert_eq!(TableKind::from_name(&kind.full_name()), Some(kind));
        }
        for bad in ["", "solana", "solana.", "public.slots", "solana.slots.x", "Slots", "txs"] {
            assert_eq!(TableKind::from_name(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn schemas_have_unique_columns_a_slot_key_and_no_metadata() {
        for kind in TableKind::ALL {
            let schema = kind.schema();
            assert!(schema.metadata().is_empty(), "{kind}");
            let first = schema.field(0);
            assert_eq!((first.name().as_str(), first.data_type(), first.is_nullable()), ("slot", &DataType::UInt64, false));
            let mut names: Vec<_> = schema.fields().iter().map(|f| f.name()).collect();
            names.sort();
            names.dedup();
            assert_eq!(names.len(), schema.fields().len(), "{kind} has a duplicate column");
            // Shared, not rebuilt per call.
            assert!(Arc::ptr_eq(&schema, &kind.schema()));
        }
    }

    #[test]
    fn block_time_is_a_utc_second_timestamp_wherever_it_appears() {
        for schema in [slots_schema(), token_transfers_schema()] {
            let field = schema.field_with_name("block_time").unwrap();
            assert_eq!(field.data_type(), &DataType::Timestamp(TimeUnit::Second, Some("UTC".into())));
            assert!(field.is_nullable());
        }
    }

    #[test]
    fn new_joins_the_dir_name_without_touching_the_disk() {
        let root = Path::new("/definitely/not/here/solana");
        let table = PrismTable::new(TableKind::TokenTransfers, root);
        assert_eq!(table.dir(), root.join("token_transfers"));
        assert_eq!(table.full_name(), "solana.token_transfers");
        assert!(Arc::ptr_eq(&table.schema(), &token_transfers_schema()));
        assert!(!table.dir_exists());
    }

    #[test]
    fn listing_options_read_only_parquet_files_with_no_partition_columns() {
        let table = PrismTable::new(TableKind::Slots, "/tmp/solana");
        let options = table.listing_options(&SessionContext::new());
        assert_eq!(options.file_extension, ".parquet");
        assert!(options.table_partition_cols.is_empty());
        assert!(options.file_sort_order.is_empty());
    }

    #[test]
    fn listing_options_follow_the_sessions_parquet_settings() {
        let ctx = SessionContext::new();
        ctx.state_ref().write().table_options_mut().parquet.global.pushdown_filters = true;
        let options = PrismTable::new(TableKind::Slots, "/tmp/solana").listing_options(&ctx);
        let format: &dyn std::any::Any = options.format.as_ref();
        let format = format.downcast_ref::<ParquetFormat>().unwrap();
        assert!(format.options().global.pushdown_filters);
    }

    #[test]
    fn listing_table_url_is_a_directory_url_scoped_to_the_table() {
        let table = PrismTable::new(TableKind::Instructions, "/data/[x]*/solana");
        let url = table.listing_table_url().unwrap();
        assert!(url.is_collection(), "{url}");
        assert_eq!(url.as_str(), "file:///data/[x]*/solana/instructions/");
        assert_eq!(url.get_table_ref().as_ref(), Some(&TableKind::Instructions.table_reference()));
    }
}
