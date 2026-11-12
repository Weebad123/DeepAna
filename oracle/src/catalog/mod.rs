//! The catalog: what ORACLE can measure, what it can group and filter by, and
//! the names it knows for addresses.
//!
//! | File              | Holds                                                         |
//! |-------------------|---------------------------------------------------------------|
//! | `mod.rs`          | [`Catalog`], the four table schemas, [`Limits`], validation   |
//! | [`measures`]      | [`Measure`] definitions: an aggregate over one table          |
//! | [`dimensions`]    | [`Dimension`] definitions: a group-by / filter expression     |
//! | [`entities`]      | [`Entity`] index: name or alias → address, and the reverse    |
//!
//! # Decisions
//!
//! * **Definitions are plain Rust values**, compiled in, reviewed like code.
//! * **They are structured data, not SQL strings.** A measure is a
//!   [`MeasureKind`] over named columns, a dimension a [`DimExpr`]. SQL is
//!   rendered from them ([`Measure::select_expr`], [`Dimension::select_expr`],
//!   [`Pred::sql`]) and only from names and literals validated at startup.
//! * **Single-table queries only.** A measure reads one [`Table`]; a dimension
//!   is defined on one table or on all of them ([`DimTable::Any`]). There
//!   are no joins.
//! * **The four schemas are constants here**, and must match what PRISM's
//!   writer puts on disk column for column (name, order, type, nullability).
//!   The `catalog_prism_test` integration test reads PRISM's real Parquet
//!   output and LENS's view of it to prove they do.
//! * **Counts use `COUNT(*)`**, as `detect/` and `digest/` do. A filtered
//!   count's filter goes in the `WHERE` clause ([`Measure::filter`]).
//! * **Measures carry warnings** ([`Measure::warnings`]) that later modules
//!   must repeat next to any number they report.
//! * **Token amounts are raw only and never summed across mints**: a
//!   [`Unit::RawTokenAmount`] sum must require the `mint` dimension and
//!   exclude rows with no mint. [`Catalog::new`] refuses anything else.
//! * **Windows are measured in slots.** There are no hour or day dimensions:
//!   `block_time` is nullable and not monotonic enough to bucket by.
//! * **Entities are a curated set plus host-supplied extras**, validated
//!   together. An extra that collides with a built-in is an error, never an
//!   override.
//! * **Fail fast at startup.** [`Catalog::new`] validates every definition
//!   and returns the first failure.
//! * **The catalog is immutable and deterministic**: every listing is sorted,
//!   and the same input always builds the same catalog. Share it as
//!   `Arc<Catalog>`.

use std::fmt;

use serde::Serialize;
use thiserror::Error;

pub mod dimensions;
pub mod entities;
pub mod measures;

pub use dimensions::{DIMENSIONS, DimExpr, DimTable, Dimension, MAX_BUCKET_WIDTH_SLOTS, ValueError, ValueRule};
pub use entities::{BUILTIN_ENTITIES, BuiltinEntity, Entity, EntityDef, EntityKind, EntityLookup, normalize};
pub use measures::{MEASURES, Measure, MeasureKind, Pred, Unit};

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

/// The four tables PRISM writes and LENS serves under the `solana` schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Table {
    Slots,
    Transactions,
    Instructions,
    TokenTransfers,
}

impl Table {
    pub const ALL: [Table; 4] = [Table::Slots, Table::Transactions, Table::Instructions, Table::TokenTransfers];

    /// Unqualified name: `slots`.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Slots => "slots",
            Self::Transactions => "transactions",
            Self::Instructions => "instructions",
            Self::TokenTransfers => "token_transfers",
        }
    }

    /// The name LENS serves it under: `solana.slots`.
    pub const fn sql_name(self) -> &'static str {
        match self {
            Self::Slots => "solana.slots",
            Self::Transactions => "solana.transactions",
            Self::Instructions => "solana.instructions",
            Self::TokenTransfers => "solana.token_transfers",
        }
    }

    /// The table's columns, in PRISM's order.
    pub const fn columns(self) -> &'static [Column] {
        match self {
            Self::Slots => &SLOTS_COLUMNS,
            Self::Transactions => &TRANSACTIONS_COLUMNS,
            Self::Instructions => &INSTRUCTIONS_COLUMNS,
            Self::TokenTransfers => &TOKEN_TRANSFERS_COLUMNS,
        }
    }

    pub fn column(self, name: &str) -> Option<&'static Column> {
        self.columns().iter().find(|c| c.name == name)
    }
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A column's type, as PRISM's Arrow schema declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColType {
    U8,
    U32,
    U64,
    I64,
    Utf8,
    Bool,
    /// Arrow `Timestamp(Second, "UTC")`: PRISM's `block_time`. Not an `I64`:
    /// it compares and casts as a timestamp.
    TimestampSecs,
}

impl ColType {
    pub const fn is_integer(self) -> bool {
        matches!(self, Self::U8 | Self::U32 | Self::U64 | Self::I64)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::U8 => "U8",
            Self::U32 => "U32",
            Self::U64 => "U64",
            Self::I64 => "I64",
            Self::Utf8 => "Utf8",
            Self::Bool => "Bool",
            Self::TimestampSecs => "TimestampSecs",
        }
    }
}

impl fmt::Display for ColType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Column {
    pub name: &'static str,
    pub ty: ColType,
    pub nullable: bool,
}

const fn col(name: &'static str, ty: ColType, nullable: bool) -> Column {
    Column { name, ty, nullable }
}

use ColType::{TimestampSecs, U8, U32, U64, Utf8};

/// `solana.slots`: PRISM's `DeepAnaSlot`.
pub const SLOTS_COLUMNS: [Column; 7] = [
    col("slot", U64, false),
    col("blockhash", Utf8, false),
    col("parent_slot", U64, false),
    col("block_time", TimestampSecs, true),
    col("block_height", U64, true),
    col("transaction_count", U32, false),
    col("leader", Utf8, true),
];

/// `solana.transactions`: PRISM's `DeepAnaTransaction`. `accounts` and
/// `log_messages` are JSON arrays stored as text.
pub const TRANSACTIONS_COLUMNS: [Column; 10] = [
    col("slot", U64, false),
    col("blockhash", Utf8, false),
    col("transaction_index", U32, false),
    col("tx_signature", Utf8, false),
    col("fee", U64, false),
    col("status", Utf8, false),
    col("err", Utf8, true),
    col("accounts", Utf8, false),
    col("log_messages", Utf8, false),
    col("compute_units_consumed", U64, true),
];

/// `solana.instructions`: PRISM's `DeepAnaInstruction`. `accounts` and
/// `decoded_data` are JSON stored as text.
pub const INSTRUCTIONS_COLUMNS: [Column; 9] = [
    col("slot", U64, false),
    col("tx_signature", Utf8, false),
    col("instruction_index", U32, false),
    col("inner_instruction_index", U32, true),
    col("program_id", Utf8, false),
    col("accounts", Utf8, false),
    col("data_base58", Utf8, false),
    col("instruction_type", Utf8, true),
    col("decoded_data", Utf8, true),
];

/// `solana.token_transfers`: PRISM's `DeepAnaTokenTransfer`.
pub const TOKEN_TRANSFERS_COLUMNS: [Column; 13] = [
    col("slot", U64, false),
    col("tx_signature", Utf8, false),
    col("instruction_index", U32, false),
    col("inner_instruction_index", U32, true),
    col("spl_token_program_id", Utf8, false),
    col("source_token_account", Utf8, false),
    col("destination_token_account", Utf8, false),
    col("source_owner", Utf8, true),
    col("destination_owner", Utf8, true),
    col("mint_token_address", Utf8, true),
    col("amount_raw", U64, false),
    col("decimals", U8, true),
    col("block_time", TimestampSecs, true),
];

/// The column every table carries, and `slot_bucket` buckets.
pub const SLOT_COLUMN: &str = "slot";

// ---------------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------------

/// Longest measure or dimension id.
pub const MAX_ID_LEN: usize = 64;
/// Longest literal a definition renders into SQL.
pub const MAX_LITERAL_LEN: usize = 64;
/// Longest entity name or alias, after trimming.
pub const MAX_ENTITY_NAME_LEN: usize = 64;
/// Most aliases one entity may have.
pub const MAX_ENTITY_ALIASES: usize = 8;
/// Most entities a host may add.
pub const MAX_EXTRA_ENTITIES: usize = 1024;
/// Largest slot a value may name: LENS casts slots to `BIGINT`.
pub const MAX_SLOT: u64 = i64::MAX as u64;

/// The catalog's fixed limits, for callers and for `/catalog`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Limits {
    pub max_bucket_width_slots: u64,
    pub max_slot: u64,
    pub max_entity_name_len: usize,
    pub max_entity_aliases: usize,
    pub max_extra_entities: usize,
}

pub const LIMITS: Limits = Limits {
    max_bucket_width_slots: MAX_BUCKET_WIDTH_SLOTS,
    max_slot: MAX_SLOT,
    max_entity_name_len: MAX_ENTITY_NAME_LEN,
    max_entity_aliases: MAX_ENTITY_ALIASES,
    max_extra_entities: MAX_EXTRA_ENTITIES,
};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why [`Catalog::new`] refused to build.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CatalogError {
    #[error("duplicate {kind} id {id:?}")]
    DuplicateId { kind: &'static str, id: String },
    #[error("{item}: no column {column:?}")]
    UnknownColumn { item: String, column: String },
    #[error("{item}: column {column:?} is {found}, expected {expected}")]
    TypeMismatch { item: String, column: String, expected: String, found: String },
    #[error("{item}: {reason}")]
    BadDefinition { item: String, reason: String },
    #[error("entity {name:?}: {reason}")]
    BadEntity { name: String, reason: String },
    #[error("entity key {key:?} belongs to both {existing:?} and {new:?}")]
    EntityConflict { key: String, existing: String, new: String },
}

pub(crate) fn bad(item: impl Into<String>, reason: impl Into<String>) -> CatalogError {
    CatalogError::BadDefinition { item: item.into(), reason: reason.into() }
}

// ---------------------------------------------------------------------------
// Shared rules
// ---------------------------------------------------------------------------

/// A measure or dimension id: `[a-z][a-z0-9_]*`, at most [`MAX_ID_LEN`].
pub fn is_valid_id(id: &str) -> bool {
    id.len() <= MAX_ID_LEN
        && id.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && id.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// A literal a definition may splice into SQL: `[a-z0-9_]+`, at most
/// [`MAX_LITERAL_LEN`]. Nothing that could close a quote.
pub fn is_safe_literal(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_LITERAL_LEN
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

/// A base58 string that decodes to exactly 32 bytes: a Solana address.
/// Base58's alphabet is alphanumeric, so a key is safe to quote in SQL.
pub fn is_base58_key(s: &str) -> bool {
    (32..=44).contains(&s.len()) && bs58::decode(s).into_vec().is_ok_and(|b| b.len() == 32)
}

/// `name` in `table`, or [`CatalogError::UnknownColumn`].
pub(crate) fn column_of(item: &str, table: Table, name: &str) -> Result<&'static Column, CatalogError> {
    table
        .column(name)
        .ok_or_else(|| CatalogError::UnknownColumn { item: item.to_owned(), column: format!("{table}.{name}") })
}

/// `column` must have a type `ok` accepts, described as `expected`.
pub(crate) fn expect_type(
    item: &str,
    table: Table,
    column: &Column,
    expected: &str,
    ok: impl Fn(ColType) -> bool,
) -> Result<(), CatalogError> {
    if ok(column.ty) {
        Ok(())
    } else {
        Err(CatalogError::TypeMismatch {
            item: item.to_owned(),
            column: format!("{table}.{}", column.name),
            expected: expected.to_owned(),
            found: column.ty.name().to_owned(),
        })
    }
}

pub(crate) fn check_text(item: &str, what: &str, s: &str) -> Result<(), CatalogError> {
    if s.trim().is_empty() { Err(bad(item, format!("empty {what}"))) } else { Ok(()) }
}

pub(crate) fn check_warnings(item: &str, warnings: &[&str]) -> Result<(), CatalogError> {
    warnings.iter().try_for_each(|w| check_text(item, "warning", w))
}

/// Every table starts with a non-null `slot` U64 and has no repeated column.
fn check_schemas() -> Result<(), CatalogError> {
    for table in Table::ALL {
        let item = format!("table {table}");
        let cols = table.columns();
        match cols.first() {
            Some(c) if c.name == SLOT_COLUMN && c.ty == ColType::U64 && !c.nullable => {}
            _ => return Err(bad(&item, "the first column must be a non-null U64 `slot`")),
        }
        for (i, c) in cols.iter().enumerate() {
            if !is_valid_id(c.name) {
                return Err(bad(&item, format!("column name {:?} is not an identifier", c.name)));
            }
            if cols[..i].iter().any(|d| d.name == c.name) {
                return Err(bad(&item, format!("column {:?} appears twice", c.name)));
            }
        }
    }
    Ok(())
}

/// Ids are well formed and unique within `kind`.
fn check_ids<'a>(kind: &'static str, ids: impl Iterator<Item = &'a str>) -> Result<(), CatalogError> {
    let mut seen = std::collections::BTreeSet::new();
    for id in ids {
        if !is_valid_id(id) {
            return Err(bad(format!("{kind} {id:?}"), "the id must match [a-z][a-z0-9_]* and be at most 64 bytes"));
        }
        if !seen.insert(id) {
            return Err(CatalogError::DuplicateId { kind, id: id.to_owned() });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/// The validated, immutable catalog. `Send + Sync`; share it as
/// `Arc<Catalog>`.
#[derive(Debug)]
pub struct Catalog {
    /// Sorted by id.
    measures: Vec<Measure>,
    /// Sorted by id.
    dimensions: Vec<Dimension>,
    entities: entities::EntityIndex,
}

impl Catalog {
    /// The built-in measures, dimensions and entities, plus `extra_entities`
    /// from the host.
    pub fn new(extra_entities: Vec<EntityDef>) -> Result<Self, CatalogError> {
        Self::from_definitions(MEASURES.to_vec(), DIMENSIONS.to_vec(), extra_entities)
    }

    /// A catalog of exactly these measures and dimensions (plus the built-in
    /// entities and `extra_entities`), under the same checks as [`new`].
    /// Tests use it to show each check fires.
    ///
    /// The checks run in this order, and the first failure is returned:
    ///
    /// 1. the four schemas: `slot` first, no repeated column;
    /// 2. measure ids: well formed, unique;
    /// 3. dimension ids: well formed, unique;
    /// 4. each dimension, by id: columns exist in every table it is on, have
    ///    the types its [`ValueRule`] needs, and its literals are safe;
    /// 5. each measure, by id: columns exist and have the types its kind
    ///    needs, its unit fits its kind, its required dimension exists and
    ///    is compatible, and raw token sums are per mint;
    /// 6. entities: built-ins then extras, each well formed, then no key or
    ///    address owned twice.
    ///
    /// [`new`]: Self::new
    pub fn from_definitions(
        mut measures: Vec<Measure>,
        mut dimensions: Vec<Dimension>,
        extra_entities: Vec<EntityDef>,
    ) -> Result<Self, CatalogError> {
        check_schemas()?;
        check_ids("measure", measures.iter().map(|m| m.id))?;
        check_ids("dimension", dimensions.iter().map(|d| d.id))?;
        measures.sort_by_key(|m| m.id);
        dimensions.sort_by_key(|d| d.id);
        for d in &dimensions {
            d.validate()?;
        }
        for m in &measures {
            m.validate(&dimensions)?;
        }
        let entities = entities::EntityIndex::build(entities::builtin_defs(), extra_entities)?;
        Ok(Self { measures, dimensions, entities })
    }

    pub fn measure(&self, id: &str) -> Option<&Measure> {
        self.measures.binary_search_by_key(&id, |m| m.id).ok().map(|i| &self.measures[i])
    }

    /// Every measure, sorted by id.
    pub fn measures(&self) -> &[Measure] {
        &self.measures
    }

    pub fn dimension(&self, id: &str) -> Option<&Dimension> {
        self.dimensions.binary_search_by_key(&id, |d| d.id).ok().map(|i| &self.dimensions[i])
    }

    /// Every dimension, sorted by id.
    pub fn dimensions(&self) -> &[Dimension] {
        &self.dimensions
    }

    /// Whether `measure_id` can be grouped or filtered by `dimension_id`:
    /// the dimension is on every table or on the measure's. `false` when
    /// either id is unknown.
    pub fn compatible(&self, measure_id: &str, dimension_id: &str) -> bool {
        match (self.measure(measure_id), self.dimension(dimension_id)) {
            (Some(m), Some(d)) => d.table.includes(m.table),
            _ => false,
        }
    }

    pub fn table_columns(&self, table: Table) -> &'static [Column] {
        table.columns()
    }

    pub fn limits(&self) -> Limits {
        LIMITS
    }

    /// The entity `name` names: by its name or an alias (normalised, see
    /// [`normalize`]), or by its exact address.
    pub fn entity(&self, name: &str) -> EntityLookup<'_> {
        self.entities.lookup(name)
    }

    /// The name of the entity at exactly `address` (case-sensitive).
    pub fn entity_name(&self, address: &str) -> Option<&str> {
        self.entities.name_of(address)
    }

    /// Every entity, sorted by slug.
    pub fn entities(&self) -> Vec<&Entity> {
        self.entities.all().iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_and_literals() {
        assert!(is_valid_id("tx_count"));
        assert!(is_valid_id("a"));
        assert!(!is_valid_id(""));
        assert!(!is_valid_id("1a"));
        assert!(!is_valid_id("_a"));
        assert!(!is_valid_id("Tx"));
        assert!(!is_valid_id("a-b"));
        assert!(!is_valid_id(&"a".repeat(65)));
        assert!(is_safe_literal("unattributed"));
        assert!(!is_safe_literal(""));
        assert!(!is_safe_literal("a'b"));
        assert!(!is_safe_literal("A"));
        assert!(!is_safe_literal("a b"));
    }

    #[test]
    fn base58_keys() {
        assert!(is_base58_key("11111111111111111111111111111111"));
        assert!(is_base58_key("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"));
        assert!(!is_base58_key("1111111111111111111111111111111")); // 31 bytes
        assert!(!is_base58_key("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5D0")); // '0'
        assert!(!is_base58_key("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'"));
        assert!(!is_base58_key(""));
    }

    #[test]
    fn schemas_are_well_formed() {
        check_schemas().unwrap();
        for t in Table::ALL {
            assert_eq!(t.sql_name(), format!("solana.{}", t.name()));
            assert_eq!(t.column("slot").map(|c| (c.ty, c.nullable)), Some((ColType::U64, false)));
            assert!(t.column("nope").is_none());
        }
    }

    #[test]
    fn the_built_in_catalog_builds() {
        let c = Catalog::new(vec![]).unwrap();
        assert_eq!(c.measures().len(), MEASURES.len());
        assert_eq!(c.dimensions().len(), DIMENSIONS.len());
        assert_eq!(c.limits(), LIMITS);
    }
}
