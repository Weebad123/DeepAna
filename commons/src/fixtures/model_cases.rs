//! Model cases, explicit chains, table row types and consistency rules.
//!
//! # Tables and rows
//!
//! A chain is four tables with **exactly** the columns of PRISM's writer
//! (`prism::storage::writer::schema_for`), every column present in every
//! row, `null` for no value. [`Table::columns`] is that schema written down;
//! a test compares it to PRISM's, column by column. Values must fit the
//! column's type:
//!
//! | Column type                  | Fixture value                                  |
//! |------------------------------|------------------------------------------------|
//! | `UInt64`, `UInt32`, `UInt8`  | an integer in range                            |
//! | `Timestamp(Second, "UTC")`   | an integer in `i64` (Unix seconds)             |
//! | `Utf8`                       | a string                                       |
//! | `Utf8` holding JSON          | the JSON itself: `accounts` and `log_messages` |
//! |                              | are arrays of strings, `decoded_data` any value |
//! | nullable                     | also `null`                                    |
//!
//! The JSON columns are written as the JSON they hold, not as a string of
//! it; their Parquet value is [`canonical_bytes`](super::canonical_bytes) of
//! it, which is byte-for-byte what PRISM's `to_json_string` writes.
//!
//! Column values are also checked for meaning, because fixtures are clean
//! data: addresses and blockhashes are base58 of 32 bytes, signatures of 64
//! bytes, `data_base58` is canonical base58, `status` is `success` or
//! `failed` and `err` is set exactly when it is `failed`, `decoded_data`
//! only comes with an `instruction_type`, and `spl_token_program_id` is SPL
//! Token or Token-2022. At most [`MAX_ROWS_PER_TABLE`] rows per table.
//!
//! # Consistency
//!
//! [`check_consistency`] holds for every chain, explicit or generated, and
//! matches the integrity checks ORACLE's `quality/` runs, on purpose:
//!
//! 1. Natural keys are unique: `slot`; `(slot, tx_signature)`;
//!    `(slot, tx_signature, instruction_index, inner_instruction_index)`
//!    for instructions and for token transfers, a null inner index being
//!    its own value.
//! 2. Every transaction's slot is in slots, and its `blockhash` is that
//!    slot's.
//! 3. Every instruction's `(slot, tx_signature)` is in transactions.
//! 4. Every token transfer's four-part key is in instructions.
//! 5. A slot's `transaction_count` is its number of transaction rows.
//! 6. A slot's `parent_slot` is strictly smaller than the slot, and is
//!    either below the chain's first slot or the slot of a row.
//! 7. A mint has one `decimals` value across all token transfers.
//!
//! # Model cases
//!
//! ```json
//! { "schema_version": 1, "model": "daily-active-programs", "case": "two-mints",
//!   "provenance": { "kind": "hand_computed", "reference": "worked on paper", "note": "…" },
//!   "chain": { "explicit": "two-mints-one-failed" },
//!   "window": { "from_slot": 100, "to_slot": 120 },
//!   "expect": { "columns": ["program_id", "n"], "ordered": false, "rows": [["…", 3]] } }
//! ```
//!
//! `chain` is `{ "explicit": "<chain-id>" }` or `{ "generated": <GenSpec> }`.
//! **Expected rows only come with explicit chains**: a generated chain is
//! exercised by invariants, never by golden rows, so a case over one has no
//! `expect`. Cells are `null`, booleans, integers or strings; there are no
//! floats, so a model output that is not an integer cannot be pinned.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::echo;
use crate::manifest::sha256;

use super::chain_gen::GenSpec;
use super::{
    FieldIssue, Obj, Provenance, StrictValue, base58_exact, canonical_bytes, join_index,
    parse_key, parse_provenance, want_array, want_int, want_name, want_str,
};

/// Most rows in one table of an explicit chain.
pub const MAX_ROWS_PER_TABLE: usize = 2_000;
/// Most rows in a model case's `expect`.
pub const MAX_EXPECT_ROWS: usize = 2_000;
/// Most columns in a model case's `expect`.
pub const MAX_EXPECT_COLUMNS: usize = 64;
/// Longest `data_base58`, in decoded bytes.
const MAX_DATA_BYTES: usize = 1232;

pub const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// A table of PRISM's writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Table {
    Slots,
    Transactions,
    Instructions,
    TokenTransfers,
}

/// How a column is stored. `JsonText` is `Utf8` on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    UInt64,
    UInt32,
    UInt8,
    /// `Timestamp(Second, "UTC")`.
    TimestampSecond,
    Utf8,
    /// `Utf8` holding JSON text.
    JsonText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Column {
    pub name: &'static str,
    pub ty: ColumnType,
    pub nullable: bool,
}

const fn col(name: &'static str, ty: ColumnType, nullable: bool) -> Column {
    Column { name, ty, nullable }
}

use ColumnType::{JsonText, TimestampSecond, UInt8, UInt32, UInt64, Utf8};

const SLOT_COLUMNS: [Column; 7] = [
    col("slot", UInt64, false),
    col("blockhash", Utf8, false),
    col("parent_slot", UInt64, false),
    col("block_time", TimestampSecond, true),
    col("block_height", UInt64, true),
    col("transaction_count", UInt32, false),
    col("leader", Utf8, true),
];

const TRANSACTION_COLUMNS: [Column; 10] = [
    col("slot", UInt64, false),
    col("blockhash", Utf8, false),
    col("transaction_index", UInt32, false),
    col("tx_signature", Utf8, false),
    col("fee", UInt64, false),
    col("status", Utf8, false),
    col("err", Utf8, true),
    col("accounts", JsonText, false),
    col("log_messages", JsonText, false),
    col("compute_units_consumed", UInt64, true),
];

const INSTRUCTION_COLUMNS: [Column; 9] = [
    col("slot", UInt64, false),
    col("tx_signature", Utf8, false),
    col("instruction_index", UInt32, false),
    col("inner_instruction_index", UInt32, true),
    col("program_id", Utf8, false),
    col("accounts", JsonText, false),
    col("data_base58", Utf8, false),
    col("instruction_type", Utf8, true),
    col("decoded_data", JsonText, true),
];

const TOKEN_TRANSFER_COLUMNS: [Column; 13] = [
    col("slot", UInt64, false),
    col("tx_signature", Utf8, false),
    col("instruction_index", UInt32, false),
    col("inner_instruction_index", UInt32, true),
    col("spl_token_program_id", Utf8, false),
    col("source_token_account", Utf8, false),
    col("destination_token_account", Utf8, false),
    col("source_owner", Utf8, true),
    col("destination_owner", Utf8, true),
    col("mint_token_address", Utf8, true),
    col("amount_raw", UInt64, false),
    col("decimals", UInt8, true),
    col("block_time", TimestampSecond, true),
];

impl Table {
    pub const ALL: [Table; 4] = [
        Table::Slots,
        Table::Transactions,
        Table::Instructions,
        Table::TokenTransfers,
    ];

    /// The key in a chain file, and PRISM's `TableKind::as_str`.
    pub fn as_str(self) -> &'static str {
        match self {
            Table::Slots => "slots",
            Table::Transactions => "transactions",
            Table::Instructions => "instructions",
            Table::TokenTransfers => "token_transfers",
        }
    }

    /// The writer's columns, in the writer's order.
    pub fn columns(self) -> &'static [Column] {
        match self {
            Table::Slots => &SLOT_COLUMNS,
            Table::Transactions => &TRANSACTION_COLUMNS,
            Table::Instructions => &INSTRUCTION_COLUMNS,
            Table::TokenTransfers => &TOKEN_TRANSFER_COLUMNS,
        }
    }

    fn column_names(self) -> Vec<&'static str> {
        self.columns().iter().map(|c| c.name).collect()
    }
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SlotRow {
    pub slot: u64,
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    pub transaction_count: u32,
    pub leader: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TxStatus {
    Success,
    Failed,
}

impl TxStatus {
    /// PRISM's storage form.
    pub fn as_str(self) -> &'static str {
        match self {
            TxStatus::Success => "success",
            TxStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionRow {
    pub slot: u64,
    pub blockhash: String,
    pub transaction_index: u32,
    pub tx_signature: String,
    pub fee: u64,
    pub status: TxStatus,
    pub err: Option<String>,
    pub accounts: Vec<String>,
    pub log_messages: Vec<String>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstructionRow {
    pub slot: u64,
    pub tx_signature: String,
    pub instruction_index: u32,
    pub inner_instruction_index: Option<u32>,
    pub program_id: String,
    pub accounts: Vec<String>,
    pub data_base58: String,
    pub instruction_type: Option<String>,
    pub decoded_data: Option<StrictValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenTransferRow {
    pub slot: u64,
    pub tx_signature: String,
    pub instruction_index: u32,
    pub inner_instruction_index: Option<u32>,
    pub spl_token_program_id: String,
    pub source_token_account: String,
    pub destination_token_account: String,
    pub source_owner: Option<String>,
    pub destination_owner: Option<String>,
    pub mint_token_address: Option<String>,
    pub amount_raw: u64,
    pub decimals: Option<u8>,
    pub block_time: Option<i64>,
}

fn object(entries: Vec<(&str, StrictValue)>) -> StrictValue {
    StrictValue::Object(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

use StrictValue as V;

impl SlotRow {
    /// The row as it is written in a chain file.
    pub fn to_strict(&self) -> StrictValue {
        object(vec![
            ("slot", V::Int(self.slot.into())),
            ("blockhash", V::Str(self.blockhash.clone())),
            ("parent_slot", V::Int(self.parent_slot.into())),
            ("block_time", V::opt_int(self.block_time)),
            ("block_height", V::opt_int(self.block_height)),
            ("transaction_count", V::Int(self.transaction_count.into())),
            ("leader", V::opt_str(self.leader.as_deref())),
        ])
    }
}

impl TransactionRow {
    pub fn to_strict(&self) -> StrictValue {
        object(vec![
            ("slot", V::Int(self.slot.into())),
            ("blockhash", V::Str(self.blockhash.clone())),
            ("transaction_index", V::Int(self.transaction_index.into())),
            ("tx_signature", V::Str(self.tx_signature.clone())),
            ("fee", V::Int(self.fee.into())),
            ("status", V::Str(self.status.as_str().into())),
            ("err", V::opt_str(self.err.as_deref())),
            ("accounts", V::str_array(&self.accounts)),
            ("log_messages", V::str_array(&self.log_messages)),
            ("compute_units_consumed", V::opt_int(self.compute_units_consumed)),
        ])
    }
}

impl InstructionRow {
    pub fn to_strict(&self) -> StrictValue {
        object(vec![
            ("slot", V::Int(self.slot.into())),
            ("tx_signature", V::Str(self.tx_signature.clone())),
            ("instruction_index", V::Int(self.instruction_index.into())),
            ("inner_instruction_index", V::opt_int(self.inner_instruction_index)),
            ("program_id", V::Str(self.program_id.clone())),
            ("accounts", V::str_array(&self.accounts)),
            ("data_base58", V::Str(self.data_base58.clone())),
            ("instruction_type", V::opt_str(self.instruction_type.as_deref())),
            ("decoded_data", self.decoded_data.clone().unwrap_or(V::Null)),
        ])
    }

    /// The natural key.
    pub fn key(&self) -> (u64, &str, u32, Option<u32>) {
        (
            self.slot,
            &self.tx_signature,
            self.instruction_index,
            self.inner_instruction_index,
        )
    }
}

impl TokenTransferRow {
    pub fn to_strict(&self) -> StrictValue {
        object(vec![
            ("slot", V::Int(self.slot.into())),
            ("tx_signature", V::Str(self.tx_signature.clone())),
            ("instruction_index", V::Int(self.instruction_index.into())),
            ("inner_instruction_index", V::opt_int(self.inner_instruction_index)),
            ("spl_token_program_id", V::Str(self.spl_token_program_id.clone())),
            ("source_token_account", V::Str(self.source_token_account.clone())),
            ("destination_token_account", V::Str(self.destination_token_account.clone())),
            ("source_owner", V::opt_str(self.source_owner.as_deref())),
            ("destination_owner", V::opt_str(self.destination_owner.as_deref())),
            ("mint_token_address", V::opt_str(self.mint_token_address.as_deref())),
            ("amount_raw", V::Int(self.amount_raw.into())),
            ("decimals", V::opt_int(self.decimals)),
            ("block_time", V::opt_int(self.block_time)),
        ])
    }

    /// The natural key.
    pub fn key(&self) -> (u64, &str, u32, Option<u32>) {
        (
            self.slot,
            &self.tx_signature,
            self.instruction_index,
            self.inner_instruction_index,
        )
    }
}

/// Four tables of rows. Rows keep the order of the file (or of the
/// generator); nothing here depends on it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct Chain {
    pub slots: Vec<SlotRow>,
    pub transactions: Vec<TransactionRow>,
    pub instructions: Vec<InstructionRow>,
    pub token_transfers: Vec<TokenTransferRow>,
}

impl Chain {
    /// Rows per table, in [`Table::ALL`] order.
    pub fn row_counts(&self) -> [usize; 4] {
        [
            self.slots.len(),
            self.transactions.len(),
            self.instructions.len(),
            self.token_transfers.len(),
        ]
    }

    /// The four tables as they are written in a chain file (without
    /// `schema_version` and `id`).
    pub fn to_strict(&self) -> StrictValue {
        object(vec![
            ("slots", V::Array(self.slots.iter().map(SlotRow::to_strict).collect())),
            (
                "transactions",
                V::Array(self.transactions.iter().map(TransactionRow::to_strict).collect()),
            ),
            (
                "instructions",
                V::Array(self.instructions.iter().map(InstructionRow::to_strict).collect()),
            ),
            (
                "token_transfers",
                V::Array(
                    self.token_transfers
                        .iter()
                        .map(TokenTransferRow::to_strict)
                        .collect(),
                ),
            ),
        ])
    }

    /// Canonical bytes of [`Chain::to_strict`]: equal exactly when the
    /// chains are.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_bytes(&self.to_strict())
    }

    /// SHA-256 of [`Chain::canonical_bytes`].
    pub fn digest(&self) -> [u8; 32] {
        sha256(&self.canonical_bytes())
    }
}

// ---------------------------------------------------------------------------
// Row parsing
// ---------------------------------------------------------------------------

/// A row object: exactly the table's columns.
struct Row<'a> {
    o: Obj<'a>,
}

impl<'a> Row<'a> {
    fn of(v: &'a StrictValue, at: &str, table: Table) -> Result<Self, FieldIssue> {
        let o = Obj::of(v, at)?;
        o.only(&table.column_names(), "column")?;
        for c in table.columns() {
            if o.opt(c.name).is_none() {
                return Err(FieldIssue::new(
                    o.path(c.name),
                    "is missing; every column must be present (null for no value)",
                ));
            }
        }
        Ok(Row { o })
    }

    fn field(&self, key: &str) -> String {
        self.o.path(key)
    }

    /// `None` for null; only for nullable columns.
    fn nullable(&self, key: &str) -> Option<&'a StrictValue> {
        match self.o.opt(key).expect("presence checked") {
            V::Null => None,
            v => Some(v),
        }
    }

    fn not_null(&self, key: &str) -> Result<&'a StrictValue, FieldIssue> {
        match self.o.opt(key).expect("presence checked") {
            V::Null => Err(FieldIssue::new(self.field(key), "must not be null")),
            v => Ok(v),
        }
    }

    fn u64(&self, key: &str) -> Result<u64, FieldIssue> {
        want_int(self.not_null(key)?, &self.field(key), "a u64")
    }

    fn u32(&self, key: &str) -> Result<u32, FieldIssue> {
        want_int(self.not_null(key)?, &self.field(key), "a u32")
    }

    fn opt_u64(&self, key: &str) -> Result<Option<u64>, FieldIssue> {
        self.nullable(key)
            .map(|v| want_int(v, &self.field(key), "a u64"))
            .transpose()
    }

    fn opt_u32(&self, key: &str) -> Result<Option<u32>, FieldIssue> {
        self.nullable(key)
            .map(|v| want_int(v, &self.field(key), "a u32"))
            .transpose()
    }

    fn opt_u8(&self, key: &str) -> Result<Option<u8>, FieldIssue> {
        self.nullable(key)
            .map(|v| want_int(v, &self.field(key), "a u8"))
            .transpose()
    }

    fn opt_time(&self, key: &str) -> Result<Option<i64>, FieldIssue> {
        self.nullable(key)
            .map(|v| want_int(v, &self.field(key), "an i64 Unix timestamp"))
            .transpose()
    }

    fn string(&self, key: &str) -> Result<String, FieldIssue> {
        Ok(want_str(self.not_null(key)?, &self.field(key))?.to_string())
    }

    fn opt_string(&self, key: &str) -> Result<Option<String>, FieldIssue> {
        self.nullable(key)
            .map(|v| want_str(v, &self.field(key)).map(str::to_string))
            .transpose()
    }

    fn non_empty(&self, key: &str, s: Option<String>) -> Result<Option<String>, FieldIssue> {
        match s {
            Some(s) if s.is_empty() => Err(FieldIssue::new(
                self.field(key),
                "must not be empty (use null for no value)",
            )),
            s => Ok(s),
        }
    }

    /// Base58 of `len` bytes.
    fn b58(&self, key: &str, len: usize, what: &str) -> Result<String, FieldIssue> {
        let s = self.string(key)?;
        check_b58(&s, len, what).map_err(|r| FieldIssue::new(self.field(key), r))?;
        Ok(s)
    }

    fn opt_b58(&self, key: &str, len: usize, what: &str) -> Result<Option<String>, FieldIssue> {
        let s = self.opt_string(key)?;
        if let Some(s) = &s {
            check_b58(s, len, what).map_err(|r| FieldIssue::new(self.field(key), r))?;
        }
        Ok(s)
    }

    fn key(&self, key: &str) -> Result<String, FieldIssue> {
        self.b58(key, 32, "an address")
    }

    fn opt_key(&self, key: &str) -> Result<Option<String>, FieldIssue> {
        self.opt_b58(key, 32, "an address")
    }

    fn signature(&self) -> Result<String, FieldIssue> {
        self.b58("tx_signature", 64, "a transaction signature")
    }

    /// A JSON column holding an array of strings; `addresses` checks each.
    fn str_list(&self, key: &str, addresses: bool) -> Result<Vec<String>, FieldIssue> {
        let field = self.field(key);
        let items = want_array(self.not_null(key)?, &field, usize::MAX)?;
        items
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let f = join_index(&field, i);
                let s = want_str(v, &f)?;
                if addresses {
                    parse_key(s, "an address").map_err(|r| FieldIssue::new(&f, r))?;
                }
                Ok(s.to_string())
            })
            .collect()
    }
}

fn check_b58(s: &str, len: usize, what: &str) -> Result<(), String> {
    base58_exact(s, Some(len), what).map(|_| ())
}

fn parse_slot(v: &StrictValue, at: &str) -> Result<SlotRow, FieldIssue> {
    let r = Row::of(v, at, Table::Slots)?;
    Ok(SlotRow {
        slot: r.u64("slot")?,
        blockhash: r.b58("blockhash", 32, "a blockhash")?,
        parent_slot: r.u64("parent_slot")?,
        block_time: r.opt_time("block_time")?,
        block_height: r.opt_u64("block_height")?,
        transaction_count: r.u32("transaction_count")?,
        leader: r.opt_key("leader")?,
    })
}

fn parse_transaction(v: &StrictValue, at: &str) -> Result<TransactionRow, FieldIssue> {
    let r = Row::of(v, at, Table::Transactions)?;
    let slot = r.u64("slot")?;
    let blockhash = r.b58("blockhash", 32, "a blockhash")?;
    let transaction_index = r.u32("transaction_index")?;
    let tx_signature = r.signature()?;
    let fee = r.u64("fee")?;
    let status = match r.string("status")?.as_str() {
        "success" => TxStatus::Success,
        "failed" => TxStatus::Failed,
        other => {
            return Err(FieldIssue::new(
                r.field("status"),
                format!("'{}' is not a status (expected success or failed)", echo(other)),
            ));
        }
    };
    let err = r.non_empty("err", r.opt_string("err")?)?;
    match (status, &err) {
        (TxStatus::Failed, None) => {
            return Err(FieldIssue::new(r.field("err"), "is null but status is 'failed'"));
        }
        (TxStatus::Success, Some(_)) => {
            return Err(FieldIssue::new(r.field("err"), "is set but status is 'success'"));
        }
        _ => {}
    }
    Ok(TransactionRow {
        slot,
        blockhash,
        transaction_index,
        tx_signature,
        fee,
        status,
        err,
        accounts: r.str_list("accounts", true)?,
        log_messages: r.str_list("log_messages", false)?,
        compute_units_consumed: r.opt_u64("compute_units_consumed")?,
    })
}

fn parse_instruction(v: &StrictValue, at: &str) -> Result<InstructionRow, FieldIssue> {
    let r = Row::of(v, at, Table::Instructions)?;
    let slot = r.u64("slot")?;
    let tx_signature = r.signature()?;
    let instruction_index = r.u32("instruction_index")?;
    let inner_instruction_index = r.opt_u32("inner_instruction_index")?;
    let program_id = r.key("program_id")?;
    let accounts = r.str_list("accounts", true)?;
    let data_base58 = r.string("data_base58")?;
    if data_base58.len() > 2 * MAX_DATA_BYTES + 2 {
        return Err(FieldIssue::new(
            r.field("data_base58"),
            format!("is longer than {MAX_DATA_BYTES} bytes of data"),
        ));
    }
    base58_exact(&data_base58, None, "instruction data")
        .map_err(|e| FieldIssue::new(r.field("data_base58"), e))?;
    let instruction_type = r.non_empty("instruction_type", r.opt_string("instruction_type")?)?;
    let decoded_data = r.nullable("decoded_data").cloned();
    if decoded_data.is_some() && instruction_type.is_none() {
        return Err(FieldIssue::new(
            r.field("decoded_data"),
            "is set but instruction_type is null; only a decoded instruction has decoded data",
        ));
    }
    Ok(InstructionRow {
        slot,
        tx_signature,
        instruction_index,
        inner_instruction_index,
        program_id,
        accounts,
        data_base58,
        instruction_type,
        decoded_data,
    })
}

fn parse_transfer(v: &StrictValue, at: &str) -> Result<TokenTransferRow, FieldIssue> {
    let r = Row::of(v, at, Table::TokenTransfers)?;
    let slot = r.u64("slot")?;
    let tx_signature = r.signature()?;
    let instruction_index = r.u32("instruction_index")?;
    let inner_instruction_index = r.opt_u32("inner_instruction_index")?;
    let spl_token_program_id = r.key("spl_token_program_id")?;
    if spl_token_program_id != SPL_TOKEN_PROGRAM_ID && spl_token_program_id != TOKEN_2022_PROGRAM_ID
    {
        return Err(FieldIssue::new(
            r.field("spl_token_program_id"),
            format!("'{spl_token_program_id}' is neither SPL Token nor Token-2022"),
        ));
    }
    Ok(TokenTransferRow {
        slot,
        tx_signature,
        instruction_index,
        inner_instruction_index,
        spl_token_program_id,
        source_token_account: r.key("source_token_account")?,
        destination_token_account: r.key("destination_token_account")?,
        source_owner: r.opt_key("source_owner")?,
        destination_owner: r.opt_key("destination_owner")?,
        mint_token_address: r.opt_key("mint_token_address")?,
        amount_raw: r.u64("amount_raw")?,
        decimals: r.opt_u8("decimals")?,
        block_time: r.opt_time("block_time")?,
    })
}

fn parse_table<T>(
    o: &Obj<'_>,
    table: Table,
    parse: fn(&StrictValue, &str) -> Result<T, FieldIssue>,
) -> Result<Vec<T>, FieldIssue> {
    let key = table.as_str();
    let field = o.path(key);
    o.array(key, MAX_ROWS_PER_TABLE)
        .map_err(|i| {
            if i.reason.contains("more than the limit") {
                FieldIssue::new(i.field, format!("{} rows per table", i.reason))
            } else {
                i
            }
        })?
        .iter()
        .enumerate()
        .map(|(i, v)| parse(v, &join_index(&field, i)))
        .collect()
}

/// The four tables of a chain, from the object holding them.
fn parse_tables(o: &Obj<'_>) -> Result<Chain, FieldIssue> {
    Ok(Chain {
        slots: parse_table(o, Table::Slots, parse_slot)?,
        transactions: parse_table(o, Table::Transactions, parse_transaction)?,
        instructions: parse_table(o, Table::Instructions, parse_instruction)?,
        token_transfers: parse_table(o, Table::TokenTransfers, parse_transfer)?,
    })
}

const CHAIN_KEYS: [&str; 6] = [
    "schema_version",
    "id",
    "slots",
    "transactions",
    "instructions",
    "token_transfers",
];

/// Parse an explicit chain file into its id and chain. All four tables are
/// required (an empty table is `[]`). Consistency is checked by the loader,
/// once every file has parsed.
pub fn parse_chain_file(v: &StrictValue) -> Result<(String, Chain), FieldIssue> {
    let o = Obj::of(v, "")?;
    o.schema_version()?;
    o.only(&CHAIN_KEYS, "field")?;
    let id = want_name(o.req("id")?, "id")?.to_string();
    Ok((id, parse_tables(&o)?))
}

// ---------------------------------------------------------------------------
// Consistency
// ---------------------------------------------------------------------------

/// The rules in the module docs, in that order; the first violation (in
/// row order within a rule) is reported, with the row's path as `field`.
pub fn check_consistency(chain: &Chain) -> Result<(), FieldIssue> {
    let at = |table: Table, i: usize| join_index(table.as_str(), i);
    let issue = |table: Table, i: usize, reason: String| FieldIssue::new(at(table, i), reason);

    // 1. Natural keys are unique.
    let mut slots: BTreeMap<u64, usize> = BTreeMap::new();
    for (i, s) in chain.slots.iter().enumerate() {
        if let Some(j) = slots.insert(s.slot, i) {
            return Err(issue(
                Table::Slots,
                i,
                format!("duplicate slot {} (also at {})", s.slot, at(Table::Slots, j)),
            ));
        }
    }
    let mut txs: BTreeMap<(u64, &str), usize> = BTreeMap::new();
    for (i, t) in chain.transactions.iter().enumerate() {
        if let Some(j) = txs.insert((t.slot, &t.tx_signature), i) {
            return Err(issue(
                Table::Transactions,
                i,
                format!(
                    "duplicate (slot, tx_signature) ({}, {}) (also at {})",
                    t.slot,
                    t.tx_signature,
                    at(Table::Transactions, j)
                ),
            ));
        }
    }
    let mut ixs: BTreeMap<(u64, &str, u32, Option<u32>), usize> = BTreeMap::new();
    for (i, x) in chain.instructions.iter().enumerate() {
        if let Some(j) = ixs.insert(x.key(), i) {
            return Err(issue(
                Table::Instructions,
                i,
                format!(
                    "duplicate instruction key {} (also at {})",
                    show_key(x.key()),
                    at(Table::Instructions, j)
                ),
            ));
        }
    }
    let mut transfers: BTreeMap<(u64, &str, u32, Option<u32>), usize> = BTreeMap::new();
    for (i, x) in chain.token_transfers.iter().enumerate() {
        if let Some(j) = transfers.insert(x.key(), i) {
            return Err(issue(
                Table::TokenTransfers,
                i,
                format!(
                    "duplicate token transfer key {} (also at {})",
                    show_key(x.key()),
                    at(Table::TokenTransfers, j)
                ),
            ));
        }
    }

    // 2. Transactions are in a slot, with its blockhash.
    for (i, t) in chain.transactions.iter().enumerate() {
        let Some(&si) = slots.get(&t.slot) else {
            return Err(issue(
                Table::Transactions,
                i,
                format!("slot {} is not in slots", t.slot),
            ));
        };
        let slot_hash = &chain.slots[si].blockhash;
        if &t.blockhash != slot_hash {
            return Err(issue(
                Table::Transactions,
                i,
                format!(
                    "blockhash {} differs from the blockhash {slot_hash} of slot {}",
                    t.blockhash, t.slot
                ),
            ));
        }
    }

    // 3. Instructions are in a transaction.
    for (i, x) in chain.instructions.iter().enumerate() {
        if !txs.contains_key(&(x.slot, x.tx_signature.as_str())) {
            return Err(issue(
                Table::Instructions,
                i,
                format!(
                    "(slot, tx_signature) ({}, {}) is not in transactions",
                    x.slot, x.tx_signature
                ),
            ));
        }
    }

    // 4. Token transfers are an instruction.
    for (i, x) in chain.token_transfers.iter().enumerate() {
        if !ixs.contains_key(&x.key()) {
            return Err(issue(
                Table::TokenTransfers,
                i,
                format!("key {} is not in instructions", show_key(x.key())),
            ));
        }
    }

    // 5. transaction_count.
    let mut per_slot: BTreeMap<u64, u64> = BTreeMap::new();
    for t in &chain.transactions {
        *per_slot.entry(t.slot).or_default() += 1;
    }
    for (i, s) in chain.slots.iter().enumerate() {
        let n = per_slot.get(&s.slot).copied().unwrap_or(0);
        if u64::from(s.transaction_count) != n {
            return Err(issue(
                Table::Slots,
                i,
                format!(
                    "transaction_count is {} but slot {} has {n} transaction rows",
                    s.transaction_count, s.slot
                ),
            ));
        }
    }

    // 6. Parents.
    if let Some(&first) = slots.keys().next() {
        for (i, s) in chain.slots.iter().enumerate() {
            if s.parent_slot >= s.slot {
                return Err(issue(
                    Table::Slots,
                    i,
                    format!(
                        "parent_slot {} is not smaller than slot {}",
                        s.parent_slot, s.slot
                    ),
                ));
            }
            if s.parent_slot >= first && !slots.contains_key(&s.parent_slot) {
                return Err(issue(
                    Table::Slots,
                    i,
                    format!(
                        "parent_slot {} is not below the chain's first slot {first} and is \
                         not a slot of the chain",
                        s.parent_slot
                    ),
                ));
            }
        }
    }

    // 7. One decimals value per mint.
    let mut decimals: BTreeMap<&str, (u8, usize)> = BTreeMap::new();
    for (i, x) in chain.token_transfers.iter().enumerate() {
        if let (Some(mint), Some(d)) = (&x.mint_token_address, x.decimals) {
            match decimals.get(mint.as_str()) {
                Some(&(d0, j)) if d0 != d => {
                    return Err(issue(
                        Table::TokenTransfers,
                        i,
                        format!(
                            "mint {mint} has decimals {d} here but {d0} at {}",
                            at(Table::TokenTransfers, j)
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    decimals.insert(mint, (d, i));
                }
            }
        }
    }
    Ok(())
}

fn show_key((slot, sig, ix, inner): (u64, &str, u32, Option<u32>)) -> String {
    let inner = inner.map_or("null".to_string(), |n| n.to_string());
    format!("({slot}, {sig}, {ix}, {inner})")
}

// ---------------------------------------------------------------------------
// Model cases
// ---------------------------------------------------------------------------

/// Which chain a model case runs over.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChainRef {
    /// `fixtures/chains/<id>.json`.
    Explicit(String),
    /// Built by [`generate`](super::generate) on demand.
    Generated(GenSpec),
}

/// One cell of an expected row. No floats: expectations are exact.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cell {
    Null,
    Bool(bool),
    Int(i128),
    Str(String),
}

impl Cell {
    pub fn to_strict(&self) -> StrictValue {
        match self {
            Cell::Null => V::Null,
            Cell::Bool(b) => V::Bool(*b),
            Cell::Int(n) => V::Int(*n),
            Cell::Str(s) => V::Str(s.clone()),
        }
    }
}

/// What a model must output, exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelExpect {
    /// Unique, lowercase SQL identifiers, in output order.
    pub columns: Vec<String>,
    /// `true`: rows must come in this order. `false`: as a multiset (same
    /// rows, same number of times each, any order).
    pub ordered: bool,
    pub rows: Vec<Vec<Cell>>,
}

impl ModelExpect {
    /// `Ok` exactly when `columns` and `rows` are what this expects. The
    /// error names the first difference. No coercion: `1` is not `"1"`,
    /// and `0` is not `false`.
    pub fn check(&self, columns: &[String], rows: &[Vec<Cell>]) -> Result<(), String> {
        if columns != self.columns.as_slice() {
            return Err(format!(
                "columns are [{}], expected [{}]",
                echo(&columns.join(", ")),
                self.columns.join(", ")
            ));
        }
        let show = |r: &[Cell]| {
            echo(&super::canonical_string(&V::Array(
                r.iter().map(Cell::to_strict).collect(),
            )))
        };
        if self.ordered {
            for (i, (e, a)) in self.rows.iter().zip(rows).enumerate() {
                if e != a {
                    return Err(format!("row {i} is {}, expected {}", show(a), show(e)));
                }
            }
            if rows.len() != self.rows.len() {
                return Err(format!(
                    "{} rows, expected {}",
                    rows.len(),
                    self.rows.len()
                ));
            }
            return Ok(());
        }
        let count = |rs: &[Vec<Cell>]| {
            let mut m: BTreeMap<Vec<Cell>, usize> = BTreeMap::new();
            for r in rs {
                *m.entry(r.clone()).or_default() += 1;
            }
            m
        };
        let (want, got) = (count(&self.rows), count(rows));
        let keys: BTreeSet<&Vec<Cell>> = want.keys().chain(got.keys()).collect();
        for k in keys {
            let (w, g) = (
                want.get(k).copied().unwrap_or(0),
                got.get(k).copied().unwrap_or(0),
            );
            if w != g {
                return Err(format!(
                    "row {} appears {g} time(s), expected {w}",
                    show(k)
                ));
            }
        }
        Ok(())
    }
}

/// A validated model case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelCase {
    pub model: String,
    /// The `case` field, equal to the file name without `.json`.
    pub name: String,
    pub provenance: Provenance,
    pub chain: ChainRef,
    /// The window, both ends inclusive; `from_slot <= to_slot`.
    pub from_slot: u64,
    pub to_slot: u64,
    /// `None` for a case checked by invariants only; always `None` for a
    /// generated chain.
    pub expect: Option<ModelExpect>,
}

const MODEL_KEYS: [&str; 7] = [
    "schema_version",
    "model",
    "case",
    "provenance",
    "chain",
    "window",
    "expect",
];

/// Parse a model case file. Fields are checked in the order of the example
/// in the module docs, after `schema_version` and unknown keys.
pub fn parse_model_case(v: &StrictValue) -> Result<ModelCase, FieldIssue> {
    let o = Obj::of(v, "")?;
    o.schema_version()?;
    o.only(&MODEL_KEYS, "field")?;
    let model = want_name(o.req("model")?, "model")?.to_string();
    let name = want_name(o.req("case")?, "case")?.to_string();
    let provenance = parse_provenance(o.req("provenance")?, "provenance")?;
    let chain = parse_chain_ref(o.req("chain")?)?;

    let w = Obj::of(o.req("window")?, "window")?;
    w.only(&["from_slot", "to_slot"], "window field")?;
    let from_slot: u64 = w.int("from_slot", "a u64")?;
    let to_slot: u64 = w.int("to_slot", "a u64")?;
    if from_slot > to_slot {
        return Err(FieldIssue::new(
            "window.to_slot",
            format!("{to_slot} is below from_slot {from_slot}"),
        ));
    }

    let expect = o.opt("expect").map(parse_model_expect).transpose()?;
    if expect.is_some() && matches!(chain, ChainRef::Generated(_)) {
        return Err(FieldIssue::new(
            "expect",
            "is not allowed with a generated chain; expected rows only come with explicit chains",
        ));
    }
    Ok(ModelCase {
        model,
        name,
        provenance,
        chain,
        from_slot,
        to_slot,
        expect,
    })
}

fn parse_chain_ref(v: &StrictValue) -> Result<ChainRef, FieldIssue> {
    let o = Obj::of(v, "chain")?;
    o.only(&["explicit", "generated"], "chain kind")?;
    match (o.opt("explicit"), o.opt("generated")) {
        (Some(id), None) => Ok(ChainRef::Explicit(
            want_name(id, "chain.explicit")?.to_string(),
        )),
        (None, Some(g)) => parse_gen_spec(g).map(ChainRef::Generated),
        _ => Err(FieldIssue::new(
            "chain",
            "must have exactly one of 'explicit' or 'generated'",
        )),
    }
}

const GEN_KEYS: [&str; 8] = [
    "seed",
    "start_slot",
    "slots",
    "skip_percent",
    "max_tx_per_slot",
    "fail_percent",
    "transfer_percent",
    "mint_null_percent",
];

fn parse_gen_spec(v: &StrictValue) -> Result<GenSpec, FieldIssue> {
    let o = Obj::of(v, "chain.generated")?;
    o.only(&GEN_KEYS, "generator field")?;
    let spec = GenSpec {
        seed: o.int("seed", "a u64")?,
        start_slot: o.int("start_slot", "a u64")?,
        slots: o.int("slots", "a u32")?,
        skip_percent: o.int("skip_percent", "a u8")?,
        max_tx_per_slot: o.int("max_tx_per_slot", "a u32")?,
        fail_percent: o.int("fail_percent", "a u8")?,
        transfer_percent: o.int("transfer_percent", "a u8")?,
        mint_null_percent: o.int("mint_null_percent", "a u8")?,
    };
    spec.validate()?;
    Ok(spec)
}

/// A lowercase SQL identifier: `[a-z_][a-z0-9_]*`, at most 64 bytes.
fn check_column_name(s: &str) -> Result<(), String> {
    let ok = !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !s.as_bytes()[0].is_ascii_digit();
    if ok {
        Ok(())
    } else {
        Err(format!(
            "'{}' is not a column name (lowercase letters, digits and '_', not starting \
             with a digit, at most 64 characters)",
            echo(s)
        ))
    }
}

fn parse_model_expect(v: &StrictValue) -> Result<ModelExpect, FieldIssue> {
    let o = Obj::of(v, "expect")?;
    o.only(&["columns", "ordered", "rows"], "expect field")?;
    let mut columns = Vec::new();
    for (i, c) in o
        .array("columns", MAX_EXPECT_COLUMNS)?
        .iter()
        .enumerate()
    {
        let f = format!("expect.columns[{i}]");
        let s = want_str(c, &f)?;
        check_column_name(s).map_err(|r| FieldIssue::new(&f, r))?;
        if columns.iter().any(|x| x == s) {
            return Err(FieldIssue::new(&f, format!("duplicate column '{s}'")));
        }
        columns.push(s.to_string());
    }
    if columns.is_empty() {
        return Err(FieldIssue::new("expect.columns", "must name at least one column"));
    }
    let ordered = o.bool("ordered")?;
    let mut rows = Vec::new();
    for (i, r) in o.array("rows", MAX_EXPECT_ROWS)?.iter().enumerate() {
        let f = format!("expect.rows[{i}]");
        let cells = want_array(r, &f, MAX_EXPECT_COLUMNS)?;
        if cells.len() != columns.len() {
            return Err(FieldIssue::new(
                &f,
                format!("has {} cells but there are {} columns", cells.len(), columns.len()),
            ));
        }
        let row = cells
            .iter()
            .enumerate()
            .map(|(j, c)| match c {
                V::Null => Ok(Cell::Null),
                V::Bool(b) => Ok(Cell::Bool(*b)),
                V::Int(n) => Ok(Cell::Int(*n)),
                V::Str(s) => Ok(Cell::Str(s.clone())),
                other => Err(FieldIssue::new(
                    join_index(&f, j),
                    format!(
                        "must be null, a boolean, an integer or a string, not {}",
                        other.type_name()
                    ),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(ModelExpect {
        columns,
        ordered,
        rows,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::{ParseLimits, parse_strict};
    use super::*;

    fn sv(src: &str) -> StrictValue {
        parse_strict(src.as_bytes(), &ParseLimits::DEFAULT).unwrap_or_else(|e| panic!("{src}: {e}"))
    }

    fn key(n: u8) -> String {
        bs58::encode([n; 32]).into_string()
    }

    fn sig(n: u8) -> String {
        bs58::encode([n; 64]).into_string()
    }

    /// A small consistent chain: slots 100 and 101 (parent 99 is below the
    /// chain), one transaction each, one top-level and one inner
    /// instruction, one transfer.
    fn chain() -> Chain {
        let slot = |s: u64, parent: u64, h: u8| SlotRow {
            slot: s,
            blockhash: key(h),
            parent_slot: parent,
            block_time: Some(1_700_000_000),
            block_height: Some(s - 10),
            transaction_count: 1,
            leader: None,
        };
        let tx = |s: u64, h: u8, g: u8| TransactionRow {
            slot: s,
            blockhash: key(h),
            transaction_index: 0,
            tx_signature: sig(g),
            fee: 5000,
            status: TxStatus::Success,
            err: None,
            accounts: vec![key(1)],
            log_messages: vec!["Program log: hi".into()],
            compute_units_consumed: Some(150),
        };
        let ix = |inner: Option<u32>| InstructionRow {
            slot: 100,
            tx_signature: sig(7),
            instruction_index: 0,
            inner_instruction_index: inner,
            program_id: SPL_TOKEN_PROGRAM_ID.into(),
            accounts: vec![key(2), key(3), key(1)],
            data_base58: bs58::encode([3u8, 1, 0, 0, 0, 0, 0, 0, 0]).into_string(),
            instruction_type: Some("Transfer".into()),
            decoded_data: Some(sv(r#"{"amount":1}"#)),
        };
        Chain {
            slots: vec![slot(100, 99, 50), slot(101, 100, 51)],
            transactions: vec![tx(100, 50, 7), tx(101, 51, 8)],
            instructions: vec![ix(None), ix(Some(0))],
            token_transfers: vec![TokenTransferRow {
                slot: 100,
                tx_signature: sig(7),
                instruction_index: 0,
                inner_instruction_index: Some(0),
                spl_token_program_id: SPL_TOKEN_PROGRAM_ID.into(),
                source_token_account: key(2),
                destination_token_account: key(3),
                source_owner: None,
                destination_owner: None,
                mint_token_address: Some(key(9)),
                amount_raw: 1,
                decimals: Some(6),
                block_time: Some(1_700_000_000),
            }],
        }
    }

    fn chain_file(c: &Chain) -> StrictValue {
        let V::Object(mut m) = c.to_strict() else { unreachable!() };
        m.insert("schema_version".into(), V::Int(1));
        m.insert("id".into(), V::Str("small".into()));
        V::Object(m)
    }

    #[track_caller]
    fn inconsistent(c: &Chain, field: &str, part: &str) {
        let e = check_consistency(c).expect_err("must be inconsistent");
        assert_eq!(e.field, field, "{e}");
        assert!(e.reason.contains(part), "expected {part:?} in {e}");
    }

    #[test]
    fn column_tables_match_the_writer_layout() {
        let names: Vec<Vec<&str>> = Table::ALL.iter().map(|t| t.column_names()).collect();
        assert_eq!(names[0].len(), 7);
        assert_eq!(names[1].len(), 10);
        assert_eq!(names[2].len(), 9);
        assert_eq!(names[3].len(), 13);
        for t in Table::ALL {
            let mut n = t.column_names();
            n.sort();
            n.dedup();
            assert_eq!(n.len(), t.columns().len(), "{t:?} has unique columns");
        }
    }

    #[test]
    fn chain_round_trips_through_its_file_form() {
        let c = chain();
        assert_eq!(check_consistency(&c), Ok(()));
        let file = chain_file(&c);
        let (id, back) = parse_chain_file(&file).unwrap();
        assert_eq!(id, "small");
        assert_eq!(back, c);
        // And through canonical bytes.
        let bytes = canonical_bytes(&file);
        let (_, again) = parse_chain_file(&parse_strict(&bytes, &ParseLimits::DEFAULT).unwrap()).unwrap();
        assert_eq!(again, c);
        assert_eq!(again.digest(), c.digest());
    }

    #[test]
    fn json_columns_hold_what_prism_writes() {
        let c = chain();
        let t = c.transactions[0].to_strict();
        let V::Object(m) = &t else { unreachable!() };
        assert_eq!(
            super::super::canonical_string(&m["accounts"]),
            format!("[\"{}\"]", key(1))
        );
        assert_eq!(super::super::canonical_string(&m["log_messages"]), "[\"Program log: hi\"]");
        let x = c.instructions[0].to_strict();
        let V::Object(m) = &x else { unreachable!() };
        assert_eq!(super::super::canonical_string(&m["decoded_data"]), "{\"amount\":1}");
    }

    fn row_err(table: &str, row: usize, col: &str, value: &str) -> FieldIssue {
        let c = chain();
        let V::Object(mut m) = chain_file(&c) else { unreachable!() };
        let V::Array(rows) = m.get_mut(table).unwrap() else { unreachable!() };
        let V::Object(r) = &mut rows[row] else { unreachable!() };
        if value == "<remove>" {
            r.remove(col);
        } else {
            r.insert(col.into(), sv(value));
        }
        parse_chain_file(&V::Object(m)).expect_err(&format!("{table}.{col} = {value}"))
    }

    #[test]
    fn row_types_must_fit_their_columns() {
        for (table, col, value, part) in [
            ("slots", "slot", "-1", "does not fit in a u64"),
            ("slots", "slot", "18446744073709551616", "does not fit in a u64"),
            ("slots", "slot", "\"100\"", "must be an integer"),
            ("slots", "slot", "null", "must not be null"),
            ("slots", "transaction_count", "4294967296", "does not fit in a u32"),
            ("slots", "block_time", "9223372036854775808", "i64"),
            ("slots", "blockhash", "\"abc\"", "not a blockhash"),
            ("slots", "leader", "\"x\"", "not an address"),
            ("slots", "leader", "<remove>", "every column must be present"),
            ("slots", "is_finalized", "true", "unknown column 'is_finalized'"),
            ("slots", "previous_blockhash", "\"x\"", "unknown column"),
            ("transactions", "tx_signature", &format!("\"{}\"", key(1)), "decodes to 32 bytes, expected 64"),
            ("transactions", "status", "\"ok\"", "not a status"),
            ("transactions", "err", "\"boom\"", "status is 'success'"),
            ("transactions", "accounts", "\"[]\"", "must be an array"),
            ("transactions", "accounts", "[\"nope\"]", "not an address"),
            ("transactions", "log_messages", "[1]", "must be a string"),
            ("transactions", "compute_units_consumed", "true", "must be an integer"),
            ("instructions", "inner_instruction_index", "-1", "does not fit in a u32"),
            ("instructions", "data_base58", "\"0\"", "not valid base58"),
            ("instructions", "instruction_type", "\"\"", "must not be empty"),
            ("instructions", "instruction_type", "null", "only a decoded instruction"),
            ("token_transfers", "decimals", "256", "does not fit in a u8"),
            ("token_transfers", "spl_token_program_id", &format!("\"{}\"", key(4)), "neither SPL Token"),
            ("token_transfers", "amount_raw", "-5", "does not fit in a u64"),
        ] {
            let e = row_err(table, 0, col, value);
            assert!(e.field.starts_with(&format!("{table}[0]")), "{table}.{col}: {e}");
            assert!(e.reason.contains(part), "{table}.{col} = {value}: expected {part:?} in {e}");
        }
        // A failed transaction needs its error.
        let e = row_err("transactions", 1, "status", "\"failed\"");
        assert_eq!(e.field, "transactions[1].err");
    }

    #[test]
    fn chain_file_keys_and_row_limit() {
        let mut f = chain_file(&chain());
        let V::Object(m) = &mut f else { unreachable!() };
        m.remove("token_transfers");
        assert_eq!(parse_chain_file(&f).unwrap_err().field, "token_transfers");
        let mut f = chain_file(&chain());
        let V::Object(m) = &mut f else { unreachable!() };
        m.insert("previous_blockhash".into(), V::Null);
        assert!(parse_chain_file(&f).unwrap_err().reason.contains("unknown field"));
        let mut f = chain_file(&chain());
        let V::Object(m) = &mut f else { unreachable!() };
        let V::Array(slots) = m.get("slots").unwrap().clone() else { unreachable!() };
        m.insert("slots".into(), V::Array(vec![slots[0].clone(); MAX_ROWS_PER_TABLE + 1]));
        let e = parse_chain_file(&f).unwrap_err();
        assert!(e.reason.contains("more than the limit of 2000 rows per table"), "{e}");
    }

    #[test]
    fn every_consistency_rule() {
        let mut c = chain();
        c.slots.push(c.slots[0].clone());
        inconsistent(&c, "slots[2]", "duplicate slot 100 (also at slots[0])");

        let mut c = chain();
        c.transactions.push(c.transactions[0].clone());
        inconsistent(&c, "transactions[2]", "duplicate (slot, tx_signature)");

        // Same signature in two slots is two keys.
        let mut c = chain();
        c.transactions[1].tx_signature = sig(7);
        c.instructions.clear();
        c.token_transfers.clear();
        assert_eq!(check_consistency(&c), Ok(()));

        let mut c = chain();
        c.instructions.push(c.instructions[1].clone());
        inconsistent(&c, "instructions[2]", "duplicate instruction key (100, ");

        let mut c = chain();
        c.token_transfers.push(c.token_transfers[0].clone());
        inconsistent(&c, "token_transfers[1]", "duplicate token transfer key");

        let mut c = chain();
        c.transactions[1].slot = 102;
        inconsistent(&c, "transactions[1]", "slot 102 is not in slots");

        let mut c = chain();
        c.transactions[1].blockhash = key(50);
        inconsistent(&c, "transactions[1]", "differs from the blockhash");

        let mut c = chain();
        c.instructions[0].tx_signature = sig(8);
        inconsistent(&c, "instructions[0]", "is not in transactions");

        // Null inner index is its own value: the transfer points at inner 0,
        // so dropping the inner instruction orphans it even though the
        // top-level one is still there.
        let mut c = chain();
        c.instructions.truncate(1);
        inconsistent(&c, "token_transfers[0]", "key (100, ");
        let mut c = chain();
        c.token_transfers[0].inner_instruction_index = None;
        c.instructions.remove(0);
        inconsistent(&c, "token_transfers[0]", "null) is not in instructions");

        let mut c = chain();
        c.slots[0].transaction_count = 2;
        inconsistent(&c, "slots[0]", "transaction_count is 2 but slot 100 has 1");
        let mut c = chain();
        c.transactions.remove(1);
        inconsistent(&c, "slots[1]", "has 0 transaction rows");

        let mut c = chain();
        c.slots[1].parent_slot = 101;
        inconsistent(&c, "slots[1]", "not smaller than slot 101");
        let mut c = chain();
        c.slots[1].parent_slot = 102;
        inconsistent(&c, "slots[1]", "not smaller");
        let mut c = chain();
        c.slots[1].slot = 103;
        c.slots[1].parent_slot = 102;
        c.transactions[1].slot = 103;
        inconsistent(&c, "slots[1]", "not a slot of the chain");
        // A skipped slot is fine: the parent is the last produced slot.
        let mut c = chain();
        c.slots[1].slot = 103;
        c.transactions[1].slot = 103;
        assert_eq!(check_consistency(&c), Ok(()));
        // Any parent below the first slot is fine.
        let mut c = chain();
        c.slots[0].parent_slot = 0;
        assert_eq!(check_consistency(&c), Ok(()));

        let mut c = chain();
        let mut t = c.token_transfers[0].clone();
        t.inner_instruction_index = None;
        t.decimals = Some(9);
        c.token_transfers.push(t);
        inconsistent(&c, "token_transfers[1]", "has decimals 9 here but 6 at token_transfers[0]");
        // No mint, or no decimals, is never a conflict.
        let mut c = chain();
        let mut t = c.token_transfers[0].clone();
        t.inner_instruction_index = None;
        t.decimals = None;
        c.token_transfers.push(t);
        assert_eq!(check_consistency(&c), Ok(()));

        assert_eq!(check_consistency(&Chain::default()), Ok(()), "an empty chain is consistent");
    }

    #[test]
    fn rules_apply_in_a_fixed_order() {
        // Breaks rules 1 (duplicate slot) and 5 (count): 1 wins.
        let mut c = chain();
        c.slots.push(c.slots[0].clone());
        c.slots[0].transaction_count = 9;
        inconsistent(&c, "slots[2]", "duplicate");
        // Breaks 3 and 6: 3 wins.
        let mut c = chain();
        c.slots[1].parent_slot = 500;
        c.instructions[0].slot = 101;
        inconsistent(&c, "instructions[0]", "not in transactions");
    }

    // -- Model cases ----------------------------------------------------------

    fn model_src(chain: &str, expect: Option<&str>) -> String {
        let expect = expect.map_or(String::new(), |e| format!(",\n\"expect\": {e}"));
        format!(
            r#"{{"schema_version": 1, "model": "daily-active-programs", "case": "two-mints",
"provenance": {{"kind": "hand_computed", "reference": "worked on paper", "note": "counts verified by hand"}},
"chain": {chain},
"window": {{"from_slot": 100, "to_slot": 120}}{expect}}}"#
        )
    }

    const EXPECT: &str = r#"{"columns": ["program_id", "n"], "ordered": false, "rows": [["p", 3], [null, -1]]}"#;
    const GEN: &str = r#"{"generated": {"seed": 7, "start_slot": 1000, "slots": 500, "skip_percent": 5,
        "max_tx_per_slot": 8, "fail_percent": 10, "transfer_percent": 30, "mint_null_percent": 20}}"#;

    fn model(src: &str) -> Result<ModelCase, FieldIssue> {
        parse_model_case(&sv(src))
    }

    #[test]
    fn parses_both_model_case_examples() {
        let c = model(&model_src(r#"{"explicit": "two-mints-one-failed"}"#, Some(EXPECT))).unwrap();
        assert_eq!(c.model, "daily-active-programs");
        assert_eq!(c.name, "two-mints");
        assert_eq!(c.chain, ChainRef::Explicit("two-mints-one-failed".into()));
        assert_eq!((c.from_slot, c.to_slot), (100, 120));
        let e = c.expect.unwrap();
        assert_eq!(e.columns, ["program_id", "n"]);
        assert!(!e.ordered);
        assert_eq!(e.rows, vec![
            vec![Cell::Str("p".into()), Cell::Int(3)],
            vec![Cell::Null, Cell::Int(-1)],
        ]);

        let c = model(&model_src(GEN, None)).unwrap();
        assert_eq!(
            c.chain,
            ChainRef::Generated(GenSpec {
                seed: 7,
                start_slot: 1000,
                slots: 500,
                skip_percent: 5,
                max_tx_per_slot: 8,
                fail_percent: 10,
                transfer_percent: 30,
                mint_null_percent: 20,
            })
        );
        assert_eq!(c.expect, None);
        // An explicit chain without expect is fine (invariants only).
        assert!(model(&model_src(r#"{"explicit": "x"}"#, None)).is_ok());
    }

    #[test]
    fn expected_rows_only_come_with_explicit_chains() {
        let e = model(&model_src(GEN, Some(EXPECT))).unwrap_err();
        assert_eq!(e.field, "expect");
        assert!(e.reason.contains("only come with explicit chains"), "{e}");
    }

    #[test]
    fn model_case_field_rules() {
        let ex = r#"{"explicit": "c"}"#;
        for (src, field, part) in [
            (model_src(r#"{}"#, None), "chain", "exactly one"),
            (model_src(r#"{"explicit": "a", "generated": {}}"#, None), "chain", "exactly one"),
            (model_src(r#"{"file": "a"}"#, None), "chain", "unknown chain kind 'file'"),
            (model_src(r#"{"explicit": "A"}"#, None), "chain.explicit", "only a-z"),
            (model_src(&GEN.replace("\"seed\": 7, ", ""), None), "chain.generated.seed", "missing"),
            (model_src(&GEN.replace("\"seed\": 7", "\"seed\": -7"), None), "chain.generated.seed", "u64"),
            (model_src(&GEN.replace("\"skip_percent\": 5", "\"skip_percent\": 101"), None), "chain.generated.skip_percent", "0 to 100"),
            (model_src(&GEN.replace("\"slots\": 500", "\"slots\": 0"), None), "chain.generated.slots", "at least 1"),
            (model_src(&GEN.replace("8, \"fail", "8, \"extra\": 1, \"fail"), None), "chain.generated", "unknown generator field 'extra'"),
            (model_src(ex, None).replace("\"to_slot\": 120", "\"to_slot\": 99"), "window.to_slot", "below from_slot"),
            (model_src(ex, None).replace("\"from_slot\": 100, ", ""), "window.from_slot", "missing"),
            (model_src(ex, Some(r#"{"columns": [], "ordered": true, "rows": []}"#)), "expect.columns", "at least one"),
            (model_src(ex, Some(r#"{"columns": ["a", "a"], "ordered": true, "rows": []}"#)), "expect.columns[1]", "duplicate column"),
            (model_src(ex, Some(r#"{"columns": ["N"], "ordered": true, "rows": []}"#)), "expect.columns[0]", "not a column name"),
            (model_src(ex, Some(r#"{"columns": ["count(*)"], "ordered": true, "rows": []}"#)), "expect.columns[0]", "not a column name"),
            (model_src(ex, Some(r#"{"columns": ["a"], "ordered": "yes", "rows": []}"#)), "expect.ordered", "true or false"),
            (model_src(ex, Some(r#"{"columns": ["a"], "ordered": true, "rows": [[1, 2]]}"#)), "expect.rows[0]", "2 cells but there are 1 columns"),
            (model_src(ex, Some(r#"{"columns": ["a"], "ordered": true, "rows": [[[1]]]}"#)), "expect.rows[0][0]", "not an array"),
            (model_src(ex, Some(r#"{"columns": ["a"], "ordered": true, "rows": [[{}]]}"#)), "expect.rows[0][0]", "not an object"),
            (model_src(ex, Some(r#"{"columns": ["a"], "ordered": true, "rows": [], "tolerance": 0}"#)), "expect", "unknown expect field 'tolerance'"),
            (model_src(ex, None).replace("\"case\": \"two-mints\"", "\"case\": \"two_mints\""), "case", "only a-z"),
        ] {
            let e = model(&src).expect_err(&src);
            assert_eq!(e.field, field, "{src}\n{e}");
            assert!(e.reason.contains(part), "{src}\nexpected {part:?} in {e}");
        }
        let many = format!(
            r#"{{"columns": ["a"], "ordered": true, "rows": [{}]}}"#,
            vec!["[1]"; MAX_EXPECT_ROWS + 1].join(",")
        );
        let e = model(&model_src(ex, Some(&many))).unwrap_err();
        assert_eq!(e.field, "expect.rows");
    }

    #[test]
    fn expectations_compare_exactly() {
        let cols = |c: &[&str]| c.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        let e = ModelExpect {
            columns: cols(&["k", "n"]),
            ordered: false,
            rows: vec![
                vec![Cell::Str("a".into()), Cell::Int(1)],
                vec![Cell::Str("a".into()), Cell::Int(1)],
                vec![Cell::Null, Cell::Int(2)],
            ],
        };
        let c = cols(&["k", "n"]);
        // Any order, same multiset.
        let mut rows = e.rows.clone();
        rows.reverse();
        assert_eq!(e.check(&c, &rows), Ok(()));
        // Multiplicity counts.
        let err = e.check(&c, &e.rows[1..]).unwrap_err();
        assert!(err.contains("row [\"a\",1] appears 1 time(s), expected 2"), "{err}");
        // The first difference is the smallest differing row, whatever
        // the input order.
        let err = e.check(&c, &rows[1..]).unwrap_err();
        assert_eq!(err, "row [null,2] appears 0 time(s), expected 1");
        // No coercion.
        let mut r = e.rows.clone();
        r[2][1] = Cell::Str("2".into());
        assert!(e.check(&c, &r).is_err());
        let mut r = e.rows.clone();
        r[2][0] = Cell::Bool(false);
        assert!(e.check(&c, &r).is_err());
        // Columns are exact, order included.
        assert!(e.check(&cols(&["n", "k"]), &e.rows).unwrap_err().contains("columns are"));
        assert!(e.check(&cols(&["k"]), &e.rows).is_err());

        let o = ModelExpect { ordered: true, ..e.clone() };
        assert_eq!(o.check(&c, &o.rows), Ok(()));
        let err = o.check(&c, &rows).unwrap_err();
        assert!(err.starts_with("row 0 is"), "{err}");
        let err = o.check(&c, &o.rows[..2]).unwrap_err();
        assert_eq!(err, "2 rows, expected 3");
        let mut more = o.rows.clone();
        more.push(vec![Cell::Null, Cell::Null]);
        assert_eq!(o.check(&c, &more).unwrap_err(), "4 rows, expected 3");
    }
}
