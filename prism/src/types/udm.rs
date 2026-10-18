//! Universal Data Model (UDM): the Solana-shaped row contracts that every
//! downstream stage (normalizer, decoder, storage, query) agrees on.
//!
//! Each `DeepAna*` struct is **one row** in one Parquet table. The Rust types
//! are the in-memory form; [`UdmRow`] defines how a batch of rows becomes an
//! Arrow [`RecordBatch`] with a fixed [`Schema`], which is exactly what lands
//! on disk. The two representations are kept in sync by the tests at the
//! bottom of this file.
//!
//! Storage conventions shared by every table:
//! * `slot` is the partition key and every table carries it.
//! * Base58 strings are used for pubkeys, signatures and blockhashes.
//! * `block_time` is a UTC timestamp with second precision (Solana's native
//!   resolution) and is nullable, because old slots may not have one.
//! * List-shaped columns (`accounts`, `log_messages`) and free-form decoded
//!   payloads (`decoded_data`) are stored as serialized JSON strings
//!   (`Utf8`) rather than Arrow lists, so they survive any Parquet reader.

use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, StringArray, TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Time zone label used for every `block_time` column.
pub const UDM_TIMEZONE: &str = "UTC";

fn block_time_type() -> DataType {
    DataType::Timestamp(TimeUnit::Second, Some(UDM_TIMEZONE.into()))
}

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A row type in the Universal Data Model.
///
/// Implementors describe their own Parquet table: its name, Arrow schema and
/// how a slice of rows is columnarised into a [`RecordBatch`].
pub trait UdmRow: Sized + Clone + fmt::Debug + Serialize + for<'de> Deserialize<'de> {
    /// Table name as it appears in storage and in SQL.
    const TABLE_NAME: &'static str;

    /// The fixed Arrow schema for this table. Shared across calls.
    fn schema() -> SchemaRef;

    /// Slot the row belongs to. Used as the partition key.
    fn slot(&self) -> u64;

    /// Columnarise `rows` into a single [`RecordBatch`] matching [`Self::schema`].
    ///
    /// An empty slice yields an empty batch with the correct schema.
    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError>;
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Encode a list-shaped column as a JSON string for storage.
///
/// Serialising a `Vec<String>` cannot fail, so this never panics in practice.
pub fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("UDM list columns are always JSON-serialisable")
}

/// Decode a JSON-string column back into its typed form.
pub fn from_json_string<T: for<'de> Deserialize<'de>>(raw: &str) -> Result<T, serde_json::Error> {
    serde_json::from_str(raw)
}

/// Convert a Unix timestamp in seconds to a `chrono` UTC datetime.
pub fn unix_to_datetime(secs: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(secs, 0)
}

fn utf8_col<'a>(it: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(it))
}

fn utf8_opt_col<'a>(it: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(it.collect::<StringArray>())
}

fn u64_col(it: impl Iterator<Item = u64>) -> ArrayRef {
    Arc::new(UInt64Array::from_iter_values(it))
}

fn u64_opt_col(it: impl Iterator<Item = Option<u64>>) -> ArrayRef {
    Arc::new(it.collect::<UInt64Array>())
}

fn u32_col(it: impl Iterator<Item = u32>) -> ArrayRef {
    Arc::new(UInt32Array::from_iter_values(it))
}

fn u32_opt_col(it: impl Iterator<Item = Option<u32>>) -> ArrayRef {
    Arc::new(it.collect::<UInt32Array>())
}

fn u8_col(it: impl Iterator<Item = u8>) -> ArrayRef {
    Arc::new(UInt8Array::from_iter_values(it))
}

fn block_time_col(it: impl Iterator<Item = Option<i64>>) -> ArrayRef {
    Arc::new(it.collect::<TimestampSecondArray>().with_timezone(UDM_TIMEZONE))
}

// ---------------------------------------------------------------------------
// DeepAnaSlot
// ---------------------------------------------------------------------------

/// One row per confirmed slot (block).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeepAnaSlot {
    /// Slot number. Primary key and partition key.
    pub slot: u64,
    /// Base58 blockhash of this slot.
    pub blockhash: String,
    /// Slot of the parent block.
    pub parent_slot: u64,
    /// Estimated production time, Unix seconds (UTC). `None` for very old slots.
    pub block_time: Option<i64>,
    /// Block height (number of ancestor blocks). `None` when the RPC omits it.
    pub block_height: Option<u64>,
    /// Number of transactions in the block.
    pub transaction_count: u32,
    /// Base58 pubkey of the validator that produced the block, when known.
    pub leader: Option<String>,
}

impl DeepAnaSlot {
    /// `block_time` as a `chrono` UTC datetime.
    pub fn block_datetime(&self) -> Option<DateTime<Utc>> {
        self.block_time.and_then(unix_to_datetime)
    }

    /// True for the genesis slot, whose parent is itself.
    pub fn is_genesis(&self) -> bool {
        self.slot == 0 && self.parent_slot == 0
    }
}

static SLOT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

impl UdmRow for DeepAnaSlot {
    const TABLE_NAME: &'static str = "solana_slots";

    fn schema() -> SchemaRef {
        SLOT_SCHEMA.clone()
    }

    fn slot(&self) -> u64 {
        self.slot
    }

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                u64_col(rows.iter().map(|r| r.slot)),
                utf8_col(rows.iter().map(|r| r.blockhash.as_str())),
                u64_col(rows.iter().map(|r| r.parent_slot)),
                block_time_col(rows.iter().map(|r| r.block_time)),
                u64_opt_col(rows.iter().map(|r| r.block_height)),
                u32_col(rows.iter().map(|r| r.transaction_count)),
                utf8_opt_col(rows.iter().map(|r| r.leader.as_deref())),
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// DeepAnaTransaction
// ---------------------------------------------------------------------------

/// Execution outcome of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionStatus {
    #[default]
    Success,
    Failed,
}

impl TransactionStatus {
    /// Storage representation (`"success"` / `"failed"`).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub const fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }

    /// Derive the status from the presence of an error.
    pub fn from_err(err: Option<&str>) -> Self {
        if err.is_some() { Self::Failed } else { Self::Success }
    }
}

impl fmt::Display for TransactionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TransactionStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown transaction status: {other:?}")),
        }
    }
}

/// One row per transaction within a slot.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeepAnaTransaction {
    /// Slot containing the transaction. Partition key.
    pub slot: u64,
    /// Base58 blockhash of the containing slot.
    pub blockhash: String,
    /// Position of the transaction within the block. Together with `slot`
    /// this gives a total ordering of transactions.
    pub transaction_index: u32,
    /// Base58 first signature; the transaction's identifier.
    pub tx_signature: String,
    /// Fee paid, in lamports.
    pub fee: u64,
    /// Success / failed.
    pub status: TransactionStatus,
    /// Error description when `status` is `Failed`, otherwise `None`.
    pub err: Option<String>,
    /// Base58 pubkeys of every account the transaction touched, in
    /// message order (static keys followed by loaded address-table keys).
    /// Stored as a JSON array string in Parquet.
    pub accounts: Vec<String>,
    /// Program log lines. Stored as a JSON array string in Parquet.
    pub log_messages: Vec<String>,
    /// Compute units consumed, when the RPC reports them.
    pub compute_units_consumed: Option<u64>,
}

impl DeepAnaTransaction {
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// The fee payer, which is always the first account.
    pub fn fee_payer(&self) -> Option<&str> {
        self.accounts.first().map(String::as_str)
    }

    /// `accounts` encoded as it is stored in Parquet.
    pub fn accounts_json(&self) -> String {
        to_json_string(&self.accounts)
    }

    /// `log_messages` encoded as it is stored in Parquet.
    pub fn log_messages_json(&self) -> String {
        to_json_string(&self.log_messages)
    }
}

static TRANSACTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

impl UdmRow for DeepAnaTransaction {
    const TABLE_NAME: &'static str = "solana_transactions";

    fn schema() -> SchemaRef {
        TRANSACTION_SCHEMA.clone()
    }

    fn slot(&self) -> u64 {
        self.slot
    }

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        let accounts: Vec<String> = rows.iter().map(Self::accounts_json).collect();
        let logs: Vec<String> = rows.iter().map(Self::log_messages_json).collect();
        RecordBatch::try_new(
            Self::schema(),
            vec![
                u64_col(rows.iter().map(|r| r.slot)),
                utf8_col(rows.iter().map(|r| r.blockhash.as_str())),
                u32_col(rows.iter().map(|r| r.transaction_index)),
                utf8_col(rows.iter().map(|r| r.tx_signature.as_str())),
                u64_col(rows.iter().map(|r| r.fee)),
                utf8_col(rows.iter().map(|r| r.status.as_str())),
                utf8_opt_col(rows.iter().map(|r| r.err.as_deref())),
                utf8_col(accounts.iter().map(String::as_str)),
                utf8_col(logs.iter().map(String::as_str)),
                u64_opt_col(rows.iter().map(|r| r.compute_units_consumed)),
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// DeepAnaInstruction
// ---------------------------------------------------------------------------

/// One row per instruction (top-level or inner/CPI) within a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeepAnaInstruction {
    /// Slot containing the transaction. Partition key.
    pub slot: u64,
    /// Base58 signature of the containing transaction.
    pub tx_signature: String,
    /// Index of the top-level instruction in the transaction message.
    pub instruction_index: u32,
    /// Index within the inner-instruction list of `instruction_index`.
    /// `None` means this row *is* the top-level instruction.
    pub inner_instruction_index: Option<u32>,
    /// Base58 pubkey of the invoked program.
    pub program_id: String,
    /// Base58 pubkeys passed to the instruction, in order.
    /// Stored as a JSON array string in Parquet.
    pub accounts: Vec<String>,
    /// Raw instruction data, base58 encoded.
    pub data_base58: String,
    /// Decoder-assigned type name (e.g. `"transfer"`), when decoded.
    pub instruction_type: Option<String>,
    /// Decoder output. Stored as a JSON string in Parquet.
    pub decoded_data: Option<serde_json::Value>,
}

impl DeepAnaInstruction {
    pub fn is_top_level(&self) -> bool {
        self.inner_instruction_index.is_none()
    }

    pub fn is_inner(&self) -> bool {
        self.inner_instruction_index.is_some()
    }

    pub fn is_decoded(&self) -> bool {
        self.instruction_type.is_some()
    }

    /// Raw instruction data decoded from base58.
    pub fn data_bytes(&self) -> Result<Vec<u8>, bs58::decode::Error> {
        bs58::decode(&self.data_base58).into_vec()
    }

    /// `accounts` encoded as it is stored in Parquet.
    pub fn accounts_json(&self) -> String {
        to_json_string(&self.accounts)
    }

    /// `decoded_data` encoded as it is stored in Parquet.
    pub fn decoded_data_json(&self) -> Option<String> {
        self.decoded_data.as_ref().map(to_json_string)
    }
}

static INSTRUCTION_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

impl UdmRow for DeepAnaInstruction {
    const TABLE_NAME: &'static str = "solana_instructions";

    fn schema() -> SchemaRef {
        INSTRUCTION_SCHEMA.clone()
    }

    fn slot(&self) -> u64 {
        self.slot
    }

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        let accounts: Vec<String> = rows.iter().map(Self::accounts_json).collect();
        let decoded: Vec<Option<String>> = rows.iter().map(Self::decoded_data_json).collect();
        RecordBatch::try_new(
            Self::schema(),
            vec![
                u64_col(rows.iter().map(|r| r.slot)),
                utf8_col(rows.iter().map(|r| r.tx_signature.as_str())),
                u32_col(rows.iter().map(|r| r.instruction_index)),
                u32_opt_col(rows.iter().map(|r| r.inner_instruction_index)),
                utf8_col(rows.iter().map(|r| r.program_id.as_str())),
                utf8_col(accounts.iter().map(String::as_str)),
                utf8_col(rows.iter().map(|r| r.data_base58.as_str())),
                utf8_opt_col(rows.iter().map(|r| r.instruction_type.as_deref())),
                utf8_opt_col(decoded.iter().map(Option::as_deref)),
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// DeepAnaTokenTransfer
// ---------------------------------------------------------------------------

/// One row per decoded SPL Token (or Token-2022) transfer.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct DeepAnaTokenTransfer {
    /// Slot containing the transaction. Partition key.
    pub slot: u64,
    /// Base58 signature of the containing transaction.
    pub tx_signature: String,
    /// Top-level instruction index of the transfer.
    pub instruction_index: u32,
    /// Inner instruction index, `None` if the transfer was top-level.
    pub inner_instruction_index: Option<u32>,
    /// Base58 program id: SPL Token or Token-2022.
    pub spl_token_program_id: String,
    /// Base58 source token account.
    pub source_token_account: String,
    /// Base58 destination token account.
    pub destination_token_account: String,
    /// Base58 owner of the source token account, when resolved.
    pub source_owner: Option<String>,
    /// Base58 owner of the destination token account, when resolved.
    pub destination_owner: Option<String>,
    /// Base58 mint. `None` when the account could not be resolved
    /// (e.g. a plain `Transfer` whose token account was later closed).
    pub mint_token_address: Option<String>,
    /// Amount in base units (before applying `decimals`).
    pub amount_raw: u64,
    /// Mint decimals, when known.
    pub decimals: Option<u8>,
    /// Block time of the containing slot, Unix seconds (UTC).
    pub block_time: Option<i64>,
}

impl DeepAnaTokenTransfer {
    /// Human-readable amount, i.e. `amount_raw / 10^decimals`.
    /// `None` when `decimals` is unknown.
    pub fn amount_ui(&self) -> Option<f64> {
        self.decimals
            .map(|d| self.amount_raw as f64 / 10f64.powi(i32::from(d)))
    }

    pub fn block_datetime(&self) -> Option<DateTime<Utc>> {
        self.block_time.and_then(unix_to_datetime)
    }

    pub fn is_token_2022(&self) -> bool {
        self.spl_token_program_id == spl_token_2022::id().to_string()
    }

    /// A self-transfer between two accounts of the same owner.
    pub fn is_self_transfer(&self) -> bool {
        match (&self.source_owner, &self.destination_owner) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
}

static TOKEN_TRANSFER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
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

impl UdmRow for DeepAnaTokenTransfer {
    const TABLE_NAME: &'static str = "solana_token_transfers";

    fn schema() -> SchemaRef {
        TOKEN_TRANSFER_SCHEMA.clone()
    }

    fn slot(&self) -> u64 {
        self.slot
    }

    fn to_record_batch(rows: &[Self]) -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                u64_col(rows.iter().map(|r| r.slot)),
                utf8_col(rows.iter().map(|r| r.tx_signature.as_str())),
                u32_col(rows.iter().map(|r| r.instruction_index)),
                u32_opt_col(rows.iter().map(|r| r.inner_instruction_index)),
                utf8_col(rows.iter().map(|r| r.spl_token_program_id.as_str())),
                utf8_col(rows.iter().map(|r| r.source_token_account.as_str())),
                utf8_col(rows.iter().map(|r| r.destination_token_account.as_str())),
                utf8_opt_col(rows.iter().map(|r| r.source_owner.as_deref())),
                utf8_opt_col(rows.iter().map(|r| r.destination_owner.as_deref())),
                utf8_opt_col(rows.iter().map(|r| r.mint_token_address.as_deref())),
                u64_col(rows.iter().map(|r| r.amount_raw)),
                Arc::new(rows.iter().map(|r| r.decimals).collect::<UInt8Array>()),
                block_time_col(rows.iter().map(|r| r.block_time)),
            ],
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{TimestampSecondType, UInt32Type, UInt64Type, UInt8Type};

    fn slot_row() -> DeepAnaSlot {
        DeepAnaSlot {
            slot: 250_000_000,
            blockhash: "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".into(),
            parent_slot: 249_999_999,
            block_time: Some(1_700_000_000),
            block_height: Some(230_000_000),
            transaction_count: 1_234,
            leader: Some("DeepAnaLeader1111111111111111111111111111111".into()),
        }
    }

    fn tx_row() -> DeepAnaTransaction {
        DeepAnaTransaction {
            slot: 250_000_000,
            blockhash: "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".into(),
            transaction_index: 7,
            tx_signature: "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW".into(),
            fee: 5_000,
            status: TransactionStatus::Failed,
            err: Some("InstructionError(0, Custom(1))".into()),
            accounts: vec!["payer".into(), "programX".into()],
            log_messages: vec!["Program log: hi".into()],
            compute_units_consumed: Some(1_500),
        }
    }

    fn ix_row() -> DeepAnaInstruction {
        DeepAnaInstruction {
            slot: 250_000_000,
            tx_signature: "sig".into(),
            instruction_index: 2,
            inner_instruction_index: Some(0),
            program_id: spl_token::id().to_string(),
            accounts: vec!["src".into(), "dst".into(), "owner".into()],
            data_base58: bs58::encode([3u8, 1, 0, 0, 0, 0, 0, 0, 0]).into_string(),
            instruction_type: Some("transfer".into()),
            decoded_data: Some(serde_json::json!({ "amount": 1 })),
        }
    }

    fn transfer_row() -> DeepAnaTokenTransfer {
        DeepAnaTokenTransfer {
            slot: 250_000_000,
            tx_signature: "sig".into(),
            instruction_index: 2,
            inner_instruction_index: None,
            spl_token_program_id: spl_token_2022::id().to_string(),
            source_token_account: "src".into(),
            destination_token_account: "dst".into(),
            source_owner: Some("alice".into()),
            destination_owner: Some("alice".into()),
            mint_token_address: Some("USDC".into()),
            amount_raw: 1_500_000,
            decimals: Some(6),
            block_time: Some(1_700_000_000),
        }
    }

    #[test]
    fn slot_batch_matches_schema() {
        let batch = DeepAnaSlot::to_record_batch(&[slot_row(), DeepAnaSlot::default()]).unwrap();
        assert_eq!(batch.schema(), DeepAnaSlot::schema());
        assert_eq!(batch.num_rows(), 2);
        let bt = batch.column(3).as_primitive::<TimestampSecondType>();
        assert_eq!(bt.value(0), 1_700_000_000);
        assert!(bt.is_null(1));
        assert_eq!(
            batch.schema().field(3).data_type(),
            &DataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
        );
        assert_eq!(slot_row().block_datetime().unwrap().timestamp(), 1_700_000_000);
    }

    #[test]
    fn transaction_batch_encodes_lists_as_json() {
        let batch = DeepAnaTransaction::to_record_batch(&[tx_row()]).unwrap();
        assert_eq!(batch.schema(), DeepAnaTransaction::schema());
        let accounts = batch.column(7).as_string::<i32>();
        assert_eq!(accounts.value(0), r#"["payer","programX"]"#);
        let logs = batch.column(8).as_string::<i32>();
        assert_eq!(logs.value(0), r#"["Program log: hi"]"#);
        let status = batch.column(5).as_string::<i32>();
        assert_eq!(status.value(0), "failed");
        let decoded: Vec<String> = from_json_string(accounts.value(0)).unwrap();
        assert_eq!(decoded, tx_row().accounts);
        assert_eq!(tx_row().fee_payer(), Some("payer"));
    }

    #[test]
    fn transaction_status_round_trips() {
        for s in [TransactionStatus::Success, TransactionStatus::Failed] {
            assert_eq!(s.to_string().parse::<TransactionStatus>().unwrap(), s);
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(json, format!("\"{s}\""));
        }
        assert!("bogus".parse::<TransactionStatus>().is_err());
        assert_eq!(TransactionStatus::from_err(None), TransactionStatus::Success);
        assert_eq!(TransactionStatus::from_err(Some("x")), TransactionStatus::Failed);
    }

    #[test]
    fn instruction_batch_and_helpers() {
        let row = ix_row();
        let batch = DeepAnaInstruction::to_record_batch(&[row.clone()]).unwrap();
        assert_eq!(batch.schema(), DeepAnaInstruction::schema());
        let inner = batch.column(3).as_primitive::<UInt32Type>();
        assert_eq!(inner.value(0), 0);
        let decoded = batch.column(8).as_string::<i32>();
        assert_eq!(decoded.value(0), r#"{"amount":1}"#);
        assert!(row.is_inner() && !row.is_top_level() && row.is_decoded());
        assert_eq!(row.data_bytes().unwrap(), vec![3u8, 1, 0, 0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn token_transfer_batch_and_helpers() {
        let row = transfer_row();
        let batch = DeepAnaTokenTransfer::to_record_batch(&[row.clone()]).unwrap();
        assert_eq!(batch.schema(), DeepAnaTokenTransfer::schema());
        assert_eq!(batch.column(10).as_primitive::<UInt64Type>().value(0), 1_500_000);
        assert_eq!(batch.column(11).as_primitive::<UInt8Type>().value(0), 6);
        assert!(batch.column(3).is_null(0));
        assert_eq!(row.amount_ui(), Some(1.5));
        assert!(row.is_token_2022());
        assert!(row.is_self_transfer());
    }

    #[test]
    fn empty_batches_keep_schema() {
        assert_eq!(DeepAnaSlot::to_record_batch(&[]).unwrap().num_rows(), 0);
        assert_eq!(DeepAnaTransaction::to_record_batch(&[]).unwrap().num_rows(), 0);
        assert_eq!(DeepAnaInstruction::to_record_batch(&[]).unwrap().num_rows(), 0);
        let b = DeepAnaTokenTransfer::to_record_batch(&[]).unwrap();
        assert_eq!(b.num_rows(), 0);
        assert_eq!(b.schema(), DeepAnaTokenTransfer::schema());
    }

    #[test]
    fn rows_round_trip_through_serde() {
        let tx = tx_row();
        let back: DeepAnaTransaction = serde_json::from_str(&serde_json::to_string(&tx).unwrap()).unwrap();
        assert_eq!(back, tx);
        let ix = ix_row();
        let back: DeepAnaInstruction = serde_json::from_str(&serde_json::to_string(&ix).unwrap()).unwrap();
        assert_eq!(back, ix);
    }

    #[test]
    fn table_names_are_distinct() {
        let names = [
            DeepAnaSlot::TABLE_NAME,
            DeepAnaTransaction::TABLE_NAME,
            DeepAnaInstruction::TABLE_NAME,
            DeepAnaTokenTransfer::TABLE_NAME,
        ];
        let set: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(set.len(), names.len());
    }
}
