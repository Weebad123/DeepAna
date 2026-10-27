//! Tests for `prism::storage`.
//!
//! Files are written under a fresh directory in the system temp dir and read
//! back with the `parquet` crate's own Arrow reader. The rows are rebuilt
//! from the Arrow columns by name, with code that shares nothing with
//! `UdmRow::to_record_batch`, so a round trip that comes back equal means
//! the file holds exactly what was buffered.
//!
//! `flushed_parquet_files_match_the_buffer_they_were_written_from` is the
//! consistency check: a known buffer, every nullable column both set and
//! unset, flushed and compared row for row, then queried (projection, filter,
//! aggregates, a join, row-group statistics) with answers computed from the
//! buffer.
//!
//! The `live_*` tests push recent Solana devnet blocks through the normalizer,
//! the SPL Token decoder and the account resolver into the writer. They need
//! `HELIUS_API_KEY` and are skipped (with a note) without it:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test storage_test live_ -- --nocapture --test-threads=1
//! ```

use std::collections::HashSet;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, AsArray, BooleanArray, RecordBatch, Scalar, StringArray, UInt64Array};
use arrow::compute::kernels::cmp::{eq, gt_eq, lt_eq};
use arrow::compute::{and, concat_batches, filter_record_batch, sum};
use arrow::datatypes::{
    DataType, TimeUnit, TimestampSecondType, UInt8Type, UInt32Type, UInt64Type,
};
use chrono::NaiveDate;
use helius::types::Cluster;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::statistics::Statistics;
use prism::account_resolver::AccountResolver;
use prism::adapter::{ChainAdapter, CommitmentLevel};
use prism::decoder::DecoderRegistry;
use prism::ingestor::{HELIUS_API_KEY_ENV, helius_api_key_from_env, helius_solana_adapter};
use prism::normalizer::Normalizer;
use prism::storage::partition::CHAIN_DIR;
use prism::storage::writer::{
    INSTRUCTION_SCHEMA, SLOT_SCHEMA, TOKEN_TRANSFER_SCHEMA, TRANSACTION_SCHEMA, schema_for,
};
use prism::storage::{FlushResult, ParquetWriter, PartitionKey, Storage, TableKind};
use prism::types::error::PrismError;
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
    UDM_TIMEZONE, UdmRow, from_json_string,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, unique directory path under the system temp dir. Nothing is
/// created.
fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-storage-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn date(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

/// 2026-10-27T12:00:00Z.
const NOON_OCT_27: i64 = 1_793_102_400;

/// Every file under `dir`, recursively, as paths relative to `dir`, sorted.
fn tree(dir: &Path) -> Vec<String> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<String>) {
        let Ok(entries) = fs::read_dir(dir) else { return };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, out);
            } else {
                out.push(path.strip_prefix(root).unwrap().display().to_string());
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// The single file of `table` under `dir`.
fn only_file(dir: &Path, table: TableKind) -> PathBuf {
    let files = table_files(dir, table);
    assert_eq!(files.len(), 1, "{table}: {files:?}");
    files.into_iter().next().unwrap()
}

/// Every `.parquet` file of `table` under `dir`, sorted by starting slot.
fn table_files(dir: &Path, table: TableKind) -> Vec<PathBuf> {
    let root = PartitionKey::table_root(dir, table);
    let mut files: Vec<PathBuf> = tree(&root)
        .into_iter()
        .map(|rel| root.join(rel))
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect();
    files.sort_by_key(|p| PartitionKey::from_path(p).unwrap().from_slot());
    files
}

/// The whole Parquet file at `path` as one batch.
fn read_file(path: &Path) -> RecordBatch {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
    let schema = builder.schema().clone();
    let batches: Vec<RecordBatch> = builder.build().unwrap().map(Result::unwrap).collect();
    concat_batches(&schema, &batches).unwrap()
}

/// Every row of `table` under `dir`, across all files, in file order.
fn read_table<R>(dir: &Path, table: TableKind, decode: fn(&RecordBatch) -> Vec<R>) -> Vec<R> {
    table_files(dir, table).iter().flat_map(|p| decode(&read_file(p))).collect()
}

// Column access by name: independent of the column order in the schema.

fn u64s(b: &RecordBatch, name: &str) -> Vec<Option<u64>> {
    b.column_by_name(name).unwrap().as_primitive::<UInt64Type>().iter().collect()
}

fn u32s(b: &RecordBatch, name: &str) -> Vec<Option<u32>> {
    b.column_by_name(name).unwrap().as_primitive::<UInt32Type>().iter().collect()
}

fn u8s(b: &RecordBatch, name: &str) -> Vec<Option<u8>> {
    b.column_by_name(name).unwrap().as_primitive::<UInt8Type>().iter().collect()
}

fn times(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    b.column_by_name(name).unwrap().as_primitive::<TimestampSecondType>().iter().collect()
}

fn strs(b: &RecordBatch, name: &str) -> Vec<Option<String>> {
    b.column_by_name(name)
        .unwrap()
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(str::to_owned))
        .collect()
}

fn req<T>(column: Vec<Option<T>>, name: &str) -> Vec<T> {
    column
        .into_iter()
        .map(|v| v.unwrap_or_else(|| panic!("null in non-nullable column {name}")))
        .collect()
}

fn decode_slots(b: &RecordBatch) -> Vec<DeepAnaSlot> {
    let slot = req(u64s(b, "slot"), "slot");
    let blockhash = req(strs(b, "blockhash"), "blockhash");
    let parent = req(u64s(b, "parent_slot"), "parent_slot");
    let block_time = times(b, "block_time");
    let height = u64s(b, "block_height");
    let count = req(u32s(b, "transaction_count"), "transaction_count");
    let leader = strs(b, "leader");
    (0..b.num_rows())
        .map(|i| DeepAnaSlot {
            slot: slot[i],
            blockhash: blockhash[i].clone(),
            parent_slot: parent[i],
            block_time: block_time[i],
            block_height: height[i],
            transaction_count: count[i],
            leader: leader[i].clone(),
        })
        .collect()
}

fn decode_transactions(b: &RecordBatch) -> Vec<DeepAnaTransaction> {
    let slot = req(u64s(b, "slot"), "slot");
    let blockhash = req(strs(b, "blockhash"), "blockhash");
    let index = req(u32s(b, "transaction_index"), "transaction_index");
    let sig = req(strs(b, "tx_signature"), "tx_signature");
    let fee = req(u64s(b, "fee"), "fee");
    let status = req(strs(b, "status"), "status");
    let err = strs(b, "err");
    let accounts = req(strs(b, "accounts"), "accounts");
    let logs = req(strs(b, "log_messages"), "log_messages");
    let cu = u64s(b, "compute_units_consumed");
    (0..b.num_rows())
        .map(|i| DeepAnaTransaction {
            slot: slot[i],
            blockhash: blockhash[i].clone(),
            transaction_index: index[i],
            tx_signature: sig[i].clone(),
            fee: fee[i],
            status: status[i].parse::<TransactionStatus>().unwrap(),
            err: err[i].clone(),
            accounts: from_json_string(&accounts[i]).unwrap(),
            log_messages: from_json_string(&logs[i]).unwrap(),
            compute_units_consumed: cu[i],
        })
        .collect()
}

fn decode_instructions(b: &RecordBatch) -> Vec<DeepAnaInstruction> {
    let slot = req(u64s(b, "slot"), "slot");
    let sig = req(strs(b, "tx_signature"), "tx_signature");
    let index = req(u32s(b, "instruction_index"), "instruction_index");
    let inner = u32s(b, "inner_instruction_index");
    let program = req(strs(b, "program_id"), "program_id");
    let accounts = req(strs(b, "accounts"), "accounts");
    let data = req(strs(b, "data_base58"), "data_base58");
    let kind = strs(b, "instruction_type");
    let decoded = strs(b, "decoded_data");
    (0..b.num_rows())
        .map(|i| DeepAnaInstruction {
            slot: slot[i],
            tx_signature: sig[i].clone(),
            instruction_index: index[i],
            inner_instruction_index: inner[i],
            program_id: program[i].clone(),
            accounts: from_json_string(&accounts[i]).unwrap(),
            data_base58: data[i].clone(),
            instruction_type: kind[i].clone(),
            decoded_data: decoded[i].as_deref().map(|raw| from_json_string(raw).unwrap()),
        })
        .collect()
}

fn decode_transfers(b: &RecordBatch) -> Vec<DeepAnaTokenTransfer> {
    let slot = req(u64s(b, "slot"), "slot");
    let sig = req(strs(b, "tx_signature"), "tx_signature");
    let index = req(u32s(b, "instruction_index"), "instruction_index");
    let inner = u32s(b, "inner_instruction_index");
    let program = req(strs(b, "spl_token_program_id"), "spl_token_program_id");
    let src = req(strs(b, "source_token_account"), "source_token_account");
    let dst = req(strs(b, "destination_token_account"), "destination_token_account");
    let src_owner = strs(b, "source_owner");
    let dst_owner = strs(b, "destination_owner");
    let mint = strs(b, "mint_token_address");
    let amount = req(u64s(b, "amount_raw"), "amount_raw");
    let decimals = u8s(b, "decimals");
    let block_time = times(b, "block_time");
    (0..b.num_rows())
        .map(|i| DeepAnaTokenTransfer {
            slot: slot[i],
            tx_signature: sig[i].clone(),
            instruction_index: index[i],
            inner_instruction_index: inner[i],
            spl_token_program_id: program[i].clone(),
            source_token_account: src[i].clone(),
            destination_token_account: dst[i].clone(),
            source_owner: src_owner[i].clone(),
            destination_owner: dst_owner[i].clone(),
            mint_token_address: mint[i].clone(),
            amount_raw: amount[i],
            decimals: decimals[i],
            block_time: block_time[i],
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// The rows of one slot, as handed to `append`.
#[derive(Debug, Clone, Default)]
struct SlotRows {
    slot: DeepAnaSlot,
    transactions: Vec<DeepAnaTransaction>,
    instructions: Vec<DeepAnaInstruction>,
    transfers: Vec<DeepAnaTokenTransfer>,
}

impl SlotRows {
    fn append_to(&self, writer: &mut ParquetWriter) {
        writer
            .append(&self.slot, &self.transactions, &self.instructions, &self.transfers)
            .unwrap();
    }
}

const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PeT5ZYGyUc8V5h";

fn sig(slot: u64, index: u32) -> String {
    format!("sig-{slot}-{index}")
}

/// A slot with no transactions at all.
fn empty_slot(slot: u64, block_time: Option<i64>) -> SlotRows {
    SlotRows {
        slot: DeepAnaSlot {
            slot,
            blockhash: format!("hash-{slot}"),
            parent_slot: slot.saturating_sub(1),
            block_time,
            block_height: None,
            transaction_count: 0,
            leader: None,
        },
        ..SlotRows::default()
    }
}

/// A slot with two transactions: a successful token transfer (one
/// top-level instruction with an inner CPI that moves tokens) and a failed
/// transaction whose single instruction did not decode. Every nullable
/// column is set in one row and unset in the other.
fn busy_slot(slot: u64, block_time: Option<i64>) -> SlotRows {
    let (ok, failed) = (sig(slot, 0), sig(slot, 1));
    SlotRows {
        slot: DeepAnaSlot {
            slot,
            blockhash: format!("hash-{slot}"),
            parent_slot: slot - 1,
            block_time,
            block_height: Some(slot.saturating_sub(10)),
            transaction_count: 2,
            leader: Some(format!("leader-{slot}")),
        },
        transactions: vec![
            DeepAnaTransaction {
                slot,
                blockhash: format!("hash-{slot}"),
                transaction_index: 0,
                tx_signature: ok.clone(),
                fee: 5_000,
                status: TransactionStatus::Success,
                err: None,
                accounts: vec!["payer".into(), "src".into(), "dst".into(), SPL_TOKEN.into()],
                log_messages: vec![
                    format!("Program {SPL_TOKEN} invoke [1]"),
                    "Program log: Instruction: TransferChecked — ünïcødé ✓".into(),
                    "quotes \" and \\ backslashes".into(),
                ],
                compute_units_consumed: Some(4_645),
            },
            DeepAnaTransaction {
                slot,
                blockhash: format!("hash-{slot}"),
                transaction_index: 1,
                tx_signature: failed.clone(),
                fee: u64::MAX,
                status: TransactionStatus::Failed,
                err: Some("InstructionError(0, Custom(1))".into()),
                accounts: vec![],
                log_messages: vec![],
                compute_units_consumed: None,
            },
        ],
        instructions: vec![
            DeepAnaInstruction {
                slot,
                tx_signature: ok.clone(),
                instruction_index: 0,
                inner_instruction_index: None,
                program_id: "Router111".into(),
                accounts: vec!["src".into(), "dst".into()],
                data_base58: "3Bxs4h24hBtQy9rw".into(),
                instruction_type: None,
                decoded_data: None,
            },
            DeepAnaInstruction {
                slot,
                tx_signature: ok.clone(),
                instruction_index: 0,
                inner_instruction_index: Some(0),
                program_id: SPL_TOKEN.into(),
                accounts: vec!["src".into(), "mint".into(), "dst".into(), "owner".into()],
                data_base58: "g7ddWGUpS7hBn".into(),
                instruction_type: Some("TransferChecked".into()),
                decoded_data: Some(json!({ "amount": 1_000_000u64, "decimals": 6, "nested": { "list": [1, null, "x"] } })),
            },
            DeepAnaInstruction {
                slot,
                tx_signature: failed,
                instruction_index: 0,
                inner_instruction_index: None,
                program_id: "Unknown111".into(),
                accounts: vec![],
                data_base58: String::new(),
                instruction_type: None,
                decoded_data: None,
            },
        ],
        transfers: vec![
            DeepAnaTokenTransfer {
                slot,
                tx_signature: ok.clone(),
                instruction_index: 0,
                inner_instruction_index: Some(0),
                spl_token_program_id: SPL_TOKEN.into(),
                source_token_account: "src".into(),
                destination_token_account: "dst".into(),
                source_owner: Some("owner".into()),
                destination_owner: None,
                mint_token_address: Some("mint".into()),
                amount_raw: 1_000_000,
                decimals: Some(6),
                block_time,
            },
            DeepAnaTokenTransfer {
                slot,
                tx_signature: ok,
                instruction_index: 1,
                inner_instruction_index: None,
                spl_token_program_id: TOKEN_2022.into(),
                source_token_account: "src2".into(),
                destination_token_account: "dst2".into(),
                source_owner: None,
                destination_owner: Some("owner2".into()),
                mint_token_address: None,
                amount_raw: u64::MAX,
                decimals: None,
                block_time: None,
            },
        ],
    }
}

/// Everything in `slots`, table by table, in append order.
fn flatten(
    slots: &[SlotRows],
) -> (Vec<DeepAnaSlot>, Vec<DeepAnaTransaction>, Vec<DeepAnaInstruction>, Vec<DeepAnaTokenTransfer>) {
    (
        slots.iter().map(|s| s.slot.clone()).collect(),
        slots.iter().flat_map(|s| s.transactions.clone()).collect(),
        slots.iter().flat_map(|s| s.instructions.clone()).collect(),
        slots.iter().flat_map(|s| s.transfers.clone()).collect(),
    )
}

/// Asserts that the four tables on disk under `dir` hold exactly `slots`.
fn assert_disk_holds(dir: &Path, slots: &[SlotRows]) {
    let (s, t, i, x) = flatten(slots);
    assert_eq!(read_table(dir, TableKind::Slots, decode_slots), s, "slots");
    assert_eq!(read_table(dir, TableKind::Transactions, decode_transactions), t, "transactions");
    assert_eq!(read_table(dir, TableKind::Instructions, decode_instructions), i, "instructions");
    assert_eq!(read_table(dir, TableKind::TokenTransfers, decode_transfers), x, "token_transfers");
}

// ---------------------------------------------------------------------------
// TableKind / PartitionKey
// ---------------------------------------------------------------------------

#[test]
fn table_kinds_name_their_directories_and_file_prefixes() {
    let names: Vec<_> = TableKind::ALL.iter().map(|t| (t.as_str(), t.file_prefix())).collect();
    assert_eq!(
        names,
        [
            ("slots", "slots"),
            ("transactions", "txs"),
            ("instructions", "ixs"),
            ("token_transfers", "transfers"),
        ]
    );
    for table in TableKind::ALL {
        assert_eq!(TableKind::from_file_prefix(table.file_prefix()), Some(table));
        assert_eq!(table.to_string(), table.as_str());
    }
    assert_eq!(TableKind::from_file_prefix("transactions"), None, "the prefix, not the dir name");
}

#[test]
fn partition_paths_follow_the_documented_layout() {
    let keys = PartitionKey::for_all_tables("/data", date(2026, 10, 27), 100, 199);
    let paths: Vec<String> = keys.iter().map(|k| k.path().display().to_string()).collect();
    assert_eq!(
        paths,
        [
            "/data/solana/slots/date=2026-10-27/slots_100_199.parquet",
            "/data/solana/transactions/date=2026-10-27/txs_100_199.parquet",
            "/data/solana/instructions/date=2026-10-27/ixs_100_199.parquet",
            "/data/solana/token_transfers/date=2026-10-27/transfers_100_199.parquet",
        ]
    );
    for key in &keys {
        assert_eq!(key.path().parent().unwrap(), key.dir());
        assert_eq!(key.path().file_name().unwrap().to_str().unwrap(), key.file_name());
        assert_eq!(key.date_dir_name(), "date=2026-10-27");
        assert_eq!((key.from_slot(), key.to_slot(), key.slot_span()), (100, 199, 100));
        assert_eq!(key.data_dir(), Path::new("/data"));
        assert_eq!(key.to_string(), key.path().display().to_string());
    }
    assert_eq!(PartitionKey::chain_root("/data"), Path::new("/data/solana"));
    assert_eq!(PartitionKey::table_root("/data", TableKind::Instructions), Path::new("/data/solana/instructions"));
}

#[test]
fn a_single_slot_key_spans_one_slot() {
    let key = PartitionKey::new("/d", TableKind::Slots, date(2026, 1, 2), 7, 7);
    assert_eq!(key.file_name(), "slots_7_7.parquet");
    assert_eq!(key.slot_span(), 1);
    assert!(key.contains_slot(7));
    assert!(!key.contains_slot(6) && !key.contains_slot(8));
}

#[test]
fn temp_path_sits_next_to_the_file_and_is_not_a_parquet_file() {
    let key = PartitionKey::new("/d", TableKind::Transactions, date(2026, 1, 2), 1, 2);
    let temp = key.temp_path();
    assert_eq!(temp.parent(), key.path().parent());
    assert_ne!(temp.extension().unwrap(), "parquet");
    assert_eq!(PartitionKey::parse_file_name(temp.file_name().unwrap().to_str().unwrap()), None);
}

#[test]
fn with_to_slot_keeps_everything_but_the_upper_bound() {
    let key = PartitionKey::new("/d", TableKind::TokenTransfers, date(2026, 1, 2), 10, 20);
    let cut = key.with_to_slot(14);
    assert_eq!((cut.table(), cut.date(), cut.from_slot(), cut.to_slot()), (key.table(), key.date(), 10, 14));
    assert_eq!(cut.dir(), key.dir());
    assert_eq!(cut.file_name(), "transfers_10_14.parquet");
}

#[test]
fn file_names_parse_back_into_table_and_range() {
    for table in TableKind::ALL {
        let key = PartitionKey::new("/d", table, date(2026, 1, 2), 350_000_000, u64::MAX);
        let parsed = PartitionKey::parse_file_name(&key.file_name()).unwrap();
        assert_eq!((parsed.table, parsed.from_slot, parsed.to_slot), (table, 350_000_000, u64::MAX));
    }
}

#[test]
fn foreign_file_names_do_not_parse() {
    for name in [
        "slots_1_2.parquet.tmp",
        "slots_1_2.csv",
        "slots_1_2parquet",
        "slots_1.parquet",
        "slots_x_2.parquet",
        "slots_1_-2.parquet",
        "slots_5_2.parquet",
        "blocks_1_2.parquet",
        "transactions_1_2.parquet",
        "_1_2.parquet",
        "slots__1_2.parquet",
        ".parquet",
        "",
    ] {
        assert_eq!(PartitionKey::parse_file_name(name), None, "{name:?}");
    }
}

#[test]
fn keys_round_trip_through_their_paths() {
    for table in TableKind::ALL {
        let key = PartitionKey::new("/var/prism", table, date(2025, 12, 31), 42, 99);
        assert_eq!(PartitionKey::from_path(&key.path()), Some(key));
    }
    let key = PartitionKey::new("relative/dir", TableKind::Slots, date(2025, 12, 31), 1, 1);
    assert_eq!(PartitionKey::from_path(&key.path()), Some(key));
}

#[test]
fn paths_outside_the_layout_do_not_parse_into_keys() {
    for path in [
        // ixs file in the transactions directory
        "/d/solana/transactions/date=2026-01-02/ixs_1_2.parquet",
        // no date directory
        "/d/solana/slots/slots_1_2.parquet",
        // malformed date
        "/d/solana/slots/date=2026-13-02/slots_1_2.parquet",
        // wrong chain directory
        "/d/ethereum/slots/date=2026-01-02/slots_1_2.parquet",
        "slots_1_2.parquet",
    ] {
        assert_eq!(PartitionKey::from_path(Path::new(path)), None, "{path}");
    }
}

#[test]
fn block_time_maps_to_its_utc_day() {
    assert_eq!(PartitionKey::date_for_block_time(Some(NOON_OCT_27)), date(2026, 10, 27));
    let midnight = NOON_OCT_27 + 12 * 3600;
    assert_eq!(PartitionKey::date_for_block_time(Some(midnight - 1)), date(2026, 10, 27));
    assert_eq!(PartitionKey::date_for_block_time(Some(midnight)), date(2026, 10, 28));
    assert_eq!(PartitionKey::date_for_block_time(None), date(1970, 1, 1));
    assert_eq!(PartitionKey::date_for_block_time(Some(i64::MAX)), date(1970, 1, 1), "out of range");
    assert_eq!(PartitionKey::parse_date_dir("date=2026-10-27"), Some(date(2026, 10, 27)));
    assert_eq!(PartitionKey::parse_date_dir("2026-10-27"), None);
}

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

#[test]
fn writer_schemas_are_the_udm_schemas() {
    assert_eq!(*SLOT_SCHEMA, DeepAnaSlot::schema());
    assert_eq!(*TRANSACTION_SCHEMA, DeepAnaTransaction::schema());
    assert_eq!(*INSTRUCTION_SCHEMA, DeepAnaInstruction::schema());
    assert_eq!(*TOKEN_TRANSFER_SCHEMA, DeepAnaTokenTransfer::schema());
    assert_eq!(schema_for(TableKind::Slots), DeepAnaSlot::schema());
    assert_eq!(schema_for(TableKind::Transactions), DeepAnaTransaction::schema());
    assert_eq!(schema_for(TableKind::Instructions), DeepAnaInstruction::schema());
    assert_eq!(schema_for(TableKind::TokenTransfers), DeepAnaTokenTransfer::schema());
}

#[test]
fn storage_is_object_safe_and_thread_safe() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ParquetWriter>();
    let mut storage: Box<dyn Storage> = Box::new(ParquetWriter::new(temp_dir()));
    assert_eq!(storage.buffered_slot_count(), 0);
    assert!(storage.flush().unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// append
// ---------------------------------------------------------------------------

#[test]
fn append_buffers_rows_without_touching_the_disk() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    busy_slot(100, Some(NOON_OCT_27)).append_to(&mut writer);
    busy_slot(101, Some(NOON_OCT_27)).append_to(&mut writer);

    assert!(!dir.exists(), "append must do no I/O");
    assert_eq!(writer.buffered_slot_count(), 2);
    assert_eq!(writer.buffered_row_counts(), [2, 4, 6, 4]);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (Some(100), Some(101)));
    assert_eq!(writer.last_flushed_slot(), None);
}

#[test]
fn append_accepts_a_slot_with_no_transactions_instructions_or_transfers() {
    let mut writer = ParquetWriter::new(temp_dir());
    writer.append(&empty_slot(5, None).slot, &[], &[], &[]).unwrap();
    assert_eq!(writer.buffered_row_counts(), [1, 0, 0, 0]);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (Some(5), Some(5)));
}

#[test]
fn append_rejects_a_slot_that_is_already_buffered() {
    let mut writer = ParquetWriter::new(temp_dir());
    let rows = busy_slot(100, None);
    rows.append_to(&mut writer);
    busy_slot(101, None).append_to(&mut writer);

    let err = writer
        .append(&rows.slot, &rows.transactions, &rows.instructions, &rows.transfers)
        .unwrap_err();
    match &err {
        PrismError::StorageError { table, slot_range, .. } => {
            assert_eq!((table.as_str(), *slot_range), ("slots", (100, 100)));
        }
        other => panic!("expected StorageError, got {other:?}"),
    }
    assert_eq!(writer.buffered_row_counts(), [2, 4, 6, 4], "nothing appended");
}

#[test]
fn an_out_of_order_backfill_widens_the_buffered_range() {
    let mut writer = ParquetWriter::new(temp_dir());
    busy_slot(200, None).append_to(&mut writer);
    busy_slot(150, None).append_to(&mut writer);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (Some(150), Some(200)));
}

// ---------------------------------------------------------------------------
// flush
// ---------------------------------------------------------------------------

#[test]
fn flushing_nothing_writes_nothing() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let result = writer.flush().unwrap();
    assert_eq!(result, FlushResult::default());
    assert!(result.is_empty());
    assert!(!dir.exists());
    assert_eq!(writer.metrics().snapshot().total_flushes, 0);
    assert_eq!(writer.last_flushed_slot(), None);
}

#[test]
fn flush_writes_one_file_per_table_and_empties_the_buffers() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    for slot in 100..=102 {
        busy_slot(slot, Some(NOON_OCT_27 + slot as i64)).append_to(&mut writer);
    }

    let result = writer.flush().unwrap();

    assert_eq!(
        tree(&dir),
        [
            "solana/instructions/date=2026-10-27/ixs_100_102.parquet",
            "solana/slots/date=2026-10-27/slots_100_102.parquet",
            "solana/token_transfers/date=2026-10-27/transfers_100_102.parquet",
            "solana/transactions/date=2026-10-27/txs_100_102.parquet",
        ],
        "no temporary file left behind"
    );
    let expected_paths: Vec<String> = PartitionKey::for_all_tables(&dir, date(2026, 10, 27), 100, 102)
        .iter()
        .map(|k| k.path().display().to_string())
        .collect();
    assert_eq!(result.parquet_paths, expected_paths, "in TableKind::ALL order");
    assert_eq!(
        (result.slots_written, result.transactions_written, result.instructions_written, result.transfers_written),
        (3, 6, 9, 6)
    );
    assert_eq!((result.from_slot, result.to_slot, result.total_rows()), (100, 102, 24));

    assert_eq!(writer.buffered_slot_count(), 0);
    assert_eq!(writer.buffered_row_counts(), [0; 4]);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (None, None));
    assert_eq!(writer.last_flushed_slot(), Some(102));
    assert!(writer.flush().unwrap().is_empty(), "a second flush has nothing to do");
}

#[test]
fn flush_records_metrics() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    busy_slot(1, None).append_to(&mut writer);
    let first = writer.flush().unwrap();
    busy_slot(2, None).append_to(&mut writer);
    empty_slot(3, None).append_to(&mut writer);
    let second = writer.flush().unwrap();
    writer.flush().unwrap();

    let on_disk: u64 = first
        .parquet_paths
        .iter()
        .chain(&second.parquet_paths)
        .map(|p| fs::metadata(p).unwrap().len())
        .sum();
    let m = writer.metrics().snapshot();
    assert_eq!(m.total_flushes, 2, "the empty flush is not counted");
    assert_eq!(m.total_rows_written, first.total_rows() + second.total_rows());
    assert_eq!(m.total_rows_written, 8 + 9);
    assert_eq!(m.total_bytes_written, on_disk);
    assert_eq!(m.last_flush_duration_ms, second.duration_ms);
    assert_eq!(m.flush_errors, 0);
}

#[test]
fn empty_tables_get_no_file_and_no_directory() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    empty_slot(10, Some(NOON_OCT_27)).append_to(&mut writer);
    empty_slot(11, Some(NOON_OCT_27)).append_to(&mut writer);

    let result = writer.flush().unwrap();

    assert_eq!(tree(&dir), ["solana/slots/date=2026-10-27/slots_10_11.parquet"]);
    assert_eq!(result.parquet_paths.len(), 1);
    assert_eq!((result.slots_written, result.total_rows()), (2, 2));
    assert!(!PartitionKey::table_root(&dir, TableKind::Transactions).exists());
}

#[test]
fn a_slot_with_transactions_but_no_transfers_skips_only_the_transfer_table() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let mut rows = busy_slot(10, Some(NOON_OCT_27));
    rows.transfers.clear();
    rows.append_to(&mut writer);

    let result = writer.flush().unwrap();
    assert_eq!(result.parquet_paths.len(), 3);
    assert!(table_files(&dir, TableKind::TokenTransfers).is_empty());
    assert_disk_holds(&dir, &[rows]);
}

#[test]
fn the_batch_is_filed_under_the_day_of_its_first_slot() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let before_midnight = NOON_OCT_27 + 12 * 3600 - 1;
    busy_slot(1, Some(before_midnight)).append_to(&mut writer);
    busy_slot(2, Some(before_midnight + 2)).append_to(&mut writer);
    writer.flush().unwrap();
    assert!(only_file(&dir, TableKind::Slots).ends_with("date=2026-10-27/slots_1_2.parquet"));
}

#[test]
fn a_first_slot_without_block_time_takes_the_day_of_the_next_one_that_has_it() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    busy_slot(1, None).append_to(&mut writer);
    busy_slot(2, Some(NOON_OCT_27)).append_to(&mut writer);
    writer.flush().unwrap();
    assert!(only_file(&dir, TableKind::Slots).ends_with("date=2026-10-27/slots_1_2.parquet"));
}

#[test]
fn a_batch_without_any_block_time_is_filed_under_the_epoch() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    busy_slot(1, None).append_to(&mut writer);
    writer.flush().unwrap();
    assert!(only_file(&dir, TableKind::Slots).ends_with("date=1970-01-01/slots_1_1.parquet"));
}

#[test]
fn successive_flushes_write_separate_files_that_together_hold_every_row() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let batches = [
        vec![busy_slot(100, Some(NOON_OCT_27)), empty_slot(101, Some(NOON_OCT_27))],
        vec![busy_slot(102, Some(NOON_OCT_27)), busy_slot(103, Some(NOON_OCT_27))],
        vec![busy_slot(104, Some(NOON_OCT_27 + 86_400))],
    ];
    for batch in &batches {
        batch.iter().for_each(|rows| rows.append_to(&mut writer));
        writer.flush().unwrap();
    }

    let slot_files: Vec<String> = table_files(&dir, TableKind::Slots)
        .iter()
        .map(|p| p.strip_prefix(&dir).unwrap().display().to_string())
        .collect();
    assert_eq!(
        slot_files,
        [
            "solana/slots/date=2026-10-27/slots_100_101.parquet",
            "solana/slots/date=2026-10-27/slots_102_103.parquet",
            "solana/slots/date=2026-10-28/slots_104_104.parquet",
        ]
    );
    assert_eq!(writer.last_flushed_slot(), Some(104));
    assert_disk_holds(&dir, &batches.concat());
}

#[test]
fn reflushing_the_same_range_replaces_the_file() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    busy_slot(7, Some(NOON_OCT_27)).append_to(&mut writer);
    writer.flush().unwrap();

    let mut replacement = busy_slot(7, Some(NOON_OCT_27));
    replacement.slot.blockhash = "replacement".into();
    let mut fresh = ParquetWriter::new(&dir);
    replacement.append_to(&mut fresh);
    fresh.flush().unwrap();

    assert_eq!(read_table(&dir, TableKind::Slots, decode_slots), [replacement.slot]);
}

/// Creates a directory where `key`'s temporary file goes, so writing that
/// table fails.
fn block_table(key: &PartitionKey) {
    fs::create_dir_all(key.temp_path()).unwrap();
}

fn unblock_table(key: &PartitionKey) {
    fs::remove_dir(key.temp_path()).unwrap();
}

#[test]
fn a_failed_flush_leaves_no_files_and_keeps_every_buffered_row() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let rows = [busy_slot(100, Some(NOON_OCT_27)), busy_slot(101, Some(NOON_OCT_27))];
    rows.iter().for_each(|r| r.append_to(&mut writer));
    // Slots and transactions are written before instructions fail.
    let keys = writer.pending_partition_keys().unwrap();
    block_table(&keys[2]);

    let err = writer.flush().unwrap_err();

    match &err {
        PrismError::StorageError { table, slot_range, .. } => {
            assert_eq!((table.as_str(), *slot_range), ("instructions", (100, 101)));
        }
        other => panic!("expected StorageError, got {other:?}"),
    }
    let files: Vec<_> = tree(&dir).into_iter().filter(|f| f.ends_with(".parquet")).collect();
    assert!(files.is_empty(), "the slot and transaction files were removed again: {files:?}");
    assert_eq!(writer.buffered_row_counts(), [2, 4, 6, 4]);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (Some(100), Some(101)));
    assert_eq!(writer.last_flushed_slot(), None);
    let m = writer.metrics().snapshot();
    assert_eq!((m.flush_errors, m.total_flushes, m.total_rows_written), (1, 0, 0));

    unblock_table(&keys[2]);
    let result = writer.flush().unwrap();
    assert_eq!(result.parquet_paths.len(), 4);
    assert_eq!(writer.last_flushed_slot(), Some(101));
    assert_disk_holds(&dir, &rows);
}

#[test]
fn rows_appended_after_a_failed_flush_go_out_with_the_retry() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let first = busy_slot(100, Some(NOON_OCT_27));
    first.append_to(&mut writer);
    let keys = writer.pending_partition_keys().unwrap();
    block_table(&keys[3]);
    writer.flush().unwrap_err();
    unblock_table(&keys[3]);

    let second = busy_slot(101, Some(NOON_OCT_27));
    second.append_to(&mut writer);
    let result = writer.flush().unwrap();

    assert_eq!((result.from_slot, result.to_slot), (100, 101));
    assert_eq!(table_files(&dir, TableKind::Slots).len(), 1, "no stale 100_100 file");
    assert_disk_holds(&dir, &[first, second]);
}

// ---------------------------------------------------------------------------
// Consistency: what is on disk is what was buffered, and it can be queried
// ---------------------------------------------------------------------------

#[test]
fn flushed_parquet_files_match_the_buffer_they_were_written_from() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let known: Vec<SlotRows> = vec![
        busy_slot(350_000_000, Some(NOON_OCT_27)),
        empty_slot(350_000_001, None),
        busy_slot(350_000_002, Some(NOON_OCT_27 + 1)),
        busy_slot(350_000_003, Some(NOON_OCT_27 + 2)),
    ];
    known.iter().for_each(|rows| rows.append_to(&mut writer));
    let (slots, txs, ixs, transfers) = flatten(&known);

    let result = writer.flush().unwrap();

    // 1. Every file the flush reported exists, is a valid Parquet file, and
    //    carries the UDM schema, time zone included.
    assert_eq!(result.parquet_paths.len(), 4);
    for (path, table) in result.parquet_paths.iter().zip(TableKind::ALL) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        assert_eq!(*builder.schema(), schema_for(table), "{table}: schema");
        let rows: i64 = builder.metadata().row_groups().iter().map(|rg| rg.num_rows()).sum();
        let expected = [slots.len(), txs.len(), ixs.len(), transfers.len()][TableKind::ALL
            .iter()
            .position(|t| *t == table)
            .unwrap()];
        assert_eq!(rows as usize, expected, "{table}: footer row count");
    }
    let block_time_type = DataType::Timestamp(TimeUnit::Second, Some(UDM_TIMEZONE.into()));
    let slot_batch = read_file(Path::new(&result.parquet_paths[0]));
    assert_eq!(slot_batch.schema().field_with_name("block_time").unwrap().data_type(), &block_time_type);

    // 2. Row for row, every table round-trips exactly.
    assert_eq!(decode_slots(&slot_batch), slots);
    let tx_batch = read_file(Path::new(&result.parquet_paths[1]));
    assert_eq!(decode_transactions(&tx_batch), txs);
    let ix_batch = read_file(Path::new(&result.parquet_paths[2]));
    assert_eq!(decode_instructions(&ix_batch), ixs);
    let transfer_batch = read_file(Path::new(&result.parquet_paths[3]));
    assert_eq!(decode_transfers(&transfer_batch), transfers);

    // 3. Nulls land where the buffer had `None`, and only there.
    let nulls = |b: &RecordBatch, c: &str| b.column_by_name(c).unwrap().null_count();
    assert_eq!(nulls(&slot_batch, "block_time"), slots.iter().filter(|s| s.block_time.is_none()).count());
    assert_eq!(nulls(&slot_batch, "leader"), slots.iter().filter(|s| s.leader.is_none()).count());
    assert_eq!(nulls(&tx_batch, "err"), txs.iter().filter(|t| t.err.is_none()).count());
    assert_eq!(nulls(&ix_batch, "decoded_data"), ixs.iter().filter(|i| i.decoded_data.is_none()).count());
    assert_eq!(nulls(&transfer_batch, "decimals"), transfers.iter().filter(|t| t.decimals.is_none()).count());
    for (batch, name) in [(&slot_batch, "slot"), (&tx_batch, "tx_signature"), (&ix_batch, "program_id"), (&transfer_batch, "amount_raw")] {
        assert_eq!(nulls(batch, name), 0, "{name} is not nullable");
    }

    // 4. Queries over the files give the answers the buffer gives.

    // Projection: read two columns only.
    let tx_path = Path::new(&result.parquet_paths[1]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(tx_path).unwrap()).unwrap();
    let mask = ProjectionMask::columns(builder.parquet_schema(), ["tx_signature", "fee"]);
    let projected: Vec<RecordBatch> = builder.with_projection(mask).build().unwrap().map(Result::unwrap).collect();
    let projected = concat_batches(&projected[0].schema(), &projected).unwrap();
    assert_eq!(projected.num_columns(), 2);
    assert_eq!(req(strs(&projected, "tx_signature"), "tx_signature"), txs.iter().map(|t| t.tx_signature.clone()).collect::<Vec<_>>());

    // SELECT tx_signature FROM transactions WHERE status = 'failed'
    let status = tx_batch.column_by_name("status").unwrap();
    let failed = filter_record_batch(&tx_batch, &eq(status, &Scalar::new(StringArray::from(vec!["failed"]))).unwrap()).unwrap();
    let expected_failed: Vec<String> = txs.iter().filter(|t| !t.is_success()).map(|t| t.tx_signature.clone()).collect();
    assert_eq!(req(strs(&failed, "tx_signature"), "tx_signature"), expected_failed);

    // SELECT SUM(compute_units_consumed) FROM transactions WHERE status = 'success'
    let ok = filter_record_batch(&tx_batch, &eq(status, &Scalar::new(StringArray::from(vec!["success"]))).unwrap()).unwrap();
    let cu_sum = sum(ok.column_by_name("compute_units_consumed").unwrap().as_primitive::<UInt64Type>());
    let expected_cu: u64 = txs.iter().filter(|t| t.is_success()).filter_map(|t| t.compute_units_consumed).sum();
    assert_eq!(cu_sum, Some(expected_cu));

    // SELECT COUNT(*) FROM instructions WHERE slot BETWEEN a AND b AND inner_instruction_index IS NOT NULL
    let (a, b) = (350_000_001u64, 350_000_002u64);
    let ix_slot = ix_batch.column_by_name("slot").unwrap();
    let in_range: BooleanArray = and(
        &gt_eq(ix_slot, &Scalar::new(UInt64Array::from(vec![a]))).unwrap(),
        &lt_eq(ix_slot, &Scalar::new(UInt64Array::from(vec![b]))).unwrap(),
    )
    .unwrap();
    let inner = arrow::compute::is_not_null(ix_batch.column_by_name("inner_instruction_index").unwrap()).unwrap();
    let hits = filter_record_batch(&ix_batch, &and(&in_range, &inner).unwrap()).unwrap();
    let expected_hits = ixs.iter().filter(|i| (a..=b).contains(&i.slot) && i.is_inner()).count();
    assert_eq!(hits.num_rows(), expected_hits);

    // Join: every instruction and transfer belongs to a transaction on disk.
    let tx_sigs: HashSet<String> = req(strs(&tx_batch, "tx_signature"), "tx_signature").into_iter().collect();
    for s in req(strs(&ix_batch, "tx_signature"), "tx_signature").iter().chain(&req(strs(&transfer_batch, "tx_signature"), "tx_signature")) {
        assert!(tx_sigs.contains(s), "{s} has no transaction row");
    }

    // JSON columns decode back to the buffered values.
    let decoded = strs(&ix_batch, "decoded_data");
    let first_decoded = decoded.iter().flatten().next().unwrap();
    let value: serde_json::Value = from_json_string(first_decoded).unwrap();
    assert_eq!(value["nested"]["list"][2], "x");

    // 5. Row-group statistics, which query engines prune on, span the
    //    flushed slot range.
    for (path, table) in result.parquet_paths.iter().zip(TableKind::ALL) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        let rg = &builder.metadata().row_groups()[0];
        let slot_col = rg.columns().iter().find(|c| c.column_path().string() == "slot").unwrap();
        match slot_col.statistics() {
            Some(Statistics::Int64(stats)) => {
                let (min, max) = (*stats.min_opt().unwrap() as u64, *stats.max_opt().unwrap() as u64);
                assert!(min >= result.from_slot && max <= result.to_slot, "{table}: {min}..={max}");
            }
            other => panic!("{table}: unexpected slot statistics {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// rollback
// ---------------------------------------------------------------------------

/// A writer that has flushed slots 100..=102, 103..=105 and 106..=108 (all
/// busy) and buffers 109 and 110.
fn three_flushes(dir: &Path) -> (ParquetWriter, Vec<SlotRows>) {
    let mut writer = ParquetWriter::new(dir);
    let all: Vec<SlotRows> = (100..=110).map(|s| busy_slot(s, Some(NOON_OCT_27))).collect();
    for chunk in all[..9].chunks(3) {
        chunk.iter().for_each(|r| r.append_to(&mut writer));
        writer.flush().unwrap();
    }
    all[9..].iter().for_each(|r| r.append_to(&mut writer));
    (writer, all)
}

fn slot_file_names(dir: &Path) -> Vec<String> {
    table_files(dir, TableKind::Slots)
        .iter()
        .map(|p| p.file_name().unwrap().to_str().unwrap().to_owned())
        .collect()
}

#[test]
fn rollback_at_a_file_boundary_deletes_the_files_at_and_above_it() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);

    writer.rollback(103).unwrap();

    assert_eq!(slot_file_names(&dir), ["slots_100_102.parquet"]);
    for table in TableKind::ALL {
        assert_eq!(table_files(&dir, table).len(), 1, "{table}");
    }
    assert_disk_holds(&dir, &all[..3]);
    assert_eq!(writer.buffered_slot_count(), 0, "buffered 109 and 110 are above 103");
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (None, None));
    assert_eq!(writer.last_flushed_slot(), Some(102));
}

#[test]
fn rollback_inside_a_file_cuts_it_down_to_the_rows_below_the_point() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);

    writer.rollback(104).unwrap();

    assert_eq!(slot_file_names(&dir), ["slots_100_102.parquet", "slots_103_103.parquet"]);
    for table in TableKind::ALL {
        let names: Vec<_> = table_files(&dir, table).iter().map(|p| p.file_name().unwrap().to_str().unwrap().to_owned()).collect();
        assert_eq!(names, [format!("{}_100_102.parquet", table.file_prefix()), format!("{}_103_103.parquet", table.file_prefix())]);
    }
    assert_disk_holds(&dir, &all[..4]);
    assert_eq!(writer.last_flushed_slot(), Some(103));
    assert!(!tree(&dir).iter().any(|f| f.ends_with(".tmp")));
}

#[test]
fn rollback_into_the_buffer_keeps_disk_and_trims_the_buffer() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);

    writer.rollback(110).unwrap();

    assert_eq!(table_files(&dir, TableKind::Slots).len(), 3);
    assert_disk_holds(&dir, &all[..9]);
    assert_eq!(writer.buffered_slot_count(), 1);
    assert_eq!(writer.buffered_row_counts(), [1, 2, 3, 2]);
    assert_eq!((writer.first_buffered_slot(), writer.last_buffered_slot()), (Some(109), Some(109)));
    assert_eq!(writer.last_flushed_slot(), Some(108), "nothing flushed was rolled back");

    writer.flush().unwrap();
    assert_disk_holds(&dir, &all[..10]);
}

#[test]
fn rollback_to_zero_removes_everything() {
    let dir = temp_dir();
    let (mut writer, _) = three_flushes(&dir);
    writer.rollback(0).unwrap();
    assert!(tree(&dir).iter().all(|f| !f.ends_with(".parquet")));
    assert_eq!(writer.buffered_slot_count(), 0);
    assert_eq!(writer.last_flushed_slot(), None);
}

#[test]
fn rollback_above_everything_changes_nothing() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);
    let before = tree(&dir);
    writer.rollback(1_000).unwrap();
    assert_eq!(tree(&dir), before);
    assert_eq!(writer.buffered_slot_count(), 2);
    assert_eq!(writer.last_flushed_slot(), Some(108));
    writer.flush().unwrap();
    assert_disk_holds(&dir, &all);
}

#[test]
fn rollback_is_idempotent() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);
    writer.rollback(104).unwrap();
    let after_first = tree(&dir);
    writer.rollback(104).unwrap();
    assert_eq!(tree(&dir), after_first);
    assert_disk_holds(&dir, &all[..4]);
}

#[test]
fn rollback_on_a_missing_data_dir_is_a_no_op() {
    let mut writer = ParquetWriter::new(temp_dir());
    writer.rollback(5).unwrap();
}

#[test]
fn rollback_finds_files_left_by_an_earlier_run() {
    let dir = temp_dir();
    let (writer, all) = three_flushes(&dir);
    drop(writer);

    let mut restarted = ParquetWriter::new(&dir);
    restarted.rollback(103).unwrap();
    assert_disk_holds(&dir, &all[..3]);
}

#[test]
fn rollback_leaves_foreign_files_alone() {
    let dir = temp_dir();
    let (mut writer, _) = three_flushes(&dir);
    let solana = dir.join(CHAIN_DIR);
    let foreign = [
        solana.join("README.txt"),
        solana.join("slots/notes_200_300.parquet"),
        solana.join("slots/date=2026-10-27/slots_200_300.parquet.tmp"),
    ];
    for path in &foreign {
        fs::write(path, b"not ours").unwrap();
    }

    writer.rollback(100).unwrap();

    for path in &foreign {
        assert!(path.exists(), "{} was removed", path.display());
    }
}

#[test]
fn rollback_cuts_files_in_every_date_partition() {
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let day_one = busy_slot(10, Some(NOON_OCT_27));
    let day_two: Vec<SlotRows> = (11..=13).map(|s| busy_slot(s, Some(NOON_OCT_27 + 86_400))).collect();
    day_one.append_to(&mut writer);
    writer.flush().unwrap();
    day_two.iter().for_each(|r| r.append_to(&mut writer));
    writer.flush().unwrap();

    writer.rollback(12).unwrap();

    let files: Vec<_> = tree(&dir).into_iter().filter(|f| f.starts_with("solana/slots/")).collect();
    assert_eq!(
        files,
        ["solana/slots/date=2026-10-27/slots_10_10.parquet", "solana/slots/date=2026-10-28/slots_11_11.parquet"]
    );
    assert_disk_holds(&dir, &[day_one, day_two[0].clone()]);
}

#[test]
fn a_fork_can_be_replayed_after_rollback_without_duplicates() {
    let dir = temp_dir();
    let (mut writer, all) = three_flushes(&dir);

    writer.rollback(104).unwrap();
    let fork: Vec<SlotRows> = (104..=106)
        .map(|s| {
            let mut rows = busy_slot(s, Some(NOON_OCT_27));
            rows.slot.blockhash = format!("fork-{s}");
            rows
        })
        .collect();
    fork.iter().for_each(|r| r.append_to(&mut writer));
    writer.flush().unwrap();

    let expected: Vec<SlotRows> = all[..4].iter().cloned().chain(fork).collect();
    assert_disk_holds(&dir, &expected);
    assert_eq!(writer.last_flushed_slot(), Some(106));
}

// ---------------------------------------------------------------------------
// Live: Solana devnet through Helius
// ---------------------------------------------------------------------------

/// Slots below the confirmed tip to start from, so every block exists.
const TIP_MARGIN: u64 = 30;

/// The rows of up to `limit` recent devnet blocks, oldest first, run through
/// the normalizer, the SPL Token decoder and the account resolver. `None`
/// (with a note) when no key is configured.
async fn live_slot_rows(limit: usize) -> Option<Vec<SlotRows>> {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return None;
    };
    let adapter = Arc::new(helius_solana_adapter(&key, Cluster::Devnet).await.unwrap_or_else(|err| {
        panic!("helius adapter: {}", err.to_string().replace(&key, "<HELIUS_API_KEY>"))
    }));
    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.expect("devnet tip");
    let top = tip.slot_or_height - TIP_MARGIN;

    let normalizer = Normalizer::new();
    let registry = DecoderRegistry::with_spl_token();
    let mut resolver = AccountResolver::new(Arc::clone(&adapter) as Arc<dyn ChainAdapter>);
    let mut out = Vec::new();
    for slot in (top.saturating_sub(limit as u64 * 4)..=top).rev() {
        let block = match adapter.get_block(slot).await {
            Ok(block) => block,
            Err(PrismError::SlotSkipped { .. }) => continue,
            Err(err) if err.is_retryable() => continue,
            Err(err) => panic!("slot {slot}: {err}"),
        };
        let mut normalized = normalizer.normalize(&block).expect("the devnet block normalizes");
        let transfers = registry.decode_block(&mut normalized);
        let transfers = resolver.resolve(transfers).await.expect("resolve");
        out.push(SlotRows {
            slot: normalized.slot,
            transactions: normalized.transactions,
            instructions: normalized.instructions,
            transfers,
        });
        if out.len() == limit {
            break;
        }
    }
    out.reverse();
    assert!(!out.is_empty(), "no devnet blocks fetched");
    Some(out)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// Real devnet blocks, flushed and read back: every row on disk equals the
/// row that was buffered, in every table, and the files land under the UTC
/// day of the first block.
#[tokio::test]
async fn live_devnet_blocks_round_trip_through_parquet() {
    let Some(blocks) = live_slot_rows(env_usize("PRISM_STORAGE_BLOCKS", 8)).await else {
        return;
    };
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    blocks.iter().for_each(|rows| rows.append_to(&mut writer));
    let buffered = writer.buffered_row_counts();

    let result = writer.flush().unwrap();

    let (first, last) = (blocks.first().unwrap().slot.slot, blocks.last().unwrap().slot.slot);
    assert_eq!((result.from_slot, result.to_slot), (first, last));
    assert_eq!(
        [result.slots_written, result.transactions_written, result.instructions_written, result.transfers_written],
        buffered.map(|n| n as u64)
    );
    let day = PartitionKey::date_for_block_time(blocks.iter().find_map(|b| b.slot.block_time));
    for path in &result.parquet_paths {
        let key = PartitionKey::from_path(Path::new(path)).expect("a path in the layout");
        assert_eq!((key.date(), key.from_slot(), key.to_slot()), (day, first, last), "{path}");
    }
    assert_disk_holds(&dir, &blocks);

    let m = writer.metrics().snapshot();
    println!(
        "\n  {} devnet blocks (slots {first}..={last}), filed under date={day}\n  \
         rows: {} slots, {} transactions, {} instructions, {} token transfers\n  \
         {} files, {} bytes, flushed in {} ms",
        blocks.len(),
        result.slots_written,
        result.transactions_written,
        result.instructions_written,
        result.transfers_written,
        result.parquet_paths.len(),
        m.total_bytes_written,
        result.duration_ms,
    );
    for path in &result.parquet_paths {
        println!("    {path} ({} bytes)", fs::metadata(path).unwrap().len());
    }
    if result.transfers_written == 0 {
        eprintln!("note: no token transfers in the fetched blocks; raise PRISM_STORAGE_BLOCKS");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// Real devnet blocks flushed in two batches, rolled back to the middle of
/// the second one and replayed: the files then hold every block exactly
/// once.
#[tokio::test]
async fn live_devnet_rollback_and_replay_leave_each_block_once() {
    let Some(blocks) = live_slot_rows(env_usize("PRISM_STORAGE_BLOCKS", 8)).await else {
        return;
    };
    if blocks.len() < 4 {
        eprintln!("note: only {} blocks fetched; need 4 for this test", blocks.len());
        return;
    }
    let dir = temp_dir();
    let mut writer = ParquetWriter::new(&dir);
    let half = blocks.len() / 2;
    for batch in [&blocks[..half], &blocks[half..]] {
        batch.iter().for_each(|rows| rows.append_to(&mut writer));
        writer.flush().unwrap();
    }
    assert_disk_holds(&dir, &blocks);

    // Inside the second file, so it gets cut rather than deleted.
    let cut = half + 1;
    let point = blocks[cut].slot.slot;
    writer.rollback(point).unwrap();
    assert_disk_holds(&dir, &blocks[..cut]);
    assert_eq!(writer.last_flushed_slot(), Some(point - 1));

    blocks[cut..].iter().for_each(|rows| rows.append_to(&mut writer));
    writer.flush().unwrap();
    assert_disk_holds(&dir, &blocks);
    println!(
        "\n  rolled back {} devnet blocks from slot {point} and replayed them; slot files: {:?}",
        blocks.len() - cut,
        slot_file_names(&dir)
    );
    let _ = fs::remove_dir_all(&dir);
}
