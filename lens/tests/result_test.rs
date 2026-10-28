//! Tests for `lens::result`: `serialize`, `serialize_json` and
//! `serialize_csv`.
//!
//! Most tests build a `QueryResult` by hand, with every Arrow type PRISM's
//! schemas use. The `engine_*` tests run SQL over tables PRISM wrote, so the
//! batches are exactly what `QueryEngine::execute` hands the API.
//!
//! The `*_roundtrip_*` tests decode the serializer's output and compare it,
//! cell by cell, with the `RecordBatch`es it came from:
//!
//! * JSON: parsed with `serde_json`; each value is checked against the Arrow
//!   cell by type (integers as exact `u64` / `i64`, timestamps as instants).
//! * CSV: read back with Arrow's CSV reader against the result's schema and
//!   compared with the original batches as arrays.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::DateTime;
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::compute::{cast, concat_batches};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::{
    DataType, Field, Float64Type, Int32Type, Int64Type, Schema, SchemaRef, TimeUnit,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use lens::{
    LensConfig, QueryEngine, QueryFormat, QueryResult, serialize, serialize_csv, serialize_json,
};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// 2026-10-27T12:00:00Z.
const NOON_OCT_27: i64 = 1_793_102_400;

/// The first integer an `f64` cannot hold exactly.
const ABOVE_F64_EXACT: u64 = (1 << 53) + 1;

/// One column of each Arrow type PRISM writes, nullable where PRISM's are.
fn udm_like_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("amount_raw", DataType::UInt64, true),
        Field::new("transaction_count", DataType::UInt32, false),
        Field::new("decimals", DataType::UInt8, true),
        Field::new("is_vote", DataType::Boolean, true),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("leader", DataType::Utf8, true),
        Field::new(
            "block_time",
            DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
            true,
        ),
    ]))
}

/// Rows for [`udm_like_schema`], covering integer extremes, nulls, and
/// strings with JSON and CSV metacharacters. No string is empty: see
/// [`csv_writes_a_null_and_an_empty_string_the_same_way`].
fn udm_like_batch(schema: &SchemaRef, offset: u64) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(vec![295_000_000 + offset, u64::MAX, ABOVE_F64_EXACT, 0])),
        Arc::new(UInt64Array::from(vec![
            Some(18_446_744_073_709_551_615),
            None,
            Some(9_007_199_254_740_993),
            Some(1_000_000_000_000_000_007),
        ])),
        Arc::new(UInt32Array::from(vec![12, u32::MAX, 0, 8])),
        Arc::new(UInt8Array::from(vec![Some(9), None, Some(u8::MAX), Some(0)])),
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None, Some(true)])),
        Arc::new(StringArray::from(vec![
            "4vJ9ShXk",
            "comma, \"quoted\" and\nnewline",
            "back\\slash\ttab",
            "ünïcødé ✓ 🚀",
        ])),
        Arc::new(StringArray::from(vec![Some("leader1"), None, Some("{\"json\":1}"), None])),
        Arc::new(
            TimestampSecondArray::from(vec![Some(NOON_OCT_27), None, Some(0), Some(NOON_OCT_27 + 59)])
                .with_timezone("UTC"),
        ),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).unwrap()
}

fn query_result(schema: SchemaRef, batches: Vec<RecordBatch>, sql: &str) -> QueryResult {
    let row_count = batches.iter().map(RecordBatch::num_rows).sum();
    QueryResult {
        sql: sql.to_owned(),
        batches,
        row_count,
        schema,
        duration_ms: 47,
        truncated: false,
        total_rows_before_limit: Some(row_count),
    }
}

/// Two batches of [`udm_like_batch`].
fn udm_like_result() -> QueryResult {
    let schema = udm_like_schema();
    let batches = vec![udm_like_batch(&schema, 0), udm_like_batch(&schema, 1)];
    query_result(schema, batches, "SELECT * FROM solana.token_transfers")
}

fn empty_result() -> QueryResult {
    query_result(udm_like_schema(), Vec::new(), "SELECT * FROM solana.slots WHERE false")
}

// ---------------------------------------------------------------------------
// Roundtrip checks
// ---------------------------------------------------------------------------

/// Parse `bytes` as the JSON wrapper and assert it holds exactly `result`:
/// the metadata fields, and one object per row whose keys are the columns
/// and whose values equal the Arrow cells.
fn assert_json_roundtrip(result: &QueryResult, bytes: &[u8]) {
    let json: Value = serde_json::from_slice(bytes).expect("serializer output is valid JSON");
    let object = json.as_object().expect("top level is an object");
    let keys: Vec<&str> = object.keys().map(String::as_str).collect();
    assert_eq!(
        sorted(keys),
        sorted(vec!["rows", "row_count", "duration_ms", "truncated", "sql"]),
    );
    assert_eq!(json["row_count"].as_u64(), Some(result.row_count as u64));
    assert_eq!(json["duration_ms"].as_u64(), Some(result.duration_ms));
    assert_eq!(json["truncated"].as_bool(), Some(result.truncated));
    assert_eq!(json["sql"].as_str(), Some(result.sql.as_str()));

    let rows = json["rows"].as_array().expect("rows is an array");
    assert_eq!(rows.len(), result.row_count, "one JSON object per row");

    let columns = result.column_names();
    let mut rows = rows.iter();
    for (b, batch) in result.batches.iter().enumerate() {
        for row in 0..batch.num_rows() {
            let object = rows.next().unwrap().as_object().expect("each row is an object");
            assert_eq!(
                sorted(object.keys().map(String::as_str).collect()),
                sorted(columns.clone()),
                "batch {b} row {row}: every column, nulls included, and nothing else"
            );
            for (c, name) in columns.iter().enumerate() {
                assert_json_cell(batch.column(c), row, &object[*name], &format!("batch {b} row {row} `{name}`"));
            }
        }
    }
}

/// Assert `value` is the JSON encoding of `array[row]`.
fn assert_json_cell(array: &ArrayRef, row: usize, value: &Value, at: &str) {
    if array.is_null(row) {
        assert!(value.is_null(), "{at}: expected null, got {value}");
        return;
    }
    match array.data_type() {
        DataType::UInt8 => assert_eq!(value.as_u64(), Some(u64::from(array.as_primitive::<UInt8Type>().value(row))), "{at}"),
        DataType::UInt16 => assert_eq!(value.as_u64(), Some(u64::from(array.as_primitive::<UInt16Type>().value(row))), "{at}"),
        DataType::UInt32 => assert_eq!(value.as_u64(), Some(u64::from(array.as_primitive::<UInt32Type>().value(row))), "{at}"),
        // `as_u64` is `None` for a float, so this also proves the value was
        // written as an integer literal.
        DataType::UInt64 => assert_eq!(value.as_u64(), Some(array.as_primitive::<UInt64Type>().value(row)), "{at}"),
        DataType::Int32 => assert_eq!(value.as_i64(), Some(i64::from(array.as_primitive::<Int32Type>().value(row))), "{at}"),
        DataType::Int64 => assert_eq!(value.as_i64(), Some(array.as_primitive::<Int64Type>().value(row)), "{at}"),
        DataType::Float64 => assert_eq!(value.as_f64(), Some(array.as_primitive::<Float64Type>().value(row)), "{at}"),
        DataType::Boolean => assert_eq!(value.as_bool(), Some(array.as_boolean().value(row)), "{at}"),
        DataType::Utf8 => assert_eq!(value.as_str(), Some(array.as_string::<i32>().value(row)), "{at}"),
        DataType::LargeUtf8 => assert_eq!(value.as_str(), Some(array.as_string::<i64>().value(row)), "{at}"),
        DataType::Utf8View => assert_eq!(value.as_str(), Some(array.as_string_view().value(row)), "{at}"),
        DataType::Timestamp(TimeUnit::Second, _) => {
            let text = value.as_str().unwrap_or_else(|| panic!("{at}: timestamp is not a string: {value}"));
            let parsed = DateTime::parse_from_rfc3339(text)
                .unwrap_or_else(|e| panic!("{at}: `{text}` is not RFC 3339: {e}"));
            assert_eq!(parsed.timestamp(), array.as_primitive::<TimestampSecondType>().value(row), "{at}");
        }
        other => panic!("{at}: no JSON check for {other}"),
    }
}

/// Read `bytes` back with Arrow's CSV reader and assert it holds exactly
/// the rows of `result`, column by column, with the header naming the
/// columns in order.
fn assert_csv_roundtrip(result: &QueryResult, bytes: &[u8]) {
    let text = std::str::from_utf8(bytes).expect("CSV is UTF-8");
    let header = text.lines().next().expect("CSV has a header line");
    assert_eq!(header, result.column_names().join(","));

    // The reader has no view types; compare those as plain strings.
    let read_schema: SchemaRef = Arc::new(Schema::new(
        result
            .schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                DataType::Utf8View | DataType::LargeUtf8 => f.as_ref().clone().with_data_type(DataType::Utf8),
                _ => f.as_ref().clone(),
            })
            .collect::<Vec<_>>(),
    ));
    let reader = csv::ReaderBuilder::new(Arc::clone(&read_schema))
        .with_header(true)
        .build(bytes)
        .unwrap();
    let read: Vec<RecordBatch> = reader.collect::<Result<_, _>>().expect("CSV reads back");
    let read = concat_batches(&read_schema, &read).unwrap();
    let original = concat_batches(&result.schema, &result.batches).unwrap();

    assert_eq!(read.num_rows(), result.row_count, "one CSV line per row");
    for (c, field) in read_schema.fields().iter().enumerate() {
        let expected = cast(original.column(c), field.data_type()).unwrap();
        assert_eq!(read.column(c).as_ref(), expected.as_ref(), "column `{}`", field.name());
    }
}

fn sorted(mut v: Vec<&str>) -> Vec<&str> {
    v.sort_unstable();
    v
}

// ---------------------------------------------------------------------------
// QueryFormat
// ---------------------------------------------------------------------------

#[test]
fn query_format_parses_names_and_defaults_to_json() {
    assert_eq!(QueryFormat::from_str("csv"), QueryFormat::Csv);
    assert_eq!(QueryFormat::from_str(" CSV "), QueryFormat::Csv);
    assert_eq!(QueryFormat::from_str("json"), QueryFormat::Json);
    assert_eq!(QueryFormat::from_str("JSON"), QueryFormat::Json);
    assert_eq!(QueryFormat::from_str(""), QueryFormat::Json);
    assert_eq!(QueryFormat::from_str("parquet"), QueryFormat::Json);
    assert_eq!(QueryFormat::default(), QueryFormat::Json);
}

#[test]
fn query_format_content_types() {
    assert_eq!(QueryFormat::Json.content_type(), "application/json");
    assert_eq!(QueryFormat::Csv.content_type(), "text/csv; charset=utf-8");
}

// ---------------------------------------------------------------------------
// serialize
// ---------------------------------------------------------------------------

#[test]
fn serialize_dispatches_and_carries_the_metadata() {
    let mut result = udm_like_result();
    result.truncated = true;
    result.total_rows_before_limit = None;

    let json = serialize(result.clone(), QueryFormat::Json).unwrap();
    assert_eq!(json.format, QueryFormat::Json);
    assert_eq!(json.bytes, serialize_json(&result).unwrap());

    let csv = serialize(result.clone(), QueryFormat::Csv).unwrap();
    assert_eq!(csv.format, QueryFormat::Csv);
    assert_eq!(csv.bytes, serialize_csv(&result).unwrap());

    for out in [&json, &csv] {
        assert_eq!(out.row_count, 8);
        assert_eq!(out.duration_ms, 47);
        assert!(out.truncated);
        assert_eq!(out.sql, result.sql);
    }
}

// ---------------------------------------------------------------------------
// JSON
// ---------------------------------------------------------------------------

#[test]
fn json_wraps_the_rows_in_an_object_with_the_metadata() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("blockhash", DataType::Utf8, false),
        Field::new("transaction_count", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![295_000_000, 295_000_001])),
            Arc::new(StringArray::from(vec!["4vJ9Sh", "7kM2Rp"])),
            Arc::new(UInt32Array::from(vec![12, 8])),
        ],
    )
    .unwrap();
    let sql = "SELECT slot, blockhash, transaction_count FROM solana.slots LIMIT 2";
    let result = query_result(schema, vec![batch], sql);

    let json = String::from_utf8(serialize_json(&result).unwrap()).unwrap();
    assert_eq!(
        json,
        "{\"rows\":[\
         {\"slot\":295000000,\"blockhash\":\"4vJ9Sh\",\"transaction_count\":12},\
         {\"slot\":295000001,\"blockhash\":\"7kM2Rp\",\"transaction_count\":8}],\
         \"row_count\":2,\"duration_ms\":47,\"truncated\":false,\
         \"sql\":\"SELECT slot, blockhash, transaction_count FROM solana.slots LIMIT 2\"}"
    );
    assert_json_roundtrip(&result, json.as_bytes());
}

#[test]
fn json_roundtrip_matches_the_record_batches() {
    let result = udm_like_result();
    let bytes = serialize_json(&result).unwrap();
    assert_json_roundtrip(&result, &bytes);
}

#[test]
fn json_roundtrip_keeps_u64_values_an_f64_cannot_hold() {
    let result = udm_like_result();
    let text = String::from_utf8(serialize_json(&result).unwrap()).unwrap();
    // As literal digits in the body, not just after parsing.
    assert!(text.contains("\"slot\":18446744073709551615"), "{text}");
    assert!(text.contains("\"amount_raw\":9007199254740993"), "{text}");
    assert!(text.contains("\"amount_raw\":1000000000000000007"), "{text}");

    let json: Value = serde_json::from_str(&text).unwrap();
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows[1]["slot"].as_u64(), Some(u64::MAX));
    assert_eq!(rows[2]["slot"].as_u64(), Some(ABOVE_F64_EXACT));
    assert_ne!(ABOVE_F64_EXACT as f64 as u64, ABOVE_F64_EXACT, "an f64 would have changed it");
}

#[test]
fn json_writes_nulls_explicitly() {
    let result = udm_like_result();
    let json: Value = serde_json::from_slice(&serialize_json(&result).unwrap()).unwrap();
    let second = &json["rows"][1];
    for column in ["amount_raw", "decimals", "leader", "block_time"] {
        assert_eq!(second.get(column), Some(&Value::Null), "`{column}`");
    }
}

#[test]
fn json_of_an_empty_result_has_no_rows_and_all_the_metadata() {
    let result = empty_result();
    let bytes = serialize_json(&result).unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        "{\"rows\":[],\"row_count\":0,\"duration_ms\":47,\"truncated\":false,\
         \"sql\":\"SELECT * FROM solana.slots WHERE false\"}"
    );
    assert_json_roundtrip(&result, &bytes);
}

#[test]
fn json_skips_empty_batches_between_full_ones() {
    let schema = udm_like_schema();
    let batches = vec![
        RecordBatch::new_empty(Arc::clone(&schema)),
        udm_like_batch(&schema, 0),
        RecordBatch::new_empty(Arc::clone(&schema)),
        udm_like_batch(&schema, 1),
    ];
    let result = query_result(schema.clone(), batches, "SELECT 1");
    assert_json_roundtrip(&result, &serialize_json(&result).unwrap());

    let only_empty = query_result(schema.clone(), vec![RecordBatch::new_empty(schema)], "SELECT 1");
    let json: Value = serde_json::from_slice(&serialize_json(&only_empty).unwrap()).unwrap();
    assert_eq!(json["rows"], Value::Array(Vec::new()));
}

#[test]
fn json_escapes_the_sql_text() {
    let mut result = empty_result();
    result.sql = "SELECT '\"quoted\"', 'back\\slash'\nFROM t -- ✓".to_owned();
    let bytes = serialize_json(&result).unwrap();
    assert_json_roundtrip(&result, &bytes);
}

#[test]
fn json_roundtrip_handles_signed_and_float_columns() {
    // Not in PRISM's schemas, but what aggregates (`COUNT`, `AVG`) return.
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, false),
        Field::new("avg", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![i64::MIN, -1, i64::MAX])),
            Arc::new(Float64Array::from(vec![Some(0.1), None, Some(-1.5e300)])),
        ],
    )
    .unwrap();
    let result = query_result(schema, vec![batch], "SELECT COUNT(*), AVG(fee)");
    assert_json_roundtrip(&result, &serialize_json(&result).unwrap());
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

#[test]
fn csv_writes_a_header_then_quoted_rows() {
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("leader", DataType::Utf8, true),
        Field::new("block_time", DataType::Timestamp(TimeUnit::Second, Some("UTC".into())), true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![u64::MAX, 7])),
            Arc::new(StringArray::from(vec![Some("a, \"b\""), None])),
            Arc::new(TimestampSecondArray::from(vec![Some(NOON_OCT_27), None]).with_timezone("UTC")),
        ],
    )
    .unwrap();
    let result = query_result(schema, vec![batch], "SELECT 1");

    let bytes = serialize_csv(&result).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "slot,leader,block_time");
    assert!(lines[1].starts_with("18446744073709551615,\"a, \"\"b\"\"\",2026-10-27T12:00:00"), "{}", lines[1]);
    assert_eq!(lines[2], "7,,");
    assert_eq!(lines.len(), 3);
    assert_csv_roundtrip(&result, &bytes);
}

#[test]
fn csv_roundtrip_matches_the_record_batches() {
    let result = udm_like_result();
    let bytes = serialize_csv(&result).unwrap();
    assert_csv_roundtrip(&result, &bytes);
}

#[test]
fn csv_header_appears_once_across_batches() {
    let result = udm_like_result();
    let text = String::from_utf8(serialize_csv(&result).unwrap()).unwrap();
    assert_eq!(text.matches("slot,amount_raw,").count(), 1);
}

#[test]
fn csv_of_an_empty_result_is_the_header_alone() {
    let result = empty_result();
    let bytes = serialize_csv(&result).unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        "slot,amount_raw,transaction_count,decimals,is_vote,blockhash,leader,block_time\n"
    );
    assert_csv_roundtrip(&result, &bytes);
}

#[test]
fn csv_writes_a_null_and_an_empty_string_the_same_way() {
    // A limitation of CSV itself, pinned here so a change is noticed.
    let schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("leader", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some(""), None])),
        ],
    )
    .unwrap();
    let result = query_result(schema, vec![batch], "SELECT 1");
    let text = String::from_utf8(serialize_csv(&result).unwrap()).unwrap();
    assert_eq!(text, "id,leader\n1,\n2,\n");
}

// ---------------------------------------------------------------------------
// End to end: PRISM's tables through QueryEngine
// ---------------------------------------------------------------------------

/// A scratch data directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-result-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Token amounts that do not fit an `f64`, as real 9-decimal mints produce.
fn amount_for(slot: u64) -> u64 {
    u64::MAX - slot
}

/// Have PRISM write one row per table for each of `slots`. Nullable columns
/// are set on even slots only, and strings carry CSV / JSON metacharacters.
fn prism_write(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        let even = slot % 2 == 0;
        let block_time = even.then_some(NOON_OCT_27 + slot as i64);
        let tx = DeepAnaTransaction {
            slot,
            blockhash: format!("hash{slot}"),
            transaction_index: 0,
            tx_signature: format!("sig{slot}"),
            fee: 5_000,
            status: if even { TransactionStatus::Success } else { TransactionStatus::Failed },
            err: (!even).then(|| "InstructionError(0, Custom(1)), \"quoted\"".to_owned()),
            accounts: vec![format!("payer{slot}")],
            log_messages: vec![format!("Program log: slot {slot}\nsecond line")],
            compute_units_consumed: even.then_some(1_000),
        };
        let instruction = DeepAnaInstruction {
            slot,
            tx_signature: tx.tx_signature.clone(),
            instruction_index: 0,
            inner_instruction_index: (!even).then_some(0),
            program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            accounts: vec![format!("src{slot}"), format!("dst{slot}")],
            data_base58: "3Bxs4h24hBtQy9rw".to_owned(),
            instruction_type: even.then(|| "transfer".to_owned()),
            decoded_data: even.then(|| serde_json::json!({ "amount": amount_for(slot) })),
        };
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: tx.tx_signature.clone(),
            instruction_index: 0,
            inner_instruction_index: (!even).then_some(0),
            spl_token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            source_token_account: format!("src{slot}"),
            destination_token_account: format!("dst{slot}"),
            source_owner: even.then(|| format!("owner_src{slot}")),
            destination_owner: even.then(|| format!("owner_dst{slot}")),
            mint_token_address: even.then(|| "So11111111111111111111111111111111111111112".to_owned()),
            amount_raw: amount_for(slot),
            decimals: even.then_some(9),
            block_time,
        };
        let slot_row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time,
            block_height: even.then_some(slot - 100),
            transaction_count: 1,
            leader: even.then(|| format!("leader{slot}")),
        };
        writer.append(&slot_row, &[tx], &[instruction], &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

fn engine(data_dir: &Path, max_query_rows: &str) -> QueryEngine {
    let data_dir = data_dir.to_str().unwrap().to_owned();
    let max_query_rows = max_query_rows.to_owned();
    let config = LensConfig::load_with(|name| match name {
        "LENS_DATA_DIR" => Some(data_dir.clone().into()),
        "LENS_MAX_QUERY_ROWS" => Some(max_query_rows.clone().into()),
        _ => None,
    })
    .unwrap();
    QueryEngine::new(Arc::new(config)).unwrap()
}

async fn run(engine: &QueryEngine, sql: &str) -> QueryResult {
    engine.execute(sql).await.unwrap_or_else(|err| panic!("`{sql}` failed: {err}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_results_roundtrip_through_json_and_csv_for_every_table() {
    let dir = TempDir::new();
    prism_write(dir.path(), 1_000..1_010);
    let engine = engine(dir.path(), "1000");

    for table in ["slots", "transactions", "instructions", "token_transfers"] {
        let sql = format!("SELECT * FROM solana.{table} ORDER BY slot");
        let result = run(&engine, &sql).await;
        assert_eq!(result.row_count, 10, "{table}");
        assert_eq!(result.sql, sql);

        let json = serialize(result.clone(), QueryFormat::Json).unwrap();
        assert_json_roundtrip(&result, &json.bytes);
        let csv = serialize(result.clone(), QueryFormat::Csv).unwrap();
        assert_csv_roundtrip(&result, &csv.bytes);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_amounts_above_2_pow_53_survive_json_exactly() {
    let dir = TempDir::new();
    prism_write(dir.path(), 1_000..1_004);
    let engine = engine(dir.path(), "1000");

    let result = run(&engine, "SELECT slot, amount_raw FROM solana.token_transfers ORDER BY slot").await;
    let json: Value = serde_json::from_slice(&serialize_json(&result).unwrap()).unwrap();
    let amounts: Vec<u64> =
        json["rows"].as_array().unwrap().iter().map(|r| r["amount_raw"].as_u64().unwrap()).collect();
    assert_eq!(amounts, (1_000..1_004).map(amount_for).collect::<Vec<_>>());

    let csv = String::from_utf8(serialize_csv(&result).unwrap()).unwrap();
    assert!(csv.contains(&format!("1000,{}\n", amount_for(1_000))), "{csv}");
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_truncated_and_empty_results_serialize() {
    let dir = TempDir::new();
    prism_write(dir.path(), 1_000..1_010);
    let engine = engine(dir.path(), "3");

    let truncated = run(&engine, "SELECT slot, leader FROM solana.slots ORDER BY slot").await;
    assert!(truncated.truncated);
    assert_eq!(truncated.total_rows_before_limit, None);
    let json = serialize(truncated.clone(), QueryFormat::Json).unwrap();
    assert!(json.truncated);
    assert_json_roundtrip(&truncated, &json.bytes);
    assert_csv_roundtrip(&truncated, &serialize_csv(&truncated).unwrap());

    let empty = run(&engine, "SELECT slot, leader FROM solana.slots WHERE slot > 99999").await;
    assert_eq!((empty.row_count, empty.truncated, empty.total_rows_before_limit), (0, false, Some(0)));
    let json: Value = serde_json::from_slice(&serialize_json(&empty).unwrap()).unwrap();
    assert_eq!(json["rows"], Value::Array(Vec::new()));
    assert_eq!(String::from_utf8(serialize_csv(&empty).unwrap()).unwrap(), "slot,leader\n");
}
