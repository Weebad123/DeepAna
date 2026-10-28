//! Tests for `lens::engine`, end to end: PRISM's `ParquetWriter` writes the
//! tables, `QueryEngine::new` registers them, and SQL runs through
//! `QueryEngine::execute`.
//!
//! The two success-rate tests measure the rate the client sees and the rate
//! the engine reports, print a per-query and per-table report, and assert
//! that the two agree with the expected rate:
//!
//! ```text
//! cargo test -p lens --test engine_test success_rate -- --nocapture
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, UInt64Type};
use lens::{
    EngineMetricsSnapshot, LensConfig, LensError, MIN_MEMORY_LIMIT_BYTES, QueryEngine, QueryResult,
    SessionSettings, TableKind,
};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A scratch data directory, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-engine-test-{}-{}",
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

/// 2026-10-27T12:00:00Z.
const NOON_OCT_27: i64 = 1_793_102_400;

/// The slots [`dataset`] writes: 10 slots, so 20 transactions (5 failed),
/// 40 instructions and 10 token transfers.
const SLOTS: std::ops::Range<u64> = 1_000..1_010;

/// Have PRISM write `slots`. Per slot: 2 transactions (the second fails on
/// odd slots), 2 instructions per transaction (one top-level, one inner),
/// one token transfer. Nullable columns are set on even slots only.
fn prism_write(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        let even = slot % 2 == 0;
        let block_time = even.then_some(NOON_OCT_27 + slot as i64);
        let transactions: Vec<DeepAnaTransaction> = (0..2u32)
            .map(|index| {
                let failed = index == 1 && !even;
                DeepAnaTransaction {
                    slot,
                    blockhash: format!("hash{slot}"),
                    transaction_index: index,
                    tx_signature: format!("sig{slot}_{index}"),
                    fee: 5_000 + u64::from(index),
                    status: if failed { TransactionStatus::Failed } else { TransactionStatus::Success },
                    err: failed.then(|| "InstructionError(0, Custom(1))".to_owned()),
                    accounts: vec![format!("payer{slot}")],
                    log_messages: vec![format!("Program log: slot {slot}")],
                    compute_units_consumed: even.then_some(1_000 * u64::from(index + 1)),
                }
            })
            .collect();
        let instructions: Vec<DeepAnaInstruction> = transactions
            .iter()
            .flat_map(|tx| {
                [None, Some(0u32)].map(|inner| DeepAnaInstruction {
                    slot,
                    tx_signature: tx.tx_signature.clone(),
                    instruction_index: 0,
                    inner_instruction_index: inner,
                    program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
                    accounts: vec![format!("src{slot}"), format!("dst{slot}")],
                    data_base58: "3Bxs4h24hBtQy9rw".to_owned(),
                    instruction_type: even.then(|| "transfer".to_owned()),
                    decoded_data: even.then(|| serde_json::json!({ "amount": slot })),
                })
            })
            .collect();
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: transactions[0].tx_signature.clone(),
            instruction_index: 0,
            inner_instruction_index: (!even).then_some(0),
            spl_token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            source_token_account: format!("src{slot}"),
            destination_token_account: format!("dst{slot}"),
            source_owner: even.then(|| format!("owner_src{slot}")),
            destination_owner: even.then(|| format!("owner_dst{slot}")),
            mint_token_address: even.then(|| "So11111111111111111111111111111111111111112".to_owned()),
            amount_raw: slot * 10,
            decimals: even.then_some(9),
            block_time,
        };
        let slot_row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time,
            block_height: even.then_some(slot - 100),
            transaction_count: 2,
            leader: even.then(|| format!("leader{slot}")),
        };
        writer.append(&slot_row, &transactions, &instructions, &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

/// Have PRISM write only slot rows, so only the `slots` table exists.
fn prism_write_slots_only(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: None,
            block_height: None,
            transaction_count: 0,
            leader: None,
        };
        writer.append(&row, &[], &[], &[]).unwrap();
    }
    writer.flush().unwrap();
}

/// A data directory with every table written for [`SLOTS`].
fn dataset() -> TempDir {
    let dir = TempDir::new();
    prism_write(dir.path(), SLOTS);
    dir
}

/// A config for `data_dir`, with `vars` on top of the defaults.
fn config(data_dir: &Path, vars: &[(&str, &str)]) -> Arc<LensConfig> {
    let data_dir = data_dir.to_str().unwrap().to_owned();
    let config = LensConfig::load_with(|name| {
        if name == "LENS_DATA_DIR" {
            return Some(data_dir.clone().into());
        }
        vars.iter().find(|(key, _)| *key == name).map(|(_, value)| (*value).into())
    })
    .unwrap();
    Arc::new(config)
}

fn engine(data_dir: &Path, vars: &[(&str, &str)]) -> QueryEngine {
    QueryEngine::new(config(data_dir, vars)).unwrap()
}

async fn ok(engine: &QueryEngine, sql: &str) -> QueryResult {
    engine.execute(sql).await.unwrap_or_else(|err| panic!("`{sql}` failed: {err}"))
}

/// The single `i64` a `SELECT COUNT(*) ...` returns.
async fn count(engine: &QueryEngine, sql: &str) -> i64 {
    let result = ok(engine, sql).await;
    assert_eq!(result.row_count, 1, "`{sql}`");
    result.batches[0].column(0).as_primitive::<Int64Type>().value(0)
}

fn u64_column(result: &QueryResult, column: usize) -> Vec<u64> {
    result
        .batches
        .iter()
        .flat_map(|b| b.column(column).as_primitive::<UInt64Type>().values().to_vec())
        .collect()
}

/// Wait for every query in flight to be counted, then snapshot.
fn settled(engine: &QueryEngine) -> EngineMetricsSnapshot {
    let snapshot = engine.metrics_snapshot();
    assert_eq!(snapshot.total_queries, snapshot.completed_queries(), "a query is still counted as running");
    snapshot
}

/// A query that cannot finish within any test timeout. `SUM` cannot be
/// answered from statistics, so every value is generated.
const ENDLESS_SQL: &str = "SELECT SUM(value) FROM generate_series(1, 1000000000000)";

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn new_registers_every_table_prism_wrote() {
    let data = dataset();
    let engine = engine(data.path(), &[]);

    let metrics = engine.catalog_metrics();
    assert_eq!((metrics.total_refreshes, metrics.tables_registered, metrics.tables_skipped), (1, 4, 0));
    for status in engine.table_status().unwrap() {
        assert!(status.registered && status.file_count == 1, "{status:?}");
    }
    assert_eq!(engine.metrics_snapshot().total_queries, 0);
    assert_eq!(engine.metrics_snapshot().success_rate, 1.0);
}

#[tokio::test]
async fn new_skips_tables_prism_has_not_written_yet() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), SLOTS);
    let engine = engine(data.path(), &[]);

    assert_eq!(engine.catalog_metrics().tables_registered, 1);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 10);
    let err = engine.execute("SELECT COUNT(*) FROM solana.transactions").await.unwrap_err();
    assert_eq!(err.kind(), "query_plan_error", "{err}");
}

#[test]
fn construction_fails_on_invalid_session_settings() {
    let data = dataset();
    let settings = SessionSettings { memory_limit_bytes: 1, ..SessionSettings::default() };
    let err = QueryEngine::with_settings(config(data.path(), &[]), &settings).unwrap_err();
    assert_eq!(err.config_field(), Some("memory_limit_bytes"), "{err}");
}

#[tokio::test]
async fn session_options_are_in_effect() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let result = ok(
        &engine,
        "SELECT name, value FROM information_schema.df_settings WHERE name IN (\
           'datafusion.execution.parquet.pushdown_filters', \
           'datafusion.execution.parquet.reorder_filters', \
           'datafusion.execution.parquet.bloom_filter_on_read', \
           'datafusion.execution.parquet.enable_page_index', \
           'datafusion.execution.collect_statistics') ORDER BY name",
    )
    .await;
    assert_eq!(result.row_count, 5);
    let batch = &result.batches[0];
    let names = batch.column(0).as_string::<i32>();
    let values = batch.column(1).as_string::<i32>();
    for row in 0..batch.num_rows() {
        assert_eq!(values.value(row), "true", "{}", names.value(row));
    }
}

// ---------------------------------------------------------------------------
// execute: results
// ---------------------------------------------------------------------------

#[tokio::test]
async fn execute_returns_rows_schema_and_metadata() {
    let data = dataset();
    let engine = engine(data.path(), &[]);

    let result = ok(&engine, "  SELECT slot, blockhash FROM solana.slots ORDER BY slot;  ").await;
    assert_eq!(result.column_names(), ["slot", "blockhash"]);
    assert_eq!(result.column_count(), 2);
    assert_eq!(result.row_count, 10);
    assert!(!result.truncated);
    assert_eq!(u64_column(&result, 0), SLOTS.collect::<Vec<_>>());
    assert_eq!(result.batches.iter().map(RecordBatch::num_rows).sum::<usize>(), result.row_count);

    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.transactions WHERE status = 'failed'").await, 5);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.instructions").await, 40);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.token_transfers").await, 10);
}

#[tokio::test]
async fn an_empty_result_keeps_its_schema() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let result = ok(&engine, "SELECT slot, leader FROM solana.slots WHERE slot > 999999").await;
    assert_eq!((result.row_count, result.truncated, result.batches.len()), (0, false, 0));
    assert_eq!(result.column_names(), ["slot", "leader"]);
}

#[tokio::test]
async fn results_over_max_query_rows_are_truncated() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_MAX_QUERY_ROWS", "7")]);

    let over = ok(&engine, "SELECT * FROM solana.instructions").await;
    assert_eq!((over.row_count, over.truncated), (7, true));
    assert_eq!(over.batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 7);

    let exact = ok(&engine, "SELECT * FROM solana.instructions LIMIT 7").await;
    assert_eq!((exact.row_count, exact.truncated), (7, false));

    let under = ok(&engine, "SELECT slot FROM solana.slots WHERE slot < 1003").await;
    assert_eq!((under.row_count, under.truncated), (3, false));

    // The limit sits on top of the query: ORDER BY is applied first.
    let top = ok(&engine, "SELECT slot FROM solana.transactions ORDER BY slot DESC").await;
    assert!(top.truncated);
    assert_eq!(u64_column(&top, 0), [1009, 1009, 1008, 1008, 1007, 1007, 1006]);

    // A user LIMIT above the engine's is cut down too.
    let user_limit = ok(&engine, "SELECT * FROM solana.instructions LIMIT 30").await;
    assert_eq!((user_limit.row_count, user_limit.truncated), (7, true));

    assert_eq!(settled(&engine).total_rows_returned, 7 + 7 + 3 + 7 + 7);
}

// ---------------------------------------------------------------------------
// execute: errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bad_sql_is_a_plan_error_and_counts_as_failed() {
    let data = dataset();
    let engine = engine(data.path(), &[]);

    for sql in [
        "SELECT * FROM solana.no_such_table",
        "SELECT no_such_column FROM solana.slots",
        "SELEC slot FROM solana.slots",
        "SELECT slot FROM solana.slots WHERE",
        "SELECT 1; SELECT 2",
    ] {
        let err = engine.execute(sql).await.unwrap_err();
        assert!(matches!(&err, LensError::QueryPlanError { sql: s, .. } if s == sql), "`{sql}`: {err:?}");
    }
    let metrics = settled(&engine);
    assert_eq!((metrics.total_queries, metrics.failed_queries, metrics.successful_queries), (5, 5, 0));
    assert_eq!(metrics.success_rate, 0.0);
}

#[tokio::test]
async fn statements_that_change_anything_are_rejected() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let copy_target = data.path().join("exfiltrated.parquet");
    let secret_dir = data.path().join("secret");
    fs::create_dir_all(&secret_dir).unwrap();

    let statements = [
        format!("CREATE EXTERNAL TABLE leak STORED AS PARQUET LOCATION '{}/'", secret_dir.display()),
        format!("COPY (SELECT * FROM solana.slots) TO '{}'", copy_target.display()),
        "CREATE TABLE stolen AS SELECT * FROM solana.slots".to_owned(),
        "CREATE VIEW v AS SELECT 1".to_owned(),
        "INSERT INTO solana.slots (slot) VALUES (1)".to_owned(),
        "DELETE FROM solana.slots".to_owned(),
        "DROP TABLE solana.slots".to_owned(),
        "DROP SCHEMA solana CASCADE".to_owned(),
        "SET datafusion.execution.parquet.pushdown_filters = false".to_owned(),
    ];
    for sql in &statements {
        let err = engine.execute(sql).await.unwrap_err();
        assert_eq!(err.kind(), "query_plan_error", "`{sql}` should be rejected: {err}");
    }

    assert!(!copy_target.exists(), "COPY wrote a file");
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 10);
    let pushdown = ok(
        &engine,
        "SELECT value FROM information_schema.df_settings \
         WHERE name = 'datafusion.execution.parquet.pushdown_filters'",
    )
    .await;
    assert_eq!(pushdown.batches[0].column(0).as_string::<i32>().value(0), "true");
    assert_eq!(settled(&engine).failed_queries, statements.len() as u64);
}

#[tokio::test]
async fn invalid_input_is_rejected_before_it_counts() {
    let data = dataset();
    let engine = engine(data.path(), &[]);

    for sql in ["", "   ", "\n\t "] {
        let err = engine.execute(sql).await.unwrap_err();
        assert_eq!(err.kind(), "query_plan_error");
    }
    let huge = format!("SELECT '{}'", "x".repeat(lens::MAX_SQL_BYTES));
    assert_eq!(engine.execute(&huge).await.unwrap_err().kind(), "query_plan_error");

    assert_eq!(engine.metrics_snapshot().total_queries, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_over_the_timeout_is_cancelled_and_the_engine_carries_on() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);

    let started = Instant::now();
    let err = engine.execute(ENDLESS_SQL).await.unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        matches!(&err, LensError::QueryTimeout { sql, timeout_secs: 1 } if sql == ENDLESS_SQL),
        "{err:?}"
    );
    assert!(elapsed >= Duration::from_secs(1) && elapsed < Duration::from_secs(3), "{elapsed:?}");

    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 10);
    let metrics = settled(&engine);
    assert_eq!((metrics.timed_out_queries, metrics.failed_queries, metrics.successful_queries), (1, 0, 1));
    assert!(metrics.p99_duration_ms >= 1_000, "{metrics:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_over_the_memory_limit_fails_without_taking_the_engine_down() {
    let data = dataset();
    let settings = SessionSettings {
        memory_limit_bytes: MIN_MEMORY_LIMIT_BYTES,
        max_spill_bytes: 0,
        target_partitions: 2,
    };
    let engine = QueryEngine::with_settings(config(data.path(), &[]), &settings).unwrap();

    // A hash join holds its whole build side in memory and cannot spill:
    // four million rows plus their hash table are far more than 16 MiB.
    // (Not a GROUP BY: generate_series is sorted, so DataFusion aggregates
    // it in streaming mode with next to no memory.)
    let err = engine
        .execute(
            "SELECT COUNT(*) FROM generate_series(1, 4000000) a \
             JOIN generate_series(1, 4000000) b ON a.value = b.value",
        )
        .await
        .unwrap_err();
    assert_eq!(err.kind(), "query_execution_error", "{err}");
    assert!(err.to_string().contains("Resources exhausted"), "{err}");

    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.transactions").await, 20);
    assert_eq!(settled(&engine).failed_queries, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_whose_caller_gives_up_counts_as_failed() {
    let data = dataset();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "60")]);

    let gave_up = tokio::time::timeout(Duration::from_millis(200), engine.execute(ENDLESS_SQL)).await;
    assert!(gave_up.is_err());

    let metrics = settled(&engine);
    assert_eq!((metrics.total_queries, metrics.failed_queries, metrics.timed_out_queries), (1, 1, 0));
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 10);
}

// ---------------------------------------------------------------------------
// Catalog refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn refresh_catalog_makes_new_files_and_tables_queryable() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), 1..4);
    let engine = engine(data.path(), &[]);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 3);

    prism_write(data.path(), SLOTS);
    // Cached file listing: the new file is not visible until a refresh.
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 3);

    let report = engine.refresh_catalog();
    assert!(report.is_success());
    assert_eq!(report.registered, TableKind::ALL);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.slots").await, 13);
    assert_eq!(count(&engine, "SELECT COUNT(*) FROM solana.transactions").await, 20);
    assert_eq!(engine.catalog_metrics().total_refreshes, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_refresh_picks_up_new_tables_and_stops_on_shutdown() {
    let data = TempDir::new();
    prism_write_slots_only(data.path(), 1..4);
    let engine = engine(data.path(), &[]);
    let handle = engine.start_catalog_refresh(1);

    prism_write(data.path(), SLOTS);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(result) = engine.execute("SELECT COUNT(*) FROM solana.transactions").await {
            assert_eq!(result.batches[0].column(0).as_primitive::<Int64Type>().value(0), 20);
            break;
        }
        assert!(Instant::now() < deadline, "the background refresh never registered transactions");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(engine.catalog_metrics().total_refreshes >= 2);

    engine.shutdown();
    assert!(engine.shutdown_token().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the refresh task did not stop after shutdown")
        .unwrap();
    let refreshes = engine.catalog_metrics().total_refreshes;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    assert_eq!(engine.catalog_metrics().total_refreshes, refreshes, "refreshed after shutdown");
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_queries_are_each_counted_once() {
    let data = dataset();
    let engine = Arc::new(engine(data.path(), &[]));

    let tasks: Vec<_> = (0..48)
        .map(|i| {
            let engine = Arc::clone(&engine);
            tokio::spawn(async move {
                let sql = if i % 4 == 3 {
                    "SELECT * FROM solana.missing".to_owned()
                } else {
                    format!("SELECT slot FROM solana.slots WHERE slot <= {}", 1_000 + i % 10)
                };
                engine.execute(&sql).await.map(|result| result.row_count)
            })
        })
        .collect();
    let mut expected_rows = 0;
    let mut expected_ok = 0;
    for (i, task) in tasks.into_iter().enumerate() {
        match task.await.unwrap() {
            Ok(rows) => {
                assert_eq!(rows, i % 10 + 1);
                expected_rows += rows as u64;
                expected_ok += 1;
            }
            Err(err) => assert_eq!((i % 4, err.kind()), (3, "query_plan_error")),
        }
    }

    let metrics = settled(&engine);
    assert_eq!(metrics.total_queries, 48);
    assert_eq!((metrics.successful_queries, metrics.failed_queries), (expected_ok, 48 - expected_ok));
    assert_eq!(metrics.total_rows_returned, expected_rows);
    assert_eq!(metrics.success_rate, expected_ok as f64 / 48.0);
}

#[tokio::test]
async fn latency_metrics_follow_the_queries() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    for _ in 0..20 {
        ok(&engine, "SELECT * FROM solana.transactions").await;
    }
    let metrics = settled(&engine);
    assert_eq!(metrics.total_rows_returned, 20 * 20);
    assert!(metrics.p50_duration_ms <= metrics.p99_duration_ms, "{metrics:?}");
    assert!(metrics.last_query_duration_ms <= metrics.p99_duration_ms, "{metrics:?}");
}

// ---------------------------------------------------------------------------
// SQL success rate against the registered tables
// ---------------------------------------------------------------------------

/// One query of a success-rate workload.
struct Case {
    /// The table the query is about, for the per-table breakdown; `None`
    /// for queries spanning tables or the information schema.
    table: Option<TableKind>,
    sql: String,
    /// `Some(rows)`: must succeed and return exactly `rows` rows.
    /// `None`: must fail.
    expect_rows: Option<usize>,
}

fn pass(table: Option<TableKind>, sql: &str, rows: usize) -> Case {
    Case { table, sql: sql.to_owned(), expect_rows: Some(rows) }
}

fn fail(table: Option<TableKind>, sql: &str) -> Case {
    Case { table, sql: sql.to_owned(), expect_rows: None }
}

/// Realistic queries over every registered table, with the rows each
/// returns for [`dataset`].
fn valid_workload() -> Vec<Case> {
    use TableKind::*;
    vec![
        pass(Some(Slots), "SELECT COUNT(*) FROM solana.slots", 1),
        pass(Some(Slots), "SELECT slot, blockhash, leader FROM solana.slots WHERE slot BETWEEN 1002 AND 1005 ORDER BY slot", 4),
        pass(Some(Slots), "SELECT MIN(slot), MAX(slot), SUM(transaction_count) FROM solana.slots", 1),
        pass(Some(Slots), "SELECT date_trunc('day', block_time) AS day, COUNT(*) FROM solana.slots GROUP BY day ORDER BY day NULLS LAST", 2),
        pass(Some(Slots), "SELECT * FROM solana.slots WHERE leader IS NOT NULL", 5),
        pass(Some(Transactions), "SELECT COUNT(*) FROM solana.transactions", 1),
        pass(Some(Transactions), "SELECT status, COUNT(*) AS n, SUM(fee) AS fees FROM solana.transactions GROUP BY status ORDER BY status", 2),
        pass(Some(Transactions), "SELECT tx_signature, err FROM solana.transactions WHERE err IS NOT NULL", 5),
        pass(Some(Transactions), "SELECT * FROM solana.transactions WHERE tx_signature = 'sig1004_1'", 1),
        pass(Some(Transactions), "SELECT AVG(compute_units_consumed) FROM solana.transactions", 1),
        pass(Some(Instructions), "SELECT COUNT(*) FROM solana.instructions", 1),
        pass(Some(Instructions), "SELECT program_id, COUNT(*) FROM solana.instructions GROUP BY program_id", 1),
        pass(Some(Instructions), "SELECT * FROM solana.instructions WHERE inner_instruction_index IS NULL", 20),
        pass(Some(Instructions), "SELECT instruction_type, COUNT(*) FROM solana.instructions GROUP BY instruction_type", 2),
        pass(Some(Instructions), "SELECT tx_signature, decoded_data FROM solana.instructions WHERE decoded_data LIKE '%amount%' ORDER BY slot LIMIT 4", 4),
        pass(Some(TokenTransfers), "SELECT COUNT(*), SUM(amount_raw) FROM solana.token_transfers", 1),
        pass(Some(TokenTransfers), "SELECT mint_token_address, SUM(amount_raw) FROM solana.token_transfers GROUP BY mint_token_address", 2),
        pass(Some(TokenTransfers), "SELECT * FROM solana.token_transfers ORDER BY slot DESC LIMIT 3", 3),
        pass(Some(TokenTransfers), "SELECT source_owner FROM solana.token_transfers WHERE decimals = 9 AND amount_raw > 10040", 2),
        pass(None, "SELECT s.slot, COUNT(t.tx_signature) FROM solana.slots s JOIN solana.transactions t ON s.slot = t.slot GROUP BY s.slot ORDER BY s.slot", 10),
        pass(None, "SELECT t.status, SUM(x.amount_raw) FROM solana.token_transfers x JOIN solana.transactions t ON x.tx_signature = t.tx_signature GROUP BY t.status", 1),
        pass(None, "SELECT i.slot, COUNT(*) FROM solana.instructions i WHERE i.slot IN (SELECT slot FROM solana.slots WHERE leader IS NULL) GROUP BY i.slot", 5),
        pass(None, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'solana' ORDER BY table_name", 4),
    ]
}

/// What one workload run measured.
struct Measured {
    succeeded: usize,
    total: usize,
    engine: EngineMetricsSnapshot,
}

impl Measured {
    fn rate(&self) -> f64 {
        self.succeeded as f64 / self.total as f64
    }
}

/// Run `cases` in order, printing one line per query and a per-table and
/// overall summary. Panics with every query whose outcome or row count was
/// not as expected.
async fn run_workload(title: &str, engine: &QueryEngine, cases: &[Case]) -> Measured {
    let percent = |n: usize, d: usize| if d == 0 { 100.0 } else { 100.0 * n as f64 / d as f64 };
    let label = |table: Option<TableKind>| table.map_or_else(|| "(cross-table)".to_owned(), TableKind::full_name);

    println!("\n=== {title} ===");
    println!("  {:>3}  {:<22} {:<8} {:>5} {:>6}  sql", "#", "table", "outcome", "rows", "ms");
    let mut unexpected = Vec::new();
    let mut per_table: Vec<(String, usize, usize)> = Vec::new();
    let mut succeeded = 0;
    for (i, case) in cases.iter().enumerate() {
        let started = Instant::now();
        let outcome = engine.execute(&case.sql).await;
        let ms = started.elapsed().as_millis();

        let (status, rows) = match &outcome {
            Ok(result) => ("ok", result.row_count.to_string()),
            Err(err) => (err.kind().trim_start_matches("query_").trim_end_matches("_error"), "-".to_owned()),
        };
        println!("  {:>3}  {:<22} {:<8} {:>5} {:>6}  {}", i + 1, label(case.table), status, rows, ms, case.sql);

        let name = label(case.table);
        let entry = match per_table.iter_mut().find(|(n, ..)| *n == name) {
            Some(entry) => entry,
            None => {
                per_table.push((name, 0, 0));
                per_table.last_mut().unwrap()
            }
        };
        entry.2 += 1;
        match (&outcome, case.expect_rows) {
            (Ok(result), Some(rows)) if result.row_count == rows => {}
            (Ok(result), Some(rows)) => {
                unexpected.push(format!("#{}: {} rows, expected {rows}: {}", i + 1, result.row_count, case.sql));
            }
            (Err(err), Some(_)) => unexpected.push(format!("#{}: failed: {err}", i + 1)),
            (Ok(_), None) => unexpected.push(format!("#{}: succeeded, expected failure: {}", i + 1, case.sql)),
            (Err(_), None) => {}
        }
        if outcome.is_ok() {
            succeeded += 1;
            entry.1 += 1;
        }
    }

    let engine_metrics = settled(engine);
    println!("\n  success rate per table:");
    for (name, ok, total) in &per_table {
        println!("    {name:<22} {ok:>3}/{total:<3} {:>7.2}%", percent(*ok, *total));
    }
    println!(
        "\n  measured by client: {succeeded}/{} = {:.2}%",
        cases.len(),
        percent(succeeded, cases.len())
    );
    println!(
        "  reported by engine: {}/{} = {:.2}%  (failed {}, timed out {}, rows {}, p50 {}ms, p99 {}ms)\n",
        engine_metrics.successful_queries,
        engine_metrics.completed_queries(),
        engine_metrics.success_rate * 100.0,
        engine_metrics.failed_queries,
        engine_metrics.timed_out_queries,
        engine_metrics.total_rows_returned,
        engine_metrics.p50_duration_ms,
        engine_metrics.p99_duration_ms,
    );

    assert!(unexpected.is_empty(), "unexpected outcomes:\n  {}", unexpected.join("\n  "));
    Measured { succeeded, total: cases.len(), engine: engine_metrics }
}

/// Every well-formed query against a registered table succeeds with the
/// right number of rows: 100%, as seen by the client and by the engine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_success_rate_against_registered_tables_is_100_percent() {
    let data = dataset();
    let engine = engine(data.path(), &[]);
    let cases = valid_workload();

    // Every table the workload queries is registered, and each is covered.
    for status in engine.table_status().unwrap() {
        assert!(status.registered, "{status:?}");
    }
    for kind in TableKind::ALL {
        assert!(cases.iter().filter(|c| c.table == Some(kind)).count() >= 4, "{kind} is under-covered");
    }

    let measured = run_workload("SQL success rate: valid queries over registered tables", &engine, &cases).await;

    assert_eq!(measured.rate(), 1.0);
    assert_eq!(measured.engine.success_rate, 1.0);
    assert_eq!(measured.engine.total_queries, cases.len() as u64);
    assert_eq!(measured.engine.successful_queries, measured.succeeded as u64);
    assert_eq!((measured.engine.failed_queries, measured.engine.timed_out_queries), (0, 0));
}

/// A workload mixing valid queries with ones that must fail in each way
/// a query can: the engine's success rate equals the expected one exactly,
/// and failures and timeouts both count against it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_success_rate_reflects_failures_and_timeouts_exactly() {
    use TableKind::*;
    let data = TempDir::new();
    // Every table but token_transfers: its queries must fail as unregistered.
    prism_write(data.path(), SLOTS);
    fs::remove_dir_all(data.path().join("solana/token_transfers")).unwrap();
    let engine = engine(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]);
    assert_eq!(engine.catalog_metrics().tables_registered, 3);

    let mut cases: Vec<Case> = valid_workload()
        .into_iter()
        .filter(|c| c.table.is_some() && c.table != Some(TokenTransfers))
        .collect();
    let valid = cases.len();
    cases.extend([
        fail(Some(TokenTransfers), "SELECT COUNT(*) FROM solana.token_transfers"),
        fail(Some(TokenTransfers), "SELECT * FROM solana.token_transfers LIMIT 1"),
        fail(Some(Slots), "SELECT no_such_column FROM solana.slots"),
        fail(Some(Slots), "SELEC * FROM solana.slots"),
        fail(Some(Transactions), "SELECT fee + status FROM solana.transactions"),
        fail(Some(Transactions), "DROP TABLE solana.transactions"),
        fail(Some(Instructions), "INSERT INTO solana.instructions (slot) VALUES (1)"),
        fail(None, "SELECT * FROM solana.no_such_table"),
        fail(None, ENDLESS_SQL),
    ]);
    let invalid = cases.len() - valid;

    let measured = run_workload("SQL success rate: mixed workload", &engine, &cases).await;

    let expected = valid as f64 / (valid + invalid) as f64;
    assert_eq!(measured.succeeded, valid);
    assert_eq!(measured.rate(), expected);
    assert_eq!(measured.engine.success_rate, expected);
    assert_eq!(measured.engine.total_queries, cases.len() as u64);
    assert_eq!(measured.engine.successful_queries, valid as u64);
    assert_eq!(measured.engine.failed_queries, invalid as u64 - 1);
    assert_eq!(measured.engine.timed_out_queries, 1);

    // The failures did not damage anything: the valid queries still all pass.
    let before = measured.engine;
    for case in cases.iter().filter(|c| c.expect_rows.is_some()) {
        assert_eq!(ok(&engine, &case.sql).await.row_count, case.expect_rows.unwrap(), "{}", case.sql);
    }
    let after = settled(&engine);
    assert_eq!(after.successful_queries, before.successful_queries + valid as u64);
    assert!(after.success_rate > before.success_rate);
}
