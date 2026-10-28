//! Tests for `lens::catalog`, against Parquet files written by PRISM's own
//! `ParquetWriter`.
//!
//! Two tests are the contract between the services:
//!
//! * `schema_consistency_between_prism_and_lens` checks LENS's four schemas
//!   against PRISM's three ways: encoded as Arrow IPC flatbuffers, byte for
//!   byte; against the schema stored in files PRISM wrote; and against the
//!   schema DataFusion reports once the tables are registered. It prints a
//!   column-by-column report:
//!
//!   ```text
//!   cargo test -p lens --test catalog_test schema_consistency -- --nocapture
//!   ```
//!
//! * `all_four_udm_tables_are_registered_after_prism_writes_them` writes
//!   every table with PRISM, registers the catalog, and queries each table.
//!
//! PRISM is built on arrow 60 and LENS on the arrow 59 DataFusion ships, so
//! the two schemas are different Rust types: they are compared through what
//! both versions agree on, the IPC encoding and the Parquet files.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int64Type, Schema, UInt64Type};
use datafusion::arrow::ipc::convert::IpcSchemaEncoder;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::SessionContext;
use lens::{
    CatalogMetrics, LensConfig, LensError, PrismCatalog, PrismTable, RegisterOutcome, SCHEMA_NAME,
    TableKind, TableStatus, ensure_schema, instructions_schema, slots_schema,
    token_transfers_schema, transactions_schema,
};
use prism::storage::partition::CHAIN_DIR;
use prism::storage::writer::schema_for;
use prism::storage::{ParquetWriter, PartitionKey, Storage, TableKind as PrismTableKind};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, unique directory path under the system temp dir, named with
/// `suffix`. Nothing is created.
fn temp_dir_named(suffix: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "lens-catalog-test-{}-{}{suffix}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn temp_dir() -> PathBuf {
    temp_dir_named("")
}

/// 2026-10-27T12:00:00Z.
const NOON_OCT_27: i64 = 1_793_102_400;

/// PRISM's kind for `kind`, matched by name so the two enums are checked
/// against each other rather than assumed equal.
fn prism_kind(kind: TableKind) -> PrismTableKind {
    PrismTableKind::ALL
        .into_iter()
        .find(|prism| prism.as_str() == kind.dir_name())
        .unwrap_or_else(|| panic!("PRISM has no table for LENS's {kind}"))
}

fn lens_schema(kind: TableKind) -> Arc<Schema> {
    match kind {
        TableKind::Slots => slots_schema(),
        TableKind::Transactions => transactions_schema(),
        TableKind::Instructions => instructions_schema(),
        TableKind::TokenTransfers => token_transfers_schema(),
    }
}

/// The rows PRISM would produce for `slot`: 2 transactions, 2 instructions
/// each (one top-level, one inner), and one token transfer. Every nullable
/// column is set on even slots and unset on odd ones.
struct SlotRows {
    slot: DeepAnaSlot,
    transactions: Vec<DeepAnaTransaction>,
    instructions: Vec<DeepAnaInstruction>,
    transfers: Vec<DeepAnaTokenTransfer>,
}

fn slot_rows(slot: u64) -> SlotRows {
    let even = slot.is_multiple_of(2);
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
                accounts: vec![format!("payer{slot}"), "11111111111111111111111111111111".to_owned()],
                log_messages: vec![format!("Program log: slot {slot}")],
                compute_units_consumed: even.then_some(1_000 * u64::from(index + 1)),
            }
        })
        .collect();
    let instructions = transactions
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
    let transfers = vec![DeepAnaTokenTransfer {
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
    }];
    SlotRows {
        slot: DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time,
            block_height: even.then_some(slot - 100),
            transaction_count: 2,
            leader: even.then(|| format!("leader{slot}")),
        },
        transactions,
        instructions,
        transfers,
    }
}

/// Have PRISM's writer append `slots` and flush them as one file per table
/// under `data_dir`.
fn prism_write(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        let rows = slot_rows(slot);
        writer.append(&rows.slot, &rows.transactions, &rows.instructions, &rows.transfers).unwrap();
    }
    writer.flush().unwrap();
}

/// Have PRISM write only slots, with no transactions, so only the `slots`
/// table gets a file.
fn prism_write_empty_slots(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        writer.append(&slot_rows(slot).slot, &[], &[], &[]).unwrap();
    }
    writer.flush().unwrap();
}

fn chain_root(data_dir: &Path) -> PathBuf {
    PartitionKey::chain_root(data_dir)
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|err| panic!("planning `{sql}`: {err}"))
        .collect()
        .await
        .unwrap_or_else(|err| panic!("running `{sql}`: {err}"))
}

/// The single `i64` a query like `SELECT COUNT(*) ...` returns.
async fn scalar_i64(ctx: &SessionContext, sql: &str) -> i64 {
    let batches = query(ctx, sql).await;
    let values: Vec<i64> = batches
        .iter()
        .flat_map(|b| b.column(0).as_primitive::<Int64Type>().iter().map(Option::unwrap))
        .collect();
    assert_eq!(values.len(), 1, "`{sql}` returned {values:?}");
    values[0]
}

async fn count(ctx: &SessionContext, kind: TableKind) -> i64 {
    scalar_i64(ctx, &format!("SELECT COUNT(*) FROM {}", kind.full_name())).await
}

fn status_of(statuses: &[TableStatus], kind: TableKind) -> &TableStatus {
    statuses.iter().find(|s| s.name == kind.full_name()).unwrap()
}

/// The Arrow IPC flatbuffer encoding of a schema, from either arrow version.
macro_rules! ipc_bytes {
    ($encoder:ty, $schema:expr) => {
        <$encoder>::new().schema_to_fb($schema).finished_data().to_vec()
    };
}

// ---------------------------------------------------------------------------
// Schema consistency: PRISM ↔ LENS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn schema_consistency_between_prism_and_lens() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 1_000..1_004);
    let ctx = SessionContext::new();
    PrismCatalog::from_chain_root(chain_root(&data_dir)).register(&ctx).unwrap();

    let mut mismatches = Vec::new();
    println!("\n=== PRISM ↔ LENS schema consistency ===");
    for kind in TableKind::ALL {
        let prism = schema_for(prism_kind(kind));
        let lens = lens_schema(kind);
        println!("\n{} ({} columns in PRISM, {} in LENS)", kind.full_name(), prism.fields().len(), lens.fields().len());

        // 1. Column by column: name, type, nullability.
        println!("  {:>3}  {:<28} {:<34} LENS", "#", "column", "PRISM");
        let width = prism.fields().len().max(lens.fields().len());
        for i in 0..width {
            let p = prism.fields().get(i);
            let l = lens.fields().get(i);
            let render = |ty: String, nullable: bool| {
                format!("{ty} {}", if nullable { "null" } else { "not null" })
            };
            let p_desc = p.map(|f| render(f.data_type().to_string(), f.is_nullable()));
            let l_desc = l.map(|f| render(f.data_type().to_string(), f.is_nullable()));
            let same = match (p, l) {
                (Some(p), Some(l)) => {
                    p.name() == l.name()
                        && p.is_nullable() == l.is_nullable()
                        && p.data_type().to_string() == l.data_type().to_string()
                        && p.metadata() == l.metadata()
                }
                _ => false,
            };
            let name = match (p, l) {
                (Some(p), Some(l)) if p.name() != l.name() => format!("{} ≠ {}", p.name(), l.name()),
                (Some(p), _) => p.name().clone(),
                (None, l) => l.unwrap().name().clone(),
            };
            println!(
                "  {i:>3}  {name:<28} {:<34} {:<34} {}",
                p_desc.unwrap_or_else(|| "-".into()),
                l_desc.unwrap_or_else(|| "-".into()),
                if same { "ok" } else { "MISMATCH" }
            );
            if !same {
                mismatches.push(format!("{kind}: column {i} ({name})"));
            }
        }
        if prism.metadata() != lens.metadata() {
            mismatches.push(format!("{kind}: schema metadata"));
        }

        // 2. Byte for byte: the IPC flatbuffer each version encodes.
        let prism_bytes = ipc_bytes!(arrow::ipc::convert::IpcSchemaEncoder, &prism);
        let lens_bytes = ipc_bytes!(IpcSchemaEncoder, &lens);
        let identical = prism_bytes == lens_bytes;
        println!(
            "  IPC flatbuffer: PRISM {} bytes, LENS {} bytes, {}",
            prism_bytes.len(),
            lens_bytes.len(),
            if identical { "identical" } else { "DIFFERENT" }
        );
        if !identical {
            mismatches.push(format!("{kind}: IPC encoding"));
        }

        // 3. On disk: the Arrow schema stored in the file PRISM wrote, read
        //    with LENS's arrow version.
        let table_root = PartitionKey::table_root(&data_dir, prism_kind(kind));
        let date_dir = fs::read_dir(&table_root).unwrap().next().unwrap().unwrap().path();
        let file = fs::read_dir(&date_dir).unwrap().next().unwrap().unwrap().path();
        let file_schema =
            ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&file).unwrap()).unwrap().schema().clone();
        let on_disk = file_schema.fields() == lens.fields() && file_schema.metadata().is_empty();
        println!(
            "  Parquet file {}: {}",
            file.file_name().unwrap().to_string_lossy(),
            if on_disk { "matches LENS" } else { "DIFFERENT" }
        );
        if !on_disk {
            mismatches.push(format!("{kind}: schema in {}", file.display()));
        }

        // 4. In DataFusion: what a query against the registered table sees.
        let df_schema = ctx.table(kind.full_name()).await.unwrap().schema().as_arrow().clone();
        let registered = df_schema.fields() == lens.fields();
        println!("  DataFusion {}: {}", kind.full_name(), if registered { "matches LENS" } else { "DIFFERENT" });
        if !registered {
            mismatches.push(format!("{kind}: DataFusion table schema"));
        }
    }
    println!();

    assert!(mismatches.is_empty(), "PRISM and LENS schemas differ:\n  {}", mismatches.join("\n  "));
}

#[test]
fn table_kinds_line_up_with_prisms() {
    assert_eq!(TableKind::ALL.len(), PrismTableKind::ALL.len());
    for (lens, prism) in TableKind::ALL.into_iter().zip(PrismTableKind::ALL) {
        assert_eq!(lens.dir_name(), prism.as_str(), "same order, same directories");
        assert_eq!(lens.table_name(), lens.dir_name());
        assert_eq!(lens.full_name(), format!("{CHAIN_DIR}.{}", prism.as_str()));
    }
    assert_eq!(SCHEMA_NAME, CHAIN_DIR);
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn all_four_udm_tables_are_registered_after_prism_writes_them() {
    let data_dir = temp_dir();
    let slots = 2_000..2_006;
    prism_write(&data_dir, slots.clone());
    let expected_rows = |kind| {
        let per_slot = match kind {
            TableKind::Slots => 1,
            TableKind::Transactions => 2,
            TableKind::Instructions => 4,
            TableKind::TokenTransfers => 1,
        };
        per_slot * slots.clone().count() as i64
    };

    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    let report = catalog.register(&ctx).unwrap();

    assert_eq!(report.registered, TableKind::ALL);
    assert!(report.skipped.is_empty() && report.deregistered.is_empty() && report.is_success());
    let metrics = catalog.metrics();
    assert_eq!(metrics.total_refreshes, 1);
    assert_eq!(metrics.tables_registered, 4);
    assert_eq!((metrics.tables_skipped, metrics.tables_failed), (0, 0));
    assert!(catalog.last_refresh().is_some());

    let statuses = catalog.table_status(&ctx).unwrap();
    assert_eq!(statuses.len(), 4);
    for kind in TableKind::ALL {
        assert_eq!(
            *status_of(&statuses, kind),
            TableStatus { name: kind.full_name(), dir_exists: true, file_count: 1, registered: true }
        );
        assert!(ctx.table_exist(kind.full_name().as_str()).unwrap());
        assert_eq!(count(&ctx, kind).await, expected_rows(kind), "{kind}");
    }

    // The tables are listed in information_schema under `solana`.
    let ctx_with_info = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_information_schema(true),
    );
    PrismCatalog::from_chain_root(chain_root(&data_dir)).register(&ctx_with_info).unwrap();
    let batches = query(
        &ctx_with_info,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_schema = 'solana' ORDER BY table_name",
    )
    .await;
    let names: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let col = b.column(0);
            (0..col.len())
                .map(|i| datafusion::arrow::util::display::array_value_to_string(col, i).unwrap())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, ["instructions", "slots", "token_transfers", "transactions"]);
}

#[tokio::test]
async fn registered_tables_return_prisms_values() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 3_000..3_004);
    let ctx = SessionContext::new();
    PrismCatalog::from_chain_root(chain_root(&data_dir)).register(&ctx).unwrap();

    // Nullable columns are set on even slots only.
    let batches = query(
        &ctx,
        "SELECT slot, block_height, CAST(block_time AS BIGINT) AS secs, leader \
         FROM solana.slots ORDER BY slot",
    )
    .await;
    let batch = &batches[0];
    let slots: Vec<u64> = batch.column(0).as_primitive::<UInt64Type>().values().to_vec();
    assert_eq!(slots, [3_000, 3_001, 3_002, 3_003]);
    let heights: Vec<Option<u64>> = batch.column(1).as_primitive::<UInt64Type>().iter().collect();
    assert_eq!(heights, [Some(2_900), None, Some(2_902), None]);
    let secs: Vec<Option<i64>> = batch.column(2).as_primitive::<Int64Type>().iter().collect();
    assert_eq!(secs, [Some(NOON_OCT_27 + 3_000), None, Some(NOON_OCT_27 + 3_002), None]);
    assert_eq!(batch.column(3).null_count(), 2);

    // block_time is a UTC timestamp, comparable with timestamp literals.
    assert_eq!(
        scalar_i64(&ctx, "SELECT COUNT(*) FROM solana.slots WHERE block_time >= TIMESTAMP '2026-10-27T00:00:00Z'").await,
        2
    );

    // Filters, aggregates and joins across tables.
    assert_eq!(scalar_i64(&ctx, "SELECT COUNT(*) FROM solana.transactions WHERE status = 'failed'").await, 2);
    assert_eq!(
        scalar_i64(&ctx, "SELECT CAST(SUM(amount_raw) AS BIGINT) FROM solana.token_transfers").await,
        (3_000..3_004).map(|s: i64| s * 10).sum::<i64>()
    );
    assert_eq!(
        scalar_i64(
            &ctx,
            "SELECT COUNT(*) FROM solana.instructions i \
             JOIN solana.transactions t ON i.tx_signature = t.tx_signature \
             WHERE i.inner_instruction_index IS NULL AND t.slot = 3002"
        )
        .await,
        2
    );
    // JSON columns come back as the strings PRISM wrote.
    let batches = query(&ctx, "SELECT accounts FROM solana.transactions WHERE tx_signature = 'sig3001_0'").await;
    let accounts = datafusion::arrow::util::display::array_value_to_string(batches[0].column(0), 0).unwrap();
    assert_eq!(accounts, r#"["payer3001","11111111111111111111111111111111"]"#);
}

#[test]
fn construction_touches_no_files() {
    let data_dir = temp_dir();
    let catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));

    assert!(!data_dir.exists(), "constructing the catalog created {}", data_dir.display());
    assert_eq!(catalog.tables().len(), 4);
    for (table, kind) in catalog.tables().iter().zip(TableKind::ALL) {
        assert_eq!(table.kind(), kind);
        assert_eq!(table.dir(), data_dir.join(CHAIN_DIR).join(kind.dir_name()));
        assert_eq!(catalog.table(kind).kind(), kind);
        assert!(!table.dir_exists());
    }
    assert_eq!(catalog.metrics(), CatalogMetrics::default());
    assert!(catalog.last_refresh().is_none());
    assert!(catalog.is_refresh_due(Duration::from_secs(3_600)));
}

#[tokio::test]
async fn catalog_built_from_config_reads_the_configured_data_dir() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 4_000..4_002);
    let data_dir_str = data_dir.to_str().unwrap().to_owned();
    let config = LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data_dir_str.clone().into())).unwrap();

    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::new(&config);
    assert_eq!(catalog.table(TableKind::Slots).dir(), config.table_dir("slots"));
    assert_eq!(catalog.register(&ctx).unwrap().registered, TableKind::ALL);
    assert_eq!(count(&ctx, TableKind::Slots).await, 2);
}

#[tokio::test]
async fn tables_without_a_directory_are_skipped() {
    let data_dir = temp_dir();
    // No transactions, so PRISM writes only the slots table.
    prism_write_empty_slots(&data_dir, 5_000..5_003);
    assert!(PartitionKey::table_root(&data_dir, PrismTableKind::Slots).is_dir());
    assert!(!PartitionKey::table_root(&data_dir, PrismTableKind::Transactions).exists());

    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    let report = catalog.register(&ctx).unwrap();
    assert_eq!(report.registered, [TableKind::Slots]);
    assert_eq!(report.skipped, [TableKind::Transactions, TableKind::Instructions, TableKind::TokenTransfers]);
    assert_eq!((catalog.metrics().tables_registered, catalog.metrics().tables_skipped), (1, 3));

    let statuses = catalog.table_status(&ctx).unwrap();
    assert_eq!(
        *status_of(&statuses, TableKind::Transactions),
        TableStatus { name: "solana.transactions".into(), dir_exists: false, file_count: 0, registered: false }
    );
    assert!(status_of(&statuses, TableKind::Slots).registered);
    assert_eq!(count(&ctx, TableKind::Slots).await, 3);
    let err = ctx.sql("SELECT * FROM solana.transactions").await.unwrap_err();
    assert!(err.to_string().contains("transactions"), "{err}");
}

#[tokio::test]
async fn a_table_directory_without_files_is_registered_and_empty() {
    let data_dir = temp_dir();
    for kind in TableKind::ALL {
        fs::create_dir_all(chain_root(&data_dir).join(kind.dir_name())).unwrap();
    }
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    assert_eq!(catalog.register(&ctx).unwrap().registered, TableKind::ALL);

    for kind in TableKind::ALL {
        assert_eq!(count(&ctx, kind).await, 0, "{kind}");
        // The schema is the declared one, not one inferred from no files.
        let schema = ctx.table(kind.full_name()).await.unwrap().schema().as_arrow().clone();
        assert_eq!(schema.fields(), lens_schema(kind).fields());
    }
    assert!(catalog.table_status(&ctx).unwrap().iter().all(|s| s.file_count == 0 && s.registered));
}

// ---------------------------------------------------------------------------
// Refresh
// ---------------------------------------------------------------------------

#[tokio::test]
async fn refresh_picks_up_files_prism_wrote_after_registration() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 6_000..6_002);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    catalog.register(&ctx).unwrap();
    assert_eq!(count(&ctx, TableKind::Slots).await, 2);

    prism_write(&data_dir, 6_002..6_005);
    // DataFusion caches the file listing and never expires it, so without a
    // refresh the new file stays invisible. This is why refresh exists; if
    // this starts failing, DataFusion's caching changed.
    assert_eq!(count(&ctx, TableKind::Slots).await, 2, "new files seen without a refresh");

    let report = catalog.refresh(&ctx);
    assert_eq!(report.registered, TableKind::ALL);
    assert_eq!(catalog.metrics().total_refreshes, 2);
    for kind in TableKind::ALL {
        assert_eq!(status_of(&catalog.table_status(&ctx).unwrap(), kind).file_count, 2, "{kind}");
    }
    assert_eq!(count(&ctx, TableKind::Slots).await, 5);
    assert_eq!(count(&ctx, TableKind::Instructions).await, 20);
}

#[tokio::test]
async fn refresh_registers_tables_whose_directory_appeared_later() {
    let data_dir = temp_dir();
    prism_write_empty_slots(&data_dir, 7_000..7_002);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    assert_eq!(catalog.register(&ctx).unwrap().registered, [TableKind::Slots]);

    prism_write(&data_dir, 7_002..7_004);
    let report = catalog.refresh(&ctx);
    assert_eq!(report.registered, TableKind::ALL);
    assert!(report.skipped.is_empty());
    assert_eq!(catalog.metrics().tables_skipped, 0);
    assert_eq!(count(&ctx, TableKind::Transactions).await, 4);
    assert_eq!(count(&ctx, TableKind::Slots).await, 4);
}

#[tokio::test]
async fn refresh_deregisters_tables_whose_directory_disappeared() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 8_000..8_002);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    catalog.register(&ctx).unwrap();

    fs::remove_dir_all(catalog.table(TableKind::TokenTransfers).dir()).unwrap();
    let report = catalog.refresh(&ctx);
    assert_eq!(report.deregistered, [TableKind::TokenTransfers]);
    assert_eq!(report.skipped, [TableKind::TokenTransfers]);
    assert_eq!(report.registered.len(), 3);
    assert!(!catalog.table(TableKind::TokenTransfers).is_registered(&ctx));
    assert!(!status_of(&catalog.table_status(&ctx).unwrap(), TableKind::TokenTransfers).registered);

    // A second refresh has nothing left to deregister.
    assert!(catalog.refresh(&ctx).deregistered.is_empty());
}

#[test]
fn tables_stay_queryable_while_being_refreshed() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 9_000..9_002);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    catalog.register(&ctx).unwrap();

    // With deregister-then-register there would be a moment with no table;
    // the replacement must be atomic.
    let done = Arc::new(AtomicBool::new(false));
    let watcher = {
        let (ctx, done) = (ctx.clone(), done.clone());
        std::thread::spawn(move || {
            let mut checks = 0u64;
            while !done.load(Ordering::Relaxed) {
                for kind in TableKind::ALL {
                    assert!(ctx.table_exist(kind.table_reference()).unwrap(), "{kind} vanished mid-refresh");
                }
                checks += 1;
            }
            checks
        })
    };
    for _ in 0..200 {
        assert!(catalog.refresh(&ctx).is_success());
    }
    done.store(true, Ordering::Relaxed);
    let checks = watcher.join().expect("a table disappeared during a refresh");
    assert!(checks > 0);
}

#[tokio::test]
async fn in_progress_files_are_neither_counted_nor_read() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 10_000..10_002);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    catalog.register(&ctx).unwrap();

    // What a PRISM flush looks like before its rename: a `.parquet.tmp`
    // next to the finished files. A copy of a real file, so reading it would
    // change the count.
    let table_root = PartitionKey::table_root(&data_dir, PrismTableKind::Slots);
    let date_dir = fs::read_dir(&table_root).unwrap().next().unwrap().unwrap().path();
    let real = fs::read_dir(&date_dir).unwrap().next().unwrap().unwrap().path();
    fs::copy(&real, date_dir.join("slots_10002_10003.parquet.tmp")).unwrap();

    catalog.refresh(&ctx);
    assert_eq!(status_of(&catalog.table_status(&ctx).unwrap(), TableKind::Slots).file_count, 1);
    assert_eq!(count(&ctx, TableKind::Slots).await, 2);
}

#[tokio::test]
async fn data_dirs_with_glob_characters_are_taken_literally() {
    let data_dir = temp_dir_named("-[weird]*?");
    prism_write(&data_dir, 11_000..11_003);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));
    assert_eq!(catalog.register(&ctx).unwrap().registered, TableKind::ALL);
    assert_eq!(count(&ctx, TableKind::Slots).await, 3);
}

#[test]
fn refresh_if_due_respects_the_interval() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 12_000..12_001);
    let ctx = SessionContext::new();
    let mut catalog = PrismCatalog::from_chain_root(chain_root(&data_dir));

    assert!(catalog.refresh_if_due(&ctx, Duration::from_secs(3_600)).is_some(), "due before the first pass");
    assert!(catalog.refresh_if_due(&ctx, Duration::from_secs(3_600)).is_none());
    assert!(catalog.refresh_if_due(&ctx, Duration::ZERO).is_some());
    assert_eq!(catalog.metrics().total_refreshes, 2);
}

#[tokio::test(start_paused = true)]
async fn refresh_loop_refreshes_every_interval_until_cancelled() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 13_000..13_002);
    let ctx = SessionContext::new();
    let catalog = Arc::new(Mutex::new(PrismCatalog::from_chain_root(chain_root(&data_dir))));
    catalog.lock().unwrap().register(&ctx).unwrap();

    let interval = Duration::from_secs(30);
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(PrismCatalog::run_refresh_loop(
        catalog.clone(),
        ctx.clone(),
        interval,
        shutdown.clone(),
    ));
    let refreshes = || catalog.lock().unwrap().metrics().total_refreshes;

    // Nothing before the first interval has passed.
    tokio::time::sleep(interval - Duration::from_secs(1)).await;
    assert_eq!(refreshes(), 1);

    tokio::time::sleep(interval * 3).await;
    assert_eq!(refreshes(), 1 + 3);

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), task).await.expect("loop did not stop").unwrap();
    tokio::time::sleep(interval * 2).await;
    assert_eq!(refreshes(), 1 + 3, "refreshed after being cancelled");
}

// ---------------------------------------------------------------------------
// The `solana` schema
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_foreign_solana_schema_is_reported_not_replaced() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 14_000..14_001);
    let ctx = SessionContext::new();
    ctx.sql("CREATE SCHEMA solana").await.unwrap();

    let err = PrismCatalog::from_chain_root(chain_root(&data_dir)).register(&ctx).unwrap_err();
    assert!(matches!(err, LensError::CatalogError { .. }), "{err}");
    assert!(err.to_string().contains("not created by LENS"), "{err}");

    // `refresh` does not fail as a whole but reports every table.
    let report = PrismCatalog::from_chain_root(chain_root(&data_dir)).refresh(&ctx);
    assert_eq!(report.failed.len(), 4);
    assert!(!report.is_success());
}

#[tokio::test]
async fn ensure_schema_is_idempotent() {
    let ctx = SessionContext::new();
    let first = ensure_schema(&ctx).unwrap();
    let second = ensure_schema(&ctx).unwrap();
    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn a_single_table_registers_and_deregisters_on_its_own() {
    let data_dir = temp_dir();
    prism_write(&data_dir, 15_000..15_002);
    let ctx = SessionContext::new();
    let table = PrismTable::new(TableKind::Transactions, chain_root(&data_dir));

    assert!(!table.is_registered(&ctx), "no solana schema yet");
    assert_eq!(table.register_with(&ctx).unwrap(), RegisterOutcome::Registered);
    assert_eq!(table.register_with(&ctx).unwrap(), RegisterOutcome::Registered, "re-registering replaces");
    assert!(table.is_registered(&ctx));
    assert_eq!(count(&ctx, TableKind::Transactions).await, 4);

    assert!(table.deregister_from(&ctx).unwrap());
    assert!(!table.deregister_from(&ctx).unwrap());
    assert!(!table.is_registered(&ctx));

    let missing = PrismTable::new(TableKind::Transactions, temp_dir());
    assert_eq!(missing.register_with(&ctx).unwrap(), RegisterOutcome::SkippedMissingDir);
}
