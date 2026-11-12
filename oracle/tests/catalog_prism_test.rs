//! The catalog against what PRISM really writes and LENS really serves.
//!
//! * the catalog's four schemas equal PRISM's writer schemas, column for
//!   column: name, order, Arrow type, nullability;
//! * they equal the schemas in the footers of the Parquet files PRISM's
//!   `ParquetWriter` puts on disk;
//! * they equal what LENS reports in `information_schema.columns` over those
//!   files, and the schemas LENS scans with;
//! * every measure, alone and under every compatible dimension, and every
//!   filter, runs on the real LENS over those files and gives the numbers
//!   computed by hand from the rows written.

mod common;

use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, TimeUnit};
use common::TempDir;
use forge::lens_client::{LensApi, LensClient};
use lens::{LensConfig, QueryEngine};
use oracle::catalog::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prism::storage::{FlushResult, ParquetWriter, Storage, TableKind as PrismTable};
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use serde_json::{Map, Value, json};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Schemas
// ---------------------------------------------------------------------------

fn prism_table(t: Table) -> PrismTable {
    match t {
        Table::Slots => PrismTable::Slots,
        Table::Transactions => PrismTable::Transactions,
        Table::Instructions => PrismTable::Instructions,
        Table::TokenTransfers => PrismTable::TokenTransfers,
    }
}

/// The Arrow type a catalog column type stands for.
fn arrow_type(t: ColType) -> DataType {
    match t {
        ColType::U8 => DataType::UInt8,
        ColType::U32 => DataType::UInt32,
        ColType::U64 => DataType::UInt64,
        ColType::I64 => DataType::Int64,
        ColType::Utf8 => DataType::Utf8,
        ColType::Bool => DataType::Boolean,
        ColType::TimestampSecs => DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
    }
}

#[track_caller]
fn assert_schema_matches(what: &str, table: Table, schema: &arrow_schema::Schema) {
    let got: Vec<(&str, DataType, bool)> =
        schema.fields().iter().map(|f| (f.name().as_str(), f.data_type().clone(), f.is_nullable())).collect();
    let want: Vec<(&str, DataType, bool)> =
        table.columns().iter().map(|c| (c.name, arrow_type(c.ty), c.nullable)).collect();
    assert_eq!(got, want, "{what}: {table}");
}

#[test]
fn the_catalog_schemas_are_prisms_writer_schemas() {
    for t in Table::ALL {
        assert_schema_matches("PRISM's writer", t, &prism::storage::writer::schema_for(prism_table(t)));
        assert_eq!(prism_table(t).as_str(), t.name());
    }
}

#[test]
fn the_catalog_schemas_are_the_schemas_in_prisms_parquet_files() {
    let dir = TempDir::new();
    let flushed = write_chain(dir.path());
    assert_eq!(flushed.parquet_paths.len(), 4, "one file per table");
    // `parquet_paths` is in `TableKind::ALL` order.
    for (t, path) in Table::ALL.into_iter().zip(&flushed.parquet_paths) {
        assert!(path.contains(&format!("/{}/", t.name())), "{path} is not a {t} file");
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap()).unwrap();
        assert_schema_matches(&format!("the footer of {path}"), t, reader.schema());
        let rows: usize = reader.build().unwrap().map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, rows_written(t), "{t}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_catalog_schemas_are_what_lens_serves() {
    let dir = TempDir::new();
    write_chain(dir.path());
    let lens = RealLens::start(dir.path()).await;
    let client = lens.client();
    for t in Table::ALL {
        let rows = query(
            &client,
            &format!(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns \
                 WHERE table_schema = 'solana' AND table_name = '{}' ORDER BY ordinal_position",
                t.name()
            ),
        )
        .await;
        let got: Vec<(String, String, String)> = rows
            .iter()
            .map(|r| (str_of(&r["column_name"]), str_of(&r["data_type"]), str_of(&r["is_nullable"])))
            .collect();
        let want: Vec<(String, String, String)> = t
            .columns()
            .iter()
            .map(|c| (c.name.to_owned(), lens_type_name(c.ty), if c.nullable { "YES" } else { "NO" }.to_owned()))
            .collect();
        assert_eq!(got, want, "information_schema for {t}");

        // The schema LENS scans the files with.
        let scanned: Vec<(String, String, bool)> = lens::TableKind::from_name(t.sql_name())
            .unwrap()
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), format!("{:?}", f.data_type()), f.is_nullable()))
            .collect();
        let want: Vec<(String, String, bool)> =
            t.columns().iter().map(|c| (c.name.to_owned(), format!("{:?}", arrow_type(c.ty)), c.nullable)).collect();
        assert_eq!(scanned, want, "LENS's scan schema for {t}");
    }
    lens.stop().await;
}

/// How DataFusion names a column type in `information_schema`.
fn lens_type_name(t: ColType) -> String {
    match t {
        ColType::TimestampSecs => r#"Timestamp(s, "UTC")"#.to_owned(),
        other => arrow_type(other).to_string(),
    }
}

// ---------------------------------------------------------------------------
// The chain
// ---------------------------------------------------------------------------

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const LO: u64 = 100;
const HI: u64 = 103;
/// Fees and amounts big enough that their sums overflow `u64`.
const BIG: u64 = 10_000_000_000_000_000_000;
const NINE: u64 = 9_000_000_000_000_000_000;

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

fn mint1() -> String {
    key(30)
}

fn mint2() -> String {
    key(31)
}

fn rows_written(t: Table) -> usize {
    match t {
        Table::Slots => 4,
        Table::Transactions => 5,
        Table::Instructions => 5,
        Table::TokenTransfers => 4,
    }
}

/// Slots 100..=103:
///
/// | slot | leader | txs (fee, status, compute units)               | instructions (program, inner, type)         | transfers (mint, amount, owners)    |
/// |------|--------|------------------------------------------------|---------------------------------------------|-------------------------------------|
/// | 100  | k1     | t0 5000 ok 200, t1 7000 failed 400             | k10 top decoded, TOKEN inner decoded        | M1 BIG, src k20                     |
/// | 101  | k1     | t2 5000 ok –                                   | k11 top undecoded                           | M1 NINE, src k20 dst k22            |
/// | 102  | k2     | t3 BIG failed 600                              | k10 inner undecoded, k10 top undecoded      | M2 5, dst k22                       |
/// | 103  | –      | t4 NINE ok –                                   |                                             | (no mint) 7 via Token-2022, src k21 |
fn write_chain(dir: &Path) -> FlushResult {
    let tx = |slot: u64, sig: &str, fee: u64, failed: bool, cu: Option<u64>| DeepAnaTransaction {
        slot,
        blockhash: format!("hash{slot}"),
        transaction_index: 0,
        tx_signature: sig.to_owned(),
        fee,
        status: if failed { TransactionStatus::Failed } else { TransactionStatus::Success },
        err: failed.then(|| "custom program error".to_owned()),
        compute_units_consumed: cu,
        ..Default::default()
    };
    let ix = |slot: u64, sig: &str, index: u32, inner: Option<u32>, program: &str, ty: Option<&str>| {
        DeepAnaInstruction {
            slot,
            tx_signature: sig.to_owned(),
            instruction_index: index,
            inner_instruction_index: inner,
            program_id: program.to_owned(),
            instruction_type: ty.map(str::to_owned),
            ..Default::default()
        }
    };
    let xfer = |slot: u64,
                sig: &str,
                program: &str,
                src: Option<String>,
                dst: Option<String>,
                mint: Option<String>,
                amount: u64| DeepAnaTokenTransfer {
        slot,
        tx_signature: sig.to_owned(),
        instruction_index: 0,
        spl_token_program_id: program.to_owned(),
        source_token_account: key(40),
        destination_token_account: key(41),
        source_owner: src,
        destination_owner: dst,
        decimals: mint.as_ref().map(|_| 6),
        mint_token_address: mint,
        amount_raw: amount,
        block_time: Some(block_time(slot)),
        ..Default::default()
    };
    let slot = |slot: u64, leader: Option<String>, txs: u32| DeepAnaSlot {
        slot,
        blockhash: format!("hash{slot}"),
        parent_slot: slot - 1,
        block_time: Some(block_time(slot)),
        block_height: (slot != 103).then_some(slot),
        transaction_count: txs,
        leader,
    };

    let mut w = ParquetWriter::new(dir);
    w.append(
        &slot(100, Some(key(1)), 2),
        &[tx(100, "t0", 5000, false, Some(200)), tx(100, "t1", 7000, true, Some(400))],
        &[ix(100, "t0", 0, None, &key(10), Some("transfer")), ix(100, "t0", 0, Some(0), TOKEN, Some("TransferChecked"))],
        &[xfer(100, "t0", TOKEN, Some(key(20)), None, Some(mint1()), BIG)],
    )
    .unwrap();
    w.append(
        &slot(101, Some(key(1)), 1),
        &[tx(101, "t2", 5000, false, None)],
        &[ix(101, "t2", 0, None, &key(11), None)],
        &[xfer(101, "t2", TOKEN, Some(key(20)), Some(key(22)), Some(mint1()), NINE)],
    )
    .unwrap();
    w.append(
        &slot(102, Some(key(2)), 1),
        &[tx(102, "t3", BIG, true, Some(600))],
        &[ix(102, "t3", 0, Some(0), &key(10), None), ix(102, "t3", 1, None, &key(10), None)],
        &[xfer(102, "t3", TOKEN, None, Some(key(22)), Some(mint2()), 5)],
    )
    .unwrap();
    w.append(
        &slot(103, None, 1),
        &[tx(103, "t4", NINE, false, None)],
        &[],
        &[xfer(103, "t4", TOKEN_2022, Some(key(21)), None, None, 7)],
    )
    .unwrap();
    w.flush().unwrap()
}

/// One UTC day, so every row lands in one partition.
fn block_time(slot: u64) -> i64 {
    1_793_102_400 + slot as i64
}

// ---------------------------------------------------------------------------
// A real LENS
// ---------------------------------------------------------------------------

struct RealLens {
    url: String,
    engine: Arc<QueryEngine>,
    server: Option<JoinHandle<lens::LensResult<()>>>,
}

impl RealLens {
    async fn start(data_dir: &Path) -> Self {
        let data_dir = data_dir.to_str().unwrap().to_owned();
        let config = LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data_dir.clone().into())).unwrap();
        let engine = Arc::new(QueryEngine::new(Arc::new(config)).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));
        Self { url, engine, server: Some(server) }
    }

    fn client(&self) -> Arc<dyn LensApi> {
        Arc::new(LensClient::from_parts(&self.url, Duration::from_secs(60)).unwrap())
    }

    async fn stop(mut self) {
        self.engine.shutdown();
        let server = self.server.take().unwrap();
        tokio::time::timeout(Duration::from_secs(10), server).await.expect("LENS stops").unwrap().unwrap();
    }
}

impl Drop for RealLens {
    fn drop(&mut self) {
        self.engine.shutdown();
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

async fn query(client: &Arc<dyn LensApi>, sql: &str) -> Vec<Map<String, Value>> {
    match client.query(sql).await {
        Ok(r) => {
            assert!(!r.truncated, "{sql}");
            r.rows
        }
        Err(e) => panic!("{sql}\n{e:?}"),
    }
}

fn str_of(v: &Value) -> String {
    v.as_str().unwrap_or_else(|| panic!("{v} is not a string")).to_owned()
}

// ---------------------------------------------------------------------------
// Definitions on the real LENS
// ---------------------------------------------------------------------------

fn bounds(lo: u64, hi: u64, m: &Measure) -> String {
    let mut w = format!("slot >= {lo} AND slot <= {hi}");
    if let Some(p) = m.filter() {
        w.push_str(" AND ");
        w.push_str(&p.sql());
    }
    w
}

fn total_sql(m: &Measure, lo: u64, hi: u64) -> String {
    format!("SELECT {} AS v FROM {} WHERE {}", m.select_expr(), m.table.sql_name(), bounds(lo, hi, m))
}

fn grouped_sql(m: &Measure, d: &Dimension, param: Option<u64>) -> String {
    let g = d.select_expr(param);
    format!(
        "SELECT {g} AS g, {} AS v FROM {} WHERE {} GROUP BY {g} ORDER BY g",
        m.select_expr(),
        m.table.sql_name(),
        bounds(LO, HI, m)
    )
}

/// What each measure is over slots 100..=103.
fn expected_totals() -> BTreeMap<&'static str, Value> {
    BTreeMap::from([
        ("slot_count", json!(4)),
        ("tx_count", json!(5)),
        ("tx_failed_count", json!(2)),
        ("tx_failure_ratio", json!(0.4)),
        ("fee_total_lamports", json!("19000000000000017000")),
        ("fee_avg_lamports", json!(19_000_000_000_000_017_000f64 / 5.0)),
        ("compute_units_avg", json!(400.0)),
        ("ix_cpi_count", json!(2)),
        ("ix_undecoded_count", json!(3)),
        ("program_count", json!(3)),
        ("transfer_count", json!(4)),
        ("transfer_unattributed_count", json!(1)),
        ("mint_count", json!(2)),
        ("transfer_volume_raw", json!("19000000000000000005")),
    ])
}

#[track_caller]
fn assert_value(id: &str, got: &Value, want: &Value) {
    match (got.as_f64(), want.as_f64()) {
        (Some(g), Some(w)) if want.is_f64() => assert!((g - w).abs() <= w.abs() * 1e-12, "{id}: {got} != {want}"),
        _ => assert_eq!(got, want, "{id}"),
    }
}

/// Group label → value.
fn groups(rows: &[Map<String, Value>]) -> BTreeMap<String, Value> {
    rows.iter()
        .map(|r| {
            let g = match &r["g"] {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (g, r["v"].clone())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_definition_runs_on_the_real_lens_and_gives_the_hand_computed_numbers() {
    let dir = TempDir::new();
    write_chain(dir.path());
    let lens = RealLens::start(dir.path()).await;
    let client = lens.client();
    let c = Catalog::new(vec![]).unwrap();
    let totals = expected_totals();
    assert_eq!(totals.len(), c.measures().len(), "every measure has an expected total");

    for m in c.measures() {
        // Alone.
        let rows = query(&client, &total_sql(m, LO, HI)).await;
        assert_eq!(rows.len(), 1, "{}", m.id);
        assert_value(m.id, &rows[0]["v"], &totals[m.id]);

        // Over no rows: counts are 0, everything else NULL.
        let rows = query(&client, &total_sql(m, 1_000, 2_000)).await;
        let empty = match m.kind {
            MeasureKind::Count { .. } | MeasureKind::CountDistinct { .. } => json!(0),
            _ => Value::Null,
        };
        assert_eq!(rows[0]["v"], empty, "{} over no rows", m.id);

        // Under every compatible dimension.
        for d in c.dimensions().iter().filter(|d| c.compatible(m.id, d.id)) {
            let param = d.parameterised.then_some(2);
            let rows = query(&client, &grouped_sql(m, d, param)).await;
            assert!(!rows.is_empty(), "{} by {}", m.id, d.id);
            // Additive measures add back up to the total.
            match m.kind {
                MeasureKind::Count { .. } => {
                    let sum: u64 = rows.iter().map(|r| r["v"].as_u64().unwrap()).sum();
                    assert_eq!(json!(sum), totals[m.id], "{} by {}", m.id, d.id);
                }
                MeasureKind::SumDecimal { .. } => {
                    let sum: u128 = rows.iter().map(|r| str_of(&r["v"]).parse::<u128>().unwrap()).sum();
                    assert_eq!(json!(sum.to_string()), totals[m.id], "{} by {}", m.id, d.id);
                }
                _ => {}
            }
        }
    }

    // Some groupings in full.
    let run = |m: &str, d: &str, p: Option<u64>| {
        let sql = grouped_sql(c.measure(m).unwrap(), c.dimension(d).unwrap(), p);
        let client = Arc::clone(&client);
        async move { groups(&query(&client, &sql).await) }
    };
    let map = |pairs: Vec<(String, Value)>| pairs.into_iter().collect::<BTreeMap<_, _>>();
    assert_eq!(run("tx_count", "tx_status", None).await, map(vec![("failed".into(), json!(2)), ("success".into(), json!(3))]));
    assert_eq!(run("tx_count", "slot_bucket", Some(2)).await, map(vec![("50".into(), json!(3)), ("51".into(), json!(2))]));
    assert_eq!(
        run("slot_count", "leader", None).await,
        map(vec![(key(1), json!(2)), (key(2), json!(1)), ("unknown".into(), json!(1))])
    );
    assert_eq!(
        run("transfer_count", "mint", None).await,
        map(vec![(mint1(), json!(2)), (mint2(), json!(1)), ("unattributed".into(), json!(1))])
    );
    // Per mint, exact, and the unattributed transfer is not summed in.
    assert_eq!(
        run("transfer_volume_raw", "mint", None).await,
        map(vec![(mint1(), json!("19000000000000000000")), (mint2(), json!("5"))])
    );
    assert_eq!(
        run("fee_total_lamports", "tx_status", None).await,
        map(vec![("failed".into(), json!("10000000000000007000")), ("success".into(), json!("9000000000000010000"))])
    );
    assert_eq!(
        run("ix_undecoded_count", "ix_level", None).await,
        map(vec![("inner".into(), json!(1)), ("top_level".into(), json!(2))])
    );
    assert_eq!(
        run("tx_failure_ratio", "slot", None).await,
        map(vec![
            ("100".into(), json!(0.5)),
            ("101".into(), json!(0.0)),
            ("102".into(), json!(1.0)),
            ("103".into(), json!(0.0)),
        ])
    );

    lens.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn every_filter_runs_on_the_real_lens_and_tests_the_raw_column() {
    let dir = TempDir::new();
    write_chain(dir.path());
    let lens = RealLens::start(dir.path()).await;
    let client = lens.client();
    let c = Catalog::new(vec![]).unwrap();

    // (dimension, value, table, rows it keeps)
    let cases: Vec<(&str, String, Table, u64)> = vec![
        ("leader", key(1), Table::Slots, 2),
        ("leader", key(2), Table::Slots, 1),
        ("leader", key(99), Table::Slots, 0),
        ("tx_status", "failed".into(), Table::Transactions, 2),
        ("tx_status", "success".into(), Table::Transactions, 3),
        ("program", key(10), Table::Instructions, 3),
        ("program", TOKEN.into(), Table::Instructions, 1),
        ("program", key(11), Table::Instructions, 1),
        ("ix_level", "top_level".into(), Table::Instructions, 3),
        ("ix_level", "inner".into(), Table::Instructions, 2),
        ("ix_decoded", "decoded".into(), Table::Instructions, 2),
        ("ix_decoded", "undecoded".into(), Table::Instructions, 3),
        ("token_program", TOKEN.into(), Table::TokenTransfers, 3),
        ("token_program", TOKEN_2022.into(), Table::TokenTransfers, 1),
        ("mint", mint1(), Table::TokenTransfers, 2),
        ("mint", mint2(), Table::TokenTransfers, 1),
        ("source_owner", key(20), Table::TokenTransfers, 2),
        ("source_owner", key(21), Table::TokenTransfers, 1),
        ("destination_owner", key(22), Table::TokenTransfers, 2),
        ("slot", "101".into(), Table::Slots, 1),
        ("slot", "101".into(), Table::Transactions, 1),
        ("slot", "101".into(), Table::Instructions, 1),
        ("slot", "101".into(), Table::TokenTransfers, 1),
        ("slot", "100..=102".into(), Table::Slots, 3),
        ("slot", "100..=102".into(), Table::Transactions, 4),
        ("slot", "100..=102".into(), Table::Instructions, 5),
        ("slot", "100..=102".into(), Table::TokenTransfers, 3),
    ];
    for d in c.dimensions().iter().filter(|d| d.filterable) {
        assert!(cases.iter().any(|(id, ..)| *id == d.id), "no filter case for {}", d.id);
    }
    for (id, value, table, want) in &cases {
        let d = c.dimension(id).unwrap();
        assert!(d.table.includes(*table));
        let filter = d.filter_sql(value).unwrap();
        // Never the rendered expression.
        assert!(!filter.contains("COALESCE") && !filter.contains("CASE"), "{id}: {filter}");
        let sql = format!(
            "SELECT COUNT(*) AS n FROM {} WHERE slot >= {LO} AND slot <= {HI} AND {filter}",
            table.sql_name()
        );
        let rows = query(&client, &sql).await;
        assert_eq!(rows[0]["n"], json!(want), "{id} = {value} on {table}");
    }
    lens.stop().await;
}
