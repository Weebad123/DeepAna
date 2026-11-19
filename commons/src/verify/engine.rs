//! The in-process SQL engine models run on: a chain becomes four tables,
//! the model runs over them, and its result comes back typed.
//!
//! # Pinned, and deterministic by construction
//!
//! - **The engine is DataFusion, the version LENS runs** (one
//!   `Cargo.lock` for the workspace; a test pins [`ENGINE_VERSION`]). The
//!   tables are registered as `solana.<table>` with exactly the Arrow
//!   schemas PRISM's writer uses (a test compares them), and JSON columns
//!   hold the text PRISM's `to_json_string` writes.
//! - **Every call builds everything fresh**: a current-thread Tokio
//!   runtime, a session, the tables. Nothing is shared between calls, so
//!   no state can leak from one case into another.
//! - **Planning is read-only**, with the options LENS uses: no DDL, no DML,
//!   no `SET`. Like LENS, the result is capped by a `LIMIT max + 1` on top.
//! - **Nothing touches the disk**: spilling is turned off, so an operator
//!   that runs out of its memory share fails its query instead of writing
//!   a file.
//! - **Partitions are explicit.** `partitions` sets `target_partitions` and
//!   splits every table into that many contiguous chunks. A model must give
//!   the same rows under every setting (`partition_stable`).
//!
//! # Output typing
//!
//! Fixtures pin booleans, integers and strings, and nothing else can be
//! compared exactly, so a result column must be `Boolean`, a signed or
//! unsigned integer of up to 64 bits, or a string type. Anything else, more
//! than [`MAX_OUTPUT_ROWS`] rows or more than [`MAX_OUTPUT_COLUMNS`]
//! columns is [`EngineError::Output`].

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, RecordBatch, StringArray, TimestampSecondArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{
    DataType, Field, Int8Type, Int16Type, Int32Type, Int64Type, Schema, SchemaRef, TimeUnit,
    UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion::catalog::{MemTable, MemorySchemaProvider};
use datafusion::common::TableReference;
use datafusion::execution::context::SQLOptions;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::config::echo;
use crate::fixtures::{Cell, Chain, ColumnType, ModelExpect, StrictValue, Table, canonical_string, json_quote};

use super::digest;
use super::sql_guard::{self, SCHEMA};

/// The DataFusion this build links, which is the one LENS links.
pub const ENGINE_VERSION: &str = datafusion::DATAFUSION_VERSION;
/// Most rows a model may return.
pub const MAX_OUTPUT_ROWS: usize = 10_000;
/// Most columns a model may return.
pub const MAX_OUTPUT_COLUMNS: usize = sql_guard::MAX_OUTPUT_COLUMNS;
/// The memory one model run may use. Spilling is off.
pub const MEMORY_LIMIT_BYTES: usize = 256 * 1024 * 1024;
/// Rows per batch; pinned so that it never depends on the default.
pub const BATCH_SIZE: usize = 8192;
/// Most partitions a run may ask for.
pub const MAX_PARTITIONS: usize = 64;
/// The catalog the `solana` schema is registered in (DataFusion's default,
/// as on LENS).
const CATALOG: &str = "datafusion";
/// Timezone of `block_time`, as PRISM writes it.
pub const BLOCK_TIME_TIMEZONE: &str = "UTC";

/// `datafusion <version>`, for the report.
pub fn engine_name() -> String {
    format!("datafusion {ENGINE_VERSION}")
}

/// Why a model produced no result. Messages may quote the SQL, so they are
/// echoed by whoever shows them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineError {
    /// The statement was rejected while rendering or planning.
    Plan(String),
    /// It failed while running (overflow, memory, a bad cast...).
    Execution(String),
    /// It ran, and its result breaks the output rules.
    Output(String),
    /// The engine could not be set up: a verifier problem, not the model's.
    Internal(String),
}

impl EngineError {
    /// The outcome kind that goes into a digest; `None` for [`Self::Internal`].
    pub fn failure(&self) -> Option<digest::ModelFailure> {
        match self {
            EngineError::Plan(_) => Some(digest::ModelFailure::Plan),
            EngineError::Execution(_) => Some(digest::ModelFailure::Execution),
            EngineError::Output(_) => Some(digest::ModelFailure::Output),
            EngineError::Internal(_) => None,
        }
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (what, m) = match self {
            EngineError::Plan(m) => ("rejected", m),
            EngineError::Execution(m) => ("failed while running", m),
            EngineError::Output(m) => ("output not allowed", m),
            EngineError::Internal(m) => ("engine error", m),
        };
        write!(f, "{what}: {}", echo(m))
    }
}

/// A model's result: column names and rows of exact cells, in the order
/// the engine produced them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypedResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
}

impl TypedResult {
    /// `Ok` when `other` has the same columns and the same rows: the same
    /// sequence if `ordered`, otherwise the same multiset. The error names
    /// the first difference. This is [`ModelExpect::check`] with `self` as
    /// the expectation, so it is exactly the golden comparison.
    pub fn same_as(&self, other: &TypedResult, ordered: bool) -> Result<(), String> {
        ModelExpect {
            columns: self.columns.clone(),
            ordered,
            rows: self.rows.clone(),
        }
        .check(&other.columns, &other.rows)
    }

    /// The canonical outcome for digests.
    pub fn outcome(&self, ordered: bool) -> StrictValue {
        digest::model_outcome(&self.columns, &self.rows, ordered)
    }
}

// ---------------------------------------------------------------------------
// Tables
// ---------------------------------------------------------------------------

/// The Arrow type PRISM's writer stores a column as.
pub fn arrow_type(ty: ColumnType) -> DataType {
    match ty {
        ColumnType::UInt64 => DataType::UInt64,
        ColumnType::UInt32 => DataType::UInt32,
        ColumnType::UInt8 => DataType::UInt8,
        ColumnType::TimestampSecond => {
            DataType::Timestamp(TimeUnit::Second, Some(BLOCK_TIME_TIMEZONE.into()))
        }
        ColumnType::Utf8 | ColumnType::JsonText => DataType::Utf8,
    }
}

/// A table's schema: [`Table::columns`] in Arrow terms.
pub fn table_schema(table: Table) -> SchemaRef {
    Arc::new(Schema::new(
        table
            .columns()
            .iter()
            .map(|c| Field::new(c.name, arrow_type(c.ty), c.nullable))
            .collect::<Vec<_>>(),
    ))
}

fn rows_of(chain: &Chain, table: Table) -> Vec<StrictValue> {
    match table {
        Table::Slots => chain.slots.iter().map(|r| r.to_strict()).collect(),
        Table::Transactions => chain.transactions.iter().map(|r| r.to_strict()).collect(),
        Table::Instructions => chain.instructions.iter().map(|r| r.to_strict()).collect(),
        Table::TokenTransfers => chain.token_transfers.iter().map(|r| r.to_strict()).collect(),
    }
}

/// One table of `chain` as a single batch, rows in chain order. Built from
/// each row's file form ([`Chain::to_strict`]), column by column in
/// [`Table::columns`] order; a JSON column holds [`canonical_string`] of
/// its value, which is the text PRISM writes.
pub fn table_batch(chain: &Chain, table: Table) -> Result<RecordBatch, EngineError> {
    let rows = rows_of(chain, table);
    let field = |row: &StrictValue, name: &str| -> StrictValue {
        match row {
            StrictValue::Object(m) => m.get(name).cloned().unwrap_or(StrictValue::Null),
            _ => StrictValue::Null,
        }
    };
    let int = |v: StrictValue| match v {
        StrictValue::Int(n) => Some(n),
        _ => None,
    };
    let bad = |c: &str| EngineError::Internal(format!("{}.{c} does not fit its column", table.as_str()));
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for c in table.columns() {
        let values: Vec<StrictValue> = rows.iter().map(|r| field(r, c.name)).collect();
        let array: ArrayRef = match c.ty {
            ColumnType::UInt64 => Arc::new(UInt64Array::from(
                values
                    .into_iter()
                    .map(|v| int(v).map(|n| u64::try_from(n).map_err(|_| bad(c.name))).transpose())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            ColumnType::UInt32 => Arc::new(UInt32Array::from(
                values
                    .into_iter()
                    .map(|v| int(v).map(|n| u32::try_from(n).map_err(|_| bad(c.name))).transpose())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            ColumnType::UInt8 => Arc::new(UInt8Array::from(
                values
                    .into_iter()
                    .map(|v| int(v).map(|n| u8::try_from(n).map_err(|_| bad(c.name))).transpose())
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            ColumnType::TimestampSecond => Arc::new(
                TimestampSecondArray::from(
                    values
                        .into_iter()
                        .map(|v| int(v).map(|n| i64::try_from(n).map_err(|_| bad(c.name))).transpose())
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .with_timezone(BLOCK_TIME_TIMEZONE),
            ),
            ColumnType::Utf8 => Arc::new(StringArray::from(
                values
                    .into_iter()
                    .map(|v| match v {
                        StrictValue::Str(s) => Some(s),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            ColumnType::JsonText => Arc::new(StringArray::from(
                values
                    .into_iter()
                    .map(|v| match v {
                        StrictValue::Null => None,
                        v => Some(canonical_string(&v)),
                    })
                    .collect::<Vec<_>>(),
            )),
        };
        if !c.nullable && array.null_count() > 0 {
            return Err(bad(c.name));
        }
        arrays.push(array);
    }
    RecordBatch::try_new(table_schema(table), arrays).map_err(|e| EngineError::Internal(e.to_string()))
}

/// `batch` cut into `n` contiguous chunks, as evenly as possible (earlier
/// chunks get the remainder). Empty chunks hold no batch.
fn split(batch: &RecordBatch, n: usize) -> Vec<Vec<RecordBatch>> {
    let rows = batch.num_rows();
    let (base, extra) = (rows / n, rows % n);
    let mut offset = 0;
    (0..n)
        .map(|i| {
            let len = base + usize::from(i < extra);
            let part = if len == 0 { Vec::new() } else { vec![batch.slice(offset, len)] };
            offset += len;
            part
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Running
// ---------------------------------------------------------------------------

/// The options LENS plans every query with.
fn read_only() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

fn runtime() -> Result<tokio::runtime::Runtime, EngineError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|e| EngineError::Internal(format!("cannot start a runtime: {e}")))
}

/// A fresh session holding `chain`'s four tables under `solana`.
pub fn session(chain: &Chain, partitions: usize) -> Result<SessionContext, EngineError> {
    let internal = |e: datafusion::error::DataFusionError| EngineError::Internal(e.to_string());
    let mut config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_batch_size(BATCH_SIZE)
        .with_information_schema(false);
    // As LENS sets it; harmless for memory tables, kept for parity.
    config.options_mut().execution.collect_statistics = true;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(MEMORY_LIMIT_BYTES)))
        .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled))
        .build_arc()
        .map_err(internal)?;
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.catalog(CATALOG)
        .ok_or_else(|| EngineError::Internal(format!("no default catalog {CATALOG}")))?
        .register_schema(SCHEMA, Arc::new(MemorySchemaProvider::new()))
        .map_err(internal)?;
    for t in Table::ALL {
        let batch = table_batch(chain, t)?;
        let mem = MemTable::try_new(table_schema(t), split(&batch, partitions)).map_err(internal)?;
        ctx.register_table(TableReference::partial(SCHEMA, t.as_str()), Arc::new(mem))
            .map_err(internal)?;
    }
    Ok(ctx)
}

/// Run the model template `sql` over `chain` for the window `from..=to`
/// with `partitions` partitions, from fresh state.
pub fn run_model(
    chain: &Chain,
    sql: &str,
    from: u64,
    to: u64,
    partitions: usize,
) -> Result<TypedResult, EngineError> {
    if !(1..=MAX_PARTITIONS).contains(&partitions) {
        return Err(EngineError::Internal(format!(
            "partitions must be 1 to {MAX_PARTITIONS}, not {partitions}"
        )));
    }
    let rendered = sql_guard::render(sql, from, to).map_err(|i| EngineError::Plan(i.to_string()))?;
    let ctx = session(chain, partitions)?;
    let (schema, batches) = runtime()?.block_on(async {
        let df = ctx
            .sql_with_options(&rendered, read_only())
            .await
            .map_err(|e| EngineError::Plan(e.to_string()))?;
        // As LENS does: one row past the limit, to tell "max" from "more".
        let df = df
            .limit(0, Some(MAX_OUTPUT_ROWS + 1))
            .map_err(|e| EngineError::Plan(e.to_string()))?;
        let schema = df.schema().as_arrow().clone();
        let batches = df.collect().await.map_err(|e| EngineError::Execution(e.to_string()))?;
        Ok::<_, EngineError>((schema, batches))
    })?;
    typed_result(&schema, &batches)
}

/// `batches` of `schema` as exact cells, with the output rules (module
/// docs). Public so that results from elsewhere (LENS, in the fidelity
/// tests) go through the same conversion.
pub fn typed_result(schema: &Schema, batches: &[RecordBatch]) -> Result<TypedResult, EngineError> {
    if schema.fields().len() > MAX_OUTPUT_COLUMNS {
        return Err(EngineError::Output(format!(
            "{} columns, more than the limit of {MAX_OUTPUT_COLUMNS}",
            schema.fields().len()
        )));
    }
    for f in schema.fields() {
        if !allowed_type(f.data_type()) {
            return Err(EngineError::Output(format!(
                "column '{}' is {}; a model outputs only booleans, integers and strings",
                echo(f.name()),
                f.data_type()
            )));
        }
    }
    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total > MAX_OUTPUT_ROWS {
        return Err(EngineError::Output(format!("more than {MAX_OUTPUT_ROWS} rows")));
    }
    let mut rows = Vec::with_capacity(total);
    for b in batches {
        let columns: Vec<Vec<Cell>> = b.columns().iter().map(|a| cells(a.as_ref())).collect::<Result<_, _>>()?;
        for i in 0..b.num_rows() {
            rows.push(columns.iter().map(|c| c[i].clone()).collect());
        }
    }
    Ok(TypedResult {
        columns: schema.fields().iter().map(|f| f.name().clone()).collect(),
        rows,
    })
}

fn allowed_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    )
}

fn cells(a: &dyn Array) -> Result<Vec<Cell>, EngineError> {
    fn ints<T: datafusion::arrow::datatypes::ArrowPrimitiveType>(a: &dyn Array) -> Vec<Cell>
    where
        T::Native: Into<i128>,
    {
        let p = a.as_primitive::<T>();
        (0..p.len())
            .map(|i| if p.is_null(i) { Cell::Null } else { Cell::Int(p.value(i).into()) })
            .collect()
    }
    let strs = |get: &dyn Fn(usize) -> String| -> Vec<Cell> {
        (0..a.len())
            .map(|i| if a.is_null(i) { Cell::Null } else { Cell::Str(get(i)) })
            .collect()
    };
    Ok(match a.data_type() {
        DataType::Boolean => {
            let b: &BooleanArray = a.as_boolean();
            (0..b.len())
                .map(|i| if b.is_null(i) { Cell::Null } else { Cell::Bool(b.value(i)) })
                .collect()
        }
        DataType::Int8 => ints::<Int8Type>(a),
        DataType::Int16 => ints::<Int16Type>(a),
        DataType::Int32 => ints::<Int32Type>(a),
        DataType::Int64 => ints::<Int64Type>(a),
        DataType::UInt8 => ints::<UInt8Type>(a),
        DataType::UInt16 => ints::<UInt16Type>(a),
        DataType::UInt32 => ints::<UInt32Type>(a),
        DataType::UInt64 => ints::<UInt64Type>(a),
        DataType::Utf8 => {
            let s = a.as_string::<i32>();
            strs(&|i| s.value(i).to_string())
        }
        DataType::LargeUtf8 => {
            let s = a.as_string::<i64>();
            strs(&|i| s.value(i).to_string())
        }
        DataType::Utf8View => {
            let s = a.as_string_view();
            strs(&|i| s.value(i).to_string())
        }
        other => return Err(EngineError::Output(format!("type {other} is not allowed"))),
    })
}

// ---------------------------------------------------------------------------
// LENS
// ---------------------------------------------------------------------------

/// Why LENS did not accept a model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LensRefusal {
    /// LENS answered and refused the statement (a 4xx): the model's fault.
    Rejected(String),
    /// LENS could not be reached, timed out, or failed (a 5xx): not the
    /// model's fault.
    Unavailable(String),
}

/// The live LENS check: send **only** the model, rendered for the window
/// `0..=0`, to `POST <base_url>query`, and require a 200. The point is only
/// that the statement plans and runs on the engine and version production
/// uses; the result is not read. The window keeps the cost to what the
/// model's own bounds let it read. The client has one hard `timeout` for
/// the whole exchange, follows no redirect and uses no proxy.
pub fn lens_accepts(base_url: &str, sql: &str, timeout: Duration) -> Result<(), LensRefusal> {
    let rendered = sql_guard::render(sql, 0, 0).map_err(|i| LensRefusal::Rejected(i.to_string()))?;
    let url = url::Url::parse(base_url)
        .and_then(|u| u.join("query"))
        .map_err(|e| LensRefusal::Unavailable(format!("bad LENS URL: {e}")))?;
    let body = format!("{{\"sql\":{}}}", json_quote(&rendered));
    // The HTTP client needs the IO driver; the engine's runtime does not.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| LensRefusal::Unavailable(format!("cannot start a runtime: {e}")))?;
    rt.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()
            .map_err(|e| LensRefusal::Unavailable(e.to_string()))?;
        let resp = client
            .post(url)
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                LensRefusal::Unavailable(if e.is_timeout() {
                    format!("no answer within {} s", timeout.as_secs())
                } else {
                    format!("cannot reach LENS: {e}")
                })
            })?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        let excerpt: String = text.chars().take(400).collect();
        if status.is_success() {
            Ok(())
        } else if status.is_client_error() {
            Err(LensRefusal::Rejected(format!("LENS answered {status}: {}", echo(&excerpt))))
        } else {
            Err(LensRefusal::Unavailable(format!("LENS answered {status}: {}", echo(&excerpt))))
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{GenSpec, generate};

    fn spec() -> GenSpec {
        GenSpec {
            seed: 3,
            start_slot: 100,
            slots: 60,
            skip_percent: 10,
            max_tx_per_slot: 4,
            fail_percent: 20,
            transfer_percent: 50,
            mint_null_percent: 20,
        }
    }

    const PER_PROGRAM: &str = "SELECT i.program_id, COUNT(*) AS n FROM solana.instructions i \
                               WHERE i.slot BETWEEN {{from_slot}} AND {{to_slot}} GROUP BY i.program_id";

    #[test]
    fn the_engine_is_pinned() {
        // Bumping DataFusion changes what models mean; it must be a
        // deliberate, reviewed change here and in LENS together.
        assert_eq!(ENGINE_VERSION, "55.2.0");
        assert_eq!(engine_name(), "datafusion 55.2.0");
    }

    #[test]
    fn results_are_exact_cells() {
        let chain = generate(&spec());
        let r = run_model(&chain, PER_PROGRAM, 100, 159, 1).unwrap();
        assert_eq!(r.columns, ["program_id", "n"]);
        let total: i128 = r.rows.iter().map(|row| match row[1] { Cell::Int(n) => n, _ => panic!() }).sum();
        assert_eq!(total, chain.instructions.len() as i128);
        let typed = run_model(
            &chain,
            "SELECT s.slot, s.leader, s.block_time IS NULL AS no_time, CAST(-1 AS TINYINT) AS neg \
             FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}} ORDER BY s.slot LIMIT 1",
            0,
            u64::MAX,
            1,
        )
        .unwrap();
        let s = &chain.slots[0];
        assert_eq!(
            typed.rows,
            [vec![
                Cell::Int(s.slot.into()),
                Cell::Str(s.leader.clone().unwrap()),
                Cell::Bool(false),
                Cell::Int(-1)
            ]]
        );
    }

    #[test]
    fn json_columns_hold_canonical_text() {
        let chain = generate(&spec());
        let r = run_model(
            &chain,
            "SELECT t.accounts AS a FROM solana.transactions t \
             WHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}} ORDER BY t.slot, t.transaction_index LIMIT 1",
            0,
            u64::MAX,
            2,
        )
        .unwrap();
        let first = &chain.transactions[0];
        assert_eq!(
            r.rows[0][0],
            Cell::Str(canonical_string(&StrictValue::str_array(&first.accounts)))
        );
    }

    #[test]
    fn partitions_do_not_change_the_multiset() {
        let chain = generate(&spec());
        let one = run_model(&chain, PER_PROGRAM, 110, 140, 1).unwrap();
        for n in [2, 3, 4, 7] {
            let many = run_model(&chain, PER_PROGRAM, 110, 140, n).unwrap();
            assert_eq!(one.same_as(&many, false), Ok(()), "{n} partitions");
            assert_eq!(one.outcome(false), many.outcome(false));
        }
        assert!(run_model(&chain, PER_PROGRAM, 0, 1, 0).is_err());
        assert!(run_model(&chain, PER_PROGRAM, 0, 1, MAX_PARTITIONS + 1).is_err());
    }

    #[test]
    fn split_is_contiguous_and_covers_every_row() {
        let chain = generate(&spec());
        let b = table_batch(&chain, Table::Slots).unwrap();
        for n in [1, 2, 5, 64] {
            let parts = split(&b, n);
            assert_eq!(parts.len(), n);
            let rows: usize = parts.iter().flatten().map(RecordBatch::num_rows).sum();
            assert_eq!(rows, b.num_rows());
        }
    }

    #[test]
    fn output_rules() {
        let chain = generate(&spec());
        let w = "FROM solana.transactions t WHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}}";
        let out = |sql: String| run_model(&chain, &sql, 0, u64::MAX, 1);
        for (sql, part) in [
            (format!("SELECT t.block_time_x AS x {w}"), ""),
            (format!("SELECT AVG(t.fee) AS x {w}"), "Float64"),
            (format!("SELECT CAST(t.fee AS DECIMAL(20,0)) AS x {w}"), "Decimal"),
            (format!("SELECT TO_TIMESTAMP_SECONDS(1) AS x {w}"), "Timestamp"),
            (format!("SELECT NULL AS x {w}"), "Null"),
        ] {
            let e = out(sql.clone()).unwrap_err();
            if part.is_empty() {
                assert!(matches!(e, EngineError::Plan(_)), "{sql}: {e:?}");
            } else {
                assert!(matches!(&e, EngineError::Output(m) if m.contains(part)), "{sql}: {e:?}");
            }
        }
        // More than MAX_OUTPUT_ROWS rows.
        let big = generate(&GenSpec { slots: 3000, max_tx_per_slot: 6, skip_percent: 0, ..spec() });
        assert!(big.instructions.len() > MAX_OUTPUT_ROWS);
        let e = run_model(
            &big,
            "SELECT i.slot FROM solana.instructions i WHERE i.slot BETWEEN {{from_slot}} AND {{to_slot}}",
            0,
            u64::MAX,
            1,
        )
        .unwrap_err();
        assert!(matches!(&e, EngineError::Output(m) if m.contains("more than 10000 rows")), "{e:?}");
        // More than 32 columns.
        let cols: Vec<String> = (0..33).map(|i| format!("t.fee AS c{i}")).collect();
        let e = out(format!("SELECT {} {w}", cols.join(", "))).unwrap_err();
        assert!(matches!(&e, EngineError::Output(m) if m.contains("33 columns")), "{e:?}");
    }

    #[test]
    fn planning_is_read_only_and_nothing_is_written() {
        let chain = Chain::default();
        for sql in [
            "CREATE TABLE x AS SELECT {{from_slot}} AS a, {{to_slot}} AS b",
            "INSERT INTO solana.slots SELECT * FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}",
            "COPY (SELECT {{from_slot}} AS a, {{to_slot}} AS b) TO '/tmp/commons-x.csv'",
            "SET datafusion.execution.batch_size = {{from_slot}} + {{to_slot}}",
        ] {
            assert!(matches!(run_model(&chain, sql, 0, 0, 1), Err(EngineError::Plan(_))), "{sql}");
        }
        assert!(!std::path::Path::new("/tmp/commons-x.csv").exists());
    }

    #[test]
    fn empty_tables_have_the_schema_and_no_rows() {
        let r = run_model(
            &Chain::default(),
            "SELECT s.slot, s.leader FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}}",
            0,
            10,
            4,
        )
        .unwrap();
        assert_eq!(r.columns, ["slot", "leader"]);
        assert!(r.rows.is_empty());
    }

    #[test]
    fn lens_is_unavailable_when_nothing_listens() {
        // Port 9 (discard) on localhost: nothing listens in CI.
        let r = lens_accepts("http://127.0.0.1:9/", PER_PROGRAM, Duration::from_secs(2));
        assert!(matches!(r, Err(LensRefusal::Unavailable(_))), "{r:?}");
        let r = lens_accepts("http://127.0.0.1:9/", "SELECT 1", Duration::from_secs(2));
        assert!(matches!(r, Err(LensRefusal::Rejected(_))), "a bad template is the model's fault: {r:?}");
    }
}
