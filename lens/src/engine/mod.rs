//! Engine: the single entry point for running SQL against the UDM tables.
//!
//! [`QueryEngine`] owns the DataFusion [`SessionContext`], the
//! [`PrismCatalog`] registered in it, and the counters the health endpoint
//! reports. The API layer only ever calls [`QueryEngine::execute`]:
//!
//! ```text
//!   QueryEngine::new(config)      create_session → PrismCatalog::register
//!        │
//!        │ execute(sql)
//!        ▼
//!   validate ─► plan (read-only) ─► LIMIT max_query_rows + 1 ─► run, under query_timeout
//!                                                                  │
//!                                     QueryResult ◄── truncate ◄───┘
//!
//!   start_catalog_refresh(secs)   background: PrismCatalog::refresh every `secs`
//!   metrics_snapshot()            EngineMetricsSnapshot
//! ```
//!
//! | File          | Holds                                                               |
//! |---------------|---------------------------------------------------------------------|
//! | `mod.rs`      | [`QueryEngine`], [`EngineMetrics`], [`EngineMetricsSnapshot`]; builds the [`QueryResult`] |
//! | [`session`]   | [`create_session`], [`SessionSettings`]: DataFusion configuration and memory limits |
//!
//! # Read-only
//!
//! Every statement is planned with DDL, DML and session statements turned
//! off, so a query cannot `CREATE EXTERNAL TABLE` over an arbitrary file,
//! `COPY ... TO` a file, `INSERT`, `DROP`, or `SET` an option for everyone
//! else. Those are rejected as [`LensError::QueryPlanError`]s.
//!
//! # Limits
//!
//! * **Rows.** Every query gets a `LIMIT max_query_rows + 1` on top, so the
//!   optimizer can stop scans early. When the extra row comes back, the
//!   result is cut to `max_query_rows` and marked [`QueryResult::truncated`].
//! * **Time.** Planning and execution together get `query_timeout_secs`. The
//!   query runs on its own Tokio task, so the deadline is met even when the
//!   query is busy computing, and that task is aborted when the deadline
//!   passes.
//! * **Memory.** See [`session`].
//!
//! # Metrics
//!
//! A query that fails [validation](QueryEngine::execute) is turned away
//! before it counts. Every other query adds one to `total_queries` when it
//! starts and one to exactly one of `successful_queries`, `failed_queries`
//! or `timed_out_queries` when it ends, so
//!
//! ```text
//! total_queries = successful + failed + timed_out + (queries still running)
//! ```
//!
//! A query whose caller stops waiting for it (e.g. the HTTP client hung up)
//! counts as failed.

pub mod session;

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::context::SQLOptions;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub use session::{
    DEFAULT_MAX_SPILL_BYTES, DEFAULT_MEMORY_LIMIT_BYTES, MIN_MEMORY_LIMIT_BYTES, SessionSettings,
    create_session, create_session_with,
};

use crate::catalog::{CatalogMetrics, PrismCatalog, RefreshReport, TableStatus};
use crate::config::LensConfig;
use crate::error::{LensError, LensResult};
use crate::result::QueryResult;

/// Longest SQL text [`QueryEngine::execute`] accepts, in bytes, after
/// trimming.
pub const MAX_SQL_BYTES: usize = 64 * 1024;

/// How many of the most recent query durations the p50 and p99 are taken
/// over.
pub const LATENCY_WINDOW: usize = 1_024;

/// How much of an oversized SQL text goes into its error, in characters.
const SQL_PREVIEW_CHARS: usize = 200;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// How a query that got past validation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryOutcome {
    Succeeded { rows: u64 },
    Failed,
    TimedOut,
}

/// Live counters of a [`QueryEngine`], updated as queries start and finish.
/// Read them through [`EngineMetrics::snapshot`].
#[derive(Debug, Default)]
pub struct EngineMetrics {
    total_queries: AtomicU64,
    successful_queries: AtomicU64,
    failed_queries: AtomicU64,
    timed_out_queries: AtomicU64,
    total_rows_returned: AtomicU64,
    last_query_duration_ms: AtomicU64,
    /// Median of `recent_durations_ms`, updated after each query.
    p50_duration_ms: AtomicU64,
    /// 99th percentile of `recent_durations_ms`, updated after each query.
    p99_duration_ms: AtomicU64,
    /// The last [`LATENCY_WINDOW`] durations of finished queries, whatever
    /// their outcome, oldest first.
    recent_durations_ms: Mutex<VecDeque<u64>>,
}

impl EngineMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// The counters as of now. Each is read on its own, so a snapshot taken
    /// while queries finish may be off by those queries.
    pub fn snapshot(&self) -> EngineMetricsSnapshot {
        let successful_queries = self.successful_queries.load(Ordering::Relaxed);
        let failed_queries = self.failed_queries.load(Ordering::Relaxed);
        let timed_out_queries = self.timed_out_queries.load(Ordering::Relaxed);
        EngineMetricsSnapshot {
            total_queries: self.total_queries.load(Ordering::Relaxed),
            successful_queries,
            failed_queries,
            timed_out_queries,
            total_rows_returned: self.total_rows_returned.load(Ordering::Relaxed),
            success_rate: success_rate(successful_queries, failed_queries, timed_out_queries),
            last_query_duration_ms: self.last_query_duration_ms.load(Ordering::Relaxed),
            p50_duration_ms: self.p50_duration_ms.load(Ordering::Relaxed),
            p99_duration_ms: self.p99_duration_ms.load(Ordering::Relaxed),
        }
    }

    fn record_start(&self) {
        self.total_queries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_finish(&self, outcome: QueryOutcome, duration_ms: u64) {
        match outcome {
            QueryOutcome::Succeeded { rows } => {
                self.successful_queries.fetch_add(1, Ordering::Relaxed);
                self.total_rows_returned.fetch_add(rows, Ordering::Relaxed);
            }
            QueryOutcome::Failed => {
                self.failed_queries.fetch_add(1, Ordering::Relaxed);
            }
            QueryOutcome::TimedOut => {
                self.timed_out_queries.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Stored under the lock, so the percentiles always describe one state
        // of the window even when queries finish together.
        let mut window = self.recent_durations_ms.lock().unwrap_or_else(PoisonError::into_inner);
        if window.len() == LATENCY_WINDOW {
            window.pop_front();
        }
        window.push_back(duration_ms);
        let mut sorted: Vec<u64> = window.iter().copied().collect();
        sorted.sort_unstable();
        self.last_query_duration_ms.store(duration_ms, Ordering::Relaxed);
        self.p50_duration_ms.store(percentile(&sorted, 50.0), Ordering::Relaxed);
        self.p99_duration_ms.store(percentile(&sorted, 99.0), Ordering::Relaxed);
    }
}

/// [`EngineMetrics`] at one moment, as plain numbers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EngineMetricsSnapshot {
    /// Queries that got past validation, including those still running.
    pub total_queries: u64,
    pub successful_queries: u64,
    /// Queries that failed to plan or execute, or whose caller stopped
    /// waiting. Timeouts are not included.
    pub failed_queries: u64,
    pub timed_out_queries: u64,
    /// Rows returned by successful queries, after truncation.
    pub total_rows_returned: u64,
    /// `successful / (successful + failed + timed_out)`, in `0.0..=1.0`.
    /// Queries still running are left out. 1.0 before any query finishes.
    pub success_rate: f64,
    /// Duration of the query that finished last, whatever its outcome.
    pub last_query_duration_ms: u64,
    /// Median duration of the last [`LATENCY_WINDOW`] queries.
    pub p50_duration_ms: u64,
    /// 99th percentile duration of the last [`LATENCY_WINDOW`] queries.
    pub p99_duration_ms: u64,
}

impl EngineMetricsSnapshot {
    /// Queries that have finished, one way or another.
    pub const fn completed_queries(&self) -> u64 {
        self.successful_queries + self.failed_queries + self.timed_out_queries
    }
}

fn success_rate(successful: u64, failed: u64, timed_out: u64) -> f64 {
    let completed = successful + failed + timed_out;
    if completed == 0 { 1.0 } else { successful as f64 / completed as f64 }
}

/// The nearest-rank `p`th percentile of `sorted`, which must be in ascending
/// order. 0 for an empty slice.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (p / 100.0 * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// One query that has been counted as started. Counts it as failed if it is
/// dropped before [`Self::finish`], which is what happens when the caller of
/// [`QueryEngine::execute`] stops polling it.
struct InFlightQuery<'a> {
    metrics: &'a EngineMetrics,
    started: Instant,
    finished: bool,
}

impl<'a> InFlightQuery<'a> {
    fn start(metrics: &'a EngineMetrics) -> Self {
        metrics.record_start();
        Self { metrics, started: Instant::now(), finished: false }
    }

    fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Record how the query ended; returns its duration in milliseconds.
    fn finish(mut self, outcome: QueryOutcome) -> u64 {
        self.finished = true;
        let duration_ms = self.elapsed_ms();
        self.metrics.record_finish(outcome, duration_ms);
        duration_ms
    }
}

impl Drop for InFlightQuery<'_> {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics.record_finish(QueryOutcome::Failed, self.elapsed_ms());
        }
    }
}

// ---------------------------------------------------------------------------
// QueryEngine
// ---------------------------------------------------------------------------

/// Runs SQL against the UDM tables. Shared by the API handlers behind an
/// `Arc`; every method takes `&self`.
pub struct QueryEngine {
    ctx: Arc<SessionContext>,
    /// Behind a mutex because refreshing needs `&mut` and the background
    /// refresh task holds a second handle to it.
    catalog: Arc<Mutex<PrismCatalog>>,
    config: Arc<LensConfig>,
    metrics: Arc<EngineMetrics>,
    /// Stops the background refresh task.
    shutdown: CancellationToken,
}

impl fmt::Debug for QueryEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryEngine")
            .field("config", &self.config)
            .field("catalog", &self.catalog)
            .field("metrics", &self.metrics.snapshot())
            .finish_non_exhaustive()
    }
}

impl QueryEngine {
    /// Create the session, register every table PRISM has written so far,
    /// and start with zeroed metrics. Uses [`SessionSettings::default`].
    ///
    /// Fails if the session cannot be created or a table whose directory
    /// exists cannot be registered. Tables whose directory does not exist
    /// yet are skipped and picked up by a later refresh.
    pub fn new(config: Arc<LensConfig>) -> LensResult<Self> {
        Self::with_settings(config, &SessionSettings::default())
    }

    /// [`Self::new`] with explicit DataFusion runtime settings.
    pub fn with_settings(config: Arc<LensConfig>, settings: &SessionSettings) -> LensResult<Self> {
        let ctx = Arc::new(session::create_session_with(&config, settings)?);
        let mut catalog = PrismCatalog::new(&config);
        catalog.register(&ctx)?;
        Ok(Self {
            ctx,
            catalog: Arc::new(Mutex::new(catalog)),
            config,
            metrics: Arc::new(EngineMetrics::new()),
            shutdown: CancellationToken::new(),
        })
    }

    pub fn config(&self) -> &LensConfig {
        &self.config
    }

    /// Run one SQL statement and return at most `max_query_rows` of its rows.
    ///
    /// 1. **Validate.** Empty (or whitespace-only) SQL and SQL longer than
    ///    [`MAX_SQL_BYTES`] are rejected as [`LensError::QueryPlanError`]
    ///    without counting as a query.
    /// 2. **Count** the query as started.
    /// 3. **Plan** it read-only; failures are [`LensError::QueryPlanError`]s.
    /// 4. **Limit** it to `max_query_rows + 1` rows.
    /// 5. **Execute** it under `query_timeout_secs`: failures are
    ///    [`LensError::QueryExecutionError`]s, an overrun is a
    ///    [`LensError::QueryTimeout`] and cancels the query.
    /// 6. **Truncate** to `max_query_rows`, then count the outcome.
    pub async fn execute(&self, sql: &str) -> LensResult<QueryResult> {
        let sql = validate_sql(sql)?;
        let query = InFlightQuery::start(&self.metrics);

        let task = SpawnedTask::spawn(run_query(
            Arc::clone(&self.ctx),
            sql.to_owned(),
            self.config.max_query_rows(),
        ));
        // On timeout the task is dropped with the `timeout` future, which
        // aborts it.
        let result = match tokio::time::timeout(self.config.query_timeout(), task).await {
            Ok(Ok(result)) => result,
            Ok(Err(join_err)) => Err(LensError::query_execution(sql, join_error_reason(join_err))),
            Err(_elapsed) => Err(LensError::query_timeout(sql, self.config.query_timeout_secs())),
        };

        match result {
            Ok(mut result) => {
                result.duration_ms =
                    query.finish(QueryOutcome::Succeeded { rows: result.row_count as u64 });
                tracing::debug!(
                    sql,
                    rows = result.row_count,
                    truncated = result.truncated,
                    duration_ms = result.duration_ms,
                    "query succeeded"
                );
                Ok(result)
            }
            Err(err) => {
                let outcome = if matches!(err, LensError::QueryTimeout { .. }) {
                    QueryOutcome::TimedOut
                } else {
                    QueryOutcome::Failed
                };
                let duration_ms = query.finish(outcome);
                tracing::warn!(kind = err.kind(), error = %err, duration_ms, "query failed");
                Err(err)
            }
        }
    }

    /// Spawn a task that refreshes the catalog every `interval_secs` seconds
    /// (at least 1), so files PRISM writes become queryable. The first
    /// refresh is one interval from now.
    ///
    /// The task runs until [`Self::shutdown`]; await the returned handle
    /// after that to wait for a refresh in progress to finish. Must be
    /// called from within a Tokio runtime.
    pub fn start_catalog_refresh(&self, interval_secs: u64) -> JoinHandle<()> {
        if interval_secs == 0 {
            tracing::warn!("catalog refresh interval of 0s requested; using 1s");
        }
        let interval = Duration::from_secs(interval_secs.max(1));
        tokio::spawn(PrismCatalog::run_refresh_loop(
            Arc::clone(&self.catalog),
            SessionContext::clone(&self.ctx),
            interval,
            self.shutdown.clone(),
        ))
    }

    /// Refresh the catalog now, on the calling thread.
    pub fn refresh_catalog(&self) -> RefreshReport {
        self.lock_catalog().refresh(&self.ctx)
    }

    /// Stop the background refresh task. Queries keep working.
    pub fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// The token [`Self::shutdown`] cancels, for tying other tasks (e.g. the
    /// HTTP server) to the same shutdown.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }

    pub fn metrics_snapshot(&self) -> EngineMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn catalog_metrics(&self) -> CatalogMetrics {
        self.lock_catalog().metrics()
    }

    /// The state of every table on disk and in the session.
    pub fn table_status(&self) -> LensResult<Vec<TableStatus>> {
        self.lock_catalog().table_status(&self.ctx)
    }

    fn lock_catalog(&self) -> std::sync::MutexGuard<'_, PrismCatalog> {
        // The catalog stays consistent if a refresh panics (see
        // `run_refresh_loop`), so a poisoned lock is still usable.
        self.catalog.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Planning options that allow queries only: no DDL, DML or `SET`.
fn read_only() -> SQLOptions {
    SQLOptions::new().with_allow_ddl(false).with_allow_dml(false).with_allow_statements(false)
}

/// `sql` trimmed, if it is fit to plan.
fn validate_sql(sql: &str) -> LensResult<&str> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err(LensError::query_plan(sql, "SQL is empty"));
    }
    if sql.len() > MAX_SQL_BYTES {
        let preview: String = sql.chars().take(SQL_PREVIEW_CHARS).collect();
        return Err(LensError::query_plan(
            format!("{preview}..."),
            format!("SQL is {} bytes; the limit is {MAX_SQL_BYTES}", sql.len()),
        ));
    }
    Ok(sql)
}

/// Plan and run `sql` against `ctx`, keeping at most `max_rows` rows.
/// `duration_ms` is left at 0 for the caller to fill in.
async fn run_query(ctx: Arc<SessionContext>, sql: String, max_rows: usize) -> LensResult<QueryResult> {
    let plan_err = |err| LensError::query_plan(sql.as_str(), err);
    let df = ctx.sql_with_options(&sql, read_only()).await.map_err(plan_err)?;
    // One row past the limit, to tell "exactly max_rows" from "more".
    let df = df.limit(0, Some(max_rows.saturating_add(1))).map_err(plan_err)?;
    let task_ctx = Arc::new(df.task_ctx());
    let plan = df.create_physical_plan().await.map_err(plan_err)?;
    let schema = plan.schema();

    let batches = collect(plan, task_ctx)
        .await
        .map_err(|err| LensError::query_execution(sql.as_str(), err))?;
    let (batches, row_count, truncated) = truncate(batches, max_rows);
    Ok(QueryResult {
        sql,
        batches,
        row_count,
        schema,
        duration_ms: 0,
        truncated,
        // Past the limit only one extra row is read, so the total is unknown.
        total_rows_before_limit: (!truncated).then_some(row_count),
    })
}

/// The first `max_rows` rows of `batches`, without empty batches, with
/// their count and whether any row was dropped.
fn truncate(batches: Vec<RecordBatch>, max_rows: usize) -> (Vec<RecordBatch>, usize, bool) {
    let mut kept = Vec::with_capacity(batches.len());
    let mut rows = 0;
    for batch in batches {
        let room = max_rows - rows;
        if batch.num_rows() > room {
            if room > 0 {
                kept.push(batch.slice(0, room));
            }
            return (kept, max_rows, true);
        }
        if batch.num_rows() > 0 {
            rows += batch.num_rows();
            kept.push(batch);
        }
    }
    (kept, rows, false)
}

fn join_error_reason(err: tokio::task::JoinError) -> String {
    if !err.is_panic() {
        return "query task was cancelled".to_owned();
    }
    let payload = err.into_panic();
    let message = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_owned());
    format!("query task panicked: {message}")
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, Int32Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: std::ops::Range<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(values))]).unwrap()
    }

    fn values(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                let column = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                column.values().to_vec()
            })
            .collect()
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        assert_eq!(percentile(&[], 50.0), 0);
        assert_eq!(percentile(&[7], 50.0), 7);
        assert_eq!(percentile(&[7], 99.0), 7);
        let hundred: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&hundred, 50.0), 50);
        assert_eq!(percentile(&hundred, 99.0), 99);
        assert_eq!(percentile(&hundred, 100.0), 100);
        assert_eq!(percentile(&[1, 2], 50.0), 1);
        assert_eq!(percentile(&[1, 2], 99.0), 2);
    }

    #[test]
    fn success_rate_is_over_finished_queries_and_one_before_any() {
        assert_eq!(success_rate(0, 0, 0), 1.0);
        assert_eq!(success_rate(3, 1, 0), 0.75);
        assert_eq!(success_rate(1, 0, 1), 0.5);
        assert_eq!(success_rate(0, 2, 2), 0.0);
    }

    #[test]
    fn metrics_count_each_outcome_once() {
        let metrics = EngineMetrics::new();
        for (outcome, ms) in [
            (QueryOutcome::Succeeded { rows: 10 }, 5),
            (QueryOutcome::Succeeded { rows: 2 }, 15),
            (QueryOutcome::Failed, 1),
            (QueryOutcome::TimedOut, 1_000),
        ] {
            metrics.record_start();
            metrics.record_finish(outcome, ms);
        }
        metrics.record_start(); // still running

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.total_queries, 5);
        assert_eq!(snapshot.completed_queries(), 4);
        assert_eq!(
            (snapshot.successful_queries, snapshot.failed_queries, snapshot.timed_out_queries),
            (2, 1, 1)
        );
        assert_eq!(snapshot.total_rows_returned, 12);
        assert_eq!(snapshot.success_rate, 0.5);
        assert_eq!(snapshot.last_query_duration_ms, 1_000);
        // Sorted window: [1, 5, 15, 1000].
        assert_eq!(snapshot.p50_duration_ms, 5);
        assert_eq!(snapshot.p99_duration_ms, 1_000);
    }

    #[test]
    fn latency_window_forgets_the_oldest_durations() {
        let metrics = EngineMetrics::new();
        metrics.record_finish(QueryOutcome::Failed, 1_000_000);
        for _ in 0..LATENCY_WINDOW {
            metrics.record_finish(QueryOutcome::Succeeded { rows: 0 }, 3);
        }
        let snapshot = metrics.snapshot();
        assert_eq!((snapshot.p50_duration_ms, snapshot.p99_duration_ms), (3, 3));
        assert_eq!(metrics.recent_durations_ms.lock().unwrap().len(), LATENCY_WINDOW);
    }

    #[test]
    fn a_dropped_in_flight_query_counts_as_failed() {
        let metrics = EngineMetrics::new();
        drop(InFlightQuery::start(&metrics));
        let snapshot = metrics.snapshot();
        assert_eq!((snapshot.total_queries, snapshot.failed_queries), (1, 1));

        InFlightQuery::start(&metrics).finish(QueryOutcome::Succeeded { rows: 1 });
        let snapshot = metrics.snapshot();
        assert_eq!((snapshot.total_queries, snapshot.successful_queries, snapshot.failed_queries), (2, 1, 1));
    }

    #[test]
    fn validate_sql_trims_and_rejects_empty_and_oversized_sql() {
        assert_eq!(validate_sql("  SELECT 1;\n").unwrap(), "SELECT 1;");
        for blank in ["", "   ", "\n\t"] {
            assert_eq!(validate_sql(blank).unwrap_err().kind(), "query_plan_error");
        }
        let at_limit = format!("SELECT '{}'", "x".repeat(MAX_SQL_BYTES - 9));
        assert_eq!(at_limit.len(), MAX_SQL_BYTES);
        assert!(validate_sql(&at_limit).is_ok());

        let over = format!("{at_limit}x");
        let err = validate_sql(&over).unwrap_err();
        let LensError::QueryPlanError { sql, .. } = &err else { panic!("{err:?}") };
        assert!(sql.len() < 1_000, "the error carries a preview, not the whole text");
        assert!(err.to_string().contains(&format!("the limit is {MAX_SQL_BYTES}")), "{err}");
    }

    #[test]
    fn truncate_cuts_inside_a_batch_and_drops_empty_ones() {
        let (kept, rows, truncated) = truncate(vec![batch(0..3), batch(0..0), batch(3..8)], 5);
        assert_eq!((rows, truncated), (5, true));
        assert_eq!(values(&kept), [0, 1, 2, 3, 4]);

        let (kept, rows, truncated) = truncate(vec![batch(0..3), batch(0..0), batch(3..5)], 5);
        assert_eq!((rows, truncated), (5, false));
        assert_eq!(kept.len(), 2);

        let (kept, rows, truncated) = truncate(vec![batch(0..5), batch(5..6)], 5);
        assert_eq!((rows, truncated, kept.len()), (5, true, 1));

        let (kept, rows, truncated) = truncate(Vec::new(), 5);
        assert_eq!((kept.len(), rows, truncated), (0, 0, false));
    }

    #[test]
    fn read_only_options_reject_ddl_dml_and_statements() {
        let ctx = SessionContext::new();
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        for sql in [
            "CREATE TABLE t AS SELECT 1",
            "CREATE VIEW v AS SELECT 1",
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '/etc/'",
            "DROP TABLE IF EXISTS t",
            "SET datafusion.execution.batch_size = 1",
        ] {
            let result = rt.block_on(ctx.sql_with_options(sql, read_only()));
            assert!(result.is_err(), "`{sql}` should be rejected");
        }
        assert!(rt.block_on(ctx.sql_with_options("SELECT 1", read_only())).is_ok());
    }
}
