//! Catalog: the UDM tables PRISM writes, registered with DataFusion.
//!
//! [`PrismCatalog`] owns one [`PrismTable`] per table and registers all of
//! them with a [`SessionContext`] under the `solana` schema of its default
//! catalog:
//!
//! ```text
//!   PrismCatalog::new(&config)               no I/O
//!        │ register(ctx)                     at startup; fails on the first error
//!        ▼
//!   datafusion.solana.{slots, transactions, instructions, token_transfers}
//!        ▲
//!        │ refresh(ctx)                      every catalog_refresh_interval_secs;
//!   run_refresh_loop                         never fails, failures are reported
//! ```
//!
//! | File          | Holds                                                          |
//! |---------------|----------------------------------------------------------------|
//! | `mod.rs`      | [`PrismCatalog`], [`CatalogMetrics`], [`TableStatus`], [`RefreshReport`], [`PrismSchemaProvider`] |
//! | [`table`]     | [`PrismTable`], [`TableKind`], the four Arrow schemas          |
//!
//! Refreshing re-registers every table, which is what makes new files
//! visible: DataFusion caches each table's file listing and, by default,
//! never expires it. A table whose directory appears after startup is
//! registered on the next refresh; one whose directory disappears is
//! deregistered.
//!
//! The `solana` schema is a [`PrismSchemaProvider`] rather than DataFusion's
//! `MemorySchemaProvider`, because the latter refuses to replace a table:
//! re-registering would mean deregistering first, and a query planned in
//! between would fail with "table not found".

pub mod table;

use std::any::Any;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::SessionContext;
use datafusion::common::TableReference;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

pub use table::{
    BLOCK_TIME_TIMEZONE, PARQUET_FILE_EXTENSION, PrismTable, RegisterOutcome, SCHEMA_NAME,
    TableKind, instructions_schema, slots_schema, token_transfers_schema, transactions_schema,
};

use crate::config::LensConfig;
use crate::error::{LensError, LensResult};

// ---------------------------------------------------------------------------
// Metrics, status, reports
// ---------------------------------------------------------------------------

/// Counters a [`PrismCatalog`] keeps. `total_refreshes` only goes up; the
/// other fields describe the most recent pass.
///
/// A pass is one [`PrismCatalog::register`] or [`PrismCatalog::refresh`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogMetrics {
    /// Passes completed, the initial registration included.
    pub total_refreshes: u64,
    /// How long the most recent pass took; 0 before the first.
    pub last_refresh_duration_ms: u64,
    /// Tables registered by the most recent pass.
    pub tables_registered: u64,
    /// Tables whose directory did not exist at the most recent pass.
    pub tables_skipped: u64,
    /// Tables that could not be registered at the most recent pass. A table
    /// that fails keeps its earlier registration, if it had one.
    pub tables_failed: u64,
}

/// One table as it is right now, on disk and in a [`SessionContext`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStatus {
    /// Fully qualified: `"solana.slots"`.
    pub name: String,
    pub dir_exists: bool,
    /// `.parquet` files under the table directory, at any depth. 0 when the
    /// directory does not exist.
    pub file_count: usize,
    /// Whether the context has the table registered.
    pub registered: bool,
}

/// What one pass over the tables did, per table, in [`TableKind::ALL`]
/// order.
#[derive(Debug, Default)]
pub struct RefreshReport {
    /// Registered or re-registered.
    pub registered: Vec<TableKind>,
    /// Directory missing; not registered.
    pub skipped: Vec<TableKind>,
    /// Directory missing and an earlier registration removed. Every table
    /// here is also in `skipped`.
    pub deregistered: Vec<TableKind>,
    /// Could not be registered. Only [`PrismCatalog::refresh`] reports
    /// failures here; [`PrismCatalog::register`] returns the first one.
    pub failed: Vec<(TableKind, LensError)>,
    pub duration_ms: u64,
}

impl RefreshReport {
    /// Whether every table was either registered or skipped.
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }
}

// ---------------------------------------------------------------------------
// PrismCatalog
// ---------------------------------------------------------------------------

/// The four UDM tables and their registration with DataFusion.
#[derive(Debug)]
pub struct PrismCatalog {
    /// In [`TableKind::ALL`] order.
    tables: Vec<PrismTable>,
    last_refresh: Option<Instant>,
    metrics: CatalogMetrics,
}

impl PrismCatalog {
    /// The tables under `config`'s chain root (`{data_dir}/solana`). Touches
    /// no file and registers nothing.
    pub fn new(config: &LensConfig) -> Self {
        Self::from_chain_root(config.chain_root())
    }

    /// The tables under `chain_root`, the directory holding `slots/`,
    /// `transactions/`, .... Touches no file and registers nothing.
    pub fn from_chain_root(chain_root: impl AsRef<Path>) -> Self {
        let chain_root = chain_root.as_ref();
        Self {
            tables: TableKind::ALL.iter().map(|&kind| PrismTable::new(kind, chain_root)).collect(),
            last_refresh: None,
            metrics: CatalogMetrics::default(),
        }
    }

    /// Every table, in [`TableKind::ALL`] order.
    pub fn tables(&self) -> &[PrismTable] {
        &self.tables
    }

    pub fn table(&self, kind: TableKind) -> &PrismTable {
        self.tables
            .iter()
            .find(|table| table.kind() == kind)
            .expect("the catalog holds every TableKind")
    }

    pub const fn metrics(&self) -> CatalogMetrics {
        self.metrics
    }

    /// When the last pass finished; `None` before [`Self::register`].
    pub const fn last_refresh(&self) -> Option<Instant> {
        self.last_refresh
    }

    /// Whether `interval` has passed since the last pass. Always true before
    /// the first.
    pub fn is_refresh_due(&self, interval: Duration) -> bool {
        self.last_refresh.is_none_or(|last| last.elapsed() >= interval)
    }

    /// Register every table whose directory exists with `ctx`. Meant for
    /// startup: the first table that cannot be registered fails the call,
    /// leaving the tables before it registered.
    pub fn register(&mut self, ctx: &SessionContext) -> LensResult<RefreshReport> {
        let report = self.run_pass(ctx, true)?;
        tracing::info!(
            registered = ?report.registered,
            skipped = ?report.skipped,
            duration_ms = report.duration_ms,
            "catalog registered"
        );
        Ok(report)
    }

    /// Re-register every table with `ctx`, picking up files written since
    /// the last pass and directories created since. Tables whose directory
    /// has gone are deregistered.
    ///
    /// Never fails as a whole: a table that cannot be registered is logged,
    /// reported in [`RefreshReport::failed`] and keeps its earlier
    /// registration, and the other tables are still refreshed.
    pub fn refresh(&mut self, ctx: &SessionContext) -> RefreshReport {
        let report = self
            .run_pass(ctx, false)
            .expect("a pass that does not stop on errors returns Ok");
        if report.is_success() {
            tracing::debug!(
                registered = report.registered.len(),
                skipped = report.skipped.len(),
                duration_ms = report.duration_ms,
                "catalog refreshed"
            );
        } else {
            tracing::warn!(
                failed = ?report.failed.iter().map(|(kind, _)| kind).collect::<Vec<_>>(),
                duration_ms = report.duration_ms,
                "catalog refresh failed for some tables; they keep their previous registration"
            );
        }
        report
    }

    /// [`Self::refresh`] if `interval` has passed since the last pass.
    pub fn refresh_if_due(
        &mut self,
        ctx: &SessionContext,
        interval: Duration,
    ) -> Option<RefreshReport> {
        self.is_refresh_due(interval).then(|| self.refresh(ctx))
    }

    /// The state of every table, in [`TableKind::ALL`] order. Walks each
    /// table directory to count its files.
    pub fn table_status(&self, ctx: &SessionContext) -> LensResult<Vec<TableStatus>> {
        self.tables
            .iter()
            .map(|table| {
                let dir_exists = table.dir_exists();
                Ok(TableStatus {
                    name: table.full_name(),
                    dir_exists,
                    file_count: if dir_exists { count_parquet_files(table.dir())? } else { 0 },
                    registered: table.is_registered(ctx),
                })
            })
            .collect()
    }

    /// Refresh `catalog` every `interval` until `shutdown` is cancelled. The
    /// first refresh is one `interval` after the call: run
    /// [`Self::register`] before starting the loop.
    ///
    /// ```ignore
    /// let catalog = Arc::new(Mutex::new(PrismCatalog::new(&config)));
    /// catalog.lock().unwrap().register(&ctx)?;
    /// tokio::spawn(PrismCatalog::run_refresh_loop(
    ///     catalog.clone(), ctx.clone(), config.catalog_refresh_interval(), shutdown.clone(),
    /// ));
    /// ```
    pub async fn run_refresh_loop(
        catalog: Arc<Mutex<Self>>,
        ctx: SessionContext,
        interval: Duration,
        shutdown: CancellationToken,
    ) {
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
        // A refresh that overruns pushes the next one back rather than
        // causing a burst to catch up.
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tracing::debug!(interval_secs = interval.as_secs(), "catalog refresh loop started");
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                _ = ticker.tick() => {
                    // A refresh that panicked leaves the catalog's state
                    // consistent (metrics are written last), so carry on.
                    catalog.lock().unwrap_or_else(PoisonError::into_inner).refresh(&ctx);
                }
            }
        }
        tracing::debug!("catalog refresh loop stopped");
    }

    /// One pass over every table. With `fail_fast`, the first failure is
    /// returned and the metrics are left as they were; without it, failures
    /// go into the report and the pass always returns `Ok`.
    fn run_pass(&mut self, ctx: &SessionContext, fail_fast: bool) -> LensResult<RefreshReport> {
        let started = Instant::now();
        let mut report = RefreshReport::default();
        for table in &self.tables {
            let kind = table.kind();
            let outcome = table.register_with(ctx).and_then(|outcome| {
                // A table that is gone from disk should not stay queryable.
                let removed = outcome == RegisterOutcome::SkippedMissingDir
                    && table.is_registered(ctx)
                    && table.deregister_from(ctx)?;
                Ok((outcome, removed))
            });
            match outcome {
                Ok((RegisterOutcome::Registered, _)) => report.registered.push(kind),
                Ok((RegisterOutcome::SkippedMissingDir, removed)) => {
                    if removed {
                        tracing::warn!(
                            table = %table.full_name(),
                            dir = %table.dir().display(),
                            "table directory is gone; table deregistered"
                        );
                        report.deregistered.push(kind);
                    }
                    report.skipped.push(kind);
                }
                Err(err) if fail_fast => return Err(err),
                Err(err) => {
                    tracing::warn!(table = %table.full_name(), error = %err, "could not register table");
                    report.failed.push((kind, err));
                }
            }
        }
        report.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);

        self.metrics = CatalogMetrics {
            total_refreshes: self.metrics.total_refreshes + 1,
            last_refresh_duration_ms: report.duration_ms,
            tables_registered: report.registered.len() as u64,
            tables_skipped: report.skipped.len() as u64,
            tables_failed: report.failed.len() as u64,
        };
        self.last_refresh = Some(Instant::now());
        Ok(report)
    }
}

/// `.parquet` files under `dir`, at any depth. Directories that disappear
/// mid-walk (a PRISM rollback) are skipped; symlinked directories are not
/// followed.
fn count_parquet_files(dir: &Path) -> LensResult<usize> {
    let io_err = |path: &Path, err| LensError::io(path.display().to_string(), err);
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(io_err(dir, err)),
    };
    let mut count = 0;
    for entry in entries {
        let entry = entry.map_err(|err| io_err(dir, err))?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| io_err(&path, err))?;
        if file_type.is_dir() {
            count += count_parquet_files(&path)?;
        } else if path.is_file()
            && path.to_str().is_some_and(|name| name.ends_with(PARQUET_FILE_EXTENSION))
        {
            count += 1;
        }
    }
    Ok(count)
}

// ---------------------------------------------------------------------------
// The `solana` schema
// ---------------------------------------------------------------------------

/// The `solana` schema: a [`SchemaProvider`] whose `register_table` replaces
/// an existing table in one step instead of failing.
#[derive(Debug, Default)]
pub struct PrismSchemaProvider {
    tables: RwLock<BTreeMap<String, Arc<dyn TableProvider>>>,
}

impl PrismSchemaProvider {
    /// Register `table` as `name`, returning the table it replaced.
    pub fn replace_table(
        &self,
        name: impl Into<String>,
        table: Arc<dyn TableProvider>,
    ) -> Option<Arc<dyn TableProvider>> {
        self.tables
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.into(), table)
    }
}

#[async_trait]
impl SchemaProvider for PrismSchemaProvider {
    /// Sorted.
    fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap_or_else(PoisonError::into_inner).keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.read().unwrap_or_else(PoisonError::into_inner).get(name).cloned())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.replace_table(name, table))
    }

    fn deregister_table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.write().unwrap_or_else(PoisonError::into_inner).remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.read().unwrap_or_else(PoisonError::into_inner).contains_key(name)
    }
}

/// The `solana` schema of `ctx`'s default catalog, created if missing.
///
/// Fails if a schema of that name exists but is not a
/// [`PrismSchemaProvider`], e.g. one made with `CREATE SCHEMA solana`.
pub fn ensure_schema(ctx: &SessionContext) -> LensResult<Arc<PrismSchemaProvider>> {
    let catalog_name = ctx.state().config().options().catalog.default_catalog.clone();
    let catalog = ctx.catalog(&catalog_name).ok_or_else(|| {
        LensError::catalog(SCHEMA_NAME, format!("default catalog {catalog_name:?} does not exist"))
    })?;
    let existing = match catalog.schema(SCHEMA_NAME) {
        Some(existing) => existing,
        None => {
            catalog
                .register_schema(SCHEMA_NAME, Arc::new(PrismSchemaProvider::default()))
                .map_err(|err| LensError::catalog(SCHEMA_NAME, err))?;
            // Read back rather than keep our own `Arc`, so a concurrent
            // caller that registered first wins for both of us.
            catalog.schema(SCHEMA_NAME).ok_or_else(|| {
                LensError::catalog(SCHEMA_NAME, "schema vanished right after it was registered")
            })?
        }
    };
    let existing: Arc<dyn Any + Send + Sync> = existing;
    existing.downcast::<PrismSchemaProvider>().map_err(|_| {
        LensError::catalog(
            SCHEMA_NAME,
            format!(
                "schema {catalog_name}.{SCHEMA_NAME} already exists and was not created by LENS"
            ),
        )
    })
}

/// Drop DataFusion's cached file listing for `table`, so its next scan lists
/// the directory again. [`SessionContext::deregister_table`] does the same.
fn invalidate_file_listing(ctx: &SessionContext, table: &TableReference) -> DataFusionResult<()> {
    match ctx.runtime_env().cache_manager.get_list_files_cache() {
        Some(cache) => cache.drop_table_entries(table),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    fn temp_dir() -> PathBuf {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-catalog-unit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, b"").unwrap();
    }

    #[test]
    fn count_parquet_files_walks_every_level_and_ignores_everything_else() {
        let dir = temp_dir();
        touch(&dir.join("date=2026-10-27/slots_1_2.parquet"));
        touch(&dir.join("date=2026-10-27/slots_3_4.parquet"));
        touch(&dir.join("date=2026-10-28/slots_5_6.parquet"));
        touch(&dir.join("top_level.parquet"));
        touch(&dir.join("date=2026-10-28/slots_7_8.parquet.tmp"));
        touch(&dir.join("date=2026-10-28/notes.txt"));
        touch(&dir.join("date=2026-10-28/parquet"));
        fs::create_dir_all(dir.join("empty.parquet")).unwrap();
        assert_eq!(count_parquet_files(&dir).unwrap(), 4);
    }

    #[test]
    fn count_parquet_files_of_a_missing_dir_is_zero() {
        assert_eq!(count_parquet_files(&temp_dir()).unwrap(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn count_parquet_files_does_not_follow_directory_symlinks() {
        let dir = temp_dir();
        touch(&dir.join("date=2026-10-27/a.parquet"));
        std::os::unix::fs::symlink(&dir, dir.join("loop")).unwrap();
        assert_eq!(count_parquet_files(&dir).unwrap(), 1);
    }

    #[tokio::test]
    async fn schema_provider_replaces_tables_in_place() {
        let provider = PrismSchemaProvider::default();
        let first: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(slots_schema()));
        let second: Arc<dyn TableProvider> = Arc::new(EmptyTable::new(slots_schema()));

        assert!(provider.register_table("slots".into(), first.clone()).unwrap().is_none());
        let replaced = provider.register_table("slots".into(), second.clone()).unwrap().unwrap();
        assert!(Arc::ptr_eq(&replaced, &first));
        assert!(Arc::ptr_eq(&provider.table("slots").await.unwrap().unwrap(), &second));

        provider.replace_table("instructions", first);
        assert_eq!(provider.table_names(), ["instructions", "slots"]);
        assert!(provider.table_exist("slots"));
        assert!(provider.deregister_table("slots").unwrap().is_some());
        assert!(provider.deregister_table("slots").unwrap().is_none());
        assert!(!provider.table_exist("slots"));
        assert!(provider.table("slots").await.unwrap().is_none());
    }

    #[test]
    fn ensure_schema_registers_under_the_default_catalog() {
        let ctx = SessionContext::new();
        let catalog_name = ctx.state().config().options().catalog.default_catalog.clone();
        assert!(ctx.catalog(&catalog_name).unwrap().schema(SCHEMA_NAME).is_none());
        ensure_schema(&ctx).unwrap();
        assert!(ctx.catalog(&catalog_name).unwrap().schema(SCHEMA_NAME).is_some());
    }

    #[test]
    fn a_failed_startup_registration_leaves_the_metrics_alone() {
        let ctx = SessionContext::new();
        let catalog_name = ctx.state().config().options().catalog.default_catalog.clone();
        ctx.catalog(&catalog_name)
            .unwrap()
            .register_schema(SCHEMA_NAME, Arc::new(datafusion::catalog::MemorySchemaProvider::new()))
            .unwrap();
        let dir = temp_dir();
        fs::create_dir_all(dir.join("slots")).unwrap();

        let mut catalog = PrismCatalog::from_chain_root(&dir);
        assert!(catalog.register(&ctx).is_err());
        assert_eq!(catalog.metrics(), CatalogMetrics::default());
        assert!(catalog.last_refresh().is_none());

        let report = catalog.refresh(&ctx);
        assert_eq!(report.failed.len(), 1);
        assert_eq!(report.skipped.len(), 3);
        let metrics = catalog.metrics();
        assert_eq!((metrics.total_refreshes, metrics.tables_failed, metrics.tables_skipped), (1, 1, 3));
    }
}
