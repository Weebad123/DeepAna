//! LENS as a library service: [`start`] builds everything the `lens` binary
//! used to build in `main`, and returns a [`Lens`] handle the host serves.
//!
//! ```ignore
//! let config = LensServiceConfig::for_parquet_directory(dir, |name| std::env::var_os(name))?;
//! let token = CancellationToken::new();
//! let lens = lens::start(config, token.clone()).await?;
//! axum::serve(listener, lens.router()).with_graceful_shutdown(token.clone().cancelled_owned()).await?;
//! token.cancel();
//! lens.wait().await?;
//! ```
//!
//! The shape matches ORACLE's `Oracle`: LENS never binds a socket and never
//! installs a tracing subscriber here; the host does both.
//!
//! # PRISM
//!
//! With [`LensServiceConfig::run_prism`], PRISM's ingestor runs in the same
//! process and writes under `data_dir`:
//!
//! ```text
//! {data_dir}/
//!   solana/{slots,transactions,instructions,token_transfers}/   Parquet (LENS reads)
//!   cursor.json  changelog.jsonl  skipped_slots.log             PRISM's own files
//! ```
//!
//! It follows the Solana devnet tip through Helius and needs
//! `HELIUS_API_KEY`. It is built before [`start`] returns, so a missing key
//! (or a change log another process holds) fails the start, not later.
//!
//! # Lifecycle
//!
//! LENS runs on a **child** of the host's token. Supervision is fail-stop:
//! PRISM ending while LENS is meant to be running (an error, a panic, or a
//! normal exit), or the catalog refresh task ending, is a failure. The
//! failure is recorded and LENS's own token is cancelled, which
//! [`Lens::stopped`] reports, so the host can stop serving: a LENS whose
//! data no longer grows would look healthy. The host's token is never
//! cancelled by LENS.
//!
//! Once LENS's token is cancelled, PRISM is told to stop (it drains its
//! channel, flushes and saves its cursor) and the catalog refresh stops.
//! [`Lens::wait`] waits for both and returns the first failure.
//!
//! Dropping a [`Lens`] without calling `wait` cancels its token.

use std::ffi::OsString;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use prism::adapter::ChainAdapter;
use prism::changelog::{ChangeLog, LocalChangeLog};
use prism::commitment::CommitmentTracker;
use prism::ingestor::{Cluster, Ingestor, IngestorConfig, helius_api_key_from_env, helius_solana_adapter};
use prism::storage::TableKind;
use prism::storage::partition::CHAIN_DIR;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::api::{self, AppState};
use crate::config::{ENV_DATA_DIR, LensConfig};
use crate::engine::QueryEngine;
use crate::error::{LensError, LensResult};

/// The cluster PRISM ingests when LENS runs it.
pub const PRISM_CLUSTER: Cluster = Cluster::Devnet;
/// PRISM's task name, in [`LensError::TaskFailed`].
pub const PRISM_TASK: &str = "prism";
/// The catalog refresh task's name.
pub const CATALOG_REFRESH_TASK: &str = "catalog_refresh";

/// What [`start`] needs.
#[derive(Debug, Clone)]
pub struct LensServiceConfig {
    pub config: Arc<LensConfig>,
    /// Run PRISM's ingestor in-process, writing under `config.data_dir()`.
    pub run_prism: bool,
}

impl LensServiceConfig {
    /// LENS alone, over a directory PRISM writes elsewhere.
    pub fn serve_only(config: LensConfig) -> Self {
        Self { config: Arc::new(config), run_prism: false }
    }

    /// What `lens --parquet-directory <DIR>` runs: create `DIR`'s table
    /// directories, load the config with `DIR` as `LENS_DATA_DIR` (every
    /// other variable from `lookup`), and run PRISM writing to `DIR`.
    pub fn for_parquet_directory<F>(dir: &Path, lookup: F) -> LensResult<Self>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        create_table_dirs(dir)?;
        let config = LensConfig::load_with(|name| {
            if name == ENV_DATA_DIR { Some(dir.as_os_str().to_owned()) } else { lookup(name) }
        })?;
        Ok(Self { config: Arc::new(config), run_prism: true })
    }
}

/// Create `{dir}/solana/{table}` for every table PRISM writes. PRISM would
/// create them on its first flush, but LENS refuses to start on a directory
/// without any, and an empty table directory reads as an empty table.
pub fn create_table_dirs(dir: &Path) -> LensResult<()> {
    for table in TableKind::ALL {
        let table_dir = dir.join(CHAIN_DIR).join(table.as_str());
        fs::create_dir_all(&table_dir).map_err(|err| LensError::io(table_dir.display().to_string(), err))?;
    }
    Ok(())
}

/// PRISM's ingestor over Helius, writing under `data_dir`.
///
/// One change log, `{data_dir}/changelog.jsonl`, is shared by all three
/// producers: the finality poller (`Final`), and through the ingestor the
/// gap handler (`Skip`) and the writer task (`New`). LENS itself does not
/// read it; it finds new files by re-scanning the directory.
pub async fn build_ingestor(data_dir: &Path) -> LensResult<Ingestor> {
    let startup = |err: prism::types::error::PrismError| LensError::prism("startup", err);
    let api_key = helius_api_key_from_env().map_err(startup)?;
    let adapter: Arc<dyn ChainAdapter> =
        Arc::new(helius_solana_adapter(&api_key, PRISM_CLUSTER).await.map_err(startup)?);
    let changelog: Arc<dyn ChangeLog> =
        Arc::new(LocalChangeLog::open(data_dir, adapter.chain_id()).map_err(startup)?);
    let tracker = CommitmentTracker::spawn(Arc::clone(&adapter), Arc::clone(&changelog));
    Ingestor::new(adapter, Arc::new(tracker), changelog, IngestorConfig::new(data_dir)).map_err(startup)
}

/// [`Lens::start`].
pub async fn start(config: LensServiceConfig, token: CancellationToken) -> LensResult<Lens> {
    Lens::start(config, token).await
}

/// A running LENS (and PRISM, if asked for). See the [module docs](self).
pub struct Lens {
    engine: Arc<QueryEngine>,
    state: AppState,
    /// LENS's own token, a child of the host's.
    token: CancellationToken,
    /// Cancels `token` if the handle is dropped without [`Lens::wait`].
    guard: Option<DropGuard>,
    supervisor: JoinHandle<LensResult<()>>,
}

impl std::fmt::Debug for Lens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lens")
            .field("engine", &self.engine)
            .field("stopping", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Lens {
    /// Build PRISM's ingestor (if asked for), then the query engine, then
    /// start the catalog refresh and PRISM under one supervisor. Must be
    /// called inside a Tokio runtime.
    pub async fn start(config: LensServiceConfig, token: CancellationToken) -> LensResult<Self> {
        let LensServiceConfig { config, run_prism } = config;
        // Before the engine, so a missing API key fails fast.
        let ingestor = if run_prism { Some(Arc::new(build_ingestor(config.data_dir()).await?)) } else { None };
        let engine = Arc::new(QueryEngine::new(Arc::clone(&config))?);

        // A child: a failure of ours never cancels the host.
        let token = token.child_token();
        let refresh = engine.start_catalog_refresh(config.catalog_refresh_interval_secs());
        let prism = ingestor.map(|ingestor| {
            let stop = ingestor.shutdown_token();
            let task = tokio::spawn(async move { ingestor.run().await });
            (task, stop)
        });
        let supervisor = tokio::spawn(supervise(Arc::clone(&engine), refresh, prism, token.clone()));

        tracing::info!(
            data_dir = %config.data_dir().display(),
            chain = config.chain(),
            prism = run_prism,
            "LENS started"
        );
        Ok(Self {
            state: AppState::new(Arc::clone(&engine)),
            engine,
            guard: Some(token.clone().drop_guard()),
            token,
            supervisor,
        })
    }

    /// The HTTP API (`POST /query`, `GET /health`). Every call builds a new
    /// router over the same engine.
    pub fn router(&self) -> Router {
        api::router(self.state.clone())
    }

    /// The query engine the router serves.
    pub fn engine(&self) -> &Arc<QueryEngine> {
        &self.engine
    }

    /// Resolves once LENS is stopping: the host cancelled its token, or a
    /// task failed.
    pub async fn stopped(&self) {
        self.token.cancelled().await;
    }

    /// Wait for LENS to stop. Cancels nothing: returns once the host has
    /// cancelled its token (or a task has failed), PRISM has flushed and
    /// stopped, and the catalog refresh has stopped. `Ok(())`, or the first
    /// failure.
    pub async fn wait(mut self) -> LensResult<()> {
        let result = match (&mut self.supervisor).await {
            Ok(result) => result,
            Err(err) => Err(LensError::task_failed("supervisor", join_reason(err))),
        };
        if let Some(guard) = self.guard.take() {
            guard.disarm();
        }
        result
    }
}

type PrismTask = (JoinHandle<prism::types::error::PrismResult<prism::ingestor::IngestorReport>>, CancellationToken);

/// Fail-stop supervision; see the [module docs](self#lifecycle).
async fn supervise(
    engine: Arc<QueryEngine>,
    mut refresh: JoinHandle<()>,
    mut prism: Option<PrismTask>,
    token: CancellationToken,
) -> LensResult<()> {
    let mut failure: Option<LensError> = None;
    let mut record = |err: LensError| {
        tracing::error!(kind = err.kind(), error = %err, "LENS task failed");
        failure.get_or_insert(err);
    };
    let engine_stopped = engine.shutdown_token();
    let mut refresh_done = false;
    let mut prism_result = None;

    // Running: any task ending is a failure.
    tokio::select! {
        () = token.cancelled() => {}
        // `QueryEngine::shutdown` called by someone else: stop, no failure.
        () = engine_stopped.cancelled() => {}
        joined = &mut refresh => {
            refresh_done = true;
            let reason = match joined {
                Ok(()) => "exited unexpectedly".to_owned(),
                Err(err) => join_reason(err),
            };
            record(LensError::task_failed(CATALOG_REFRESH_TASK, reason));
        }
        joined = async {
            match prism.as_mut() {
                Some((task, _)) => task.await,
                None => std::future::pending().await,
            }
        } => {
            match joined {
                Ok(Ok(report)) => {
                    log_report(&report);
                    record(LensError::task_failed(PRISM_TASK, "exited unexpectedly"));
                }
                Ok(Err(err)) => record(LensError::prism("ingest", err)),
                Err(err) => record(LensError::task_failed(PRISM_TASK, join_reason(err))),
            }
            prism_result = Some(());
        }
    }

    // Stopping: let PRISM flush and save its cursor, stop the refresh.
    token.cancel();
    engine.shutdown();
    if let Some((task, stop)) = prism.take() {
        stop.cancel();
        if prism_result.is_none() {
            match task.await {
                Ok(Ok(report)) => log_report(&report),
                Ok(Err(err)) => record(LensError::prism("ingest", err)),
                Err(err) => record(LensError::task_failed(PRISM_TASK, join_reason(err))),
            }
        }
    }
    if !refresh_done {
        if let Err(err) = refresh.await {
            tracing::warn!(error = %err, "catalog refresh task did not stop cleanly");
        }
    }
    match failure {
        None => {
            tracing::info!("LENS stopped");
            Ok(())
        }
        Some(err) => Err(err),
    }
}

fn log_report(report: &prism::ingestor::IngestorReport) {
    tracing::info!(
        slots_processed = report.slots_processed,
        slots_skipped = report.slots_skipped,
        last_flush_slot = report.cursor.last_flush_slot,
        "PRISM stopped"
    );
}

/// Why a task's join failed, in words.
fn join_reason(err: JoinError) -> String {
    if err.is_panic() {
        let payload = err.into_panic();
        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        format!("panicked: {message}")
    } else {
        "was cancelled".to_owned()
    }
}
