//! ORACLE: anomaly detection, entity profiles, pipeline health checks,
//! digests and structured queries over LENS, as a library.
//!
//! A host builds an [`OracleConfig`], calls [`Oracle::start`] with a
//! [`CancellationToken`], serves [`Oracle::router`] on a listener of its
//! choosing, and on shutdown cancels its token and awaits [`Oracle::wait`].
//! ORACLE never binds a socket, never installs a tracing subscriber, and
//! never reads the environment after [`OracleConfig::load`].
//!
//! ```ignore
//! let config = OracleConfig::load()?;
//! let token = CancellationToken::new();
//! let oracle = Oracle::start(config, token.clone()).await?;
//! let app = oracle.router();
//! axum::serve(listener, app).with_graceful_shutdown(token.clone().cancelled_owned()).await?;
//! oracle.wait().await?;
//! ```
//!
//! # Startup
//!
//! [`Oracle::start_with`] runs these steps in order, so that a failure
//! leaves nothing running and nothing locked:
//!
//! 1. **Validate** what the modules would otherwise assert on (the bucket
//!    and digest window sizes). No side effects.
//! 2. Build the [`Catalog`], with the host's extra entities.
//! 3. Build the [`Interpreter`] (it validates its phrase tables).
//! 4. Build the LENS client, or take the host's. Building makes no request.
//! 5. **Open the [`Store`]**: creates the file if missing and takes its
//!    lock. The first side effect, so every failure before it touches
//!    nothing on disk.
//! 6. Build the [`SpecRunner`], [`Profiler`], [`Auditor`], the detector
//!    [`Runner`] and the [`Digester`].
//! 7. Assemble the [`AppState`].
//! 8. **A read-only startup check**: if the latest stored digest covers a
//!    different number of slots than the current window (`50 *
//!    bucket_slots`), `ORACLE_BUCKET_SECS` changed since it was built, and a
//!    warning says so. Digests already stored are kept as they are.
//! 9. Spawn the two tasks, the detector loop and the digester, under one
//!    supervisor.
//!
//! A failure in steps 6 to 8 drops the store, which releases its lock; a
//! database file created in step 5 stays, holding only the schema.
//!
//! `start` makes **no LENS request** and does not need LENS to be up: the
//! modules already cope with LENS being down or empty (detectors report
//! `NotReady`, `/health` reports `degraded`). The spawned tasks start
//! asking LENS straight away, in the background.
//!
//! # Lifecycle
//!
//! ORACLE runs on a **child** of the host's token. The host cancelling its
//! token stops ORACLE; ORACLE stopping on a failure cancels only its own
//! child, never the host's. A token already cancelled when `start` is
//! called is not an error: everything is built, the tasks see the
//! cancellation at once and stop, and `wait` returns `Ok(())`.
//!
//! **Supervision is fail-stop.** A task that ends while ORACLE is meant to
//! be running (a panic, or a normal exit) is a failure: it is recorded and
//! ORACLE's token is cancelled, which stops the other task and makes every
//! long request answer `ShuttingDown`. Nothing is restarted: a host that
//! wants a restart does it by calling `start` again. Once the token is
//! cancelled, the tasks get [`SHUTDOWN_GRACE_SECS`] to stop, and a task
//! still running then is aborted and reported as `did not stop`.
//! [`Oracle::wait`] returns the first failure, as
//! [`OracleError::TaskFailed`], or `Ok(())`.
//!
//! Dropping an [`Oracle`] without calling `wait` cancels its token, so its
//! tasks never outlive a forgotten handle.
//!
//! # The store lock
//!
//! [`AppState`] holds the [`Store`], and every router holds an
//! `Arc<AppState>`, so the database lock is held as long as any router
//! built by [`Oracle::router`] (or any clone of the state) is alive, even
//! after `wait` has returned. A host that restarts ORACLE on the same
//! database drops its old router first.
//!
//! # Logging
//!
//! Logging belongs to the host. [`OracleConfig::log_level`] is advisory:
//! the host applies it when it builds its own subscriber.

pub mod api;
pub mod catalog;
pub mod config;
pub mod detect;
pub mod digest;
pub mod intent;
pub mod profile;
pub mod quality;
pub mod spec;
pub mod store;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge::lens_client::{LensApi, LensClient};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info, warn};

pub use config::{BoxError, OracleConfig, OracleError, OracleResult};
/// The clock every module reads; re-exported so a host or test can supply
/// its own through [`OracleParts`].
pub use quality::{Clock, SystemClock};

use catalog::{Catalog, EntityDef};
use detect::detectors::{SLOT_MS, bucket_slots};
use detect::{Detector, Runner, RunnerHandle, default_detectors};
use digest::{DigestHandle, Digester, window_slots};
use intent::Interpreter;
use profile::Profiler;
use quality::Auditor;
use spec::SpecRunner;
use store::Store;

/// How long each task may take to stop once ORACLE's token is cancelled.
pub const SHUTDOWN_GRACE_SECS: u64 = 10;
/// The detector loop's task name, in [`OracleError::TaskFailed`].
pub const DETECT_TASK: &str = "detect";
/// The digester's task name.
pub const DIGEST_TASK: &str = "digest";
/// The supervisor itself, should it fail.
pub const SUPERVISOR_TASK: &str = "supervisor";

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Everything the HTTP API reads. Built by [`Oracle::start_with`] once every
/// module is up; `api/` only reads it and never builds a part.
///
/// Shared as `Arc<AppState>`; every field is itself cheap to share. It owns
/// the [`Store`], so it holds the database lock (see the
/// [crate docs](crate#the-store-lock)).
#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub catalog: Arc<Catalog>,
    pub interpreter: Arc<Interpreter>,
    pub specs: Arc<SpecRunner>,
    pub profiler: Arc<Profiler>,
    pub auditor: Arc<Auditor>,
    pub digests: DigestHandle,
    pub detectors: RunnerHandle,
    /// The one LENS client every module shares.
    pub lens: Arc<dyn LensApi>,
    pub config: Arc<OracleConfig>,
    /// ORACLE's own token (a child of the host's): long requests end as
    /// `ShuttingDown` once it is cancelled.
    pub shutdown: CancellationToken,
    /// When ORACLE started, for `uptime_secs`.
    pub start_time: Instant,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("store", &self.store)
            .field("specs", &self.specs)
            .field("profiler", &self.profiler)
            .field("auditor", &self.auditor)
            .field("shutting_down", &self.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// OracleParts
// ---------------------------------------------------------------------------

/// What a host (or a test) may supply instead of ORACLE's defaults.
#[derive(Default)]
pub struct OracleParts {
    /// The LENS client; a [`LensClient`] for `lens_url` and
    /// `lens_timeout_secs` if `None`.
    pub lens: Option<Arc<dyn LensApi>>,
    /// The clock; [`SystemClock`] if `None`.
    pub clock: Option<Arc<dyn Clock>>,
    /// Entities added to the catalog's built-in set.
    pub extra_entities: Vec<EntityDef>,
    /// The detectors, in run order; [`default_detectors`] if `None`.
    pub detectors: Option<Vec<Arc<dyn Detector>>>,
}

impl std::fmt::Debug for OracleParts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleParts")
            .field("lens", &self.lens.is_some())
            .field("clock", &self.clock.is_some())
            .field("extra_entities", &self.extra_entities.len())
            .field("detectors", &self.detectors.as_ref().map(Vec::len))
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

/// A running ORACLE. See the [crate docs](crate#lifecycle).
pub struct Oracle {
    state: Arc<AppState>,
    /// ORACLE's own token, a child of the host's.
    token: CancellationToken,
    /// Cancels `token` if the handle is dropped without [`Oracle::wait`].
    guard: Option<DropGuard>,
    supervisor: JoinHandle<OracleResult<()>>,
}

impl std::fmt::Debug for Oracle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Oracle")
            .field("state", &self.state)
            .field("stopping", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Oracle {
    /// [`Self::start_with`] with every default part. Must be called inside a
    /// Tokio runtime.
    pub async fn start(config: OracleConfig, token: CancellationToken) -> OracleResult<Self> {
        Self::start_with(config, OracleParts::default(), token).await
    }

    /// Build every module in the [documented order](crate#startup), spawn
    /// the two tasks, and return the handle. ORACLE runs until `token` (the
    /// host's) is cancelled or a task fails.
    pub async fn start_with(config: OracleConfig, parts: OracleParts, token: CancellationToken) -> OracleResult<Self> {
        // 1. Validate what a constructor below would assert on.
        let bucket_slots = bucket_slots(config.bucket_secs());
        let window = window_slots(bucket_slots).filter(|_| bucket_slots > 0).ok_or_else(|| {
            OracleError::config(
                "bucket_secs",
                format!("{} s gives no whole slot per bucket at {SLOT_MS} ms per slot", config.bucket_secs()),
            )
        })?;
        let config = Arc::new(config);
        let OracleParts { lens, clock, extra_entities, detectors } = parts;

        // 2. Catalog.
        let catalog = Arc::new(
            Catalog::new(extra_entities).map_err(|e| OracleError::config("extra_entities", e.to_string()))?,
        );

        // 3. Interpreter.
        let interpreter = Arc::new(
            Interpreter::new(Arc::clone(&catalog), SLOT_MS as u32, config.max_rows())
                .map_err(|e| OracleError::config("interpreter", e.to_string()))?,
        );

        // 4. LENS client: built, never called.
        let lens: Arc<dyn LensApi> = match lens {
            Some(lens) => lens,
            None => Arc::new(
                LensClient::from_parts(config.lens_url(), config.lens_timeout())
                    .map_err(|e| OracleError::lens(config.lens_url(), e))?,
            ),
        };
        let clock = clock.unwrap_or_else(|| Arc::new(SystemClock));

        // 5. Store: the first side effect.
        let store = {
            let config = Arc::clone(&config);
            tokio::task::spawn_blocking(move || Store::open(&config))
                .await
                .map_err(|e| OracleError::task_failed("startup", format!("opening the store: {e}")))??
        };

        // A child: a failure of ours never cancels the host.
        let token = token.child_token();

        // 6. The modules.
        let specs = Arc::new(SpecRunner::new(Arc::clone(&catalog), Arc::clone(&lens), &config));
        let profiler = Arc::new(Profiler::new(Arc::clone(&lens)).with_catalog(Arc::clone(&catalog)));
        let auditor = Arc::new(Auditor::new(Arc::clone(&lens), Arc::clone(&clock)));
        let runner = Runner::with_detectors(
            store.clone(),
            Arc::clone(&lens),
            &config,
            token.clone(),
            detectors.unwrap_or_else(default_detectors),
        );
        let detector_handle = runner.handle();
        if !config.detect_enabled() {
            for id in detector_handle.ids() {
                detector_handle.set_enabled(&id, false);
            }
        }
        let digester = Digester::new(
            store.clone(),
            Arc::clone(&lens),
            Arc::clone(&auditor),
            Arc::new(detector_handle.clone()),
            clock,
            bucket_slots,
        );

        // 7. The state.
        let state = Arc::new(AppState {
            store,
            catalog,
            interpreter,
            specs,
            profiler,
            auditor,
            digests: digester.handle(),
            detectors: detector_handle,
            lens,
            config: Arc::clone(&config),
            shutdown: token.clone(),
            start_time: Instant::now(),
        });

        // 8. Read-only startup check.
        {
            let store = state.store.clone();
            let latest = tokio::task::spawn_blocking(move || store.latest_digest())
                .await
                .map_err(|e| OracleError::task_failed("startup", format!("reading the latest digest: {e}")))??;
            if let Some(d) = latest {
                check_window_size(d.start_slot, d.end_slot, window, config.bucket_secs());
            }
        }

        // 9. The tasks.
        let mut tasks = JoinSet::new();
        let mut names = HashMap::new();
        names.insert(tasks.spawn(runner.run()).id(), DETECT_TASK);
        names.insert(tasks.spawn(digester.run(token.clone())).id(), DIGEST_TASK);
        let auditor = Arc::clone(&state.auditor);
        let supervisor = tokio::spawn(supervise(
            tasks,
            names,
            token.clone(),
            Duration::from_secs(SHUTDOWN_GRACE_SECS),
            move || auditor.shutdown(),
        ));

        info!(
            db_path = %config.db_path().display(),
            lens_url = config.lens_url(),
            detect_enabled = config.detect_enabled(),
            bucket_slots,
            digest_window_slots = window,
            already_cancelled = token.is_cancelled(),
            "ORACLE started"
        );
        Ok(Self { state, guard: Some(token.clone().drop_guard()), token, supervisor })
    }

    /// The HTTP API over this ORACLE's state. Every call builds a new router
    /// over the same state; each one keeps the store lock alive while it is.
    pub fn router(&self) -> axum::Router {
        api::router(Arc::clone(&self.state))
    }

    /// The state the API reads.
    pub fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// Resolves once ORACLE is stopping: the host cancelled its token, or a
    /// task failed. A host can `select!` on it to notice a failure.
    pub async fn stopped(&self) {
        self.token.cancelled().await;
    }

    /// Wait for ORACLE to stop. Cancels nothing: it returns once the host
    /// has cancelled its token (or a task has failed) and both tasks have
    /// stopped or been aborted. `Ok(())`, or the first
    /// [`OracleError::TaskFailed`].
    pub async fn wait(mut self) -> OracleResult<()> {
        let supervisor = &mut self.supervisor;
        let result = match supervisor.await {
            Ok(result) => result,
            Err(e) => Err(OracleError::task_failed(SUPERVISOR_TASK, join_reason(e))),
        };
        // Stopped already; nothing left for the guard to cancel.
        if let Some(guard) = self.guard.take() {
            guard.disarm();
        }
        result
    }
}

/// Step 8: warn when the stored digests were built with another window.
fn check_window_size(start_slot: u64, end_slot: u64, window: u64, bucket_secs: u64) -> bool {
    let stored = end_slot.saturating_sub(start_slot).saturating_add(1);
    if stored == window {
        return false;
    }
    warn!(
        stored_window_slots = stored,
        window_slots = window,
        bucket_secs,
        latest_digest_start = start_slot,
        latest_digest_end = end_slot,
        "the latest stored digest covers a different number of slots than the current digest window: \
         ORACLE_BUCKET_SECS changed since it was built. Stored digests are kept; new ones use the new \
         window, starting after the latest one ends"
    );
    true
}

// ---------------------------------------------------------------------------
// Supervision
// ---------------------------------------------------------------------------

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

/// Fail-stop supervision of `tasks` (see the [crate docs](crate#lifecycle)).
///
/// 1. A task that ends before `token` is cancelled is a failure: the first
///    one is recorded (`panicked: ...` or `exited unexpectedly`) and
///    `token` is cancelled.
/// 2. Once `token` is cancelled, `on_cancel` runs, then the remaining tasks
///    get `grace` to finish; any still running are aborted and recorded as
///    `did not stop`. A panic while stopping is recorded too.
/// 3. `Ok(())` if nothing was recorded, else the first failure.
pub async fn supervise(
    mut tasks: JoinSet<()>,
    names: HashMap<tokio::task::Id, &'static str>,
    token: CancellationToken,
    grace: Duration,
    on_cancel: impl FnOnce(),
) -> OracleResult<()> {
    let name = |id: tokio::task::Id| names.get(&id).copied().unwrap_or("unknown");
    let mut failure: Option<OracleError> = None;
    let mut record = |task: &str, reason: String| {
        error!(task, reason = %reason, "ORACLE task failed");
        failure.get_or_insert_with(|| OracleError::task_failed(task, reason));
    };

    // 1. Running: any task ending is a failure.
    while !token.is_cancelled() {
        tokio::select! {
            biased;
            () = token.cancelled() => {}
            joined = tasks.join_next_with_id() => match joined {
                None => break,
                Some(Ok((id, ()))) => {
                    record(name(id), "exited unexpectedly".to_owned());
                    token.cancel();
                }
                Some(Err(e)) => {
                    let task = name(e.id());
                    record(task, join_reason(e));
                    token.cancel();
                }
            },
        }
    }

    // 2. Stopping: bounded.
    token.cancel();
    on_cancel();
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match tokio::time::timeout_at(deadline, tasks.join_next_with_id()).await {
            Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            // A panic while stopping is a failure; a cancellation is only
            // ever our own abort below.
            Ok(Some(Err(e))) => {
                if e.is_panic() {
                    let task = name(e.id());
                    record(task, join_reason(e));
                }
            }
            Err(_) => {
                tasks.abort_all();
                // Only tasks still in the set are stuck.
                let mut stuck: Vec<&str> = Vec::new();
                while let Some(joined) = tasks.join_next_with_id().await {
                    let id = match &joined {
                        Ok((id, ())) => *id,
                        Err(e) => e.id(),
                    };
                    stuck.push(name(id));
                }
                stuck.sort_unstable();
                for task in stuck {
                    record(task, "did not stop".to_owned());
                }
                break;
            }
        }
    }
    match failure {
        None => {
            info!("ORACLE stopped");
            Ok(())
        }
        Some(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Task = std::pin::Pin<Box<dyn Future<Output = ()> + Send>>;
    type Names = HashMap<tokio::task::Id, &'static str>;

    fn set(tasks: Vec<(&'static str, Task)>) -> (JoinSet<()>, Names) {
        let mut set = JoinSet::new();
        let mut names = HashMap::new();
        for (name, fut) in tasks {
            names.insert(set.spawn(fut).id(), name);
        }
        (set, names)
    }

    fn until(token: &CancellationToken) -> Task {
        let t = token.clone();
        Box::pin(async move { t.cancelled().await })
    }

    fn task_failure(r: OracleResult<()>) -> (String, String) {
        match r {
            Err(OracleError::TaskFailed { task, reason }) => (task, reason),
            other => panic!("expected TaskFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_host_cancel_stops_cleanly() {
        let token = CancellationToken::new();
        let (tasks, names) = set(vec![("a", until(&token)), ("b", until(&token))]);
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let c = Arc::clone(&called);
        let sup = tokio::spawn(supervise(tasks, names, token.clone(), Duration::from_secs(1), move || {
            c.store(true, std::sync::atomic::Ordering::SeqCst)
        }));
        token.cancel();
        sup.await.unwrap().unwrap();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst), "on_cancel runs");
    }

    #[tokio::test]
    async fn an_early_exit_is_a_failure_and_stops_the_others() {
        let token = CancellationToken::new();
        let (tasks, names) = set(vec![("a", Box::pin(async {})), ("b", until(&token))]);
        let r = supervise(tasks, names, token.clone(), Duration::from_secs(1), || {}).await;
        assert_eq!(task_failure(r), ("a".to_owned(), "exited unexpectedly".to_owned()));
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn a_panic_is_reported_with_its_message() {
        let token = CancellationToken::new();
        let (tasks, names) = set(vec![("a", until(&token)), ("b", Box::pin(async { panic!("boom") }))]);
        let r = supervise(tasks, names, token.clone(), Duration::from_secs(1), || {}).await;
        assert_eq!(task_failure(r), ("b".to_owned(), "panicked: boom".to_owned()));
    }

    #[tokio::test]
    async fn only_the_first_failure_is_returned() {
        let token = CancellationToken::new();
        let t = token.clone();
        // `b` panics while stopping, after `a` already failed.
        let b = Box::pin(async move {
            t.cancelled().await;
            panic!("late")
        });
        let (tasks, names) = set(vec![("a", Box::pin(async {})), ("b", b)]);
        let r = supervise(tasks, names, token, Duration::from_secs(1), || {}).await;
        assert_eq!(task_failure(r).0, "a");
    }

    #[tokio::test(start_paused = true)]
    async fn a_task_that_ignores_cancellation_is_aborted_after_the_grace() {
        let token = CancellationToken::new();
        let stubborn: Task = Box::pin(std::future::pending());
        let (tasks, names) = set(vec![("a", until(&token)), ("stuck", stubborn)]);
        let started = tokio::time::Instant::now();
        let sup = tokio::spawn(supervise(tasks, names, token.clone(), Duration::from_secs(SHUTDOWN_GRACE_SECS), || {}));
        token.cancel();
        let r = sup.await.unwrap();
        assert_eq!(task_failure(r), ("stuck".to_owned(), "did not stop".to_owned()));
        let waited = started.elapsed();
        assert!(waited >= Duration::from_secs(SHUTDOWN_GRACE_SECS), "{waited:?}");
        assert!(waited < Duration::from_secs(SHUTDOWN_GRACE_SECS + 1), "{waited:?}");
    }

    #[tokio::test]
    async fn an_already_cancelled_token_is_not_an_error() {
        let token = CancellationToken::new();
        token.cancel();
        let (tasks, names) = set(vec![("a", until(&token)), ("b", until(&token))]);
        supervise(tasks, names, token, Duration::from_secs(1), || {}).await.unwrap();
    }

    #[test]
    fn a_changed_window_is_detected() {
        assert!(!check_window_size(0, 37_499, 37_500, 300));
        assert!(check_window_size(0, 9_999, 37_500, 300));
    }
}
