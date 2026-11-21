//! FORGE as a library service: [`start`] builds everything the `forge`
//! binary used to build in `main`, and returns a [`Forge`] handle the host
//! serves.
//!
//! ```ignore
//! let config = ForgeConfig::load()?;
//! let token = CancellationToken::new();
//! let forge = forge::start(config, token.clone()).await?;
//! axum::serve(listener, forge.router()).with_graceful_shutdown(token.clone().cancelled_owned()).await?;
//! token.cancel();
//! forge.wait().await?;
//! ```
//!
//! The shape matches ORACLE's `Oracle`. FORGE never binds a socket here and
//! never installs a tracing subscriber; the host does both, so
//! [`ForgeConfig::bind_address`] is only read by the `forge` binary.
//!
//! # Startup
//!
//! ```text
//!   Registry::open (takes the lock) ─► CheckpointStore + ChangelogReader::open
//!     ─► LensClient ─► WebhookService::start ─► AlertEngine (handle taken, run spawned)
//!     ─► release_final(committed Final slot) ─► AppState
//! ```
//!
//! # Lifecycle
//!
//! FORGE runs on a **child** of the host's token. Supervision is fail-stop:
//! the alert engine ending while FORGE is meant to be running (a panic, or
//! a normal exit) is a failure. It is recorded and FORGE's own token is
//! cancelled, which [`Forge::stopped`] reports, so the host can stop
//! serving: a FORGE that answers requests but evaluates no alerts would look
//! healthy. The host's token is never cancelled by FORGE.
//!
//! Once FORGE's token is cancelled the engine stops and the webhook
//! dispatcher is shut down. [`Forge::wait`] waits for both and returns the
//! first failure.
//!
//! # The registry lock
//!
//! The [`AppState`] holds the [`Registry`], and every router holds a clone
//! of the state, so the database lock is held as long as the [`Forge`] or
//! any router built by [`Forge::router`] is alive. [`Forge::wait`] consumes
//! the handle; a host that wants the lock released drops its routers too.
//!
//! Dropping a [`Forge`] without calling `wait` cancels its token.

use std::sync::Arc;

use axum::Router;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::sync::{CancellationToken, DropGuard};

use crate::api::{self, AppState};
use crate::config::{ForgeConfig, ForgeError, ForgeResult};
use crate::{AlertEngine, ChangelogReader, CheckpointStore, LensApi, LensClient, Registry, WebhookService};

/// The alert engine's task name, in [`ForgeError::TaskFailed`].
pub const ALERT_ENGINE_TASK: &str = "alert_engine";

/// [`Forge::start`].
pub async fn start(config: ForgeConfig, token: CancellationToken) -> ForgeResult<Forge> {
    Forge::start(config, token).await
}

/// A running FORGE. See the [module docs](self).
pub struct Forge {
    state: AppState,
    /// FORGE's own token, a child of the host's.
    token: CancellationToken,
    /// Cancels `token` if the handle is dropped without [`Forge::wait`].
    guard: Option<DropGuard>,
    supervisor: JoinHandle<ForgeResult<()>>,
}

impl std::fmt::Debug for Forge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forge")
            .field("state", &self.state)
            .field("stopping", &self.token.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Forge {
    /// Build every part in the [documented order](self#startup) and spawn
    /// the alert engine. Must be called inside a Tokio runtime.
    pub async fn start(config: ForgeConfig, token: CancellationToken) -> ForgeResult<Self> {
        let config = Arc::new(config);
        let registry = Arc::new(Registry::open(&config)?);
        let reader = ChangelogReader::open(&config, CheckpointStore::for_config(&config))?;
        let committed_final = reader.highest_final_slot();
        let lens: Arc<dyn LensApi> = Arc::new(LensClient::new(&config)?);
        let webhook = Arc::new(WebhookService::start(&config, registry.db().clone())?);

        let engine = match AlertEngine::new(registry.clone(), reader, lens.clone(), webhook.clone(), &config) {
            Ok(engine) => engine,
            Err(err) => {
                webhook.shutdown().await;
                return Err(err);
            }
        };
        let handle = engine.handle();

        // A child: a failure of ours never cancels the host.
        let token = token.child_token();
        let engine_task = tokio::spawn({
            let token = token.clone();
            async move { engine.run(token).await }
        });

        // Deliveries held for slots the committed change log already
        // finalized, in case the last run stopped between the two.
        if committed_final > 0 {
            match webhook.release_final(committed_final) {
                Ok(released) => tracing::info!(final_slot = committed_final, released, "released held deliveries"),
                Err(err) => tracing::warn!(final_slot = committed_final, error = %err, "cannot release held deliveries"),
            }
        }

        let state = AppState::new(registry, lens, webhook.clone(), handle, Arc::clone(&config));
        let supervisor = tokio::spawn(supervise(engine_task, webhook, token.clone()));
        tracing::info!(
            db_path = %config.db_path().display(),
            changelog_path = %config.changelog_path().display(),
            lens_url = config.lens_url(),
            "FORGE started"
        );
        Ok(Self { state, guard: Some(token.clone().drop_guard()), token, supervisor })
    }

    /// The HTTP API. Every call builds a new router over the same state;
    /// each one keeps the registry lock alive while it is.
    pub fn router(&self) -> Router {
        api::router(self.state.clone())
    }

    /// The state the API reads.
    pub fn state(&self) -> &AppState {
        &self.state
    }

    /// Resolves once FORGE is stopping: the host cancelled its token, or the
    /// alert engine stopped on its own.
    pub async fn stopped(&self) {
        self.token.cancelled().await;
    }

    /// Wait for FORGE to stop. Cancels nothing: returns once the host has
    /// cancelled its token (or the engine has failed), the engine has
    /// stopped and the webhook dispatcher has shut down. `Ok(())`, or the
    /// first failure.
    pub async fn wait(mut self) -> ForgeResult<()> {
        let result = match (&mut self.supervisor).await {
            Ok(result) => result,
            Err(err) => Err(ForgeError::task_failed("supervisor", join_reason(err))),
        };
        if let Some(guard) = self.guard.take() {
            guard.disarm();
        }
        tracing::info!("FORGE stopped");
        result
    }
}

/// Fail-stop supervision; see the [module docs](self#lifecycle).
async fn supervise(
    mut engine: JoinHandle<()>,
    webhook: Arc<WebhookService>,
    token: CancellationToken,
) -> ForgeResult<()> {
    let mut failure = None;
    let mut engine_done = false;
    tokio::select! {
        () = token.cancelled() => {}
        joined = &mut engine => {
            engine_done = true;
            let reason = match joined {
                Ok(()) => "exited unexpectedly".to_owned(),
                Err(err) => join_reason(err),
            };
            let err = ForgeError::task_failed(ALERT_ENGINE_TASK, reason);
            tracing::error!(kind = err.kind(), error = %err, "FORGE task failed");
            failure = Some(err);
        }
    }
    token.cancel();
    webhook.shutdown().await;
    if !engine_done {
        if let Err(err) = engine.await {
            let err = ForgeError::task_failed(ALERT_ENGINE_TASK, join_reason(err));
            tracing::error!(kind = err.kind(), error = %err, "FORGE task failed");
            failure.get_or_insert(err);
        }
    }
    failure.map_or(Ok(()), Err)
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
