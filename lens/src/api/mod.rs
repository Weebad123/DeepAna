//! API: LENS over HTTP.
//!
//! | Route          | Handler                         | File       |
//! |----------------|---------------------------------|------------|
//! | `POST /query`  | [`query::query_handler`]        | [`query`]  |
//! | `GET /health`  | [`health::health_handler`]      | [`health`] |
//!
//! [`serve`] runs the server and the catalog refresh task together, and
//! stops both on one shutdown:
//!
//! ```text
//!   serve(engine, signal)
//!     AppState ─► router ─► start_catalog_refresh ─► bind ─► axum::serve
//!                                                               │ signal, or engine.shutdown()
//!                                                               ▼
//!                         await refresh task ◄── cancel token ◄── drain in-flight requests
//! ```
//!
//! # State
//!
//! [`AppState`] has no catalog of its own: the [`QueryEngine`] owns the one
//! registered in its session (and the refresh task refreshes that one), and
//! the health check reads it through [`QueryEngine::table_status`]. A second
//! handle here could only drift from it.

pub mod health;
pub mod query;

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use axum::Router;
use axum::routing::{get, post};
use tokio::net::TcpListener;

pub use health::{CatalogHealth, EngineHealth, HealthResponse, HealthStatus, compute_status, health_handler};
pub use query::{ApiError, ErrorBody, QueryRequest, error_message, parse_format, query_handler, status_for};

use crate::config::LensConfig;
use crate::engine::QueryEngine;
use crate::error::{LensError, LensResult};

/// What every handler gets. Cheap to clone.
#[derive(Debug, Clone)]
pub struct AppState {
    pub engine: Arc<QueryEngine>,
    pub config: Arc<LensConfig>,
    /// When the server started, for `uptime_secs`.
    pub start_time: Instant,
}

impl AppState {
    /// State for `engine`, started now.
    pub fn new(engine: Arc<QueryEngine>) -> Self {
        let config = Arc::new(engine.config().clone());
        Self { engine, config, start_time: Instant::now() }
    }
}

/// Every route, with `state`.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/query", post(query::query_handler))
        .route("/health", get(health::health_handler))
        .with_state(state)
}

/// Serve the API on the configured `bind_address` until `shutdown`
/// resolves or [`QueryEngine::shutdown`] is called. See [`serve_on`].
pub async fn serve<F>(engine: Arc<QueryEngine>, shutdown: F) -> LensResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let addr = engine.config().socket_addr();
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| LensError::io(format!("bind address {addr}"), err))?;
    serve_on(listener, engine, shutdown).await
}

/// Serve the API on `listener` until `shutdown` resolves or
/// [`QueryEngine::shutdown`] is called, with the catalog refreshed in the
/// background meanwhile.
///
/// On shutdown, requests in flight are allowed to finish, the engine's
/// shutdown token is cancelled, and the refresh task is awaited, so a
/// refresh in progress is not cut off. Returns once all of that is done.
pub async fn serve_on<F>(listener: TcpListener, engine: Arc<QueryEngine>, shutdown: F) -> LensResult<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let state = AppState::new(Arc::clone(&engine));
    let config = Arc::clone(&state.config);
    let app = router(state);
    let refresh = engine.start_catalog_refresh(config.catalog_refresh_interval_secs());

    let local_addr = listener.local_addr().map_err(|err| LensError::io("listener address", err))?;
    tracing::info!(
        address = %local_addr,
        chain = config.chain(),
        data_dir = %config.data_dir().display(),
        max_query_rows = config.max_query_rows(),
        query_timeout_secs = config.query_timeout_secs(),
        "LENS listening"
    );

    let token = engine.shutdown_token();
    let stop = async move {
        tokio::select! {
            () = shutdown => tracing::info!("shutdown signal received; draining requests"),
            () = token.cancelled() => tracing::info!("engine shut down; draining requests"),
        }
    };
    let served = axum::serve(listener, app).with_graceful_shutdown(stop).await;

    engine.shutdown();
    if let Err(err) = refresh.await {
        tracing::warn!(error = %err, "catalog refresh task did not stop cleanly");
    }
    tracing::info!("LENS stopped");
    served.map_err(|err| LensError::io(format!("serving on {local_addr}"), err))
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %err, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                tracing::error!(error = %err, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
