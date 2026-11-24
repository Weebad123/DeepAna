//! One process, one port: LENS (with PRISM), FORGE and ORACLE, the
//! dashboard and its live stream.
//!
//! ```text
//!   root CancellationToken
//!     ├─ child ─ lens::start    (PRISM writes <DIR>, LENS serves it)
//!     ├─ child ─ forge::start   (tails <DIR>/changelog.jsonl, LENS via /api/lens)
//!     ├─ child ─ Oracle::start  (LENS via /api/lens)
//!     └─ Hub::run               (the one poller behind /api/stream)
//!
//!   axum on 127.0.0.1:<port>
//!     /                     the dashboard
//!     /api/lens/...         LENS's router
//!     /api/forge/...        FORGE's router
//!     /api/oracle/...       ORACLE's router
//!     /api/stream           SSE
//!     /api/commons/report   last COMMONS verify report
//!     /api/commons/verify   POST: run verify now
//!     /api/demo/alert       POST: create FORGE's demo alert
//!     /demo/hook            POST: the demo alert's webhook
//! ```
//!
//! # Supervision: fail-stop
//!
//! Each service already stops itself when one of its own tasks fails, and
//! reports it through `stopped()`. DeepAna watches all three and the HTTP
//! server: the first of them to stop while DeepAna is meant to be running
//! is logged as the cause and cancels the root token, which stops every
//! other service (PRISM flushes and saves its cursor, FORGE and ORACLE
//! drain). Nothing is restarted. [`run`] then waits for all of them and
//! returns an error naming the first failure, so the process exits
//! non-zero. Ctrl-C / SIGTERM cancels the root token too, and is not a
//! failure.

use std::net::SocketAddr;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use oracle::Oracle;
use serde_json::json;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::commons_panel::CommonsPanel;
use crate::config::{FORGE_PREFIX, LENS_PREFIX, ORACLE_PREFIX, ServiceConfigs};
use crate::demo_hook::{self, DEMO_ALERT_NAME, DEMO_HOOK_PATH, DemoHook};
use crate::layout::Layout;
use crate::stream::{self, Hub};

/// The dashboard page, compiled in.
pub const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// What the DeepAna-level handlers share.
#[derive(Debug, Clone)]
pub struct AppState {
    pub hub: Hub,
    pub commons: CommonsPanel,
    pub demo: DemoHook,
    /// The nested service routers, for in-process calls.
    pub api: Router,
}

/// The services' routers, nested under their prefixes.
pub fn api_router(lens: Router, forge: Router, oracle: Router) -> Router {
    Router::new().nest(LENS_PREFIX, lens).nest(FORGE_PREFIX, forge).nest(ORACLE_PREFIX, oracle)
}

/// The whole app: the dashboard routes merged with `api`.
pub fn app_router(api: Router, state: AppState) -> Router {
    let own = Router::new()
        .route("/", get(dashboard))
        .route("/api/stream", get(stream_handler))
        .route("/api/commons/report", get(commons_report))
        .route("/api/commons/verify", post(commons_verify))
        .route("/api/demo/alert", post(demo_alert))
        .route(DEMO_HOOK_PATH, post(demo_receive))
        .with_state(state);
    own.merge(api)
}

async fn dashboard() -> Response {
    ([(header::CACHE_CONTROL, "no-store")], Html(DASHBOARD_HTML)).into_response()
}

async fn stream_handler(State(state): State<AppState>) -> Response {
    stream::sse(&state.hub).into_response()
}

async fn commons_report(State(state): State<AppState>) -> Response {
    let report = state.hub.snapshot().commons;
    match report {
        Some(report) => axum::Json(report).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            axum::Json(json!({ "error": "verify has not run yet", "repo_root": state.commons.root().display().to_string() })),
        )
            .into_response(),
    }
}

async fn commons_verify(State(state): State<AppState>) -> Response {
    let report = state.commons.verify().await;
    state.hub.publish_commons(report.clone());
    axum::Json(report).into_response()
}

async fn demo_alert(State(state): State<AppState>) -> Response {
    let (status, body) = state.demo.create(&state.api).await;
    (status, axum::Json(body)).into_response()
}

async fn demo_receive(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let (status, receipt) = state.demo.receive(&headers, &body, demo_hook::now_secs());
    if !status.is_success() {
        tracing::warn!(reason = %receipt["error"], "refused a /demo/hook request");
    }
    state.hub.publish_demo_delivery(receipt);
    let answer = if status.is_success() { json!({ "ok": true }) } else { json!({ "error": "signature check failed" }) };
    (status, axum::Json(answer)).into_response()
}

/// What [`run`] needs.
#[derive(Debug)]
pub struct RunOptions {
    pub layout: Layout,
    pub listen: SocketAddr,
    pub configs: ServiceConfigs,
    pub commons: CommonsPanel,
    /// Open the dashboard in a browser once the server is listening.
    pub open_browser: bool,
}

/// Why [`run`] failed: the first failure, and every one after it.
#[derive(Debug)]
pub struct RunError {
    pub failures: Vec<String>,
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.failures.join("; "))
    }
}

impl std::error::Error for RunError {}

impl RunError {
    fn one(message: impl Into<String>) -> Self {
        Self { failures: vec![message.into()] }
    }
}

/// Start everything, serve until `root` is cancelled or something fails,
/// then stop everything. See the [module docs](self).
pub async fn run(options: RunOptions, root: CancellationToken) -> Result<(), RunError> {
    let RunOptions { layout, listen, configs, commons, open_browser } = options;

    // Bound first: a port in use fails before anything touches the disk
    // beyond LENS's empty table directories.
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|err| RunError::one(format!("cannot listen on {listen}: {err}")))?;
    serve_on(listener, layout, configs, commons, open_browser, root).await
}

/// [`run`] on a listener the caller bound (tests bind port 0).
pub async fn serve_on(
    listener: TcpListener,
    layout: Layout,
    configs: ServiceConfigs,
    commons: CommonsPanel,
    open_browser: bool,
    root: CancellationToken,
) -> Result<(), RunError> {
    let local = listener.local_addr().map_err(|err| RunError::one(format!("listener address: {err}")))?;
    let ServiceConfigs { lens: lens_config, forge: forge_config, oracle: oracle_config } = configs;
    let settings = dashboard_settings(&oracle_config);

    // LENS first: it creates the change log FORGE opens.
    let lens = lens::start(lens_config, root.clone())
        .await
        .map_err(|err| RunError::one(format!("LENS could not start: {err}")))?;
    let forge = match forge::start(forge_config, root.clone()).await {
        Ok(forge) => forge,
        Err(err) => {
            root.cancel();
            let mut failures = vec![format!("FORGE could not start: {err}")];
            if let Err(err) = lens.wait().await {
                failures.push(format!("LENS: {err}"));
            }
            return Err(RunError { failures });
        }
    };
    let oracle = match Oracle::start(oracle_config, root.clone()).await {
        Ok(oracle) => oracle,
        Err(err) => {
            root.cancel();
            let mut failures = vec![format!("ORACLE could not start: {err}")];
            let (l, f) = tokio::join!(lens.wait(), forge.wait());
            failures.extend(l.err().map(|e| format!("LENS: {e}")));
            failures.extend(f.err().map(|e| format!("FORGE: {e}")));
            return Err(RunError { failures });
        }
    };

    let api = api_router(lens.router(), forge.router(), oracle.router());
    let hub = Hub::new(layout.changelog(), layout.root().display().to_string(), root.clone());
    hub.set_settings(settings);
    let poller = tokio::spawn(hub.clone().run(api.clone()));
    let demo = DemoHook::new(local);
    let state = AppState { hub: hub.clone(), commons: commons.clone(), demo: demo.clone(), api: api.clone() };
    let app = app_router(api.clone(), state);

    // An existing demo alert gets this process's secret and webhook URL.
    let demo_sync = tokio::spawn({
        let root = root.clone();
        async move {
            tokio::select! {
                () = root.cancelled() => {}
                synced = demo.sync(&api) => match synced {
                    Ok(Some(_)) => tracing::info!(alert = DEMO_ALERT_NAME, "demo alert now delivers to this process"),
                    Ok(None) => {}
                    Err(err) => tracing::warn!(alert = DEMO_ALERT_NAME, error = %err, "cannot update the demo alert"),
                },
            }
        }
    });

    // A first COMMONS report, so the panel is not empty.
    let first_verify = tokio::spawn({
        let hub = hub.clone();
        let commons = commons.clone();
        let root = root.clone();
        async move {
            tokio::select! {
                () = root.cancelled() => {}
                report = commons.verify() => hub.publish_commons(report),
            }
        }
    });

    let url = format!("http://{local}/");
    tracing::info!(
        address = %local,
        data_dir = %layout.root().display(),
        lens_url = %crate::config::lens_url(local),
        commons_root = %commons.root().display(),
        "DeepAna listening; dashboard at {url}"
    );
    if open_browser {
        open_in_browser(&url);
    }

    let mut server = tokio::spawn({
        let root = root.clone();
        async move { axum::serve(listener, app).with_graceful_shutdown(root.cancelled_owned()).await }
    });

    // Fail-stop: the first thing to stop while we are running is the cause.
    let mut failures = Vec::new();
    let mut server_result = None;
    tokio::select! {
        () = root.cancelled() => tracing::info!("shutting down"),
        () = lens.stopped() => failures.push("LENS (or PRISM) stopped on its own".to_owned()),
        () = forge.stopped() => failures.push("FORGE stopped on its own".to_owned()),
        () = oracle.stopped() => failures.push("ORACLE stopped on its own".to_owned()),
        joined = &mut server => {
            failures.push(match &joined {
                Ok(Ok(())) => "the HTTP server stopped on its own".to_owned(),
                Ok(Err(err)) => format!("the HTTP server failed: {err}"),
                Err(err) => format!("the HTTP server task failed: {err}"),
            });
            server_result = Some(());
        }
    }
    if let Some(cause) = failures.first() {
        tracing::error!(cause = %cause, "stopping every service");
    }
    root.cancel();

    // Drain: the server (SSE streams end on the token), then the poller,
    // then the services, in parallel.
    if server_result.is_none() {
        match server.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => failures.push(format!("the HTTP server failed: {err}")),
            Err(err) => failures.push(format!("the HTTP server task failed: {err}")),
        }
    }
    let _ = poller.await;
    let _ = first_verify.await;
    let _ = demo_sync.await;
    let (l, f, o) = tokio::join!(lens.wait(), forge.wait(), oracle.wait());
    failures.extend(l.err().map(|e| format!("LENS: {e}")));
    failures.extend(f.err().map(|e| format!("FORGE: {e}")));
    failures.extend(o.err().map(|e| format!("ORACLE: {e}")));

    if failures.is_empty() {
        tracing::info!("DeepAna stopped");
        Ok(())
    } else {
        Err(RunError { failures })
    }
}

/// What the dashboard needs that no API serves: ORACLE's slot time and
/// detector baseline, and the demo alert's name.
pub fn dashboard_settings(oracle: &oracle::OracleConfig) -> serde_json::Value {
    use oracle::detect::detectors::{SLOT_MS, bucket_slots};
    json!({
        "ms_per_slot": SLOT_MS,
        "bucket_secs": oracle.bucket_secs(),
        "bucket_slots": bucket_slots(oracle.bucket_secs()),
        // Buckets of history a detector needs before it scores anything.
        "baseline_need_buckets": oracle::detect::min_history(u64::from(oracle.baseline_buckets())),
        "digest_buckets": oracle::digest::DIGEST_BUCKETS,
        "demo_alert_name": DEMO_ALERT_NAME,
    })
}

/// Open `url` in the default browser. Best effort: a failure is logged.
pub fn open_in_browser(url: &str) {
    let (program, args): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("open", vec![url])
    } else if cfg!(target_os = "windows") {
        ("cmd", vec!["/C", "start", "", url])
    } else {
        ("xdg-open", vec![url])
    };
    let spawned = std::process::Command::new(program)
        .args(&args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    match spawned {
        Ok(_) => tracing::info!(url, "opened the dashboard in the browser"),
        Err(err) => tracing::warn!(url, program, error = %err, "cannot open a browser; open the URL yourself"),
    }
}
