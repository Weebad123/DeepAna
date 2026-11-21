//! The `forge` binary: load the config, open the registry and the change
//! log, start the webhook dispatcher and the alert engine, serve the API
//! until Ctrl-C or SIGTERM.
//!
//! ```text
//!   config ─► tracing ─► forge::start (registry, change log, LENS client,
//!     webhook dispatcher, alert engine) ─► bind ─► axum::serve(forge.router())
//!                                                   │ Ctrl-C / SIGTERM, or FORGE stops
//!                                                   ▼
//!       drop the registry (releases the lock) ◄── forge.wait() ◄── cancel the token
//! ```
//!
//! Everything but binding and serving lives in the library; see
//! `forge::start`.
//!
//! Every variable is read from the environment; see `forge::config`. A
//! config error is reported on stderr before tracing is set up, since the
//! log level is itself part of the config.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::str::FromStr;

use forge::{ForgeConfig, ForgeError};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match ForgeConfig::load() {
        Ok(config) => config,
        Err(err) => {
            eprintln!("forge: {err}");
            return ExitCode::FAILURE;
        }
    };
    let level = Level::from_str(config.log_level()).expect("log_level is validated when the config is loaded");
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    config.display();

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "FORGE failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: ForgeConfig) -> forge::ForgeResult<()> {
    let addr = config.socket_addr();
    let lens_url = config.lens_url().to_owned();
    let token = CancellationToken::new();
    let forge = forge::start(config, token.clone()).await?;
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            token.cancel();
            let _ = forge.wait().await;
            return Err(ForgeError::io(format!("bind address {addr}"), err));
        }
    };
    tracing::info!(
        address = %addr,
        lens_url,
        max_body_bytes = forge::MAX_BODY_BYTES,
        request_timeout_secs = forge::API_REQUEST_TIMEOUT.as_secs(),
        "FORGE listening"
    );

    // Stop serving on a signal, and also if FORGE stops on its own: a FORGE
    // that answers requests but evaluates no alerts would look healthy.
    let stop = {
        let token = token.clone();
        async move {
            tokio::select! {
                () = forge::shutdown_signal() => tracing::info!("shutdown signal received; draining requests"),
                () = token.cancelled() => {}
            }
        }
    };
    let router = forge.router();
    let serve = async {
        let served = axum::serve(listener, router).with_graceful_shutdown(stop).await;
        token.cancel();
        served
    };
    let (served, ()) = tokio::join!(serve, async {
        forge.stopped().await;
        token.cancel();
    });

    token.cancel();
    // Consumes the handle: with the router already dropped by `serve`, this
    // releases the registry lock.
    let stopped = forge.wait().await;
    // A task failure is the root cause of a serve error that follows it.
    stopped.and(served.map_err(|err| ForgeError::io(format!("serving on {addr}"), err)))
}
