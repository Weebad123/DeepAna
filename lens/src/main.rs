//! The `lens` binary: load the config, start the engine, serve until
//! Ctrl-C or SIGTERM.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use lens::{LensConfig, QueryEngine};
use tracing::Level;

#[tokio::main]
async fn main() -> ExitCode {
    // Before tracing is set up (its level is in the config), so report
    // config errors on stderr directly.
    let config = match LensConfig::load() {
        Ok(config) => Arc::new(config),
        Err(err) => {
            eprintln!("lens: {err}");
            return ExitCode::FAILURE;
        }
    };

    let level = Level::from_str(config.log_level()).expect("log_level is validated when the config is loaded");
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    config.log_startup();

    let engine = match QueryEngine::new(config) {
        Ok(engine) => Arc::new(engine),
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "could not start the query engine");
            return ExitCode::FAILURE;
        }
    };

    match lens::serve(engine, lens::shutdown_signal()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "server failed");
            ExitCode::FAILURE
        }
    }
}
