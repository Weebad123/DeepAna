//! The `deepana` binary: PRISM, LENS, FORGE and ORACLE in one process,
//! served with the dashboard on one port.
//!
//! ```text
//! deepana --parquet-directory <DIR> [--open] [--commons-root <PATH>]
//! ```
//!
//! Every file any service writes is under `<DIR>` (see `deepana::layout`).
//! DeepAna listens on `127.0.0.1:$DEEPANA_PORT`, default 8099. PRISM needs
//! `HELIUS_API_KEY`. Ctrl-C or SIGTERM stops everything, and so does any one
//! service failing (the exit code is then 1).

use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use clap::Parser;
use deepana::app::{self, RunOptions};
use deepana::commons_panel::CommonsPanel;
use deepana::config::{self, ServiceConfigs};
use deepana::layout::Layout;
use tokio_util::sync::CancellationToken;
use tracing::Level;

/// Run every DeepAna service in one process, with a live dashboard.
#[derive(Debug, Parser)]
#[command(name = "deepana", version)]
struct Cli {
    /// The data directory. PRISM's Parquet tables go in <DIR>/solana/; every
    /// other file any service writes goes directly in <DIR>. Created if
    /// missing.
    #[arg(long, value_name = "DIR")]
    parquet_directory: PathBuf,
    /// Open the dashboard in the default browser once it is up.
    #[arg(long)]
    open: bool,
    /// The COMMONS repository (artifacts/ + fixtures/) the dashboard
    /// verifies. Only read. Default: <DIR>/commons.
    #[arg(long, value_name = "PATH")]
    commons_root: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let env = |name: &str| std::env::var_os(name);

    // Before tracing is set up, so errors go to stderr directly.
    let level = match config::log_level(env) {
        Ok(level) => level,
        Err(message) => return fail(&message),
    };
    let listen = match config::listen_address(env) {
        Ok(listen) => listen,
        Err(message) => return fail(&message),
    };
    let layout = match Layout::create(&cli.parquet_directory) {
        Ok(layout) => layout,
        Err(err) => return fail(&format!("data directory {}: {err}", cli.parquet_directory.display())),
    };
    let configs = match ServiceConfigs::build(&layout, listen, env) {
        Ok(configs) => configs,
        Err(message) => return fail(&message),
    };

    tracing_subscriber::fmt()
        .with_max_level(Level::from_str(&level).expect("validated above"))
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    configs.lens.config.log_startup();
    configs.forge.display();
    configs.oracle.display();

    let commons = CommonsPanel::new(cli.commons_root.unwrap_or_else(|| layout.commons_root()));
    let root = CancellationToken::new();
    tokio::spawn({
        let root = root.clone();
        async move {
            lens::shutdown_signal().await;
            tracing::info!("shutdown signal received");
            root.cancel();
        }
    });

    let options = RunOptions { layout, listen, configs, commons, open_browser: cli.open };
    match app::run(options, root).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            for failure in &err.failures {
                tracing::error!(failure = %failure, "DeepAna failed");
            }
            ExitCode::FAILURE
        }
    }
}

fn fail(message: &str) -> ExitCode {
    eprintln!("deepana: {message}");
    ExitCode::FAILURE
}
