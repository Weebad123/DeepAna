//! The `lens` binary: load the config, start the engine, serve until
//! Ctrl-C or SIGTERM.
//!
//! ```text
//! lens                                 serve the directory in LENS_DATA_DIR
//! lens --parquet-directory <DIR>       run PRISM writing to DIR, and serve DIR
//! ```
//!
//! With `--parquet-directory`, PRISM's ingestor runs in the same process and
//! writes its Parquet files, cursor, skip log and change log
//! (`changelog.jsonl`, the file FORGE tails) under `DIR`, and `DIR` is
//! used as `LENS_DATA_DIR` (the flag wins over the variable). PRISM follows
//! the Solana devnet tip through Helius and needs `HELIUS_API_KEY`. The two
//! stop together: on Ctrl-C or SIGTERM, or as soon as either one fails.

use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::PathBuf;
use std::process::ExitCode;
use std::str::FromStr;

use lens::{LensConfig, LensError, LensResult, LensServiceConfig};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;

const PARQUET_DIRECTORY_FLAG: &str = "--parquet-directory";

const USAGE: &str = "\
usage: lens [--parquet-directory <DIR>]

  --parquet-directory <DIR>  run PRISM in-process, writing Parquet files to DIR,
                             and serve DIR (overrides LENS_DATA_DIR).
                             PRISM reads its Helius API key from HELIUS_API_KEY.
  -h, --help                 print this help";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
enum Cli {
    Help,
    Run { parquet_directory: Option<PathBuf> },
}

#[tokio::main]
async fn main() -> ExitCode {
    let parquet_directory = match parse_args(std::env::args_os().skip(1)) {
        Ok(Cli::Run { parquet_directory }) => parquet_directory,
        Ok(Cli::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("lens: {message}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    // Before tracing is set up (its level is in the config), so report
    // config errors on stderr directly.
    let loaded = match &parquet_directory {
        Some(dir) => LensServiceConfig::for_parquet_directory(dir, |name| std::env::var_os(name)),
        None => LensConfig::load().map(LensServiceConfig::serve_only),
    };
    let service = match loaded {
        Ok(service) => service,
        Err(err) => {
            eprintln!("lens: {err}");
            return ExitCode::FAILURE;
        }
    };

    let level = Level::from_str(service.config.log_level()).expect("log_level is validated when the config is loaded");
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    service.config.log_startup();

    match run(service).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "LENS failed");
            ExitCode::FAILURE
        }
    }
}

/// Start LENS (and PRISM), serve until Ctrl-C, SIGTERM, or LENS stopping on
/// its own, then wait for everything to stop.
async fn run(service: LensServiceConfig) -> LensResult<()> {
    let addr = service.config.socket_addr();
    let token = CancellationToken::new();
    let lens = lens::start(service, token.clone()).await?;
    let listener = match TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(err) => {
            // Lets PRISM flush and release its change log.
            token.cancel();
            let _ = lens.wait().await;
            return Err(LensError::io(format!("bind address {addr}"), err));
        }
    };
    tracing::info!(address = %addr, "LENS listening");

    // Stop serving on a signal, and also if LENS stops on its own: LENS
    // would otherwise keep serving data that no longer grows.
    let stop = {
        let token = token.clone();
        async move {
            tokio::select! {
                () = lens::shutdown_signal() => tracing::info!("shutdown signal received; draining requests"),
                () = token.cancelled() => {}
            }
        }
    };
    let watch = {
        let token = token.clone();
        let router = lens.router();
        async move {
            let served = axum::serve(listener, router).with_graceful_shutdown(stop).await;
            token.cancel();
            served
        }
    };
    let (served, ()) = tokio::join!(watch, async {
        lens.stopped().await;
        token.cancel();
    });

    // Lets PRISM flush its buffer and save its cursor if LENS stopped first.
    token.cancel();
    let stopped = lens.wait().await;
    // A task failure is the root cause of a serve error that follows it.
    stopped.and(served.map_err(|err| LensError::io(format!("serving on {addr}"), err)))
}

/// Parse the arguments after the program name.
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Cli, String> {
    let mut parquet_directory = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = if arg == PARQUET_DIRECTORY_FLAG {
            args.next()
                .ok_or_else(|| format!("{PARQUET_DIRECTORY_FLAG} needs a directory"))?
        } else if let Some(value) = arg
            .to_str()
            .and_then(|arg| arg.strip_prefix(PARQUET_DIRECTORY_FLAG)?.strip_prefix('='))
        {
            OsString::from(value)
        } else if arg == "-h" || arg == "--help" {
            return Ok(Cli::Help);
        } else {
            return Err(format!("unexpected argument {:?}", arg.to_string_lossy()));
        };
        if value.is_empty() {
            return Err(format!("{PARQUET_DIRECTORY_FLAG} needs a directory"));
        }
        if parquet_directory.replace(PathBuf::from(value)).is_some() {
            return Err(format!("{PARQUET_DIRECTORY_FLAG} given more than once"));
        }
    }
    Ok(Cli::Run { parquet_directory })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, String> {
        parse_args(args.iter().map(OsString::from))
    }

    fn run(dir: Option<&str>) -> Result<Cli, String> {
        Ok(Cli::Run { parquet_directory: dir.map(PathBuf::from) })
    }

    #[test]
    fn no_arguments_means_no_parquet_directory() {
        assert_eq!(parse(&[]), run(None));
    }

    #[test]
    fn parquet_directory_accepts_both_spellings() {
        assert_eq!(parse(&["--parquet-directory", "./data"]), run(Some("./data")));
        assert_eq!(parse(&["--parquet-directory=./data"]), run(Some("./data")));
    }

    #[test]
    fn help_is_recognised() {
        assert_eq!(parse(&["--help"]), Ok(Cli::Help));
        assert_eq!(parse(&["-h"]), Ok(Cli::Help));
    }

    #[test]
    fn bad_arguments_are_rejected() {
        for args in [
            &["--parquet-directory"][..],
            &["--parquet-directory="],
            &["--parquet-directory", ""],
            &["--parquet-directory", "a", "--parquet-directory", "b"],
            &["--parquet-directoryx"],
            &["./data"],
        ] {
            assert!(parse(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn table_dirs_satisfy_the_lens_config() {
        use lens::config::ENV_DATA_DIR;
        use std::fs;
        let dir = std::env::temp_dir().join(format!("lens-main-test-{}", std::process::id()));
        lens::create_table_dirs(&dir).unwrap();
        let config = LensConfig::load_with(|name| (name == ENV_DATA_DIR).then(|| dir.clone().into_os_string()))
            .unwrap();
        for table in lens::config::TABLE_DIRS {
            assert!(config.table_dir(table).is_dir(), "{table}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }
}
