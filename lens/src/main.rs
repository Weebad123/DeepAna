//! The `lens` binary: load the config, start the engine, serve until
//! Ctrl-C or SIGTERM.
//!
//! ```text
//! lens                                 serve the directory in LENS_DATA_DIR
//! lens --parquet-directory <DIR>       run PRISM writing to DIR, and serve DIR
//! ```
//!
//! With `--parquet-directory`, PRISM's ingestor runs in the same process and
//! writes its Parquet files, cursor and skip log under `DIR`, and `DIR` is
//! used as `LENS_DATA_DIR` (the flag wins over the variable). PRISM follows
//! the Solana devnet tip through Helius and needs `HELIUS_API_KEY`. The two
//! stop together: on Ctrl-C or SIGTERM, or as soon as either one fails.

use std::ffi::OsString;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use lens::config::ENV_DATA_DIR;
use lens::{LensConfig, LensError, LensResult, QueryEngine};
use prism::adapter::ChainAdapter;
use prism::changelog::ChangeLog;
use prism::commitment::CommitmentTracker;
use prism::ingestor::{Cluster, Ingestor, IngestorConfig, helius_api_key_from_env, helius_solana_adapter};
use prism::storage::TableKind;
use prism::storage::partition::CHAIN_DIR;
use prism::types::error::PrismResult;
use prism::types::event::ChangeEvent;
use tokio_util::sync::CancellationToken;
use tracing::Level;

const PARQUET_DIRECTORY_FLAG: &str = "--parquet-directory";

/// The cluster PRISM ingests when started with `--parquet-directory`.
const PRISM_CLUSTER: Cluster = Cluster::Devnet;

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
        Some(dir) => create_table_dirs(dir).and_then(|()| {
            LensConfig::load_with(|name| {
                if name == ENV_DATA_DIR { Some(dir.clone().into_os_string()) } else { std::env::var_os(name) }
            })
        }),
        None => LensConfig::load(),
    };
    let config = match loaded {
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

    // Built before LENS starts listening, so a missing API key fails fast.
    let ingestor = match parquet_directory {
        Some(_) => match build_ingestor(config.data_dir()).await {
            Ok(ingestor) => Some(Arc::new(ingestor)),
            Err(err) => {
                tracing::error!(kind = err.kind(), error = %err, "could not start PRISM");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let engine = match QueryEngine::new(config) {
        Ok(engine) => Arc::new(engine),
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "could not start the query engine");
            return ExitCode::FAILURE;
        }
    };

    let shutdown = CancellationToken::new();
    tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            lens::shutdown_signal().await;
            shutdown.cancel();
        }
    });

    let prism = ingestor.map(|ingestor| {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let stop = ingestor.shutdown_token();
            let forward = tokio::spawn({
                let shutdown = shutdown.clone();
                async move {
                    shutdown.cancelled().await;
                    stop.cancel();
                }
            });
            let result = ingestor.run().await;
            forward.abort();
            // LENS would otherwise keep serving data that no longer grows.
            shutdown.cancel();
            result
        })
    });

    let mut ok = match lens::serve(engine, shutdown.clone().cancelled_owned()).await {
        Ok(()) => true,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "server failed");
            false
        }
    };
    // Lets PRISM flush its buffer and save its cursor if LENS stopped first.
    shutdown.cancel();

    if let Some(prism) = prism {
        match prism.await {
            Ok(Ok(report)) => tracing::info!(
                slots_processed = report.slots_processed,
                slots_skipped = report.slots_skipped,
                last_flush_slot = report.cursor.last_flush_slot,
                "PRISM stopped"
            ),
            Ok(Err(err)) => {
                tracing::error!(kind = err.kind(), error = %err, "PRISM failed");
                ok = false;
            }
            Err(err) => {
                tracing::error!(error = %err, "PRISM task panicked");
                ok = false;
            }
        }
    }

    if ok { ExitCode::SUCCESS } else { ExitCode::FAILURE }
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

/// Create `{dir}/solana/{table}` for every table PRISM writes. PRISM would
/// create them on its first flush, but LENS refuses to start on a directory
/// without any, and an empty table directory reads as an empty table.
fn create_table_dirs(dir: &Path) -> LensResult<()> {
    for table in TableKind::ALL {
        let table_dir = dir.join(CHAIN_DIR).join(table.as_str());
        fs::create_dir_all(&table_dir).map_err(|err| LensError::io(table_dir.display().to_string(), err))?;
    }
    Ok(())
}

/// PRISM's ingestor over Helius, writing under `data_dir`.
async fn build_ingestor(data_dir: &Path) -> PrismResult<Ingestor> {
    let api_key = helius_api_key_from_env()?;
    let adapter: Arc<dyn ChainAdapter> = Arc::new(helius_solana_adapter(&api_key, PRISM_CLUSTER).await?);
    let changelog: Arc<dyn ChangeLog> = Arc::new(TracingChangeLog);
    let tracker = CommitmentTracker::spawn(Arc::clone(&adapter), Arc::clone(&changelog));
    Ingestor::new(adapter, Arc::new(tracker), changelog, IngestorConfig::new(data_dir))
}

/// Logs PRISM's change events. PRISM ships no change log sink yet, and
/// nothing in this process needs one: LENS finds new files by re-scanning
/// the directory.
struct TracingChangeLog;

#[async_trait]
impl ChangeLog for TracingChangeLog {
    async fn emit(&self, event: ChangeEvent) -> PrismResult<()> {
        tracing::debug!(kind = event.kind(), slot = event.slot(), "{event}");
        Ok(())
    }
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
        let dir = std::env::temp_dir().join(format!("lens-main-test-{}", std::process::id()));
        create_table_dirs(&dir).unwrap();
        let config = LensConfig::load_with(|name| (name == ENV_DATA_DIR).then(|| dir.clone().into_os_string()))
            .unwrap();
        for table in lens::config::TABLE_DIRS {
            assert!(config.table_dir(table).is_dir(), "{table}");
        }
        fs::remove_dir_all(&dir).unwrap();
    }
}
