//! The `oracle` binary: load the config, point ORACLE at LENS, start it,
//! serve its API until Ctrl-C or SIGTERM.
//!
//! ```text
//! oracle                                   LENS at ORACLE_LENS_URL, or http://127.0.0.1:8080
//! oracle --lens-url http://10.0.0.5:8080   LENS at that URL
//! oracle --bind 0.0.0.0:9000               listen there instead of 127.0.0.1:8082
//! ```
//!
//! ```text
//!   args ─► config (LENS URL resolved) ─► tracing ─► bind listener ─► probe LENS /health (log only)
//!     ─► Oracle::start ─► axum::serve
//!                            │ Ctrl-C / SIGTERM, or ORACLE stops on a task failure
//!                            ▼
//!          exit code ◄── Oracle::wait ◄── cancel the token, drain requests
//! ```
//!
//! # Where LENS is
//!
//! LENS is ORACLE's only data source. The binary resolves its URL, first
//! match wins:
//!
//! 1. `--lens-url <URL>`
//! 2. `ORACLE_LENS_URL`
//! 3. [`DEFAULT_LENS_URL`], LENS's own default bind address, so a LENS
//!    started with `cargo run -p lens` on the same machine is found with no
//!    configuration (FORGE uses the same default).
//!
//! The library validates the result like any other `ORACLE_LENS_URL`. A
//! variable that is set but empty is still rejected rather than replaced by
//! the default.
//!
//! LENS does not have to be up when ORACLE starts: the binary asks LENS's
//! `/health` once and logs the answer, and ORACLE's own `/health` reports
//! `degraded` until LENS answers.
//!
//! # Where ORACLE listens
//!
//! The library never binds a socket, so the address is the binary's own:
//! `--bind <ip:port>`, then `ORACLE_BIND_ADDRESS`, then
//! [`DEFAULT_BIND_ADDRESS`] (LENS defaults to 8080, FORGE to 8081). The
//! listener is bound before ORACLE opens its database, so a port in use
//! fails before anything touches the disk.
//!
//! Every other setting is an `ORACLE_*` variable; see `oracle::config`.
//! Config and argument errors are reported on stderr before tracing is set
//! up, since the log level is itself part of the config.

use std::ffi::OsString;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::time::Duration;

use forge::lens_client::LensClient;
use oracle::config::ENV_LENS_URL;
use oracle::{Oracle, OracleConfig, OracleError};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::Level;

/// Where LENS listens when started with its defaults.
pub const DEFAULT_LENS_URL: &str = "http://127.0.0.1:8080";
/// Where ORACLE listens unless told otherwise.
pub const DEFAULT_BIND_ADDRESS: &str = "127.0.0.1:8082";
/// Read by the binary only; the library never binds.
pub const ENV_BIND_ADDRESS: &str = "ORACLE_BIND_ADDRESS";
/// The startup probe of LENS's `/health` gives up after this long.
const LENS_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

const LENS_URL_FLAG: &str = "--lens-url";
const BIND_FLAG: &str = "--bind";

const USAGE: &str = "\
usage: oracle [--lens-url <URL>] [--bind <ADDR>]

  --lens-url <URL>  base URL of LENS (overrides ORACLE_LENS_URL;
                    default http://127.0.0.1:8080)
  --bind <ADDR>     ip:port the API listens on (overrides ORACLE_BIND_ADDRESS;
                    default 127.0.0.1:8082)
  -h, --help        print this help

Every other setting is read from ORACLE_* environment variables.";

/// What the command line asks for.
#[derive(Debug, Default, PartialEq, Eq)]
struct Args {
    lens_url: Option<OsString>,
    bind: Option<OsString>,
}

#[derive(Debug, PartialEq, Eq)]
enum Cli {
    Help,
    Run(Args),
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = match parse_args(std::env::args_os().skip(1)) {
        Ok(Cli::Run(args)) => args,
        Ok(Cli::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Err(message) => {
            eprintln!("oracle: {message}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let env = |name: &str| std::env::var_os(name);
    let config = match OracleConfig::from_lookup(|name| lookup(&args, name, env)) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("oracle: {err}");
            return ExitCode::FAILURE;
        }
    };
    let bind = match bind_address(&args, env) {
        Ok(bind) => bind,
        Err(message) => {
            eprintln!("oracle: {message}");
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

    match run(config, bind).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "ORACLE failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: OracleConfig, bind: SocketAddr) -> oracle::OracleResult<()> {
    // Before the store is opened: a port in use leaves nothing on disk.
    let listener = TcpListener::bind(bind)
        .await
        .map_err(|err| OracleError::io(format!("bind address {bind}"), err))?;
    let local_addr = listener.local_addr().map_err(|err| OracleError::io("listener address", err))?;

    probe_lens(config.lens_url()).await;

    let lens_url = config.lens_url().to_owned();
    let token = CancellationToken::new();
    let oracle = Oracle::start(config, token.clone()).await?;
    tracing::info!(address = %local_addr, lens_url, "ORACLE listening");

    // Stop serving on a signal, and also if ORACLE stops on its own: an API
    // whose detectors and digester have died would look healthy.
    let stop = {
        let token = token.clone();
        let stopped = oracle.state().shutdown.clone();
        async move {
            tokio::select! {
                () = shutdown_signal() => tracing::info!("shutdown signal received; draining requests"),
                () = stopped.cancelled() => tracing::warn!("ORACLE stopped on its own; draining requests"),
            }
            token.cancel();
        }
    };
    // The router owns an `Arc<AppState>`, and with it the store lock; it is
    // dropped when `serve` returns, before `wait`.
    let served = axum::serve(listener, oracle.router())
        .with_graceful_shutdown(stop)
        .await
        .map_err(|err| OracleError::io(format!("serving on {local_addr}"), err));

    token.cancel();
    let stopped = oracle.wait().await;
    // A task failure is the root cause of a serve error that follows it.
    stopped.and(served)
}

/// Ask LENS's `/health` once, and log what it says. Never fails: ORACLE
/// starts whether LENS is up or not.
async fn probe_lens(lens_url: &str) {
    let client = match LensClient::from_parts(lens_url, LENS_PROBE_TIMEOUT) {
        Ok(client) => client,
        Err(err) => {
            tracing::warn!(lens_url, error = %err, "cannot build a client to probe LENS");
            return;
        }
    };
    match client.health().await {
        Ok(health) => tracing::info!(
            lens_url,
            status = health.status.as_str(),
            round_trip_ms = health.round_trip_ms,
            "LENS reachable"
        ),
        Err(err) => tracing::warn!(
            lens_url,
            error = %err,
            "LENS is not reachable yet; ORACLE starts anyway and reports degraded until it is"
        ),
    }
}

/// The config lookup: the process environment, with `--lens-url` over
/// `ORACLE_LENS_URL`, and [`DEFAULT_LENS_URL`] when neither is given.
fn lookup(args: &Args, name: &str, env: impl Fn(&str) -> Option<OsString>) -> Option<OsString> {
    if name == ENV_LENS_URL {
        args.lens_url
            .clone()
            .or_else(|| env(name))
            .or_else(|| Some(DEFAULT_LENS_URL.into()))
    } else {
        env(name)
    }
}

/// `--bind`, else `ORACLE_BIND_ADDRESS`, else [`DEFAULT_BIND_ADDRESS`]: an
/// `ip:port` with a non-zero port. Hostnames are not resolved.
fn bind_address(args: &Args, env: impl Fn(&str) -> Option<OsString>) -> Result<SocketAddr, String> {
    let (source, raw) = match (&args.bind, env(ENV_BIND_ADDRESS)) {
        (Some(flag), _) => (BIND_FLAG, flag.clone()),
        (None, Some(var)) => (ENV_BIND_ADDRESS, var),
        (None, None) => return Ok(DEFAULT_BIND_ADDRESS.parse().expect("the default bind address is valid")),
    };
    let raw = raw.to_str().ok_or_else(|| format!("{source}: {raw:?} is not valid UTF-8"))?.trim();
    if raw.is_empty() {
        return Err(format!("{source} is set but empty"));
    }
    let addr: SocketAddr = raw
        .parse()
        .map_err(|err| format!("{source}: {raw:?} is not a valid socket address (expected ip:port): {err}"))?;
    if addr.port() == 0 {
        return Err(format!("{source}: {raw:?} has port 0; a fixed port is required"));
    }
    Ok(addr)
}

/// Parse the arguments after the program name. Each flag takes its value
/// as the next argument or after `=`.
fn parse_args(args: impl IntoIterator<Item = OsString>) -> Result<Cli, String> {
    let mut parsed = Args::default();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "-h" || arg == "--help" {
            return Ok(Cli::Help);
        }
        let Some((flag, inline)) = [LENS_URL_FLAG, BIND_FLAG].into_iter().find_map(|flag| {
            if arg == flag {
                Some((flag, None))
            } else {
                let value = arg.to_str()?.strip_prefix(flag)?.strip_prefix('=')?;
                Some((flag, Some(OsString::from(value))))
            }
        }) else {
            return Err(format!("unexpected argument {:?}", arg.to_string_lossy()));
        };
        let value = match inline {
            Some(value) => value,
            None => args.next().ok_or_else(|| format!("{flag} needs a value"))?,
        };
        if value.is_empty() {
            return Err(format!("{flag} needs a value"));
        }
        let slot = if flag == LENS_URL_FLAG { &mut parsed.lens_url } else { &mut parsed.bind };
        if slot.replace(value).is_some() {
            return Err(format!("{flag} given more than once"));
        }
    }
    Ok(Cli::Run(parsed))
}

/// Resolves on Ctrl-C, or SIGTERM on Unix.
async fn shutdown_signal() {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Cli, String> {
        parse_args(args.iter().map(OsString::from))
    }

    fn run(lens_url: Option<&str>, bind: Option<&str>) -> Result<Cli, String> {
        Ok(Cli::Run(Args { lens_url: lens_url.map(OsString::from), bind: bind.map(OsString::from) }))
    }

    fn env_of(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<OsString> {
        move |name| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| OsString::from(v))
    }

    fn args(lens_url: Option<&str>, bind: Option<&str>) -> Args {
        Args { lens_url: lens_url.map(OsString::from), bind: bind.map(OsString::from) }
    }

    #[test]
    fn no_arguments_means_defaults() {
        assert_eq!(parse(&[]), run(None, None));
    }

    #[test]
    fn flags_accept_both_spellings() {
        assert_eq!(parse(&["--lens-url", "http://h:1"]), run(Some("http://h:1"), None));
        assert_eq!(parse(&["--lens-url=http://h:1"]), run(Some("http://h:1"), None));
        assert_eq!(parse(&["--bind", "0.0.0.0:9000"]), run(None, Some("0.0.0.0:9000")));
        assert_eq!(
            parse(&["--bind=0.0.0.0:9000", "--lens-url", "http://h:1"]),
            run(Some("http://h:1"), Some("0.0.0.0:9000"))
        );
    }

    #[test]
    fn help_is_recognised() {
        assert_eq!(parse(&["--help"]), Ok(Cli::Help));
        assert_eq!(parse(&["-h"]), Ok(Cli::Help));
    }

    #[test]
    fn bad_arguments_are_rejected() {
        for args in [
            &["--lens-url"][..],
            &["--lens-url="],
            &["--lens-url", ""],
            &["--lens-url", "a", "--lens-url", "b"],
            &["--bind"],
            &["--bind", "a", "--bind=b"],
            &["--lens-urlx"],
            &["http://h:1"],
        ] {
            assert!(parse(args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn lens_url_flag_wins_over_env_which_wins_over_default() {
        let env = env_of(&[(ENV_LENS_URL, "http://env:1")]);
        let flagged = args(Some("http://flag:1"), None);
        assert_eq!(lookup(&flagged, ENV_LENS_URL, &env), Some("http://flag:1".into()));
        assert_eq!(lookup(&Args::default(), ENV_LENS_URL, &env), Some("http://env:1".into()));
        assert_eq!(lookup(&Args::default(), ENV_LENS_URL, env_of(&[])), Some(DEFAULT_LENS_URL.into()));
    }

    #[test]
    fn other_variables_come_from_the_environment() {
        let env = env_of(&[("ORACLE_MAX_ROWS", "5")]);
        let flagged = args(Some("http://flag:1"), None);
        assert_eq!(lookup(&flagged, "ORACLE_MAX_ROWS", &env), Some("5".into()));
        assert_eq!(lookup(&flagged, "ORACLE_BUCKET_SECS", &env), None);
    }

    #[test]
    fn an_empty_lens_url_variable_is_still_rejected() {
        // Set but empty is a broken deployment, not a request for the default.
        let env = env_of(&[(ENV_LENS_URL, "")]);
        let err = OracleConfig::from_lookup(|name| lookup(&Args::default(), name, &env)).unwrap_err();
        assert_eq!(err.config_field(), Some("lens_url"));
    }

    #[test]
    fn the_default_lens_url_is_a_valid_config() {
        let config = OracleConfig::from_lookup(|name| lookup(&Args::default(), name, env_of(&[]))).unwrap();
        assert_eq!(config.lens_url(), DEFAULT_LENS_URL);
    }

    #[test]
    fn bind_address_resolution() {
        let none = env_of(&[]);
        assert_eq!(bind_address(&Args::default(), &none).unwrap(), DEFAULT_BIND_ADDRESS.parse().unwrap());
        let env = env_of(&[(ENV_BIND_ADDRESS, "127.0.0.1:9001")]);
        assert_eq!(bind_address(&Args::default(), &env).unwrap(), "127.0.0.1:9001".parse().unwrap());
        assert_eq!(
            bind_address(&args(None, Some("[::1]:9002")), &env).unwrap(),
            "[::1]:9002".parse().unwrap()
        );
    }

    #[test]
    fn bad_bind_addresses_are_rejected() {
        for bad in ["", "  ", "localhost:8082", "127.0.0.1", "127.0.0.1:0"] {
            assert!(bind_address(&args(None, Some(bad)), env_of(&[])).is_err(), "{bad:?}");
        }
        assert!(bind_address(&Args::default(), env_of(&[(ENV_BIND_ADDRESS, "")])).is_err());
    }
}
