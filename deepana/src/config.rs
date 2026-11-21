//! DeepAna's own settings, and every service config, built in code from the
//! data directory.
//!
//! # Where DeepAna listens
//!
//! `127.0.0.1:<port>`, with `<port>` from [`ENV_PORT`] (`DEEPANA_PORT`) or
//! [`DEFAULT_PORT`] (8099).
//!
//! # Service configs
//!
//! Each service still validates its own config, through the same
//! `from_lookup` / `load_with` its binary uses. DeepAna supplies a lookup
//! that **forces** every variable naming a location, so neither the
//! environment nor the working directory can move a file out of `<DIR>`:
//!
//! | Service | Forced variables |
//! |---------|------------------|
//! | LENS    | `LENS_DATA_DIR` = `<DIR>`, `LENS_BIND_ADDRESS` = the DeepAna address |
//! | FORGE   | `FORGE_CHANGELOG_PATH` = `<DIR>/changelog.jsonl`, `FORGE_DB_PATH` = `<DIR>/forge.db`, `FORGE_LENS_URL` = `http://<listen>/api/lens`, `FORGE_BIND_ADDRESS` (see below) |
//! | ORACLE  | `ORACLE_DB_PATH` = `<DIR>/oracle.db`, `ORACLE_LENS_URL` = `http://<listen>/api/lens` |
//!
//! Every other `LENS_*`, `FORGE_*` and `ORACLE_*` variable (timeouts,
//! limits, detector tuning) is still read from the environment.
//!
//! None of the three binds a socket here: DeepAna serves their routers. The
//! services' `bind_address` settings are only validated. FORGE refuses a
//! `bind_address` equal to its LENS's address, so it is given the port next
//! to DeepAna's, which it never opens.

use std::ffi::OsString;
use std::net::{Ipv4Addr, SocketAddr};

use forge::ForgeConfig;
use lens::LensServiceConfig;
use oracle::OracleConfig;

use crate::layout::Layout;

/// The variable holding DeepAna's port.
pub const ENV_PORT: &str = "DEEPANA_PORT";
/// The port DeepAna listens on when [`ENV_PORT`] is unset.
pub const DEFAULT_PORT: u16 = 8099;
/// The variable holding DeepAna's log level.
pub const ENV_LOG_LEVEL: &str = "DEEPANA_LOG_LEVEL";
pub const DEFAULT_LOG_LEVEL: &str = "info";
pub const LOG_LEVELS: [&str; 5] = ["trace", "debug", "info", "warn", "error"];
/// Where each service's router is nested.
pub const LENS_PREFIX: &str = "/api/lens";
pub const FORGE_PREFIX: &str = "/api/forge";
pub const ORACLE_PREFIX: &str = "/api/oracle";

/// `127.0.0.1:<port>` from `DEEPANA_PORT`, or port 8099 when unset. A
/// variable that is set but blank, not a number, or 0 is rejected.
pub fn listen_address(lookup: impl Fn(&str) -> Option<OsString>) -> Result<SocketAddr, String> {
    let port = match lookup(ENV_PORT) {
        None => DEFAULT_PORT,
        Some(raw) => {
            let raw = raw.to_str().ok_or_else(|| format!("{ENV_PORT} is not valid UTF-8"))?.trim().to_owned();
            if raw.is_empty() {
                return Err(format!("{ENV_PORT} is set but empty"));
            }
            let port: u16 = raw
                .parse()
                .map_err(|err| format!("{ENV_PORT}={raw:?} is not a valid port (1..=65535): {err}"))?;
            if port == 0 {
                return Err(format!("{ENV_PORT}=0: a fixed port is required"));
            }
            port
        }
    };
    Ok(SocketAddr::from((Ipv4Addr::LOCALHOST, port)))
}

/// `DEEPANA_LOG_LEVEL`, lowercased, or `info`.
pub fn log_level(lookup: impl Fn(&str) -> Option<OsString>) -> Result<String, String> {
    let Some(raw) = lookup(ENV_LOG_LEVEL) else {
        return Ok(DEFAULT_LOG_LEVEL.to_owned());
    };
    let level = raw.to_string_lossy().trim().to_ascii_lowercase();
    if LOG_LEVELS.contains(&level.as_str()) {
        Ok(level)
    } else {
        Err(format!("{ENV_LOG_LEVEL}={level:?} is not one of {}", LOG_LEVELS.join(", ")))
    }
}

/// The base URL FORGE and ORACLE reach LENS at: through DeepAna's own port.
pub fn lens_url(listen: SocketAddr) -> String {
    format!("http://{listen}{LENS_PREFIX}")
}

/// Every service config, built from one data directory.
#[derive(Debug, Clone)]
pub struct ServiceConfigs {
    pub lens: LensServiceConfig,
    pub forge: ForgeConfig,
    pub oracle: OracleConfig,
}

impl ServiceConfigs {
    /// Build and validate all three. `env` is the process environment (or a
    /// test's stand-in); see the [module docs](self) for what is forced.
    pub fn build<F>(layout: &Layout, listen: SocketAddr, env: F) -> Result<Self, String>
    where
        F: Fn(&str) -> Option<OsString>,
    {
        let lens_url = lens_url(listen);

        // LENS: PRISM writes `<DIR>`, LENS serves it.
        let lens = LensServiceConfig::for_parquet_directory(layout.root(), |name| match name {
            lens::config::ENV_BIND_ADDRESS => Some(listen.to_string().into()),
            _ => env(name),
        })
        .map_err(|err| format!("LENS config: {err}"))?;

        // FORGE: never binds here; see the module docs.
        let forge_bind = SocketAddr::new(listen.ip(), if listen.port() == u16::MAX { listen.port() - 1 } else { listen.port() + 1 });
        let forge = ForgeConfig::from_lookup(|name| match name {
            forge::config::ENV_CHANGELOG_PATH => Some(layout.changelog().into_os_string()),
            forge::config::ENV_DB_PATH => Some(layout.forge_db().into_os_string()),
            forge::config::ENV_LENS_URL => Some(lens_url.clone().into()),
            forge::config::ENV_BIND_ADDRESS => Some(forge_bind.to_string().into()),
            _ => env(name),
        })
        .map_err(|err| format!("FORGE config: {err}"))?;

        let oracle = OracleConfig::from_lookup(|name| match name {
            oracle::config::ENV_DB_PATH => Some(layout.oracle_db().into_os_string()),
            oracle::config::ENV_LENS_URL => Some(lens_url.clone().into()),
            _ => env(name),
        })
        .map_err(|err| format!("ORACLE config: {err}"))?;

        Ok(Self { lens, forge, oracle })
    }
}
