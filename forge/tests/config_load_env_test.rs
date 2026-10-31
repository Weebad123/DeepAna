//! `ForgeConfig::load` reads the real process environment.
//!
//! Everything else is tested through `from_lookup`; this only checks that
//! `load` is wired to `std::env`.
//!
//! This file must contain exactly one `#[test]`: it mutates the process
//! environment, which is only sound while no other thread reads it.

mod common;

use common::Fixture;
use forge::ForgeConfig;
use forge::config::*;

#[test]
fn load_reads_forge_variables_from_the_process_environment() {
    let fixture = Fixture::new();

    // SAFETY: the only test in this binary, so no other thread touches the
    // environment while it runs.
    unsafe {
        for var in ENV_VARS {
            std::env::remove_var(var);
        }
    }
    let err = ForgeConfig::load().unwrap_err();
    assert_eq!(err.config_field(), Some("changelog_path"));

    // SAFETY: as above.
    unsafe {
        std::env::set_var(ENV_CHANGELOG_PATH, &fixture.changelog);
        std::env::set_var(ENV_DB_PATH, &fixture.db);
        std::env::set_var(ENV_POLL_INTERVAL_MS, "777");
        std::env::set_var(ENV_LOG_LEVEL, "ERROR");
    }
    let config = ForgeConfig::load().unwrap();
    assert_eq!(config.poll_interval_ms(), 777);
    assert_eq!(config.log_level(), "error");
    assert_eq!(config.bind_address(), DEFAULT_BIND_ADDRESS);

    // SAFETY: as above.
    unsafe { std::env::set_var(ENV_WEBHOOK_MAX_ATTEMPTS, "21") };
    assert_eq!(ForgeConfig::load().unwrap_err().config_field(), Some("webhook_max_attempts"));
}
