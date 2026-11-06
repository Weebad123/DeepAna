//! `OracleConfig::load` reads the real process environment.
//!
//! Everything else is tested through `from_lookup`; this only checks that
//! `load` is wired to `std::env`.
//!
//! This file must contain exactly one `#[test]`: it mutates the process
//! environment, which is only sound while no other thread reads it.

mod common;

use common::Fixture;
use oracle::OracleConfig;
use oracle::config::*;

#[test]
fn load_reads_oracle_variables_from_the_process_environment() {
    let fixture = Fixture::new();

    // SAFETY: the only test in this binary, so no other thread touches the
    // environment while it runs.
    unsafe {
        for var in ENV_VARS {
            std::env::remove_var(var);
        }
    }
    // Nothing set: the one required variable is missing.
    let err = OracleConfig::load().unwrap_err();
    assert_eq!(err.config_field(), Some("lens_url"));

    // SAFETY: as above.
    unsafe {
        std::env::set_var(ENV_LENS_URL, "http://lens.internal:8080/");
        std::env::set_var(ENV_DB_PATH, &fixture.db);
        std::env::set_var(ENV_MAX_ROWS, "777");
        std::env::set_var(ENV_ANOMALY_THRESHOLD, "4.25");
    }
    let config = OracleConfig::load().unwrap();
    assert_eq!(config.lens_url(), "http://lens.internal:8080");
    assert_eq!(config.max_rows(), 777);
    assert_eq!(config.anomaly_threshold(), 4.25);
    // Unset variables take their defaults.
    assert_eq!(config.bucket_secs(), DEFAULT_BUCKET_SECS);
    assert_eq!(config.detect_interval_secs(), DEFAULT_DETECT_INTERVAL_SECS);

    // SAFETY: as above.
    unsafe { std::env::set_var(ENV_BASELINE_BUCKETS, "2017") };
    assert_eq!(OracleConfig::load().unwrap_err().config_field(), Some("baseline_buckets"));

    // SAFETY: as above. Set but empty is an error, not "use the default".
    unsafe {
        std::env::remove_var(ENV_BASELINE_BUCKETS);
        std::env::set_var(ENV_BUCKET_SECS, "");
    }
    assert_eq!(OracleConfig::load().unwrap_err().config_field(), Some("bucket_secs"));
}
