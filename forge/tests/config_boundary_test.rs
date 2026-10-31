//! Boundary tests for every `ForgeConfig` validation rule.
//!
//! Each numeric range is probed at both edges from both sides: one below the
//! minimum, exactly the minimum, one above it, one below the maximum, exactly
//! the maximum and one above it, plus the edges of the integer type itself.
//! Rejections must name the field under test, so a case cannot pass by
//! tripping some other rule. Cross-field rules, ports, lengths of blank input
//! and the enumerated `log_level` get the same treatment.

mod common;

use common::{Fixture, rejected_field, rejection_reason};
use forge::config::*;

// ---------------------------------------------------------------------------
// Numeric ranges
// ---------------------------------------------------------------------------

/// Generates one module of boundary tests for a numeric field.
///
/// `extra` holds variables needed so that only the field under test can fail
/// (e.g. a low webhook timeout, so the cross-field rule does not reject the
/// bottom of the visibility range).
macro_rules! range_boundaries {
    (
        $module:ident,
        var: $var:expr,
        field: $field:literal,
        ty: $ty:ty,
        min: $min:expr,
        max: $max:expr,
        getter: $getter:ident,
        extra: [$(($extra_var:expr, $extra_value:expr)),* $(,)?] $(,)?
    ) => {
        mod $module {
            use super::*;

            const MIN: $ty = $min;
            const MAX: $ty = $max;

            fn load_with(raw: &str) -> Result<forge::ForgeConfig, forge::ForgeError> {
                let fixture = Fixture::new();
                fixture.load(&[$(($extra_var, $extra_value),)* ($var, raw)])
            }

            fn accepts(value: $ty) {
                let config = load_with(&value.to_string())
                    .unwrap_or_else(|err| panic!("{} rejected {value}: {err}", $var));
                assert_eq!(config.$getter(), value);
            }

            fn rejects(raw: &str) {
                assert_eq!(rejected_field(load_with(raw)), $field, "{}={raw:?}", $var);
            }

            #[test]
            fn bounds_are_ordered() {
                assert!(MIN > 0, "zero must stay invalid");
                assert!(MIN < MAX);
            }

            #[test]
            fn rejects_zero() {
                rejects("0");
            }

            #[test]
            fn rejects_one_below_min() {
                rejects(&(MIN - 1).to_string());
            }

            #[test]
            fn accepts_exactly_min() {
                accepts(MIN);
            }

            #[test]
            fn accepts_one_above_min() {
                accepts(MIN + 1);
            }

            #[test]
            fn accepts_one_below_max() {
                accepts(MAX - 1);
            }

            #[test]
            fn accepts_exactly_max() {
                accepts(MAX);
            }

            #[test]
            fn rejects_one_above_max() {
                rejects(&(MAX + 1).to_string());
            }

            #[test]
            fn rejects_type_max() {
                rejects(&<$ty>::MAX.to_string());
            }

            #[test]
            fn rejects_one_past_type_max() {
                rejects(&(u128::from(<$ty>::MAX) + 1).to_string());
            }

            #[test]
            fn rejects_negative_one() {
                rejects("-1");
            }

            #[test]
            fn rejects_fractional_values_at_the_bounds() {
                rejects(&format!("{MIN}.0"));
                rejects(&format!("{MAX}.0"));
                rejects(&format!("{}.5", MIN - 1));
                rejects(&format!("{MAX}.5"));
            }

            #[test]
            fn rejects_blank_and_non_numeric_values() {
                for raw in ["", " ", "\t", "abc", "1_000", "1e2", "0x10", "+-1"] {
                    rejects(raw);
                }
            }

            #[test]
            fn surrounding_whitespace_is_trimmed_at_the_bounds() {
                assert_eq!(load_with(&format!("  {MIN}\t")).unwrap().$getter(), MIN);
                assert_eq!(load_with(&format!("\n{MAX} ")).unwrap().$getter(), MAX);
            }

            #[test]
            fn leading_zeros_do_not_change_the_value() {
                assert_eq!(load_with(&format!("000{MIN}")).unwrap().$getter(), MIN);
                rejects(&format!("000{}", MAX + 1));
            }

            #[test]
            fn explicit_plus_sign_is_accepted_at_the_bounds() {
                assert_eq!(load_with(&format!("+{MIN}")).unwrap().$getter(), MIN);
                rejects(&format!("+{}", MAX + 1));
            }
        }
    };
}

range_boundaries!(
    poll_interval_ms,
    var: ENV_POLL_INTERVAL_MS,
    field: "poll_interval_ms",
    ty: u64,
    min: MIN_POLL_INTERVAL_MS,
    max: MAX_POLL_INTERVAL_MS,
    getter: poll_interval_ms,
    extra: [],
);

range_boundaries!(
    visibility_timeout_secs,
    var: ENV_VISIBILITY_TIMEOUT_SECS,
    field: "visibility_timeout_secs",
    ty: u64,
    min: MIN_VISIBILITY_TIMEOUT_SECS,
    max: MAX_VISIBILITY_TIMEOUT_SECS,
    getter: visibility_timeout_secs,
    // Low enough that the bottom of the range passes the cross-field rule.
    extra: [(ENV_WEBHOOK_TIMEOUT_SECS, "1")],
);

range_boundaries!(
    webhook_max_attempts,
    var: ENV_WEBHOOK_MAX_ATTEMPTS,
    field: "webhook_max_attempts",
    ty: u32,
    min: 1,
    max: MAX_WEBHOOK_MAX_ATTEMPTS,
    getter: webhook_max_attempts,
    extra: [],
);

range_boundaries!(
    webhook_timeout_secs,
    var: ENV_WEBHOOK_TIMEOUT_SECS,
    field: "webhook_timeout_secs",
    ty: u64,
    min: 1,
    max: MAX_WEBHOOK_TIMEOUT_SECS,
    getter: webhook_timeout_secs,
    // High enough that the top of the range passes the cross-field rule.
    extra: [(ENV_VISIBILITY_TIMEOUT_SECS, "3600")],
);

#[test]
fn the_documented_bounds_have_not_drifted() {
    assert_eq!((MIN_POLL_INTERVAL_MS, MAX_POLL_INTERVAL_MS), (50, 60_000));
    assert_eq!((MIN_VISIBILITY_TIMEOUT_SECS, MAX_VISIBILITY_TIMEOUT_SECS), (5, 3_600));
    assert_eq!(MAX_WEBHOOK_MAX_ATTEMPTS, 20);
    assert_eq!(MAX_WEBHOOK_TIMEOUT_SECS, 60);
}

// ---------------------------------------------------------------------------
// visibility_timeout_secs > webhook_timeout_secs
// ---------------------------------------------------------------------------

fn load_timeouts(visibility: u64, webhook: u64) -> Result<forge::ForgeConfig, forge::ForgeError> {
    Fixture::new().load(&[
        (ENV_VISIBILITY_TIMEOUT_SECS, &visibility.to_string()),
        (ENV_WEBHOOK_TIMEOUT_SECS, &webhook.to_string()),
    ])
}

#[test]
fn visibility_equal_to_webhook_timeout_is_rejected() {
    for value in [MIN_VISIBILITY_TIMEOUT_SECS, 30, MAX_WEBHOOK_TIMEOUT_SECS] {
        let reason = rejection_reason(load_timeouts(value, value), "visibility_timeout_secs");
        assert!(reason.contains("greater than webhook_timeout_secs"), "{value}: {reason}");
    }
}

#[test]
fn visibility_one_above_webhook_timeout_is_accepted() {
    for webhook in [MIN_VISIBILITY_TIMEOUT_SECS - 1, MIN_VISIBILITY_TIMEOUT_SECS, 30, MAX_WEBHOOK_TIMEOUT_SECS] {
        let config = load_timeouts(webhook + 1, webhook)
            .unwrap_or_else(|err| panic!("visibility {} / webhook {webhook}: {err}", webhook + 1));
        assert_eq!(config.visibility_timeout_secs(), webhook + 1);
        assert_eq!(config.webhook_timeout_secs(), webhook);
    }
}

#[test]
fn visibility_one_below_webhook_timeout_is_rejected() {
    for webhook in [MIN_VISIBILITY_TIMEOUT_SECS + 1, 30, MAX_WEBHOOK_TIMEOUT_SECS] {
        assert_eq!(rejected_field(load_timeouts(webhook - 1, webhook)), "visibility_timeout_secs", "{webhook}");
    }
}

#[test]
fn minimum_visibility_needs_a_webhook_timeout_below_it() {
    let min = MIN_VISIBILITY_TIMEOUT_SECS;
    assert!(load_timeouts(min, min - 1).is_ok());
    assert!(load_timeouts(min, 1).is_ok());
    assert_eq!(rejected_field(load_timeouts(min, min)), "visibility_timeout_secs");
    assert_eq!(rejected_field(load_timeouts(min, MAX_WEBHOOK_TIMEOUT_SECS)), "visibility_timeout_secs");
}

#[test]
fn maximum_webhook_timeout_fits_under_maximum_and_default_visibility() {
    assert!(load_timeouts(MAX_VISIBILITY_TIMEOUT_SECS, MAX_WEBHOOK_TIMEOUT_SECS).is_ok());
    // Raising only the webhook timeout to its maximum must not trip the
    // cross-field rule against the default visibility timeout.
    let config = Fixture::new()
        .load(&[(ENV_WEBHOOK_TIMEOUT_SECS, &MAX_WEBHOOK_TIMEOUT_SECS.to_string())])
        .unwrap();
    assert_eq!(config.visibility_timeout_secs(), DEFAULT_VISIBILITY_TIMEOUT_SECS);
}

#[test]
fn lowering_only_visibility_to_the_default_webhook_timeout_is_rejected() {
    let at = DEFAULT_WEBHOOK_TIMEOUT_SECS;
    let fixture = Fixture::new();
    assert_eq!(
        rejected_field(fixture.load(&[(ENV_VISIBILITY_TIMEOUT_SECS, &at.to_string())])),
        "visibility_timeout_secs"
    );
    assert!(fixture.load(&[(ENV_VISIBILITY_TIMEOUT_SECS, &(at + 1).to_string())]).is_ok());
}

#[test]
fn an_out_of_range_webhook_timeout_is_blamed_before_the_cross_field_rule() {
    // 61 > 60 = MAX_WEBHOOK_TIMEOUT_SECS, and also > visibility 5: the range
    // error is the one reported.
    assert_eq!(
        rejected_field(load_timeouts(MIN_VISIBILITY_TIMEOUT_SECS, MAX_WEBHOOK_TIMEOUT_SECS + 1)),
        "webhook_timeout_secs"
    );
}

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

#[test]
fn bind_address_port_boundaries() {
    let fixture = Fixture::new();
    let load = |addr: &str| fixture.load(&[(ENV_BIND_ADDRESS, addr)]);

    assert_eq!(rejected_field(load("127.0.0.1:0")), "bind_address");
    assert_eq!(load("127.0.0.1:1").unwrap().socket_addr().port(), 1);
    assert_eq!(load("127.0.0.1:65535").unwrap().socket_addr().port(), 65_535);
    assert_eq!(rejected_field(load("127.0.0.1:65536")), "bind_address");

    assert_eq!(rejected_field(load("[::1]:0")), "bind_address");
    assert_eq!(load("[::1]:1").unwrap().socket_addr().port(), 1);
    assert_eq!(load("[::1]:65535").unwrap().socket_addr().port(), 65_535);
    assert_eq!(rejected_field(load("[::1]:65536")), "bind_address");
}

#[test]
fn bind_address_octet_boundaries() {
    let fixture = Fixture::new();
    let load = |addr: &str| fixture.load(&[(ENV_BIND_ADDRESS, addr)]);

    assert!(load("0.0.0.0:8081").is_ok());
    assert!(load("255.255.255.255:8081").is_ok());
    assert_eq!(rejected_field(load("256.0.0.1:8081")), "bind_address");
    assert_eq!(rejected_field(load("127.0.0:8081")), "bind_address");
    assert_eq!(rejected_field(load("127.0.0.1.1:8081")), "bind_address");
}

#[test]
fn lens_url_port_boundaries() {
    let fixture = Fixture::new();
    let load = |url: &str| fixture.load(&[(ENV_LENS_URL, url)]);

    assert_eq!(rejected_field(load("http://lens:0")), "lens_url");
    assert_eq!(load("http://lens:1").unwrap().lens_url(), "http://lens:1");
    assert_eq!(load("http://lens:65535").unwrap().lens_url(), "http://lens:65535");
    assert_eq!(rejected_field(load("http://lens:65536")), "lens_url");
}

// ---------------------------------------------------------------------------
// Blank values
// ---------------------------------------------------------------------------

#[test]
fn every_variable_rejects_empty_and_whitespace_only_values() {
    let fields = [
        (ENV_CHANGELOG_PATH, "changelog_path"),
        (ENV_LENS_URL, "lens_url"),
        (ENV_DB_PATH, "db_path"),
        (ENV_BIND_ADDRESS, "bind_address"),
        (ENV_POLL_INTERVAL_MS, "poll_interval_ms"),
        (ENV_VISIBILITY_TIMEOUT_SECS, "visibility_timeout_secs"),
        (ENV_WEBHOOK_MAX_ATTEMPTS, "webhook_max_attempts"),
        (ENV_WEBHOOK_TIMEOUT_SECS, "webhook_timeout_secs"),
        (ENV_LOG_LEVEL, "log_level"),
    ];
    assert_eq!(fields.len(), ENV_VARS.len());
    let fixture = Fixture::new();
    for (var, field) in fields {
        for blank in ["", " ", "\t", "\n", " \t\r\n "] {
            let reason = rejection_reason(fixture.load(&[(var, blank)]), field);
            assert!(reason.contains("set but empty"), "{var}={blank:?}: {reason}");
        }
    }
}

#[test]
fn a_single_character_is_no_longer_blank() {
    // The shortest non-blank values get past the blank check and are judged
    // on their content instead.
    let fixture = Fixture::new();
    let reason = rejection_reason(fixture.load(&[(ENV_LOG_LEVEL, "x")]), "log_level");
    assert!(!reason.contains("set but empty"), "{reason}");
    let reason = rejection_reason(fixture.load(&[(ENV_POLL_INTERVAL_MS, "x")]), "poll_interval_ms");
    assert!(!reason.contains("set but empty"), "{reason}");
}

// ---------------------------------------------------------------------------
// log_level
// ---------------------------------------------------------------------------

#[test]
fn every_log_level_is_accepted_in_any_case() {
    let fixture = Fixture::new();
    for level in LOG_LEVELS {
        for raw in [level.to_owned(), level.to_ascii_uppercase(), format!(" {level} ")] {
            assert_eq!(fixture.load(&[(ENV_LOG_LEVEL, &raw)]).unwrap().log_level(), level, "{raw:?}");
        }
    }
}

#[test]
fn near_miss_log_levels_are_rejected() {
    let fixture = Fixture::new();
    for raw in ["inf", "infoo", "warning", "err", "tracing", "off", "info,forge=debug", "in fo"] {
        assert_eq!(rejected_field(fixture.load(&[(ENV_LOG_LEVEL, raw)])), "log_level", "{raw:?}");
    }
}
