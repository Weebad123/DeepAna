//! Boundary tests for every `OracleConfig` validation rule.
//!
//! Each numeric range is probed at both edges from both sides: one below the
//! minimum, exactly the minimum, one above it, one below the maximum, exactly
//! the maximum and one above it, plus the edges of the integer type itself.
//! The float threshold is probed one ULP either side of each bound.
//! Rejections must name the field under test, so a case cannot pass by
//! tripping some other rule. The cross-field rule, ports and blank input get
//! the same treatment.

// Some tests pin invariants between constants; asserting on constants is the point.
#![allow(clippy::assertions_on_constants)]

mod common;

use common::{Fixture, rejected_field, rejection_reason};
use oracle::config::*;
use oracle::{OracleConfig, OracleError};

// ---------------------------------------------------------------------------
// Integer ranges
// ---------------------------------------------------------------------------

/// Generates one module of boundary tests for an integer field.
///
/// `extra` holds variables needed so that only the field under test can fail
/// (e.g. a large bucket, so the cross-field rule does not reject the top of
/// the detection interval range).
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

            fn load_with(raw: &str) -> Result<OracleConfig, OracleError> {
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
            fn rejects_negative_values() {
                rejects("-1");
                rejects(&format!("-{MIN}"));
            }

            #[test]
            fn out_of_range_reason_names_both_bounds() {
                let reason = rejection_reason(load_with(&(MAX + 1).to_string()), $field);
                assert!(reason.contains("out of range"), "{reason}");
                assert!(reason.contains(&format!("between {MIN} and {MAX}")), "{reason}");
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
                for raw in ["", " ", "\t", "abc", "1_000", "1e2", "0x10", "+-1", "1 0", "１０"] {
                    rejects(raw);
                }
            }

            #[test]
            fn parse_errors_say_unsigned_integer() {
                let reason = rejection_reason(load_with("12abc"), $field);
                assert!(reason.contains("not a valid unsigned integer"), "{reason}");
                assert!(reason.contains(&format!("{}=\"12abc\"", $var)), "{reason}");
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
    lens_timeout_secs,
    var: ENV_LENS_TIMEOUT_SECS,
    field: "lens_timeout_secs",
    ty: u64,
    min: MIN_LENS_TIMEOUT_SECS,
    max: MAX_LENS_TIMEOUT_SECS,
    getter: lens_timeout_secs,
    extra: [],
);

range_boundaries!(
    max_rows,
    var: ENV_MAX_ROWS,
    field: "max_rows",
    ty: u32,
    min: MIN_MAX_ROWS,
    max: MAX_MAX_ROWS,
    getter: max_rows,
    extra: [],
);

range_boundaries!(
    max_slot_span,
    var: ENV_MAX_SLOT_SPAN,
    field: "max_slot_span",
    ty: u64,
    min: MIN_MAX_SLOT_SPAN,
    max: MAX_MAX_SLOT_SPAN,
    getter: max_slot_span,
    extra: [],
);

range_boundaries!(
    detect_interval_secs,
    var: ENV_DETECT_INTERVAL_SECS,
    field: "detect_interval_secs",
    ty: u64,
    min: MIN_DETECT_INTERVAL_SECS,
    max: MAX_DETECT_INTERVAL_SECS,
    getter: detect_interval_secs,
    // Large enough that the top of the range passes the cross-field rule.
    extra: [(ENV_BUCKET_SECS, "86400")],
);

range_boundaries!(
    bucket_secs,
    var: ENV_BUCKET_SECS,
    field: "bucket_secs",
    ty: u64,
    min: MIN_BUCKET_SECS,
    max: MAX_BUCKET_SECS,
    getter: bucket_secs,
    // Small enough that the bottom of the range passes the cross-field rule.
    extra: [(ENV_DETECT_INTERVAL_SECS, "10")],
);

range_boundaries!(
    baseline_buckets,
    var: ENV_BASELINE_BUCKETS,
    field: "baseline_buckets",
    ty: u32,
    min: MIN_BASELINE_BUCKETS,
    max: MAX_BASELINE_BUCKETS,
    getter: baseline_buckets,
    extra: [],
);

#[test]
fn the_documented_bounds_have_not_drifted() {
    assert_eq!((MIN_LENS_TIMEOUT_SECS, MAX_LENS_TIMEOUT_SECS, DEFAULT_LENS_TIMEOUT_SECS), (1, 330, 35));
    assert_eq!((MIN_MAX_ROWS, MAX_MAX_ROWS, DEFAULT_MAX_ROWS), (1, 10_000, 1_000));
    assert_eq!((MIN_MAX_SLOT_SPAN, MAX_MAX_SLOT_SPAN, DEFAULT_MAX_SLOT_SPAN), (150, 6_480_000, 216_000));
    assert_eq!((MIN_DETECT_INTERVAL_SECS, MAX_DETECT_INTERVAL_SECS, DEFAULT_DETECT_INTERVAL_SECS), (10, 3_600, 60));
    assert_eq!((MIN_BUCKET_SECS, MAX_BUCKET_SECS, DEFAULT_BUCKET_SECS), (60, 86_400, 300));
    assert_eq!((MIN_BASELINE_BUCKETS, MAX_BASELINE_BUCKETS, DEFAULT_BASELINE_BUCKETS), (5, 2_016, 288));
    assert_eq!((MIN_ANOMALY_THRESHOLD, MAX_ANOMALY_THRESHOLD, DEFAULT_ANOMALY_THRESHOLD), (1.0, 20.0, 3.5));
    assert_eq!(DEFAULT_DB_PATH, "oracle.db");
}

#[test]
fn the_lens_derived_bounds_sit_above_lens_limits() {
    // LENS's default and maximum query_timeout_secs are 30 and 300; ORACLE's
    // client timeout must outlast each so LENS's own 504 is what surfaces.
    assert!(DEFAULT_LENS_TIMEOUT_SECS > 30);
    assert!(MAX_LENS_TIMEOUT_SECS > 300);
    // LENS's default max_query_rows is 10000.
    assert_eq!(MAX_MAX_ROWS, 10_000);
}

// ---------------------------------------------------------------------------
// anomaly_threshold (f64)
// ---------------------------------------------------------------------------

mod anomaly_threshold {
    use super::*;

    fn load_with(raw: &str) -> Result<OracleConfig, OracleError> {
        Fixture::new().load(&[(ENV_ANOMALY_THRESHOLD, raw)])
    }

    fn accepts(raw: &str, expected: f64) {
        let config = load_with(raw).unwrap_or_else(|err| panic!("rejected {raw:?}: {err}"));
        assert_eq!(config.anomaly_threshold(), expected, "{raw:?}");
    }

    fn rejects(raw: &str) -> String {
        rejection_reason(load_with(raw), "anomaly_threshold")
    }

    #[test]
    fn bounds_are_ordered_and_positive() {
        assert!(MIN_ANOMALY_THRESHOLD > 0.0);
        assert!(MIN_ANOMALY_THRESHOLD < MAX_ANOMALY_THRESHOLD);
        assert!(MIN_ANOMALY_THRESHOLD.is_finite() && MAX_ANOMALY_THRESHOLD.is_finite());
    }

    #[test]
    fn accepts_exactly_the_bounds_in_any_spelling() {
        for raw in ["1", "1.0", "1.000", "+1", "01.0", "1e0", "0.1e1", "10e-1"] {
            accepts(raw, MIN_ANOMALY_THRESHOLD);
        }
        for raw in ["20", "20.0", "+20", "020.00", "2e1", "2E1", "200e-1"] {
            accepts(raw, MAX_ANOMALY_THRESHOLD);
        }
    }

    #[test]
    fn rejects_one_ulp_outside_each_bound() {
        let below_min = MIN_ANOMALY_THRESHOLD.next_down();
        let above_max = MAX_ANOMALY_THRESHOLD.next_up();
        // f64's Display round-trips, so these strings parse back exactly.
        assert!(rejects(&below_min.to_string()).contains("out of range"));
        assert!(rejects(&above_max.to_string()).contains("out of range"));
    }

    #[test]
    fn accepts_one_ulp_inside_each_bound() {
        let above_min = MIN_ANOMALY_THRESHOLD.next_up();
        let below_max = MAX_ANOMALY_THRESHOLD.next_down();
        accepts(&above_min.to_string(), above_min);
        accepts(&below_max.to_string(), below_max);
    }

    #[test]
    fn rejects_values_just_outside_written_as_decimals() {
        for raw in ["0.9999999", "0.99", "20.0000001", "20.01", "21", "2.1e1"] {
            assert!(rejects(raw).contains("out of range"), "{raw}");
        }
    }

    #[test]
    fn rejects_zero_and_negative_values() {
        for raw in ["0", "0.0", "-0", "-0.0", "-1", "-3.5", "-20"] {
            assert!(rejects(raw).contains("out of range"), "{raw}");
        }
    }

    #[test]
    fn rejects_non_finite_values_even_though_they_parse() {
        // NaN compares false against both bounds, so without the explicit
        // check it would pass the range test.
        for raw in ["NaN", "nan", "-NaN", "inf", "-inf", "+inf", "infinity", "Infinity", "1e309", "-1e309"] {
            let reason = rejects(raw);
            assert!(reason.contains("not a finite number"), "{raw}: {reason}");
        }
    }

    #[test]
    fn rejects_malformed_values() {
        for raw in ["abc", "3,5", "3.5.0", "1_0", "0x10", "3.5x", "--3", "+-3", "3 .5", "e1", "."] {
            let reason = rejects(raw);
            assert!(reason.contains("not a valid number"), "{raw}: {reason}");
        }
    }

    #[test]
    fn tiny_subnormals_are_finite_but_out_of_range() {
        assert!(rejects("5e-324").contains("out of range"));
    }

    #[test]
    fn surrounding_whitespace_is_trimmed_at_the_bounds() {
        accepts(" 1.0\t", MIN_ANOMALY_THRESHOLD);
        accepts("\n20 ", MAX_ANOMALY_THRESHOLD);
    }

    #[test]
    fn the_default_is_used_only_when_unset() {
        assert_eq!(Fixture::new().load(&[]).unwrap().anomaly_threshold(), DEFAULT_ANOMALY_THRESHOLD);
        assert!(rejects(" ").contains("set but empty"));
    }
}

// ---------------------------------------------------------------------------
// detect_interval_secs <= bucket_secs
// ---------------------------------------------------------------------------

fn load_detection(interval: u64, bucket: u64) -> Result<OracleConfig, OracleError> {
    Fixture::new().load(&[
        (ENV_DETECT_INTERVAL_SECS, &interval.to_string()),
        (ENV_BUCKET_SECS, &bucket.to_string()),
    ])
}

#[test]
fn interval_equal_to_bucket_is_accepted() {
    for value in [MIN_BUCKET_SECS, DEFAULT_BUCKET_SECS, MAX_DETECT_INTERVAL_SECS] {
        let config = load_detection(value, value).unwrap_or_else(|err| panic!("{value}: {err}"));
        assert_eq!((config.detect_interval_secs(), config.bucket_secs()), (value, value));
    }
}

#[test]
fn interval_one_above_bucket_is_rejected() {
    for bucket in [MIN_BUCKET_SECS, DEFAULT_BUCKET_SECS, MAX_DETECT_INTERVAL_SECS - 1] {
        let reason = rejection_reason(load_detection(bucket + 1, bucket), "detect_interval_secs");
        assert!(reason.contains("must not exceed bucket_secs"), "{bucket}: {reason}");
    }
}

#[test]
fn interval_one_below_bucket_is_accepted() {
    for bucket in [MIN_BUCKET_SECS, DEFAULT_BUCKET_SECS, MAX_DETECT_INTERVAL_SECS] {
        assert!(load_detection(bucket - 1, bucket).is_ok(), "{bucket}");
    }
}

#[test]
fn extreme_combinations() {
    // Smallest of both, and the largest interval under the largest bucket.
    assert!(load_detection(MIN_DETECT_INTERVAL_SECS, MIN_BUCKET_SECS).is_ok());
    assert!(load_detection(MAX_DETECT_INTERVAL_SECS, MAX_BUCKET_SECS).is_ok());
    assert!(load_detection(MIN_DETECT_INTERVAL_SECS, MAX_BUCKET_SECS).is_ok());
    // The largest interval over the smallest bucket.
    assert_eq!(
        rejected_field(load_detection(MAX_DETECT_INTERVAL_SECS, MIN_BUCKET_SECS)),
        "detect_interval_secs"
    );
}

#[test]
fn raising_only_the_interval_past_the_default_bucket_is_rejected() {
    let fixture = Fixture::new();
    let at = DEFAULT_BUCKET_SECS;
    assert!(fixture.load(&[(ENV_DETECT_INTERVAL_SECS, &at.to_string())]).is_ok());
    assert_eq!(
        rejected_field(fixture.load(&[(ENV_DETECT_INTERVAL_SECS, &(at + 1).to_string())])),
        "detect_interval_secs"
    );
}

#[test]
fn lowering_only_the_bucket_to_its_minimum_fits_the_default_interval() {
    let config = Fixture::new().load(&[(ENV_BUCKET_SECS, &MIN_BUCKET_SECS.to_string())]).unwrap();
    assert_eq!(config.detect_interval_secs(), DEFAULT_DETECT_INTERVAL_SECS);
    assert_eq!(config.bucket_secs(), MIN_BUCKET_SECS);
}

#[test]
fn an_out_of_range_interval_is_blamed_by_its_range_not_the_cross_field_rule() {
    let reason = rejection_reason(
        load_detection(MAX_DETECT_INTERVAL_SECS + 1, MIN_BUCKET_SECS),
        "detect_interval_secs",
    );
    assert!(reason.contains("out of range"), "{reason}");
}

#[test]
fn an_out_of_range_bucket_is_blamed_before_the_cross_field_rule() {
    // 59 is below the bucket range and below the default interval (60):
    // the bucket's range error is the one reported.
    assert_eq!(rejected_field(load_detection(DEFAULT_DETECT_INTERVAL_SECS, MIN_BUCKET_SECS - 1)), "bucket_secs");
}

#[test]
fn the_baseline_span_is_exact_at_the_extremes() {
    let fixture = Fixture::new();
    let longest = fixture
        .load(&[
            (ENV_DETECT_INTERVAL_SECS, "10"),
            (ENV_BUCKET_SECS, &MAX_BUCKET_SECS.to_string()),
            (ENV_BASELINE_BUCKETS, &MAX_BASELINE_BUCKETS.to_string()),
        ])
        .unwrap();
    assert_eq!(longest.baseline_span().as_secs(), MAX_BUCKET_SECS * u64::from(MAX_BASELINE_BUCKETS));

    let shortest = fixture
        .load(&[
            (ENV_DETECT_INTERVAL_SECS, "10"),
            (ENV_BUCKET_SECS, &MIN_BUCKET_SECS.to_string()),
            (ENV_BASELINE_BUCKETS, &MIN_BASELINE_BUCKETS.to_string()),
        ])
        .unwrap();
    assert_eq!(shortest.baseline_span().as_secs(), MIN_BUCKET_SECS * u64::from(MIN_BASELINE_BUCKETS));
}

// ---------------------------------------------------------------------------
// lens_url ports
// ---------------------------------------------------------------------------

#[test]
fn lens_url_port_boundaries() {
    let fixture = Fixture::new();
    let load = |url: &str| fixture.load(&[(ENV_LENS_URL, url)]);

    assert_eq!(rejected_field(load("http://lens:0")), "lens_url");
    assert_eq!(load("http://lens:1").unwrap().lens_url(), "http://lens:1");
    assert_eq!(load("http://lens:65535").unwrap().lens_url(), "http://lens:65535");
    assert_eq!(rejected_field(load("http://lens:65536")), "lens_url");

    assert_eq!(rejected_field(load("http://[::1]:0")), "lens_url");
    assert_eq!(load("http://[::1]:65535").unwrap().lens_url(), "http://[::1]:65535");
    assert_eq!(rejected_field(load("http://[::1]:65536")), "lens_url");
}

#[test]
fn lens_url_ipv4_octet_boundaries() {
    let fixture = Fixture::new();
    let load = |url: &str| fixture.load(&[(ENV_LENS_URL, url)]);

    assert_eq!(load("http://0.0.0.0:8080").unwrap().lens_url(), "http://0.0.0.0:8080");
    assert_eq!(load("http://255.255.255.255:8080").unwrap().lens_url(), "http://255.255.255.255:8080");
    assert_eq!(rejected_field(load("http://256.0.0.1:8080")), "lens_url");
}

// ---------------------------------------------------------------------------
// Blank values
// ---------------------------------------------------------------------------

#[test]
fn every_variable_rejects_empty_and_whitespace_only_values() {
    let fields = [
        (ENV_LENS_URL, "lens_url"),
        (ENV_DB_PATH, "db_path"),
        (ENV_LENS_TIMEOUT_SECS, "lens_timeout_secs"),
        (ENV_MAX_ROWS, "max_rows"),
        (ENV_MAX_SLOT_SPAN, "max_slot_span"),
        (ENV_DETECT_INTERVAL_SECS, "detect_interval_secs"),
        (ENV_BUCKET_SECS, "bucket_secs"),
        (ENV_BASELINE_BUCKETS, "baseline_buckets"),
        (ENV_ANOMALY_THRESHOLD, "anomaly_threshold"),
    ];
    assert_eq!(fields.len(), ENV_VARS.len());
    for (var, _) in fields {
        assert!(ENV_VARS.contains(&var), "{var} missing from ENV_VARS");
    }
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
    for (var, field) in [
        (ENV_LENS_URL, "lens_url"),
        (ENV_MAX_ROWS, "max_rows"),
        (ENV_ANOMALY_THRESHOLD, "anomaly_threshold"),
    ] {
        let reason = rejection_reason(fixture.load(&[(var, "x")]), field);
        assert!(!reason.contains("set but empty"), "{var}: {reason}");
    }
    // A one-character database name is a valid file name.
    assert!(fixture.load(&[(ENV_DB_PATH, &fixture.dir.file("x"))]).is_ok());
}
