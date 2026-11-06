//! `OracleConfig::from_lookup` against an in-memory environment and a scratch
//! directory, plus `OracleError` formatting. Range edges are covered in
//! `config_boundary_test.rs`.

mod common;

use std::fs;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Fixture, LENS_URL, TempDir, load, rejected_field, rejection_reason};
use oracle::config::*;
use oracle::{OracleConfig, OracleError};

// ---------------------------------------------------------------------------
// Defaults and overrides
// ---------------------------------------------------------------------------

#[test]
fn only_lens_url_set_yields_the_defaults() {
    let config = load(&[(ENV_LENS_URL, LENS_URL)]).unwrap();

    assert_eq!(config.lens_url(), LENS_URL);
    // The default database path is relative to the working directory.
    assert_eq!(config.db_path(), std::env::current_dir().unwrap().canonicalize().unwrap().join("oracle.db"));
    assert_eq!(config.lens_timeout_secs(), 35);
    assert_eq!(config.max_rows(), 1_000);
    assert_eq!(config.max_slot_span(), 216_000);
    assert_eq!(config.detect_interval_secs(), 60);
    assert_eq!(config.bucket_secs(), 300);
    assert_eq!(config.baseline_buckets(), 288);
    assert_eq!(config.anomaly_threshold(), 3.5);
}

#[test]
fn the_default_baseline_spans_exactly_one_day() {
    let config = Fixture::new().load(&[]).unwrap();
    assert_eq!(config.baseline_span(), Duration::from_secs(86_400));
}

#[test]
fn every_variable_overrides_its_default() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            (ENV_LENS_URL, "https://lens.internal:9443/v1"),
            (ENV_LENS_TIMEOUT_SECS, "90"),
            (ENV_MAX_ROWS, "250"),
            (ENV_MAX_SLOT_SPAN, "9000"),
            (ENV_DETECT_INTERVAL_SECS, "30"),
            (ENV_BUCKET_SECS, "120"),
            (ENV_BASELINE_BUCKETS, "48"),
            (ENV_ANOMALY_THRESHOLD, "5.25"),
        ])
        .unwrap();

    let canonical_dir = fs::canonicalize(fixture.dir.path()).unwrap();
    assert_eq!(config.db_path(), canonical_dir.join("oracle.db"));
    assert_eq!(config.lens_url(), "https://lens.internal:9443/v1");
    assert_eq!(config.lens_timeout(), Duration::from_secs(90));
    assert_eq!(config.max_rows(), 250);
    assert_eq!(config.max_slot_span(), 9_000);
    assert_eq!(config.detect_interval(), Duration::from_secs(30));
    assert_eq!(config.bucket(), Duration::from_secs(120));
    assert_eq!(config.baseline_buckets(), 48);
    assert_eq!(config.baseline_span(), Duration::from_secs(120 * 48));
    assert_eq!(config.anomaly_threshold(), 5.25);
}

#[test]
fn seconds_and_duration_getters_agree() {
    let config = Fixture::new()
        .load(&[(ENV_LENS_TIMEOUT_SECS, "7"), (ENV_DETECT_INTERVAL_SECS, "11"), (ENV_BUCKET_SECS, "61")])
        .unwrap();
    assert_eq!(config.lens_timeout().as_secs(), config.lens_timeout_secs());
    assert_eq!(config.detect_interval().as_secs(), config.detect_interval_secs());
    assert_eq!(config.bucket().as_secs(), config.bucket_secs());
}

#[test]
fn values_are_trimmed_and_case_normalized() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            (ENV_LENS_URL, "  HTTP://LENS:8080/  "),
            (ENV_MAX_ROWS, "\t100 "),
            (ENV_ANOMALY_THRESHOLD, " 4.5\n"),
        ])
        .unwrap();
    assert_eq!(config.lens_url(), "http://lens:8080");
    assert_eq!(config.max_rows(), 100);
    assert_eq!(config.anomaly_threshold(), 4.5);
}

#[test]
fn unrelated_variables_are_ignored() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            ("ORACLE_UNKNOWN", "x"),
            ("FORGE_LENS_URL", "not a url"),
            ("LENS_MAX_QUERY_ROWS", "nope"),
            // Variable names are matched exactly, not case-insensitively.
            ("oracle_max_rows", "nope"),
            ("ORACLE_MAX_ROWS_", "nope"),
        ])
        .unwrap();
    assert_eq!(config.max_rows(), DEFAULT_MAX_ROWS);
}

#[test]
fn config_is_comparable_and_cloneable() {
    let fixture = Fixture::new();
    let a = fixture.load(&[]).unwrap();
    let b = fixture.load(&[]).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.clone(), a);
    assert_ne!(a, fixture.load(&[(ENV_MAX_ROWS, "999")]).unwrap());
    assert_ne!(a, fixture.load(&[(ENV_ANOMALY_THRESHOLD, "3.6")]).unwrap());
}

#[test]
fn oracle_config_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<OracleConfig>();
}

// ---------------------------------------------------------------------------
// lens_url
// ---------------------------------------------------------------------------

#[test]
fn lens_url_is_required() {
    let reason = rejection_reason(load(&[]), "lens_url");
    assert!(reason.contains(ENV_LENS_URL), "{reason}");
    assert!(reason.contains("must be set"), "{reason}");
}

#[test]
fn lens_url_is_required_even_when_everything_else_is_set() {
    let fixture = Fixture::new();
    let result = load(&[
        (ENV_DB_PATH, &fixture.db),
        (ENV_LENS_TIMEOUT_SECS, "10"),
        (ENV_MAX_ROWS, "10"),
        (ENV_MAX_SLOT_SPAN, "1000"),
        (ENV_DETECT_INTERVAL_SECS, "60"),
        (ENV_BUCKET_SECS, "60"),
        (ENV_BASELINE_BUCKETS, "10"),
        (ENV_ANOMALY_THRESHOLD, "3"),
    ]);
    assert_eq!(rejected_field(result), "lens_url");
}

#[test]
fn lens_url_drops_the_trailing_slash_and_keeps_a_base_path() {
    let fixture = Fixture::new();
    for (raw, expected) in [
        ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
        ("http://lens/prefix/", "http://lens/prefix"),
        ("http://lens/a/b/", "http://lens/a/b"),
        ("https://lens.example.com", "https://lens.example.com"),
        ("https://lens.example.com:443/", "https://lens.example.com"),
        ("http://[::1]:8080", "http://[::1]:8080"),
        ("http://localhost:8080", "http://localhost:8080"),
    ] {
        assert_eq!(fixture.load(&[(ENV_LENS_URL, raw)]).unwrap().lens_url(), expected, "{raw}");
    }
}

#[test]
fn lens_url_rejections_name_the_problem() {
    let fixture = Fixture::new();
    for (raw, expected) in [
        ("lens:8080", "scheme"),
        ("127.0.0.1:8080", "not a valid URL"),
        ("ftp://lens", "scheme"),
        ("ws://lens", "scheme"),
        ("http://lens?x=1", "query string"),
        ("http://lens#frag", "fragment"),
        ("http://lens:0", "port 0"),
        ("http://admin:secret@lens", "credentials"),
        ("http://admin@lens", "credentials"),
        ("ftp://admin:secret@lens", "credentials"),
    ] {
        let reason = rejection_reason(fixture.load(&[(ENV_LENS_URL, raw)]), "lens_url");
        assert!(reason.contains(expected), "{raw}: {reason}");
        assert!(!reason.contains("secret"), "{raw}: {reason}");
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_lens_url_is_rejected() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let result = OracleConfig::from_lookup(|name| {
        (name == ENV_LENS_URL).then(|| OsString::from_vec(b"http://lens\xff".to_vec()))
    });
    let reason = rejection_reason(result, "lens_url");
    assert!(reason.contains("not valid UTF-8"), "{reason}");
}

// ---------------------------------------------------------------------------
// db_path
// ---------------------------------------------------------------------------

#[test]
fn an_existing_regular_file_is_accepted_and_canonicalized() {
    let fixture = Fixture::new();
    fs::write(&fixture.db, b"").unwrap();

    // Reach the file through a `..` detour.
    fs::create_dir(fixture.dir.path().join("sub")).unwrap();
    let detour = fixture.dir.file("sub/../oracle.db");

    let config = fixture.load(&[(ENV_DB_PATH, &detour)]).unwrap();
    assert_eq!(config.db_path(), fs::canonicalize(&fixture.db).unwrap());
    assert!(config.db_path().is_absolute());
}

#[test]
fn a_missing_file_in_an_existing_directory_is_accepted() {
    let fixture = Fixture::new();
    let config = fixture.load(&[]).unwrap();
    assert!(!config.db_path().exists());
    assert_eq!(config.db_path().parent().unwrap(), fs::canonicalize(fixture.dir.path()).unwrap());
    assert_eq!(config.db_path().file_name().unwrap(), "oracle.db");
}

#[test]
fn a_missing_file_under_a_detour_is_canonicalized_through_its_parent() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.dir.path().join("sub")).unwrap();
    let config = fixture.load(&[(ENV_DB_PATH, &fixture.dir.file("sub/../new.db"))]).unwrap();
    assert_eq!(config.db_path(), fs::canonicalize(fixture.dir.path()).unwrap().join("new.db"));
}

#[test]
fn loading_does_not_create_the_database() {
    let fixture = Fixture::new();
    fixture.load(&[]).unwrap();
    assert!(!std::path::Path::new(&fixture.db).exists());
}

#[test]
fn unusable_db_paths_are_rejected_with_a_reason() {
    let fixture = Fixture::new();
    let a_file = fixture.dir.file("a_file");
    fs::write(&a_file, b"").unwrap();
    let a_dir = fixture.dir.file("a_dir");
    fs::create_dir(&a_dir).unwrap();

    for (raw, expected) in [
        (a_dir.clone(), "is a directory"),
        (format!("{a_dir}/"), "does not name a file"),
        (fixture.dir.file("missing/oracle.db"), "does not exist"),
        (fixture.dir.file("missing/deeper/oracle.db"), "does not exist"),
        (format!("{a_file}/child.db"), "is not a directory"),
        ("/".to_owned(), "does not name a file"),
        ("..".to_owned(), "does not name a file"),
    ] {
        let reason = rejection_reason(fixture.load(&[(ENV_DB_PATH, &raw)]), "db_path");
        assert!(reason.contains(expected), "{raw}: {reason}");
    }
}

#[cfg(unix)]
#[test]
fn special_files_are_rejected() {
    let fixture = Fixture::new();
    let reason = rejection_reason(fixture.load(&[(ENV_DB_PATH, "/dev/null")]), "db_path");
    assert!(reason.contains("not a regular file"), "{reason}");
}

#[cfg(unix)]
#[test]
fn a_symlink_to_a_file_resolves_to_its_target() {
    let fixture = Fixture::new();
    let target = fixture.dir.file("real.db");
    fs::write(&target, b"").unwrap();
    let link = fixture.dir.file("link.db");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    let config = fixture.load(&[(ENV_DB_PATH, &link)]).unwrap();
    assert_eq!(config.db_path(), fs::canonicalize(&target).unwrap());
}

#[cfg(unix)]
#[test]
fn a_symlink_to_a_directory_is_rejected() {
    let fixture = Fixture::new();
    let link = fixture.dir.file("dir_link");
    std::os::unix::fs::symlink(fixture.dir.path(), &link).unwrap();
    let reason = rejection_reason(fixture.load(&[(ENV_DB_PATH, &link)]), "db_path");
    assert!(reason.contains("is a directory"), "{reason}");
}

#[cfg(unix)]
#[test]
fn a_non_utf8_db_path_is_accepted() {
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    let fixture = Fixture::new();
    let mut raw = fixture.dir.path().as_os_str().as_bytes().to_vec();
    raw.extend_from_slice(b"/db\xff.sqlite");
    let config = OracleConfig::from_lookup(|name| match name {
        ENV_LENS_URL => Some(OsString::from(LENS_URL)),
        ENV_DB_PATH => Some(OsString::from_vec(raw.clone())),
        _ => None,
    })
    .unwrap();
    assert_eq!(config.db_path().file_name().unwrap().as_bytes(), b"db\xff.sqlite");
}

#[cfg(unix)]
#[test]
fn an_unreadable_parent_is_a_config_error_not_an_io_error() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new();
    let locked = dir.path().join("locked");
    fs::create_dir(&locked).unwrap();
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
    // Root ignores permission bits, so there is nothing to observe.
    if fs::read_dir(&locked).is_ok() {
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let db = locked.join("inner").join("oracle.db");
    let result = load(&[(ENV_LENS_URL, LENS_URL), (ENV_DB_PATH, db.to_str().unwrap())]);
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

    let reason = rejection_reason(result, "db_path");
    assert!(reason.contains("cannot inspect"), "{reason}");
    assert!(reason.to_lowercase().contains("permission denied"), "{reason}");
}

// ---------------------------------------------------------------------------
// Ordering and error kind
// ---------------------------------------------------------------------------

#[test]
fn pure_checks_are_reported_before_the_filesystem_is_touched() {
    // Both the path and the number are bad; the number is reported.
    let result = Fixture::new().load(&[
        (ENV_DB_PATH, "/definitely/not/a/real/dir/oracle.db"),
        (ENV_ANOMALY_THRESHOLD, "loud"),
    ]);
    assert_eq!(rejected_field(result), "anomaly_threshold");
}

#[test]
fn the_first_invalid_field_in_declaration_order_is_reported() {
    let all_bad = [
        (ENV_LENS_URL, "ftp://lens"),
        (ENV_DB_PATH, "/definitely/not/a/real/dir/oracle.db"),
        (ENV_LENS_TIMEOUT_SECS, "0"),
        (ENV_MAX_ROWS, "0"),
        (ENV_MAX_SLOT_SPAN, "0"),
        (ENV_DETECT_INTERVAL_SECS, "0"),
        (ENV_BUCKET_SECS, "0"),
        (ENV_BASELINE_BUCKETS, "0"),
        (ENV_ANOMALY_THRESHOLD, "0"),
    ];
    // Check order: everything pure in declaration order, db_path last.
    let expected = [
        "lens_url",
        "lens_timeout_secs",
        "max_rows",
        "max_slot_span",
        "detect_interval_secs",
        "bucket_secs",
        "baseline_buckets",
        "anomaly_threshold",
        "db_path",
    ];
    // Fix one field at a time, front to back; the next one is blamed.
    let fixes = [
        (ENV_LENS_URL, LENS_URL),
        (ENV_LENS_TIMEOUT_SECS, "10"),
        (ENV_MAX_ROWS, "10"),
        (ENV_MAX_SLOT_SPAN, "1000"),
        (ENV_DETECT_INTERVAL_SECS, "60"),
        (ENV_BUCKET_SECS, "60"),
        (ENV_BASELINE_BUCKETS, "10"),
        (ENV_ANOMALY_THRESHOLD, "3"),
    ];
    let mut vars: Vec<(&str, &str)> = all_bad.to_vec();
    for (step, field) in expected.iter().enumerate() {
        assert_eq!(rejected_field(load(&vars)), *field, "after {step} fixes");
        if let Some(&(var, good)) = fixes.get(step) {
            vars.iter_mut().find(|(name, _)| *name == var).unwrap().1 = good;
        }
    }
}

#[test]
fn every_config_failure_is_a_config_error() {
    let fixture = Fixture::new();
    for vars in [
        vec![(ENV_LENS_URL, "nope")],
        vec![(ENV_DB_PATH, "/no/such/dir/oracle.db")],
        vec![(ENV_DB_PATH, "/dev/null")],
        vec![(ENV_MAX_ROWS, "-5")],
        vec![(ENV_ANOMALY_THRESHOLD, "NaN")],
        vec![(ENV_DETECT_INTERVAL_SECS, "600"), (ENV_BUCKET_SECS, "300")],
    ] {
        let err = fixture.load(&vars).unwrap_err();
        assert_eq!(err.kind(), "config_error", "{vars:?}: {err}");
        assert!(err.config_field().is_some(), "{vars:?}");
    }
}

// ---------------------------------------------------------------------------
// display()
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn capture(max_level: tracing::Level, config: &OracleConfig) -> String {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(max_level)
        .finish();
    tracing::subscriber::with_default(subscriber, || config.display());
    String::from_utf8(captured.0.lock().unwrap().clone()).unwrap()
}

#[test]
fn display_emits_one_info_event_with_every_field() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            (ENV_LENS_URL, "http://lens:7000/"),
            (ENV_LENS_TIMEOUT_SECS, "40"),
            (ENV_MAX_ROWS, "123"),
            (ENV_MAX_SLOT_SPAN, "4567"),
            (ENV_DETECT_INTERVAL_SECS, "20"),
            (ENV_BUCKET_SECS, "90"),
            (ENV_BASELINE_BUCKETS, "12"),
            (ENV_ANOMALY_THRESHOLD, "2.75"),
        ])
        .unwrap();

    let output = capture(tracing::Level::INFO, &config);
    assert_eq!(output.lines().count(), 1, "{output}");
    assert!(output.contains("INFO"), "{output}");
    assert!(output.contains("ORACLE configuration loaded"), "{output}");
    for expected in [
        "lens_url=http://lens:7000".to_owned(),
        format!("db_path={}", config.db_path().display()),
        "lens_timeout_secs=40".to_owned(),
        "max_rows=123".to_owned(),
        "max_slot_span=4567".to_owned(),
        "detect_interval_secs=20".to_owned(),
        "bucket_secs=90".to_owned(),
        "baseline_buckets=12".to_owned(),
        "anomaly_threshold=2.75".to_owned(),
    ] {
        assert!(output.contains(&expected), "missing {expected:?} in {output}");
    }
}

#[test]
fn display_is_silent_below_info() {
    let config = Fixture::new().load(&[]).unwrap();
    assert_eq!(capture(tracing::Level::WARN, &config), "");
}

// ---------------------------------------------------------------------------
// OracleError
// ---------------------------------------------------------------------------

#[test]
fn error_display_includes_context_and_cause() {
    let cause = || io::Error::other("boom");
    let cases: Vec<(OracleError, &str, &str)> = vec![
        (OracleError::config("lens_url", "bad"), "config_error", "invalid config field lens_url: bad"),
        (
            OracleError::lens("http://lens/query", cause()),
            "lens_error",
            "LENS request to http://lens/query failed: boom",
        ),
        (
            OracleError::store("insert baseline", cause()),
            "store_error",
            "store operation insert baseline failed: boom",
        ),
        (
            OracleError::spec("window", "exceeds max_slot_span"),
            "spec_error",
            "invalid spec field window: exceeds max_slot_span",
        ),
        (
            OracleError::detect("failure_rate", cause()),
            "detect_error",
            "detector for failure_rate failed: boom",
        ),
        (OracleError::io("/data/oracle.db", cause()), "io_error", "I/O failure on /data/oracle.db: boom"),
    ];
    for (err, kind, display) in cases {
        assert_eq!(err.kind(), kind);
        assert_eq!(err.to_string(), display);
    }
}

#[test]
fn boxed_variants_accept_strings_as_causes() {
    let err = OracleError::lens("http://lens", format!("status {}", 500));
    assert_eq!(err.to_string(), "LENS request to http://lens failed: status 500");
    let err = OracleError::store("open", "database is locked");
    assert_eq!(err.to_string(), "store operation open failed: database is locked");
    let err = OracleError::detect("new_mints", "baseline empty");
    assert_eq!(err.to_string(), "detector for new_mints failed: baseline empty");
}

#[test]
fn error_source_exposes_the_cause() {
    use std::error::Error;

    let err = OracleError::io("/data", io::Error::new(io::ErrorKind::NotFound, "gone"));
    let source = err.source().expect("IoError has a source");
    assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::NotFound);

    for err in [
        OracleError::lens("http://lens", io::Error::new(io::ErrorKind::TimedOut, "slow")),
        OracleError::store("claim", io::Error::new(io::ErrorKind::TimedOut, "locked")),
        OracleError::detect("transfers", io::Error::new(io::ErrorKind::TimedOut, "slow")),
    ] {
        let source = err.source().unwrap_or_else(|| panic!("{} has a source", err.kind()));
        assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::TimedOut);
    }

    assert!(OracleError::config("f", "r").source().is_none());
    assert!(OracleError::spec("f", "r").source().is_none());
}

#[test]
fn config_field_is_only_set_for_config_errors() {
    assert_eq!(OracleError::config("db_path", "r").config_field(), Some("db_path"));
    // A spec error also has a `field`, but it is not a config field.
    assert_eq!(OracleError::spec("window", "r").config_field(), None);
    assert_eq!(OracleError::lens("u", "e").config_field(), None);
    assert_eq!(OracleError::store("o", "e").config_field(), None);
    assert_eq!(OracleError::detect("m", "e").config_field(), None);
    assert_eq!(OracleError::io("p", io::Error::other("e")).config_field(), None);
}

#[test]
fn error_kinds_are_distinct() {
    let kinds = [
        OracleError::config("f", "r").kind(),
        OracleError::lens("u", "e").kind(),
        OracleError::store("o", "e").kind(),
        OracleError::spec("f", "r").kind(),
        OracleError::detect("m", "e").kind(),
        OracleError::io("p", io::Error::other("e")).kind(),
    ];
    let unique: std::collections::HashSet<_> = kinds.iter().collect();
    assert_eq!(unique.len(), kinds.len(), "{kinds:?}");
}

#[test]
fn oracle_error_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<OracleError>();
}
