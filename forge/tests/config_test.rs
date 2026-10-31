//! `ForgeConfig::from_lookup` against an in-memory environment and a scratch
//! directory, plus `ForgeError` formatting. Range edges are covered in
//! `config_boundary_test.rs`.

mod common;

use std::fs;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Fixture, TempDir, load, rejected_field, rejection_reason};
use forge::config::*;
use forge::{ForgeConfig, ForgeError};

// ---------------------------------------------------------------------------
// Defaults and overrides
// ---------------------------------------------------------------------------

#[test]
fn only_changelog_path_set_yields_the_defaults() {
    let fixture = Fixture::new();
    let config = load(&[(ENV_CHANGELOG_PATH, &fixture.changelog)]).unwrap();

    let canonical_dir = fs::canonicalize(fixture.dir.path()).unwrap();
    assert_eq!(config.changelog_path(), canonical_dir.join("changelog.jsonl"));
    assert_eq!(config.lens_url(), DEFAULT_LENS_URL);
    // The default database path is relative to the working directory.
    assert_eq!(config.db_path(), std::env::current_dir().unwrap().canonicalize().unwrap().join("forge.db"));
    assert_eq!(config.bind_address(), "127.0.0.1:8081");
    assert_eq!(config.poll_interval_ms(), 500);
    assert_eq!(config.visibility_timeout_secs(), 120);
    assert_eq!(config.webhook_max_attempts(), 5);
    assert_eq!(config.webhook_timeout_secs(), 10);
    assert_eq!(config.log_level(), "info");
}

#[test]
fn every_variable_overrides_its_default() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            (ENV_LENS_URL, "https://lens.internal:9443/v1"),
            (ENV_BIND_ADDRESS, "0.0.0.0:9000"),
            (ENV_POLL_INTERVAL_MS, "250"),
            (ENV_VISIBILITY_TIMEOUT_SECS, "90"),
            (ENV_WEBHOOK_MAX_ATTEMPTS, "8"),
            (ENV_WEBHOOK_TIMEOUT_SECS, "15"),
            (ENV_LOG_LEVEL, "debug"),
        ])
        .unwrap();

    let canonical_dir = fs::canonicalize(fixture.dir.path()).unwrap();
    assert_eq!(config.db_path(), canonical_dir.join("forge.db"));
    assert_eq!(config.lens_url(), "https://lens.internal:9443/v1");
    assert_eq!(config.bind_address(), "0.0.0.0:9000");
    assert_eq!(config.socket_addr(), "0.0.0.0:9000".parse::<SocketAddr>().unwrap());
    assert_eq!(config.poll_interval(), Duration::from_millis(250));
    assert_eq!(config.visibility_timeout(), Duration::from_secs(90));
    assert_eq!(config.webhook_max_attempts(), 8);
    assert_eq!(config.webhook_timeout(), Duration::from_secs(15));
    assert_eq!(config.log_level(), "debug");
}

#[test]
fn values_are_trimmed_and_case_normalized() {
    let fixture = Fixture::new();
    let config = fixture
        .load(&[
            (ENV_LENS_URL, "  HTTP://LENS:8080/  "),
            (ENV_BIND_ADDRESS, " 127.0.0.1:9000\n"),
            (ENV_POLL_INTERVAL_MS, "\t100 "),
            (ENV_LOG_LEVEL, " WARN "),
        ])
        .unwrap();
    assert_eq!(config.lens_url(), "http://lens:8080");
    assert_eq!(config.bind_address(), "127.0.0.1:9000");
    assert_eq!(config.poll_interval_ms(), 100);
    assert_eq!(config.log_level(), "warn");
}

#[test]
fn unrelated_variables_are_ignored() {
    let fixture = Fixture::new();
    let config = fixture.load(&[("FORGE_UNKNOWN", "x"), ("LENS_BIND_ADDRESS", "not an address")]).unwrap();
    assert_eq!(config.bind_address(), DEFAULT_BIND_ADDRESS);
}

#[test]
fn config_is_comparable_and_cloneable() {
    let fixture = Fixture::new();
    let a = fixture.load(&[]).unwrap();
    let b = fixture.load(&[]).unwrap();
    assert_eq!(a, b);
    assert_eq!(a.clone(), a);
    assert_ne!(a, fixture.load(&[(ENV_POLL_INTERVAL_MS, "51")]).unwrap());
}

// ---------------------------------------------------------------------------
// lens_url
// ---------------------------------------------------------------------------

#[test]
fn lens_url_drops_the_trailing_slash_and_keeps_a_base_path() {
    let fixture = Fixture::new();
    for (raw, expected) in [
        ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
        ("http://lens/prefix/", "http://lens/prefix"),
        ("https://lens.example.com", "https://lens.example.com"),
        ("http://[::1]:8080", "http://[::1]:8080"),
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
        ("http://lens?x=1", "query string"),
        ("http://lens#frag", "fragment"),
        ("http://lens:0", "port 0"),
        ("http://admin:secret@lens", "credentials"),
    ] {
        let reason = rejection_reason(fixture.load(&[(ENV_LENS_URL, raw)]), "lens_url");
        assert!(reason.contains(expected), "{raw}: {reason}");
        assert!(!reason.contains("secret"), "{raw}: {reason}");
    }
}

// ---------------------------------------------------------------------------
// changelog_path and db_path
// ---------------------------------------------------------------------------

#[test]
fn changelog_path_is_required() {
    let reason = rejection_reason(load(&[]), "changelog_path");
    assert!(reason.contains(ENV_CHANGELOG_PATH), "{reason}");
}

#[test]
fn existing_regular_files_are_accepted_and_canonicalized() {
    let fixture = Fixture::new();
    fs::write(&fixture.changelog, b"{}\n").unwrap();
    fs::write(&fixture.db, b"").unwrap();

    // Reach both files through a `..` detour.
    let sub = fixture.dir.path().join("sub");
    fs::create_dir(&sub).unwrap();
    let detour = |name: &str| sub.join("..").join(name).to_str().unwrap().to_owned();

    let config = load(&[(ENV_CHANGELOG_PATH, &detour("changelog.jsonl")), (ENV_DB_PATH, &detour("forge.db"))])
        .unwrap();
    assert_eq!(config.changelog_path(), fs::canonicalize(&fixture.changelog).unwrap());
    assert_eq!(config.db_path(), fs::canonicalize(&fixture.db).unwrap());
    assert!(config.changelog_path().is_absolute());
}

#[test]
fn missing_files_in_an_existing_directory_are_accepted() {
    let fixture = Fixture::new();
    let config = fixture.load(&[]).unwrap();
    assert!(!config.changelog_path().exists());
    assert!(!config.db_path().exists());
    assert_eq!(config.changelog_path().parent().unwrap(), fs::canonicalize(fixture.dir.path()).unwrap());
}

#[test]
fn path_rules_apply_to_both_path_fields() {
    for (var, field) in [(ENV_CHANGELOG_PATH, "changelog_path"), (ENV_DB_PATH, "db_path")] {
        let fixture = Fixture::new();
        let a_file = fixture.dir.file("a_file");
        fs::write(&a_file, b"").unwrap();
        let a_dir = fixture.dir.file("a_dir");
        fs::create_dir(&a_dir).unwrap();

        let cases = [
            (a_dir.clone(), "is a directory"),
            (format!("{a_dir}/"), "does not name a file"),
            (fixture.dir.file("missing/changelog"), "does not exist"),
            (fixture.dir.file("missing/deeper/changelog"), "does not exist"),
            (format!("{a_file}/child"), "is not a directory"),
        ];
        for (raw, expected) in cases {
            let reason = rejection_reason(fixture.load(&[(var, &raw)]), field);
            assert!(reason.contains(expected), "{var}={raw}: {reason}");
        }
    }
}

#[cfg(unix)]
#[test]
fn special_files_are_rejected() {
    let fixture = Fixture::new();
    let reason = rejection_reason(fixture.load(&[(ENV_CHANGELOG_PATH, "/dev/null")]), "changelog_path");
    assert!(reason.contains("not a regular file"), "{reason}");
}

#[test]
fn db_path_must_differ_from_changelog_path() {
    let fixture = Fixture::new();
    let reason = rejection_reason(fixture.load(&[(ENV_DB_PATH, &fixture.changelog)]), "db_path");
    assert!(reason.contains("same file as changelog_path"), "{reason}");
}

#[test]
fn db_path_spelled_differently_but_resolving_to_the_changelog_is_rejected() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.dir.path().join("sub")).unwrap();
    let detour = fixture.dir.file("sub/../changelog.jsonl");
    assert_eq!(rejected_field(fixture.load(&[(ENV_DB_PATH, &detour)])), "db_path");
}

#[cfg(unix)]
#[test]
fn db_path_symlinked_to_the_changelog_is_rejected() {
    let fixture = Fixture::new();
    fs::write(&fixture.changelog, b"").unwrap();
    let link = fixture.dir.file("link.db");
    std::os::unix::fs::symlink(&fixture.changelog, &link).unwrap();
    assert_eq!(rejected_field(fixture.load(&[(ENV_DB_PATH, &link)])), "db_path");
}

#[cfg(unix)]
#[test]
fn unreadable_parent_is_an_io_error_not_a_config_error() {
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
    let changelog = locked.join("inner").join("changelog.jsonl");
    let result = load(&[(ENV_CHANGELOG_PATH, changelog.to_str().unwrap())]);
    fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

    match result {
        Err(err @ ForgeError::IoError { .. }) => assert_eq!(err.kind(), "io_error"),
        other => panic!("expected IoError, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Ordering
// ---------------------------------------------------------------------------

#[test]
fn pure_checks_are_reported_before_the_filesystem_is_touched() {
    // Both the path and the number are bad; the number is reported.
    let result = load(&[
        (ENV_CHANGELOG_PATH, "/definitely/not/a/real/dir/changelog"),
        (ENV_POLL_INTERVAL_MS, "12abc"),
    ]);
    assert_eq!(rejected_field(result), "poll_interval_ms");
}

#[test]
fn the_first_invalid_field_in_declaration_order_is_reported() {
    let fixture = Fixture::new();
    let result = fixture.load(&[
        (ENV_LENS_URL, "ftp://lens"),
        (ENV_BIND_ADDRESS, "nope"),
        (ENV_LOG_LEVEL, "loud"),
    ]);
    assert_eq!(rejected_field(result), "lens_url");
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

fn capture(max_level: tracing::Level, config: &ForgeConfig) -> String {
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
            (ENV_BIND_ADDRESS, "127.0.0.1:7777"),
            (ENV_POLL_INTERVAL_MS, "1234"),
            (ENV_VISIBILITY_TIMEOUT_SECS, "45"),
            (ENV_WEBHOOK_MAX_ATTEMPTS, "3"),
            (ENV_WEBHOOK_TIMEOUT_SECS, "7"),
            (ENV_LOG_LEVEL, "trace"),
        ])
        .unwrap();

    let output = capture(tracing::Level::INFO, &config);
    assert_eq!(output.lines().count(), 1, "{output}");
    assert!(output.contains("INFO"), "{output}");
    assert!(output.contains("FORGE configuration loaded"), "{output}");
    for expected in [
        format!("changelog_path={}", config.changelog_path().display()),
        "lens_url=http://lens:7000".to_owned(),
        format!("db_path={}", config.db_path().display()),
        "bind_address=127.0.0.1:7777".to_owned(),
        "poll_interval_ms=1234".to_owned(),
        "visibility_timeout_secs=45".to_owned(),
        "webhook_max_attempts=3".to_owned(),
        "webhook_timeout_secs=7".to_owned(),
        "log_level=trace".to_owned(),
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
// ForgeError
// ---------------------------------------------------------------------------

#[test]
fn error_display_includes_context_and_cause() {
    let cause = || io::Error::other("boom");
    let cases: Vec<(ForgeError, &str, &str)> = vec![
        (ForgeError::config("lens_url", "bad"), "config_error", "invalid config field lens_url: bad"),
        (
            ForgeError::changelog("/var/prism/changelog", "truncated record"),
            "changelog_error",
            "change log /var/prism/changelog is unusable: truncated record",
        ),
        (
            ForgeError::lens_client("http://lens/query", cause()),
            "lens_client_error",
            "LENS request to http://lens/query failed: boom",
        ),
        (
            ForgeError::registry("insert alert", cause()),
            "registry_error",
            "registry operation insert alert failed: boom",
        ),
        (ForgeError::webhook("d-42", cause()), "webhook_error", "webhook delivery d-42 failed: boom"),
        (ForgeError::io("/data/forge.db", cause()), "io_error", "I/O failure on /data/forge.db: boom"),
    ];
    for (err, kind, display) in cases {
        assert_eq!(err.kind(), kind);
        assert_eq!(err.to_string(), display);
    }
}

#[test]
fn boxed_variants_accept_strings_as_causes() {
    let err = ForgeError::webhook("d-1", "HTTP 503");
    assert_eq!(err.to_string(), "webhook delivery d-1 failed: HTTP 503");
    let err = ForgeError::lens_client("http://lens", format!("status {}", 500));
    assert_eq!(err.to_string(), "LENS request to http://lens failed: status 500");
}

#[test]
fn error_source_exposes_the_cause() {
    use std::error::Error;

    let err = ForgeError::io("/data", io::Error::new(io::ErrorKind::NotFound, "gone"));
    let source = err.source().expect("IoError has a source");
    assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::NotFound);

    let err = ForgeError::registry("claim", io::Error::new(io::ErrorKind::TimedOut, "locked"));
    let source = err.source().expect("RegistryError has a source");
    assert_eq!(source.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::TimedOut);

    assert!(ForgeError::config("f", "r").source().is_none());
    assert!(ForgeError::changelog("p", "r").source().is_none());
}

#[test]
fn config_field_is_only_set_for_config_errors() {
    assert_eq!(ForgeError::config("db_path", "r").config_field(), Some("db_path"));
    assert_eq!(ForgeError::changelog("p", "r").config_field(), None);
    assert_eq!(ForgeError::webhook("d", "e").config_field(), None);
}

#[test]
fn forge_error_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<ForgeError>();
}
