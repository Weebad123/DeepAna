//! `Store` through its public API: opening, every validation rule at its
//! boundaries, progress, listing, digests, pruning and stats. The all-or-
//! nothing and crash guarantees of scan commits are in
//! `store_atomicity_test.rs`.

mod common;

use std::fs;

use common::{LENS_URL, TempDir, load};
use oracle::config::{ENV_DB_PATH, ENV_LENS_URL};
use oracle::store::*;
use oracle::{OracleError, OracleResult};
use rusqlite::Connection;
use serde_json::json;

fn open(dir: &TempDir) -> Store {
    Store::open_path(dir.path().join("oracle.db")).unwrap()
}

fn raw(dir: &TempDir) -> Connection {
    Connection::open(dir.path().join("oracle.db")).unwrap()
}

fn anomaly(subject: &str, bucket: u64) -> NewAnomaly {
    NewAnomaly {
        subject: subject.into(),
        bucket,
        start_slot: bucket * 10,
        end_slot: bucket * 10 + 9,
        kind: AnomalyKind::Series,
        value: 42.0,
        baseline: Some(10.0),
        scale: Some(2.0),
        score: 10.8,
        detail: None,
    }
}

/// `(http status, message)` of a store failure.
fn store_error<T: std::fmt::Debug>(result: OracleResult<T>) -> (u16, String) {
    let err = result.unwrap_err();
    let status = err.as_store_error().unwrap_or_else(|| panic!("not a StoreError: {err:?}")).http_status();
    (status, err.to_string())
}

/// The `field` of an `Invalid` store failure.
fn invalid_field<T: std::fmt::Debug>(result: OracleResult<T>) -> String {
    let err = result.unwrap_err();
    match err.as_store_error() {
        Some(StoreError::Invalid { field, .. }) => field.clone(),
        other => panic!("expected Invalid, got {other:?} ({err})"),
    }
}

/// Commit `anomaly` alone, through its own bucket, on a fresh store.
fn commit_one(new: NewAnomaly) -> OracleResult<ScanCommit> {
    let dir = TempDir::new();
    let store = open(&dir);
    let bucket = new.bucket;
    let result = store.commit_scan("m", &[new], bucket);
    if result.is_err() {
        // Rejected before the database: nothing at all was written.
        assert_eq!(store.stats().unwrap().anomalies, 0);
        assert_eq!(store.progress("m").unwrap(), None);
    }
    result
}

fn set_detected_at(dir: &TempDir, subject: &str, unix_ms: i64) {
    raw(dir).execute("UPDATE anomalies SET detected_at = ?1 WHERE subject = ?2", rusqlite::params![unix_ms, subject])
        .unwrap();
}

// ---------------------------------------------------------------------------
// Opening
// ---------------------------------------------------------------------------

#[test]
fn open_from_config_creates_the_file_and_migrates() {
    let dir = TempDir::new();
    let db = dir.file("oracle.db");
    let config = load(&[(ENV_LENS_URL, LENS_URL), (ENV_DB_PATH, &db)]).unwrap();
    assert!(!config.db_path().exists());

    let store = Store::open(&config).unwrap();
    assert!(config.db_path().is_file());
    assert_eq!(store.path(), config.db_path());
    assert_eq!(store.schema_version().unwrap(), MIGRATIONS.len() as u32);
    assert!(sidecar_lock_path(config.db_path()).is_file());
}

#[test]
fn the_file_is_marked_as_oracle_and_uses_wal() {
    let dir = TempDir::new();
    let _store = open(&dir);
    let conn = raw(&dir);
    let app_id: i32 = conn.query_row("PRAGMA application_id", [], |r| r.get(0)).unwrap();
    let mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0)).unwrap();
    let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
    assert_eq!(app_id, APPLICATION_ID);
    assert_eq!(mode, "wal");
    assert_eq!(version, MIGRATIONS.len() as u32);
}

#[test]
fn reopen_keeps_data_and_version() {
    let dir = TempDir::new();
    {
        let store = open(&dir);
        store.commit_scan("m", &[anomaly("", 3)], 3).unwrap();
        assert!(store.insert_digest(1, 2, &json!({"n": 1})).unwrap());
    }
    let store = open(&dir);
    assert_eq!(store.schema_version().unwrap(), MIGRATIONS.len() as u32);
    assert_eq!(store.progress("m").unwrap(), Some(3));
    assert_eq!(store.list_anomalies(&AnomalyFilter::default()).unwrap().len(), 1);
    assert_eq!(store.latest_digest().unwrap().unwrap().body, json!({"n": 1}));
}

#[test]
fn a_second_open_is_refused_while_any_clone_is_alive() {
    let dir = TempDir::new();
    let first = open(&dir);
    let clone = first.clone();

    let (status, message) = store_error(Store::open_path(dir.path().join("oracle.db")));
    assert_eq!(status, 409);
    assert!(message.contains("second ORACLE"), "{message}");

    // The lock is shared by every clone and released with the last one.
    drop(first);
    assert_eq!(store_error(Store::open_path(dir.path().join("oracle.db"))).0, 409);
    drop(clone);
    open(&dir);
}

#[test]
fn a_database_from_a_newer_oracle_is_refused() {
    let dir = TempDir::new();
    drop(open(&dir));
    raw(&dir).pragma_update(None, "user_version", MIGRATIONS.len() as u32 + 1).unwrap();
    let (status, message) = store_error(Store::open_path(dir.path().join("oracle.db")));
    assert_eq!(status, 409);
    assert!(message.contains("newer than this ORACLE"), "{message}");
}

#[test]
fn another_services_database_is_refused_and_left_untouched() {
    // FORGE's registry: versioned, no application_id. An unversioned file
    // with tables. A file stamped by some other application.
    let setups: [(&str, &str); 3] = [
        ("forge-like", "CREATE TABLE alerts (id TEXT); PRAGMA user_version = 2;"),
        ("unversioned", "CREATE TABLE things (id INTEGER);"),
        ("other app", "PRAGMA application_id = 1234; PRAGMA user_version = 1;"),
    ];
    for (name, setup) in setups {
        let dir = TempDir::new();
        let path = dir.path().join("oracle.db");
        Connection::open(&path).unwrap().execute_batch(setup).unwrap();

        let (status, message) = store_error(Store::open_path(&path));
        assert_eq!(status, 409, "{name}");
        assert!(message.contains("not an ORACLE database"), "{name}: {message}");

        let anomalies: i64 = Connection::open(&path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM sqlite_schema WHERE name = 'anomalies'", [], |r| r.get(0))
            .unwrap();
        assert_eq!(anomalies, 0, "{name}: ORACLE's tables must not be added");
    }
}

#[test]
fn a_file_that_is_not_sqlite_is_refused_and_left_untouched() {
    let dir = TempDir::new();
    let path = dir.path().join("oracle.db");
    let contents = b"this is not a database, it is a note".repeat(200);
    fs::write(&path, &contents).unwrap();
    assert_eq!(store_error(Store::open_path(&path)).0, 500);
    assert_eq!(fs::read(&path).unwrap(), contents);
}

#[test]
fn an_empty_existing_file_is_a_new_database() {
    let dir = TempDir::new();
    fs::write(dir.path().join("oracle.db"), b"").unwrap();
    assert_eq!(open(&dir).schema_version().unwrap(), MIGRATIONS.len() as u32);
}

#[test]
fn an_unwritable_lock_location_is_an_io_error() {
    let dir = TempDir::new();
    let err = Store::open_path(dir.path().join("missing-dir").join("oracle.db")).unwrap_err();
    assert!(matches!(err, OracleError::IoError { .. }), "{err:?}");
}

#[test]
fn store_is_send_sync_and_clone() {
    fn assert_traits<T: Send + Sync + Clone + 'static>() {}
    assert_traits::<Store>();
}

// ---------------------------------------------------------------------------
// Validation: metric
// ---------------------------------------------------------------------------

fn commit_metric(metric: &str) -> OracleResult<ScanCommit> {
    let dir = TempDir::new();
    open(&dir).commit_scan(metric, &[anomaly("", 1)], 1)
}

#[test]
fn metric_length_boundaries() {
    assert!(commit_metric("m").is_ok());
    assert!(commit_metric(&"m".repeat(MAX_METRIC_CHARS)).is_ok());
    assert_eq!(invalid_field(commit_metric(&"m".repeat(MAX_METRIC_CHARS + 1))), "metric");
    assert_eq!(invalid_field(commit_metric("")), "metric");
}

#[test]
fn metric_charset() {
    for ok in ["tx_per_bucket", "failure.rate", "first-seen-mints", "m2", "0", "a.b-c_9"] {
        assert!(commit_metric(ok).is_ok(), "{ok}");
    }
    for bad in ["Tx", "tx rate", " tx", "tx\n", "tx/rate", "é", "tx:rate", "tx*"] {
        assert_eq!(invalid_field(commit_metric(bad)), "metric", "{bad:?}");
    }
}

// ---------------------------------------------------------------------------
// Validation: anomaly fields
// ---------------------------------------------------------------------------

#[test]
fn subject_length_is_counted_in_characters() {
    let commit = |subject: String| commit_one(NewAnomaly { subject, ..anomaly("", 1) });
    assert!(commit(String::new()).is_ok());
    assert!(commit("s".repeat(MAX_SUBJECT_CHARS)).is_ok());
    assert_eq!(invalid_field(commit("s".repeat(MAX_SUBJECT_CHARS + 1))), "anomalies[0].subject");
    // Two bytes each: 256 bytes is still 128 characters.
    assert!(commit("é".repeat(MAX_SUBJECT_CHARS)).is_ok());
    assert_eq!(invalid_field(commit("é".repeat(MAX_SUBJECT_CHARS + 1))), "anomalies[0].subject");
}

#[test]
fn subject_rejects_control_characters_and_surrounding_whitespace() {
    let commit = |subject: &str| commit_one(NewAnomaly { subject: subject.into(), ..anomaly("", 1) });
    assert!(commit("So11111111111111111111111111111111111111112").is_ok());
    assert!(commit("mint with inner spaces").is_ok());
    for bad in [" mint", "mint ", "\tmint", "mi\nnt", "mint\0", "\u{7f}"] {
        assert_eq!(invalid_field(commit(bad)), "anomalies[0].subject", "{bad:?}");
    }
}

#[test]
fn bucket_and_slots_at_the_i64_boundary() {
    let max = i64::MAX as u64;
    let edge = NewAnomaly { bucket: max, start_slot: max, end_slot: max, ..anomaly("", 0) };
    assert!(commit_one(edge.clone()).is_ok());

    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(invalid_field(store.commit_scan("m", &[], max + 1)), "progress_bucket");
    // Progress at the edge, so only the row's own bucket overflows.
    assert_eq!(
        invalid_field(store.commit_scan("m", &[NewAnomaly { bucket: max + 1, ..edge.clone() }], max)),
        "anomalies[0].bucket"
    );
    assert_eq!(store.stats().unwrap().anomalies, 0);
    assert_eq!(
        invalid_field(commit_one(NewAnomaly { start_slot: max + 1, end_slot: max + 1, ..edge.clone() })),
        "anomalies[0].start_slot"
    );
    assert_eq!(invalid_field(commit_one(NewAnomaly { end_slot: u64::MAX, ..edge })), "anomalies[0].end_slot");
}

#[test]
fn slot_range_must_be_ordered() {
    assert!(commit_one(NewAnomaly { start_slot: 5, end_slot: 5, ..anomaly("", 1) }).is_ok());
    assert!(commit_one(NewAnomaly { start_slot: 0, end_slot: 0, ..anomaly("", 1) }).is_ok());
    assert_eq!(
        invalid_field(commit_one(NewAnomaly { start_slot: 6, end_slot: 5, ..anomaly("", 1) })),
        "anomalies[0].end_slot"
    );
}

#[test]
fn every_float_must_be_finite() {
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let base = anomaly("", 1);
        let cases = [
            ("value", NewAnomaly { value: bad, ..base.clone() }),
            ("score", NewAnomaly { score: bad, ..base.clone() }),
            ("baseline", NewAnomaly { baseline: Some(bad), ..base.clone() }),
            ("scale", NewAnomaly { scale: Some(bad), ..base.clone() }),
        ];
        for (name, new) in cases {
            assert_eq!(invalid_field(commit_one(new)), format!("anomalies[0].{name}"), "{name} = {bad}");
        }
    }
}

#[test]
fn finite_extremes_are_stored_exactly() {
    let dir = TempDir::new();
    let store = open(&dir);
    let new = NewAnomaly {
        value: f64::MAX,
        baseline: Some(f64::MIN),
        scale: Some(f64::MIN_POSITIVE),
        score: -f64::MAX,
        ..anomaly("", 1)
    };
    store.commit_scan("m", std::slice::from_ref(&new), 1).unwrap();
    let stored = store.list_anomalies(&AnomalyFilter::default()).unwrap().remove(0);
    assert_eq!(stored.value, f64::MAX);
    assert_eq!(stored.baseline, Some(f64::MIN));
    assert_eq!(stored.scale, Some(f64::MIN_POSITIVE));
    assert_eq!(stored.score, -f64::MAX);
}

#[test]
fn scale_bounds_and_its_dependency_on_baseline() {
    let with = |baseline: Option<f64>, scale: Option<f64>| commit_one(NewAnomaly { baseline, scale, ..anomaly("", 1) });
    assert!(with(Some(1.0), Some(0.0)).is_ok(), "a MAD of 0 is a flat baseline");
    assert!(with(Some(1.0), Some(-0.0)).is_ok());
    assert!(with(Some(1.0), None).is_ok());
    assert!(with(None, None).is_ok(), "first-seen findings have neither");
    assert_eq!(invalid_field(with(Some(1.0), Some(-f64::MIN_POSITIVE))), "anomalies[0].scale");
    assert_eq!(invalid_field(with(Some(1.0), Some(-1.0))), "anomalies[0].scale");
    assert_eq!(invalid_field(with(None, Some(1.0))), "anomalies[0].scale");
}

#[test]
fn negative_scores_and_values_are_allowed() {
    // A drop below the baseline scores negative.
    assert!(commit_one(NewAnomaly { value: -3.0, score: -12.5, ..anomaly("", 1) }).is_ok());
}

/// A detail object whose compact JSON is exactly `bytes` long.
fn detail_of(bytes: usize) -> serde_json::Value {
    let overhead = r#"{"k":""}"#.len();
    let value = json!({ "k": "x".repeat(bytes - overhead) });
    assert_eq!(serde_json::to_string(&value).unwrap().len(), bytes);
    value
}

#[test]
fn detail_size_boundary_in_bytes() {
    let commit = |detail| commit_one(NewAnomaly { detail: Some(detail), ..anomaly("", 1) });
    assert!(commit(detail_of(MAX_DETAIL_BYTES - 1)).is_ok());
    assert!(commit(detail_of(MAX_DETAIL_BYTES)).is_ok());
    assert_eq!(invalid_field(commit(detail_of(MAX_DETAIL_BYTES + 1))), "anomalies[0].detail");
    // Multi-byte characters count by their UTF-8 size.
    let wide = json!({ "k": "é".repeat(MAX_DETAIL_BYTES / 2) });
    assert_eq!(invalid_field(commit(wide)), "anomalies[0].detail");
}

#[test]
fn detail_must_be_an_object() {
    let commit = |detail| commit_one(NewAnomaly { detail: Some(detail), ..anomaly("", 1) });
    assert!(commit(json!({})).is_ok());
    assert!(commit(json!({"nested": {"a": [1, 2]}})).is_ok());
    for bad in [json!(null), json!([1]), json!("text"), json!(3), json!(true)] {
        assert_eq!(invalid_field(commit(bad.clone())), "anomalies[0].detail", "{bad}");
    }
}

#[test]
fn every_kind_round_trips() {
    let dir = TempDir::new();
    let store = open(&dir);
    let rows: Vec<_> = AnomalyKind::ALL
        .iter()
        .enumerate()
        .map(|(i, &kind)| NewAnomaly { kind, ..anomaly(&format!("s{i}"), 1) })
        .collect();
    store.commit_scan("m", &rows, 1).unwrap();
    let mut kinds: Vec<_> = store.list_anomalies(&AnomalyFilter::default()).unwrap().into_iter().map(|a| a.kind).collect();
    kinds.reverse();
    assert_eq!(kinds, AnomalyKind::ALL);
}

#[test]
fn a_stored_anomaly_round_trips_every_field() {
    let dir = TempDir::new();
    let store = open(&dir);
    let new = NewAnomaly {
        subject: "Mint111".into(),
        bucket: 7,
        start_slot: 700,
        end_slot: 799,
        kind: AnomalyKind::FirstSeen,
        value: 1.0,
        baseline: None,
        scale: None,
        score: 0.0,
        detail: Some(json!({"first_slot": 712, "symbol": "TKN"})),
    };
    let before = unix_now_ms();
    store.commit_scan("first_seen_mints", std::slice::from_ref(&new), 9).unwrap();
    let after = unix_now_ms();

    let stored = store.list_anomalies(&AnomalyFilter::default()).unwrap().remove(0);
    assert!(stored.id > 0);
    assert_eq!(stored.metric, "first_seen_mints");
    assert_eq!(stored.subject, new.subject);
    assert_eq!((stored.bucket, stored.start_slot, stored.end_slot), (7, 700, 799));
    assert_eq!(stored.kind, AnomalyKind::FirstSeen);
    assert_eq!((stored.value, stored.baseline, stored.scale, stored.score), (1.0, None, None, 0.0));
    assert_eq!(stored.detail, new.detail);
    assert!((before..=after).contains(&stored.detected_at), "{before} <= {} <= {after}", stored.detected_at);
}

#[test]
fn new_anomaly_validate_matches_commit() {
    assert!(anomaly("", 1).validate().is_ok());
    let err = NewAnomaly { score: f64::NAN, ..anomaly("", 1) }.validate().unwrap_err();
    assert!(matches!(err, StoreError::Invalid { ref field, .. } if field == "anomaly.score"), "{err}");
}

// ---------------------------------------------------------------------------
// Validation: the batch
// ---------------------------------------------------------------------------

#[test]
fn an_anomaly_may_not_be_after_the_progress_it_commits_with() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(invalid_field(store.commit_scan("m", &[anomaly("", 6)], 5)), "anomalies[0].bucket");
    assert!(store.commit_scan("m", &[anomaly("", 5)], 5).unwrap().progress_advanced);
}

#[test]
fn a_repeated_key_within_one_batch_is_rejected() {
    let dir = TempDir::new();
    let store = open(&dir);
    let (status, message) = store_error(store.commit_scan("m", &[anomaly("a", 1), anomaly("b", 1), anomaly("a", 1)], 1));
    assert_eq!(status, 400);
    assert!(message.contains("anomalies[2]") && message.contains("repeats anomalies[0]"), "{message}");

    // Same subject in another bucket, or another subject in the same
    // bucket, are distinct keys.
    let commit = store.commit_scan("m", &[anomaly("a", 1), anomaly("a", 2), anomaly("b", 1), anomaly("", 1)], 2).unwrap();
    assert_eq!(commit.inserted, 4);
}

#[test]
fn errors_name_the_offending_row() {
    let dir = TempDir::new();
    let store = open(&dir);
    let rows = [anomaly("a", 1), anomaly("b", 1), NewAnomaly { score: f64::NAN, ..anomaly("c", 1) }];
    assert_eq!(invalid_field(store.commit_scan("m", &rows, 1)), "anomalies[2].score");
}

#[test]
fn batch_size_boundary() {
    let dir = TempDir::new();
    let store = open(&dir);
    let rows: Vec<_> = (0..=MAX_ANOMALIES_PER_SCAN).map(|i| anomaly(&format!("s{i}"), 1)).collect();
    assert_eq!(invalid_field(store.commit_scan("m", &rows, 1)), "anomalies");
    let commit = store.commit_scan("m", &rows[..MAX_ANOMALIES_PER_SCAN], 1).unwrap();
    assert_eq!(commit.inserted, MAX_ANOMALIES_PER_SCAN);
}

#[test]
fn an_empty_scan_still_advances_progress() {
    let dir = TempDir::new();
    let store = open(&dir);
    let commit = store.commit_scan("m", &[], 4).unwrap();
    assert_eq!(commit, ScanCommit { inserted: 0, duplicates: 0, progress_advanced: true });
    assert_eq!(store.progress("m").unwrap(), Some(4));
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

#[test]
fn progress_is_per_metric_and_listed_by_name() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(store.progress("b").unwrap(), None);
    assert!(store.list_progress().unwrap().is_empty());

    let before = unix_now_ms();
    store.commit_scan("b", &[], 10).unwrap();
    store.commit_scan("a", &[], 3).unwrap();
    store.commit_scan("b", &[], 11).unwrap();
    let after = unix_now_ms();

    assert_eq!(store.progress("a").unwrap(), Some(3));
    assert_eq!(store.progress("b").unwrap(), Some(11));
    assert_eq!(store.progress("c").unwrap(), None);
    let listed = store.list_progress().unwrap();
    assert_eq!(listed.iter().map(|p| (p.metric.as_str(), p.bucket)).collect::<Vec<_>>(), [("a", 3), ("b", 11)]);
    for progress in listed {
        assert!((before..=after).contains(&progress.updated_at));
    }
}

#[test]
fn progress_only_moves_forward() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan("m", &[], 10).unwrap();
    for stale in [9, 1, 0] {
        let (status, message) = store_error(store.commit_scan("m", &[], stale));
        assert_eq!(status, 409, "{stale}");
        assert!(message.contains("stale") && message.contains("reset_progress"), "{message}");
    }
    assert!(!store.commit_scan("m", &[], 10).unwrap().progress_advanced, "equal is a replay");
    assert!(store.commit_scan("m", &[], 11).unwrap().progress_advanced);
    // Progress may jump; gaps are the detector's business.
    assert!(store.commit_scan("m", &[], 1_000_000).unwrap().progress_advanced);
    assert_eq!(store.progress("m").unwrap(), Some(1_000_000));
}

#[test]
fn progress_starts_at_any_bucket_including_zero() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert!(store.commit_scan("m", &[anomaly("", 0)], 0).unwrap().progress_advanced);
    assert_eq!(store.progress("m").unwrap(), Some(0));
    assert_eq!(store_error(store.commit_scan("m", &[anomaly("x", 0)], 1)).0, 409, "bucket 0 was already judged");
}

#[test]
fn reset_progress_clears_one_metric_and_keeps_anomalies() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan("m", &[anomaly("", 900)], 1_000).unwrap();
    store.commit_scan("other", &[], 5).unwrap();

    assert_eq!(store.reset_progress("m").unwrap(), Some(1_000));
    assert_eq!(store.progress("m").unwrap(), None);
    assert_eq!(store.progress("other").unwrap(), Some(5));
    assert_eq!(store.stats().unwrap().anomalies, 1);
    // Idempotent; unknown metrics are fine.
    assert_eq!(store.reset_progress("m").unwrap(), None);
    assert_eq!(store.reset_progress("never-seen").unwrap(), None);

    // The ledger-reset case: low buckets are accepted again.
    assert!(store.commit_scan("m", &[anomaly("", 2)], 3).unwrap().progress_advanced);
    assert_eq!(store.progress("m").unwrap(), Some(3));
}

// ---------------------------------------------------------------------------
// list_anomalies
// ---------------------------------------------------------------------------

/// Metric `a`: buckets 1..=3, subjects "" and "x". Metric `b`: bucket 2, "x".
fn seeded() -> (TempDir, Store) {
    let dir = TempDir::new();
    let store = open(&dir);
    for bucket in 1..=3 {
        store.commit_scan("a", &[anomaly("", bucket), anomaly("x", bucket)], bucket).unwrap();
    }
    store.commit_scan("b", &[anomaly("x", 2)], 2).unwrap();
    (dir, store)
}

fn keys(anomalies: &[Anomaly]) -> Vec<(String, String, u64)> {
    anomalies.iter().map(|a| (a.metric.clone(), a.subject.clone(), a.bucket)).collect()
}

fn key(metric: &str, subject: &str, bucket: u64) -> (String, String, u64) {
    (metric.into(), subject.into(), bucket)
}

#[test]
fn anomalies_are_listed_newest_bucket_first_then_newest_id() {
    let (_dir, store) = seeded();
    let all = store.list_anomalies(&AnomalyFilter::default()).unwrap();
    assert_eq!(
        keys(&all),
        [
            key("a", "x", 3),
            key("a", "", 3),
            key("b", "x", 2),
            key("a", "x", 2),
            key("a", "", 2),
            key("a", "x", 1),
            key("a", "", 1),
        ]
    );
    assert!(all.windows(2).all(|w| (w[0].bucket, w[0].id) > (w[1].bucket, w[1].id)));
}

#[test]
fn filters_combine() {
    let (_dir, store) = seeded();
    let list = |metric: Option<&str>, subject: Option<&str>, since: Option<u64>| {
        keys(
            &store
                .list_anomalies(&AnomalyFilter {
                    metric: metric.map(Into::into),
                    subject: subject.map(Into::into),
                    since_bucket: since,
                    ..AnomalyFilter::default()
                })
                .unwrap(),
        )
    };
    assert_eq!(list(Some("b"), None, None), [key("b", "x", 2)]);
    assert_eq!(list(Some("a"), Some(""), None), [key("a", "", 3), key("a", "", 2), key("a", "", 1)]);
    assert_eq!(list(None, Some("x"), Some(2)), [key("a", "x", 3), key("b", "x", 2), key("a", "x", 2)]);
    assert_eq!(list(Some("a"), Some("x"), Some(3)), [key("a", "x", 3)]);
    assert!(list(Some("nope"), None, None).is_empty());
    assert!(list(None, Some("nope"), None).is_empty());
}

#[test]
fn since_bucket_is_inclusive() {
    let (_dir, store) = seeded();
    let count = |since| store.list_anomalies(&AnomalyFilter { since_bucket: Some(since), ..Default::default() }).unwrap().len();
    assert_eq!(count(0), 7);
    assert_eq!(count(1), 7);
    assert_eq!(count(2), 5);
    assert_eq!(count(3), 2);
    assert_eq!(count(4), 0);
    assert_eq!(count(i64::MAX as u64), 0);
    assert_eq!(
        invalid_field(store.list_anomalies(&AnomalyFilter { since_bucket: Some(i64::MAX as u64 + 1), ..Default::default() })),
        "since_bucket"
    );
}

#[test]
fn list_limit_boundaries() {
    let dir = TempDir::new();
    let store = open(&dir);
    let rows: Vec<_> = (0..MAX_LIST_LIMIT + 5).map(|i| anomaly(&format!("s{i}"), 1)).collect();
    store.commit_scan("m", &rows, 1).unwrap();

    let list = |limit| store.list_anomalies(&AnomalyFilter { limit, ..Default::default() });
    assert_eq!(invalid_field(list(0)), "limit");
    assert_eq!(list(1).unwrap().len(), 1);
    assert_eq!(list(MAX_LIST_LIMIT - 1).unwrap().len(), MAX_LIST_LIMIT - 1);
    assert_eq!(list(MAX_LIST_LIMIT).unwrap().len(), MAX_LIST_LIMIT);
    assert_eq!(list(MAX_LIST_LIMIT + 1).unwrap().len(), MAX_LIST_LIMIT, "capped");
    assert_eq!(list(usize::MAX).unwrap().len(), MAX_LIST_LIMIT, "capped");
    assert_eq!(store.list_anomalies(&AnomalyFilter::default()).unwrap().len(), DEFAULT_LIST_LIMIT);
    // The cap keeps the newest: the last inserted ids.
    let newest = list(1).unwrap().remove(0);
    assert_eq!(newest.subject, format!("s{}", MAX_LIST_LIMIT + 4));
}

// ---------------------------------------------------------------------------
// Digests
// ---------------------------------------------------------------------------

#[test]
fn insert_digest_is_idempotent_per_range_and_keeps_the_first_body() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert!(store.insert_digest(100, 199, &json!({"v": 1})).unwrap());
    assert!(!store.insert_digest(100, 199, &json!({"v": 2})).unwrap());
    assert!(store.insert_digest(100, 200, &json!({"v": 3})).unwrap(), "a different range");
    assert!(store.insert_digest(5, 5, &json!({})).unwrap(), "a one-slot range");

    let digests = store.list_digests(10).unwrap();
    assert_eq!(digests.len(), 3);
    let first = digests.iter().find(|d| (d.start_slot, d.end_slot) == (100, 199)).unwrap();
    assert_eq!(first.body, json!({"v": 1}));
}

#[test]
fn digests_are_newest_first_by_insertion_not_by_slot() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(store.latest_digest().unwrap(), None);
    assert!(store.list_digests(10).unwrap().is_empty());

    store.insert_digest(1_000, 1_999, &json!({"n": "old ledger"})).unwrap();
    store.insert_digest(2_000, 2_999, &json!({"n": "old ledger, later"})).unwrap();
    // A ledger reset: the newest digest covers the lowest slots.
    store.insert_digest(0, 999, &json!({"n": "new ledger"})).unwrap();

    let before = unix_now_ms();
    assert_eq!(store.latest_digest().unwrap().unwrap().body, json!({"n": "new ledger"}));
    let ranges: Vec<_> = store.list_digests(10).unwrap().iter().map(|d| d.start_slot).collect();
    assert_eq!(ranges, [0, 2_000, 1_000]);
    assert!(store.latest_digest().unwrap().unwrap().created_at <= before);
}

#[test]
fn digest_validation_boundaries() {
    let dir = TempDir::new();
    let store = open(&dir);
    let max = i64::MAX as u64;
    assert!(store.insert_digest(max, max, &json!({})).unwrap());
    assert_eq!(invalid_field(store.insert_digest(max + 1, max + 1, &json!({}))), "start_slot");
    assert_eq!(invalid_field(store.insert_digest(0, max + 1, &json!({}))), "end_slot");
    assert_eq!(invalid_field(store.insert_digest(10, 9, &json!({}))), "end_slot");
    for bad in [json!(null), json!([]), json!("s"), json!(1)] {
        assert_eq!(invalid_field(store.insert_digest(1, 2, &bad)), "body", "{bad}");
    }

    let body_of = |bytes: usize| json!({ "k": "x".repeat(bytes - r#"{"k":""}"#.len()) });
    assert!(store.insert_digest(10, 10, &body_of(MAX_DIGEST_BYTES)).unwrap());
    assert_eq!(invalid_field(store.insert_digest(11, 11, &body_of(MAX_DIGEST_BYTES + 1))), "body");
    assert_eq!(store.stats().unwrap().digests, 2);
}

#[test]
fn digest_list_limit_boundaries() {
    let dir = TempDir::new();
    let store = open(&dir);
    for slot in 0..(MAX_LIST_LIMIT as u64 + 2) {
        store.insert_digest(slot, slot, &json!({})).unwrap();
    }
    assert_eq!(invalid_field(store.list_digests(0)), "limit");
    assert_eq!(store.list_digests(1).unwrap().len(), 1);
    assert_eq!(store.list_digests(MAX_LIST_LIMIT).unwrap().len(), MAX_LIST_LIMIT);
    assert_eq!(store.list_digests(MAX_LIST_LIMIT + 1).unwrap().len(), MAX_LIST_LIMIT);
}

// ---------------------------------------------------------------------------
// Pruning and stats
// ---------------------------------------------------------------------------

#[test]
fn prune_anomalies_is_strictly_before_the_cutoff() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan("m", &[anomaly("old", 1), anomaly("mid", 1), anomaly("new", 1)], 1).unwrap();
    set_detected_at(&dir, "old", 1_000);
    set_detected_at(&dir, "mid", 2_000);
    set_detected_at(&dir, "new", 3_000);

    assert_eq!(store.prune_anomalies_before(i64::MIN).unwrap(), 0);
    assert_eq!(store.prune_anomalies_before(1_000).unwrap(), 0, "equal to the cutoff is kept");
    assert_eq!(store.prune_anomalies_before(1_001).unwrap(), 1);
    assert_eq!(store.prune_anomalies_before(2_001).unwrap(), 1);
    assert_eq!(store.prune_anomalies_before(2_001).unwrap(), 0, "idempotent");
    let left: Vec<_> = store.list_anomalies(&AnomalyFilter::default()).unwrap().into_iter().map(|a| a.subject).collect();
    assert_eq!(left, ["new"]);
    // Progress is untouched.
    assert_eq!(store.progress("m").unwrap(), Some(1));
    assert_eq!(store.prune_anomalies_before(i64::MAX).unwrap(), 1);
}

#[test]
fn ids_are_never_reused_after_pruning_everything() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan("m", &[anomaly("a", 1)], 1).unwrap();
    let first = store.list_anomalies(&AnomalyFilter::default()).unwrap()[0].id;
    store.prune_anomalies_before(i64::MAX).unwrap();
    store.commit_scan("m", &[anomaly("b", 2)], 2).unwrap();
    assert!(store.list_anomalies(&AnomalyFilter::default()).unwrap()[0].id > first);
}

#[test]
fn prune_digests_is_strictly_before_the_cutoff() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.insert_digest(1, 1, &json!({})).unwrap();
    store.insert_digest(2, 2, &json!({})).unwrap();
    raw(&dir).execute("UPDATE digests SET created_at = start_slot * 1000", []).unwrap();

    assert_eq!(store.prune_digests_before(1_000).unwrap(), 0);
    assert_eq!(store.prune_digests_before(1_001).unwrap(), 1);
    assert_eq!(store.latest_digest().unwrap().unwrap().start_slot, 2);
    // A pruned range can be stored again.
    assert!(store.insert_digest(1, 1, &json!({})).unwrap());
}

#[test]
fn stats_count_every_table_and_span_the_anomalies() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(
        store.stats().unwrap(),
        StoreStats {
            schema_version: MIGRATIONS.len() as u32,
            anomalies: 0,
            detectors: 0,
            digests: 0,
            oldest_anomaly_at: None,
            newest_anomaly_at: None,
        }
    );

    store.commit_scan("a", &[anomaly("x", 1), anomaly("y", 1)], 1).unwrap();
    store.commit_scan("b", &[], 9).unwrap();
    store.insert_digest(0, 9, &json!({})).unwrap();
    set_detected_at(&dir, "x", 5_000);
    set_detected_at(&dir, "y", 7_000);

    assert_eq!(
        store.stats().unwrap(),
        StoreStats {
            schema_version: MIGRATIONS.len() as u32,
            anomalies: 2,
            detectors: 2,
            digests: 1,
            oldest_anomaly_at: Some(5_000),
            newest_anomaly_at: Some(7_000),
        }
    );
}

#[test]
fn row_types_serialise_for_the_api() {
    let (_dir, store) = seeded();
    let anomaly = &store.list_anomalies(&AnomalyFilter { limit: 1, ..Default::default() }).unwrap()[0];
    let value = serde_json::to_value(anomaly).unwrap();
    assert_eq!(value["kind"], "series");
    assert_eq!(value["metric"], "a");
    assert_eq!(value["detail"], serde_json::Value::Null);
    let stats = serde_json::to_value(store.stats().unwrap()).unwrap();
    assert_eq!(stats["anomalies"], 7);
}
