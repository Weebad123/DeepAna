//! A scan commit (a detector's anomalies plus its advanced progress) is one
//! all-or-nothing, idempotent, crash-safe unit.
//!
//! Each test measures both halves before and after, on a connection of its
//! own (the metric's stored progress, and its anomaly rows), and asserts
//! that they moved together or not at all:
//!
//! * **Atomicity.** An invalid row anywhere in the batch, a stale scan, a
//!   row for an already-judged bucket, or a replay that would add rows
//!   writes nothing, including the rows the transaction had already
//!   inserted.
//! * **Idempotency.** Committing the same scan again, any number of times,
//!   across reopens, changes nothing and says so in its [`ScanCommit`].
//! * **Crash safety.** A child process commits scans in a loop and is
//!   killed with SIGKILL at varying moments, many times over. After every
//!   kill each bucket up to the stored progress has all of its anomalies,
//!   none beyond it has any, and every commit the child reported is still
//!   there. The deterministic crash points (after the inserts, after the
//!   advance, after commit) are covered by the unit tests in `store/mod.rs`,
//!   which can reach the stages.
//! * **Concurrency.** Writers race on the same buckets while an independent
//!   reader checks, in every snapshot it takes, that rows and progress agree.

mod common;

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use common::TempDir;
use oracle::store::*;
use oracle::OracleResult;
use rusqlite::{Connection, OptionalExtension};
use serde_json::json;

const METRIC: &str = "m";

fn db_path(dir: &TempDir) -> std::path::PathBuf {
    dir.path().join("oracle.db")
}

fn open(dir: &TempDir) -> Store {
    Store::open_path(db_path(dir)).unwrap()
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
        detail: Some(json!({ "bucket": bucket })),
    }
}

/// `count` anomalies for `bucket`, subjects `s0..`.
fn scan(bucket: u64, count: usize) -> Vec<NewAnomaly> {
    (0..count).map(|i| anomaly(&format!("s{i}"), bucket)).collect()
}

/// Both halves of a metric's state, read in one snapshot on a connection of
/// the test's own.
#[derive(Debug, Clone, PartialEq, Eq)]
struct State {
    progress: Option<i64>,
    /// `(subject, bucket) -> score bits`, so a changed value is visible too.
    rows: BTreeMap<(String, i64), u64>,
}

fn state_of(conn: &Connection, metric: &str) -> State {
    let progress = conn
        .query_row("SELECT bucket FROM detector_progress WHERE metric = ?1", [metric], |r| r.get(0))
        .optional()
        .unwrap();
    let mut stmt = conn.prepare("SELECT subject, bucket, score FROM anomalies WHERE metric = ?1").unwrap();
    let rows = stmt
        .query_map([metric], |r| Ok(((r.get(0)?, r.get(1)?), r.get::<_, f64>(2)?.to_bits())))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    State { progress, rows }
}

fn state(dir: &TempDir) -> State {
    let mut conn = Connection::open(db_path(dir)).unwrap();
    let tx = conn.transaction().unwrap();
    let state = state_of(&tx, METRIC);
    tx.finish().unwrap();
    state
}

fn status<T: std::fmt::Debug>(result: OracleResult<T>) -> u16 {
    result.unwrap_err().as_store_error().expect("a StoreError source").http_status()
}

// ---------------------------------------------------------------------------
// Atomicity
// ---------------------------------------------------------------------------

#[test]
fn success_commits_both_halves_durably() {
    let dir = TempDir::new();
    let store = open(&dir);
    assert_eq!(state(&dir), State { progress: None, rows: BTreeMap::new() });

    let commit = store.commit_scan(METRIC, &scan(5, 3), 5).unwrap();
    assert_eq!(commit, ScanCommit { inserted: 3, duplicates: 0, progress_advanced: true });
    let after = state(&dir);
    assert_eq!(after.progress, Some(5));
    assert_eq!(after.rows.len(), 3);

    // Durable: a fresh process (fresh connection, fresh store) sees the same.
    drop(store);
    let _reopened = open(&dir);
    assert_eq!(state(&dir), after);
}

#[test]
fn a_scan_commits_with_one_timestamp() {
    // The unit is observable as one: every row of a scan shares the commit's
    // detected_at, and progress's updated_at is that same instant.
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(1, 50), 1).unwrap();
    let conn = Connection::open(db_path(&dir)).unwrap();
    let (distinct, at): (i64, i64) = conn
        .query_row("SELECT COUNT(DISTINCT detected_at), MAX(detected_at) FROM anomalies", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let updated: i64 = conn.query_row("SELECT updated_at FROM detector_progress", [], |r| r.get(0)).unwrap();
    assert_eq!((distinct, at), (1, updated));
}

#[test]
fn an_invalid_row_anywhere_in_the_batch_writes_nothing() {
    let invalid = [
        ("score", NewAnomaly { score: f64::NAN, ..anomaly("bad", 2) }),
        ("baseline", NewAnomaly { baseline: Some(f64::NAN), ..anomaly("bad", 2) }),
        ("value", NewAnomaly { value: f64::INFINITY, ..anomaly("bad", 2) }),
        ("scale", NewAnomaly { scale: Some(-1.0), ..anomaly("bad", 2) }),
        ("subject", NewAnomaly { subject: " bad".into(), ..anomaly("", 2) }),
        ("end_slot", NewAnomaly { start_slot: 9, end_slot: 8, ..anomaly("bad", 2) }),
        ("detail", NewAnomaly { detail: Some(json!([1])), ..anomaly("bad", 2) }),
        ("bucket", NewAnomaly { bucket: i64::MAX as u64 + 1, ..anomaly("bad", 2) }),
    ];
    for (field, bad) in invalid {
        for position in [0, 2, 4] {
            let dir = TempDir::new();
            let store = open(&dir);
            store.commit_scan(METRIC, &scan(1, 2), 1).unwrap();
            let before = state(&dir);

            let mut rows = scan(2, 4);
            rows.insert(position, bad.clone());
            assert_eq!(status(store.commit_scan(METRIC, &rows, 2)), 400, "{field} at {position}");
            assert_eq!(state(&dir), before, "{field} at {position}: nothing may be written");

            // Bucket 2 was not skipped: the corrected batch commits in full.
            rows.remove(position);
            assert_eq!(store.commit_scan(METRIC, &rows, 2).unwrap().inserted, 4);
            assert_eq!(state(&dir).progress, Some(2));
        }
    }
}

#[test]
fn a_stale_scan_writes_none_of_its_new_rows() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(10, 2), 10).unwrap();
    let before = state(&dir);

    // Valid, brand-new rows; the scan is simply behind.
    for stale in [9, 5, 0] {
        assert_eq!(status(store.commit_scan(METRIC, &[anomaly("fresh", stale)], stale)), 409, "{stale}");
        assert_eq!(state(&dir), before, "{stale}");
    }
}

#[test]
fn an_advancing_scan_with_a_row_for_an_already_judged_bucket_writes_nothing() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(10, 1), 10).unwrap();
    let before = state(&dir);

    // Rows for 11 and 12 are fine; the one for 10 belongs to the last scan.
    let rows = [anomaly("a", 11), anomaly("b", 12), anomaly("late", 10)];
    assert_eq!(status(store.commit_scan(METRIC, &rows, 12)), 409);
    assert_eq!(state(&dir), before, "neither the rows for 11 and 12 nor the advance may land");
}

#[test]
fn a_replay_that_would_add_rows_rolls_back_the_rows_it_inserted() {
    // Progress is 5. A commit through 5 is a replay; this one carries one
    // stored row plus two new ones. The new rows are inserted inside the
    // transaction before the replay check fails, so this proves they are
    // rolled back rather than never written.
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(5, 1), 5).unwrap();
    let before = state(&dir);

    let rows = [anomaly("s0", 5), anomaly("new1", 5), anomaly("new2", 4)];
    assert_eq!(status(store.commit_scan(METRIC, &rows, 5)), 409);
    assert_eq!(state(&dir), before);
}

#[test]
fn a_failed_commit_on_one_metric_leaves_the_others_alone() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan("other", &scan(1, 3), 1).unwrap();
    store.commit_scan(METRIC, &scan(1, 1), 1).unwrap();
    let conn = Connection::open(db_path(&dir)).unwrap();
    let other_before = state_of(&conn, "other");

    assert_eq!(status(store.commit_scan(METRIC, &[NewAnomaly { score: f64::NAN, ..anomaly("x", 2) }], 2)), 400);
    assert_eq!(status(store.commit_scan(METRIC, &[], 0)), 409);
    assert_eq!(state_of(&conn, "other"), other_before);
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

#[test]
fn committing_the_same_scan_again_changes_nothing() {
    let dir = TempDir::new();
    let store = open(&dir);
    let rows = scan(7, 5);
    assert_eq!(store.commit_scan(METRIC, &rows, 7).unwrap().inserted, 5);
    let after_first = state(&dir);

    for _ in 0..5 {
        let replay = store.commit_scan(METRIC, &rows, 7).unwrap();
        assert_eq!(replay, ScanCommit { inserted: 0, duplicates: 5, progress_advanced: false });
        assert_eq!(state(&dir), after_first);
    }
}

#[test]
fn a_retry_after_a_lost_response_is_safe_across_a_restart() {
    // The commit landed, but the detector never heard (it crashed, or the
    // call timed out). After a restart it retries the same scan.
    let dir = TempDir::new();
    let rows = scan(3, 4);
    {
        let store = open(&dir);
        let _lost = store.commit_scan(METRIC, &rows, 3).unwrap();
    }
    let after = state(&dir);

    let store = open(&dir);
    let retry = store.commit_scan(METRIC, &rows, 3).unwrap();
    assert_eq!(retry, ScanCommit { inserted: 0, duplicates: 4, progress_advanced: false });
    assert_eq!(state(&dir), after);
}

#[test]
fn a_replay_with_different_values_keeps_the_first_rows() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(2, 2), 2).unwrap();
    let before = state(&dir);

    let rescored: Vec<_> = scan(2, 2).into_iter().map(|a| NewAnomaly { score: 99.0, ..a }).collect();
    let replay = store.commit_scan(METRIC, &rescored, 2).unwrap();
    assert_eq!(replay, ScanCommit { inserted: 0, duplicates: 2, progress_advanced: false });
    assert_eq!(state(&dir), before, "first write wins");
}

#[test]
fn a_partial_replay_is_accepted() {
    // A replay need not repeat every row; it may only not add any.
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(2, 3), 2).unwrap();
    let before = state(&dir);
    assert_eq!(store.commit_scan(METRIC, &scan(2, 1), 2).unwrap().duplicates, 1);
    assert_eq!(store.commit_scan(METRIC, &[], 2).unwrap(), ScanCommit { inserted: 0, duplicates: 0, progress_advanced: false });
    assert_eq!(state(&dir), before);
}

#[test]
fn a_replay_after_its_rows_were_pruned_does_not_resurrect_them() {
    let dir = TempDir::new();
    let store = open(&dir);
    store.commit_scan(METRIC, &scan(2, 2), 2).unwrap();
    store.prune_anomalies_before(i64::MAX).unwrap();
    let before = state(&dir);
    assert_eq!(status(store.commit_scan(METRIC, &scan(2, 2), 2)), 409);
    assert_eq!(state(&dir), before);
}

#[test]
fn rescanning_after_reset_progress_counts_the_kept_rows_as_duplicates() {
    let dir = TempDir::new();
    let store = open(&dir);
    for bucket in 1..=3 {
        store.commit_scan(METRIC, &scan(bucket, 2), bucket).unwrap();
    }
    let rows_before = state(&dir).rows;
    assert_eq!(store.reset_progress(METRIC).unwrap(), Some(3));

    // The new scan covers buckets 1..=3 again: kept rows are duplicates,
    // one new finding is stored, and progress is re-established.
    let mut rows: Vec<_> = (1..=3).flat_map(|bucket| scan(bucket, 2)).collect();
    rows.push(anomaly("new", 3));
    let commit = store.commit_scan(METRIC, &rows, 3).unwrap();
    assert_eq!(commit, ScanCommit { inserted: 1, duplicates: 6, progress_advanced: true });
    let after = state(&dir);
    assert_eq!(after.progress, Some(3));
    assert_eq!(after.rows.len(), rows_before.len() + 1);
}

// ---------------------------------------------------------------------------
// Crash safety: SIGKILL at arbitrary moments
// ---------------------------------------------------------------------------

const STORM_DIR_VAR: &str = "ORACLE_STORE_STORM_DIR";
const STORM_METRIC: &str = "storm";
/// Anomalies per scan, each with a padded detail, so a commit has real work
/// in it for a kill to interrupt.
const STORM_ROWS: usize = 60;

fn storm_scan(bucket: u64) -> Vec<NewAnomaly> {
    (0..STORM_ROWS)
        .map(|i| NewAnomaly {
            detail: Some(json!({ "pad": "x".repeat(400), "i": i })),
            ..anomaly(&format!("s{i}"), bucket)
        })
        .collect()
}

/// Child half of [`sigkill_at_arbitrary_moments_never_tears_a_scan`]: runs
/// only when re-invoked with [`STORM_DIR_VAR`] set. Commits one scan per
/// bucket, resuming after the stored progress, and reports each commit on
/// stdout only after `commit_scan` returned, until it is killed.
#[test]
fn storm_child() {
    let Some(dir) = std::env::var_os(STORM_DIR_VAR) else {
        return;
    };
    let store = Store::open_path(std::path::Path::new(&dir).join("oracle.db")).unwrap();
    let first = store.progress(STORM_METRIC).unwrap().map_or(1, |p| p + 1);
    for bucket in first..first + 1_000_000 {
        store.commit_scan(STORM_METRIC, &storm_scan(bucket), bucket).unwrap();
        println!("committed {bucket}");
    }
}

fn reported(line: &str) -> Option<u64> {
    line.trim().strip_prefix("committed ")?.parse().ok()
}

/// Every bucket up to the stored progress holds exactly one scan's rows, all
/// stamped by one commit; no bucket beyond it holds any. Returns the
/// progress.
fn assert_storm_invariant(path: &std::path::Path, round: usize) -> u64 {
    let conn = Connection::open(path).unwrap();
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(integrity, "ok", "round {round}");

    let progress: Option<i64> = conn
        .query_row("SELECT bucket FROM detector_progress WHERE metric = ?1", [STORM_METRIC], |r| r.get(0))
        .optional()
        .unwrap();
    let progress = progress.unwrap_or(0);
    let mut stmt = conn
        .prepare(
            "SELECT bucket, COUNT(*), COUNT(DISTINCT detected_at) FROM anomalies WHERE metric = ?1
             GROUP BY bucket ORDER BY bucket",
        )
        .unwrap();
    let buckets: Vec<(i64, i64, i64)> = stmt
        .query_map([STORM_METRIC], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect();

    assert_eq!(buckets.len() as i64, progress, "round {round}: one group of rows per evaluated bucket");
    for (index, (bucket, rows, stamps)) in buckets.into_iter().enumerate() {
        assert_eq!(bucket, index as i64 + 1, "round {round}: no bucket missing or beyond progress");
        assert_eq!(rows, STORM_ROWS as i64, "round {round}: bucket {bucket} is torn");
        assert_eq!(stamps, 1, "round {round}: bucket {bucket} mixes commits");
    }
    progress as u64
}

#[test]
fn sigkill_at_arbitrary_moments_never_tears_a_scan() {
    const ROUNDS: usize = 12;

    let dir = TempDir::new();
    drop(open(&dir));
    let mut progress = 0;
    let mut total_reported = 0;

    for round in 0..ROUNDS {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "storm_child", "--test-threads=1", "--nocapture"])
            .env(STORM_DIR_VAR, dir.path())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();

        // Let it get going, then kill it after a different delay each round
        // so the kill lands at different points of a commit.
        let mut last_reported = loop {
            let line = lines.next().expect("the child exited before its first commit").unwrap();
            if let Some(bucket) = reported(&line) {
                break bucket;
            }
        };
        thread::sleep(Duration::from_micros(500 + 1_700 * round as u64));
        child.kill().unwrap();
        child.wait().unwrap();
        for line in lines {
            if let Some(bucket) = reported(&line.unwrap()) {
                last_reported = bucket;
            }
        }
        total_reported += 1;

        let now = assert_storm_invariant(&db_path(&dir), round);
        assert!(now >= last_reported, "round {round}: reported commit {last_reported} lost (progress {now})");
        assert!(now > progress, "round {round}: no progress made");
        progress = now;
    }

    // The store opens normally afterwards and resumes where the child stopped.
    let store = open(&dir);
    assert_eq!(store.progress(STORM_METRIC).unwrap(), Some(progress));
    assert!(store.commit_scan(STORM_METRIC, &storm_scan(progress + 1), progress + 1).unwrap().progress_advanced);
    assert_eq!(total_reported, ROUNDS);
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

/// Many writers race to commit the same buckets (identical rows per bucket)
/// while a reader on its own connection samples the state.
///
/// For each bucket exactly one commit may advance progress; a racer that
/// arrives at the same bucket is a replay (all duplicates), one that arrives
/// later is stale (409). In **every** committed state the metric holds
/// exactly `ROWS * progress` rows and the highest bucket equals progress;
/// a torn commit would break that in some snapshot.
#[test]
fn racing_writers_and_an_observer_never_see_half_a_scan() {
    const WRITERS: usize = 6;
    const BUCKETS: u64 = 80;
    const ROWS: usize = 4;

    let dir = TempDir::new();
    let store = open(&dir);
    let advanced = Arc::new(AtomicUsize::new(0));
    let replays = Arc::new(AtomicUsize::new(0));
    let stale = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(WRITERS + 1));

    let reader = {
        let (path, done, start) = (db_path(&dir), done.clone(), start.clone());
        thread::spawn(move || {
            let mut conn = Connection::open(path).unwrap();
            conn.busy_timeout(Duration::from_secs(5)).unwrap();
            start.wait();
            let mut snapshots = 0u64;
            while !done.load(Ordering::Acquire) {
                let tx = conn.transaction().unwrap();
                let progress: Option<i64> = tx
                    .query_row("SELECT bucket FROM detector_progress WHERE metric = ?1", [METRIC], |r| r.get(0))
                    .optional()
                    .unwrap();
                let (rows, top): (i64, Option<i64>) = tx
                    .query_row("SELECT COUNT(*), MAX(bucket) FROM anomalies WHERE metric = ?1", [METRIC], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .unwrap();
                tx.finish().unwrap();
                assert_eq!(rows, ROWS as i64 * progress.unwrap_or(0), "torn after {snapshots} snapshots");
                assert_eq!(top, progress, "torn after {snapshots} snapshots");
                snapshots += 1;
            }
            snapshots
        })
    };

    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let (store, advanced, replays, stale, start) =
                (store.clone(), advanced.clone(), replays.clone(), stale.clone(), start.clone());
            thread::spawn(move || {
                start.wait();
                for bucket in 1..=BUCKETS {
                    match store.commit_scan(METRIC, &scan(bucket, ROWS), bucket) {
                        Ok(commit) if commit.progress_advanced => {
                            assert_eq!(commit.inserted, ROWS);
                            advanced.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(commit) => {
                            assert_eq!(commit, ScanCommit { inserted: 0, duplicates: ROWS, progress_advanced: false });
                            replays.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            assert_eq!(err.as_store_error().unwrap().http_status(), 409, "{err}");
                            stale.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();
    for writer in writers {
        writer.join().unwrap();
    }
    done.store(true, Ordering::Release);
    let snapshots = reader.join().unwrap();
    assert!(snapshots > 0, "the reader must have observed something");

    let (advanced, replays, stale) =
        (advanced.load(Ordering::Relaxed), replays.load(Ordering::Relaxed), stale.load(Ordering::Relaxed));
    assert_eq!(advanced as u64, BUCKETS, "each bucket advanced exactly once");
    assert_eq!(advanced + replays + stale, WRITERS * BUCKETS as usize, "every attempt accounted for");
    let final_state = state(&dir);
    assert_eq!(final_state.progress, Some(BUCKETS as i64));
    assert_eq!(final_state.rows.len(), ROWS * BUCKETS as usize);
}

#[test]
fn metrics_committed_concurrently_do_not_interfere() {
    const METRICS: usize = 4;
    const BUCKETS: u64 = 40;

    let dir = TempDir::new();
    let store = open(&dir);
    let handles: Vec<_> = (0..METRICS)
        .map(|m| {
            let store = store.clone();
            thread::spawn(move || {
                let metric = format!("metric-{m}");
                for bucket in 1..=BUCKETS {
                    let commit = store.commit_scan(&metric, &scan(bucket, m + 1), bucket).unwrap();
                    assert!(commit.progress_advanced);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().unwrap();
    }

    let conn = Connection::open(db_path(&dir)).unwrap();
    for m in 0..METRICS {
        let state = state_of(&conn, &format!("metric-{m}"));
        assert_eq!(state.progress, Some(BUCKETS as i64));
        assert_eq!(state.rows.len(), (m + 1) * BUCKETS as usize);
    }
}
