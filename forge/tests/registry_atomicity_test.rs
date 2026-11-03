//! "Advance an alert's progress + insert its outbox delivery" is one
//! all-or-nothing unit.
//!
//! Each test measures both halves before and after (the alert's
//! `last_evaluated_slot` and the `deliveries` row count) and asserts that
//! they moved together or not at all. That covers every way the unit can
//! fail: the delivery insert fails, the advance fails, the closure errors,
//! the closure panics, the process dies before commit, and many writers race
//! while an independent reader watches.
//!
//! The delivery half is the real `Outbox::enqueue`, in the same
//! transaction, the way the evaluator runs it.

mod common;

use std::process::Command;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use common::TempDir;
use forge::{Enqueued, ForgeError, NewAlert, NewDelivery, Outbox, Registry, RegistryError};
use rusqlite::{Connection, OptionalExtension, Transaction};

fn open(dir: &TempDir) -> Registry {
    Registry::open_path(dir.path().join("forge.db")).unwrap()
}

fn create_alert(registry: &Registry, name: &str) -> String {
    registry
        .create_alert(NewAlert {
            name: name.into(),
            sql: "SELECT * FROM t WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}".into(),
            webhook_url: "https://hooks.example.com/x".into(),
            secret: "0123456789abcdef".into(),
            wait_for_final: false,
        })
        .unwrap()
        .id
}

fn delivery(alert_id: &str, slot: u64) -> NewDelivery {
    NewDelivery {
        alert_id: alert_id.into(),
        to_slot: slot,
        payload: format!(r#"{{"to_slot":{slot}}}"#),
        release_after_slot: None,
    }
}

/// One pending row for the fired range.
fn enqueue(tx: &Transaction<'_>, alert_id: &str, slot: u64) -> Result<Enqueued, RegistryError> {
    Outbox::enqueue(tx, &delivery(alert_id, slot), 0)
}

/// The unit under test, as the evaluator runs it.
fn fire(registry: &Registry, alert_id: &str, to_slot: u64) -> Result<(), ForgeError> {
    registry.db().transaction("fire alert", |tx| {
        Registry::advance_progress(tx, alert_id, to_slot)?;
        enqueue(tx, alert_id, to_slot)?;
        Ok(())
    })
}

/// Both halves of the state, read in one snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct State {
    slot: Option<i64>,
    deliveries: i64,
}

fn state_of(conn: &Connection, alert_id: &str) -> State {
    let slot = conn
        .query_row("SELECT last_evaluated_slot FROM alerts WHERE id = ?1", [alert_id], |r| r.get(0))
        .optional()
        .unwrap()
        .flatten();
    let deliveries = conn
        .query_row("SELECT COUNT(*) FROM deliveries WHERE alert_id = ?1", [alert_id], |r| r.get(0))
        .unwrap();
    State { slot, deliveries }
}

fn state(registry: &Registry, alert_id: &str) -> State {
    registry.db().with_conn("state", |conn| Ok(state_of(conn, alert_id))).unwrap()
}

fn status(err: ForgeError) -> u16 {
    err.as_registry_error().expect("a RegistryError source").http_status()
}

#[test]
fn success_commits_both_halves_durably() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    let before = state(&registry, &id);
    assert_eq!(before, State { slot: None, deliveries: 0 });

    fire(&registry, &id, 10).unwrap();
    let after = State { slot: Some(10), deliveries: 1 };
    assert_eq!(state(&registry, &id), after);

    // Durable: a fresh process (fresh connection) sees the same.
    drop(registry);
    assert_eq!(state(&open(&dir), &id), after);
}

#[test]
fn a_failed_delivery_insert_rolls_back_the_advance() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    fire(&registry, &id, 10).unwrap();
    let before = state(&registry, &id);

    // The advance to 20 succeeds inside the transaction, then the insert
    // rejects the payload.
    let err = registry
        .db()
        .transaction("fire", |tx| {
            Registry::advance_progress(tx, &id, 20)?;
            Outbox::enqueue(tx, &NewDelivery { payload: "not json".into(), ..delivery(&id, 20) }, 0)
        })
        .unwrap_err();
    assert_eq!(status(err), 400);
    assert_eq!(state(&registry, &id), before, "advance must not survive a failed insert");

    // And slot 20 can still be fired: nothing was skipped.
    fire(&registry, &id, 20).unwrap();
    assert_eq!(state(&registry, &id), State { slot: Some(20), deliveries: 2 });
}

#[test]
fn a_delivery_for_a_vanished_alert_rolls_back() {
    // Foreign keys are enforced: the outbox row cannot point at nothing.
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    let before = state(&registry, &id);
    let err = registry
        .db()
        .transaction("fire", |tx| {
            Registry::advance_progress(tx, &id, 5)?;
            enqueue(tx, "no-such-alert", 5)
        })
        .unwrap_err();
    assert_eq!(status(err), 500);
    assert_eq!(state(&registry, &id), before);
}

#[test]
fn a_stale_advance_rolls_back_an_already_inserted_delivery() {
    // Insert first, then advance. A stale slot fails the advance, and the
    // delivery written earlier in the same transaction must go with it.
    // (Slot 10 already has its row, so that enqueue is a no-op duplicate.)
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    fire(&registry, &id, 10).unwrap();
    let before = state(&registry, &id);

    for stale in [10u64, 9, 0] {
        let err = registry
            .db()
            .transaction("fire", |tx| {
                enqueue(tx, &id, stale)?;
                Registry::advance_progress(tx, &id, stale)
            })
            .unwrap_err();
        assert_eq!(status(err), 409, "{stale}");
        assert_eq!(state(&registry, &id), before, "{stale}");
    }
}

#[test]
fn an_error_after_both_writes_rolls_back_both() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    let before = state(&registry, &id);
    let err = registry
        .db()
        .transaction("fire", |tx| {
            Registry::advance_progress(tx, &id, 7)?;
            enqueue(tx, &id, 7)?;
            // Both writes are visible inside the transaction...
            assert_eq!(state_of(tx, &id), State { slot: Some(7), deliveries: 1 });
            Err::<(), _>(RegistryError::storage("signer unavailable"))
        })
        .unwrap_err();
    assert_eq!(status(err), 500);
    // ...and gone after it.
    assert_eq!(state(&registry, &id), before);
}

#[test]
fn a_panic_after_both_writes_rolls_back_both_and_the_db_stays_usable() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = create_alert(&registry, "a");
    let before = state(&registry, &id);

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = registry.db().transaction("fire", |tx| {
            Registry::advance_progress(tx, &id, 7)?;
            enqueue(tx, &id, 7)?;
            panic!("evaluator bug");
            #[allow(unreachable_code)]
            Ok(())
        });
    }));
    assert!(result.is_err());
    assert_eq!(state(&registry, &id), before);

    // The poisoned mutex is recovered and the connection is not stuck in
    // a transaction.
    fire(&registry, &id, 7).unwrap();
    assert_eq!(state(&registry, &id), State { slot: Some(7), deliveries: 1 });
}

// ---------------------------------------------------------------------------
// Crash before commit
// ---------------------------------------------------------------------------

const CRASH_DIR_VAR: &str = "FORGE_ATOMICITY_CRASH_DIR";

/// Child half of [`a_process_crash_before_commit_leaves_neither_half`]: runs
/// only when re-invoked with [`CRASH_DIR_VAR`] set. It writes both halves and
/// aborts the process before commit. No destructor runs, so no rollback
/// either; only SQLite's recovery stands between the half-written
/// transaction and the next open.
#[test]
fn crash_child() {
    let Some(dir) = std::env::var_os(CRASH_DIR_VAR) else {
        return;
    };
    let registry = Registry::open_path(std::path::Path::new(&dir).join("forge.db")).unwrap();
    let id = registry.list_alerts().unwrap().remove(0).id;
    let _ = registry.db().transaction("fire", |tx| {
        Registry::advance_progress(tx, &id, 99)?;
        enqueue(tx, &id, 99)?;
        std::process::abort();
        #[allow(unreachable_code)]
        Ok(())
    });
}

#[test]
fn a_process_crash_before_commit_leaves_neither_half() {
    let dir = TempDir::new();
    let id = {
        let registry = open(&dir);
        let id = create_alert(&registry, "a");
        fire(&registry, &id, 10).unwrap();
        id
    };
    let before = State { slot: Some(10), deliveries: 1 };

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--test-threads=1", "--nocapture"])
        .env(CRASH_DIR_VAR, dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success(), "the child must have aborted: {output:?}");

    // The lock died with the child; reopening runs WAL recovery.
    let registry = open(&dir);
    assert_eq!(state(&registry, &id), before);
    fire(&registry, &id, 99).unwrap();
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

/// Many writers race to fire the same slots while a reader on its own
/// connection samples the state.
///
/// Every transaction sets `last_evaluated_slot = k` and inserts exactly one
/// delivery whose payload says `k`. So in **every** committed state:
///
/// * the number of deliveries equals the number of successful fires,
/// * the highest delivered slot equals `last_evaluated_slot`,
/// * no slot is delivered twice.
///
/// The reader checks the second rule on every snapshot it takes; a torn
/// transaction (progress without its row, or the reverse) would break it.
#[test]
fn concurrent_fires_and_an_observer_never_see_half_a_unit() {
    const WRITERS: usize = 8;
    const SLOTS: u64 = 200;

    let dir = TempDir::new();
    let registry = Arc::new(open(&dir));
    let id = Arc::new(create_alert(&registry, "a"));
    let committed = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(WRITERS + 1));

    let reader = {
        let (path, id, done, start) = (registry.path().to_path_buf(), id.clone(), done.clone(), start.clone());
        thread::spawn(move || {
            let mut conn = Connection::open(path).unwrap();
            conn.busy_timeout(std::time::Duration::from_secs(5)).unwrap();
            start.wait();
            let mut snapshots = 0u64;
            while !done.load(Ordering::Acquire) {
                let tx = conn.transaction().unwrap();
                let slot: Option<i64> = tx
                    .query_row("SELECT last_evaluated_slot FROM alerts WHERE id = ?1", [&*id], |r| r.get(0))
                    .unwrap();
                let top: Option<i64> = tx
                    .query_row(
                        "SELECT MAX(json_extract(payload, '$.to_slot')) FROM deliveries WHERE alert_id = ?1",
                        [&*id],
                        |r| r.get(0),
                    )
                    .unwrap();
                tx.finish().unwrap();
                assert_eq!(slot, top, "torn state observed after {snapshots} snapshots");
                snapshots += 1;
            }
            snapshots
        })
    };

    let writers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let (registry, id, committed, start) = (registry.clone(), id.clone(), committed.clone(), start.clone());
            thread::spawn(move || {
                start.wait();
                // Every writer tries every slot, so each slot is contested.
                for slot in 1..=SLOTS {
                    match fire(&registry, &id, slot) {
                        Ok(()) => {
                            committed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => assert_eq!(status(err), 409, "only a lost race may fail"),
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

    let final_state = state(&registry, &id);
    let committed = committed.load(Ordering::Relaxed) as i64;
    assert_eq!(final_state.deliveries, committed, "one delivery per committed fire");
    assert_eq!(final_state.slot, Some(SLOTS as i64));
    assert!(committed <= SLOTS as i64, "a slot can be fired at most once; got {committed}");

    let distinct: i64 = registry
        .db()
        .with_conn("distinct", |conn| {
            Ok(conn.query_row(
                "SELECT COUNT(DISTINCT json_extract(payload, '$.to_slot')) FROM deliveries WHERE alert_id = ?1",
                [&*id],
                |r| r.get(0),
            )?)
        })
        .unwrap();
    assert_eq!(distinct, committed, "no slot delivered twice");
}

#[test]
fn deleting_an_alert_cascades_to_its_deliveries() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let keep = create_alert(&registry, "keep");
    let drop_id = create_alert(&registry, "drop");
    fire(&registry, &keep, 1).unwrap();
    fire(&registry, &drop_id, 1).unwrap();
    fire(&registry, &drop_id, 2).unwrap();

    registry.delete_alert(&drop_id).unwrap();
    assert_eq!(state(&registry, &drop_id), State { slot: None, deliveries: 0 });
    assert_eq!(state(&registry, &keep), State { slot: Some(1), deliveries: 1 });
}
