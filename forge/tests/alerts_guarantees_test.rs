//! The alert engine's guarantee, measured: **every matching row in an
//! alert's range appears in exactly one delivery, and nothing is skipped,
//! repeated, or evaluated out of range**, whatever happens on the way.
//!
//! | Test                         | What happens on the way                                                   |
//! |------------------------------|---------------------------------------------------------------------------|
//! | `through_replays_...`        | the real loop; LENS lags the gate; PRISM re-announces slots; the change log is replaced and replayed from the start; LENS is unavailable, times out, answers garbage, has not registered a table; results truncate and overflow the payload budget |
//! | `shutdown_at_every_call`     | shutdown fires at each LENS call of the scenario in turn, then a restart  |
//! | `crash_at_every_call`        | the process dies at each LENS call of the scenario in turn (a child process exits there, no cleanup), then a restart |
//!
//! Every LENS call is followed by a commit or a re-query, so "each call in
//! turn" puts the stop between every pair of steps the engine takes. The
//! scenario (`scenario_files`, `scenario_lens`) has duplicates, a skip,
//! finals, an unknown event, a LENS failure every few calls, truncation and
//! payload-budget splits.
//!
//! "Measured" means two independent checks, both of which must be clean:
//!
//! * the outbox, read straight from SQLite after the run: each delivery's
//!   rows inside its own range, ranges disjoint and inside the alert's,
//!   every expected row present exactly once and no other
//!   ([`alerts_support::exactly_once`]);
//! * every query, as the fake LENS received it, checked at that moment
//!   against the alert's stored progress (the range starts right after
//!   it), the highest announced slot and the highest visible slot (the
//!   range ends at or below both). A crash child logs its checks to a
//!   file the parent reads.

mod alerts_support;
mod common;
mod webhook_support;

use std::collections::HashSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use alerts_support::*;
use forge::{EngineSettings, LensCallError, Pass};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use webhook_support::{Receiver, Reply};

/// Every alert starts evaluated through `BASE`.
const BASE: u64 = 999;
/// The crash and shutdown scenarios end here.
const END: u64 = 1079;
/// `(table, dataset seed, wait_for_final)`, one alert each.
const ALERTS: [(&str, u64, bool); 3] =
    [("token_transfers", 1, false), ("transactions", 2, false), ("instructions", 3, true)];

/// Small batches, chunks, row cap and payload budget, so a short scenario
/// crosses every boundary many times.
fn scenario_settings() -> EngineSettings {
    EngineSettings {
        batch_size: 7,
        max_range_slots: 16,
        payload_budget_bytes: 1_100,
        missing_table_retries: 2,
        ..engine_settings()
    }
}

/// The fake LENS of the crash and shutdown scenarios: everything announced
/// is visible, and so are 15 slots past it (PRISM writes `slots` first, so
/// LENS can see a flush before it is announced); rows capped at 12; a LENS
/// failure every few calls.
fn scenario_lens(lens: &FakeLens, end: u64) {
    for (table, seed, _) in ALERTS {
        lens.table(table, dataset(BASE + 1..=end + 15, seed));
    }
    lens.max_rows(12);
    lens.show(end + 15);
    lens.announce(end);
    lens.fault(|q| match q.call % 13 {
        4 => Some(LensCallError::Unavailable { message: "connection reset".into() }),
        9 => Some(LensCallError::Timeout { message: "504".into() }),
        _ => None,
    });
}

/// The alerts, at `BASE`, and the whole change log of the crash and
/// shutdown scenarios: duplicates, a skip, finals and an unknown event.
fn scenario_files(h: &mut Harness) -> Vec<String> {
    let ids = ALERTS
        .iter()
        .map(|(table, _, wait_for_final)| {
            let id = h.alert(&format!("on-{table}"), table, *wait_for_final);
            h.set_progress(&id, BASE);
            id
        })
        .collect();
    let log = &mut h.changelog;
    (1000..=1039).for_each(|slot| log.new_slot(slot));
    (1020..=1029).for_each(|slot| log.new_slot(slot));
    log.finalized(1019);
    log.skip(1040);
    (1041..=1059).for_each(|slot| log.new_slot(slot));
    log.finalized(1049);
    log.unknown();
    (1060..=END).for_each(|slot| log.new_slot(slot));
    log.finalized(END);
    ids
}

/// Run the real loop until every alert is at `end`.
async fn run_to(h: &Harness, ids: &[String], end: u64, settings: EngineSettings) {
    let engine = h.engine(settings);
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });
    wait_for(&format!("every alert to reach {end}"), 60, || ids.iter().all(|id| h.progress(id) == Some(end))).await;
    cancel.cancel();
    task.await.unwrap();
    assert!(engine.status().failing_alerts.is_empty(), "{:?}", engine.status());
}

/// Both checks, for every alert; panics listing every breach.
fn assert_exactly_once(h: &Harness, ids: &[String], end: u64, context: &str) {
    let mut breaches = h.lens.violations();
    for (id, (table, seed, _)) in ids.iter().zip(ALERTS) {
        let data = dataset(BASE + 1..=end, seed);
        breaches.extend(
            exactly_once(&h.deliveries(id), &data, BASE + 1, end)
                .into_iter()
                .map(|breach| format!("{table}: {breach}")),
        );
    }
    assert!(breaches.is_empty(), "{context}: {} breaches:\n{}", breaches.len(), breaches.join("\n"));
}

async fn receiver() -> Receiver {
    Receiver::start(|_| Reply::Status(200)).await
}

// ---------------------------------------------------------------------------
// Replays, duplicates and LENS failures, through the real loop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn through_replays_duplicates_and_lens_failures_every_row_is_delivered_exactly_once() {
    const LAST: u64 = 1299;
    let rx = receiver().await;
    let mut h = Harness::new(&rx.url("/hook"));
    for (table, seed, _) in ALERTS {
        h.lens.table(table, dataset(BASE + 1..=LAST, seed));
    }
    let ids: Vec<String> = ALERTS
        .iter()
        .map(|(table, _, wait_for_final)| {
            let id = h.alert(&format!("on-{table}"), table, *wait_for_final);
            h.set_progress(&id, BASE);
            id
        })
        .collect();

    h.lens.max_rows(12);
    // `transactions` is not registered until stage 3.
    h.lens.unregister("transactions");
    let mut queries = 0usize;
    h.lens.fault(move |q| {
        queries += 1;
        match (queries, q.call % 11, q.call % 17) {
            (20, ..) => Some(LensCallError::Protocol { message: "row_count 3 but 2 rows".into() }),
            (_, 3, _) => Some(LensCallError::Unavailable { message: "connection refused".into() }),
            (_, _, 8) => Some(LensCallError::Timeout { message: "504 Gateway Timeout".into() }),
            _ => None,
        }
    });

    let engine = h.engine(scenario_settings());
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });

    // 1. LENS lags: the first gates time out before it catches up.
    h.publish(1000..=1049);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(h.lens.queries().is_empty(), "nothing evaluated before LENS saw it");
    h.lens.show(1049);
    wait_for("stage 1 (transfers, instructions)", 30, || behind(&h, &ids, 1049) <= 1).await;

    // 2. PRISM re-announces after a restart, then moves on.
    h.publish(1020..=1049);
    h.publish(1050..=1099);
    h.lens.show(1099);
    h.changelog.finalized(1080);

    // 3. The table appears; then the change log is replaced and its new
    //    contents start with slots already evaluated.
    tokio::time::sleep(Duration::from_millis(150)).await;
    h.lens.register("transactions");
    h.changelog.replace();
    h.publish(1000..=1010);
    h.publish(1100..=1199);
    h.lens.show(1199);
    h.changelog.finalized(1150);

    // 4. LENS sees further than anything announced (PRISM writes `slots`
    //    first). Stage 3 is drained first, so the engine meets this state.
    wait_for("stage 3", 30, || behind(&h, &ids, 1199) == 0).await;
    h.publish(1200..=1230);
    h.lens.show(1260);
    wait_for("stage 4", 30, || behind(&h, &ids, 1230) == 0).await;
    h.publish(1231..=LAST);
    h.lens.show(LAST);
    h.changelog.finalized(LAST);

    wait_for("every alert to reach the last slot", 60, || behind(&h, &ids, LAST) == 0).await;
    // Release runs before commit, so once the last `Final` is committed
    // every held delivery has been released.
    let last_id = h.changelog.last_id();
    wait_for("the change log to be fully committed", 30, || h.committed_id() == last_id).await;
    assert_eq!(engine.status().changelog_bytes_behind, 0);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(10), task).await.unwrap().unwrap();

    assert_exactly_once(&h, &ids, LAST, "outbox");
    let status = engine.status();
    assert!(status.failing_alerts.is_empty(), "{status:?}");
    assert!(status.transient_failures > 0 && status.permanent_failures > 0, "the faults happened: {status:?}");
    assert!(status.last_gate_wait_ms.is_some());
    assert!(
        h.lens.queries().iter().any(|q| q.failed),
        "some queries failed, and the run still delivered everything"
    );

    // End to end: the receiver got every delivery (at least once), and the
    // bodies, one per delivery id, carry every row exactly once.
    let outbox_ids = delivery_ids(&h);
    wait_for("every delivery at the receiver", 30, || {
        rx.received().iter().map(|r| r.delivery_id().to_owned()).collect::<HashSet<_>>() == outbox_ids
    })
    .await;
    let mut by_alert: Vec<Vec<(u64, Value)>> = vec![Vec::new(); ids.len()];
    let mut seen = HashSet::new();
    for request in rx.received() {
        if !seen.insert(request.delivery_id().to_owned()) {
            continue;
        }
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let index = ids.iter().position(|id| body["alert"]["id"] == *id).unwrap();
        by_alert[index].push((body["to_slot"].as_u64().unwrap(), body));
    }
    for ((table, seed, _), received) in ALERTS.iter().zip(&by_alert) {
        let breaches = exactly_once(received, &dataset(BASE + 1..=LAST, *seed), BASE + 1, LAST);
        assert!(breaches.is_empty(), "{table} at the receiver:\n{}", breaches.join("\n"));
    }
}

/// Alerts not yet evaluated through `slot`.
fn behind(h: &Harness, ids: &[String], slot: u64) -> usize {
    ids.iter().filter(|id| !h.progress(id).is_some_and(|p| p >= slot)).count()
}

fn delivery_ids(h: &Harness) -> HashSet<String> {
    h.registry
        .db()
        .with_conn("ids", |conn| {
            let mut stmt = conn.prepare("SELECT delivery_id FROM deliveries")?;
            let ids = stmt.query_map([], |row| row.get(0))?.collect::<Result<_, _>>()?;
            Ok(ids)
        })
        .unwrap()
}

// ---------------------------------------------------------------------------
// Shutdown and crash at every call
// ---------------------------------------------------------------------------

/// Iterations of the every-call tests run at once, each on its own files.
const PARALLEL: usize = 8;

/// `iteration(k)` for every `k` in `1..=n`, [`PARALLEL`] at a time. A
/// panicking iteration fails the test with its own message.
async fn for_every<Fut>(n: usize, iteration: impl Fn(usize) -> Fut)
where
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let settle = |result: Result<(), tokio::task::JoinError>| {
        if let Err(err) = result {
            std::panic::resume_unwind(err.into_panic());
        }
    };
    let mut running = tokio::task::JoinSet::new();
    for k in 1..=n {
        if running.len() >= PARALLEL {
            settle(running.join_next().await.unwrap());
        }
        running.spawn(iteration(k));
    }
    while let Some(result) = running.join_next().await {
        settle(result);
    }
}

/// LENS calls in an undisturbed run of the scenario, after checking that
/// run too.
async fn calls_in_a_clean_run(url: &str) -> usize {
    let mut h = Harness::new(url);
    scenario_lens(&h.lens, END);
    let ids = scenario_files(&mut h);
    run_to(&h, &ids, END, scenario_settings()).await;
    assert_exactly_once(&h, &ids, END, "clean run");
    h.lens.calls()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_at_every_call_still_delivers_every_row_exactly_once() {
    let rx = receiver().await;
    let url = rx.url("/hook");
    let calls = calls_in_a_clean_run(&url).await;
    assert!(calls > 20, "the scenario makes {calls} calls");

    let interrupted = Arc::new(AtomicUsize::new(0));
    for_every(calls, |stop_at| {
        let (url, interrupted) = (url.clone(), interrupted.clone());
        async move {
            let mut h = Harness::new(&url);
            scenario_lens(&h.lens, END);
            let ids = scenario_files(&mut h);
            let cancel = CancellationToken::new();
            let trigger = cancel.clone();
            h.lens.on_call(move |call| {
                if call == stop_at {
                    trigger.cancel();
                }
            });
            let engine = h.engine(scenario_settings());
            let mut outcome = Pass::Idle;
            while !cancel.is_cancelled() && !ids.iter().all(|id| h.progress(id) == Some(END)) {
                outcome = engine.run_once(&cancel).await;
                if matches!(outcome, Pass::Idle | Pass::GateTimedOut | Pass::Failed) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            }
            if outcome == Pass::Interrupted {
                interrupted.fetch_add(1, Ordering::Relaxed);
            }
            drop(engine);

            h.lens.on_call(|_| {});
            let h = h.restart().await;
            run_to(&h, &ids, END, scenario_settings()).await;
            assert_exactly_once(&h, &ids, END, &format!("shutdown at call {stop_at} of {calls}"));
        }
    })
    .await;
    assert!(interrupted.load(Ordering::Relaxed) > 0, "some shutdowns landed mid-pass");
}

const CRASH_DIR_VAR: &str = "FORGE_ALERTS_CRASH_DIR";
const CRASH_AT_VAR: &str = "FORGE_ALERTS_CRASH_AT";
const CRASH_URL_VAR: &str = "FORGE_ALERTS_CRASH_URL";
/// Exit code of a child that died at its crash point.
const CRASHED: i32 = 86;
/// Exit code of a child that finished before reaching it.
const FINISHED: i32 = 0;
const LENS_LOG: &str = "lens.log";

/// The crash child: run the scenario on the parent's files and exit, with
/// no cleanup of any kind, at LENS call `FORGE_ALERTS_CRASH_AT`. A no-op
/// when run as an ordinary test.
#[test]
fn crash_child() {
    let Some(root) = std::env::var_os(CRASH_DIR_VAR) else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let crash_at: usize = std::env::var(CRASH_AT_VAR).unwrap().parse().unwrap();
    let url = std::env::var(CRASH_URL_VAR).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap();
    runtime.block_on(async move {
        let lens = FakeLens::new();
        scenario_lens(&lens, END);
        lens.log_to(root.join(LENS_LOG));
        lens.on_call(move |call| {
            if call == crash_at {
                std::process::exit(CRASHED);
            }
        });
        let changelog = Changelog::new(root.join("changelog.jsonl"));
        let h = Harness::open(root, None, lens, &url, changelog);
        let ids: Vec<String> = h.registry.list_alerts().unwrap().into_iter().map(|alert| alert.id).collect();
        let engine = h.engine(scenario_settings());
        tokio::spawn({
            let engine = engine.clone();
            async move { engine.run(CancellationToken::new()).await }
        });
        wait_for("the child to finish", 60, || ids.iter().all(|id| h.progress(id) == Some(END))).await;
        std::process::exit(FINISHED);
    });
}

fn run_child(root: &Path, crash_at: usize, url: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--test-threads=1", "--nocapture"])
        .env(CRASH_DIR_VAR, root)
        .env(CRASH_AT_VAR, crash_at.to_string())
        .env(CRASH_URL_VAR, url)
        .output()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_at_every_call_still_delivers_every_row_exactly_once() {
    let rx = receiver().await;
    let url = rx.url("/hook");
    let calls = calls_in_a_clean_run(&url).await;

    let crashed = Arc::new(AtomicUsize::new(0));
    // One past the clean run's calls, so a child that outlives its crash
    // point (its retries made it shorter) is covered too.
    for_every(calls + 1, |crash_at| {
        let (url, crashed) = (url.clone(), crashed.clone());
        async move {
            let mut h = Harness::new(&url);
            let ids = scenario_files(&mut h);
            let closed = h.close().await;
            let root = closed.root.clone();

            let output = {
                let (root, url) = (root.clone(), url.clone());
                tokio::task::spawn_blocking(move || run_child(&root, crash_at, &url)).await.unwrap()
            };
            match output.status.code() {
                Some(CRASHED) => {
                    crashed.fetch_add(1, Ordering::Relaxed);
                }
                Some(FINISHED) => {}
                other => panic!(
                    "crash child at call {crash_at} exited with {other:?}:\n{}\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ),
            }
            let child_log = std::fs::read_to_string(root.join(LENS_LOG)).unwrap_or_default();
            let child_violations: Vec<&str> = child_log.lines().filter(|line| line.starts_with("VIOLATION")).collect();
            assert!(
                child_violations.is_empty(),
                "crash at call {crash_at}, before the crash:\n{}",
                child_violations.join("\n")
            );

            // Restart on what the dead process left behind.
            let h = closed.open();
            scenario_lens(&h.lens, END);
            run_to(&h, &ids, END, scenario_settings()).await;
            assert_exactly_once(&h, &ids, END, &format!("crash at call {crash_at} of about {calls}"));
        }
    })
    .await;
    let crashed = crashed.load(Ordering::Relaxed);
    assert!(crashed >= calls / 2, "{crashed} of {} children crashed", calls + 1);
}
