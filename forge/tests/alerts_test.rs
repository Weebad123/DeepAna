//! The alert engine, one behavior per test, against a fake LENS, a real
//! registry file, a real change log and a running webhook service.
//!
//! Most tests drive [`AlertEngine::run_once`] so each step is deterministic;
//! the last ones run the real loop.

mod alerts_support;
mod common;
mod webhook_support;

use std::time::Duration;

use alerts_support::*;
use forge::{AlertUpdate, EngineSettings, FailureKind, LensCallError, Pass};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use webhook_support::{Receiver, Reply, SECRET};

const TRANSFERS: &str = "token_transfers";
const TXS: &str = "transactions";

async fn receiver() -> Receiver {
    Receiver::start(|_| Reply::Status(200)).await
}

fn settings() -> EngineSettings {
    engine_settings()
}

async fn harness() -> (Harness, Receiver) {
    let receiver = receiver().await;
    (Harness::new(&receiver.url("/hook")), receiver)
}

fn no() -> CancellationToken {
    CancellationToken::new()
}

fn ranges(h: &Harness, table: &str) -> Vec<(u64, u64)> {
    h.lens.queries().into_iter().filter(|q| q.table == table).map(|q| (q.from, q.to)).collect()
}

fn assert_clean(h: &Harness) {
    assert_eq!(h.lens.violations(), Vec::<String>::new());
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_alert_is_baselined_at_the_gates_max_slot_without_a_query() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 1));
    let id = h.alert("a", TRANSFERS, false);
    let engine = h.engine(settings());

    h.publish([100]);
    h.lens.show(105); // LENS is already further than the event.
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 1 });
    assert_eq!(h.progress(&id), Some(105));
    assert!(h.lens.queries().is_empty());
    assert!(h.deliveries(&id).is_empty());

    // A target at or below the baseline is nothing to do.
    h.publish([103, 105]);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 2 });
    assert!(h.lens.queries().is_empty());

    // The first range starts right after the baseline.
    h.publish([110]);
    h.lens.show(110);
    engine.run_once(&no()).await;
    assert_eq!(ranges(&h, TRANSFERS), [(106, 110)]);
    assert_eq!(h.progress(&id), Some(110));
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_match_advances_and_enqueues_one_signed_delivery() {
    let (mut h, rx) = harness().await;
    let mut data = Table::new();
    data.insert(101, vec![row(101, u64::MAX), row(101, 7)]);
    h.lens.table(TRANSFERS, data);
    let id = h.alert_at("big-transfers", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish([101, 102]);
    h.lens.show(102);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 2 });

    assert_eq!(h.progress(&id), Some(102));
    let deliveries = h.deliveries(&id);
    assert_eq!(deliveries.len(), 1);
    let (to_slot, payload) = &deliveries[0];
    assert_eq!(*to_slot, 102);
    let evaluated_at = payload["evaluated_at"].as_i64().unwrap();
    assert_eq!(
        *payload,
        json!({
            "version": 1,
            "alert": { "id": id, "name": "big-transfers" },
            "from_slot": 101, "to_slot": 102, "evaluated_at": evaluated_at,
            "row_count": 2, "truncated": false, "rows_omitted": 0,
            "rows": [{ "slot": 101, "n": u64::MAX }, { "slot": 101, "n": 7 }]
        })
    );

    // The webhook service sends it, signed, with the stored bytes as body.
    wait_for("the delivery to arrive", 10, || rx.count() >= 1).await;
    let received = &rx.received()[0];
    assert!(received.signature_is_valid(SECRET));
    let body: Value = serde_json::from_slice(&received.body).unwrap();
    assert_eq!(body, *payload);
    assert!(String::from_utf8_lossy(&received.body).contains("18446744073709551615"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_match_advances_without_a_delivery() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, Table::new());
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish([101, 102, 103]);
    h.lens.show(103);
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(103));
    assert!(h.deliveries(&id).is_empty());
    let status = engine.status();
    assert_eq!((status.evaluations, status.matches, status.no_matches), (1, 0, 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_is_coalesced_into_one_gate_and_one_range() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 2));
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish(101..=150);
    h.lens.show(150);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 50 });
    assert_eq!(h.lens.gates(), [150]);
    assert_eq!(ranges(&h, TRANSFERS), [(101, 150)]);
    assert_eq!(h.progress(&id), Some(150));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_final_and_unknown_events_do_not_move_the_target() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 3));
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish([101]);
    h.changelog.skip(102);
    h.changelog.finalized(101);
    h.changelog.unknown();
    h.lens.show(101);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 4 });
    assert_eq!(h.lens.gates(), [101], "never gated on the skipped slot");
    assert_eq!(engine.status().target_slot, Some(101));
    assert_eq!(h.progress(&id), Some(101));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_and_replayed_events_neither_gate_nor_query() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 4));
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());
    h.publish(101..=120);
    h.lens.show(120);
    engine.run_once(&no()).await;
    let (gates, queries, deliveries) = (h.lens.gates().len(), h.lens.queries().len(), h.deliveries(&id).len());

    // PRISM re-announces after a restart; then the whole file is replaced
    // and read again from the start.
    h.publish(110..=120);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 11 });
    h.changelog.replace();
    h.publish(101..=115);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 15 });

    assert_eq!(h.lens.gates().len(), gates);
    assert_eq!(h.lens.queries().len(), queries);
    assert_eq!(h.deliveries(&id).len(), deliveries);
    assert_eq!(h.progress(&id), Some(120));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranges_are_split_at_max_range_slots_and_each_commits() {
    let (mut h, _rx) = harness().await;
    let data = dataset(1..=1200, 5);
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", TRANSFERS, 999);
    let engine = h.engine(EngineSettings { max_range_slots: 30, ..settings() });

    h.publish(1000..=1099);
    h.lens.show(1099);
    engine.run_once(&no()).await;
    assert_eq!(ranges(&h, TRANSFERS), [(1000, 1029), (1030, 1059), (1060, 1089), (1090, 1099)]);
    let deliveries = h.deliveries(&id);
    // One delivery per matching chunk, each covering its own chunk.
    assert!(deliveries.iter().all(|(to, _)| [1029, 1059, 1089, 1099].contains(to)));
    assert_eq!(exactly_once(&deliveries, &data, 1000, 1099), Vec::<String>::new());
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranges_end_at_the_announced_target_not_at_lens_max_slot() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 6));
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());
    h.publish([110]);
    // LENS already shows slots of a flush PRISM has not announced: their
    // other tables may still be being written.
    h.lens.show(140);
    engine.run_once(&no()).await;
    assert_eq!(ranges(&h, TRANSFERS), [(101, 110)]);
    assert_eq!(h.progress(&id), Some(110));
}

// ---------------------------------------------------------------------------
// Incomplete results
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_truncated_range_is_halved_until_lens_returns_it_whole() {
    let (mut h, _rx) = harness().await;
    let data: Table = (101..=116).map(|slot| (slot, vec![row(slot, slot), row(slot, slot + 1000)])).collect();
    h.lens.table(TRANSFERS, data.clone());
    h.lens.max_rows(5);
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish(101..=116);
    h.lens.show(116);
    engine.run_once(&no()).await;

    let deliveries = h.deliveries(&id);
    assert_eq!(exactly_once(&deliveries, &data, 101, 116), Vec::<String>::new());
    // 2 rows a slot under a 5-row cap: no delivery spans more than 2 slots.
    for (_, payload) in &deliveries {
        assert!(payload["to_slot"].as_u64().unwrap() - payload["from_slot"].as_u64().unwrap() < 2, "{payload}");
    }
    assert_eq!(h.progress(&id), Some(116));
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_slot_lens_truncates_is_delivered_flagged() {
    let (mut h, _rx) = harness().await;
    let mut data = Table::new();
    data.insert(101, vec![row(101, 1)]);
    data.insert(102, (0..8).map(|n| row(102, n)).collect());
    data.insert(103, vec![row(103, 1)]);
    h.lens.table(TRANSFERS, data);
    h.lens.max_rows(5);
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish(101..=103);
    h.lens.show(103);
    engine.run_once(&no()).await;

    let flagged: Vec<(u64, u64, bool, u64)> = h
        .deliveries(&id)
        .iter()
        .map(|(_, p)| {
            (p["from_slot"].as_u64().unwrap(), p["to_slot"].as_u64().unwrap(), p["truncated"].as_bool().unwrap(), p["row_count"].as_u64().unwrap())
        })
        .collect();
    assert_eq!(flagged, [(101, 101, false, 1), (102, 102, true, 5), (103, 103, false, 1)]);
    assert_eq!(h.progress(&id), Some(103));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_over_the_payload_budget_split_the_range_instead_of_being_dropped() {
    let (mut h, _rx) = harness().await;
    let data: Table = (101..=140).map(|slot| (slot, vec![row(slot, u64::MAX - slot)])).collect();
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(EngineSettings { payload_budget_bytes: 700, ..settings() });

    h.publish(101..=140);
    h.lens.show(140);
    engine.run_once(&no()).await;

    let deliveries = h.deliveries(&id);
    assert!(deliveries.len() > 1);
    assert_eq!(exactly_once(&deliveries, &data, 101, 140), Vec::<String>::new());
    for (to_slot, _) in &deliveries {
        let stored = h
            .registry
            .db()
            .with_conn("len", |c| {
                Ok(c.query_row("SELECT length(payload) FROM deliveries WHERE to_slot = ?1", [*to_slot as i64], |r| {
                    r.get::<_, i64>(0)
                })?)
            })
            .unwrap();
        assert!(stored <= 700, "{stored}");
    }
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_failing_alert_does_not_hold_back_the_others_or_the_checkpoint() {
    let (mut h, _rx) = harness().await;
    let transfers = dataset(1..=300, 7);
    let txs = dataset(1..=300, 8);
    h.lens.table(TRANSFERS, transfers.clone());
    h.lens.table(TXS, txs.clone());
    let broken = h.alert_at("broken", TRANSFERS, 100);
    let healthy = h.alert_at("healthy", TXS, 100);
    h.lens.fault(|q| (q.table == TRANSFERS).then(|| LensCallError::Unavailable { message: "connection reset".into() }));
    let engine = h.engine(settings());

    h.publish(101..=120);
    h.lens.show(120);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 20 });
    assert_eq!(h.committed_id(), 20, "the checkpoint moved despite the failure");
    assert_eq!(h.progress(&broken), Some(100));
    assert_eq!(h.progress(&healthy), Some(120));

    let status = engine.status();
    assert_eq!(status.transient_failures, 1);
    assert_eq!(status.failing_alerts.len(), 1);
    let failing = &status.failing_alerts[0];
    assert_eq!((failing.id.as_str(), failing.kind, failing.consecutive), (broken.as_str(), FailureKind::Transient, 1));
    assert!(failing.last_error.contains("connection reset"), "{}", failing.last_error);

    // LENS recovers; with no new event the retry still runs once it is due.
    h.lens.clear_fault();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(engine.run_once(&no()).await, Pass::Retried);
    assert_eq!(h.progress(&broken), Some(120));
    assert!(engine.status().failing_alerts.is_empty());
    assert_eq!(exactly_once(&h.deliveries(&broken), &transfers, 101, 120), Vec::<String>::new());
    assert_eq!(exactly_once(&h.deliveries(&healthy), &txs, 101, 120), Vec::<String>::new());
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_alert_is_not_retried_before_its_back_off() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 9));
    let id = h.alert_at("a", TRANSFERS, 100);
    h.lens.fault(|_| Some(LensCallError::Timeout { message: "slow".into() }));
    let engine = h.engine(EngineSettings { retry_min: Duration::from_secs(60), retry_max: Duration::from_secs(60), ..settings() });

    h.publish([101]);
    h.lens.show(101);
    engine.run_once(&no()).await;
    h.publish([102]);
    h.lens.show(102);
    engine.run_once(&no()).await;
    assert_eq!(h.lens.queries().len(), 1, "the second pass skipped the alert");
    assert_eq!(engine.run_once(&no()).await, Pass::Idle);
    assert_eq!(h.progress(&id), Some(100));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mid_range_failure_keeps_the_chunks_already_committed() {
    let (mut h, _rx) = harness().await;
    let data = dataset(1..=300, 10);
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", TRANSFERS, 100);
    h.lens.fault(|q| (q.from == 121).then(|| LensCallError::Unavailable { message: "down".into() }));
    let engine = h.engine(EngineSettings { max_range_slots: 10, ..settings() });

    h.publish(101..=150);
    h.lens.show(150);
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(120));
    assert_eq!(ranges(&h, TRANSFERS), [(101, 110), (111, 120), (121, 130)]);

    h.lens.clear_fault();
    tokio::time::sleep(Duration::from_millis(20)).await;
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(150));
    assert_eq!(exactly_once(&h.deliveries(&id), &data, 101, 150), Vec::<String>::new());
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_sql_is_permanent_and_fixing_the_sql_retries_at_once() {
    let (mut h, _rx) = harness().await;
    let data = dataset(1..=300, 11);
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", "nonexistent_column", 100);
    h.lens.fault(|q| {
        (q.table == "nonexistent_column")
            .then(|| LensCallError::BadQuery { message: "query_plan_error: Schema error: No field named nope".into() })
    });
    let engine = h.engine(EngineSettings { permanent_retry: Duration::from_secs(3600), ..settings() });

    h.publish(101..=110);
    h.lens.show(110);
    engine.run_once(&no()).await;
    let status = engine.status();
    assert_eq!((status.permanent_failures, status.failing_alerts[0].kind), (1, FailureKind::Permanent));
    assert_eq!(h.progress(&id), Some(100));

    // Not retried within the hour...
    h.publish([111]);
    h.lens.show(111);
    engine.run_once(&no()).await;
    assert_eq!(h.lens.queries().len(), 1);

    // ...unless the SQL changes. Nothing between progress and the target
    // was skipped while it was broken.
    h.registry
        .update_alert(&id, AlertUpdate { sql: Some(alert_sql(TRANSFERS)), ..AlertUpdate::default() })
        .unwrap();
    h.publish([112]);
    h.lens.show(112);
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(112));
    assert!(engine.status().failing_alerts.is_empty());
    assert_eq!(exactly_once(&h.deliveries(&id), &data, 101, 112), Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_protocol_error_is_permanent() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 12));
    h.alert_at("a", TRANSFERS, 100);
    h.lens.fault(|_| Some(LensCallError::Protocol { message: "row_count 2 but 1 rows".into() }));
    let engine = h.engine(settings());
    h.publish([101]);
    h.lens.show(101);
    engine.run_once(&no()).await;
    assert_eq!(engine.status().failing_alerts[0].kind, FailureKind::Permanent);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_table_is_transient_up_to_the_bound_then_permanent_and_recovers() {
    let (mut h, _rx) = harness().await;
    let data = dataset(1..=300, 13);
    h.lens.table(TRANSFERS, data.clone());
    h.lens.unregister(TRANSFERS);
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(EngineSettings { missing_table_retries: 2, ..settings() });

    h.publish(101..=110);
    h.lens.show(110);
    let mut kinds = Vec::new();
    for _ in 0..4 {
        engine.run_once(&no()).await;
        kinds.push(engine.status().failing_alerts[0].kind);
        tokio::time::sleep(Duration::from_millis(45)).await;
    }
    use FailureKind::{Permanent, Transient};
    assert_eq!(kinds, [Transient, Transient, Permanent, Permanent]);
    let status = engine.status();
    assert_eq!((status.transient_failures, status.permanent_failures), (2, 2));
    assert_eq!(status.failing_alerts[0].consecutive, 4);
    assert_eq!(h.progress(&id), Some(100));

    // PRISM writes the table, LENS registers it: the permanent retry picks
    // it up and nothing was skipped.
    h.lens.register(TRANSFERS);
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(110));
    assert_eq!(exactly_once(&h.deliveries(&id), &data, 101, 110), Vec::<String>::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gate_timeout_rewinds_the_batch_without_committing() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 14));
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());

    h.publish(101..=105);
    h.lens.show(103);
    assert_eq!(engine.run_once(&no()).await, Pass::GateTimedOut);
    assert_eq!(h.committed_id(), 0);
    assert!(h.lens.queries().is_empty(), "nothing is evaluated before the gate opens");
    assert_eq!(h.progress(&id), Some(100));
    assert!(engine.status().last_gate_wait_ms.unwrap() >= 150);

    h.lens.show(105);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 5 }, "the same batch, read again");
    assert_eq!(h.progress(&id), Some(105));
    assert_eq!(h.committed_id(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_alert_is_dropped_silently() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 15));
    let id = h.alert_at("a", TRANSFERS, 100);
    let registry = h.registry.clone();
    let victim = id.clone();
    h.lens.on_call(move |call| {
        if call == 2 {
            registry.delete_alert(&victim).unwrap();
        }
    });
    let engine = h.engine(settings());
    h.publish([101]);
    h.lens.show(101);
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 1 });
    let status = engine.status();
    assert!(status.failing_alerts.is_empty());
    assert_eq!(status.transient_failures + status.permanent_failures, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disabled_alerts_are_not_evaluated_and_re_enabling_baselines() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, dataset(1..=300, 16));
    let id = h.alert_at("a", TRANSFERS, 100);
    h.registry.set_enabled(&id, false).unwrap();
    let engine = h.engine(settings());
    h.publish([110]);
    h.lens.show(110);
    engine.run_once(&no()).await;
    assert!(h.lens.queries().is_empty());

    h.registry.set_enabled(&id, true).unwrap();
    h.publish([120]);
    h.lens.show(125);
    engine.run_once(&no()).await;
    assert_eq!(h.progress(&id), Some(125), "baselined, not a 100..=120 catch-up");
    assert!(h.lens.queries().is_empty());
}

// ---------------------------------------------------------------------------
// Finality
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wait_for_final_deliveries_are_held_until_a_final_event_covers_them() {
    let (mut h, rx) = harness().await;
    let mut data = Table::new();
    data.insert(101, vec![row(101, 1)]);
    data.insert(106, vec![row(106, 2)]);
    h.lens.table(TRANSFERS, data);
    let id = h.alert("f", TRANSFERS, true);
    h.set_progress(&id, 100);
    let engine = h.engine(EngineSettings { max_range_slots: 5, ..settings() });

    h.publish(101..=110);
    h.lens.show(110);
    engine.run_once(&no()).await;
    assert_eq!(h.delivery_statuses(&id), ["held", "held"]);

    h.changelog.finalized(105);
    engine.run_once(&no()).await;
    let statuses = h.delivery_statuses(&id);
    assert_ne!(statuses[0], "held", "released (and maybe already delivered)");
    assert_eq!(statuses[1], "held");
    h.changelog.finalized(110);
    engine.run_once(&no()).await;
    wait_for("both deliveries", 10, || rx.count() >= 2).await;
    assert_eq!(h.delivery_statuses(&id).len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn on_start_the_engine_releases_what_the_committed_change_log_finalized() {
    let (mut h, _rx) = harness().await;
    let mut data = Table::new();
    data.insert(101, vec![row(101, 1)]);
    h.lens.table(TRANSFERS, data);
    let id = h.alert("f", TRANSFERS, true);
    h.set_progress(&id, 100);
    h.publish([101]);
    h.lens.show(101);
    h.engine(settings()).run_once(&no()).await;
    assert_eq!(h.delivery_statuses(&id), ["held"]);

    // A Final committed by a run that stopped before releasing: commit it
    // through a bare reader.
    h.changelog.finalized(101);
    {
        let mut reader = forge::ChangelogReader::open_path(h.changelog.path(), h.checkpoint_store()).unwrap();
        let batch = reader.poll(10).unwrap();
        reader.commit(batch.last().unwrap()).unwrap();
    }
    assert_eq!(h.delivery_statuses(&id), ["held"]);

    let engine = h.engine(settings());
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });
    wait_for("the release", 10, || h.delivery_statuses(&id) != ["held"]).await;
    cancel.cancel();
    task.await.unwrap();
}

// ---------------------------------------------------------------------------
// Status and the loop
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_counts_what_happened() {
    let (mut h, _rx) = harness().await;
    let mut data = Table::new();
    data.insert(105, vec![row(105, 1)]);
    h.lens.table(TRANSFERS, data);
    h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(EngineSettings { max_range_slots: 5, batch_size: 8, ..settings() });

    h.publish(101..=110);
    h.lens.show(110);
    engine.run_once(&no()).await;
    let status = engine.status();
    assert_eq!(status.target_slot, Some(108));
    assert_eq!(status.events_processed, 8);
    assert_eq!((status.evaluations, status.matches, status.no_matches), (2, 1, 1));
    assert!(status.changelog_bytes_behind > 0, "two events left to read");
    assert!(status.last_gate_wait_ms.is_some());

    engine.run_once(&no()).await;
    let status = engine.status();
    assert_eq!((status.target_slot, status.events_processed, status.changelog_bytes_behind), (Some(110), 10, 0));

    let json = serde_json::to_value(engine.status()).unwrap();
    for field in [
        "target_slot", "last_gate_wait_ms", "events_processed", "evaluations", "matches", "no_matches",
        "transient_failures", "permanent_failures", "failing_alerts", "changelog_bytes_behind",
    ] {
        assert!(json.get(field).is_some(), "{field} in {json}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_mid_batch_commits_no_checkpoint_and_a_restart_finishes_exactly_once() {
    let (mut h, _rx) = harness().await;
    let data = dataset(1..=300, 17);
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", TRANSFERS, 100);
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    // Call 1 is the gate; cancel while chunk 3 is being evaluated. The fake
    // answers without yielding, so that chunk still completes and commits;
    // the loop stops before chunk 4.
    h.lens.on_call(move |call| {
        if call == 4 {
            trigger.cancel();
        }
    });
    let engine = h.engine(EngineSettings { max_range_slots: 10, ..settings() });
    h.publish(101..=150);
    h.lens.show(150);
    assert_eq!(engine.run_once(&cancel).await, Pass::Interrupted);
    assert_eq!(h.committed_id(), 0, "an interrupted batch is not committed");
    assert_eq!(h.progress(&id), Some(130), "committed chunks stay committed");
    drop(engine);

    h.lens.on_call(|_| {});
    let h = h.restart().await;
    let engine = h.engine(EngineSettings { max_range_slots: 10, ..settings() });
    assert_eq!(engine.run_once(&no()).await, Pass::Committed { events: 50 });
    assert_eq!(h.progress(&id), Some(150));
    assert_eq!(exactly_once(&h.deliveries(&id), &data, 101, 150), Vec::<String>::new());
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_loop_follows_the_change_log_and_stops_on_cancel() {
    let (mut h, rx) = harness().await;
    let data = dataset(1..=400, 18);
    h.lens.table(TRANSFERS, data.clone());
    let id = h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(settings());
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });

    for end in [130u64, 170, 200] {
        h.publish(end - 29..=end);
        tokio::time::sleep(Duration::from_millis(30)).await;
        h.lens.show(end);
        wait_for("the alert to catch up", 10, || h.progress(&id) == Some(end)).await;
    }
    let deliveries = h.deliveries(&id);
    assert_eq!(exactly_once(&deliveries, &data, 101, 200), Vec::<String>::new());
    wait_for("every delivery to arrive", 10, || {
        let ids: std::collections::HashSet<String> = rx.received().iter().map(|r| r.delivery_id().to_owned()).collect();
        ids.len() == deliveries.len()
    })
    .await;

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await.expect("run returns on cancel").unwrap();
    assert_clean(&h);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_during_a_gate_wait_returns_promptly() {
    let (mut h, _rx) = harness().await;
    h.lens.table(TRANSFERS, Table::new());
    h.alert_at("a", TRANSFERS, 100);
    let engine = h.engine(EngineSettings { gate_timeout: Duration::from_secs(3600), ..settings() });
    h.publish([101]);
    let cancel = CancellationToken::new();
    let task = tokio::spawn({
        let engine = engine.clone();
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });
    wait_for("the gate", 10, || !h.lens.gates().is_empty()).await;
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task).await.expect("run returns on cancel").unwrap();
    assert_eq!(h.committed_id(), 0);
}

#[test]
fn engine_settings_from_config_add_no_config_fields() {
    let fixture = common::Fixture::new();
    let config = fixture.load(&[(forge::config::ENV_POLL_INTERVAL_MS, "250")]).unwrap();
    let settings = EngineSettings::from_config(&config);
    assert_eq!(settings.poll_interval, Duration::from_millis(250));
    assert_eq!(settings.gate_timeout, forge::DEFAULT_GATE_TIMEOUT);
    assert_eq!(forge::config::ENV_VARS.len(), 9);
}
