//! `forge::WebhookService` against a scripted receiver on a real socket.
//!
//! | Section      | Pins down                                                        |
//! |--------------|------------------------------------------------------------------|
//! | Contract     | method, headers, ids, the attempt counter                        |
//! | Failures     | non-2xx, redirects, timeouts, refused connections, error hygiene |
//! | Concurrency  | one in flight per alert, the global cap                          |
//! | Lifecycle    | finality holds, requeue, duplicates, cascade, shutdown, depth    |
//!
//! The end-to-end guarantees (terminal states, schedule, crashes, signatures,
//! bodies) are in `webhook_guarantees_test.rs`.

mod common;
mod webhook_support;

use std::time::Duration;

use forge::outbox::MAX_ERROR_LEN;
use forge::{
    DeliveryStatus, Enqueued, ForgeError, HEADER_ATTEMPT, HEADER_DELIVERY_ID, HEADER_SIGNATURE, HEADER_TIMESTAMP,
    NewDelivery, Registry, USER_AGENT_VALUE,
};
use webhook_support::*;

fn http_status(err: ForgeError) -> u16 {
    err.as_registry_error().expect("a RegistryError source").http_status()
}

// ===========================================================================
// Contract
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delivery_follows_the_contract() {
    let receiver = Receiver::start(|_| Reply::Status(200)).await;
    let h = Harness::new(settings(3, &[5]));
    let alert = h.alert("a", &receiver.url("/hooks/in?team=x"));
    let id = h.fire(&alert, 42, r#"{"rows":[]}"#, false);
    let row = h.settled(&id).await;
    assert_eq!((row.status, row.attempts, row.last_status_code), (DeliveryStatus::Delivered, 1, Some(200)));
    assert_eq!((row.delivered_at, row.last_error.as_deref()), (Some(T0), None));

    let [r] = receiver.received().try_into().unwrap();
    assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/hooks/in?team=x"));
    assert_eq!(r.header("content-type"), Some("application/json"));
    assert_eq!(r.header("user-agent"), Some(USER_AGENT_VALUE));
    assert!(USER_AGENT_VALUE.starts_with("helios-forge/"));
    assert_eq!(r.header(HEADER_DELIVERY_ID), Some(id.as_str()));
    let hex = id.strip_prefix("dlv_").unwrap();
    assert!(hex.len() == 32 && hex.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()), "{id}");
    assert_eq!(r.header(HEADER_ATTEMPT), Some("1"));
    assert_eq!(r.header(HEADER_TIMESTAMP), Some((T0 / 1000).to_string().as_str()));
    let signature = r.header(HEADER_SIGNATURE).unwrap();
    assert!(signature.starts_with("v1=") && signature.len() == 67, "{signature}");
    assert!(r.signature_is_valid(SECRET));
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_delivery_id_is_stable_and_the_attempt_counts_up() {
    let receiver = Receiver::start(|r| Reply::Status(if r.nth < 2 { 500 } else { 200 })).await;
    let h = Harness::new(settings(5, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    for n in 1..=2 {
        h.retrying_after(&id, n).await;
        h.clock.advance(Duration::from_secs(1));
    }
    assert_eq!(h.settled(&id).await.attempts, 3);
    let requests = receiver.received();
    assert!(requests.iter().all(|r| r.delivery_id() == id));
    assert_eq!(requests.iter().map(|r| r.attempt()).collect::<Vec<_>>(), [1, 2, 3]);
    h.service.shutdown().await;
}

// ===========================================================================
// Failures
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_non_2xx_is_a_failure_with_its_status_and_body() {
    for status in [301, 400, 404, 410, 429, 500, 503] {
        let receiver = Receiver::start(move |_| Reply::WithBody(status, format!("nope {status}"))).await;
        let h = Harness::new(settings(1, &[1]));
        let alert = h.alert("a", &receiver.url("/hook"));
        let id = h.fire(&alert, 1, "{}", false);
        let row = h.settled(&id).await;
        assert_eq!((row.status, row.last_status_code), (DeliveryStatus::Failed, Some(status)));
        let error = row.last_error.unwrap();
        assert!(error.starts_with(&format!("HTTP {status}")) && error.ends_with(&format!("nope {status}")), "{error}");
        h.service.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redirects_are_not_followed() {
    let receiver = Receiver::start(|r| {
        if r.path == "/hook" { Reply::Redirect("/elsewhere".into()) } else { Reply::Status(200) }
    })
    .await;
    let h = Harness::new(settings(1, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    let row = h.settled(&id).await;
    assert_eq!((row.status, row.last_status_code), (DeliveryStatus::Failed, Some(302)));
    assert!(row.last_error.unwrap().contains("redirects are not followed"));
    idle(100).await;
    assert_eq!(receiver.count(), 1, "the Location was never requested");
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hung_receiver_times_out() {
    let receiver = Receiver::start(|_| Reply::Hang).await;
    let mut settings = settings(1, &[1]);
    settings.request_timeout = Duration::from_millis(300);
    let h = Harness::new(settings);
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    let row = h.settled(&id).await;
    assert_eq!((row.status, row.last_status_code), (DeliveryStatus::Failed, None));
    assert!(row.last_error.as_deref().unwrap().starts_with("timed out"), "{row:?}");
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_connection_is_a_failure_without_the_url() {
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let h = Harness::new(settings(1, &[1]));
    let url = format!("http://127.0.0.1:{port}/private/path?token=sekrit");
    let alert = h.alert("a", &url);
    let id = h.fire(&alert, 1, "{}", false);
    let row = h.settled(&id).await;
    let error = row.last_error.unwrap();
    assert!(error.starts_with("cannot connect"), "{error}");
    for leak in ["/private/path", "sekrit", &url] {
        assert!(!error.contains(leak), "{leak:?} in {error:?}");
    }
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn last_error_never_holds_the_secret_or_the_url_and_is_bounded() {
    let url_holder = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let echo = url_holder.clone();
    let receiver = Receiver::start(move |_| {
        let url = echo.lock().unwrap().clone();
        Reply::WithBody(400, format!("bad: url={url} secret={SECRET}\n{}", "z".repeat(10_000)))
    })
    .await;
    let url = receiver.url("/a/b?key=hush");
    *url_holder.lock().unwrap() = url.clone();
    let h = Harness::new(settings(1, &[1]));
    let alert = h.alert("a", &url);
    let id = h.fire(&alert, 1, "{}", false);
    let error = h.settled(&id).await.last_error.unwrap();
    for leak in [SECRET, url.as_str(), "/a/b", "key=hush"] {
        assert!(!error.contains(leak), "{leak:?} in {error:?}");
    }
    assert!(error.contains("<redacted>") && error.len() <= MAX_ERROR_LEN, "{error}");
    assert!(!error.contains('\n'));
    h.service.shutdown().await;
}

// ===========================================================================
// Concurrency
// ===========================================================================

/// Alert `slow` has five deliveries to a receiver that takes 150 ms each;
/// they go one at a time. Meanwhile alert `fast` is not held up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_one_delivery_per_alert_is_in_flight() {
    let receiver = Receiver::start(|r| {
        if r.path == "/slow" { Reply::After(Duration::from_millis(150), 200) } else { Reply::Status(200) }
    })
    .await;
    let h = Harness::new(settings(1, &[1]));
    let slow = h.alert("slow", &receiver.url("/slow"));
    let fast = h.alert("fast", &receiver.url("/fast"));
    let slow_ids: Vec<String> = (1..=5).map(|s| h.fire(&slow, s, "{}", false)).collect();
    let fast_id = h.fire(&fast, 1, "{}", false);

    h.settled(&fast_id).await;
    let slow_done = slow_ids.iter().filter(|id| h.status(id).is_terminal()).count();
    assert!(slow_done < 5, "fast waited behind slow");
    for id in &slow_ids {
        assert_eq!(h.settled(id).await.status, DeliveryStatus::Delivered);
    }
    assert_eq!(receiver.max_active_per_path()["/slow"], 1);
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_flight_sends_are_capped() {
    let receiver = Receiver::start(|_| Reply::After(Duration::from_millis(100), 200)).await;
    let mut settings = settings(1, &[1]);
    settings.max_in_flight = 3;
    let h = Harness::new(settings);
    let ids: Vec<String> = (0..9)
        .map(|i| {
            let alert = h.alert(&format!("a{i}"), &receiver.url(&format!("/{i}")));
            h.fire(&alert, 1, "{}", false)
        })
        .collect();
    for id in &ids {
        h.settled(id).await;
    }
    assert_eq!(receiver.max_active_total(), 3);
    h.service.shutdown().await;
}

// ===========================================================================
// Lifecycle
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_delivery_waits_for_its_slot_to_be_final() {
    let receiver = Receiver::start(|_| Reply::Status(200)).await;
    let h = Harness::new(settings(3, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 100, "{}", true);
    let row = h.get(&id);
    assert_eq!((row.status, row.release_after_slot), (DeliveryStatus::Held, Some(100)));

    h.clock.advance(Duration::from_secs(3_600));
    h.service.wake();
    idle(150).await;
    assert_eq!(receiver.count(), 0);
    assert_eq!(h.service.release_final(99).unwrap(), 0);
    idle(100).await;
    assert_eq!(receiver.count(), 0);

    assert_eq!(h.service.release_final(100).unwrap(), 1);
    assert_eq!(h.settled(&id).await.status, DeliveryStatus::Delivered);
    assert_eq!(h.service.release_final(100).unwrap(), 0, "idempotent");
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_delivery_can_be_requeued() {
    let receiver = Receiver::start(|_| Reply::Status(500)).await;
    let h = Harness::new(settings(2, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    h.retrying_after(&id, 1).await;
    h.clock.advance(Duration::from_secs(1));
    assert_eq!(h.settled(&id).await.status, DeliveryStatus::Failed);

    receiver.set_script(|_| Reply::Status(200));
    let requeued = h.service.requeue(&id).unwrap();
    assert_eq!((requeued.status, requeued.attempts), (DeliveryStatus::Pending, 0));
    let row = h.settled(&id).await;
    assert_eq!((row.status, row.attempts), (DeliveryStatus::Delivered, 1));
    assert_eq!(receiver.received().last().unwrap().attempt(), 1);

    assert_eq!(http_status(h.service.requeue(&id).unwrap_err()), 409, "delivered is final");
    assert_eq!(http_status(h.service.requeue("dlv_00000000000000000000000000000000").unwrap_err()), 404);
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn re_enqueueing_a_range_is_a_duplicate_not_a_second_send() {
    let receiver = Receiver::start(|_| Reply::Status(200)).await;
    let h = Harness::new(settings(3, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 7, r#"{"v":1}"#, false);
    h.settled(&id).await;

    // A re-evaluation after a crash, with the alert re-enabled (progress
    // cleared), produces the same range again.
    h.registry.set_enabled(&alert, false).unwrap();
    h.registry.set_enabled(&alert, true).unwrap();
    let again = NewDelivery {
        alert_id: alert.clone(),
        to_slot: 7,
        payload: r#"{"v":2}"#.into(),
        release_after_slot: None,
    };
    let enqueued = h
        .registry
        .db()
        .transaction("refire", |tx| {
            Registry::advance_progress(tx, &alert, 7)?;
            h.service.enqueue(tx, &again)
        })
        .unwrap();
    assert_eq!(enqueued, Enqueued::Duplicate { delivery_id: id.clone() });
    h.service.wake();
    idle(150).await;
    assert_eq!(receiver.count(), 1);
    assert_eq!(h.stored_payload_bytes(&id), br#"{"v":1}"#);
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_an_alert_drops_its_unsent_deliveries() {
    let receiver = Receiver::start(|_| Reply::Status(200)).await;
    let h = Harness::new(settings(3, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 5, "{}", true);
    h.registry.delete_alert(&alert).unwrap();
    assert_eq!(h.service.release_final(5).unwrap(), 0);
    assert_eq!(http_status(h.service.outbox().get(&id).unwrap_err()), 404);
    idle(100).await;
    assert_eq!(receiver.count(), 0);
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_lets_in_flight_sends_finish_and_stops_claiming() {
    let receiver = Receiver::start(|_| Reply::After(Duration::from_millis(300), 200)).await;
    let h = Harness::new(settings(3, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    eventually("the send to start", || receiver.count() == 1).await;
    h.service.shutdown().await;
    assert_eq!(h.status(&id), DeliveryStatus::Delivered, "the in-flight send was allowed to finish");

    let later = h.fire(&alert, 2, "{}", false);
    idle(150).await;
    assert_eq!(receiver.count(), 1, "nothing is claimed after shutdown");
    assert_eq!(h.status(&later), DeliveryStatus::Pending);
    h.service.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_abandons_sends_that_outlive_the_grace_period() {
    let receiver = Receiver::start(|_| Reply::Hang).await;
    let mut settings = settings(3, &[1]);
    settings.shutdown_grace = Duration::from_millis(100);
    let h = Harness::new(settings);
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, "{}", false);
    eventually("the send to start", || receiver.count() == 1).await;
    let started = std::time::Instant::now();
    h.service.shutdown().await;
    assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
    // Abandoned: the attempt counts, no outcome, due again after the lease.
    let row = h.get(&id);
    assert_eq!((row.status, row.attempts, row.next_attempt_at), (DeliveryStatus::Pending, 1, T0 + LEASE_MS));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn depth_and_history() {
    let receiver = Receiver::start(|r| Reply::Status(if r.path == "/ok" { 200 } else { 500 })).await;
    let h = Harness::new(settings(1, &[1]));
    let ok = h.alert("ok", &receiver.url("/ok"));
    let bad = h.alert("bad", &receiver.url("/bad"));
    let delivered = h.fire(&ok, 1, "{}", false);
    let failed = h.fire(&bad, 1, "{}", false);
    h.settled(&delivered).await;
    h.settled(&failed).await;
    h.fire(&ok, 2, "{}", true);
    h.clock.advance(Duration::from_secs(9));

    let depth = h.service.depth().unwrap();
    assert_eq!((depth.held, depth.pending, depth.delivered, depth.failed), (1, 0, 1, 1));
    assert_eq!(depth.oldest_pending_age_ms, None);

    let history = h.service.list(&ok, 10).unwrap();
    assert_eq!(history.iter().map(|d| (d.to_slot, d.status)).collect::<Vec<_>>(), [
        (2, DeliveryStatus::Held),
        (1, DeliveryStatus::Delivered)
    ]);
    let json = serde_json::to_value(&history[1]).unwrap();
    assert_eq!(json["status"], "delivered");
    assert!(json.get("secret").is_none() && json.get("webhook_url").is_none());
    h.service.shutdown().await;
}
