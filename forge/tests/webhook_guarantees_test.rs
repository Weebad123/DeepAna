//! The webhook outbox's guarantees, each measured end to end against a real
//! socket and a real registry file:
//!
//! | Section     | Guarantee                                                            |
//! |-------------|----------------------------------------------------------------------|
//! | Terminal    | every enqueued delivery ends in exactly one terminal state           |
//! | Schedule    | retries happen when the policy says, not before, and stop at the cap |
//! | Crashes     | nothing committed is lost to a crash, at any point                   |
//! | Signatures  | every request verifies under an HMAC written from scratch here       |
//! | Bodies      | every body is the stored payload, byte for byte                      |
//!
//! "Measured" means each test counts what the receiver saw and what the
//! database says, independently, and asserts the two agree.

mod common;
mod webhook_support;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::TempDir;
use forge::{
    DeliveryStatus, NewDelivery, Outbox, Registry, RetryPolicy, SystemClock, WebhookService, WebhookSettings,
    signer,
};
use serde_json::{Value, json};
use webhook_support::*;

fn all_rows(h: &Harness) -> Vec<forge::Delivery> {
    let ids: Vec<String> = h
        .registry
        .db()
        .with_conn("ids", |conn| {
            let mut stmt = conn.prepare("SELECT delivery_id FROM deliveries ORDER BY delivery_id")?;
            let ids = stmt.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
            Ok(ids)
        })
        .unwrap();
    ids.iter().map(|id| h.get(id)).collect()
}

// ===========================================================================
// Terminal
// ===========================================================================

/// 30 deliveries over 6 alerts. Each payload tells the receiver how many
/// times to fail it first (500, a dropped connection, or 429, in turn), from
/// 0 to 4, against a cap of 3 attempts. Time is driven forward until nothing
/// is left to do, while every intermediate state is sampled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_enqueued_delivery_ends_in_exactly_one_terminal_state() {
    const MAX: u32 = 3;
    let receiver = Receiver::start(|r| {
        let body: Value = serde_json::from_slice(&r.body).unwrap();
        let fail_first = body["fail_first"].as_u64().unwrap() as usize;
        match r.nth {
            n if n >= fail_first => Reply::Status(if n % 2 == 0 { 200 } else { 204 }),
            n => [Reply::Status(500), Reply::Close, Reply::Status(429)][n % 3].clone(),
        }
    })
    .await;
    let h = Harness::new(settings(MAX, &[5, 10]));

    // Expected (status, attempts) per delivery.
    let mut expected: BTreeMap<String, (DeliveryStatus, u32)> = BTreeMap::new();
    for a in 0..6u64 {
        let alert = h.alert(&format!("a{a}"), &receiver.url(&format!("/hook/{a}")));
        for slot in 1..=5u64 {
            let fail_first = (a + slot) % 5;
            let payload = json!({"alert": a, "slot": slot, "fail_first": fail_first}).to_string();
            let id = h.fire(&alert, slot, &payload, false);
            let want = if fail_first < u64::from(MAX) {
                (DeliveryStatus::Delivered, fail_first as u32 + 1)
            } else {
                (DeliveryStatus::Failed, MAX)
            };
            expected.insert(id, want);
        }
    }

    // Drive time, sampling every row on every step: once a row is terminal
    // it must never be seen in any other state.
    let mut first_terminal: HashMap<String, DeliveryStatus> = HashMap::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let rows = all_rows(&h);
        for row in &rows {
            if let Some(seen) = first_terminal.get(&row.delivery_id) {
                assert_eq!(row.status, *seen, "{} left its terminal state", row.delivery_id);
            } else if row.status.is_terminal() {
                first_terminal.insert(row.delivery_id.clone(), row.status);
            }
            assert!(row.attempts <= MAX, "{row:?}");
        }
        if rows.iter().all(|r| r.status.is_terminal()) {
            break;
        }
        assert!(Instant::now() < deadline, "not settled: {rows:#?}");
        h.clock.advance(Duration::from_secs(10));
        h.service.wake();
        idle(15).await;
    }

    // Nothing more ever happens, however long we wait.
    let settled_rows = all_rows(&h);
    let settled_requests = receiver.count();
    h.clock.advance(Duration::from_secs(86_400 * 30));
    h.service.wake();
    idle(300).await;
    assert_eq!(receiver.count(), settled_requests, "a settled delivery was sent again");
    assert_eq!(all_rows(&h), settled_rows, "a settled delivery changed");

    // The database: exactly the rows fired, each in its expected state.
    assert_eq!(settled_rows.len(), expected.len());
    let answered = receiver.answered();
    for row in &settled_rows {
        let id = row.delivery_id.as_str();
        let (status, attempts) = expected[id];
        assert_eq!((row.status, row.attempts), (status, attempts), "{row:?}");
        assert_eq!(row.delivered_at.is_some(), status == DeliveryStatus::Delivered, "{row:?}");
        if status == DeliveryStatus::Failed {
            // Failed by its own last attempt (the third reply is a 429), not
            // swept up later as interrupted.
            assert_eq!(row.last_status_code, Some(429), "{row:?}");
            assert!(row.last_error.as_deref().unwrap().starts_with("HTTP 429"), "{row:?}");
        }

        // The receiver, counted independently: one request per recorded
        // attempt, numbered 1..=n, and exactly one 2xx for a delivered row.
        let requests = receiver.for_delivery(id);
        let numbers: Vec<u32> = requests.iter().map(|r| r.attempt()).collect();
        assert_eq!(numbers, (1..=attempts).collect::<Vec<_>>(), "{id}");
        let successes = answered.iter().filter(|(d, s)| d == id && (200..300).contains(s)).count();
        assert_eq!(successes, usize::from(status == DeliveryStatus::Delivered), "{id}");
    }
    let seen_ids: HashSet<String> = receiver.received().iter().map(|r| r.delivery_id().to_owned()).collect();
    let row_ids: HashSet<String> = expected.keys().cloned().collect();
    assert_eq!(seen_ids, row_ids, "the receiver saw exactly the enqueued ids");

    let depth = h.service.depth().unwrap();
    let delivered = expected.values().filter(|(s, _)| *s == DeliveryStatus::Delivered).count() as u64;
    assert_eq!((depth.held, depth.pending), (0, 0));
    assert_eq!((depth.delivered, depth.failed), (delivered, expected.len() as u64 - delivered));
    assert!(delivered > 0 && delivered < expected.len() as u64, "both terminal states are exercised");
    h.service.shutdown().await;
}

// ===========================================================================
// Schedule
// ===========================================================================

/// One delivery that always gets a 503. After each failed attempt `n` the
/// next is due exactly `delays[n - 1]` later; one millisecond before that,
/// nothing is sent; at that instant, attempt `n + 1` is sent, stamped with
/// that instant. After the last attempt, nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retries_happen_exactly_on_schedule_and_stop_at_the_cap() {
    const DELAYS: [u64; 3] = [7, 19, 41];
    const MAX: u32 = 5;
    let receiver = Receiver::start(|_| Reply::Status(503)).await;
    let h = Harness::new(settings(MAX, &DELAYS));
    let alert = h.alert("a", &receiver.url("/hook"));
    let id = h.fire(&alert, 1, r#"{"n":1}"#, false);

    let mut sent_at = vec![];
    for n in 1..=MAX {
        eventually(&format!("attempt {n}"), || receiver.for_delivery(&id).len() == n as usize).await;
        let request = receiver.for_delivery(&id).pop().unwrap();
        assert_eq!(request.attempt(), n);
        assert_eq!(request.timestamp(), h.clock.get() / 1000, "stamped at send time");
        sent_at.push(h.clock.get());
        if n == MAX {
            break;
        }

        let row = h.retrying_after(&id, n).await;
        assert_eq!(row.last_attempt_at, Some(h.clock.get()));
        let delay = DELAYS[(n as usize - 1).min(DELAYS.len() - 1)] * 1000;
        assert_eq!(row.next_attempt_at - row.last_attempt_at.unwrap(), delay as i64, "after attempt {n}");
        assert_eq!(row.last_status_code, Some(503));

        // Not a millisecond early, whether woken or ticking.
        h.clock.set(row.next_attempt_at - 1);
        h.service.wake();
        idle(150).await;
        assert_eq!(receiver.for_delivery(&id).len(), n as usize, "attempt {} sent early", n + 1);

        h.clock.set(row.next_attempt_at);
    }

    // The measured gaps between attempts are the policy.
    let gaps: Vec<i64> = sent_at.windows(2).map(|w| (w[1] - w[0]) / 1000).collect();
    assert_eq!(gaps, [7, 19, 41, 41]);

    let row = h.settled(&id).await;
    assert_eq!((row.status, row.attempts), (DeliveryStatus::Failed, MAX));
    h.clock.advance(Duration::from_secs(86_400));
    h.service.wake();
    idle(200).await;
    assert_eq!(receiver.for_delivery(&id).len(), MAX as usize, "no attempt past the cap");
    h.service.shutdown().await;
}

/// With jitter, every measured gap stays inside the policy's band, and the
/// gaps are not all the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn jittered_retries_stay_within_their_band() {
    let receiver = Receiver::start(|_| Reply::Status(500)).await;
    let mut settings = settings(2, &[100]);
    settings.retry = RetryPolicy {
        delays: vec![Duration::from_secs(100)],
        jitter_pct: 20,
    };
    settings.max_in_flight = 8;
    let h = Harness::new(settings);
    let ids: Vec<String> = (0..16)
        .map(|i| {
            let alert = h.alert(&format!("a{i}"), &receiver.url(&format!("/hook/{i}")));
            h.fire(&alert, 1, "{}", false)
        })
        .collect();

    let mut gaps = HashSet::new();
    for id in &ids {
        let row = h.retrying_after(id, 1).await;
        let gap = row.next_attempt_at - row.last_attempt_at.unwrap();
        assert!((80_000..=120_000).contains(&gap), "gap {gap} ms outside 100 s ± 20 %");
        gaps.insert(gap);
    }
    assert!(gaps.len() > 1, "jitter must spread retries: {gaps:?}");
    h.service.shutdown().await;
}

// ===========================================================================
// Crashes
// ===========================================================================

/// The process dies after the commit but before `wake()` (or before any
/// dispatcher ever ran). The row is on disk, and the next process sends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delivery_committed_before_a_crash_is_sent_after_restart() {
    let receiver = Receiver::start(|_| Reply::Status(200)).await;
    let dir = TempDir::new();
    let (id, held_id) = {
        let registry = Registry::open_path(dir.path().join("forge.db")).unwrap();
        let alert = create_alert(&registry, "a", &receiver.url("/hook"), SECRET);
        registry
            .db()
            .transaction("fire", |tx| {
                Registry::advance_progress(tx, &alert, 2)?;
                let new = |slot: u64, hold: bool| NewDelivery {
                    alert_id: alert.clone(),
                    to_slot: slot,
                    payload: json!({"slot": slot}).to_string(),
                    release_after_slot: hold.then_some(slot),
                };
                let a = Outbox::enqueue(tx, &new(1, false), T0)?;
                let b = Outbox::enqueue(tx, &new(2, true), T0)?;
                Ok((a.delivery_id().to_owned(), b.delivery_id().to_owned()))
            })
            .unwrap()
        // Dropped here with no dispatcher and no wake: the "crash".
    };

    let registry = Registry::open_path(dir.path().join("forge.db")).unwrap();
    let h = Harness::on(dir, registry, settings(3, &[5]), ManualClock::new(T0));
    let row = h.settled(&id).await;
    assert_eq!((row.status, row.attempts), (DeliveryStatus::Delivered, 1));
    assert_eq!(receiver.for_delivery(&id).len(), 1);

    // The finality hold survived the restart too: still held, never sent...
    idle(150).await;
    assert_eq!(h.status(&held_id), DeliveryStatus::Held);
    assert!(receiver.for_delivery(&held_id).is_empty());
    // ...until its slot is final.
    assert_eq!(h.service.release_final(2).unwrap(), 1);
    assert_eq!(h.settled(&held_id).await.status, DeliveryStatus::Delivered);
    h.service.shutdown().await;
}

/// The process dies while a request is in flight: every task stops where it
/// is (the runtime is torn down under it), so no outcome is recorded. The
/// attempt was recorded before the send, so the row is `pending` with one
/// attempt and a lease; after a restart it is sent again once the lease runs
/// out, with the same delivery id, the next attempt number, the same body.
#[test]
fn a_send_interrupted_by_a_crash_is_retried_after_restart() {
    let receiver_rt = tokio::runtime::Runtime::new().unwrap();
    let receiver = receiver_rt.block_on(Receiver::start(|r| if r.nth == 0 { Reply::Hang } else { Reply::Status(200) }));
    let dir = TempDir::new();
    let registry = Registry::open_path(dir.path().join("forge.db")).unwrap();
    let db = registry.db().clone();
    let clock = ManualClock::new(T0);
    let alert = create_alert(&registry, "a", &receiver.url("/hook"), SECRET);

    let first_life = tokio::runtime::Runtime::new().unwrap();
    let (service, id) = first_life.block_on(async {
        let service = WebhookService::start_with(db.clone(), settings(3, &[5]), clock.clone()).unwrap();
        let new = NewDelivery {
            alert_id: alert.clone(),
            to_slot: 1,
            payload: r#"{"slot":1,"note":"in flight at the crash"}"#.into(),
            release_after_slot: None,
        };
        let id = db.transaction("fire", |tx| service.enqueue(tx, &new)).unwrap().delivery_id().to_owned();
        service.wake();
        eventually("the first attempt to reach the receiver", || receiver.count() == 1).await;
        (service, id)
    });
    first_life.shutdown_background();
    drop(service);

    let outbox = Outbox::new(db.clone(), 3, Duration::from_millis(LEASE_MS as u64)).unwrap();
    let row = outbox.get(&id).unwrap();
    assert_eq!(row.status, DeliveryStatus::Pending);
    assert_eq!((row.attempts, row.last_attempt_at, row.next_attempt_at), (1, Some(T0), T0 + LEASE_MS));
    assert_eq!((row.delivered_at, row.last_status_code, row.last_error), (None, None, None));

    let second_life = tokio::runtime::Runtime::new().unwrap();
    second_life.block_on(async {
        let service = WebhookService::start_with(db.clone(), settings(3, &[5]), clock.clone()).unwrap();
        idle(150).await;
        assert_eq!(receiver.count(), 1, "the lease still holds the row");

        clock.set(T0 + LEASE_MS);
        service.wake();
        eventually("redelivery", || outbox.get(&id).unwrap().status.is_terminal()).await;
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.status, row.attempts), (DeliveryStatus::Delivered, 2));

        let requests = receiver.for_delivery(&id);
        assert_eq!(requests.iter().map(|r| r.attempt()).collect::<Vec<_>>(), [1, 2]);
        let stored = stored_payload_bytes(&registry, &id);
        assert!(requests.iter().all(|r| r.body == stored && r.signature_is_valid(SECRET)));
        service.shutdown().await;
    });
}

const CRASH_DB_VAR: &str = "FORGE_WEBHOOK_CRASH_DB";

/// Child half of [`deliveries_survive_a_killed_process`]: runs only when
/// re-invoked with [`CRASH_DB_VAR`] set, and dispatches until SIGKILLed.
#[test]
fn crash_child() {
    let Some(db_path) = std::env::var_os(CRASH_DB_VAR) else {
        return;
    };
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let registry = Registry::open_path(db_path).unwrap();
        let _service = WebhookService::start_with(registry.db().clone(), settings(5, &[1]), Arc::new(SystemClock))
            .unwrap();
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
}

/// A real FORGE process, mid-way through 20 deliveries with 4 in flight, is
/// SIGKILLed: no destructor, no rollback, no flush. A new process on the
/// same file delivers every one of them. Rows the dead process recorded as
/// delivered are never sent again; only sends that were in flight at the
/// kill can reach the receiver twice.
#[test]
fn deliveries_survive_a_killed_process() {
    const ALERTS: u64 = 4;
    const SLOTS: u64 = 5;
    let receiver_rt = tokio::runtime::Runtime::new().unwrap();
    let receiver = receiver_rt.block_on(Receiver::start(|_| Reply::After(Duration::from_millis(150), 200)));
    let dir = TempDir::new();
    let db_path = dir.path().join("forge.db");

    let mut ids = vec![];
    {
        let registry = Registry::open_path(&db_path).unwrap();
        for a in 0..ALERTS {
            let alert = create_alert(&registry, &format!("a{a}"), &receiver.url(&format!("/hook/{a}")), SECRET);
            for slot in 1..=SLOTS {
                let new = NewDelivery {
                    alert_id: alert.clone(),
                    to_slot: slot,
                    payload: json!({"alert": a, "slot": slot}).to_string(),
                    release_after_slot: None,
                };
                let now = forge::Clock::now_ms(&SystemClock);
                let id = registry.db().transaction("fire", |tx| Outbox::enqueue(tx, &new, now)).unwrap();
                ids.push(id.delivery_id().to_owned());
            }
        }
    }

    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--test-threads=1"])
        .env(CRASH_DB_VAR, &db_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while receiver.count() < 6 {
        assert!(Instant::now() < deadline, "the child never got going");
        std::thread::sleep(Duration::from_millis(5));
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let seen_before_kill = receiver.count();

    // What the dead process left behind, read before anything else runs:
    // every row, none lost.
    let registry = Registry::open_path(&db_path).unwrap();
    let outbox = Outbox::new(registry.db().clone(), 5, Duration::from_millis(LEASE_MS as u64)).unwrap();
    let before: HashMap<String, forge::Delivery> =
        ids.iter().map(|id| (id.clone(), outbox.get(id).unwrap())).collect();
    let delivered_before: Vec<&String> =
        ids.iter().filter(|id| before[*id].status == DeliveryStatus::Delivered).collect();
    let interrupted: Vec<&String> = ids
        .iter()
        .filter(|id| before[*id].status == DeliveryStatus::Pending && before[*id].attempts > 0)
        .collect();
    assert!(
        interrupted.len() <= 4,
        "at most max_in_flight sends were open: {:#?}",
        interrupted.iter().map(|id| &before[*id]).collect::<Vec<_>>()
    );

    // Restart once every lease has run out.
    let restart_at = forge::Clock::now_ms(&SystemClock) + LEASE_MS;
    let _runtime = receiver_rt.enter();
    let h = Harness::on(dir, registry, settings(5, &[1]), ManualClock::new(restart_at));
    assert_eq!(all_rows(&h).len(), ids.len(), "no row lost, none invented");
    h.service.wake();
    for id in &ids {
        assert_eq!(receiver_rt.block_on(h.settled(id)).status, DeliveryStatus::Delivered, "{id}");
    }

    let mut twice = 0;
    for id in &ids {
        let requests = receiver.for_delivery(id);
        let row = h.get(id);
        assert!(!requests.is_empty(), "{id} never reached the receiver");
        assert!(requests.len() as u32 <= row.attempts, "{id}: more requests than recorded attempts");
        let numbers: Vec<u32> = requests.iter().map(|r| r.attempt()).collect();
        assert!(numbers.windows(2).all(|w| w[0] < w[1]), "{id}: attempts {numbers:?}");
        if delivered_before.contains(&id) {
            assert_eq!(requests.len(), 1, "{id} was delivered before the kill and sent again");
        }
        if requests.len() > 1 {
            assert!(interrupted.contains(&id), "{id} was sent twice but was not in flight at the kill");
            twice += 1;
        }
    }
    assert!(twice <= 4, "{twice} duplicates");
    assert!(seen_before_kill >= 6 && seen_before_kill < ids.len() + 4, "{seen_before_kill}");
    receiver_rt.block_on(h.service.shutdown());
}

// ===========================================================================
// Signatures
// ===========================================================================

/// The test's own HMAC, checked against RFC 4231 before it checks FORGE.
#[test]
fn the_independent_hmac_passes_rfc_4231() {
    let unhex = |s: &str| (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect::<Vec<u8>>();
    // Test case 1.
    assert_eq!(
        to_hex(&hmac_sha256(&[0x0b; 20], b"Hi There")),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
    // Test case 2: a key shorter than the block.
    assert_eq!(
        to_hex(&hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    // Test case 4.
    assert_eq!(
        to_hex(&hmac_sha256(&unhex("0102030405060708090a0b0c0d0e0f10111213141516171819"), &[0xcd; 50])),
        "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b"
    );
    // Test case 6: a key longer than the block is hashed first.
    assert_eq!(
        to_hex(&hmac_sha256(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First")),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

/// Three alerts with very different secrets (minimum length, maximum
/// length, non-ASCII), three deliveries each, two attempts each (the first
/// gets a 500). Every one of the 18 requests verifies under the independent
/// HMAC, over its own timestamp header and its exact body bytes, and under
/// `forge::signer::verify`; none verifies under another alert's secret. A
/// retry carries a new timestamp and so a new signature.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_request_carries_a_signature_an_independent_implementation_accepts() {
    let receiver = Receiver::start(|r| Reply::Status(if r.nth == 0 { 500 } else { 200 })).await;
    let h = Harness::new(settings(3, &[3]));
    let long_secret: String = (0..512).map(|i| (b'!' + (i % 90) as u8) as char).collect();
    let secrets = ["0123456789abcdef".to_owned(), long_secret, "ключ-🔑-sécret-$%^&*()".to_owned()];

    let mut by_path = HashMap::new();
    let mut ids = vec![];
    for (i, secret) in secrets.iter().enumerate() {
        let path = format!("/hook/{i}");
        let alert = h.alert_with_secret(&format!("a{i}"), &receiver.url(&path), secret);
        by_path.insert(path, secret.clone());
        for slot in 1..=3 {
            ids.push(h.fire(&alert, slot, &json!({"i": i, "slot": slot, "s": "é🚀"}).to_string(), false));
        }
    }
    for id in &ids {
        h.retrying_after(id, 1).await;
    }
    h.clock.advance(Duration::from_millis(3_500));
    h.service.wake();
    for id in &ids {
        assert_eq!(h.settled(id).await.status, DeliveryStatus::Delivered);
    }

    let requests = receiver.received();
    assert_eq!(requests.len(), 18);
    for r in &requests {
        let secret = &by_path[&r.path];
        assert_eq!(r.header_count("x-forge-signature"), 1);
        assert!(r.signature_is_valid(secret), "independent HMAC rejected {r:?}");
        let signature = r.header("x-forge-signature").unwrap();
        signer::verify(secret.as_bytes(), r.timestamp(), &r.body, signature, h.clock.get() / 1000, signer::DEFAULT_TOLERANCE)
            .unwrap();
        for other in secrets.iter().filter(|s| *s != secret) {
            assert!(!r.signature_is_valid(other), "verified under the wrong secret");
        }
    }
    for id in &ids {
        let [first, second] = receiver.for_delivery(id).try_into().unwrap();
        assert_eq!((first.timestamp(), second.timestamp()), (T0 / 1000, (T0 + 3_500) / 1000));
        assert_ne!(first.header("x-forge-signature"), second.header("x-forge-signature"));
    }
    h.service.shutdown().await;
}

// ===========================================================================
// Bodies
// ===========================================================================

/// Payloads that a re-serializer would change in some way (whitespace,
/// escapes, number spellings, duplicate keys, non-ASCII, size) arrive
/// exactly as stored, on the first attempt and on a retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_body_is_the_stored_payload_byte_for_byte() {
    let large = format!(
        r#"{{"rows":[{}]}}"#,
        (0..20_000).map(|i| format!(r#"{{"slot":{i},"memo":"ünïcødé ☃ {i}"}}"#)).collect::<Vec<_>>().join(",")
    );
    let payloads = [
        r#"{"a":1}"#.to_owned(),
        "  {\"spaced\" :  [ 1 , 2 ] }\n".to_owned(),
        "{\r\n\t\"crlf\": true\r\n}".to_owned(),
        r#"{"unicode":"héllo wörld — 日本語 🚀","escaped":"é\n\t\"q\"\\ \/"}"#.to_owned(),
        r#"{"num":1.0e+10,"neg":-0,"big":123456789012345678901234567890,"f":0.1000}"#.to_owned(),
        r#"[{"dup":1,"dup":2}]"#.to_owned(),
        r#""just a string""#.to_owned(),
        large,
    ];
    let receiver = Receiver::start(|r| Reply::Status(if r.nth == 0 { 502 } else { 200 })).await;
    let h = Harness::new(settings(2, &[1]));
    let alert = h.alert("a", &receiver.url("/hook"));
    let ids: Vec<String> = payloads
        .iter()
        .enumerate()
        .map(|(slot, payload)| h.fire(&alert, slot as u64 + 1, payload, false))
        .collect();

    let deadline = Instant::now() + Duration::from_secs(20);
    while !ids.iter().all(|id| h.status(id).is_terminal()) {
        assert!(Instant::now() < deadline, "not settled");
        h.clock.advance(Duration::from_secs(1));
        h.service.wake();
        idle(15).await;
    }

    for (id, payload) in ids.iter().zip(&payloads) {
        assert_eq!(h.status(id), DeliveryStatus::Delivered);
        let stored = h.stored_payload_bytes(id);
        assert_eq!(stored, payload.as_bytes(), "the outbox stored what it was given");
        let requests = receiver.for_delivery(id);
        assert_eq!(requests.len(), 2);
        for r in &requests {
            assert!(r.body == stored, "attempt {} of {id}: body differs from the stored payload", r.attempt());
            assert_eq!(r.header("content-length"), Some(stored.len().to_string().as_str()));
            assert_eq!(r.header("content-type"), Some("application/json"));
            assert_eq!(r.header("content-encoding"), None);
        }
    }
    h.service.shutdown().await;
}

#[test]
fn settings_used_here_are_valid() {
    settings(5, &[1]).validate().unwrap();
    assert!(WebhookSettings { lease: Duration::from_secs(2), ..settings(1, &[1]) }.validate().is_err());
}
