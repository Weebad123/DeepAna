//! The HTTP API, end to end through the router: a scripted LENS, and a real
//! registry file, change log reader, webhook service and alert engine.
//!
//! The suite has three parts:
//!
//! * **Contract**: every route answers its documented status with its
//!   documented body, field for field.
//! * **Bad input**: malformed, mistyped, unknown, duplicated or invalid
//!   input is rejected with LENS's error shape, before anything is written
//!   and before LENS is asked.
//! * **Secrets**: an alert's secret appears in exactly one response, the
//!   `201` that created it, and in no other body, error or log line.

mod api_support;
mod common;
mod webhook_support;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use api_support::*;
use axum::http::{Method, StatusCode};
use forge::{
    DeliveryStatus, LensCallError, LensHealth, LensHealthStatus, MAX_BODY_BYTES, Pass, VISIBILITY_SQL,
};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use webhook_support::{Receiver, Reply as Hook, eventually};

/// The saved query from the README's curl example, byte for byte.
fn transfers_by_mint() -> Value {
    json!({
        "name": "transfers-by-mint",
        "sql": "SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_address = {{mint}} ORDER BY slot DESC LIMIT {{n}}",
        "params": [
            {"name": "mint", "kind": "string", "required": true},
            {"name": "n", "kind": "u64", "required": false, "default": "10"}
        ]
    })
}

const SECRET: &str = "whsec-api-test-0123456789";
const OTHER_SECRET: &str = "whsec-rotated-9876543210";
const HOOK_URL: &str = "https://hooks.example.com/forge";

fn bound(mint_literal: &str, n: u64) -> String {
    format!(
        "SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_address = {mint_literal} ORDER BY slot DESC LIMIT {n}"
    )
}

/// `value` without `created_at` / `updated_at`, which are clock readings.
fn without_times(mut value: Value) -> Value {
    let object = value.as_object_mut().unwrap();
    let created = object.remove("created_at").and_then(|v| v.as_i64()).expect("created_at");
    let updated = object.remove("updated_at").and_then(|v| v.as_i64()).expect("updated_at");
    assert!(created > 1_700_000_000 && updated >= created, "{created} {updated}");
    value
}

fn keys(value: &Value) -> Vec<&str> {
    let mut keys: Vec<&str> = value.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys
}

/// A change log line announcing `slot`, as PRISM writes it.
fn announce(api: &Api, id: u64, slot: u64) {
    let line = json!({
        "id": id, "emitted_at": 1_730_000_000, "chain": "solana-devnet", "event": "new", "slot": slot,
        "blockhash": format!("h{slot}"), "parent_slot": slot - 1, "block_time": 1_730_000_000,
        "transaction_count": 3, "parquet_paths": []
    });
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(api.changelog_path()).unwrap();
    writeln!(file, "{line}").unwrap();
}

// ===========================================================================
// Contract: saved queries
// ===========================================================================

#[tokio::test]
async fn post_queries_answers_201_with_a_location_and_the_stored_query() {
    let api = Api::new();
    let reply = api.post("/queries", &transfers_by_mint()).await;

    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text());
    assert_eq!(reply.header("location"), "/queries/transfers-by-mint");
    assert_eq!(reply.header("content-type"), "application/json");
    assert_eq!(
        without_times(reply.json()),
        json!({
            "name": "transfers-by-mint",
            "sql": transfers_by_mint()["sql"],
            "params": [
                {"name": "mint", "kind": "string", "required": true, "default": null},
                {"name": "n", "kind": "u64", "required": false, "default": "10"}
            ]
        })
    );
    assert!(api.lens.queries().is_empty(), "saving a query does not run it");
    assert_eq!(api.registry.get_query("transfers-by-mint").unwrap().params.len(), 2);
}

#[tokio::test]
async fn get_queries_lists_get_query_reads_and_delete_query_answers_204() {
    let api = Api::new();
    let created = api.post("/queries", &transfers_by_mint()).await.json();
    let other = json!({"name": "a-first", "sql": "SELECT 1"});
    assert_eq!(api.post("/queries", &other).await.status, StatusCode::CREATED);

    let one = api.get("/queries/transfers-by-mint").await;
    assert_eq!(one.status, StatusCode::OK);
    assert_eq!(one.json(), created);

    let list = api.get("/queries").await;
    assert_eq!(list.status, StatusCode::OK);
    let list = list.json();
    assert_eq!(keys(&list), ["queries"]);
    let names: Vec<&str> = list["queries"].as_array().unwrap().iter().map(|q| q["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["a-first", "transfers-by-mint"], "by name");

    let deleted = api.delete("/queries/transfers-by-mint").await;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    assert!(deleted.body.is_empty());
    assert_eq!(api.get("/queries/transfers-by-mint").await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
    assert_eq!(api.delete("/queries/transfers-by-mint").await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
}

// ===========================================================================
// Contract: running a saved query
// ===========================================================================

#[tokio::test]
async fn get_q_binds_parameters_and_returns_lens_envelope_with_the_query_name() {
    let api = Api::new();
    api.post("/queries", &transfers_by_mint()).await;
    api.lens.set_rows(json!([{ "tx_signature": "sig1", "amount_raw": u64::MAX }]));

    // A quote in the value is doubled, never spliced in raw.
    let reply = api.get("/q/transfers-by-mint?mint=So1%27%3B+DROP&n=5").await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    let sql = bound("'So1''; DROP'", 5);
    assert_eq!(api.lens.queries(), [sql.as_str()]);
    assert_eq!(
        reply.json(),
        json!({
            "query": "transfers-by-mint",
            "rows": [{ "tx_signature": "sig1", "amount_raw": u64::MAX }],
            "row_count": 1,
            "duration_ms": 12,
            "truncated": false,
            "sql": sql
        })
    );
    // Full u64 precision survives the round trip.
    assert_eq!(reply.json()["rows"][0]["amount_raw"].as_u64(), Some(u64::MAX));

    // An absent optional parameter takes its default.
    let reply = api.get("/q/transfers-by-mint?mint=abc").await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.json()["sql"], json!(bound("'abc'", 10)));
}

#[tokio::test]
async fn get_q_rejects_bad_parameters_before_asking_lens() {
    let api = Api::new();
    api.post("/queries", &transfers_by_mint()).await;
    for (uri, status, kind) in [
        ("/q/transfers-by-mint?mint=a&mint=b", 400, "invalid_request"),
        ("/q/transfers-by-mint?mint=a&n=1&n=2", 400, "invalid_request"),
        ("/q/transfers-by-mint?mint=%zz", 400, "invalid_request"),
        ("/q/transfers-by-mint?mint=%FF", 400, "invalid_request"),
        ("/q/transfers-by-mint?mint", 400, "invalid_request"),
        ("/q/transfers-by-mint?mint=a&typo=1", 400, "validation_error"),
        ("/q/transfers-by-mint", 400, "validation_error"),
        ("/q/transfers-by-mint?n=5", 400, "validation_error"),
        ("/q/transfers-by-mint?mint=a&n=-1", 400, "validation_error"),
        ("/q/transfers-by-mint?mint=a&n=1.5", 400, "validation_error"),
        ("/q/transfers-by-mint?mint=a&n=18446744073709551616", 400, "validation_error"),
        ("/q/no-such-query?mint=a", 404, "not_found"),
    ] {
        let reply = api.get(uri).await;
        let error = reply.error();
        assert_eq!((reply.status.as_u16(), error.error_kind.as_str()), (status, kind), "{uri}: {}", error.error);
        assert_eq!(error.sql, None, "{uri}");
    }
    let reply = api.get("/q/transfers-by-mint?mint=a&typo=1").await;
    assert_eq!(reply.error().error, "invalid typo: is not a parameter of this query");
    assert!(api.lens.queries().is_empty(), "{:?}", api.lens.queries());
}

#[tokio::test]
async fn get_q_reports_lens_failures_with_their_status_and_the_bound_sql() {
    let api = Api::new();
    api.post("/queries", &transfers_by_mint()).await;
    let sql = bound("'abc'", 10);
    for (err, status, kind) in [
        (
            LensCallError::BadQuery { message: "query_plan_error: Schema error: No field named mint_address".into() },
            StatusCode::BAD_REQUEST,
            "lens_bad_query",
        ),
        (LensCallError::Protocol { message: "not JSON".into() }, StatusCode::BAD_GATEWAY, "lens_protocol"),
        (LensCallError::Unavailable { message: "refused".into() }, StatusCode::SERVICE_UNAVAILABLE, "lens_unavailable"),
        (LensCallError::Timeout { message: "504".into() }, StatusCode::GATEWAY_TIMEOUT, "lens_timeout"),
    ] {
        api.lens.clear_failures();
        api.lens.fail_when("token_transfers", err.clone());
        let reply = api.get("/q/transfers-by-mint?mint=abc").await;
        let error = reply.error();
        assert_eq!((reply.status, error.error_kind.as_str()), (status, kind));
        assert_eq!(error.error, err.to_string());
        assert_eq!(error.sql.as_deref(), Some(sql.as_str()));
    }
}

// ===========================================================================
// Contract: alerts
// ===========================================================================

#[tokio::test]
async fn post_alerts_validates_dry_runs_creates_seeds_and_answers_201_with_the_secret() {
    let api = Api::new();
    api.lens.set_max_slot(Some(295_000_999));
    let mut body = alert_body("whales", HOOK_URL, Some(SECRET));
    body["wait_for_final"] = json!(true);
    let reply = api.post("/alerts", &body).await;

    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text());
    let created = reply.json();
    let id = created["id"].as_str().unwrap().to_owned();
    assert_eq!(reply.header("location"), format!("/alerts/{id}"));
    assert_eq!(id.len(), 36, "a UUID: {id}");
    assert_eq!(
        without_times(created.clone()),
        json!({
            "id": id,
            "name": "whales",
            "sql": ALERT_SQL,
            "webhook_url": HOOK_URL,
            "wait_for_final": true,
            "enabled": true,
            "last_evaluated_slot": 295_000_999,
            "secret": SECRET
        })
    );
    // Dry run over 0..=0 first, then the seed; nothing else.
    assert_eq!(api.lens.queries(), [ALERT_DRY_RUN, VISIBILITY_SQL]);
    let stored = api.registry.get_alert(&id).unwrap();
    assert_eq!((stored.last_evaluated_slot, stored.secret.as_str()), (Some(295_000_999), SECRET));

    // Every later read is the same alert, without the secret.
    let mut view = created.clone();
    view.as_object_mut().unwrap().remove("secret");
    let one = api.get(&format!("/alerts/{id}")).await;
    assert_eq!(one.status, StatusCode::OK);
    assert_eq!(one.json(), view);
    let list = api.get("/alerts").await;
    assert_eq!(list.status, StatusCode::OK);
    assert_eq!(list.json(), json!({ "alerts": [view] }));
}

#[tokio::test]
async fn post_alerts_without_a_secret_generates_the_one_deliveries_are_signed_with() {
    let api = Api::new();
    let receiver = Receiver::start(|_| Hook::Status(200)).await;
    let reply = api.post("/alerts", &alert_body("generated", &receiver.url("/hook"), None)).await;
    assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.text());
    let secret = reply.json()["secret"].as_str().unwrap().to_owned();
    assert_eq!(secret.len(), 64);
    assert!(secret.bytes().all(|b| b.is_ascii_hexdigit()), "{secret}");

    let id = reply.json()["id"].as_str().unwrap().to_owned();
    api.enqueue(&id, 7, false);
    eventually("the delivery", || receiver.count() == 1).await;
    assert!(receiver.received()[0].signature_is_valid(&secret), "signed with the secret the API returned");

    let again = api.post("/alerts", &alert_body("generated-2", &receiver.url("/hook"), None)).await;
    assert_ne!(again.json()["secret"], json!(secret), "each alert gets its own");
}

#[tokio::test]
async fn post_alerts_stops_at_the_first_failure_and_leaves_nothing_behind() {
    let api = Api::new();

    // 1. Validation: LENS is not asked.
    let reply = api.post("/alerts", &alert_body("a", "ftp://hooks.example.com", Some(SECRET))).await;
    assert_eq!(reply.kind(), (StatusCode::BAD_REQUEST, "validation_error".into()));
    let mut no_bounds = alert_body("a", HOOK_URL, Some(SECRET));
    no_bounds["sql"] = json!("SELECT * FROM solana.slots WHERE slot <= {{to_slot}}");
    assert_eq!(api.post("/alerts", &no_bounds).await.kind(), (StatusCode::BAD_REQUEST, "validation_error".into()));
    assert!(api.lens.queries().is_empty());

    // 2. Dry run: LENS rejects the SQL, or is down.
    api.lens.fail_when("BETWEEN 0 AND 0", LensCallError::BadQuery { message: "No field named amount_raw".into() });
    let reply = api.post("/alerts", &alert_body("a", HOOK_URL, Some(SECRET))).await;
    let error = reply.error();
    assert_eq!((reply.status, error.error_kind.as_str()), (StatusCode::BAD_REQUEST, "lens_bad_query"));
    assert_eq!(error.sql.as_deref(), Some(ALERT_DRY_RUN));
    api.lens.clear_failures();
    api.lens.fail_when("BETWEEN 0 AND 0", LensCallError::Unavailable { message: "refused".into() });
    assert_eq!(
        api.post("/alerts", &alert_body("a", HOOK_URL, Some(SECRET))).await.kind(),
        (StatusCode::SERVICE_UNAVAILABLE, "lens_unavailable".into())
    );
    assert_eq!(api.lens.queries(), [ALERT_DRY_RUN, ALERT_DRY_RUN], "no seed after a failed dry run");
    assert!(api.registry.list_alerts().unwrap().is_empty());

    // 3. Create: the name is taken. Not seeded.
    api.lens.clear_failures();
    api.alert("taken", HOOK_URL, SECRET).await;
    let before = api.lens.queries().len();
    let reply = api.post("/alerts", &alert_body("taken", HOOK_URL, Some(SECRET))).await;
    assert_eq!(reply.kind(), (StatusCode::CONFLICT, "conflict".into()));
    assert_eq!(api.lens.queries()[before..], [ALERT_DRY_RUN]);

    // 4. Seed: LENS fails; the alert just created is removed again.
    api.lens.fail_when("MAX(slot)", LensCallError::Timeout { message: "slow".into() });
    let reply = api.post("/alerts", &alert_body("unseeded", HOOK_URL, Some(SECRET))).await;
    let error = reply.error();
    assert_eq!((reply.status, error.error_kind.as_str()), (StatusCode::GATEWAY_TIMEOUT, "lens_timeout"));
    assert_eq!(error.sql.as_deref(), Some(VISIBILITY_SQL));
    let names: Vec<String> = api.registry.list_alerts().unwrap().into_iter().map(|a| a.name).collect();
    assert_eq!(names, ["taken"]);
}

#[tokio::test]
async fn post_alerts_on_an_empty_slots_table_leaves_the_baseline_to_the_engine() {
    let api = Api::new();
    api.lens.set_max_slot(None);
    let reply = api.post("/alerts", &alert_body("early", HOOK_URL, Some(SECRET))).await;
    assert_eq!(reply.status, StatusCode::CREATED);
    assert_eq!(reply.json()["last_evaluated_slot"], Value::Null);
}

#[tokio::test]
async fn patch_alerts_updates_what_is_set_and_dry_runs_changed_sql() {
    let api = Api::new();
    let id = api.alert("whales", HOOK_URL, SECRET).await;
    let uri = format!("/alerts/{id}");
    let asked = || api.lens.queries().len();

    // Plain fields: no LENS call, progress kept.
    let before = asked();
    let reply = api.patch(&uri, &json!({ "name": "renamed", "wait_for_final": true })).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    let view = reply.json();
    assert_eq!((view["name"].clone(), view["wait_for_final"].clone()), (json!("renamed"), json!(true)));
    assert_eq!(view["last_evaluated_slot"], json!(295_000_999));
    assert_eq!(keys(&view), ["created_at", "enabled", "id", "last_evaluated_slot", "name", "sql", "updated_at", "wait_for_final", "webhook_url"]);
    assert_eq!(asked(), before);

    // New SQL is dry-run; the same SQL again is not.
    let new_sql = "SELECT * FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";
    let reply = api.patch(&uri, &json!({ "sql": new_sql })).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(api.lens.queries().last().unwrap(), "SELECT * FROM solana.slots WHERE slot BETWEEN 0 AND 0");
    let before = asked();
    assert_eq!(api.patch(&uri, &json!({ "sql": new_sql })).await.status, StatusCode::OK);
    assert_eq!(asked(), before);

    // Disabling keeps progress; re-enabling clears it.
    let reply = api.patch(&uri, &json!({ "enabled": false })).await;
    assert_eq!((reply.json()["enabled"].clone(), reply.json()["last_evaluated_slot"].clone()), (json!(false), json!(295_000_999)));
    let reply = api.patch(&uri, &json!({ "enabled": true })).await;
    assert_eq!((reply.json()["enabled"].clone(), reply.json()["last_evaluated_slot"].clone()), (json!(true), Value::Null));

    // A rotated secret is stored, and not echoed.
    let reply = api.patch(&uri, &json!({ "secret": OTHER_SECRET })).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert!(!reply.text().contains(OTHER_SECRET));
    assert_eq!(api.registry.get_alert(&id).unwrap().secret, OTHER_SECRET);
}

#[tokio::test]
async fn patch_alerts_rejects_bad_updates_without_writing_or_asking_lens_needlessly() {
    let api = Api::new();
    let id = api.alert("whales", HOOK_URL, SECRET).await;
    api.alert("taken", HOOK_URL, SECRET).await;
    let uri = format!("/alerts/{id}");
    let stored = api.registry.get_alert(&id).unwrap();
    let asked = api.lens.queries().len();

    for (body, status, kind) in [
        (json!({}), 400, "validation_error"),
        (json!({ "webhook_url": "ftp://x", "sql": "SELECT 1 WHERE {{from_slot}} < {{to_slot}}" }), 400, "validation_error"),
        (json!({ "sql": "SELECT '{{from_slot}}', {{to_slot}}" }), 400, "validation_error"),
        (json!({ "secret": "short" }), 400, "validation_error"),
        (json!({ "name": "1-starts-with-a-digit" }), 400, "validation_error"),
        (json!({ "name": "taken" }), 409, "conflict"),
        (json!({ "enabled": "yes" }), 422, "invalid_request"),
        (json!({ "last_evaluated_slot": 0 }), 422, "invalid_request"),
        (json!({ "id": "other" }), 422, "invalid_request"),
    ] {
        let reply = api.patch(&uri, &body).await;
        let error = reply.error();
        assert_eq!((reply.status.as_u16(), error.error_kind.as_str()), (status, kind), "{body}: {}", error.error);
    }
    assert_eq!(api.lens.queries().len(), asked, "nothing above reached the dry run");

    // LENS rejects the new SQL: nothing is written.
    api.lens.fail_when("solana.nope", LensCallError::BadQuery { message: "table 'solana.nope' not found".into() });
    let reply = api.patch(&uri, &json!({ "sql": "SELECT * FROM solana.nope WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "name": "moved" })).await;
    assert_eq!(reply.kind(), (StatusCode::BAD_REQUEST, "lens_bad_query".into()));
    assert_eq!(api.registry.get_alert(&id).unwrap(), stored, "unchanged");

    assert_eq!(api.patch("/alerts/no-such-id", &json!({ "name": "x" })).await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
}

#[tokio::test]
async fn delete_alerts_answers_204_and_takes_the_deliveries_with_it() {
    let api = Api::new();
    let id = api.alert("whales", HOOK_URL, SECRET).await;
    let delivery = api.enqueue(&id, 10, true);

    let reply = api.delete(&format!("/alerts/{id}")).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT);
    assert!(reply.body.is_empty());
    assert_eq!(api.get(&format!("/alerts/{id}")).await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
    assert_eq!(api.delete(&format!("/alerts/{id}")).await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
    assert!(api.webhook.outbox().get(&delivery).is_err(), "cascaded");
}

// ===========================================================================
// Contract: deliveries
// ===========================================================================

/// An alert whose receiver fails every send, with deliveries for slots 1
/// and 2 (both `failed`, one attempt each) and slot 3 (`held`).
async fn alert_with_history(api: &Api, receiver: &Receiver) -> (String, [String; 3]) {
    let id = api.alert("history", &receiver.url("/hook"), SECRET).await;
    let one = api.enqueue(&id, 1, false);
    let two = api.enqueue(&id, 2, false);
    let three = api.enqueue(&id, 3, true);
    eventually("both sends to fail", || {
        [&one, &two].iter().all(|d| api.delivery(d).status == DeliveryStatus::Failed)
    })
    .await;
    (id, [one, two, three])
}

#[tokio::test]
async fn get_deliveries_lists_metadata_newest_first_filtered_by_status_and_limit() {
    let api = Api::new();
    let receiver = Receiver::start(|_| Hook::Status(500)).await;
    let (id, [one, two, three]) = alert_with_history(&api, &receiver).await;
    let ids = |reply: &Reply| -> Vec<String> {
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
        assert_eq!(reply.json()["alert_id"], json!(id));
        reply.json()["deliveries"].as_array().unwrap().iter().map(|d| d["delivery_id"].as_str().unwrap().to_owned()).collect()
    };
    let uri = format!("/alerts/{id}/deliveries");

    let all = api.get(&uri).await;
    assert_eq!(ids(&all), [three.clone(), two.clone(), one.clone()]);
    assert_eq!(ids(&api.get(&format!("{uri}?limit=1")).await), [three.as_str()]);
    assert_eq!(ids(&api.get(&format!("{uri}?status=failed")).await), [two.clone(), one.clone()]);
    assert_eq!(ids(&api.get(&format!("{uri}?status=failed&limit=1")).await), [two.as_str()]);
    assert_eq!(ids(&api.get(&format!("{uri}?status=held")).await), [three.as_str()]);
    assert!(ids(&api.get(&format!("{uri}?status=delivered")).await).is_empty());

    // Metadata only, field for field.
    let failed = &all.json()["deliveries"][1];
    let stored = api.delivery(&two);
    assert_eq!(
        *failed,
        json!({
            "delivery_id": two,
            "alert_id": id,
            "to_slot": 2,
            "status": "failed",
            "release_after_slot": null,
            "attempts": 1,
            "last_status_code": 500,
            "last_error": stored.last_error,
            "payload_bytes": stored.payload.len(),
            "created_at": stored.created_at,
            "next_attempt_at": stored.next_attempt_at,
            "last_attempt_at": stored.last_attempt_at,
            "delivered_at": null
        })
    );
    assert!(!all.text().contains("payload body"), "payloads are not returned: {}", all.text());
    assert_eq!(all.json()["deliveries"][0]["release_after_slot"], json!(3));
}

#[tokio::test]
async fn get_deliveries_rejects_bad_parameters_and_unknown_alerts() {
    let api = Api::new();
    let id = api.alert("whales", HOOK_URL, SECRET).await;
    for (query, status, kind) in [
        ("limit=0", 400, "validation_error"),
        ("limit=1001", 400, "validation_error"),
        ("limit=abc", 400, "validation_error"),
        ("limit=", 400, "validation_error"),
        ("limit=-1", 400, "validation_error"),
        ("status=bogus", 400, "validation_error"),
        ("status=FAILED", 400, "validation_error"),
        ("limit=1&limit=2", 400, "invalid_request"),
        ("status=held&status=failed", 400, "invalid_request"),
        ("offset=5", 400, "invalid_request"),
        ("limit=%zz", 400, "invalid_request"),
    ] {
        let reply = api.get(&format!("/alerts/{id}/deliveries?{query}")).await;
        let error = reply.error();
        assert_eq!((reply.status.as_u16(), error.error_kind.as_str()), (status, kind), "{query}: {}", error.error);
    }
    assert_eq!(api.get("/alerts/no-such-id/deliveries").await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
    let reply = api.get(&format!("/alerts/{id}/deliveries?limit=1000")).await;
    assert_eq!(reply.status, StatusCode::OK);
}

#[tokio::test]
async fn post_replay_answers_202_for_a_failed_delivery_only() {
    let api = Api::new();
    let receiver = Receiver::start(|_| Hook::Status(500)).await;
    let (_, [one, _, held]) = alert_with_history(&api, &receiver).await;
    receiver.set_script(|_| Hook::Status(200));

    let reply = api.post_empty(&format!("/deliveries/{one}/replay")).await;
    assert_eq!(reply.status, StatusCode::ACCEPTED, "{}", reply.text());
    let body = reply.json();
    assert_eq!((body["delivery_id"].clone(), body["status"].clone(), body["attempts"].clone()), (json!(one), json!("pending"), json!(0)));
    assert!(body.get("payload").is_none());
    eventually("the replayed send", || api.delivery(&one).status == DeliveryStatus::Delivered).await;

    // Delivered, held, unknown.
    assert_eq!(api.post_empty(&format!("/deliveries/{one}/replay")).await.kind(), (StatusCode::CONFLICT, "conflict".into()));
    assert_eq!(api.post_empty(&format!("/deliveries/{held}/replay")).await.kind(), (StatusCode::CONFLICT, "conflict".into()));
    assert_eq!(api.post_empty("/deliveries/dlv_nope/replay").await.kind(), (StatusCode::NOT_FOUND, "not_found".into()));
    assert_eq!(api.get(&format!("/deliveries/{one}/replay")).await.kind(), (StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed".into()));
}

// ===========================================================================
// Contract: health
// ===========================================================================

#[tokio::test]
async fn get_health_answers_200_with_every_section() {
    let api = Api::new();
    let reply = api.get("/health").await;
    assert_eq!(reply.status, StatusCode::OK);
    let mut body = reply.json();
    assert!(body["uptime_secs"].as_u64().is_some());
    body.as_object_mut().unwrap().remove("uptime_secs");
    assert_eq!(
        body,
        json!({
            "status": "healthy",
            "reasons": [],
            "lens": { "reachable": true, "status": "healthy", "round_trip_ms": 4 },
            "changelog": { "bytes_behind": 0, "last_target_slot": null },
            "engine": { "events_processed": 0, "evaluations": 0, "matches": 0,
                        "transient_failures": 0, "permanent_failures": 0, "failing_alerts": [] },
            "outbox": { "held": 0, "pending": 0, "delivered": 0, "failed": 0, "oldest_pending_age_secs": null },
            "registry": { "schema_version": 2, "alerts": 0, "saved_queries": 0 }
        })
    );
}

#[tokio::test]
async fn get_health_reads_the_running_engine_through_its_handle() {
    let api = Api::new();
    api.lens.set_max_slot(Some(100));
    let id = api.alert("whales", HOOK_URL, SECRET).await;
    api.post("/queries", &transfers_by_mint()).await;
    api.enqueue(&id, 50, true);

    // The engine gates to 200; the alert's range 101..=200 fails for good.
    api.lens.set_max_slot(Some(200));
    api.lens.fail_when("BETWEEN 101 AND 200", LensCallError::BadQuery { message: "No field named amount_raw".into() });
    announce(&api, 1, 200);
    assert_eq!(api.engine.run_once(&CancellationToken::new()).await, Pass::Committed { events: 1 });

    let body = api.get("/health").await.json();
    assert_eq!(body["status"], json!("degraded"));
    assert_eq!(body["reasons"], json!([format!("1 alert(s) failing: {id}")]));
    assert_eq!(body["changelog"], json!({ "bytes_behind": 0, "last_target_slot": 200 }));
    let engine = &body["engine"];
    assert_eq!(
        (engine["events_processed"].clone(), engine["evaluations"].clone(), engine["permanent_failures"].clone()),
        (json!(1), json!(1), json!(1))
    );
    let failing = &engine["failing_alerts"][0];
    assert_eq!((failing["id"].clone(), failing["kind"].clone(), failing["consecutive"].clone()), (json!(id), json!("permanent"), json!(1)));
    assert_eq!(body["outbox"]["held"], json!(1));
    assert_eq!(body["registry"], json!({ "schema_version": 2, "alerts": 1, "saved_queries": 1 }));
}

#[tokio::test]
async fn get_health_is_200_even_when_lens_is_down_degraded_or_hung() {
    let api = Api::new();

    api.lens.set_health(Err(LensCallError::Unavailable { message: "connection refused".into() }));
    let reply = api.get("/health").await;
    assert_eq!(reply.status, StatusCode::OK);
    let body = reply.json();
    assert_eq!(body["status"], json!("unhealthy"));
    assert_eq!(
        body["lens"],
        json!({ "reachable": false, "status": null, "round_trip_ms": null, "error": "LENS is unavailable: connection refused" })
    );
    assert_eq!(body["reasons"], json!(["LENS is unreachable: LENS is unavailable: connection refused"]));

    api.lens.set_health(Ok(LensHealth { status: LensHealthStatus::Unhealthy, round_trip_ms: 3 }));
    assert_eq!(api.get("/health").await.json()["status"], json!("unhealthy"));

    api.lens.set_health(Ok(LensHealth { status: LensHealthStatus::Degraded, round_trip_ms: 3 }));
    let body = api.get("/health").await.json();
    assert_eq!((body["status"].clone(), body["lens"]["status"].clone()), (json!("degraded"), json!("degraded")));

    api.lens.set_health(Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 3 }));
    api.lens.set_health_delay(Duration::from_secs(30));
    let started = Instant::now();
    let reply = api.get("/health").await;
    assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!((reply.json()["status"].clone(), reply.json()["lens"]["reachable"].clone()), (json!("unhealthy"), json!(false)));
}

// ===========================================================================
// Bad input and the error contract
// ===========================================================================

#[tokio::test]
async fn unknown_routes_and_methods_have_the_lens_error_shape() {
    let api = Api::new();
    for (method, uri, status, kind) in [
        (Method::GET, "/", 404, "not_found"),
        (Method::GET, "/nope", 404, "not_found"),
        (Method::GET, "/alerts/x/y/z", 404, "not_found"),
        (Method::DELETE, "/health", 405, "method_not_allowed"),
        (Method::PUT, "/queries", 405, "method_not_allowed"),
        (Method::POST, "/q/x", 405, "method_not_allowed"),
        (Method::PUT, "/alerts/x", 405, "method_not_allowed"),
    ] {
        let reply = api.raw(method.clone(), uri, None, "").await;
        let error = reply.error();
        assert_eq!((reply.status.as_u16(), error.error_kind.as_str()), (status, kind), "{method} {uri}");
        assert_eq!(error.sql, None);
    }
}

#[tokio::test]
async fn bodies_must_be_json_of_exactly_the_route_type() {
    let api = Api::new();
    let alert = alert_body("a", HOOK_URL, Some(SECRET)).to_string();
    let with = |patch: Value| {
        let mut body = alert_body("a", HOOK_URL, Some(SECRET));
        body.as_object_mut().unwrap().extend(patch.as_object().unwrap().clone());
        body.to_string()
    };
    let mut without_sql = alert_body("a", HOOK_URL, Some(SECRET));
    without_sql.as_object_mut().unwrap().remove("sql");

    for (uri, content_type, body, status) in [
        ("/alerts", None, alert.clone(), 415),
        ("/alerts", Some("text/plain"), alert.clone(), 415),
        ("/alerts", Some("application/x-www-form-urlencoded"), alert.clone(), 415),
        ("/alerts", Some("application/json"), "{\"name\":".to_owned(), 400),
        ("/alerts", Some("application/json"), "".to_owned(), 400),
        ("/alerts", Some("application/json"), "[]".to_owned(), 422),
        ("/alerts", Some("application/json"), without_sql.to_string(), 422),
        ("/alerts", Some("application/json"), with(json!({ "name": 7 })), 422),
        ("/alerts", Some("application/json"), with(json!({ "wait_for_final": "true" })), 422),
        // A typo is rejected, not silently defaulted.
        ("/alerts", Some("application/json"), with(json!({ "wait_for_fnal": true })), 422),
        ("/alerts", Some("application/json"), with(json!({ "enabled": false })), 422),
        ("/alerts", Some("application/json"), with(json!({ "id": "chosen" })), 422),
        ("/queries", Some("application/json"), json!({ "name": "q", "sql": "SELECT 1", "parms": [] }).to_string(), 422),
        (
            "/queries",
            Some("application/json"),
            json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u32", "required": true }] }).to_string(),
            422,
        ),
        (
            "/queries",
            Some("application/json"),
            json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64", "required": true, "extra": 1 }] }).to_string(),
            422,
        ),
        (
            "/queries",
            Some("application/json"),
            json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64", "required": false, "default": 10 }] }).to_string(),
            422,
        ),
        (
            "/queries",
            Some("application/json"),
            json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64" }] }).to_string(),
            422,
        ),
    ] {
        let reply = api.raw(Method::POST, uri, content_type, body.clone()).await;
        let error = reply.error();
        assert_eq!(
            (reply.status.as_u16(), error.error_kind.as_str()),
            (status, "invalid_request"),
            "{uri} {content_type:?} {body}: {}",
            error.error
        );
    }
    assert!(api.registry.list_alerts().unwrap().is_empty());
    assert!(api.registry.list_queries().unwrap().is_empty());
    assert!(api.lens.queries().is_empty());
}

#[tokio::test]
async fn bodies_over_the_limit_are_413() {
    let api = Api::new();
    let padding = "x".repeat(MAX_BODY_BYTES);
    let body = json!({ "name": "q", "sql": format!("SELECT '{padding}'") }).to_string();
    let reply = api.raw(Method::POST, "/queries", Some("application/json"), body).await;
    assert_eq!(reply.kind(), (StatusCode::PAYLOAD_TOO_LARGE, "invalid_request".into()));
}

#[tokio::test]
async fn post_queries_rejects_what_the_registry_rejects() {
    let api = Api::new();
    for body in [
        json!({ "name": "", "sql": "SELECT 1" }),
        json!({ "name": "has space", "sql": "SELECT 1" }),
        json!({ "name": "q", "sql": "  " }),
        json!({ "name": "q", "sql": "SELECT 1; DROP TABLE x" }),
        json!({ "name": "q", "sql": "SELECT * FROM t WHERE m = '{{mint}}'",
                "params": [{ "name": "mint", "kind": "string", "required": true }] }),
        json!({ "name": "q", "sql": "SELECT {{a}}" }),
        json!({ "name": "q", "sql": "SELECT 1", "params": [{ "name": "a", "kind": "u64", "required": true }] }),
        json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64", "required": false }] }),
        json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64", "required": true, "default": "1" }] }),
        json!({ "name": "q", "sql": "SELECT {{a}}", "params": [{ "name": "a", "kind": "u64", "required": false, "default": "-1" }] }),
    ] {
        let reply = api.post("/queries", &body).await;
        let error = reply.error();
        assert_eq!((reply.status, error.error_kind.as_str()), (StatusCode::BAD_REQUEST, "validation_error"), "{body}");
        assert!(error.error.starts_with("invalid "), "{}", error.error);
        assert_eq!(error.sql, None);
    }
    assert!(api.registry.list_queries().unwrap().is_empty());

    assert_eq!(api.post("/queries", &transfers_by_mint()).await.status, StatusCode::CREATED);
    assert_eq!(api.post("/queries", &transfers_by_mint()).await.kind(), (StatusCode::CONFLICT, "conflict".into()));
}

// ===========================================================================
// Secrets never leak
// ===========================================================================

/// Log output written into memory.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn the_secret_is_returned_once_and_never_again_in_any_body_error_or_log() {
    // Single-threaded: every handler and the dispatcher log through this.
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let api = Api::new();
    // The receiver quotes the secret back, so it would reach `last_error`.
    let receiver = Receiver::start(|_| Hook::WithBody(500, format!("bad signature for {SECRET}"))).await;

    let created = api.post("/alerts", &alert_body("whales", &receiver.url("/hook?token=abc"), Some(SECRET))).await;
    assert_eq!(created.status, StatusCode::CREATED);
    assert_eq!(created.json()["secret"], json!(SECRET), "returned exactly once, here");
    let id = created.json()["id"].as_str().unwrap().to_owned();
    let failed = api.enqueue(&id, 1, false);
    eventually("the send to fail", || api.delivery(&failed).status == DeliveryStatus::Failed).await;

    let mut replies = Vec::new();
    for uri in ["/alerts".to_owned(), format!("/alerts/{id}"), format!("/alerts/{id}/deliveries"), "/health".to_owned()] {
        replies.push(api.get(&uri).await);
    }
    replies.push(api.post_empty(&format!("/deliveries/{failed}/replay")).await);
    replies.push(api.patch(&format!("/alerts/{id}"), &json!({ "name": "renamed" })).await);
    replies.push(api.patch(&format!("/alerts/{id}"), &json!({ "secret": OTHER_SECRET })).await);
    for reply in &replies {
        assert!(reply.status.is_success(), "{}", reply.text());
    }

    // Errors that see a submitted secret must not quote it, whatever its type.
    let numeric = "12345678901234567890";
    let bad_bodies = [
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"{HOOK_URL}","secret":{numeric}}}"#),
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"{HOOK_URL}","secret":["{SECRET}"]}}"#),
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"{HOOK_URL}","secret":"{SECRET}","typo":1}}"#),
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"ftp://x","secret":"{SECRET}"}}"#),
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"{HOOK_URL}","secret":"{SECRET}\n"}}"#),
        format!(r#"{{"name":"n","sql":"{ALERT_SQL}","webhook_url":"{HOOK_URL}","secret":" {SECRET}"}}"#),
    ];
    for body in &bad_bodies {
        let reply = api.raw(Method::POST, "/alerts", Some("application/json"), body.clone()).await;
        reply.error();
        replies.push(reply);
    }
    for body in [format!(r#"{{"secret":{numeric}}}"#), format!(r#"{{"secret":"{SECRET}","wat":true}}"#), r#"{"secret":"tiny-secret"}"#.to_owned()] {
        let reply = api.raw(Method::PATCH, &format!("/alerts/{id}"), Some("application/json"), body).await;
        reply.error();
        replies.push(reply);
    }

    for reply in &replies {
        let text = reply.text();
        for secret in [SECRET, OTHER_SECRET, numeric, "tiny-secret"] {
            assert!(!text.contains(secret), "{secret} leaked ({}): {text}", reply.status);
        }
    }
    let history = api.get(&format!("/alerts/{id}/deliveries")).await.json();
    assert!(history["deliveries"][0]["last_error"].as_str().unwrap().contains("<redacted>"), "{history}");

    drop(_guard);
    let logs = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("alert created"), "the capture works: {logs}");
    for secret in [SECRET, OTHER_SECRET, numeric, "tiny-secret"] {
        assert!(!logs.contains(secret), "{secret} reached the logs");
    }
}
