//! Tests for `forge::lens_client` against a scripted fake LENS on a real
//! socket (see `lens_support`).
//!
//! | Section       | Pins down                                                          |
//! |---------------|--------------------------------------------------------------------|
//! | Construction  | settings validation, URLs                                          |
//! | Request       | the exact bytes FORGE sends, checked against `lens::QueryRequest`  |
//! | Response      | the exact parse of LENS's body, and every way a 200 can be wrong   |
//! | Classification| every status and transport failure lands in its variant, unretried |
//! | Health        | `GET /health`                                                      |
//! | Visibility    | the gate: when it opens, what it reports when it does not, timing  |

mod common;
mod lens_support;

use std::time::{Duration, Instant};

use forge::{
    ForgeError, LensCallError, LensClient, LensClientSettings, LensHealthStatus, LensResponse, VISIBILITY_SQL,
    Visibility,
};
use lens_support::{FakeLens, Reply, client_for, config_for, fast_settings, refused_url};
use serde_json::{Map, Value, json};

const SQL: &str = "SELECT slot, blockhash FROM solana.slots ORDER BY slot LIMIT 3";

/// Unwrap an error, checking its variant by `kind` and that it says `needle`.
#[track_caller]
fn expect_err<T: std::fmt::Debug>(result: Result<T, LensCallError>, kind: &str, needle: &str) -> LensCallError {
    let err = result.expect_err("expected the call to fail");
    assert_eq!(err.kind(), kind, "{err}");
    assert!(err.message().contains(needle), "{needle:?} not in {err}");
    err
}

fn settings_with(request_timeout_ms: u64, poll_ms: u64) -> LensClientSettings {
    LensClientSettings {
        request_timeout: Duration::from_millis(request_timeout_ms),
        visibility_poll_interval: Duration::from_millis(poll_ms),
        ..fast_settings()
    }
}

// ===========================================================================
// Construction
// ===========================================================================

#[test]
fn new_uses_the_default_settings_and_the_configured_url() {
    let client = LensClient::new(&config_for("http://127.0.0.1:8080/")).unwrap();
    assert_eq!(client.query_url(), "http://127.0.0.1:8080/query");
    assert_eq!(client.health_url(), "http://127.0.0.1:8080/health");
    assert_eq!(*client.settings(), LensClientSettings::default());
    assert!(
        client.settings().request_timeout > Duration::from_secs(30),
        "LENS's own 30 s query timeout must fire first"
    );
}

#[test]
fn a_lens_url_with_a_path_prefix_keeps_it() {
    let client = LensClient::new(&config_for("https://lens.internal/api/v1/")).unwrap();
    assert_eq!(client.query_url(), "https://lens.internal/api/v1/query");
    assert_eq!(client.health_url(), "https://lens.internal/api/v1/health");
}

#[test]
fn zero_durations_are_rejected_naming_the_setting() {
    let config = config_for("http://127.0.0.1:8080");
    let cases = [
        ("request_timeout", LensClientSettings { request_timeout: Duration::ZERO, ..Default::default() }),
        ("connect_timeout", LensClientSettings { connect_timeout: Duration::ZERO, ..Default::default() }),
        (
            "visibility_poll_interval",
            LensClientSettings { visibility_poll_interval: Duration::ZERO, ..Default::default() },
        ),
    ];
    for (name, settings) in cases {
        match LensClient::with_settings(&config, settings) {
            Err(ForgeError::LensClientError { url, source }) => {
                assert_eq!(url, "http://127.0.0.1:8080");
                assert!(source.to_string().contains(name), "{source}");
            }
            other => panic!("expected LensClientError for {name}, got {other:?}"),
        }
    }
}

#[test]
fn from_parts_needs_no_forge_config() {
    let client = LensClient::from_parts("https://lens.internal/api/v1/", Duration::from_secs(60)).unwrap();
    assert_eq!(client.query_url(), "https://lens.internal/api/v1/query", "trailing slash dropped");
    assert_eq!(client.health_url(), "https://lens.internal/api/v1/health");
    assert_eq!(
        *client.settings(),
        LensClientSettings { request_timeout: Duration::from_secs(60), ..Default::default() },
        "the timeout bounds requests; everything else is the default"
    );
}

#[test]
fn from_parts_never_lets_connecting_outlast_the_request() {
    let timeout = Duration::from_secs(2);
    assert!(timeout < LensClientSettings::default().connect_timeout);
    let client = LensClient::from_parts("http://127.0.0.1:8080", timeout).unwrap();
    assert_eq!(client.settings().request_timeout, timeout);
    assert_eq!(client.settings().connect_timeout, timeout);
}

#[test]
fn from_parts_rejects_a_zero_timeout() {
    match LensClient::from_parts("http://127.0.0.1:8080/", Duration::ZERO) {
        Err(ForgeError::LensClientError { url, source }) => {
            assert_eq!(url, "http://127.0.0.1:8080");
            assert!(source.to_string().contains("request_timeout"), "{source}");
        }
        other => panic!("expected LensClientError, got {other:?}"),
    }
}

#[tokio::test]
async fn a_from_parts_client_queries_and_times_out_like_any_other() {
    let lens = FakeLens::always(Reply::rows(json!([{ "slot": 7 }]), SQL)).await;
    let client = LensClient::from_parts(&lens.url(), Duration::from_secs(5)).unwrap();
    let response = client.query(SQL).await.unwrap();
    assert_eq!(response.rows[0]["slot"], json!(7));
    assert_eq!(lens.only_request().target, "/query");

    let hanging = FakeLens::always(Reply::Hang).await;
    let client = LensClient::from_parts(&hanging.url(), Duration::from_millis(300)).unwrap();
    let started = Instant::now();
    let err = expect_err(client.query("S").await, "timeout", "within the timeout");
    assert!(err.is_retryable());
    assert!(started.elapsed() < Duration::from_secs(2), "gave up promptly: {:?}", started.elapsed());
}

// ===========================================================================
// Request: exactly what LENS expects
// ===========================================================================

#[tokio::test]
async fn query_sends_one_post_to_query_with_a_json_body_lens_accepts() {
    let lens = FakeLens::always(Reply::rows(json!([]), SQL)).await;
    lens.client().query(SQL).await.unwrap();

    let request = lens.only_request();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/query", "no query string, no trailing slash");
    assert_eq!(request.version, "HTTP/1.1");
    assert_eq!(request.header("content-type"), "application/json");
    assert_eq!(request.header("accept"), "application/json");
    assert_eq!(request.header("host"), lens.url().trim_start_matches("http://"));
    assert_eq!(request.header("content-length"), request.body.len().to_string());
    assert!(!request.has_header("transfer-encoding"), "LENS gets a sized body");
    assert!(!request.has_header("accept-encoding"), "FORGE never asks for a compressed body");
    assert!(!request.has_header("authorization"));

    // Byte for byte: two fields, in this order, nothing else.
    assert_eq!(request.body_text(), format!(r#"{{"sql":"{SQL}","format":"json"}}"#));

    // And LENS's own request type reads it as intended.
    let parsed: lens::QueryRequest = serde_json::from_slice(&request.body).unwrap();
    assert_eq!(parsed, lens::QueryRequest { sql: SQL.into(), format: Some("json".into()) });
    assert_eq!(lens::parse_format(parsed.format.as_deref()), Ok(lens::QueryFormat::Json));
}

#[tokio::test]
async fn sql_reaches_lens_byte_for_byte() {
    let statements = [
        "SELECT 'it''s' AS quoted, \"slot\" FROM solana.slots",
        "SELECT '\\' AS backslash, '\\n' AS not_a_newline",
        "  SELECT 1  \n\t-- leading/trailing whitespace is LENS's to trim, not FORGE's\n",
        "SELECT 'héllo wörld ✓ 🚀 日本語' AS unicode",
        "SELECT '\u{0001}\u{001f}\u{007f}' AS control_chars",
        "SELECT '</script><script>' AS markup, '{\"json\":true}' AS embedded_json",
        "SELECT ?, $1, :name, %s AS placeholders_are_not_bound",
        "",
    ];
    let lens = FakeLens::start(|request, _| Reply::rows(json!([]), &request.sql())).await;
    let client = lens.client();
    for sql in statements {
        client.query(sql).await.unwrap();
    }
    let requests = lens.requests();
    assert_eq!(requests.len(), statements.len());
    for (sql, request) in statements.iter().zip(&requests) {
        let parsed: lens::QueryRequest = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(parsed.sql, *sql);
        assert_eq!(request.header("content-length"), request.body.len().to_string());
    }
}

#[tokio::test]
async fn a_long_statement_is_sent_whole() {
    let sql = format!("SELECT {} AS wide", vec!["slot"; 20_000].join(" + "));
    let lens = FakeLens::start(|request, _| Reply::rows(json!([]), &request.sql())).await;
    lens.client().query(&sql).await.unwrap();
    assert_eq!(lens.only_request().sql(), sql);
}

#[tokio::test]
async fn requests_go_below_a_path_prefix() {
    let lens = FakeLens::start(|request, _| match request.target.as_str() {
        "/lens/query" => Reply::rows(json!([]), "S"),
        "/lens/health" => Reply::json(200, &json!({ "status": "healthy" })),
        _ => Reply::empty(404),
    })
    .await;
    let client = client_for(&format!("{}/lens/", lens.url()), fast_settings());
    client.query("S").await.unwrap();
    client.health().await.unwrap();
    let targets: Vec<String> = lens.requests().into_iter().map(|request| request.target).collect();
    assert_eq!(targets, ["/lens/query", "/lens/health"]);
}

// ===========================================================================
// Response: exactly what LENS returns
// ===========================================================================

#[tokio::test]
async fn the_documented_lens_body_parses_field_for_field() {
    let body = r#"{"rows":[{"slot":295000000,"blockhash":"4vJ9Sh"}],"row_count":1,"duration_ms":47,"truncated":false,"sql":"SELECT slot, blockhash FROM solana.slots"}"#;
    let lens = FakeLens::always(Reply::text(200, "application/json", body)).await;
    let response = lens.client().query("SELECT slot, blockhash FROM solana.slots").await.unwrap();

    let mut row = Map::new();
    row.insert("slot".into(), json!(295_000_000));
    row.insert("blockhash".into(), json!("4vJ9Sh"));
    assert_eq!(
        response,
        LensResponse {
            rows: vec![row],
            row_count: 1,
            duration_ms: 47,
            truncated: false,
            sql: "SELECT slot, blockhash FROM solana.slots".into(),
            round_trip_ms: response.round_trip_ms,
        }
    );
}

#[tokio::test]
async fn values_come_back_exactly_as_lens_wrote_them() {
    let body = format!(
        r#"{{"rows":[{{"u64_max":{},"i64_min":{},"zero":0,"null":null,"float":0.125,"neg":-1.5e-7,"bool":true,"text":"é\n\"✓\"","ts":"2026-11-01T00:00:00+00:00","list":["a",null,3],"nested":{{"k":[{{}}]}}}}],"row_count":1,"duration_ms":0,"truncated":false,"sql":"S"}}"#,
        u64::MAX,
        i64::MIN
    );
    let lens = FakeLens::always(Reply::text(200, "application/json", &body)).await;
    let response = lens.client().query("S").await.unwrap();
    let row = &response.rows[0];

    assert_eq!(row["u64_max"].as_u64(), Some(u64::MAX), "no f64 rounding of large integers");
    assert_eq!(row["i64_min"].as_i64(), Some(i64::MIN));
    assert_eq!(row["zero"].as_u64(), Some(0));
    assert_eq!(row["null"], Value::Null);
    assert!(row.contains_key("null"), "an explicit null is kept as a column");
    assert_eq!(row["float"].as_f64(), Some(0.125));
    assert_eq!(row["neg"].as_f64(), Some(-1.5e-7));
    assert_eq!(row["bool"], json!(true));
    assert_eq!(row["text"], json!("é\n\"✓\""));
    assert_eq!(row["ts"], json!("2026-11-01T00:00:00+00:00"));
    assert_eq!(row["list"], json!(["a", null, 3]));
    assert_eq!(row["nested"], json!({ "k": [{}] }));
    assert_eq!(row.len(), 11, "no column added or dropped");
}

#[tokio::test]
async fn every_row_keeps_every_column() {
    let body = r#"{"rows":[{"z":1,"a":2,"m":null},{"z":4,"a":5,"m":6}],"row_count":2,"duration_ms":0,"sql":"S"}"#;
    let lens = FakeLens::always(Reply::text(200, "application/json", body)).await;
    let response = lens.client().query("S").await.unwrap();
    assert_eq!(response.rows.len(), 2);
    for row in &response.rows {
        let mut columns: Vec<&str> = row.keys().map(String::as_str).collect();
        columns.sort_unstable();
        assert_eq!(columns, ["a", "m", "z"]);
    }
    assert_eq!(response.rows[0]["m"], Value::Null);
    assert_eq!(response.rows[1]["a"], json!(5));
}

#[tokio::test]
async fn truncated_is_read_and_defaults_to_false() {
    let lens = FakeLens::sequence(vec![
        Reply::text(200, "application/json", r#"{"rows":[{"n":1}],"row_count":1,"duration_ms":2,"truncated":true,"sql":"S"}"#),
        Reply::text(200, "application/json", r#"{"rows":[{"n":1}],"row_count":1,"duration_ms":2,"sql":"S"}"#),
    ])
    .await;
    let client = lens.client();
    assert!(client.query("S").await.unwrap().truncated);
    assert!(!client.query("S").await.unwrap().truncated);
}

#[tokio::test]
async fn fields_lens_may_add_later_are_ignored() {
    let body = r#"{"rows":[],"row_count":0,"duration_ms":1,"truncated":false,"sql":"S","columns":["n"],"engine":{"v":2}}"#;
    let lens = FakeLens::always(Reply::text(200, "application/json", body)).await;
    let response = lens.client().query("S").await.unwrap();
    assert!(response.rows.is_empty());
    assert_eq!(response.row_count, 0);
}

#[tokio::test]
async fn round_trip_is_measured_by_forge_and_duration_read_from_lens() {
    let lens = FakeLens::always(Reply::rows(json!([{ "n": 1 }]), "S").after(Duration::from_millis(250))).await;
    let started = Instant::now();
    let response = lens.client().query("S").await.unwrap();
    let elapsed = started.elapsed();

    assert_eq!(response.duration_ms, 3, "LENS-side time comes from the body");
    assert!(response.round_trip_ms >= 250, "{}", response.round_trip_ms);
    assert!(u128::from(response.round_trip_ms) <= elapsed.as_millis(), "{response:?} vs {elapsed:?}");
}

#[tokio::test]
async fn a_200_that_is_not_a_lens_result_is_a_protocol_error() {
    let cases = [
        Reply::text(200, "text/csv; charset=utf-8", "slot,blockhash\n1,a\n"),
        Reply::text(200, "text/html", "<html><body>Welcome to nginx!</body></html>"),
        Reply::empty(200),
        Reply::text(200, "application/json", r#"{"rows":[{"slot":1}"#),
        Reply::text(200, "application/json", r#"{"rows":[{"slot":1}],"row_count":2,"duration_ms":0,"sql":"S"}"#),
        Reply::text(200, "application/json", r#"{"rows":[[1]],"row_count":1,"duration_ms":0,"sql":"S"}"#),
        Reply::text(200, "application/json", r#"{"rows":[],"row_count":0,"duration_ms":0}"#),
        Reply::json(200, &json!({ "error": "x", "error_kind": "y", "sql": "S" })),
    ];
    for reply in cases {
        let lens = FakeLens::always(reply.clone()).await;
        let err = expect_err(lens.client().query("S").await, "protocol", "200");
        assert!(!err.is_retryable(), "{reply:?}");
        assert_eq!(lens.requests().len(), 1, "not retried: {reply:?}");
    }
}

#[tokio::test]
async fn a_protocol_error_quotes_the_body_but_not_all_of_it() {
    let junk = "x".repeat(50_000);
    let lens = FakeLens::always(Reply::text(200, "text/plain", &junk)).await;
    let err = expect_err(lens.client().query("S").await, "protocol", "xxxxx");
    assert!(err.message().len() < 2_000, "{} bytes", err.message().len());
    assert!(err.message().contains("50000 bytes"), "{err}");
}

// ===========================================================================
// Classification
// ===========================================================================

#[tokio::test]
async fn statuses_over_the_wire_map_to_their_variant() {
    // (status, reply body, expected kind, text the message must contain)
    let error_body = |kind: &str, error: &str| json!({ "error": error, "error_kind": kind, "sql": "S" });
    let cases: Vec<(u16, Reply, &str, &str)> = vec![
        (400, Reply::json(400, &error_body("query_plan_error", "table 'solana.nope' not found")), "bad_query", "query_plan_error: table 'solana.nope' not found"),
        (400, Reply::json(400, &error_body("query_execution_error", "Divide by zero error")), "bad_query", "Divide by zero"),
        (400, Reply::json(400, &error_body("invalid_format", "unsupported format")), "bad_query", "invalid_format"),
        (400, Reply::text(400, "text/plain", "bad"), "bad_query", "bad"),
        (408, Reply::empty(408), "timeout", "408 Request Timeout"),
        (504, Reply::json(504, &error_body("query_timeout", "query exceeded the 30s timeout and was cancelled")), "timeout", "query_timeout"),
        (500, Reply::json(500, &error_body("io_error", "disk on fire")), "unavailable", "500 Internal Server Error: io_error: disk on fire"),
        (502, Reply::text(502, "text/html", "<h1>Bad Gateway</h1>"), "unavailable", "<h1>Bad Gateway</h1>"),
        (503, Reply::json(503, &error_body("query_execution_error", "Resources exhausted")), "unavailable", "Resources exhausted"),
        (599, Reply::empty(599), "unavailable", "599"),
        (201, Reply::empty(201), "protocol", "201 Created"),
        (204, Reply::empty(204), "protocol", "204 No Content"),
        (301, Reply::empty(301), "protocol", "301"),
        (401, Reply::empty(401), "protocol", "401 Unauthorized"),
        (404, Reply::text(404, "text/plain", "not here"), "protocol", "not here"),
        (405, Reply::empty(405), "protocol", "405"),
        (413, Reply::empty(413), "protocol", "413"),
        (415, Reply::json(415, &error_body("invalid_request", "Expected request with `Content-Type: application/json`")), "protocol", "invalid_request"),
        (422, Reply::json(422, &error_body("invalid_request", "missing field `sql`")), "protocol", "missing field"),
        (429, Reply::empty(429), "protocol", "429"),
    ];
    for (status, reply, kind, needle) in cases {
        let lens = FakeLens::always(reply).await;
        let err = expect_err(lens.client().query("S").await, kind, needle);
        assert_eq!(err.is_retryable(), matches!(kind, "timeout" | "unavailable"), "{status}: {err}");
        assert_eq!(lens.requests().len(), 1, "HTTP {status} was retried");
        if kind != "bad_query" {
            assert!(err.message().contains(&format!("POST {}/query", lens.url())), "names the call: {err}");
        }
    }
}

#[tokio::test]
async fn a_redirect_is_not_followed() {
    let lens = FakeLens::start(|request, _| match request.target.as_str() {
        "/query" => Reply::Raw(b"HTTP/1.1 307 Temporary Redirect\r\nLocation: /elsewhere\r\nContent-Length: 0\r\n\r\n".to_vec()),
        _ => Reply::rows(json!([]), "S"),
    })
    .await;
    expect_err(lens.client().query("S").await, "protocol", "307");
    assert_eq!(lens.only_request().target, "/query");
}

#[tokio::test]
async fn connection_refused_is_unavailable() {
    let client = client_for(&refused_url(), fast_settings());
    let err = expect_err(client.query("S").await, "unavailable", "cannot connect");
    assert!(err.is_retryable());
}

#[tokio::test]
async fn an_unresolvable_host_is_unavailable() {
    let client = client_for("http://lens.invalid:8080", fast_settings());
    expect_err(client.query("S").await, "unavailable", "lens.invalid");
}

#[tokio::test]
async fn a_connection_closed_without_an_answer_is_unavailable_and_not_retried() {
    let lens = FakeLens::always(Reply::Close).await;
    let err = expect_err(lens.client().query("S").await, "unavailable", "connection");
    assert!(err.is_retryable());
    assert_eq!(lens.requests().len(), 1);
    assert_eq!(lens.connections(), 1, "no second attempt on a new connection");
}

#[tokio::test]
async fn a_reset_connection_is_unavailable() {
    let lens = FakeLens::always(Reply::Reset).await;
    expect_err(lens.client().query("S").await, "unavailable", "");
    assert_eq!(lens.connections(), 1);
}

#[tokio::test]
async fn a_body_cut_short_is_unavailable() {
    let lens = FakeLens::always(Reply::TruncateBody { content_length: 500, sent: br#"{"rows":[{"slot":"#.to_vec() })
        .await;
    expect_err(lens.client().query("S").await, "unavailable", "");
    assert_eq!(lens.connections(), 1);
}

#[tokio::test]
async fn a_reply_that_is_not_http_is_a_protocol_error() {
    for garbage in [
        &b"SSH-2.0-OpenSSH_9.6\r\n"[..],
        b"HTTP/1.1 abc Nope\r\n\r\n",
        b"\x16\x03\x01\x02\x00\x01\x00\x01\xfc\x03\x03",
    ] {
        let lens = FakeLens::always(Reply::Raw(garbage.to_vec())).await;
        let err = expect_err(lens.client().query("S").await, "protocol", "not valid HTTP");
        assert!(!err.is_retryable(), "{}", String::from_utf8_lossy(garbage));
    }
}

#[tokio::test]
async fn no_answer_within_the_request_timeout_is_a_timeout() {
    let lens = FakeLens::always(Reply::Hang).await;
    let client = lens.client_with(settings_with(300, 20));
    let started = Instant::now();
    let err = expect_err(client.query("S").await, "timeout", "within the timeout");
    let elapsed = started.elapsed();
    assert!(err.is_retryable());
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "gave up promptly: {elapsed:?}");
}

#[tokio::test]
async fn a_body_that_stalls_past_the_timeout_is_a_timeout() {
    let lens = FakeLens::always(Reply::StallBody { content_length: 100, sent: br#"{"rows":["#.to_vec() }).await;
    let client = lens.client_with(settings_with(300, 20));
    let started = Instant::now();
    expect_err(client.query("S").await, "timeout", "");
    assert!(started.elapsed() < Duration::from_secs(2), "the timeout covers the body too");
}

#[tokio::test]
async fn a_slow_but_complete_answer_within_the_timeout_succeeds() {
    let lens = FakeLens::always(Reply::rows(json!([{ "n": 1 }]), "S").after(Duration::from_millis(150))).await;
    let response = lens.client_with(settings_with(1_000, 20)).query("S").await.unwrap();
    assert_eq!(response.rows.len(), 1);
}

#[tokio::test]
async fn errors_carry_their_category_in_display() {
    let lens = FakeLens::always(Reply::table_not_found()).await;
    let err = lens.client().query(VISIBILITY_SQL).await.unwrap_err();
    assert_eq!(
        err,
        LensCallError::BadQuery { message: "query_plan_error: table 'datafusion.solana.slots' not found".into() }
    );
    assert_eq!(err.to_string(), "LENS rejected the query: query_plan_error: table 'datafusion.solana.slots' not found");
    // Usable as the cause of a ForgeError.
    let forge = ForgeError::lens_client("http://lens/query", err);
    assert_eq!(forge.kind(), "lens_client_error");
    assert!(forge.to_string().contains("not found"), "{forge}");
}

// ===========================================================================
// Health
// ===========================================================================

#[tokio::test]
async fn health_sends_a_bodiless_get_and_reads_every_status() {
    for (raw, status) in [
        ("healthy", LensHealthStatus::Healthy),
        ("degraded", LensHealthStatus::Degraded),
        ("unhealthy", LensHealthStatus::Unhealthy),
    ] {
        // LENS's full body; only `status` is read.
        let body = json!({
            "status": raw, "chain": "solana", "data_dir": "/data",
            "tables": { "solana.slots": { "registered": true, "file_count": 12, "dir_exists": true } },
            "catalog": { "total_refreshes": 1, "last_refresh_duration_ms": 4, "tables_failed": 0 },
            "engine": { "total_queries": 0, "successful_queries": 0, "failed_queries": 0, "timed_out_queries": 0,
                        "success_rate": 0.0, "last_query_duration_ms": 0, "p50_duration_ms": 0, "p99_duration_ms": 0 },
            "uptime_secs": 3847
        });
        let lens = FakeLens::always(Reply::json(200, &body).after(Duration::from_millis(30))).await;
        let health = lens.client().health().await.unwrap();
        assert_eq!(health.status, status);
        assert!(health.round_trip_ms >= 30, "{health:?}");

        let request = lens.only_request();
        assert_eq!(request.method, "GET");
        assert_eq!(request.target, "/health");
        assert_eq!(request.header("accept"), "application/json");
        assert!(request.body.is_empty());
        assert!(!request.has_header("content-type"));
    }
}

#[tokio::test]
async fn health_failures_are_classified() {
    let cases = [
        (Reply::json(200, &json!({ "status": "ok" })), "protocol"),
        (Reply::json(200, &json!({ "chain": "solana" })), "protocol"),
        (Reply::text(200, "text/plain", "OK"), "protocol"),
        (Reply::empty(400), "protocol"),
        (Reply::empty(404), "protocol"),
        (Reply::empty(500), "unavailable"),
        (Reply::empty(503), "unavailable"),
        (Reply::empty(504), "timeout"),
        (Reply::Close, "unavailable"),
        (Reply::Raw(b"garbage\r\n\r\n".to_vec()), "protocol"),
    ];
    for (reply, kind) in cases {
        let lens = FakeLens::always(reply.clone()).await;
        let err = lens.client().health().await.unwrap_err();
        assert_eq!(err.kind(), kind, "{reply:?}: {err}");
    }
    let err = client_for(&refused_url(), fast_settings()).health().await.unwrap_err();
    assert_eq!(err.kind(), "unavailable", "{err}");
}

#[tokio::test]
async fn health_times_out() {
    let lens = FakeLens::always(Reply::Hang).await;
    let err = lens.client_with(settings_with(200, 20)).health().await.unwrap_err();
    assert_eq!(err.kind(), "timeout", "{err}");
}

// ===========================================================================
// Visibility
// ===========================================================================

const WAIT: Duration = Duration::from_secs(5);
/// A gate timeout for tests that must see a few polls complete before it
/// runs out: generous, so a loaded CI machine does not turn into a flake.
const SETTLE: Duration = Duration::from_secs(3);

/// Settings under which only the gate's own deadline ever stops a poll.
fn patient(poll_ms: u64) -> LensClientSettings {
    settings_with(60_000, poll_ms)
}

#[tokio::test]
async fn visible_at_once_takes_one_poll_with_the_visibility_sql() {
    let lens = FakeLens::always(Reply::max_slot(Some(1_000))).await;
    let visibility = lens.client().wait_until_visible(1_000, WAIT).await;
    assert_eq!(visibility, Visibility::Visible { max_slot: 1_000 });
    assert!(visibility.is_visible());

    let request = lens.only_request();
    assert_eq!(request.target, "/query");
    assert_eq!(request.body_text(), format!(r#"{{"sql":"{VISIBILITY_SQL}","format":"json"}}"#));
}

#[tokio::test]
async fn a_max_slot_past_the_slot_is_visible_and_reported_as_is() {
    let lens = FakeLens::always(Reply::max_slot(Some(1_500))).await;
    assert_eq!(lens.client().wait_until_visible(1_000, WAIT).await, Visibility::Visible { max_slot: 1_500 });
}

#[tokio::test]
async fn the_gate_opens_exactly_when_max_slot_reaches_the_slot() {
    let lens = FakeLens::sequence(vec![
        Reply::max_slot(None),
        Reply::max_slot(Some(997)),
        Reply::max_slot(Some(998)),
        Reply::max_slot(Some(999)),
        Reply::max_slot(Some(1_000)),
        Reply::max_slot(Some(1_001)),
    ])
    .await;
    let visibility = lens.client().wait_until_visible(1_000, WAIT).await;
    assert_eq!(visibility, Visibility::Visible { max_slot: 1_000 });
    assert_eq!(lens.requests().len(), 5, "stopped polling as soon as it opened");
}

#[tokio::test]
async fn slot_zero_is_visible_once_any_slot_is() {
    let lens = FakeLens::sequence(vec![Reply::max_slot(None), Reply::max_slot(Some(0))]).await;
    assert_eq!(lens.client().wait_until_visible(0, WAIT).await, Visibility::Visible { max_slot: 0 });
    assert_eq!(lens.requests().len(), 2, "an empty table does not count as slot 0");
}

#[tokio::test]
async fn the_largest_slot_is_compared_without_loss() {
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(u64::MAX - 1)), Reply::max_slot(Some(u64::MAX))]).await;
    assert_eq!(lens.client().wait_until_visible(u64::MAX, WAIT).await, Visibility::Visible { max_slot: u64::MAX });
    assert_eq!(lens.requests().len(), 2);
}

#[tokio::test]
async fn the_gate_polls_through_every_kind_of_failure() {
    let lens = FakeLens::sequence(vec![
        Reply::table_not_found(),
        Reply::Close,
        Reply::Reset,
        Reply::empty(503),
        Reply::lens_error(504, "query_timeout", "query exceeded the 30s timeout and was cancelled", VISIBILITY_SQL),
        Reply::Raw(b"not http\r\n\r\n".to_vec()),
        Reply::text(200, "text/html", "<html/>"),
        Reply::rows(json!([{ "max_slot": "nope" }]), VISIBILITY_SQL),
        Reply::rows(json!([]), VISIBILITY_SQL),
        Reply::empty(404),
        Reply::max_slot(Some(42)),
    ])
    .await;
    let visibility = lens.client().wait_until_visible(42, WAIT).await;
    assert_eq!(visibility, Visibility::Visible { max_slot: 42 });
    assert_eq!(lens.requests().len(), 11, "every failure was followed by another poll");
}

#[tokio::test]
async fn polls_are_spaced_by_the_poll_interval() {
    let interval = Duration::from_millis(80);
    let lens = FakeLens::sequence(vec![
        Reply::max_slot(Some(1)),
        Reply::max_slot(Some(2)),
        Reply::max_slot(Some(3)),
        Reply::max_slot(Some(4)),
    ])
    .await;
    let client = lens.client_with(LensClientSettings { visibility_poll_interval: interval, ..fast_settings() });
    assert!(client.wait_until_visible(4, WAIT).await.is_visible());

    let times: Vec<Instant> = lens.requests().iter().map(|request| request.at).collect();
    assert_eq!(times.len(), 4);
    for pair in times.windows(2) {
        let gap = pair[1] - pair[0];
        assert!(gap >= interval - Duration::from_millis(5), "polls {gap:?} apart, interval {interval:?}");
        assert!(gap < interval * 5, "polls {gap:?} apart, interval {interval:?}");
    }
}

#[tokio::test]
async fn timing_out_behind_reports_the_last_slot_seen_and_no_error() {
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(990)), Reply::max_slot(Some(995))]).await;
    let client = lens.client_with(patient(50));
    let started = Instant::now();
    let visibility = client.wait_until_visible(1_000, SETTLE).await;
    let elapsed = started.elapsed();

    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(995), last_error: None });
    assert!(!visibility.is_visible());
    assert!(elapsed >= SETTLE, "waited the whole timeout: {elapsed:?}");
    assert!(elapsed < SETTLE + Duration::from_secs(1), "and not much longer: {elapsed:?}");
    assert!(lens.requests().len() >= 3, "kept polling: {}", lens.requests().len());
}

#[tokio::test]
async fn last_seen_is_the_latest_answer_even_if_lower() {
    // LENS restarted over fewer files: the newest answer is the truth.
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(995)), Reply::max_slot(Some(980))]).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(1_000, SETTLE).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(980), last_error: None });
}

#[tokio::test]
async fn an_empty_table_until_the_end_reports_nothing_seen_and_no_error() {
    let lens = FakeLens::always(Reply::max_slot(None)).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(1, SETTLE).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: None, last_error: None });
}

#[tokio::test]
async fn a_failing_last_poll_reports_its_error_and_keeps_the_last_slot_seen() {
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(990)), Reply::empty(503)]).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(1_000, SETTLE).await;
    let Visibility::TimedOut { last_seen, last_error: Some(error) } = visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert_eq!(last_seen, Some(990));
    assert!(error.starts_with("LENS is unavailable: "), "{error}");
    assert!(error.contains("503"), "{error}");
}

#[tokio::test]
async fn a_success_after_failures_clears_the_error() {
    let lens = FakeLens::sequence(vec![Reply::empty(503), Reply::table_not_found(), Reply::max_slot(Some(10))]).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(11, SETTLE).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(10), last_error: None });
}

#[tokio::test]
async fn a_table_that_never_appears_reports_the_bad_query() {
    let lens = FakeLens::always(Reply::table_not_found()).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(1, SETTLE).await;
    assert_eq!(
        visibility,
        Visibility::TimedOut {
            last_seen: None,
            last_error: Some("LENS rejected the query: query_plan_error: table 'datafusion.solana.slots' not found".into()),
        }
    );
    assert!(lens.requests().len() >= 2, "a BadQuery does not stop the gate");
}

#[tokio::test]
async fn an_unreachable_lens_reports_the_connect_error() {
    let client = client_for(&refused_url(), patient(30));
    let visibility = client.wait_until_visible(1, SETTLE).await;
    let Visibility::TimedOut { last_seen: None, last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.contains("cannot connect"), "{error}");
}

#[tokio::test]
async fn a_malformed_answer_reports_a_protocol_error() {
    let lens = FakeLens::always(Reply::rows(json!([{ "max_slot": -5 }]), VISIBILITY_SQL)).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(1, SETTLE).await;
    let Visibility::TimedOut { last_seen: None, last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.starts_with("unexpected response from LENS: "), "{error}");
    assert!(error.contains("max_slot = -5"), "{error}");
}

#[tokio::test]
async fn a_hanging_lens_cannot_hold_the_gate_past_its_timeout() {
    // The request timeout is far longer than the gate's: each poll may only
    // take the time the gate has left.
    let lens = FakeLens::always(Reply::Hang).await;
    let client = lens.client_with(patient(30));
    let started = Instant::now();
    let visibility = client.wait_until_visible(1, Duration::from_millis(300)).await;
    let elapsed = started.elapsed();

    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(2_300), "the gate overran its timeout: {elapsed:?}");
    let Visibility::TimedOut { last_seen: None, last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.starts_with("LENS timed out: "), "{error}");
}

#[tokio::test]
async fn a_last_poll_cut_short_by_the_deadline_does_not_hide_earlier_findings() {
    // LENS answers "behind" once, then the next poll is still running when
    // the gate's time is up. That poll says nothing about LENS.
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(5)), Reply::Hang]).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(6, SETTLE).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(5), last_error: None });
    assert_eq!(lens.requests().len(), 2);

    // Same after a failure: the failure is what is reported.
    let lens = FakeLens::sequence(vec![Reply::empty(503), Reply::Hang]).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(6, SETTLE).await;
    let Visibility::TimedOut { last_seen: None, last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.contains("503"), "{error}");
}

#[tokio::test]
async fn a_poll_stopped_by_its_own_request_timeout_is_reported() {
    // Here the request timeout, not the gate's deadline, stopped the poll:
    // LENS really did not answer in time, and that replaces older news.
    let lens = FakeLens::sequence(vec![Reply::max_slot(Some(5)), Reply::Hang]).await;
    let visibility = lens.client_with(settings_with(1_000, 30)).wait_until_visible(6, SETTLE).await;
    let Visibility::TimedOut { last_seen: Some(5), last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.starts_with("LENS timed out: "), "{error}");
}

#[tokio::test]
async fn a_slow_answer_that_lands_within_the_timeout_opens_the_gate() {
    let lens = FakeLens::always(Reply::max_slot(Some(7)).after(Duration::from_millis(200))).await;
    let visibility = lens.client_with(patient(30)).wait_until_visible(7, SETTLE).await;
    assert_eq!(visibility, Visibility::Visible { max_slot: 7 });
}

#[tokio::test]
async fn a_zero_timeout_returns_at_once_without_asking_lens() {
    let lens = FakeLens::always(Reply::max_slot(Some(1_000))).await;
    let started = Instant::now();
    let visibility = lens.client().wait_until_visible(1, Duration::ZERO).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: None, last_error: None });
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(lens.requests().is_empty());
}

#[tokio::test]
async fn the_gate_never_sleeps_past_its_deadline() {
    // A poll interval longer than the whole timeout is cut to the time left.
    let lens = FakeLens::always(Reply::max_slot(Some(1))).await;
    let client = lens.client_with(patient(60_000));
    let started = Instant::now();
    let visibility = client.wait_until_visible(2, SETTLE).await;
    let elapsed = started.elapsed();
    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(1), last_error: None });
    assert!(elapsed >= SETTLE, "{elapsed:?}");
    assert!(elapsed < SETTLE + Duration::from_secs(1), "slept past the deadline: {elapsed:?}");
    assert_eq!(lens.requests().len(), 1);
}

#[tokio::test]
async fn concurrent_gates_on_one_client_are_independent() {
    let lens = FakeLens::always(Reply::max_slot(Some(100))).await;
    let client = lens.client_with(patient(20));
    let (low, high) = tokio::join!(
        client.wait_until_visible(50, Duration::from_secs(2)),
        client.wait_until_visible(150, SETTLE),
    );
    assert_eq!(low, Visibility::Visible { max_slot: 100 });
    assert_eq!(high, Visibility::TimedOut { last_seen: Some(100), last_error: None });
}
