//! Contract tests: FORGE's `LensClient` against the real LENS.
//!
//! PRISM's `ParquetWriter` writes the tables, `lens::serve_on` serves them
//! over a real socket with the catalog refreshing in the background, and the
//! client talks to it exactly as FORGE does in production. Where
//! `lens_client_test` pins each side of the wire against a script, these
//! check that the two crates actually agree:
//!
//! * every request FORGE sends is one LENS accepts and runs;
//! * every body LENS returns parses, values intact;
//! * every failure LENS can produce lands in the right [`LensCallError`];
//! * the visibility gate opens when, and only when, LENS's catalog has
//!   picked up the slot PRISM wrote.

mod common;
mod lens_support;

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::TempDir;
use forge::{LensCallError, LensClient, LensClientSettings, LensHealthStatus, Visibility};
use lens::{LensConfig, QueryEngine};
use lens_support::client_for;
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::DeepAnaSlot;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// A query no test timeout lets finish.
const ENDLESS_SQL: &str = "SELECT SUM(value) FROM generate_series(1, 1000000000000)";

fn slot_row(slot: u64) -> DeepAnaSlot {
    DeepAnaSlot {
        slot,
        blockhash: format!("hash{slot}"),
        parent_slot: slot - 1,
        block_time: Some(1_793_102_400 + slot as i64),
        block_height: Some(slot - 100),
        transaction_count: 0,
        leader: (slot % 2 == 0).then(|| format!("leader{slot}")),
    }
}

/// Have PRISM write slot rows for `slots` (the `slots` table only).
fn prism_write_slots(data_dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in slots {
        writer.append(&slot_row(slot), &[], &[], &[]).unwrap();
    }
    writer.flush().unwrap();
}

/// A real LENS on a free port. Stops when dropped.
struct RealLens {
    url: String,
    engine: Arc<QueryEngine>,
    server: Option<JoinHandle<lens::LensResult<()>>>,
}

impl RealLens {
    /// LENS over `data_dir`, with `vars` on top of its defaults.
    async fn start(data_dir: &Path, vars: &[(&str, &str)]) -> Self {
        let data_dir = data_dir.to_str().unwrap().to_owned();
        let config = LensConfig::load_with(|name| {
            if name == "LENS_DATA_DIR" {
                return Some(data_dir.clone().into());
            }
            vars.iter().find(|(key, _)| *key == name).map(|(_, value)| (*value).into())
        })
        .unwrap();
        let engine = Arc::new(QueryEngine::new(Arc::new(config)).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(lens::serve_on(listener, Arc::clone(&engine), std::future::pending()));
        Self { url, engine, server: Some(server) }
    }

    fn client(&self) -> LensClient {
        client_for(&self.url, settings())
    }
}

impl Drop for RealLens {
    fn drop(&mut self) {
        self.engine.shutdown();
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

fn settings() -> LensClientSettings {
    LensClientSettings {
        // Far above any LENS query timeout used here, so LENS's own answer
        // always arrives first, however loaded the machine.
        request_timeout: Duration::from_secs(120),
        connect_timeout: Duration::from_secs(1),
        visibility_poll_interval: Duration::from_millis(50),
    }
}

/// A data dir PRISM has not written a slot to yet. LENS refuses a data dir
/// without any table directory, so another table's (empty) one is there.
fn no_slots_yet() -> TempDir {
    let dir = TempDir::new();
    fs::create_dir_all(dir.path().join("solana/token_transfers")).unwrap();
    dir
}

/// A data dir with slots 1000..1010 written.
fn dataset() -> TempDir {
    let dir = TempDir::new();
    prism_write_slots(dir.path(), 1_000..1_010);
    dir
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lens_runs_what_forge_sends_and_forge_parses_what_lens_returns() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let sql = "  SELECT slot, blockhash, leader, block_time FROM solana.slots ORDER BY slot LIMIT 3  ";
    let response = lens.client().query(sql).await.unwrap();

    assert_eq!(response.row_count, 3);
    assert_eq!(response.rows.len(), 3);
    assert!(!response.truncated);
    assert_eq!(response.sql, sql.trim(), "LENS echoes the SQL it ran, trimmed");
    assert!(response.round_trip_ms >= response.duration_ms, "{response:?}");

    let first = &response.rows[0];
    assert_eq!(first["slot"], json!(1_000));
    assert_eq!(first["blockhash"], json!("hash1000"));
    assert_eq!(first["leader"], json!("leader1000"));
    assert_eq!(first["block_time"], json!("2026-10-27T12:16:40Z"));
    let second = &response.rows[1];
    assert!(second.contains_key("leader"), "LENS writes explicit nulls: {second:?}");
    assert_eq!(second["leader"], json!(null));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integers_survive_the_round_trip_at_full_width() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let response = lens
        .client()
        .query("SELECT arrow_cast('18446744073709551615', 'UInt64') AS u64_max, arrow_cast('-9223372036854775808', 'Int64') AS i64_min")
        .await
        .unwrap();
    assert_eq!(response.rows[0]["u64_max"].as_u64(), Some(u64::MAX));
    assert_eq!(response.rows[0]["i64_min"].as_i64(), Some(i64::MIN));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn awkward_sql_text_reaches_lens_intact() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let sql = "SELECT 'it''s \"quoted\" \\ é ✓ 🚀' AS text -- a comment\n";
    let response = lens.client().query(sql).await.unwrap();
    assert_eq!(response.rows[0]["text"], json!("it's \"quoted\" \\ é ✓ 🚀"));
    assert_eq!(response.sql, sql.trim());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_result_and_a_truncated_one_parse() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[("LENS_MAX_QUERY_ROWS", "4")]).await;
    let client = lens.client();

    let empty = client.query("SELECT slot FROM solana.slots WHERE slot > 99999").await.unwrap();
    assert_eq!((empty.row_count, empty.rows.len(), empty.truncated), (0, 0, false));

    let truncated = client.query("SELECT slot FROM solana.slots").await.unwrap();
    assert_eq!((truncated.row_count, truncated.rows.len(), truncated.truncated), (4, 4, true));
}

// ---------------------------------------------------------------------------
// Failures LENS really produces
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lens_rejections_are_bad_queries() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let client = lens.client();
    for (sql, needle) in [
        ("SELECT * FROM solana.nonexistent", "query_plan_error: "),
        ("SELECT * FROM solana.nonexistent", "not found"),
        ("SELEC slot FROM solana.slots", "query_plan_error"),
        ("", "query_plan_error"),
        ("DROP TABLE solana.slots", "query_plan_error"),
        ("SELECT 1 / 0 AS boom", "ivide by zero"),
    ] {
        let err = client.query(sql).await.unwrap_err();
        assert!(matches!(err, LensCallError::BadQuery { .. }), "{sql:?}: {err}");
        assert!(err.message().contains(needle), "{sql:?}: {err}");
        assert!(!err.is_retryable());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lens_query_timeout_504_is_a_timeout() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "1")]).await;
    let err = lens.client().query(ENDLESS_SQL).await.unwrap_err();
    assert!(matches!(err, LensCallError::Timeout { .. }), "{err}");
    assert!(err.message().contains("504"), "{err}");
    assert!(err.message().contains("query_timeout: query exceeded the 1s timeout"), "{err}");
    assert!(err.is_retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forge_gives_up_on_its_own_timeout_when_lens_is_slower() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[("LENS_QUERY_TIMEOUT_SECS", "30")]).await;
    let client = client_for(&lens.url, LensClientSettings { request_timeout: Duration::from_millis(500), ..settings() });
    let started = Instant::now();
    let err = client.query(ENDLESS_SQL).await.unwrap_err();
    assert!(matches!(err, LensCallError::Timeout { .. }), "{err}");
    assert!(err.message().contains("within the timeout"), "FORGE's own timeout, not LENS's: {err}");
    assert!(started.elapsed() < Duration::from_secs(60), "{:?}", started.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_parquet_file_500_is_unavailable() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let dir = data.path().join("solana/slots/date=2099-01-01");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join("slots_9_9.parquet"), b"this is not a parquet file, not even close to one").unwrap();
    lens.engine.refresh_catalog();

    let err = lens.client().query("SELECT COUNT(*) FROM solana.slots WHERE leader IS NOT NULL").await.unwrap_err();
    assert!(matches!(err, LensCallError::Unavailable { .. }), "{err}");
    assert!(err.message().contains("500"), "{err}");
    assert!(err.is_retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_lens_is_unavailable() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let client = lens.client();
    client.query("SELECT 1").await.unwrap();
    let url = lens.url.clone();
    drop(lens);
    // Shutdown drains; give the listener a moment to close.
    let mut last = None;
    for _ in 0..50 {
        match client_for(&url, settings()).query("SELECT 1").await {
            Err(err) => {
                last = Some(err);
                break;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let err = last.expect("LENS stopped answering");
    assert!(matches!(err, LensCallError::Unavailable { .. }), "{err}");
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_reads_the_real_verdicts() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let health = lens.client().health().await.unwrap();
    assert_eq!(health.status, LensHealthStatus::Healthy);

    // A table directory LENS has not registered yet: degraded.
    let stranded = data.path().join("solana/transactions");
    fs::create_dir_all(&stranded).unwrap();
    assert_eq!(lens.client().health().await.unwrap().status, LensHealthStatus::Degraded);

    // Nothing registered: unhealthy, but reachable.
    fs::remove_dir_all(data.path().join("solana")).unwrap();
    lens.engine.refresh_catalog();
    let health = lens.client().health().await.unwrap();
    assert_eq!(health.status, LensHealthStatus::Unhealthy);
    assert!(!health.status.is_serving());
}

// ---------------------------------------------------------------------------
// Visibility, end to end
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_already_in_lens_is_visible_at_once() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let client = lens.client();
    assert_eq!(client.wait_until_visible(1_009, Duration::from_secs(60)).await, Visibility::Visible { max_slot: 1_009 });
    assert_eq!(client.wait_until_visible(1_000, Duration::from_secs(60)).await, Visibility::Visible { max_slot: 1_009 });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_prism_has_not_written_times_out_reporting_lens_max() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[]).await;
    let visibility = lens.client().wait_until_visible(1_010, Duration::from_secs(5)).await;
    assert_eq!(visibility, Visibility::TimedOut { last_seen: Some(1_009), last_error: None });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn before_the_slots_table_exists_the_gate_reports_why() {
    let empty = no_slots_yet();
    let lens = RealLens::start(empty.path(), &[]).await;
    let visibility = lens.client().wait_until_visible(1, Duration::from_secs(5)).await;
    let Visibility::TimedOut { last_seen: None, last_error: Some(error) } = &visibility else {
        panic!("expected a timeout with an error, got {visibility:?}");
    };
    assert!(error.starts_with("LENS rejected the query: query_plan_error: "), "{error}");
    assert!(error.contains("not found"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gate_opens_once_lens_refreshes_onto_new_prism_files() {
    let data = dataset();
    let lens = RealLens::start(data.path(), &[("LENS_CATALOG_REFRESH_INTERVAL_SECS", "1")]).await;
    let client = lens.client();

    // Slot 1015 only exists after the write below, so `Visible { 1015 }`
    // proves the gate opened on it, not before.
    let gate = {
        let client = client.clone();
        tokio::spawn(async move { client.wait_until_visible(1_015, Duration::from_secs(60)).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    prism_write_slots(data.path(), 1_010..=1_015);

    let visibility = gate.await.unwrap();
    assert_eq!(visibility, Visibility::Visible { max_slot: 1_015 });

    // And what the gate saw is what queries now see.
    let response = client.query("SELECT COUNT(*) AS n FROM solana.slots WHERE slot >= 1010").await.unwrap();
    assert_eq!(response.rows[0]["n"], json!(6));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_gate_waits_through_the_table_appearing_for_the_first_time() {
    // A brand-new table is only registered by LENS's next catalog refresh.
    let empty = no_slots_yet();
    let lens = RealLens::start(empty.path(), &[("LENS_CATALOG_REFRESH_INTERVAL_SECS", "1")]).await;
    let gate = {
        let client = lens.client();
        tokio::spawn(async move { client.wait_until_visible(500, Duration::from_secs(60)).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    prism_write_slots(empty.path(), 498..=500);
    assert_eq!(gate.await.unwrap(), Visibility::Visible { max_slot: 500 });
}
