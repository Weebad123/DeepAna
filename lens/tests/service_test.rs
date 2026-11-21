//! `lens::start`: the library service the `lens` binary and DeepAna both run.
//!
//! PRISM itself is not started here (it needs Helius); these tests cover
//! LENS alone over Parquet files PRISM's writer made, and the lifecycle of
//! the handle.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use lens::config::ENV_DATA_DIR;
use lens::{LensConfig, LensServiceConfig};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::DeepAnaSlot;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lens-service-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_slots(dir: &Path, slots: impl IntoIterator<Item = u64>) {
    let mut writer = ParquetWriter::new(dir);
    for slot in slots {
        let row = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_793_102_400 + slot as i64),
            block_height: Some(slot),
            transaction_count: 0,
            leader: None,
        };
        writer.append(&row, &[], &[], &[]).unwrap();
    }
    writer.flush().unwrap();
}

fn serve_only(dir: &Path) -> LensServiceConfig {
    let config = LensConfig::load_with(|name| (name == ENV_DATA_DIR).then(|| dir.as_os_str().to_owned())).unwrap();
    LensServiceConfig::serve_only(config)
}

async fn call(router: &axum::Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn start_serves_queries_and_health_through_its_router() {
    let dir = TempDir::new();
    write_slots(dir.path(), 100..105);
    let token = CancellationToken::new();
    let lens = lens::start(serve_only(dir.path()), token.clone()).await.unwrap();
    let router = lens.router();

    let health = Request::get("/health").body(Body::empty()).unwrap();
    let (status, body) = call(&router, health).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "healthy");

    let query = Request::builder()
        .method(Method::POST)
        .uri("/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json!({ "sql": "SELECT MAX(slot) AS m FROM solana.slots" }).to_string()))
        .unwrap();
    let (status, body) = call(&router, query).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["rows"][0]["m"], 104);

    token.cancel();
    tokio::time::timeout(Duration::from_secs(10), lens.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn the_host_cancelling_its_token_stops_lens_and_the_engine() {
    let dir = TempDir::new();
    write_slots(dir.path(), 1..3);
    let token = CancellationToken::new();
    let lens = lens::start(serve_only(dir.path()), token.clone()).await.unwrap();
    let engine_stop = lens.engine().shutdown_token();
    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), lens.stopped()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), lens.wait()).await.unwrap().unwrap();
    assert!(engine_stop.is_cancelled(), "the catalog refresh is stopped");
}

#[tokio::test]
async fn lens_runs_on_a_child_token_and_never_cancels_the_host() {
    let dir = TempDir::new();
    write_slots(dir.path(), 1..3);
    let host = CancellationToken::new();
    let lens = lens::start(serve_only(dir.path()), host.clone()).await.unwrap();
    // The engine shutting down stops LENS, but is not the host's business.
    lens.engine().shutdown();
    tokio::time::timeout(Duration::from_secs(5), lens.stopped()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), lens.wait()).await.unwrap().unwrap();
    assert!(!host.is_cancelled());
}

#[tokio::test]
async fn dropping_the_handle_without_wait_stops_lens() {
    let dir = TempDir::new();
    write_slots(dir.path(), 1..3);
    let token = CancellationToken::new();
    let lens = lens::start(serve_only(dir.path()), token.clone()).await.unwrap();
    let engine_stop = lens.engine().shutdown_token();
    drop(lens);
    tokio::time::timeout(Duration::from_secs(5), engine_stop.cancelled()).await.unwrap();
    assert!(!token.is_cancelled());
}

#[test]
fn for_parquet_directory_creates_the_table_dirs_and_forces_the_data_dir() {
    let dir = TempDir::new();
    let config = LensServiceConfig::for_parquet_directory(dir.path(), |name| {
        // A LENS_DATA_DIR in the environment loses to the flag.
        (name == ENV_DATA_DIR).then(|| "/somewhere/else".into())
    })
    .unwrap();
    assert!(config.run_prism);
    assert_eq!(config.config.data_dir(), fs::canonicalize(dir.path()).unwrap());
    for table in lens::config::TABLE_DIRS {
        assert!(dir.path().join("solana").join(table).is_dir(), "{table}");
    }
    // Nothing else is created.
    let entries: Vec<_> = fs::read_dir(dir.path()).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(entries, vec![std::ffi::OsString::from("solana")]);
}

#[tokio::test]
async fn a_missing_helius_key_fails_the_start_before_anything_runs() {
    if std::env::var_os("HELIUS_API_KEY").is_some() {
        return; // Would start a real PRISM.
    }
    let dir = TempDir::new();
    let config = LensServiceConfig::for_parquet_directory(dir.path(), |_| None).unwrap();
    let err = lens::start(config, CancellationToken::new()).await.unwrap_err();
    assert_eq!(err.kind(), "prism_error", "{err}");
    // PRISM never opened its change log.
    assert!(!dir.path().join("changelog.jsonl").exists());
}
