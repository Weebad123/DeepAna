//! `forge::start`: the library service the `forge` binary and DeepAna run.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::Fixture;
use forge::Registry;
use http_body_util::BodyExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

/// A LENS URL nothing listens on: FORGE starts without LENS, and its
/// `/health` reports LENS unreachable.
const NO_LENS: &str = "http://127.0.0.1:9";

async fn get(router: &axum::Router, uri: &str) -> (StatusCode, Value) {
    let response = router.clone().oneshot(Request::get(uri).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn start_serves_the_api_and_stops_on_the_host_token() {
    let fixture = Fixture::new();
    let config = fixture.load(&[("FORGE_LENS_URL", NO_LENS)]).unwrap();
    let token = CancellationToken::new();
    let forge = forge::start(config, token.clone()).await.unwrap();
    let router = forge.router();

    let (status, body) = get(&router, "/health").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["lens"]["reachable"], false);
    let (status, body) = get(&router, "/alerts").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["alerts"], Value::Array(vec![]));

    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), forge.stopped()).await.unwrap();
    drop(router);
    tokio::time::timeout(Duration::from_secs(10), forge.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn wait_releases_the_registry_lock_once_the_routers_are_gone() {
    let fixture = Fixture::new();
    let config = fixture.load(&[("FORGE_LENS_URL", NO_LENS)]).unwrap();
    let token = CancellationToken::new();
    let forge = forge::start(config.clone(), token.clone()).await.unwrap();
    // Held while FORGE runs.
    assert!(Registry::open(&config).is_err(), "a second FORGE must not open the same database");
    token.cancel();
    tokio::time::timeout(Duration::from_secs(10), forge.wait()).await.unwrap().unwrap();
    Registry::open(&config).expect("the lock is released after wait");
}

#[tokio::test]
async fn forge_runs_on_a_child_token_and_never_cancels_the_host() {
    let fixture = Fixture::new();
    let config = fixture.load(&[("FORGE_LENS_URL", NO_LENS)]).unwrap();
    let host = CancellationToken::new();
    let forge = forge::start(config, host.clone()).await.unwrap();
    drop(forge);
    // Dropping the handle stops FORGE (its own token) but not the host.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!host.is_cancelled());
}

#[tokio::test]
async fn files_stay_next_to_the_configured_database() {
    let fixture = Fixture::new();
    let config = fixture.load(&[("FORGE_LENS_URL", NO_LENS)]).unwrap();
    let token = CancellationToken::new();
    let forge = forge::start(config, token.clone()).await.unwrap();
    token.cancel();
    forge.wait().await.unwrap();

    let allowed = ["forge.db", "forge.db.lock", "forge.db-wal", "forge.db-shm", "forge.checkpoint.json"];
    for entry in std::fs::read_dir(fixture.dir.path()).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        assert!(allowed.contains(&name.as_str()), "unexpected file {name}");
    }
    assert!(fixture.dir.path().join("forge.db").is_file());
}
