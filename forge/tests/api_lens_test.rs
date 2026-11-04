//! FORGE's API against the real LENS, both over real sockets: PRISM's
//! `ParquetWriter` writes the tables, `lens::serve_on` serves them, and
//! `forge::serve_on` serves FORGE wired to it as `main.rs` wires it. A
//! client talks to FORGE over HTTP, as `curl` would.
//!
//! What a scripted LENS cannot show: that the SQL FORGE binds is SQL LENS
//! runs, that a dry run really catches an unknown column, and that the seed
//! is LENS's real `MAX(slot)`.

mod common;

use std::collections::HashMap;
use std::ffi::OsString;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::TempDir;
use forge::config::{ENV_BIND_ADDRESS, ENV_CHANGELOG_PATH, ENV_DB_PATH, ENV_LENS_URL};
use forge::{
    AlertEngine, AppState, ChangelogReader, CheckpointStore, ForgeConfig, LensApi, LensClient, LensClientSettings,
    Registry, WebhookService,
};
use lens::{LensConfig, QueryEngine};
use prism::storage::{ParquetWriter, Storage};
use prism::types::udm::{DeepAnaSlot, DeepAnaTokenTransfer};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const SLOTS: std::ops::Range<u64> = 1_000..1_010;

/// Slots, and one token transfer per slot: mint `MintA` on even slots,
/// `MintB` on odd ones, `amount_raw = slot * 10`.
fn prism_write(data_dir: &Path) {
    let mut writer = ParquetWriter::new(data_dir);
    for slot in SLOTS {
        let block = DeepAnaSlot {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_793_102_400 + slot as i64),
            block_height: Some(slot - 100),
            transaction_count: 1,
            leader: None,
        };
        let transfer = DeepAnaTokenTransfer {
            slot,
            tx_signature: format!("sig{slot}"),
            instruction_index: 0,
            inner_instruction_index: None,
            spl_token_program_id: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_owned(),
            source_token_account: format!("src{slot}"),
            destination_token_account: format!("dst{slot}"),
            source_owner: None,
            destination_owner: None,
            mint_token_address: Some(if slot % 2 == 0 { "MintA" } else { "MintB" }.to_owned()),
            amount_raw: slot * 10,
            decimals: Some(9),
            block_time: None,
        };
        writer.append(&block, &[], &[], &[transfer]).unwrap();
    }
    writer.flush().unwrap();
}

/// LENS and FORGE, each on a free port, over one scratch directory.
struct Stack {
    _dir: TempDir,
    lens_url: String,
    forge_url: String,
    http: reqwest::Client,
    lens_engine: Arc<QueryEngine>,
    cancel: CancellationToken,
}

impl Stack {
    async fn start() -> Self {
        let dir = TempDir::new();
        let data_dir = dir.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        prism_write(&data_dir);

        let data = data_dir.to_str().unwrap().to_owned();
        let lens_config = LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data.clone().into())).unwrap();
        let lens_engine = Arc::new(QueryEngine::new(Arc::new(lens_config)).unwrap());
        let lens_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let lens_url = format!("http://{}", lens_listener.local_addr().unwrap());
        tokio::spawn(lens::serve_on(lens_listener, Arc::clone(&lens_engine), std::future::pending()));

        let config = Arc::new(forge_config(dir.path(), &lens_url, None).unwrap());
        let registry = Arc::new(Registry::open(&config).unwrap());
        let reader = ChangelogReader::open(&config, CheckpointStore::for_config(&config)).unwrap();
        let settings = LensClientSettings {
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(1),
            visibility_poll_interval: Duration::from_millis(50),
        };
        let lens: Arc<dyn LensApi> = Arc::new(LensClient::with_settings(&config, settings).unwrap());
        let webhook = Arc::new(WebhookService::start(&config, registry.db().clone()).unwrap());
        let engine = AlertEngine::new(registry.clone(), reader, lens.clone(), webhook.clone(), &config).unwrap();
        let state = AppState::new(registry, lens, webhook, engine.handle(), config);
        let cancel = CancellationToken::new();
        tokio::spawn({
            let cancel = cancel.clone();
            async move { engine.run(cancel).await }
        });

        let forge_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let forge_url = format!("http://{}", forge_listener.local_addr().unwrap());
        tokio::spawn(forge::serve_on(forge_listener, state, cancel.clone().cancelled_owned()));

        Self { _dir: dir, lens_url, forge_url, http: reqwest::Client::new(), lens_engine, cancel }
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> (u16, Value) {
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        let body = response.bytes().await.unwrap();
        let value = if body.is_empty() { Value::Null } else { serde_json::from_slice(&body).unwrap() };
        (status, value)
    }

    async fn post(&self, path: &str, body: &Value) -> (u16, Value) {
        let request = self.http.post(format!("{}{path}", self.forge_url));
        self.send(request.header("Content-Type", "application/json").body(body.to_string())).await
    }

    async fn get(&self, path: &str) -> (u16, Value) {
        self.send(self.http.get(format!("{}{path}", self.forge_url))).await
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.lens_engine.shutdown();
    }
}

fn forge_config(dir: &Path, lens_url: &str, bind: Option<&str>) -> forge::ForgeResult<ForgeConfig> {
    let mut vars: HashMap<&str, OsString> = HashMap::from([
        (ENV_CHANGELOG_PATH, dir.join("changelog.jsonl").into_os_string()),
        (ENV_DB_PATH, dir.join("forge.db").into_os_string()),
        (ENV_LENS_URL, lens_url.into()),
    ]);
    if let Some(bind) = bind {
        vars.insert(ENV_BIND_ADDRESS, bind.into());
    }
    ForgeConfig::from_lookup(|name| vars.get(name).cloned())
}

/// The curl example's body, byte for byte.
const CURL_BODY: &str = r#"{ "name": "transfers-by-mint", "sql": "SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_address = {{mint}} ORDER BY slot DESC LIMIT {{n}}", "params": [ {"name": "mint", "kind": "string", "required": true}, {"name": "n", "kind": "u64", "required": false, "default": "10"} ]}"#;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_curl_example_is_saved_but_names_a_column_lens_does_not_have() {
    let stack = Stack::start().await;
    let (status, body) = stack
        .send(
            stack
                .http
                .post(format!("{}/queries", stack.forge_url))
                .header("Content-Type", "application/json")
                .body(CURL_BODY),
        )
        .await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["name"], json!("transfers-by-mint"));
    assert_eq!(body["params"][1], json!({"name": "n", "kind": "u64", "required": false, "default": "10"}));

    // PRISM's column is `mint_token_address`; LENS says so, through FORGE.
    let (status, body) = stack.get("/q/transfers-by-mint?mint=MintA").await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error_kind"], json!("lens_bad_query"));
    assert!(body["error"].as_str().unwrap().contains("mint_address"), "{body}");
    assert_eq!(
        body["sql"],
        json!("SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_address = 'MintA' ORDER BY slot DESC LIMIT 10")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_saved_query_runs_on_lens_with_its_parameters_bound() {
    let stack = Stack::start().await;
    let query = json!({
        "name": "transfers-by-mint",
        "sql": "SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_token_address = {{mint}} ORDER BY slot DESC LIMIT {{n}}",
        "params": [
            {"name": "mint", "kind": "string", "required": true},
            {"name": "n", "kind": "u64", "required": false, "default": "10"}
        ]
    });
    assert_eq!(stack.post("/queries", &query).await.0, 201);

    let (status, body) = stack.get("/q/transfers-by-mint?mint=MintA&n=3").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["query"], json!("transfers-by-mint"));
    assert_eq!(body["row_count"], json!(3));
    assert_eq!(body["truncated"], json!(false));
    assert_eq!(
        body["rows"],
        json!([
            {"tx_signature": "sig1008", "amount_raw": 10_080},
            {"tx_signature": "sig1006", "amount_raw": 10_060},
            {"tx_signature": "sig1004", "amount_raw": 10_040}
        ])
    );
    assert_eq!(
        body["sql"],
        json!("SELECT tx_signature, amount_raw FROM solana.token_transfers WHERE mint_token_address = 'MintA' ORDER BY slot DESC LIMIT 3")
    );

    // The default, and a value that would break out of an unquoted literal.
    let (_, body) = stack.get("/q/transfers-by-mint?mint=MintB").await;
    assert_eq!(body["row_count"], json!(5));
    let (status, body) = stack.get("/q/transfers-by-mint?mint=x%27+OR+%271%27%3D%271").await;
    assert_eq!((status, body["row_count"].clone()), (200, json!(0)), "{body}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_alerts_is_dry_run_and_seeded_on_the_real_lens() {
    let stack = Stack::start().await;
    let alert = |sql: &str| {
        json!({
            "name": "whales",
            "sql": sql,
            "webhook_url": "http://127.0.0.1:9/hook",
            "secret": "whsec-0123456789abcdef"
        })
    };

    let typo = "SELECT slot, amount FROM solana.token_transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";
    let (status, body) = stack.post("/alerts", &alert(typo)).await;
    assert_eq!((status, body["error_kind"].clone()), (400, json!("lens_bad_query")), "{body}");
    assert_eq!(
        body["sql"],
        json!("SELECT slot, amount FROM solana.token_transfers WHERE slot BETWEEN 0 AND 0")
    );
    assert_eq!(stack.get("/alerts").await.1, json!({ "alerts": [] }), "nothing created");

    let good = "SELECT slot, amount_raw FROM solana.token_transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";
    let (status, body) = stack.post("/alerts", &alert(good)).await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["last_evaluated_slot"], json!(SLOTS.end - 1), "seeded at LENS's MAX(slot)");
    assert_eq!(body["secret"], json!("whsec-0123456789abcdef"));
    let id = body["id"].as_str().unwrap();
    let (status, view) = stack.get(&format!("/alerts/{id}")).await;
    assert_eq!(status, 200);
    assert!(view.get("secret").is_none(), "{view}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_composes_the_real_lens_verdict() {
    let stack = Stack::start().await;
    let (status, body) = stack.get("/health").await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], json!("healthy"), "{body}");
    assert_eq!(body["lens"]["reachable"], json!(true));
    assert_eq!(body["lens"]["status"], json!("healthy"));
    assert_eq!(body["registry"], json!({ "schema_version": 2, "alerts": 0, "saved_queries": 0 }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forge_refuses_to_bind_the_address_lens_serves_on() {
    let stack = Stack::start().await;
    let dir = TempDir::new();
    let lens_addr = stack.lens_url.trim_start_matches("http://");
    let err = forge_config(dir.path(), &stack.lens_url, Some(lens_addr)).unwrap_err();
    assert_eq!(err.config_field(), Some("bind_address"), "{err}");
    let port = lens_addr.rsplit(':').next().unwrap();
    let err = forge_config(dir.path(), &stack.lens_url, Some(&format!("0.0.0.0:{port}"))).unwrap_err();
    assert_eq!(err.config_field(), Some("bind_address"), "{err}");
}
