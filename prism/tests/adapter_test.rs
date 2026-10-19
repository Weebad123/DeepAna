//! Tests for `prism::adapter`.
//!
//! Most adapter tests drive [`SolanaAdapter`] through a scripted
//! [`RpcSender`], which replays canned JSON-RPC results and records what was
//! asked. The tests at the bottom go through the SDK's real HTTP transport
//! against a local socket, for the behaviour that only exists there (status
//! codes, URLs in errors).

use std::collections::VecDeque;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use prism::adapter::config::{
    AdapterConfig, MAX_ACCOUNTS_PER_BATCH, SolanaAdapterConfig, TransactionEncoding, redact_url,
};
use prism::adapter::solana::SolanaAdapter;
use prism::adapter::{
    AccountData, AdapterMetricsSnapshot, ChainAdapter, ChainBlockData, ChainFamily, ChainTip,
    CommitmentLevel,
};
use prism::types::error::PrismError;
use serde_json::{Value, json};
use solana_client::client_error::{ClientError, Result as ClientResult};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_client::RpcClientConfig;
use solana_client::rpc_request::{RpcError, RpcRequest, RpcResponseErrorData};
use solana_client::rpc_sender::{RpcSender, RpcTransportStats};
use solana_transaction_status::UiTransactionEncoding;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const CLOCK_SYSVAR: &str = "SysvarC1ock11111111111111111111111111111111";

const BLOCKHASH: &str = "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU";
const PARENT_BLOCKHASH: &str = "4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi";

const SLOT_SKIPPED: i64 = -32007;
const LONG_TERM_STORAGE_SLOT_SKIPPED: i64 = -32009;
const BLOCK_NOT_AVAILABLE: i64 = -32004;
const NODE_UNHEALTHY: i64 = -32005;
const BLOCK_CLEANED_UP: i64 = -32001;
const INVALID_PARAMS: i64 = -32602;

// ------------------------------------------------------------------ scripted sender

/// What the scripted sender will answer, and what it was asked.
#[derive(Clone, Default)]
struct Script {
    replies: Arc<Mutex<VecDeque<ClientResult<Value>>>>,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
}

impl Script {
    /// `(method, params)` of every request sent so far.
    fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().unwrap().clone()
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

struct ScriptedSender(Script);

#[async_trait]
impl RpcSender for ScriptedSender {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        self.0
            .calls
            .lock()
            .unwrap()
            .push((request.to_string(), params));
        self.0
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("the adapter sent more requests than the test scripted")
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        RpcTransportStats::default()
    }

    fn url(&self) -> String {
        "scripted".into()
    }
}

/// A valid config with a rate limit and retry delays small enough not to
/// slow the tests down.
fn test_config() -> SolanaAdapterConfig {
    SolanaAdapterConfig {
        base: AdapterConfig {
            max_retries: 3,
            retry_base_delay_ms: 1,
            retry_max_delay_ms: 4,
            ..AdapterConfig::new("solana-testnet")
        },
        requests_per_second: 1_000,
        ..SolanaAdapterConfig::new("solana-testnet", "http://127.0.0.1:8899")
    }
}

fn scripted(
    config: SolanaAdapterConfig,
    replies: Vec<ClientResult<Value>>,
) -> (SolanaAdapter, Script) {
    let script = Script {
        replies: Arc::new(Mutex::new(replies.into())),
        ..Script::default()
    };
    let client = RpcClient::new_sender(ScriptedSender(script.clone()), RpcClientConfig::default());
    let adapter = SolanaAdapter::with_client(config, client).expect("test config is valid");
    (adapter, script)
}

fn rpc_error(code: i64, message: &str) -> ClientError {
    RpcError::RpcResponseError {
        code,
        message: message.into(),
        data: RpcResponseErrorData::Empty,
    }
    .into()
}

fn timeout() -> ClientError {
    io::Error::new(io::ErrorKind::TimedOut, "request timed out").into()
}

fn block_json() -> Value {
    json!({
        "previousBlockhash": PARENT_BLOCKHASH,
        "blockhash": BLOCKHASH,
        "parentSlot": 249_999_998u64,
        "transactions": [
            { "transaction": ["AQID", "base58"], "meta": null, "version": 0 },
            { "transaction": ["BAUG", "base58"], "meta": null, "version": "legacy" },
            { "transaction": ["BwgJ", "base58"], "meta": null, "version": 1 },
        ],
        "rewards": [],
        "blockTime": 1_700_000_000,
        "blockHeight": 230_000_000u64,
    })
}

fn tip_json(slot: u64) -> Value {
    json!({
        "context": { "slot": slot },
        "value": { "blockhash": BLOCKHASH, "lastValidBlockHeight": slot + 150 },
    })
}

fn account_json(owner: &str, lamports: u64, data_base64: &str) -> Value {
    json!({
        "lamports": lamports,
        "data": [data_base64, "base64"],
        "owner": owner,
        "executable": false,
        "rentEpoch": u64::MAX,
        "space": 3,
    })
}

fn accounts_json(accounts: Vec<Value>) -> Value {
    json!({ "context": { "slot": 250_000_000u64 }, "value": accounts })
}

fn addresses(list: &[&str]) -> Vec<String> {
    list.iter().map(|a| a.to_string()).collect()
}

// ------------------------------------------------------------------ config

#[test]
fn adapter_config_defaults() {
    let config = AdapterConfig::new("solana-mainnet");
    assert_eq!(config.chain_id, "solana-mainnet");
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.retry_base_delay_ms, 500);
    assert_eq!(config.retry_max_delay_ms, 30_000);
    assert_eq!(config.request_timeout_ms, 10_000);
    assert_eq!(config.health_check_interval_s, 30);

    assert_eq!(config.retry_base_delay(), Duration::from_millis(500));
    assert_eq!(config.retry_max_delay(), Duration::from_secs(30));
    assert_eq!(config.request_timeout(), Duration::from_secs(10));
    assert_eq!(config.health_check_interval(), Duration::from_secs(30));
    config.validate().unwrap();
}

#[test]
fn solana_adapter_config_defaults() {
    let config = SolanaAdapterConfig::new("solana-mainnet", "https://api.mainnet-beta.solana.com");
    assert_eq!(config.base, AdapterConfig::new("solana-mainnet"));
    assert_eq!(config.rpc_url, "https://api.mainnet-beta.solana.com");
    assert_eq!(config.ws_url, None);
    assert_eq!(config.commitment, CommitmentLevel::Confirmed);
    assert_eq!(config.requests_per_second, 50);
    assert_eq!(config.max_accounts_per_batch, 100);
    assert!(config.skip_failed_transactions);
    assert_eq!(config.encoding, TransactionEncoding::Base58);
    config.validate().unwrap();
}

#[test]
fn validate_names_the_offending_field() {
    type Break = fn(&mut SolanaAdapterConfig);
    let cases: Vec<(&str, Break)> = vec![
        ("chain_id", |c| c.base.chain_id = "  ".into()),
        ("retry_base_delay_ms", |c| c.base.retry_base_delay_ms = 0),
        ("retry_max_delay_ms", |c| {
            c.base.retry_base_delay_ms = 1_000;
            c.base.retry_max_delay_ms = 999;
        }),
        ("request_timeout_ms", |c| c.base.request_timeout_ms = 0),
        ("health_check_interval_s", |c| {
            c.base.health_check_interval_s = 0
        }),
        ("rpc_url", |c| c.rpc_url = String::new()),
        ("rpc_url", |c| c.rpc_url = "https://".into()),
        ("rpc_url", |c| c.rpc_url = "wss://example.com".into()),
        ("ws_url", |c| c.ws_url = Some("https://example.com".into())),
        ("requests_per_second", |c| c.requests_per_second = 0),
        ("max_accounts_per_batch", |c| c.max_accounts_per_batch = 0),
        ("max_accounts_per_batch", |c| {
            c.max_accounts_per_batch = MAX_ACCOUNTS_PER_BATCH + 1
        }),
    ];

    for (expected_field, break_it) in cases {
        let mut config = test_config();
        break_it(&mut config);
        match config.validate() {
            Err(PrismError::ConfigError { field, .. }) => assert_eq!(field, expected_field),
            other => panic!("expected ConfigError on {expected_field}, got {other:?}"),
        }
    }
}

#[test]
fn validate_accepts_the_edges() {
    let mut config = test_config();
    config.base.max_retries = 0;
    config.base.retry_max_delay_ms = config.base.retry_base_delay_ms;
    config.ws_url = Some("wss://example.com/ws".into());
    config.max_accounts_per_batch = 1;
    config.commitment = CommitmentLevel::Processed;
    config.validate().unwrap();
}

#[test]
fn config_deserializes_with_defaults_for_missing_fields() {
    let config: SolanaAdapterConfig = serde_json::from_value(json!({
        "base": { "chain_id": "solana-devnet", "max_retries": 2 },
        "rpc_url": "https://api.devnet.solana.com",
        "commitment": "finalized",
        "encoding": "base64",
    }))
    .unwrap();

    let expected = SolanaAdapterConfig {
        base: AdapterConfig {
            max_retries: 2,
            ..AdapterConfig::new("solana-devnet")
        },
        commitment: CommitmentLevel::Finalized,
        encoding: TransactionEncoding::Base64,
        ..SolanaAdapterConfig::new("solana-devnet", "https://api.devnet.solana.com")
    };
    assert_eq!(config, expected);

    // The required fields have no usable default: omitting them parses, but
    // does not validate.
    let empty: SolanaAdapterConfig = serde_json::from_value(json!({})).unwrap();
    assert!(empty.validate().is_err());
}

#[test]
fn config_round_trips_through_json() {
    let config = SolanaAdapterConfig {
        ws_url: Some("wss://example.com".into()),
        encoding: TransactionEncoding::JsonParsed,
        ..test_config()
    };
    let json = serde_json::to_string(&config).unwrap();
    assert_eq!(
        serde_json::from_str::<SolanaAdapterConfig>(&json).unwrap(),
        config
    );
}

#[test]
fn enums_serialize_as_their_str_form() {
    for encoding in TransactionEncoding::ALL {
        assert_eq!(serde_json::to_value(encoding).unwrap(), encoding.as_str());
        assert_eq!(encoding.to_string(), encoding.as_str());
    }
    for commitment in [
        CommitmentLevel::Confirmed,
        CommitmentLevel::Finalized,
        CommitmentLevel::Processed,
    ] {
        assert_eq!(
            serde_json::to_value(commitment).unwrap(),
            commitment.as_str()
        );
        assert_eq!(commitment.to_string(), commitment.as_str());
    }
    for family in [ChainFamily::Solana, ChainFamily::Evm] {
        assert_eq!(serde_json::to_value(family).unwrap(), family.as_str());
        assert_eq!(family.to_string(), family.as_str());
    }
}

#[test]
fn transaction_encoding_maps_onto_the_sdk_encoding() {
    let expected = [
        (TransactionEncoding::Base58, UiTransactionEncoding::Base58),
        (TransactionEncoding::Base64, UiTransactionEncoding::Base64),
        (TransactionEncoding::Json, UiTransactionEncoding::Json),
        (
            TransactionEncoding::JsonParsed,
            UiTransactionEncoding::JsonParsed,
        ),
    ];
    assert_eq!(expected.len(), TransactionEncoding::ALL.len());
    for (ours, theirs) in expected {
        assert_eq!(UiTransactionEncoding::from(ours), theirs);
    }
}

#[test]
fn redact_url_keeps_only_scheme_and_host() {
    let cases = [
        (
            "https://mainnet.helius-rpc.com/?api-key=SECRET",
            "https://mainnet.helius-rpc.com/<redacted>",
        ),
        (
            "https://solana-mainnet.g.alchemy.com/v2/SECRET",
            "https://solana-mainnet.g.alchemy.com/<redacted>",
        ),
        (
            "https://example.com?api-key=SECRET",
            "https://example.com/<redacted>",
        ),
        (
            "https://user:SECRET@example.com:8899/rpc",
            "https://example.com:8899/<redacted>",
        ),
        (
            "https://api.devnet.solana.com",
            "https://api.devnet.solana.com",
        ),
        (
            "https://api.devnet.solana.com/",
            "https://api.devnet.solana.com",
        ),
        ("http://127.0.0.1:8899", "http://127.0.0.1:8899"),
        ("", ""),
    ];
    for (url, expected) in cases {
        assert_eq!(redact_url(url), expected, "redacting {url:?}");
    }
}

#[test]
fn debug_output_does_not_contain_the_api_key() {
    let config = SolanaAdapterConfig {
        ws_url: Some("wss://mainnet.helius-rpc.com/?api-key=SECRET".into()),
        ..SolanaAdapterConfig::new(
            "solana-mainnet",
            "https://mainnet.helius-rpc.com/?api-key=SECRET",
        )
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("SECRET"), "{debug}");
    assert!(debug.contains("mainnet.helius-rpc.com"), "{debug}");

    let adapter = SolanaAdapter::new(config).unwrap();
    let debug = format!("{adapter:?}");
    assert!(!debug.contains("SECRET"), "{debug}");
}

// ------------------------------------------------------------------ construction

#[test]
fn new_rejects_an_invalid_config() {
    let mut config = test_config();
    config.requests_per_second = 0;
    match SolanaAdapter::new(config) {
        Err(PrismError::ConfigError { field, .. }) => assert_eq!(field, "requests_per_second"),
        other => panic!("expected ConfigError, got {other:?}"),
    }
}

#[test]
fn reports_its_chain() {
    let (adapter, _) = scripted(test_config(), vec![]);
    assert_eq!(adapter.chain_family(), ChainFamily::Solana);
    assert_eq!(adapter.chain_id(), "solana-testnet");
    assert_eq!(adapter.config(), &test_config());
    assert_eq!(
        adapter.metrics().snapshot(),
        AdapterMetricsSnapshot::default()
    );
}

#[tokio::test]
async fn is_usable_as_a_shared_trait_object() {
    let (adapter, script) = scripted(test_config(), vec![Ok(tip_json(1)), Ok(tip_json(2))]);
    let adapter: Arc<dyn ChainAdapter> = Arc::new(adapter);

    let tasks: Vec<_> = (0..2)
        .map(|_| {
            let adapter = Arc::clone(&adapter);
            tokio::spawn(async move { adapter.get_tip(CommitmentLevel::Confirmed).await })
        })
        .collect();
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert_eq!(script.call_count(), 2);
}

// ------------------------------------------------------------------ get_tip

#[tokio::test]
async fn get_tip_reads_slot_and_blockhash_from_one_call() {
    let (adapter, script) = scripted(test_config(), vec![Ok(tip_json(250_000_000))]);

    let tip = adapter.get_tip(CommitmentLevel::Finalized).await.unwrap();

    assert_eq!(
        tip,
        ChainTip {
            slot_or_height: 250_000_000,
            block_id: BLOCKHASH.into(),
            commitment: CommitmentLevel::Finalized,
        }
    );
    assert_eq!(
        script.calls(),
        vec![(
            "getLatestBlockhash".to_string(),
            json!([{ "commitment": "finalized" }])
        )]
    );
}

#[tokio::test]
async fn get_tip_uses_the_requested_commitment_not_the_configured_one() {
    let mut config = test_config();
    config.commitment = CommitmentLevel::Finalized;
    let (adapter, script) = scripted(config, vec![Ok(tip_json(7))]);

    let tip = adapter.get_tip(CommitmentLevel::Processed).await.unwrap();

    assert_eq!(tip.commitment, CommitmentLevel::Processed);
    assert_eq!(script.calls()[0].1, json!([{ "commitment": "processed" }]));
}

// ------------------------------------------------------------------ get_block

#[tokio::test]
async fn get_block_wraps_the_raw_block_in_a_chain_agnostic_header() {
    let (adapter, script) = scripted(test_config(), vec![Ok(block_json())]);

    let block = adapter.get_block(250_000_000).await.unwrap();

    assert_eq!(block.slot_or_height, 250_000_000);
    assert_eq!(block.block_id, BLOCKHASH);
    assert_eq!(block.parent_id, PARENT_BLOCKHASH);
    assert_eq!(block.timestamp, Some(1_700_000_000));
    let ChainBlockData::Solana(raw) = &block.raw;
    assert_eq!(raw.blockhash, BLOCKHASH);
    assert_eq!(raw.previous_blockhash, PARENT_BLOCKHASH);
    assert_eq!(raw.parent_slot, 249_999_998);
    assert_eq!(raw.block_height, Some(230_000_000));
    assert_eq!(raw.transactions.len(), 3);

    assert_eq!(
        script.calls(),
        vec![(
            "getBlock".to_string(),
            json!([
                250_000_000u64,
                {
                    "encoding": "base58",
                    "transactionDetails": "full",
                    "rewards": true,
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 1,
                }
            ])
        )]
    );
    assert_eq!(
        adapter.metrics().snapshot(),
        AdapterMetricsSnapshot {
            requests: 1,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn get_block_passes_the_timestamp_through_when_absent() {
    let mut block = block_json();
    block["blockTime"] = Value::Null;
    let (adapter, _) = scripted(test_config(), vec![Ok(block)]);

    assert_eq!(adapter.get_block(1).await.unwrap().timestamp, None);
}

#[tokio::test]
async fn get_block_requests_the_configured_encoding_and_commitment() {
    let mut config = test_config();
    config.encoding = TransactionEncoding::Base64;
    config.commitment = CommitmentLevel::Finalized;
    let (adapter, script) = scripted(config, vec![Ok(block_json())]);

    adapter.get_block(1).await.unwrap();

    let options = &script.calls()[0].1[1];
    assert_eq!(options["encoding"], "base64");
    assert_eq!(options["commitment"], "finalized");
}

#[tokio::test]
async fn get_block_falls_back_to_confirmed_when_configured_for_processed() {
    let mut config = test_config();
    config.commitment = CommitmentLevel::Processed;
    let (adapter, script) = scripted(config, vec![Ok(block_json())]);

    adapter.get_block(1).await.unwrap();

    assert_eq!(script.calls()[0].1[1]["commitment"], "confirmed");
}

#[tokio::test]
async fn get_block_reports_a_skipped_slot_without_retrying() {
    for code in [SLOT_SKIPPED, LONG_TERM_STORAGE_SLOT_SKIPPED] {
        let (adapter, script) = scripted(
            test_config(),
            vec![Err(rpc_error(code, "Slot 42 was skipped"))],
        );

        match adapter.get_block(42).await {
            Err(PrismError::SlotSkipped { slot }) => assert_eq!(slot, 42),
            other => panic!("expected SlotSkipped for code {code}, got {other:?}"),
        }
        assert_eq!(script.call_count(), 1);
        assert_eq!(
            adapter.metrics().snapshot(),
            AdapterMetricsSnapshot {
                requests: 1,
                slots_skipped: 1,
                ..Default::default()
            }
        );
    }
}

// ------------------------------------------------------------------ retry

#[tokio::test]
async fn transient_failures_are_retried_until_success() {
    let (adapter, script) = scripted(
        test_config(),
        vec![
            Err(timeout()),
            Err(rpc_error(
                BLOCK_NOT_AVAILABLE,
                "Block not available for slot 9",
            )),
            Ok(block_json()),
        ],
    );

    let block = adapter.get_block(9).await.unwrap();

    assert_eq!(block.block_id, BLOCKHASH);
    assert_eq!(script.call_count(), 3);
    // Every attempt repeats the same request.
    let calls = script.calls();
    assert!(calls.iter().all(|call| call == &calls[0]));
    assert_eq!(
        adapter.metrics().snapshot(),
        AdapterMetricsSnapshot {
            requests: 3,
            errors: 2,
            retries: 2,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn gives_up_after_max_retries_and_returns_the_last_error() {
    let config = test_config();
    let max_retries = config.base.max_retries as usize;
    let mut replies: Vec<ClientResult<Value>> = (0..max_retries).map(|_| Err(timeout())).collect();
    replies.push(Err(rpc_error(
        NODE_UNHEALTHY,
        "Node is behind by 300 slots",
    )));
    let (adapter, script) = scripted(config, replies);

    let err = adapter.get_block(9).await.unwrap_err();

    match &err {
        PrismError::RpcError { slot, method, .. } => {
            assert_eq!(*slot, Some(9));
            assert_eq!(method, "getBlock");
        }
        other => panic!("expected RpcError, got {other:?}"),
    }
    assert!(err.to_string().contains("Node is behind"), "{err}");
    assert_eq!(script.call_count(), max_retries + 1);
    assert_eq!(
        adapter.metrics().snapshot(),
        AdapterMetricsSnapshot {
            requests: 4,
            errors: 4,
            retries: 3,
            ..Default::default()
        }
    );
}

#[tokio::test]
async fn failures_that_retrying_cannot_fix_are_not_retried() {
    let permanent = [
        rpc_error(INVALID_PARAMS, "Invalid params"),
        rpc_error(BLOCK_CLEANED_UP, "Block 9 cleaned up"),
        RpcError::ParseError("Hash".into()).into(),
        serde_json::from_str::<u64>("not json").unwrap_err().into(),
    ];
    for error in permanent {
        let description = error.to_string();
        let (adapter, script) = scripted(test_config(), vec![Err(error)]);

        let err = adapter.get_block(9).await.unwrap_err();

        assert!(
            matches!(err, PrismError::RpcError { .. }),
            "{description}: {err:?}"
        );
        assert_eq!(script.call_count(), 1, "{description}");
        assert_eq!(adapter.metrics().snapshot().retries, 0, "{description}");
    }
}

#[tokio::test]
async fn zero_max_retries_means_a_single_attempt() {
    let mut config = test_config();
    config.base.max_retries = 0;
    let (adapter, script) = scripted(config, vec![Err(timeout())]);

    adapter
        .get_tip(CommitmentLevel::Confirmed)
        .await
        .unwrap_err();

    assert_eq!(script.call_count(), 1);
}

#[tokio::test]
async fn retry_delays_grow_from_the_base_delay() {
    let mut config = test_config();
    config.base.max_retries = 3;
    config.base.retry_base_delay_ms = 40;
    config.base.retry_max_delay_ms = 1_000;
    let (adapter, _) = scripted(
        config,
        vec![
            Err(timeout()),
            Err(timeout()),
            Err(timeout()),
            Ok(tip_json(1)),
        ],
    );

    let started = Instant::now();
    adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();

    // 40 + 80 + 160 ms, each jittered down by at most half.
    assert!(
        started.elapsed() >= Duration::from_millis(140),
        "retried too fast: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn retry_delays_are_capped_at_the_max_delay() {
    let mut config = test_config();
    config.base.max_retries = 8;
    config.base.retry_base_delay_ms = 2;
    config.base.retry_max_delay_ms = 2;
    let mut replies: Vec<ClientResult<Value>> = (0..8).map(|_| Err(timeout())).collect();
    replies.push(Ok(tip_json(1)));
    let (adapter, _) = scripted(config, replies);

    let started = Instant::now();
    adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();

    // Uncapped, eight doublings from 2 ms would wait at least 255 ms even
    // with the jitter at its lowest; capped, 16 ms plus scheduling noise.
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "delays were not capped: {:?}",
        started.elapsed()
    );
}

// ------------------------------------------------------------------ get_accounts

#[tokio::test]
async fn get_accounts_returns_accounts_in_request_order() {
    let (adapter, script) = scripted(
        test_config(),
        vec![Ok(accounts_json(vec![
            account_json(TOKEN_PROGRAM, 2_039_280, "AQID"),
            Value::Null,
            account_json(SYSTEM_PROGRAM, 1, ""),
        ]))],
    );

    let accounts = adapter
        .get_accounts(&addresses(&[USDC_MINT, WSOL_MINT, CLOCK_SYSVAR]))
        .await
        .unwrap();

    assert_eq!(
        accounts,
        vec![
            Some(AccountData {
                owner_program: TOKEN_PROGRAM.into(),
                lamports: 2_039_280,
                data: vec![1, 2, 3],
                executable: false,
                rent_epoch: u64::MAX,
            }),
            None,
            Some(AccountData {
                owner_program: SYSTEM_PROGRAM.into(),
                lamports: 1,
                data: vec![],
                executable: false,
                rent_epoch: u64::MAX,
            }),
        ]
    );

    let calls = script.calls();
    assert_eq!(calls.len(), 1);
    let (method, params) = &calls[0];
    assert_eq!(method, "getMultipleAccounts");
    assert_eq!(params[0], json!([USDC_MINT, WSOL_MINT, CLOCK_SYSVAR]));
    assert_eq!(params[1]["commitment"], "confirmed");
    assert_eq!(params[1]["encoding"], "base64+zstd");
}

#[tokio::test]
async fn get_accounts_splits_requests_into_batches() {
    let mut config = test_config();
    config.max_accounts_per_batch = 2;
    let (adapter, script) = scripted(
        config,
        vec![
            Ok(accounts_json(vec![
                account_json(SYSTEM_PROGRAM, 1, ""),
                account_json(SYSTEM_PROGRAM, 2, ""),
            ])),
            Ok(accounts_json(vec![
                Value::Null,
                account_json(SYSTEM_PROGRAM, 4, ""),
            ])),
            Ok(accounts_json(vec![account_json(SYSTEM_PROGRAM, 5, "")])),
        ],
    );

    let accounts = adapter
        .get_accounts(&addresses(&[
            SYSTEM_PROGRAM,
            TOKEN_PROGRAM,
            USDC_MINT,
            WSOL_MINT,
            CLOCK_SYSVAR,
        ]))
        .await
        .unwrap();

    let lamports: Vec<Option<u64>> = accounts
        .iter()
        .map(|a| a.as_ref().map(|a| a.lamports))
        .collect();
    assert_eq!(lamports, vec![Some(1), Some(2), None, Some(4), Some(5)]);

    let requested: Vec<Value> = script
        .calls()
        .into_iter()
        .map(|(_, p)| p[0].clone())
        .collect();
    assert_eq!(
        requested,
        vec![
            json!([SYSTEM_PROGRAM, TOKEN_PROGRAM]),
            json!([USDC_MINT, WSOL_MINT]),
            json!([CLOCK_SYSVAR]),
        ]
    );
}

#[tokio::test]
async fn get_accounts_with_no_addresses_makes_no_request() {
    let (adapter, script) = scripted(test_config(), vec![]);

    assert_eq!(adapter.get_accounts(&[]).await.unwrap(), vec![]);
    assert_eq!(script.call_count(), 0);
}

#[tokio::test]
async fn get_accounts_rejects_a_malformed_address_before_any_request() {
    let (adapter, script) = scripted(test_config(), vec![]);

    let err = adapter
        .get_accounts(&addresses(&[USDC_MINT, "not-a-pubkey"]))
        .await
        .unwrap_err();

    match err {
        PrismError::AccountResolveError { pubkey, .. } => assert_eq!(pubkey, "not-a-pubkey"),
        other => panic!("expected AccountResolveError, got {other:?}"),
    }
    assert_eq!(script.call_count(), 0);
}

#[tokio::test]
async fn get_accounts_rejects_a_response_of_the_wrong_length() {
    let (adapter, _) = scripted(
        test_config(),
        vec![Ok(accounts_json(vec![account_json(SYSTEM_PROGRAM, 1, "")]))],
    );

    let err = adapter
        .get_accounts(&addresses(&[USDC_MINT, WSOL_MINT]))
        .await
        .unwrap_err();

    assert!(
        matches!(err, PrismError::DeserializeError { .. }),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("asked for 2 accounts, got 1"),
        "{err}"
    );
}

#[tokio::test]
async fn get_accounts_rejects_data_that_is_not_binary() {
    let mut parsed = account_json(TOKEN_PROGRAM, 1, "");
    parsed["data"] = json!({ "program": "spl-token", "parsed": {}, "space": 82 });
    let (adapter, _) = scripted(test_config(), vec![Ok(accounts_json(vec![parsed]))]);

    let err = adapter
        .get_accounts(&addresses(&[USDC_MINT]))
        .await
        .unwrap_err();

    assert!(
        matches!(err, PrismError::DeserializeError { .. }),
        "{err:?}"
    );
    assert!(err.to_string().contains(USDC_MINT), "{err}");
}

#[tokio::test]
async fn get_accounts_retries_a_failed_batch() {
    let (adapter, script) = scripted(
        test_config(),
        vec![
            Err(timeout()),
            Ok(accounts_json(vec![account_json(SYSTEM_PROGRAM, 1, "")])),
        ],
    );

    let accounts = adapter
        .get_accounts(&addresses(&[USDC_MINT]))
        .await
        .unwrap();

    assert_eq!(accounts.len(), 1);
    assert_eq!(script.call_count(), 2);
}

// ------------------------------------------------------------------ health_check

#[tokio::test]
async fn health_check_passes_when_the_node_says_ok() {
    let (adapter, script) = scripted(test_config(), vec![Ok(json!("ok"))]);

    adapter.health_check().await.unwrap();

    assert_eq!(script.calls()[0].0, "getHealth");
}

#[tokio::test]
async fn health_check_reports_an_unhealthy_node_without_retrying() {
    let (adapter, script) = scripted(
        test_config(),
        vec![Err(rpc_error(
            NODE_UNHEALTHY,
            "Node is behind by 300 slots",
        ))],
    );

    let err = adapter.health_check().await.unwrap_err();

    match err {
        PrismError::RpcError { method, slot, .. } => {
            assert_eq!(method, "getHealth");
            assert_eq!(slot, None);
        }
        other => panic!("expected RpcError, got {other:?}"),
    }
    assert_eq!(script.call_count(), 1);
}

// ------------------------------------------------------------------ rate limiting

#[tokio::test]
async fn requests_are_spaced_by_the_rate_limit() {
    let mut config = test_config();
    config.requests_per_second = 20;
    let (adapter, _) = scripted(config, (0..4).map(|slot| Ok(tip_json(slot))).collect());

    let started = Instant::now();
    for _ in 0..4 {
        adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();
    }

    // The first request goes out at once; each of the other three waits 50 ms.
    assert!(
        started.elapsed() >= Duration::from_millis(140),
        "4 requests at 20/s took only {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn retries_are_rate_limited_too() {
    let mut config = test_config();
    config.requests_per_second = 20;
    let (adapter, _) = scripted(
        config,
        vec![
            Err(timeout()),
            Err(timeout()),
            Err(timeout()),
            Ok(tip_json(1)),
        ],
    );

    let started = Instant::now();
    adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();

    // Backoff alone (1 + 2 + 4 ms at most) would finish far sooner.
    assert!(
        started.elapsed() >= Duration::from_millis(140),
        "4 attempts at 20/s took only {:?}",
        started.elapsed()
    );
}

// ------------------------------------------------------------------ over HTTP

struct HttpReply {
    status: u16,
    extra_headers: &'static str,
    body: String,
}

impl HttpReply {
    fn result(result: Value) -> Self {
        Self {
            status: 200,
            extra_headers: "",
            body: json!({ "jsonrpc": "2.0", "id": 0, "result": result }).to_string(),
        }
    }

    fn status(status: u16) -> Self {
        Self {
            status,
            extra_headers: "",
            body: String::new(),
        }
    }
}

/// Serve `replies` in order, one per HTTP request, repeating the last one
/// forever. Returns the server's base URL and a counter of requests served.
async fn serve(replies: Vec<HttpReply>) -> (String, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let hits = Arc::new(AtomicUsize::new(0));

    let served = Arc::clone(&hits);
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            if !read_request(&mut stream).await {
                continue;
            }
            let index = served.fetch_add(1, Ordering::SeqCst);
            let reply = &replies[index.min(replies.len() - 1)];
            let response = format!(
                "HTTP/1.1 {} Scripted\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                reply.status,
                reply.body.len(),
                reply.extra_headers,
                reply.body,
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            let _ = stream.shutdown().await;
        }
    });
    (url, hits)
}

/// Read one full HTTP request off `stream`. `false` when the peer hung up
/// without sending one.
async fn read_request(stream: &mut TcpStream) -> bool {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            return false;
        }
        request.extend_from_slice(&chunk[..read]);

        let Some(head_len) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&request[..head_len]).to_ascii_lowercase();
        let body_len = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |value| value.trim().parse::<usize>().unwrap());
        if request.len() >= head_len + 4 + body_len {
            return true;
        }
    }
}

fn http_config(url: String) -> SolanaAdapterConfig {
    SolanaAdapterConfig {
        rpc_url: url,
        ..test_config()
    }
}

#[tokio::test]
async fn http_round_trip() {
    let (url, hits) = serve(vec![HttpReply::result(tip_json(123))]).await;
    let adapter = SolanaAdapter::new(http_config(url)).unwrap();

    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();

    assert_eq!(tip.slot_or_height, 123);
    assert_eq!(tip.block_id, BLOCKHASH);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_json_rpc_error_for_a_skipped_slot() {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 0,
        "error": {
            "code": SLOT_SKIPPED,
            "message": "Slot 42 was skipped, or missing due to ledger jump to recent snapshot",
        },
    });
    let (url, hits) = serve(vec![HttpReply {
        status: 200,
        extra_headers: "",
        body: body.to_string(),
    }])
    .await;
    let adapter = SolanaAdapter::new(http_config(url)).unwrap();

    let err = adapter.get_block(42).await.unwrap_err();

    assert!(
        matches!(err, PrismError::SlotSkipped { slot: 42 }),
        "{err:?}"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_server_errors_are_retried() {
    let (url, hits) = serve(vec![
        HttpReply::status(503),
        HttpReply::status(502),
        HttpReply::result(tip_json(5)),
    ])
    .await;
    let adapter = SolanaAdapter::new(http_config(url)).unwrap();

    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();

    assert_eq!(tip.slot_or_height, 5);
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    assert_eq!(adapter.metrics().snapshot().retries, 2);
}

#[tokio::test]
async fn http_client_errors_are_not_retried() {
    for status in [401, 403, 404] {
        let (url, hits) = serve(vec![HttpReply::status(status)]).await;
        let adapter = SolanaAdapter::new(http_config(url)).unwrap();

        let err = adapter
            .get_tip(CommitmentLevel::Confirmed)
            .await
            .unwrap_err();

        assert!(matches!(err, PrismError::RpcError { .. }), "{err:?}");
        assert!(err.to_string().contains(&status.to_string()), "{err}");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "status {status}");
    }
}

#[tokio::test]
async fn http_429_becomes_a_rate_limit_error() {
    // `Retry-After: 0` makes the SDK transport's own 429 retries immediate.
    let (url, hits) = serve(vec![HttpReply {
        status: 429,
        extra_headers: "Retry-After: 0\r\n",
        body: String::new(),
    }])
    .await;
    let mut config = http_config(url);
    config.base.max_retries = 1;
    let adapter = SolanaAdapter::new(config).unwrap();

    let err = adapter
        .get_tip(CommitmentLevel::Confirmed)
        .await
        .unwrap_err();

    match err {
        PrismError::RateLimitError { retry_after_ms } => assert_eq!(retry_after_ms, None),
        other => panic!("expected RateLimitError, got {other:?}"),
    }
    assert_eq!(
        adapter.metrics().snapshot(),
        AdapterMetricsSnapshot {
            requests: 2,
            errors: 2,
            retries: 1,
            rate_limited: 2,
            slots_skipped: 0,
        }
    );
    // The quirk described in `adapter/solana.rs`: the SDK transport re-sends
    // a 429'd request five times by itself, so each of the adapter's two
    // attempts reached the server six times.
    assert_eq!(hits.load(Ordering::SeqCst), 12);
}

#[tokio::test]
async fn transport_errors_do_not_leak_the_rpc_url() {
    // Bind and drop a listener to get a port nothing is listening on.
    let port = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut config = http_config(format!("http://127.0.0.1:{port}/?api-key=SECRET"));
    config.base.max_retries = 1;
    let adapter = SolanaAdapter::new(config).unwrap();

    let err = adapter
        .get_tip(CommitmentLevel::Confirmed)
        .await
        .unwrap_err();

    assert!(matches!(err, PrismError::RpcError { .. }), "{err:?}");
    assert!(err.is_retryable());
    for rendered in [err.to_string(), format!("{err:?}")] {
        assert!(!rendered.contains("SECRET"), "{rendered}");
        assert!(!rendered.contains("api-key"), "{rendered}");
    }
    // A refused connection is transient, so it was retried.
    assert_eq!(adapter.metrics().snapshot().requests, 2);
}

// ------------------------------------------------------------------ live

/// Smoke test against a real cluster. Not run by default:
///
/// ```text
/// cargo test -p prism --test adapter_test -- --ignored live
/// PRISM_TEST_RPC_URL=https://... cargo test -p prism --test adapter_test -- --ignored live
/// ```
#[tokio::test]
#[ignore = "talks to a live Solana RPC"]
async fn live_tip_block_and_accounts_agree() {
    let url = std::env::var("PRISM_TEST_RPC_URL")
        .unwrap_or_else(|_| "https://api.devnet.solana.com".into());
    let mut config = SolanaAdapterConfig::new("solana-live", url);
    config.commitment = CommitmentLevel::Finalized;
    config.encoding = TransactionEncoding::Base64;
    config.requests_per_second = 2;
    config.base.request_timeout_ms = 30_000;
    let adapter = SolanaAdapter::new(config).unwrap();

    adapter.health_check().await.unwrap();

    // The tip's blockhash must be the blockhash of the block at the tip's
    // slot: this is what reading the tip from `getLatestBlockhash` relies on.
    let tip = adapter.get_tip(CommitmentLevel::Finalized).await.unwrap();
    let block = adapter.get_block(tip.slot_or_height).await.unwrap();
    assert_eq!(block.slot_or_height, tip.slot_or_height);
    assert_eq!(block.block_id, tip.block_id);
    assert!(block.timestamp.is_some());
    let ChainBlockData::Solana(raw) = &block.raw;
    assert!(!raw.transactions.is_empty());

    let confirmed = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();
    assert!(confirmed.slot_or_height >= tip.slot_or_height);

    // One account that exists everywhere, one that exists nowhere.
    let accounts = adapter
        .get_accounts(&addresses(&[
            TOKEN_PROGRAM,
            "PrismDoesNotExist11111111111111111111111111",
        ]))
        .await
        .unwrap();
    assert_eq!(accounts.len(), 2);
    let token_program = accounts[0].as_ref().expect("the token program exists");
    assert!(token_program.executable);
    assert!(!token_program.data.is_empty());
    assert!(accounts[1].is_none());

    assert_eq!(adapter.metrics().snapshot().errors, 0);
}
