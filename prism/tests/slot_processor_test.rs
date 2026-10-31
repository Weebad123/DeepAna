//! Tests for `prism::slot_processor`.
//!
//! The fetcher, gap handler and processor tests run against a
//! [`ScriptedAdapter`]: every slot has a block unless a script of outcomes
//! (skip, RPC failure, rate limit, hang, ...) says otherwise. Change events
//! land in a [`RecordingChangeLog`]. Anything that waits (backoff, timeouts,
//! RPC latency) runs on Tokio's paused clock, so it is exact and instant.
//!
//! The `live_*` tests run the processor against Solana devnet through the
//! Helius SDK. They need `HELIUS_API_KEY` and are skipped (with a note)
//! without it. To see the numbers:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test slot_processor_test live_ -- --nocapture --test-threads=1
//! ```

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use helius::types::Cluster;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::ingestor::{HELIUS_API_KEY_ENV, helius_api_key_from_env, helius_solana_adapter};
use prism::normalizer::Normalizer;
use prism::slot_processor::{
    FetcherMetricsSnapshot, GapHandler, GapHandlerMetricsSnapshot, ProcessedSlot, SlotFetcher,
    SlotFetcherConfig, SlotProcessor,
};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::{ChangeEvent, SkipReason};
use serde_json::{Value, json};
use solana_client::client_error::ClientError;
use solana_transaction_status::EncodedConfirmedBlock;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, unique path under the system temp dir. Nothing is created.
fn temp_path(name: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-slot-processor-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir.join(name)
}

fn hash(slot: u64) -> String {
    format!("hash-{slot}")
}

fn rpc_failure(slot: u64) -> PrismError {
    PrismError::rpc(
        Some(slot),
        "getBlock",
        ClientError::from(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset")),
    )
}

const PAYER: &str = "11111111111111111111111111111112";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// One system-program transaction in the RPC's `json` encoding.
fn tx_json(signature: &str) -> Value {
    json!({
        "transaction": {
            "signatures": [signature],
            "message": {
                "header": {"numRequiredSignatures": 1, "numReadonlySignedAccounts": 0, "numReadonlyUnsignedAccounts": 1},
                "accountKeys": [PAYER, SYSTEM_PROGRAM],
                "recentBlockhash": "11111111111111111111111111111111",
                "instructions": [{"programIdIndex": 1, "accounts": [0], "data": "", "stackHeight": null}],
            }
        },
        "meta": {
            "err": null, "status": {"Ok": null}, "fee": 5_000,
            "preBalances": [], "postBalances": [], "computeUnitsConsumed": 150,
        }
    })
}

/// A block at `slot` whose parent is `slot - 1`, holding `slot % 4`
/// transactions so different slots differ.
fn chain_block(slot: u64) -> ChainBlock {
    let transactions: Vec<Value> = (0..slot % 4).map(|i| tx_json(&format!("sig-{slot}-{i}"))).collect();
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": hash(slot.saturating_sub(1)),
        "blockhash": hash(slot),
        "parentSlot": slot.saturating_sub(1),
        "transactions": transactions,
        "rewards": [],
        "blockTime": 1_700_000_000 + slot as i64,
        "blockHeight": slot.saturating_sub(10),
    }))
    .expect("test block JSON is valid");
    ChainBlock {
        slot_or_height: slot,
        block_id: hash(slot),
        parent_id: hash(slot.saturating_sub(1)),
        timestamp: raw.block_time,
        raw: ChainBlockData::Solana(Box::new(raw)),
    }
}

/// Small delays so retry tests stay readable; the clock is paused anyway.
fn fast_config() -> SlotFetcherConfig {
    SlotFetcherConfig {
        max_retries: 3,
        base_delay_ms: 10,
        max_delay_ms: 100,
        request_timeout_ms: 1_000,
    }
}

fn read_lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_owned)
        .collect()
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// What the scripted adapter does for one `get_block` call.
#[derive(Debug, Clone, Copy)]
enum Step {
    Block,
    Skipped,
    RpcFailure,
    RateLimited(Option<u64>),
    /// A non-retryable error.
    Garbage,
    /// Never answers (well past any timeout).
    Hang,
}

/// A chain where every slot has a block unless scripted otherwise.
#[derive(Default)]
struct ScriptedAdapter {
    /// Per-slot outcomes, consumed one per call; an empty script means
    /// [`Step::Block`].
    scripts: Mutex<HashMap<u64, VecDeque<Step>>>,
    /// Slots whose every call fails with [`Step::RpcFailure`].
    down: Mutex<Vec<u64>>,
    /// How long each call takes before answering.
    latency: Mutex<Duration>,
    calls: Mutex<Vec<u64>>,
}

impl ScriptedAdapter {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn script(&self, slot: u64, steps: &[Step]) {
        self.scripts.lock().unwrap().entry(slot).or_default().extend(steps);
    }

    fn skip(&self, slot: u64) {
        self.script(slot, &[Step::Skipped]);
    }

    fn take_down(&self, slot: u64) {
        self.down.lock().unwrap().push(slot);
    }

    fn set_latency(&self, latency: Duration) {
        *self.latency.lock().unwrap() = latency;
    }

    fn calls(&self) -> Vec<u64> {
        self.calls.lock().unwrap().clone()
    }

    fn calls_for(&self, slot: u64) -> usize {
        self.calls().iter().filter(|&&s| s == slot).count()
    }
}

#[async_trait]
impl ChainAdapter for ScriptedAdapter {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-test"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        Ok(ChainTip { slot_or_height: 0, block_id: hash(0), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        self.calls.lock().unwrap().push(slot);
        let latency = *self.latency.lock().unwrap();
        if !latency.is_zero() {
            tokio::time::sleep(latency).await;
        }
        let step = if self.down.lock().unwrap().contains(&slot) {
            Step::RpcFailure
        } else {
            self.scripts
                .lock()
                .unwrap()
                .get_mut(&slot)
                .and_then(VecDeque::pop_front)
                .unwrap_or(Step::Block)
        };
        match step {
            Step::Block => Ok(chain_block(slot)),
            Step::Skipped => Err(PrismError::slot_skipped(slot)),
            Step::RpcFailure => Err(rpc_failure(slot)),
            Step::RateLimited(hint) => Err(PrismError::rate_limited(hint)),
            Step::Garbage => Err(PrismError::deserialize(Some(slot), "block", "garbage")),
            Step::Hang => {
                tokio::time::sleep(Duration::from_secs(3_600)).await;
                Ok(chain_block(slot))
            }
        }
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

/// A change log that keeps every event, and can be made to fail.
#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
    failing: AtomicBool,
}

impl RecordingChangeLog {
    fn events(&self) -> Vec<ChangeEvent> {
        self.events.lock().unwrap().clone()
    }

    fn fail(&self) {
        self.failing.store(true, Ordering::Relaxed);
    }
}

impl ChangeLog for RecordingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        if self.failing.load(Ordering::Relaxed) {
            return Err(PrismError::config("changelog", "unavailable"));
        }
        let mut events = self.events.lock().unwrap();
        events.push(event);
        Ok(events.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn fetcher(adapter: &Arc<ScriptedAdapter>, config: SlotFetcherConfig) -> SlotFetcher {
    SlotFetcher::new(Arc::clone(adapter) as Arc<dyn ChainAdapter>, config).unwrap()
}

/// A processor over `adapter`, with its change log and skip log path.
struct Harness {
    adapter: Arc<ScriptedAdapter>,
    changelog: Arc<RecordingChangeLog>,
    skip_log: PathBuf,
    processor: SlotProcessor,
}

fn harness() -> Harness {
    harness_with(fast_config())
}

fn harness_with(config: SlotFetcherConfig) -> Harness {
    let adapter = ScriptedAdapter::new();
    let changelog = Arc::new(RecordingChangeLog::default());
    let skip_log = temp_path("skipped_slots.log");
    let gap_handler = GapHandler::new(Arc::clone(&changelog) as Arc<dyn ChangeLog>, &skip_log);
    let processor = SlotProcessor::new(fetcher(&adapter, config), gap_handler, Normalizer::new());
    Harness { adapter, changelog, skip_log, processor }
}

impl Harness {
    fn fetcher_metrics(&self) -> FetcherMetricsSnapshot {
        self.processor.fetcher().metrics().snapshot()
    }

    fn gap_metrics(&self) -> GapHandlerMetricsSnapshot {
        self.processor.gap_handler().metrics().snapshot()
    }
}

// ---------------------------------------------------------------------------
// SlotFetcherConfig
// ---------------------------------------------------------------------------

#[test]
fn default_fetcher_config_is_valid_and_matches_the_spec() {
    let config = SlotFetcherConfig::default();
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.base_delay_ms, 500);
    assert_eq!(config.max_delay_ms, 15_000);
    assert_eq!(config.request_timeout_ms, 10_000);
    config.validate().unwrap();
}

#[test]
fn invalid_fetcher_configs_name_the_offending_field() {
    let cases = [
        ("base_delay_ms", SlotFetcherConfig { base_delay_ms: 0, ..fast_config() }),
        ("max_delay_ms", SlotFetcherConfig { base_delay_ms: 200, max_delay_ms: 100, ..fast_config() }),
        ("request_timeout_ms", SlotFetcherConfig { request_timeout_ms: 0, ..fast_config() }),
    ];
    for (field, config) in cases {
        match config.validate() {
            Err(PrismError::ConfigError { field: got, .. }) => assert_eq!(got, field),
            other => panic!("{field}: expected a config error, got {other:?}"),
        }
    }
    // Zero retries is a valid choice, not a mistake.
    SlotFetcherConfig { max_retries: 0, ..fast_config() }.validate().unwrap();
}

#[test]
fn fetcher_new_rejects_an_invalid_config() {
    let adapter: Arc<dyn ChainAdapter> = ScriptedAdapter::new();
    let config = SlotFetcherConfig { request_timeout_ms: 0, ..fast_config() };
    assert!(matches!(SlotFetcher::new(adapter, config), Err(PrismError::ConfigError { .. })));
}

// ---------------------------------------------------------------------------
// SlotFetcher
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn produced_slot_is_returned_as_some_block() {
    let adapter = ScriptedAdapter::new();
    let fetcher = fetcher(&adapter, fast_config());

    let block = fetcher.fetch(42).await.unwrap().expect("slot 42 has a block");

    assert_eq!(block, chain_block(42));
    assert_eq!(adapter.calls(), vec![42]);
    let m = fetcher.metrics().snapshot();
    assert_eq!((m.total_fetches, m.successful_fetches, m.skipped_slots, m.failed_fetches), (1, 1, 0, 0));
    assert_eq!(m.total_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn skipped_slot_is_none_and_not_retried() {
    let adapter = ScriptedAdapter::new();
    adapter.skip(7);
    let fetcher = fetcher(&adapter, fast_config());

    assert_eq!(fetcher.fetch(7).await.unwrap(), None);

    assert_eq!(adapter.calls_for(7), 1);
    let m = fetcher.metrics().snapshot();
    assert_eq!((m.total_fetches, m.successful_fetches, m.skipped_slots, m.failed_fetches), (1, 0, 1, 0));
    assert_eq!(m.total_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn transient_rpc_failures_are_retried_until_the_block_arrives() {
    let adapter = ScriptedAdapter::new();
    adapter.script(5, &[Step::RpcFailure, Step::RpcFailure]);
    let fetcher = fetcher(&adapter, fast_config());

    assert!(fetcher.fetch(5).await.unwrap().is_some());

    assert_eq!(adapter.calls_for(5), 3);
    let m = fetcher.metrics().snapshot();
    assert_eq!((m.successful_fetches, m.failed_fetches, m.total_retries), (1, 0, 2));
}

#[tokio::test(start_paused = true)]
async fn rate_limit_errors_are_retried() {
    let adapter = ScriptedAdapter::new();
    adapter.script(5, &[Step::RateLimited(None)]);
    let fetcher = fetcher(&adapter, fast_config());

    assert!(fetcher.fetch(5).await.unwrap().is_some());
    assert_eq!(adapter.calls_for(5), 2);
    assert_eq!(fetcher.metrics().snapshot().total_retries, 1);
}

#[tokio::test(start_paused = true)]
async fn a_skip_after_a_transient_failure_is_still_a_skip() {
    let adapter = ScriptedAdapter::new();
    adapter.script(9, &[Step::RpcFailure, Step::Skipped]);
    let fetcher = fetcher(&adapter, fast_config());

    assert_eq!(fetcher.fetch(9).await.unwrap(), None);
    assert_eq!(adapter.calls_for(9), 2);
}

#[tokio::test(start_paused = true)]
async fn exhausted_retries_return_the_last_error() {
    let adapter = ScriptedAdapter::new();
    adapter.take_down(3);
    let config = SlotFetcherConfig { max_retries: 5, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let err = fetcher.fetch(3).await.unwrap_err();

    assert!(matches!(err, PrismError::RpcError { slot: Some(3), .. }), "{err:?}");
    assert_eq!(adapter.calls_for(3), 6, "one attempt plus max_retries retries");
    let m = fetcher.metrics().snapshot();
    assert_eq!((m.total_fetches, m.failed_fetches, m.total_retries), (1, 1, 5));
}

#[tokio::test(start_paused = true)]
async fn zero_max_retries_makes_a_single_attempt() {
    let adapter = ScriptedAdapter::new();
    adapter.take_down(3);
    let fetcher = fetcher(&adapter, SlotFetcherConfig { max_retries: 0, ..fast_config() });

    assert!(fetcher.fetch(3).await.is_err());
    assert_eq!(adapter.calls_for(3), 1);
    assert_eq!(fetcher.metrics().snapshot().total_retries, 0);
}

#[tokio::test(start_paused = true)]
async fn non_retryable_errors_fail_on_the_first_attempt() {
    let adapter = ScriptedAdapter::new();
    adapter.script(8, &[Step::Garbage]);
    let fetcher = fetcher(&adapter, fast_config());

    let err = fetcher.fetch(8).await.unwrap_err();

    assert!(matches!(err, PrismError::DeserializeError { .. }), "{err:?}");
    assert_eq!(adapter.calls_for(8), 1);
    let m = fetcher.metrics().snapshot();
    assert_eq!((m.failed_fetches, m.total_retries), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn an_attempt_past_the_timeout_is_a_retryable_rpc_error() {
    let adapter = ScriptedAdapter::new();
    adapter.script(11, &[Step::Hang, Step::Hang]);
    let config = SlotFetcherConfig { max_retries: 1, request_timeout_ms: 2_000, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let started = Instant::now();
    let err = fetcher.fetch(11).await.unwrap_err();

    match &err {
        PrismError::RpcError { slot, method, .. } => {
            assert_eq!(*slot, Some(11));
            assert_eq!(method, "getBlock");
        }
        other => panic!("expected an RPC error, got {other:?}"),
    }
    assert!(err.is_retryable());
    assert!(err.to_string().contains("2000 ms"), "{err}");
    assert_eq!(adapter.calls_for(11), 2);
    // Two timed-out attempts and one backoff, not the adapter's hour.
    assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
}

#[tokio::test(start_paused = true)]
async fn a_hang_followed_by_an_answer_recovers() {
    let adapter = ScriptedAdapter::new();
    adapter.script(11, &[Step::Hang]);
    let fetcher = fetcher(&adapter, fast_config());

    assert!(fetcher.fetch(11).await.unwrap().is_some());
    assert_eq!(fetcher.metrics().snapshot().total_retries, 1);
}

#[tokio::test(start_paused = true)]
async fn backoff_grows_from_the_base_delay_with_jitter() {
    let adapter = ScriptedAdapter::new();
    adapter.take_down(1);
    let config = SlotFetcherConfig { max_retries: 3, base_delay_ms: 1_000, max_delay_ms: 60_000, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let started = Instant::now();
    let _ = fetcher.fetch(1).await;
    let waited = started.elapsed();

    // Nominal 1 s + 2 s + 4 s, each jittered by ±50 %.
    assert!(waited >= Duration::from_millis(3_500), "{waited:?}");
    assert!(waited <= Duration::from_millis(10_500), "{waited:?}");
}

#[tokio::test(start_paused = true)]
async fn every_delay_is_capped_at_max_delay() {
    let adapter = ScriptedAdapter::new();
    adapter.take_down(1);
    let config = SlotFetcherConfig { max_retries: 6, base_delay_ms: 1_000, max_delay_ms: 1_000, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let started = Instant::now();
    let _ = fetcher.fetch(1).await;

    assert!(started.elapsed() <= Duration::from_millis(6_000), "{:?}", started.elapsed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_hint_stretches_the_delay() {
    let adapter = ScriptedAdapter::new();
    adapter.script(2, &[Step::RateLimited(Some(5_000))]);
    let config = SlotFetcherConfig { base_delay_ms: 10, max_delay_ms: 15_000, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let started = Instant::now();
    assert!(fetcher.fetch(2).await.unwrap().is_some());

    assert!(started.elapsed() >= Duration::from_millis(5_000), "{:?}", started.elapsed());
}

#[tokio::test(start_paused = true)]
async fn a_retry_after_hint_is_capped_at_max_delay() {
    let adapter = ScriptedAdapter::new();
    adapter.script(2, &[Step::RateLimited(Some(60_000))]);
    let config = SlotFetcherConfig { base_delay_ms: 10, max_delay_ms: 2_000, ..fast_config() };
    let fetcher = fetcher(&adapter, config);

    let started = Instant::now();
    assert!(fetcher.fetch(2).await.unwrap().is_some());

    assert!(started.elapsed() <= Duration::from_millis(2_000), "{:?}", started.elapsed());
}

#[tokio::test(start_paused = true)]
async fn last_fetch_latency_tracks_the_most_recent_fetch() {
    let adapter = ScriptedAdapter::new();
    let fetcher = fetcher(&adapter, fast_config());

    adapter.set_latency(Duration::from_millis(1_234));
    fetcher.fetch(1).await.unwrap();
    assert_eq!(fetcher.metrics().snapshot().last_fetch_latency_ms, 1_234);

    adapter.set_latency(Duration::from_millis(40));
    fetcher.fetch(2).await.unwrap();
    assert_eq!(fetcher.metrics().snapshot().last_fetch_latency_ms, 40);
}

#[test]
fn success_rate_counts_skips_as_answers() {
    let m = FetcherMetricsSnapshot {
        total_fetches: 10,
        successful_fetches: 7,
        skipped_slots: 2,
        failed_fetches: 1,
        ..FetcherMetricsSnapshot::default()
    };
    assert!((m.success_rate() - 0.9).abs() < f64::EPSILON);
    assert!((FetcherMetricsSnapshot::default().success_rate() - 1.0).abs() < f64::EPSILON);
}

// ---------------------------------------------------------------------------
// GapHandler
// ---------------------------------------------------------------------------

fn gap_handler() -> (GapHandler, Arc<RecordingChangeLog>, PathBuf) {
    let changelog = Arc::new(RecordingChangeLog::default());
    let path = temp_path("logs/skipped_slots.log");
    let handler = GapHandler::new(Arc::clone(&changelog) as Arc<dyn ChangeLog>, &path);
    (handler, changelog, path)
}

#[tokio::test]
async fn handle_appends_timestamp_slot_reason_lines() {
    let (handler, _, path) = gap_handler();
    let before = chrono::Utc::now().timestamp();

    handler.handle(350_123_457, SkipReason::ValidatorSkipped).await.unwrap();
    handler.handle(350_123_460, SkipReason::RpcUnavailable).await.unwrap();

    let after = chrono::Utc::now().timestamp();
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 2, "{lines:?}");
    let expected = [(350_123_457, "validator_skipped"), (350_123_460, "rpc_unavailable")];
    for (line, (slot, reason)) in lines.iter().zip(expected) {
        let fields: Vec<&str> = line.split(',').collect();
        assert_eq!(fields.len(), 3, "{line}");
        let ts: i64 = fields[0].parse().unwrap();
        assert!((before..=after).contains(&ts), "{line}");
        assert_eq!(fields[1].parse::<u64>().unwrap(), slot);
        assert_eq!(fields[2], reason);
        assert_eq!(fields[2].parse::<SkipReason>().unwrap().as_str(), reason);
    }
}

#[tokio::test]
async fn skip_log_is_append_only_across_handlers() {
    let (first, changelog, path) = gap_handler();
    first.handle(1, SkipReason::ValidatorSkipped).await.unwrap();
    drop(first);

    // A restart: a new handler on the same file keeps what is there.
    let second = GapHandler::new(changelog as Arc<dyn ChangeLog>, &path);
    second.handle(2, SkipReason::BelowCursor).await.unwrap();

    let slots: Vec<String> = read_lines(&path).iter().map(|l| l.split(',').nth(1).unwrap().to_owned()).collect();
    assert_eq!(slots, ["1", "2"]);
}

#[tokio::test]
async fn handle_emits_a_skip_event() {
    let (handler, changelog, _) = gap_handler();

    handler.handle(77, SkipReason::ValidatorSkipped).await.unwrap();

    assert_eq!(changelog.events(), vec![ChangeEvent::skip(77, SkipReason::ValidatorSkipped)]);
}

#[tokio::test]
async fn metrics_count_each_reason() {
    let (handler, _, _) = gap_handler();
    for (slot, reason) in [
        (1, SkipReason::ValidatorSkipped),
        (2, SkipReason::ValidatorSkipped),
        (3, SkipReason::RpcUnavailable),
        (4, SkipReason::BelowCursor),
    ] {
        handler.handle(slot, reason).await.unwrap();
    }
    assert_eq!(
        handler.metrics().snapshot(),
        GapHandlerMetricsSnapshot { total_skips_recorded: 4, validator_skips: 2, rpc_unavailable_skips: 1 }
    );
}

#[tokio::test]
async fn changelog_failure_is_returned_and_nothing_is_logged_or_counted() {
    let (handler, changelog, path) = gap_handler();
    changelog.fail();

    let err = handler.handle(5, SkipReason::ValidatorSkipped).await.unwrap_err();

    assert!(matches!(err, PrismError::ConfigError { .. }), "{err:?}");
    assert!(read_lines(&path).is_empty());
    assert_eq!(handler.metrics().snapshot(), GapHandlerMetricsSnapshot::default());
}

#[tokio::test]
async fn an_unwritable_skip_log_does_not_fail_the_skip() {
    let changelog = Arc::new(RecordingChangeLog::default());
    // A directory where the file should be: opening it for append fails.
    let path = temp_path("skipped_slots.log");
    std::fs::create_dir_all(&path).unwrap();
    let handler = GapHandler::new(Arc::clone(&changelog) as Arc<dyn ChangeLog>, &path);

    handler.handle(6, SkipReason::ValidatorSkipped).await.unwrap();

    assert_eq!(changelog.events().len(), 1);
    assert_eq!(handler.metrics().snapshot().validator_skips, 1);
}

// ---------------------------------------------------------------------------
// SlotProcessor
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn produced_slot_becomes_a_udm_slot_row() {
    let h = harness();

    let processed = h.processor.process(103).await.unwrap();

    assert_eq!(processed.slot, 103);
    assert!(!processed.was_skipped);
    let row = processed.udm_slot.expect("slot row");
    assert_eq!(row.slot, 103);
    assert_eq!(row.blockhash, hash(103));
    assert_eq!(row.parent_slot, 102);
    assert_eq!(row.block_time, Some(1_700_000_103));
    assert_eq!(row.block_height, Some(93));
    assert_eq!(row.transaction_count, 3);
    assert!(processed.udm_transfers.is_empty());
    // Nothing is a gap, so nothing is published or logged.
    assert!(h.changelog.events().is_empty());
    assert!(read_lines(&h.skip_log).is_empty());
}

#[tokio::test(start_paused = true)]
async fn skipped_slot_is_a_processed_skip_with_an_event_and_a_log_line() {
    let h = harness();
    h.adapter.skip(200);

    let processed = h.processor.process(200).await.unwrap();

    assert!(processed.was_skipped);
    assert_eq!(processed.udm_slot, None);
    assert!(processed.udm_transactions.is_empty() && processed.udm_instructions.is_empty());
    assert_eq!(h.changelog.events(), vec![ChangeEvent::skip(200, SkipReason::ValidatorSkipped)]);
    let lines = read_lines(&h.skip_log);
    assert_eq!(lines.len(), 1);
    assert!(lines[0].ends_with(",200,validator_skipped"), "{}", lines[0]);
    assert_eq!(h.gap_metrics().validator_skips, 1);
}

#[tokio::test(start_paused = true)]
async fn exhausted_retries_record_an_rpc_unavailable_gap_and_return_the_error() {
    let h = harness();
    h.adapter.take_down(300);

    let err = h.processor.process(300).await.unwrap_err();

    assert!(matches!(err, PrismError::RpcError { slot: Some(300), .. }), "{err:?}");
    assert_eq!(h.changelog.events(), vec![ChangeEvent::skip(300, SkipReason::RpcUnavailable)]);
    assert!(read_lines(&h.skip_log)[0].ends_with(",300,rpc_unavailable"));
    assert_eq!(h.gap_metrics().rpc_unavailable_skips, 1);
    assert_eq!(h.processor.metrics().snapshot().total_slots_processed, 0);
}

#[tokio::test(start_paused = true)]
async fn a_failed_gap_record_does_not_mask_the_fetch_error() {
    let h = harness();
    h.adapter.take_down(300);
    h.changelog.fail();

    let err = h.processor.process(300).await.unwrap_err();

    assert!(matches!(err, PrismError::RpcError { .. }), "{err:?}");
}

#[tokio::test(start_paused = true)]
async fn non_retryable_fetch_errors_are_not_data_gaps() {
    let h = harness();
    h.adapter.script(400, &[Step::Garbage]);

    let err = h.processor.process(400).await.unwrap_err();

    assert!(matches!(err, PrismError::DeserializeError { .. }), "{err:?}");
    assert!(h.changelog.events().is_empty());
    assert_eq!(h.gap_metrics(), GapHandlerMetricsSnapshot::default());
}

#[tokio::test(start_paused = true)]
async fn an_unpublishable_skip_fails_the_slot() {
    let h = harness();
    h.adapter.skip(500);
    h.changelog.fail();

    assert!(h.processor.process(500).await.is_err());
    assert_eq!(h.processor.metrics().snapshot().total_slots_processed, 0);
}

#[tokio::test(start_paused = true)]
async fn fetch_duration_is_the_rpc_time_retries_included() {
    let h = harness();
    h.adapter.set_latency(Duration::from_millis(750));
    assert_eq!(h.processor.process(1).await.unwrap().fetch_duration_ms, 750);

    // Two failed attempts and their backoff count too.
    h.adapter.script(2, &[Step::RpcFailure, Step::RpcFailure]);
    let processed = h.processor.process(2).await.unwrap();
    assert!(processed.fetch_duration_ms >= 3 * 750, "{}", processed.fetch_duration_ms);
}

#[tokio::test(start_paused = true)]
async fn processor_metrics_summarise_a_run() {
    let h = harness();
    h.adapter.skip(11);
    h.adapter.skip(13);
    h.adapter.set_latency(Duration::from_millis(100));

    for slot in 10..=14 {
        h.processor.process(slot).await.unwrap();
    }

    let m = h.processor.metrics().snapshot();
    assert_eq!(m.total_slots_processed, 5);
    assert_eq!(m.slots_with_data, 3);
    assert_eq!(m.slots_skipped, 2);
    assert_eq!(m.last_processed_slot, 14);
    assert_eq!(m.avg_processing_time_ms, 100);
    assert_eq!(h.fetcher_metrics().total_fetches, 5);
}

/// The ingestor's loop, as it will drive the processor: every slot is
/// forwarded in order, skips included, and the first error halts it.
#[tokio::test(start_paused = true)]
async fn ingestor_loop_forwards_slots_in_order_and_halts_on_an_rpc_error() {
    let h = harness();
    h.adapter.skip(102);
    h.adapter.take_down(104);

    let (slot_tx, mut slot_rx) = mpsc::channel(16);
    let (processed_tx, mut processed_rx) = mpsc::channel::<ProcessedSlot>(16);
    let shutdown = CancellationToken::new();
    for slot in 100..=106 {
        slot_tx.send(slot).await.unwrap();
    }
    drop(slot_tx);

    let mut halted_on = None;
    while let Some(slot) = slot_rx.recv().await {
        match h.processor.process(slot).await {
            Ok(processed) => processed_tx.send(processed).await.unwrap(),
            Err(PrismError::RpcError { .. }) => {
                shutdown.cancel();
                halted_on = Some(slot);
                break;
            }
            Err(e) => panic!("unexpected error at slot {slot}: {e}"),
        }
    }
    drop(processed_tx);

    let mut forwarded = Vec::new();
    while let Some(processed) = processed_rx.recv().await {
        forwarded.push((processed.slot, processed.was_skipped));
    }
    assert_eq!(forwarded, [(100, false), (101, false), (102, true), (103, false)]);
    assert_eq!(halted_on, Some(104));
    assert!(shutdown.is_cancelled());
    assert!(!h.adapter.calls().contains(&105), "nothing past the failed slot is fetched");
    let reasons: Vec<_> = h.changelog.events().iter().filter_map(ChangeEvent::skip_reason).collect();
    assert_eq!(reasons, [SkipReason::ValidatorSkipped, SkipReason::RpcUnavailable]);
}

// ---------------------------------------------------------------------------
// Live: Solana devnet through Helius
// ---------------------------------------------------------------------------

/// Solana's target slot time. Devnet runs the same schedule as mainnet.
const NOMINAL_SLOT_TIME: Duration = Duration::from_millis(400);

/// Lowest share of fetches that must get an answer (a block or a skip).
const MIN_FETCH_SUCCESS_RATE: f64 = 0.95;

/// Confirmed slots this far below the tip have settled, so `getBlock`
/// answers for them instead of "not available yet".
const TIP_MARGIN: u64 = 10;

/// A processor over the devnet adapter with production defaults, or `None`
/// (with a note) when no key is configured.
async fn live_harness() -> Option<(Arc<dyn ChainAdapter>, SlotProcessor, Arc<RecordingChangeLog>)> {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return None;
    };
    let adapter: Arc<dyn ChainAdapter> =
        Arc::new(helius_solana_adapter(&key, Cluster::Devnet).await.expect("helius adapter"));
    let changelog = Arc::new(RecordingChangeLog::default());
    let gap_handler =
        GapHandler::new(Arc::clone(&changelog) as Arc<dyn ChangeLog>, temp_path("skipped_slots.log"));
    let fetcher = SlotFetcher::new(Arc::clone(&adapter), SlotFetcherConfig::default()).unwrap();
    let processor = SlotProcessor::new(fetcher, gap_handler, Normalizer::new());
    Some((adapter, processor, changelog))
}

async fn confirmed_tip(adapter: &dyn ChainAdapter) -> u64 {
    adapter.get_tip(CommitmentLevel::Confirmed).await.expect("devnet tip").slot_or_height
}

/// Devnet's actual slot time, from two tip reads `window` apart.
async fn measured_slot_time(adapter: &dyn ChainAdapter, window: Duration) -> Duration {
    let (first, started) = (confirmed_tip(adapter).await, Instant::now());
    tokio::time::sleep(window).await;
    let (last, elapsed) = (confirmed_tip(adapter).await, started.elapsed());
    assert!(last > first, "devnet produced no slot in {window:?} ({first} -> {last})");
    elapsed / u32::try_from(last - first).unwrap()
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank]
}

/// The processor must clear a slot faster, on average, than devnet produces
/// one; otherwise it falls further behind the tip with every slot.
#[tokio::test]
async fn live_processing_latency_keeps_pace_with_devnet_slot_production() {
    const SAMPLE: u64 = 30;
    let Some((adapter, processor, _)) = live_harness().await else { return };

    let slot_time = measured_slot_time(adapter.as_ref(), Duration::from_secs(8)).await;
    let last = confirmed_tip(adapter.as_ref()).await - TIP_MARGIN;
    let first = last - SAMPLE + 1;

    let mut latencies = Vec::new();
    let mut failed = Vec::new();
    for slot in first..=last {
        let started = Instant::now();
        match processor.process(slot).await {
            Ok(_) => latencies.push(started.elapsed()),
            Err(err) => failed.push((slot, err.to_string())),
        }
    }
    latencies.sort();
    let mean = latencies.iter().sum::<Duration>() / u32::try_from(latencies.len()).unwrap();
    let m = processor.metrics().snapshot();

    println!(
        "devnet slot time: {slot_time:?} measured (nominal {NOMINAL_SLOT_TIME:?})\n\
         processed slots {first}..={last}: {} with data, {} skipped, {} failed {failed:?}\n\
         latency per slot: mean {mean:?} | p50 {:?} | p95 {:?} | max {:?} | metrics avg {} ms\n\
         headroom: {:.1}x the slot rate",
        m.slots_with_data,
        m.slots_skipped,
        failed.len(),
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        latencies.last().unwrap(),
        m.avg_processing_time_ms,
        slot_time.as_secs_f64() / mean.as_secs_f64(),
    );
    assert!(latencies.len() as u64 >= SAMPLE / 2, "too few slots processed: {failed:?}");
    assert!(
        mean < slot_time,
        "mean processing latency {mean:?} is above devnet's slot time {slot_time:?}: the processor cannot keep up"
    );
    assert!(
        m.avg_processing_time_ms <= mean.as_millis() as u64,
        "processor metrics disagree with the measured mean"
    );
}

/// Of the fetches made over a run of recent devnet slots, the share that got
/// an answer (a block or a skip) rather than an error after every retry.
#[tokio::test]
async fn live_fetch_success_rate_over_recent_devnet_slots() {
    const SAMPLE: u64 = 50;
    let Some((adapter, processor, changelog)) = live_harness().await else { return };

    let last = confirmed_tip(adapter.as_ref()).await - TIP_MARGIN;
    let first = last - SAMPLE + 1;
    let mut failed = Vec::new();
    for slot in first..=last {
        // Unlike the ingestor, carry on past a failure so it is counted.
        if let Err(err) = processor.process(slot).await {
            failed.push((slot, err.to_string()));
        }
    }

    let f = processor.fetcher().metrics().snapshot();
    let gaps = processor.gap_handler().metrics().snapshot();
    println!(
        "fetched slots {first}..={last}: {} blocks, {} skipped, {} failed, {} retries\n\
         fetch success rate {:.2}% (minimum {:.0}%) | failures {failed:?}",
        f.successful_fetches,
        f.skipped_slots,
        f.failed_fetches,
        f.total_retries,
        f.success_rate() * 100.0,
        MIN_FETCH_SUCCESS_RATE * 100.0,
    );
    assert_eq!(f.total_fetches, SAMPLE);
    assert_eq!(f.successful_fetches + f.skipped_slots + f.failed_fetches, SAMPLE);
    assert!(
        f.success_rate() >= MIN_FETCH_SUCCESS_RATE,
        "fetch success rate {:.2}% is below {:.0}%",
        f.success_rate() * 100.0,
        MIN_FETCH_SUCCESS_RATE * 100.0
    );
    // Every outcome is accounted for downstream as well.
    assert_eq!(processor.metrics().snapshot().total_slots_processed, f.successful_fetches + f.skipped_slots);
    assert_eq!(gaps.validator_skips, f.skipped_slots);
    // Only failures that were retried to exhaustion are data gaps.
    assert!(gaps.rpc_unavailable_skips <= f.failed_fetches);
    assert_eq!(changelog.events().len() as u64, f.skipped_slots + gaps.rpc_unavailable_skips);
}
