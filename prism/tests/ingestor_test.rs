//! Tests for `prism::ingestor`.
//!
//! The pipeline tests run against a [`ScriptedChain`]: an in-memory chain
//! with a movable tip, skipped slots and injectable failures. Flushed data
//! lands in a [`RecordingSink`].
//!
//! The `live_*` tests talk to Solana devnet through the Helius SDK and print
//! the real transactions the ingestor buffers. They need `HELIUS_API_KEY`
//! and are skipped (with a note) without it. To see the output:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test ingestor_test live_ -- --nocapture --test-threads=1
//! ```

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use helius::types::Cluster;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::adapter::config::TransactionEncoding;
use prism::commitment::{CommitmentTracker, FinalityWatermark};
use prism::ingestor::{
    BufferedSlot, BufferedTransaction, CursorState, FlushSink, HELIUS_API_KEY_ENV, Ingestor,
    IngestorConfig, IngestorCursor, LogSink, VOTE_PROGRAM_ID, helius_api_key_from_env,
    helius_chain_id, helius_solana_adapter,
};
use prism::types::error::{PrismError, PrismResult};
use serde_json::{Value, json};
use solana_client::client_error::ClientError;
use solana_transaction_status::EncodedConfirmedBlock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A fresh, unique path under the system temp dir. Nothing is created.
fn temp_path(name: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-ingestor-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir.join(name)
}

fn hash(slot: u64) -> String {
    format!("hash-{slot}")
}

fn rpc_failure() -> PrismError {
    PrismError::rpc(
        None,
        "getBlock",
        ClientError::from(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out")),
    )
}

const PAYER: &str = "11111111111111111111111111111112";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGqPFXCWuBvf9Ss623VQ5DA";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

/// One transaction in the RPC's `json` encoding.
fn tx_json(signature: &str, program_ids: &[&str], err: Option<Value>, fee: u64) -> Value {
    let mut account_keys = vec![PAYER.to_owned()];
    let mut instructions = Vec::new();
    for program in program_ids {
        let index = account_keys
            .iter()
            .position(|key| key == program)
            .unwrap_or_else(|| {
                account_keys.push((*program).to_owned());
                account_keys.len() - 1
            });
        instructions.push(json!({"programIdIndex": index, "accounts": [0], "data": "", "stackHeight": null}));
    }
    json!({
        "transaction": {
            "signatures": [signature],
            "message": {
                "header": {"numRequiredSignatures": 1, "numReadonlySignedAccounts": 0, "numReadonlyUnsignedAccounts": 1},
                "accountKeys": account_keys,
                "recentBlockhash": "11111111111111111111111111111111",
                "instructions": instructions,
            }
        },
        "meta": {
            "err": err,
            "status": if err.is_some() { json!({"Err": err}) } else { json!({"Ok": null}) },
            "fee": fee,
            "preBalances": [], "postBalances": [],
            "computeUnitsConsumed": 1_000 + fee,
        }
    })
}

fn chain_block(slot: u64, transactions: Vec<Value>) -> ChainBlock {
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": hash(slot.saturating_sub(1)),
        "blockhash": hash(slot),
        "parentSlot": slot.saturating_sub(1),
        "transactions": transactions,
        "rewards": [],
        "blockTime": 1_700_000_000 + slot as i64,
        "blockHeight": slot,
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

/// A block with `count` simple transfers.
fn simple_block(slot: u64, count: usize) -> ChainBlock {
    let txs = (0..count)
        .map(|i| tx_json(&format!("sig-{slot}-{i}"), &[SYSTEM_PROGRAM], None, 5_000))
        .collect();
    chain_block(slot, txs)
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// An in-memory chain.
#[derive(Default)]
struct ScriptedChain {
    tip: AtomicU64,
    skipped: Mutex<HashSet<u64>>,
    /// Transient failures still to be served, per slot.
    transient: Mutex<HashMap<u64, usize>>,
    /// Slots that fail permanently.
    fatal: Mutex<HashSet<u64>>,
    /// Tip failures still to be served.
    tip_failures: AtomicUsize,
    block_calls: Mutex<Vec<u64>>,
    /// When set, the tip moves up by one on every tip read.
    advancing: std::sync::atomic::AtomicBool,
}

impl ScriptedChain {
    fn with_tip(tip: u64) -> Arc<Self> {
        let chain = Self::default();
        chain.tip.store(tip, Ordering::SeqCst);
        Arc::new(chain)
    }

    fn skip(&self, slots: &[u64]) {
        self.skipped.lock().unwrap().extend(slots);
    }

    fn block_calls(&self) -> Vec<u64> {
        self.block_calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChainAdapter for ScriptedChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-scripted"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        if self
            .tip_failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(rpc_failure());
        }
        let slot = if self.advancing.load(Ordering::SeqCst) {
            self.tip.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            self.tip.load(Ordering::SeqCst)
        };
        Ok(ChainTip {
            slot_or_height: slot,
            block_id: hash(slot),
            commitment,
        })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        self.block_calls.lock().unwrap().push(slot);
        if self.fatal.lock().unwrap().contains(&slot) {
            return Err(PrismError::deserialize(Some(slot), "block", "corrupt"));
        }
        if let Some(left) = self.transient.lock().unwrap().get_mut(&slot)
            && *left > 0
        {
            *left -= 1;
            return Err(rpc_failure());
        }
        if self.skipped.lock().unwrap().contains(&slot) {
            return Err(PrismError::slot_skipped(slot));
        }
        Ok(simple_block(slot, (slot % 3) as usize + 1))
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

/// Keeps every batch it is given; can be told to fail.
#[derive(Default)]
struct RecordingSink {
    batches: Mutex<Vec<Vec<BufferedSlot>>>,
    failures: AtomicUsize,
}

impl RecordingSink {
    fn slots(&self) -> Vec<u64> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .map(|slot| slot.slot)
            .collect()
    }
}

#[async_trait]
impl FlushSink for RecordingSink {
    async fn flush(&self, slots: &[BufferedSlot]) -> PrismResult<()> {
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(PrismError::storage(
                "transactions",
                (0, 0),
                std::io::Error::other("disk full"),
            ));
        }
        self.batches.lock().unwrap().push(slots.to_vec());
        Ok(())
    }
}

fn tracker() -> Arc<CommitmentTracker> {
    Arc::new(CommitmentTracker::from_watermark(Arc::new(FinalityWatermark::new())))
}

fn fast_config(cursor_path: PathBuf) -> IngestorConfig {
    IngestorConfig {
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_millis(20),
        ..IngestorConfig::new(cursor_path)
    }
}

fn ingestor(
    chain: Arc<ScriptedChain>,
    config: IngestorConfig,
    sink: Arc<RecordingSink>,
) -> Ingestor {
    Ingestor::new(chain, tracker(), config, sink).expect("valid ingestor config")
}

// ---------------------------------------------------------------------------
// Cursor
// ---------------------------------------------------------------------------

#[test]
fn fresh_cursor_has_no_resume_point_and_writes_nothing() {
    let path = temp_path("cursor.json");
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    assert!(cursor.state().is_fresh());
    assert_eq!(cursor.state(), CursorState::default());
    assert_eq!(cursor.resume_slot(), None);
    assert!(!path.exists());
}

#[test]
fn first_processed_slot_becomes_the_resume_point() {
    let cursor = IngestorCursor::load_or_init(temp_path("cursor.json")).unwrap();
    assert!(cursor.record_processed(1_000, "h1000"));
    let state = cursor.state();
    assert_eq!(state.last_processed_slot, 1_000);
    assert_eq!(state.last_processed_blockhash, "h1000");
    assert_eq!(state.last_flush_slot, 999);
    assert_eq!(state.unflushed_slots(), 1);
    assert!(state.updated_at > 0);
    // A crash before any flush restarts at the first slot, not at slot 1.
    assert_eq!(cursor.resume_slot(), Some(1_000));
}

#[test]
fn processed_edge_only_moves_forward() {
    let cursor = IngestorCursor::load_or_init(temp_path("cursor.json")).unwrap();
    assert!(cursor.record_processed(10, "h10"));
    assert!(cursor.record_processed(12, "h12"));
    assert!(!cursor.record_processed(12, "again"));
    assert!(!cursor.record_processed(11, "late"));
    assert!(!cursor.record_skipped(5));
    let state = cursor.state();
    assert_eq!(state.last_processed_slot, 12);
    assert_eq!(state.last_processed_blockhash, "h12");
}

#[test]
fn skipped_slot_advances_the_edge_but_keeps_the_last_blockhash() {
    let cursor = IngestorCursor::load_or_init(temp_path("cursor.json")).unwrap();
    cursor.record_processed(10, "h10");
    assert!(cursor.record_skipped(11));
    let state = cursor.state();
    assert_eq!(state.last_processed_slot, 11);
    assert_eq!(state.last_processed_blockhash, "h10");
}

#[test]
fn skipped_first_slot_also_sets_the_resume_point() {
    let cursor = IngestorCursor::load_or_init(temp_path("cursor.json")).unwrap();
    assert!(cursor.record_skipped(50));
    assert_eq!(cursor.resume_slot(), Some(50));
    assert_eq!(cursor.state().last_processed_blockhash, "");
}

#[test]
fn flush_is_clamped_monotonic_and_persisted() {
    let path = temp_path("nested/dir/cursor.json");
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    cursor.record_processed(100, "h100");
    cursor.record_processed(105, "h105");

    assert_eq!(cursor.record_flush(103).unwrap(), 103);
    assert_eq!(cursor.resume_slot(), Some(104));
    // Nothing unprocessed can be on disk.
    assert_eq!(cursor.record_flush(9_999).unwrap(), 105);
    // The flush edge never moves back.
    assert_eq!(cursor.record_flush(101).unwrap(), 105);

    assert!(path.exists(), "parent dirs are created");
    let mut tmp = path.clone().into_os_string();
    tmp.push(".tmp");
    assert!(!PathBuf::from(tmp).exists(), "temp file is renamed away");

    let reloaded = IngestorCursor::load_or_init(&path).unwrap();
    assert_eq!(reloaded.state(), cursor.state());
    assert_eq!(reloaded.resume_slot(), Some(106));
}

#[test]
fn unflushed_window_is_lost_on_restart_and_refetched() {
    let path = temp_path("cursor.json");
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    cursor.record_processed(200, "h200");
    cursor.record_flush(200).unwrap();
    cursor.record_processed(201, "h201");
    cursor.record_processed(202, "h202");
    // Crash: the processed edge only exists in memory.
    drop(cursor);
    let reloaded = IngestorCursor::load_or_init(&path).unwrap();
    assert_eq!(reloaded.state().last_processed_slot, 200);
    assert_eq!(reloaded.resume_slot(), Some(201));
}

#[test]
fn persisted_file_is_readable_json() {
    let path = temp_path("cursor.json");
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    cursor.record_processed(7, "h7");
    cursor.persist().unwrap();
    let value: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(value["last_processed_slot"], 7);
    assert_eq!(value["last_processed_blockhash"], "h7");
    assert_eq!(value["last_flush_slot"], 6);
    assert!(value["updated_at"].as_i64().unwrap() > 0);
}

#[test]
fn clones_share_state() {
    let cursor = IngestorCursor::load_or_init(temp_path("cursor.json")).unwrap();
    let clone = cursor.clone();
    clone.record_processed(42, "h42");
    assert_eq!(cursor.state().last_processed_slot, 42);
    assert_eq!(cursor.path(), clone.path());
}

fn write(path: &PathBuf, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

#[test]
fn corrupt_cursor_file_is_a_cursor_error() {
    let path = temp_path("cursor.json");
    write(&path, "{ not json");
    let err = IngestorCursor::load_or_init(&path).unwrap_err();
    assert_eq!(err.kind(), "cursor_error");
    assert!(err.to_string().contains("cursor.json"), "{err}");
}

#[test]
fn cursor_file_with_flush_above_processed_is_rejected() {
    let path = temp_path("cursor.json");
    write(
        &path,
        r#"{"last_processed_slot":5,"last_processed_blockhash":"h","last_flush_slot":9,"updated_at":1}"#,
    );
    let err = IngestorCursor::load_or_init(&path).unwrap_err();
    assert_eq!(err.kind(), "cursor_error");
    assert!(err.to_string().contains("last_flush_slot 9"), "{err}");
}

#[test]
fn unreadable_cursor_path_is_a_cursor_error() {
    let path = temp_path("cursor.json");
    std::fs::create_dir_all(&path).unwrap(); // a directory, not a file
    let err = IngestorCursor::load_or_init(&path).unwrap_err();
    assert_eq!(err.kind(), "cursor_error");
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[test]
fn default_config_is_valid() {
    IngestorConfig::new("cursor.json").validate().unwrap();
}

#[test]
fn invalid_configs_name_the_offending_field() {
    let base = IngestorConfig::new("cursor.json");
    let cases: Vec<(&str, IngestorConfig)> = vec![
        ("cursor_path", IngestorConfig { cursor_path: PathBuf::new(), ..base.clone() }),
        ("end_slot", IngestorConfig { start_slot: Some(10), end_slot: Some(9), ..base.clone() }),
        ("tip_poll_interval", IngestorConfig { tip_poll_interval: Duration::ZERO, ..base.clone() }),
        ("channel_capacity", IngestorConfig { channel_capacity: 0, ..base.clone() }),
        ("flush_interval", IngestorConfig { flush_interval: Duration::ZERO, ..base.clone() }),
        ("flush_max_slots", IngestorConfig { flush_max_slots: 0, ..base.clone() }),
    ];
    for (field, config) in cases {
        match config.validate() {
            Err(PrismError::ConfigError { field: got, .. }) => assert_eq!(got, field),
            other => panic!("{field}: expected a ConfigError, got {other:?}"),
        }
    }
}

#[test]
fn ingestor_new_rejects_an_invalid_config() {
    let config = IngestorConfig { channel_capacity: 0, ..IngestorConfig::new(temp_path("c.json")) };
    let result = Ingestor::new(ScriptedChain::with_tip(1), tracker(), config, Arc::new(LogSink));
    assert!(matches!(result, Err(PrismError::ConfigError { .. })));
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

#[test]
fn block_decodes_into_buffered_transactions() {
    let block = chain_block(
        77,
        vec![
            tx_json("ok", &[SYSTEM_PROGRAM, TOKEN_PROGRAM, SYSTEM_PROGRAM], None, 5_000),
            tx_json("failed", &[TOKEN_PROGRAM], Some(json!({"InstructionError": [0, {"Custom": 1}]})), 7_000),
            tx_json("vote", &[VOTE_PROGRAM_ID], None, 5_000),
        ],
    );
    let slot = BufferedSlot::from_block(&block, true).unwrap();
    assert_eq!(slot.slot, 77);
    assert_eq!(slot.blockhash, hash(77));
    assert_eq!(slot.parent_blockhash, hash(76));
    assert_eq!(slot.block_time, Some(1_700_000_077));
    assert!(slot.is_finalized);
    assert_eq!(slot.transactions.len(), 3);

    let ok = &slot.transactions[0];
    assert_eq!(
        ok,
        &BufferedTransaction {
            index: 0,
            signature: "ok".into(),
            version: "unknown".into(),
            success: true,
            error: None,
            fee: 5_000,
            compute_units: Some(6_000),
            instruction_count: 3,
            account_count: 3,
            program_ids: vec![SYSTEM_PROGRAM.into(), TOKEN_PROGRAM.into()],
            is_vote: false,
        }
    );

    let failed = &slot.transactions[1];
    assert_eq!(failed.index, 1);
    assert!(!failed.success);
    assert!(failed.error.as_deref().unwrap().contains("InstructionError"));
    assert_eq!(failed.fee, 7_000);

    assert!(slot.transactions[2].is_vote);
    let non_votes: Vec<_> = slot.non_vote_transactions().map(|tx| tx.signature.as_str()).collect();
    assert_eq!(non_votes, ["ok", "failed"]);
}

#[test]
fn empty_block_decodes_to_no_transactions() {
    let slot = BufferedSlot::from_block(&chain_block(5, vec![]), false).unwrap();
    assert!(slot.transactions.is_empty());
    assert!(!slot.is_finalized);
}

#[test]
fn transaction_without_meta_counts_as_successful_with_unknown_fee() {
    let mut tx = tx_json("nometa", &[SYSTEM_PROGRAM], None, 0);
    tx["meta"] = Value::Null;
    let slot = BufferedSlot::from_block(&chain_block(5, vec![tx]), false).unwrap();
    let tx = &slot.transactions[0];
    assert!(tx.success);
    assert_eq!(tx.fee, 0);
    assert_eq!(tx.compute_units, None);
}

#[test]
fn undecodable_transaction_is_a_deserialize_error() {
    let mut tx = tx_json("x", &[SYSTEM_PROGRAM], None, 0);
    tx["transaction"] = json!(["!!not base64!!", "base64"]);
    let err = BufferedSlot::from_block(&chain_block(9, vec![tx]), false).unwrap_err();
    assert_eq!(err.kind(), "deserialize_error");
    assert_eq!(err.slot(), Some(9));
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_run_ingests_every_slot_in_order() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.skip(&[103, 106]);
    let sink = Arc::new(RecordingSink::default());
    let path = temp_path("cursor.json");
    let config = IngestorConfig { start_slot: Some(100), end_slot: Some(110), ..fast_config(path.clone()) };
    let ingestor = ingestor(Arc::clone(&chain), config, Arc::clone(&sink));

    let report = ingestor.run().await.unwrap();

    assert_eq!(report.first_slot, Some(100));
    assert_eq!(report.slots_processed, 9);
    assert_eq!(report.slots_skipped, 2);
    let expected: Vec<u64> = (100..=110).filter(|s| ![103, 106].contains(s)).collect();
    assert_eq!(sink.slots(), expected);
    let txs: u64 = expected.iter().map(|s| s % 3 + 1).sum();
    assert_eq!(report.transactions_processed, txs);
    assert_eq!(chain.block_calls(), (100..=110).collect::<Vec<_>>());

    assert_eq!(report.cursor.last_processed_slot, 110);
    assert_eq!(report.cursor.last_processed_blockhash, hash(110));
    assert_eq!(report.cursor.last_flush_slot, 110);
    assert!(ingestor.buffer().is_empty());
    assert_eq!(IngestorCursor::load_or_init(&path).unwrap().state(), report.cursor);
}

#[tokio::test]
async fn trailing_skipped_slots_still_move_the_flush_edge() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.skip(&[9, 10]);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { start_slot: Some(8), end_slot: Some(10), ..fast_config(temp_path("c.json")) };
    let report = ingestor(chain, config, Arc::clone(&sink)).run().await.unwrap();
    assert_eq!(sink.slots(), [8]);
    assert_eq!(report.cursor.last_processed_slot, 10);
    assert_eq!(report.cursor.last_processed_blockhash, hash(8));
    assert_eq!(report.cursor.last_flush_slot, 10);
}

#[tokio::test]
async fn restart_resumes_one_past_the_last_flush() {
    let path = temp_path("cursor.json");
    let chain = ScriptedChain::with_tip(1_000);
    let sink = Arc::new(RecordingSink::default());
    let first = IngestorConfig { start_slot: Some(50), end_slot: Some(54), ..fast_config(path.clone()) };
    ingestor(Arc::clone(&chain), first, Arc::clone(&sink)).run().await.unwrap();

    // start_slot is ignored once the cursor has history.
    let second = IngestorConfig { start_slot: Some(1), end_slot: Some(57), ..fast_config(path.clone()) };
    let report = ingestor(Arc::clone(&chain), second, Arc::clone(&sink)).run().await.unwrap();

    assert_eq!(report.first_slot, Some(55));
    assert_eq!(sink.slots(), (50..=57).collect::<Vec<_>>());
    assert_eq!(report.cursor.last_flush_slot, 57);
}

#[tokio::test]
async fn fresh_cursor_without_start_slot_begins_at_the_tip() {
    let chain = ScriptedChain::with_tip(500);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { end_slot: Some(502), ..fast_config(temp_path("c.json")) };
    let ingestor = ingestor(Arc::clone(&chain), config, Arc::clone(&sink));
    assert_eq!(ingestor.starting_slot().await.unwrap(), 500);

    // The fetch loop waits for the tip to reach end_slot.
    let mover = {
        let chain = Arc::clone(&chain);
        tokio::spawn(async move {
            for tip in 501..=502 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                chain.tip.store(tip, Ordering::SeqCst);
            }
        })
    };
    let report = tokio::time::timeout(Duration::from_secs(5), ingestor.run()).await.unwrap().unwrap();
    mover.await.unwrap();
    assert_eq!(report.first_slot, Some(500));
    assert_eq!(sink.slots(), [500, 501, 502]);
}

#[tokio::test]
async fn end_below_resume_point_is_a_no_op() {
    let path = temp_path("cursor.json");
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    cursor.record_processed(90, "h90");
    cursor.record_flush(90).unwrap();

    let chain = ScriptedChain::with_tip(1_000);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { end_slot: Some(80), ..fast_config(path) };
    let report = ingestor(Arc::clone(&chain), config, Arc::clone(&sink)).run().await.unwrap();
    assert_eq!(report.first_slot, None);
    assert_eq!(report.slots_processed, 0);
    assert!(chain.block_calls().is_empty());
    assert_eq!(report.cursor.last_flush_slot, 90);
}

#[tokio::test]
async fn transient_failures_are_retried() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.transient.lock().unwrap().insert(21, 2);
    chain.tip_failures.store(2, Ordering::SeqCst);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { start_slot: Some(20), end_slot: Some(22), ..fast_config(temp_path("c.json")) };
    let report = ingestor(Arc::clone(&chain), config, Arc::clone(&sink)).run().await.unwrap();
    assert_eq!(sink.slots(), [20, 21, 22]);
    assert_eq!(chain.block_calls(), [20, 21, 21, 21, 22]);
    assert_eq!(report.cursor.last_flush_slot, 22);
}

#[tokio::test]
async fn permanent_failure_halts_and_keeps_what_was_flushed() {
    let path = temp_path("cursor.json");
    let chain = ScriptedChain::with_tip(1_000);
    chain.fatal.lock().unwrap().insert(33);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { start_slot: Some(30), end_slot: Some(40), ..fast_config(path.clone()) };
    let ingestor = ingestor(Arc::clone(&chain), config, Arc::clone(&sink));

    let err = ingestor.run().await.unwrap_err();
    assert_eq!(err.kind(), "deserialize_error");
    assert_eq!(err.slot(), Some(33));
    // Slots before the failure are flushed on the way out.
    assert_eq!(sink.slots(), [30, 31, 32]);
    let cursor = IngestorCursor::load_or_init(&path).unwrap();
    assert_eq!(cursor.state().last_flush_slot, 32);
    assert_eq!(cursor.resume_slot(), Some(33));
    assert!(!chain.block_calls().contains(&34));
}

#[tokio::test]
async fn sink_failure_halts_without_moving_the_flush_edge() {
    let path = temp_path("cursor.json");
    let chain = ScriptedChain::with_tip(1_000);
    let sink = Arc::new(RecordingSink::default());
    sink.failures.store(usize::MAX, Ordering::SeqCst);
    let config = IngestorConfig { start_slot: Some(60), end_slot: Some(62), ..fast_config(path.clone()) };
    let ingestor = ingestor(chain, config, Arc::clone(&sink));

    let err = ingestor.run().await.unwrap_err();
    assert_eq!(err.kind(), "storage_error");
    assert!(sink.slots().is_empty());
    // The unwritten slots stay buffered, in order.
    let buffered: Vec<u64> = ingestor.buffer().snapshot().iter().map(|s| s.slot).collect();
    assert!(!buffered.is_empty());
    assert!(buffered.windows(2).all(|w| w[0] < w[1]), "{buffered:?}");
    assert_eq!(ingestor.cursor().state().last_flush_slot, 59);
}

#[tokio::test]
async fn full_buffer_triggers_a_flush_before_the_interval() {
    let chain = ScriptedChain::with_tip(1);
    chain.advancing.store(true, Ordering::SeqCst);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig {
        start_slot: Some(1),
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots: 2,
        ..fast_config(temp_path("c.json"))
    };
    let ingestor = Arc::new(ingestor(chain, config, Arc::clone(&sink)));
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    // Neither the hour-long interval nor shutdown can explain a flush here.
    tokio::time::timeout(Duration::from_secs(5), async {
        while sink.slots().len() < 4 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("size-triggered flush");
    assert!(!run.is_finished());
    assert!(ingestor.cursor().state().last_flush_slot >= 2);

    ingestor.shutdown();
    let report = run.await.unwrap().unwrap();
    assert!(report.flushes >= 2, "flushes: {}", report.flushes);
    let flushed = sink.slots();
    assert_eq!(flushed, (1..=*flushed.last().unwrap()).collect::<Vec<_>>());
}

#[tokio::test]
async fn shutdown_flushes_the_buffer_and_persists_the_cursor() {
    let path = temp_path("cursor.json");
    let chain = ScriptedChain::with_tip(10);
    chain.advancing.store(true, Ordering::SeqCst);
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig {
        start_slot: Some(10),
        flush_interval: Duration::from_secs(3_600),
        ..fast_config(path.clone())
    };
    let ingestor = Arc::new(ingestor(chain, config, Arc::clone(&sink)));

    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    // Wait until something is buffered, then stop.
    tokio::time::timeout(Duration::from_secs(5), async {
        while ingestor.buffer().len() < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(sink.slots().is_empty(), "nothing flushed before shutdown");
    ingestor.shutdown();

    let report = tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
    assert!(ingestor.buffer().is_empty());
    let flushed = sink.slots();
    assert!(flushed.len() >= 3);
    assert_eq!(flushed[0], 10);
    assert_eq!(report.cursor.last_flush_slot, *flushed.last().unwrap());
    assert_eq!(report.cursor.last_flush_slot, report.cursor.last_processed_slot);
    assert_eq!(IngestorCursor::load_or_init(&path).unwrap().state(), report.cursor);
}

#[tokio::test]
async fn buffered_slots_carry_the_trackers_finality() {
    let watermark = Arc::new(FinalityWatermark::new());
    let _ = watermark.advance_confirmed(110, &hash(110));
    let _ = watermark.advance_finalized(102, &hash(102));
    let tracker = Arc::new(CommitmentTracker::from_watermark(watermark));
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig { start_slot: Some(100), end_slot: Some(104), ..fast_config(temp_path("c.json")) };
    Ingestor::new(ScriptedChain::with_tip(1_000), tracker, config, Arc::clone(&sink) as Arc<dyn FlushSink>)
        .unwrap()
        .run()
        .await
        .unwrap();
    let finality: Vec<(u64, bool)> = sink
        .batches
        .lock()
        .unwrap()
        .iter()
        .flatten()
        .map(|s| (s.slot, s.is_finalized))
        .collect();
    assert_eq!(finality, [(100, true), (101, true), (102, true), (103, false), (104, false)]);
}

// ---------------------------------------------------------------------------
// Helius setup (offline)
// ---------------------------------------------------------------------------

#[test]
fn helius_chain_ids() {
    assert_eq!(helius_chain_id(&Cluster::Devnet), "solana-devnet");
    assert_eq!(helius_chain_id(&Cluster::MainnetBeta), "solana-mainnet");
    assert_eq!(helius_chain_id(&Cluster::StakedMainnetBeta), "solana-mainnet");
}

#[tokio::test]
async fn helius_adapter_is_built_without_a_request() {
    let adapter = helius_solana_adapter("00000000-0000-0000-0000-000000000000", Cluster::Devnet)
        .await
        .unwrap();
    assert_eq!(adapter.chain_id(), "solana-devnet");
    assert!(adapter.config().rpc_url.starts_with("https://"));
    assert!(adapter.config().rpc_url.contains("devnet"));
    // The key never shows up in Debug output.
    assert!(!format!("{:?}", adapter.config()).contains("00000000-0000"));
    assert_eq!(adapter.config().encoding, TransactionEncoding::Base64);
    assert_eq!(adapter.metrics().snapshot().requests, 0);
}

#[tokio::test]
async fn helius_adapter_rejects_an_empty_key() {
    let err = helius_solana_adapter("", Cluster::Devnet).await.err().expect("empty key fails");
    assert_eq!(err.kind(), "config_error");
}

// ---------------------------------------------------------------------------
// Live: Solana devnet through Helius
// ---------------------------------------------------------------------------

/// The devnet adapter, or `None` (with a note) when no key is configured.
async fn live_adapter() -> Option<Arc<prism::adapter::solana::SolanaAdapter>> {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return None;
    };
    Some(Arc::new(helius_solana_adapter(&key, Cluster::Devnet).await.expect("helius adapter")))
}

fn print_slot(slot: &BufferedSlot, limit: usize) {
    println!(
        "\n=== slot {} | blockhash {} | parent {} | block_time {:?} | finalized {} | {} txs ({} non-vote) ===",
        slot.slot,
        slot.blockhash,
        slot.parent_blockhash,
        slot.block_time,
        slot.is_finalized,
        slot.transactions.len(),
        slot.non_vote_transactions().count(),
    );
    // Non-vote transactions first: they are the interesting ones.
    let mut txs: Vec<&BufferedTransaction> = slot.non_vote_transactions().collect();
    txs.extend(slot.transactions.iter().filter(|tx| tx.is_vote));
    for tx in txs.into_iter().take(limit) {
        println!(
            "  #{:<4} {} v{:<6} {} fee={:<7} cu={:<8} ix={} accts={} vote={} programs={:?}{}",
            tx.index,
            tx.signature,
            tx.version,
            if tx.success { "ok  " } else { "FAIL" },
            tx.fee,
            tx.compute_units.map_or("-".into(), |cu| cu.to_string()),
            tx.instruction_count,
            tx.account_count,
            tx.is_vote,
            tx.program_ids,
            tx.error.as_ref().map(|e| format!(" err={e}")).unwrap_or_default(),
        );
    }
}

/// Walk back from the confirmed tip to the newest block holding at least
/// `min_txs` transactions.
async fn recent_busy_block(adapter: &dyn ChainAdapter, min_txs: usize) -> ChainBlock {
    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap().slot_or_height;
    for slot in (tip.saturating_sub(200)..=tip).rev() {
        match adapter.get_block(slot).await {
            Ok(block) => {
                let ChainBlockData::Solana(raw) = &block.raw;
                if raw.transactions.len() >= min_txs {
                    return block;
                }
            }
            Err(PrismError::SlotSkipped { .. }) => {}
            Err(err) if err.is_retryable() => eprintln!("slot {slot}: {err}"),
            Err(err) => panic!("slot {slot}: {err}"),
        }
    }
    panic!("no block with {min_txs}+ transactions in the last 200 slots");
}

#[tokio::test]
async fn live_devnet_tip_is_reachable() {
    let Some(adapter) = live_adapter().await else { return };
    adapter.health_check().await.expect("devnet health");
    // Finalized first: read the other way round, finalized can overtake the
    // confirmed tip read a moment earlier.
    let finalized = adapter.get_tip(CommitmentLevel::Finalized).await.unwrap();
    let confirmed = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap();
    println!("devnet confirmed tip {} ({})", confirmed.slot_or_height, confirmed.block_id);
    println!("devnet finalized tip {} ({})", finalized.slot_or_height, finalized.block_id);
    assert!(confirmed.slot_or_height > 0);
    assert!(finalized.slot_or_height <= confirmed.slot_or_height);
    assert!(!confirmed.block_id.is_empty());
}

#[tokio::test]
async fn live_recent_slot_decodes_and_prints_at_least_ten_transactions() {
    let Some(adapter) = live_adapter().await else { return };
    let block = recent_busy_block(adapter.as_ref(), 10).await;
    let slot = BufferedSlot::from_block(&block, false).unwrap();

    print_slot(&slot, slot.transactions.len().clamp(10, 25));

    assert!(slot.transactions.len() >= 10);
    let mut signatures = HashSet::new();
    for (i, tx) in slot.transactions.iter().enumerate() {
        assert_eq!(tx.index, i);
        assert!(bs58::decode(&tx.signature).into_vec().is_ok_and(|b| b.len() == 64), "{}", tx.signature);
        assert!(signatures.insert(tx.signature.clone()), "duplicate {}", tx.signature);
        assert!(["legacy", "0", "1"].contains(&tx.version.as_str()));
        assert!(tx.instruction_count > 0);
        assert!(tx.account_count > 0);
        assert!(!tx.program_ids.is_empty());
        assert_eq!(tx.success, tx.error.is_none());
    }
}

/// Runs the real pipeline over recent devnet slots with a flush interval long
/// enough that everything stays buffered, prints the buffer, then shuts down.
#[tokio::test]
async fn live_ingestor_buffers_real_devnet_transactions() {
    let Some(adapter) = live_adapter().await else { return };
    let busy = recent_busy_block(adapter.as_ref(), 10).await.slot_or_height;
    let start = busy.saturating_sub(2);
    let end = busy + 2;

    let path = temp_path("cursor.json");
    let sink = Arc::new(RecordingSink::default());
    let config = IngestorConfig {
        start_slot: Some(start),
        end_slot: Some(end),
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots: 1_000,
        ..IngestorConfig::new(&path)
    };
    let ingestor = Arc::new(
        Ingestor::new(adapter, tracker(), config, Arc::clone(&sink) as Arc<dyn FlushSink>).unwrap(),
    );
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });

    // Watch the in-memory buffer fill up.
    let mut seen = 0;
    let buffered = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let snapshot = ingestor.buffer().snapshot();
            if snapshot.len() != seen {
                seen = snapshot.len();
                let state = ingestor.cursor().state();
                println!(
                    "buffer: {} slots | cursor processed={} flushed={} unflushed={}",
                    seen, state.last_processed_slot, state.last_flush_slot, state.unflushed_slots()
                );
            }
            if snapshot.last().is_some_and(|s| s.slot >= busy) || run.is_finished() {
                return snapshot;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("buffer filled within 60 s");

    println!("\n##### in-memory buffer (not yet flushed) #####");
    for slot in &buffered {
        print_slot(slot, 12);
    }
    let total: usize = buffered.iter().map(|s| s.transactions.len()).sum();
    assert!(total >= 10, "only {total} transactions buffered");

    let report = run.await.unwrap().unwrap();
    println!("\nreport: {report:?}");
    assert_eq!(report.first_slot, Some(start));
    assert_eq!(report.slots_processed + report.slots_skipped, end - start + 1);
    assert_eq!(report.cursor.last_flush_slot, end);
    assert!(ingestor.buffer().is_empty());
    let flushed = sink.slots();
    assert!(flushed.contains(&busy));
    assert!(flushed.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(IngestorCursor::load_or_init(&path).unwrap().resume_slot(), Some(end + 1));
}
