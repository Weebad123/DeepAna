//! Tests for `prism::ingestor`.
//!
//! The pipeline tests run against a [`ScriptedChain`]: an in-memory chain
//! with a movable tip, skipped slots, token transfers and injectable
//! failures. Every run writes real Parquet files under a fresh temp
//! directory, which the tests read back, and publishes to a
//! [`RecordingChangeLog`].
//!
//! The `live_*` tests run the ingestor against Solana devnet through the
//! Helius SDK and print what it wrote. They need `HELIUS_API_KEY` and are
//! skipped (with a note) without it. To see the output:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test ingestor_test live_ -- --nocapture --test-threads=1
//! ```

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::{UInt8Type, UInt64Type};
use async_trait::async_trait;
use helius::types::Cluster;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prism::adapter::config::TransactionEncoding;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::{CHANGELOG_FILE_NAME, ChangeLog, LocalChangeLog};
use prism::commitment::{CommitmentTracker, FinalityWatermark};
use prism::decoder::spl_token::SPL_TOKEN_PROGRAM_ID;
use prism::ingestor::{
    CURSOR_FILE_NAME, CursorState, HELIUS_API_KEY_ENV, Ingestor, IngestorConfig, IngestorCursor, IngestorReport,
    SKIP_LOG_FILE_NAME, helius_api_key_from_env, helius_chain_id, helius_solana_adapter,
};
use prism::slot_processor::SlotFetcherConfig;
use prism::storage::{PartitionKey, TableKind};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::{ChangeEvent, EventEnvelope, SkipReason};
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
    let _ = fs::remove_dir_all(&dir);
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
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
const SOURCE: &str = "Src1111111111111111111111111111111111111111";
const DESTINATION: &str = "Dst1111111111111111111111111111111111111111";
const MINT: &str = "Mint111111111111111111111111111111111111111";
const TRANSFER_AMOUNT: u64 = 1_500_000;
const TRANSFER_DECIMALS: u8 = 6;

fn meta_json(err: Option<Value>, fee: u64) -> Value {
    json!({
        "err": err,
        "status": if err.is_some() { json!({"Err": err}) } else { json!({"Ok": null}) },
        "fee": fee,
        "preBalances": [], "postBalances": [],
        "computeUnitsConsumed": 1_000 + fee,
    })
}

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
        "meta": meta_json(err, fee),
    })
}

/// A transaction whose one instruction is an SPL Token `TransferChecked`
/// of [`TRANSFER_AMOUNT`] [`MINT`] from [`SOURCE`] to [`DESTINATION`].
fn transfer_checked_tx_json(signature: &str) -> Value {
    let mut data = vec![12u8];
    data.extend(TRANSFER_AMOUNT.to_le_bytes());
    data.push(TRANSFER_DECIMALS);
    json!({
        "transaction": {
            "signatures": [signature],
            "message": {
                "header": {"numRequiredSignatures": 1, "numReadonlySignedAccounts": 0, "numReadonlyUnsignedAccounts": 2},
                "accountKeys": [PAYER, SOURCE, MINT, DESTINATION, SPL_TOKEN_PROGRAM_ID],
                "recentBlockhash": "11111111111111111111111111111111",
                "instructions": [{
                    "programIdIndex": 4,
                    "accounts": [1, 2, 3, 0],
                    "data": bs58::encode(data).into_string(),
                    "stackHeight": null,
                }],
            }
        },
        "meta": meta_json(None, 5_000),
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

/// Transactions the [`ScriptedChain`] serves for `slot` without a token
/// transfer.
fn tx_count(slot: u64) -> u64 {
    slot % 3 + 1
}

/// Every `.parquet` file of `table` under `data_dir`, by starting slot.
fn table_files(data_dir: &Path, table: TableKind) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![PartitionKey::table_root(data_dir, table)];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else { continue };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort_by_key(|p| PartitionKey::from_path(p).expect("a path in the layout").from_slot());
    files
}

/// Every batch of every file of `table`, in file order.
fn read_table(data_dir: &Path, table: TableKind) -> Vec<RecordBatch> {
    table_files(data_dir, table)
        .iter()
        .flat_map(|path| {
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// The `slot` column of every row of `table`, across all files.
fn table_slots(data_dir: &Path, table: TableKind) -> Vec<u64> {
    read_table(data_dir, table)
        .iter()
        .flat_map(|b| b.column_by_name("slot").unwrap().as_primitive::<UInt64Type>().values().to_vec())
        .collect()
}

fn table_rows(data_dir: &Path, table: TableKind) -> usize {
    read_table(data_dir, table).iter().map(RecordBatch::num_rows).sum()
}

/// Waits up to 5 s for `done`.
async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/// An in-memory chain.
#[derive(Default)]
struct ScriptedChain {
    tip: AtomicU64,
    skipped: Mutex<HashSet<u64>>,
    /// Slots whose block holds one SPL Token `TransferChecked`.
    with_transfer: Mutex<HashSet<u64>>,
    /// Transient failures still to be served, per slot.
    transient: Mutex<HashMap<u64, usize>>,
    /// Slots that fail permanently.
    fatal: Mutex<HashSet<u64>>,
    /// Tip failures still to be served.
    tip_failures: AtomicUsize,
    block_calls: Mutex<Vec<u64>>,
    /// When set, the tip moves up by one on every tip read.
    advancing: AtomicBool,
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
        if self.with_transfer.lock().unwrap().contains(&slot) {
            return Ok(chain_block(slot, vec![transfer_checked_tx_json(&format!("xfer-{slot}"))]));
        }
        Ok(simple_block(slot, tx_count(slot) as usize))
    }

    /// No account exists: mints stay unresolved.
    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

/// Keeps every event it is given; can be told to refuse `New` or `Final`
/// events.
#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
    refuse_new: AtomicBool,
    refuse_final: AtomicBool,
}

impl RecordingChangeLog {
    fn events(&self) -> Vec<ChangeEvent> {
        self.events.lock().unwrap().clone()
    }

    /// Slots of the `New` events, in emission order.
    fn new_slots(&self) -> Vec<u64> {
        self.events()
            .iter()
            .filter_map(|e| match e {
                ChangeEvent::New { slot, .. } => Some(*slot),
                _ => None,
            })
            .collect()
    }

    fn skips(&self) -> Vec<(u64, SkipReason)> {
        self.events()
            .iter()
            .filter_map(|e| match e {
                ChangeEvent::Skip { slot, reason } => Some((*slot, *reason)),
                _ => None,
            })
            .collect()
    }
}

impl ChangeLog for RecordingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        if matches!(event, ChangeEvent::New { .. }) && self.refuse_new.load(Ordering::SeqCst) {
            return Err(PrismError::config("changelog", "unavailable"));
        }
        if matches!(event, ChangeEvent::Final { .. }) && self.refuse_final.load(Ordering::SeqCst) {
            return Err(PrismError::changelog("changelog.jsonl", "append event 1", "disk full"));
        }
        let mut events = self.events.lock().unwrap();
        events.push(event);
        Ok(events.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn tracker() -> Arc<CommitmentTracker> {
    Arc::new(CommitmentTracker::from_watermark(Arc::new(FinalityWatermark::new())))
}

fn fast_fetcher() -> SlotFetcherConfig {
    SlotFetcherConfig {
        max_retries: 3,
        base_delay_ms: 1,
        max_delay_ms: 2,
        request_timeout_ms: 1_000,
    }
}

fn fast_config(data_dir: PathBuf) -> IngestorConfig {
    IngestorConfig {
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_millis(20),
        fetcher: fast_fetcher(),
        ..IngestorConfig::new(data_dir)
    }
}

fn ingestor(
    chain: Arc<ScriptedChain>,
    config: IngestorConfig,
    changelog: &Arc<RecordingChangeLog>,
) -> Ingestor {
    Ingestor::new(chain, tracker(), Arc::clone(changelog) as Arc<dyn ChangeLog>, config)
        .expect("valid ingestor config")
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
fn default_config_is_valid_and_keeps_its_files_in_the_data_dir() {
    let config = IngestorConfig::new("/var/prism");
    config.validate().unwrap();
    assert_eq!(config.data_dir, Path::new("/var/prism"));
    assert_eq!(config.cursor_path, Path::new("/var/prism").join(CURSOR_FILE_NAME));
    assert_eq!(config.skip_log_path, Path::new("/var/prism").join(SKIP_LOG_FILE_NAME));
}

#[test]
fn invalid_configs_name_the_offending_field() {
    let base = IngestorConfig::new("data");
    let cases: Vec<(&str, IngestorConfig)> = vec![
        ("data_dir", IngestorConfig { data_dir: PathBuf::new(), ..base.clone() }),
        ("cursor_path", IngestorConfig { cursor_path: PathBuf::new(), ..base.clone() }),
        ("skip_log_path", IngestorConfig { skip_log_path: PathBuf::new(), ..base.clone() }),
        ("end_slot", IngestorConfig { start_slot: Some(10), end_slot: Some(9), ..base.clone() }),
        ("tip_poll_interval", IngestorConfig { tip_poll_interval: Duration::ZERO, ..base.clone() }),
        (
            "base_delay_ms",
            IngestorConfig { fetcher: SlotFetcherConfig { base_delay_ms: 0, ..fast_fetcher() }, ..base.clone() },
        ),
        ("channel_capacity", IngestorConfig { channel_capacity: 0, ..base.clone() }),
        ("flush_interval", IngestorConfig { flush_interval: Duration::ZERO, ..base.clone() }),
        ("flush_max_slots", IngestorConfig { flush_max_slots: 0, ..base.clone() }),
        ("max_flush_failures", IngestorConfig { max_flush_failures: 0, ..base.clone() }),
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
    let config = IngestorConfig { channel_capacity: 0, ..IngestorConfig::new(temp_path("data")) };
    let changelog: Arc<dyn ChangeLog> = Arc::new(RecordingChangeLog::default());
    let result = Ingestor::new(ScriptedChain::with_tip(1), tracker(), changelog, config);
    assert!(matches!(result, Err(PrismError::ConfigError { .. })));
}

#[test]
fn ingestor_new_writes_nothing() {
    let dir = temp_path("data");
    let changelog = Arc::new(RecordingChangeLog::default());
    let ingestor = ingestor(ScriptedChain::with_tip(1), fast_config(dir.clone()), &changelog);
    assert!(!dir.exists());
    assert_eq!(ingestor.buffered_slot_count(), 0);
    assert_eq!(ingestor.writer_metrics().snapshot().total_flushes, 0);
    assert!(ingestor.decoders().has_decoder(SPL_TOKEN_PROGRAM_ID));
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bounded_run_writes_every_slot_to_parquet_and_announces_it_once() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.skip(&[103, 106]);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig { start_slot: Some(100), end_slot: Some(110), ..fast_config(dir.clone()) };
    let ingestor = ingestor(Arc::clone(&chain), config, &changelog);

    let report = ingestor.run().await.unwrap();

    let expected: Vec<u64> = (100..=110).filter(|s| ![103, 106].contains(s)).collect();
    let txs: u64 = expected.iter().copied().map(tx_count).sum();
    assert_eq!(report.first_slot, Some(100));
    assert_eq!((report.slots_processed, report.slots_skipped, report.transactions_processed), (9, 2, txs));
    assert_eq!(chain.block_calls(), (100..=110).collect::<Vec<_>>());

    // On disk: every slot with data, once, in order; every transaction.
    assert_eq!(table_slots(&dir, TableKind::Slots), expected);
    assert_eq!(table_rows(&dir, TableKind::Transactions) as u64, txs);
    assert_eq!(table_rows(&dir, TableKind::Instructions) as u64, txs, "one instruction per transaction");
    assert!(table_files(&dir, TableKind::TokenTransfers).is_empty(), "no transfers, no file");

    // The change log: one Skip per skipped slot (published by the slot
    // processor, not again by the writer) and one New per slot with data,
    // in slot order, naming files that exist and cover the slot.
    assert_eq!(changelog.skips(), [(103, SkipReason::ValidatorSkipped), (106, SkipReason::ValidatorSkipped)]);
    assert_eq!(changelog.new_slots(), expected);
    for event in changelog.events() {
        let ChangeEvent::New { slot, blockhash, parquet_paths, transaction_count, .. } = event else { continue };
        assert_eq!(blockhash, hash(slot));
        assert_eq!(u64::from(transaction_count), tx_count(slot));
        assert_eq!(parquet_paths.len(), 3, "slots, transactions, instructions");
        for path in &parquet_paths {
            let key = PartitionKey::from_path(Path::new(path)).expect("a path in the layout");
            assert!(Path::new(path).exists() && key.contains_slot(slot), "{slot}: {path}");
        }
    }
    let skip_log = fs::read_to_string(dir.join(SKIP_LOG_FILE_NAME)).unwrap();
    assert_eq!(skip_log.lines().count(), 2);

    assert_eq!(report.cursor.last_processed_slot, 110);
    assert_eq!(report.cursor.last_processed_blockhash, hash(110));
    assert_eq!(report.cursor.last_flush_slot, 110);
    assert_eq!(ingestor.buffered_slot_count(), 0);
    assert_eq!(IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap().state(), report.cursor);
    let m = ingestor.writer_metrics().snapshot();
    assert_eq!(m.total_flushes, report.flushes);
    assert_eq!(m.flush_errors, 0);
}

#[tokio::test]
async fn token_transfers_are_decoded_resolved_and_written() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.with_transfer.lock().unwrap().extend([6, 8]);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig {
        start_slot: Some(5),
        end_slot: Some(8),
        // One flush, so every New event of the run shares one batch.
        flush_interval: Duration::from_secs(3_600),
        ..fast_config(dir.clone())
    };
    let ingestor = ingestor(chain, config, &changelog);

    ingestor.run().await.unwrap();

    let batches = read_table(&dir, TableKind::TokenTransfers);
    let slots: Vec<u64> = batches
        .iter()
        .flat_map(|b| b.column_by_name("slot").unwrap().as_primitive::<UInt64Type>().values().to_vec())
        .collect();
    assert_eq!(slots, [6, 8]);
    for batch in &batches {
        let col = |name: &str| batch.column_by_name(name).unwrap().clone();
        assert!(col("amount_raw").as_primitive::<UInt64Type>().values().iter().all(|a| *a == TRANSFER_AMOUNT));
        assert!(col("decimals").as_primitive::<UInt8Type>().iter().all(|d| d == Some(TRANSFER_DECIMALS)));
        assert!(col("mint_token_address").as_string::<i32>().iter().all(|m| m == Some(MINT)));
        assert!(col("source_token_account").as_string::<i32>().iter().all(|s| s == Some(SOURCE)));
        assert!(col("destination_token_account").as_string::<i32>().iter().all(|d| d == Some(DESTINATION)));
    }
    // The instruction rows carry the decoder's verdict.
    let types: Vec<Option<String>> = read_table(&dir, TableKind::Instructions)
        .iter()
        .flat_map(|b| {
            b.column_by_name("instruction_type").unwrap().as_string::<i32>().iter().map(|t| t.map(str::to_owned)).collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(types.iter().filter(|t| t.as_deref() == Some("TransferChecked")).count(), 2);

    // The resolver saw both transfers; the chain has no mint account.
    let r = ingestor.resolver_metrics().snapshot();
    assert_eq!((r.total_transfers_processed, r.mint_fetches_attempted), (2, 1));
    // The New events of the transfer slots name the transfer file too.
    for event in changelog.events() {
        if let ChangeEvent::New { slot, parquet_paths, .. } = event {
            let has_transfers = parquet_paths.iter().any(|p| p.contains("/token_transfers/"));
            assert!(has_transfers, "slot {slot}: the batch has transfers");
        }
    }
}

#[tokio::test]
async fn trailing_skipped_slots_still_move_the_flush_edge() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.skip(&[9, 10]);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig { start_slot: Some(8), end_slot: Some(10), ..fast_config(dir.clone()) };
    let report = ingestor(chain, config, &changelog).run().await.unwrap();
    assert_eq!(table_slots(&dir, TableKind::Slots), [8]);
    assert_eq!(changelog.new_slots(), [8]);
    assert_eq!(changelog.skips().len(), 2);
    assert_eq!(report.cursor.last_processed_slot, 10);
    assert_eq!(report.cursor.last_processed_blockhash, hash(8));
    assert_eq!(report.cursor.last_flush_slot, 10);
}

#[tokio::test]
async fn a_run_of_only_skipped_slots_writes_no_file_but_moves_the_cursor() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.skip(&[20, 21]);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig { start_slot: Some(20), end_slot: Some(21), ..fast_config(dir.clone()) };
    let report = ingestor(chain, config, &changelog).run().await.unwrap();
    assert!(table_files(&dir, TableKind::Slots).is_empty());
    assert!(changelog.new_slots().is_empty());
    assert_eq!(report.cursor.last_flush_slot, 21);
}

#[tokio::test]
async fn restart_resumes_one_past_the_last_flush() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    let changelog = Arc::new(RecordingChangeLog::default());
    let first = IngestorConfig { start_slot: Some(50), end_slot: Some(54), ..fast_config(dir.clone()) };
    ingestor(Arc::clone(&chain), first, &changelog).run().await.unwrap();

    // start_slot is ignored once the cursor has history.
    let second = IngestorConfig { start_slot: Some(1), end_slot: Some(57), ..fast_config(dir.clone()) };
    let report = ingestor(Arc::clone(&chain), second, &changelog).run().await.unwrap();

    assert_eq!(report.first_slot, Some(55));
    assert_eq!(table_slots(&dir, TableKind::Slots), (50..=57).collect::<Vec<_>>());
    assert_eq!(changelog.new_slots(), (50..=57).collect::<Vec<_>>());
    assert_eq!(report.cursor.last_flush_slot, 57);
}

#[tokio::test]
async fn fresh_cursor_without_start_slot_begins_at_the_tip() {
    let chain = ScriptedChain::with_tip(500);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig { end_slot: Some(502), ..fast_config(dir.clone()) };
    let ingestor = ingestor(Arc::clone(&chain), config, &changelog);
    assert_eq!(ingestor.starting_slot().await.unwrap(), 500);

    // The process loop waits for the tip to reach end_slot.
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
    assert_eq!(table_slots(&dir, TableKind::Slots), [500, 501, 502]);
}

#[tokio::test]
async fn end_below_resume_point_is_a_no_op() {
    let dir = temp_path("data");
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    cursor.record_processed(90, "h90");
    cursor.record_flush(90).unwrap();

    let chain = ScriptedChain::with_tip(1_000);
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig { end_slot: Some(80), ..fast_config(dir) };
    let report = ingestor(Arc::clone(&chain), config, &changelog).run().await.unwrap();
    assert_eq!(report.first_slot, None);
    assert_eq!(report.slots_processed, 0);
    assert!(chain.block_calls().is_empty());
    assert!(changelog.events().is_empty());
    assert_eq!(report.cursor.last_flush_slot, 90);
}

#[tokio::test]
async fn transient_failures_are_retried() {
    let chain = ScriptedChain::with_tip(1_000);
    chain.transient.lock().unwrap().insert(21, 2);
    chain.tip_failures.store(2, Ordering::SeqCst);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig { start_slot: Some(20), end_slot: Some(22), ..fast_config(dir.clone()) };
    let ingestor = ingestor(Arc::clone(&chain), config, &changelog);
    let report = ingestor.run().await.unwrap();
    assert_eq!(table_slots(&dir, TableKind::Slots), [20, 21, 22]);
    assert_eq!(chain.block_calls(), [20, 21, 21, 21, 22]);
    assert!(changelog.skips().is_empty());
    assert_eq!(ingestor.processor().fetcher().metrics().snapshot().total_retries, 2);
    assert_eq!(report.cursor.last_flush_slot, 22);
}

#[tokio::test]
async fn exhausted_retries_record_a_data_gap_and_halt_before_it() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    chain.transient.lock().unwrap().insert(21, usize::MAX);
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig { start_slot: Some(20), end_slot: Some(25), ..fast_config(dir.clone()) };

    let err = ingestor(Arc::clone(&chain), config, &changelog).run().await.unwrap_err();

    assert_eq!(err.kind(), "rpc_error");
    assert_eq!(changelog.skips(), [(21, SkipReason::RpcUnavailable)]);
    assert_eq!(chain.block_calls().iter().filter(|s| **s == 21).count(), 1 + fast_fetcher().max_retries as usize);
    assert!(!chain.block_calls().contains(&22));
    // Slot 20 is flushed on the way out; 21 is fetched again on restart.
    assert_eq!(table_slots(&dir, TableKind::Slots), [20]);
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    assert_eq!(cursor.resume_slot(), Some(21));
}

#[tokio::test]
async fn permanent_failure_halts_and_keeps_what_was_flushed() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    chain.fatal.lock().unwrap().insert(33);
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig { start_slot: Some(30), end_slot: Some(40), ..fast_config(dir.clone()) };

    let err = ingestor(Arc::clone(&chain), config, &changelog).run().await.unwrap_err();

    assert_eq!(err.kind(), "deserialize_error");
    assert_eq!(err.slot(), Some(33));
    assert_eq!(table_slots(&dir, TableKind::Slots), [30, 31, 32]);
    assert_eq!(changelog.new_slots(), [30, 31, 32]);
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    assert_eq!(cursor.state().last_flush_slot, 32);
    assert_eq!(cursor.resume_slot(), Some(33));
    assert!(!chain.block_calls().contains(&34));
}

/// Makes every flush under `dir` fail: `solana/slots` is a file, so the
/// slot table's date directory cannot be created. The startup rollback's
/// scan skips it.
fn block_writes(dir: &Path) {
    fs::create_dir_all(PartitionKey::chain_root(dir)).unwrap();
    fs::write(PartitionKey::table_root(dir, TableKind::Slots), b"in the way").unwrap();
}

fn unblock_writes(dir: &Path) {
    fs::remove_file(PartitionKey::table_root(dir, TableKind::Slots)).unwrap();
}

#[tokio::test]
async fn repeated_flush_failures_halt_without_moving_the_flush_edge() {
    let dir = temp_path("data");
    block_writes(&dir);
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig { start_slot: Some(60), end_slot: Some(62), ..fast_config(dir.clone()) };
    let ingestor = ingestor(ScriptedChain::with_tip(1_000), config, &changelog);

    let err = ingestor.run().await.unwrap_err();

    assert_eq!(err.kind(), "storage_error");
    assert!(changelog.new_slots().is_empty(), "nothing announced");
    assert!(ingestor.writer_metrics().snapshot().flush_errors >= 1);
    // The rows stay buffered and the flush edge stays put.
    assert!(ingestor.buffered_slot_count() > 0);
    assert_eq!(ingestor.cursor().state().last_flush_slot, 59);
    assert_eq!(ingestor.cursor().resume_slot(), Some(60));
}

#[tokio::test]
async fn a_failed_flush_keeps_its_rows_and_is_retried_at_the_next_interval() {
    let dir = temp_path("data");
    block_writes(&dir);
    let changelog = Arc::new(RecordingChangeLog::default());
    // No end slot: the run keeps going until shut down, so the failures
    // happen on the timer rather than in the final flush.
    let config = IngestorConfig { start_slot: Some(60), max_flush_failures: 1_000, ..fast_config(dir.clone()) };
    let ingestor = Arc::new(ingestor(ScriptedChain::with_tip(62), config, &changelog));
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });

    wait_for("a failed flush", || ingestor.writer_metrics().snapshot().flush_errors >= 2).await;
    assert!(!run.is_finished(), "a failed flush does not halt");
    assert_eq!(ingestor.cursor().state().last_flush_slot, 59);
    assert!(changelog.new_slots().is_empty());

    unblock_writes(&dir);
    wait_for("the retried flush", || ingestor.cursor().state().last_flush_slot == 62).await;
    ingestor.shutdown();
    let report = run.await.unwrap().unwrap();

    assert_eq!(table_slots(&dir, TableKind::Slots), [60, 61, 62]);
    assert_eq!(table_files(&dir, TableKind::Slots).len(), 1, "one file, written by the retry");
    assert_eq!(changelog.new_slots(), [60, 61, 62]);
    assert_eq!(report.cursor.last_flush_slot, 62);
}

#[tokio::test]
async fn files_of_an_unrecorded_flush_are_rolled_back_on_restart() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    let changelog = Arc::new(RecordingChangeLog::default());
    let first = IngestorConfig { start_slot: Some(66), end_slot: Some(69), ..fast_config(dir.clone()) };
    ingestor(Arc::clone(&chain), first, &changelog).run().await.unwrap();

    // Second run: 70..=71 are flushed, but their New events are refused, so
    // the cursor never records the flush and the run halts.
    let refusing = Arc::new(RecordingChangeLog::default());
    refusing.refuse_new.store(true, Ordering::SeqCst);
    let second = IngestorConfig {
        end_slot: Some(72),
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots: 2,
        ..fast_config(dir.clone())
    };
    let err = ingestor(Arc::clone(&chain), second, &refusing).run().await.unwrap_err();
    assert_eq!(err.kind(), "config_error", "the change log's error");
    assert_eq!(table_slots(&dir, TableKind::Slots), (66..=71).collect::<Vec<_>>(), "70, 71 written, not recorded");
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
    assert_eq!(cursor.resume_slot(), Some(70));

    // Third run: the orphaned file goes, and 70..=72 are written once.
    let third = IngestorConfig { end_slot: Some(72), ..fast_config(dir.clone()) };
    let report = ingestor(Arc::clone(&chain), third, &changelog).run().await.unwrap();

    assert_eq!(report.first_slot, Some(70));
    assert_eq!(table_slots(&dir, TableKind::Slots), (66..=72).collect::<Vec<_>>());
    assert_eq!(changelog.new_slots(), (66..=72).collect::<Vec<_>>());
}

#[tokio::test]
async fn files_left_before_the_first_cursor_save_are_rolled_back_too() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    let refusing = Arc::new(RecordingChangeLog::default());
    refusing.refuse_new.store(true, Ordering::SeqCst);
    let config = |flush_max_slots| IngestorConfig {
        start_slot: Some(70),
        end_slot: Some(72),
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots,
        ..fast_config(dir.clone())
    };
    ingestor(Arc::clone(&chain), config(2), &refusing).run().await.unwrap_err();
    assert!(!dir.join(CURSOR_FILE_NAME).exists(), "the cursor was never saved");
    assert_eq!(table_slots(&dir, TableKind::Slots), [70, 71]);

    // A different flush grouping, so the retry's file does not simply
    // overwrite the orphaned one.
    let changelog = Arc::new(RecordingChangeLog::default());
    ingestor(Arc::clone(&chain), config(1_000), &changelog).run().await.unwrap();
    assert_eq!(table_slots(&dir, TableKind::Slots), [70, 71, 72]);
    assert_eq!(table_files(&dir, TableKind::Slots).len(), 1);
}

#[tokio::test]
async fn full_buffer_triggers_a_flush_before_the_interval() {
    let chain = ScriptedChain::with_tip(1);
    chain.advancing.store(true, Ordering::SeqCst);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_path("data");
    let config = IngestorConfig {
        start_slot: Some(1),
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots: 2,
        ..fast_config(dir.clone())
    };
    let ingestor = Arc::new(ingestor(chain, config, &changelog));
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    // Neither the hour-long interval nor shutdown can explain a flush here.
    wait_for("size-triggered flushes", || changelog.new_slots().len() >= 4).await;
    assert!(!run.is_finished());
    assert!(ingestor.cursor().state().last_flush_slot >= 2);
    assert!(table_files(&dir, TableKind::Slots).len() >= 2);

    ingestor.shutdown();
    let report = run.await.unwrap().unwrap();
    assert!(report.flushes >= 2, "flushes: {}", report.flushes);
    let written = table_slots(&dir, TableKind::Slots);
    assert_eq!(written, (1..=*written.last().unwrap()).collect::<Vec<_>>());
    assert_eq!(changelog.new_slots(), written);
}

#[tokio::test]
async fn shutdown_flushes_the_buffer_and_persists_the_cursor() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(10);
    chain.advancing.store(true, Ordering::SeqCst);
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig {
        start_slot: Some(10),
        flush_interval: Duration::from_secs(3_600),
        ..fast_config(dir.clone())
    };
    let ingestor = Arc::new(ingestor(chain, config, &changelog));

    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });
    wait_for("buffered slots", || ingestor.buffered_slot_count() >= 3).await;
    assert!(table_files(&dir, TableKind::Slots).is_empty(), "nothing flushed before shutdown");
    ingestor.shutdown();

    let report = tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
    assert_eq!(ingestor.buffered_slot_count(), 0);
    let written = table_slots(&dir, TableKind::Slots);
    assert!(written.len() >= 3);
    assert_eq!(written[0], 10);
    assert_eq!(changelog.new_slots(), written);
    assert_eq!(report.cursor.last_flush_slot, *written.last().unwrap());
    assert_eq!(report.cursor.last_flush_slot, report.cursor.last_processed_slot);
    assert_eq!(IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap().state(), report.cursor);
}

// ---------------------------------------------------------------------------
// With the local change log
// ---------------------------------------------------------------------------

/// Every envelope in `{dir}/changelog.jsonl`, after checking that the ids
/// run 1, 2, 3, ... in file order.
fn gap_free_envelopes(dir: &Path) -> Vec<EventEnvelope> {
    let contents = fs::read_to_string(dir.join(CHANGELOG_FILE_NAME)).unwrap();
    assert!(contents.ends_with('\n'), "partial last line");
    let envelopes: Vec<EventEnvelope> =
        contents.lines().map(|line| EventEnvelope::from_json(line).unwrap()).collect();
    for (index, envelope) in envelopes.iter().enumerate() {
        assert_eq!(envelope.id, index as u64 + 1, "line {}", index + 1);
        assert_eq!(envelope.chain, "solana-scripted");
    }
    envelopes
}

/// One `LocalChangeLog`, shared by all three producers at once: the gap
/// handler and the writer task through the ingestor, and a finality poller
/// polling every 2 ms while they run.
async fn run_with_local_changelog(dir: &Path, chain: &Arc<ScriptedChain>, range: (u64, u64)) -> IngestorReport {
    let changelog: Arc<dyn ChangeLog> = Arc::new(LocalChangeLog::open(dir, chain.chain_id()).unwrap());
    let tracker = Arc::new(
        CommitmentTracker::spawn_with_interval(
            Arc::clone(chain) as Arc<dyn ChainAdapter>,
            Arc::clone(&changelog),
            Duration::from_millis(2),
        )
        .unwrap(),
    );
    let config = IngestorConfig {
        start_slot: Some(range.0),
        end_slot: Some(range.1),
        flush_max_slots: 7,
        ..fast_config(dir.to_path_buf())
    };
    let ingestor = Ingestor::new(
        Arc::clone(chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&tracker),
        changelog,
        config,
    )
    .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(10), ingestor.run())
        .await
        .expect("run finished")
        .unwrap();
    drop(ingestor);
    // Stop the poller before the file is read, and release the file lock.
    Arc::try_unwrap(tracker).ok().expect("only owner left").shutdown().await;
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn all_three_producers_share_one_local_change_log_with_gap_free_ids() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    chain.advancing.store(true, Ordering::SeqCst);
    chain.skip(&[103, 106, 130, 131]);

    let report = run_with_local_changelog(&dir, &chain, (100, 140)).await;
    assert_eq!(report.slots_skipped, 4);

    let envelopes = gap_free_envelopes(&dir);
    let events: Vec<&ChangeEvent> = envelopes.iter().map(|e| &e.event).collect();
    let new_slots: Vec<u64> = events.iter().filter(|e| e.is_new()).map(|e| e.slot()).collect();
    let skip_slots: Vec<u64> = events.iter().filter(|e| e.is_skip()).map(|e| e.slot()).collect();
    assert_eq!(new_slots, table_slots(&dir, TableKind::Slots), "one New per written slot, in order");
    assert_eq!(skip_slots, [103, 106, 130, 131]);

    // Finals chain up: each names the slot the one before it announced.
    let finals: Vec<(u64, u64)> = events
        .iter()
        .filter_map(|e| match e {
            ChangeEvent::Final { slot, previous_slot, .. } => Some((*slot, *previous_slot)),
            _ => None,
        })
        .collect();
    assert!(!finals.is_empty(), "the poller announced finality during the run");
    let mut previous = 0;
    for (slot, previous_slot) in finals {
        assert_eq!(previous_slot, previous);
        assert!(slot > previous);
        previous = slot;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ids_continue_gap_free_when_the_pipeline_restarts_on_the_same_data_dir() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(1_000);
    chain.advancing.store(true, Ordering::SeqCst);
    chain.skip(&[52]);

    run_with_local_changelog(&dir, &chain, (50, 60)).await;
    let first_run = gap_free_envelopes(&dir).len();
    // The second run resumes from the cursor (61), whatever range it is given.
    run_with_local_changelog(&dir, &chain, (50, 70)).await;
    let envelopes = gap_free_envelopes(&dir);
    assert!(envelopes.len() > first_run);
    let new_slots: Vec<u64> = envelopes.iter().filter(|e| e.event.is_new()).map(|e| e.event.slot()).collect();
    let expected: Vec<u64> = (50..=70).filter(|s| *s != 52).collect();
    assert_eq!(new_slots, expected, "each slot announced once across both runs");
}

#[tokio::test]
async fn a_change_log_failure_in_the_finality_poller_halts_the_ingestor() {
    let dir = temp_path("data");
    let chain = ScriptedChain::with_tip(10);
    chain.advancing.store(true, Ordering::SeqCst);
    let changelog = Arc::new(RecordingChangeLog::default());
    changelog.refuse_final.store(true, Ordering::SeqCst);
    let tracker = Arc::new(
        CommitmentTracker::spawn_with_interval(
            Arc::clone(&chain) as Arc<dyn ChainAdapter>,
            Arc::clone(&changelog) as Arc<dyn ChangeLog>,
            Duration::from_millis(5),
        )
        .unwrap(),
    );
    // No end slot: only the halt can stop this run.
    let config = IngestorConfig { start_slot: Some(10), ..fast_config(dir.clone()) };
    let ingestor = Ingestor::new(
        chain,
        Arc::clone(&tracker),
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        config,
    )
    .unwrap();

    let err = tokio::time::timeout(Duration::from_secs(5), ingestor.run())
        .await
        .expect("the halt stops the run")
        .unwrap_err();
    assert_eq!(err.kind(), "changelog_error", "{err}");
    assert!(!tracker.is_running());
    // The writer still flushed and announced what it had on the way out.
    let written = table_slots(&dir, TableKind::Slots);
    assert_eq!(changelog.new_slots(), written);
    let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap().state();
    if let Some(last) = written.last() {
        assert!(cursor.last_flush_slot >= *last, "the cursor covers every announced slot");
    }
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

/// Runs the whole pipeline over recent devnet slots around a busy block,
/// then checks what landed on disk and in the change log against the run's
/// report, and prints it.
#[tokio::test]
async fn live_ingestor_writes_real_devnet_slots_to_parquet() {
    let Some(adapter) = live_adapter().await else { return };
    let busy = recent_busy_block(adapter.as_ref(), 10).await.slot_or_height;
    let (start, end) = (busy.saturating_sub(2), busy + 2);

    let dir = temp_path("data");
    let changelog = Arc::new(RecordingChangeLog::default());
    let config = IngestorConfig {
        start_slot: Some(start),
        end_slot: Some(end),
        // One flush, at the end: the whole range lands in one file per table.
        flush_interval: Duration::from_secs(3_600),
        flush_max_slots: 1_000,
        ..IngestorConfig::new(&dir)
    };
    let ingestor = Ingestor::new(
        adapter,
        tracker(),
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        config,
    )
    .unwrap();

    let report = tokio::time::timeout(Duration::from_secs(120), ingestor.run())
        .await
        .expect("run finished within 120 s")
        .unwrap();
    println!("\nreport: {report:?}");

    assert_eq!(report.first_slot, Some(start));
    assert_eq!(report.slots_processed + report.slots_skipped, end - start + 1);
    assert_eq!(report.cursor.last_flush_slot, end);
    assert_eq!(ingestor.buffered_slot_count(), 0);

    let written = table_slots(&dir, TableKind::Slots);
    assert!(written.contains(&busy));
    assert_eq!(written.len() as u64, report.slots_processed);
    assert_eq!(table_rows(&dir, TableKind::Transactions) as u64, report.transactions_processed);
    assert!(report.transactions_processed >= 10);
    assert_eq!(changelog.new_slots(), written);
    assert_eq!(changelog.skips().len() as u64, report.slots_skipped);

    let r = ingestor.resolver_metrics().snapshot();
    println!(
        "\n  devnet slots {start}..={end}: {} with data, {} skipped\n  \
         rows: {} transactions, {} instructions, {} token transfers ({} fully enriched)",
        report.slots_processed,
        report.slots_skipped,
        report.transactions_processed,
        table_rows(&dir, TableKind::Instructions),
        table_rows(&dir, TableKind::TokenTransfers),
        r.fully_enriched,
    );
    for table in TableKind::ALL {
        for path in table_files(&dir, table) {
            println!("    {} ({} bytes)", path.display(), fs::metadata(&path).unwrap().len());
        }
    }
    assert_eq!(
        IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap().resume_slot(),
        Some(end + 1)
    );
    let _ = fs::remove_dir_all(&dir);
}
