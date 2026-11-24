//! Finality tracking and reconciliation: correctness, safety and crash
//! consistency.
//!
//! * `finality.json`: round trip, other chains, corruption, versions.
//! * A persistent [`CommitmentTracker`]: starts from the saved watermark (so
//!   finality is known before the first poll), resumes announcing where it
//!   stopped instead of from 0, saves after each poll, halts when it cannot.
//! * [`classify_blocks`]: the "same block?" checks, case by case.
//! * End to end, with a real [`Ingestor`] and a [`Reconciler`] against a
//!   [`Chain`] whose confirmed view can differ from its finalized one:
//!   - a stored block from another fork at a slot the finalized chain also
//!     has is found (lineage, and blockhash when verifying) and corrected;
//!   - no `New` / `Skip` of the abandoned data is announced after its
//!     `Correction`, even with the writer flushing continuously;
//!   - a crash right after a rewind was decided is finished on restart, with
//!     the ingestor and the reconciler both live, counted once, announced,
//!     and re-ingested at `finalized` before the first finality poll;
//!   - a crash at any later point of the rewind is finished the same way.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use prism::adapter::{
    AccountData, BlockHeader, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::commitment::{
    CommitmentTracker, FINALITY_FILE_NAME, FinalityRecord, FinalityStore, FinalityWatermark,
};
use prism::ingestor::{CURSOR_FILE_NAME, Ingestor, IngestorConfig, IngestorCursor, SKIP_LOG_FILE_NAME};
use prism::reconcile::sources::{stored_blocks, stored_slots};
use prism::reconcile::{
    Attempt, FinalizedChain, Finding, FindingKind, HaltReason, InFlight, PassOutcome, ReconcileConfig,
    ReconcileContext, ReconcileState, Reconciler, Rewind, SlotWindow, StateStore, StoredBlock, Strike,
    classify_blocks,
};
use prism::slot_processor::SlotFetcherConfig;
use prism::storage::{ParquetWriter, Storage};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::{ChangeEvent, CorrectionReason};
use serde_json::json;
use solana_client::client_error::ClientError;
use solana_transaction_status::EncodedConfirmedBlock;
use tokio_util::sync::CancellationToken;

const CHAIN_ID: &str = "solana-fake";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-finality-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn window(start: u64, end: u64) -> SlotWindow {
    SlotWindow::new(start, end).unwrap()
}

fn hash(slot: u64) -> String {
    format!("hash-{slot}")
}

fn rpc_failure() -> PrismError {
    PrismError::rpc(None, "getLatestBlockhash", ClientError::from(std::io::Error::other("unreachable")))
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

fn stored(dir: &Path) -> BTreeSet<u64> {
    stored_slots(dir, window(0, u64::MAX - 1)).unwrap()
}

// ---------------------------------------------------------------------------
// A chain whose confirmed view can differ from its finalized one
// ---------------------------------------------------------------------------

/// Every slot up to `tip` has a block, except where a set says otherwise:
///
/// * `orphans`: a block at `confirmed` (the fork PRISM first saw) that the
///   finalized chain does not have;
/// * `late`: no block at `confirmed` (PRISM logs a skip), a block at
///   `finalized`;
/// * `duplicates`: at `confirmed` a block with another blockhash than the
///   finalized one, same parent.
///
/// A block's parent is the previous block of the same view.
struct Chain {
    tip: u64,
    orphans: Mutex<HashSet<u64>>,
    late: Mutex<HashSet<u64>>,
    duplicates: Mutex<HashMap<u64, String>>,
    /// `None`: reading the finalized tip fails (the poller learns nothing).
    finalized_tip: Mutex<Option<u64>>,
    block_delay: Duration,
    finalized_reads: Mutex<Vec<u64>>,
    header_reads: AtomicUsize,
}

impl Chain {
    fn new(tip: u64) -> Arc<Self> {
        Self::with_delay(tip, Duration::ZERO)
    }

    fn with_delay(tip: u64, block_delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            tip,
            orphans: Mutex::default(),
            late: Mutex::default(),
            duplicates: Mutex::default(),
            finalized_tip: Mutex::new(Some(tip)),
            block_delay,
            finalized_reads: Mutex::default(),
            header_reads: AtomicUsize::new(0),
        })
    }

    fn finalized(commitment: CommitmentLevel) -> bool {
        commitment == CommitmentLevel::Finalized
    }

    fn has_block(&self, slot: u64, commitment: CommitmentLevel) -> bool {
        if slot > self.tip {
            return false;
        }
        if Self::finalized(commitment) {
            !self.orphans.lock().unwrap().contains(&slot)
        } else {
            !self.late.lock().unwrap().contains(&slot)
        }
    }

    fn blockhash(&self, slot: u64, commitment: CommitmentLevel) -> String {
        if !Self::finalized(commitment)
            && let Some(dup) = self.duplicates.lock().unwrap().get(&slot)
        {
            return dup.clone();
        }
        hash(slot)
    }

    fn parent(&self, slot: u64, commitment: CommitmentLevel) -> u64 {
        (0..slot).rev().find(|s| self.has_block(*s, commitment)).unwrap_or(0)
    }

    fn finalized_reads(&self) -> Vec<u64> {
        self.finalized_reads.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChainAdapter for Chain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        CHAIN_ID
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        let slot = if Self::finalized(commitment) {
            self.finalized_tip.lock().unwrap().ok_or_else(rpc_failure)?
        } else {
            self.tip
        };
        Ok(ChainTip { slot_or_height: slot, block_id: self.blockhash(slot, commitment), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        self.get_block_at(slot, CommitmentLevel::Confirmed).await
    }

    async fn get_block_at(&self, slot: u64, commitment: CommitmentLevel) -> PrismResult<ChainBlock> {
        if !self.block_delay.is_zero() {
            tokio::time::sleep(self.block_delay).await;
        }
        if Self::finalized(commitment) {
            self.finalized_reads.lock().unwrap().push(slot);
        }
        if !self.has_block(slot, commitment) {
            return Err(PrismError::slot_skipped(slot));
        }
        let parent = self.parent(slot, commitment);
        let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
            "previousBlockhash": self.blockhash(parent, commitment),
            "blockhash": self.blockhash(slot, commitment),
            "parentSlot": parent,
            "transactions": [],
            "rewards": [],
            "blockTime": 1_700_000_000 + slot as i64,
            "blockHeight": slot,
        }))
        .unwrap();
        Ok(ChainBlock {
            slot_or_height: slot,
            block_id: raw.blockhash.clone(),
            parent_id: raw.previous_blockhash.clone(),
            timestamp: raw.block_time,
            raw: ChainBlockData::Solana(Box::new(raw)),
        })
    }

    async fn get_block_header(&self, slot: u64, commitment: CommitmentLevel) -> PrismResult<BlockHeader> {
        self.header_reads.fetch_add(1, Ordering::SeqCst);
        Ok(BlockHeader::of(&self.get_block_at(slot, commitment).await?))
    }

    async fn get_blocks_in_range(&self, from: u64, to: u64, commitment: CommitmentLevel) -> PrismResult<Vec<u64>> {
        Ok((from..=to.min(self.tip)).filter(|s| self.has_block(*s, commitment)).collect())
    }

    async fn genesis_hash(&self) -> PrismResult<String> {
        Ok("genesis".to_owned())
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
}

impl RecordingChangeLog {
    fn events(&self) -> Vec<ChangeEvent> {
        self.events.lock().unwrap().clone()
    }

    fn corrections(&self) -> Vec<ChangeEvent> {
        self.events().into_iter().filter(ChangeEvent::is_correction).collect()
    }

    fn finals(&self) -> Vec<ChangeEvent> {
        self.events().into_iter().filter(ChangeEvent::is_final).collect()
    }
}

impl ChangeLog for RecordingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        let mut events = self.events.lock().unwrap();
        events.push(event);
        Ok(events.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn config(dir: &Path) -> IngestorConfig {
    IngestorConfig {
        start_slot: Some(100),
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_millis(20),
        fetcher: SlotFetcherConfig { max_retries: 1, base_delay_ms: 1, max_delay_ms: 2, request_timeout_ms: 5_000 },
        ..IngestorConfig::new(dir)
    }
}

fn ingestor(
    chain: &Arc<Chain>,
    tracker: Arc<CommitmentTracker>,
    changelog: &Arc<RecordingChangeLog>,
    config: IngestorConfig,
) -> Arc<Ingestor> {
    Arc::new(
        Ingestor::new(
            Arc::clone(chain) as Arc<dyn ChainAdapter>,
            tracker,
            Arc::clone(changelog) as Arc<dyn ChangeLog>,
            config,
        )
        .unwrap(),
    )
}

fn spawn_run(ingestor: &Arc<Ingestor>) -> tokio::task::JoinHandle<PrismResult<prism::ingestor::IngestorReport>> {
    let ingestor = Arc::clone(ingestor);
    tokio::spawn(async move { ingestor.run().await })
}

fn reconciler(
    chain: &Arc<Chain>,
    ingestor: &Ingestor,
    changelog: &Arc<RecordingChangeLog>,
    dir: &Path,
    config: ReconcileConfig,
) -> Reconciler {
    Reconciler::new(
        config,
        Arc::clone(chain) as Arc<dyn ChainAdapter>,
        Arc::new(ingestor.reconcile_context("genesis")),
        Arc::new(ingestor.rewinder()),
        Arc::clone(changelog) as Arc<dyn ChangeLog>,
        dir,
    )
    .unwrap()
}

fn plain_tracker() -> (Arc<FinalityWatermark>, Arc<CommitmentTracker>) {
    let watermark = Arc::new(FinalityWatermark::new());
    (Arc::clone(&watermark), Arc::new(CommitmentTracker::from_watermark(watermark)))
}

/// Ingest 100..=`tip` once with a tracker that knows no finality, then shut
/// down. Returns the directory, as a first life of PRISM leaves it.
async fn first_life(chain: &Arc<Chain>, changelog: &Arc<RecordingChangeLog>, tip: u64) -> PathBuf {
    let dir = temp_dir();
    let (_, tracker) = plain_tracker();
    let ingestor = ingestor(chain, tracker, changelog, config(&dir));
    let run = spawn_run(&ingestor);
    wait_for("the first ingest", || ingestor.cursor().durable_flush_slot() >= tip).await;
    ingestor.shutdown();
    run.await.unwrap().unwrap();
    dir
}

// ---------------------------------------------------------------------------
// finality.json
// ---------------------------------------------------------------------------

#[test]
fn the_finality_record_round_trips_and_belongs_to_one_chain() {
    let dir = temp_dir();
    let store = FinalityStore::in_dir(&dir);
    assert_eq!(store.path(), dir.join(FINALITY_FILE_NAME));
    assert_eq!(store.load(CHAIN_ID).unwrap(), None, "no file yet");

    let record = FinalityRecord::new(CHAIN_ID, 500, "h500", 480);
    store.save(&record).unwrap();
    assert_eq!(store.load(CHAIN_ID).unwrap(), Some(record));
    assert_eq!(store.load("solana-mainnet").unwrap(), None, "another chain's watermark is not trusted");
    assert!(!dir.join("finality.json.tmp").exists(), "replaced through a temp file");

    // `announced_slot` is never above the watermark.
    assert_eq!(FinalityRecord::new(CHAIN_ID, 10, "h", 99).announced_slot, 10);
}

#[test]
fn a_corrupt_finality_record_is_quarantined_and_a_newer_one_refused() {
    let dir = temp_dir();
    let store = FinalityStore::in_dir(&dir);
    fs::write(store.path(), "{ not json").unwrap();
    assert_eq!(store.load(CHAIN_ID).unwrap(), None);
    assert!(!store.path().exists());
    assert!(fs::read_dir(&dir).unwrap().any(|e| e.unwrap().file_name().to_string_lossy().contains(".corrupt-")));

    // Breaking the invariant counts as corrupt too.
    let mut bad = FinalityRecord::new(CHAIN_ID, 10, "h", 10);
    bad.announced_slot = 11;
    fs::write(store.path(), serde_json::to_vec(&bad).unwrap()).unwrap();
    assert_eq!(store.load(CHAIN_ID).unwrap(), None);

    let mut newer = FinalityRecord::new(CHAIN_ID, 10, "h", 10);
    newer.version = 99;
    fs::write(store.path(), serde_json::to_vec(&newer).unwrap()).unwrap();
    assert!(matches!(store.load(CHAIN_ID), Err(PrismError::CursorError { .. })));
    assert!(store.path().exists(), "left for a newer build");
}

// ---------------------------------------------------------------------------
// The persistent tracker
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_persistent_tracker_saves_each_advance_and_resumes_announcing_after_a_restart() {
    let dir = temp_dir();
    let path = dir.join(FINALITY_FILE_NAME);
    let chain = Chain::new(1_000);
    *chain.finalized_tip.lock().unwrap() = Some(900);
    let changelog = Arc::new(RecordingChangeLog::default());

    let mut tracker = CommitmentTracker::spawn_persistent(
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        Duration::from_millis(5),
        &path,
    )
    .unwrap();
    assert!(tracker.is_persistent());
    wait_for("the first save", || tracker.durable_finalized_slot() == 900).await;
    assert_eq!(FinalityStore::new(&path).load(CHAIN_ID).unwrap().unwrap().announced_slot, 900);
    *chain.finalized_tip.lock().unwrap() = Some(950);
    wait_for("the second save", || tracker.durable_finalized_slot() == 950).await;
    tracker.shutdown().await;
    assert_eq!(changelog.finals(), vec![ChangeEvent::finalized(900, hash(900), 0), ChangeEvent::finalized(950, hash(950), 900)]);

    // Restart while the finalized tip cannot be read: the watermark is known
    // at once, from the file, and nothing is announced again.
    *chain.finalized_tip.lock().unwrap() = None;
    let mut restarted = CommitmentTracker::spawn_persistent(
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        Duration::from_millis(5),
        &path,
    )
    .unwrap();
    assert_eq!(restarted.finalized_slot(), 950);
    assert_eq!(restarted.durable_finalized_slot(), 950);
    assert!(restarted.is_finalized(950) && !restarted.is_finalized(951));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(changelog.finals().len(), 2, "no re-announcement from 0");

    // Once the chain answers again, announcing continues from 950.
    *chain.finalized_tip.lock().unwrap() = Some(990);
    wait_for("the next announcement", || changelog.finals().len() == 3).await;
    assert_eq!(changelog.finals()[2], ChangeEvent::finalized(990, hash(990), 950));
    restarted.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_announcement_lost_in_a_crash_is_made_after_the_restart() {
    // The record says 950 is finalized but only 900 was announced (a crash
    // between the advance and the save of the announcement).
    let dir = temp_dir();
    let path = dir.join(FINALITY_FILE_NAME);
    FinalityStore::new(&path).save(&FinalityRecord::new(CHAIN_ID, 950, hash(950), 900)).unwrap();
    let chain = Chain::new(1_000);
    *chain.finalized_tip.lock().unwrap() = Some(950);
    let changelog = Arc::new(RecordingChangeLog::default());
    let mut tracker = CommitmentTracker::spawn_persistent(
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::clone(&changelog) as Arc<dyn ChangeLog>,
        Duration::from_millis(5),
        &path,
    )
    .unwrap();
    wait_for("the catch-up announcement", || !changelog.finals().is_empty()).await;
    tracker.shutdown().await;
    assert_eq!(changelog.finals(), vec![ChangeEvent::finalized(950, hash(950), 900)]);
    assert_eq!(FinalityStore::new(&path).load(CHAIN_ID).unwrap().unwrap().announced_slot, 950);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_that_cannot_save_halts_and_keeps_its_durable_slot() {
    let dir = temp_dir();
    let path = dir.join(FINALITY_FILE_NAME);
    fs::create_dir_all(dir.join("finality.json.tmp")).unwrap(); // the temp file cannot be created
    let chain = Chain::new(1_000);
    let tracker = CommitmentTracker::spawn_persistent(
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::new(RecordingChangeLog::default()),
        Duration::from_millis(5),
        &path,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), tracker.halted()).await.expect("the poller halts");
    assert!(matches!(tracker.take_error(), Some(PrismError::CursorError { .. })));
    assert_eq!(tracker.finalized_slot(), 1_000, "advanced in memory");
    assert_eq!(tracker.durable_finalized_slot(), 0, "but never saved, so not durable");
}

#[tokio::test]
async fn the_reconciler_sees_only_durable_finality() {
    let dir = temp_dir();
    let chain = Chain::new(1_000);
    let changelog = Arc::new(RecordingChangeLog::default());

    // A tracker without a file has nothing to lose: durable is live.
    let (watermark, tracker) = plain_tracker();
    let _ = watermark.advance_finalized(700, &hash(700));
    let plain = ingestor(&chain, tracker, &changelog, config(&dir));
    assert_eq!(plain.reconcile_context("genesis").finalized_slot(), 700);

    // A persistent one: the context reads what is saved.
    FinalityStore::in_dir(&dir).save(&FinalityRecord::new(CHAIN_ID, 600, hash(600), 600)).unwrap();
    *chain.finalized_tip.lock().unwrap() = None;
    let tracker = Arc::new(
        CommitmentTracker::spawn_persistent(
            Arc::clone(&chain) as Arc<dyn ChainAdapter>,
            Arc::clone(&changelog) as Arc<dyn ChangeLog>,
            Duration::from_secs(3_600),
            dir.join(FINALITY_FILE_NAME),
        )
        .unwrap(),
    );
    let _ = tracker.watermark().advance_finalized(800, &hash(800)); // in memory only
    let persistent = ingestor(&chain, Arc::clone(&tracker), &changelog, config(&temp_dir()));
    assert_eq!(tracker.finalized_slot(), 800);
    assert_eq!(persistent.reconcile_context("genesis").finalized_slot(), 600);
}

// ---------------------------------------------------------------------------
// classify_blocks
// ---------------------------------------------------------------------------

fn rows(entries: &[(u64, u64)]) -> BTreeMap<u64, StoredBlock> {
    entries.iter().map(|&(slot, parent)| (slot, StoredBlock { blockhash: hash(slot), parent_slot: parent })).collect()
}

fn chain_of(produced: impl IntoIterator<Item = u64>, predecessor: Option<u64>) -> FinalizedChain {
    FinalizedChain { produced: produced.into_iter().collect(), predecessor }
}

#[test]
fn a_stored_block_with_another_parent_is_mismatched() {
    // Finalized: 10, 11, 12, 13. Stored 12 says its parent is 10.
    let c = classify_blocks(
        window(10, 13),
        &rows(&[(10, 9), (11, 10), (12, 10), (13, 12)]),
        &chain_of(10..=13, Some(9)),
        &BTreeSet::new(),
        &BTreeMap::new(),
    );
    assert_eq!(c.findings, vec![Finding { slot: 12, kind: FindingKind::Mismatched }]);
    assert_eq!(FindingKind::Mismatched.correction_reason(), Some(CorrectionReason::Mismatched));
    assert!(CorrectionReason::Orphaned > CorrectionReason::Mismatched);
    assert!(CorrectionReason::Mismatched > CorrectionReason::Missing);
}

#[test]
fn the_child_of_an_orphan_is_not_reported_twice() {
    // Finalized skips 11; PRISM stored 11 (orphan) and 12 on top of it.
    let c = classify_blocks(
        window(10, 13),
        &rows(&[(10, 9), (11, 10), (12, 11), (13, 12)]),
        &chain_of([10, 12, 13], Some(9)),
        &BTreeSet::new(),
        &BTreeMap::new(),
    );
    assert_eq!(c.findings, vec![Finding { slot: 11, kind: FindingKind::Orphaned }]);
}

#[test]
fn an_unknown_finalized_parent_is_not_evidence() {
    // Nothing found in the lookback: the first block's parent is unchecked,
    // the rest are.
    let c = classify_blocks(
        window(10, 12),
        &rows(&[(10, 3), (11, 10), (12, 10)]),
        &chain_of(10..=12, None),
        &BTreeSet::new(),
        &BTreeMap::new(),
    );
    assert_eq!(c.findings, vec![Finding { slot: 12, kind: FindingKind::Mismatched }]);
    // The predecessor from the lookback checks the first one too.
    let c = classify_blocks(window(10, 11), &rows(&[(10, 3), (11, 10)]), &chain_of(10..=11, Some(8)), &BTreeSet::new(), &BTreeMap::new());
    assert_eq!(c.findings, vec![Finding { slot: 10, kind: FindingKind::Mismatched }]);
}

#[test]
fn a_different_blockhash_is_mismatched_when_verified() {
    let stored = rows(&[(10, 9), (11, 10)]);
    let mut hashes = BTreeMap::from([(10, hash(10)), (11, "other".to_owned())]);
    let c = classify_blocks(window(10, 11), &stored, &chain_of(10..=11, Some(9)), &BTreeSet::new(), &hashes);
    assert_eq!(c.findings, vec![Finding { slot: 11, kind: FindingKind::Mismatched }]);
    hashes.insert(11, hash(11));
    assert!(classify_blocks(window(10, 11), &stored, &chain_of(10..=11, Some(9)), &BTreeSet::new(), &hashes).is_clean());
}

// ---------------------------------------------------------------------------
// End to end: the "same block?" checks
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_from_another_fork_at_a_finalized_slot_is_found_by_lineage_and_corrected() {
    // At confirmed, 129 was skipped and 130 built on 128; the finalized chain
    // has 129, and its 130 builds on 129.
    let chain = Chain::new(150);
    chain.late.lock().unwrap().insert(129);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_dir();
    let (watermark, tracker) = plain_tracker();
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let run = spawn_run(&ingestor);
    wait_for("the first ingest", || ingestor.cursor().durable_flush_slot() >= 150).await;
    assert_eq!(stored_blocks(&dir, window(130, 130)).unwrap()[&130].parent_slot, 128);
    let _ = watermark.advance_finalized(150, &hash(150));

    let mut reconciler = reconciler(&chain, &ingestor, &changelog, &dir, ReconcileConfig::default());
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    assert_eq!(
        reconciler.state().strikes,
        vec![
            Strike { slot: 129, kind: FindingKind::FalseSkip, seen: 1 },
            Strike { slot: 130, kind: FindingKind::Mismatched, seen: 1 },
        ]
    );
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Rewound { from_slot: 129, to_slot: 130 });
    assert_eq!(changelog.corrections(), vec![ChangeEvent::correction(129, 130, CorrectionReason::Mismatched, 2)]);

    wait_for("the re-ingest", || ingestor.cursor().durable_flush_slot() >= 150).await;
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Examined);
    assert_eq!(reconciler.state().reconciled_through, 150);
    assert!(reconciler.state().strikes.is_empty());
    let blocks = stored_blocks(&dir, window(128, 131)).unwrap();
    assert_eq!(blocks.keys().copied().collect::<Vec<_>>(), [128, 129, 130, 131]);
    assert_eq!(blocks[&130].parent_slot, 129, "the finalized block now");
    assert_eq!(reconciler.handle().stats().last_pass.unwrap().mismatched, 0);

    ingestor.shutdown();
    run.await.unwrap().unwrap();
}

/// Ingest a chain whose slot 140 PRISM first saw as a duplicate block (same
/// parent, other blockhash), make 170 finalized and reconcile it with
/// `verify_blockhashes`. Returns the directory, the chain and the outcomes
/// of three passes.
async fn reconcile_duplicate(verify_blockhashes: bool) -> (PathBuf, Arc<Chain>, Vec<PassOutcome>) {
    let chain = Chain::new(170);
    chain.duplicates.lock().unwrap().insert(140, "dup-140".to_owned());
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_dir();
    let (watermark, tracker) = plain_tracker();
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let run = spawn_run(&ingestor);
    wait_for("the first ingest", || ingestor.cursor().durable_flush_slot() >= 170).await;
    assert_eq!(stored_blocks(&dir, window(140, 140)).unwrap()[&140].blockhash, "dup-140");
    let _ = watermark.advance_finalized(170, &hash(170));

    let config = ReconcileConfig { verify_blockhashes, ..ReconcileConfig::default() };
    let mut reconciler = reconciler(&chain, &ingestor, &changelog, &dir, config);
    let mut outcomes = Vec::new();
    for _ in 0..3 {
        outcomes.push(reconciler.run_pass().await.unwrap());
        wait_for("the pipeline to catch up", || ingestor.cursor().durable_flush_slot() >= 170).await;
    }
    ingestor.shutdown();
    run.await.unwrap().unwrap();
    (dir, chain, outcomes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_duplicate_block_is_found_only_by_verifying_blockhashes() {
    // Without verification: same slot, same parent, so the free checks pass.
    let (dir, chain, outcomes) = reconcile_duplicate(false).await;
    assert_eq!(outcomes[0], PassOutcome::Examined);
    assert_eq!(chain.header_reads.load(Ordering::SeqCst), 0);
    assert_eq!(stored_blocks(&dir, window(140, 140)).unwrap()[&140].blockhash, "dup-140", "the documented blind spot");

    // With it: struck, confirmed, rewound, re-ingested at finalized.
    let (dir, chain, outcomes) = reconcile_duplicate(true).await;
    assert_eq!(outcomes[0], PassOutcome::Examined);
    assert_eq!(outcomes[1], PassOutcome::Rewound { from_slot: 140, to_slot: 140 });
    assert_eq!(outcomes[2], PassOutcome::Examined);
    assert!(chain.header_reads.load(Ordering::SeqCst) >= 71, "one header per stored block");
    assert_eq!(stored_blocks(&dir, window(140, 140)).unwrap()[&140].blockhash, hash(140));
}

// ---------------------------------------------------------------------------
// End to end: ordering of the correction
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_of_the_abandoned_data_is_announced_after_its_correction() {
    // The pipeline keeps flushing small batches while the rewind happens.
    let chain = Chain::with_delay(3_000, Duration::from_micros(500));
    chain.orphans.lock().unwrap().insert(120);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_dir();
    let (watermark, tracker) = plain_tracker();
    let busy = IngestorConfig { flush_max_slots: 3, flush_interval: Duration::from_millis(2), ..config(&dir) };
    let ingestor = ingestor(&chain, tracker, &changelog, busy);
    let run = spawn_run(&ingestor);
    wait_for("the orphan to be flushed", || ingestor.cursor().durable_flush_slot() >= 150).await;
    let _ = watermark.advance_finalized(150, &hash(150));

    let mut reconciler = reconciler(&chain, &ingestor, &changelog, &dir, ReconcileConfig::default());
    reconciler.run_pass().await.unwrap();
    let ahead = ingestor.cursor().durable_flush_slot();
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Rewound { from_slot: 120, to_slot: 120 });
    wait_for("the re-ingest to pass where it was", || ingestor.cursor().durable_flush_slot() > ahead + 50).await;
    ingestor.shutdown();
    run.await.unwrap().unwrap();

    let events = changelog.events();
    let at = events.iter().position(ChangeEvent::is_correction).unwrap();
    // After the correction, every slot at or above 120 is announced in
    // strictly increasing order from 120 on: only the re-ingest speaks.
    let after: Vec<u64> = events[at + 1..]
        .iter()
        .filter(|e| e.is_new() || e.is_skip())
        .map(ChangeEvent::slot)
        .filter(|s| *s >= 120)
        .collect();
    assert_eq!(after.first(), Some(&120), "the re-ingest starts at the rewind point");
    assert!(after.windows(2).all(|w| w[0] < w[1]), "a stale announcement slipped in: {after:?}");
    assert!(matches!(events[at + 1..].iter().find(|e| e.slot() == 120), Some(ChangeEvent::Skip { .. })));
    // And every file they point at is there.
    for event in &events[at + 1..] {
        for path in event.parquet_paths() {
            assert!(Path::new(path).exists(), "{path} announced after the correction but deleted");
        }
    }
    assert!(!stored(&dir).contains(&120));
}

// ---------------------------------------------------------------------------
// End to end: crashes
// ---------------------------------------------------------------------------

/// The state the reconciler saves right before announcing a rewind of 120.
fn in_flight_state() -> ReconcileState {
    ReconcileState {
        genesis_hash: Some("genesis".to_owned()),
        reconciled_through: 119,
        strikes: vec![Strike { slot: 120, kind: FindingKind::Orphaned, seen: 2 }],
        in_flight: Some(InFlight {
            from_slot: 120,
            to_slot: 120,
            reason: CorrectionReason::Orphaned,
            affected: 1,
            slots: vec![120],
            started_at: 1_800_000_000,
        }),
        updated_at: 1_800_000_000,
        ..ReconcileState::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_deciding_a_rewind_is_finished_once_on_restart_and_read_at_finalized() {
    let chain = Chain::new(150);
    chain.orphans.lock().unwrap().insert(120);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = first_life(&chain, &changelog, 150).await;
    assert!(stored(&dir).contains(&120), "the orphan was stored");

    // The crash: `in_flight` saved, the correction never emitted, and 150
    // known finalized. After the restart the finalized tip cannot be read,
    // so only the saved watermark knows about finality.
    StateStore::new(&dir).save(&in_flight_state()).unwrap();
    FinalityStore::in_dir(&dir).save(&FinalityRecord::new(CHAIN_ID, 150, hash(150), 150)).unwrap();
    *chain.finalized_tip.lock().unwrap() = None;
    let before = changelog.events().len();

    // The restart, wired as in production: tracker, ingestor and reconciler
    // all live at once, racing to finish the rewind.
    let tracker = Arc::new(
        CommitmentTracker::spawn_persistent(
            Arc::clone(&chain) as Arc<dyn ChainAdapter>,
            Arc::clone(&changelog) as Arc<dyn ChangeLog>,
            Duration::from_millis(5),
            dir.join(FINALITY_FILE_NAME),
        )
        .unwrap(),
    );
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let reconciler = reconciler(
        &chain,
        &ingestor,
        &changelog,
        &dir,
        ReconcileConfig { interval: Duration::from_millis(5), ..ReconcileConfig::default() },
    );
    let handle = reconciler.handle();
    let stop = CancellationToken::new();
    let reconciling = tokio::spawn(reconciler.run(stop.clone()));
    let run = spawn_run(&ingestor);

    wait_for("reconciliation through 150", || handle.state().reconciled_through == 150).await;
    stop.cancel();
    reconciling.await.unwrap().unwrap();
    ingestor.shutdown();
    run.await.unwrap().unwrap();

    let state = StateStore::new(&dir).load().unwrap().unwrap();
    assert_eq!(state.in_flight, None);
    // Counted once: recovery recorded the rewind, the reconciler adopted that
    // record instead of completing it again (which would add a second
    // rewind time). Its attempt was dropped as 120 got reconciled.
    assert_eq!(state.rewinds.len(), 1, "the rewind is counted once");
    assert!(state.attempts.is_empty());
    assert!(state.strikes.is_empty() && state.unresolved.is_empty());
    // Announced (at least once) before anything of the re-ingest.
    let events = &changelog.events()[before..];
    let first_correction = events.iter().position(ChangeEvent::is_correction).expect("announced");
    assert_eq!(events[first_correction], ChangeEvent::correction(120, 120, CorrectionReason::Orphaned, 1));
    assert!(events[..first_correction].iter().all(|e| !e.is_new() && !e.is_skip()));
    // Re-ingested at finalized with no poll ever succeeding: the orphan is gone.
    assert!(chain.finalized_reads().contains(&120));
    assert_eq!(stored(&dir), (100..=150).filter(|s| *s != 120).collect());
    let skip_log = fs::read_to_string(dir.join(SKIP_LOG_FILE_NAME)).unwrap();
    assert!(skip_log.contains(",120,validator_skipped"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reconciler_parked_on_an_interrupted_rewind_adopts_what_recovery_recorded() {
    // The interleaving that used to count a rewind twice, forced: the
    // reconciler's pass loads `in_flight` and waits for the ingestor; the
    // ingestor starts, recovery completes the rewind in the file, then the
    // epoch serves the reconciler's request.
    let chain = Chain::new(150);
    chain.orphans.lock().unwrap().insert(120);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = first_life(&chain, &changelog, 150).await;
    StateStore::new(&dir).save(&in_flight_state()).unwrap();

    let (watermark, tracker) = plain_tracker();
    let _ = watermark.advance_finalized(150, &hash(150));
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let mut reconciler = reconciler(&chain, &ingestor, &changelog, &dir, ReconcileConfig::default());
    let handle = reconciler.handle();
    let pass = tokio::spawn(async move {
        let outcome = reconciler.run_pass().await;
        (reconciler, outcome)
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!pass.is_finished(), "parked on the rewind request");
    assert!(handle.state().in_flight.is_some());

    let run = spawn_run(&ingestor);
    let (reconciler, outcome) = pass.await.unwrap();
    outcome.unwrap();
    let state = StateStore::new(&dir).load().unwrap().unwrap();
    assert_eq!(state.in_flight, None);
    assert_eq!(state.rewinds.len(), 1, "recovery's record adopted, not completed again");
    assert!(state.attempts.iter().all(|a| a.count == 1), "{:?}", state.attempts);
    assert_eq!(*reconciler.state(), state, "the reconciler's copy is the file's");
    ingestor.shutdown();
    run.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_saved_watermark_a_restart_would_read_the_orphan_again() {
    // The contrast to the test above: the same crash with a tracker that
    // keeps nothing. Before its first poll every slot reads as unfinalized,
    // the re-ingest reads 120 at confirmed and stores the orphan again
    // (the reconciler would have to correct it a second time).
    let chain = Chain::new(150);
    chain.orphans.lock().unwrap().insert(120);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = first_life(&chain, &changelog, 150).await;
    StateStore::new(&dir).save(&in_flight_state()).unwrap();

    let (_, tracker) = plain_tracker();
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let run = spawn_run(&ingestor);
    wait_for("the re-ingest", || {
        let events = changelog.events();
        events
            .iter()
            .position(ChangeEvent::is_correction)
            .is_some_and(|at| events[at..].iter().any(|e| e.is_new() && e.slot() == 150))
    })
    .await;
    ingestor.shutdown();
    run.await.unwrap().unwrap();
    assert!(!chain.finalized_reads().contains(&120));
    assert!(stored(&dir).contains(&120));
}

#[test]
fn a_crash_at_any_step_of_the_rewind_is_finished_the_same_way() {
    // Files 100..=125 and 126..=150, cursor flushed to 150.
    let setup = || {
        let dir = temp_dir();
        let mut writer = ParquetWriter::new(&dir);
        for batch in [100..=125u64, 126..=150] {
            for slot in batch {
                let row = prism::types::udm::DeepAnaSlot {
                    slot,
                    blockhash: hash(slot),
                    parent_slot: slot - 1,
                    ..Default::default()
                };
                writer.append(&row, &[], &[], &[]).unwrap();
            }
            writer.flush().unwrap();
        }
        let cursor = IngestorCursor::load_or_init(dir.join(CURSOR_FILE_NAME)).unwrap();
        cursor.record_processed(100, "h");
        cursor.record_processed(150, "h");
        cursor.record_flush(150).unwrap();
        StateStore::new(&dir).save(&in_flight_state()).unwrap();
        (dir, cursor)
    };
    let expected: BTreeSet<u64> = (100..=119).collect();
    let finished = |dir: &Path, cursor: &IngestorCursor| {
        let state = StateStore::new(dir).load().unwrap().unwrap();
        assert_eq!(state.in_flight, None);
        assert_eq!(state.attempts, vec![Attempt { slot: 120, count: 1 }]);
        assert_eq!(cursor.durable_flush_slot(), 119);
        assert_eq!(cursor.resume_slot(), Some(120));
        assert_eq!(stored(dir), expected);
    };

    // 1. Crash before anything was announced or changed.
    let (dir, cursor) = setup();
    let log = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &log).unwrap(), Some(120));
    finished(&dir, &cursor);
    assert_eq!(log.corrections().len(), 1);

    // 2. Crash after the cursor was lowered, before the files went.
    let (dir, cursor) = setup();
    cursor.rewind(120).unwrap();
    let log = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &log).unwrap(), Some(120));
    finished(&dir, &cursor);

    // 3. Crash after the files went, before the rewind was recorded (the
    //    truncated file 100..=119 is already in place).
    let (dir, cursor) = setup();
    cursor.rewind(120).unwrap();
    ParquetWriter::new(&dir).rollback(120).unwrap();
    let log = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &log).unwrap(), Some(120));
    finished(&dir, &cursor);
    assert_eq!(log.corrections().len(), 1, "announced again: at least once");

    // 4. A recovery that itself crashed halfway is finished by the next one.
    let (dir, cursor) = setup();
    let failing = FailingChangeLog;
    assert!(Reconciler::recover(&dir, &cursor, &failing).is_err());
    assert_eq!(stored(&dir), (100..=150).collect(), "nothing deleted without the announcement");
    assert!(StateStore::new(&dir).load().unwrap().unwrap().in_flight.is_some());
    let log = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &log).unwrap(), Some(120));
    finished(&dir, &cursor);

    // Done is done.
    let log = RecordingChangeLog::default();
    assert_eq!(Reconciler::recover(&dir, &cursor, &log).unwrap(), None);
    assert!(log.events().is_empty());
}

struct FailingChangeLog;

impl ChangeLog for FailingChangeLog {
    fn emit(&self, _: ChangeEvent) -> PrismResult<u64> {
        Err(PrismError::changelog("changelog.jsonl", "append event", "disk full"))
    }
    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Rewind::correct and the state file as the source of truth
// ---------------------------------------------------------------------------

/// A rewinder that records the order of announcement and rewind.
#[derive(Default)]
struct OrderRewinder {
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Rewind for OrderRewinder {
    async fn rewind_to(&self, from_slot: u64) -> PrismResult<()> {
        self.log.lock().unwrap().push(format!("rewind {from_slot}"));
        Ok(())
    }
}

struct OrderChangeLog {
    log: Arc<Mutex<Vec<String>>>,
}

impl ChangeLog for OrderChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        self.log.lock().unwrap().push(event.kind().to_owned());
        Ok(1)
    }
    fn flush(&self) -> PrismResult<()> {
        self.log.lock().unwrap().push("flush".to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn the_default_correct_announces_durably_before_rewinding() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let rewinder = OrderRewinder { log: Arc::clone(&log) };
    let changelog = OrderChangeLog { log: Arc::clone(&log) };
    let in_flight = in_flight_state().in_flight.unwrap();
    rewinder.correct(&in_flight, &changelog).await.unwrap();
    assert_eq!(*log.lock().unwrap(), ["correction", "flush", "rewind 120"]);
    assert_eq!(in_flight.event(), ChangeEvent::correction(120, 120, CorrectionReason::Orphaned, 1));
}

#[tokio::test]
async fn an_operator_edit_of_the_state_file_takes_effect_at_the_next_pass() {
    let chain = Chain::new(1_000);
    let changelog = Arc::new(RecordingChangeLog::default());
    let dir = temp_dir();
    let (_, tracker) = plain_tracker();
    let ingestor = ingestor(&chain, tracker, &changelog, config(&dir));
    let mut halted = in_flight_state();
    halted.in_flight = None;
    halted.strikes.clear();
    halted.halted = Some(HaltReason::MassMismatch);
    StateStore::new(&dir).save(&halted).unwrap();

    let mut reconciler = reconciler(&chain, &ingestor, &changelog, &dir, ReconcileConfig::default());
    assert_eq!(reconciler.run_pass().await.unwrap(), PassOutcome::Halted(HaltReason::MassMismatch));
    // The operator clears `halted` in the file; no restart needed.
    halted.halted = None;
    StateStore::new(&dir).save(&halted).unwrap();
    assert_ne!(reconciler.run_pass().await.unwrap(), PassOutcome::Halted(HaltReason::MassMismatch));
    assert_eq!(reconciler.handle().state().halted, None);
}
