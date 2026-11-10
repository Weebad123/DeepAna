//! Anomaly detection: the runner that schedules the detectors.
//!
//! | File           | Holds                                                    |
//! |----------------|----------------------------------------------------------|
//! | `baseline.rs`  | median/MAD scoring, pure functions                        |
//! | `detectors.rs` | the [`Detector`] trait and the detector set               |
//! | `mod.rs`       | [`Runner`]: schedule, watermark, per-detector progress, status |
//!
//! # One tick, every `detect_interval_secs`
//!
//! 1. Gather the facts with four queries, in this order (see [`LensFacts`]):
//!    `MAX(slot)` of `solana.slots`, the newest slot of `solana.slots` with
//!    `transaction_count > 0`, `MAX(slot)` of `solana.transactions`, and
//!    `MIN(slot)` of `solana.slots`.
//! 2. The watermark `M` is the newest slot both tables cover. PRISM writes a
//!    slot's rows together, but LENS may have picked up one table's newest
//!    file before the other's. `solana.slots` says which slots carry
//!    transactions, so once `solana.transactions` holds the newest of them,
//!    it is caught up and `M` is `MAX(slot)` of `solana.slots`, empty
//!    trailing slots included. Until then `M` is `MAX(slot)` of
//!    `solana.transactions`. A bucket is complete when its last slot is at
//!    most `M`. `complete_max = (M + 1) / bucket_slots - 1` is the newest
//!    complete bucket. `first_complete_bucket = ceil(MIN(slot) /
//!    bucket_slots)` is the first one ingestion covered from its first slot,
//!    because the bucket ingestion started in is partial. If no bucket is
//!    complete, the tick ends.
//! 3. Each enabled detector takes its turn, in the fixed order of
//!    [`default_detectors`]. One detector's failure never stops another.
//!
//! # One detector's turn
//!
//! Read `P = store.progress(id)`, then:
//!
//! * **No progress**: commit no anomalies with progress `complete_max`. The
//!   first run baselines from now and reports nothing; otherwise a fresh
//!   database would flag every historical wobble (FORGE's rule for new
//!   alerts).
//! * **`P > complete_max`**: LENS's data is behind the stored progress,
//!   usually because the ledger was reset. Nothing is scanned. The status
//!   says so and names the remedy, [`RunnerHandle::reset_progress`].
//! * **`P < complete_max`**: scan `P + 1 ..` in chunks of at most the
//!   detector's chunk size, committing each chunk's anomalies together with
//!   its last bucket. A chunk that finds nothing still commits, so progress
//!   moves past empty buckets. At most [`MAX_CHUNKS_PER_TURN`] chunks per
//!   turn, so a long catch-up cannot starve the detectors after it.
//!
//! A truncated LENS result is never scored. Progress stays, the chunk size
//! is halved, and the chunk is retried. A chunk of one bucket that is still
//! truncated means the history alone exceeds LENS's row limit: the detector
//! is marked failing with a message to lower `ORACLE_BASELINE_BUCKETS` (or
//! raise `LENS_MAX_QUERY_ROWS`).
//!
//! # Exactly once
//!
//! Each chunk's anomalies and its progress commit in one store transaction,
//! and the store judges each commit against the stored progress (see
//! `store/`). A crash before the commit rescans the chunk. A crash after it
//! resumes from the next bucket. A turn and a
//! [`RunnerHandle::reset_progress`] for the same detector never overlap: both
//! hold the detector's lock.
//!
//! # Shutdown
//!
//! The [`CancellationToken`] is checked between detectors, between chunks,
//! and around every LENS call: a pending query is dropped at once. A commit is
//! never raced against it, so it either happens or doesn't. A cancelled turn
//! leaves progress where it was, and the next start rescans the same buckets.
//!
//! # State that lives in memory
//!
//! Per-detector enable state, chunk sizes and status are not persisted:
//! every detector starts enabled after a restart. Progress is in the store.

pub mod baseline;
pub mod detectors;

use std::sync::{Arc, Mutex, PoisonError};

use forge::lens_client::{LensApi, LensResponse};
use serde::Serialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::config::{MIN_BASELINE_BUCKETS, OracleConfig, OracleError, OracleResult};
use crate::store::{AnomalyKind, Store, unix_now_ms};
pub use baseline::{Direction, NoVerdict, ScoreParams, Verdict};
pub use detectors::{Detector, ScanContext, ScanError, ScanOutput, default_detectors};

/// Buckets one chunk scores before any truncation has halved it.
pub const DEFAULT_CHUNK_BUCKETS: u64 = 32;
/// Most chunks one detector scans per turn.
pub const MAX_CHUNKS_PER_TURN: u32 = 16;

pub const MAX_SLOTS_SQL: &str = "SELECT MAX(slot) AS max_slot FROM solana.slots";
/// The newest slot that should have rows in `solana.transactions`. PRISM sets
/// `transaction_count` from the same normalized block it writes the
/// transaction rows from, so the two always agree.
pub const LAST_TX_SLOT_SQL: &str = "SELECT MAX(slot) AS max_slot FROM solana.slots WHERE transaction_count > 0";
pub const MAX_TX_SLOT_SQL: &str = "SELECT MAX(slot) AS max_slot FROM solana.transactions";
pub const MIN_SLOTS_SQL: &str = "SELECT MIN(slot) AS min_slot FROM solana.slots";

// ---------------------------------------------------------------------------
// Watermark
// ---------------------------------------------------------------------------

/// What the watermark queries returned; `None` is an empty result.
///
/// The runner reads them in field order. `last_tx_slot` must be read no
/// earlier than `max_slot`, so that it judges every slot up to `max_slot`,
/// and `max_tx_slot` no earlier than `last_tx_slot`. LENS only ever adds
/// files between two reads, so a later read can only see more.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LensFacts {
    /// [`MAX_SLOTS_SQL`].
    pub max_slot: Option<u64>,
    /// [`LAST_TX_SLOT_SQL`]: `None` when no slot has transactions yet.
    pub last_tx_slot: Option<u64>,
    /// [`MAX_TX_SLOT_SQL`].
    pub max_tx_slot: Option<u64>,
    /// [`MIN_SLOTS_SQL`].
    pub min_slot: Option<u64>,
}

impl LensFacts {
    /// The newest slot both `solana.slots` and `solana.transactions` cover.
    ///
    /// Once `solana.transactions` holds `last_tx_slot`, no slot after it
    /// should have transaction rows, so the transactions table covers all of
    /// `solana.slots`. Until then it covers only up to its own `MAX(slot)`.
    pub fn covered_slot(&self) -> Option<u64> {
        let max_slot = self.max_slot?;
        let caught_up = match (self.last_tx_slot, self.max_tx_slot) {
            (None, _) => true,
            (Some(expected), Some(seen)) => seen >= expected,
            (Some(_), None) => false,
        };
        if caught_up { Some(max_slot) } else { Some(max_slot.min(self.max_tx_slot?)) }
    }
}

/// What one tick learned about LENS's data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Watermark {
    /// The newest slot both tables cover ([`LensFacts::covered_slot`]).
    pub max_slot: u64,
    pub min_slot: u64,
    /// Newest complete bucket.
    pub complete_max: u64,
    /// First bucket ingestion covered from its first slot.
    pub first_complete_bucket: u64,
}

impl Watermark {
    /// The watermark for these facts, or `None` when no bucket is complete
    /// (or a table is empty).
    pub fn compute(facts: &LensFacts, bucket_slots: u64) -> Option<Self> {
        let max_slot = facts.covered_slot()?;
        let min_slot = facts.min_slot?;
        if bucket_slots == 0 || min_slot > max_slot {
            return None;
        }
        let complete_max = (max_slot.checked_add(1)? / bucket_slots).checked_sub(1)?;
        let first_complete_bucket = min_slot.div_ceil(bucket_slots);
        (first_complete_bucket <= complete_max).then_some(Self {
            max_slot,
            min_slot,
            complete_max,
            first_complete_bucket,
        })
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Where a detector stands after its last turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "state", content = "message", rename_all = "snake_case")]
pub enum DetectorState {
    /// No turn yet.
    Pending,
    /// Turned off through [`RunnerHandle::set_enabled`].
    Disabled,
    /// First run: progress set to the newest complete bucket, nothing scored.
    Baselined,
    /// Progress is at the newest complete bucket.
    UpToDate,
    /// Scanned its chunk budget and has more to do next tick.
    CatchingUp,
    /// LENS has no table to answer from yet.
    NotReady(String),
    /// LENS timed out or was unreachable; retried next tick.
    Retrying(String),
    /// Stored progress is beyond LENS's data (a ledger reset?).
    AheadOfData(String),
    /// A permanent failure; retried every tick, but will not fix itself.
    Failing(String),
}

/// One detector, for `/detectors`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DetectorStatus {
    pub id: String,
    pub kind: AnomalyKind,
    pub enabled: bool,
    #[serde(flatten)]
    pub state: DetectorState,
    /// Last committed progress seen by the runner.
    pub progress: Option<u64>,
    pub chunk_buckets: u64,
    /// When its last turn ended, unix ms.
    pub last_turn_at: Option<i64>,
    /// Anomalies newly stored since this process started.
    pub anomalies_inserted: u64,
}

/// The whole runner, for `/detectors` and `/health`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunnerStatus {
    pub last_tick_at: Option<i64>,
    pub watermark: Option<Watermark>,
    /// Why the last tick scanned nothing, if it did not.
    pub tick_error: Option<String>,
    pub detectors: Vec<DetectorStatus>,
}

struct Slot {
    detector: Arc<dyn Detector>,
    /// Serialises a turn with `reset_progress`.
    turn: tokio::sync::Mutex<()>,
    status: Mutex<DetectorStatus>,
}

struct Shared {
    store: Store,
    lens: Arc<dyn LensApi>,
    bucket_slots: u64,
    history_buckets: u64,
    params: ScoreParams,
    detect_interval: std::time::Duration,
    slots: Vec<Slot>,
    tick: Mutex<(Option<i64>, Option<Watermark>, Option<String>)>,
}

impl Shared {
    fn slot(&self, id: &str) -> Option<&Slot> {
        self.slots.iter().find(|slot| slot.detector.id() == id)
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Runs the detectors every `detect_interval_secs` until shutdown.
pub struct Runner {
    shared: Arc<Shared>,
    shutdown: CancellationToken,
}

/// A cloneable view of a [`Runner`] for the API: status, enable/disable,
/// and resetting progress.
#[derive(Clone)]
pub struct RunnerHandle {
    shared: Arc<Shared>,
}

impl Runner {
    /// A runner over [`default_detectors`].
    pub fn new(store: Store, lens: Arc<dyn LensApi>, config: Arc<OracleConfig>, shutdown: CancellationToken) -> Self {
        Self::with_detectors(store, lens, &config, shutdown, default_detectors())
    }

    /// A runner over `detectors`, visited in the given order. Ids must be
    /// unique.
    pub fn with_detectors(
        store: Store,
        lens: Arc<dyn LensApi>,
        config: &OracleConfig,
        shutdown: CancellationToken,
        detectors: Vec<Arc<dyn Detector>>,
    ) -> Self {
        let history_buckets = u64::from(config.baseline_buckets());
        let params = ScoreParams {
            threshold: config.anomaly_threshold(),
            min_history: min_history(history_buckets),
            // Replaced by each detector's own.
            min_scale: 1.0,
            rel_floor: 0.0,
            direction: Direction::Both,
        };
        let slots = detectors
            .into_iter()
            .map(|detector| Slot {
                status: Mutex::new(DetectorStatus {
                    id: detector.id().to_owned(),
                    kind: detector.kind(),
                    enabled: true,
                    state: DetectorState::Pending,
                    progress: None,
                    chunk_buckets: DEFAULT_CHUNK_BUCKETS,
                    last_turn_at: None,
                    anomalies_inserted: 0,
                }),
                turn: tokio::sync::Mutex::new(()),
                detector,
            })
            .collect::<Vec<_>>();
        let mut ids: Vec<&str> = slots.iter().map(|s| s.detector.id()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), slots.len(), "detector ids must be unique");

        Self {
            shared: Arc::new(Shared {
                store,
                lens,
                bucket_slots: detectors::bucket_slots(config.bucket_secs()),
                history_buckets,
                params,
                detect_interval: config.detect_interval(),
                slots,
                tick: Mutex::new((None, None, None)),
            }),
            shutdown,
        }
    }

    pub fn handle(&self) -> RunnerHandle {
        RunnerHandle { shared: Arc::clone(&self.shared) }
    }

    /// Run a tick now, then one every `detect_interval_secs`, until shutdown.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!(
                detectors = self.shared.slots.len(),
                bucket_slots = self.shared.bucket_slots,
                interval_secs = self.shared.detect_interval.as_secs(),
                "anomaly detection started"
            );
            loop {
                self.run_once().await;
                tokio::select! {
                    () = self.shutdown.cancelled() => break,
                    () = tokio::time::sleep(self.shared.detect_interval) => {}
                }
            }
            info!("anomaly detection stopped");
        })
    }

    /// One tick (see the module docs). [`Runner::spawn`] calls this on its
    /// schedule; tests call it directly.
    pub async fn run_once(&self) {
        let shared = &self.shared;
        let watermark = match self.facts().await {
            None => return, // cancelled
            Some(Ok(Some(watermark))) => watermark,
            Some(Ok(None)) => {
                debug!("no complete bucket yet");
                *lock(&shared.tick) = (Some(unix_now_ms()), None, Some("no complete bucket yet".into()));
                return;
            }
            Some(Err(err)) => {
                warn!(error = %err, "detection tick could not read LENS's watermark");
                *lock(&shared.tick) = (Some(unix_now_ms()), None, Some(err.to_string()));
                return;
            }
        };
        *lock(&shared.tick) = (Some(unix_now_ms()), Some(watermark), None);

        for slot in &shared.slots {
            if self.shutdown.is_cancelled() {
                return;
            }
            if !lock(&slot.status).enabled {
                continue;
            }
            let _turn = slot.turn.lock().await;
            // `enabled` may have flipped while waiting for the lock.
            if !lock(&slot.status).enabled {
                continue;
            }
            self.turn(slot, &watermark).await;
        }
    }

    /// The four watermark queries, in [`LensFacts`]'s order. `None` when
    /// cancelled.
    async fn facts(&self) -> Option<Result<Option<Watermark>, ScanError>> {
        let mut values = [None; 4];
        for (i, (sql, column)) in [
            (MAX_SLOTS_SQL, "max_slot"),
            (LAST_TX_SLOT_SQL, "max_slot"),
            (MAX_TX_SLOT_SQL, "max_slot"),
            (MIN_SLOTS_SQL, "min_slot"),
        ]
        .into_iter()
        .enumerate()
        {
            let response = tokio::select! {
                () = self.shutdown.cancelled() => return None,
                response = self.shared.lens.query(sql) => response,
            };
            match response.map_err(ScanError::from).and_then(|r| single_slot(&r, column)) {
                Ok(value) => values[i] = value,
                Err(err) => return Some(Err(err)),
            }
        }
        let [max_slot, last_tx_slot, max_tx_slot, min_slot] = values;
        let facts = LensFacts { max_slot, last_tx_slot, max_tx_slot, min_slot };
        Some(Ok(Watermark::compute(&facts, self.shared.bucket_slots)))
    }

    /// One detector's turn. The caller holds `slot.turn`.
    async fn turn(&self, slot: &Slot, watermark: &Watermark) {
        let state = self.scan_until_done(slot, watermark).await;
        let mut status = lock(&slot.status);
        if let Some(state) = state {
            match &state {
                DetectorState::Failing(message) => warn!(detector = %status.id, message, "detector failing"),
                DetectorState::Retrying(message) | DetectorState::NotReady(message) => {
                    debug!(detector = %status.id, message, "detector will retry")
                }
                _ => {}
            }
            status.state = state;
        }
        status.last_turn_at = Some(unix_now_ms());
    }

    /// The turn's work. Returns the new state, `None` when cancelled.
    async fn scan_until_done(&self, slot: &Slot, wm: &Watermark) -> Option<DetectorState> {
        let shared = &self.shared;
        let id = slot.detector.id().to_owned();
        let progress = match self.store_call({
            let id = id.clone();
            move |store| store.progress(&id)
        })
        .await
        {
            Ok(progress) => progress,
            Err(err) => return Some(DetectorState::Failing(err.to_string())),
        };
        lock(&slot.status).progress = progress;

        let Some(mut progress) = progress else {
            return Some(match self.commit(slot, Vec::new(), wm.complete_max).await {
                Ok(()) => {
                    info!(detector = %id, bucket = wm.complete_max, "detector baselined from now");
                    DetectorState::Baselined
                }
                Err(state) => state,
            });
        };
        if progress > wm.complete_max {
            return Some(DetectorState::AheadOfData(format!(
                "stored progress {progress} is beyond LENS's newest complete bucket {}; if the ledger was reset, \
                 reset this detector's progress",
                wm.complete_max
            )));
        }

        let mut chunks = 0;
        while progress < wm.complete_max {
            if chunks == MAX_CHUNKS_PER_TURN {
                return Some(DetectorState::CatchingUp);
            }
            if self.shutdown.is_cancelled() {
                return None;
            }
            let chunk = lock(&slot.status).chunk_buckets;
            let from = progress + 1;
            let to = from.saturating_add(chunk - 1).min(wm.complete_max);
            let scored_from = from.max(wm.first_complete_bucket);

            let anomalies = if scored_from > to {
                // Every bucket here predates the data LENS holds.
                Vec::new()
            } else {
                let ctx = ScanContext {
                    lens: Arc::clone(&shared.lens),
                    bucket_slots: shared.bucket_slots,
                    history_buckets: shared.history_buckets,
                    params: shared.params,
                    from_bucket: scored_from,
                    to_bucket: to,
                    first_complete_bucket: wm.first_complete_bucket,
                };
                let result = tokio::select! {
                    () = self.shutdown.cancelled() => return None,
                    result = slot.detector.scan(&ctx) => result,
                };
                match result {
                    Ok(output) => output.anomalies,
                    Err(ScanError::Truncated) if chunk > 1 => {
                        let halved = chunk / 2;
                        debug!(detector = %id, chunk, halved, "truncated LENS result; halving the chunk");
                        lock(&slot.status).chunk_buckets = halved;
                        continue;
                    }
                    Err(ScanError::Truncated) => {
                        return Some(DetectorState::Failing(format!(
                            "LENS truncates even a one-bucket scan: the history alone exceeds its row limit; lower \
                             ORACLE_BASELINE_BUCKETS (now {}) or raise LENS_MAX_QUERY_ROWS",
                            shared.history_buckets
                        )));
                    }
                    Err(ScanError::NotReady(message)) => return Some(DetectorState::NotReady(message)),
                    Err(ScanError::Transient(message)) => return Some(DetectorState::Retrying(message)),
                    Err(ScanError::Permanent(message)) => return Some(DetectorState::Failing(message)),
                }
            };
            if let Err(state) = self.commit(slot, anomalies, to).await {
                return Some(state);
            }
            progress = to;
            chunks += 1;
        }
        Some(DetectorState::UpToDate)
    }

    /// Commit a chunk and record it in the status.
    async fn commit(&self, slot: &Slot, anomalies: Vec<crate::store::NewAnomaly>, to: u64) -> Result<(), DetectorState> {
        let id = slot.detector.id().to_owned();
        let result = self
            .store_call({
                let id = id.clone();
                move |store| store.commit_scan(&id, &anomalies, to)
            })
            .await;
        match result {
            Ok(commit) => {
                let mut status = lock(&slot.status);
                status.progress = Some(to);
                status.anomalies_inserted += commit.inserted as u64;
                if commit.inserted > 0 {
                    info!(detector = %id, bucket = to, inserted = commit.inserted, "anomalies found");
                }
                Ok(())
            }
            Err(err) => Err(DetectorState::Failing(err.to_string())),
        }
    }

    /// Run a store call off the async threads. Not raced against shutdown:
    /// once started it finishes, so a commit is never half done.
    async fn store_call<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Store) -> OracleResult<T> + Send + 'static,
    ) -> OracleResult<T> {
        let store = self.shared.store.clone();
        tokio::task::spawn_blocking(move || f(&store))
            .await
            .unwrap_or_else(|err| Err(OracleError::detect("store", format!("store task failed: {err}"))))
    }
}

/// Fewest history buckets a verdict needs: a quarter of the window, and never
/// fewer than the config's own minimum.
pub fn min_history(history_buckets: u64) -> usize {
    (history_buckets / 4).max(u64::from(MIN_BASELINE_BUCKETS)) as usize
}

/// The one nullable slot of a one-row aggregate.
fn single_slot(response: &LensResponse, column: &str) -> Result<Option<u64>, ScanError> {
    if response.truncated {
        return Err(ScanError::Truncated);
    }
    let [row] = response.rows.as_slice() else {
        return Err(ScanError::Permanent(format!("{} returned {} rows, expected 1", response.sql, response.rows.len())));
    };
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .map(Some)
            .ok_or_else(|| ScanError::Permanent(format!("{} returned {column} = {value}, not a slot", response.sql))),
        None => Err(ScanError::Permanent(format!("{} returned no {column} column", response.sql))),
    }
}

// ---------------------------------------------------------------------------
// RunnerHandle
// ---------------------------------------------------------------------------

impl RunnerHandle {
    /// Every detector's status, in run order, and the last tick's outcome.
    pub fn status(&self) -> RunnerStatus {
        let (last_tick_at, watermark, tick_error) = lock(&self.shared.tick).clone();
        RunnerStatus {
            last_tick_at,
            watermark,
            tick_error,
            detectors: self.shared.slots.iter().map(|slot| lock(&slot.status).clone()).collect(),
        }
    }

    /// One detector's status; `None` for an unknown id.
    pub fn detector(&self, id: &str) -> Option<DetectorStatus> {
        self.shared.slot(id).map(|slot| lock(&slot.status).clone())
    }

    /// Detector ids, in run order.
    pub fn ids(&self) -> Vec<String> {
        self.shared.slots.iter().map(|slot| slot.detector.id().to_owned()).collect()
    }

    /// Turn a detector on or off, in memory only. Returns its new status;
    /// `None` for an unknown id. A detector turned back on catches up from
    /// its stored progress.
    pub fn set_enabled(&self, id: &str, enabled: bool) -> Option<DetectorStatus> {
        let slot = self.shared.slot(id)?;
        let mut status = lock(&slot.status);
        if status.enabled != enabled {
            info!(detector = id, enabled, "detector toggled");
        }
        status.enabled = enabled;
        status.state = if enabled { DetectorState::Pending } else { DetectorState::Disabled };
        Some(status.clone())
    }

    /// Forget a detector's progress, so its next turn baselines again (the
    /// remedy for a ledger reset). Waits for a turn in flight to end. Returns
    /// the cleared progress. Fails for an unknown id with an
    /// [`OracleError::DetectError`].
    pub async fn reset_progress(&self, id: &str) -> OracleResult<Option<u64>> {
        let Some(slot) = self.shared.slot(id) else {
            return Err(OracleError::detect(id, "no such detector"));
        };
        let _turn = slot.turn.lock().await;
        let store = self.shared.store.clone();
        let metric = id.to_owned();
        let cleared = tokio::task::spawn_blocking(move || store.reset_progress(&metric))
            .await
            .unwrap_or_else(|err| Err(OracleError::detect(id, format!("store task failed: {err}"))))?;
        let mut status = lock(&slot.status);
        status.progress = None;
        status.chunk_buckets = DEFAULT_CHUNK_BUCKETS;
        if status.enabled {
            status.state = DetectorState::Pending;
        }
        Ok(cleared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BS: u64 = 150;

    /// Facts for data in `min ..= max`, every slot carrying transactions.
    fn busy(min: u64, max: u64) -> LensFacts {
        facts(Some(max), Some(max), Some(max), Some(min))
    }

    fn facts(max_slot: Option<u64>, last_tx_slot: Option<u64>, max_tx_slot: Option<u64>, min: Option<u64>) -> LensFacts {
        LensFacts { max_slot, last_tx_slot, max_tx_slot, min_slot: min }
    }

    fn complete_max(facts: LensFacts) -> Option<u64> {
        Watermark::compute(&facts, BS).map(|wm| wm.complete_max)
    }

    #[test]
    fn watermark_counts_only_whole_buckets() {
        // Slots 0..=299 ingested: buckets 0 and 1 complete.
        assert_eq!(complete_max(busy(0, 299)), Some(1));
        // One slot short of bucket 1's end: only bucket 0.
        assert_eq!(complete_max(busy(0, 298)), Some(0));
        // Nothing complete.
        assert_eq!(complete_max(busy(0, 148)), None);
        assert_eq!(complete_max(facts(None, Some(1), Some(1), Some(0))), None);
        assert_eq!(complete_max(facts(Some(299), Some(299), Some(299), None)), None);
    }

    #[test]
    fn a_lagging_transactions_table_decides() {
        // Slots says slot 1_000 has transactions; LENS has them only to 298.
        assert_eq!(complete_max(facts(Some(1_000), Some(1_000), Some(298), Some(0))), Some(0));
        // Slots says there are transactions, but none are visible yet.
        assert_eq!(complete_max(facts(Some(1_000), Some(10), None, Some(0))), None);
    }

    #[test]
    fn slots_without_transactions_do_not_hold_the_watermark_back() {
        // The last 150 slots are empty, and transactions holds the newest
        // slot that is not: both tables cover all 450 slots.
        assert_eq!(complete_max(facts(Some(449), Some(299), Some(299), Some(0))), Some(2));
        // A transactions table that is past it (a later read saw more) too.
        assert_eq!(complete_max(facts(Some(449), Some(299), Some(320), Some(0))), Some(2));
        // No slot has transactions at all: nothing to wait for.
        assert_eq!(complete_max(facts(Some(449), None, None, Some(0))), Some(2));
        // But one empty bucket after a lagging table changes nothing.
        assert_eq!(complete_max(facts(Some(449), Some(299), Some(298), Some(0))), Some(0));
    }

    #[test]
    fn the_bucket_ingestion_started_in_is_partial() {
        let at = |min| Watermark::compute(&busy(min, 10_000), BS).unwrap().first_complete_bucket;
        assert_eq!(at(0), 0);
        assert_eq!(at(150), 1, "an aligned start is complete");
        assert_eq!(at(151), 2);
        assert_eq!(at(299), 2);
        // Started inside the newest bucket: nothing complete.
        assert_eq!(complete_max(busy(151, 299)), None);
        assert_eq!(complete_max(busy(151, 449)), Some(2));
    }

    #[test]
    fn min_history_is_a_quarter_but_never_below_the_config_minimum() {
        assert_eq!(min_history(5), 5);
        assert_eq!(min_history(24), 6);
        assert_eq!(min_history(288), 72);
    }
}
