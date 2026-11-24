//! Reconciliation: checking the slots PRISM stored against the finalized
//! chain, and having the ingestor re-ingest the ones that are wrong.
//!
//! PRISM writes at `confirmed`, so a slot it stored can turn out to be on a
//! fork the cluster abandoned. [`commitment`](crate::commitment) says which
//! slots are finalized; this module looks back at those and compares what is
//! on disk with what the finalized chain holds:
//!
//! * **orphaned**: rows are stored for a slot that has no finalized block;
//! * **missing**: a finalized block exists, nothing is stored and no skip
//!   was recorded;
//! * **false skip**: a finalized block exists, nothing is stored and a skip
//!   *was* recorded.
//!
//! | File          | Holds                                                         |
//! |---------------|---------------------------------------------------------------|
//! | `mod.rs`      | [`Reconciler`]: the timer loop, one pass, the rewind trigger   |
//! | [`classify`]  | pure: stored blocks, produced and logged slots → findings      |
//! | [`decide`]    | pure: two-pass rule, caps, mass-mismatch refusal → action      |
//! | [`sources`]   | stored slots from Parquet, skip-log parser, chain truth        |
//! | [`state`]     | `reconcile.json`: atomic persistence, quarantine               |
//!
//! The reconciler reads Parquet and the skip log and never edits a file or a
//! buffer of the running pipeline. **It decides, the ingestor executes**:
//! acting on a finding means asking a [`Rewind`] to move ingestion back.
//!
//! # Rules
//!
//! * **Slots are compared as sets first**: stored, on the finalized chain,
//!   logged as skipped. A slot both stored and on the chain must then be
//!   the *same block*: its stored `parent_slot` must be the finalized
//!   parent (free, from the block list) and, with
//!   [`ReconcileConfig::verify_blockhashes`], its blockhash the finalized
//!   one. Otherwise it is `mismatched`. Transactions and transfers are not
//!   compared row by row: a slot re-ingested at `finalized` is the chain's.
//! * **Only safe slots are examined**: at or below both the durable flush
//!   edge (so their files are complete) and the finalized watermark (so the
//!   chain's answer is final).
//! * **A failure is never evidence.** If any source cannot be read the pass
//!   concludes nothing and changes nothing.
//! * **Two consecutive passes must agree** on a slot, with the same kind,
//!   before it is acted on.
//! * **Mass mismatch means refuse**: when most of a window disagrees, the
//!   likelier cause is a wrong RPC or data directory, so the reconciler
//!   halts (`halted: mass_mismatch`) instead of deleting data.
//! * **The cluster is pinned**: the first pass records the genesis hash;
//!   a later mismatch halts (`halted: cluster_mismatch`).
//! * **The action is always a rewind** to the lowest confirmed slot.
//! * **Limits stop loops**: a slot is corrected at most twice, never more
//!   than 5,000 slots below the cursor, and at most 3 rewinds happen per
//!   hour. Past those the finding is `unresolved` and blocks progress until
//!   it is ok again.
//! * **Re-ingested finalized slots are fetched at `finalized`**, so a
//!   re-ingest can never pick the orphan up again.
//! * **Events are at least once**: a `Correction` may repeat.
//!
//! # The rewind
//!
//! 1. `in_flight` is written to `reconcile.json`.
//! 2. The [`Rewind`] is asked to [correct](Rewind::correct): the ingestor
//!    stops its pipeline without flushing, emits the
//!    [`ChangeEvent::Correction`] and flushes the change log, lowers its
//!    cursor to `from_slot - 1`, deletes every file at or above `from_slot`
//!    and resumes. Stopping first means no `New` or `Skip` for a slot at or
//!    above `from_slot` can land between the correction and the deletion:
//!    every such event after the correction is from the re-ingest. Lowering
//!    the cursor and deleting are idempotent, and the cursor goes first, so
//!    a crash between the two is finished by the restart's own rollback.
//! 4. The attempt and the rewind time are recorded and `in_flight` cleared,
//!    unless recovery already did so (see below).
//!
//! A failure in 2 propagates out of [`Reconciler::run`] with `in_flight`
//! still set. [`Reconciler::recover`], which the ingestor calls before its
//! pipeline starts, then announces the correction again (it may never have
//! been emitted), performs the rest of 2, and records 4.
//!
//! `reconcile.json` is the single source of truth: the reconciler reloads it
//! at the start of every pass and before recording a rewind as done, so a
//! completion recovery wrote is adopted rather than counted a second time.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::adapter::ChainAdapter;
use crate::changelog::ChangeLog;
use crate::ingestor::{IngestorCursor, SKIP_LOG_FILE_NAME};
use crate::storage::{ParquetWriter, Storage};
use crate::types::error::{PrismError, PrismResult};
pub mod classify;
pub mod decide;
pub mod sources;
pub mod state;

pub use classify::{
    Classification, FinalizedChain, Finding, FindingKind, SlotWindow, StoredBlock, classify, classify_blocks,
};
pub use decide::{Action, Decision, Limits, RewindPlan, decide, plan_window};
pub use sources::SkipLogReader;
pub use state::{
    Attempt, HaltReason, InFlight, RECONCILE_FILE_NAME, ReconcileState, StateStore, Strike,
    Unresolved, UnresolvedReason,
};

// ---------------------------------------------------------------------------
// Contracts
// ---------------------------------------------------------------------------

/// What the reconciler needs to know about the running pipeline.
pub trait ReconcileContext: Send + Sync {
    /// Last slot whose files are on disk and recorded as such.
    fn durable_last_flush_slot(&self) -> u64;
    /// The ingestion cursor: the last slot processed.
    fn cursor_slot(&self) -> u64;
    /// The finalized watermark; 0 before it is known.
    fn finalized_slot(&self) -> u64;
    /// The genesis hash of the cluster the pipeline ingests from.
    fn genesis_hash(&self) -> String;
}

/// Moves ingestion back. Implemented by the ingestor.
#[async_trait]
pub trait Rewind: Send + Sync {
    /// Stop ingesting, lower the cursor to `from_slot - 1`, delete every
    /// file at or above `from_slot` and resume from there. Returns once the
    /// cursor and the files are changed. Idempotent.
    async fn rewind_to(&self, from_slot: u64) -> PrismResult<()>;

    /// Announce `correction` on `changelog` and rewind to its `from_slot`.
    ///
    /// The default emits and flushes first, then calls [`Self::rewind_to`].
    /// An implementation that owns a running pipeline should override it to
    /// stop the pipeline *before* announcing, so that nothing it writes for
    /// slots at or above `from_slot` is announced after the correction (the
    /// ingestor's [`IngestorRewinder`](crate::ingestor::IngestorRewinder)
    /// does, through its own change log). Either way, nothing is rewound
    /// unless the announcement was made durable.
    async fn correct(&self, correction: &InFlight, changelog: &dyn ChangeLog) -> PrismResult<()> {
        changelog.emit(correction.event())?;
        changelog.flush()?;
        self.rewind_to(correction.from_slot).await
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How the reconciler runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileConfig {
    /// Pause between passes.
    pub interval: Duration,
    /// Where reconciliation starts on a fresh state. `None` starts at the
    /// lowest slot of any stored `slots` file.
    pub start_slot: Option<u64>,
    /// The skip log to read. `None` is `{data_dir}/skipped_slots.log`.
    pub skip_log_path: Option<PathBuf>,
    /// How far below a window to look for the finalized parent of its first
    /// block (the lineage check). Longer runs of skipped slots than this
    /// leave that one slot's lineage unchecked.
    pub lineage_lookback: u64,
    /// Also compare every stored blockhash of a window with the finalized
    /// block's: one header request per stored slot. Catches a stored block
    /// with the right parent but other contents (a duplicate block), which
    /// the free checks cannot see. Off by default: it costs as many requests
    /// as the window has blocks, shared with ingestion's rate limit.
    pub verify_blockhashes: bool,
    pub limits: Limits,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            start_slot: None,
            skip_log_path: None,
            lineage_lookback: 512,
            verify_blockhashes: false,
            limits: Limits::default(),
        }
    }
}

impl ReconcileConfig {
    /// The first offending field, as a [`PrismError::ConfigError`].
    pub fn validate(&self) -> PrismResult<()> {
        if self.interval.is_zero() {
            return Err(PrismError::config("interval", "must be greater than 0"));
        }
        if self.start_slot == Some(0) {
            return Err(PrismError::config("start_slot", "must be greater than 0"));
        }
        self.limits.validate()
    }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

/// How one pass ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassOutcome {
    /// The reconciler is halted; nothing was examined.
    Halted(HaltReason),
    /// Not enough new safe slots, or nothing stored yet.
    NotReady,
    /// A source could not be read; nothing was concluded.
    Inconclusive,
    /// Examined; nothing confirmed to act on.
    Examined,
    /// Confirmed findings wait for the hourly rewind cap.
    RateLimited { until: i64 },
    /// A rewind to `from_slot` was carried out.
    Rewound { from_slot: u64, to_slot: u64 },
}

/// What the last examined window looked like.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PassReport {
    pub window: SlotWindow,
    pub examined: u64,
    pub orphaned: u64,
    pub mismatched: u64,
    pub missing: u64,
    pub false_skip: u64,
    pub unlogged_skip: u64,
    /// `reconciled_through` after the pass.
    pub reconciled_through: u64,
    pub outcome: PassOutcome,
    /// Unix seconds.
    pub at: i64,
}

/// Counters of a [`Reconciler`] since it was created.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    /// Passes that classified a window.
    pub passes: u64,
    /// Ticks with nothing to examine yet.
    pub passes_not_ready: u64,
    /// Passes that could not read a source.
    pub passes_inconclusive: u64,
    pub slots_examined: u64,
    /// Slots moved past (reconciled) by this reconciler.
    pub slots_reconciled: u64,
    /// `unlogged_skip` slots among the reconciled ones: chain skips with no
    /// `Skip` event.
    pub unlogged_skips: u64,
    pub rewinds: u64,
    /// Passes whose confirmed findings waited for the rewind cap.
    pub rate_limited: u64,
    /// Findings that became unresolved.
    pub unresolved: u64,
    /// Skip log lines that did not parse.
    pub skip_log_malformed_lines: u64,
    pub last_pass: Option<PassReport>,
    /// The last source failure, as text.
    pub last_error: Option<String>,
}

/// A read-only view of a running reconciler.
#[derive(Debug, Clone)]
pub struct ReconcileHandle {
    stats: Arc<Mutex<ReconcileStats>>,
    state: Arc<Mutex<ReconcileState>>,
}

impl ReconcileHandle {
    pub fn stats(&self) -> ReconcileStats {
        lock(&self.stats).clone()
    }

    /// The state as last persisted.
    pub fn state(&self) -> ReconcileState {
        lock(&self.state).clone()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // Plain data, replaced whole: a panic cannot leave it half-written.
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

/// The background task that reconciles stored slots with the chain. See the
/// [module docs](self).
pub struct Reconciler {
    config: ReconcileConfig,
    adapter: Arc<dyn ChainAdapter>,
    ctx: Arc<dyn ReconcileContext>,
    rewinder: Arc<dyn Rewind>,
    changelog: Arc<dyn ChangeLog>,
    data_dir: PathBuf,
    store: StateStore,
    skip_log: SkipLogReader,
    /// The working copy; published to `shared_state` on every save.
    state: ReconcileState,
    shared_state: Arc<Mutex<ReconcileState>>,
    stats: Arc<Mutex<ReconcileStats>>,
}

impl std::fmt::Debug for Reconciler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reconciler")
            .field("config", &self.config)
            .field("data_dir", &self.data_dir)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl Reconciler {
    /// Load (or start) the state in `{data_dir}/reconcile.json`. A corrupt
    /// file is quarantined. No request is made until [`Self::run`].
    pub fn new(
        config: ReconcileConfig,
        adapter: Arc<dyn ChainAdapter>,
        ctx: Arc<dyn ReconcileContext>,
        rewinder: Arc<dyn Rewind>,
        changelog: Arc<dyn ChangeLog>,
        data_dir: impl Into<PathBuf>,
    ) -> PrismResult<Self> {
        config.validate()?;
        let data_dir = data_dir.into();
        let store = StateStore::new(&data_dir);
        let state = store.load()?.unwrap_or_default();
        let skip_log_path = config
            .skip_log_path
            .clone()
            .unwrap_or_else(|| data_dir.join(SKIP_LOG_FILE_NAME));
        Ok(Self {
            config,
            adapter,
            ctx,
            rewinder,
            changelog,
            skip_log: SkipLogReader::new(skip_log_path),
            store,
            data_dir,
            shared_state: Arc::new(Mutex::new(state.clone())),
            state,
            stats: Arc::default(),
        })
    }

    /// Finish a rewind a crash or failure interrupted: announce its
    /// correction on `changelog` again (a crash may have come before it was
    /// emitted; delivery is at least once), lower `cursor` to
    /// `from_slot - 1`, delete the files at or above `from_slot` (both
    /// idempotent) and record the rewind as done. Run before the pipeline
    /// starts, which [`Ingestor::run`](crate::ingestor::Ingestor::run) does.
    ///
    /// Returns the rewind point when there was a rewind to finish. A missing
    /// state file has none; a corrupt one is quarantined. Nothing is deleted
    /// unless the announcement was made durable.
    pub fn recover(
        data_dir: impl AsRef<Path>,
        cursor: &IngestorCursor,
        changelog: &dyn ChangeLog,
    ) -> PrismResult<Option<u64>> {
        let data_dir = data_dir.as_ref();
        let store = StateStore::new(data_dir);
        let Some(mut state) = store.load()? else {
            return Ok(None);
        };
        let Some(in_flight) = state.in_flight.clone() else {
            return Ok(None);
        };
        let from_slot = in_flight.from_slot;
        warn!(
            from_slot,
            to_slot = in_flight.to_slot,
            reason = %in_flight.reason,
            "finishing an interrupted reconcile rewind"
        );
        changelog.emit(in_flight.event())?;
        changelog.flush()?;
        cursor.rewind(from_slot)?;
        ParquetWriter::new(data_dir).rollback(from_slot)?;
        state.complete_in_flight(now());
        store.save(&state)?;
        Ok(Some(from_slot))
    }

    /// A view of the stats and state that stays valid while [`Self::run`]
    /// owns the reconciler.
    pub fn handle(&self) -> ReconcileHandle {
        ReconcileHandle {
            stats: Arc::clone(&self.stats),
            state: Arc::clone(&self.shared_state),
        }
    }

    pub fn config(&self) -> &ReconcileConfig {
        &self.config
    }

    /// The state as it is now.
    pub fn state(&self) -> &ReconcileState {
        &self.state
    }

    /// Run passes every [`ReconcileConfig::interval`] (the first right
    /// away) until `shutdown` is cancelled or the reconciler halts. While
    /// reconciliation is behind, a pass that reconciled its whole window is
    /// followed by the next one at once.
    ///
    /// A source failure is logged and retried at the next tick. A failure to
    /// persist the state, to publish the correction or to rewind is
    /// returned: the change log and the cursor are shared with the pipeline,
    /// which halts on the same failure.
    pub async fn run(mut self, shutdown: CancellationToken) -> PrismResult<()> {
        info!(
            data_dir = %self.data_dir.display(),
            interval_s = self.config.interval.as_secs(),
            reconciled_through = self.state.reconciled_through,
            "reconciler started"
        );
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut immediately = false;
        loop {
            if !immediately {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => break,
                    _ = ticker.tick() => {}
                }
            }
            let before = self.state.reconciled_through;
            let outcome = tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                outcome = self.run_pass() => outcome?,
            };
            match outcome {
                PassOutcome::Halted(reason) => {
                    error!(
                        reason = reason.as_str(),
                        path = %self.store.path().display(),
                        "reconciler halted; clear `halted` in the state file to resume"
                    );
                    return Ok(());
                }
                PassOutcome::Examined => {
                    let advanced = self.state.reconciled_through > before;
                    let safe = self.safe_upper();
                    immediately = advanced
                        && self.state.lowest_pending().is_none()
                        && safe.saturating_sub(self.state.reconciled_through) >= self.config.limits.min_window;
                }
                _ => immediately = false,
            }
            if immediately {
                tokio::task::yield_now().await;
            }
        }
        info!("reconciler stopped");
        Ok(())
    }

    /// One pass: finish a rewind left in flight, check the cluster, plan
    /// the window, read the sources, classify, decide, persist, act.
    pub async fn run_pass(&mut self) -> PrismResult<PassOutcome> {
        // The file is the truth: recovery may have changed it since.
        self.reload()?;
        if let Some(reason) = self.state.halted {
            return Ok(PassOutcome::Halted(reason));
        }
        if let Some(in_flight) = self.state.in_flight.clone() {
            // Recovery did not run (or the rewind failed after it): the
            // ingestor finishes it now, announcing it again. Idempotent.
            self.rewinder.correct(&in_flight, self.changelog.as_ref()).await?;
            self.finish_rewind(&in_flight)?;
        }

        let genesis = self.ctx.genesis_hash();
        match &self.state.genesis_hash {
            Some(pinned) if *pinned != genesis => {
                error!(pinned = %pinned, pipeline = %genesis, "cluster changed under the data directory");
                self.state.halted = Some(HaltReason::ClusterMismatch);
                self.state.updated_at = now();
                self.save()?;
                return Ok(PassOutcome::Halted(HaltReason::ClusterMismatch));
            }
            Some(_) => {}
            None => {
                let start = match self.config.start_slot {
                    Some(slot) => Some(slot),
                    None => match sources::lowest_stored_slot(&self.data_dir) {
                        Ok(slot) => slot,
                        Err(err) => return Ok(self.inconclusive(&err)),
                    },
                };
                let Some(start) = start else {
                    lock(&self.stats).passes_not_ready += 1;
                    return Ok(PassOutcome::NotReady);
                };
                self.state.genesis_hash = Some(genesis);
                self.state.reconciled_through = start.saturating_sub(1);
                self.state.updated_at = now();
                self.save()?;
                info!(start_slot = start, "reconcile state initialised; cluster pinned");
            }
        }

        let Some(window) = plan_window(self.state.reconciled_through, self.safe_upper(), &self.config.limits)
        else {
            lock(&self.stats).passes_not_ready += 1;
            return Ok(PassOutcome::NotReady);
        };
        let classification = match self.gather(window).await {
            Ok(classification) => classification,
            Err(err) => return Ok(self.inconclusive(&err)),
        };

        let previous_through = self.state.reconciled_through;
        let decision = decide(
            &classification,
            &self.state,
            self.ctx.cursor_slot(),
            now(),
            &self.config.limits,
        );
        for entry in &decision.newly_unresolved {
            warn!(
                slot = entry.slot,
                kind = %entry.kind,
                reason = entry.reason.as_str(),
                "finding is unresolved; it holds reconciliation back until it is ok again"
            );
        }
        self.state = decision.state;
        self.save()?;
        self.skip_log.prune_through(self.state.reconciled_through);

        let outcome = match decision.action {
            Action::Refuse { actionable, examined } => {
                let sample: Vec<u64> = classification.actionable().take(10).map(|f| f.slot).collect();
                error!(
                    %window,
                    actionable,
                    examined,
                    ?sample,
                    "most of the window disagrees with the chain; refusing to correct it"
                );
                PassOutcome::Halted(HaltReason::MassMismatch)
            }
            Action::Nothing => PassOutcome::Examined,
            Action::Wait { confirmed, until } => {
                warn!(confirmed, until, "confirmed findings wait: the hourly rewind cap is reached");
                PassOutcome::RateLimited { until }
            }
            Action::Rewind(plan) => {
                self.trigger_rewind(&plan).await?;
                PassOutcome::Rewound {
                    from_slot: plan.from_slot,
                    to_slot: plan.to_slot,
                }
            }
        };
        self.record_pass(&classification, previous_through, &outcome, decision.newly_unresolved.len());
        Ok(outcome)
    }

    /// The highest slot that may be examined.
    fn safe_upper(&self) -> u64 {
        self.ctx
            .durable_last_flush_slot()
            .min(self.ctx.finalized_slot())
    }

    /// Read the three sources for `window` and classify it.
    async fn gather(&mut self, window: SlotWindow) -> PrismResult<Classification> {
        self.skip_log.refresh()?;
        let logged = self.skip_log.logged_in(window);
        let stored = sources::stored_blocks(&self.data_dir, window)?;
        let chain = sources::finalized_chain(self.adapter.as_ref(), window, self.config.lineage_lookback).await?;
        let hashes = if self.config.verify_blockhashes {
            let both = stored.keys().copied().filter(|slot| chain.produced.contains(slot));
            sources::finalized_blockhashes(self.adapter.as_ref(), both).await?
        } else {
            std::collections::BTreeMap::new()
        };
        Ok(classify_blocks(window, &stored, &chain, &logged, &hashes))
    }

    /// Steps 1 to 4 of the module docs.
    async fn trigger_rewind(&mut self, plan: &RewindPlan) -> PrismResult<()> {
        info!(
            from_slot = plan.from_slot,
            to_slot = plan.to_slot,
            reason = %plan.reason,
            affected = plan.affected(),
            "correcting stored slots: rewinding the ingestor"
        );
        self.state.in_flight = Some(InFlight {
            from_slot: plan.from_slot,
            to_slot: plan.to_slot,
            reason: plan.reason,
            affected: plan.affected(),
            slots: plan.slots.clone(),
            started_at: now(),
        });
        self.save()?;
        let in_flight = self.state.in_flight.clone().expect("just set");
        self.rewinder.correct(&in_flight, self.changelog.as_ref()).await?;
        self.finish_rewind(&in_flight)?;
        Ok(())
    }

    /// Step 4: record `in_flight` as done, unless someone else (recovery, in
    /// [`Ingestor::run`](crate::ingestor::Ingestor::run)) already did, in
    /// which case their record is adopted and the rewind not counted twice.
    fn finish_rewind(&mut self, in_flight: &InFlight) -> PrismResult<()> {
        self.reload()?;
        match &self.state.in_flight {
            Some(current) if current.is_same_rewind(in_flight) => {
                self.state.complete_in_flight(now());
                self.save()?;
            }
            _ => info!(
                from_slot = in_flight.from_slot,
                "the rewind was already recorded as done by recovery"
            ),
        }
        lock(&self.stats).rewinds += 1;
        Ok(())
    }

    /// Replace the working copy with what `reconcile.json` holds (a fresh
    /// state when there is no file), and publish it.
    fn reload(&mut self) -> PrismResult<()> {
        self.state = self.store.load()?.unwrap_or_default();
        *lock(&self.shared_state) = self.state.clone();
        Ok(())
    }

    fn save(&self) -> PrismResult<()> {
        self.store.save(&self.state)?;
        *lock(&self.shared_state) = self.state.clone();
        Ok(())
    }

    fn inconclusive(&self, err: &PrismError) -> PassOutcome {
        warn!(error = %err, "reconcile pass inconclusive: a source could not be read");
        let mut stats = lock(&self.stats);
        stats.passes_inconclusive += 1;
        stats.last_error = Some(err.to_string());
        stats.skip_log_malformed_lines = self.skip_log.malformed_lines();
        PassOutcome::Inconclusive
    }

    fn record_pass(
        &self,
        classification: &Classification,
        previous_through: u64,
        outcome: &PassOutcome,
        newly_unresolved: usize,
    ) {
        let through = self.state.reconciled_through;
        let report = PassReport {
            window: classification.window,
            examined: classification.examined,
            orphaned: classification.count(FindingKind::Orphaned),
            mismatched: classification.count(FindingKind::Mismatched),
            missing: classification.count(FindingKind::Missing),
            false_skip: classification.count(FindingKind::FalseSkip),
            unlogged_skip: classification.count(FindingKind::UnloggedSkip),
            reconciled_through: through,
            outcome: outcome.clone(),
            at: now(),
        };
        debug!(?report, "reconcile pass");
        let mut stats = lock(&self.stats);
        stats.passes += 1;
        stats.slots_examined += classification.examined;
        stats.slots_reconciled += through.saturating_sub(previous_through);
        stats.unlogged_skips += classification
            .findings
            .iter()
            .filter(|f| f.kind == FindingKind::UnloggedSkip && f.slot > previous_through && f.slot <= through)
            .count() as u64;
        stats.unresolved += newly_unresolved as u64;
        if matches!(outcome, PassOutcome::RateLimited { .. }) {
            stats.rate_limited += 1;
        }
        stats.skip_log_malformed_lines = self.skip_log.malformed_lines();
        stats.last_pass = Some(report);
    }
}

fn now() -> i64 {
    chrono::Utc::now().timestamp()
}
