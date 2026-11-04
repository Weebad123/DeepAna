//! The alert engine: the loop that turns PRISM's change log into webhook
//! deliveries.
//!
//! | File            | Holds                                                         |
//! |-----------------|---------------------------------------------------------------|
//! | `mod.rs`        | [`AlertEngine`]: the loop, progress, failure isolation, status; [`EngineHandle`] |
//! | [`evaluator`]   | one alert over one slot range, and its payload                |
//!
//! It is the one place the other modules meet: events come from the
//! [`ChangelogReader`], visibility and rows from LENS ([`LensApi`]),
//! progress from the [`Registry`], and deliveries go to the
//! [`WebhookService`].
//!
//! # Progress is a slot, the change log a wake-up
//!
//! What an alert has covered is its `last_evaluated_slot` in the registry,
//! nothing else. The change log only says that there may be more to cover,
//! and how far: its `New` events give the engine a **target** slot. Every
//! decision below compares a slot with a slot, so a replayed, duplicated or
//! reordered event can never make an alert evaluate a range twice, or skip
//! one.
//!
//! # The loop
//!
//! [`AlertEngine::run`] repeats [`AlertEngine::run_once`]:
//!
//! 1. **Poll** up to [`EngineSettings::batch_size`] events.
//! 2. **Fold** the batch into one target: its highest `New` slot. A batch
//!    is gated and processed once however many events it holds. `Skip`
//!    slots are left out (they never appear in `solana.slots`, so gating on
//!    one could wait for ever), as are `Final` and unknown events.
//! 3. **Gate**: a target above the engine's current one waits for
//!    [`LensApi::wait_until_visible`]. On `Visible { max_slot }` the target
//!    is adopted and stored; on a timeout the batch is rewound and read
//!    again after a back-off. A target at or below the current one needs no
//!    gate.
//! 4. **List** the enabled alerts.
//! 5. **Process** each one; see below. Between alerts the loop checks for
//!    shutdown.
//! 6. **Release** held deliveries up to the highest `Final` slot read
//!    ([`WebhookService::release_final`]).
//! 7. **Commit** the batch's last event to the change log checkpoint,
//!    unless the pass was interrupted. A failing alert does not hold the
//!    commit back: its progress is in the registry, and it is retried
//!    against the same target once its back-off has run out, new events or
//!    not.
//! 8. **Back off** after a gate timeout or an infrastructure failure
//!    (reading the change log, listing alerts, committing); sleep
//!    [`EngineSettings::poll_interval`] when there was nothing to do.
//!
//! The target is kept in the registry too ([`Registry::raise_engine_target`]),
//! so after a restart one pass brings every alert that was behind it up to
//! it, without waiting for PRISM's next event. Baselines are the exception:
//! they need a `max_slot` from a gate in this process.
//!
//! # Processing one alert
//!
//! * **No progress** (new, or re-enabled): the alert is *baselined* at the
//!   gate's `max_slot`, with no evaluation. Its first range starts after
//!   it. Rows that matched between the alert's creation and that point are
//!   not reported; in exchange "from now on" means the same thing whenever
//!   the engine gets to it, replays included.
//! * **target <= progress**: nothing to do, and LENS is not asked. This is
//!   what makes duplicate and replayed events harmless.
//! * **Otherwise** `progress + 1 ..= target` is evaluated in chunks of at
//!   most [`EngineSettings::max_range_slots`], each committed on its own:
//!   a `Match` advances the progress and enqueues the delivery in one
//!   transaction (so the delivery exists exactly when the progress covers
//!   it); a `NoMatch` advances the progress alone.
//!
//! A chunk whose result is incomplete is halved and evaluated again before
//! anything is committed: when LENS truncated it, or when its rows do not
//! fit [`EngineSettings::payload_budget_bytes`]. Only a single slot that is
//! too big on its own is delivered incomplete, with `truncated` or
//! `rows_omitted` saying so.
//!
//! Ranges end at the target, never at the gate's `max_slot`: PRISM writes
//! `slots` first in a flush, so `max_slot` can name a batch whose other
//! tables are not written yet. The target is a slot PRISM has announced,
//! after all its files were written.
//!
//! # Failures
//!
//! A failure stops that alert's chunk loop, leaves its progress at the last
//! committed chunk, and is recorded against the alert only. Every other
//! alert carries on.
//!
//! | Failure                                            | Reported as | Retried after                            |
//! |----------------------------------------------------|-------------|------------------------------------------|
//! | [`EvalError::Transient`]                           | transient   | `retry_delay(consecutive)`               |
//! | missing table, up to `missing_table_retries` times in a row | transient | `retry_delay(consecutive)`        |
//! | missing table, after that                          | permanent   | `permanent_retry`                        |
//! | [`EvalError::Permanent`]                           | permanent   | `permanent_retry`                        |
//! | registry storage failure or conflict               | transient   | `retry_delay(consecutive)`               |
//! | registry rejects the write as invalid              | permanent   | `permanent_retry`                        |
//!
//! A permanent failure is still retried, rarely, because a LENS upgrade or a
//! table PRISM finally writes can fix it; until then the alert stays where
//! it is. Changing the alert's SQL clears its failure at once. A success
//! clears it too. An alert deleted mid-pass is dropped silently.
//!
//! # Status
//!
//! [`AlertEngine::run`] borrows the engine for as long as it runs, so the
//! engine is usually moved into the task that runs it. Take an
//! [`EngineHandle`] first: it is cheap to clone, shares the engine's state,
//! and is how the HTTP API reads [`EngineStatus`] for `/health`.
//!
//! # Shutdown
//!
//! A [`CancellationToken`] stops the loop between alerts and between
//! chunks; a gate wait or LENS query in flight is abandoned, since nothing
//! depends on it. A registry transaction in flight is never abandoned: it
//! commits or it does not, so there is no half state. The change log
//! checkpoint of an interrupted batch is not committed; the batch is read
//! again on the next start, and the slot comparisons make that harmless.

pub mod evaluator;

pub use evaluator::{
    EvalError, EvalOutcome, FailureKind, PAYLOAD_VERSION, Payload, build_payload, evaluate, names_missing_table,
    render_payload,
};

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::changelog_reader::{ChangelogReader, EventRecord, ReadEvent};
use crate::config::{ForgeConfig, ForgeError, ForgeResult};
use crate::lens_client::{LensApi, Visibility};
use crate::registry::{Alert, Registry, RegistryError, unix_now};
use crate::webhook::{Enqueued, NewDelivery, WebhookService};

/// Events read per pass.
pub const DEFAULT_BATCH_SIZE: usize = 1_000;
/// Three of LENS's default 30 s catalog refreshes: a slot PRISM has
/// announced is normally visible after one.
pub const DEFAULT_GATE_TIMEOUT: Duration = Duration::from_secs(90);
/// About seven minutes of Solana slots per evaluation.
pub const DEFAULT_MAX_RANGE_SLOTS: u64 = 1_000;
/// Largest webhook body the engine builds, where the rows allow.
pub const DEFAULT_PAYLOAD_BUDGET_BYTES: usize = 256 * 1024;
/// First retry delay after a transient failure; doubled per failure.
pub const DEFAULT_RETRY_MIN: Duration = Duration::from_secs(1);
/// Longest retry delay after a transient failure.
pub const DEFAULT_RETRY_MAX: Duration = Duration::from_secs(60);
/// Retry delay after a permanent failure.
pub const DEFAULT_PERMANENT_RETRY: Duration = Duration::from_secs(300);
/// Missing-table failures in a row reported as transient. With the default
/// delays this is about five minutes: long enough for LENS to refresh onto
/// a table PRISM has just created.
pub const DEFAULT_MISSING_TABLE_RETRIES: u32 = 10;

// ---------------------------------------------------------------------------
// EngineSettings
// ---------------------------------------------------------------------------

/// How the engine runs. [`EngineSettings::from_config`] is what FORGE
/// uses; tests and embedders build their own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineSettings {
    /// Pause after a pass that found nothing to do.
    pub poll_interval: Duration,
    /// Events read per pass.
    pub batch_size: usize,
    /// Longest wait for LENS to see a target.
    pub gate_timeout: Duration,
    /// Most slots one evaluation covers.
    pub max_range_slots: u64,
    /// Largest payload built, where the rows allow; see [`render_payload`].
    pub payload_budget_bytes: usize,
    /// First transient retry delay, and the loop's first back-off.
    pub retry_min: Duration,
    /// Cap of the doubling transient delay and of the loop's back-off.
    pub retry_max: Duration,
    /// Delay before retrying an alert that failed permanently.
    pub permanent_retry: Duration,
    /// Missing-table failures in a row reported as transient.
    pub missing_table_retries: u32,
}

impl EngineSettings {
    /// The defaults, with the config's poll interval.
    pub fn from_config(config: &ForgeConfig) -> Self {
        Self {
            poll_interval: config.poll_interval(),
            batch_size: DEFAULT_BATCH_SIZE,
            gate_timeout: DEFAULT_GATE_TIMEOUT,
            max_range_slots: DEFAULT_MAX_RANGE_SLOTS,
            payload_budget_bytes: DEFAULT_PAYLOAD_BUDGET_BYTES,
            retry_min: DEFAULT_RETRY_MIN,
            retry_max: DEFAULT_RETRY_MAX,
            permanent_retry: DEFAULT_PERMANENT_RETRY,
            missing_table_retries: DEFAULT_MISSING_TABLE_RETRIES,
        }
    }

    pub fn validate(&self) -> ForgeResult<()> {
        let invalid = |field: &str, reason: &str| Err(ForgeError::config(field, reason));
        if self.poll_interval.is_zero() {
            return invalid("alerts_poll_interval", "must be greater than zero");
        }
        if self.batch_size == 0 {
            return invalid("alerts_batch_size", "must be at least 1");
        }
        if self.gate_timeout.is_zero() {
            return invalid("alerts_gate_timeout", "must be greater than zero");
        }
        if self.max_range_slots == 0 {
            return invalid("alerts_max_range_slots", "must be at least 1");
        }
        if self.retry_min.is_zero() {
            return invalid("alerts_retry_min", "must be greater than zero");
        }
        if self.retry_max < self.retry_min {
            return invalid("alerts_retry_max", "must be at least alerts_retry_min");
        }
        if self.permanent_retry.is_zero() {
            return invalid("alerts_permanent_retry", "must be greater than zero");
        }
        Ok(())
    }

    /// `retry_min` doubled per failure after the first, capped at
    /// `retry_max`. `consecutive` counts from 1.
    pub fn retry_delay(&self, consecutive: u32) -> Duration {
        let doublings = consecutive.saturating_sub(1).min(31);
        self.retry_min.saturating_mul(1 << doublings).min(self.retry_max)
    }
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// What the engine has done since it started, for `/health`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct EngineStatus {
    /// The slot alerts are being brought up to; `None` until the first gate.
    pub target_slot: Option<u64>,
    /// How long the last gate waited, whatever its outcome.
    pub last_gate_wait_ms: Option<u64>,
    /// Events in committed batches.
    pub events_processed: u64,
    /// LENS queries made for alerts.
    pub evaluations: u64,
    /// Evaluations that returned rows.
    pub matches: u64,
    /// Evaluations that returned none.
    pub no_matches: u64,
    /// Ranges that failed, by how they were reported. Failed evaluations and
    /// failed registry writes both count.
    pub transient_failures: u64,
    pub permanent_failures: u64,
    /// Alerts whose last attempt failed, by id.
    pub failing_alerts: Vec<FailingAlert>,
    /// Change log bytes not read yet, at the last poll.
    pub changelog_bytes_behind: u64,
}

/// An alert whose last attempt failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FailingAlert {
    pub id: String,
    /// How the last failure was reported.
    pub kind: FailureKind,
    /// Failures in a row.
    pub consecutive: u32,
    /// Unix seconds of the first failure in the row.
    pub since: i64,
    pub last_error: String,
}

/// What one [`AlertEngine::run_once`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pass {
    /// No new events and no retry due.
    Idle,
    /// A batch of `events` was processed and committed.
    Committed { events: usize },
    /// No new events; alerts due a retry, or a catch-up after a restart,
    /// were processed.
    Retried,
    /// LENS did not see the batch's target in time. The batch is read again.
    GateTimedOut,
    /// Reading the change log, listing alerts or committing failed. The
    /// batch is read again.
    Failed,
    /// Shutdown was requested mid-pass. Nothing of the batch was committed
    /// to the change log.
    Interrupted,
}

// ---------------------------------------------------------------------------
// AlertEngine
// ---------------------------------------------------------------------------

/// The alert loop. See the [module docs](self).
pub struct AlertEngine {
    registry: Arc<Registry>,
    reader: Arc<Mutex<ChangelogReader>>,
    lens: Arc<dyn LensApi>,
    webhook: Arc<WebhookService>,
    settings: EngineSettings,
    state: Arc<Mutex<State>>,
}

/// A cloneable, read-only view of a running [`AlertEngine`]. See the
/// [module docs](self#status).
#[derive(Clone)]
pub struct EngineHandle {
    state: Arc<Mutex<State>>,
}

impl EngineHandle {
    /// A snapshot of what the engine has done; see [`AlertEngine::status`].
    pub fn status(&self) -> EngineStatus {
        snapshot(&lock_state(&self.state))
    }
}

impl std::fmt::Debug for EngineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineHandle").finish_non_exhaustive()
    }
}

/// The engine's memory between passes.
#[derive(Default)]
struct State {
    status: EngineStatus,
    gate: Option<Gate>,
    failures: BTreeMap<String, Failure>,
    /// A pass is due even without events: set at start when a target was
    /// stored, so alerts left behind it by the last run catch up.
    catch_up: bool,
}

/// What the gate has established so far.
#[derive(Debug, Clone, Copy)]
struct Gate {
    /// The highest announced `New` slot LENS has been seen to cover.
    target: u64,
    /// LENS's `MAX(slot)` at the gate that last moved `target`; `None`
    /// for a target loaded from the registry, until this process gates.
    max_slot: Option<u64>,
}

/// A failing alert's record.
struct Failure {
    kind: FailureKind,
    consecutive: u32,
    missing_table_streak: u32,
    since: i64,
    last_error: String,
    retry_at: Instant,
    /// The SQL that failed; another SQL starts afresh.
    sql: String,
}

/// How processing one alert ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AlertPass {
    UpToDate,
    Baselined,
    Advanced,
    Failed,
    Gone,
    Interrupted,
}

/// Why a registry write for an alert failed.
enum WriteError {
    /// The alert no longer exists.
    Gone,
    Failed { kind: FailureKind, message: String },
}

impl AlertEngine {
    /// An engine with [`EngineSettings::from_config`].
    pub fn new(
        registry: Arc<Registry>,
        reader: ChangelogReader,
        lens: Arc<dyn LensApi>,
        webhook: Arc<WebhookService>,
        config: &ForgeConfig,
    ) -> ForgeResult<Self> {
        Self::with_settings(registry, reader, lens, webhook, EngineSettings::from_config(config))
    }

    /// [`Self::new`] with explicit settings. Loads the stored target.
    pub fn with_settings(
        registry: Arc<Registry>,
        reader: ChangelogReader,
        lens: Arc<dyn LensApi>,
        webhook: Arc<WebhookService>,
        settings: EngineSettings,
    ) -> ForgeResult<Self> {
        settings.validate()?;
        let mut state = State::default();
        if let Some(target) = registry.engine_target()? {
            state.gate = Some(Gate { target, max_slot: None });
            state.status.target_slot = Some(target);
            state.catch_up = true;
        }
        Ok(Self {
            registry,
            reader: Arc::new(Mutex::new(reader)),
            lens,
            webhook,
            settings,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub const fn settings(&self) -> &EngineSettings {
        &self.settings
    }

    /// A snapshot of what the engine has done.
    pub fn status(&self) -> EngineStatus {
        snapshot(&self.state())
    }

    /// A handle sharing this engine's state; see [`EngineHandle`].
    pub fn handle(&self) -> EngineHandle {
        EngineHandle { state: self.state.clone() }
    }

    /// Run passes until `cancel` fires. Releases deliveries held for slots
    /// the committed change log has already finalized first, in case the
    /// last run stopped between the two.
    pub async fn run(&self, cancel: CancellationToken) {
        let committed_final = self.lock_reader().highest_final_slot();
        self.release(committed_final).await;
        info!(
            poll_interval_ms = self.settings.poll_interval.as_millis() as u64,
            batch_size = self.settings.batch_size,
            "alert engine started"
        );
        let mut failures = 0u32;
        while !cancel.is_cancelled() {
            let pause = match self.run_once(&cancel).await {
                Pass::Committed { .. } | Pass::Retried => {
                    failures = 0;
                    None
                }
                Pass::Idle => {
                    failures = 0;
                    Some(self.settings.poll_interval)
                }
                Pass::GateTimedOut | Pass::Failed => {
                    failures = failures.saturating_add(1);
                    Some(self.settings.retry_delay(failures))
                }
                Pass::Interrupted => break,
            };
            if let Some(pause) = pause {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    () = tokio::time::sleep(pause) => {}
                }
            }
        }
        info!("alert engine stopped");
    }

    /// One pass of the loop, without its pause. See the
    /// [module docs](self#the-loop).
    pub async fn run_once(&self, cancel: &CancellationToken) -> Pass {
        if cancel.is_cancelled() {
            return Pass::Interrupted;
        }

        // 1. Poll.
        let batch = match self.poll().await {
            Ok(batch) => batch,
            Err(err) => {
                warn!(error = %err, "cannot read the change log");
                self.rewind();
                return Pass::Failed;
            }
        };

        // 2-3. Fold and gate.
        let target = fold(batch.iter().map(|event| &event.envelope.event));
        if let Some(target) = target.filter(|target| self.needs_gate(*target)) {
            let started = Instant::now();
            let visibility = tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    self.rewind();
                    return Pass::Interrupted;
                }
                visibility = self.lens.wait_until_visible(target, self.settings.gate_timeout) => visibility,
            };
            let waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            self.state().status.last_gate_wait_ms = Some(waited_ms);
            match visibility {
                Visibility::Visible { max_slot } => {
                    self.adopt(target, max_slot);
                    let registry = self.registry.clone();
                    if let Err(err) = blocking(move || registry.raise_engine_target(target)).await {
                        // Only a restart's catch-up depends on it.
                        warn!(target, error = %err, "cannot store the engine target");
                    }
                }
                Visibility::TimedOut { last_seen, last_error } => {
                    warn!(target, last_seen, last_error, waited_ms, "LENS did not see the target in time; batch rewound");
                    self.rewind();
                    return Pass::GateTimedOut;
                }
            }
        }
        let catch_up = std::mem::take(&mut self.state().catch_up);
        if batch.is_empty() && !catch_up && !self.retry_due() {
            return Pass::Idle;
        }

        // 4-5. List and process.
        if let Some(gate) = self.gate() {
            let registry = self.registry.clone();
            let alerts = match blocking(move || registry.list_alerts()).await {
                Ok(alerts) => alerts,
                Err(err) => {
                    warn!(error = %err, "cannot list alerts");
                    self.state().catch_up |= catch_up;
                    self.rewind();
                    return Pass::Failed;
                }
            };
            let enabled: Vec<Alert> = alerts.into_iter().filter(|alert| alert.enabled).collect();
            let ids: HashSet<&str> = enabled.iter().map(|alert| alert.id.as_str()).collect();
            self.state().failures.retain(|id, _| ids.contains(id.as_str()));
            for alert in &enabled {
                if cancel.is_cancelled() {
                    self.rewind();
                    return Pass::Interrupted;
                }
                if self.backing_off(alert) {
                    continue;
                }
                if self.process(alert, gate, cancel).await == AlertPass::Interrupted {
                    self.rewind();
                    return Pass::Interrupted;
                }
            }
        }

        // 6. Release.
        let read_final = self.lock_reader().highest_read_final_slot();
        self.release(read_final).await;

        // 7. Commit.
        let Some(last) = batch.last().cloned() else {
            return Pass::Retried;
        };
        let reader = self.reader.clone();
        match blocking(move || lock(&reader).commit(&last)).await {
            Ok(()) => {
                self.state().status.events_processed += batch.len() as u64;
                Pass::Committed { events: batch.len() }
            }
            Err(err) => {
                warn!(error = %err, "cannot commit the change log checkpoint; batch rewound");
                self.rewind();
                Pass::Failed
            }
        }
    }

    // -- one alert -----------------------------------------------------------

    /// Bring `alert` up to `gate.target`. See the
    /// [module docs](self#processing-one-alert).
    async fn process(&self, alert: &Alert, gate: Gate, cancel: &CancellationToken) -> AlertPass {
        let Some(progress) = alert.last_evaluated_slot else {
            let Some(baseline) = gate.max_slot else {
                debug!(alert_id = %alert.id, "no gate in this process yet; baseline deferred");
                return AlertPass::UpToDate;
            };
            return match self.write(alert, baseline, None).await {
                Ok(()) => {
                    info!(alert_id = %alert.id, baseline, "alert baselined");
                    self.succeeded(alert);
                    AlertPass::Baselined
                }
                Err(err) => self.write_failed(alert, err, baseline, baseline),
            };
        };
        if gate.target <= progress {
            return AlertPass::UpToDate;
        }

        let mut from = progress + 1;
        loop {
            if cancel.is_cancelled() {
                return AlertPass::Interrupted;
            }
            let mut to = gate.target.min(from.saturating_add(self.settings.max_range_slots - 1));
            let payload = loop {
                let evaluated = tokio::select! {
                    biased;
                    () = cancel.cancelled() => return AlertPass::Interrupted,
                    evaluated = evaluate(self.lens.as_ref(), alert, from, to) => evaluated,
                };
                let outcome = {
                    let mut state = self.state();
                    state.status.evaluations += 1;
                    match evaluated {
                        Ok(outcome) => {
                            if outcome.is_match() {
                                state.status.matches += 1;
                            } else {
                                state.status.no_matches += 1;
                            }
                            outcome
                        }
                        Err(err) => {
                            drop(state);
                            let missing_table = err.is_missing_table();
                            self.failed(alert, err.kind(), err.to_string(), missing_table, from, to);
                            return AlertPass::Failed;
                        }
                    }
                };
                if outcome.truncated() && to > from {
                    debug!(alert_id = %alert.id, from, to, "LENS truncated the range; halving it");
                    to = halve(from, to);
                    continue;
                }
                if !outcome.is_match() {
                    break None;
                }
                let payload = render_payload(alert, from, to, &outcome, unix_now(), self.settings.payload_budget_bytes);
                if payload.rows_omitted > 0 && to > from {
                    debug!(alert_id = %alert.id, from, to, "rows exceed the payload budget; halving the range");
                    to = halve(from, to);
                    continue;
                }
                if outcome.truncated() || payload.rows_omitted > 0 {
                    warn!(
                        alert_id = %alert.id,
                        slot = from,
                        truncated = outcome.truncated(),
                        rows_omitted = payload.rows_omitted,
                        "one slot's rows do not fit a delivery; sending what fits"
                    );
                }
                break Some((payload.json, outcome.row_count()));
            };

            let rows = payload.as_ref().map(|(_, rows)| *rows);
            match self.write(alert, to, payload.map(|(json, _)| json)).await {
                Ok(()) => {
                    match rows {
                        Some(rows) => info!(alert_id = %alert.id, from, to, rows, "alert fired"),
                        None => debug!(alert_id = %alert.id, from, to, "no match"),
                    }
                    self.succeeded(alert);
                }
                Err(err) => return self.write_failed(alert, err, from, to),
            }
            if to >= gate.target {
                return AlertPass::Advanced;
            }
            from = to + 1;
        }
    }

    /// Advance `alert` to `to_slot` and, with a payload, enqueue its
    /// delivery, in one transaction; wake the dispatcher once it committed.
    async fn write(&self, alert: &Alert, to_slot: u64, payload: Option<String>) -> Result<(), WriteError> {
        let registry = self.registry.clone();
        let webhook = self.webhook.clone();
        let alert_id = alert.id.clone();
        let release_after_slot = alert.wait_for_final.then_some(to_slot);
        let written = blocking(move || {
            registry.db().transaction("evaluate alert", |tx| {
                Registry::advance_progress(tx, &alert_id, to_slot)?;
                payload
                    .map(|payload| {
                        let delivery = NewDelivery {
                            alert_id: alert_id.clone(),
                            to_slot,
                            payload,
                            release_after_slot,
                        };
                        webhook.enqueue(tx, &delivery)
                    })
                    .transpose()
            })
        })
        .await;
        match written {
            Ok(Some(Enqueued::Inserted { .. })) => {
                self.webhook.wake();
                Ok(())
            }
            Ok(Some(Enqueued::Duplicate { delivery_id })) => {
                // Only possible when an alert's progress was reset below a
                // range it had already fired.
                warn!(alert_id = %alert.id, to_slot, delivery_id, "a delivery for this range already exists");
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(err) => Err(match err.as_registry_error() {
                Some(RegistryError::NotFound { .. }) => WriteError::Gone,
                Some(RegistryError::Invalid { .. }) => WriteError::Failed {
                    kind: FailureKind::Permanent,
                    message: err.to_string(),
                },
                _ => WriteError::Failed {
                    kind: FailureKind::Transient,
                    message: err.to_string(),
                },
            }),
        }
    }

    /// Record a failed write of `from..=to`.
    fn write_failed(&self, alert: &Alert, err: WriteError, from: u64, to: u64) -> AlertPass {
        match err {
            WriteError::Gone => {
                debug!(alert_id = %alert.id, "alert deleted mid-pass");
                self.state().failures.remove(&alert.id);
                AlertPass::Gone
            }
            WriteError::Failed { kind, message } => {
                self.failed(alert, kind, message, false, from, to);
                AlertPass::Failed
            }
        }
    }

    // -- failure bookkeeping -------------------------------------------------

    fn failed(&self, alert: &Alert, reported: FailureKind, message: String, missing_table: bool, from: u64, to: u64) {
        let mut state = self.state();
        let failure = state.failures.entry(alert.id.clone()).or_insert_with(|| Failure {
            kind: reported,
            consecutive: 0,
            missing_table_streak: 0,
            since: unix_now(),
            last_error: String::new(),
            retry_at: Instant::now(),
            sql: alert.sql.clone(),
        });
        failure.consecutive = failure.consecutive.saturating_add(1);
        failure.missing_table_streak = if missing_table { failure.missing_table_streak.saturating_add(1) } else { 0 };
        let kind = if missing_table && failure.missing_table_streak > self.settings.missing_table_retries {
            FailureKind::Permanent
        } else {
            reported
        };
        let delay = match kind {
            FailureKind::Transient => self.settings.retry_delay(failure.consecutive),
            FailureKind::Permanent => self.settings.permanent_retry,
        };
        failure.kind = kind;
        failure.last_error = message;
        failure.retry_at = Instant::now() + delay;
        let consecutive = failure.consecutive;
        warn!(
            alert_id = %alert.id,
            kind = kind.as_str(),
            consecutive,
            from,
            to,
            retry_in_ms = delay.as_millis() as u64,
            error = %failure.last_error,
            "alert evaluation failed; its progress stays put"
        );
        match kind {
            FailureKind::Transient => state.status.transient_failures += 1,
            FailureKind::Permanent => state.status.permanent_failures += 1,
        }
    }

    fn succeeded(&self, alert: &Alert) {
        if self.state().failures.remove(&alert.id).is_some() {
            info!(alert_id = %alert.id, "alert recovered");
        }
    }

    /// Whether `alert` failed with its current SQL and its retry is not due.
    /// A changed SQL forgets the failure.
    fn backing_off(&self, alert: &Alert) -> bool {
        let mut state = self.state();
        let Some(failure) = state.failures.get(&alert.id) else {
            return false;
        };
        if failure.sql != alert.sql {
            state.failures.remove(&alert.id);
            return false;
        }
        Instant::now() < failure.retry_at
    }

    /// Whether a failing alert is due a retry that a pass could run.
    fn retry_due(&self) -> bool {
        let state = self.state();
        let now = Instant::now();
        state.gate.is_some() && state.failures.values().any(|failure| failure.retry_at <= now)
    }

    // -- gate ----------------------------------------------------------------

    fn gate(&self) -> Option<Gate> {
        self.state().gate
    }

    fn needs_gate(&self, target: u64) -> bool {
        self.gate().is_none_or(|gate| target > gate.target)
    }

    fn adopt(&self, target: u64, max_slot: u64) {
        let mut state = self.state();
        let gate = Gate { target, max_slot: Some(max_slot) };
        state.gate = Some(gate);
        state.status.target_slot = Some(target);
        debug!(target, max_slot, "target visible in LENS");
    }

    // -- change log and outbox -----------------------------------------------

    async fn poll(&self) -> ForgeResult<Vec<ReadEvent>> {
        let reader = self.reader.clone();
        let batch_size = self.settings.batch_size;
        let (batch, behind) = blocking(move || {
            let mut reader = lock(&reader);
            let batch = reader.poll(batch_size)?;
            Ok((batch, reader.stats().bytes_behind))
        })
        .await?;
        self.state().status.changelog_bytes_behind = behind;
        Ok(batch)
    }

    fn rewind(&self) {
        self.lock_reader().rewind_to_committed();
    }

    async fn release(&self, final_slot: u64) {
        if final_slot == 0 {
            return;
        }
        let webhook = self.webhook.clone();
        match blocking(move || webhook.release_final(final_slot)).await {
            Ok(0) => {}
            Ok(released) => info!(final_slot, released, "released deliveries held for finality"),
            Err(err) => warn!(final_slot, error = %err, "cannot release held deliveries; retried next pass"),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        lock_state(&self.state)
    }

    fn lock_reader(&self) -> MutexGuard<'_, ChangelogReader> {
        lock(&self.reader)
    }
}

impl std::fmt::Debug for AlertEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertEngine").field("settings", &self.settings).finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// The batch's target: its highest `New` slot.
fn fold<'a>(events: impl IntoIterator<Item = &'a EventRecord>) -> Option<u64> {
    events
        .into_iter()
        .filter_map(|event| match event {
            EventRecord::New { slot, .. } => Some(*slot),
            _ => None,
        })
        .max()
}

/// [`EngineStatus`] with the failing alerts filled in from `state`.
fn snapshot(state: &State) -> EngineStatus {
    let mut status = state.status.clone();
    status.failing_alerts = state
        .failures
        .iter()
        .map(|(id, failure)| FailingAlert {
            id: id.clone(),
            kind: failure.kind,
            consecutive: failure.consecutive,
            since: failure.since,
            last_error: failure.last_error.clone(),
        })
        .collect();
    status
}

fn lock_state(state: &Mutex<State>) -> MutexGuard<'_, State> {
    // Each update is a few field writes under the lock; a panic between two
    // leaves counters at worst one event apart.
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The end of the first half of `from..=to`, for `from < to`.
const fn halve(from: u64, to: u64) -> u64 {
    from + (to - from) / 2
}

fn lock(reader: &Mutex<ChangelogReader>) -> MutexGuard<'_, ChangelogReader> {
    // The reader's fields move together inside each call; a panic in one
    // leaves at worst a position the next rewind or commit resets.
    reader.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Run blocking `f` (SQLite, file I/O) on the blocking pool.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> ForgeResult<T> + Send + 'static) -> ForgeResult<T> {
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|err| Err(ForgeError::registry("alert engine task", format!("blocking task failed: {err}"))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> EngineSettings {
        EngineSettings {
            poll_interval: Duration::from_millis(500),
            batch_size: DEFAULT_BATCH_SIZE,
            gate_timeout: DEFAULT_GATE_TIMEOUT,
            max_range_slots: DEFAULT_MAX_RANGE_SLOTS,
            payload_budget_bytes: DEFAULT_PAYLOAD_BUDGET_BYTES,
            retry_min: DEFAULT_RETRY_MIN,
            retry_max: DEFAULT_RETRY_MAX,
            permanent_retry: DEFAULT_PERMANENT_RETRY,
            missing_table_retries: DEFAULT_MISSING_TABLE_RETRIES,
        }
    }

    fn new(slot: u64) -> EventRecord {
        EventRecord::New {
            slot,
            blockhash: "h".into(),
            parent_slot: slot - 1,
            block_time: None,
            transaction_count: 0,
            parquet_paths: vec![],
        }
    }

    #[test]
    fn fold_takes_the_highest_new_slot_only() {
        assert_eq!(fold(&[]), None);
        let skip = |slot| EventRecord::Skip { slot, reason: "validator_skipped".into() };
        let fin = |slot| EventRecord::Final { slot, blockhash: "h".into(), previous_slot: 0 };
        assert_eq!(fold(&[skip(50), fin(60), EventRecord::Unknown]), None);
        // Out of order, as a re-announcement after a PRISM restart makes it.
        assert_eq!(fold(&[new(12), new(10), skip(99), new(11)]), Some(12));
    }

    #[test]
    fn halving_always_shrinks_and_keeps_from() {
        for (from, to) in [(0, 1), (5, 6), (5, 7), (10, 1_009), (0, u64::MAX)] {
            let mid = halve(from, to);
            assert!(from <= mid && mid < to, "{from}..={to} -> {mid}");
        }
    }

    #[test]
    fn retry_delay_doubles_up_to_the_cap() {
        let s = settings();
        let secs: Vec<u64> = (1..=9).map(|n| s.retry_delay(n).as_secs()).collect();
        assert_eq!(secs, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(s.retry_delay(0), s.retry_min);
        assert_eq!(s.retry_delay(u32::MAX), s.retry_max);
    }

    #[test]
    fn settings_validation_names_the_field() {
        settings().validate().unwrap();
        let field = |s: EngineSettings| s.validate().unwrap_err().config_field().unwrap().to_owned();
        assert_eq!(field(EngineSettings { poll_interval: Duration::ZERO, ..settings() }), "alerts_poll_interval");
        assert_eq!(field(EngineSettings { batch_size: 0, ..settings() }), "alerts_batch_size");
        assert_eq!(field(EngineSettings { gate_timeout: Duration::ZERO, ..settings() }), "alerts_gate_timeout");
        assert_eq!(field(EngineSettings { max_range_slots: 0, ..settings() }), "alerts_max_range_slots");
        assert_eq!(field(EngineSettings { retry_min: Duration::ZERO, ..settings() }), "alerts_retry_min");
        assert_eq!(field(EngineSettings { retry_max: Duration::from_millis(1), ..settings() }), "alerts_retry_max");
        assert_eq!(field(EngineSettings { permanent_retry: Duration::ZERO, ..settings() }), "alerts_permanent_retry");
    }

    #[test]
    fn default_gate_timeout_outlasts_lens_refreshes_and_the_client_timeout() {
        assert!(DEFAULT_GATE_TIMEOUT > crate::lens_client::DEFAULT_REQUEST_TIMEOUT);
        assert!(DEFAULT_GATE_TIMEOUT >= Duration::from_secs(2 * 30));
    }
}
