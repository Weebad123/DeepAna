//! The verification pipeline: every check an artifact must pass before it
//! can be published, and a verdict CI can act on.
//!
//! [`Pipeline::run`] takes loaded artifacts (`manifest/`) and the pinned
//! fixture set (`fixtures/`), and returns a [`RunReport`]. It does not
//! execute contributed code: a decoder is an Anchor IDL interpreted by
//! PRISM's decoder ([`backend`]), a model is a SQL statement run by an
//! in-process DataFusion over fixture data ([`engine`]).
//!
//! | File               | Holds                                                         |
//! |--------------------|---------------------------------------------------------------|
//! | `mod.rs`           | [`Pipeline`]: stage order, per-artifact isolation, deadlines  |
//! | [`backend`]        | [`DecoderBackend`], [`Outcome`], [`PrismBackend`]              |
//! | [`static_checks`]  | stage 1: IDL checks, model-content checks                     |
//! | [`sql_guard`]      | static analysis of model SQL                                  |
//! | [`engine`]         | the in-process SQL engine                                     |
//! | [`golden`]         | stage 2: fixture cases, compared exactly                      |
//! | [`invariants`]     | stage 3: fuzz and metamorphic properties                      |
//! | [`digest`]         | canonical outcomes, case digests, run digest                  |
//! | [`report`]         | [`RunReport`], rendering, exit code                           |
//!
//! # Decisions
//!
//! 1. **Four stages per artifact, in a fixed order**: static, golden,
//!    invariants, reproducible. A stage runs only when every stage before
//!    it passed; the rest are reported as skipped. (With `--lens-url`, a
//!    model also gets a fifth, `lens`.)
//! 2. **Artifacts are independent.** Each is verified on its own thread,
//!    from its own fresh state (decoder, engine sessions, generated
//!    chains), one after the other; one artifact's failure, panic or
//!    timeout never changes another's verdict.
//! 3. **A verdict depends only on the artifact and the fixtures**: no
//!    clock, environment, path, status or order of arguments goes into any
//!    check. (The deadline is the one exception, and it only ever fails.)
//! 4. **Model SQL follows the FORGE alert placeholder convention**
//!    (`sql_guard.rs`).
//! 5. **The guard is a conservative static check; the metamorphic test
//!    (`invariant.bound_independence`) is the proof.**
//! 6. **Fixtures cannot pin floats, so models cannot output them**, and a
//!    decoder IDL may not reach one.
//! 7. **The engine is pinned and deterministic by construction**
//!    (`engine.rs`).
//! 8. **Digests cover outputs and nothing else** (`digest.rs`).
//! 9. **Decoder checks include behaviour nobody wrote a fixture for**
//!    (`invariants.rs`).
//! 10. **Evidence quality is part of the verdict** (`golden.rs`).
//! 11. **Deadlines are enforced even when code does not cooperate.** Every
//!     stage checks the deadline between cases; on top of that, the
//!     artifact's thread is abandoned when it passes, and the artifact
//!     fails with `deadline.exceeded` in the stage it was in.
//! 12. **Panics are contained.** A panic in PRISM or DataFusion on one
//!     input fails that check; a panic anywhere else in the verifier is an
//!     error for that artifact (exit 3), and the next artifact still runs.
//! 13. **Output is safe by construction** (`report.rs`).
//! 14. **No file is written.** `verify` reads the repository and prints the
//!     report; the engine cannot spill to disk.

pub mod backend;
pub mod digest;
pub mod engine;
pub mod golden;
pub mod invariants;
pub mod report;
pub mod sql_guard;
pub mod static_checks;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use crate::config::{CommonsConfig, echo};
use crate::fixtures::FixtureSet;
use crate::manifest::{Artifact, Kind};

pub use backend::{BackendError, DecoderBackend, InstructionInfo, LoadedDecoder, Outcome, PrismBackend};
pub use engine::{EngineError, TypedResult, run_model};
pub use report::{ArtifactReport, CheckResult, RunReport, Stage, StageReport, Status, Verdict};
pub use sql_guard::{GuardIssue, SqlInfo, analyse};

use golden::StageRun;
use static_checks::Learned;

/// Check id of the reproducible stage.
pub const CHECK_DIGEST_STABLE: &str = "reproducible.digest_stable";
/// Check id of the live LENS stage.
pub const CHECK_LENS_ACCEPTED: &str = "lens.accepted";
/// Check id for a panic in the verifier itself.
pub const CHECK_INTERNAL_PANIC: &str = "internal.panic";
/// Stack of an artifact's thread: planning nests deeply.
const THREAD_STACK_BYTES: usize = 64 * 1024 * 1024;
/// Longest a LENS request may take, within the artifact's deadline.
pub const LENS_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Deadline
// ---------------------------------------------------------------------------

/// When the current artifact must be done by.
#[derive(Debug, Clone, Copy)]
pub struct Deadline {
    at: Instant,
    budget: Duration,
}

impl Deadline {
    pub fn after(budget: Duration) -> Deadline {
        Deadline {
            at: Instant::now() + budget,
            budget,
        }
    }

    pub fn expired(&self) -> bool {
        Instant::now() >= self.at
    }

    pub fn budget(&self) -> Duration {
        self.budget
    }

    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Everything an artifact's thread needs, shared read-only.
#[derive(Clone)]
struct Shared {
    fixtures: Arc<FixtureSet>,
    backend: Arc<dyn DecoderBackend>,
    lens_url: Option<String>,
}

/// The verification pipeline. See the module docs.
pub struct Pipeline {
    shared: Shared,
    timeout: Duration,
}

/// The stages an artifact's thread has finished, and the one it is in.
#[derive(Default)]
struct Progress {
    done: Vec<StageReport>,
    current: Option<Stage>,
}

impl Pipeline {
    pub fn new(config: &CommonsConfig, fixtures: Arc<FixtureSet>, backend: Arc<dyn DecoderBackend>) -> Pipeline {
        Pipeline {
            shared: Shared {
                fixtures,
                backend,
                lens_url: config.lens_url.clone(),
            },
            timeout: config.verify_timeout,
        }
    }

    /// The same pipeline with another per-artifact deadline.
    pub fn with_timeout(mut self, timeout: Duration) -> Pipeline {
        self.timeout = timeout;
        self
    }

    /// Verify every artifact, one after the other, each in isolation.
    /// Reports keep the order of `artifacts`.
    pub fn run(&self, artifacts: Vec<Artifact>) -> RunReport {
        let reports = artifacts.iter().map(|a| self.isolated(a)).collect();
        RunReport::new(reports, self.shared.fixtures.set_hash(), engine::engine_name())
    }

    fn isolated(&self, artifact: &Artifact) -> ArtifactReport {
        let deadline = Deadline::after(self.timeout);
        let progress = Arc::new(Mutex::new(Progress::default()));
        let (tx, rx) = mpsc::channel();
        let shared = self.shared.clone();
        let a = artifact.clone();
        let p = Arc::clone(&progress);
        let spawned = thread::Builder::new()
            .name("commons-verify".into())
            .stack_size(THREAD_STACK_BYTES)
            .spawn(move || {
                let r = catch_unwind(AssertUnwindSafe(|| verify(&a, &shared, &deadline, &p)));
                let _ = tx.send(r);
            });
        if let Err(e) = spawned {
            return blank(artifact)
                .with_stage(Stage::Static, CheckResult::error(CHECK_INTERNAL_PANIC, "", format!("cannot start a thread: {e}")));
        }
        let partial = |check: CheckResult| {
            let mut pr = progress.lock().unwrap_or_else(PoisonError::into_inner);
            let stage = pr.current.unwrap_or(Stage::Static);
            let mut report = blank(artifact);
            report.stages = std::mem::take(&mut pr.done);
            report.with_stage(stage, check)
        };
        match rx.recv_timeout(self.timeout) {
            Ok(Ok(report)) => report,
            Ok(Err(payload)) => {
                let msg = payload
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| payload.downcast_ref::<&str>().copied())
                    .unwrap_or("no message");
                partial(CheckResult::error(
                    CHECK_INTERNAL_PANIC,
                    "",
                    format!("the verifier panicked: {}", echo(msg)),
                ))
            }
            // The thread is abandoned: it cannot be stopped, and it holds
            // nothing anyone else uses.
            Err(RecvTimeoutError::Timeout) => partial(CheckResult::fail(
                golden::CHECK_DEADLINE,
                "",
                format!(
                    "verification did not finish within {} s; the artifact's work was abandoned",
                    self.timeout.as_secs()
                ),
            )),
            Err(RecvTimeoutError::Disconnected) => partial(CheckResult::error(
                CHECK_INTERNAL_PANIC,
                "",
                "the verifier's thread ended without a report",
            )),
        }
    }
}

/// A report with no stage yet.
fn blank(a: &Artifact) -> ArtifactReport {
    ArtifactReport {
        label: a.label(),
        kind: a.manifest.kind,
        artifact_id: a.artifact_id,
        content_sha256: a.manifest.content_sha256,
        verdict: Verdict::Pass,
        stages: Vec::new(),
        digests: Default::default(),
        run_digest: None,
    }
}

impl ArtifactReport {
    /// Add `check` to `stage` (creating it), skip every later core stage,
    /// and finish.
    fn with_stage(mut self, stage: Stage, check: CheckResult) -> ArtifactReport {
        match self.stages.iter_mut().find(|s| s.stage == stage) {
            Some(s) => {
                s.checks.push(check);
                s.status = report::worst(&s.checks);
            }
            None => self.stages.push(StageReport::ran(stage, vec![check])),
        }
        for s in Stage::CORE {
            if !self.stages.iter().any(|r| r.stage == s) {
                self.stages.push(StageReport::skipped(s));
            }
        }
        self.stages.sort_by_key(|s| s.stage);
        self.finish()
    }
}

/// One run of golden and invariants, from fresh state. `Err` is a check to
/// report (the decoder could not be rebuilt).
fn golden_and_invariants(
    artifact: &Artifact,
    learned: &Learned,
    shared: &Shared,
    deadline: &Deadline,
    progress: &Mutex<Progress>,
    stages: &mut Vec<StageReport>,
    record: bool,
) -> Result<(StageRun, Option<StageRun>), CheckResult> {
    let fixtures = &shared.fixtures;
    let seed = invariants::seed(&artifact.artifact_id);
    let enter = |s: Stage| progress.lock().unwrap_or_else(PoisonError::into_inner).current = Some(s);
    match learned {
        Learned::Decoder { idl, program_id } => {
            let loaded = match catch_unwind(AssertUnwindSafe(|| shared.backend.load(&artifact.content, program_id))) {
                Ok(Ok(l)) => l,
                Ok(Err(e)) => {
                    return Err(CheckResult::error(
                        static_checks::CHECK_IDL_LOADS,
                        "",
                        format!("the IDL loaded once and then not again: {}", echo(&e.0)),
                    ));
                }
                Err(_) => return Err(CheckResult::error(static_checks::CHECK_IDL_LOADS, "", "loading panicked")),
            };
            let cases = fixtures.decoder_cases(program_id);
            if record {
                enter(Stage::Golden);
            }
            let g = golden::decoders(loaded.as_ref(), cases, deadline);
            if record {
                push(stages, progress, StageReport::ran(Stage::Golden, g.checks.clone()));
                if !passed(stages) {
                    return Ok((g, None));
                }
                enter(Stage::Invariants);
            }
            let i = invariants::decoders(loaded.as_ref(), idl, program_id, cases, seed, deadline);
            if record {
                push(stages, progress, StageReport::ran(Stage::Invariants, i.checks.clone()));
            }
            Ok((g, Some(i)))
        }
        Learned::Model { sql, info } => {
            let cases = fixtures.model_cases(&artifact.manifest.name);
            if record {
                enter(Stage::Golden);
            }
            let g = golden::models(fixtures, cases, sql, info, deadline);
            if record {
                push(stages, progress, StageReport::ran(Stage::Golden, g.checks.clone()));
                if !passed(stages) {
                    return Ok((g, None));
                }
                enter(Stage::Invariants);
            }
            let i = invariants::models(fixtures, cases, sql, info, seed, deadline);
            if record {
                push(stages, progress, StageReport::ran(Stage::Invariants, i.checks.clone()));
            }
            Ok((g, Some(i)))
        }
    }
}

fn passed(stages: &[StageReport]) -> bool {
    stages.iter().all(|s| s.status == Status::Pass)
}

fn push(stages: &mut Vec<StageReport>, progress: &Mutex<Progress>, s: StageReport) {
    progress.lock().unwrap_or_else(PoisonError::into_inner).done.push(s.clone());
    stages.push(s);
}

fn merged(g: &StageRun, i: Option<&StageRun>) -> digest::Digests {
    let mut d = g.digests.clone();
    if let Some(i) = i {
        d.extend(i.digests.clone());
    }
    d
}

/// Every stage of one artifact (module docs).
fn verify(artifact: &Artifact, shared: &Shared, deadline: &Deadline, progress: &Mutex<Progress>) -> ArtifactReport {
    let mut report = blank(artifact);
    let mut stages = Vec::new();
    let finish = |mut report: ArtifactReport, mut stages: Vec<StageReport>| {
        for s in Stage::CORE {
            if !stages.iter().any(|r: &StageReport| r.stage == s) {
                stages.push(StageReport::skipped(s));
            }
        }
        report.stages = stages;
        report.finish()
    };

    // 1. Static.
    progress.lock().unwrap_or_else(PoisonError::into_inner).current = Some(Stage::Static);
    let (checks, learned) = static_checks::run(artifact, shared.backend.as_ref());
    push(&mut stages, progress, StageReport::ran(Stage::Static, checks));
    let Some(learned) = learned.filter(|_| passed(&stages)) else {
        return finish(report, stages);
    };

    // 2 and 3. Golden and invariants.
    let first = golden_and_invariants(artifact, &learned, shared, deadline, progress, &mut stages, true);
    let (g1, i1) = match first {
        Ok(r) => r,
        Err(check) => {
            push(&mut stages, progress, StageReport::ran(Stage::Golden, vec![check]));
            return finish(report, stages);
        }
    };
    let d1 = merged(&g1, i1.as_ref());
    report.run_digest = Some(digest::run_digest(&d1));
    report.digests = d1.clone();
    if !passed(&stages) {
        return finish(report, stages);
    }

    // 4. Reproducible: everything again, from fresh state.
    progress.lock().unwrap_or_else(PoisonError::into_inner).current = Some(Stage::Reproducible);
    let check = match golden_and_invariants(artifact, &learned, shared, deadline, progress, &mut Vec::new(), false) {
        Err(c) => c,
        Ok((g2, i2)) => {
            let d2 = merged(&g2, i2.as_ref());
            let differ = digest::differing_keys(&d1, &d2);
            let again: Vec<&CheckResult> = g2.checks.iter().chain(i2.iter().flat_map(|i| &i.checks)).collect();
            if deadline.expired() {
                CheckResult::fail(golden::CHECK_DEADLINE, "", format!("verification took longer than {} s", deadline.budget().as_secs()))
            } else if !differ.is_empty() {
                CheckResult::fail(
                    CHECK_DIGEST_STABLE,
                    "",
                    format!(
                        "a second run from fresh state gave different outputs for {}; run digests {} and {}",
                        differ.join(", "),
                        crate::manifest::to_hex(&digest::run_digest(&d1)),
                        crate::manifest::to_hex(&digest::run_digest(&d2))
                    ),
                )
            } else if let Some(c) = again.iter().find(|c| !c.passed()) {
                CheckResult::fail(
                    CHECK_DIGEST_STABLE,
                    "",
                    format!("a second run from fresh state did not pass: {} {}: {}", c.id, c.subject, c.detail),
                )
            } else {
                CheckResult::pass(CHECK_DIGEST_STABLE, "")
            }
        }
    };
    push(&mut stages, progress, StageReport::ran(Stage::Reproducible, vec![check]));

    // 5. LENS, for models, when asked for.
    if let (Some(url), Kind::Model, Learned::Model { sql, .. }) = (&shared.lens_url, artifact.manifest.kind, &learned) {
        if !passed(&stages) {
            stages.push(StageReport::skipped(Stage::Lens));
        } else {
            progress.lock().unwrap_or_else(PoisonError::into_inner).current = Some(Stage::Lens);
            let timeout = LENS_TIMEOUT.min(deadline.remaining()).max(Duration::from_secs(1));
            let check = match engine::lens_accepts(url, sql, timeout) {
                Ok(()) => CheckResult::pass(CHECK_LENS_ACCEPTED, ""),
                Err(engine::LensRefusal::Rejected(m)) => CheckResult::fail(CHECK_LENS_ACCEPTED, "", m),
                Err(engine::LensRefusal::Unavailable(m)) => CheckResult::error(CHECK_LENS_ACCEPTED, "", m),
            };
            push(&mut stages, progress, StageReport::ran(Stage::Lens, vec![check]));
        }
    }
    finish(report, stages)
}
