//! Stage 2: run the artifact on every fixture case and compare exactly.
//!
//! # Decoder cases
//!
//! Every case of the artifact's program (`decoder_cases(program_id)`) is
//! run with its data and accounts, inside `catch_unwind`:
//!
//! | Expected       | Passes when                                                              |
//! |----------------|--------------------------------------------------------------------------|
//! | `decoded`      | the outcome is `Decoded`, `instruction_type` is equal, and the decoder's JSON parses strictly to a value with the expected canonical bytes |
//! | `not_claimed`  | the outcome is `NotClaimed`                                              |
//! | `decode_error` | the outcome is `DecodeError`; the message is not compared, its wording changes between PRISM versions |
//!
//! # Model cases
//!
//! Every case of the model: resolve its chain (explicit or generated), run
//! the SQL for its window under both [`GOLDEN_PARTITIONS`], require the same
//! rows from both (`golden.partition_stable`), and compare with `expect`
//! when there is one: columns exactly, rows in order when `ordered`, as a
//! multiset otherwise. An `ordered` expectation needs a top-level `ORDER
//! BY`, or the order it pins is not an output.
//!
//! # Evidence
//!
//! Evidence quality is part of the verdict:
//!
//! - `golden.no_cases`: an artifact with no case at all fails.
//! - `golden.no_explicit_case`: a model needs a case over an explicit chain.
//! - `golden.no_expected_rows`: a model needs a case that expects at least
//!   one row; a model only ever checked against empty output proves little.
//!
//! Every case's canonical outcome is digested (`digest.rs`), pass or fail.

use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::config::echo;
use crate::fixtures::{
    ChainRef, DecoderCase, DecoderExpect, FixtureSet, ModelCase, ParseLimits, StrictValue,
    canonical_bytes, first_difference, parse_strict,
};

use super::Deadline;
use super::backend::{LoadedDecoder, Outcome};
use super::digest::{self, CaseKind, Digests, ModelFailure};
use super::engine::{self, TypedResult};
use super::report::CheckResult;
use super::sql_guard::SqlInfo;

pub const CHECK_DECODER_CASE: &str = "golden.decoder_case";
pub const CHECK_MODEL_CASE: &str = "golden.model_case";
pub const CHECK_PARTITION_STABLE: &str = "golden.partition_stable";
pub const CHECK_NO_CASES: &str = "golden.no_cases";
pub const CHECK_NO_EXPLICIT_CASE: &str = "golden.no_explicit_case";
pub const CHECK_NO_EXPECTED_ROWS: &str = "golden.no_expected_rows";
/// Reported, in whichever stage was running, when time ran out.
pub const CHECK_DEADLINE: &str = "deadline.exceeded";

/// The two partition settings every model case runs under.
pub const GOLDEN_PARTITIONS: [usize; 2] = [1, 4];

/// What one run of a stage produced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StageRun {
    pub checks: Vec<CheckResult>,
    pub digests: Digests,
}

impl StageRun {
    /// Record that the deadline passed, and say whether it did.
    pub(crate) fn out_of_time(&mut self, deadline: &Deadline) -> bool {
        if deadline.expired() {
            self.checks.push(CheckResult::fail(
                CHECK_DEADLINE,
                "",
                format!("verification took longer than {} s", deadline.budget().as_secs()),
            ));
            true
        } else {
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Decoders
// ---------------------------------------------------------------------------

/// One decoder call: `outcome` is `None` when it panicked, and `strict` is
/// the decoded JSON as a strict value, when it is one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Call {
    pub outcome: Option<Outcome>,
    pub strict: Result<StrictValue, String>,
}

impl Call {
    /// The canonical outcome (`digest.rs`).
    pub fn canonical(&self) -> StrictValue {
        digest::decoder_outcome(self.outcome.as_ref(), self.strict.as_ref().ok())
    }

    /// For messages; contributor text is echoed.
    pub fn describe(&self) -> String {
        match &self.outcome {
            None => "a panic".into(),
            Some(Outcome::NotClaimed) => "not claimed".into(),
            Some(Outcome::DecodeError(m)) => format!("a decode error ({})", echo(m)),
            Some(Outcome::Decoded { instruction_type, data_json }) => {
                format!("decoded as '{}' with {}", echo(instruction_type), echo(data_json))
            }
        }
    }
}

/// Run `loaded` on one instruction, containing a panic.
pub fn call(loaded: &dyn LoadedDecoder, program_id: &[u8; 32], data: &[u8], accounts: &[[u8; 32]]) -> Call {
    let outcome = catch_unwind(AssertUnwindSafe(|| loaded.run(program_id, data, accounts))).ok();
    let strict = match &outcome {
        Some(Outcome::Decoded { data_json, .. }) => {
            parse_strict(data_json.as_bytes(), &ParseLimits::DEFAULT).map_err(|e| e.to_string())
        }
        _ => Err("not decoded".into()),
    };
    Call { outcome, strict }
}

/// `Ok` when `call` is what `expect` says.
pub fn compare(expect: &DecoderExpect, c: &Call) -> Result<(), String> {
    match (expect, &c.outcome) {
        (DecoderExpect::NotClaimed, Some(Outcome::NotClaimed)) => Ok(()),
        (DecoderExpect::DecodeError, Some(Outcome::DecodeError(_))) => Ok(()),
        (
            DecoderExpect::Decoded { instruction_type, data },
            Some(Outcome::Decoded { instruction_type: got, .. }),
        ) => {
            if got != instruction_type {
                return Err(format!(
                    "instruction_type is '{}', expected '{instruction_type}'",
                    echo(got)
                ));
            }
            let value = c
                .strict
                .as_ref()
                .map_err(|e| format!("the decoder's output is not strict JSON, so it cannot match: {}", echo(e)))?;
            if canonical_bytes(value) == canonical_bytes(data) {
                Ok(())
            } else {
                let d = first_difference(data, value).map_or_else(String::new, |d| d.to_string());
                Err(format!("data differs at {d}"))
            }
        }
        _ => Err(format!("expected {}, got {}", expect.result_str(), c.describe())),
    }
}

/// Every decoder case, in name order.
pub fn decoders(loaded: &dyn LoadedDecoder, cases: &[DecoderCase], deadline: &Deadline) -> StageRun {
    let mut run = StageRun::default();
    if cases.is_empty() {
        run.checks.push(CheckResult::fail(
            CHECK_NO_CASES,
            "",
            "there is no fixture case for this program; add cases under fixtures/decoders/<program_id>/",
        ));
        return run;
    }
    run.checks.push(CheckResult::pass(CHECK_NO_CASES, ""));
    for case in cases {
        if run.out_of_time(deadline) {
            return run;
        }
        let c = call(loaded, &case.program_id, &case.data, &case.accounts);
        run.digests
            .insert(CaseKind::Decoder.key(&case.name), digest::case_digest(CaseKind::Decoder, &c.canonical()));
        run.checks
            .push(CheckResult::from_result(CHECK_DECODER_CASE, &case.name, compare(&case.expect, &c)));
    }
    run
}

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

/// [`engine::run_model`], containing a panic as an execution failure.
pub fn run_contained(
    chain: &crate::fixtures::Chain,
    sql: &str,
    from: u64,
    to: u64,
    partitions: usize,
) -> Result<TypedResult, (Option<ModelFailure>, String)> {
    match catch_unwind(AssertUnwindSafe(|| engine::run_model(chain, sql, from, to, partitions))) {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err((e.failure(), e.to_string())),
        Err(_) => Err((Some(ModelFailure::Panic), "the engine panicked".into())),
    }
}

/// The check for an engine failure: the model's fault, unless the engine
/// could not even be set up.
pub fn engine_check(id: &str, subject: &str, failure: Option<ModelFailure>, msg: &str) -> CheckResult {
    match failure {
        Some(_) => CheckResult::fail(id, subject, msg),
        None => CheckResult::error(id, subject, msg),
    }
}

/// The canonical outcome of a run.
pub fn outcome_of(r: &Result<TypedResult, (Option<ModelFailure>, String)>, ordered: bool) -> StrictValue {
    match r {
        Ok(t) => t.outcome(ordered),
        Err((f, _)) => digest::model_failure(f.unwrap_or(ModelFailure::Execution)),
    }
}

/// Every model case, in name order.
pub fn models(
    fixtures: &FixtureSet,
    cases: &[ModelCase],
    sql: &str,
    info: &SqlInfo,
    deadline: &Deadline,
) -> StageRun {
    let mut run = StageRun::default();
    if cases.is_empty() {
        run.checks.push(CheckResult::fail(
            CHECK_NO_CASES,
            "",
            "there is no fixture case for this model; add cases under fixtures/models/<name>/",
        ));
        return run;
    }
    run.checks.push(CheckResult::pass(CHECK_NO_CASES, ""));
    for case in cases {
        if run.out_of_time(deadline) {
            return run;
        }
        let chain = match fixtures.resolve_chain(&case.chain) {
            Ok(c) => c,
            Err(e) => {
                run.checks.push(CheckResult::error(CHECK_MODEL_CASE, &case.name, e.to_string()));
                continue;
            }
        };
        let results: Vec<_> = GOLDEN_PARTITIONS
            .iter()
            .map(|&p| run_contained(&chain, sql, case.from_slot, case.to_slot, p))
            .collect();
        let first = &results[0];
        run.digests.insert(
            CaseKind::Model.key(&case.name),
            digest::case_digest(CaseKind::Model, &outcome_of(first, info.ordered)),
        );
        let r1 = match first {
            Ok(r) => r,
            Err((f, m)) => {
                run.checks.push(engine_check(CHECK_MODEL_CASE, &case.name, *f, m));
                continue;
            }
        };
        let stable = match &results[1] {
            Ok(r) => r1.same_as(r, info.ordered).map_err(|d| {
                format!(
                    "{} partitions give different rows than {}: {d}",
                    GOLDEN_PARTITIONS[1], GOLDEN_PARTITIONS[0]
                )
            }),
            Err((_, m)) => Err(format!(
                "fails with {} partitions but not {}: {m}",
                GOLDEN_PARTITIONS[1], GOLDEN_PARTITIONS[0]
            )),
        };
        run.checks.push(CheckResult::from_result(CHECK_PARTITION_STABLE, &case.name, stable));
        let verdict = match &case.expect {
            None => Ok(()),
            Some(e) if e.ordered && !info.ordered => Err(
                "expect.ordered is true but the model has no top-level ORDER BY, so its row order is \
                 not an output"
                    .to_string(),
            ),
            Some(e) => e.check(&r1.columns, &r1.rows),
        };
        run.checks.push(CheckResult::from_result(CHECK_MODEL_CASE, &case.name, verdict));
    }
    let explicit = cases.iter().any(|c| matches!(c.chain, ChainRef::Explicit(_)));
    run.checks.push(CheckResult::from_result(
        CHECK_NO_EXPLICIT_CASE,
        "",
        if explicit {
            Ok(())
        } else {
            Err("every case runs over a generated chain; at least one must run over an explicit, \
                 reviewed chain with expected rows"
                .into())
        },
    ));
    let rows = cases.iter().any(|c| c.expect.as_ref().is_some_and(|e| !e.rows.is_empty()));
    run.checks.push(CheckResult::from_result(
        CHECK_NO_EXPECTED_ROWS,
        "",
        if rows {
            Ok(())
        } else {
            Err("no case expects a row; at least one case must pin non-empty output".into())
        },
    ));
    run
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn decoded(t: &str, json: &str) -> Call {
        Call {
            outcome: Some(Outcome::Decoded { instruction_type: t.into(), data_json: json.into() }),
            strict: parse_strict(json.as_bytes(), &ParseLimits::DEFAULT).map_err(|e| e.to_string()),
        }
    }

    fn sv(s: &str) -> StrictValue {
        parse_strict(s.as_bytes(), &ParseLimits::DEFAULT).unwrap()
    }

    #[test]
    fn comparison_is_exact() {
        let e = DecoderExpect::Decoded { instruction_type: "deposit".into(), data: sv(r#"{"amount":1000}"#) };
        assert_eq!(compare(&e, &decoded("deposit", r#"{"amount":1000}"#)), Ok(()));
        // Key order and spacing in the decoder's text do not matter...
        let e2 = DecoderExpect::Decoded { instruction_type: "t".into(), data: sv(r#"{"a":1,"b":[true]}"#) };
        assert_eq!(compare(&e2, &decoded("t", r#"{ "b": [true], "a": 1 }"#)), Ok(()));
        // ...values do, with no coercion.
        for (t, json, part) in [
            ("deposit", r#"{"amount":"1000"}"#, "data differs at amount"),
            ("deposit", r#"{"amount":1001}"#, "data differs at amount"),
            ("deposit", r#"{"amount":1000,"x":null}"#, "data differs at x"),
            ("Deposit", r#"{"amount":1000}"#, "instruction_type is 'Deposit'"),
            ("deposit", r#"{"amount":1000.0}"#, "not strict JSON"),
        ] {
            let err = compare(&e, &decoded(t, json)).unwrap_err();
            assert!(err.contains(part), "{json}: {err}");
        }
        let not = Call { outcome: Some(Outcome::NotClaimed), strict: Err(String::new()) };
        let fail = Call { outcome: Some(Outcome::DecodeError("x".into())), strict: Err(String::new()) };
        let panic = Call { outcome: None, strict: Err(String::new()) };
        assert_eq!(compare(&DecoderExpect::NotClaimed, &not), Ok(()));
        assert_eq!(compare(&DecoderExpect::DecodeError, &fail), Ok(()));
        assert!(compare(&DecoderExpect::NotClaimed, &fail).unwrap_err().contains("expected not_claimed, got a decode error"));
        assert!(compare(&DecoderExpect::DecodeError, &not).unwrap_err().contains("got not claimed"));
        assert!(compare(&e, &panic).unwrap_err().contains("a panic"));
        assert!(compare(&DecoderExpect::DecodeError, &decoded("t", "{}")).is_err());
    }
}
