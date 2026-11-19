//! End-to-end tests for `commons::verify`, over real directory trees.
//!
//! `tests/data/verify/` is a complete repository (`artifacts/`, `fixtures/`,
//! `fixtures.lock`) that passes every stage: one Anchor decoder (an escrow
//! IDL; every fixture case's bytes are worked out from the IDL by hand,
//! data starting with the 8-byte discriminator) and two models. Each test
//! copies it to a scratch directory and changes what it needs.
//!
//! The tests are in four groups, matching what `verify` must guarantee:
//!
//! 1. **Verdict correctness**: the sample passes; every kind of defect
//!    fails with the check that names it, in the stage that owns it; one
//!    artifact's failure, panic or hang never changes another's verdict;
//!    exit codes are 0, 1 and 3 exactly when they should be.
//! 2. **Engine fidelity**: the in-process engine's tables are PRISM's
//!    writer schemas, hold what PRISM writes, and every model gives the
//!    same rows as LENS's own `QueryEngine` over Parquet files PRISM wrote.
//! 3. **Determinism**: the same artifact and fixture bytes give the same
//!    report byte for byte, anywhere, in any order, on any thread; the
//!    identities in it are the bytes on disk; digests move with outputs
//!    and nothing else.
//! 4. **Output safety and the binary**: contributor text never reaches
//!    the terminal raw, and `commons verify` exits with the report's code
//!    and writes nothing.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use commons::config::{CommonsConfig, DEFAULT_MAX_ARTIFACT_BYTES, LogLevel, OutputFormat};
use commons::fixtures::{
    Chain, FixtureSet, GENERATOR_VERSION, GenSpec, StrictValue, Table, canonical_string, generate, set_hash,
};
use commons::manifest::{self, Artifact, sha256, to_hex};
use commons::verify::digest::{self, CaseKind};
use commons::verify::engine::{self, TypedResult};
use commons::verify::{
    ArtifactReport, BackendError, CheckResult, Deadline, DecoderBackend, InstructionInfo, LoadedDecoder, Outcome,
    Pipeline, PrismBackend, RunReport, SqlInfo, Stage, Status, Verdict, invariants, sql_guard,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const DATA: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/verify");
const PROGRAM: &str = "CiUQWNDZ5Rhn2iULcgoHDagk2GFXrLwCDSknUMv5ioCv";
const DECODER: &str = "experimental/decoder/escrow/1.0.0";
const ACTIVITY: &str = "experimental/model/program-activity/1.0.0";
const FEES: &str = "verified/model/fee-by-status/1.0.0";
/// `sha256sum` of the sample's fixture listing (`fixtures/mod.rs`).
const SAMPLE_SET_HASH: &str = "f2abda4562d3a53796bc2d43ba40f43924b69364a59e8ae0d46e59dffebbb168";
/// `printf 'commons-verify-case v1\ndecoder\n{"data":{"id":1,...},"instruction_type":"make_offer","result":"decoded"}' | sha256sum`.
const MAKE_OFFER_DIGEST: &str = "fdc76c3f4e7ee5e9d3bb917cc087a412eb5173cc28116881332f279e3ae9ea99";
/// The same for fee-by-status over slots 100..=101, rows in canonical order.
const FEES_WINDOW_DIGEST: &str = "276a131c9b41ac6712cda45f1563d02dc7a987156b9654d6cc4f6023da0484a1";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct Repo {
    _tmp: TempDir,
    root: PathBuf,
    config: CommonsConfig,
}

fn copy_tree(from: &Path, to: &Path, reverse: bool) {
    let mut entries: Vec<_> = fs::read_dir(from).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    if reverse {
        entries.reverse();
    }
    for e in entries {
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            fs::create_dir_all(&dest).unwrap();
            copy_tree(&e.path(), &dest, reverse);
        } else {
            fs::copy(e.path(), dest).unwrap();
        }
    }
}

fn config_for(root: &Path) -> CommonsConfig {
    CommonsConfig {
        repo_root: root.to_path_buf(),
        artifacts_dir: root.join("artifacts"),
        fixtures_dir: Some(root.join("fixtures")),
        format: OutputFormat::Text,
        log_level: LogLevel::Info,
        max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        lens_url: None,
        verify_timeout: Duration::from_secs(120),
    }
}

/// A copy of the sample, files created in name order or in reverse.
fn repo_in_order(reverse: bool) -> Repo {
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    copy_tree(Path::new(DATA), &root, reverse);
    let config = config_for(&root);
    Repo { _tmp: tmp, root, config }
}

fn repo() -> Repo {
    repo_in_order(false)
}

impl Repo {
    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    fn read(&self, rel: &str) -> String {
        fs::read_to_string(self.path(rel)).unwrap()
    }

    fn write(&self, rel: &str, text: &str) {
        let p = self.path(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, text).unwrap();
    }

    fn edit(&self, rel: &str, from: &str, to: &str) {
        let s = self.read(rel);
        assert!(s.contains(from), "{rel} does not contain {from:?}");
        self.write(rel, &s.replacen(from, to, 1));
    }

    fn edit_json(&self, rel: &str, f: impl FnOnce(&mut Value)) {
        let mut v: Value = serde_json::from_str(&self.read(rel)).unwrap();
        f(&mut v);
        self.write(rel, &(serde_json::to_string_pretty(&v).unwrap() + "\n"));
    }

    /// Replace an artifact's content file and its manifest hash.
    fn set_content(&self, label: &str, text: &str) {
        let file = if label.contains("/decoder/") { "idl.json" } else { "model.sql" };
        let dir = format!("artifacts/{label}");
        let old = to_hex(&sha256(self.read(&format!("{dir}/{file}")).as_bytes()));
        self.write(&format!("{dir}/{file}"), text);
        self.edit(&format!("{dir}/manifest.json"), &old, &to_hex(&sha256(text.as_bytes())));
    }

    fn edit_idl(&self, f: impl FnOnce(&mut Value)) {
        let mut v: Value = serde_json::from_str(&self.read(&format!("artifacts/{DECODER}/idl.json"))).unwrap();
        f(&mut v);
        self.set_content(DECODER, &(serde_json::to_string_pretty(&v).unwrap() + "\n"));
    }

    fn remove(&self, rel: &str) {
        let p = self.path(rel);
        if p.is_dir() {
            fs::remove_dir_all(p).unwrap();
        } else {
            fs::remove_file(p).unwrap();
        }
    }

    /// Pin the fixture set as it is now.
    fn relock(&self) {
        let mut files = Vec::new();
        collect(&self.path("fixtures"), "fixtures", &mut files);
        let pairs: Vec<(&str, &[u8])> = files.iter().map(|(p, b)| (p.as_str(), b.as_slice())).collect();
        self.write("fixtures.lock", &format!("{}\n", to_hex(&set_hash(&pairs, GENERATOR_VERSION))));
    }

    fn artifacts(&self) -> Vec<Artifact> {
        let dirs = manifest::discover(std::slice::from_ref(&self.config.artifacts_dir), &self.config).unwrap();
        let all: Vec<Artifact> = dirs.iter().map(|d| manifest::load(d, &self.config).unwrap()).collect();
        manifest::check_unique(&all).unwrap();
        manifest::check_unique_paths(&self.config.artifacts_dir).unwrap();
        all
    }

    fn fixtures(&self) -> Arc<FixtureSet> {
        Arc::new(FixtureSet::load(&self.config).unwrap())
    }

    fn verify_with(&self, backend: Arc<dyn DecoderBackend>, timeout: Duration) -> RunReport {
        Pipeline::new(&self.config, self.fixtures(), backend).with_timeout(timeout).run(self.artifacts())
    }

    fn verify(&self) -> RunReport {
        self.verify_with(Arc::new(PrismBackend), Duration::from_secs(120))
    }
}

fn collect(dir: &Path, rel: &str, out: &mut Vec<(String, Vec<u8>)>) {
    for e in fs::read_dir(dir).unwrap() {
        let e = e.unwrap();
        let name = e.file_name().into_string().unwrap();
        let r = format!("{rel}/{name}");
        if e.file_type().unwrap().is_dir() {
            collect(&e.path(), &r, out);
        } else {
            out.push((r, fs::read(e.path()).unwrap()));
        }
    }
}

fn artifact<'a>(r: &'a RunReport, label: &str) -> &'a ArtifactReport {
    r.artifacts
        .iter()
        .find(|a| a.label == label)
        .unwrap_or_else(|| panic!("no report for {label}"))
}

fn stage(a: &ArtifactReport, s: Stage) -> Status {
    a.stages.iter().find(|x| x.stage == s).map_or_else(|| panic!("{} has no {s:?}", a.label), |x| x.status)
}

fn not_passed(a: &ArtifactReport) -> Vec<(Stage, &CheckResult)> {
    a.stages
        .iter()
        .flat_map(|s| s.checks.iter().map(move |c| (s.stage, c)))
        .filter(|(_, c)| !c.passed())
        .collect()
}

/// `a` failed exactly in `at`, with a failing check `id` on `subject` whose
/// detail contains `part`; every later core stage was skipped.
#[track_caller]
fn assert_fails(a: &ArtifactReport, at: Stage, id: &str, subject: &str, part: &str) {
    assert_eq!(a.verdict, Verdict::Fail, "{}: {:#?}", a.label, not_passed(a));
    let bad = not_passed(a);
    assert!(
        bad.iter().any(|(s, c)| *s == at && c.id == id && c.subject == subject && c.status == Status::Fail && c.detail.contains(part)),
        "{}: expected {id} [{subject}] containing {part:?} in {at:?}, got {bad:#?}",
        a.label
    );
    for s in Stage::CORE {
        let st = stage(a, s);
        if s < at {
            assert_eq!(st, Status::Pass, "{}: {s:?}", a.label);
        } else if s == at {
            assert_eq!(st, Status::Fail, "{}: {s:?}", a.label);
        } else {
            assert_eq!(st, Status::Skipped, "{}: {s:?} must not run after a failure", a.label);
        }
    }
}

#[track_caller]
fn assert_passes(a: &ArtifactReport) {
    assert_eq!(a.verdict, Verdict::Pass, "{}: {:#?}", a.label, not_passed(a));
    for s in Stage::CORE {
        assert_eq!(stage(a, s), Status::Pass, "{}: {s:?}", a.label);
    }
}

fn ids(a: &ArtifactReport, s: Stage) -> Vec<(String, String)> {
    a.stages
        .iter()
        .find(|x| x.stage == s)
        .unwrap()
        .checks
        .iter()
        .map(|c| (c.id.clone(), c.subject.clone()))
        .collect()
}

fn decoder_case(_: &Repo, case: &str) -> String {
    format!("fixtures/decoders/{PROGRAM}/{case}.json")
}

// ---------------------------------------------------------------------------
// 1. Verdict correctness
// ---------------------------------------------------------------------------

#[test]
fn the_sample_repository_passes_every_stage() {
    let r = repo();
    let report = r.verify();
    assert_eq!(report.overall, Verdict::Pass, "{}", report.render_text());
    assert_eq!(report.exit_code(), 0);
    assert!(report.errors().is_empty());
    let labels: Vec<&str> = report.artifacts.iter().map(|a| a.label.as_str()).collect();
    assert_eq!(labels, [DECODER, ACTIVITY, FEES]);
    for a in &report.artifacts {
        assert_passes(a);
        assert!(a.run_digest.is_some());
    }

    // The decoder ran every case and every property on every instruction.
    let d = artifact(&report, DECODER);
    let golden = ids(d, Stage::Golden);
    for case in ["configure-fixed-mode", "make-offer", "take-offer", "too-short", "truncated-make-offer", "unknown-instruction"] {
        assert!(golden.contains(&("golden.decoder_case".into(), case.into())), "{case}: {golden:?}");
    }
    let inv = ids(d, Stage::Invariants);
    for ix in ["make_offer", "take_offer", "refund_offer", "configure"] {
        for check in ["invariant.roundtrip", "invariant.trailing_bytes", "invariant.foreign_program"] {
            assert!(inv.contains(&(check.into(), ix.into())), "{check} {ix}: {inv:?}");
        }
    }
    // Instructions with arguments are truncated; those without cannot be.
    assert!(inv.contains(&("invariant.truncation".into(), "configure".into())));
    assert!(!inv.contains(&("invariant.truncation".into(), "take_offer".into())));
    for check in ["invariant.unknown_discriminator", "invariant.deterministic", "invariant.no_panic"] {
        assert!(inv.contains(&(check.into(), String::new())), "{check}");
    }
    assert!(d.digests.contains_key("fuzz"));
    assert_eq!(d.digests.keys().filter(|k| k.starts_with("decoder/")).count(), 6);

    // The models ran every case under both partition settings, and every
    // metamorphic property on every case.
    let a = artifact(&report, ACTIVITY);
    let golden = ids(a, Stage::Golden);
    for case in ["generated-busy", "two-mints-all", "two-mints-window"] {
        assert!(golden.contains(&("golden.model_case".into(), case.into())));
        assert!(golden.contains(&("golden.partition_stable".into(), case.into())));
    }
    let inv = ids(a, Stage::Invariants);
    for case in ["generated-busy", "two-mints-all", "two-mints-window"] {
        for check in ["invariant.row_order", "invariant.bound_independence", "invariant.output_types"] {
            assert!(inv.contains(&(check.into(), case.into())), "{check} {case}");
        }
    }
    assert!(inv.contains(&("invariant.partition_stable".into(), "generated-busy".into())));
    assert!(!inv.contains(&("invariant.partition_stable".into(), "two-mints-window".into())), "explicit chains are covered by golden");
    assert!(inv.contains(&("invariant.bound_not_exercised".into(), "solana.instructions".into())));
    assert_eq!(ids(artifact(&report, FEES), Stage::Reproducible), [("reproducible.digest_stable".into(), String::new())]);
}

#[test]
fn a_wrong_decoder_expectation_fails_golden_and_only_that_artifact() {
    let r = repo();
    r.edit(&decoder_case(&r, "make-offer"), "\"token_a_offered_amount\": 1000000", "\"token_a_offered_amount\": 1000001");
    r.relock();
    let report = r.verify();
    assert_eq!(report.exit_code(), 1);
    assert_fails(
        artifact(&report, DECODER),
        Stage::Golden,
        "golden.decoder_case",
        "make-offer",
        "data differs at token_a_offered_amount: expected 1000001 but got 1000000",
    );
    // Artifacts are independent.
    assert_passes(artifact(&report, ACTIVITY));
    assert_passes(artifact(&report, FEES));
    let errs = report.errors();
    assert_eq!(errs.len(), 1);
    assert_eq!(errs[0].exit_code(), 1);
    assert!(errs[0].to_string().starts_with(&format!("artifact {DECODER}: verify stage golden: golden.decoder_case [make-offer]")));
}

#[test]
fn every_decoder_result_kind_is_checked() {
    for (case, from, to, part) in [
        ("truncated-make-offer", "decode_error", "not_claimed", "expected not_claimed, got a decode error"),
        ("unknown-instruction", "not_claimed", "decode_error", "expected decode_error, got not claimed"),
        ("take-offer", "\"take_offer\"", "\"refund_offer\"", "instruction_type is 'take_offer', expected 'refund_offer'"),
    ] {
        let r = repo();
        r.edit(&decoder_case(&r, case), from, to);
        r.relock();
        assert_fails(artifact(&r.verify(), DECODER), Stage::Golden, "golden.decoder_case", case, part);
    }
}

#[test]
fn a_decoder_reading_the_wrong_layout_fails() {
    // make_offer's id as a u32: every later field shifts by 4 bytes.
    let r = repo();
    r.edit_idl(|v| v["instructions"][0]["args"][0]["type"] = json!("u32"));
    let report = r.verify();
    assert_fails(artifact(&report, DECODER), Stage::Golden, "golden.decoder_case", "make-offer", "data differs at");
}

#[test]
fn static_defects_fail_the_static_stage_and_nothing_else_runs() {
    type Mutation = Box<dyn Fn(&mut Value)>;
    let cases: Vec<(Mutation, &str, &str)> = vec![
        (Box::new(|v| v["instructions"][0]["args"][1]["type"] = json!("f64")), "static.idl_types", "float"),
        (Box::new(|v| v["address"] = json!("11111111111111111111111111111111")), "static.idl_address", "program_id is"),
        (Box::new(|v| v["instructions"][1]["discriminator"] = v["instructions"][0]["discriminator"].clone()), "static.idl_instructions", "also the discriminator of 'make_offer'"),
        (Box::new(|v| v["instructions"][0]["discriminator"] = json!([1, 2, 3])), "static.idl_instructions", "explicit 8-byte"),
        (Box::new(|v| v["extra"] = json!(1)), "static.idl_shape", "unknown key 'extra'"),
        (Box::new(|v| v["metadata"]["spec"] = json!("0.0.1")), "static.idl_shape", "spec 0.1.0"),
        (Box::new(|v| v["instructions"][3]["args"][0]["type"] = json!({"defined": {"name": "Nope"}})), "static.idl_types", "undefined type 'Nope'"),
    ];
    for (mutate, id, part) in cases {
        let r = repo();
        r.edit_idl(|v| mutate(v));
        let report = r.verify();
        assert_fails(artifact(&report, DECODER), Stage::Static, id, "", part);
        assert!(artifact(&report, DECODER).run_digest.is_none(), "nothing ran");
    }

    let base = "SELECT t.status, COUNT(*) AS txs, SUM(t.fee) AS fees\nFROM solana.transactions AS t\n";
    for (sql, id, part, line) in [
        (format!("{base}WHERE t.slot <= {{{{to_slot}}}} AND {{{{from_slot}}}} >= 0\nGROUP BY t.status\n"), "static.sql_bounds", "missing the lower bound", Some(2)),
        ("SELECT *\nFROM solana.transactions AS t\nWHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}}\n".to_string(), "static.sql_shape", "* is not allowed", Some(1)),
        (format!("{base}WHERE t.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}} AND t.slot < now()\nGROUP BY t.status\n"), "static.sql_functions", "now (not deterministic)", Some(3)),
        (format!("{base}WHERE t.slot BETWEEN {{{{from_slot}}}} AND {{{{wallet}}}}\nGROUP BY t.status\n"), "static.sql_placeholders", "{{wallet}} is not allowed", Some(3)),
        (format!("{base}WHERE t.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}} AND t.status = 'ſuccess'\nGROUP BY t.status\n"), "static.sql_text", "U+017F", Some(3)),
        (format!("{base}JOIN solana.accounts a ON a.slot = t.slot\nWHERE t.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}}\nGROUP BY t.status\n"), "static.sql_tables", "solana.accounts", Some(3)),
        (format!("{base}WHERE t.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}}\nGROUP BY t.status\nLIMIT 1\n"), "static.sql_shape", "without ORDER BY", None),
    ] {
        let r = repo();
        r.set_content(FEES, &sql);
        let report = r.verify();
        let a = artifact(&report, FEES);
        assert_fails(a, Stage::Static, id, "", part);
        if let Some(l) = line {
            let c = a.stages[0].checks.iter().find(|c| c.id == id && !c.passed()).unwrap();
            assert_eq!(c.excerpt.as_ref().map(|e| e.line), Some(l), "{sql}: the excerpt points at the line");
        }
        assert_passes(artifact(&report, DECODER));
        assert_passes(artifact(&report, ACTIVITY));
    }
}

#[test]
fn model_expectations_are_exact() {
    let activity = "fixtures/models/program-activity/two-mints-window.json";
    let r = repo();
    r.edit_json(activity, |v| v["expect"]["rows"][0][1] = json!(3));
    r.relock();
    assert_fails(
        artifact(&r.verify(), ACTIVITY),
        Stage::Golden,
        "golden.model_case",
        "two-mints-window",
        "row 0 is [\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",2], expected [\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\",3]",
    );

    // Order is an output only when the model orders.
    let r = repo();
    r.edit_json(activity, |v| {
        let rows = v["expect"]["rows"].as_array_mut().unwrap();
        rows.reverse();
    });
    r.relock();
    assert_fails(artifact(&r.verify(), ACTIVITY), Stage::Golden, "golden.model_case", "two-mints-window", "row 0 is");
    let fees = "fixtures/models/fee-by-status/two-mints-window.json";
    let r = repo();
    r.edit_json(fees, |v| v["expect"]["rows"].as_array_mut().unwrap().reverse());
    r.relock();
    assert_passes(artifact(&r.verify(), FEES));
    let r = repo();
    r.edit_json(fees, |v| v["expect"]["ordered"] = json!(true));
    r.relock();
    assert_fails(artifact(&r.verify(), FEES), Stage::Golden, "golden.model_case", "two-mints-window", "no top-level ORDER BY");

    // Columns are exact, names and order.
    let r = repo();
    r.edit_json(fees, |v| v["expect"]["columns"] = json!(["status", "fees", "txs"]));
    r.relock();
    assert_fails(artifact(&r.verify(), FEES), Stage::Golden, "golden.model_case", "two-mints-window", "columns are [status, txs, fees]");
}

#[test]
fn outputs_that_fixtures_cannot_pin_are_rejected() {
    let r = repo();
    r.set_content(
        FEES,
        "SELECT t.status, to_timestamp_seconds(MIN(t.slot)) AS first_slot\nFROM solana.transactions AS t\n\
         WHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}}\nGROUP BY t.status\n",
    );
    assert_fails(
        artifact(&r.verify(), FEES),
        Stage::Golden,
        "golden.model_case",
        "generated-busy",
        "a model outputs only booleans, integers and strings",
    );
}

#[test]
fn evidence_quality_is_part_of_the_verdict() {
    // No case at all.
    let r = repo();
    r.remove(&format!("fixtures/decoders/{PROGRAM}"));
    r.relock();
    assert_fails(artifact(&r.verify(), DECODER), Stage::Golden, "golden.no_cases", "", "no fixture case");

    // Only generated chains: no reviewed expectation.
    let r = repo();
    r.remove("fixtures/models/fee-by-status/two-mints-window.json");
    r.relock();
    let report = r.verify();
    let a = artifact(&report, FEES);
    assert_fails(a, Stage::Golden, "golden.no_explicit_case", "", "explicit, reviewed chain");
    assert_fails(a, Stage::Golden, "golden.no_expected_rows", "", "non-empty output");

    // Windows that leave nothing out cannot show the model ignores what is
    // outside them.
    let r = repo();
    r.edit_json("fixtures/models/fee-by-status/two-mints-window.json", |v| {
        v["window"]["to_slot"] = json!(102);
        v["expect"]["rows"] = json!([["success", 2, 10000], ["failed", 1, 5000]]);
    });
    r.edit_json("fixtures/models/fee-by-status/generated-busy.json", |v| {
        v["window"] = json!({"from_slot": 1000, "to_slot": 1119});
    });
    r.relock();
    assert_fails(
        artifact(&r.verify(), FEES),
        Stage::Invariants,
        "invariant.bound_not_exercised",
        "solana.transactions",
        "no case's chain has transactions rows outside its window",
    );
}

fn activity_cases(r: &Repo) -> (Arc<FixtureSet>, Vec<commons::fixtures::ModelCase>) {
    let f = r.fixtures();
    let cases = f.model_cases("program-activity").to_vec();
    (f, cases)
}

#[test]
fn the_metamorphic_checks_catch_what_the_guard_cannot() {
    let r = repo();
    let (fixtures, cases) = activity_cases(&r);
    let deadline = Deadline::after(Duration::from_secs(120));

    // Passes the guard, but ROW_NUMBER over slot has ties within a slot:
    // which instruction gets which number depends on input order.
    let ties = "SELECT i.program_id, ROW_NUMBER() OVER (ORDER BY i.slot) AS rn\n\
                FROM solana.instructions AS i\nWHERE i.slot BETWEEN {{from_slot}} AND {{to_slot}}\n";
    let info = sql_guard::analyse(ties).expect("the guard accepts it");
    let run = invariants::models(&fixtures, &cases, ties, &info, 7, &deadline);
    let row_order = run.checks.iter().find(|c| c.id == "invariant.row_order" && c.subject == "generated-busy").unwrap();
    assert_eq!(row_order.status, Status::Fail, "{row_order:?}");
    assert!(row_order.detail.contains("shuffling the input rows changes the result"), "{}", row_order.detail);

    // A bound the guard would refuse, run anyway: removing the rows
    // outside the window changes the answer, which is the proof the guard
    // stands in for.
    let leaky = "SELECT COUNT(*) AS n FROM solana.instructions AS i WHERE i.slot <= {{to_slot}} AND {{from_slot}} >= 0";
    assert!(sql_guard::analyse(leaky).is_err());
    let info = SqlInfo {
        columns: vec!["n".into()],
        tables: [Table::Instructions].into_iter().collect(),
        ordered: false,
        functions: ["count".to_string()].into_iter().collect(),
    };
    let run = invariants::models(&fixtures, &cases, leaky, &info, 7, &deadline);
    let bound = run.checks.iter().find(|c| c.id == "invariant.bound_independence" && c.subject == "generated-busy").unwrap();
    assert_eq!(bound.status, Status::Fail);
    assert!(bound.detail.contains("reads outside its window"), "{}", bound.detail);
    // On a case whose window starts at the chain's first slot, the same leak
    // is invisible: which is why bound_not_exercised exists.
    let edge = run.checks.iter().find(|c| c.id == "invariant.bound_independence" && c.subject == "two-mints-window").unwrap();
    assert_eq!(edge.status, Status::Pass);
}

// Backends that misbehave, to show isolation.

struct Delegate {
    inner: Box<dyn LoadedDecoder>,
    on_run: fn(&[u8]) -> Option<Outcome>,
}

impl LoadedDecoder for Delegate {
    fn instructions(&self) -> Vec<InstructionInfo> {
        self.inner.instructions()
    }

    fn run(&self, program_id: &[u8; 32], data: &[u8], accounts: &[[u8; 32]]) -> Outcome {
        (self.on_run)(data).unwrap_or_else(|| self.inner.run(program_id, data, accounts))
    }
}

/// PRISM, except: `on_run` overrides any call it answers, and `on_load`
/// may refuse or panic on the n-th load (0 is the static stage's).
struct Faulty {
    loads: AtomicUsize,
    on_load: fn(usize),
    on_run: fn(&[u8]) -> Option<Outcome>,
    from_load: usize,
}

impl Faulty {
    fn new(on_load: fn(usize), on_run: fn(&[u8]) -> Option<Outcome>, from_load: usize) -> Arc<Faulty> {
        Arc::new(Faulty { loads: AtomicUsize::new(0), on_load, on_run, from_load })
    }
}

impl DecoderBackend for Faulty {
    fn load(&self, idl: &[u8], program_id: &[u8; 32]) -> Result<Box<dyn LoadedDecoder>, BackendError> {
        let n = self.loads.fetch_add(1, Ordering::SeqCst);
        (self.on_load)(n);
        let inner = PrismBackend.load(idl, program_id)?;
        let on_run = if n >= self.from_load { self.on_run } else { |_: &[u8]| None };
        Ok(Box::new(Delegate { inner, on_run }))
    }
}

fn disc(name: &str) -> [u8; 8] {
    sha256(format!("global:{name}").as_bytes())[..8].try_into().unwrap()
}

#[test]
fn a_panic_in_the_decoder_fails_only_its_check_and_its_artifact() {
    let r = repo();
    let backend = Faulty::new(|_| {}, |d| (d == disc("take_offer")).then(|| panic!("boom")), 0);
    let report = r.verify_with(backend, Duration::from_secs(120));
    assert_fails(artifact(&report, DECODER), Stage::Golden, "golden.decoder_case", "take-offer", "got a panic");
    assert_passes(artifact(&report, ACTIVITY));
    assert_passes(artifact(&report, FEES));
    assert_eq!(report.exit_code(), 1);

    let r = repo();
    let report = r.verify_with(Faulty::new(|_| panic!("no"), |_| None, 0), Duration::from_secs(120));
    assert_fails(artifact(&report, DECODER), Stage::Static, "static.idl_loads", "", "panicked");
    assert_passes(artifact(&report, FEES));
}

#[test]
fn a_hang_is_cut_off_at_the_deadline_and_the_next_artifact_still_runs() {
    let r = repo();
    let backend = Faulty::new(
        |_| {},
        |d| {
            if d == disc("take_offer") {
                std::thread::sleep(Duration::from_secs(3600));
            }
            None
        },
        0,
    );
    let started = std::time::Instant::now();
    let report = r.verify_with(backend, Duration::from_secs(10));
    let a = artifact(&report, DECODER);
    assert_fails(a, Stage::Golden, "deadline.exceeded", "", "did not finish within 10 s");
    assert_eq!(stage(a, Stage::Static), Status::Pass, "finished stages are kept");
    assert_passes(artifact(&report, ACTIVITY));
    assert_passes(artifact(&report, FEES));
    assert!(started.elapsed() < Duration::from_secs(60), "the hang did not hold up the run");
}

#[test]
fn state_that_changes_between_runs_fails_reproducible() {
    // The third load (reproducible's) gives another amount for make-offer.
    let r = repo();
    let make: fn(&[u8]) -> Option<Outcome> = |d| {
        (d.len() == 32 && d.starts_with(&disc("make_offer"))).then(|| Outcome::Decoded {
            instruction_type: "make_offer".into(),
            data_json: r#"{"id":1,"token_a_offered_amount":999,"token_b_wanted_amount":2500000}"#.into(),
        })
    };
    let report = r.verify_with(Faulty::new(|_| {}, make, 2), Duration::from_secs(120));
    let a = artifact(&report, DECODER);
    assert_fails(a, Stage::Reproducible, "reproducible.digest_stable", "", "decoder/make-offer");
    assert_eq!(report.exit_code(), 1);
}

#[test]
fn a_verifier_fault_is_an_error_with_exit_code_3() {
    // Loads once for the static stage, then refuses: not the artifact's
    // fault, so not a failure.
    let r = repo();
    let report = r.verify_with(Faulty::new(|n| assert!(n == 0, "load {n}"), |_| None, 0), Duration::from_secs(120));
    let a = artifact(&report, DECODER);
    assert_eq!(a.verdict, Verdict::Error, "{:#?}", not_passed(a));
    assert_eq!(report.overall, Verdict::Error);
    assert_eq!(report.exit_code(), 3);
    assert!(report.errors().iter().any(|e| e.exit_code() == 3));
    assert_passes(artifact(&report, FEES));
}

// ---------------------------------------------------------------------------
// 2. Engine fidelity
// ---------------------------------------------------------------------------

/// Arrow types of both arrow versions as one spelling.
fn prism_type(t: &arrow_schema::DataType) -> String {
    use arrow_schema::{DataType as D, TimeUnit};
    match t {
        D::UInt64 => "UInt64".into(),
        D::UInt32 => "UInt32".into(),
        D::UInt8 => "UInt8".into(),
        D::Utf8 => "Utf8".into(),
        D::Timestamp(TimeUnit::Second, Some(tz)) => format!("Timestamp(s,{tz})"),
        other => format!("other {other:?}"),
    }
}

fn engine_type(t: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::{DataType as D, TimeUnit};
    match t {
        D::UInt64 => "UInt64".into(),
        D::UInt32 => "UInt32".into(),
        D::UInt8 => "UInt8".into(),
        D::Utf8 => "Utf8".into(),
        D::Timestamp(TimeUnit::Second, Some(tz)) => format!("Timestamp(s,{tz})"),
        other => format!("other {other:?}"),
    }
}

#[test]
fn the_engine_tables_are_prisms_writer_schemas() {
    use prism::storage::TableKind;
    use prism::storage::writer::schema_for;
    for (t, k) in [
        (Table::Slots, TableKind::Slots),
        (Table::Transactions, TableKind::Transactions),
        (Table::Instructions, TableKind::Instructions),
        (Table::TokenTransfers, TableKind::TokenTransfers),
    ] {
        let ours = engine::table_schema(t);
        let theirs = schema_for(k);
        let a: Vec<(String, String, bool)> =
            ours.fields().iter().map(|f| (f.name().clone(), engine_type(f.data_type()), f.is_nullable())).collect();
        let b: Vec<(String, String, bool)> =
            theirs.fields().iter().map(|f| (f.name().clone(), prism_type(f.data_type()), f.is_nullable())).collect();
        assert_eq!(a, b, "{t:?}");
    }
}

#[test]
fn the_engine_is_the_datafusion_lens_links() {
    // One Cargo.lock for the workspace, so one DataFusion: LENS's.
    let lock = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../Cargo.lock")).unwrap();
    let versions: Vec<&str> = lock
        .split("[[package]]")
        .filter(|p| p.contains("\nname = \"datafusion\"\n"))
        .filter_map(|p| p.lines().find_map(|l| l.strip_prefix("version = \"")?.strip_suffix('"')))
        .collect();
    assert_eq!(versions, [engine::ENGINE_VERSION], "exactly one datafusion in the build");
    assert_eq!(datafusion::DATAFUSION_VERSION, engine::ENGINE_VERSION);
}

/// `chain` as PRISM's writer lays it out, under `dir`.
fn prism_write(dir: &Path, chain: &Chain) {
    use prism::storage::{ParquetWriter, Storage};
    use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
    let json = |v: &StrictValue| -> serde_json::Value { serde_json::from_str(&canonical_string(v)).unwrap() };
    let mut w = ParquetWriter::new(dir);
    for s in &chain.slots {
        let txs: Vec<DeepAnaTransaction> = chain
            .transactions
            .iter()
            .filter(|t| t.slot == s.slot)
            .map(|t| DeepAnaTransaction {
                slot: t.slot,
                blockhash: t.blockhash.clone(),
                transaction_index: t.transaction_index,
                tx_signature: t.tx_signature.clone(),
                fee: t.fee,
                status: if t.status.as_str() == "failed" { TransactionStatus::Failed } else { TransactionStatus::Success },
                err: t.err.clone(),
                accounts: t.accounts.clone(),
                log_messages: t.log_messages.clone(),
                compute_units_consumed: t.compute_units_consumed,
            })
            .collect();
        let ixs: Vec<DeepAnaInstruction> = chain
            .instructions
            .iter()
            .filter(|x| x.slot == s.slot)
            .map(|x| DeepAnaInstruction {
                slot: x.slot,
                tx_signature: x.tx_signature.clone(),
                instruction_index: x.instruction_index,
                inner_instruction_index: x.inner_instruction_index,
                program_id: x.program_id.clone(),
                accounts: x.accounts.clone(),
                data_base58: x.data_base58.clone(),
                instruction_type: x.instruction_type.clone(),
                decoded_data: x.decoded_data.as_ref().map(json),
            })
            .collect();
        let transfers: Vec<DeepAnaTokenTransfer> = chain
            .token_transfers
            .iter()
            .filter(|x| x.slot == s.slot)
            .map(|x| DeepAnaTokenTransfer {
                slot: x.slot,
                tx_signature: x.tx_signature.clone(),
                instruction_index: x.instruction_index,
                inner_instruction_index: x.inner_instruction_index,
                spl_token_program_id: x.spl_token_program_id.clone(),
                source_token_account: x.source_token_account.clone(),
                destination_token_account: x.destination_token_account.clone(),
                source_owner: x.source_owner.clone(),
                destination_owner: x.destination_owner.clone(),
                mint_token_address: x.mint_token_address.clone(),
                amount_raw: x.amount_raw,
                decimals: x.decimals,
                block_time: x.block_time,
            })
            .collect();
        let slot = DeepAnaSlot {
            slot: s.slot,
            blockhash: s.blockhash.clone(),
            parent_slot: s.parent_slot,
            block_time: s.block_time,
            block_height: s.block_height,
            transaction_count: s.transaction_count,
            leader: s.leader.clone(),
        };
        w.append(&slot, &txs, &ixs, &transfers).unwrap();
    }
    w.flush().unwrap();
}

/// LENS over PRISM's files for `chain`, and a way to run rendered SQL on it.
struct Lens {
    _dir: TempDir,
    engine: lens::QueryEngine,
    rt: tokio::runtime::Runtime,
}

impl Lens {
    fn over(chain: &Chain) -> Lens {
        let dir = tempfile::tempdir().unwrap();
        prism_write(dir.path(), chain);
        let data = dir.path().as_os_str().to_owned();
        let config = lens::LensConfig::load_with(|name| (name == "LENS_DATA_DIR").then(|| data.clone())).unwrap();
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
        let engine = rt.block_on(async { lens::QueryEngine::new(Arc::new(config)) }).unwrap();
        Lens { _dir: dir, engine, rt }
    }

    fn try_run(&self, sql: &str, from: u64, to: u64) -> Result<TypedResult, String> {
        let rendered = sql_guard::render(sql, from, to).unwrap();
        let res = self.rt.block_on(self.engine.execute(&rendered)).map_err(|e| format!("LENS: {rendered}: {e}"))?;
        assert!(!res.truncated);
        engine::typed_result(&res.schema, &res.batches).map_err(|e| e.to_string())
    }

    fn run(&self, sql: &str, from: u64, to: u64) -> TypedResult {
        self.try_run(sql, from, to).unwrap_or_else(|e| panic!("{e}"))
    }
}

fn fidelity_chain() -> Chain {
    generate(&GenSpec {
        seed: 21,
        start_slot: 5000,
        slots: 80,
        skip_percent: 10,
        max_tx_per_slot: 5,
        fail_percent: 15,
        transfer_percent: 50,
        mint_null_percent: 25,
    })
}

/// Every column of `t`, with block_time as seconds (a timestamp is not a
/// model output), in natural-key order.
fn select_all(t: Table) -> String {
    let cols: Vec<String> = t
        .columns()
        .iter()
        .map(|c| if c.name == "block_time" { "to_unixtime(x.block_time) AS block_time".into() } else { format!("x.{}", c.name) })
        .collect();
    let key = match t {
        Table::Slots => "x.slot",
        Table::Transactions => "x.slot, x.tx_signature",
        _ => "x.slot, x.tx_signature, x.instruction_index, x.inner_instruction_index NULLS FIRST",
    };
    format!(
        "SELECT {} FROM solana.{} AS x WHERE x.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}} ORDER BY {key}",
        cols.join(", "),
        t.as_str()
    )
}

#[test]
fn the_engine_tables_hold_exactly_what_prism_writes_and_lens_reads() {
    for chain in [fidelity_chain(), {
        let r = repo();
        let f = r.fixtures();
        (*f.resolve_chain(&commons::fixtures::ChainRef::Explicit("two-mints-one-failed".into())).unwrap()).clone()
    }] {
        let lens = Lens::over(&chain);
        for t in Table::ALL {
            let sql = select_all(t);
            let ours = engine::run_model(&chain, &sql, 0, u64::MAX, 1).unwrap();
            let theirs = lens.run(&sql, 0, u64::MAX);
            assert_eq!(ours.rows.len(), chain.row_counts()[Table::ALL.iter().position(|x| *x == t).unwrap()]);
            assert_eq!(ours, theirs, "{t:?}: every value, every JSON byte, in order");
        }
    }
}

#[test]
fn models_give_the_same_rows_in_process_and_on_lens() {
    let chain = fidelity_chain();
    let lens = Lens::over(&chain);
    let w = |t: &str| format!("{t}.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}}");
    let mut models: Vec<(String, bool)> = vec![
        (fs::read_to_string(format!("{DATA}/artifacts/{ACTIVITY}/model.sql")).unwrap(), true),
        (fs::read_to_string(format!("{DATA}/artifacts/{FEES}/model.sql")).unwrap(), false),
    ];
    models.extend([
        // Joins across three tables.
        (format!(
            "SELECT t.status, x.spl_token_program_id AS token_program, COUNT(*) AS n, SUM(x.amount_raw) AS amount\n\
             FROM solana.transactions t JOIN solana.instructions i ON i.slot = t.slot AND i.tx_signature = t.tx_signature\n\
             JOIN solana.token_transfers x ON x.slot = i.slot AND x.tx_signature = i.tx_signature\n\
               AND x.instruction_index = i.instruction_index AND x.inner_instruction_index IS NOT DISTINCT FROM i.inner_instruction_index\n\
             WHERE {} AND {} AND {}\nGROUP BY t.status, x.spl_token_program_id",
            w("t"), w("i"), w("x")
        ), false),
        // A CTE, a window function over a total order, a string function.
        (format!(
            "WITH f AS (SELECT t.slot, t.transaction_index, t.fee FROM solana.transactions t WHERE {})\n\
             SELECT f.slot, f.transaction_index, f.fee, RANK() OVER (PARTITION BY f.slot ORDER BY f.fee DESC, f.transaction_index) AS r\n\
             FROM f ORDER BY f.slot, f.transaction_index",
            w("t")
        ), true),
        // A derived table, UNION ALL, CASE, IN (subquery), substr, LIMIT
        // under ORDER BY.
        (format!(
            "SELECT q.p, q.depth_kind, COUNT(*) AS n FROM (\n\
               SELECT substr(i.program_id, 1, 6) AS p, CASE WHEN i.inner_instruction_index IS NULL THEN 'top' ELSE 'inner' END AS depth_kind\n\
               FROM solana.instructions i WHERE {} AND i.tx_signature IN (SELECT t.tx_signature FROM solana.transactions t WHERE {} AND t.status = 'failed')\n\
             ) AS q GROUP BY q.p, q.depth_kind\n\
             UNION ALL\nSELECT 'slots' AS p, 'all' AS depth_kind, COUNT(*) AS n FROM solana.slots s WHERE {}\nORDER BY p, depth_kind LIMIT 50",
            w("i"), w("t"), w("s")
        ), true),
        // Nulls, coalesce, a correlated EXISTS, integer arithmetic.
        (format!(
            "SELECT s.slot, coalesce(s.leader, '-') AS leader, s.transaction_count * 2 + 1 AS odd\n\
             FROM solana.slots s WHERE {} AND EXISTS (SELECT 1 FROM solana.token_transfers x WHERE {} \
             AND x.slot = s.slot AND x.mint_token_address IS NULL)",
            w("s"), w("x")
        ), false),
    ]);
    for (sql, ordered) in &models {
        let info = sql_guard::analyse(sql).unwrap_or_else(|e| panic!("{sql}\n{e:#?}"));
        assert_eq!(info.ordered, *ordered, "{sql}");
        for (from, to) in [(5010, 5050), (0, u64::MAX), (5079, 5079)] {
            let theirs = lens.run(sql, from, to);
            for p in [1, 4] {
                let ours = engine::run_model(&chain, sql, from, to, p).unwrap();
                assert_eq!(theirs.same_as(&ours, *ordered), Ok(()), "{sql}\nwindow {from}..={to}, {p} partitions");
                assert_eq!(ours.outcome(*ordered), theirs.outcome(*ordered), "the same digest");
            }
        }
    }

    // What production cannot run, the engine cannot either: DataFusion
    // 55.2 does not plan EXISTS in a SELECT list. The guard accepts it; the
    // model then fails golden, as it would fail on LENS.
    let unsupported = format!(
        "SELECT s.slot, EXISTS (SELECT 1 FROM solana.token_transfers x WHERE {} AND x.slot = s.slot) AS moved\n\
         FROM solana.slots s WHERE {}",
        w("x"),
        w("s")
    );
    sql_guard::analyse(&unsupported).unwrap();
    assert!(lens.try_run(&unsupported, 0, u64::MAX).is_err());
    assert!(engine::run_model(&chain, &unsupported, 0, u64::MAX, 1).is_err());
}

// ---------------------------------------------------------------------------
// 3. Determinism
// ---------------------------------------------------------------------------

#[test]
fn the_same_bytes_give_the_same_report_byte_for_byte() {
    let a = repo_in_order(false);
    let b = repo_in_order(true);
    let ra = a.verify();
    let rb = b.verify();
    assert_eq!(ra.render_json(), rb.render_json(), "another directory, another creation order");
    assert_eq!(ra.render_text(), rb.render_text());
    assert_eq!(ra, rb);
    let again = a.verify();
    assert_eq!(ra.render_json(), again.render_json(), "the same directory, again");
    // Nothing machine-specific leaks in.
    let j = ra.render_json();
    assert!(!j.contains(a.root.to_str().unwrap()) && !j.contains(b.root.to_str().unwrap()), "no absolute path");
}

#[test]
fn every_artifact_report_is_independent_of_the_others() {
    let r = repo();
    let fixtures = r.fixtures();
    let all = r.artifacts();
    let pipeline = Pipeline::new(&r.config, fixtures, Arc::new(PrismBackend));
    let together = pipeline.run(all.clone());
    let mut reversed = all.clone();
    reversed.reverse();
    let backwards = pipeline.run(reversed);
    for a in &together.artifacts {
        assert_eq!(artifact(&backwards, &a.label), a, "{}", a.label);
        let alone = pipeline.run(all.iter().filter(|x| x.label() == a.label).cloned().collect());
        assert_eq!(&alone.artifacts[0], a, "{} alone", a.label);
    }
}

#[test]
fn verification_is_the_same_on_every_thread() {
    let r = Arc::new(repo());
    let expected = r.verify().render_json();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let r = Arc::clone(&r);
            std::thread::spawn(move || r.verify().render_json())
        })
        .collect();
    for h in handles {
        assert_eq!(h.join().unwrap(), expected);
    }
}

#[test]
fn the_identities_in_a_report_are_the_bytes_on_disk() {
    let r = repo();
    let report = r.verify();
    // Fixture set: the lock, the documented sha256sum listing, the report.
    assert_eq!(to_hex(&report.fixture_set_hash), SAMPLE_SET_HASH);
    assert_eq!(r.read("fixtures.lock").trim_end(), SAMPLE_SET_HASH);
    let mut files = Vec::new();
    collect(&r.path("fixtures"), "fixtures", &mut files);
    files.sort();
    let mut listing = String::from("commons-fixtures v1\ngenerator 1\n");
    for (p, b) in &files {
        listing.push_str(&format!("{}  {p}\n", to_hex(&sha256(b))));
    }
    assert_eq!(to_hex(&sha256(listing.as_bytes())), SAMPLE_SET_HASH, "recomputed independently");
    // Artifacts: content hash is the raw file, id is the canonical manifest.
    for a in r.artifacts() {
        let rep = artifact(&report, &a.label());
        let file = if a.manifest.kind == manifest::Kind::Decoder { "idl.json" } else { "model.sql" };
        let bytes = fs::read(r.path(&format!("artifacts/{}/{file}", a.label()))).unwrap();
        assert_eq!(rep.content_sha256, sha256(&bytes));
        assert_eq!(rep.artifact_id, a.manifest.artifact_id());
        assert_eq!(rep.artifact_id.0, sha256(&a.manifest.canonical_bytes()));
    }
    // Case digests, computed outside Rust.
    assert_eq!(to_hex(&artifact(&report, DECODER).digests["decoder/make-offer"]), MAKE_OFFER_DIGEST);
    assert_eq!(to_hex(&artifact(&report, FEES).digests["model/two-mints-window"]), FEES_WINDOW_DIGEST);
}

/// Recorded from this implementation: a regression pin. Any change to an
/// output, to the fuzz generator's draws, or to the digest format moves
/// one, and must be a reviewed change of `VERIFIER_VERSION`. (The digest
/// format itself is pinned outside Rust by the case digests above.)
#[test]
fn run_digests_are_pinned() {
    let report = repo().verify();
    let pinned = [
        (DECODER, "1389259c57fad95c773ef75fb027871cedc795b296d9604be9787d663aaf88eb"),
        (ACTIVITY, "d85c4f8375da2b690b92561de1f1ca64a3931c3eb677e5a30347719c8444001a"),
        (FEES, "0aa44eb857610fdb430d9584b43f8b02df98f8670fb4e2096bb41fbe3c821ad9"),
    ];
    for (label, hex) in pinned {
        let a = artifact(&report, label);
        assert_eq!(a.run_digest.map(|d| to_hex(&d)).as_deref(), Some(hex), "{label}");
        assert_eq!(a.run_digest, Some(digest::run_digest(&a.digests)), "{label}: the run digest is of the case digests");
    }
}

#[test]
fn digests_move_with_outputs_and_nothing_else() {
    let base = repo().verify();

    // A new description is a new artifact id, so a new fuzz seed: the
    // fuzz inputs and their digest change, every case digest does not, and
    // the verdict does not.
    let r = repo();
    r.edit(&format!("artifacts/{DECODER}/manifest.json"), "Decodes the escrow example program.", "Decodes the escrow example.");
    r.edit(&format!("artifacts/{FEES}/manifest.json"), "Transactions and fees by status.", "Fees by status.");
    let moved = r.verify();
    let (d0, d1) = (artifact(&base, DECODER), artifact(&moved, DECODER));
    assert_ne!(d0.artifact_id, d1.artifact_id);
    assert_passes(d1);
    assert_ne!(d0.digests["fuzz"], d1.digests["fuzz"], "the seed moved");
    for (k, v) in &d0.digests {
        if k != "fuzz" {
            assert_eq!(&d1.digests[k], v, "{k}: the same output");
        }
    }
    // A model's invariant outputs are its results, so nothing moves at all.
    let (f0, f1) = (artifact(&base, FEES), artifact(&moved, FEES));
    assert_ne!(f0.artifact_id, f1.artifact_id);
    assert_eq!(f0.digests, f1.digests);
    assert_eq!(f0.run_digest, f1.run_digest);

    // Reformatting a fixture changes the pinned set, never an output.
    let r = repo();
    let case = decoder_case(&r, "make-offer");
    let compact: Value = serde_json::from_str(&r.read(&case)).unwrap();
    r.write(&case, &(compact.to_string() + "\n"));
    r.relock();
    let reformatted = r.verify();
    assert_ne!(reformatted.fixture_set_hash, base.fixture_set_hash);
    for a in &base.artifacts {
        assert_eq!(artifact(&reformatted, &a.label).digests, a.digests, "{}", a.label);
    }

    // A changed output moves exactly its case digest.
    let r = repo();
    r.set_content(FEES, &r.read(&format!("artifacts/{FEES}/model.sql")).replace("SUM(t.fee)", "MAX(t.fee)"));
    let changed = r.verify();
    let a = artifact(&changed, FEES);
    assert_passes(a); // 5000 is both the sum and the max of one fee.
    let differ = digest::differing_keys(&f0.digests, &a.digests);
    assert_eq!(differ, ["invariants", "model/generated-busy"], "only the outputs that changed");
}

#[test]
fn case_digests_ignore_partitions_and_input_row_order() {
    let r = repo();
    let fixtures = r.fixtures();
    let sql = r.read(&format!("artifacts/{FEES}/model.sql"));
    for case in fixtures.model_cases("fee-by-status") {
        let chain = fixtures.resolve_chain(&case.chain).unwrap();
        let mut shuffled = (*chain).clone();
        shuffled.transactions.reverse();
        let digests: Vec<[u8; 32]> = [(&*chain, 1), (&*chain, 4), (&shuffled, 1), (&shuffled, 7)]
            .iter()
            .map(|(c, p)| {
                let res = engine::run_model(c, &sql, case.from_slot, case.to_slot, *p).unwrap();
                digest::case_digest(CaseKind::Model, &res.outcome(false))
            })
            .collect();
        assert!(digests.windows(2).all(|w| w[0] == w[1]), "{}", case.name);
    }
}

// ---------------------------------------------------------------------------
// 4. Output safety and the binary
// ---------------------------------------------------------------------------

#[test]
fn contributor_text_never_reaches_the_terminal_raw() {
    let r = repo();
    r.edit_idl(|v| v["\u{202E}\u{200B}evil"] = json!(1));
    let report = r.verify();
    assert_fails(artifact(&report, DECODER), Stage::Static, "static.idl_shape", "", "\\u{202E}\\u{200B}evil");
    for out in [report.render_text(), report.render_json()] {
        assert!(!out.contains('\u{202E}') && !out.contains('\u{200B}') && !out.contains('\u{1b}'), "{out}");
        assert!(out.chars().all(|c| !c.is_control() || c == '\n'));
    }
    assert!(report.render_json().is_ascii());
}

/// Every path under `root` with its bytes, for a before/after comparison.
fn snapshot(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut files = Vec::new();
    collect(root, "", &mut files);
    files.into_iter().collect()
}

#[test]
fn the_binary_prints_the_report_exits_with_its_code_and_writes_nothing() {
    let bin = env!("CARGO_BIN_EXE_commons");
    let r = repo();
    let before = snapshot(&r.root);
    let out = std::process::Command::new(bin)
        .args(["verify", r.path("artifacts").to_str().unwrap(), "--repo-root", r.root.to_str().unwrap(), "--format", "json"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", String::from_utf8_lossy(&out.stderr));
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["overall"], "pass");
    assert_eq!(v["artifacts"].as_array().unwrap().len(), 3);
    assert_eq!(snapshot(&r.root), before, "verify writes no file");
    // The same JSON the library renders.
    assert_eq!(String::from_utf8(out.stdout).unwrap(), r.verify().render_json());

    // One failing artifact: exit 1, text by default.
    r.edit(&decoder_case(&r, "take-offer"), "\"take_offer\"", "\"refund_offer\"");
    r.relock();
    let out = std::process::Command::new(bin)
        .args(["verify", r.path(&format!("artifacts/{DECODER}")).to_str().unwrap(), "--repo-root", r.root.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.starts_with("commons verify: FAIL\n"), "{text}");
    assert!(text.contains("FAIL golden.decoder_case [take-offer]"), "{text}");

    // A fixture set the lock does not pin: exit 1 before anything runs.
    r.write(&decoder_case(&r, "take-offer"), &r.read(&decoder_case(&r, "take-offer")).replace("refund_offer", "take_offer"));
    let out = std::process::Command::new(bin)
        .args(["verify", r.path("artifacts").to_str().unwrap(), "--repo-root", r.root.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("fixtures.lock"));

    // A bad invocation: exit 2.
    let out = std::process::Command::new(bin)
        .args(["verify", r.path("artifacts/nope").to_str().unwrap(), "--repo-root", r.root.to_str().unwrap()])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}
