//! [`RunReport`]: what `verify` found, its exit code, and its text and JSON
//! forms.
//!
//! # Verdicts
//!
//! Every check ends [`Status::Pass`], [`Status::Fail`] (the artifact or its
//! evidence is wrong: exit 1) or [`Status::Error`] (the verifier or the
//! machine is: exit 3). A stage is the worst of its checks, or
//! [`Status::Skipped`] when an earlier stage did not pass. An artifact's
//! [`Verdict`] is the worst of its stages, and the run's is the worst of its
//! artifacts. [`RunReport::exit_code`] maps it to the crate's exit codes.
//!
//! # Output is safe by construction
//!
//! Every string a report holds went through [`clean`] when it was stored:
//! control and invisible format characters become `\u{XXXX}`, and length
//! is bounded. Contributor-derived pieces are also [`echo`]ed by whoever
//! builds the message, so they are unambiguous. On top of that,
//! [`RunReport::render_json`] is pure ASCII ([`json_quote`]) and
//! [`RunReport::render_text`] runs [`clean`] again on every line, so
//! neither can carry a terminal escape whatever a check stored.
//!
//! Nothing in a report depends on time, paths outside the repository, or
//! the machine: two runs over the same bytes render the same bytes.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use crate::config::{
    CommonsError, EXIT_ARTIFACT, EXIT_INTERNAL, EXIT_OK, echo, is_invisible_format,
};
use crate::fixtures::json_quote;
use crate::manifest::{ArtifactId, Kind, to_hex};

/// Names the verifier in every report. Changing what any check means is a
/// new value.
pub const VERIFIER_VERSION: &str = concat!("commons ", env!("CARGO_PKG_VERSION"), " verify v1");

/// Longest detail or subject a check stores, in chars, after [`clean`].
pub const MAX_DETAIL_CHARS: usize = 1_000;
/// Longest excerpt line, in chars, before [`echo`]'s own bound.
pub const MAX_EXCERPT_CHARS: usize = 160;

// ---------------------------------------------------------------------------
// Sanitising
// ---------------------------------------------------------------------------

/// `s` with every control or invisible format character written as
/// `\u{XXXX}`, cut so the result has at most `max` chars plus a trailing
/// `…`. Unlike [`echo`] it does not double backslashes, so it can run over
/// text that is already echoed without changing it.
pub fn clean_to(s: &str, max: usize) -> String {
    let mut out = String::with_capacity(s.len().min(max.saturating_add(8)));
    let mut count = 0usize;
    for c in s.chars() {
        let piece = if c.is_control() || is_invisible_format(c) {
            format!("\\u{{{:04X}}}", c as u32)
        } else {
            c.to_string()
        };
        let n = piece.chars().count();
        if count + n > max {
            out.push('…');
            break;
        }
        out.push_str(&piece);
        count += n;
    }
    out
}

/// [`clean_to`] with [`MAX_DETAIL_CHARS`].
pub fn clean(s: &str) -> String {
    clean_to(s, MAX_DETAIL_CHARS)
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// The stages, in the order they run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stage {
    Static,
    Golden,
    Invariants,
    Reproducible,
    /// Only with `--lens-url`.
    Lens,
}

impl Stage {
    /// The four stages every artifact goes through.
    pub const CORE: [Stage; 4] = [
        Stage::Static,
        Stage::Golden,
        Stage::Invariants,
        Stage::Reproducible,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Static => "static",
            Stage::Golden => "golden",
            Stage::Invariants => "invariants",
            Stage::Reproducible => "reproducible",
            Stage::Lens => "lens",
        }
    }
}

/// How a check, or a stage, ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Status {
    Pass,
    /// The artifact, or the evidence for it, is wrong.
    Fail,
    /// The verifier or the machine failed; says nothing about the artifact.
    Error,
    /// Not run, because an earlier stage did not pass.
    Skipped,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pass => "pass",
            Status::Fail => "fail",
            Status::Error => "error",
            Status::Skipped => "skipped",
        }
    }
}

/// The outcome of an artifact or of the whole run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verdict {
    Pass,
    Fail,
    Error,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Pass => "pass",
            Verdict::Fail => "fail",
            Verdict::Error => "error",
        }
    }

    pub fn exit_code(self) -> i32 {
        match self {
            Verdict::Pass => EXIT_OK,
            Verdict::Fail => EXIT_ARTIFACT,
            Verdict::Error => EXIT_INTERNAL,
        }
    }

    fn of(status: Status) -> Verdict {
        match status {
            Status::Pass | Status::Skipped => Verdict::Pass,
            Status::Fail => Verdict::Fail,
            Status::Error => Verdict::Error,
        }
    }
}

/// One line of the content a check is about, for the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Excerpt {
    /// 1-based.
    pub line: u64,
    /// Echoed and bounded.
    pub text: String,
}

/// Line `line` (1-based) of `content`, echoed and cut to
/// [`MAX_EXCERPT_CHARS`]; `None` if there is no such line.
pub fn excerpt(content: &[u8], line: u64) -> Option<Excerpt> {
    let index = usize::try_from(line.checked_sub(1)?).ok()?;
    let text = String::from_utf8_lossy(content);
    let raw = text.split('\n').nth(index)?;
    let cut: String = raw.chars().take(MAX_EXCERPT_CHARS).collect();
    let mut shown = echo(&cut);
    if raw.chars().count() > MAX_EXCERPT_CHARS && !shown.ends_with('…') {
        shown.push('…');
    }
    Some(Excerpt { line, text: shown })
}

/// One check of one stage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckResult {
    /// `<stage>.<name>`, e.g. `golden.decoder_case`. A fixed string.
    pub id: String,
    /// What it ran on (a case, an instruction, a table), or empty.
    pub subject: String,
    pub status: Status,
    /// Why it did not pass; empty when it did.
    pub detail: String,
    pub excerpt: Option<Excerpt>,
}

impl CheckResult {
    fn new(id: &str, subject: &str, status: Status, detail: &str) -> Self {
        CheckResult {
            id: clean(id),
            subject: clean(subject),
            status,
            detail: clean(detail),
            excerpt: None,
        }
    }

    pub fn pass(id: &str, subject: &str) -> Self {
        Self::new(id, subject, Status::Pass, "")
    }

    pub fn fail(id: &str, subject: &str, detail: impl AsRef<str>) -> Self {
        Self::new(id, subject, Status::Fail, detail.as_ref())
    }

    pub fn error(id: &str, subject: &str, detail: impl AsRef<str>) -> Self {
        Self::new(id, subject, Status::Error, detail.as_ref())
    }

    /// `pass` when `result` is `Ok`, otherwise `fail` with the reason.
    pub fn from_result(id: &str, subject: &str, result: Result<(), String>) -> Self {
        match result {
            Ok(()) => Self::pass(id, subject),
            Err(r) => Self::fail(id, subject, r),
        }
    }

    pub fn with_excerpt(mut self, e: Option<Excerpt>) -> Self {
        self.excerpt = e.map(|e| Excerpt {
            line: e.line,
            text: clean_to(&e.text, MAX_EXCERPT_CHARS + 1),
        });
        self
    }

    pub fn passed(&self) -> bool {
        self.status == Status::Pass
    }
}

/// The worst status among `checks`: error, then fail, then pass. A stage
/// that ran no check passed.
pub fn worst(checks: &[CheckResult]) -> Status {
    if checks.iter().any(|c| c.status == Status::Error) {
        Status::Error
    } else if checks.iter().any(|c| c.status == Status::Fail) {
        Status::Fail
    } else {
        Status::Pass
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageReport {
    pub stage: Stage,
    pub status: Status,
    pub checks: Vec<CheckResult>,
}

impl StageReport {
    pub fn ran(stage: Stage, checks: Vec<CheckResult>) -> Self {
        StageReport {
            stage,
            status: worst(&checks),
            checks,
        }
    }

    pub fn skipped(stage: Stage) -> Self {
        StageReport {
            stage,
            status: Status::Skipped,
            checks: Vec::new(),
        }
    }
}

/// What `verify` found for one artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactReport {
    /// `<status>/<kind>/<name>/<version>`.
    pub label: String,
    pub kind: Kind,
    pub artifact_id: ArtifactId,
    pub content_sha256: [u8; 32],
    pub verdict: Verdict,
    /// Every stage, in [`Stage`] order.
    pub stages: Vec<StageReport>,
    /// The first run's case digests (and fuzz digest), by key. Empty when
    /// golden did not run.
    pub digests: BTreeMap<String, [u8; 32]>,
    /// The first run's run digest; `None` when golden did not run.
    pub run_digest: Option<[u8; 32]>,
}

impl ArtifactReport {
    /// Fill in `verdict` from the stages.
    pub fn finish(mut self) -> Self {
        self.verdict = self
            .stages
            .iter()
            .map(|s| Verdict::of(s.status))
            .max()
            .unwrap_or(Verdict::Pass);
        self
    }

    /// Every check that did not pass, as the crate's error type: a failure
    /// is [`CommonsError::VerifyError`] (exit 1), an error is
    /// [`CommonsError::Internal`] (exit 3). In stage order, then check
    /// order.
    pub fn errors(&self) -> Vec<CommonsError> {
        let mut out = Vec::new();
        for s in &self.stages {
            for c in &s.checks {
                let reason = check_reason(c);
                match c.status {
                    Status::Fail => out.push(CommonsError::VerifyError {
                        artifact: self.label.clone(),
                        stage: s.stage.as_str().to_string(),
                        reason,
                    }),
                    Status::Error => out.push(CommonsError::Internal(format!(
                        "artifact {}: verify stage {}: {reason}",
                        self.label,
                        s.stage.as_str()
                    ))),
                    Status::Pass | Status::Skipped => {}
                }
            }
        }
        out
    }
}

/// `<check> [<subject>]: <detail>`.
fn check_reason(c: &CheckResult) -> String {
    let subject = if c.subject.is_empty() {
        String::new()
    } else {
        format!(" [{}]", c.subject)
    };
    format!("{}{subject}: {}", c.id, c.detail)
}

/// What `verify` found, for every artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunReport {
    pub overall: Verdict,
    /// In the order the pipeline was given them.
    pub artifacts: Vec<ArtifactReport>,
    pub fixture_set_hash: [u8; 32],
    pub verifier_version: String,
    /// The in-process SQL engine, e.g. `datafusion 55.2.0`.
    pub engine: String,
}

impl RunReport {
    pub fn new(
        artifacts: Vec<ArtifactReport>,
        fixture_set_hash: [u8; 32],
        engine: String,
    ) -> RunReport {
        let overall = artifacts
            .iter()
            .map(|a| a.verdict)
            .max()
            .unwrap_or(Verdict::Pass);
        RunReport {
            overall,
            artifacts,
            fixture_set_hash,
            verifier_version: VERIFIER_VERSION.to_string(),
            engine,
        }
    }

    /// 0 when every artifact passed, 1 when any failed and none errored, 3
    /// when any errored.
    pub fn exit_code(&self) -> i32 {
        self.overall.exit_code()
    }

    /// Every check that did not pass, across artifacts (see
    /// [`ArtifactReport::errors`]).
    pub fn errors(&self) -> Vec<CommonsError> {
        self.artifacts.iter().flat_map(ArtifactReport::errors).collect()
    }

    /// The report for people: a header, then per artifact its verdict, its
    /// stages and every check that did not pass, with excerpts.
    pub fn render_text(&self) -> String {
        let mut lines = Vec::new();
        let count = |v: Verdict| self.artifacts.iter().filter(|a| a.verdict == v).count();
        lines.push(format!("commons verify: {}", self.overall.as_str().to_uppercase()));
        lines.push(format!("verifier: {}", self.verifier_version));
        lines.push(format!("engine: {}", self.engine));
        lines.push(format!("fixtures: {}", to_hex(&self.fixture_set_hash)));
        lines.push(format!(
            "artifacts: {} ({} passed, {} failed, {} errors)",
            self.artifacts.len(),
            count(Verdict::Pass),
            count(Verdict::Fail),
            count(Verdict::Error)
        ));
        for a in &self.artifacts {
            lines.push(String::new());
            lines.push(format!("{} {}", a.verdict.as_str().to_uppercase(), a.label));
            lines.push(format!("  id {}", a.artifact_id));
            if let Some(d) = &a.run_digest {
                lines.push(format!("  run digest {}", to_hex(d)));
            }
            for s in &a.stages {
                let n = s.checks.len();
                let passed = s.checks.iter().filter(|c| c.passed()).count();
                let summary = if s.status == Status::Skipped {
                    String::new()
                } else {
                    format!("  {passed}/{n} checks passed")
                };
                lines.push(format!("  {:<12} {}{summary}", s.stage.as_str(), s.status.as_str()));
                for c in s.checks.iter().filter(|c| !c.passed()) {
                    lines.push(format!("    {} {}", c.status.as_str().to_uppercase(), check_reason(c)));
                    if let Some(e) = &c.excerpt {
                        lines.push(format!("      line {} | {}", e.line, e.text));
                    }
                }
            }
        }
        let mut out = String::new();
        for l in lines {
            out.push_str(&clean_to(&l, usize::MAX));
            out.push('\n');
        }
        out
    }

    /// The whole report as one JSON document, pure ASCII, keys in a fixed
    /// order, every check included. Ends with a newline.
    pub fn render_json(&self) -> String {
        let q = |s: &str| json_quote(s);
        let mut o = String::new();
        let _ = write!(
            o,
            "{{\"overall\":{},\"verifier_version\":{},\"engine\":{},\"fixture_set_hash\":{},\
             \"exit_code\":{},\"artifacts\":[",
            q(self.overall.as_str()),
            q(&self.verifier_version),
            q(&self.engine),
            q(&to_hex(&self.fixture_set_hash)),
            self.exit_code()
        );
        for (i, a) in self.artifacts.iter().enumerate() {
            if i > 0 {
                o.push(',');
            }
            let _ = write!(
                o,
                "{{\"label\":{},\"kind\":{},\"artifact_id\":{},\"content_sha256\":{},\
                 \"verdict\":{},\"run_digest\":{},\"digests\":{{",
                q(&a.label),
                q(a.kind.as_str()),
                q(&a.artifact_id.to_hex()),
                q(&to_hex(&a.content_sha256)),
                q(a.verdict.as_str()),
                a.run_digest.map_or("null".to_string(), |d| q(&to_hex(&d)))
            );
            for (j, (k, d)) in a.digests.iter().enumerate() {
                if j > 0 {
                    o.push(',');
                }
                let _ = write!(o, "{}:{}", q(k), q(&to_hex(d)));
            }
            o.push_str("},\"stages\":[");
            for (j, s) in a.stages.iter().enumerate() {
                if j > 0 {
                    o.push(',');
                }
                let _ = write!(
                    o,
                    "{{\"stage\":{},\"status\":{},\"checks\":[",
                    q(s.stage.as_str()),
                    q(s.status.as_str())
                );
                for (k, c) in s.checks.iter().enumerate() {
                    if k > 0 {
                        o.push(',');
                    }
                    let excerpt = c.excerpt.as_ref().map_or("null".to_string(), |e| {
                        format!("{{\"line\":{},\"text\":{}}}", e.line, q(&e.text))
                    });
                    let _ = write!(
                        o,
                        "{{\"id\":{},\"subject\":{},\"status\":{},\"detail\":{},\"excerpt\":{excerpt}}}",
                        q(&c.id),
                        q(&c.subject),
                        q(c.status.as_str()),
                        q(&c.detail)
                    );
                }
                o.push_str("]}");
            }
            o.push_str("]}");
        }
        o.push_str("]}\n");
        o
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn artifact(stages: Vec<StageReport>) -> ArtifactReport {
        ArtifactReport {
            label: "experimental/model/m/1.0.0".into(),
            kind: Kind::Model,
            artifact_id: ArtifactId([7; 32]),
            content_sha256: [8; 32],
            verdict: Verdict::Pass,
            stages,
            digests: [("model/a".to_string(), [1; 32])].into_iter().collect(),
            run_digest: Some([2; 32]),
        }
        .finish()
    }

    fn ran(stage: Stage, checks: Vec<CheckResult>) -> StageReport {
        StageReport::ran(stage, checks)
    }

    #[test]
    fn verdicts_and_exit_codes() {
        let ok = artifact(vec![ran(Stage::Static, vec![CheckResult::pass("static.x", "")])]);
        assert_eq!(ok.verdict, Verdict::Pass);
        let bad = artifact(vec![
            ran(Stage::Static, vec![CheckResult::pass("static.x", "")]),
            ran(Stage::Golden, vec![CheckResult::fail("golden.y", "c", "wrong")]),
            StageReport::skipped(Stage::Invariants),
        ]);
        assert_eq!(bad.verdict, Verdict::Fail);
        let broken = artifact(vec![ran(
            Stage::Golden,
            vec![
                CheckResult::fail("golden.y", "c", "wrong"),
                CheckResult::error("golden.z", "", "disk"),
            ],
        )]);
        assert_eq!(broken.verdict, Verdict::Error);
        assert_eq!(StageReport::skipped(Stage::Lens).status, Status::Skipped);
        assert_eq!(ran(Stage::Lens, vec![]).status, Status::Pass);

        let h = [0; 32];
        let e = String::from("datafusion");
        assert_eq!(RunReport::new(vec![], h, e.clone()).exit_code(), 0);
        assert_eq!(RunReport::new(vec![ok.clone()], h, e.clone()).exit_code(), 0);
        assert_eq!(RunReport::new(vec![ok.clone(), bad.clone()], h, e.clone()).exit_code(), 1);
        assert_eq!(RunReport::new(vec![bad.clone(), broken.clone(), ok], h, e).exit_code(), 3);

        let errs = bad.errors();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].exit_code(), 1);
        assert_eq!(
            errs[0].to_string(),
            "artifact experimental/model/m/1.0.0: verify stage golden: golden.y [c]: wrong"
        );
        let errs = broken.errors();
        assert_eq!(errs.iter().map(CommonsError::exit_code).collect::<Vec<_>>(), [1, 3]);
    }

    #[test]
    fn stored_text_is_clean_and_bounded() {
        let c = CheckResult::fail("golden.x", "case\u{1b}[2J", format!("bad {}", "\u{202E}x".repeat(800)));
        assert!(!c.subject.contains('\u{1b}'));
        assert!(c.subject.contains("\\u{001B}"));
        assert!(!c.detail.contains('\u{202E}'));
        assert!(c.detail.chars().count() <= MAX_DETAIL_CHARS + 1, "bounded after escaping");
        assert!(c.detail.ends_with('…'));
        // Already-echoed text is left alone.
        let echoed = echo("a\\b\n");
        assert_eq!(clean(&echoed), echoed);
    }

    #[test]
    fn excerpts_pick_one_line_echoed() {
        let src = b"SELECT 1\nFROM t\x07\n";
        assert_eq!(excerpt(src, 1).unwrap().text, "SELECT 1");
        assert_eq!(excerpt(src, 2).unwrap().text, "FROM t\\u{0007}");
        assert_eq!(excerpt(src, 0), None);
        assert_eq!(excerpt(src, 9), None);
        let long = format!("{}\n", "x".repeat(500));
        let e = excerpt(long.as_bytes(), 1).unwrap();
        assert!(e.text.chars().count() <= MAX_EXCERPT_CHARS + 1 && e.text.ends_with('…'));
    }

    fn sample() -> RunReport {
        let c = CheckResult::fail("static.sql_bounds", "solana.instructions", "not bounded \u{1b}[31m")
            .with_excerpt(excerpt(b"SELECT 1\nFROM solana.instructions\n", 2));
        let a = artifact(vec![
            ran(Stage::Static, vec![CheckResult::pass("static.sql_text", ""), c]),
            StageReport::skipped(Stage::Golden),
        ]);
        RunReport::new(vec![a], [5; 32], "datafusion 55.2.0".into())
    }

    #[test]
    fn text_rendering_is_stable_and_terminal_safe() {
        let r = sample();
        let t = r.render_text();
        assert_eq!(t, r.render_text(), "pure");
        assert!(!t.chars().any(|c| c.is_control() && c != '\n'), "{t}");
        assert!(t.starts_with("commons verify: FAIL\n"), "{t}");
        assert!(t.contains("FAIL experimental/model/m/1.0.0\n"), "{t}");
        assert!(t.contains("  static       fail  1/2 checks passed\n"), "{t}");
        assert!(t.contains("    FAIL static.sql_bounds [solana.instructions]: not bounded \\u{001B}[31m\n"), "{t}");
        assert!(t.contains("      line 2 | FROM solana.instructions\n"), "{t}");
        assert!(t.contains("  golden       skipped\n"), "{t}");
        assert!(t.contains(&format!("fixtures: {}\n", "05".repeat(32))), "{t}");
    }

    #[test]
    fn json_rendering_is_ascii_valid_and_complete() {
        let r = sample();
        let j = r.render_json();
        assert!(j.is_ascii() && j.ends_with("}\n"));
        assert_eq!(j, r.render_json(), "pure");
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["overall"], "fail");
        assert_eq!(v["exit_code"], 1);
        assert_eq!(v["verifier_version"], VERIFIER_VERSION);
        let a = &v["artifacts"][0];
        assert_eq!(a["artifact_id"], "07".repeat(32));
        assert_eq!(a["digests"]["model/a"], "01".repeat(32));
        let checks = &a["stages"][0]["checks"];
        assert_eq!(checks.as_array().unwrap().len(), 2, "passed checks are included");
        assert_eq!(checks[1]["excerpt"]["line"], 2);
        assert_eq!(a["stages"][1]["status"], "skipped");
        // Keys come in a fixed order, so the bytes are fixed too.
        assert!(j.starts_with("{\"overall\":\"fail\",\"verifier_version\":"), "{j}");
    }
}
