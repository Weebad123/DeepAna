//! Integration tests for `prism::metrics`.
//!
//! What is checked, and how:
//!
//! - **Exactness under concurrency.** Threads replay a deterministic
//!   recording script against one shared [`Metrics`]; the same script is
//!   replayed single-threaded into a plain [`Model`] built from `u64`s and an
//!   independent bucket search. The snapshot must equal the model in every
//!   counter, every bucket, every sum. Readers snapshot while writers record
//!   and every counter must be monotonic between successive snapshots.
//! - **Boundaries.** Every histogram bound (inclusive), sub-microsecond and
//!   `Duration::MAX` durations, the unknown-gauge sentinel, slot 0, lag
//!   saturation, zero and huge counter increments, state transitions.
//! - **Valid Prometheus text.** [`parse_exposition`] is a strict parser of
//!   the 0.0.4 text format (names, labels, escapes, HELP/TYPE ordering,
//!   contiguous families, no duplicate series, histogram invariants). Every
//!   rendered output in this file goes through it.
//! - **Every rendered line is right.** [`expected_series`] derives the full
//!   series set and every value from the snapshot, independently of the
//!   renderer; the parsed text must equal it exactly (no missing, no extra
//!   series).
//! - **Fixed cardinality.** The series count is computed from the label enum
//!   sizes and bucket tables, and recording anything (including hostile
//!   halt text) never adds or removes a series.
//! - **The scrubber removes every secret**: every credential name × case ×
//!   separator, quoted values, JSON keys, bearer/basic tokens, URLs of many
//!   schemes, registered secrets raw and percent-encoded, overlapping
//!   secrets, control characters, both length caps, and a randomized sweep.
//! - **The numbers agree with the system.** Error classes against real
//!   `SolanaAdapter` failures over HTTP, instruction counts against the real
//!   `DecoderRegistry`'s own counters, event counts and change-log bytes
//!   against a real `LocalChangeLog` and the file on disk, source counters
//!   field by field, and label sets against the crate's own enums.
//! - **Nothing leaks.** Real adapter errors carrying an API key, and error
//!   text with URLs and credentials, go through `set_halted` /
//!   `set_last_error`; neither the JSON snapshot nor the Prometheus text may
//!   contain any of it, and free text never reaches Prometheus at all.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use prism::account_resolver::ResolverMetricsSnapshot;
use prism::adapter::config::{AdapterConfig, SolanaAdapterConfig};
use prism::adapter::solana::SolanaAdapter;
use prism::adapter::{ChainAdapter, CommitmentLevel};
use prism::changelog::{ChangeLog, LocalChangeLog};
use prism::decoder::DecoderRegistry;
use prism::metrics::histogram::{
    FLUSH_BUCKETS_US, RATE_LIMIT_WAIT_BUCKETS_US, RPC_LATENCY_BUCKETS_US,
};
use prism::metrics::{
    DecodeOutcome, ErrorClass, EventType, Histogram, HistogramSnapshot, Metrics,
    MetricsSnapshot, PipelineState, ProgramClass, RpcMethod, Sources, Table, render_prometheus,
    scrub,
};
use prism::reconcile::ReconcileStats;
use prism::storage::partition::TableKind;
use prism::types::error::PrismError;
use prism::types::event::{ChangeEvent, CorrectionReason, SkipReason};
use prism::types::udm::DeepAnaInstruction;
use serde_json::Value;
use solana_sdk::pubkey::Pubkey;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

// ===========================================================================
// Strict Prometheus 0.0.4 text parser
// ===========================================================================

#[derive(Debug, Clone)]
struct Sample {
    name: String,
    labels: Vec<(String, String)>,
    /// The value exactly as rendered.
    raw: String,
}

#[derive(Debug)]
struct Family {
    name: String,
    kind: String,
    samples: Vec<Sample>,
}

fn is_metric_name(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

fn is_label_name(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !s.starts_with("__")
}

/// Parse one sample line: `name{k="v",...} value`.
fn parse_sample(line: &str) -> Sample {
    let name_end = line
        .find(['{', ' '])
        .unwrap_or_else(|| panic!("sample without value: {line:?}"));
    let name = &line[..name_end];
    assert!(is_metric_name(name), "bad metric name {name:?} in {line:?}");

    let mut labels = Vec::new();
    let mut rest = &line[name_end..];
    if let Some(after) = rest.strip_prefix('{') {
        let mut chars = after.char_indices().peekable();
        let close;
        loop {
            // label name
            let start = chars.peek().map(|&(i, _)| i).expect("unterminated labels");
            let mut eq = None;
            for (i, c) in chars.by_ref() {
                if c == '=' {
                    eq = Some(i);
                    break;
                }
            }
            let eq = eq.unwrap_or_else(|| panic!("label without '=' in {line:?}"));
            let lname = &after[start..eq];
            assert!(is_label_name(lname), "bad label name {lname:?} in {line:?}");
            assert_eq!(chars.next().map(|(_, c)| c), Some('"'), "unquoted label in {line:?}");
            let mut value = String::new();
            loop {
                match chars.next().map(|(_, c)| c) {
                    Some('\\') => match chars.next().map(|(_, c)| c) {
                        Some('\\') => value.push('\\'),
                        Some('"') => value.push('"'),
                        Some('n') => value.push('\n'),
                        other => panic!("bad escape {other:?} in {line:?}"),
                    },
                    Some('"') => break,
                    Some('\n') | None => panic!("unterminated label value in {line:?}"),
                    Some(c) => value.push(c),
                }
            }
            assert!(
                !labels.iter().any(|(k, _): &(String, String)| k == lname),
                "duplicate label {lname} in {line:?}"
            );
            labels.push((lname.to_owned(), value));
            match chars.next() {
                Some((_, ',')) => continue,
                Some((i, '}')) => {
                    close = i;
                    break;
                }
                other => panic!("expected ',' or '}}', got {other:?} in {line:?}"),
            }
        }
        rest = &after[close + 1..];
    }
    let raw = rest
        .strip_prefix(' ')
        .unwrap_or_else(|| panic!("missing single space before value in {line:?}"));
    assert!(!raw.contains(' '), "timestamps / trailing fields not expected: {line:?}");
    let value: f64 = match raw {
        "+Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        v => v.parse().unwrap_or_else(|_| panic!("unparsable value {v:?} in {line:?}")),
    };
    assert!(value.is_finite() && value >= 0.0, "unexpected value in {line:?}");
    Sample {
        name: name.to_owned(),
        labels,
        raw: raw.to_owned(),
    }
}

/// Parse and validate a whole exposition. Panics on anything invalid.
fn parse_exposition(text: &str) -> Vec<Family> {
    assert!(!text.is_empty(), "empty exposition");
    assert!(text.ends_with('\n'), "exposition must end with a newline");
    assert!(!text.contains('\r'), "carriage return in exposition");
    assert!(text.is_ascii(), "non-ASCII byte in exposition");

    let mut families: Vec<Family> = Vec::new();
    let mut names = BTreeSet::new();
    let mut series = BTreeSet::new();
    let mut lines = text.lines().peekable();

    while let Some(line) = lines.next() {
        assert!(!line.is_empty(), "empty line in exposition");
        if let Some(help) = line.strip_prefix("# HELP ") {
            let (name, doc) = help.split_once(' ').expect("HELP without text");
            assert!(is_metric_name(name), "bad HELP name {name:?}");
            assert!(!doc.trim().is_empty(), "empty HELP for {name}");
            let ty = lines.next().expect("HELP not followed by TYPE");
            let ty = ty
                .strip_prefix("# TYPE ")
                .unwrap_or_else(|| panic!("HELP for {name} not followed by TYPE: {ty:?}"));
            let (ty_name, kind) = ty.split_once(' ').expect("TYPE without kind");
            assert_eq!(ty_name, name, "HELP/TYPE name mismatch");
            assert!(
                matches!(kind, "counter" | "gauge" | "histogram"),
                "unexpected type {kind} for {name}"
            );
            if kind == "counter" {
                assert!(name.ends_with("_total"), "counter {name} must end in _total");
            } else {
                assert!(!name.ends_with("_total"), "non-counter {name} ends in _total");
            }
            if kind == "histogram" {
                assert!(name.ends_with("_seconds"), "histogram {name} must be in seconds");
            }
            assert!(names.insert(name.to_owned()), "family {name} declared twice");
            families.push(Family {
                name: name.to_owned(),
                kind: kind.to_owned(),
                samples: Vec::new(),
            });
            continue;
        }
        assert!(!line.starts_with('#'), "unexpected comment line {line:?}");

        let sample = parse_sample(line);
        let family = families
            .last_mut()
            .unwrap_or_else(|| panic!("sample before any TYPE: {line:?}"));
        let belongs = if family.kind == "histogram" {
            [
                format!("{}_bucket", family.name),
                format!("{}_sum", family.name),
                format!("{}_count", family.name),
            ]
            .contains(&sample.name)
        } else {
            sample.name == family.name
        };
        assert!(
            belongs,
            "sample {} is not part of family {} (families must be contiguous)",
            sample.name, family.name
        );
        if family.kind == "histogram" && sample.name.ends_with("_bucket") {
            assert!(sample.labels.iter().any(|(k, _)| k == "le"), "bucket without le");
        } else {
            assert!(!sample.labels.iter().any(|(k, _)| k == "le"), "le outside a bucket");
        }
        assert!(series.insert(series_key(&sample.name, &sample.labels)), "duplicate series {line}");
        family.samples.push(sample);
    }

    for family in &families {
        assert!(!family.samples.is_empty(), "family {} has no samples", family.name);
        if family.kind == "histogram" {
            check_histogram_family(family);
        }
    }
    families
}

/// Bucket bounds ascend and end in `+Inf`, counts are cumulative, `+Inf`
/// equals `_count`, and `_sum` exists, for every label set.
fn check_histogram_family(family: &Family) {
    let mut groups: BTreeMap<Vec<(String, String)>, Vec<&Sample>> = BTreeMap::new();
    for s in &family.samples {
        let base: Vec<_> = s.labels.iter().filter(|(k, _)| k != "le").cloned().collect();
        groups.entry(base).or_default().push(s);
    }
    for (labels, samples) in groups {
        let buckets: Vec<_> = samples.iter().filter(|s| s.name.ends_with("_bucket")).collect();
        let sum: Vec<_> = samples.iter().filter(|s| s.name.ends_with("_sum")).collect();
        let count: Vec<_> = samples.iter().filter(|s| s.name.ends_with("_count")).collect();
        assert_eq!(sum.len(), 1, "{} {labels:?}: one _sum", family.name);
        assert_eq!(count.len(), 1, "{} {labels:?}: one _count", family.name);
        assert!(buckets.len() >= 2, "{} {labels:?}: buckets", family.name);

        let mut prev_le = f64::NEG_INFINITY;
        let mut prev_n = 0u64;
        for b in &buckets {
            let le = &b.labels.iter().find(|(k, _)| k == "le").unwrap().1;
            let le_v: f64 = if le == "+Inf" { f64::INFINITY } else { le.parse().unwrap() };
            assert!(le_v > prev_le, "{}: le not strictly ascending", family.name);
            prev_le = le_v;
            let n: u64 = b.raw.parse().expect("bucket counts are integers");
            assert!(n >= prev_n, "{}: bucket counts not cumulative", family.name);
            prev_n = n;
        }
        let last_le = &buckets.last().unwrap().labels.iter().find(|(k, _)| k == "le").unwrap().1;
        assert_eq!(last_le, "+Inf", "{}: last bucket must be +Inf", family.name);
        assert_eq!(buckets.last().unwrap().raw, count[0].raw, "{}: +Inf != _count", family.name);
    }
}

fn series_key(name: &str, labels: &[(String, String)]) -> String {
    let mut labels = labels.to_vec();
    labels.sort();
    let mut key = name.to_owned();
    for (k, v) in labels {
        // `le` compared numerically, not by spelling.
        let v = if k == "le" && v != "+Inf" {
            format!("{}", v.parse::<f64>().unwrap())
        } else {
            v
        };
        key.push_str(&format!("|{k}={v}"));
    }
    key
}

/// Validate and flatten to `series key -> raw value`.
fn render_and_parse(s: &MetricsSnapshot) -> (String, BTreeMap<String, String>) {
    let text = render_prometheus(s);
    let families = parse_exposition(&text);
    let map = families
        .iter()
        .flat_map(|f| &f.samples)
        .map(|s| (series_key(&s.name, &s.labels), s.raw.clone()))
        .collect();
    (text, map)
}

// ===========================================================================
// Expected series, derived from the snapshot independently of the renderer
// ===========================================================================

#[derive(Debug, Clone, Copy, PartialEq)]
enum Exp {
    Int(u64),
    /// Microseconds, rendered in seconds.
    Secs(u64),
}

fn key(name: &str, labels: &[(&str, &str)]) -> String {
    let labels: Vec<(String, String)> =
        labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    series_key(name, &labels)
}

/// Parse a rendered seconds value back to exact integer microseconds. Fails
/// on exponent notation, more than 6 decimals, or anything not plain decimal.
fn seconds_to_micros(raw: &str) -> u64 {
    let (whole, frac) = raw.split_once('.').unwrap_or((raw, ""));
    assert!(!whole.is_empty() && whole.bytes().all(|b| b.is_ascii_digit()), "bad seconds {raw}");
    assert!(frac.len() <= 6 && frac.bytes().all(|b| b.is_ascii_digit()), "bad seconds {raw}");
    assert!(!frac.ends_with('0'), "trailing zero in {raw}");
    let frac_us: u64 = format!("{frac:0<6}").parse().unwrap();
    whole.parse::<u64>().unwrap() * 1_000_000 + frac_us
}

fn le_label(bound_us: u64) -> String {
    format!("{}", bound_us as f64 / 1e6)
}

fn expect_hist(
    out: &mut BTreeMap<String, Exp>,
    name: &str,
    labels: &[(&str, &str)],
    h: &HistogramSnapshot,
) {
    for &(bound, n) in &h.buckets {
        let le = le_label(bound);
        let mut l = labels.to_vec();
        l.push(("le", &le));
        out.insert(key(&format!("{name}_bucket"), &l), Exp::Int(n));
    }
    let mut l = labels.to_vec();
    l.push(("le", "+Inf"));
    out.insert(key(&format!("{name}_bucket"), &l), Exp::Int(h.count));
    out.insert(key(&format!("{name}_sum"), labels), Exp::Secs(h.sum));
    out.insert(key(&format!("{name}_count"), labels), Exp::Int(h.count));
}

fn expected_series(s: &MetricsSnapshot) -> BTreeMap<String, Exp> {
    let mut out = BTreeMap::new();
    for &st in PipelineState::ALL {
        out.insert(
            key("prism_state", &[("state", st.as_str())]),
            Exp::Int(u64::from(st == s.state)),
        );
    }
    for (name, v) in [
        ("prism_tip_slot", s.tip_slot),
        ("prism_processed_slot", s.processed_slot),
        ("prism_flushed_slot", s.flushed_slot),
        ("prism_finalized_slot", s.finalized_slot),
        ("prism_lag_slots", s.lag_slots),
        ("prism_changelog_bytes", s.changelog_bytes),
        ("prism_changelog_segments", s.changelog_segments),
    ] {
        if let Some(v) = v {
            out.insert(key(name, &[]), Exp::Int(v));
        }
    }
    for (m, r) in &s.rpc {
        out.insert(key("prism_rpc_requests_total", &[("method", m)]), Exp::Int(r.requests));
        out.insert(key("prism_rpc_retries_total", &[("method", m)]), Exp::Int(r.retries));
        for (c, &n) in &r.errors {
            out.insert(
                key("prism_rpc_errors_total", &[("method", m), ("class", c)]),
                Exp::Int(n),
            );
        }
        expect_hist(&mut out, "prism_rpc_latency_seconds", &[("method", m)], &r.latency_us);
    }
    expect_hist(&mut out, "prism_rpc_rate_limit_wait_seconds", &[], &s.rate_limit_wait_us);
    for (t, v) in &s.tables {
        out.insert(key("prism_rows_written_total", &[("table", t)]), Exp::Int(v.rows));
        out.insert(key("prism_bytes_written_total", &[("table", t)]), Exp::Int(v.bytes));
        out.insert(key("prism_files_written_total", &[("table", t)]), Exp::Int(v.files));
    }
    out.insert(key("prism_flushes_total", &[("outcome", "ok")]), Exp::Int(s.flushes_ok));
    out.insert(key("prism_flushes_total", &[("outcome", "failed")]), Exp::Int(s.flushes_failed));
    expect_hist(&mut out, "prism_flush_duration_seconds", &[], &s.flush_duration_us);
    for (e, &n) in &s.events {
        out.insert(key("prism_events_emitted_total", &[("event", e)]), Exp::Int(n));
    }
    out.insert(key("prism_slots_processed_total", &[]), Exp::Int(s.slots_processed));
    for (r, &n) in &s.slots_skipped {
        out.insert(key("prism_slots_skipped_total", &[("reason", r)]), Exp::Int(n));
    }
    for (p, outcomes) in &s.instructions {
        for (o, &n) in outcomes {
            out.insert(
                key("prism_instructions_total", &[("program_class", p), ("outcome", o)]),
                Exp::Int(n),
            );
        }
    }
    for (prefix, counters) in [
        ("resolver", &s.sources.resolver),
        ("reconcile", &s.sources.reconcile),
    ] {
        for (field, &n) in counters.iter().flatten() {
            out.insert(key(&format!("prism_{prefix}_{field}_total"), &[]), Exp::Int(n));
        }
    }
    out
}

/// The rendered text must contain exactly the expected series, each with
/// exactly the expected value.
fn assert_render_agrees(s: &MetricsSnapshot) -> String {
    let (text, parsed) = render_and_parse(s);
    let expected = expected_series(s);
    let parsed_keys: BTreeSet<_> = parsed.keys().collect();
    let expected_keys: BTreeSet<_> = expected.keys().collect();
    let missing: Vec<_> = expected_keys.difference(&parsed_keys).collect();
    let extra: Vec<_> = parsed_keys.difference(&expected_keys).collect();
    assert!(missing.is_empty(), "series missing from output: {missing:?}");
    assert!(extra.is_empty(), "unexpected series in output: {extra:?}");
    for (k, exp) in &expected {
        let raw = &parsed[k];
        match *exp {
            Exp::Int(n) => assert_eq!(raw, &n.to_string(), "value of {k}"),
            Exp::Secs(us) => assert_eq!(seconds_to_micros(raw), us, "value of {k}: {raw}"),
        }
    }
    text
}

// ===========================================================================
// Model: the same recording replayed into plain integers
// ===========================================================================

/// Inclusive upper-bound bucket search, written independently of the crate.
fn bucket_index(bounds: &[u64], v: u64) -> usize {
    for (i, &b) in bounds.iter().enumerate() {
        if v <= b {
            return i;
        }
    }
    bounds.len()
}

#[derive(Clone, PartialEq, Debug)]
struct ModelHist {
    bounds: &'static [u64],
    counts: Vec<u64>,
    sum: u64,
}

impl ModelHist {
    fn new(bounds: &'static [u64]) -> Self {
        Self { bounds, counts: vec![0; bounds.len() + 1], sum: 0 }
    }
    fn record(&mut self, us: u64) {
        self.counts[bucket_index(self.bounds, us)] += 1;
        self.sum = self.sum.wrapping_add(us);
    }
    fn assert_eq_snapshot(&self, s: &HistogramSnapshot, what: &str) {
        let mut cum = 0;
        let expected: Vec<(u64, u64)> = self
            .bounds
            .iter()
            .zip(&self.counts)
            .map(|(&b, &n)| {
                cum += n;
                (b, cum)
            })
            .collect();
        assert_eq!(s.buckets, expected, "{what}: buckets");
        assert_eq!(s.count, self.counts.iter().sum::<u64>(), "{what}: count");
        assert_eq!(s.sum, self.sum, "{what}: sum");
    }
}

struct Model {
    requests: HashMap<RpcMethod, u64>,
    retries: HashMap<RpcMethod, u64>,
    errors: HashMap<(RpcMethod, ErrorClass), u64>,
    latency: HashMap<RpcMethod, ModelHist>,
    wait: ModelHist,
    rows: HashMap<TableKind, u64>,
    bytes: HashMap<TableKind, u64>,
    files: HashMap<TableKind, u64>,
    flush_ok: u64,
    flush_failed: u64,
    flush: ModelHist,
    events: HashMap<EventType, u64>,
    processed: u64,
    skipped: HashMap<SkipReason, u64>,
    instructions: HashMap<(ProgramClass, DecodeOutcome), u64>,
}

impl Model {
    fn new() -> Self {
        Self {
            requests: HashMap::new(),
            retries: HashMap::new(),
            errors: HashMap::new(),
            latency: RpcMethod::ALL
                .iter()
                .map(|&m| (m, ModelHist::new(RPC_LATENCY_BUCKETS_US)))
                .collect(),
            wait: ModelHist::new(RATE_LIMIT_WAIT_BUCKETS_US),
            rows: HashMap::new(),
            bytes: HashMap::new(),
            files: HashMap::new(),
            flush_ok: 0,
            flush_failed: 0,
            flush: ModelHist::new(FLUSH_BUCKETS_US),
            events: HashMap::new(),
            processed: 0,
            skipped: HashMap::new(),
            instructions: HashMap::new(),
        }
    }

    fn assert_matches(&self, s: &MetricsSnapshot) {
        for &m in RpcMethod::ALL {
            let r = &s.rpc[m.as_str()];
            assert_eq!(r.requests, *self.requests.get(&m).unwrap_or(&0), "requests {m:?}");
            assert_eq!(r.retries, *self.retries.get(&m).unwrap_or(&0), "retries {m:?}");
            for &c in ErrorClass::ALL {
                assert_eq!(
                    r.errors[c.as_str()],
                    *self.errors.get(&(m, c)).unwrap_or(&0),
                    "errors {m:?} {c:?}"
                );
            }
            self.latency[&m].assert_eq_snapshot(&r.latency_us, m.as_str());
        }
        self.wait.assert_eq_snapshot(&s.rate_limit_wait_us, "rate limit wait");
        for &t in &TableKind::ALL {
            let v = &s.tables[t.as_str()];
            assert_eq!(v.rows, *self.rows.get(&t).unwrap_or(&0), "rows {t:?}");
            assert_eq!(v.bytes, *self.bytes.get(&t).unwrap_or(&0), "bytes {t:?}");
            assert_eq!(v.files, *self.files.get(&t).unwrap_or(&0), "files {t:?}");
        }
        assert_eq!(s.flushes_ok, self.flush_ok);
        assert_eq!(s.flushes_failed, self.flush_failed);
        self.flush.assert_eq_snapshot(&s.flush_duration_us, "flush");
        for &e in EventType::ALL {
            assert_eq!(s.events[e.as_str()], *self.events.get(&e).unwrap_or(&0), "{e:?}");
        }
        assert_eq!(s.slots_processed, self.processed);
        for &r in &SkipReason::ALL {
            assert_eq!(s.slots_skipped[r.as_str()], *self.skipped.get(&r).unwrap_or(&0));
        }
        for &p in ProgramClass::ALL {
            for &o in DecodeOutcome::ALL {
                assert_eq!(
                    s.instructions[p.as_str()][o.as_str()],
                    *self.instructions.get(&(p, o)).unwrap_or(&0),
                    "instructions {p:?} {o:?}"
                );
            }
        }
    }
}

/// Values chosen to sit on, just above and just below bucket bounds.
fn boundary_values(bounds: &[u64]) -> Vec<u64> {
    let mut v = vec![0, 1];
    for &b in bounds {
        v.extend([b - 1, b, b + 1]);
    }
    v.push(u64::MAX / (1 << 20)); // far above every bound, no sum overflow
    v
}

/// One step of the deterministic recording script, applied to both the real
/// metrics and the model. `step` selects every label combination in turn.
fn script_step(m: &Metrics, model: Option<&mut Model>, step: u64) {
    let method = RpcMethod::ALL[(step % RpcMethod::COUNT as u64) as usize];
    let class = ErrorClass::ALL[((step / 3) % ErrorClass::COUNT as u64) as usize];
    let table = TableKind::ALL[(step % 4) as usize];
    let event = EventType::ALL[((step / 2) % EventType::COUNT as u64) as usize];
    let reason = SkipReason::ALL[(step % 3) as usize];
    let pclass = ProgramClass::ALL[((step / 5) % ProgramClass::COUNT as u64) as usize];
    let outcome = DecodeOutcome::ALL[(step % DecodeOutcome::COUNT as u64) as usize];
    let lat_values = boundary_values(RPC_LATENCY_BUCKETS_US);
    let lat = lat_values[(step as usize * 7) % lat_values.len()];
    let wait_values = boundary_values(RATE_LIMIT_WAIT_BUCKETS_US);
    let wait = wait_values[(step as usize * 11) % wait_values.len()];
    let flush_values = boundary_values(FLUSH_BUCKETS_US);
    let flush = flush_values[(step as usize * 13) % flush_values.len()];
    let retry = step.is_multiple_of(2);
    let error = step.is_multiple_of(5);
    let ok = !step.is_multiple_of(7);
    let rows = step % 1000;

    m.rpc_request(method);
    if retry {
        m.rpc_retry(method);
    }
    if error {
        m.rpc_error(method, class);
    }
    m.rpc_latency(method, Duration::from_micros(lat));
    m.rpc_rate_limit_wait(Duration::from_micros(wait));
    m.rows_written(table, rows);
    m.bytes_written(table, rows * 100);
    m.files_written(table, 1);
    m.flush_done(ok, Duration::from_micros(flush));
    m.event_emitted(event);
    m.slot_processed();
    m.slot_skipped(reason);
    m.instruction(pclass, outcome);

    if let Some(model) = model {
        *model.requests.entry(method).or_default() += 1;
        if retry {
            *model.retries.entry(method).or_default() += 1;
        }
        if error {
            *model.errors.entry((method, class)).or_default() += 1;
        }
        model.latency.get_mut(&method).unwrap().record(lat);
        model.wait.record(wait);
        *model.rows.entry(table).or_default() += rows;
        *model.bytes.entry(table).or_default() += rows * 100;
        *model.files.entry(table).or_default() += 1;
        if ok {
            model.flush_ok += 1;
        } else {
            model.flush_failed += 1;
        }
        model.flush.record(flush);
        *model.events.entry(event).or_default() += 1;
        model.processed += 1;
        *model.skipped.entry(reason).or_default() += 1;
        *model.instructions.entry((pclass, outcome)).or_default() += 1;
    }
}

fn fresh_snapshot(m: &Metrics) -> MetricsSnapshot {
    m.snapshot(&Sources::default())
}

/// Every monotonic number in a snapshot, keyed by its series.
fn monotonic_values(s: &MetricsSnapshot) -> BTreeMap<String, u64> {
    expected_series(s)
        .into_iter()
        .filter(|(k, _)| {
            !k.starts_with("prism_state")
                && !k.ends_with("_slot")
                && !k.starts_with("prism_lag_slots")
                && !k.starts_with("prism_changelog_")
        })
        .map(|(k, e)| match e {
            Exp::Int(n) | Exp::Secs(n) => (k, n),
        })
        .collect()
}

// ===========================================================================
// Concurrency
// ===========================================================================

const THREADS: u64 = 8;
const STEPS_PER_THREAD: u64 = 5_000;

#[test]
fn concurrent_recording_is_exact() {
    let metrics = Metrics::new();
    let barrier = Barrier::new(THREADS as usize);
    thread::scope(|scope| {
        for t in 0..THREADS {
            let (metrics, barrier) = (&metrics, &barrier);
            scope.spawn(move || {
                barrier.wait();
                for i in 0..STEPS_PER_THREAD {
                    script_step(metrics, None, t * STEPS_PER_THREAD + i);
                }
            });
        }
    });

    let mut model = Model::new();
    let scratch = Metrics::new();
    for step in 0..THREADS * STEPS_PER_THREAD {
        script_step(&scratch, Some(&mut model), step);
    }
    let snapshot = fresh_snapshot(&metrics);
    model.assert_matches(&snapshot);
    assert_eq!(snapshot.slots_processed, THREADS * STEPS_PER_THREAD);
    assert_render_agrees(&snapshot);
}

#[test]
fn concurrent_snapshots_are_monotonic_and_always_valid() {
    let metrics = Metrics::new();
    let done = AtomicBool::new(false);
    let snapshots_taken = AtomicUsize::new(0);

    thread::scope(|scope| {
        for t in 0..4u64 {
            let metrics = &metrics;
            scope.spawn(move || {
                for i in 0..STEPS_PER_THREAD {
                    script_step(metrics, None, t * STEPS_PER_THREAD + i);
                }
            });
        }
        for _ in 0..2 {
            let (metrics, done, taken) = (&metrics, &done, &snapshots_taken);
            scope.spawn(move || {
                let mut previous = monotonic_values(&fresh_snapshot(metrics));
                while !done.load(Ordering::Relaxed) {
                    let snapshot = fresh_snapshot(metrics);
                    // Histogram invariants hold in every intermediate snapshot.
                    for h in snapshot
                        .rpc
                        .values()
                        .map(|r| &r.latency_us)
                        .chain([&snapshot.rate_limit_wait_us, &snapshot.flush_duration_us])
                    {
                        assert!(h.buckets.windows(2).all(|w| w[0].1 <= w[1].1));
                        assert!(h.buckets.last().unwrap().1 <= h.count);
                    }
                    let current = monotonic_values(&snapshot);
                    assert_eq!(current.len(), previous.len(), "series set changed");
                    for (k, &now) in &current {
                        assert!(now >= previous[k], "{k} went backwards: {} -> {now}", previous[k]);
                    }
                    previous = current;
                    parse_exposition(&render_prometheus(&snapshot));
                    taken.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        // Stop the readers once the writers are done.
        let (metrics, done) = (&metrics, &done);
        scope.spawn(move || {
            while fresh_snapshot(metrics).slots_processed < 4 * STEPS_PER_THREAD {
                thread::yield_now();
            }
            done.store(true, Ordering::Relaxed);
        });
    });

    assert!(snapshots_taken.load(Ordering::Relaxed) > 0);
    let mut model = Model::new();
    let scratch = Metrics::new();
    for step in 0..4 * STEPS_PER_THREAD {
        script_step(&scratch, Some(&mut model), step);
    }
    let final_snapshot = fresh_snapshot(&metrics);
    model.assert_matches(&final_snapshot);
    assert_render_agrees(&final_snapshot);
}

#[test]
fn concurrent_gauges_and_state_never_tear() {
    let metrics = Metrics::new();
    let reasons: Vec<String> = (0..8).map(|i| format!("halt reason number {i}")).collect();
    thread::scope(|scope| {
        for t in 0..8u64 {
            let (metrics, reasons) = (&metrics, &reasons);
            scope.spawn(move || {
                for i in 0..2_000u64 {
                    metrics.set_tip(t * 1_000_000 + i);
                    metrics.set_processed(t * 1_000_000 + i / 2);
                    if i % 3 == 0 {
                        metrics.set_halted(&reasons[t as usize]);
                    } else {
                        metrics.set_running();
                    }
                    metrics.set_last_error(&reasons[t as usize]);
                }
            });
        }
        let (metrics, reasons) = (&metrics, &reasons);
        scope.spawn(move || {
            for _ in 0..2_000 {
                let s = fresh_snapshot(metrics);
                for text in [&s.halt_reason, &s.last_error].into_iter().flatten() {
                    assert!(reasons.contains(text), "torn text {text:?}");
                }
                if let (Some(t), Some(p)) = (s.tip_slot, s.processed_slot) {
                    assert_eq!(s.lag_slots, Some(t.saturating_sub(p)));
                }
                parse_exposition(&render_prometheus(&s));
            }
        });
    });
    let s = fresh_snapshot(&metrics);
    let tip = s.tip_slot.unwrap();
    assert_eq!(tip % 1_000_000, 1_999, "the last write of some thread wins");
    assert!(reasons.contains(s.last_error.as_ref().unwrap()));
}

// ===========================================================================
// Boundaries
// ===========================================================================

#[test]
fn every_histogram_bound_is_inclusive() {
    for bounds in [RPC_LATENCY_BUCKETS_US, FLUSH_BUCKETS_US, RATE_LIMIT_WAIT_BUCKETS_US] {
        for (i, &b) in bounds.iter().enumerate() {
            for (value, expected_bucket) in [(b - 1, i), (b, i), (b + 1, i + 1)] {
                // b - 1 belongs to bucket i unless it is at or below bound i-1.
                let expected_bucket = if i > 0 && value <= bounds[i - 1] {
                    i - 1
                } else {
                    expected_bucket
                };
                let h = Histogram::new(bounds);
                h.record(value);
                let s = h.snapshot();
                for (j, &(_, cum)) in s.buckets.iter().enumerate() {
                    assert_eq!(cum, u64::from(j >= expected_bucket), "value {value} bucket {j}");
                }
                assert_eq!((s.count, s.sum), (1, value));
            }
        }
    }
}

#[test]
fn duration_extremes() {
    let m = Metrics::new();
    m.rpc_latency(RpcMethod::GetBlock, Duration::ZERO);
    m.rpc_latency(RpcMethod::GetBlock, Duration::from_nanos(999)); // truncates to 0 µs
    m.rpc_latency(RpcMethod::GetHealth, Duration::MAX); // saturates to u64::MAX µs
    let s = fresh_snapshot(&m);

    let block = &s.rpc["getBlock"].latency_us;
    assert_eq!((block.count, block.sum), (2, 0));
    assert!(block.buckets.iter().all(|&(_, n)| n == 2), "both in the first bucket");

    let health = &s.rpc["getHealth"].latency_us;
    assert_eq!((health.count, health.sum), (1, u64::MAX));
    assert!(health.buckets.iter().all(|&(_, n)| n == 0), "only in +Inf");

    let (text, parsed) = render_and_parse(&s);
    assert_eq!(
        parsed[&key("prism_rpc_latency_seconds_bucket", &[("method", "getHealth"), ("le", "+Inf")])],
        "1"
    );
    assert!(text.contains("prism_rpc_latency_seconds_sum{method=\"getHealth\"} 18446744073709.551615\n"));
    assert!(text.contains("prism_rpc_latency_seconds_sum{method=\"getBlock\"} 0\n"));
    assert_render_agrees(&s);
}

#[test]
fn seconds_are_rendered_exactly() {
    let m = Metrics::new();
    for us in [1, 10, 1_000, 1_500_000, 60_000_000, 1_234_567] {
        m.flush_done(true, Duration::from_micros(us));
    }
    let text = render_prometheus(&fresh_snapshot(&m));
    // 1 + 10 + 1000 + 1_500_000 + 60_000_000 + 1_234_567 = 62_735_578 µs.
    assert!(text.contains("prism_flush_duration_seconds_sum 62.735578\n"), "{text}");
    for le in ["0.001", "0.01", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10", "30", "60", "120"] {
        assert!(
            text.contains(&format!("prism_flush_duration_seconds_bucket{{le=\"{le}\"}}")),
            "le {le} spelled exactly"
        );
    }
    assert!(text.contains("prism_flush_duration_seconds_bucket{le=\"0.001\"} 3\n"));
    assert!(text.contains("prism_flush_duration_seconds_bucket{le=\"2.5\"} 5\n"));
    assert!(text.contains("prism_flush_duration_seconds_bucket{le=\"30\"} 5\n"));
    assert!(text.contains("prism_flush_duration_seconds_bucket{le=\"60\"} 6\n"));
    assert!(text.contains("prism_flush_duration_seconds_bucket{le=\"120\"} 6\n"));
}

#[test]
fn gauges_unknown_is_not_zero() {
    let m = Metrics::new();
    let s = fresh_snapshot(&m);
    for g in [
        s.tip_slot,
        s.processed_slot,
        s.flushed_slot,
        s.finalized_slot,
        s.lag_slots,
        s.changelog_bytes,
        s.changelog_segments,
    ] {
        assert_eq!(g, None);
    }
    let text = assert_render_agrees(&s);
    for name in ["_slot ", "lag_slots", "changelog_bytes", "changelog_segments"] {
        assert!(!text.contains(name), "{name} rendered while unknown");
    }
    let json = serde_json::to_value(&s).unwrap();
    for f in ["tip_slot", "processed_slot", "lag_slots", "changelog_bytes"] {
        assert!(json[f].is_null(), "{f} must be null, not 0");
    }

    // Zero is a real value.
    m.set_tip(0);
    m.set_processed(0);
    m.set_flushed(0);
    m.set_finalized(0);
    m.set_changelog(0, 0);
    let s = fresh_snapshot(&m);
    assert_eq!(
        (s.tip_slot, s.processed_slot, s.flushed_slot, s.finalized_slot, s.lag_slots),
        (Some(0), Some(0), Some(0), Some(0), Some(0))
    );
    assert_eq!((s.changelog_bytes, s.changelog_segments), (Some(0), Some(0)));
    let text = assert_render_agrees(&s);
    assert!(text.contains("prism_tip_slot 0\n"));
    assert!(text.contains("prism_lag_slots 0\n"));
    assert_eq!(serde_json::to_value(&s).unwrap()["tip_slot"], 0);
}

#[test]
fn gauge_sentinel_and_lag_edges() {
    let m = Metrics::new();
    m.set_tip(u64::MAX); // the sentinel is not a value: ignored
    assert_eq!(fresh_snapshot(&m).tip_slot, None);
    m.set_tip(u64::MAX - 1);
    m.set_processed(0);
    let s = fresh_snapshot(&m);
    assert_eq!(s.tip_slot, Some(u64::MAX - 1));
    assert_eq!(s.lag_slots, Some(u64::MAX - 1));
    m.set_tip(u64::MAX); // ignored, the previous value stays
    assert_eq!(fresh_snapshot(&m).tip_slot, Some(u64::MAX - 1));

    // Processed ahead of a stale tip: lag saturates at zero, never wraps.
    m.set_tip(10);
    m.set_processed(20);
    assert_eq!(fresh_snapshot(&m).lag_slots, Some(0));

    // Lag needs both ends: flushed/finalized do not count.
    let m = Metrics::new();
    m.set_flushed(5);
    m.set_finalized(5);
    m.set_tip(9);
    assert_eq!(fresh_snapshot(&m).lag_slots, None);
    m.set_processed(4);
    let s = fresh_snapshot(&m);
    assert_eq!(s.lag_slots, Some(5));
    assert_render_agrees(&s);
}

#[test]
fn counter_increments_zero_and_huge() {
    let m = Metrics::new();
    m.rows_written(Table::Slots, 0);
    m.bytes_written(Table::Slots, 0);
    m.files_written(Table::Slots, 0);
    let s = fresh_snapshot(&m);
    assert_eq!(s.tables["slots"].rows, 0);
    let (_, parsed) = render_and_parse(&s);
    assert_eq!(parsed[&key("prism_rows_written_total", &[("table", "slots")])], "0");

    m.bytes_written(Table::TokenTransfers, u64::MAX - 1);
    m.bytes_written(Table::TokenTransfers, 1);
    let s = fresh_snapshot(&m);
    assert_eq!(s.tables["token_transfers"].bytes, u64::MAX);
    let text = assert_render_agrees(&s);
    assert!(text.contains("prism_bytes_written_total{table=\"token_transfers\"} 18446744073709551615\n"));
}

#[test]
fn state_transitions() {
    let m = Metrics::new();
    let check = |state: PipelineState, reason: Option<&str>| {
        let s = fresh_snapshot(&m);
        assert_eq!(s.state, state);
        assert_eq!(s.halt_reason.as_deref(), reason);
        let (_, parsed) = render_and_parse(&s);
        for &st in PipelineState::ALL {
            let want = if st == state { "1" } else { "0" };
            assert_eq!(parsed[&key("prism_state", &[("state", st.as_str())])], want);
        }
        assert_eq!(serde_json::to_value(&s).unwrap()["state"], state.as_str());
    };
    check(PipelineState::Starting, None);
    m.set_running();
    check(PipelineState::Running, None);
    m.set_halted("first");
    check(PipelineState::Halted, Some("first"));
    m.set_halted("second");
    check(PipelineState::Halted, Some("second"));
    m.set_running();
    check(PipelineState::Running, None);
    m.set_halted("");
    check(PipelineState::Halted, Some(""));
}

// ===========================================================================
// Valid text and fixed cardinality
// ===========================================================================

/// The number of series a snapshot renders with no gauges and no sources,
/// computed from the label enums and bucket tables.
fn fixed_series_count() -> usize {
    let m = RpcMethod::COUNT;
    let hist = |bounds: &[u64]| bounds.len() + 3; // + +Inf, _sum, _count
    PipelineState::COUNT
        + m // requests
        + m // retries
        + m * ErrorClass::COUNT
        + m * hist(RPC_LATENCY_BUCKETS_US)
        + hist(RATE_LIMIT_WAIT_BUCKETS_US)
        + 3 * TableKind::ALL.len()
        + 2 // flushes ok / failed
        + hist(FLUSH_BUCKETS_US)
        + EventType::COUNT
        + 1 // slots processed
        + SkipReason::ALL.len()
        + ProgramClass::COUNT * DecodeOutcome::COUNT
}

const GAUGES: usize = 7;
const RESOLVER_FIELDS: usize = 8;
const RECONCILE_FIELDS: usize = 10;

fn full_sources() -> Sources {
    Sources {
        resolver: Some(ResolverMetricsSnapshot::default()),
        reconcile: Some(ReconcileStats::default()),
    }
}

#[test]
fn cardinality_is_fixed_and_known() {
    assert_eq!(fixed_series_count(), 261, "label set changed: update this pin deliberately");

    let m = Metrics::new();
    let (_, fresh) = render_and_parse(&fresh_snapshot(&m));
    assert_eq!(fresh.len(), fixed_series_count());

    let (_, with_sources) = render_and_parse(&m.snapshot(&full_sources()));
    assert_eq!(with_sources.len(), fixed_series_count() + RESOLVER_FIELDS + RECONCILE_FIELDS);

    // Record everything, every label combination, extreme values and hostile
    // free text: the series set (beyond the 7 optional gauges) must not move.
    for step in 0..2_000 {
        script_step(&m, None, step);
    }
    for &meth in RpcMethod::ALL {
        for &c in ErrorClass::ALL {
            m.rpc_error(meth, c);
        }
        m.rpc_latency(meth, Duration::MAX);
    }
    m.set_halted("evil\"} 1\n# TYPE prism_evil counter\nprism_evil_total{a=\"b\"} 9");
    m.set_last_error("method=\"getEvil\" class=\"x\"\nprism_state{state=\"pwned\"} 1");
    m.set_tip(1);
    m.set_processed(1);
    m.set_flushed(1);
    m.set_finalized(1);
    m.set_changelog(1, 1);

    let (text, busy) = render_and_parse(&m.snapshot(&full_sources()));
    assert_eq!(busy.len(), fixed_series_count() + GAUGES + RESOLVER_FIELDS + RECONCILE_FIELDS);
    let fresh_keys: BTreeSet<_> = fresh.keys().collect();
    let busy_keys: BTreeSet<_> = busy.keys().collect();
    assert!(fresh_keys.is_subset(&busy_keys));
    assert!(!text.contains("evil") && !text.contains("pwned") && !text.contains("getEvil"));

    // Line count is fixed too: one HELP + TYPE pair per family.
    let families = parse_exposition(&text);
    assert_eq!(text.lines().count(), busy.len() + 2 * families.len());
}

#[test]
fn label_values_are_exactly_the_enums() {
    let (_, parsed) = render_and_parse(&Metrics::new().snapshot(&full_sources()));
    let mut by_label: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for k in parsed.keys() {
        for part in k.split('|').skip(1) {
            let (name, value) = part.split_once('=').unwrap();
            if name != "le" {
                by_label.entry(name.to_owned()).or_default().insert(value.to_owned());
            }
        }
    }
    let set = |v: Vec<&str>| v.into_iter().map(str::to_owned).collect::<BTreeSet<_>>();
    assert_eq!(by_label["method"], set(RpcMethod::ALL.iter().map(|m| m.as_str()).collect()));
    assert_eq!(by_label["class"], set(ErrorClass::ALL.iter().map(|c| c.as_str()).collect()));
    assert_eq!(by_label["table"], set(TableKind::ALL.iter().map(|t| t.as_str()).collect()));
    assert_eq!(by_label["event"], set(EventType::ALL.iter().map(|e| e.as_str()).collect()));
    assert_eq!(by_label["reason"], set(SkipReason::ALL.iter().map(|r| r.as_str()).collect()));
    assert_eq!(
        by_label["program_class"],
        set(ProgramClass::ALL.iter().map(|p| p.as_str()).collect())
    );
    assert_eq!(
        by_label["outcome"],
        set(DecodeOutcome::ALL
            .iter()
            .map(|o| o.as_str())
            .chain(["ok", "failed"])
            .collect())
    );
    assert_eq!(by_label["state"], set(PipelineState::ALL.iter().map(|s| s.as_str()).collect()));
    assert_eq!(by_label.len(), 8, "unexpected label names: {:?}", by_label.keys());

    // Labels serialize to JSON as the same strings.
    for &m in RpcMethod::ALL {
        assert_eq!(serde_json::to_value(m).unwrap(), m.as_str());
    }
    for &c in ErrorClass::ALL {
        assert_eq!(serde_json::to_value(c).unwrap(), c.as_str());
    }
    for &s in PipelineState::ALL {
        assert_eq!(serde_json::to_value(s).unwrap(), s.as_str());
    }
    for &r in &SkipReason::ALL {
        assert_eq!(serde_json::to_value(r).unwrap(), r.as_str());
    }
}

#[test]
fn json_and_prometheus_carry_the_same_numbers() {
    let m = Metrics::new();
    for step in 0..500 {
        script_step(&m, None, step);
    }
    m.set_tip(1_000);
    m.set_processed(990);
    m.set_flushed(980);
    m.set_finalized(950);
    m.set_changelog(4_096, 3);
    let s = m.snapshot(&full_sources());
    let (_, parsed) = render_and_parse(&s);
    let json: Value = serde_json::to_value(&s).unwrap();

    for (field, series) in [
        ("tip_slot", "prism_tip_slot"),
        ("processed_slot", "prism_processed_slot"),
        ("flushed_slot", "prism_flushed_slot"),
        ("finalized_slot", "prism_finalized_slot"),
        ("lag_slots", "prism_lag_slots"),
        ("changelog_bytes", "prism_changelog_bytes"),
        ("changelog_segments", "prism_changelog_segments"),
        ("slots_processed", "prism_slots_processed_total"),
    ] {
        assert_eq!(json[field].as_u64().unwrap().to_string(), parsed[&key(series, &[])], "{field}");
    }
    assert_eq!(json["lag_slots"], 10);
    for &meth in RpcMethod::ALL {
        let j = &json["rpc"][meth.as_str()];
        assert_eq!(
            j["requests"].as_u64().unwrap().to_string(),
            parsed[&key("prism_rpc_requests_total", &[("method", meth.as_str())])]
        );
        assert_eq!(
            j["latency_us"]["count"].as_u64().unwrap().to_string(),
            parsed[&key("prism_rpc_latency_seconds_count", &[("method", meth.as_str())])]
        );
        for &c in ErrorClass::ALL {
            assert_eq!(
                j["errors"][c.as_str()].as_u64().unwrap().to_string(),
                parsed[&key(
                    "prism_rpc_errors_total",
                    &[("method", meth.as_str()), ("class", c.as_str())]
                )]
            );
        }
    }
    for &t in &TableKind::ALL {
        assert_eq!(
            json["tables"][t.as_str()]["rows"].as_u64().unwrap().to_string(),
            parsed[&key("prism_rows_written_total", &[("table", t.as_str())])]
        );
    }
    assert_render_agrees(&s);
}

// ===========================================================================
// Scrubber
// ===========================================================================

const NAMES: [&str; 12] = [
    "api-key",
    "api_key",
    "apikey",
    "key",
    "token",
    "access_token",
    "auth",
    "authorization",
    "secret",
    "password",
    "sig",
    "signature",
];

fn case_variants(name: &str) -> Vec<String> {
    let mut title = name.to_owned();
    title[..1].make_ascii_uppercase();
    vec![name.to_owned(), name.to_ascii_uppercase(), title]
}

#[test]
fn every_credential_name_separator_and_case_is_redacted() {
    let separators = ["=", ":", " = ", ": ", " :", "\t=\t", "  :  "];
    let values = ["V4lu3xYz", "a.b-c_d/e+f=g~h", "%2Fenc%3Dval", "0123456789abcdef"];
    for name in NAMES {
        for written in case_variants(name) {
            for sep in separators {
                for value in values {
                    for (before, after) in [("", ""), ("ctx ", " tail"), ("(", ")"), ("x&", "&y=1"), ("[", ";next")] {
                        let input = format!("{before}{written}{sep}{value}{after}");
                        let out = scrub::clean(&input);
                        assert!(!out.contains(value), "{input:?} -> {out:?}");
                        assert!(
                            out.contains(&format!("{written}=<redacted>")),
                            "{input:?} -> {out:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn quoted_and_json_style_credentials_are_redacted() {
    for (input, secret) in [
        ("password=\"has spaces inside\" end", "has spaces inside"),
        ("password='single quoted' end", "single quoted"),
        ("{\"api_key\":\"JSONSECRET1\"}", "JSONSECRET1"),
        ("{\"token\": \"JSONSECRET2\", \"x\": 1}", "JSONSECRET2"),
        ("{'secret': 'JSONSECRET3'}", "JSONSECRET3"),
        ("headers: {\"Authorization\": \"Bearer JSONSECRET4\"}", "JSONSECRET4"),
    ] {
        let out = scrub::clean(input);
        assert!(!out.contains(secret), "{input:?} -> {out:?}");
        assert!(out.contains("<redacted>"), "{input:?} -> {out:?}");
    }
}

#[test]
fn adjacent_credentials_are_each_redacted() {
    for (input, leaks) in [
        ("password: AAA1:password: BBB2", &["AAA1", "BBB2"][..]),
        ("token=AAA1:secret=BBB2", &["AAA1", "BBB2"]),
        ("key=AAA1/api_key: BBB2", &["AAA1", "BBB2"]),
        ("sig=AAA1;signature=BBB2,auth=CCC3", &["AAA1", "BBB2", "CCC3"]),
        ("token=AAA1.Bearer BBB2", &["AAA1", "BBB2"]),
    ] {
        let out = scrub::clean(input);
        for leak in leaks {
            assert!(!out.contains(leak), "{input:?} -> {out:?}");
        }
        assert_eq!(scrub::clean(&out), out, "idempotent for {input:?}");
    }
}

#[test]
fn bearer_and_basic_tokens_are_redacted() {
    for (input, token) in [
        ("Authorization: Bearer eyJhbGciOi.payload.sig", "eyJhbGciOi"),
        ("authorization=bearer abc.def.ghi", "abc.def.ghi"),
        ("AUTHORIZATION:   BEARER   tok123", "tok123"),
        ("Authorization: Basic dXNlcjpwYXNz", "dXNlcjpwYXNz"),
        ("auth: Bearer zzz999", "zzz999"),
        ("sent header Bearer standalone-tok", "standalone-tok"),
        ("bearer\tTABBED_TOKEN", "TABBED_TOKEN"),
    ] {
        let out = scrub::clean(input);
        assert!(!out.contains(token), "{input:?} -> {out:?}");
        assert!(out.contains("=<redacted>"), "{input:?} -> {out:?}");
    }
}

#[test]
fn urls_of_every_shape_are_replaced() {
    for (input, secret) in [
        ("https://mainnet.helius-rpc.com/?api-key=URLKEY1", "URLKEY1"),
        ("http://user:URLPASS2@host:8899/path", "URLPASS2"),
        ("wss://ws.example.com/v1/URLPATH3", "URLPATH3"),
        ("ws://127.0.0.1:8900/?token=URLTOK4", "URLTOK4"),
        ("postgres+ssl://u:URLPW5@db/x", "URLPW5"),
        ("redis://:URLPW6@cache:6379/0", "URLPW6"),
        ("svn+ssh.v2://h/URLX7", "URLX7"),
        ("HTTPS://UPPER.CASE/URLX8", "URLX8"),
    ] {
        for wrapped in [
            input.to_owned(),
            format!("error sending request for url ({input}): timed out"),
            format!("\"{input}\""),
            format!("'{input}'"),
            format!("<{input}>"),
            format!("at {input}\nnext line"),
            input.to_string(),
            format!("prefix{input}"),
        ] {
            let out = scrub::clean(&wrapped);
            assert!(!out.contains(secret), "{wrapped:?} -> {out:?}");
            assert!(out.contains("<url>"), "{wrapped:?} -> {out:?}");
            assert!(!out.contains("://"), "{wrapped:?} -> {out:?}");
        }
    }
    // Two URLs, and text around them survives.
    let out = scrub::clean("from https://a.io/K1 to http://b.io/K2 done");
    assert_eq!(out, "from <url> to <url> done");
    // Not a URL: kept.
    assert_eq!(scrub::clean("ratio 3://4 and ://x"), "ratio 3://4 and ://x");
}

#[test]
fn registered_secrets_are_redacted_in_every_form() {
    scrub::register_secret("  RegSecret/With+Specials=&?  "); // trimmed
    scrub::register_secret("Overlap-Inner-Secret");
    scrub::register_secret("Prefix-Overlap-Inner-Secret-Suffix");
    scrub::register_secret("Overlap-Inner-Secret"); // twice: harmless

    for input in [
        "RegSecret/With+Specials=&?",
        "RegSecret%2FWith%2BSpecials%3D%26%3F",
        "RegSecret%2fWith%2bSpecials%3d%26%3f",
        "no delimiter:RegSecret/With+Specials=&?stuck-on",
        "twice RegSecret/With+Specials=&? and RegSecret/With+Specials=&?",
        "Prefix-Overlap-Inner-Secret-Suffix",
        "Overlap-Inner-Secret",
        "a Overlap-Inner-Secret b Prefix-Overlap-Inner-Secret-Suffix c",
    ] {
        let out = scrub::clean(input);
        for fragment in ["RegSecret", "Overlap-Inner", "Prefix-Overlap", "Specials"] {
            assert!(!out.contains(fragment), "{input:?} -> {out:?}");
        }
        assert!(out.contains("<redacted>"), "{input:?} -> {out:?}");
    }
    assert_eq!(
        scrub::clean("twice RegSecret/With+Specials=&? and RegSecret/With+Specials=&?"),
        "twice <redacted> and <redacted>"
    );
}

#[test]
fn short_secrets_are_not_registered() {
    scrub::register_secret("");
    scrub::register_secret("   ");
    scrub::register_secret("abc");
    assert_eq!(scrub::clean("abc abcd"), "abc abcd");
    assert_eq!(scrub::clean(""), "");
}

#[test]
fn control_characters_become_spaces() {
    let controls: String = (0u32..0x20).chain([0x7f]).chain(0x80..0xa0).filter_map(char::from_u32).collect();
    let out = scrub::clean(&format!("a{controls}b"));
    assert_eq!(out.chars().count(), controls.chars().count() + 2);
    assert!(out.chars().all(|c| !c.is_control()), "{out:?}");
    assert_eq!(out, format!("a{}b", " ".repeat(controls.chars().count())));
    // Control characters cannot hide a credential from redaction, wherever
    // they sit around the separator.
    for input in [
        "token=\u{0}SNEAKY",
        "token:\nSNEAKY",
        "token\u{0}=SNEAKY",
        "password\r\n:\r\nSNEAKY",
        "Authorization:\u{1b}Bearer\u{7f}SNEAKY",
        "Bearer\u{0}SNEAKY",
    ] {
        let out = scrub::clean(input);
        assert!(!out.contains("SNEAKY"), "{input:?} -> {out:?}");
    }
}

#[test]
fn output_cap_is_exact_at_the_boundary() {
    let exactly = "x".repeat(scrub::MAX_OUTPUT_CHARS);
    assert_eq!(scrub::clean(&exactly), exactly);

    let over = "y".repeat(scrub::MAX_OUTPUT_CHARS + 1);
    let out = scrub::clean(&over);
    assert_eq!(out.chars().count(), scrub::MAX_OUTPUT_CHARS);
    assert_eq!(out, format!("{}...", "y".repeat(scrub::MAX_OUTPUT_CHARS - 3)));

    for ch in ['é', '€', '𝄞'] {
        let at = ch.to_string().repeat(scrub::MAX_OUTPUT_CHARS);
        assert_eq!(scrub::clean(&at), at, "{ch} at the cap is untouched");
        let over = ch.to_string().repeat(scrub::MAX_OUTPUT_CHARS + 50);
        let out = scrub::clean(&over);
        assert_eq!(out.chars().count(), scrub::MAX_OUTPUT_CHARS);
        assert!(out.ends_with("..."));
    }
}

#[test]
fn input_cut_never_splits_a_character() {
    // A 4-byte char straddling every offset around the 64 KiB cut.
    for pad in 0..4 {
        let input = format!("{}{}", "a".repeat(scrub::MAX_INPUT_BYTES - pad), "𝄞".repeat(10));
        let out = scrub::clean(&input);
        assert_eq!(out.chars().count(), scrub::MAX_OUTPUT_CHARS);
    }
    // 8 MiB of input is cut, not scanned whole.
    let huge = "token=abc ".repeat(800_000);
    let out = scrub::clean(&huge);
    assert!(out.chars().count() <= scrub::MAX_OUTPUT_CHARS);
    assert!(!out.contains("abc"));
}

#[test]
fn clean_is_idempotent_and_never_leaks_randomized() {
    scrub::register_secret("FuzzRegisteredSecret42");
    let fragments = [
        "token=FUZZLEAK", "https://h.io/?api-key=FUZZLEAK", "FuzzRegisteredSecret42",
        "Bearer FUZZLEAK", " ", "\n", "\t", "\u{0}",
        "é", "𝄞", "word", "key", "=", ":", "\"", "'", "(", ")", "<", ">", "&", "/", "monkey=fine",
        "password: FUZZLEAK", "Authorization: Basic FUZZLEAK",
    ];
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..3_000 {
        let n = (next() % 30) as usize;
        let input: String = (0..n).map(|_| fragments[(next() % fragments.len() as u64) as usize]).collect();
        let out = scrub::clean(&input);
        assert!(out.chars().count() <= scrub::MAX_OUTPUT_CHARS, "{input:?}");
        assert!(out.chars().all(|c| !c.is_control()), "{input:?} -> {out:?}");
        assert!(!out.contains("FuzzRegisteredSecret42"), "{input:?} -> {out:?}");
        assert!(!out.contains("h.io"), "{input:?} -> {out:?}");
        // Only a whole leaked token counts; FUZZLEAK only appears after a
        // credential name, a URL or "Bearer" in the fragment list, but the
        // generator can glue it after an unrelated word, so check the
        // well-formed cases precisely instead.
        if !out.ends_with("...") {
            assert_eq!(scrub::clean(&out), out, "not idempotent for {input:?}");
        }
    }
    for f in fragments.iter().filter(|f| f.contains("FUZZLEAK")) {
        for (before, after) in [("", ""), (" ", " "), ("x ", " y")] {
            let input = format!("{before}{f}{after}");
            assert!(!scrub::clean(&input).contains("FUZZLEAK"), "{input:?}");
        }
    }
}

// ===========================================================================
// Agreement with the system: real adapter errors over HTTP
// ===========================================================================

struct HttpReply {
    status: u16,
    body: String,
}

/// Answer every request with `reply`, or never answer when `None`.
async fn serve(reply: Option<HttpReply>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let reply = reply.map(Arc::new);
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let reply = reply.clone();
            tokio::spawn(async move {
                if !read_request(&mut stream).await {
                    return;
                }
                let Some(reply) = reply else {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    return;
                };
                let response = format!(
                    "HTTP/1.1 {} Scripted\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.status,
                    reply.body.len(),
                    reply.body,
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

async fn read_request(stream: &mut TcpStream) -> bool {
    let mut request = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let Ok(read) = stream.read(&mut chunk).await else { return false };
        if read == 0 {
            return false;
        }
        request.extend_from_slice(&chunk[..read]);
        let Some(head_len) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
            continue;
        };
        let head = String::from_utf8_lossy(&request[..head_len]).to_ascii_lowercase();
        let body_len = head
            .lines()
            .find_map(|l| l.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse::<usize>().unwrap());
        if request.len() >= head_len + 4 + body_len {
            return true;
        }
    }
}

const API_KEY: &str = "AdapterApiKey-7f3a9c";

fn adapter_for(url: &str) -> SolanaAdapter {
    let config = SolanaAdapterConfig {
        base: AdapterConfig {
            max_retries: 1,
            retry_base_delay_ms: 1,
            retry_max_delay_ms: 2,
            request_timeout_ms: 300,
            ..AdapterConfig::new("solana-metrics-test")
        },
        requests_per_second: 1_000,
        ..SolanaAdapterConfig::new("solana-metrics-test", format!("{url}/?api-key={API_KEY}"))
    };
    SolanaAdapter::new(config).unwrap()
}

/// Run one failing call the way the pipeline meters it, and return the error.
async fn metered_failure(metrics: &Metrics, url: &str) -> PrismError {
    let adapter = adapter_for(url);
    let err = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap_err();
    let method = RpcMethod::from_name("getLatestBlockhash");
    let a = adapter.metrics().snapshot();
    for _ in 0..a.requests {
        metrics.rpc_request(method);
    }
    for _ in 0..a.retries {
        metrics.rpc_retry(method);
    }
    for _ in 0..a.errors {
        metrics.rpc_error(method, ErrorClass::of(&err));
    }
    metrics.set_last_error(&err.to_string());
    metrics.set_halted(&format!("{err:?}"));
    err
}

fn rpc_error_body(code: i64, message: &str) -> String {
    serde_json::json!({"jsonrpc": "2.0", "id": 0, "error": {"code": code, "message": message}})
        .to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_classes_match_real_adapter_failures() {
    let refused = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        format!("http://{}", l.local_addr().unwrap())
    }; // dropped: nothing listens there
    let cases: Vec<(&str, String, ErrorClass)> = vec![
        ("refused", refused, ErrorClass::Transport),
        (
            "http 500",
            serve(Some(HttpReply { status: 500, body: String::new() })).await,
            ErrorClass::Http,
        ),
        (
            "http 429",
            serve(Some(HttpReply { status: 429, body: String::new() })).await,
            ErrorClass::RateLimited,
        ),
        (
            "json-rpc error",
            serve(Some(HttpReply {
                status: 200,
                body: rpc_error_body(-32602, &format!("Invalid params: api-key={API_KEY}")),
            }))
            .await,
            ErrorClass::Rpc,
        ),
        ("timeout", serve(None).await, ErrorClass::Timeout),
    ];

    for (name, url, expected) in cases {
        let metrics = Metrics::new();
        let err = metered_failure(&metrics, &url).await;
        assert_eq!(ErrorClass::of(&err), expected, "{name}: {err:?}");

        let s = fresh_snapshot(&metrics);
        let r = &s.rpc["getLatestBlockhash"];
        assert!(r.requests >= 1, "{name}");
        assert_eq!(
            r.errors[expected.as_str()],
            r.errors.values().sum::<u64>(),
            "{name}: every error lands in one class"
        );
        assert_no_leak(&metrics, &[API_KEY, &url], name);
    }
}

/// Neither JSON nor Prometheus contains any of `secrets`, and free text never
/// reaches Prometheus.
fn assert_no_leak(metrics: &Metrics, secrets: &[&str], what: &str) {
    let s = metrics.snapshot(&full_sources());
    let json = serde_json::to_string(&s).unwrap();
    let prom = assert_render_agrees(&s);
    for secret in secrets {
        assert!(!json.contains(secret), "{what}: JSON leaks {secret}: {json}");
        assert!(!prom.contains(secret), "{what}: Prometheus leaks {secret}");
    }
    for leaked in ["api-key=", "api_key", "://", "Bearer ", "password"] {
        assert!(!prom.contains(leaked), "{what}: Prometheus contains {leaked}");
    }
    for text in [&s.halt_reason, &s.last_error].into_iter().flatten() {
        assert!(text.chars().count() <= scrub::MAX_OUTPUT_CHARS);
        assert!(text.chars().all(|c| !c.is_control()));
        if text.len() > 12 {
            assert!(!prom.contains(text.as_str()), "{what}: free text in Prometheus");
        }
    }
}

#[test]
fn error_text_never_leaks_through_snapshots() {
    scrub::register_secret("Registered-Rpc-Url-Secret-91");
    let metrics = Metrics::new();
    let texts = [
        PrismError::config("rpc_url", "https://rpc.io/v1?api-key=LEAK_A must be ws").to_string(),
        format!("{:?}", PrismError::account_resolve("Mint111", "token=LEAK_B rejected")),
        PrismError::changelog("/data/x", "sync", "Authorization: Bearer LEAK_C").to_string(),
        "upstream said password=LEAK_D\nretry with secret: LEAK_E".to_owned(),
        "dial wss://user:LEAK_F@node:8900 failed".to_owned(),
        "proxy for Registered-Rpc-Url-Secret-91 rejected".to_owned(),
        format!("{}token=LEAK_G", "pad ".repeat(100)),
    ];
    for text in &texts {
        metrics.set_halted(text);
        metrics.set_last_error(text);
        assert_no_leak(
            &metrics,
            &["LEAK_A", "LEAK_B", "LEAK_C", "LEAK_D", "LEAK_E", "LEAK_F", "Registered-Rpc-Url-Secret-91"],
            text,
        );
        let s = fresh_snapshot(&metrics);
        assert!(s.halt_reason.is_some() && s.last_error.is_some());
    }
    // A secret past the 200-character cap cannot survive either.
    assert!(!fresh_snapshot(&metrics).last_error.unwrap().contains("LEAK_G"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adapter_error_with_key_in_url_does_not_leak_even_unregistered() {
    // No register_secret for this key: the patterns alone must catch it.
    const UNREGISTERED: &str = "UnregisteredKey-55e1";
    let url = serve(Some(HttpReply { status: 500, body: format!("bad key {UNREGISTERED}") })).await;
    let config = SolanaAdapterConfig {
        base: AdapterConfig {
            max_retries: 1,
            retry_base_delay_ms: 1,
            retry_max_delay_ms: 2,
            ..AdapterConfig::new("solana-metrics-test")
        },
        ..SolanaAdapterConfig::new("solana-metrics-test", format!("{url}/?api-key={UNREGISTERED}"))
    };
    let adapter = SolanaAdapter::new(config).unwrap();
    let err = adapter.get_tip(CommitmentLevel::Confirmed).await.unwrap_err();
    let metrics = Metrics::new();
    metrics.set_last_error(&err.to_string());
    metrics.set_halted(&format!("{err:?}"));
    // The body text is the server's, not a credential assignment; the URL
    // with the key is what must not appear.
    let s = fresh_snapshot(&metrics);
    for text in [s.halt_reason.as_ref().unwrap(), s.last_error.as_ref().unwrap()] {
        assert!(!text.contains(&format!("api-key={UNREGISTERED}")), "{text}");
        assert!(!text.contains(&url), "{text}");
    }
    assert_no_leak(&metrics, &[&format!("api-key={UNREGISTERED}"), &url], "unregistered");
}

// ===========================================================================
// Agreement with the system: decoder, change log, sources, labels
// ===========================================================================

fn ix(program_id: &str, data_base58: String) -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot: 1,
        tx_signature: "sig".to_owned(),
        instruction_index: 0,
        inner_instruction_index: None,
        program_id: program_id.to_owned(),
        accounts: (1..=3u8).map(|n| Pubkey::new_from_array([n; 32]).to_string()).collect(),
        data_base58,
        instruction_type: None,
        decoded_data: None,
    }
}

#[test]
fn instruction_counts_agree_with_the_decoder_registry() {
    let key = |n: u8| Pubkey::new_from_array([n; 32]);
    let spl = spl_token_interface::id();
    let t22 = spl_token_2022_interface::id();
    #[allow(deprecated)]
    let transfer = spl_token_interface::instruction::transfer(&spl, &key(1), &key(2), &key(3), &[], 7)
        .unwrap();
    #[allow(deprecated)]
    let transfer_22 =
        spl_token_2022_interface::instruction::transfer(&t22, &key(1), &key(2), &key(3), &[], 9)
            .unwrap();
    let enc = |d: &[u8]| bs58::encode(d).into_string();

    let mut batch = Vec::new();
    for _ in 0..13 {
        batch.push(ix(&spl.to_string(), enc(&transfer.data)));
    }
    for _ in 0..5 {
        batch.push(ix(&t22.to_string(), enc(&transfer_22.data)));
    }
    for _ in 0..7 {
        batch.push(ix("11111111111111111111111111111111", enc(&[2, 0, 0, 0])));
        batch.push(ix("ComputeBudget111111111111111111111111111111", enc(&[3])));
        batch.push(ix(&key(9).to_string(), enc(&[1, 2, 3])));
    }
    for _ in 0..4 {
        batch.push(ix(&spl.to_string(), "0OIl-not-base58".to_owned()));
        batch.push(ix(&spl.to_string(), enc(&[3]))); // truncated Transfer
        batch.push(ix(&spl.to_string(), enc(&[250, 1, 2]))); // unknown tag
    }

    let registry = DecoderRegistry::with_spl_token();
    let metrics = Metrics::new();
    let keys: Vec<String> = batch[0].accounts.clone();
    for row in &batch {
        let outcome = match registry.decode(row, &keys) {
            Ok(Some(_)) => DecodeOutcome::Decoded,
            Ok(None) => DecodeOutcome::NoDecoder,
            Err(_) => DecodeOutcome::Error,
        };
        metrics.instruction(ProgramClass::of(&row.program_id), outcome);
    }

    let s = fresh_snapshot(&metrics);
    let reg = registry.metrics().snapshot();
    let total = |o: DecodeOutcome| -> u64 {
        s.instructions.values().map(|by| by[o.as_str()]).sum()
    };
    assert_eq!(total(DecodeOutcome::Decoded), reg.successful_decodes);
    assert_eq!(total(DecodeOutcome::NoDecoder), reg.no_decoder_found);
    assert_eq!(total(DecodeOutcome::Error), reg.decode_errors);
    assert_eq!(
        s.instructions.values().flat_map(|by| by.values()).sum::<u64>(),
        reg.total_decode_attempts
    );
    assert_eq!(reg.total_decode_attempts, batch.len() as u64);

    assert_eq!(s.instructions["spl_token"]["decoded"], 13);
    assert_eq!(s.instructions["token_2022"]["decoded"], 5);
    assert_eq!(s.instructions["system"]["no_decoder"], 7);
    assert_eq!(s.instructions["compute_budget"]["no_decoder"], 7);
    assert_eq!(s.instructions["other"]["no_decoder"], 7);
    assert_eq!(
        s.instructions["spl_token"]["error"] + s.instructions["spl_token"]["no_decoder"],
        12
    );
    assert!(s.instructions["spl_token"]["error"] >= 4, "invalid base58 is an error");

    // And the rendered lines carry the same totals.
    let (_, parsed) = render_and_parse(&s);
    let rendered_decoded: u64 = ProgramClass::ALL
        .iter()
        .map(|p| {
            parsed[&key_of("prism_instructions_total", &[("program_class", p.as_str()), ("outcome", "decoded")])]
                .parse::<u64>()
                .unwrap()
        })
        .sum();
    assert_eq!(rendered_decoded, reg.successful_decodes);
}

fn key_of(name: &str, labels: &[(&str, &str)]) -> String {
    key(name, labels)
}

fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-metrics-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn event_counts_and_changelog_bytes_agree_with_the_change_log() {
    let dir = temp_dir();
    let log = LocalChangeLog::open(&dir, "solana-metrics-test").unwrap();
    let metrics = Metrics::new();

    let mut events = Vec::new();
    for slot in 100..130u64 {
        events.push(ChangeEvent::New {
            slot,
            blockhash: format!("hash{slot}"),
            parent_slot: slot - 1,
            block_time: Some(1_700_000_000),
            transaction_count: 3,
            parquet_paths: vec![format!("slots_{slot}_{slot}.parquet")],
        });
        if slot % 4 == 0 {
            events.push(ChangeEvent::Skip { slot: slot + 1000, reason: SkipReason::ValidatorSkipped });
        }
        if slot % 10 == 0 {
            events.push(ChangeEvent::Final { slot, blockhash: format!("hash{slot}"), previous_slot: slot - 10 });
        }
    }
    events.push(ChangeEvent::Correction {
        from_slot: 100,
        to_slot: 105,
        reason: CorrectionReason::Mismatched,
        affected: 2,
    });

    for event in events {
        let kind = EventType::of(&event);
        log.emit(event).unwrap();
        metrics.event_emitted(kind);
    }
    log.flush().unwrap();
    let stats = log.stats();
    let on_disk = std::fs::metadata(log.path()).unwrap().len();
    metrics.set_changelog(stats.bytes_written, 1);

    let s = fresh_snapshot(&metrics);
    assert_eq!(s.events["new"], stats.new_events);
    assert_eq!(s.events["skip"], stats.skip_events);
    assert_eq!(s.events["final"], stats.final_events);
    assert_eq!(s.events["correction"], stats.correction_events);
    assert_eq!(s.events.values().sum::<u64>(), stats.total_events());
    assert_eq!(s.changelog_bytes, Some(on_disk), "gauge agrees with the file on disk");
    assert_eq!(stats.bytes_written, on_disk);

    let (_, parsed) = render_and_parse(&s);
    assert_eq!(parsed[&key("prism_events_emitted_total", &[("event", "new")])], "30");
    assert_eq!(parsed[&key("prism_changelog_bytes", &[])], on_disk.to_string());
    assert_render_agrees(&s);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn source_counters_agree_field_by_field() {
    // Distinct values per field, so a swapped field cannot go unnoticed.
    let resolver = ResolverMetricsSnapshot {
        total_transfers_processed: 101,
        fully_enriched: 102,
        partially_enriched: 103,
        mint_fetches_attempted: 104,
        mint_fetches_succeeded: 105,
        mint_fetches_failed: 106,
        cache_hits: 107,
        filled_from_metadata: 108,
    };
    let reconcile = ReconcileStats {
        passes: 201,
        passes_not_ready: 202,
        passes_inconclusive: 203,
        slots_examined: 204,
        slots_reconciled: 205,
        unlogged_skips: 206,
        rewinds: 207,
        rate_limited: 208,
        unresolved: 209,
        skip_log_malformed_lines: 210,
        last_pass: None,
        last_error: Some("source read failed: https://rpc.io/?api-key=RECONCILE_LEAK token=RECONCILE_LEAK".to_owned()),
    };
    let metrics = Metrics::new();
    let s = metrics.snapshot(&Sources { resolver: Some(resolver), reconcile: Some(reconcile.clone()) });
    let (_, parsed) = render_and_parse(&s);
    let get = |name: &str| parsed[&key(name, &[])].parse::<u64>().unwrap();

    assert_eq!(get("prism_resolver_transfers_processed_total"), resolver.total_transfers_processed);
    assert_eq!(get("prism_resolver_fully_enriched_total"), resolver.fully_enriched);
    assert_eq!(get("prism_resolver_partially_enriched_total"), resolver.partially_enriched);
    assert_eq!(get("prism_resolver_mint_fetches_attempted_total"), resolver.mint_fetches_attempted);
    assert_eq!(get("prism_resolver_mint_fetches_succeeded_total"), resolver.mint_fetches_succeeded);
    assert_eq!(get("prism_resolver_mint_fetches_failed_total"), resolver.mint_fetches_failed);
    assert_eq!(get("prism_resolver_cache_hits_total"), resolver.cache_hits);
    assert_eq!(get("prism_resolver_filled_from_metadata_total"), resolver.filled_from_metadata);

    assert_eq!(get("prism_reconcile_passes_total"), reconcile.passes);
    assert_eq!(get("prism_reconcile_passes_not_ready_total"), reconcile.passes_not_ready);
    assert_eq!(get("prism_reconcile_passes_inconclusive_total"), reconcile.passes_inconclusive);
    assert_eq!(get("prism_reconcile_slots_examined_total"), reconcile.slots_examined);
    assert_eq!(get("prism_reconcile_slots_reconciled_total"), reconcile.slots_reconciled);
    assert_eq!(get("prism_reconcile_unlogged_skips_total"), reconcile.unlogged_skips);
    assert_eq!(get("prism_reconcile_rewinds_total"), reconcile.rewinds);
    assert_eq!(get("prism_reconcile_rate_limited_total"), reconcile.rate_limited);
    assert_eq!(get("prism_reconcile_unresolved_total"), reconcile.unresolved);
    assert_eq!(
        get("prism_reconcile_skip_log_malformed_lines_total"),
        reconcile.skip_log_malformed_lines
    );

    let json = serde_json::to_value(&s).unwrap();
    assert_eq!(json["sources"]["resolver"]["cache_hits"], 107);
    assert_eq!(json["sources"]["reconcile"]["rewinds"], 207);
    // An absent source is null in JSON and absent from Prometheus.
    let s = metrics.snapshot(&Sources { resolver: None, reconcile: Some(reconcile) });
    assert!(serde_json::to_value(&s).unwrap()["sources"]["resolver"].is_null());
    let (text, _) = render_and_parse(&s);
    assert!(!text.contains("prism_resolver_"));
    assert_render_agrees(&s);
    // The reconciler's free-text `last_error` is never exposed.
    let json = serde_json::to_string(&s).unwrap();
    assert!(!json.contains("RECONCILE_LEAK") && !text.contains("RECONCILE_LEAK"));
}

#[test]
fn every_rpc_method_the_adapter_calls_has_its_own_label() {
    let source = include_str!("../src/adapter/solana.rs");
    let mut methods = BTreeSet::new();
    for marker in ["with_retry(\"", "attempt(\""] {
        for (i, _) in source.match_indices(marker) {
            let rest = &source[i + marker.len()..];
            let name = &rest[..rest.find('"').unwrap()];
            methods.insert(name.to_owned());
        }
    }
    assert!(methods.len() >= 5, "found {methods:?}");
    for name in &methods {
        let label = RpcMethod::from_name(name);
        assert_ne!(label, RpcMethod::Other, "adapter method {name} has no label");
        assert_eq!(label.as_str(), name);
    }
    assert_eq!(RpcMethod::from_name("getUnknownThing"), RpcMethod::Other);
    assert_eq!(RpcMethod::from_name(""), RpcMethod::Other);
}

#[test]
fn table_and_skip_labels_are_the_systems_own_names() {
    let m = Metrics::new();
    for (i, &t) in TableKind::ALL.iter().enumerate() {
        m.rows_written(t, i as u64 + 1);
    }
    for (i, &r) in SkipReason::ALL.iter().enumerate() {
        for _ in 0..=i {
            m.slot_skipped(r);
        }
    }
    let s = fresh_snapshot(&m);
    for (i, &t) in TableKind::ALL.iter().enumerate() {
        // The label is the table directory name the writer uses on disk.
        assert_eq!(s.tables[t.as_str()].rows, i as u64 + 1);
    }
    for (i, &r) in SkipReason::ALL.iter().enumerate() {
        assert_eq!(s.slots_skipped[r.as_str()], i as u64 + 1);
        let event = ChangeEvent::Skip { slot: 1, reason: r };
        assert_eq!(EventType::of(&event), EventType::Skip);
    }
    assert_render_agrees(&s);
}

#[test]
fn program_classes_agree_with_the_decoder_program_ids() {
    use prism::decoder::spl_token::{SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
    assert_eq!(ProgramClass::of(SPL_TOKEN_PROGRAM_ID), ProgramClass::SplToken);
    assert_eq!(ProgramClass::of(TOKEN_2022_PROGRAM_ID), ProgramClass::Token2022);
    assert_eq!(ProgramClass::of(&spl_token_interface::id().to_string()), ProgramClass::SplToken);
    assert_eq!(
        ProgramClass::of(&spl_associated_token_account::id().to_string()),
        ProgramClass::AssociatedToken
    );
    assert_eq!(ProgramClass::of("11111111111111111111111111111111"), ProgramClass::System);
    assert_eq!(ProgramClass::of(""), ProgramClass::Other);
    assert_eq!(ProgramClass::of("not-a-program"), ProgramClass::Other);
}
