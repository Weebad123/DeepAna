//! `digest/`: exact output and scheduling.
//!
//! * **Assembly** (pure): episodes merge exactly, rank exactly, counts and
//!   `omitted` are exact, every sentence template renders exactly, and no
//!   permutation of the anomaly rows changes a byte of the body.
//! * **Scheduling** (scripted LENS, fake coverage, real store and auditor):
//!   the first window is "from now", a window is built only when its data is
//!   in and every enabled detector has scored it, a catch-up is in order and
//!   capped per tick, a stored digest is never replaced, a failed build
//!   stores nothing and is retried, and a ledger reset builds nothing.

mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::TempDir;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensHealthStatus, LensResponse, Visibility};
use oracle::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL};
use oracle::digest::*;
use oracle::quality::{
    self, Auditor, CheckResult, Clock, Group, Overall, QualityReport, Status, StatusSummary, TOKEN_PROGRAM_ID, Window,
};
use oracle::store::{Anomaly, AnomalyKind, NewAnomaly, Store};
use serde_json::{Map, Value, json};

const BS: u64 = 200;

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

// ---------------------------------------------------------------------------
// Assembly fixtures
// ---------------------------------------------------------------------------

/// Window 1: buckets 50..=99, slots 10000..=19999.
fn win() -> DigestWindow {
    DigestWindow::at(1, BS).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn row(id: i64, metric: &str, subject: &str, bucket: u64, kind: AnomalyKind, value: f64, baseline: Option<f64>, score: f64, detail: Option<Value>) -> Anomaly {
    Anomaly {
        id,
        metric: metric.into(),
        subject: subject.into(),
        bucket,
        start_slot: bucket * BS,
        end_slot: bucket * BS + BS - 1,
        kind,
        value,
        baseline,
        scale: Some(1.0),
        score,
        detail,
        detected_at: 0,
    }
}

fn series(id: i64, metric: &str, bucket: u64, value: f64, baseline: f64, score: f64) -> Anomaly {
    let dir = if score < 0.0 { "down" } else { "up" };
    row(id, metric, "", bucket, AnomalyKind::Series, value, Some(baseline), score, Some(json!({ "direction": dir })))
}

fn first_seen(id: i64, metric: &str, subject: &str, bucket: u64, first_slot: u64) -> Anomaly {
    row(id, metric, subject, bucket, AnomalyKind::FirstSeen, 1.0, None, 1.0, Some(json!({ "first_slot": first_slot })))
}

fn data() -> WindowData {
    WindowData {
        overview: Overview { transactions: 1000, failed_transactions: 50, instructions: 3000, token_transfers: 400 },
        programs: vec![(TOKEN_PROGRAM_ID.into(), 1200), (key(5), 300)],
        mints: vec![(key(1), 250)],
    }
}

fn check(id: &'static str, status: Status, violations: u64) -> CheckResult {
    CheckResult {
        id,
        group: Group::Integrity,
        status,
        examined: 10,
        violations,
        samples: vec![],
        message: "",
        error_kind: None,
        lag_secs: None,
        query_count: 1,
        duration_ms: 0,
    }
}

fn report() -> QualityReport {
    let checks = vec![
        check("slot_duplicates", Status::Pass, 0),
        check("spl_decode_rate", Status::Warn, 4),
        check("orphan_transactions", Status::Fail, 2),
        check("parent_link_missing", Status::Fail, 1),
    ];
    QualityReport {
        as_of_slot: 20_400,
        window: Window { from_slot: 401, to_slot: 20_400, slots: 20_000 },
        generated_at: 0,
        overall: Overall::Fail,
        summary: StatusSummary::of(&checks),
        checks,
        queries_run: 0,
        duration_ms: 0,
    }
}

fn coverage() -> CoverageSection {
    CoverageSection {
        complete: true,
        detectors: vec![DetectorCoverage { id: "tx_count".into(), enabled: true, evaluated_through: Some(99) }],
        data_through_slot: None,
    }
}

fn build_with(rows: &[Anomaly], data: &WindowData, quality: Result<&QualityReport, String>) -> Digest {
    assemble(AssembleInput {
        window: win(),
        provisional: false,
        coverage: coverage(),
        data,
        anomalies: rows,
        anomalies_truncated: false,
        quality,
    })
}

fn build(rows: &[Anomaly]) -> Digest {
    let r = report();
    build_with(rows, &data(), Ok(&r))
}

fn ep_keys(d: &Digest) -> Vec<(String, String, String, u64, u64, u64, u64)> {
    d.anomalies
        .iter()
        .map(|e| (e.metric.clone(), e.subject.clone(), e.direction.clone(), e.first_bucket, e.last_bucket, e.buckets, e.peak_bucket))
        .collect()
}

fn s(v: &str) -> String {
    v.to_owned()
}

// ---------------------------------------------------------------------------
// Merging
// ---------------------------------------------------------------------------

#[test]
fn consecutive_buckets_of_one_key_merge_into_one_episode() {
    let rows = vec![
        series(1, "tx_count", 60, 500.0, 100.0, 5.0),
        series(2, "tx_count", 61, 900.0, 100.0, 9.0),
        series(3, "tx_count", 62, 700.0, 100.0, 7.0),
        // A gap: a new episode.
        series(4, "tx_count", 64, 400.0, 100.0, 4.0),
        // Same metric and bucket range, other direction: its own episode.
        series(5, "tx_count", 63, 10.0, 100.0, -6.0),
        // Other metric, adjacent bucket: its own episode.
        series(6, "transfer_count", 62, 90.0, 30.0, 3.0),
    ];
    let d = build(&rows);
    assert_eq!(
        ep_keys(&d),
        vec![
            (s("tx_count"), s(""), s("up"), 60, 62, 3, 61),
            (s("tx_count"), s(""), s("down"), 63, 63, 1, 63),
            (s("tx_count"), s(""), s("up"), 64, 64, 1, 64),
            (s("transfer_count"), s(""), s("up"), 62, 62, 1, 62),
        ]
    );
    let first = &d.anomalies[0];
    assert_eq!((first.peak_score, first.peak_value, first.baseline), (9.0, 900.0, Some(100.0)));
    assert_eq!(d.omitted.anomalies, 0);
}

#[test]
fn direction_falls_back_to_the_sign_of_value_minus_baseline() {
    let rows = vec![
        row(1, "fee_per_tx", "", 70, AnomalyKind::Series, 10.0, Some(50.0), -4.0, None),
        row(2, "fee_per_tx", "", 71, AnomalyKind::Series, 90.0, Some(50.0), 4.5, Some(json!({}))),
        row(3, "fee_per_tx", "", 72, AnomalyKind::Series, 90.0, None, 3.0, None),
        // detail.direction wins over the sign.
        row(4, "fee_per_tx", "", 73, AnomalyKind::Series, 90.0, Some(50.0), 3.5, Some(json!({"direction": "down"}))),
    ];
    let dirs: Vec<_> = rows.iter().map(direction_of).collect();
    assert_eq!(dirs, ["down", "up", "up", "down"]);
    // 71 and 72 merge (both up); 70 and 73 are separate downs (not adjacent).
    let d = build(&rows);
    assert_eq!(
        ep_keys(&d),
        vec![
            (s("fee_per_tx"), s(""), s("up"), 71, 72, 2, 71),
            (s("fee_per_tx"), s(""), s("down"), 70, 70, 1, 70),
            (s("fee_per_tx"), s(""), s("down"), 73, 73, 1, 73),
        ]
    );
}

#[test]
fn a_peak_tie_keeps_the_earliest_bucket_and_negative_scores_rank_by_magnitude() {
    let rows = vec![
        series(1, "tx_count", 80, 30.0, 300.0, -8.0),
        series(2, "tx_count", 81, 20.0, 300.0, -9.0),
        series(3, "tx_count", 82, 25.0, 300.0, -9.0),
    ];
    let d = build(&rows);
    assert_eq!(ep_keys(&d), vec![(s("tx_count"), s(""), s("down"), 80, 82, 3, 81)]);
    assert_eq!(d.anomalies[0].peak_score, 9.0);
}

#[test]
fn per_subject_rows_merge_per_subject() {
    let a = key(7);
    let b = key(8);
    let pf = |id, subj: &str, bucket, score| {
        row(id, "program_failure_rate", subj, bucket, AnomalyKind::ProgramFailure, 0.4, Some(0.02), score, Some(json!({"direction": "up"})))
    };
    let rows = vec![pf(1, &a, 55, 6.0), pf(2, &b, 56, 7.0), pf(3, &a, 56, 6.5), pf(4, &b, 58, 5.0)];
    let d = build(&rows);
    let mut want = vec![
        (s("program_failure_rate"), b.clone(), s("up"), 56, 56, 1, 56),
        (s("program_failure_rate"), a.clone(), s("up"), 55, 56, 2, 56),
        (s("program_failure_rate"), b.clone(), s("up"), 58, 58, 1, 58),
    ];
    assert_eq!(ep_keys(&d), want);
    want.clear();
}

#[test]
fn rows_outside_the_window_and_first_seen_rows_never_become_episodes() {
    let rows = vec![
        series(1, "tx_count", 49, 900.0, 100.0, 50.0),
        series(2, "tx_count", 100, 900.0, 100.0, 50.0),
        first_seen(3, FIRST_SEEN_PROGRAM, &key(3), 60, 12_001),
        series(4, "tx_count", 99, 900.0, 100.0, 5.0),
    ];
    let d = build(&rows);
    assert_eq!(ep_keys(&d), vec![(s("tx_count"), s(""), s("up"), 99, 99, 1, 99)]);
    assert_eq!(d.new_entities.programs.len(), 1);
}

// ---------------------------------------------------------------------------
// Ranking and counting
// ---------------------------------------------------------------------------

#[test]
fn ranking_is_by_peak_then_metric_subject_first_bucket() {
    let rows = vec![
        series(1, "tx_count", 90, 1.0, 0.5, 5.0),
        series(2, "fee_per_tx", 90, 1.0, 0.5, 5.0),
        series(3, "fee_per_tx", 60, 1.0, 0.5, 5.0),
        series(4, "transfer_count", 70, 1.0, 0.5, 11.0),
        row(5, "top_mint_share", &key(2), 75, AnomalyKind::Concentration, 0.9, Some(0.3), 5.0, None),
        row(6, "top_mint_share", &key(1), 75, AnomalyKind::Concentration, 0.9, Some(0.3), 5.0, None),
    ];
    let d = build(&rows);
    let order: Vec<_> = d.anomalies.iter().map(|e| (e.metric.as_str(), e.subject.clone(), e.first_bucket)).collect();
    let (k1, k2) = (key(1), key(2));
    let mut by_subject = [k1.clone(), k2.clone()];
    by_subject.sort();
    assert_eq!(
        order,
        vec![
            ("transfer_count", s(""), 70),
            ("fee_per_tx", s(""), 60),
            ("fee_per_tx", s(""), 90),
            ("top_mint_share", by_subject[0].clone(), 75),
            ("top_mint_share", by_subject[1].clone(), 75),
            ("tx_count", s(""), 90),
        ]
    );
}

#[test]
fn only_the_top_anomalies_are_kept_and_the_rest_are_counted() {
    // 13 isolated episodes (every other bucket), scores 1..=13.
    let rows: Vec<_> = (0..13).map(|i| series(i, "tx_count", 50 + 2 * i as u64, 500.0, 100.0, (i + 1) as f64)).collect();
    let d = build(&rows);
    assert_eq!(d.anomalies.len(), TOP_ANOMALIES);
    assert_eq!(d.omitted.anomalies, 3);
    assert_eq!(d.omitted.trimmed_for_size, 0);
    let peaks: Vec<f64> = d.anomalies.iter().map(|e| e.peak_score).collect();
    assert_eq!(peaks, vec![13.0, 12.0, 11.0, 10.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0]);
    // Headline: overview + 10 episodes + quality (3 sentences) + busiest (2).
    assert_eq!(d.headline.len(), 1 + 10 + 4 + 2);
}

// ---------------------------------------------------------------------------
// Order never matters
// ---------------------------------------------------------------------------

fn mixed_rows() -> Vec<Anomaly> {
    let mut rows = vec![
        series(1, "tx_count", 60, 500.0, 100.0, 5.0),
        series(2, "tx_count", 61, 900.0, 100.0, 9.0),
        series(3, "tx_count", 62, 700.0, 100.0, 9.0),
        series(4, "tx_count", 70, 10.0, 100.0, -6.0),
        series(5, "fee_per_tx", 61, 9000.0, 5000.0, 6.0),
        series(6, "transfer_count", 61, 90.0, 30.0, 6.0),
        row(7, "top_mint_share", &key(9), 61, AnomalyKind::Concentration, 0.9, Some(0.3), 6.0, None),
        row(8, "program_failure_rate", "bad id", 63, AnomalyKind::ProgramFailure, 0.5, Some(0.1), 7.0, None),
        first_seen(20, FIRST_SEEN_MINT, "", 66, 13_200),
    ];
    for i in 0..8u8 {
        rows.push(first_seen(30 + i64::from(i), FIRST_SEEN_PROGRAM, &key(40 + i), 60 + u64::from(i % 3), 12_000 + u64::from(i)));
        rows.push(first_seen(50 + i64::from(i), FIRST_SEEN_MINT, &key(60 + i), 65 - u64::from(i % 2), 13_000 + u64::from(i)));
    }
    for i in 0..10 {
        rows.push(series(100 + i, "tx_failure_rate", 72 + 2 * i as u64, 0.3, 0.05, 3.0 + (i % 4) as f64));
    }
    rows
}

/// A deterministic shuffle.
fn shuffled<T: Clone>(v: &[T], seed: u64) -> Vec<T> {
    let mut out = v.to_vec();
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    for i in (1..out.len()).rev() {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let j = (x >> 33) as usize % (i + 1);
        out.swap(i, j);
    }
    out
}

#[test]
fn no_permutation_of_the_rows_changes_the_body() {
    let rows = mixed_rows();
    let want = build(&rows);
    let want_json = serde_json::to_string(&want).unwrap();
    assert!(want.omitted.anomalies > 0, "the fixture must exercise the cut-off");
    assert!(want.new_entities.omitted > 0, "the fixture must exercise and-N-more");

    let mut reversed = rows.clone();
    reversed.reverse();
    let mut variants = vec![reversed];
    for k in 1..rows.len() {
        let mut r = rows.clone();
        r.rotate_left(k);
        variants.push(r);
    }
    for seed in 0..200 {
        variants.push(shuffled(&rows, seed));
    }
    for v in variants {
        let got = build(&v);
        assert_eq!(serde_json::to_string(&got).unwrap(), want_json);
    }
}

// ---------------------------------------------------------------------------
// Sentences
// ---------------------------------------------------------------------------

fn one_text(rows: Vec<Anomaly>) -> String {
    let d = build(&rows);
    assert_eq!(d.anomalies.len(), 1, "{:?}", d.anomalies);
    d.anomalies[0].text.clone()
}

#[test]
fn count_sentence_with_a_baseline_of_at_least_one() {
    assert_eq!(
        one_text(vec![series(1, "tx_count", 75, 910.0, 280.5, 12.7)]),
        "Transactions per bucket rose to 910 in slots 15000–15199, 3.2× its usual 280.5."
    );
    assert_eq!(
        one_text(vec![series(1, "fee_per_tx", 75, 5123.4, 5000.0, 4.0)]),
        "Mean fee per transaction rose to 5123.4 in slots 15000–15199, 1.0× its usual 5000."
    );
}

#[test]
fn count_sentence_going_down_over_several_buckets() {
    assert_eq!(
        one_text(vec![series(1, "tx_count", 80, 30.0, 300.0, -8.0), series(2, "tx_count", 81, 20.0, 300.0, -9.0)]),
        "Transactions per bucket fell to 20 at its peak in slots 16200–16399, 0.07× its usual 300. \
         The episode spanned 2 buckets, slots 16000–16399."
    );
}

#[test]
fn count_sentence_with_a_baseline_below_one() {
    assert_eq!(
        one_text(vec![series(1, "transfer_count", 51, 12.0, 0.5, 6.0)]),
        "Token transfers per bucket rose to 12 in slots 10200–10399; it is usually 0.50."
    );
    assert_eq!(
        one_text(vec![series(1, "transfer_count", 51, 12.0, 0.0, 6.0)]),
        "Token transfers per bucket rose to 12 in slots 10200–10399; it is usually 0.00."
    );
}

#[test]
fn ratio_sentence() {
    assert_eq!(
        one_text(vec![series(1, "tx_failure_rate", 52, 0.25, 0.031, 9.0)]),
        "Transaction failure rate rose to 25.0% in slots 10400–10599, from a usual 3.1%."
    );
}

#[test]
fn per_subject_sentences_name_known_programs_and_always_show_the_id() {
    let pf = row(1, "program_failure_rate", TOKEN_PROGRAM_ID, 53, AnomalyKind::ProgramFailure, 0.4, Some(0.02), 8.0, None);
    assert_eq!(
        one_text(vec![pf]),
        format!("Program failure rate for SPL Token ({TOKEN_PROGRAM_ID}) rose to 40.0% in slots 10600–10799, from a usual 2.0%.")
    );
    let m = key(1);
    let tm = row(1, "top_mint_share", &m, 54, AnomalyKind::Concentration, 0.875, Some(0.3), 8.0, None);
    assert_eq!(
        one_text(vec![tm]),
        format!("Busiest mint's share of transfers for {m} rose to 87.5% in slots 10800–10999, from a usual 30.0%.")
    );
}

#[test]
fn an_invalid_subject_is_never_echoed_and_is_counted() {
    let rows = vec![
        row(1, "top_mint_share", "'; DROP TABLE x; --", 54, AnomalyKind::Concentration, 0.9, Some(0.3), 9.0, None),
        row(2, "program_failure_rate", &bs58::encode([1u8; 31]).into_string(), 54, AnomalyKind::ProgramFailure, 0.9, Some(0.3), 8.0, None),
    ];
    let d = build(&rows);
    assert_eq!(d.anomalies[0].text, "Busiest mint's share of transfers for [invalid id] rose to 90.0% in slots 10800–10999, from a usual 30.0%.");
    assert_eq!(d.anomalies[0].subject, INVALID_ID);
    assert_eq!(d.anomalies[1].subject, INVALID_ID);
    assert_eq!(d.warnings, vec!["2 ids rendered as [invalid id]: not base58 of 32 bytes".to_owned()]);
    let json = serde_json::to_string(&d).unwrap();
    assert!(!json.contains("DROP"), "{json}");
}

#[test]
fn unknown_metrics_and_missing_or_non_finite_numbers() {
    assert_eq!(
        one_text(vec![row(1, "new_metric_2", "", 55, AnomalyKind::Series, 5.0, None, 4.0, None)]),
        "new_metric_2 rose to 5 in slots 11000–11199."
    );
    assert_eq!(
        one_text(vec![row(1, "Weird-Metric", "", 55, AnomalyKind::Series, 5.0, None, 4.0, None)]),
        "[unknown metric] rose to 5 in slots 11000–11199."
    );
    // Rendered directly: the store never holds a non-finite value, but the
    // renderer must still leave it out.
    let e = Episode {
        metric: "tx_count".into(),
        subject: String::new(),
        direction: "up".into(),
        first_bucket: 55,
        last_bucket: 55,
        buckets: 1,
        peak_bucket: 55,
        peak_score: 4.0,
        peak_value: f64::NAN,
        baseline: Some(f64::INFINITY),
        text: String::new(),
    };
    assert_eq!(render::episode(&e, BS), "Transactions per bucket was unusual in slots 11000–11199.");
}

#[test]
fn overview_sentences() {
    let d = build(&[]);
    assert_eq!(d.headline[0], "Slots 10000–19999: 1000 transactions, 50 failed (5.0%), 3000 instructions, 400 token transfers.");
    let empty = WindowData::default();
    let r = report();
    let d = build_with(&[], &empty, Ok(&r));
    assert_eq!(d.headline[0], "Slots 10000–19999: 0 transactions, 0 failed, 0 instructions, 0 token transfers.");
    let one = WindowData {
        overview: Overview { transactions: 1, failed_transactions: 1, instructions: 1, token_transfers: 1 },
        ..WindowData::default()
    };
    let d = build_with(&[], &one, Ok(&r));
    assert_eq!(d.headline[0], "Slots 10000–19999: 1 transaction, 1 failed (100.0%), 1 instruction, 1 token transfer.");
    assert_eq!(
        render::overview(&win(), 12_345, true, &one.overview),
        "Slots 10000–12345 (provisional): 1 transaction, 1 failed (100.0%), 1 instruction, 1 token transfer."
    );
}

// ---------------------------------------------------------------------------
// First seen
// ---------------------------------------------------------------------------

#[test]
fn first_seen_groups_are_ordered_capped_and_counted() {
    let p: Vec<String> = (0..7).map(|i| key(40 + i)).collect();
    let rows = vec![
        // Out of order on purpose: (bucket, subject) decides.
        first_seen(1, FIRST_SEEN_PROGRAM, &p[0], 62, 12_450),
        first_seen(2, FIRST_SEEN_PROGRAM, &p[1], 60, 12_010),
        first_seen(3, FIRST_SEEN_PROGRAM, &p[2], 60, 12_005),
        first_seen(4, FIRST_SEEN_PROGRAM, &p[3], 61, 12_300),
        first_seen(5, FIRST_SEEN_PROGRAM, &p[4], 63, 12_600),
        first_seen(6, FIRST_SEEN_PROGRAM, &p[5], 64, 12_800),
        first_seen(7, FIRST_SEEN_PROGRAM, TOKEN_PROGRAM_ID, 65, 13_000),
        // The cap marker.
        first_seen(8, FIRST_SEEN_PROGRAM, "", 64, 12_800),
        first_seen(9, FIRST_SEEN_MINT, &key(1), 70, 14_123),
        // An unknown first-seen metric.
        first_seen(10, "first_seen_account", &key(2), 70, 14_000),
    ];
    let d = build(&rows);
    let mut at60 = [(p[1].clone(), 12_010u64), (p[2].clone(), 12_005u64)];
    at60.sort();
    let want: Vec<(String, u64)> =
        vec![at60[0].clone(), at60[1].clone(), (p[3].clone(), 12_300), (p[0].clone(), 12_450), (p[4].clone(), 12_600)];
    let got: Vec<_> = d.new_entities.programs.iter().map(|e| (e.id.clone(), e.first_slot)).collect();
    assert_eq!(got, want);
    assert_eq!(d.new_entities.programs[0].text, format!("New program {} first seen at slot {}.", at60[0].0, at60[0].1));
    assert_eq!(d.new_entities.mints.len(), 1);
    assert_eq!(d.new_entities.mints[0].text, format!("New mint {} first seen at slot 14123.", key(1)));
    assert_eq!(d.new_entities.omitted, 2);
    assert_eq!(
        d.new_entities.notes,
        vec![
            "And 2 more new programs.".to_owned(),
            "New programs in slots 12800–12999 reached the detector's cap; the list may be incomplete.".to_owned(),
        ]
    );
    assert_eq!(d.warnings, vec!["1 first-seen rows of an unknown metric were skipped".to_owned()]);
    assert!(d.anomalies.is_empty(), "first-seen rows are never scored");
}

#[test]
fn a_known_new_program_is_named() {
    let d = build(&[first_seen(1, FIRST_SEEN_PROGRAM, TOKEN_PROGRAM_ID, 65, 13_000)]);
    assert_eq!(d.new_entities.programs[0].text, format!("New program SPL Token ({TOKEN_PROGRAM_ID}) first seen at slot 13000."));
    assert_eq!(render::more(1, "new mint", "new mints"), "And 1 more new mint.");
}

// ---------------------------------------------------------------------------
// Quality and busiest
// ---------------------------------------------------------------------------

#[test]
fn quality_lists_failing_checks_first_then_warnings() {
    let d = build(&[]);
    assert_eq!(d.quality.status, "available");
    assert_eq!(d.quality.overall, Some(Overall::Fail));
    assert_eq!(d.quality.as_of_slot, Some(20_400));
    let issues: Vec<_> = d.quality.issues.iter().map(|i| (i.id.as_str(), i.status.as_str(), i.violations)).collect();
    assert_eq!(issues, vec![("orphan_transactions", "fail", 2), ("parent_link_missing", "fail", 1), ("spl_decode_rate", "warn", 4)]);
    assert_eq!(
        render::quality(&d.quality),
        vec![
            "Pipeline quality is fail as of slot 20400.",
            "Quality check orphan_transactions failed: 2 violations.",
            "Quality check parent_link_missing failed: 1 violation.",
            "Quality check spl_decode_rate warned: 4 violations.",
        ]
    );
}

#[test]
fn unavailable_quality_still_builds_the_digest() {
    let d = build_with(&[], &data(), Err("timeout".into()));
    assert_eq!(d.quality.status, "unavailable");
    assert_eq!(d.quality.reason.as_deref(), Some("timeout"));
    assert!(d.quality.issues.is_empty());
    assert!(d.headline.contains(&"Pipeline quality is unavailable for this digest.".to_owned()));
    assert_eq!(d.overview, data().overview);
}

#[test]
fn busiest_sentences_and_invalid_busiest_ids() {
    let d = build(&[]);
    let n = d.headline.len();
    assert_eq!(
        d.headline[n - 2],
        format!("Busiest program: SPL Token ({TOKEN_PROGRAM_ID}) with 1200 instructions (40.0% of all instructions).")
    );
    assert_eq!(d.headline[n - 1], format!("Busiest mint: {} with 250 token transfers (62.5% of all token transfers).", key(1)));
    assert_eq!(d.busiest.programs[1], BusyProgram { id: key(5), instructions: 300 });

    let bad = WindowData { mints: vec![("x".into(), 3)], ..data() };
    let r = report();
    let d = build_with(&[], &bad, Ok(&r));
    assert_eq!(d.busiest.mints[0].id, INVALID_ID);
    assert_eq!(d.headline.last().unwrap(), "Busiest mint: [invalid id] with 3 token transfers (0.8% of all token transfers).");
    assert_eq!(d.warnings, vec!["1 id rendered as [invalid id]: not base58 of 32 bytes".to_owned()]);
}

// ---------------------------------------------------------------------------
// The whole body
// ---------------------------------------------------------------------------

#[test]
fn the_headline_is_every_sentence_in_section_order() {
    let rows = vec![
        series(1, "tx_count", 75, 910.0, 280.5, 12.7),
        series(2, "tx_failure_rate", 52, 0.25, 0.031, 9.0),
        first_seen(3, FIRST_SEEN_PROGRAM, &key(40), 60, 12_010),
        first_seen(4, FIRST_SEEN_MINT, &key(1), 70, 14_123),
    ];
    let d = build(&rows);
    assert_eq!(
        d.headline,
        vec![
            "Slots 10000–19999: 1000 transactions, 50 failed (5.0%), 3000 instructions, 400 token transfers.".to_owned(),
            "Transactions per bucket rose to 910 in slots 15000–15199, 3.2× its usual 280.5.".to_owned(),
            "Transaction failure rate rose to 25.0% in slots 10400–10599, from a usual 3.1%.".to_owned(),
            format!("New program {} first seen at slot 12010.", key(40)),
            format!("New mint {} first seen at slot 14123.", key(1)),
            "Pipeline quality is fail as of slot 20400.".to_owned(),
            "Quality check orphan_transactions failed: 2 violations.".to_owned(),
            "Quality check parent_link_missing failed: 1 violation.".to_owned(),
            "Quality check spl_decode_rate warned: 4 violations.".to_owned(),
            format!("Busiest program: SPL Token ({TOKEN_PROGRAM_ID}) with 1200 instructions (40.0% of all instructions)."),
            format!("Busiest mint: {} with 250 token transfers (62.5% of all token transfers).", key(1)),
        ]
    );
    assert_eq!(headline(&d), d.headline);
}

#[test]
fn the_body_has_the_documented_shape_and_no_generation_time() {
    let d = build(&[series(1, "tx_count", 75, 910.0, 280.5, 12.7)]);
    let v = serde_json::to_value(&d).unwrap();
    let keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
    let mut want = vec![
        "version", "provisional", "window", "coverage", "overview", "anomalies", "new_entities", "quality", "busiest",
        "omitted", "anomalies_truncated", "warnings", "headline",
    ];
    want.sort();
    let mut keys_sorted = keys.clone();
    keys_sorted.sort();
    assert_eq!(keys_sorted, want);
    assert_eq!(v["version"], 1);
    assert_eq!(
        v["window"],
        json!({"start_slot": 10000, "end_slot": 19999, "slots": 10000, "bucket_slots": 200, "first_bucket": 50, "last_bucket": 99})
    );
    assert_eq!(v["anomalies"][0]["peak_value"], 910.0);
    assert!(!v.to_string().contains("generated_at"));
    let back: Digest = serde_json::from_value(v).unwrap();
    assert_eq!(back, d);
}

#[test]
fn an_oversize_body_drops_the_lowest_ranked_anomalies_first_and_keeps_the_overview() {
    let rows: Vec<_> = (0..10).map(|i| series(i, "tx_count", 50 + 2 * i as u64, 500.0, 100.0, (i + 1) as f64)).collect();
    let mut d = build(&rows);
    let full = body_size(&d);
    let per = body_size(&d) - {
        let mut one_less = d.clone();
        one_less.anomalies.pop();
        one_less.headline = headline(&one_less);
        body_size(&one_less)
    };
    // Room for about 7 of the 10, with the warning added.
    fit(&mut d, full - 3 * per - 10);
    assert!(body_size(&d) <= full - 3 * per - 10);
    let peaks: Vec<f64> = d.anomalies.iter().map(|e| e.peak_score).collect();
    assert_eq!(peaks, (0..peaks.len()).map(|i| (10 - i) as f64).collect::<Vec<_>>(), "the highest ranked stay");
    assert!(peaks.len() <= 6 && peaks.len() >= 5, "{peaks:?}");
    assert_eq!(d.omitted.trimmed_for_size, 10 - peaks.len() as u64);
    assert_eq!(d.omitted.anomalies, d.omitted.trimmed_for_size);
    assert!(d.headline[0].starts_with("Slots 10000–19999: 1000 transactions"));
    assert_eq!(d.headline, headline(&d));
    assert!(d.warnings.last().unwrap().starts_with("trimmed to fit"));
}

#[test]
fn blocking_lists_enabled_detectors_behind_the_window() {
    let det = |id: &str, enabled, through| DetectorCoverage { id: id.into(), enabled, evaluated_through: through };
    let ds = vec![det("z", true, Some(98)), det("a", true, None), det("off", false, None), det("ok", true, Some(99))];
    assert_eq!(blocking(&ds, 99), vec!["a".to_owned(), "z".to_owned()]);
    assert!(blocking(&ds[2..], 99).is_empty());
}

// ---------------------------------------------------------------------------
// Scheduling: scripted LENS
// ---------------------------------------------------------------------------

type Reply = Result<LensResponse, LensCallError>;

fn response(rows: Vec<Value>, truncated: bool) -> LensResponse {
    let rows: Vec<Map<String, Value>> = rows.into_iter().map(|v| v.as_object().unwrap().clone()).collect();
    LensResponse { row_count: rows.len() as u64, rows, duration_ms: 1, truncated, sql: String::new(), round_trip_ms: 1 }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fault {
    None,
    /// `MAX(slot)` of `solana.slots` is `NULL`.
    EmptySlots,
    /// `solana.slots` does not exist.
    MissingTable,
    /// The watermark queries are unreachable.
    Unreachable,
    /// The busiest-programs query is truncated.
    TruncatedBuild,
}

struct Lens {
    max_slot: AtomicU64,
    max_tx: AtomicU64,
    fault: Mutex<Fault>,
    log: Mutex<Vec<String>>,
    /// While set, digest window queries wait.
    hold: tokio::sync::Semaphore,
}

impl Lens {
    fn new(w: u64) -> Arc<Self> {
        Arc::new(Self {
            max_slot: AtomicU64::new(w),
            max_tx: AtomicU64::new(w),
            fault: Mutex::new(Fault::None),
            log: Mutex::new(vec![]),
            hold: tokio::sync::Semaphore::new(tokio::sync::Semaphore::MAX_PERMITS),
        })
    }
    fn set_w(&self, w: u64) {
        self.max_slot.store(w, Ordering::SeqCst);
        self.max_tx.store(w, Ordering::SeqCst);
    }
    fn fault(&self, f: Fault) {
        *self.fault.lock().unwrap() = f;
    }
    /// The digest's own window reads; the auditor's checks (which share this
    /// LENS and use their own range) are excluded.
    fn window_queries(&self) -> Vec<String> {
        self.log.lock().unwrap().iter().filter(|q| q.contains("slot >= ") && !q.contains("AS violations")).cloned().collect()
    }
}

#[async_trait]
impl LensApi for Lens {
    async fn query(&self, sql: &str) -> Reply {
        self.log.lock().unwrap().push(sql.to_owned());
        let fault = *self.fault.lock().unwrap();
        if sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL {
            return match fault {
                Fault::Unreachable => Err(LensCallError::Unavailable { message: "connection refused".into() }),
                Fault::MissingTable => Err(LensCallError::BadQuery { message: "table 'solana.slots' not found".into() }),
                Fault::EmptySlots if sql == MAX_SLOTS_SQL => Ok(response(vec![json!({"max_slot": null})], false)),
                _ => {
                    let v = if sql == MAX_SLOTS_SQL { &self.max_slot } else { &self.max_tx };
                    Ok(response(vec![json!({"max_slot": v.load(Ordering::SeqCst)})], false))
                }
            };
        }
        if sql == quality::sql::DATA_FRESHNESS {
            return Ok(response(vec![json!({"newest": NOW as i64})], false));
        }
        if sql.contains("AS transactions") {
            let _p = self.hold.acquire().await.unwrap();
            return Ok(response(vec![json!({"transactions": 100, "failed": 5})], false));
        }
        if sql.contains("AS subject") && sql.contains("solana.instructions") {
            if fault == Fault::TruncatedBuild {
                return Ok(response(vec![], true));
            }
            return Ok(response(vec![json!({"subject": TOKEN_PROGRAM_ID, "n": 40})], false));
        }
        if sql.contains("AS subject") {
            return Ok(response(vec![json!({"subject": key(1), "n": 7})], false));
        }
        if sql.starts_with("SELECT COUNT(*) AS n FROM solana.instructions") {
            return Ok(response(vec![json!({"n": 300})], false));
        }
        if sql.starts_with("SELECT COUNT(*) AS n FROM solana.token_transfers") {
            return Ok(response(vec![json!({"n": 20})], false));
        }
        // Quality checks: healthy.
        Ok(response(vec![json!({"examined": 10, "violations": 0})], false))
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unimplemented!()
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 1 })
    }
}

/// A LENS that is always down, for the auditor.
struct DownLens;

#[async_trait]
impl LensApi for DownLens {
    async fn query(&self, _sql: &str) -> Reply {
        Err(LensCallError::Unavailable { message: "down".into() })
    }
    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unimplemented!()
    }
    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Err(LensCallError::Unavailable { message: "down".into() })
    }
}

const NOW: u64 = 1_800_000_000;

struct FixedClock(AtomicU64);
impl Clock for FixedClock {
    fn now_unix_secs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Detectors whose coverage a test sets.
struct FakeCoverage(Mutex<Vec<DetectorCoverage>>);

impl FakeCoverage {
    fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new(vec![
            DetectorCoverage { id: "tx_count".into(), enabled: true, evaluated_through: None },
            DetectorCoverage { id: "first_seen_program".into(), enabled: true, evaluated_through: None },
            DetectorCoverage { id: "fee_per_tx".into(), enabled: false, evaluated_through: None },
        ])))
    }
    /// Every enabled detector through `bucket`.
    fn through(&self, bucket: u64) {
        for d in self.0.lock().unwrap().iter_mut().filter(|d| d.enabled) {
            d.evaluated_through = Some(bucket);
        }
    }
    fn set(&self, id: &str, bucket: Option<u64>) {
        self.0.lock().unwrap().iter_mut().find(|d| d.id == id).unwrap().evaluated_through = bucket;
    }
}

impl Coverage for FakeCoverage {
    fn detectors(&self) -> Vec<DetectorCoverage> {
        self.0.lock().unwrap().clone()
    }
}

struct Rig {
    _dir: TempDir,
    store: Store,
    lens: Arc<Lens>,
    coverage: Arc<FakeCoverage>,
    digester: Digester,
    handle: DigestHandle,
}

fn rig_with(w: u64, auditor_lens: Option<Arc<dyn LensApi>>) -> Rig {
    let dir = TempDir::new();
    let store = Store::open_path(dir.path().join("oracle.db")).unwrap();
    let lens = Lens::new(w);
    let coverage = FakeCoverage::new();
    let clock: Arc<dyn Clock> = Arc::new(FixedClock(AtomicU64::new(NOW)));
    let auditor_lens = auditor_lens.unwrap_or_else(|| lens.clone() as Arc<dyn LensApi>);
    let auditor = Arc::new(Auditor::new(auditor_lens, Arc::clone(&clock)));
    let digester = Digester::new(store.clone(), lens.clone(), auditor, coverage.clone(), clock, BS);
    let handle = digester.handle();
    Rig { _dir: dir, store, lens, coverage, digester, handle }
}

fn rig(w: u64) -> Rig {
    rig_with(w, None)
}

fn stored(r: &Rig) -> Vec<(u64, u64)> {
    let mut v: Vec<_> = r.store.list_digests(1000).unwrap().into_iter().map(|d| (d.id, d.start_slot, d.end_slot)).collect();
    v.sort();
    v.into_iter().map(|(_, s, e)| (s, e)).collect()
}

const WS: u64 = 50 * BS; // 10_000

#[tokio::test]
async fn the_first_window_starts_from_now_and_waits_for_its_data() {
    // W = 10_199: complete_max = 50, so the first window is buckets 100..=149.
    let r = rig(10_199);
    r.coverage.through(10_000);
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![]);
    assert_eq!(r.handle.status().state, DigestState::Idle);

    // One slot short of the window's end: still nothing.
    r.lens.set_w(3 * WS - 2);
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![]);
    assert_eq!(r.handle.status().lag_windows, 0);

    // Window 1 (10000..19999) is complete but predates "now": never built.
    r.lens.set_w(3 * WS - 1);
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![(2 * WS, 3 * WS - 1)]);
    let st = r.handle.status();
    assert_eq!((st.state, st.windows_built, st.lag_windows), (DigestState::Idle, 1, 0));
    assert_eq!(st.last_window.unwrap().first_bucket, 100);
    assert_eq!(st.last_tick_at, Some(NOW));

    let latest = r.handle.latest().unwrap().unwrap();
    let body: Digest = serde_json::from_value(latest.body).unwrap();
    assert!(!body.provisional && body.coverage.complete);
    assert_eq!(body.overview, Overview { transactions: 100, failed_transactions: 5, instructions: 300, token_transfers: 20 });
    assert_eq!(body.quality.status, "available");
    assert_eq!(body.coverage.detectors.len(), 3);
    // Every window query was bounded to exactly this window.
    let qs = r.lens.window_queries();
    assert_eq!(qs.iter().filter(|q| q.contains("slot >= 20000 AND slot <= 29999")).count(), 5, "{qs:#?}");
}

#[tokio::test]
async fn an_enabled_detector_behind_the_window_blocks_it_and_a_disabled_one_does_not() {
    let r = rig(10_199);
    r.digester.run_once().await;
    r.lens.set_w(3 * WS - 1);
    r.coverage.set("tx_count", Some(149));
    r.coverage.set("first_seen_program", Some(148));
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![]);
    assert_eq!(r.handle.status().state, DigestState::Waiting { on: vec!["first_seen_program".into()] });
    assert_eq!(r.handle.status().lag_windows, 1);

    r.coverage.set("first_seen_program", None);
    r.digester.run_once().await;
    assert_eq!(r.handle.status().state, DigestState::Waiting { on: vec!["first_seen_program".into()] });

    // `fee_per_tx` is disabled with no progress at all: it does not block.
    r.coverage.set("first_seen_program", Some(149));
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![(2 * WS, 3 * WS - 1)]);
    assert_eq!(r.handle.status().state, DigestState::Idle);
}

#[tokio::test]
async fn a_catch_up_is_in_order_capped_per_tick_and_never_skips() {
    let r = rig(10_199);
    r.digester.run_once().await;
    // Ten windows due (2..=11), detectors through all of them.
    r.lens.set_w(12 * WS - 1);
    r.coverage.through(12 * 50);
    r.digester.run_once().await;
    let got = stored(&r);
    assert_eq!(got.len(), MAX_WINDOWS_PER_TICK);
    assert_eq!(got, (2..2 + MAX_WINDOWS_PER_TICK as u64).map(|k| (k * WS, (k + 1) * WS - 1)).collect::<Vec<_>>());
    assert_eq!(r.handle.status().lag_windows, 10 - MAX_WINDOWS_PER_TICK as u64);

    r.digester.run_once().await;
    assert_eq!(stored(&r), (2..12).map(|k| (k * WS, (k + 1) * WS - 1)).collect::<Vec<_>>());
    let st = r.handle.status();
    assert_eq!((st.windows_built, st.lag_windows, st.state), (10, 0, DigestState::Idle));

    // Insertion order is slot order: newest stored is the last window.
    assert_eq!(r.handle.list(1).unwrap()[0].start_slot, 11 * WS);
}

#[tokio::test]
async fn a_catch_up_stops_at_the_first_window_a_detector_has_not_covered() {
    let r = rig(10_199);
    r.digester.run_once().await;
    r.lens.set_w(6 * WS - 1);
    r.coverage.through(3 * 50 + 49); // through window 3
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![(2 * WS, 3 * WS - 1), (3 * WS, 4 * WS - 1)]);
    assert_eq!(r.handle.status().state, DigestState::Waiting { on: vec!["first_seen_program".into(), "tx_count".into()] });
    assert_eq!(r.handle.status().lag_windows, 2);
}

#[tokio::test]
async fn a_stored_digest_is_never_replaced_or_rebuilt() {
    let r = rig(10_199);
    r.digester.run_once().await;
    r.lens.set_w(3 * WS - 1);
    r.coverage.through(149);
    r.digester.run_once().await;
    let first = r.handle.latest().unwrap().unwrap();
    let before = r.lens.window_queries().len();
    r.digester.run_once().await;
    r.digester.run_once().await;
    assert_eq!(r.lens.window_queries().len(), before, "a built window is not rebuilt");
    assert_eq!(r.handle.list(10).unwrap(), vec![first]);
}

#[tokio::test]
async fn a_failed_build_stores_nothing_and_the_next_tick_retries_the_same_window() {
    let r = rig(10_199);
    r.digester.run_once().await;
    r.lens.set_w(4 * WS - 1);
    r.coverage.through(199);
    r.lens.fault(Fault::TruncatedBuild);
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![]);
    let st = r.handle.status();
    assert_eq!(st.state, DigestState::Failing);
    assert!(st.last_error.unwrap().contains("truncated"));

    r.lens.fault(Fault::None);
    r.digester.run_once().await;
    assert_eq!(stored(&r), vec![(2 * WS, 3 * WS - 1), (3 * WS, 4 * WS - 1)]);
    assert_eq!(r.handle.status().last_error, None);
}

#[tokio::test]
async fn digests_ahead_of_the_data_build_nothing() {
    let r = rig(10_199);
    r.store.insert_digest(90 * WS, 91 * WS - 1, &json!({"version": 1})).unwrap();
    r.coverage.through(10_000);
    r.lens.set_w(5 * WS - 1);
    r.digester.run_once().await;
    let st = r.handle.status();
    assert_eq!(st.state, DigestState::AheadOfData);
    assert!(st.last_error.unwrap().contains("ledger was reset"));
    assert_eq!(stored(&r), vec![(90 * WS, 91 * WS - 1)]);
    assert!(r.lens.window_queries().is_empty());
}

#[tokio::test]
async fn after_a_stored_digest_the_next_window_follows_it_even_if_old() {
    // Downtime: the store's latest is window 1, the data runs to window 4.
    let r = rig(5 * WS - 1);
    r.store.insert_digest(WS, 2 * WS - 1, &json!({"version": 1})).unwrap();
    r.coverage.through(1_000);
    r.digester.run_once().await;
    assert_eq!(stored(&r), (1..5).map(|k| (k * WS, (k + 1) * WS - 1)).collect::<Vec<_>>());
}

#[tokio::test]
async fn missing_or_empty_tables_are_not_ready_and_a_transient_failure_skips_the_tick() {
    let r = rig(10_199);
    r.lens.fault(Fault::EmptySlots);
    r.digester.run_once().await;
    assert_eq!(r.handle.status().state, DigestState::NotReady);
    r.lens.fault(Fault::MissingTable);
    r.digester.run_once().await;
    assert_eq!(r.handle.status().state, DigestState::NotReady);

    // Fewer slots than one bucket: no complete bucket, no first window.
    r.lens.fault(Fault::None);
    r.lens.set_w(BS - 2);
    r.digester.run_once().await;
    let st = r.handle.status();
    assert_eq!(st.state, DigestState::NotReady);
    assert_eq!(st.last_error.as_deref(), Some("no complete bucket yet"));

    r.lens.set_w(3 * WS - 1);
    r.coverage.through(149);
    r.digester.run_once().await; // first window: from now = window 1 (cm = 149)... none due
    let state_before = r.handle.status().state;
    r.lens.fault(Fault::Unreachable);
    r.digester.run_once().await;
    let st = r.handle.status();
    assert_eq!(st.state, state_before, "a transient failure keeps the state");
    assert!(st.last_error.unwrap().contains("unavailable"));
}

#[tokio::test]
async fn store_anomalies_of_the_window_reach_the_digest() {
    let r = rig(10_199);
    r.digester.run_once().await;
    let a = |bucket, value, score| NewAnomaly {
        subject: String::new(),
        bucket,
        start_slot: bucket * BS,
        end_slot: bucket * BS + BS - 1,
        kind: AnomalyKind::Series,
        value,
        baseline: Some(100.0),
        scale: Some(10.0),
        score,
        detail: Some(json!({"direction": "up"})),
    };
    // Bucket 99 is before the window, 150 after it.
    r.store.commit_scan("tx_count", &[a(99, 900.0, 50.0), a(120, 500.0, 6.0), a(121, 800.0, 9.0), a(150, 900.0, 50.0)], 160).unwrap();
    r.lens.set_w(3 * WS - 1);
    r.coverage.through(160);
    r.digester.run_once().await;
    let body: Digest = serde_json::from_value(r.handle.latest().unwrap().unwrap().body).unwrap();
    assert_eq!(body.anomalies.len(), 1);
    let e = &body.anomalies[0];
    assert_eq!((e.first_bucket, e.last_bucket, e.buckets, e.peak_bucket, e.peak_value), (120, 121, 2, 121, 800.0));
    assert_eq!(
        e.text,
        "Transactions per bucket rose to 800 at its peak in slots 24200–24399, 8.0× its usual 100. \
         The episode spanned 2 buckets, slots 24000–24399."
    );
    assert!(!body.anomalies_truncated);
}

#[tokio::test]
async fn an_unavailable_auditor_still_builds_the_digest() {
    let r = rig_with(10_199, Some(Arc::new(DownLens)));
    r.digester.run_once().await;
    r.lens.set_w(3 * WS - 1);
    r.coverage.through(149);
    r.digester.run_once().await;
    let body: Digest = serde_json::from_value(r.handle.latest().unwrap().unwrap().body).unwrap();
    assert_eq!(body.quality.status, "unavailable");
    assert_eq!(body.quality.reason.as_deref(), Some("unavailable"));
    assert!(body.headline.contains(&"Pipeline quality is unavailable for this digest.".to_owned()));
}

// ---------------------------------------------------------------------------
// Preview
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_preview_of_the_current_window_is_provisional_and_never_stored() {
    let r = rig(25_432);
    r.coverage.through(120);
    let d = r.handle.preview(None).await.unwrap();
    assert!(d.provisional);
    assert_eq!(d.window, DigestWindow::at(2, BS).unwrap());
    assert!(!d.coverage.complete);
    assert_eq!(d.coverage.data_through_slot, Some(25_432));
    assert!(d.headline[0].starts_with("Slots 20000–25432 (provisional): "), "{}", d.headline[0]);
    assert!(r.lens.window_queries().iter().all(|q| q.contains("slot >= 20000 AND slot <= 25432")));
    assert_eq!(stored(&r), vec![]);

    // An old, complete window that predates "from now": a full preview.
    let d = r.handle.preview(Some(WS)).await.unwrap();
    assert!(!d.provisional && d.coverage.complete);
    assert_eq!(d.coverage.data_through_slot, None);
    assert_eq!(stored(&r), vec![]);
}

#[tokio::test]
async fn preview_validates_its_window_and_runs_one_at_a_time() {
    let r = rig(25_432);
    assert_eq!(r.handle.preview(Some(WS + 1)).await.unwrap_err().kind(), "invalid");
    assert!(matches!(r.handle.preview(Some(3 * WS)).await, Err(DigestError::NotReady(_))));

    r.lens.hold.forget_permits(tokio::sync::Semaphore::MAX_PERMITS);
    let h = r.handle.clone();
    let first = tokio::spawn(async move { h.preview(None).await });
    while r.lens.log.lock().unwrap().iter().all(|q| !q.contains("AS transactions")) {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    assert_eq!(r.handle.preview(None).await.unwrap_err(), DigestError::Busy);
    r.lens.hold.add_permits(tokio::sync::Semaphore::MAX_PERMITS);
    assert!(first.await.unwrap().unwrap().provisional);
}
