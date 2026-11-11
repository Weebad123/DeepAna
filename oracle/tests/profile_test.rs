//! `profile/` against a scripted LENS: input safety, snapshot consistency,
//! the fixed query plan, error handling, the concurrency limit and the
//! deadline. Query results are checked against the real LENS in
//! `profile_lens_test.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensResponse, Visibility};
use oracle::profile::*;
use serde_json::{Map, Value, json};

const MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MAX_SLOTS: u64 = 300_000;
const MAX_TX: u64 = 299_990;

type Answer = dyn Fn(&str, usize) -> Result<LensResponse, LensCallError> + Send + Sync;

/// A LENS that logs every statement, tracks how many run at once, and
/// answers through `answer(sql, index)`.
struct Fake {
    log: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    delay: Duration,
    answer: Box<Answer>,
}

impl Fake {
    fn new(answer: impl Fn(&str, usize) -> Result<LensResponse, LensCallError> + Send + Sync + 'static) -> Arc<Self> {
        Self::slow(Duration::ZERO, answer)
    }

    fn slow(
        delay: Duration,
        answer: impl Fn(&str, usize) -> Result<LensResponse, LensCallError> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            log: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            delay,
            answer: Box::new(answer),
        })
    }

    fn standard() -> Arc<Self> {
        Self::new(|sql, _| Ok(empty_activity(sql)))
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    fn calls(&self) -> usize {
        self.log.lock().unwrap().len()
    }
}

#[async_trait]
impl LensApi for Fake {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        let index = {
            let mut log = self.log.lock().unwrap();
            log.push(sql.to_owned());
            log.len() - 1
        };
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        (self.answer)(sql, index)
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unimplemented!()
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        unimplemented!()
    }
}

fn response(sql: &str, rows: Vec<Value>) -> LensResponse {
    let rows: Vec<Map<String, Value>> = rows.into_iter().map(|r| r.as_object().unwrap().clone()).collect();
    LensResponse { row_count: rows.len() as u64, rows, duration_ms: 0, truncated: false, sql: sql.to_owned(), round_trip_ms: 0 }
}

fn snapshot(sql: &str) -> Option<LensResponse> {
    if sql == MAX_SLOTS_SQL {
        Some(response(sql, vec![json!({ "max_slot": MAX_SLOTS })]))
    } else if sql == MAX_TX_SLOT_SQL {
        Some(response(sql, vec![json!({ "max_slot": MAX_TX })]))
    } else {
        None
    }
}

/// Every column any single-row statement reads, all zero or null.
fn zero_row() -> Value {
    json!({
        "transfers": 0, "first_slot": null, "last_slot": null, "sources": 0, "destinations": 0,
        "volume_raw": null, "min_decimals": null, "max_decimals": null,
        "failed_excluded": 0, "unattributed": 0,
        "sent": 0, "received": 0, "volume_out_raw": null, "volume_in_raw": null,
        "mints": 0, "mint": null, "owners": 0, "owner": null,
        "invocations": 0, "inner_invocations": 0, "transactions": 0, "decoded": 0,
        "transactions_all": 0, "failed_transactions": 0,
    })
}

/// No activity at all: aggregates are one zero row, lists are empty.
fn empty_activity(sql: &str) -> LensResponse {
    snapshot(sql).unwrap_or_else(|| {
        if sql.contains("GROUP BY") { response(sql, vec![]) } else { response(sql, vec![zero_row()]) }
    })
}

fn req(kind: Kind) -> ProfileRequest {
    ProfileRequest::new(kind, MINT)
}

fn invalid_field(result: Result<Profile, ProfileError>) -> String {
    match result {
        Err(ProfileError::Invalid { field, .. }) => field,
        other => panic!("expected Invalid, got {other:?}"),
    }
}

/// Every `<column> <op> <integer>` bound in `sql`.
fn bounds(sql: &str, op: &str) -> Vec<u64> {
    let needle = format!("slot {op} ");
    sql.match_indices(&needle)
        .map(|(at, _)| {
            let rest = &sql[at + needle.len()..];
            let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
            rest[..end].parse().unwrap()
        })
        .collect()
}

/// The single-quoted literals of `sql`, `''` unescaped.
fn literals(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\'' {
            continue;
        }
        let mut lit = String::new();
        loop {
            match chars.next() {
                Some('\'') if chars.peek() == Some(&'\'') => {
                    chars.next();
                    lit.push('\'');
                }
                Some('\'') => break,
                Some(c) => lit.push(c),
                None => panic!("unterminated literal in {sql}"),
            }
        }
        out.push(lit);
    }
    out
}

// ---------------------------------------------------------------------------
// Input safety
// ---------------------------------------------------------------------------

#[tokio::test]
async fn invalid_requests_never_reach_lens() {
    let lens = Fake::standard();
    let profiler = Profiler::new(lens.clone());
    let with = |id: &str, window: Option<u64>, as_of: Option<u64>| ProfileRequest {
        kind: Kind::Mint,
        id: id.to_owned(),
        window_slots: window,
        as_of_slot: as_of,
    };
    let long = "1".repeat(10_000);
    let cases: Vec<(ProfileRequest, &str)> = vec![
        (with("", None, None), "id"),
        (with(&long, None, None), "id"),
        (with(&bs58::encode([7u8; 31]).into_string(), None, None), "id"), // 31 bytes
        (with(&bs58::encode([7u8; 33]).into_string(), None, None), "id"), // 33 bytes
        (with(&MINT[..31], None, None), "id"), // too short
        (with("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v ", None, None), "id"),
        (with(" EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", None, None), "id"),
        (with("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1O", None, None), "id"), // 'O'
        (with("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDtl", None, None), "id"), // 'l'
        (with("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTD'", None, None), "id"),
        (with("x' OR '1'='1", None, None), "id"),
        (with("EPjFWdd5AufqSSqeM2qN1xzy'; DROP TABLE solana.slots; --", None, None), "id"),
        (with("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1\u{0}", None, None), "id"),
        (with("ЕPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", None, None), "id"), // Cyrillic Е
        (with("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", None, None), "id"), // 33 bytes
        (with(MINT, Some(0), None), "window_slots"),
        (with(MINT, Some(MAX_WINDOW_SLOTS + 1), None), "window_slots"),
        (with(MINT, Some(u64::MAX), None), "window_slots"),
        (with(MINT, None, Some(i64::MAX as u64 + 1)), "as_of_slot"),
        (with(MINT, None, Some(u64::MAX)), "as_of_slot"),
    ];
    for (request, field) in cases {
        let result = profiler.profile(request.clone()).await;
        assert_eq!(invalid_field(result), field, "{request:?}");
    }
    assert_eq!(lens.calls(), 0, "an invalid request reached LENS: {:?}", lens.log());
}

#[tokio::test]
async fn boundary_requests_are_accepted() {
    let lens = Fake::standard();
    let profiler = Profiler::new(lens.clone());
    for (id, window) in [
        ("11111111111111111111111111111111", Some(MIN_WINDOW_SLOTS)), // 32 chars, 32 zero bytes
        (MINT, Some(MAX_WINDOW_SLOTS)),
        ("So11111111111111111111111111111111111111112", None),
    ] {
        let p = profiler
            .profile(ProfileRequest { kind: Kind::Mint, id: id.into(), window_slots: window, as_of_slot: None })
            .await
            .unwrap_or_else(|e| panic!("{id}: {e}"));
        let asked = window.unwrap_or(DEFAULT_WINDOW_SLOTS);
        assert_eq!(p.window.slots, asked.min(MAX_TX + 1), "{id}");
    }
}

#[tokio::test]
async fn the_id_reaches_sql_only_as_a_quoted_literal() {
    for kind in Kind::ALL {
        let lens = Fake::standard();
        Profiler::new(lens.clone()).profile(req(kind)).await.unwrap();
        for sql in lens.log().iter().skip(SNAPSHOT_QUERIES as usize) {
            let lits = literals(sql);
            assert!(lits.iter().any(|l| l == MINT), "{kind}: id missing from {sql}");
            for lit in &lits {
                assert!(
                    [MINT, "success", ""].contains(&lit.as_str()),
                    "{kind}: unexpected literal {lit:?} in {sql}"
                );
            }
            // Outside literals the id never appears, and there is one statement.
            let outside: String = sql.split('\'').step_by(2).collect();
            assert!(!outside.contains(MINT), "{sql}");
            assert!(!sql.contains(';'), "{sql}");
            assert!(!sql.contains("--"), "{sql}");
        }
    }
}

#[test]
fn the_second_defence_escapes_quotes() {
    // Validation stops these first; the builder must still be safe on its own.
    let w = Window::ending_at(1_000, 100);
    let evil = "x' OR '1'='1";
    let sql = sql::mint_summary(&forge::quote_sql_string(evil), &w);
    let lits = literals(&sql);
    assert!(lits.contains(&evil.to_owned()), "{sql}");
    assert!(lits.iter().all(|l| l == evil || l == "success"), "{lits:?}");
}

// ---------------------------------------------------------------------------
// Snapshot and window
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_query_is_bounded_by_one_snapshot() {
    for kind in Kind::ALL {
        let lens = Fake::standard();
        let p = Profiler::new(lens.clone())
            .profile(ProfileRequest { window_slots: Some(1_000), ..req(kind) })
            .await
            .unwrap();
        // W is the newest slot both tables cover.
        assert_eq!(p.as_of_slot, MAX_TX);
        assert_eq!(p.window, Window { from_slot: MAX_TX - 999, to_slot: MAX_TX, slots: 1_000 });

        let log = lens.log();
        assert_eq!(&log[..2], [MAX_SLOTS_SQL, MAX_TX_SLOT_SQL]);
        for sql in &log[2..] {
            let (lo, hi) = (bounds(sql, ">="), bounds(sql, "<="));
            // Every table in the statement is bounded on both sides.
            let tables = sql.matches("FROM solana.").count() + sql.matches("JOIN solana.").count();
            assert_eq!(lo.len(), tables, "{sql}");
            assert_eq!(hi.len(), tables, "{sql}");
            assert!(lo.iter().all(|&s| s == p.window.from_slot), "{kind}: {sql}");
            assert!(hi.iter().all(|&s| s == p.window.to_slot), "{kind}: {sql}");
        }
    }
}

#[tokio::test]
async fn the_snapshot_does_not_move_when_lens_refreshes_mid_profile() {
    // Every MAX(slot) read after the first two pretends LENS picked up newer
    // files; a profile must never ask again.
    let refreshes = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&refreshes);
    let lens = Fake::new(move |sql, index| {
        if index >= 2 && (sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL) {
            r.fetch_add(1, Ordering::SeqCst);
        }
        Ok(empty_activity(sql))
    });
    let p = Profiler::new(lens.clone()).profile(req(Kind::Program)).await.unwrap();
    assert_eq!(refreshes.load(Ordering::SeqCst), 0);
    assert_eq!(p.as_of_slot, MAX_TX);
    let snapshot_reads = lens.log().iter().filter(|s| s.contains("MAX(slot) AS max_slot")).count();
    assert_eq!(snapshot_reads, 2);
}

#[tokio::test]
async fn as_of_slot_pins_and_may_not_exceed_the_snapshot() {
    let lens = Fake::standard();
    let profiler = Profiler::new(lens.clone());

    let pinned = profiler.profile(ProfileRequest { as_of_slot: Some(1_234), ..req(Kind::Mint) }).await.unwrap();
    assert_eq!(pinned.as_of_slot, 1_234);
    assert!(lens.log()[2..].iter().all(|sql| bounds(sql, "<=").iter().all(|&s| s == 1_234)));

    let at_w = profiler.profile(ProfileRequest { as_of_slot: Some(MAX_TX), ..req(Kind::Mint) }).await.unwrap();
    assert_eq!(at_w.as_of_slot, MAX_TX);

    let before = lens.calls();
    let above = profiler.profile(ProfileRequest { as_of_slot: Some(MAX_TX + 1), ..req(Kind::Mint) }).await;
    assert_eq!(invalid_field(above), "as_of_slot");
    // Only the snapshot was read: nothing past W was ever queried.
    assert_eq!(lens.calls() - before, SNAPSHOT_QUERIES as usize);
}

#[tokio::test]
async fn a_window_reaching_before_genesis_is_clipped_and_says_so() {
    let lens = Fake::standard();
    let p = Profiler::new(lens.clone())
        .profile(ProfileRequest { as_of_slot: Some(99), window_slots: Some(1_000), ..req(Kind::Mint) })
        .await
        .unwrap();
    assert_eq!(p.window, Window { from_slot: 0, to_slot: 99, slots: 100 });
    assert!(p.warnings.iter().any(|w| w == WARN_WINDOW_CLIPPED), "{:?}", p.warnings);
    assert_eq!(p.series.len() as u64, p.window.buckets());
}

// ---------------------------------------------------------------------------
// The query plan
// ---------------------------------------------------------------------------

#[tokio::test]
async fn each_kind_runs_a_fixed_number_of_queries_one_at_a_time() {
    assert_eq!(Kind::Mint.queries(), 7);
    assert_eq!(Kind::TokenAccount.queries(), 6);
    assert_eq!(Kind::Program.queries(), 5);
    for kind in Kind::ALL {
        let lens = Fake::slow(Duration::from_millis(5), |sql, _| Ok(empty_activity(sql)));
        let p = Profiler::new(lens.clone()).profile(req(kind)).await.unwrap();
        assert_eq!(p.queries_run, kind.queries(), "{kind}");
        assert_eq!(lens.calls() as u32, kind.queries(), "{kind}");
        assert_eq!(lens.max_in_flight.load(Ordering::SeqCst), 1, "{kind}: queries overlapped");
        assert!(!p.found);
        assert!(p.warnings.iter().any(|w| w == WARN_NOT_FOUND));
    }
}

#[tokio::test]
async fn the_plan_does_not_depend_on_the_id_or_the_data() {
    // Same statements in the same order, up to the id and the bounds.
    let shape = |log: Vec<String>, id: &str| -> Vec<String> {
        log.into_iter()
            .map(|s| {
                let mut out = String::new();
                for c in s.replace(id, "ID").chars() {
                    if !c.is_ascii_digit() {
                        out.push(c);
                    } else if !out.ends_with('#') {
                        out.push('#');
                    }
                }
                out
            })
            .collect()
    };
    let other = "So11111111111111111111111111111111111111112";
    for kind in Kind::ALL {
        let a = Fake::standard();
        Profiler::new(a.clone()).profile(req(kind)).await.unwrap();
        let b = Fake::standard();
        Profiler::new(b.clone())
            .profile(ProfileRequest { kind, id: other.into(), window_slots: Some(7), as_of_slot: Some(500) })
            .await
            .unwrap();
        assert_eq!(shape(a.log(), MINT), shape(b.log(), other), "{kind}");
    }
}

#[tokio::test]
async fn every_top_list_breaks_ties_by_key() {
    for kind in Kind::ALL {
        let lens = Fake::standard();
        Profiler::new(lens.clone()).profile(req(kind)).await.unwrap();
        let tops: Vec<String> = lens.log().into_iter().filter(|s| s.contains(" LIMIT ")).collect();
        assert!(!tops.is_empty());
        for sql in tops {
            assert!(sql.contains(&format!("ORDER BY n DESC, subject ASC LIMIT {TOP_N}")), "{sql}");
        }
    }
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn any_failure_fails_the_whole_profile() {
    let errors = [
        (LensCallError::Timeout { message: "504".into() }, "timeout"),
        (LensCallError::Unavailable { message: "conn refused".into() }, "unavailable"),
        (LensCallError::BadQuery { message: "table 'solana.instructions' not found".into() }, "not_ready"),
        (LensCallError::BadQuery { message: "syntax error".into() }, "permanent"),
        (LensCallError::Protocol { message: "garbage".into() }, "permanent"),
    ];
    for kind in Kind::ALL {
        for at in 0..kind.queries() as usize {
            for (err, expected) in &errors {
                let err = err.clone();
                let lens = Fake::new(move |sql, i| if i == at { Err(err.clone()) } else { Ok(empty_activity(sql)) });
                let result = Profiler::new(lens.clone()).profile(req(kind)).await;
                let got = result.expect_err("a failed query must fail the profile");
                assert_eq!(got.kind(), *expected, "{kind} at {at}: {got}");
                assert_eq!(lens.calls(), at + 1, "{kind}: kept querying after a failure");
            }
        }
    }
}

#[tokio::test]
async fn a_lens_timeout_advises_a_narrower_window() {
    let lens = Fake::new(|sql, i| {
        if i == 2 { Err(LensCallError::Timeout { message: "query exceeded 30 s".into() }) } else { Ok(empty_activity(sql)) }
    });
    let err = Profiler::new(lens).profile(ProfileRequest { window_slots: Some(2_000_000), ..req(Kind::Program) }).await;
    let err = err.unwrap_err();
    assert_eq!(err.http_status(), 504);
    assert!(err.to_string().contains("window_slots = 2000000"), "{err}");
}

#[tokio::test]
async fn a_truncated_result_is_refused() {
    for kind in Kind::ALL {
        for at in 0..kind.queries() as usize {
            let lens = Fake::new(move |sql, i| {
                let mut r = empty_activity(sql);
                r.truncated = i == at;
                Ok(r)
            });
            let err = Profiler::new(lens).profile(req(kind)).await.unwrap_err();
            assert_eq!(err, ProfileError::Truncated, "{kind} at {at}");
        }
    }
}

#[tokio::test]
async fn empty_tables_are_not_ready() {
    for null_sql in [MAX_SLOTS_SQL, MAX_TX_SLOT_SQL] {
        let lens = Fake::new(move |sql, _| {
            if sql == null_sql { Ok(response(sql, vec![json!({ "max_slot": null })])) } else { Ok(empty_activity(sql)) }
        });
        let err = Profiler::new(lens).profile(req(Kind::Mint)).await.unwrap_err();
        assert_eq!(err.kind(), "not_ready", "{null_sql}: {err}");
    }
}

#[tokio::test]
async fn malformed_results_are_permanent() {
    let mangle: Vec<(&str, Box<dyn Fn(&str) -> Option<LensResponse> + Send + Sync>)> = vec![
        ("two summary rows", Box::new(|sql: &str| (sql.contains("AS transfers")).then(|| response(sql, vec![zero_row(), zero_row()])))),
        ("missing column", Box::new(|sql: &str| (sql.contains("AS transfers")).then(|| response(sql, vec![json!({})])))),
        ("float count", Box::new(|sql: &str| (sql.contains("AS transfers")).then(|| {
            let mut row = zero_row();
            row["transfers"] = json!(1.5);
            response(sql, vec![row])
        }))),
        ("non-numeric volume", Box::new(|sql: &str| (sql.contains("AS transfers")).then(|| {
            let mut row = zero_row();
            row["volume_raw"] = json!("1e30");
            response(sql, vec![row])
        }))),
        ("bucket out of range", Box::new(|sql: &str| (sql.contains("AS bucket")).then(|| response(sql, vec![json!({"bucket": 99, "n": 0})])))),
    ];
    for (what, f) in mangle {
        let lens = Fake::new(move |sql, _| Ok(f(sql).unwrap_or_else(|| empty_activity(sql))));
        let err = Profiler::new(lens).profile(req(Kind::Mint)).await.unwrap_err();
        assert_eq!(err.kind(), "permanent", "{what}: {err}");
    }
}

#[tokio::test]
async fn sections_that_disagree_are_not_served() {
    // The summary says 5 transfers, the series says 3: rows under the
    // snapshot changed between the two reads.
    let lens = Fake::new(|sql, _| {
        Ok(if sql.contains("AS transfers") {
            let mut row = zero_row();
            row["transfers"] = json!(5);
            row["volume_raw"] = json!("5");
            response(sql, vec![row])
        } else if sql.contains("AS bucket") {
            response(sql, vec![json!({"bucket": 0, "n": 3})])
        } else {
            empty_activity(sql)
        })
    });
    let err = Profiler::new(lens).profile(req(Kind::Mint)).await.unwrap_err();
    assert_eq!(err.kind(), "unavailable", "{err}");
    assert!(err.is_retryable());
}

// ---------------------------------------------------------------------------
// Concurrency, deadline, cancellation
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_max_concurrent_profiles_run_at_once() {
    let lens = Fake::slow(Duration::from_millis(20), |sql, _| Ok(empty_activity(sql)));
    let profiler = Profiler::new(lens.clone());
    let tasks: Vec<_> = (0..MAX_CONCURRENT_PROFILES * 3)
        .map(|_| {
            let p = profiler.clone();
            tokio::spawn(async move { p.profile(req(Kind::Program)).await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    // One query in flight per running profile.
    assert_eq!(lens.max_in_flight.load(Ordering::SeqCst), MAX_CONCURRENT_PROFILES);
}

#[tokio::test(start_paused = true)]
async fn the_deadline_covers_the_whole_call() {
    let lens = Fake::slow(Duration::from_secs(30), |sql, _| Ok(empty_activity(sql)));
    let profiler = Profiler::with_limits(lens.clone(), 1, Duration::from_secs(50));
    let err = profiler.profile(req(Kind::Mint)).await.unwrap_err();
    assert_eq!(err.kind(), "timeout");
    assert!(err.to_string().contains("narrow window_slots"), "{err}");
    // 30 s per query: the second query was cut off by the 50 s deadline.
    assert_eq!(lens.calls(), 2);
}

#[tokio::test(start_paused = true)]
async fn the_wait_for_a_permit_counts_against_the_deadline() {
    let lens = Fake::slow(Duration::from_secs(10), |sql, _| Ok(empty_activity(sql)));
    let profiler = Profiler::with_limits(lens.clone(), 1, Duration::from_secs(100));
    let first = tokio::spawn({
        let p = profiler.clone();
        async move { p.profile(req(Kind::Mint)).await }
    });
    tokio::task::yield_now().await;
    // The first holds the permit for 70 s; the second gets 30 s of work.
    let second = profiler.profile(req(Kind::Mint)).await;
    assert_eq!(second.unwrap_err().kind(), "timeout");
    assert!(first.await.unwrap().is_ok());
}

#[tokio::test(start_paused = true)]
async fn a_dropped_profile_releases_its_permit() {
    let lens = Fake::slow(Duration::from_secs(10), |sql, _| Ok(empty_activity(sql)));
    let profiler = Profiler::with_limits(lens.clone(), 1, Duration::from_secs(1_000));
    // Cancel mid-flight, between two LENS calls.
    let cut = tokio::time::timeout(Duration::from_secs(25), profiler.profile(req(Kind::Mint))).await;
    assert!(cut.is_err());
    let calls = lens.calls();
    assert_eq!(calls, 3);
    // With one permit, this would hang if the dropped call had kept it.
    let p = tokio::time::timeout(Duration::from_secs(500), profiler.profile(req(Kind::Mint))).await;
    assert!(p.expect("permit leaked").is_ok());
    // The cancelled profile issued nothing after it was dropped.
    assert_eq!(lens.calls(), calls + Kind::Mint.queries() as usize);
}

// ---------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_report_has_the_documented_shape_per_kind() {
    let expected: [(Kind, &[&str], &[&str]); 3] = [
        (Kind::Mint, &["top_sources", "top_destinations"], &["transfers_in_failed_tx_excluded", "unattributed_transfers"]),
        (Kind::TokenAccount, &["top_counterparties"], &["transfers_in_failed_tx_excluded"]),
        (Kind::Program, &["top_instruction_types"], &["instructions_in_failed_tx_excluded"]),
    ];
    for (kind, tops, coverage) in expected {
        let p = Profiler::new(Fake::standard()).profile(req(kind)).await.unwrap();
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["kind"], kind.as_str());
        assert_eq!(v["id"], MINT);
        for key in ["found", "as_of_slot", "window", "summary", "series", "coverage", "warnings", "queries_run", "duration_ms"] {
            assert!(v.get(key).is_some(), "{kind}: no {key}");
        }
        for top in tops {
            assert_eq!(v[top], json!([]), "{kind}");
        }
        for c in coverage {
            assert_eq!(v["coverage"][c], 0, "{kind}");
        }
        // Volumes are strings, never numbers.
        for (k, val) in v["summary"].as_object().unwrap() {
            if k.contains("volume") && !val.is_null() {
                assert!(val.is_string(), "{kind}: {k} = {val}");
            }
        }
    }
}

#[tokio::test]
async fn a_huge_volume_survives_as_exact_text() {
    let lens = Fake::new(|sql, _| {
        Ok(if sql.contains("AS transfers") {
            let mut row = zero_row();
            row["transfers"] = json!(10);
            row["volume_raw"] = json!("184467440737095516150");
            row["min_decimals"] = json!(6);
            row["max_decimals"] = json!(6);
            response(sql, vec![row])
        } else if sql.contains("AS bucket") {
            response(sql, vec![json!({"bucket": 3, "n": 10})])
        } else {
            empty_activity(sql)
        })
    });
    let p = Profiler::new(lens).profile(req(Kind::Mint)).await.unwrap();
    let Summary::Mint(s) = &p.summary else { panic!() };
    assert_eq!(s.volume_raw, "184467440737095516150");
    assert_eq!(s.volume_ui.as_deref(), Some("184467440737095.516150"));
    let text = serde_json::to_string(&p).unwrap();
    assert!(text.contains(r#""volume_raw":"184467440737095516150""#), "{text}");
}
