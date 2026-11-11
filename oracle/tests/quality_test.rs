//! `quality/` against a scripted LENS: the audit's behaviour.
//!
//! * Each check maps its own count to its own status, and only its own.
//! * A failure inside one check is that check's status; every other check
//!   still runs and passes, and `overall` ranks it correctly.
//! * Queries run one after another, in the fixed order, never concurrently,
//!   even when several audits are asked for at once.
//! * `report` caches by the injected clock, is single-flight, does not
//!   cache failures, and survives a cancelled leader; `run` always runs.
//! * Snapshot failures, the deadline and shutdown are `QualityError`s.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensHealthStatus, LensResponse, Visibility};
use oracle::quality::*;
use serde_json::{Map, Value, json};
use tokio::sync::Semaphore;

// ---------------------------------------------------------------------------
// Scripted LENS
// ---------------------------------------------------------------------------

type Reply = Result<LensResponse, LensCallError>;
type Responder = Arc<dyn Fn(&str) -> Option<Reply> + Send + Sync>;

const W: u64 = 1_000;
const NEWEST: i64 = 1_800_000_000;

fn window() -> Window {
    Window::ending_at(W, DEFAULT_WINDOW_SLOTS)
}

fn response(rows: Vec<Value>) -> LensResponse {
    let rows: Vec<Map<String, Value>> = rows.into_iter().map(|v| v.as_object().unwrap().clone()).collect();
    LensResponse { row_count: rows.len() as u64, rows, duration_ms: 1, truncated: false, sql: String::new(), round_trip_ms: 1 }
}

fn ok(rows: Vec<Value>) -> Option<Reply> {
    Some(Ok(response(rows)))
}

fn counts(examined: u64, violations: u64) -> Option<Reply> {
    ok(vec![json!({"examined": examined, "violations": violations})])
}

fn unavailable() -> LensCallError {
    LensCallError::Unavailable { message: "connection reset".into() }
}

/// A healthy pipeline: a snapshot at [`W`], fresh data, 10 rows examined
/// and no violations everywhere.
fn healthy(sql: &str) -> Reply {
    if sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL {
        return Ok(response(vec![json!({"max_slot": W})]));
    }
    if sql == sql::DATA_FRESHNESS {
        return Ok(response(vec![json!({"newest": NEWEST})]));
    }
    Ok(response(vec![json!({"examined": 10, "violations": 0})]))
}

struct Script {
    /// Tried first; `None` falls through to [`healthy`].
    responder: Mutex<Responder>,
    log: Mutex<Vec<String>>,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    /// Every query takes and returns a permit: 0 permits holds them all.
    hold: Semaphore,
}

impl Script {
    fn new() -> Arc<Self> {
        Self::with(|_| None)
    }

    fn with(f: impl Fn(&str) -> Option<Reply> + Send + Sync + 'static) -> Arc<Self> {
        Arc::new(Self {
            responder: Mutex::new(Arc::new(f)),
            log: Mutex::new(Vec::new()),
            in_flight: AtomicUsize::new(0),
            max_in_flight: AtomicUsize::new(0),
            hold: Semaphore::new(Semaphore::MAX_PERMITS),
        })
    }

    /// Like [`Self::with`], every query held until [`Self::release`].
    fn held(f: impl Fn(&str) -> Option<Reply> + Send + Sync + 'static) -> Arc<Self> {
        let s = Self::with(f);
        s.hold.forget_permits(Semaphore::MAX_PERMITS);
        s
    }

    fn release(&self) {
        self.hold.add_permits(Semaphore::MAX_PERMITS);
    }

    fn set(&self, f: impl Fn(&str) -> Option<Reply> + Send + Sync + 'static) {
        *self.responder.lock().unwrap() = Arc::new(f);
    }

    fn queries(&self) -> usize {
        self.log.lock().unwrap().len()
    }

    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }

    /// Wait until `n` queries have been sent.
    async fn sent(&self, n: usize) {
        while self.queries() < n {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

#[async_trait]
impl LensApi for Script {
    async fn query(&self, sql: &str) -> Reply {
        self.log.lock().unwrap().push(sql.to_owned());
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let _permit = self.hold.acquire().await.unwrap();
        // A yield, so overlapping callers would really overlap.
        tokio::task::yield_now().await;
        let responder = Arc::clone(&self.responder.lock().unwrap());
        let reply = responder(sql).unwrap_or_else(|| healthy(sql));
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        reply
    }

    async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
        unreachable!("the auditor never waits for visibility")
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 0 })
    }
}

struct TestClock(AtomicU64);

impl TestClock {
    fn at(secs: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(secs)))
    }
    fn set(&self, secs: u64) {
        self.0.store(secs, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_unix_secs(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

const NOW: u64 = NEWEST as u64 + 5;

fn auditor(script: &Arc<Script>) -> Auditor {
    Auditor::new(Arc::clone(script) as Arc<dyn LensApi>, TestClock::at(NOW))
}

fn count_of(id: &str) -> String {
    count_sql(id, &window()).unwrap()
}

fn sample_of(id: &str) -> String {
    sample_sql(id, &window()).unwrap()
}

/// The checks that are not in their healthy state (pass, or info).
fn unhealthy(r: &QualityReport) -> BTreeSet<&'static str> {
    r.checks.iter().filter(|c| !matches!(c.status, Status::Pass | Status::Info)).map(|c| c.id).collect()
}

const ALL_QUERIES: usize = SNAPSHOT_QUERIES as usize + CHECKS.len();

// ---------------------------------------------------------------------------
// Each check, its own status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_healthy_pipeline_passes_and_reports_in_the_documented_shape() {
    let script = Script::new();
    let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
    assert_eq!(unhealthy(&r), BTreeSet::new());
    assert_eq!(r.overall, Overall::Pass);
    assert_eq!(r.summary, StatusSummary { pass: 16, info: 4, ..Default::default() });
    assert_eq!(r.summary.total() as usize, CHECKS.len());
    assert_eq!(r.queries_run as usize, ALL_QUERIES);
    assert_eq!(r.as_of_slot, W);
    assert_eq!(r.window, Window { from_slot: 0, to_slot: W, slots: W + 1 }, "clipped at slot 0");
    assert_eq!(r.generated_at, NOW);

    let v = serde_json::to_value(&r).unwrap();
    let keys: BTreeSet<_> = v.as_object().unwrap().keys().cloned().collect();
    let want = ["as_of_slot", "window", "generated_at", "overall", "summary", "checks", "queries_run", "duration_ms"];
    assert_eq!(keys, want.iter().map(|s| s.to_string()).collect());
    assert_eq!(v["overall"], "pass");
    assert_eq!(v["summary"], json!({"pass": 16, "warn": 0, "fail": 0, "info": 4, "error": 0, "not_ready": 0}));
    let c = &v["checks"][0];
    for k in ["id", "group", "status", "examined", "violations", "samples", "message", "query_count", "duration_ms"] {
        assert!(c.get(k).is_some(), "{k} missing from {c}");
    }
    assert!(c.get("error_kind").is_none(), "absent when there is no error");
}

#[tokio::test]
async fn each_check_reports_exactly_its_own_violations() {
    for target in CHECKS {
        let id = target.id;
        let script = if id == "data_freshness" {
            Script::with(|sql| (sql == sql::DATA_FRESHNESS).then(|| Ok(response(vec![json!({"newest": NEWEST - 10_000})]))))
        } else {
            let (count, sample) = (count_of(id), sample_sql(id, &window()));
            Script::with(move |sql| {
                if sql == count {
                    return counts(40, 3);
                }
                if Some(sql) == sample.as_deref() {
                    return ok(vec![json!({"c0": 7, "c1": "sig", "c2": null}), json!({"c0": 8, "c1": "sig2", "c2": 1})]);
                }
                None
            })
        };
        let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
        let c = r.check(id).unwrap();
        let (status, overall) = match target.severity {
            Severity::Fail => (Status::Fail, Overall::Fail),
            Severity::Warn => (Status::Warn, Overall::Warn),
            Severity::Info => (Status::Info, Overall::Pass),
        };
        assert_eq!(c.status, status, "{id}");
        assert_eq!(r.overall, overall, "{id}");
        let expect_unhealthy: BTreeSet<_> = if status == Status::Info { BTreeSet::new() } else { BTreeSet::from([id]) };
        assert_eq!(unhealthy(&r), expect_unhealthy, "{id}: no other check may fire");
        assert_eq!(c.message, target.message(status), "{id}");
        if id == "data_freshness" {
            assert_eq!((c.violations, c.lag_secs), (1, Some(10_005)));
        } else if target.severity == Severity::Info {
            assert_eq!((c.examined, c.violations, c.query_count), (40, 3, 1), "{id}: info never samples");
            assert!(c.samples.is_empty());
        } else {
            assert_eq!((c.examined, c.violations, c.query_count), (40, 3, 2), "{id}");
            assert_eq!(c.samples, ["7:sig:-", "8:sig2:1"], "{id}");
        }
        // Every other check: its healthy numbers, one query each.
        for other in r.checks.iter().filter(|o| o.id != id && o.id != "data_freshness") {
            assert_eq!((other.examined, other.violations, other.query_count), (10, 0, 1), "{id} -> {}", other.id);
        }
    }
}

#[tokio::test]
async fn messages_never_contain_data_read_from_lens() {
    let script = Script::with(|sql| {
        if sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL || sql == sql::DATA_FRESHNESS {
            return None;
        }
        if sql.contains("LIMIT") {
            return ok(vec![json!({"c0": "EVIL-SAMPLE"})]);
        }
        counts(5, 1)
    });
    let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
    for c in &r.checks {
        assert!(!c.message.contains("EVIL"), "{}", c.id);
        assert_eq!(c.message, def(c.id).unwrap().message(c.status));
    }
    assert_eq!(r.check("slot_duplicates").unwrap().samples, ["EVIL-SAMPLE"]);
}

// ---------------------------------------------------------------------------
// A failure stays inside its check
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failing_check_is_isolated_and_makes_the_audit_incomplete() {
    let truncated = || {
        let mut r = response(vec![json!({"examined": 1, "violations": 0})]);
        r.truncated = true;
        Some(Ok(r))
    };
    let cases: Vec<(&str, Box<dyn Fn() -> Option<Reply> + Send + Sync>, Status, Option<&str>)> = vec![
        ("unavailable", Box::new(|| Some(Err(unavailable()))), Status::Error, Some("unavailable")),
        ("timeout", Box::new(|| Some(Err(LensCallError::Timeout { message: "504".into() }))), Status::Error, Some("timeout")),
        ("protocol", Box::new(|| Some(Err(LensCallError::Protocol { message: "garbage".into() }))), Status::Error, Some("protocol")),
        ("bad query", Box::new(|| Some(Err(LensCallError::BadQuery { message: "syntax error".into() }))), Status::Error, Some("bad_query")),
        ("truncated", Box::new(truncated), Status::Error, Some("truncated")),
        ("more violations than examined", Box::new(|| counts(1, 2)), Status::Error, Some("malformed")),
        ("missing column", Box::new(|| ok(vec![json!({"examined": 1})])), Status::Error, Some("malformed")),
        ("two rows", Box::new(|| ok(vec![json!({"examined": 1, "violations": 0}), json!({"examined": 1, "violations": 0})])), Status::Error, Some("malformed")),
        ("negative", Box::new(|| ok(vec![json!({"examined": -1, "violations": 0})])), Status::Error, Some("malformed")),
        (
            "table missing",
            Box::new(|| Some(Err(LensCallError::BadQuery { message: "table 'datafusion.solana.token_transfers' not found".into() }))),
            Status::NotReady,
            None,
        ),
    ];
    for (name, reply, status, kind) in cases {
        let target = count_of("duplicate_transactions");
        let reply = Arc::new(reply);
        let script = Script::with(move |sql| if sql == target { reply() } else { None });
        let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
        let c = r.check("duplicate_transactions").unwrap();
        assert_eq!((c.status, c.error_kind), (status, kind), "{name}");
        assert_eq!((c.examined, c.violations), (0, 0), "{name}: no number from a failed check");
        assert_eq!(c.message, if status == Status::Error { MSG_ERROR } else { MSG_NOT_READY }, "{name}");
        assert_eq!(unhealthy(&r), BTreeSet::from(["duplicate_transactions"]), "{name}");
        assert_eq!(r.overall, Overall::Incomplete, "{name}");
        assert_eq!(script.queries(), ALL_QUERIES, "{name}: every later check still ran");
        assert_eq!(c.query_count, 1, "{name}");
    }
}

#[tokio::test]
async fn overall_ranks_fail_over_incomplete_over_warn() {
    let (fail, warn, broken) = (count_of("slot_duplicates"), count_of("spl_decode_rate"), count_of("orphan_transfers"));
    let script = Script::with(move |sql| {
        if sql == broken {
            Some(Err(unavailable()))
        } else if sql == fail || sql == warn {
            counts(5, 1)
        } else {
            None
        }
    });
    let a = auditor(&script);
    let r = a.run(AuditRequest::default()).await.unwrap();
    assert_eq!(r.overall, Overall::Fail);
    assert_eq!((r.summary.fail, r.summary.warn, r.summary.error), (1, 1, 1));

    // Without the failure, the unknown outranks the warning.
    let (warn, broken) = (count_of("spl_decode_rate"), count_of("orphan_transfers"));
    script.set(move |sql| if sql == broken { Some(Err(unavailable())) } else if sql == warn { counts(5, 1) } else { None });
    assert_eq!(a.run(AuditRequest::default()).await.unwrap().overall, Overall::Incomplete);

    let warn = count_of("spl_decode_rate");
    script.set(move |sql| if sql == warn { counts(5, 1) } else { None });
    assert_eq!(a.run(AuditRequest::default()).await.unwrap().overall, Overall::Warn);
}

#[tokio::test]
async fn an_info_check_that_cannot_run_hides_nothing() {
    let target = count_of("owner_resolution");
    let script = Script::with(move |sql| if sql == target { Some(Err(unavailable())) } else { None });
    let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
    assert_eq!(r.check("owner_resolution").unwrap().status, Status::Error);
    assert_eq!(r.summary.error, 1);
    assert_eq!(r.overall, Overall::Pass);
}

#[tokio::test]
async fn finalized_flag_is_not_ready_without_its_column_and_broken_otherwise() {
    let target = count_of("finalized_flag");
    let script = Script::with(move |sql| {
        (sql == target).then(|| Err(LensCallError::BadQuery { message: "Schema error: No field named is_finalized.".into() }))
    });
    let a = auditor(&script);
    assert_eq!(a.run(AuditRequest::default()).await.unwrap().check("finalized_flag").unwrap().status, Status::NotReady);

    // The same error on another check is a bug, not a missing column.
    let target = count_of("slot_duplicates");
    script.set(move |sql| (sql == target).then(|| Err(LensCallError::BadQuery { message: "No field named is_finalized".into() })));
    let c = a.run(AuditRequest::default()).await.unwrap().check("slot_duplicates").cloned().unwrap();
    assert_eq!((c.status, c.error_kind), (Status::Error, Some("bad_query")));
}

#[tokio::test]
async fn a_failed_sample_keeps_the_count() {
    let (count, sample) = (count_of("orphan_instructions"), sample_of("orphan_instructions"));
    let script = Script::with(move |sql| {
        if sql == count {
            counts(9, 2)
        } else if sql == sample {
            Some(Err(unavailable()))
        } else {
            None
        }
    });
    let a = auditor(&script);
    let r = a.run(AuditRequest::default()).await.unwrap();
    let c = r.check("orphan_instructions").unwrap();
    assert_eq!((c.status, c.violations, c.error_kind, c.query_count), (Status::Fail, 2, Some("unavailable"), 2));
    assert!(c.samples.is_empty());
    assert_eq!(c.message, def("orphan_instructions").unwrap().found_message);
    assert_eq!(r.overall, Overall::Fail);

    // A sample longer than the limit is malformed, not served.
    let (count, sample) = (count_of("orphan_instructions"), sample_of("orphan_instructions"));
    script.set(move |sql| {
        if sql == count {
            counts(9, 9)
        } else if sql == sample {
            ok((0..=SAMPLE_LIMIT).map(|i| json!({"c0": i})).collect())
        } else {
            None
        }
    });
    let r = a.run(AuditRequest::default()).await.unwrap();
    let c = r.check("orphan_instructions").unwrap();
    assert_eq!((c.status, c.error_kind), (Status::Fail, Some("malformed")));
    assert!(c.samples.is_empty());
}

#[tokio::test]
async fn freshness_edge_cases() {
    for (newest, status, lag, kind) in [
        (json!(null), Status::NotReady, None, None),
        (json!(NEWEST + 100), Status::Pass, Some(0), None),
        (json!(NOW as i64 - FRESHNESS_WARN_SECS as i64), Status::Pass, Some(FRESHNESS_WARN_SECS), None),
        (json!(NOW as i64 - FRESHNESS_WARN_SECS as i64 - 1), Status::Warn, Some(FRESHNESS_WARN_SECS + 1), None),
        (json!("yesterday"), Status::Error, None, Some("malformed")),
    ] {
        let script = Script::with(move |sql| (sql == sql::DATA_FRESHNESS).then(|| Ok(response(vec![json!({"newest": newest.clone()})]))));
        let r = auditor(&script).run(AuditRequest::default()).await.unwrap();
        let c = r.check("data_freshness").unwrap();
        assert_eq!((c.status, c.lag_secs, c.error_kind), (status, lag, kind), "{c:?}");
    }
}

// ---------------------------------------------------------------------------
// Snapshot and request
// ---------------------------------------------------------------------------

#[tokio::test]
async fn one_snapshot_bounds_every_check() {
    // transactions lag slots: W is the smaller.
    let script = Script::with(|sql| (sql == MAX_SLOTS_SQL).then(|| ok(vec![json!({"max_slot": W + 50})])).flatten());
    let r = auditor(&script).run(AuditRequest { window_slots: Some(100), as_of_slot: None }).await.unwrap();
    assert_eq!(r.as_of_slot, W);
    let w = Window::ending_at(W, 100);
    let log = script.log();
    assert_eq!(log[..2], [MAX_SLOTS_SQL.to_owned(), MAX_TX_SLOT_SQL.to_owned()]);
    let expected: Vec<String> = CHECKS.iter().map(|c| count_sql(c.id, &w).unwrap()).collect();
    assert_eq!(log[2..], expected[..], "the fixed order, every check on one window");
}

#[tokio::test]
async fn snapshot_failures_are_audit_errors() {
    let empty = || ok(vec![json!({"max_slot": null})]);
    let cases: Vec<(Box<dyn Fn() -> Option<Reply> + Send + Sync>, &str)> = vec![
        (Box::new(empty), "not_ready"),
        (Box::new(|| Some(Err(LensCallError::BadQuery { message: "table 'solana.slots' not found".into() }))), "not_ready"),
        (Box::new(|| Some(Err(unavailable()))), "unavailable"),
        (Box::new(|| Some(Err(LensCallError::Timeout { message: "x".into() }))), "timeout"),
        (Box::new(|| Some(Err(LensCallError::Protocol { message: "x".into() }))), "permanent"),
        (Box::new(|| Some(Err(LensCallError::BadQuery { message: "syntax".into() }))), "permanent"),
        (Box::new(|| ok(vec![json!({"max_slot": "x"})])), "permanent"),
        (
            Box::new(|| {
                let mut r = response(vec![json!({"max_slot": 1})]);
                r.truncated = true;
                Some(Ok(r))
            }),
            "permanent",
        ),
    ];
    for (reply, kind) in cases {
        let script = Script::with(move |sql| if sql == MAX_TX_SLOT_SQL { reply() } else { None });
        let err = auditor(&script).run(AuditRequest::default()).await.unwrap_err();
        assert_eq!(err.kind(), kind, "{err}");
        assert_eq!(script.queries(), 2, "no check runs without a snapshot");
    }
}

#[tokio::test]
async fn bad_requests_send_nothing_and_as_of_above_the_snapshot_is_invalid() {
    let script = Script::new();
    let a = auditor(&script);
    for req in [
        AuditRequest { window_slots: Some(0), as_of_slot: None },
        AuditRequest { window_slots: Some(MAX_WINDOW_SLOTS + 1), as_of_slot: None },
        AuditRequest { window_slots: None, as_of_slot: Some(u64::MAX) },
    ] {
        assert_eq!(a.run(req).await.unwrap_err().kind(), "invalid", "{req:?}");
    }
    assert_eq!(script.queries(), 0);

    let err = a.run(AuditRequest { window_slots: None, as_of_slot: Some(W + 1) }).await.unwrap_err();
    assert!(matches!(err, QualityError::Invalid { ref field, .. } if field == "as_of_slot"), "{err:?}");
    assert_eq!(script.queries(), 2);
    let r = a.run(AuditRequest { window_slots: Some(10), as_of_slot: Some(500) }).await.unwrap();
    assert_eq!(r.window, Window { from_slot: 491, to_slot: 500, slots: 10 });
}

#[tokio::test]
async fn the_same_inputs_give_the_same_report() {
    let script = Script::with(|sql| sql.starts_with("SELECT COUNT(*) AS examined").then(|| counts(5, 1)).flatten());
    let a = auditor(&script);
    let strip = |mut r: QualityReport| {
        r.duration_ms = 0;
        r.checks.iter_mut().for_each(|c| c.duration_ms = 0);
        serde_json::to_string(&r).unwrap()
    };
    let x = strip(a.run(AuditRequest::default()).await.unwrap());
    let y = strip(a.run(AuditRequest::default()).await.unwrap());
    assert_eq!(x, y);
}

// ---------------------------------------------------------------------------
// One at a time
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_never_overlap_even_across_concurrent_audits() {
    let script = Script::new();
    let a = Arc::new(auditor(&script));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let a = Arc::clone(&a);
            tokio::spawn(async move { a.run(AuditRequest::default()).await.unwrap() })
        })
        .collect();
    for h in handles {
        h.await.unwrap();
    }
    assert_eq!(script.queries(), 4 * ALL_QUERIES);
    assert_eq!(script.max_in_flight.load(Ordering::SeqCst), 1);
    // And each audit's queries are contiguous: audits do not interleave.
    for chunk in script.log().chunks(ALL_QUERIES) {
        assert_eq!(chunk[0], MAX_SLOTS_SQL);
    }
}

// ---------------------------------------------------------------------------
// The cached report
// ---------------------------------------------------------------------------

#[tokio::test]
async fn report_is_cached_by_the_injected_clock() {
    let script = Script::new();
    let clock = TestClock::at(NOW);
    let a = Auditor::new(Arc::clone(&script) as Arc<dyn LensApi>, Arc::clone(&clock) as Arc<dyn Clock>);
    let max_age = Duration::from_secs(60);

    let first = a.report(max_age).await.unwrap();
    assert_eq!(script.queries(), ALL_QUERIES);
    assert_eq!(first.generated_at, NOW);

    clock.set(NOW + 60);
    let again = a.report(max_age).await.unwrap();
    assert!(Arc::ptr_eq(&first, &again), "exactly max_age old is still fresh");
    assert_eq!(script.queries(), ALL_QUERIES);
    // A tighter caller does not accept it.
    let tight = a.report(Duration::from_secs(59)).await.unwrap();
    assert!(!Arc::ptr_eq(&first, &tight));
    assert_eq!(script.queries(), 2 * ALL_QUERIES);
    assert_eq!(tight.generated_at, NOW + 60);

    // A clock that went backwards: age 0, not a wrap-around.
    clock.set(NOW);
    assert!(Arc::ptr_eq(&tight, &a.report(Duration::ZERO).await.unwrap()));
    assert_eq!(script.queries(), 2 * ALL_QUERIES);
}

#[tokio::test]
async fn run_always_runs_and_never_touches_the_cache() {
    let script = Script::new();
    let a = auditor(&script);
    let cached = a.report(Duration::from_secs(600)).await.unwrap();
    a.run(AuditRequest::default()).await.unwrap();
    assert_eq!(script.queries(), 2 * ALL_QUERIES, "run ignores a fresh cache");
    // A custom run does not replace the cached default report.
    a.run(AuditRequest { window_slots: Some(10), as_of_slot: None }).await.unwrap();
    assert!(Arc::ptr_eq(&cached, &a.report(Duration::from_secs(600)).await.unwrap()));
}

#[tokio::test]
async fn a_failed_audit_is_not_cached() {
    let script = Script::with(|sql| (sql == MAX_SLOTS_SQL).then(|| Err(unavailable())));
    let a = auditor(&script);
    assert_eq!(a.report(Duration::from_secs(600)).await.unwrap_err().kind(), "unavailable");
    script.set(|_| None);
    let r = a.report(Duration::from_secs(600)).await.unwrap();
    assert_eq!(r.overall, Overall::Pass);
    assert_eq!(script.queries(), 1 + ALL_QUERIES);
}

#[tokio::test]
async fn a_report_with_failed_checks_is_still_a_report_and_is_cached() {
    let target = count_of("orphan_transfers");
    let script = Script::with(move |sql| (sql == target).then(|| Err(unavailable())));
    let a = auditor(&script);
    let r = a.report(Duration::from_secs(600)).await.unwrap();
    assert_eq!(r.overall, Overall::Incomplete);
    assert!(Arc::ptr_eq(&r, &a.report(Duration::from_secs(600)).await.unwrap()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_report_callers_share_one_audit() {
    let script = Script::held(|_| None);
    let a = Arc::new(auditor(&script));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let a = Arc::clone(&a);
            tokio::spawn(async move { a.report(Duration::from_secs(60)).await.unwrap() })
        })
        .collect();
    script.sent(1).await;
    // Let every caller reach the gate before the audit can finish.
    tokio::time::sleep(Duration::from_millis(50)).await;
    script.release();
    let reports: Vec<_> = futures_join(handles).await;
    for r in &reports[1..] {
        assert!(Arc::ptr_eq(&reports[0], r));
    }
    assert_eq!(script.queries(), ALL_QUERIES, "one audit for eight callers");
    assert_eq!(script.max_in_flight.load(Ordering::SeqCst), 1);
}

async fn futures_join<T>(handles: Vec<tokio::task::JoinHandle<T>>) -> Vec<T> {
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.unwrap());
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_leader_does_not_strand_its_followers() {
    let script = Script::held(|_| None);
    let a = Arc::new(auditor(&script));
    let leader = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.report(Duration::from_secs(60)).await })
    };
    script.sent(1).await;
    let follower = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.report(Duration::from_secs(60)).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    script.release();
    let r = follower.await.unwrap().unwrap();
    assert_eq!(r.overall, Overall::Pass);
    // The aborted audit's first query, then the follower's whole audit.
    assert_eq!(script.queries(), 1 + ALL_QUERIES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_cancels_running_and_later_audits() {
    let script = Script::held(|_| None);
    let a = Arc::new(auditor(&script));
    let running = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.run(AuditRequest::default()).await })
    };
    let waiting = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.report(Duration::from_secs(60)).await })
    };
    script.sent(1).await;
    a.shutdown();
    assert_eq!(running.await.unwrap().unwrap_err(), QualityError::Cancelled);
    assert_eq!(waiting.await.unwrap().unwrap_err(), QualityError::Cancelled);
    script.release();
    let sent = script.queries();
    assert_eq!(a.run(AuditRequest::default()).await.unwrap_err(), QualityError::Cancelled);
    assert_eq!(a.report(Duration::from_secs(60)).await.unwrap_err(), QualityError::Cancelled);
    assert_eq!(script.queries(), sent, "nothing is sent after shutdown");
}

#[tokio::test]
async fn the_deadline_is_a_timeout() {
    let script = Script::held(|_| None);
    let a = Auditor::with_deadline(Arc::clone(&script) as Arc<dyn LensApi>, TestClock::at(NOW), Duration::from_millis(50));
    let err = a.run(AuditRequest::default()).await.unwrap_err();
    assert_eq!(err.kind(), "timeout", "{err}");
    assert!(err.is_retryable());
    // The gate was released: the next audit runs.
    script.release();
    assert!(a.run(AuditRequest::default()).await.is_ok());
}
