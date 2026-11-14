//! `spec/mod.rs` against a scripted LENS: the fixed query plan (exactly
//! three queries), the snapshot and window rules, the error mapping,
//! shaping, `limit + 1`, truncation, cancellation and the permit limit.
//! The numbers themselves are checked against the real LENS in
//! `spec_lens_test.rs`.

mod common;
mod spec_support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensResponse, Visibility};
use oracle::catalog::Catalog;
use oracle::config::ENV_MAX_ROWS;
use oracle::spec::compile::compile;
use oracle::spec::guard::{SpecLimits, validate};
use oracle::spec::*;
use serde_json::{Map, Value, json};
use spec_support::{KEY_A, KEY_B, filter, group, limits, order, spec};
use tokio_util::sync::CancellationToken;

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

    /// The standard snapshot, and `rows` for the statement.
    fn rows(rows: Vec<Value>) -> Arc<Self> {
        Self::new(move |sql, _| Ok(snapshot(sql).unwrap_or_else(|| response(sql, rows.clone()))))
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

fn runner(lens: Arc<Fake>) -> SpecRunner {
    SpecRunner::with_limits(Arc::new(Catalog::new(vec![]).unwrap()), lens, limits())
}

async fn run(lens: &Arc<Fake>, s: &QuerySpec) -> Result<SpecAnswer, SpecError> {
    runner(Arc::clone(lens)).run(s, &CancellationToken::new()).await
}

/// The statement the runner should send for `s` over `window`.
fn expected_sql(s: &QuerySpec, window: SlotWindow) -> String {
    let c = Catalog::new(vec![]).unwrap();
    compile(&validate(s, &c, &limits()).unwrap(), &c, window).unwrap().sql
}

fn by_mint() -> QuerySpec {
    QuerySpec {
        group_by: vec![group("mint")],
        order_by: vec![order("transfer_count", Direction::Desc)],
        limit: Some(3),
        ..spec(&["transfer_count", "transfer_volume_raw"])
    }
}

fn mint_row(mint: &str, n: u64, volume: Option<&str>) -> Value {
    json!({ "mint": mint, "transfer_count": n, "transfer_volume_raw": volume })
}

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_run_issues_exactly_the_snapshot_then_the_statement() {
    let lens = Fake::rows(vec![mint_row(KEY_A, 5, Some("184467440737095516150")), mint_row("unattributed", 2, None)]);
    let a = run(&lens, &by_mint()).await.unwrap();

    let w = MAX_TX; // the smaller of the two
    let window = SlotWindow::new(w - 399, w);
    assert_eq!(lens.log(), [MAX_SLOTS_SQL.to_owned(), MAX_TX_SLOT_SQL.to_owned(), expected_sql(&by_mint(), window)]);
    assert_eq!(a.queries_run, SPEC_QUERIES);
    assert_eq!(a.as_of_slot, w);
    assert_eq!((a.requested_window, a.window), (window, window));
    assert_eq!(a.sql, lens.log()[2]);

    // Rows are arrays in column order; the decimal stays exact text.
    let names: Vec<_> = a.columns.iter().map(|c| c.name).collect();
    assert_eq!(names, ["mint", "transfer_count", "transfer_volume_raw"]);
    assert_eq!(a.rows, vec![vec![json!(KEY_A), json!(5), json!("184467440737095516150")], vec![json!("unattributed"), json!(2), Value::Null]]);
    assert!(!a.has_more && !a.truncated);
    assert_eq!(a.limit, 3);
    let c = Catalog::new(vec![]).unwrap();
    let expected: Vec<String> = c.measure("transfer_volume_raw").unwrap().warnings.iter().chain(c.dimension("mint").unwrap().warnings).map(|w| w.to_string()).collect();
    assert_eq!(a.warnings, expected);

    // And it serialises as the API will send it.
    let v = serde_json::to_value(&a).unwrap();
    assert_eq!(v["rows"][0], json!([KEY_A, 5, "184467440737095516150"]));
    assert_eq!(v["columns"][2], json!({ "name": "transfer_volume_raw", "role": "measure", "type": "decimal", "nullable": true, "unit": "raw_token_amount" }));
    assert_eq!(v["window"], json!({ "from_slot": w - 399, "to_slot": w, "slots": 400 }));
}

#[tokio::test]
async fn an_invalid_spec_never_reaches_lens() {
    let lens = Fake::rows(vec![]);
    for s in [
        spec(&[]),
        spec(&["nope"]),
        spec(&["transfer_volume_raw"]),
        QuerySpec { window: Window::LastSlots(0), ..spec(&["tx_count"]) },
        QuerySpec { filters: vec![filter("mint", FilterOp::Eq, &["x' OR 1=1 --"])], ..spec(&["transfer_count"]) },
    ] {
        assert_eq!(run(&lens, &s).await.unwrap_err().kind(), "invalid");
    }
    assert_eq!(lens.calls(), 0);
}

// ---------------------------------------------------------------------------
// Snapshot and window
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_snapshot_is_the_newest_slot_both_tables_cover() {
    for (slots, txs) in [(500, 400), (400, 500), (450, 450)] {
        let lens = Fake::new(move |sql, _| {
            Ok(match sql {
                s if s == MAX_SLOTS_SQL => response(sql, vec![json!({ "max_slot": slots })]),
                s if s == MAX_TX_SLOT_SQL => response(sql, vec![json!({ "max_slot": txs })]),
                _ => response(sql, vec![json!({ "tx_count": 1 })]),
            })
        });
        let a = run(&lens, &spec(&["tx_count"])).await.unwrap();
        assert_eq!(a.as_of_slot, slots.min(txs));
        assert_eq!(a.window.to_slot, slots.min(txs));
    }
}

#[tokio::test]
async fn as_of_slot_pins_the_snapshot_and_may_not_pass_it() {
    let lens = Fake::rows(vec![json!({ "tx_count": 7 })]);
    let pinned = QuerySpec { as_of_slot: Some(1_000), ..spec(&["tx_count"]) };
    let a = run(&lens, &pinned).await.unwrap();
    assert_eq!(a.as_of_slot, 1_000);
    assert_eq!(a.window, SlotWindow::new(601, 1_000));
    assert_eq!(lens.log()[2], expected_sql(&pinned, SlotWindow::new(601, 1_000)));

    let at_w = QuerySpec { as_of_slot: Some(MAX_TX), ..spec(&["tx_count"]) };
    assert_eq!(run(&lens, &at_w).await.unwrap().as_of_slot, MAX_TX);

    let lens = Fake::rows(vec![]);
    let above = QuerySpec { as_of_slot: Some(MAX_TX + 1), ..spec(&["tx_count"]) };
    match run(&lens, &above).await {
        Err(SpecError::Invalid { field, reason }) => {
            assert_eq!(field, "as_of_slot");
            assert!(reason.contains(&format!("above {MAX_TX}")), "{reason}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(lens.calls(), 2, "judged by the snapshot, nothing more");
}

#[tokio::test]
async fn windows_resolve_against_the_snapshot() {
    let lens = Fake::rows(vec![json!({ "tx_count": 1 })]);
    let window_of = |window: Window, as_of: Option<u64>| QuerySpec { window, as_of_slot: as_of, ..spec(&["tx_count"]) };

    // The last N slots end at W.
    let a = run(&lens, &window_of(Window::LastSlots(1), None)).await.unwrap();
    assert_eq!(a.window, SlotWindow::new(MAX_TX, MAX_TX));
    // Clipped at slot 0, never wrapped.
    let a = run(&lens, &window_of(Window::LastSlots(5_000), Some(100))).await.unwrap();
    assert_eq!((a.requested_window, a.window), (SlotWindow::new(0, 100), SlotWindow::new(0, 100)));
    // A range is used as given, up to W.
    let range = Window::Range { from_slot: MAX_TX - 1_000, to_slot: MAX_TX };
    assert_eq!(run(&lens, &window_of(range, None)).await.unwrap().window, SlotWindow::new(MAX_TX - 1_000, MAX_TX));
    let n = lens.calls();
    let past = Window::Range { from_slot: MAX_TX - 1_000, to_slot: MAX_TX + 1 };
    match run(&lens, &window_of(past, None)).await {
        Err(SpecError::Invalid { field, .. }) => assert_eq!(field, "window.to_slot"),
        other => panic!("{other:?}"),
    }
    assert_eq!(lens.calls(), n + 2);
    // A pin moves the line a range must stay under.
    let pinned = window_of(Window::Range { from_slot: 10, to_slot: 500 }, Some(499));
    assert_eq!(run(&lens, &pinned).await.unwrap_err().kind(), "invalid");
}

#[tokio::test]
async fn bucket_grouping_snaps_the_window_inward() {
    let lens = Fake::rows(vec![json!({ "slot_bucket": 2_999, "tx_count": 1 })]);
    let bucketed = |window, as_of| QuerySpec {
        group_by: vec![GroupBy { dimension: "slot_bucket".into(), bucket_width: Some(100) }],
        window,
        as_of_slot: as_of,
        ..spec(&["tx_count"])
    };
    // W = 299_990: the last 500 slots are 299_491..=299_990; whole buckets
    // of 100 inside are 299_500..=299_899.
    let a = run(&lens, &bucketed(Window::LastSlots(500), None)).await.unwrap();
    assert_eq!(a.requested_window, SlotWindow::new(299_491, 299_990));
    assert_eq!(a.window, SlotWindow::new(299_500, 299_899));
    assert!(lens.log()[2].contains("WHERE slot >= 299500 AND slot <= 299899"), "{}", lens.log()[2]);
    assert_eq!(a.columns[0].bucket_width, Some(100));

    // Aligned already: unchanged.
    let a = run(&lens, &bucketed(Window::Range { from_slot: 1_000, to_slot: 1_999 }, None)).await.unwrap();
    assert_eq!(a.window, a.requested_window);

    // Clipped at 0 with less than one bucket of data: not ready yet.
    let n = lens.calls();
    let err = run(&lens, &bucketed(Window::LastSlots(500), Some(42))).await.unwrap_err();
    assert_eq!(err.kind(), "not_ready", "{err}");
    assert_eq!(lens.calls(), n + 2);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

fn failing_at(position: usize, err: LensCallError) -> Arc<Fake> {
    Fake::new(move |sql, index| {
        if index == position {
            return Err(err.clone());
        }
        Ok(snapshot(sql).unwrap_or_else(|| response(sql, vec![json!({ "tx_count": 1 })])))
    })
}

#[tokio::test]
async fn lens_errors_map_as_the_table_says_at_every_step() {
    let s = |m: &str| m.to_owned();
    let cases = [
        (LensCallError::Unavailable { message: s("connection refused") }, "unavailable"),
        (LensCallError::Timeout { message: s("504") }, "timeout"),
        (LensCallError::BadQuery { message: s("table 'datafusion.solana.transactions' not found") }, "not_ready"),
        (LensCallError::BadQuery { message: s("Schema error: No field named fee2.") }, "internal"),
        (LensCallError::BadQuery { message: s("sql parser error: Expected end of statement") }, "internal"),
        (LensCallError::Protocol { message: s("status 418") }, "internal"),
    ];
    for position in 0..3 {
        for (err, kind) in &cases {
            let lens = failing_at(position, err.clone());
            let got = run(&lens, &spec(&["tx_count"])).await.unwrap_err();
            assert_eq!(got.kind(), *kind, "query {position}: {err:?} → {got:?}");
            assert_eq!(lens.calls(), position + 1, "nothing runs after a failure");
        }
    }
    let got = run(&failing_at(2, LensCallError::Timeout { message: s("took 30 s") }), &spec(&["tx_count"])).await.unwrap_err();
    assert_eq!(got, SpecError::Timeout("took 30 s".into()));
    assert!(got.is_retryable() && got.http_status() == 504);
    assert_eq!(SpecError::Internal(String::new()).http_status(), 500);
    assert!(!SpecError::Internal(String::new()).is_retryable());
}

#[tokio::test]
async fn an_empty_or_malformed_snapshot_is_reported() {
    let with_snapshot = |rows: Vec<Value>, truncated: bool| {
        Fake::new(move |sql, _| {
            let mut r = response(sql, if sql == MAX_SLOTS_SQL { rows.clone() } else { vec![json!({ "max_slot": 5, "tx_count": 1 })] });
            r.truncated = truncated && sql == MAX_SLOTS_SQL;
            Ok(r)
        })
    };
    let kind = |lens: Arc<Fake>| async move { run(&lens, &spec(&["tx_count"])).await.unwrap_err().kind() };
    assert_eq!(kind(with_snapshot(vec![json!({ "max_slot": null })], false)).await, "not_ready");
    assert_eq!(kind(with_snapshot(vec![], false)).await, "internal");
    assert_eq!(kind(with_snapshot(vec![json!({ "max": 5 })], false)).await, "internal");
    assert_eq!(kind(with_snapshot(vec![json!({ "max_slot": "5" })], false)).await, "internal");
    assert_eq!(kind(with_snapshot(vec![json!({ "max_slot": -1 })], false)).await, "internal");
    assert_eq!(kind(with_snapshot(vec![json!({ "max_slot": 5 })], true)).await, "internal");
}

#[tokio::test]
async fn a_missing_renamed_or_mistyped_column_is_internal() {
    let bad_rows = [
        ("missing", vec![json!({ "mint": KEY_A, "transfer_count": 1 })]),
        ("renamed", vec![json!({ "mint": KEY_A, "count": 1, "transfer_volume_raw": "1" })]),
        ("extra", vec![json!({ "mint": KEY_A, "transfer_count": 1, "transfer_volume_raw": "1", "x": 1 })]),
        ("count as text", vec![mint_row(KEY_A, 0, None).tap(|r| r["transfer_count"] = json!("1"))]),
        ("negative count", vec![mint_row(KEY_A, 0, None).tap(|r| r["transfer_count"] = json!(-1))]),
        ("float count", vec![mint_row(KEY_A, 0, None).tap(|r| r["transfer_count"] = json!(1.5))]),
        ("null count", vec![mint_row(KEY_A, 0, None).tap(|r| r["transfer_count"] = Value::Null)]),
        ("null dimension", vec![mint_row(KEY_A, 1, None).tap(|r| r["mint"] = Value::Null)]),
        ("numeric dimension", vec![mint_row(KEY_A, 1, None).tap(|r| r["mint"] = json!(7))]),
        ("decimal not digits", vec![mint_row(KEY_A, 1, Some("12.5"))]),
        ("decimal empty", vec![mint_row(KEY_A, 1, Some(""))]),
        ("decimal as number", vec![mint_row(KEY_A, 1, None).tap(|r| r["transfer_volume_raw"] = json!(12))]),
        ("past LIMIT + 1", (0..5).map(|i| mint_row(KEY_A, i, None)).collect()),
    ];
    for (label, rows) in bad_rows {
        let lens = Fake::rows(rows);
        match run(&lens, &by_mint()).await {
            Err(SpecError::Internal(msg)) => assert!(!msg.is_empty(), "{label}"),
            other => panic!("{label}: {other:?}"),
        }
    }
    // An aggregate without GROUP BY returns exactly one row.
    for rows in [vec![], vec![json!({ "tx_count": 1 }), json!({ "tx_count": 2 })]] {
        let lens = Fake::rows(rows);
        assert_eq!(run(&lens, &spec(&["tx_count"])).await.unwrap_err().kind(), "internal");
    }
    // Floats and nulls where the catalog allows them.
    let lens = Fake::rows(vec![json!({ "fee_avg_lamports": 5000.5, "tx_failure_ratio": null, "fee_total_lamports": null })]);
    let a = run(&lens, &spec(&["fee_avg_lamports", "tx_failure_ratio", "fee_total_lamports"])).await.unwrap();
    assert_eq!(a.rows, vec![vec![json!(5000.5), Value::Null, Value::Null]]);
}

trait Tap {
    fn tap(self, f: impl FnOnce(&mut Self)) -> Self;
}

impl Tap for Value {
    fn tap(mut self, f: impl FnOnce(&mut Self)) -> Self {
        f(&mut self);
        self
    }
}

// ---------------------------------------------------------------------------
// Limit and truncation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_extra_row_sets_has_more_and_is_dropped() {
    let rows = |n: u64| (0..n).map(|i| mint_row(KEY_A, 10 - i, Some("1"))).collect::<Vec<_>>();
    // limit 3, LIMIT 4 asked.
    for (n, kept, more) in [(0, 0, false), (2, 2, false), (3, 3, false), (4, 3, true)] {
        let a = run(&Fake::rows(rows(n)), &by_mint()).await.unwrap();
        assert_eq!((a.rows.len(), a.has_more, a.truncated), (kept, more, false), "{n} rows");
        if n == 4 {
            assert_eq!(a.rows.last().unwrap()[1], json!(8), "the first `limit` rows are kept");
        }
    }
}

#[tokio::test]
async fn a_result_cut_by_lens_is_flagged_not_hidden_and_not_an_error() {
    let lens = Fake::new(|sql, _| {
        Ok(snapshot(sql).unwrap_or_else(|| {
            let mut r = response(sql, vec![mint_row(KEY_A, 9, Some("1")), mint_row(KEY_B, 8, Some("2"))]);
            r.truncated = true;
            r
        }))
    });
    let a = run(&lens, &by_mint()).await.unwrap();
    assert!(a.truncated, "flagged");
    assert!(a.has_more);
    assert_eq!(a.rows.len(), 2, "what LENS returned is returned");
    assert_eq!(serde_json::to_value(&a).unwrap()["truncated"], json!(true));
}

// ---------------------------------------------------------------------------
// Concurrency, cancellation, config
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn at_most_four_specs_run_at_once() {
    let lens = Fake::slow(Duration::from_millis(30), |sql, _| {
        Ok(snapshot(sql).unwrap_or_else(|| response(sql, vec![json!({ "tx_count": 1 })])))
    });
    let r = runner(Arc::clone(&lens));
    let token = CancellationToken::new();
    let s = spec(&["tx_count"]);
    let tasks: Vec<_> = (0..12)
        .map(|_| {
            let (r, token, s) = (r.clone(), token.clone(), s.clone());
            tokio::spawn(async move { r.run(&s, &token).await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().unwrap();
    }
    assert_eq!(lens.max_in_flight.load(Ordering::SeqCst), MAX_CONCURRENT_SPECS);
    assert_eq!(lens.calls(), 12 * SPEC_QUERIES as usize);
}

#[tokio::test]
async fn cancellation_stops_a_run_before_or_during_it() {
    // Before: validation still answers first, then nothing is sent.
    let lens = Fake::rows(vec![json!({ "tx_count": 1 })]);
    let token = CancellationToken::new();
    token.cancel();
    assert_eq!(runner(Arc::clone(&lens)).run(&spec(&["tx_count"]), &token).await, Err(SpecError::Cancelled));
    assert_eq!(runner(Arc::clone(&lens)).run(&spec(&["nope"]), &token).await.unwrap_err().kind(), "invalid");
    assert_eq!(lens.calls(), 0);

    // During: a slow LENS is abandoned.
    let lens = Fake::slow(Duration::from_secs(30), |sql, _| Ok(response(sql, vec![])));
    let token = CancellationToken::new();
    let r = runner(Arc::clone(&lens));
    let (t2, s) = (token.clone(), spec(&["tx_count"]));
    let task = tokio::spawn(async move { r.run(&s, &t2).await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    token.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), task).await.expect("cancel is prompt").unwrap();
    assert_eq!(result, Err(SpecError::Cancelled));
    assert_eq!(SpecError::Cancelled.http_status(), 503);
}

#[tokio::test]
async fn the_runner_takes_its_limits_from_the_config() {
    let fixture = common::Fixture::new();
    let config = fixture.load(&[(ENV_MAX_ROWS, "5")]).unwrap();
    let lens = Fake::rows(vec![json!({ "tx_count": 1 })]);
    let r = SpecRunner::new(Arc::new(Catalog::new(vec![]).unwrap()), lens, &config);
    assert_eq!(r.limits(), SpecLimits { max_rows: 5, max_slot_span: config.max_slot_span() });
    let token = CancellationToken::new();
    let a = r.run(&QuerySpec { limit: None, ..spec(&["tx_count"]) }, &token).await.unwrap();
    assert_eq!(a.limit, 5);
    assert!(a.sql.ends_with(" LIMIT 6"), "{}", a.sql);
    match r.run(&QuerySpec { limit: Some(6), ..spec(&["tx_count"]) }, &token).await {
        Err(SpecError::Invalid { field, .. }) => assert_eq!(field, "limit"),
        other => panic!("{other:?}"),
    }
}
