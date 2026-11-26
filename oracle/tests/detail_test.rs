//! The detail reports (`Profiler::transaction`, `Profiler::slot`) against a
//! scripted LENS: validation before anything is sent, the fixed query
//! sequence and its slot bounds, the snapshot pin, failure handling, and
//! that they share the profiler's permit pool and deadline. What the
//! reports contain is checked against the real LENS in
//! `detail_lens_test.rs`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use forge::lens_client::{LensApi, LensCallError, LensHealth, LensResponse, Visibility};
use forge::quote_sql_string;
use oracle::profile::*;
use serde_json::{Map, Value, json};

const MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const MAX_SLOTS: u64 = 300_000;
/// The snapshot: `solana.transactions` is behind `solana.slots`.
const W: u64 = 299_990;
/// Where the scripted transaction is.
const TX_SLOT: u64 = 250_000;

fn signature() -> String {
    bs58::encode([7u8; 64]).into_string()
}

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
        Self::new(|sql, _| Ok(scripted(sql)))
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

fn tx_row() -> Value {
    json!({
        "slot": TX_SLOT, "blockhash": "hash", "transaction_index": 0, "tx_signature": signature(), "fee": 5000,
        "status": "success", "err": null, "accounts": "[\"payer\"]", "log_messages": "[]",
        "compute_units_consumed": null,
    })
}

/// One transaction at [`TX_SLOT`] with one instruction and no transfers; a
/// slot with that one transaction. Entity-profile statements get zeros.
fn scripted(sql: &str) -> LensResponse {
    if sql == MAX_SLOTS_SQL {
        return response(sql, vec![json!({ "max_slot": MAX_SLOTS })]);
    }
    if sql == MAX_TX_SLOT_SQL {
        return response(sql, vec![json!({ "max_slot": W })]);
    }
    if sql.contains("log_messages") {
        return response(sql, vec![tx_row()]);
    }
    if sql.starts_with("SELECT slot, blockhash, parent_slot") {
        return response(
            sql,
            vec![json!({
                "slot": TX_SLOT, "blockhash": "hash", "parent_slot": TX_SLOT - 1, "block_time": 1_793_102_400i64,
                "block_height": null, "transaction_count": 1, "leader": null,
            })],
        );
    }
    if sql.starts_with("SELECT instruction_index, inner_instruction_index, program_id") {
        return response(
            sql,
            vec![json!({
                "instruction_index": 0, "inner_instruction_index": null, "program_id": MINT, "accounts": "[]",
                "data_base58": "", "instruction_type": null, "decoded_data": null,
            })],
        );
    }
    if sql.starts_with("SELECT instruction_index, inner_instruction_index, spl_token_program_id") {
        return response(sql, vec![]);
    }
    if sql.contains("AS stored_rows") {
        return response(sql, vec![json!({ "transactions": 1, "failed": 0, "stored_rows": 1 })]);
    }
    if sql.starts_with("SELECT transaction_index, tx_signature") {
        return response(
            sql,
            vec![json!({
                "transaction_index": 0, "tx_signature": signature(), "status": "success", "err": null, "fee": 5000,
                "compute_units_consumed": null,
            })],
        );
    }
    // An entity profile over no activity.
    if sql.contains("GROUP BY") {
        return response(sql, vec![]);
    }
    response(
        sql,
        vec![json!({
            "transfers": 0, "first_slot": null, "last_slot": null, "sources": 0, "destinations": 0,
            "volume_raw": null, "min_decimals": null, "max_decimals": null, "failed_excluded": 0, "unattributed": 0,
        })],
    )
}

fn tx_req() -> TransactionRequest {
    TransactionRequest::in_slot(signature(), TX_SLOT)
}

fn invalid_field<T: std::fmt::Debug>(result: Result<T, ProfileError>) -> String {
    match result {
        Err(ProfileError::Invalid { field, .. }) => field,
        other => panic!("expected Invalid, got {other:?}"),
    }
}

/// Every `slot <op> <integer>` bound in `sql`.
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

/// The single-quoted literals of `sql`.
fn literals(sql: &str) -> Vec<&str> {
    sql.split('\'').skip(1).step_by(2).collect()
}

// ---------------------------------------------------------------------------
// Validation comes first
// ---------------------------------------------------------------------------

#[tokio::test]
async fn nothing_reaches_lens_before_validation() {
    let lens = Fake::standard();
    let profiler = Profiler::new(lens.clone());
    let sig = signature();
    let long = "1".repeat(89);
    let cut = &sig[..sig.len() - 30];
    let zero = format!("{}0", &sig[..sig.len() - 1]);
    let wide = "z".repeat(88);
    let bad_signatures = [
        "",
        "short",
        MINT,            // a 32-byte key, not a signature
        "x' OR '1'='1",  // hostile
        cut,             // too few characters
        long.as_str(),   // too many
        zero.as_str(),   // '0' is not base58
        wide.as_str(),   // 88 characters, but 65 bytes
    ];
    for bad in bad_signatures {
        assert_eq!(invalid_field(profiler.transaction(TransactionRequest::new(bad)).await), "signature", "{bad:?}");
    }
    let hostile = profiler.transaction(TransactionRequest::new("x'; DROP TABLE solana.slots; --")).await.unwrap_err();
    assert!(!hostile.to_string().contains("DROP"), "the reason echoes the input: {hostile}");

    let both = TransactionRequest { window_slots: Some(10), ..tx_req() };
    assert_eq!(invalid_field(profiler.transaction(both).await), "window_slots");
    for window in [0, MAX_WINDOW_SLOTS + 1] {
        let req = TransactionRequest { window_slots: Some(window), ..TransactionRequest::new(signature()) };
        assert_eq!(invalid_field(profiler.transaction(req).await), "window_slots");
    }
    let huge = TransactionRequest { slot: Some(u64::MAX), ..TransactionRequest::new(signature()) };
    assert_eq!(invalid_field(profiler.transaction(huge).await), "slot");
    let pin = TransactionRequest { as_of_slot: Some(u64::MAX), ..tx_req() };
    assert_eq!(invalid_field(profiler.transaction(pin).await), "as_of_slot");

    assert_eq!(invalid_field(profiler.slot(SlotRequest::new(u64::MAX)).await), "slot");
    for limit in [0, MAX_SLOT_PAGE + 1] {
        assert_eq!(invalid_field(profiler.slot(SlotRequest { limit: Some(limit), ..SlotRequest::new(5) }).await), "limit");
    }
    assert_eq!(invalid_field(profiler.slot(SlotRequest { as_of_slot: Some(u64::MAX), ..SlotRequest::new(5) }).await), "as_of_slot");

    assert_eq!(lens.calls(), 0, "an invalid request reached LENS: {:?}", lens.log());
}

#[tokio::test]
async fn a_literal_is_quoted_even_though_validation_already_refused_it() {
    // Validation is the first defence; the quoting routine is the second.
    let hostile = "x' OR '1'='1";
    for sql in [
        sql::transaction_rows(&quote_sql_string(hostile), 1, 2),
        sql::transaction_instructions(&quote_sql_string(hostile), 1),
        sql::transaction_transfers(&quote_sql_string(hostile), 1),
    ] {
        assert!(sql.contains("tx_signature = 'x'' OR ''1''=''1' "), "{sql}");
        // With `''` read as an escaped quote, the statement has one literal.
        assert_eq!(sql.replace("''", "").matches('\'').count(), 2, "{sql}");
    }
}

// ---------------------------------------------------------------------------
// The query sequence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_transaction_report_runs_its_six_statements_in_order() {
    let lens = Fake::standard();
    let report = Profiler::new(lens.clone()).transaction(tx_req()).await.unwrap();
    assert!(report.found);
    assert_eq!(report.as_of_slot, W);
    assert_eq!(report.queries_run, TRANSACTION_QUERIES);
    assert_eq!(TRANSACTION_QUERIES, 6);

    let log = lens.log();
    assert_eq!(log.len(), 6);
    assert_eq!(log[0], MAX_SLOTS_SQL);
    assert_eq!(log[1], MAX_TX_SLOT_SQL);
    for (sql, table) in log[2..].iter().zip(["transactions", "slots", "instructions", "token_transfers"]) {
        assert!(sql.contains(&format!("FROM solana.{table} WHERE slot >= ")), "{sql}");
        // Every statement is bounded to the transaction's slot, which is at or below W.
        assert_eq!((bounds(sql, ">="), bounds(sql, "<=")), (vec![TX_SLOT], vec![TX_SLOT]), "{sql}");
        assert!(sql.contains(" LIMIT "), "{sql}");
        assert!(sql.contains(" ORDER BY "), "{sql}");
    }
    // The signature is the only literal anywhere, and only where it filters.
    assert_eq!(literals(&log[2]), vec![signature()]);
    assert!(literals(&log[3]).is_empty(), "{}", log[3]);
    assert_eq!(literals(&log[4]), vec![signature()]);
    assert_eq!(literals(&log[5]), vec![signature()]);
}

#[tokio::test]
async fn a_search_without_a_slot_is_bounded_by_the_window_ending_at_the_snapshot() {
    let lens = Fake::standard();
    let profiler = Profiler::new(lens.clone());
    let report = profiler.transaction(TransactionRequest::new(signature())).await.unwrap();
    assert_eq!(report.searched, Window::ending_at(W, DEFAULT_WINDOW_SLOTS));
    let search = &lens.log()[2];
    assert_eq!((bounds(search, ">="), bounds(search, "<=")), (vec![W - (DEFAULT_WINDOW_SLOTS - 1)], vec![W]));

    // Pinned: the window ends at the pin, and the pin is reported.
    let lens = Fake::standard();
    let req = TransactionRequest { window_slots: Some(1_000), as_of_slot: Some(250_400), ..TransactionRequest::new(signature()) };
    let report = Profiler::new(lens.clone()).transaction(req).await.unwrap();
    assert_eq!(report.as_of_slot, 250_400);
    assert_eq!((bounds(&lens.log()[2], ">="), bounds(&lens.log()[2], "<=")), (vec![249_401], vec![250_400]));

    // A pin or a slot above W cannot be judged without the snapshot, and is refused with it.
    let lens = Fake::standard();
    let above = TransactionRequest { as_of_slot: Some(W + 1), ..tx_req() };
    assert_eq!(invalid_field(Profiler::new(lens.clone()).transaction(above).await), "as_of_slot");
    let beyond = TransactionRequest::in_slot(signature(), W + 1);
    assert_eq!(invalid_field(Profiler::new(lens.clone()).transaction(beyond).await), "slot");
    assert_eq!(invalid_field(Profiler::new(lens.clone()).slot(SlotRequest::new(W + 1)).await), "slot");
    assert!(lens.log().iter().all(|sql| sql == MAX_SLOTS_SQL || sql == MAX_TX_SLOT_SQL), "{:?}", lens.log());
}

#[tokio::test]
async fn a_signature_that_is_not_found_stops_after_the_search() {
    let lens = Fake::new(|sql, _| Ok(if sql.contains("log_messages") { response(sql, vec![]) } else { scripted(sql) }));
    let report = Profiler::new(lens.clone()).transaction(tx_req()).await.unwrap();
    assert!(!report.found);
    assert_eq!(report.transaction, None);
    assert_eq!(report.queries_run, TRANSACTION_QUERIES_NOT_FOUND);
    assert_eq!(lens.calls(), 3);
    assert_eq!(report.warnings, vec![transaction::WARN_NOT_FOUND]);
}

#[tokio::test]
async fn a_slot_report_runs_its_five_statements_in_order() {
    let lens = Fake::standard();
    let report = Profiler::new(lens.clone())
        .slot(SlotRequest { after_index: Some(9), limit: Some(20), ..SlotRequest::new(TX_SLOT) })
        .await
        .unwrap();
    assert!(report.found);
    assert_eq!(report.queries_run, SLOT_QUERIES);
    let log = lens.log();
    assert_eq!(log.len(), 5);
    assert_eq!((log[0].as_str(), log[1].as_str()), (MAX_SLOTS_SQL, MAX_TX_SLOT_SQL));
    for (sql, table) in log[2..].iter().zip(["slots", "transactions", "transactions"]) {
        assert!(sql.contains(&format!("FROM solana.{table} WHERE slot >= ")), "{sql}");
        assert_eq!((bounds(sql, ">="), bounds(sql, "<=")), (vec![TX_SLOT], vec![TX_SLOT]), "{sql}");
    }
    assert!(log[3].contains("COUNT(DISTINCT tx_signature) AS transactions"), "{}", log[3]);
    // The page: after the index asked, one row more than the limit, in a total order.
    assert!(log[4].contains("AND transaction_index > 9 ORDER BY transaction_index ASC, tx_signature ASC"), "{}", log[4]);
    assert!(log[4].ends_with(" LIMIT 21"), "{}", log[4]);
    assert_eq!(report.page.limit, 20);
}

// ---------------------------------------------------------------------------
// Failure handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn any_lens_failure_fails_the_whole_report() {
    let errors = [
        (LensCallError::Timeout { message: "504".into() }, "timeout"),
        (LensCallError::Unavailable { message: "conn refused".into() }, "unavailable"),
        (LensCallError::BadQuery { message: "table 'solana.instructions' not found".into() }, "not_ready"),
        (LensCallError::BadQuery { message: "syntax error".into() }, "permanent"),
        (LensCallError::Protocol { message: "garbage".into() }, "permanent"),
    ];
    for (err, expected) in &errors {
        for at in 0..TRANSACTION_QUERIES as usize {
            let err = err.clone();
            let lens = Fake::new(move |sql, i| if i == at { Err(err.clone()) } else { Ok(scripted(sql)) });
            let got = Profiler::new(lens.clone()).transaction(tx_req()).await.expect_err("must fail");
            assert_eq!(got.kind(), *expected, "transaction at {at}: {got}");
            assert_eq!(lens.calls(), at + 1, "kept querying after a failure");
        }
        for at in 0..SLOT_QUERIES as usize {
            let err = err.clone();
            let lens = Fake::new(move |sql, i| if i == at { Err(err.clone()) } else { Ok(scripted(sql)) });
            let got = Profiler::new(lens.clone()).slot(SlotRequest::new(TX_SLOT)).await.expect_err("must fail");
            assert_eq!(got.kind(), *expected, "slot at {at}: {got}");
            assert_eq!(lens.calls(), at + 1);
        }
    }
}

#[tokio::test]
async fn a_truncated_or_overfull_result_is_refused() {
    for at in 0..TRANSACTION_QUERIES as usize {
        let lens = Fake::new(move |sql, i| {
            let mut r = scripted(sql);
            r.truncated = i == at;
            Ok(r)
        });
        assert_eq!(Profiler::new(lens).transaction(tx_req()).await.unwrap_err(), ProfileError::Truncated, "at {at}");
    }
    for at in 0..SLOT_QUERIES as usize {
        let lens = Fake::new(move |sql, i| {
            let mut r = scripted(sql);
            r.truncated = i == at;
            Ok(r)
        });
        assert_eq!(Profiler::new(lens).slot(SlotRequest::new(TX_SLOT)).await.unwrap_err(), ProfileError::Truncated);
    }
    // More rows than a statement's own cap: the report would be built from a part.
    let lens = Fake::new(|sql, _| {
        let mut r = scripted(sql);
        if sql.contains("log_messages") {
            r.rows = vec![r.rows[0].clone(); MAX_TRANSACTION_ROWS as usize + 1];
        }
        Ok(r)
    });
    assert_eq!(Profiler::new(lens).transaction(tx_req()).await.unwrap_err(), ProfileError::Truncated);
    // Exactly at the cap is every row, replays and all.
    let lens = Fake::new(|sql, _| {
        let mut r = scripted(sql);
        if sql.contains("log_messages") {
            r.rows = vec![r.rows[0].clone(); MAX_TRANSACTION_ROWS as usize];
        }
        Ok(r)
    });
    let report = Profiler::new(lens).transaction(tx_req()).await.unwrap();
    assert_eq!(report.transaction.unwrap().duplicates.transaction_rows, MAX_TRANSACTION_ROWS as usize - 1);
}

#[tokio::test]
async fn an_answer_of_the_wrong_shape_is_a_permanent_error() {
    let lens = Fake::new(|sql, _| {
        Ok(if sql.contains("log_messages") { response(sql, vec![json!({ "slot": TX_SLOT })]) } else { scripted(sql) })
    });
    assert_eq!(Profiler::new(lens).transaction(tx_req()).await.unwrap_err().kind(), "permanent");
    let lens = Fake::new(|sql, _| Ok(if sql.contains("stored_rows") { response(sql, vec![]) } else { scripted(sql) }));
    assert_eq!(Profiler::new(lens).slot(SlotRequest::new(TX_SLOT)).await.unwrap_err().kind(), "permanent");
}

// ---------------------------------------------------------------------------
// The profiler's permits and deadline apply
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn detail_reports_share_the_profile_permits() {
    // Two permits for everything the profiler does: entity profiles,
    // transaction reports and slot reports together.
    let lens = Fake::slow(Duration::from_millis(20), |sql, _| Ok(scripted(sql)));
    let profiler = Profiler::with_limits(lens.clone(), 2, Duration::from_secs(PROFILE_DEADLINE_SECS));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let p = profiler.clone();
        tasks.spawn(async move { p.transaction(tx_req()).await.map(|_| ()) });
        let p = profiler.clone();
        tasks.spawn(async move { p.slot(SlotRequest::new(TX_SLOT)).await.map(|_| ()) });
        let p = profiler.clone();
        tasks.spawn(async move { p.profile(ProfileRequest::new(Kind::Mint, MINT)).await.map(|_| ()) });
    }
    while let Some(done) = tasks.join_next().await {
        done.unwrap().unwrap();
    }
    assert_eq!(lens.max_in_flight.load(Ordering::SeqCst), 2, "a report ran outside the permit pool");
    assert_eq!(lens.calls(), 4 * (6 + 5 + 7));
}

#[tokio::test(start_paused = true)]
async fn detail_reports_have_the_profile_deadline_and_it_covers_the_wait_for_a_permit() {
    // Each statement takes 30 s: six of them do not fit in 120 s.
    let lens = Fake::slow(Duration::from_secs(30), |sql, _| Ok(scripted(sql)));
    let profiler = Profiler::new(lens.clone());
    let started = tokio::time::Instant::now();
    let err = profiler.transaction(tx_req()).await.unwrap_err();
    assert_eq!(err.kind(), "timeout");
    assert_eq!(err.http_status(), 504);
    assert_eq!(started.elapsed(), Duration::from_secs(PROFILE_DEADLINE_SECS));
    // A slot report's five do not either.
    let err = profiler.slot(SlotRequest::new(TX_SLOT)).await.unwrap_err();
    assert_eq!(err.kind(), "timeout");

    // One permit, held by a report that never finishes: a second one times
    // out while still waiting for it, having sent nothing.
    let lens = Fake::slow(Duration::from_secs(3_600), |sql, _| Ok(scripted(sql)));
    let profiler = Profiler::with_limits(lens.clone(), 1, Duration::from_secs(5));
    let holder = tokio::spawn({
        let p = profiler.clone();
        async move { p.slot(SlotRequest::new(TX_SLOT)).await }
    });
    tokio::task::yield_now().await;
    assert_eq!(lens.calls(), 1);
    let err = profiler.transaction(tx_req()).await.unwrap_err();
    assert_eq!(err.kind(), "timeout");
    assert_eq!(lens.calls(), 1, "the waiting report reached LENS without a permit");
    assert_eq!(holder.await.unwrap().unwrap_err().kind(), "timeout");
}
