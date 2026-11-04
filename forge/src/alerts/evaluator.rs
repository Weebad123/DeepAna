//! One alert over one slot range: the LENS query, what came back, and the
//! webhook payload describing it.
//!
//! Nothing here decides about progress or delivery, and nothing here keeps
//! state: [`evaluate`] makes exactly one LENS call, [`build_payload`] is a
//! pure function. [`super::AlertEngine`] decides what to do with both.
//!
//! # Evaluating
//!
//! [`evaluate`]`(lens, alert, from_slot, to_slot)`:
//!
//! 1. [`Alert::bind_range`] renders the SQL for the inclusive range,
//! 2. [`LensApi::query`] runs it,
//! 3. zero rows is [`EvalOutcome::NoMatch`]; one or more is
//!    [`EvalOutcome::Match`], with LENS's `truncated` flag and row count.
//!
//! # Errors
//!
//! Every [`LensCallError`] becomes an [`EvalError`] whose [`FailureKind`]
//! says whether a later attempt may succeed:
//!
//! | `LensCallError`                                 | `EvalError`                         | Why                                                        |
//! |-------------------------------------------------|-------------------------------------|------------------------------------------------------------|
//! | `Unavailable`, `Timeout`                        | `Transient`                         | LENS may recover                                           |
//! | `BadQuery` naming a table LENS has not registered | `Transient`, `missing_table: true` | a catalog refresh in progress, or PRISM has not written that table yet; the engine bounds the retries |
//! | `BadQuery` otherwise                            | `Permanent`                         | the alert's SQL is wrong, and retrying will not fix it     |
//! | `Protocol`                                      | `Permanent`                         | FORGE and LENS disagree, which is a bug                    |
//!
//! LENS reports an unregistered table (or a missing `solana` schema) as a
//! `query_plan_error` reading `table '<name>' not found`;
//! [`names_missing_table`] looks for exactly that.
//!
//! # Payload
//!
//! ```json
//! {
//!   "version": 1,
//!   "alert": { "id": "…", "name": "…" },
//!   "from_slot": 295000000,
//!   "to_slot": 295000100,
//!   "evaluated_at": 1730000000,
//!   "row_count": 2,
//!   "truncated": false,
//!   "rows_omitted": 0,
//!   "rows": [ { "slot": 295000100, "n": 18446744073709551615 }, … ]
//! }
//! ```
//!
//! | Field          | Meaning                                                                 |
//! |----------------|-------------------------------------------------------------------------|
//! | `version`      | [`PAYLOAD_VERSION`]; bumped on any incompatible change                  |
//! | `from_slot`, `to_slot` | the inclusive range evaluated                                   |
//! | `evaluated_at` | unix seconds                                                            |
//! | `row_count`    | rows LENS returned                                                      |
//! | `truncated`    | LENS dropped rows past its `max_query_rows`                             |
//! | `rows_omitted` | rows LENS returned that did not fit the payload budget                  |
//! | `rows`         | the first `row_count - rows_omitted` rows, as LENS sent them            |
//!
//! Row values pass through untouched, so integers keep full `u64` precision.
//! The budget bounds the whole body: rows are included in order while the
//! body stays within it. The envelope itself is always sent, even when a
//! budget is too small for it.

use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::lens_client::{LensApi, LensCallError};
use crate::registry::Alert;

/// The payload's `version` field.
pub const PAYLOAD_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// What one evaluation found.
#[derive(Debug, Clone, PartialEq)]
pub enum EvalOutcome {
    /// LENS returned no rows.
    NoMatch,
    /// LENS returned at least one row.
    Match {
        rows: Vec<Map<String, Value>>,
        /// LENS dropped rows past its `max_query_rows`.
        truncated: bool,
        /// Rows LENS returned, `rows.len()`.
        row_count: u64,
    },
}

impl EvalOutcome {
    pub const fn is_match(&self) -> bool {
        matches!(self, Self::Match { .. })
    }

    /// The rows; empty for [`Self::NoMatch`].
    pub fn rows(&self) -> &[Map<String, Value>] {
        match self {
            Self::NoMatch => &[],
            Self::Match { rows, .. } => rows,
        }
    }

    pub const fn row_count(&self) -> u64 {
        match self {
            Self::NoMatch => 0,
            Self::Match { row_count, .. } => *row_count,
        }
    }

    /// Whether LENS dropped rows.
    pub const fn truncated(&self) -> bool {
        match self {
            Self::NoMatch => false,
            Self::Match { truncated, .. } => *truncated,
        }
    }
}

/// Whether a later attempt at the same evaluation may succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Transient,
    Permanent,
}

impl FailureKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transient => "transient",
            Self::Permanent => "permanent",
        }
    }
}

/// Why an evaluation failed. See the [module docs](self#errors).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EvalError {
    /// LENS may answer later. `missing_table` marks a query naming a table
    /// LENS has not registered: retried, but only so often.
    #[error("{error}")]
    Transient { error: LensCallError, missing_table: bool },
    /// Retrying the same SQL will not help.
    #[error("{error}")]
    Permanent { error: LensCallError },
}

impl EvalError {
    /// The [`EvalError`] for a failed LENS call.
    pub fn classify(error: LensCallError) -> Self {
        match &error {
            LensCallError::Unavailable { .. } | LensCallError::Timeout { .. } => Self::Transient {
                error,
                missing_table: false,
            },
            LensCallError::BadQuery { message } if names_missing_table(message) => Self::Transient {
                error,
                missing_table: true,
            },
            LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } => Self::Permanent { error },
        }
    }

    pub const fn kind(&self) -> FailureKind {
        match self {
            Self::Transient { .. } => FailureKind::Transient,
            Self::Permanent { .. } => FailureKind::Permanent,
        }
    }

    pub const fn is_missing_table(&self) -> bool {
        matches!(self, Self::Transient { missing_table: true, .. })
    }

    pub const fn lens_error(&self) -> &LensCallError {
        match self {
            Self::Transient { error, .. } | Self::Permanent { error } => error,
        }
    }
}

/// Whether a LENS rejection says a table is not registered:
/// `table '<name>' not found`, in any case.
pub fn names_missing_table(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message
        .find("table '")
        .is_some_and(|start| message[start + "table '".len()..].contains("' not found"))
}

// ---------------------------------------------------------------------------
// Evaluating
// ---------------------------------------------------------------------------

/// Run `alert` over `from_slot..=to_slot` on LENS. One query, no retries.
/// The caller passes `from_slot <= to_slot`.
pub async fn evaluate(
    lens: &dyn LensApi,
    alert: &Alert,
    from_slot: u64,
    to_slot: u64,
) -> Result<EvalOutcome, EvalError> {
    let sql = alert.bind_range(from_slot, to_slot);
    let response = lens.query(&sql).await.map_err(EvalError::classify)?;
    if response.rows.is_empty() {
        return Ok(EvalOutcome::NoMatch);
    }
    Ok(EvalOutcome::Match {
        row_count: response.row_count,
        truncated: response.truncated,
        rows: response.rows,
    })
}

// ---------------------------------------------------------------------------
// Payload
// ---------------------------------------------------------------------------

/// A rendered payload, and how many rows the budget left out of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Payload {
    pub json: String,
    pub rows_omitted: u64,
}

/// The body field for field, in the documented order.
#[derive(Serialize)]
struct Body<'a> {
    version: u32,
    alert: AlertRef<'a>,
    from_slot: u64,
    to_slot: u64,
    evaluated_at: i64,
    row_count: u64,
    truncated: bool,
    rows_omitted: u64,
    rows: &'a [Map<String, Value>],
}

#[derive(Serialize)]
struct AlertRef<'a> {
    id: &'a str,
    name: &'a str,
}

/// The webhook body for `outcome` of `alert` over `from_slot..=to_slot`,
/// evaluated at `now` (unix seconds), within `budget_bytes` where the
/// envelope allows. See the [module docs](self#payload).
pub fn build_payload(
    alert: &Alert,
    from_slot: u64,
    to_slot: u64,
    outcome: &EvalOutcome,
    now: i64,
    budget_bytes: usize,
) -> String {
    render_payload(alert, from_slot, to_slot, outcome, now, budget_bytes).json
}

/// [`build_payload`], also saying how many rows did not fit.
pub fn render_payload(
    alert: &Alert,
    from_slot: u64,
    to_slot: u64,
    outcome: &EvalOutcome,
    now: i64,
    budget_bytes: usize,
) -> Payload {
    let rows = outcome.rows();
    let body = |rows: &'_ [Map<String, Value>], rows_omitted: u64| {
        let body = Body {
            version: PAYLOAD_VERSION,
            alert: AlertRef {
                id: &alert.id,
                name: &alert.name,
            },
            from_slot,
            to_slot,
            evaluated_at: now,
            row_count: outcome.row_count(),
            truncated: outcome.truncated(),
            rows_omitted,
            rows,
        };
        serde_json::to_string(&body).expect("a payload of JSON values always serializes")
    };

    // Only `rows` and the digits of `rows_omitted` change with the number of
    // rows kept, so the size of every candidate follows from one empty body
    // (whose `rows_omitted` is the single digit `0`) and each row's size.
    let empty = body(&[], 0).len();
    let total = rows.len() as u64;
    let mut kept = 0;
    let mut rows_len = 0;
    for (index, row) in rows.iter().enumerate() {
        let row_len = serde_json::to_string(row).expect("a JSON object always serializes").len() + usize::from(index > 0);
        let omitted = total - (index as u64 + 1);
        if empty - 1 + digits(omitted) + rows_len + row_len > budget_bytes {
            break;
        }
        rows_len += row_len;
        kept = index + 1;
    }

    let rows_omitted = total - kept as u64;
    let json = body(&rows[..kept], rows_omitted);
    debug_assert!(kept == 0 || json.len() <= budget_bytes, "{} > {budget_bytes}", json.len());
    Payload { json, rows_omitted }
}

/// Decimal digits of `n`.
fn digits(n: u64) -> usize {
    n.checked_ilog10().map_or(1, |log| log as usize + 1)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::lens_client::{LensHealth, LensResponse, Visibility};

    const SQL: &str = "SELECT slot, n FROM solana.token_transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";

    fn alert() -> Alert {
        Alert {
            id: "5b0c2a52-8f5e-4d34-9f6b-0d8a7f1b2c3d".into(),
            name: "big \"transfers\"".into(),
            sql: SQL.into(),
            webhook_url: "https://hooks.example.com/x".into(),
            secret: "0123456789abcdef".into(),
            wait_for_final: false,
            enabled: true,
            last_evaluated_slot: Some(9),
            created_at: 0,
            updated_at: 0,
        }
    }

    fn rows(value: Value) -> Vec<Map<String, Value>> {
        serde_json::from_value(value).unwrap()
    }

    fn matched(value: Value, truncated: bool) -> EvalOutcome {
        let rows = rows(value);
        EvalOutcome::Match {
            row_count: rows.len() as u64,
            rows,
            truncated,
        }
    }

    /// Answers every query with one scripted result and records the SQL.
    struct Scripted {
        answer: Result<LensResponse, LensCallError>,
        sql: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl LensApi for Scripted {
        async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
            self.sql.lock().unwrap().push(sql.to_owned());
            self.answer.clone()
        }

        async fn wait_until_visible(&self, _slot: u64, _timeout: Duration) -> Visibility {
            unreachable!("the evaluator never gates")
        }

        async fn health(&self) -> Result<LensHealth, LensCallError> {
            unreachable!("the evaluator never checks health")
        }
    }

    fn lens(answer: Result<LensResponse, LensCallError>) -> Scripted {
        Scripted {
            answer,
            sql: Mutex::new(Vec::new()),
        }
    }

    fn response(value: Value, truncated: bool) -> LensResponse {
        let rows = rows(value);
        LensResponse {
            row_count: rows.len() as u64,
            rows,
            duration_ms: 1,
            truncated,
            sql: String::new(),
            round_trip_ms: 1,
        }
    }

    // -- evaluate ------------------------------------------------------------

    #[tokio::test]
    async fn evaluate_binds_the_range_and_runs_exactly_one_query() {
        let lens = lens(Ok(response(json!([]), false)));
        assert_eq!(evaluate(&lens, &alert(), 10, 20).await, Ok(EvalOutcome::NoMatch));
        assert_eq!(
            *lens.sql.lock().unwrap(),
            ["SELECT slot, n FROM solana.token_transfers WHERE slot BETWEEN 10 AND 20"]
        );
    }

    #[tokio::test]
    async fn one_or_more_rows_is_a_match_with_lens_flags() {
        let lens = lens(Ok(response(json!([{ "slot": 12, "n": u64::MAX }, { "slot": 13, "n": 0 }]), true)));
        let outcome = evaluate(&lens, &alert(), 10, 20).await.unwrap();
        assert_eq!(outcome, matched(json!([{ "slot": 12, "n": u64::MAX }, { "slot": 13, "n": 0 }]), true));
        assert!(outcome.is_match() && outcome.truncated());
        assert_eq!(outcome.row_count(), 2);
    }

    #[tokio::test]
    async fn lens_failures_come_back_classified() {
        let err = LensCallError::Unavailable { message: "down".into() };
        let got = evaluate(&lens(Err(err.clone())), &alert(), 1, 1).await.unwrap_err();
        assert_eq!(got, EvalError::Transient { error: err, missing_table: false });
    }

    // -- classify ------------------------------------------------------------

    #[test]
    fn classification_follows_the_documented_table() {
        let bad = |message: &str| LensCallError::BadQuery { message: message.into() };
        for (error, kind, missing) in [
            (LensCallError::Unavailable { message: "m".into() }, FailureKind::Transient, false),
            (LensCallError::Timeout { message: "m".into() }, FailureKind::Transient, false),
            (bad("query_plan_error: table 'datafusion.solana.token_transfers' not found"), FailureKind::Transient, true),
            (bad("query_plan_error: Error during planning: table 'solana.slots' not found"), FailureKind::Transient, true),
            (bad("query_plan_error: Schema error: No field named nope"), FailureKind::Permanent, false),
            (bad("query_plan_error: sql parser error: Expected: an SQL statement"), FailureKind::Permanent, false),
            (bad("query_execution_error: Arrow error: Divide by zero error"), FailureKind::Permanent, false),
            (LensCallError::Protocol { message: "table 'x' not found".into() }, FailureKind::Permanent, false),
        ] {
            let classified = EvalError::classify(error.clone());
            assert_eq!(classified.kind(), kind, "{error}");
            assert_eq!(classified.is_missing_table(), missing, "{error}");
            assert_eq!(classified.lens_error(), &error);
            assert_eq!(classified.to_string(), error.to_string());
        }
    }

    #[test]
    fn missing_table_needs_the_whole_phrase() {
        assert!(names_missing_table("TABLE 'A' NOT FOUND"));
        assert!(!names_missing_table("table not found"));
        assert!(!names_missing_table("column 'slot' not found"));
        assert!(!names_missing_table("' not found ... table '"));
        assert!(!names_missing_table(""));
    }

    // -- payload -------------------------------------------------------------

    #[test]
    fn payload_has_the_documented_shape_and_order() {
        let outcome = matched(json!([{ "slot": 295000100u64, "n": u64::MAX }]), false);
        let json = build_payload(&alert(), 295000000, 295000100, &outcome, 1730000000, usize::MAX);
        assert_eq!(
            json,
            concat!(
                r#"{"version":1,"alert":{"id":"5b0c2a52-8f5e-4d34-9f6b-0d8a7f1b2c3d","name":"big \"transfers\""},"#,
                r#""from_slot":295000000,"to_slot":295000100,"evaluated_at":1730000000,"row_count":1,"#,
                r#""truncated":false,"rows_omitted":0,"rows":[{"n":18446744073709551615,"slot":295000100}]}"#
            )
        );
        // u64 precision survives a round trip through any JSON parser that
        // keeps integers.
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["rows"][0]["n"].as_u64(), Some(u64::MAX));
    }

    #[test]
    fn the_secret_and_webhook_url_never_reach_the_payload() {
        let a = alert();
        let json = build_payload(&a, 1, 2, &matched(json!([{ "x": 1 }]), false), 0, usize::MAX);
        assert!(!json.contains(&a.secret) && !json.contains(&a.webhook_url), "{json}");
    }

    #[test]
    fn lens_truncation_is_reported_as_is() {
        let payload = render_payload(&alert(), 1, 2, &matched(json!([{ "x": 1 }]), true), 0, usize::MAX);
        let parsed: Value = serde_json::from_str(&payload.json).unwrap();
        assert_eq!((parsed["truncated"].as_bool(), parsed["rows_omitted"].as_u64()), (Some(true), Some(0)));
        assert_eq!(payload.rows_omitted, 0);
    }

    #[test]
    fn the_budget_keeps_a_prefix_of_rows_and_counts_the_rest() {
        let many: Vec<Value> = (0..250u64).map(|n| json!({ "slot": 1000 + n, "n": n })).collect();
        let outcome = matched(Value::Array(many), false);
        let unbounded = render_payload(&alert(), 1000, 1249, &outcome, 7, usize::MAX);
        assert_eq!(unbounded.rows_omitted, 0);

        // Every budget from "envelope only" to "everything": the body never
        // exceeds it once a row is kept, rows are a prefix, the counts add
        // up, and one more row would not have fitted.
        let floor = render_payload(&alert(), 1000, 1249, &outcome, 7, 0).json.len();
        for budget in (floor - 5..=unbounded.json.len() + 5).step_by(7) {
            let payload = render_payload(&alert(), 1000, 1249, &outcome, 7, budget);
            let parsed: Value = serde_json::from_str(&payload.json).unwrap();
            let kept = parsed["rows"].as_array().unwrap();
            assert_eq!(kept.len() as u64 + payload.rows_omitted, 250, "budget {budget}");
            assert_eq!(parsed["rows_omitted"].as_u64(), Some(payload.rows_omitted));
            assert_eq!(parsed["row_count"].as_u64(), Some(250));
            assert_eq!(kept.as_slice(), &outcome.rows().iter().take(kept.len()).cloned().map(Value::Object).collect::<Vec<_>>()[..]);
            if !kept.is_empty() {
                assert!(payload.json.len() <= budget, "budget {budget}: {}", payload.json.len());
            }
            if payload.rows_omitted > 0 {
                let one_more = kept.len() + 1;
                let bigger = Value::Array(outcome.rows()[..one_more].iter().cloned().map(Value::Object).collect());
                let mut with_more = parsed.clone();
                with_more["rows"] = bigger;
                with_more["rows_omitted"] = json!(payload.rows_omitted - 1);
                assert!(serde_json::to_string(&with_more).unwrap().len() > budget, "budget {budget}");
            }
        }
    }

    #[test]
    fn the_budget_is_exact_at_the_boundary() {
        let outcome = matched(json!([{ "a": 1 }, { "a": 2 }, { "a": 3 }]), false);
        let full = render_payload(&alert(), 1, 3, &outcome, 0, usize::MAX).json;
        assert_eq!(render_payload(&alert(), 1, 3, &outcome, 0, full.len()).rows_omitted, 0);
        assert_eq!(render_payload(&alert(), 1, 3, &outcome, 0, full.len() - 1).rows_omitted, 1);
    }

    #[test]
    fn a_budget_below_the_envelope_still_sends_the_envelope() {
        let payload = render_payload(&alert(), 1, 2, &matched(json!([{ "x": 1 }]), false), 0, 10);
        let parsed: Value = serde_json::from_str(&payload.json).unwrap();
        assert_eq!(parsed["rows"], json!([]));
        assert_eq!(payload.rows_omitted, 1);
    }

    #[test]
    fn no_match_renders_an_empty_body() {
        let parsed: Value =
            serde_json::from_str(&build_payload(&alert(), 4, 5, &EvalOutcome::NoMatch, 0, usize::MAX)).unwrap();
        assert_eq!((parsed["row_count"].as_u64(), parsed["rows"].clone()), (Some(0), json!([])));
    }

    #[test]
    fn digits_of_every_magnitude() {
        assert_eq!([0, 9, 10, 99, 100, u64::MAX].map(digits), [1, 1, 2, 2, 3, 20]);
    }
}
