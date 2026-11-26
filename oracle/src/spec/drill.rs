//! Drill-through: the rows behind one row of an answer.
//!
//! A [`SpecAnswer`](super::SpecAnswer) is aggregates: "mint M, 412
//! transfers". It names no transaction, because no dimension is one. A
//! drill is how a client gets from that row to the things it counted: it
//! sends the spec back with the row's group-by values, and receives the
//! newest underlying rows of the spec's table, each carrying the `slot`
//! (and, for every table but `slots`, the `tx_signature`) that opens a
//! [detail report](crate::profile#detail-reports).
//!
//! ```json
//! {
//!   "spec": { "measures": ["transfer_count"], "group_by": [{ "dimension": "mint" }],
//!             "window": { "last_slots": 9000 }, "as_of_slot": 412345678 },
//!   "row": { "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" },
//!   "limit": 50
//! }
//! ```
//!
//! Set `spec.as_of_slot` to the answer's `as_of_slot`: the drill then reads
//! exactly the window the answer read, bucket snap included.
//!
//! # Which rows
//!
//! The rows of the spec's table that satisfy, all at once:
//!
//! 1. the spec's window, resolved and snapped exactly as
//!    [`SpecRunner::run`] does;
//! 2. the measures' row conditions, **when every measure has the same
//!    ones** (the rule [`compile`](super::compile) uses to hoist them): a
//!    drill from `tx_failed_count` lists failed transactions. Measures that
//!    disagree read different rows, so none of their conditions is applied
//!    and the drill lists every row of the group;
//! 3. the spec's filters, compiled by the catalog as for the query;
//! 4. one condition per group-by dimension, from `row`: the value the
//!    answer showed for it.
//!
//! `row` must hold exactly the spec's group-by dimensions; an ungrouped
//! spec takes an empty one and lists the rows its one aggregate read.
//!
//! # A row value becomes a condition on the raw column
//!
//! | Dimension expression        | Value                | Condition                         |
//! |-----------------------------|----------------------|-----------------------------------|
//! | a column                    | a slot number        | `slot = N`                        |
//! |                             | a key or enum member | `column = '…'`                    |
//! | `COALESCE(column, 'x')`     | `x`, the placeholder | `column IS NULL`                  |
//! |                             | anything else        | `column = '…'`                    |
//! | `CASE WHEN p THEN a ELSE b` | `a` / `b`            | `p` / not `p`                     |
//! | `slot_bucket` of width `w`  | bucket `k`           | `slot >= k*w AND slot <= k*w+w-1` |
//!
//! So a drill works for every value an answer can show, the placeholders
//! (`unattributed`, `unknown`) and buckets included, which a filter cannot
//! express. No text from the request is rendered unless it is proven first:
//! a key must be a base58 key, an enum member is replaced by the catalog's
//! own constant, a number is parsed.
//!
//! # The statement
//!
//! ```sql
//! SELECT {column} AS "{name}", …
//! FROM solana.{table}
//! WHERE slot >= {from} AND slot <= {to} [AND {condition}]…
//! GROUP BY {column}, …
//! ORDER BY slot DESC NULLS LAST, {the other columns} ASC NULLS LAST
//! LIMIT {limit + 1}
//! ```
//!
//! `GROUP BY` every selected column collapses rows a PRISM replay wrote
//! twice. The order is total over the selected columns, newest slot first,
//! so the same data gives the same rows; `limit + 1` tells "exactly
//! `limit`" from "more". The statement goes through the same
//! [`self_check`](super::compile::self_check) as a compiled spec.
//!
//! A drill issues [`SPEC_QUERIES`](super::SPEC_QUERIES) LENS queries and
//! shares the spec runner's permits.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use crate::catalog::{Catalog, DimExpr, Dimension, EntityHint, MAX_SLOT, Pred, Table, ValueRule, is_base58_key};

use super::compile::{row_conditions, self_check};
use super::guard::{self, ValidSpec, echo};
use super::{QuerySpec, Session, SlotWindow, SpecError, SpecRunner, resolve_window};

/// Rows a drill returns when the request has no `limit`.
pub const DEFAULT_DRILL_ROWS: u32 = 50;
/// Most rows one drill may return.
pub const MAX_DRILL_ROWS: u32 = 200;

/// "Show me what this row counted."
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrillRequest {
    /// The spec that produced the answer, with `as_of_slot` set to the
    /// answer's to read the same window.
    pub spec: QuerySpec,
    /// Group-by dimension id to the value the answer's row shows.
    #[serde(default)]
    pub row: Map<String, Value>,
    /// Rows to return, [`DEFAULT_DRILL_ROWS`] if absent.
    #[serde(default)]
    pub limit: Option<u32>,
}

/// What a drilled row opens as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Detail {
    /// `GET /profile/transaction/{tx_signature}?slot={slot}`.
    Transaction,
    /// `GET /profile/slot/{slot}`.
    Slot,
}

/// The rows behind one answer row.
///
/// Every field is a pure function of the request and the data at or below
/// `as_of_slot`, except `duration_ms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DrillAnswer {
    pub table: Table,
    /// The detail report each row opens; its key columns are in `columns`.
    pub detail: Detail,
    /// Each with its [`EntityHint`], so every signature, slot, mint, program
    /// and token account is linked from the answer alone.
    pub columns: Vec<DrillColumn>,
    /// One array per row, in `columns` order, newest slot first.
    pub rows: Vec<Vec<Value>>,
    pub has_more: bool,
    /// LENS's own row cap cut the result short of `limit`.
    pub truncated: bool,
    pub limit: u32,
    pub as_of_slot: u64,
    /// The window the drill read: the answer's.
    pub window: SlotWindow,
    /// The statement LENS ran.
    pub sql: String,
    pub queries_run: u32,
    pub duration_ms: u64,
}

/// One column of a drill answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DrillColumn {
    pub name: &'static str,
    /// What the values are, as on a [`ColumnInfo`](super::compile::ColumnInfo).
    pub entity: Option<EntityHint>,
}

/// One selected column: its name in the answer and the expression read.
struct Col {
    name: &'static str,
    /// The table column grouped and ordered by.
    column: &'static str,
    /// `None` selects the column itself.
    cast: Option<&'static str>,
    entity: Option<EntityHint>,
}

const fn col(name: &'static str) -> Col {
    Col { name, column: name, cast: None, entity: None }
}

const fn linked(name: &'static str, entity: EntityHint) -> Col {
    Col { name, column: name, cast: None, entity: Some(entity) }
}

/// What a drill shows of each table: the keys of a detail report first,
/// then what tells one row from the next.
fn columns_of(table: Table) -> Vec<Col> {
    match table {
        Table::Slots => vec![
            linked("slot", EntityHint::Slot),
            col("blockhash"),
            linked("parent_slot", EntityHint::Slot),
            // Unix seconds: LENS would otherwise write a timestamp string.
            Col { name: "block_time", column: "block_time", cast: Some("BIGINT"), entity: None },
            col("transaction_count"),
            col("leader"),
        ],
        Table::Transactions => vec![
            linked("slot", EntityHint::Slot),
            linked("tx_signature", EntityHint::TxSignature),
            col("transaction_index"),
            col("status"),
            col("fee"),
            col("compute_units_consumed"),
        ],
        Table::Instructions => vec![
            linked("slot", EntityHint::Slot),
            linked("tx_signature", EntityHint::TxSignature),
            col("instruction_index"),
            col("inner_instruction_index"),
            linked("program_id", EntityHint::Program),
            col("instruction_type"),
        ],
        Table::TokenTransfers => vec![
            linked("slot", EntityHint::Slot),
            linked("tx_signature", EntityHint::TxSignature),
            col("instruction_index"),
            col("inner_instruction_index"),
            linked("mint_token_address", EntityHint::Mint),
            linked("source_token_account", EntityHint::TokenAccount),
            linked("destination_token_account", EntityHint::TokenAccount),
            // Text: a raw amount can exceed what a JSON client's number holds.
            Col { name: "amount_raw", column: "amount_raw", cast: Some("VARCHAR"), entity: None },
            col("decimals"),
        ],
    }
}

fn invalid_row(dimension: &str, reason: impl Into<String>) -> SpecError {
    SpecError::invalid(format!("row.{dimension}"), reason)
}

/// The condition "this dimension shows `value`", on the raw column. See the
/// [module docs](self#a-row-value-becomes-a-condition-on-the-raw-column).
pub fn row_condition(d: &Dimension, table: Table, width: Option<u64>, value: &Value) -> Result<String, SpecError> {
    let bad = |expected: String| invalid_row(d.id, format!("expected {expected}, got {}", echo(&value.to_string())));
    let text = |expected: &dyn Fn() -> String| value.as_str().ok_or_else(|| bad(expected()));
    // A proven key, or the catalog's own constant for an enum member.
    let literal = |v: &str| -> Result<String, SpecError> {
        match d.values {
            ValueRule::Base58Key if is_base58_key(v) => Ok(format!("'{v}'")),
            ValueRule::Enum(values) => values
                .iter()
                .find(|m| **m == v)
                .map(|m| format!("'{m}'"))
                .ok_or_else(|| bad(d.values.expected())),
            _ => Err(bad(d.values.expected())),
        }
    };
    match d.expr {
        DimExpr::SlotBucket(c) => {
            let width = width.ok_or_else(|| SpecError::Internal(format!("dimension {} has no bucket width", d.id)))?;
            let k = value.as_u64().ok_or_else(|| bad("a bucket number".to_owned()))?;
            let range = k
                .checked_mul(width)
                .and_then(|lo| lo.checked_add(width - 1).map(|hi| (lo, hi)))
                .filter(|(_, hi)| *hi <= MAX_SLOT);
            let (lo, hi) = range.ok_or_else(|| bad("a bucket whose slots fit in i64".to_owned()))?;
            Ok(format!("{c} >= {lo} AND {c} <= {hi}"))
        }
        DimExpr::Column(c) => {
            let column = table.column(c).ok_or_else(|| SpecError::Internal(format!("{table} has no column {c}")))?;
            if value.is_null() && column.nullable {
                return Ok(format!("{c} IS NULL"));
            }
            if column.ty.is_integer() {
                let n = value.as_u64().filter(|n| *n <= MAX_SLOT).ok_or_else(|| bad(d.values.expected()))?;
                return Ok(format!("{c} = {n}"));
            }
            Ok(format!("{c} = {}", literal(text(&|| d.values.expected())?)?))
        }
        DimExpr::Coalesce(c, placeholder) => {
            let v = text(&|| d.values.expected())?;
            if v == placeholder { Ok(format!("{c} IS NULL")) } else { Ok(format!("{c} = {}", literal(v)?)) }
        }
        DimExpr::Case { when, then, otherwise } => {
            let v = text(&|| format!("{then} or {otherwise}"))?;
            if v == then {
                Ok(when.sql())
            } else if v == otherwise {
                Ok(when.negated_sql())
            } else {
                Err(bad(format!("{then} or {otherwise}")))
            }
        }
    }
}

/// The measures' row conditions if they all share the same ones, else none.
fn shared_conditions(valid: &ValidSpec, catalog: &Catalog) -> Result<Vec<Pred>, SpecError> {
    let mut all = Vec::with_capacity(valid.measures.len());
    for id in &valid.measures {
        let m = catalog.measure(id).ok_or_else(|| SpecError::Internal(format!("measure {id} vanished from the catalog")))?;
        all.push(row_conditions(m, valid.table, catalog));
    }
    let shared = all.windows(2).all(|pair| pair[0] == pair[1]);
    Ok(if shared { all.into_iter().next().unwrap_or_default() } else { Vec::new() })
}

/// A drill ready to run: everything but the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidDrill {
    pub spec: ValidSpec,
    /// Every condition after the window, in the documented order.
    pub conditions: Vec<String>,
    pub limit: u32,
}

/// Check `req` against the catalog and the limits. Pure: no LENS call.
pub fn validate(req: &DrillRequest, catalog: &Catalog, limits: &guard::SpecLimits) -> Result<ValidDrill, SpecError> {
    let spec = guard::validate(&req.spec, catalog, limits)?;
    let limit = req.limit.unwrap_or(DEFAULT_DRILL_ROWS);
    if !(1..=MAX_DRILL_ROWS).contains(&limit) {
        return Err(SpecError::invalid("limit", format!("must be 1..={MAX_DRILL_ROWS}, got {limit}")));
    }
    if let Some(extra) = req.row.keys().find(|k| !spec.group_by.iter().any(|g| g.dimension == k.as_str())) {
        return Err(SpecError::invalid("row", format!("{} is not a group-by dimension of the spec", echo(extra))));
    }

    let mut conditions: Vec<String> = shared_conditions(&spec, catalog)?.iter().map(Pred::sql).collect();
    let dimension = |id: &str| {
        catalog.dimension(id).ok_or_else(|| SpecError::Internal(format!("dimension {id} vanished from the catalog")))
    };
    for f in &spec.filters {
        let sql = dimension(f.dimension)?.filter_any_sql(&f.values).map_err(|e| SpecError::Internal(e.to_string()))?;
        conditions.push(sql);
    }
    for g in &spec.group_by {
        let value = req.row.get(g.dimension).ok_or_else(|| {
            invalid_row(g.dimension, "the spec groups by this dimension, so the row must give its value")
        })?;
        conditions.push(row_condition(dimension(g.dimension)?, spec.table, g.bucket_width, value)?);
    }
    Ok(ValidDrill { spec, conditions, limit })
}

/// The statement of `drill` over `window`, and its column names.
pub fn statement(drill: &ValidDrill, window: SlotWindow) -> Result<(String, Vec<DrillColumn>), SpecError> {
    let table = drill.spec.table;
    let cols = columns_of(table);
    let select: Vec<String> = cols
        .iter()
        .map(|c| match c.cast {
            None => format!("{} AS \"{}\"", c.column, c.name),
            Some(ty) => format!("CAST({} AS {ty}) AS \"{}\"", c.column, c.name),
        })
        .collect();
    let group: Vec<&str> = cols.iter().map(|c| c.column).collect();
    let order: Vec<String> = cols
        .iter()
        .map(|c| format!("{} {} NULLS LAST", c.column, if c.column == "slot" { "DESC" } else { "ASC" }))
        .collect();
    let mut sql = format!(
        "SELECT {} FROM {} WHERE slot >= {} AND slot <= {}",
        select.join(", "),
        table.sql_name(),
        window.from_slot,
        window.to_slot
    );
    for condition in &drill.conditions {
        sql.push_str(" AND (");
        sql.push_str(condition);
        sql.push(')');
    }
    sql.push_str(&format!(
        " GROUP BY {} ORDER BY {} LIMIT {}",
        group.join(", "),
        order.join(", "),
        u64::from(drill.limit) + 1
    ));
    self_check(&sql, table, cols.len()).map_err(|e| SpecError::Internal(format!("the drill statement {e}: {sql}")))?;
    Ok((sql, cols.iter().map(|c| DrillColumn { name: c.name, entity: c.entity }).collect()))
}

impl SpecRunner {
    /// Validate `req`, then list the rows behind its answer row. See the
    /// [module docs](self).
    pub async fn drill(&self, req: &DrillRequest, shutdown: &CancellationToken) -> Result<DrillAnswer, SpecError> {
        let valid = validate(req, &self.catalog, &self.limits)?;
        tokio::select! {
            biased;
            () = shutdown.cancelled() => Err(SpecError::Cancelled),
            result = self.execute_drill(valid) => result,
        }
    }

    async fn execute_drill(&self, valid: ValidDrill) -> Result<DrillAnswer, SpecError> {
        let _permit =
            self.permits.acquire().await.map_err(|_| SpecError::Internal("the permit pool is closed".to_owned()))?;
        let started = Instant::now();
        let mut s = Session { lens: self.lens.as_ref(), queries: 0 };

        let w = s.snapshot(valid.spec.as_of_slot).await?;
        let requested = resolve_window(valid.spec.window, w)?;
        let window = match valid.spec.bucket_width() {
            None => requested,
            Some(width) => requested.snap(width).ok_or_else(|| {
                SpecError::NotReady(format!(
                    "the window {}..={} holds no whole bucket of {width} slots yet",
                    requested.from_slot, requested.to_slot
                ))
            })?,
        };

        let (sql, columns) = statement(&valid, window)?;
        let response = s.query(&sql).await?;
        let asked = u64::from(valid.limit) + 1;
        if response.rows.len() as u64 > asked {
            return Err(SpecError::Internal(format!("LENS returned {} rows for LIMIT {asked}", response.rows.len())));
        }
        let mut rows = Vec::with_capacity(response.rows.len());
        for (i, row) in response.rows.iter().enumerate() {
            let values = columns
                .iter()
                .map(|c| row.get(c.name).cloned().ok_or_else(|| format!("column {} is missing", c.name)))
                .collect::<Result<Vec<Value>, String>>()
                .map_err(|e| SpecError::Internal(format!("row {i}: {e}")))?;
            rows.push(values);
        }
        let over = rows.len() > valid.limit as usize;
        rows.truncate(valid.limit as usize);

        Ok(DrillAnswer {
            table: valid.spec.table,
            detail: if valid.spec.table == Table::Slots { Detail::Slot } else { Detail::Transaction },
            columns,
            rows,
            has_more: over || response.truncated,
            truncated: response.truncated,
            limit: valid.limit,
            as_of_slot: w,
            window,
            sql,
            queries_run: s.queries,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::spec::{Filter, FilterOp, GroupBy, Window};

    const KEY: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    fn catalog() -> Catalog {
        Catalog::new(vec![]).unwrap()
    }

    fn spec(measures: &[&str], group_by: &[(&str, Option<u64>)]) -> QuerySpec {
        QuerySpec {
            measures: measures.iter().map(|m| (*m).to_owned()).collect(),
            group_by: group_by
                .iter()
                .map(|(d, w)| GroupBy { dimension: (*d).to_owned(), bucket_width: *w })
                .collect(),
            filters: vec![],
            window: Window::Range { from_slot: 1_000, to_slot: 1_999 },
            order_by: vec![],
            limit: None,
            as_of_slot: None,
        }
    }

    fn request(spec: QuerySpec, row: Value) -> DrillRequest {
        DrillRequest { spec, row: row.as_object().unwrap().clone(), limit: None }
    }

    fn conditions(req: &DrillRequest) -> Vec<String> {
        validate(req, &catalog(), &guard::SpecLimits::default()).unwrap().conditions
    }

    fn rejected(req: &DrillRequest) -> String {
        match validate(req, &catalog(), &guard::SpecLimits::default()) {
            Err(SpecError::Invalid { field, .. }) => field,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn every_kind_of_dimension_value_becomes_a_raw_column_condition() {
        let one = |measure: &str, dim: &str, width: Option<u64>, value: Value| {
            conditions(&request(spec(&[measure], &[(dim, width)]), json!({ dim: value }))).pop().unwrap()
        };
        assert_eq!(one("transfer_count", "mint", None, json!(KEY)), format!("mint_token_address = '{KEY}'"));
        // The placeholders a filter cannot express.
        assert_eq!(one("transfer_count", "mint", None, json!("unattributed")), "mint_token_address IS NULL");
        assert_eq!(one("transfer_count", "source_owner", None, json!("unknown")), "source_owner IS NULL");
        assert_eq!(one("slot_count", "leader", None, json!("unknown")), "leader IS NULL");
        assert_eq!(one("tx_count", "tx_status", None, json!("failed")), "status = 'failed'");
        assert_eq!(one("ix_count", "program", None, json!(KEY)), format!("program_id = '{KEY}'"));
        assert_eq!(one("ix_count", "ix_level", None, json!("top_level")), "inner_instruction_index IS NULL");
        assert_eq!(one("ix_count", "ix_level", None, json!("inner")), "inner_instruction_index IS NOT NULL");
        assert_eq!(one("ix_count", "ix_decoded", None, json!("decoded")), "instruction_type IS NOT NULL");
        assert_eq!(one("tx_count", "slot", None, json!(1_234)), "slot = 1234");
        assert_eq!(one("tx_count", "slot_bucket", Some(100), json!(12)), "slot >= 1200 AND slot <= 1299");
    }

    #[test]
    fn nothing_unproven_is_rendered() {
        let bad = |measure: &str, dim: &str, width: Option<u64>, value: Value| {
            rejected(&request(spec(&[measure], &[(dim, width)]), json!({ dim: value })))
        };
        assert_eq!(bad("transfer_count", "mint", None, json!("x' OR '1'='1")), "row.mint");
        assert_eq!(bad("transfer_count", "mint", None, json!(7)), "row.mint");
        assert_eq!(bad("transfer_count", "mint", None, Value::Null), "row.mint");
        assert_eq!(bad("tx_count", "tx_status", None, json!("FAILED")), "row.tx_status");
        assert_eq!(bad("ix_count", "ix_level", None, json!("outer")), "row.ix_level");
        assert_eq!(bad("tx_count", "slot", None, json!("12")), "row.slot");
        assert_eq!(bad("tx_count", "slot", None, json!(-1)), "row.slot");
        assert_eq!(bad("tx_count", "slot_bucket", Some(100), json!(u64::MAX)), "row.slot_bucket");
        assert_eq!(bad("tx_count", "slot_bucket", Some(100), json!("12")), "row.slot_bucket");
    }

    #[test]
    fn the_row_must_match_the_group_by_exactly() {
        let grouped = spec(&["transfer_count"], &[("mint", None)]);
        assert_eq!(rejected(&request(grouped.clone(), json!({}))), "row.mint");
        assert_eq!(rejected(&request(grouped, json!({ "mint": KEY, "program": KEY }))), "row");
        assert_eq!(rejected(&request(spec(&["tx_count"], &[]), json!({ "slot": 1 }))), "row");
        // An ungrouped spec drills with an empty row.
        assert!(conditions(&request(spec(&["tx_count"], &[]), json!({}))).is_empty());
        // A spec the guard refuses is refused the same way.
        assert_eq!(rejected(&request(spec(&["nope"], &[]), json!({}))), "measures[0]");
        let mut over = request(spec(&["tx_count"], &[]), json!({}));
        over.limit = Some(MAX_DRILL_ROWS + 1);
        assert_eq!(rejected(&over), "limit");
        over.limit = Some(0);
        assert_eq!(rejected(&over), "limit");
    }

    #[test]
    fn measure_conditions_apply_only_when_every_measure_shares_them() {
        let failed = request(spec(&["tx_failed_count"], &[]), json!({}));
        assert_eq!(conditions(&failed), vec!["status = 'failed'"]);
        // One measure reads failed rows, the other every row: neither condition applies.
        let mixed = request(spec(&["tx_failed_count", "tx_count"], &[]), json!({}));
        assert!(conditions(&mixed).is_empty());
        // A measure that needs a mint excludes rows without one.
        let volume = request(spec(&["transfer_volume_raw"], &[("mint", None)]), json!({ "mint": KEY }));
        assert_eq!(conditions(&volume), vec!["mint_token_address IS NOT NULL".to_owned(), format!("mint_token_address = '{KEY}'")]);
    }

    #[test]
    fn the_statement_is_one_checked_select_in_a_total_order() {
        let mut s = spec(&["transfer_count"], &[("mint", None), ("slot_bucket", Some(100))]);
        s.filters = vec![Filter { dimension: "token_program".into(), op: FilterOp::Eq, values: vec![KEY.into()] }];
        let drill = validate(&request(s, json!({ "mint": "unattributed", "slot_bucket": 12 })), &catalog(), &guard::SpecLimits::default())
            .unwrap();
        let (sql, columns) = statement(&drill, SlotWindow::new(1_000, 1_999)).unwrap();
        assert_eq!(
            sql,
            format!(
                "SELECT slot AS \"slot\", tx_signature AS \"tx_signature\", instruction_index AS \"instruction_index\", \
                 inner_instruction_index AS \"inner_instruction_index\", mint_token_address AS \"mint_token_address\", \
                 source_token_account AS \"source_token_account\", destination_token_account AS \"destination_token_account\", \
                 CAST(amount_raw AS VARCHAR) AS \"amount_raw\", decimals AS \"decimals\" \
                 FROM solana.token_transfers WHERE slot >= 1000 AND slot <= 1999 \
                 AND (spl_token_program_id = '{KEY}') AND (mint_token_address IS NULL) AND (slot >= 1200 AND slot <= 1299) \
                 GROUP BY slot, tx_signature, instruction_index, inner_instruction_index, mint_token_address, \
                 source_token_account, destination_token_account, amount_raw, decimals \
                 ORDER BY slot DESC NULLS LAST, tx_signature ASC NULLS LAST, instruction_index ASC NULLS LAST, \
                 inner_instruction_index ASC NULLS LAST, mint_token_address ASC NULLS LAST, \
                 source_token_account ASC NULLS LAST, destination_token_account ASC NULLS LAST, \
                 amount_raw ASC NULLS LAST, decimals ASC NULLS LAST LIMIT 51"
            )
        );
        let names: Vec<&str> = columns.iter().map(|c| c.name).collect();
        assert_eq!(names[..2], ["slot", "tx_signature"]);
        let hinted: Vec<(&str, &str)> =
            columns.iter().filter_map(|c| c.entity.map(|e| (c.name, e.as_str()))).collect();
        assert_eq!(
            hinted,
            vec![
                ("slot", "slot"),
                ("tx_signature", "tx_signature"),
                ("mint_token_address", "mint"),
                ("source_token_account", "token_account"),
                ("destination_token_account", "token_account"),
            ]
        );
        // Every table's statement passes the self-check, and leads with its detail key.
        for (measure, table) in [("slot_count", Table::Slots), ("tx_count", Table::Transactions), ("ix_count", Table::Instructions)] {
            let drill = validate(&request(spec(&[measure], &[]), json!({})), &catalog(), &guard::SpecLimits::default()).unwrap();
            assert_eq!(drill.spec.table, table);
            let (_, columns) = statement(&drill, SlotWindow::new(0, 9)).unwrap();
            assert_eq!((columns[0].name, columns[0].entity), ("slot", Some(EntityHint::Slot)));
            let signature = columns.iter().find(|c| c.name == "tx_signature");
            assert_eq!(signature.is_some(), table != Table::Slots);
            assert!(signature.is_none_or(|c| c.entity == Some(EntityHint::TxSignature)));
        }
    }
}
