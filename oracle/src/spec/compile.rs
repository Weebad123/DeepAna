//! Compilation: a [`ValidSpec`] and a resolved window into one `SELECT`.
//!
//! [`compile`] is pure. Its only runtime input is the window, already
//! resolved against the snapshot `W`, snapped to whole buckets and pinned
//! by `as_of_slot`. The statement is assembled **only** from catalog
//! fragments ([`Measure::aggregate_sql`], [`Dimension::select_expr`],
//! [`Dimension::filter_any_sql`], [`Pred::sql`]), catalog ids, and integers
//! this module computes. No client text reaches it except filter values,
//! which the catalog renders only after proving them (a base58 key, an enum
//! member it replaces with its own constant, a parsed slot number).
//!
//! # The statement
//!
//! ```sql
//! SELECT {dimension} AS "{id}", …, {measure} AS "{id}", …
//! FROM solana.{table}
//! WHERE slot >= {from} AND slot <= {to} [AND {measure filter}] [AND {filter}]…
//! [GROUP BY {dimension}, …]
//! [ORDER BY {key} ASC|DESC NULLS LAST, …]
//! LIMIT {limit + 1}
//! ```
//!
//! 1. **Select list**: dimensions first, then measures, each in the order
//!    requested, aliased by catalog id (quoted; ids are `[a-z][a-z0-9_]*`).
//! 2. **From**: `solana.<table>`, the one table every measure reads.
//! 3. **Where**: the window first, so it can prune files. Then each
//!    measure's row conditions, **if every measure has the same ones**;
//!    otherwise each measure folds its own into its aggregate
//!    ([`Measure::aggregate_sql`]) so it does not drop rows another
//!    measure reads. Then the client's filters, sorted by dimension, each on
//!    the raw column.
//! 4. **Group by**: the dimension expressions repeated in full, never the
//!    aliases: `source_owner` is both an alias and a column.
//! 5. **Order by**: the requested keys, then every group-by dimension not
//!    already a key, ascending. Group-by values identify a row, so the
//!    order is total and the same data always gives the same rows. Every
//!    key is `NULLS LAST`. A measure sorts by [`Measure::sort_sql`], so a
//!    decimal sum sorts as a number, not as its text.
//! 6. **Limit**: `limit + 1`, so the runner can tell "exactly `limit`" from
//!    "more than `limit`".
//! 7. **Self-check**: the finished statement is tokenized and parsed with
//!    `sqlparser` on every call, not only in tests ([`self_check`]). A
//!    failure is a compiler bug, reported as [`SpecError::Internal`].
//! 8. **Warnings**: the catalog warnings of the measures, then of the
//!    group-by dimensions, in order, each once.
//!
//! A measure's **row conditions** are its own [`Measure::filter`], plus
//! `{column} IS NOT NULL` when it requires a dimension whose raw column is
//! nullable: a measure that is only meaningful per mint does not count rows
//! with no mint.
//!
//! [`Measure::filter`]: crate::catalog::Measure::filter

use std::ops::ControlFlow;

use serde::Serialize;
use sqlparser::ast::{Expr, SetExpr, Statement, TableFactor, visit_expressions, visit_relations};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

use crate::catalog::{Catalog, ColType, DimExpr, Dimension, MAX_SLOT, Measure, MeasureKind, Pred, SLOT_COLUMN, Table, Unit};

use super::guard::{OrderKey, ValidSpec};
use super::{Direction, SlotWindow, SpecError};

/// The compiled statement and what the runner needs to shape its rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Compiled {
    pub sql: String,
    /// One per select item, in order.
    pub columns: Vec<ColumnInfo>,
    pub warnings: Vec<String>,
    /// The window the statement reads.
    pub window: SlotWindow,
}

/// Whether a column is a group-by dimension or a measure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Dimension,
    Measure,
}

/// The JSON type a column's values come back as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    /// A non-negative integer.
    Integer,
    /// A JSON number.
    Float,
    /// A string.
    Text,
    /// An exact non-negative integer as a string of digits: it can exceed
    /// what a JSON number holds.
    Decimal,
    Bool,
}

/// One column of the answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ColumnInfo {
    /// The catalog id: the select alias and the column's name.
    pub name: &'static str,
    pub role: Role,
    #[serde(rename = "type")]
    pub value_type: ValueType,
    /// Whether a value may be `null`.
    pub nullable: bool,
    /// A measure's unit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<Unit>,
    /// `slot_bucket`'s width: bucket `k` holds slots `k * width ..= k *
    /// width + width - 1`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_width: Option<u64>,
}

impl ColumnInfo {
    /// Whether `value` is what this column should hold.
    pub fn accepts(&self, value: &serde_json::Value) -> bool {
        use serde_json::Value;
        match (self.value_type, value) {
            (_, Value::Null) => self.nullable,
            (ValueType::Integer, v) => v.as_u64().is_some(),
            (ValueType::Float, Value::Number(_)) => true,
            (ValueType::Text, Value::String(_)) => true,
            (ValueType::Decimal, Value::String(s)) => !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()),
            (ValueType::Bool, Value::Bool(_)) => true,
            _ => false,
        }
    }
}

fn internal(message: impl Into<String>) -> SpecError {
    SpecError::Internal(message.into())
}

/// A measure's row conditions: its filter, and the null exclusion of the
/// dimension it requires. See the [module docs](self).
pub fn row_conditions(measure: &Measure, table: Table, catalog: &Catalog) -> Vec<Pred> {
    let mut conds: Vec<Pred> = measure.filter().copied().into_iter().collect();
    if let Some(required) = measure.requires_dimension.and_then(|id| catalog.dimension(id)) {
        let raw = required.raw_column();
        let not_null = Pred::IsNotNull(raw);
        if table.column(raw).is_some_and(|c| c.nullable) && !conds.contains(&not_null) {
            conds.push(not_null);
        }
    }
    conds
}

/// The JSON type and nullability of a dimension's values on `table`.
fn dimension_type(d: &Dimension, table: Table) -> (ValueType, bool) {
    match d.expr {
        DimExpr::Column(c) => match table.column(c) {
            Some(col) => {
                let ty = match col.ty {
                    t if t.is_integer() => ValueType::Integer,
                    ColType::Bool => ValueType::Bool,
                    _ => ValueType::Text,
                };
                (ty, col.nullable)
            }
            None => (ValueType::Text, true),
        },
        DimExpr::Coalesce(..) | DimExpr::Case { .. } => (ValueType::Text, false),
        DimExpr::SlotBucket(_) => (ValueType::Integer, false),
    }
}

/// The JSON type and nullability of a measure's values.
fn measure_type(m: &Measure) -> (ValueType, bool) {
    match m.kind {
        MeasureKind::Count { .. } | MeasureKind::CountDistinct { .. } => (ValueType::Integer, false),
        // `SUM` over no rows is NULL: a group none of whose rows the measure reads.
        MeasureKind::SumDecimal { .. } => (ValueType::Decimal, true),
        MeasureKind::Avg { .. } | MeasureKind::Ratio { .. } => (ValueType::Float, true),
    }
}

/// Compile `valid` over `window`. See the [module docs](self).
///
/// Every error is [`SpecError::Internal`]: `valid` passed the guard, so a
/// failure here is a bug, not the client's mistake.
pub fn compile(valid: &ValidSpec, catalog: &Catalog, window: SlotWindow) -> Result<Compiled, SpecError> {
    let table = valid.table;

    // Resolve again; the guard did, so a miss is a bug.
    let measures = valid
        .measures
        .iter()
        .map(|id| catalog.measure(id).ok_or_else(|| internal(format!("measure {id} vanished from the catalog"))))
        .collect::<Result<Vec<_>, _>>()?;
    let dimensions = valid
        .group_by
        .iter()
        .map(|g| {
            catalog
                .dimension(g.dimension)
                .map(|d| (d, g.bucket_width))
                .ok_or_else(|| internal(format!("dimension {} vanished from the catalog", g.dimension)))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if measures.is_empty() || measures.iter().any(|m| m.table != table) {
        return Err(internal("the measures do not all read the spec's table"));
    }
    for (d, width) in &dimensions {
        d.check_param(*width).map_err(|e| internal(e.to_string()))?;
    }

    // The window must be sane and, with buckets, snapped.
    if window.from_slot > window.to_slot || window.to_slot > MAX_SLOT || window.slots != window.to_slot - window.from_slot + 1 {
        return Err(internal(format!("window {}..={} is malformed", window.from_slot, window.to_slot)));
    }
    if let Some(w) = valid.bucket_width()
        && (!window.from_slot.is_multiple_of(w) || !(window.to_slot + 1).is_multiple_of(w))
    {
        return Err(internal(format!("window {}..={} is not snapped to buckets of {w}", window.from_slot, window.to_slot)));
    }

    // Row conditions: hoisted when shared, folded otherwise.
    let conds: Vec<Vec<Pred>> = measures.iter().map(|m| row_conditions(m, table, catalog)).collect();
    let shared = conds.windows(2).all(|pair| pair[0] == pair[1]);
    let when = |i: usize| -> &[Pred] { if shared { &[] } else { &conds[i] } };

    // 1. Select list.
    let dim_exprs: Vec<String> = dimensions.iter().map(|(d, w)| d.select_expr(*w)).collect();
    let mut select: Vec<String> = Vec::with_capacity(dimensions.len() + measures.len());
    for ((d, _), expr) in dimensions.iter().zip(&dim_exprs) {
        select.push(format!("{expr} AS \"{}\"", d.id));
    }
    for (i, m) in measures.iter().enumerate() {
        select.push(format!("{} AS \"{}\"", m.aggregate_sql(when(i)), m.id));
    }

    // 3. Where.
    let (from, to) = (window.from_slot, window.to_slot);
    let mut conditions = vec![format!("{SLOT_COLUMN} >= {from} AND {SLOT_COLUMN} <= {to}")];
    if shared {
        conditions.extend(conds[0].iter().map(Pred::sql));
    }
    for f in &valid.filters {
        let d = catalog
            .dimension(f.dimension)
            .ok_or_else(|| internal(format!("dimension {} vanished from the catalog", f.dimension)))?;
        if !d.table.includes(table) {
            return Err(internal(format!("filter dimension {} is not on {table}", d.id)));
        }
        conditions.push(d.filter_any_sql(&f.values).map_err(|e| internal(format!("a validated filter failed: {e}")))?);
    }

    // 5. Order by.
    let dir = |d: Direction| match d {
        Direction::Asc => "ASC",
        Direction::Desc => "DESC",
    };
    let mut order: Vec<String> = Vec::new();
    for o in &valid.order_by {
        let key = match o.key {
            OrderKey::Measure(i) => measures.get(i).map(|m| m.sort_sql(when(i))),
            OrderKey::GroupBy(i) => dim_exprs.get(i).cloned(),
        }
        .ok_or_else(|| internal("an order key points past the select list"))?;
        order.push(format!("{key} {} NULLS LAST", dir(o.direction)));
    }
    for (i, expr) in dim_exprs.iter().enumerate() {
        if !valid.order_by.iter().any(|o| o.key == OrderKey::GroupBy(i)) {
            order.push(format!("{expr} ASC NULLS LAST"));
        }
    }

    // Assemble.
    let mut sql = format!("SELECT {} FROM {} WHERE {}", select.join(", "), table.sql_name(), conditions.join(" AND "));
    if !dim_exprs.is_empty() {
        sql.push_str(&format!(" GROUP BY {}", dim_exprs.join(", ")));
    }
    if !order.is_empty() {
        sql.push_str(&format!(" ORDER BY {}", order.join(", ")));
    }
    sql.push_str(&format!(" LIMIT {}", u64::from(valid.limit) + 1));

    // 7. Self-check.
    self_check(&sql, table, select.len()).map_err(|e| internal(format!("compiled SQL failed its self-check: {e}")))?;

    // Columns and 8. warnings.
    let mut columns = Vec::with_capacity(select.len());
    for (d, width) in &dimensions {
        let (value_type, nullable) = dimension_type(d, table);
        columns.push(ColumnInfo { name: d.id, role: Role::Dimension, value_type, nullable, unit: None, bucket_width: *width });
    }
    for m in &measures {
        let (value_type, nullable) = measure_type(m);
        columns.push(ColumnInfo { name: m.id, role: Role::Measure, value_type, nullable, unit: Some(m.unit), bucket_width: None });
    }
    let mut warnings: Vec<String> = Vec::new();
    let all = measures.iter().flat_map(|m| m.warnings).chain(dimensions.iter().flat_map(|(d, _)| d.warnings));
    for w in all {
        if !warnings.iter().any(|seen| seen == w) {
            warnings.push((*w).to_owned());
        }
    }

    Ok(Compiled { sql, columns, warnings, window })
}

/// Prove `sql` is exactly one plain `SELECT` of `columns` items over
/// `table` and nothing else: no second statement, comment, placeholder,
/// `WITH`, set operation, join, subquery, `HAVING`, `DISTINCT` or `INTO`,
/// and a `LIMIT`. Run on every compiled statement.
pub fn self_check(sql: &str, table: Table, columns: usize) -> Result<(), String> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().map_err(|e| format!("does not tokenize: {e}"))?;
    for token in &tokens {
        match token {
            Token::SemiColon => return Err("contains a semicolon".to_owned()),
            Token::Placeholder(_) => return Err("contains a placeholder".to_owned()),
            Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)) => {
                return Err("contains a comment".to_owned());
            }
            _ => {}
        }
    }

    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| format!("does not parse: {e}"))?;
    let [Statement::Query(query)] = statements.as_slice() else {
        return Err(format!("is {} statements, or not a query", statements.len()));
    };
    if query.with.is_some() {
        return Err("has a WITH clause".to_owned());
    }
    if query.limit_clause.is_none() {
        return Err("has no LIMIT".to_owned());
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err("is not a plain SELECT".to_owned());
    };
    if select.distinct.is_some() || select.having.is_some() || select.into.is_some() {
        return Err("has DISTINCT, HAVING or INTO".to_owned());
    }
    if select.projection.len() != columns {
        return Err(format!("selects {} items, expected {columns}", select.projection.len()));
    }
    match select.from.as_slice() {
        [from] if from.joins.is_empty() => match &from.relation {
            TableFactor::Table { name, args: None, .. } if name.to_string() == table.sql_name() => {}
            other => return Err(format!("reads {other}, expected {}", table.sql_name())),
        },
        _ => return Err("must read exactly one table, with no join".to_owned()),
    }
    let mut relations = 0;
    let _ = visit_relations(&statements, |_| {
        relations += 1;
        ControlFlow::<()>::Continue(())
    });
    if relations != 1 {
        return Err(format!("names {relations} relations"));
    }
    let subquery = visit_expressions(&statements, |e| match e {
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    });
    if subquery.is_break() {
        return Err("has a subquery".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_self_check_refuses_anything_but_one_plain_select() {
        let t = Table::Transactions;
        let ok = "SELECT COUNT(*) AS \"tx_count\" FROM solana.transactions WHERE slot >= 1 AND slot <= 2 LIMIT 2";
        assert_eq!(self_check(ok, t, 1), Ok(()));
        for bad in [
            format!("{ok}; DROP TABLE x"),
            format!("{ok} -- hi"),
            "SELECT COUNT(*) FROM solana.transactions WHERE slot = $1 LIMIT 2".to_owned(),
            "WITH a AS (SELECT 1) SELECT COUNT(*) FROM solana.transactions LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.transactions UNION SELECT COUNT(*) FROM solana.slots LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.transactions t JOIN solana.slots s ON t.slot = s.slot LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.transactions WHERE slot IN (SELECT slot FROM solana.slots) LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.transactions GROUP BY fee HAVING COUNT(*) > 1 LIMIT 2".to_owned(),
            "SELECT DISTINCT COUNT(*) FROM solana.transactions LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.slots LIMIT 2".to_owned(),
            "SELECT COUNT(*) FROM solana.transactions".to_owned(),
            "SELECT COUNT(*), 1 FROM solana.transactions LIMIT 2".to_owned(),
            "SELECT COUNT(* FROM solana.transactions LIMIT 2".to_owned(),
            "DELETE FROM solana.transactions".to_owned(),
        ] {
            assert!(self_check(&bad, t, 1).is_err(), "accepted: {bad}");
        }
    }
}
