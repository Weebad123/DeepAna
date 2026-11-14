//! Shared by the `spec/` tests: a generator of accepted specs that covers
//! the whole catalog, and an independent check that a compiled statement is
//! one safe `SELECT`.

#![allow(dead_code)]

use std::collections::BTreeSet;

use oracle::catalog::{Catalog, DimExpr, Dimension, MeasureKind, Pred, Table, ValueRule};
use oracle::spec::compile::{Compiled, compile};
use oracle::spec::guard::{SpecLimits, ValidSpec, validate};
use oracle::spec::{Direction, Filter, FilterOp, GroupBy, Order, QuerySpec, SlotWindow, Window, resolve_window};
use sqlparser::ast::{GroupByExpr, OrderByKind, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

pub const KEY_A: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const KEY_B: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
/// Bucket width the generator uses.
pub const WIDTH: u64 = 10;
/// A `last_slots` that fits a bucket of [`WIDTH`] however it is aligned.
pub const LAST: u64 = 400;
pub const LIMIT: u32 = 25;

pub fn catalog() -> Catalog {
    Catalog::new(vec![]).unwrap()
}

pub fn limits() -> SpecLimits {
    SpecLimits { max_rows: 1_000, max_slot_span: 216_000 }
}

pub fn spec(measures: &[&str]) -> QuerySpec {
    QuerySpec {
        measures: measures.iter().map(|m| (*m).to_owned()).collect(),
        group_by: vec![],
        filters: vec![],
        window: Window::LastSlots(LAST),
        order_by: vec![],
        limit: Some(LIMIT),
        as_of_slot: None,
    }
}

pub fn group(dimension: &str) -> GroupBy {
    GroupBy { dimension: dimension.to_owned(), bucket_width: (dimension == "slot_bucket").then_some(WIDTH) }
}

pub fn filter(dimension: &str, op: FilterOp, values: &[&str]) -> Filter {
    Filter { dimension: dimension.to_owned(), op, values: values.iter().map(|v| (*v).to_owned()).collect() }
}

pub fn order(target: &str, direction: Direction) -> Order {
    Order { target: target.to_owned(), direction }
}

/// Valid filter values for `d`: one, and several.
pub fn sample_values(d: &Dimension) -> (Vec<&'static str>, Vec<&'static str>) {
    match d.values {
        ValueRule::Base58Key => (vec![KEY_A], vec![KEY_A, KEY_B]),
        ValueRule::Enum(values) => (vec![values[0]], values.to_vec()),
        ValueRule::SlotRange => (vec!["1005"], vec!["1000..=1010", "1300"]),
        ValueRule::Bool => (vec!["true"], vec!["true", "false"]),
        ValueRule::BucketWidth => (vec![], vec![]),
    }
}

/// Satisfy `measure`'s required dimension, if any, with a one-value filter.
fn pin_required(catalog: &Catalog, s: &mut QuerySpec) {
    for m in s.measures.clone() {
        if let Some(req) = catalog.measure(&m).unwrap().requires_dimension
            && !s.group_by.iter().any(|g| g.dimension == req)
            && !s.filters.iter().any(|f| f.dimension == req)
        {
            let (one, _) = sample_values(catalog.dimension(req).unwrap());
            s.filters.push(filter(req, FilterOp::Eq, &one));
        }
    }
}

/// Specs covering every measure, every compatible group-by and filter
/// dimension, both directions on both kinds of order key, filters with one
/// and several values, measures combined per table, and both window forms.
/// Every one of them must pass the guard.
pub fn accepted_specs(catalog: &Catalog) -> Vec<(String, QuerySpec)> {
    let mut out: Vec<(String, QuerySpec)> = Vec::new();
    let mut push = |label: String, mut s: QuerySpec| {
        pin_required(catalog, &mut s);
        out.push((label, s));
    };
    for m in catalog.measures() {
        let dims: Vec<&Dimension> = catalog.dimensions().iter().filter(|d| d.table.includes(m.table)).collect();
        push(format!("{} alone", m.id), spec(&[m.id]));
        push(
            format!("{} over a range", m.id),
            QuerySpec { window: Window::Range { from_slot: 1_000, to_slot: 1_299 }, ..spec(&[m.id]) },
        );
        for d in &dims {
            for (label, ord) in [
                ("", vec![]),
                (" by measure desc", vec![order(m.id, Direction::Desc)]),
                (" by measure asc", vec![order(m.id, Direction::Asc)]),
                (" by dimension desc", vec![order(d.id, Direction::Desc)]),
            ] {
                push(
                    format!("{} grouped by {}{label}", m.id, d.id),
                    QuerySpec { group_by: vec![group(d.id)], order_by: ord, ..spec(&[m.id]) },
                );
            }
            if d.filterable {
                let (one, many) = sample_values(d);
                push(
                    format!("{} where {} eq", m.id, d.id),
                    QuerySpec { filters: vec![filter(d.id, FilterOp::Eq, &one)], ..spec(&[m.id]) },
                );
                if m.requires_dimension != Some(d.id) {
                    push(
                        format!("{} where {} in", m.id, d.id),
                        QuerySpec { filters: vec![filter(d.id, FilterOp::In, &many)], ..spec(&[m.id]) },
                    );
                }
            }
        }
    }
    for table in Table::ALL {
        let ms: Vec<&str> = catalog.measures().iter().filter(|m| m.table == table).map(|m| m.id).collect();
        let dims: Vec<&Dimension> = catalog.dimensions().iter().filter(|d| d.table.includes(table)).collect();
        push(format!("every {table} measure"), spec(&ms));
        for (i, a) in dims.iter().enumerate() {
            for b in &dims[i + 1..] {
                let mut s = QuerySpec {
                    group_by: vec![group(a.id), group(b.id)],
                    order_by: ms.iter().rev().map(|m| order(m, Direction::Desc)).collect(),
                    ..spec(&ms)
                };
                // Filter on every other filterable dimension too.
                for f in dims.iter().filter(|f| f.filterable && f.id != a.id && f.id != b.id) {
                    let (one, many) = sample_values(f);
                    // A required dimension that is not grouped must be pinned to one value.
                    let required = ms.iter().any(|m| catalog.measure(m).unwrap().requires_dimension == Some(f.id));
                    s.filters.push(filter(f.id, FilterOp::In, if required { &one } else { &many }));
                }
                push(format!("every {table} measure by {} and {}", a.id, b.id), s);
            }
        }
    }
    out
}

/// The window the runner would compile `valid` over with snapshot `w`.
pub fn window_for(valid: &ValidSpec, w: u64) -> SlotWindow {
    let requested = resolve_window(valid.window, w).unwrap();
    match valid.bucket_width() {
        None => requested,
        Some(width) => requested.snap(width).unwrap(),
    }
}

pub fn compile_at(catalog: &Catalog, s: &QuerySpec, w: u64) -> (ValidSpec, Compiled) {
    let valid = validate(s, catalog, &limits()).unwrap_or_else(|e| panic!("rejected: {e}\n{s:#?}"));
    let window = window_for(&valid, w);
    let compiled = compile(&valid, catalog, window).unwrap_or_else(|e| panic!("did not compile: {e}\n{s:#?}"));
    (valid, compiled)
}

/// Every string literal a catalog definition may render.
pub fn catalog_literals(catalog: &Catalog) -> BTreeSet<String> {
    let mut preds: Vec<Pred> = Vec::new();
    let mut out = BTreeSet::new();
    for m in catalog.measures() {
        match m.kind {
            MeasureKind::Count { filter: Some(p) } | MeasureKind::SumDecimal { filter: Some(p), .. } => preds.push(p),
            MeasureKind::Ratio { numerator, denominator } => preds.extend(std::iter::once(numerator).chain(denominator)),
            _ => {}
        }
    }
    for d in catalog.dimensions() {
        match d.expr {
            DimExpr::Coalesce(_, lit) => {
                out.insert(lit.to_owned());
            }
            DimExpr::Case { when, then, otherwise } => {
                preds.push(when);
                out.insert(then.to_owned());
                out.insert(otherwise.to_owned());
            }
            _ => {}
        }
        if let ValueRule::Enum(values) = d.values {
            out.extend(values.iter().map(|v| (*v).to_owned()));
        }
    }
    out.extend(preds.iter().filter_map(|p| match p {
        Pred::EqStr(_, v) => Some((*v).to_owned()),
        _ => None,
    }));
    out
}

/// Every word an unquoted identifier or keyword in a compiled statement
/// may be, beyond the table's own columns.
const SQL_WORDS: &[&str] = &[
    "SELECT", "AS", "FROM", "WHERE", "AND", "OR", "IS", "NOT", "NULL", "GROUP", "BY", "ORDER", "ASC", "DESC",
    "NULLS", "LAST", "LIMIT", "COUNT", "DISTINCT", "SUM", "AVG", "CAST", "DECIMAL", "VARCHAR", "DOUBLE", "BIGINT",
    "COALESCE", "NULLIF", "CASE", "WHEN", "THEN", "ELSE", "END", "TRUE", "FALSE", "solana",
];

/// Prove, independently of `spec::compile::self_check`, that `compiled`
/// is exactly one safe `SELECT` for `valid`:
///
/// * one statement, a plain `SELECT` over `solana.<table>` with no join;
/// * every token is punctuation, a number this spec accounts for, a string
///   literal that is a catalog constant or one of the spec's filter values,
///   a word from a fixed list or a column of the table, or a quoted alias
///   that is one of the compiled column names; no comment, no `;`;
/// * the window is the first condition, the `LIMIT` is `limit + 1`;
/// * one `GROUP BY` expression per group-by dimension;
/// * every sort key is `NULLS LAST`, and the keys end with every
///   group-by dimension not ordered explicitly, ascending.
pub fn assert_safe_select(catalog: &Catalog, valid: &ValidSpec, compiled: &Compiled) {
    let sql = compiled.sql.as_str();
    let table = valid.table;
    let w = compiled.window;
    let dialect = GenericDialect {};

    // Tokens.
    let literals = catalog_literals(catalog);
    let filter_values: BTreeSet<&str> = valid.filters.iter().flat_map(|f| f.values.iter().map(String::as_str)).collect();
    let mut numbers: BTreeSet<String> = ["0", "1", "38"].into_iter().map(str::to_owned).collect();
    numbers.extend([w.from_slot, w.to_slot, u64::from(valid.limit) + 1].map(|n| n.to_string()));
    if let Some(width) = valid.bucket_width() {
        numbers.insert(width.to_string());
    }
    for v in &filter_values {
        for part in v.split("..=") {
            if !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()) {
                numbers.insert(part.parse::<u64>().unwrap().to_string());
            }
        }
    }
    let aliases: BTreeSet<&str> = compiled.columns.iter().map(|c| c.name).collect();
    let columns: BTreeSet<&str> = table.columns().iter().map(|c| c.name).collect();
    for token in Tokenizer::new(&dialect, sql).tokenize().unwrap() {
        match &token {
            Token::Whitespace(Whitespace::Space)
            | Token::Comma
            | Token::LParen
            | Token::RParen
            | Token::Period
            | Token::Eq
            | Token::GtEq
            | Token::LtEq
            | Token::Mul
            | Token::Div => {}
            Token::Number(n, false) => assert!(numbers.contains(n), "number {n} is not accounted for: {sql}"),
            Token::SingleQuotedString(s) => assert!(
                literals.contains(s) || filter_values.contains(s.as_str()),
                "literal {s:?} is neither a catalog constant nor a filter value: {sql}"
            ),
            Token::Word(word) => match word.quote_style {
                Some('"') => assert!(aliases.contains(word.value.as_str()), "alias {:?}: {sql}", word.value),
                None => assert!(
                    SQL_WORDS.contains(&word.value.as_str())
                        || columns.contains(word.value.as_str())
                        || word.value == table.name(),
                    "word {:?}: {sql}",
                    word.value
                ),
                Some(q) => panic!("quote {q}: {sql}"),
            },
            other => panic!("token {other:?} has no business in a compiled statement: {sql}"),
        }
    }

    // Structure.
    let statements = Parser::parse_sql(&dialect, sql).unwrap_or_else(|e| panic!("{e}: {sql}"));
    assert_eq!(statements.len(), 1, "{sql}");
    let Statement::Query(query) = &statements[0] else { panic!("not a query: {sql}") };
    assert!(query.with.is_none(), "{sql}");
    let SetExpr::Select(select) = query.body.as_ref() else { panic!("not a plain SELECT: {sql}") };
    assert_eq!(select.from.len(), 1, "{sql}");
    assert!(select.from[0].joins.is_empty(), "{sql}");
    assert_eq!(select.from[0].relation.to_string(), table.sql_name(), "{sql}");
    assert!(select.having.is_none() && select.distinct.is_none(), "{sql}");
    assert_eq!(select.projection.len(), valid.measures.len() + valid.group_by.len(), "{sql}");
    assert!(
        sql.contains(&format!("FROM {} WHERE slot >= {} AND slot <= {}", table.sql_name(), w.from_slot, w.to_slot)),
        "the window is not the first condition: {sql}"
    );
    assert!(sql.ends_with(&format!(" LIMIT {}", u64::from(valid.limit) + 1)), "{sql}");

    let group_exprs = match &select.group_by {
        GroupByExpr::Expressions(exprs, modifiers) => {
            assert!(modifiers.is_empty(), "{sql}");
            exprs.iter().map(ToString::to_string).collect::<Vec<_>>()
        }
        GroupByExpr::All(_) => panic!("GROUP BY ALL: {sql}"),
    };
    assert_eq!(group_exprs.len(), valid.group_by.len(), "{sql}");

    let keys = match &query.order_by {
        None => vec![],
        Some(o) => match &o.kind {
            OrderByKind::Expressions(keys) => keys.clone(),
            OrderByKind::All(_) => panic!("ORDER BY ALL: {sql}"),
        },
    };
    assert_eq!(keys.is_empty(), valid.order_by.is_empty() && valid.group_by.is_empty(), "{sql}");
    for k in &keys {
        assert_eq!(k.options.nulls_first, Some(false), "a key is not NULLS LAST: {sql}");
        assert!(k.options.asc.is_some(), "a key has no direction: {sql}");
    }
    let explicit = valid.order_by.len();
    assert!(keys.len() >= explicit, "{sql}");
    let tail: Vec<String> = keys[explicit..].iter().map(|k| k.expr.to_string()).collect();
    let expected_tail: Vec<String> = group_exprs
        .iter()
        .enumerate()
        .filter(|(i, _)| !valid.order_by.iter().any(|o| o.key == oracle::spec::guard::OrderKey::GroupBy(*i)))
        .map(|(_, e)| e.clone())
        .collect();
    assert_eq!(tail, expected_tail, "the order does not end with the group-by dimensions: {sql}");
    assert!(keys[explicit..].iter().all(|k| k.options.asc == Some(true)), "{sql}");
}
