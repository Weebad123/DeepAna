//! Static analysis of model SQL: placeholders, shape, tables, functions,
//! types and window bounds.
//!
//! The guard is **conservative**: it accepts a subset of what DataFusion
//! accepts, chosen so that a model that passes reads only the window it is
//! given, means the same thing on every run, and can be compared exactly.
//! It is a static check, not the proof: `invariant.bound_independence`
//! (removing every row outside the window changes nothing) is.
//!
//! The parser is sqlparser with the generic dialect, the version and
//! dialect DataFusion itself parses with, so the guard reads the statement
//! the engine will run.
//!
//! # Placeholders
//!
//! The FORGE alert convention, exactly: the window is `{{from_slot}}` and
//! `{{to_slot}}`, both required, no other placeholder. Each is **bare**: not
//! inside a string, quoted identifier or comment, and not touching an
//! identifier character, a quote, `.` or `$`. The statement is single: one
//! trailing `;` at most. [`render`] writes both as plain decimal integers,
//! as FORGE's `bind_range` does. `$` may not appear in plain SQL at all, so
//! no other placeholder syntax can pass for these two.
//!
//! # Rules
//!
//! | Check                     | A model must                                                         |
//! |---------------------------|----------------------------------------------------------------------|
//! | `static.sql_placeholders` | follow the placeholder convention above                              |
//! | `static.sql_parse`        | parse as exactly one statement, at most 64 KiB                       |
//! | `static.sql_shape`        | be one `SELECT` query (non-recursive `WITH`, set operations, joins and subqueries allowed), name every output column with a unique lowercase identifier (`expr AS name`), at most 32 of them, use `LIMIT`/`OFFSET` only with `ORDER BY` and as integer literals, no `*`, `DISTINCT ON`, `INTO`, `TOP`, `QUALIFY`, named windows, `NATURAL`/`APPLY`/`ASOF` joins, `LATERAL` or column-renaming aliases, at most 8 table references |
//! | `static.sql_tables`       | read only `solana.slots`, `solana.transactions`, `solana.instructions`, `solana.token_transfers` and its own CTEs, with no table function or hint |
//! | `static.sql_functions`    | call only the deterministic, exact functions in [`ALLOWED_FUNCTIONS`]; ranking and offset window functions need `ORDER BY` in `OVER` |
//! | `static.sql_types`        | use no float: no fractional or exponent literal, no cast to `FLOAT`, `REAL` or `DOUBLE` |
//! | `static.sql_bounds`       | bound every reference to a `solana.*` table, in the `WHERE` of the `SELECT` whose `FROM` holds it, by a top-level `AND` conjunct `<t>.slot BETWEEN {{from_slot}} AND {{to_slot}}` (or `>= {{from_slot}}` and `<= {{to_slot}}`), where `<t>` is its alias, its table name, or nothing when it is the only relation in that `FROM` |
//!
//! Every problem is reported, sorted by line, then check, then reason.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    CastKind, Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, LimitClause, ObjectName, ObjectNamePart, OrderByKind, Query,
    Select, SelectFlavor, SelectItem, SetExpr, Spanned, Statement, TableFactor, TableWithJoins,
    Value, ValueWithSpan, Visit, Visitor, WindowType,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

use crate::config::echo;
use crate::fixtures::Table;

pub const FROM_SLOT: &str = "from_slot";
pub const TO_SLOT: &str = "to_slot";
/// FORGE's limit for alert and saved-query SQL.
pub const MAX_SQL_BYTES: usize = 64 * 1024;
/// Most references to tables (base tables and CTEs) in one model.
pub const MAX_TABLE_REFS: usize = 8;
/// Most output columns; the engine enforces the same bound.
pub const MAX_OUTPUT_COLUMNS: usize = 32;
/// The schema PRISM's tables live in, on LENS and in the engine.
pub const SCHEMA: &str = "solana";
const PARSER_RECURSION_LIMIT: usize = 64;

pub const CHECK_PLACEHOLDERS: &str = "static.sql_placeholders";
pub const CHECK_PARSE: &str = "static.sql_parse";
pub const CHECK_SHAPE: &str = "static.sql_shape";
pub const CHECK_TABLES: &str = "static.sql_tables";
pub const CHECK_FUNCTIONS: &str = "static.sql_functions";
pub const CHECK_TYPES: &str = "static.sql_types";
pub const CHECK_BOUNDS: &str = "static.sql_bounds";
/// Every check the guard makes, in report order.
pub const GUARD_CHECKS: [&str; 7] = [
    CHECK_PLACEHOLDERS,
    CHECK_PARSE,
    CHECK_SHAPE,
    CHECK_TABLES,
    CHECK_FUNCTIONS,
    CHECK_TYPES,
    CHECK_BOUNDS,
];

/// Functions a model may call: deterministic, exact (no float result), and
/// independent of the session, the clock and the machine.
pub const ALLOWED_FUNCTIONS: &[&str] = &[
    // Aggregates.
    "count", "sum", "min", "max", "bool_and", "bool_or",
    // Conditionals.
    "coalesce", "nullif", "nvl", "nvl2", "greatest", "least",
    // Integers.
    "abs", "gcd", "lcm",
    // Strings.
    "length", "char_length", "character_length", "octet_length", "bit_length", "lower", "upper",
    "concat", "concat_ws", "ltrim", "rtrim", "btrim", "left", "right", "lpad", "rpad", "replace",
    "reverse", "repeat", "split_part", "starts_with", "ends_with", "strpos", "substr",
    "translate", "initcap", "ascii", "chr", "to_hex", "md5", "regexp_like", "regexp_replace",
    // Time.
    "date_trunc", "date_part", "date_bin", "to_timestamp_seconds", "to_unixtime",
    "from_unixtime",
    // Window.
    "row_number", "rank", "dense_rank", "ntile", "lag", "lead", "first_value", "last_value",
    "nth_value",
];

/// Window functions whose result depends on row order: `OVER` must have
/// `ORDER BY`.
const ORDER_DEPENDENT: &[&str] = &[
    "row_number", "rank", "dense_rank", "ntile", "lag", "lead", "first_value", "last_value",
    "nth_value",
];

/// One problem with a model's SQL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GuardIssue {
    /// 1-based line of the SQL it is on, when known.
    pub line: Option<u64>,
    /// One of [`GUARD_CHECKS`].
    pub check: &'static str,
    /// Contributor text in it is echoed.
    pub reason: String,
}

impl GuardIssue {
    fn new(check: &'static str, line: Option<u64>, reason: impl Into<String>) -> Self {
        GuardIssue {
            line: line.filter(|&l| l > 0),
            check,
            reason: reason.into(),
        }
    }
}

impl std::fmt::Display for GuardIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.line {
            Some(l) => write!(f, "{} (line {l}): {}", self.check, self.reason),
            None => write!(f, "{}: {}", self.check, self.reason),
        }
    }
}

/// What the guard learned about a model that passed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlInfo {
    /// Output column names, in order.
    pub columns: Vec<String>,
    /// The `solana.*` tables it reads.
    pub tables: BTreeSet<Table>,
    /// The statement has a top-level `ORDER BY`, so its row order is an
    /// output.
    pub ordered: bool,
    /// Every function it calls, lowercase.
    pub functions: BTreeSet<String>,
}

// ---------------------------------------------------------------------------
// Template
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Segment<'a> {
    Text(&'a str),
    From,
    To,
}

fn line_at(sql: &str, offset: usize) -> u64 {
    1 + sql.as_bytes()[..offset].iter().filter(|&&b| b == b'\n').count() as u64
}

/// A byte that may not touch a placeholder: FORGE's set.
const fn sticks_to_placeholder(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'\'' | b'"' | b'`' | b'.' | b'$')
}

/// `{{name}}` at `at`: its name (any identifier-shaped text) and the offset
/// just past `}}`.
fn placeholder_at(sql: &str, at: usize) -> Option<(&str, usize)> {
    let rest = sql.get(at..)?.strip_prefix("{{")?;
    let end = rest.find("}}")?;
    let name = &rest[..end];
    let mut b = name.bytes();
    let ok = matches!(b.next(), Some(c) if c.is_ascii_alphabetic() || c == b'_')
        && b.all(|c| c.is_ascii_alphanumeric() || c == b'_')
        && name.len() <= 64;
    ok.then_some((name, at + 2 + end + 2))
}

fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

/// Split `sql` at its placeholders, with FORGE's template rules and the
/// model rules on top (only the two window placeholders, both present, no
/// `$` in plain SQL).
fn template(sql: &str) -> Result<Vec<Segment<'_>>, GuardIssue> {
    let issue = |at: usize, reason: String| GuardIssue::new(CHECK_PLACEHOLDERS, Some(line_at(sql, at)), reason);
    if sql.trim().is_empty() {
        return Err(GuardIssue::new(CHECK_PARSE, None, "is blank"));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(GuardIssue::new(
            CHECK_PARSE,
            None,
            format!("is {} bytes, more than the limit of {MAX_SQL_BYTES}", sql.len()),
        ));
    }
    if let Some(at) = sql.find('\0') {
        return Err(issue(at, "contains a NUL byte".into()));
    }
    let bytes = sql.as_bytes();
    let mut segments = Vec::new();
    let mut text_start = 0;
    let mut terminated = false;
    let mut i = 0;
    let single = |at: usize| issue(at, "must be a single statement; nothing may follow a ';'".into());
    let no_placeholder_in = |start: usize, end: usize, what: &str| -> Result<(), GuardIssue> {
        let region = &sql[start..end];
        let mut from = 0;
        while let Some(n) = region[from..].find("{{") {
            if let Some((name, _)) = placeholder_at(region, from + n) {
                return Err(issue(
                    start + from + n,
                    format!("placeholder {{{{{}}}}} is inside a {what}; write it bare", echo(name)),
                ));
            }
            from += n + 2;
        }
        Ok(())
    };
    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();
        match b {
            b'\'' | b'"' | b'`' => {
                let what = match b {
                    b'\'' => "string literal",
                    _ => "quoted identifier",
                };
                let end = quoted_end(bytes, i, b).ok_or_else(|| issue(i, format!("has an unterminated {what}")))?;
                no_placeholder_in(i, end, what)?;
                if terminated {
                    return Err(single(i));
                }
                i = end;
            }
            b'-' if next == Some(b'-') => {
                let end = sql[i..].find('\n').map_or(bytes.len(), |n| i + n);
                no_placeholder_in(i, end, "comment")?;
                i = end;
            }
            b'/' if next == Some(b'*') => {
                let end = sql[i + 2..]
                    .find("*/")
                    .map(|n| i + 2 + n + 2)
                    .ok_or_else(|| issue(i, "has an unterminated block comment".into()))?;
                no_placeholder_in(i, end, "comment")?;
                i = end;
            }
            b';' => {
                if terminated {
                    return Err(single(i));
                }
                terminated = true;
                i += 1;
            }
            b'$' => {
                return Err(issue(
                    i,
                    "'$' is not allowed outside strings; the window is {{from_slot}} and {{to_slot}}".into(),
                ));
            }
            b'{' if next == Some(b'{') => {
                let Some((name, end)) = placeholder_at(sql, i) else {
                    return Err(issue(i, "has a malformed placeholder; write {{name}} with no spaces".into()));
                };
                if terminated {
                    return Err(single(i));
                }
                let seg = match name {
                    FROM_SLOT => Segment::From,
                    TO_SLOT => Segment::To,
                    other => {
                        return Err(issue(
                            i,
                            format!(
                                "placeholder {{{{{}}}}} is not allowed; models use only {{{{from_slot}}}} \
                                 and {{{{to_slot}}}}",
                                echo(other)
                            ),
                        ));
                    }
                };
                let before = i.checked_sub(1).map(|j| bytes[j]);
                let after = bytes.get(end).copied();
                if before.is_some_and(sticks_to_placeholder) || after.is_some_and(sticks_to_placeholder) {
                    return Err(issue(
                        i,
                        format!(
                            "placeholder {{{{{name}}}}} must be bare: separate it from identifiers, \
                             quotes, '.' and '$'"
                        ),
                    ));
                }
                if text_start < i {
                    segments.push(Segment::Text(&sql[text_start..i]));
                }
                segments.push(seg);
                text_start = end;
                i = end;
            }
            _ if b.is_ascii_whitespace() => i += 1,
            _ => {
                if terminated {
                    return Err(single(i));
                }
                i += 1;
            }
        }
    }
    if text_start < bytes.len() {
        segments.push(Segment::Text(&sql[text_start..]));
    }
    for (seg, name) in [(Segment::From, FROM_SLOT), (Segment::To, TO_SLOT)] {
        if !segments.contains(&seg) {
            return Err(GuardIssue::new(
                CHECK_PLACEHOLDERS,
                None,
                format!("must use {{{{{name}}}}}; a model reads exactly the window it is given"),
            ));
        }
    }
    Ok(segments)
}

fn join(segments: &[Segment<'_>], from: &str, to: &str) -> String {
    let mut out = String::new();
    for s in segments {
        match s {
            Segment::Text(t) => out.push_str(t),
            Segment::From => out.push_str(from),
            Segment::To => out.push_str(to),
        }
    }
    out
}

/// The statement the engine runs for the window `from..=to`: each
/// placeholder replaced by the plain decimal integer, nothing else changed.
pub fn render(sql: &str, from: u64, to: u64) -> Result<String, GuardIssue> {
    Ok(join(&template(sql)?, &from.to_string(), &to.to_string()))
}

// ---------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------

const FROM_MARK: &str = "$from_slot";
const TO_MARK: &str = "$to_slot";

/// Check `sql` against every rule (module docs). `Ok` describes a model
/// that passed all of them.
pub fn analyse(sql: &str) -> Result<SqlInfo, Vec<GuardIssue>> {
    let segments = template(sql).map_err(|i| vec![i])?;
    // The analysed text keeps every line where it was: the placeholders
    // become sqlparser placeholders, which nothing else can spell because
    // '$' is not allowed in plain SQL.
    let marked = join(&segments, FROM_MARK, TO_MARK);
    let statements = Parser::new(&GenericDialect {})
        .with_recursion_limit(PARSER_RECURSION_LIMIT)
        .try_with_sql(&marked)
        .and_then(|mut p| p.parse_statements())
        .map_err(|e| vec![parse_issue(&e)])?;
    let query = match statements.as_slice() {
        [Statement::Query(q)] => q,
        [] => return Err(vec![GuardIssue::new(CHECK_PARSE, None, "holds no statement")]),
        [other] => {
            return Err(vec![GuardIssue::new(
                CHECK_SHAPE,
                line(other.span()),
                "must be a SELECT query; DDL, DML and session statements are not allowed",
            )]);
        }
        _ => {
            return Err(vec![GuardIssue::new(CHECK_PARSE, None, "must be a single statement")]);
        }
    };

    let mut c = Collector::default();
    let _ = query.visit(&mut c);
    let columns = output_columns(query, &mut c.issues);
    if c.table_refs > MAX_TABLE_REFS {
        c.issues.push(GuardIssue::new(
            CHECK_SHAPE,
            None,
            format!("references {} tables, more than the limit of {MAX_TABLE_REFS}", c.table_refs),
        ));
    }
    if c.issues.is_empty() {
        Ok(SqlInfo {
            columns,
            tables: c.tables,
            ordered: query.order_by.is_some(),
            functions: c.functions,
        })
    } else {
        c.issues.sort();
        c.issues.dedup();
        Err(c.issues)
    }
}

fn parse_issue(e: &ParserError) -> GuardIssue {
    let msg = match e {
        ParserError::RecursionLimitExceeded => {
            format!("nests deeper than {PARSER_RECURSION_LIMIT} levels")
        }
        ParserError::TokenizerError(m) | ParserError::ParserError(m) => m.clone(),
    };
    // sqlparser ends messages with " at Line: N, Column: M".
    let line = msg
        .rsplit_once("Line: ")
        .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next()?.parse().ok());
    GuardIssue::new(CHECK_PARSE, line, format!("does not parse: {}", echo(&msg)))
}

fn line(span: sqlparser::tokenizer::Span) -> Option<u64> {
    Some(span.start.line)
}

/// `ident` as DataFusion resolves it: unquoted identifiers are lowercased.
fn norm(ident: &Ident) -> String {
    if ident.quote_style.is_none() {
        ident.value.to_ascii_lowercase()
    } else {
        ident.value.clone()
    }
}

fn idents(name: &ObjectName) -> Option<Vec<&Ident>> {
    name.0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i),
            ObjectNamePart::Function(_) => None,
        })
        .collect()
}

fn table_of(name: &str) -> Option<Table> {
    Table::ALL.into_iter().find(|t| t.as_str() == name)
}

/// A lowercase SQL identifier, as fixture columns are named.
fn is_column_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !s.as_bytes()[0].is_ascii_digit()
}

fn is_mark(e: &Expr, mark: &str) -> bool {
    matches!(e, Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) if p == mark)
}

fn shown(e: &impl std::fmt::Display) -> String {
    let s = e.to_string().replace(FROM_MARK, "{{from_slot}}").replace(TO_MARK, "{{to_slot}}");
    echo(&s.chars().take(80).collect::<String>())
}

/// A `solana.*` table in a FROM, and the names its columns are qualified by.
struct BaseTable {
    table: Table,
    qualifiers: Vec<String>,
    line: Option<u64>,
}

#[derive(Default)]
struct Collector {
    issues: Vec<GuardIssue>,
    ctes: BTreeSet<String>,
    tables: BTreeSet<Table>,
    functions: BTreeSet<String>,
    table_refs: usize,
}

impl Collector {
    fn push(&mut self, check: &'static str, line: Option<u64>, reason: impl Into<String>) {
        self.issues.push(GuardIssue::new(check, line, reason));
    }

    /// The `solana.*` tables directly in this FROM (not inside derived
    /// tables, which are queries of their own), with the qualifiers that
    /// name them, and whether the FROM has exactly one relation.
    fn base_tables(from: &[TableWithJoins]) -> (Vec<BaseTable>, bool) {
        fn walk(f: &TableFactor, out: &mut Vec<BaseTable>, count: &mut usize) {
            *count += 1;
            match f {
                TableFactor::Table { name, alias, .. } => {
                    let Some(parts) = idents(name) else { return };
                    if let [schema, table] = parts.as_slice()
                        && norm(schema) == SCHEMA
                        && let Some(t) = table_of(&norm(table))
                    {
                        let qualifiers = match alias {
                            Some(a) => vec![norm(&a.name)],
                            None => vec![t.as_str().to_string(), format!("{SCHEMA}.{}", t.as_str())],
                        };
                        out.push(BaseTable { table: t, qualifiers, line: line(f.span()) });
                    }
                }
                TableFactor::NestedJoin { table_with_joins, .. } => {
                    walk(&table_with_joins.relation, out, count);
                    for j in &table_with_joins.joins {
                        walk(&j.relation, out, count);
                    }
                }
                _ => {}
            }
        }
        let mut out = Vec::new();
        let mut count = 0;
        for twj in from {
            walk(&twj.relation, &mut out, &mut count);
            for j in &twj.joins {
                walk(&j.relation, &mut out, &mut count);
            }
        }
        (out, count == 1)
    }

    fn check_joins(&mut self, from: &[TableWithJoins]) {
        fn factor(c: &mut Collector, f: &TableFactor) {
            if let TableFactor::NestedJoin { table_with_joins, .. } = f {
                c.check_joins(std::slice::from_ref(table_with_joins));
            }
        }
        for twj in from {
            factor(self, &twj.relation);
            for j in &twj.joins {
                factor(self, &j.relation);
                let constraint = match &j.join_operator {
                    JoinOperator::Join(c)
                    | JoinOperator::Inner(c)
                    | JoinOperator::Left(c)
                    | JoinOperator::LeftOuter(c)
                    | JoinOperator::Right(c)
                    | JoinOperator::RightOuter(c)
                    | JoinOperator::FullOuter(c)
                    | JoinOperator::CrossJoin(c)
                    | JoinOperator::Semi(c)
                    | JoinOperator::LeftSemi(c)
                    | JoinOperator::RightSemi(c)
                    | JoinOperator::Anti(c)
                    | JoinOperator::LeftAnti(c)
                    | JoinOperator::RightAnti(c) => Some(c),
                    _ => None,
                };
                match constraint {
                    None => self.push(
                        CHECK_SHAPE,
                        line(j.relation.span()),
                        "only INNER, LEFT, RIGHT, FULL, CROSS, SEMI and ANTI joins are allowed",
                    ),
                    Some(JoinConstraint::Natural) => self.push(
                        CHECK_SHAPE,
                        line(j.relation.span()),
                        "NATURAL joins depend on column names; join ON or USING explicit columns",
                    ),
                    Some(_) => {}
                }
            }
        }
    }

    /// Every `solana.*` table in this SELECT's FROM is bounded by its WHERE.
    fn check_bounds(&mut self, s: &Select) {
        let (tables, single) = Self::base_tables(&s.from);
        if tables.is_empty() {
            return;
        }
        let mut conjuncts = Vec::new();
        if let Some(w) = &s.selection {
            split_and(w, &mut conjuncts);
        }
        for BaseTable { table: t, qualifiers: quals, line: at } in tables {
            let is_slot = |e: &Expr| match e {
                Expr::Identifier(i) => single && norm(i) == "slot",
                Expr::CompoundIdentifier(parts) => {
                    let names: Vec<String> = parts.iter().map(norm).collect();
                    match names.split_last() {
                        Some((last, q)) if last == "slot" => quals.contains(&q.join(".")),
                        _ => false,
                    }
                }
                _ => false,
            };
            let mut low = false;
            let mut high = false;
            for c in &conjuncts {
                match c {
                    Expr::Between { expr, negated: false, low: l, high: h } if is_slot(expr) => {
                        low |= is_mark(l, FROM_MARK);
                        high |= is_mark(h, TO_MARK);
                    }
                    Expr::BinaryOp { left, op, right } => {
                        use sqlparser::ast::BinaryOperator::{GtEq, LtEq};
                        match op {
                            GtEq if is_slot(left) && is_mark(right, FROM_MARK) => low = true,
                            LtEq if is_mark(left, FROM_MARK) && is_slot(right) => low = true,
                            LtEq if is_slot(left) && is_mark(right, TO_MARK) => high = true,
                            GtEq if is_mark(left, TO_MARK) && is_slot(right) => high = true,
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            if !(low && high) {
                let q = &quals[0];
                let missing = match (low, high) {
                    (false, false) => "both bounds",
                    (false, true) => "the lower bound",
                    _ => "the upper bound",
                };
                self.push(
                    CHECK_BOUNDS,
                    at,
                    format!(
                        "{SCHEMA}.{} is missing {missing}: the WHERE of the SELECT that reads it needs \
                         `{q}.slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}}` as a top-level AND condition",
                        t.as_str()
                    ),
                );
            }
        }
    }

    fn check_function(&mut self, e: &Expr, f: &sqlparser::ast::Function) {
        let at = line(e.span());
        let name = match idents(&f.name).as_deref() {
            Some([one]) => norm(one),
            _ => {
                self.push(CHECK_FUNCTIONS, at, format!("function {} is not allowed", shown(&f.name)));
                return;
            }
        };
        if !ALLOWED_FUNCTIONS.contains(&name.as_str()) {
            let why = match name.as_str() {
                "now" | "current_date" | "current_time" | "current_timestamp" | "localtime"
                | "localtimestamp" | "random" | "uuid" => " (not deterministic)",
                "avg" | "stddev" | "variance" | "sqrt" | "power" | "pow" | "ln" | "log" | "exp"
                | "round" | "percent_rank" | "cume_dist" => " (float result)",
                "array_agg" | "string_agg" => " (order of the result depends on input order)",
                _ => "",
            };
            self.push(CHECK_FUNCTIONS, at, format!("function {}{why} is not allowed", echo(&name)));
        }
        self.functions.insert(name.clone());
        if !matches!(f.parameters, FunctionArguments::None) || !f.within_group.is_empty() || f.uses_odbc_syntax {
            self.push(CHECK_SHAPE, at, format!("{}: parameters, WITHIN GROUP and ODBC syntax are not allowed", echo(&name)));
        }
        match &f.args {
            FunctionArguments::List(list) => {
                if !list.clauses.is_empty() {
                    self.push(CHECK_SHAPE, at, format!("{}: clauses inside the argument list are not allowed", echo(&name)));
                }
                for a in &list.args {
                    match a {
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(_)) => {}
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) if name == "count" => {}
                        _ => self.push(
                            CHECK_SHAPE,
                            at,
                            format!("{}: only plain arguments are allowed (and * only in count(*))", echo(&name)),
                        ),
                    }
                }
            }
            FunctionArguments::None => {}
            FunctionArguments::Subquery(_) => {
                self.push(CHECK_SHAPE, at, format!("{}: a subquery argument needs parentheses", echo(&name)));
            }
        }
        match &f.over {
            Some(WindowType::WindowSpec(spec)) => {
                if spec.window_name.is_some() {
                    self.push(CHECK_SHAPE, at, "named windows are not allowed");
                }
                if ORDER_DEPENDENT.contains(&name.as_str()) && spec.order_by.is_empty() {
                    self.push(
                        CHECK_FUNCTIONS,
                        at,
                        format!("{} depends on row order: its OVER needs ORDER BY", echo(&name)),
                    );
                }
            }
            Some(WindowType::NamedWindow(_)) => self.push(CHECK_SHAPE, at, "named windows are not allowed"),
            None if ORDER_DEPENDENT.contains(&name.as_str()) => self.push(
                CHECK_FUNCTIONS,
                at,
                format!("{} is a window function and needs OVER (... ORDER BY ...)", echo(&name)),
            ),
            None => {}
        }
    }
}

fn split_and<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::BinaryOp { left, op: sqlparser::ast::BinaryOperator::And, right } => {
            split_and(left, out);
            split_and(right, out);
        }
        Expr::Nested(inner) => split_and(inner, out),
        other => out.push(other),
    }
}

impl Visitor for Collector {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        let at = line(q.span());
        if let Some(w) = &q.with {
            if w.recursive {
                self.push(CHECK_SHAPE, at, "WITH RECURSIVE is not allowed");
            }
            for cte in &w.cte_tables {
                let name = norm(&cte.alias.name);
                if name == SCHEMA || table_of(&name).is_some() {
                    self.push(
                        CHECK_TABLES,
                        at,
                        format!("CTE '{}' would shadow a {SCHEMA} table", echo(&name)),
                    );
                }
                if cte.from.is_some() || cte.materialized.is_some() {
                    self.push(CHECK_SHAPE, at, "CTE options are not allowed");
                }
                self.ctes.insert(name);
            }
        }
        if q.fetch.is_some()
            || !q.locks.is_empty()
            || q.for_clause.is_some()
            || q.settings.is_some()
            || q.format_clause.is_some()
            || !q.pipe_operators.is_empty()
        {
            self.push(CHECK_SHAPE, at, "FETCH, FOR, SETTINGS, FORMAT, locks and pipe operators are not allowed");
        }
        if let Some(ob) = &q.order_by
            && let OrderByKind::All(_) = ob.kind
        {
            self.push(CHECK_SHAPE, at, "ORDER BY ALL is not allowed; name the sort keys");
        }
        if let Some(limit) = &q.limit_clause {
            if q.order_by.is_none() {
                self.push(
                    CHECK_SHAPE,
                    at,
                    "LIMIT and OFFSET without ORDER BY pick arbitrary rows; add an ORDER BY",
                );
            }
            let int = |e: &Expr| {
                matches!(e, Expr::Value(ValueWithSpan { value: Value::Number(n, _), .. })
                    if n.bytes().all(|b| b.is_ascii_digit()))
            };
            match limit {
                LimitClause::LimitOffset { limit, offset, limit_by } => {
                    let ok = limit.as_ref().is_none_or(int)
                        && offset.as_ref().is_none_or(|o| int(&o.value))
                        && limit_by.is_empty();
                    if !ok {
                        self.push(CHECK_SHAPE, at, "LIMIT and OFFSET must be integer literals (no LIMIT BY)");
                    }
                }
                LimitClause::OffsetCommaLimit { .. } => {
                    self.push(CHECK_SHAPE, at, "LIMIT a, b is not allowed; write LIMIT b OFFSET a");
                }
            }
        }
        match q.body.as_ref() {
            SetExpr::Select(_) | SetExpr::Query(_) | SetExpr::SetOperation { .. } => {}
            _ => self.push(CHECK_SHAPE, at, "the query body must be SELECT (VALUES, TABLE and DML are not allowed)"),
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        let at = line(s.span());
        let mut bad = Vec::new();
        if matches!(s.distinct, Some(Distinct::On(_))) {
            bad.push("DISTINCT ON");
        }
        if s.top.is_some() {
            bad.push("TOP");
        }
        if s.into.is_some() {
            bad.push("INTO");
        }
        if s.exclude.is_some() || s.select_modifiers.is_some() || !s.optimizer_hints.is_empty() {
            bad.push("select modifiers and hints");
        }
        if !s.lateral_views.is_empty() || s.prewhere.is_some() || !s.connect_by.is_empty() {
            bad.push("LATERAL VIEW, PREWHERE and CONNECT BY");
        }
        if !s.cluster_by.is_empty() || !s.distribute_by.is_empty() || !s.sort_by.is_empty() {
            bad.push("CLUSTER, DISTRIBUTE and SORT BY");
        }
        if s.qualify.is_some() {
            bad.push("QUALIFY");
        }
        if !s.named_window.is_empty() {
            bad.push("named windows");
        }
        if s.value_table_mode.is_some() || !matches!(s.flavor, SelectFlavor::Standard) {
            bad.push("this SELECT form");
        }
        match &s.group_by {
            GroupByExpr::All(m) | GroupByExpr::Expressions(_, m) if !m.is_empty() => bad.push("GROUP BY modifiers"),
            _ => {}
        }
        for b in bad {
            self.push(CHECK_SHAPE, at, format!("{b} is not allowed"));
        }
        for item in &s.projection {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => self.push(
                    CHECK_SHAPE,
                    at,
                    "* is not allowed: its columns change whenever a table's do; list them",
                ),
                SelectItem::ExprWithAliases { .. } => self.push(CHECK_SHAPE, at, "multiple aliases are not allowed"),
                _ => {}
            }
        }
        self.check_joins(&s.from);
        self.check_bounds(s);
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, f: &TableFactor) -> ControlFlow<()> {
        let at = line(f.span());
        self.table_refs += 1;
        match f {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                if args.is_some() {
                    self.push(CHECK_TABLES, at, format!("table function {} is not allowed", shown(name)));
                    return ControlFlow::Continue(());
                }
                if !with_hints.is_empty()
                    || version.is_some()
                    || *with_ordinality
                    || !partitions.is_empty()
                    || json_path.is_some()
                    || sample.is_some()
                    || !index_hints.is_empty()
                {
                    self.push(CHECK_TABLES, at, format!("{}: table hints, versions and samples are not allowed", shown(name)));
                }
                let parts = idents(name).unwrap_or_default();
                let names: Vec<String> = parts.iter().map(|i| norm(i)).collect();
                match names.as_slice() {
                    [one] if self.ctes.contains(one) => {}
                    [schema, table] if schema == SCHEMA && table_of(table).is_some() => {
                        self.tables.insert(table_of(table).expect("checked"));
                        if alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
                            self.push(
                                CHECK_SHAPE,
                                at,
                                format!("{}: column-renaming aliases are not allowed on {SCHEMA} tables", shown(name)),
                            );
                        }
                    }
                    _ => self.push(
                        CHECK_TABLES,
                        at,
                        format!(
                            "table {} is not allowed; a model reads only {SCHEMA}.slots, \
                             {SCHEMA}.transactions, {SCHEMA}.instructions, {SCHEMA}.token_transfers \
                             and its own CTEs",
                            shown(name)
                        ),
                    ),
                }
            }
            TableFactor::Derived { lateral, .. } => {
                if *lateral {
                    self.push(CHECK_SHAPE, at, "LATERAL is not allowed");
                }
            }
            TableFactor::NestedJoin { .. } => {}
            other => self.push(CHECK_TABLES, at, format!("{} is not allowed as a table", shown(other))),
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        let at = line(e.span());
        match e {
            Expr::Function(f) => self.check_function(e, f),
            Expr::Cast { kind, data_type, array, format, .. } => {
                let t = data_type.to_string().to_ascii_uppercase();
                if t.contains("FLOAT") || t.contains("REAL") || t.contains("DOUBLE") {
                    self.push(CHECK_TYPES, at, format!("cast to {} is not allowed: models are exact, no floats", echo(&t)));
                }
                if *array || format.is_some() || !matches!(kind, CastKind::Cast | CastKind::DoubleColon | CastKind::TryCast) {
                    self.push(CHECK_SHAPE, at, format!("{} is not allowed", shown(e)));
                }
            }
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsTrue(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsDistinctFrom(..)
            | Expr::IsNotDistinctFrom(..)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Between { .. }
            | Expr::BinaryOp { .. }
            | Expr::Like { .. }
            | Expr::ILike { .. }
            | Expr::UnaryOp { .. }
            | Expr::Extract { .. }
            | Expr::Ceil { .. }
            | Expr::Floor { .. }
            | Expr::Position { .. }
            | Expr::Substring { .. }
            | Expr::Trim { .. }
            | Expr::Nested(_)
            | Expr::Value(_)
            | Expr::TypedString(_)
            | Expr::Case { .. }
            | Expr::Exists { .. }
            | Expr::Subquery(_)
            | Expr::Tuple(_)
            | Expr::Interval(_) => {}
            other => self.push(CHECK_SHAPE, at, format!("expression {} is not allowed", shown(other))),
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, v: &ValueWithSpan) -> ControlFlow<()> {
        let at = line(v.span);
        match &v.value {
            Value::Number(n, _) if n.bytes().all(|b| b.is_ascii_digit()) => {}
            Value::Number(n, _) => self.push(
                CHECK_TYPES,
                at,
                format!("number {} is not an integer: models are exact, no floats", echo(n)),
            ),
            Value::Placeholder(p) if p == FROM_MARK || p == TO_MARK => {}
            Value::Placeholder(p) => self.push(
                CHECK_PLACEHOLDERS,
                at,
                format!("placeholder {} is not allowed", echo(p)),
            ),
            Value::SingleQuotedString(_)
            | Value::HexStringLiteral(_)
            | Value::Boolean(_)
            | Value::Null => {}
            other => self.push(
                CHECK_SHAPE,
                at,
                format!("literal {} is not allowed; use a plain '...' string", echo(&other.to_string())),
            ),
        }
        ControlFlow::Continue(())
    }
}

/// The names of the output columns, from the leftmost SELECT of the
/// outermost query (that is where DataFusion takes them from).
fn output_columns(q: &Query, issues: &mut Vec<GuardIssue>) -> Vec<String> {
    fn leftmost(body: &SetExpr) -> Option<&Select> {
        match body {
            SetExpr::Select(s) => Some(s),
            SetExpr::Query(q) => leftmost(&q.body),
            SetExpr::SetOperation { left, .. } => leftmost(left),
            _ => None,
        }
    }
    let Some(select) = leftmost(&q.body) else { return Vec::new() };
    let mut out: Vec<String> = Vec::new();
    for (i, item) in select.projection.iter().enumerate() {
        let at = line(item.span());
        let name = match item {
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(id),
            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => parts.last(),
            SelectItem::ExprWithAlias { alias, .. } => Some(alias),
            SelectItem::UnnamedExpr(e) => {
                issues.push(GuardIssue::new(
                    CHECK_SHAPE,
                    at,
                    format!("output column {} ({}) has no name; write `<expr> AS <name>`", i + 1, shown(e)),
                ));
                None
            }
            _ => None,
        };
        let Some(ident) = name else { continue };
        let n = ident.value.clone();
        if !is_column_name(&n) {
            issues.push(GuardIssue::new(
                CHECK_SHAPE,
                at,
                format!(
                    "output column '{}' is not a lowercase identifier ([a-z_][a-z0-9_]*, at most 64)",
                    echo(&n)
                ),
            ));
        } else if out.contains(&n) {
            issues.push(GuardIssue::new(CHECK_SHAPE, at, format!("output column '{n}' appears twice")));
        }
        out.push(n);
    }
    if out.len() > MAX_OUTPUT_COLUMNS {
        issues.push(GuardIssue::new(
            CHECK_SHAPE,
            None,
            format!("has {} output columns, more than the limit of {MAX_OUTPUT_COLUMNS}", out.len()),
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const GOOD: &str = "SELECT i.program_id, COUNT(*) AS n\n\
                        FROM solana.instructions AS i\n\
                        WHERE i.slot BETWEEN {{from_slot}} AND {{to_slot}}\n\
                        GROUP BY i.program_id\n";

    #[track_caller]
    fn ok(sql: &str) -> SqlInfo {
        analyse(sql).unwrap_or_else(|e| panic!("{sql}\n{e:#?}"))
    }

    #[track_caller]
    fn bad(sql: &str, check: &str, part: &str) -> Vec<GuardIssue> {
        let issues = analyse(sql).expect_err(sql);
        assert!(
            issues.iter().any(|i| i.check == check && i.reason.contains(part)),
            "{sql}\nexpected {check} containing {part:?}, got {issues:#?}"
        );
        issues
    }

    #[test]
    fn a_good_model_passes_and_is_described() {
        let info = ok(GOOD);
        assert_eq!(info.columns, ["program_id", "n"]);
        assert_eq!(info.tables.iter().copied().collect::<Vec<_>>(), [Table::Instructions]);
        assert!(!info.ordered);
        assert_eq!(info.functions.iter().map(String::as_str).collect::<Vec<_>>(), ["count"]);
        let info = ok(&format!("{}ORDER BY n DESC, program_id;\n", GOOD));
        assert!(info.ordered);
    }

    #[test]
    fn render_writes_plain_integers_and_nothing_else() {
        assert_eq!(
            render(GOOD, 10, u64::MAX).unwrap(),
            GOOD.replace("{{from_slot}}", "10").replace("{{to_slot}}", "18446744073709551615")
        );
        assert!(render("SELECT 1", 0, 0).is_err());
    }

    #[test]
    fn placeholder_convention_is_forges() {
        for (sql, part) in [
            ("SELECT slot FROM solana.slots WHERE slot <= {{to_slot}}", "must use {{from_slot}}"),
            ("SELECT slot FROM solana.slots WHERE slot >= {{from_slot}}", "must use {{to_slot}}"),
            ("SELECT {{wallet}}, {{from_slot}}, {{to_slot}}", "{{wallet}} is not allowed"),
            ("SELECT '{{from_slot}}', {{to_slot}}", "inside a string literal"),
            ("SELECT \"{{from_slot}}\", {{to_slot}}", "inside a quoted identifier"),
            ("SELECT {{to_slot}} -- {{from_slot}}", "inside a comment"),
            ("SELECT {{to_slot}} /* {{from_slot}} */", "inside a comment"),
            ("SELECT col{{from_slot}}, {{to_slot}}", "must be bare"),
            ("SELECT t.{{from_slot}}, {{to_slot}}", "must be bare"),
            ("SELECT ${{from_slot}}, {{to_slot}}", "'$' is not allowed"),
            ("SELECT $1, {{from_slot}}, {{to_slot}}", "'$' is not allowed"),
            ("SELECT {{ from_slot }}, {{to_slot}}", "malformed placeholder"),
            ("SELECT {{from_slot}}, {{to_slot}}; SELECT 1", "single statement"),
            ("SELECT {{from_slot}}, {{to_slot}};;", "single statement"),
            ("SELECT 'open, {{from_slot}}, {{to_slot}}", "unterminated string"),
        ] {
            let e = analyse(sql).expect_err(sql);
            assert_eq!(e.len(), 1, "{sql}: {e:?}");
            assert_eq!(e[0].check, CHECK_PLACEHOLDERS, "{sql}: {e:?}");
            assert!(e[0].reason.contains(part), "{sql}: expected {part:?} in {e:?}");
        }
        // One trailing ';' and a '$' inside a string are fine.
        ok("SELECT s.slot FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}} AND s.leader <> '$x';\n");
    }

    #[test]
    fn only_one_select_query() {
        bad("DELETE FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", CHECK_SHAPE, "must be a SELECT");
        bad("CREATE TABLE x AS SELECT {{from_slot}}, {{to_slot}}", CHECK_SHAPE, "must be a SELECT");
        bad("VALUES ({{from_slot}}, {{to_slot}})", CHECK_SHAPE, "query body must be SELECT");
        bad("SELEC {{from_slot}}, {{to_slot}}", CHECK_PARSE, "does not parse");
        let deep = format!("SELECT {}{{{{from_slot}}}}{} AS x, {{{{to_slot}}}} AS y", "(".repeat(200), ")".repeat(200));
        bad(&deep, CHECK_PARSE, "nests deeper");
    }

    #[test]
    fn tables_are_the_four_and_ctes() {
        let info = ok("WITH t AS (SELECT x.slot, x.fee FROM solana.transactions x \
                       WHERE x.slot >= {{from_slot}} AND x.slot <= {{to_slot}})\n\
                       SELECT t.slot, t.fee FROM t");
        assert_eq!(info.tables.iter().copied().collect::<Vec<_>>(), [Table::Transactions]);
        for (sql, part) in [
            ("SELECT slot FROM slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "table slots is not allowed"),
            ("SELECT slot FROM public.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "public.slots"),
            ("SELECT slot FROM solana.accounts WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "solana.accounts"),
            ("SELECT table_name AS t FROM information_schema.tables WHERE {{from_slot}} < {{to_slot}}", "information_schema"),
            ("SELECT x AS x FROM read_parquet('/etc') WHERE {{from_slot}} < {{to_slot}}", "table function"),
            ("WITH slots AS (SELECT 1 AS a) SELECT a FROM slots WHERE {{from_slot}} < {{to_slot}}", "shadow"),
        ] {
            bad(sql, CHECK_TABLES, part);
        }
    }

    #[test]
    fn every_table_reference_is_bounded_in_its_own_where() {
        // Unqualified slot is fine for a single relation.
        ok("SELECT slot FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}");
        ok("SELECT slot FROM solana.slots WHERE {{from_slot}} <= slot AND {{to_slot}} >= slot AND leader IS NULL");
        ok("SELECT slots.slot FROM solana.slots WHERE (solana.slots.slot >= {{from_slot}}) AND slots.slot <= {{to_slot}}");
        // A join needs both, each by its own name.
        ok("SELECT t.slot, i.program_id FROM solana.transactions t JOIN solana.instructions i \
            ON i.slot = t.slot AND i.tx_signature = t.tx_signature \
            WHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}} AND i.slot BETWEEN {{from_slot}} AND {{to_slot}}");
        for (sql, part) in [
            ("SELECT slot, {{from_slot}} AS a, {{to_slot}} AS b FROM solana.slots", "missing both bounds"),
            ("SELECT slot FROM solana.slots WHERE slot >= {{from_slot}} AND {{to_slot}} > 0", "missing the upper bound"),
            ("SELECT slot FROM solana.slots WHERE slot BETWEEN {{to_slot}} AND {{from_slot}}", "missing both bounds"),
            ("SELECT slot FROM solana.slots WHERE slot NOT BETWEEN {{from_slot}} AND {{to_slot}}", "missing both"),
            ("SELECT slot FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}} OR leader IS NULL", "missing both"),
            ("SELECT slot FROM solana.slots WHERE slot > {{from_slot}} AND slot <= {{to_slot}}", "missing the lower"),
            ("SELECT slot FROM solana.slots WHERE parent_slot BETWEEN {{from_slot}} AND {{to_slot}}", "missing both"),
            // The bound is on the wrong table.
            ("SELECT t.slot FROM solana.transactions t JOIN solana.instructions i ON i.slot = t.slot \
              WHERE t.slot BETWEEN {{from_slot}} AND {{to_slot}}", "solana.instructions is missing"),
            // Unqualified slot is ambiguous with two relations.
            ("SELECT t.slot FROM solana.transactions t, solana.slots s WHERE slot BETWEEN {{from_slot}} AND {{to_slot}} \
              AND s.slot BETWEEN {{from_slot}} AND {{to_slot}}", "solana.transactions is missing"),
            // A bound in an outer query does not bound the subquery.
            ("SELECT x.slot FROM (SELECT slot FROM solana.slots) x WHERE x.slot BETWEEN {{from_slot}} AND {{to_slot}}", "solana.slots is missing"),
            // Nor in ON.
            ("SELECT s.slot FROM solana.slots s JOIN solana.transactions t ON t.slot BETWEEN {{from_slot}} AND {{to_slot}} \
              WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}}", "solana.transactions is missing"),
            // Subqueries in WHERE are queries of their own.
            ("SELECT s.slot FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}} \
              AND EXISTS (SELECT 1 FROM solana.transactions t WHERE t.slot = s.slot)", "solana.transactions is missing"),
            // An alias hides the table name.
            ("SELECT s.slot FROM solana.slots s WHERE slots.slot BETWEEN {{from_slot}} AND {{to_slot}}", "missing both"),
        ] {
            bad(sql, CHECK_BOUNDS, part);
        }
    }

    #[test]
    fn functions_are_allowlisted() {
        ok("SELECT s.slot, ROW_NUMBER() OVER (ORDER BY s.slot) AS rn, coalesce(s.leader, '') AS l \
            FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}}");
        for (sql, part) in [
            ("SELECT now() AS t, {{from_slot}} AS a, {{to_slot}} AS b", "now (not deterministic)"),
            ("SELECT CURRENT_TIMESTAMP AS t, {{from_slot}} AS a, {{to_slot}} AS b", "current_timestamp"),
            ("SELECT random() AS r, {{from_slot}} AS a, {{to_slot}} AS b", "random"),
            ("SELECT avg(fee) AS f FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "float result"),
            ("SELECT array_agg(fee) AS f FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "input order"),
            ("SELECT s.f() AS x, {{from_slot}} AS a, {{to_slot}} AS b FROM solana.slots s WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}}", "s.f"),
            ("SELECT slot, row_number() OVER () AS rn FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "needs ORDER BY"),
            ("SELECT slot, lag(slot) AS p FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "needs OVER"),
        ] {
            bad(sql, CHECK_FUNCTIONS, part);
        }
    }

    #[test]
    fn no_floats_anywhere() {
        for (sql, part) in [
            ("SELECT slot * 1.5 AS x FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "1.5"),
            ("SELECT 1e3 AS x, {{from_slot}} AS a, {{to_slot}} AS b", "1e3"),
            ("SELECT CAST(fee AS DOUBLE) AS x FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "DOUBLE"),
            ("SELECT fee::float AS x FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "FLOAT"),
            ("SELECT CAST(fee AS REAL) AS x FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}", "REAL"),
        ] {
            bad(sql, CHECK_TYPES, part);
        }
        ok("SELECT CAST(fee AS BIGINT) AS x, fee / 2 AS h FROM solana.transactions WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}");
    }

    #[test]
    fn shape_rules() {
        let w = "FROM solana.slots WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";
        for (sql, part) in [
            (format!("SELECT * {w}"), "* is not allowed"),
            (format!("SELECT slots.* {w}"), "* is not allowed"),
            (format!("SELECT slot + 1 {w}"), "has no name"),
            (format!("SELECT slot AS Slot {w}"), "not a lowercase identifier"),
            (format!("SELECT slot, leader AS slot {w}"), "appears twice"),
            (format!("SELECT slot {w} LIMIT 5"), "without ORDER BY"),
            (format!("SELECT slot {w} ORDER BY slot LIMIT {{{{to_slot}}}}"), "integer literals"),
            (format!("SELECT DISTINCT ON (leader) slot {w}"), "DISTINCT ON"),
            (format!("WITH RECURSIVE r AS (SELECT 1 AS a) SELECT slot {w}"), "RECURSIVE"),
            ("SELECT a.slot FROM solana.slots a NATURAL JOIN solana.slots b WHERE a.slot BETWEEN {{from_slot}} AND {{to_slot}} AND b.slot BETWEEN {{from_slot}} AND {{to_slot}}".to_string(), "NATURAL"),
            ("SELECT s.slot FROM solana.slots AS s(slot2, slot) WHERE s.slot BETWEEN {{from_slot}} AND {{to_slot}}".to_string(), "column-renaming"),
            (format!("SELECT slot, ARRAY[1, 2] AS a {w}"), "expression"),
        ] {
            bad(&sql, CHECK_SHAPE, part);
        }
        ok(&format!("SELECT slot {w} ORDER BY slot LIMIT 5 OFFSET 2"));
        ok(&format!("SELECT slot {w} UNION ALL SELECT slot {w}"));
        let many: Vec<String> = (0..9).map(|i| format!("SELECT slot AS s{i} {w}")).collect();
        let union = many.join(" UNION ALL ");
        bad(&union, CHECK_SHAPE, "references 9 tables");
    }

    #[test]
    fn issues_are_all_reported_sorted_with_lines() {
        let sql = "SELECT now() AS t,\n  1.5 AS f\nFROM solana.slots\nWHERE {{from_slot}} < {{to_slot}}\n";
        let e = analyse(sql).unwrap_err();
        let summary: Vec<(Option<u64>, &str)> = e.iter().map(|i| (i.line, i.check)).collect();
        // The bounds issue is on the line of the table it is about.
        assert_eq!(
            summary,
            [(Some(1), CHECK_FUNCTIONS), (Some(2), CHECK_TYPES), (Some(3), CHECK_BOUNDS)]
        );
        assert_eq!(analyse(sql).unwrap_err(), e, "deterministic");
    }

    #[test]
    fn contributor_text_in_issues_is_echoed() {
        let e = analyse("SELECT f\u{202E}x() AS a, {{from_slot}} AS b, {{to_slot}} AS c").unwrap_err();
        assert!(e.iter().all(|i| !i.reason.contains('\u{202E}')), "{e:?}");
    }
}
