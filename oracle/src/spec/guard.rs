//! Validation: a client's [`QuerySpec`] against the catalog and the limits.
//!
//! [`validate`] is pure and stateless: no LENS call, no snapshot, no clock.
//! It either returns a [`ValidSpec`], the normalised form the compiler
//! works from, or the **first** rule that failed as a
//! [`SpecError::Invalid`] naming the offending part of the spec by path
//! (`filters[1].values[0]`).
//!
//! # Rules, in the order they are checked
//!
//! | # | Field                       | Rule                                                                    |
//! |---|-----------------------------|-------------------------------------------------------------------------|
//! | 1 | `measures`                  | 1 to [`MAX_MEASURES`]                                                   |
//! | 2 | `measures[i]`               | a catalog measure, not repeated, on the same table as `measures[0]`     |
//! | 3 | `group_by`                  | at most [`MAX_GROUP_BY`]                                                |
//! | 4 | `group_by[i].dimension`     | a catalog dimension, on the query's table, not repeated, not a measure's name |
//! | 5 | `group_by[i].bucket_width`  | given exactly when the dimension takes one, and in range               |
//! | 6 | `filters`                   | at most [`MAX_FILTERS`]                                                 |
//! | 7 | `filters[i].dimension`      | a catalog dimension, on the table, filterable, filtered by no other entry |
//! | 8 | `filters[i].values`         | `eq`: exactly one; `in`: 1 to [`MAX_FILTER_VALUES`]                     |
//! | 9 | `filters[i].values[j]`      | passes the dimension's [`ValueRule`], not repeated                      |
//! |10 | `measures[i]`               | a required dimension is grouped by, or filtered to exactly one value   |
//! |11 | `window.*`                  | `last_slots` in `1..=max_slot_span`; a range within `0..=MAX_SLOT`, `from <= to`, at most `max_slot_span` slots |
//! |12 | `group_by[i].bucket_width`  | the window always holds at least one whole bucket (see below)          |
//! |13 | `order_by`, `order_by[i].target` | at most [`MAX_ORDER_BY`]; each a requested measure or group-by dimension, not repeated |
//! |14 | `limit`                     | `1..=max_rows`; absent means `max_rows`                                 |
//! |15 | `as_of_slot`                | at most `MAX_SLOT`                                                      |
//!
//! Rule 12: grouping by `slot_bucket` snaps the window inward to whole
//! buckets. For a `range` the snap is known now, and must leave at least one
//! bucket. For `last_slots` it depends on the snapshot, so the window must
//! be at least `2 * width - 1` slots: the shortest that holds a whole bucket
//! however it is aligned.
//!
//! # Errors never quote much
//!
//! A reason repeats at most [`MAX_ECHO_CHARS`] characters of an offending
//! value, Rust-escaped (see [`echo`]). Everything else in it comes from the
//! catalog or is a number.
//!
//! # Normal form
//!
//! [`ValidSpec`] holds catalog ids (`&'static str` from the definitions,
//! never the client's copy), order targets as indices, the limit filled
//! in, and filters sorted by dimension id with their values sorted and
//! distinct, so `eq` and a one-value `in` are the same filter. Two specs
//! that mean the same compile to the same statement.
//!
//! [`ValueRule`]: crate::catalog::ValueRule

use crate::catalog::{Catalog, MAX_SLOT, Measure, Table};
use crate::config::{DEFAULT_MAX_ROWS, DEFAULT_MAX_SLOT_SPAN, OracleConfig};

use super::{Direction, FilterOp, QuerySpec, SlotWindow, SpecError, Window};

/// Most measures in one spec.
pub const MAX_MEASURES: usize = 8;
/// Most group-by dimensions in one spec.
pub const MAX_GROUP_BY: usize = 4;
/// Most filters in one spec.
pub const MAX_FILTERS: usize = 8;
/// Most values in one `in` filter.
pub const MAX_FILTER_VALUES: usize = 64;
/// Most sort keys: every target is a distinct measure or group-by dimension.
pub const MAX_ORDER_BY: usize = MAX_MEASURES + MAX_GROUP_BY;
/// Most characters of a client's value an error repeats.
pub const MAX_ECHO_CHARS: usize = 32;

/// The configured limits a spec is checked against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpecLimits {
    /// Most rows an answer may hold: `ORACLE_MAX_ROWS`.
    pub max_rows: u32,
    /// Most slots a window may cover: `ORACLE_MAX_SLOT_SPAN`.
    pub max_slot_span: u64,
}

impl SpecLimits {
    pub const fn from_config(config: &OracleConfig) -> Self {
        Self { max_rows: config.max_rows(), max_slot_span: config.max_slot_span() }
    }
}

impl Default for SpecLimits {
    /// The config defaults.
    fn default() -> Self {
        Self { max_rows: DEFAULT_MAX_ROWS, max_slot_span: DEFAULT_MAX_SLOT_SPAN }
    }
}

// ---------------------------------------------------------------------------
// The normal form
// ---------------------------------------------------------------------------

/// A spec that passed [`validate`]. Every id is the catalog's own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidSpec {
    /// The one table every measure reads.
    pub table: Table,
    /// Measure ids, in the order requested.
    pub measures: Vec<&'static str>,
    /// In the order requested.
    pub group_by: Vec<ValidGroupBy>,
    /// Sorted by dimension id; at most one per dimension.
    pub filters: Vec<ValidFilter>,
    pub window: Window,
    /// In the order requested.
    pub order_by: Vec<ValidOrder>,
    /// Rows the answer may hold.
    pub limit: u32,
    pub as_of_slot: Option<u64>,
}

impl ValidSpec {
    /// The `slot_bucket` width, if the spec groups by it.
    pub fn bucket_width(&self) -> Option<u64> {
        self.group_by.iter().find_map(|g| g.bucket_width)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidGroupBy {
    pub dimension: &'static str,
    /// Checked by [`Dimension::check_param`](crate::catalog::Dimension::check_param).
    pub bucket_width: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidFilter {
    pub dimension: &'static str,
    /// Each passed the dimension's value rule. Sorted, distinct, at least one.
    pub values: Vec<String>,
}

/// What an [`order_by`](QuerySpec::order_by) entry sorts by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderKey {
    /// Index into [`ValidSpec::measures`].
    Measure(usize),
    /// Index into [`ValidSpec::group_by`].
    GroupBy(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidOrder {
    pub key: OrderKey,
    pub direction: Direction,
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// `value` as an error may show it: at most [`MAX_ECHO_CHARS`] characters,
/// quoted and escaped, with `…` if cut.
pub fn echo(value: &str) -> String {
    let mut chars = value.chars();
    let head: String = chars.by_ref().take(MAX_ECHO_CHARS).collect();
    if chars.next().is_some() { format!("{head:?}…") } else { format!("{head:?}") }
}

fn invalid(field: impl Into<String>, reason: impl Into<String>) -> SpecError {
    SpecError::invalid(field, reason)
}

/// Check `spec` against `catalog` and `limits`. See the [module docs](self)
/// for the rules and their order.
pub fn validate(spec: &QuerySpec, catalog: &Catalog, limits: &SpecLimits) -> Result<ValidSpec, SpecError> {
    // --- measures ---
    if spec.measures.is_empty() {
        return Err(invalid("measures", "at least one measure is required"));
    }
    if spec.measures.len() > MAX_MEASURES {
        return Err(invalid("measures", format!("{} measures, at most {MAX_MEASURES}", spec.measures.len())));
    }
    let mut measures: Vec<&Measure> = Vec::with_capacity(spec.measures.len());
    for (i, id) in spec.measures.iter().enumerate() {
        let field = format!("measures[{i}]");
        let m = catalog.measure(id).ok_or_else(|| invalid(&field, format!("unknown measure {}", echo(id))))?;
        if measures.iter().any(|p| p.id == m.id) {
            return Err(invalid(field, format!("measure {} is requested twice", m.id)));
        }
        if let Some(first) = measures.first()
            && first.table != m.table
        {
            return Err(invalid(
                field,
                format!(
                    "measure {} reads {}, but {} reads {}: a query reads one table",
                    m.id, m.table, first.id, first.table
                ),
            ));
        }
        measures.push(m);
    }
    let table = measures[0].table;

    // --- group_by ---
    if spec.group_by.len() > MAX_GROUP_BY {
        return Err(invalid("group_by", format!("{} dimensions, at most {MAX_GROUP_BY}", spec.group_by.len())));
    }
    let mut group_by: Vec<ValidGroupBy> = Vec::with_capacity(spec.group_by.len());
    for (i, g) in spec.group_by.iter().enumerate() {
        let field = format!("group_by[{i}].dimension");
        let d = catalog
            .dimension(&g.dimension)
            .ok_or_else(|| invalid(&field, format!("unknown dimension {}", echo(&g.dimension))))?;
        if !d.table.includes(table) {
            return Err(invalid(field, format!("dimension {} is not on {table}", d.id)));
        }
        if group_by.iter().any(|p| p.dimension == d.id) {
            return Err(invalid(field, format!("dimension {} is grouped by twice", d.id)));
        }
        if measures.iter().any(|m| m.id == d.id) {
            return Err(invalid(field, format!("dimension {} has the same name as a requested measure", d.id)));
        }
        d.check_param(g.bucket_width).map_err(|e| invalid(format!("group_by[{i}].bucket_width"), e.to_string()))?;
        group_by.push(ValidGroupBy { dimension: d.id, bucket_width: g.bucket_width });
    }

    // --- filters ---
    if spec.filters.len() > MAX_FILTERS {
        return Err(invalid("filters", format!("{} filters, at most {MAX_FILTERS}", spec.filters.len())));
    }
    let mut filters: Vec<ValidFilter> = Vec::with_capacity(spec.filters.len());
    for (i, f) in spec.filters.iter().enumerate() {
        let field = format!("filters[{i}].dimension");
        let d = catalog
            .dimension(&f.dimension)
            .ok_or_else(|| invalid(&field, format!("unknown dimension {}", echo(&f.dimension))))?;
        if !d.table.includes(table) {
            return Err(invalid(field, format!("dimension {} is not on {table}", d.id)));
        }
        if !d.filterable {
            return Err(invalid(field, format!("dimension {} cannot be filtered on", d.id)));
        }
        if let Some(k) = spec.filters[..i].iter().position(|p| p.dimension == d.id) {
            return Err(invalid(
                field,
                format!("dimension {} is already filtered by filters[{k}]; list every value in one `in` filter", d.id),
            ));
        }
        let n = f.values.len();
        let count_ok = match f.op {
            FilterOp::Eq => n == 1,
            FilterOp::In => (1..=MAX_FILTER_VALUES).contains(&n),
        };
        if !count_ok {
            let reason = match f.op {
                FilterOp::Eq => format!("`eq` takes exactly one value, got {n}"),
                FilterOp::In => format!("`in` takes 1 to {MAX_FILTER_VALUES} values, got {n}"),
            };
            return Err(invalid(format!("filters[{i}].values"), reason));
        }
        let mut values: Vec<String> = Vec::with_capacity(n);
        for (j, v) in f.values.iter().enumerate() {
            let field = format!("filters[{i}].values[{j}]");
            d.filter_sql(v).map_err(|e| invalid(&field, format!("{}: {e}", echo(v))))?;
            if values.contains(v) {
                return Err(invalid(field, format!("{} is listed twice", echo(v))));
            }
            values.push(v.clone());
        }
        values.sort_unstable();
        filters.push(ValidFilter { dimension: d.id, values });
    }
    filters.sort_by_key(|f| f.dimension);

    // --- required dimensions ---
    for (i, m) in measures.iter().enumerate() {
        if let Some(required) = m.requires_dimension {
            let grouped = group_by.iter().any(|g| g.dimension == required);
            let pinned = filters.iter().any(|f| f.dimension == required && f.values.len() == 1);
            if !grouped && !pinned {
                return Err(invalid(
                    format!("measures[{i}]"),
                    format!(
                        "measure {} is only meaningful per {required}: group by {required}, \
                         or filter {required} to exactly one value",
                        m.id
                    ),
                ));
            }
        }
    }

    // --- window ---
    let max_span = limits.max_slot_span;
    match spec.window {
        Window::LastSlots(n) => {
            if !(1..=max_span).contains(&n) {
                return Err(invalid("window.last_slots", format!("{n} is outside 1..={max_span}")));
            }
        }
        Window::Range { from_slot, to_slot } => {
            if from_slot > MAX_SLOT {
                return Err(invalid("window.from_slot", format!("{from_slot} is above the largest slot, {MAX_SLOT}")));
            }
            if to_slot > MAX_SLOT {
                return Err(invalid("window.to_slot", format!("{to_slot} is above the largest slot, {MAX_SLOT}")));
            }
            if from_slot > to_slot {
                return Err(invalid("window.to_slot", format!("{to_slot} is below from_slot, {from_slot}")));
            }
            let span = to_slot - from_slot + 1;
            if span > max_span {
                return Err(invalid("window", format!("covers {span} slots, at most {max_span}")));
            }
        }
    }

    // --- the bucket fits the window ---
    if let Some((i, width)) = group_by.iter().enumerate().find_map(|(i, g)| g.bucket_width.map(|w| (i, w))) {
        let field = format!("group_by[{i}].bucket_width");
        match spec.window {
            Window::LastSlots(n) => {
                // At most 2 * 216_000 - 1: no overflow.
                let least = 2 * width - 1;
                if n < least {
                    return Err(invalid(
                        field,
                        format!(
                            "a window of the last {n} slots is not sure to hold a whole bucket of {width}; \
                             it must be at least 2 * {width} - 1 = {least} slots"
                        ),
                    ));
                }
            }
            Window::Range { from_slot, to_slot } => {
                if SlotWindow::new(from_slot, to_slot).snap(width).is_none() {
                    return Err(invalid(
                        field,
                        format!("the window {from_slot}..={to_slot} holds no whole bucket of {width} slots"),
                    ));
                }
            }
        }
    }

    // --- order_by ---
    if spec.order_by.len() > MAX_ORDER_BY {
        return Err(invalid("order_by", format!("{} sort keys, at most {MAX_ORDER_BY}", spec.order_by.len())));
    }
    let mut order_by: Vec<ValidOrder> = Vec::with_capacity(spec.order_by.len());
    for (i, o) in spec.order_by.iter().enumerate() {
        let field = format!("order_by[{i}].target");
        let key = if let Some(k) = measures.iter().position(|m| m.id == o.target) {
            OrderKey::Measure(k)
        } else if let Some(k) = group_by.iter().position(|g| g.dimension == o.target) {
            OrderKey::GroupBy(k)
        } else {
            return Err(invalid(
                field,
                format!("{} is not a measure or group-by dimension of this spec", echo(&o.target)),
            ));
        };
        if order_by.iter().any(|p| p.key == key) {
            return Err(invalid(field, format!("{} is ordered by twice", echo(&o.target))));
        }
        order_by.push(ValidOrder { key, direction: o.direction });
    }

    // --- limit ---
    let limit = match spec.limit {
        None => limits.max_rows,
        Some(0) => return Err(invalid("limit", "must be at least 1")),
        Some(n) if n > limits.max_rows => {
            return Err(invalid("limit", format!("{n} is above the row cap, {}", limits.max_rows)));
        }
        Some(n) => n,
    };

    // --- as_of_slot ---
    if let Some(slot) = spec.as_of_slot
        && slot > MAX_SLOT
    {
        return Err(invalid("as_of_slot", format!("{slot} is above the largest slot, {MAX_SLOT}")));
    }

    Ok(ValidSpec {
        table,
        measures: measures.iter().map(|m| m.id).collect(),
        group_by,
        filters,
        window: spec.window,
        order_by,
        limit,
        as_of_slot: spec.as_of_slot,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn echo_cuts_and_escapes() {
        assert_eq!(echo("mint"), "\"mint\"");
        assert_eq!(echo(&"x".repeat(32)), format!("\"{}\"", "x".repeat(32)));
        assert_eq!(echo(&"x".repeat(33)), format!("\"{}\"…", "x".repeat(32)));
        assert_eq!(echo("a\"b\n"), "\"a\\\"b\\n\"");
        // Characters, not bytes: a multi-byte value is not split mid-character.
        assert_eq!(echo(&"é".repeat(40)), format!("\"{}\"…", "é".repeat(32)));
    }
}
