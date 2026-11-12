//! Dimensions: what a measure can be grouped by and filtered on.
//!
//! | id                  | table           | expression                                       | values      |
//! |---------------------|-----------------|--------------------------------------------------|-------------|
//! | `slot`              | any             | `slot`                                           | slot range  |
//! | `slot_bucket`       | any             | `CAST(slot AS BIGINT) / {width}`                 | bucket width|
//! | `leader`            | slots           | `COALESCE(leader, 'unknown')`                    | base58 key  |
//! | `tx_status`         | transactions    | `status`                                         | enum        |
//! | `program`           | instructions    | `program_id`                                     | base58 key  |
//! | `ix_level`          | instructions    | `CASE inner_instruction_index IS NULL …`         | enum        |
//! | `ix_decoded`        | instructions    | `CASE instruction_type IS NULL …`                | enum        |
//! | `token_program`     | token_transfers | `spl_token_program_id`                           | base58 key  |
//! | `mint`              | token_transfers | `COALESCE(mint_token_address, 'unattributed')`   | base58 key  |
//! | `source_owner`      | token_transfers | `COALESCE(source_owner, 'unknown')`              | base58 key  |
//! | `destination_owner` | token_transfers | `COALESCE(destination_owner, 'unknown')`         | base58 key  |
//!
//! Rules:
//!
//! * `slot_bucket` is the only parameterised dimension: its width, in slots,
//!   is `1..=`[`MAX_BUCKET_WIDTH_SLOTS`]. It groups but does not filter: a
//!   query's window already bounds its slots.
//! * A filter on a `COALESCE`d dimension tests the **raw column**
//!   (`mint_token_address = '…'`), never the rendered expression, so it can
//!   use the column's statistics and cannot match the placeholder.
//! * No dimension renders text from outside. A filter value is checked by
//!   its [`ValueRule`] first; an enum member is rendered from the
//!   definition's own constant, a key only once it is proven base58
//!   (alphanumeric), a slot only once it is parsed as a number.
//! * There are no hour or day dimensions: windows are measured in slots.

use serde::Serialize;
use thiserror::Error;

use super::{
    CatalogError, ColType, MAX_SLOT, Pred, Table, bad, check_text, check_warnings, column_of, expect_type,
    is_base58_key, is_safe_literal,
};

/// Widest `slot_bucket`: about a day at 400 ms a slot, matching
/// `ORACLE_BUCKET_SECS`' ceiling.
pub const MAX_BUCKET_WIDTH_SLOTS: u64 = 216_000;

/// What a dimension's filter values (or, for `slot_bucket`, its parameter)
/// must be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "values")]
pub enum ValueRule {
    /// A 32-byte base58 key.
    Base58Key,
    /// One of these, sorted.
    Enum(&'static [&'static str]),
    Bool,
    /// `N` or `LO..=HI`, each within `0..=i64::MAX`.
    SlotRange,
    /// A bucket width in slots, `1..=MAX_BUCKET_WIDTH_SLOTS`.
    BucketWidth,
}

impl ValueRule {
    /// What a valid value looks like, for error messages.
    pub fn expected(&self) -> String {
        match self {
            Self::Base58Key => "a 32-byte base58 address".to_owned(),
            Self::Enum(values) => format!("one of: {}", values.join(", ")),
            Self::Bool => "true or false".to_owned(),
            Self::SlotRange => format!("a slot N or a range LO..=HI, each at most {MAX_SLOT}"),
            Self::BucketWidth => format!("a width in slots from 1 to {MAX_BUCKET_WIDTH_SLOTS}"),
        }
    }
}

/// Parse `N` or `LO..=HI` as an inclusive slot range.
fn parse_slot_range(value: &str) -> Option<(u64, u64)> {
    let num = |s: &str| -> Option<u64> {
        // `u64::from_str` accepts a leading `+`; a slot is digits only.
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        s.parse::<u64>().ok().filter(|&n| n <= MAX_SLOT)
    };
    match value.split_once("..=") {
        None => num(value).map(|n| (n, n)),
        Some((lo, hi)) => {
            let (lo, hi) = (num(lo)?, num(hi)?);
            (lo <= hi).then_some((lo, hi))
        }
    }
}

/// Which tables a dimension exists on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DimTable {
    One(Table),
    /// Every table: only for columns all four carry.
    Any,
}

impl DimTable {
    pub fn includes(self, table: Table) -> bool {
        match self {
            Self::One(t) => t == table,
            Self::Any => true,
        }
    }

    pub fn tables(self) -> Vec<Table> {
        match self {
            Self::One(t) => vec![t],
            Self::Any => Table::ALL.to_vec(),
        }
    }
}

/// A dimension's group-by expression, as structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DimExpr {
    /// The column itself.
    Column(&'static str),
    /// `COALESCE(column, 'literal')`: NULLs grouped under the literal.
    Coalesce(&'static str, &'static str),
    /// `CASE WHEN when THEN 'then' ELSE 'otherwise' END`.
    Case { when: Pred, then: &'static str, otherwise: &'static str },
    /// `CAST(column AS BIGINT) / width`: the parameterised bucket.
    SlotBucket(&'static str),
}

/// Why a filter value or parameter was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ValueError {
    #[error("dimension {dimension} cannot be filtered on")]
    NotFilterable { dimension: String },
    #[error("dimension {dimension}: expected {expected}")]
    BadValue { dimension: String, expected: String },
    #[error("dimension {dimension}: {reason}")]
    BadParam { dimension: String, reason: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Dimension {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub table: DimTable,
    pub expr: DimExpr,
    pub values: ValueRule,
    pub filterable: bool,
    /// Takes a parameter: only `slot_bucket`, its width.
    pub parameterised: bool,
    pub warnings: &'static [&'static str],
}

impl Dimension {
    /// The column the expression is built on; what a filter tests.
    pub const fn raw_column(&self) -> &'static str {
        match self.expr {
            DimExpr::Column(c) | DimExpr::Coalesce(c, _) | DimExpr::SlotBucket(c) => c,
            DimExpr::Case { when, .. } => when.column(),
        }
    }

    /// Every column the dimension reads.
    pub fn columns(&self) -> Vec<&'static str> {
        vec![self.raw_column()]
    }

    /// `param` is what this dimension takes: a width in range for a
    /// parameterised one, nothing otherwise.
    pub fn check_param(&self, param: Option<u64>) -> Result<(), ValueError> {
        let err = |reason: String| Err(ValueError::BadParam { dimension: self.id.to_owned(), reason });
        match (self.parameterised, param) {
            (false, None) => Ok(()),
            (false, Some(_)) => err("takes no parameter".to_owned()),
            (true, None) => err(format!("needs {}", self.values.expected())),
            (true, Some(w)) if (1..=MAX_BUCKET_WIDTH_SLOTS).contains(&w) => Ok(()),
            (true, Some(_)) => err(format!("expected {}", self.values.expected())),
        }
    }

    /// The group-by expression.
    ///
    /// # Panics
    ///
    /// If `param` fails [`check_param`](Self::check_param); check it first.
    pub fn select_expr(&self, param: Option<u64>) -> String {
        if let Err(e) = self.check_param(param) {
            panic!("select_expr called with an unchecked parameter: {e}");
        }
        match self.expr {
            DimExpr::Column(c) => c.to_owned(),
            DimExpr::Coalesce(c, lit) => format!("COALESCE({c}, '{lit}')"),
            DimExpr::Case { when, then, otherwise } => {
                format!("CASE WHEN {} THEN '{then}' ELSE '{otherwise}' END", when.sql())
            }
            DimExpr::SlotBucket(c) => format!("CAST({c} AS BIGINT) / {}", param.expect("checked")),
        }
    }

    /// The `WHERE` condition for "this dimension equals `value`". Tests the
    /// raw column; renders only validated text.
    pub fn filter_sql(&self, value: &str) -> Result<String, ValueError> {
        if !self.filterable {
            return Err(ValueError::NotFilterable { dimension: self.id.to_owned() });
        }
        let bad_value = || ValueError::BadValue { dimension: self.id.to_owned(), expected: self.values.expected() };
        let col = self.raw_column();
        match (self.values, self.expr) {
            (ValueRule::Base58Key, _) => {
                if is_base58_key(value) {
                    Ok(format!("{col} = '{value}'"))
                } else {
                    Err(bad_value())
                }
            }
            (ValueRule::Enum(values), expr) => {
                // Render the definition's constant, not the input.
                let member = *values.iter().find(|v| **v == value).ok_or_else(bad_value)?;
                Ok(match expr {
                    DimExpr::Case { when, then, .. } => {
                        if member == then {
                            when.sql()
                        } else {
                            when.negated_sql()
                        }
                    }
                    DimExpr::Coalesce(_, lit) if member == lit => format!("{col} IS NULL"),
                    _ => format!("{col} = '{member}'"),
                })
            }
            (ValueRule::Bool, _) => match value {
                "true" => Ok(format!("{col} = TRUE")),
                "false" => Ok(format!("{col} = FALSE")),
                _ => Err(bad_value()),
            },
            (ValueRule::SlotRange, _) => {
                let (lo, hi) = parse_slot_range(value).ok_or_else(bad_value)?;
                Ok(if lo == hi { format!("{col} = {lo}") } else { format!("{col} >= {lo} AND {col} <= {hi}") })
            }
            (ValueRule::BucketWidth, _) => Err(ValueError::NotFilterable { dimension: self.id.to_owned() }),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), CatalogError> {
        let item = format!("dimension {}", self.id);
        let item = item.as_str();
        check_text(item, "label", self.label)?;
        check_text(item, "description", self.description)?;
        check_warnings(item, self.warnings)?;

        let is_bucket = matches!(self.expr, DimExpr::SlotBucket(_));
        if self.parameterised != is_bucket {
            return Err(bad(item, "only a slot bucket, and every slot bucket, is parameterised"));
        }
        if is_bucket != (self.values == ValueRule::BucketWidth) {
            return Err(bad(item, "a slot bucket, and only a slot bucket, takes a bucket width"));
        }
        if is_bucket && self.filterable {
            return Err(bad(item, "a slot bucket cannot be filtered on"));
        }
        if let ValueRule::Enum(values) = self.values {
            if values.is_empty() {
                return Err(bad(item, "an enum needs at least one value"));
            }
            if let Some(v) = values.iter().find(|v| !is_safe_literal(v)) {
                return Err(bad(item, format!("enum value {v:?} must match [a-z0-9_]+")));
            }
            if !values.windows(2).all(|w| w[0] < w[1]) {
                return Err(bad(item, "enum values must be sorted and unique"));
            }
        }

        for t in self.table.tables() {
            match self.expr {
                DimExpr::Column(c) => {
                    let column = column_of(item, t, c)?;
                    match self.values {
                        ValueRule::Base58Key | ValueRule::Enum(_) => {
                            expect_type(item, t, column, "Utf8", |ty| ty == ColType::Utf8)?;
                        }
                        ValueRule::Bool => expect_type(item, t, column, "Bool", |ty| ty == ColType::Bool)?,
                        ValueRule::SlotRange => {
                            expect_type(item, t, column, "U64", |ty| ty == ColType::U64)?;
                            if column.nullable {
                                return Err(bad(item, "a slot range needs a non-null column"));
                            }
                        }
                        ValueRule::BucketWidth => unreachable!("checked above"),
                    }
                }
                DimExpr::Coalesce(c, lit) => {
                    let column = column_of(item, t, c)?;
                    expect_type(item, t, column, "Utf8", |ty| ty == ColType::Utf8)?;
                    if !column.nullable {
                        return Err(bad(item, format!("{t}.{c} is never NULL; COALESCE is pointless")));
                    }
                    if !is_safe_literal(lit) {
                        return Err(bad(item, format!("placeholder {lit:?} must match [a-z0-9_]+")));
                    }
                    match self.values {
                        ValueRule::Base58Key if is_base58_key(lit) => {
                            return Err(bad(item, format!("placeholder {lit:?} could be mistaken for an address")));
                        }
                        ValueRule::Base58Key => {}
                        ValueRule::Enum(values) if values.contains(&lit) => {}
                        ValueRule::Enum(_) => {
                            return Err(bad(item, format!("placeholder {lit:?} must be one of the enum values")));
                        }
                        _ => return Err(bad(item, "a COALESCE takes base58 keys or an enum")),
                    }
                }
                DimExpr::Case { when, then, otherwise } => {
                    when.check(item, t)?;
                    for lit in [then, otherwise] {
                        if !is_safe_literal(lit) {
                            return Err(bad(item, format!("label {lit:?} must match [a-z0-9_]+")));
                        }
                    }
                    let mut labels = [then, otherwise];
                    labels.sort_unstable();
                    match self.values {
                        ValueRule::Enum(values) if then != otherwise && values == labels => {}
                        _ => return Err(bad(item, "a CASE takes an enum of exactly its two distinct labels")),
                    }
                }
                DimExpr::SlotBucket(c) => {
                    let column = column_of(item, t, c)?;
                    expect_type(item, t, column, "U64", |ty| ty == ColType::U64)?;
                    if column.nullable {
                        return Err(bad(item, "a slot bucket needs a non-null column"));
                    }
                }
            }
        }
        Ok(())
    }
}

use DimTable::{Any, One};
use Table::{Instructions, Slots, TokenTransfers, Transactions};

/// The built-in dimensions.
pub const DIMENSIONS: &[Dimension] = &[
    Dimension {
        id: "slot",
        label: "Slot",
        description: "The slot a row belongs to.",
        table: Any,
        expr: DimExpr::Column("slot"),
        values: ValueRule::SlotRange,
        filterable: true,
        parameterised: false,
        warnings: &["One group per slot; group by slot_bucket for a series."],
    },
    Dimension {
        id: "slot_bucket",
        label: "Slot bucket",
        description: "Fixed-width slot buckets: bucket k holds slots k*width ..= k*width + width - 1.",
        table: Any,
        expr: DimExpr::SlotBucket("slot"),
        values: ValueRule::BucketWidth,
        filterable: false,
        parameterised: true,
        warnings: &["A bucket at either end of the window may be partly outside it."],
    },
    Dimension {
        id: "leader",
        label: "Leader",
        description: "The validator that produced the slot.",
        table: One(Slots),
        expr: DimExpr::Coalesce("leader", "unknown"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &["Slots whose leader PRISM did not record are grouped as unknown."],
    },
    Dimension {
        id: "tx_status",
        label: "Transaction status",
        description: "Whether the transaction succeeded or failed.",
        table: One(Transactions),
        expr: DimExpr::Column("status"),
        values: ValueRule::Enum(&["failed", "success"]),
        filterable: true,
        parameterised: false,
        warnings: &[],
    },
    Dimension {
        id: "program",
        label: "Program",
        description: "The program an instruction invokes.",
        table: One(Instructions),
        expr: DimExpr::Column("program_id"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &[],
    },
    Dimension {
        id: "ix_level",
        label: "Instruction level",
        description: "Top-level instructions, or inner ones invoked by another program.",
        table: One(Instructions),
        expr: DimExpr::Case { when: Pred::IsNull("inner_instruction_index"), then: "top_level", otherwise: "inner" },
        values: ValueRule::Enum(&["inner", "top_level"]),
        filterable: true,
        parameterised: false,
        warnings: &[],
    },
    Dimension {
        id: "ix_decoded",
        label: "Instruction decoded",
        description: "Whether PRISM decoded the instruction into a named type.",
        table: One(Instructions),
        expr: DimExpr::Case { when: Pred::IsNull("instruction_type"), then: "undecoded", otherwise: "decoded" },
        values: ValueRule::Enum(&["decoded", "undecoded"]),
        filterable: true,
        parameterised: false,
        warnings: &[],
    },
    Dimension {
        id: "token_program",
        label: "Token program",
        description: "SPL Token or Token-2022.",
        table: One(TokenTransfers),
        expr: DimExpr::Column("spl_token_program_id"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &[],
    },
    Dimension {
        id: "mint",
        label: "Mint",
        description: "The mint of the transferred token.",
        table: One(TokenTransfers),
        expr: DimExpr::Coalesce("mint_token_address", "unattributed"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &["Transfers whose mint PRISM could not resolve are grouped as unattributed."],
    },
    Dimension {
        id: "source_owner",
        label: "Source owner",
        description: "The owner of the account tokens left.",
        table: One(TokenTransfers),
        expr: DimExpr::Coalesce("source_owner", "unknown"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &["Transfers whose source owner PRISM could not resolve are grouped as unknown."],
    },
    Dimension {
        id: "destination_owner",
        label: "Destination owner",
        description: "The owner of the account tokens arrived in.",
        table: One(TokenTransfers),
        expr: DimExpr::Coalesce("destination_owner", "unknown"),
        values: ValueRule::Base58Key,
        filterable: true,
        parameterised: false,
        warnings: &["Transfers whose destination owner PRISM could not resolve are grouped as unknown."],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    fn d(id: &str) -> &'static Dimension {
        DIMENSIONS.iter().find(|d| d.id == id).unwrap()
    }

    #[test]
    fn select_expressions() {
        assert_eq!(d("program").select_expr(None), "program_id");
        assert_eq!(d("mint").select_expr(None), "COALESCE(mint_token_address, 'unattributed')");
        assert_eq!(
            d("ix_level").select_expr(None),
            "CASE WHEN inner_instruction_index IS NULL THEN 'top_level' ELSE 'inner' END"
        );
        assert_eq!(d("slot_bucket").select_expr(Some(150)), "CAST(slot AS BIGINT) / 150");
    }

    #[test]
    fn the_bucket_parameter() {
        let b = d("slot_bucket");
        assert!(b.check_param(Some(1)).is_ok());
        assert!(b.check_param(Some(MAX_BUCKET_WIDTH_SLOTS)).is_ok());
        assert!(b.check_param(Some(0)).is_err());
        assert!(b.check_param(Some(MAX_BUCKET_WIDTH_SLOTS + 1)).is_err());
        assert!(b.check_param(None).is_err());
        assert!(d("program").check_param(None).is_ok());
        assert!(d("program").check_param(Some(5)).is_err());
    }

    #[test]
    #[should_panic(expected = "unchecked parameter")]
    fn an_unchecked_parameter_panics() {
        d("slot_bucket").select_expr(Some(0));
    }

    #[test]
    fn filters_test_the_raw_column() {
        assert_eq!(d("mint").filter_sql(KEY).unwrap(), format!("mint_token_address = '{KEY}'"));
        assert_eq!(d("leader").filter_sql(KEY).unwrap(), format!("leader = '{KEY}'"));
        assert!(d("mint").filter_sql("unattributed").is_err());
        assert_eq!(d("tx_status").filter_sql("failed").unwrap(), "status = 'failed'");
        assert_eq!(d("ix_level").filter_sql("top_level").unwrap(), "inner_instruction_index IS NULL");
        assert_eq!(d("ix_level").filter_sql("inner").unwrap(), "inner_instruction_index IS NOT NULL");
        assert_eq!(d("ix_decoded").filter_sql("undecoded").unwrap(), "instruction_type IS NULL");
        assert_eq!(d("slot").filter_sql("42").unwrap(), "slot = 42");
        assert_eq!(d("slot").filter_sql("10..=20").unwrap(), "slot >= 10 AND slot <= 20");
        assert!(matches!(d("slot_bucket").filter_sql("1"), Err(ValueError::NotFilterable { .. })));
    }

    #[test]
    fn filter_values_are_refused_unless_valid() {
        for v in ["", "x", "FAILED", "failed ", "failed' OR '1'='1", "success;--"] {
            assert!(d("tx_status").filter_sql(v).is_err(), "{v:?}");
        }
        for v in ["", "abc", &format!("{KEY}'"), &format!("{KEY} "), "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl"] {
            assert!(d("program").filter_sql(v).is_err(), "{v:?}");
        }
        for v in ["", "-1", "+1", "1.0", "20..=10", "1..2", "9223372036854775808", "1..=", "..=1", " 1"] {
            assert!(d("slot").filter_sql(v).is_err(), "{v:?}");
        }
        assert_eq!(d("slot").filter_sql("9223372036854775807").unwrap(), "slot = 9223372036854775807");
    }

    #[test]
    fn a_bool_rule() {
        let b = Dimension {
            id: "flag",
            expr: DimExpr::Column("f"),
            values: ValueRule::Bool,
            ..*d("program")
        };
        assert_eq!(b.filter_sql("true").unwrap(), "f = TRUE");
        assert_eq!(b.filter_sql("false").unwrap(), "f = FALSE");
        assert!(b.filter_sql("TRUE").is_err());
    }
}
