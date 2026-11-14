//! Measures: one aggregate over one table.
//!
//! | id                            | table           | kind                                   | unit             |
//! |-------------------------------|-----------------|----------------------------------------|------------------|
//! | `slot_count`                  | slots           | `Count`                                | count            |
//! | `tx_count`                    | transactions    | `Count`                                | count            |
//! | `tx_failed_count`             | transactions    | `Count(status = 'failed')`             | count            |
//! | `tx_failure_ratio`            | transactions    | `Ratio(status = 'failed' / all)`       | ratio            |
//! | `fee_total_lamports`          | transactions    | `SumDecimal(fee)`                      | lamports         |
//! | `fee_avg_lamports`            | transactions    | `Avg(fee)`                             | lamports         |
//! | `compute_units_avg`           | transactions    | `Avg(compute_units_consumed)`          | compute units    |
//! | `ix_count`                    | instructions    | `Count`                                | count            |
//! | `ix_cpi_count`                | instructions    | `Count(inner_instruction_index set)`   | count            |
//! | `ix_undecoded_count`          | instructions    | `Count(instruction_type IS NULL)`      | count            |
//! | `program_count`               | instructions    | `CountDistinct(program_id)`            | count            |
//! | `transfer_count`              | token_transfers | `Count`                                | count            |
//! | `transfer_unattributed_count` | token_transfers | `Count(mint_token_address IS NULL)`    | count            |
//! | `mint_count`                  | token_transfers | `CountDistinct(mint_token_address)`    | count            |
//! | `transfer_volume_raw`         | token_transfers | `SumDecimal(amount_raw)`, per mint     | raw token amount |

use serde::Serialize;

use super::{
    CatalogError, ColType, Dimension, Table, bad, check_text, check_warnings, column_of, expect_type, is_safe_literal,
};

/// A row predicate on one column. The string is a definition constant,
/// checked by [`is_safe_literal`] at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Pred {
    IsNull(&'static str),
    IsNotNull(&'static str),
    EqStr(&'static str, &'static str),
    EqBool(&'static str, bool),
}

impl Pred {
    pub const fn column(&self) -> &'static str {
        match *self {
            Self::IsNull(c) | Self::IsNotNull(c) | Self::EqStr(c, _) | Self::EqBool(c, _) => c,
        }
    }

    /// The predicate as SQL, e.g. `status = 'failed'`.
    pub fn sql(&self) -> String {
        match *self {
            Self::IsNull(c) => format!("{c} IS NULL"),
            Self::IsNotNull(c) => format!("{c} IS NOT NULL"),
            Self::EqStr(c, v) => format!("{c} = '{v}'"),
            Self::EqBool(c, v) => format!("{c} = {}", if v { "TRUE" } else { "FALSE" }),
        }
    }

    /// SQL true exactly where [`sql`](Self::sql) is not: false *or* NULL.
    /// What a `CASE WHEN p THEN a ELSE b END` puts in `b`.
    pub fn negated_sql(&self) -> String {
        match *self {
            Self::IsNull(c) => format!("{c} IS NOT NULL"),
            Self::IsNotNull(c) => format!("{c} IS NULL"),
            Self::EqStr(..) | Self::EqBool(..) => format!("({}) IS NOT TRUE", self.sql()),
        }
    }

    /// The column exists in `table` and suits the test.
    pub(crate) fn check(&self, item: &str, table: Table) -> Result<(), CatalogError> {
        let column = column_of(item, table, self.column())?;
        match *self {
            Self::IsNull(_) | Self::IsNotNull(_) => {
                if !column.nullable {
                    return Err(bad(item, format!("{table}.{} is never NULL; the test is constant", column.name)));
                }
            }
            Self::EqStr(_, v) => {
                expect_type(item, table, column, "Utf8", |t| t == ColType::Utf8)?;
                if !is_safe_literal(v) {
                    return Err(bad(item, format!("literal {v:?} must match [a-z0-9_]+")));
                }
            }
            Self::EqBool(..) => expect_type(item, table, column, "Bool", |t| t == ColType::Bool)?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum MeasureKind {
    /// `COUNT(*)` of the rows `filter` keeps.
    Count { filter: Option<Pred> },
    /// `COUNT(DISTINCT column)`; NULLs are not counted.
    CountDistinct { column: &'static str },
    /// The exact sum of an integer column, as a decimal string: sums of
    /// `u64`s overflow `u64` and lose precision as `f64`. Only the rows
    /// `filter` keeps are summed.
    SumDecimal { column: &'static str, filter: Option<Pred> },
    /// `AVG(column)` as `Float64`; NULLs are left out.
    Avg { column: &'static str },
    /// Rows matching `numerator` over rows matching `denominator` (all rows
    /// when `None`), as `Float64`; NULL when the denominator is 0.
    Ratio { numerator: Pred, denominator: Option<Pred> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    Count,
    Lamports,
    ComputeUnits,
    Ratio,
    /// Token base units, before `decimals`. Only comparable within a mint.
    RawTokenAmount,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Measure {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub table: Table,
    pub kind: MeasureKind,
    pub unit: Unit,
    /// A dimension that must be grouped by, or filtered on, for the number
    /// to mean anything.
    pub requires_dimension: Option<&'static str>,
    /// Caveats a caller must repeat next to the number.
    pub warnings: &'static [&'static str],
}

impl Measure {
    /// The aggregate expression, e.g. `COUNT(*)`. Pair it with
    /// [`filter`](Self::filter) in the `WHERE` clause.
    pub fn select_expr(&self) -> String {
        self.aggregate_sql(&[])
    }

    /// The aggregate over only the rows where every predicate in `when`
    /// holds, e.g. `COUNT(CASE WHEN status = 'failed' THEN 1 END)`. With
    /// `when` empty it is [`select_expr`](Self::select_expr).
    ///
    /// For a query whose `WHERE` clause cannot carry the measure's own
    /// [`filter`](Self::filter), because another measure in it reads the rows
    /// that filter drops: pass the filter here instead. `when` is catalog
    /// structure, never SQL text, so the result is still rendered from
    /// definitions only.
    pub fn aggregate_sql(&self, when: &[Pred]) -> String {
        self.render(when, false)
    }

    /// What to `ORDER BY` for this measure: [`aggregate_sql`] except that a
    /// decimal sum stays a `DECIMAL`. Its select expression is text, and
    /// text sorts `"9"` above `"10"`.
    ///
    /// [`aggregate_sql`]: Self::aggregate_sql
    pub fn sort_sql(&self, when: &[Pred]) -> String {
        self.render(when, true)
    }

    fn render(&self, when: &[Pred], numeric: bool) -> String {
        // `CASE WHEN {cond} THEN {then} END`, or `otherwise` with no condition.
        let gate = |extra: Option<&Pred>, then: &str, otherwise: &str| -> String {
            let conds: Vec<String> = when.iter().chain(extra).map(Pred::sql).collect();
            if conds.is_empty() { otherwise.to_owned() } else { format!("CASE WHEN {} THEN {then} END", conds.join(" AND ")) }
        };
        match self.kind {
            MeasureKind::Count { .. } => format!("COUNT({})", gate(None, "1", "*")),
            MeasureKind::CountDistinct { column } => format!("COUNT(DISTINCT {})", gate(None, column, column)),
            MeasureKind::SumDecimal { column, .. } => {
                let cast = format!("CAST({column} AS DECIMAL(38, 0))");
                let sum = format!("SUM({})", gate(None, &cast, &cast));
                if numeric { sum } else { format!("CAST({sum} AS VARCHAR)") }
            }
            MeasureKind::Avg { column } => format!("AVG({})", gate(None, column, column)),
            MeasureKind::Ratio { numerator, denominator } => {
                let num = format!("COUNT({})", gate(Some(&numerator), "1", "*"));
                let den = format!("COUNT({})", gate(denominator.as_ref(), "1", "*"));
                format!("CAST({num} AS DOUBLE) / NULLIF({den}, 0)")
            }
        }
    }

    /// The row filter the measure needs in the `WHERE` clause, if any.
    pub const fn filter(&self) -> Option<&Pred> {
        match &self.kind {
            MeasureKind::Count { filter } | MeasureKind::SumDecimal { filter, .. } => filter.as_ref(),
            _ => None,
        }
    }

    /// Every column the measure reads, `slot` aside.
    pub fn columns(&self) -> Vec<&'static str> {
        let mut v = Vec::new();
        match self.kind {
            MeasureKind::Count { filter } => v.extend(filter.map(|p| p.column())),
            MeasureKind::CountDistinct { column } | MeasureKind::Avg { column } => v.push(column),
            MeasureKind::SumDecimal { column, filter } => {
                v.push(column);
                v.extend(filter.map(|p| p.column()));
            }
            MeasureKind::Ratio { numerator, denominator } => {
                v.push(numerator.column());
                v.extend(denominator.map(|p| p.column()));
            }
        }
        v.sort_unstable();
        v.dedup();
        v
    }

    /// `dimensions` is the validated set, sorted by id.
    pub(crate) fn validate(&self, dimensions: &[Dimension]) -> Result<(), CatalogError> {
        let item = format!("measure {}", self.id);
        let item = item.as_str();
        let t = self.table;
        check_text(item, "label", self.label)?;
        check_text(item, "description", self.description)?;
        check_warnings(item, self.warnings)?;

        let unit_ok = match self.kind {
            MeasureKind::Count { filter } => {
                if let Some(p) = filter {
                    p.check(item, t)?;
                }
                self.unit == Unit::Count
            }
            MeasureKind::CountDistinct { column } => {
                column_of(item, t, column)?;
                self.unit == Unit::Count
            }
            MeasureKind::SumDecimal { column, filter } => {
                let c = column_of(item, t, column)?;
                expect_type(item, t, c, "an integer (U8, U32, U64, I64)", ColType::is_integer)?;
                if let Some(p) = filter {
                    p.check(item, t)?;
                }
                matches!(self.unit, Unit::Lamports | Unit::ComputeUnits | Unit::RawTokenAmount)
            }
            MeasureKind::Avg { column } => {
                let c = column_of(item, t, column)?;
                expect_type(item, t, c, "an integer (U8, U32, U64, I64)", ColType::is_integer)?;
                matches!(self.unit, Unit::Lamports | Unit::ComputeUnits)
            }
            MeasureKind::Ratio { numerator, denominator } => {
                numerator.check(item, t)?;
                if let Some(d) = denominator {
                    d.check(item, t)?;
                    if d == numerator {
                        return Err(bad(item, "the numerator and denominator are the same; the ratio is constant"));
                    }
                }
                self.unit == Unit::Ratio
            }
        };
        if !unit_ok {
            return Err(bad(item, format!("unit {:?} does not fit its kind", self.unit)));
        }

        let required = match self.requires_dimension {
            None => None,
            Some(id) => {
                let i = dimensions
                    .binary_search_by_key(&id, |d| d.id)
                    .map_err(|_| bad(item, format!("requires unknown dimension {id:?}")))?;
                let d = &dimensions[i];
                if !d.table.includes(t) {
                    return Err(bad(item, format!("requires dimension {id:?}, which is not on {t}")));
                }
                if !d.filterable {
                    return Err(bad(item, format!("requires dimension {id:?}, which cannot be filtered on")));
                }
                Some(d)
            }
        };

        // Decision 7: raw amounts are summed within one mint, never across.
        if self.unit == Unit::RawTokenAmount {
            let MeasureKind::SumDecimal { filter, .. } = self.kind else { unreachable!("checked with the unit") };
            let Some(d) = required else {
                return Err(bad(item, "a raw token amount must require the dimension that names its mint"));
            };
            let raw = d.raw_column();
            if column_of(item, t, raw)?.nullable && filter != Some(Pred::IsNotNull(raw)) {
                return Err(bad(
                    item,
                    format!("a raw token amount must exclude rows with no {raw}: filter {raw} IS NOT NULL"),
                ));
            }
            if self.warnings.is_empty() {
                return Err(bad(item, "a raw token amount must warn that decimals are not applied"));
            }
        }
        Ok(())
    }
}

use MeasureKind::{Avg, Count, CountDistinct, Ratio, SumDecimal};
use Table::{Instructions, Slots, TokenTransfers, Transactions};

const DISTINCT_NOT_ADDITIVE: &str =
    "Distinct counts do not add up: the sum over buckets or groups over-counts anything seen in more than one.";
const UNKNOWN_MINT_EXCLUDED: &str = "Transfers whose mint PRISM could not resolve are not counted.";

/// The built-in measures.
pub const MEASURES: &[Measure] = &[
    Measure {
        id: "slot_count",
        label: "Slots",
        description: "Slots PRISM has written.",
        table: Slots,
        kind: Count { filter: None },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &["Skipped slots, and slots PRISM has not ingested yet, are not counted."],
    },
    Measure {
        id: "tx_count",
        label: "Transactions",
        description: "Transactions, successful or failed.",
        table: Transactions,
        kind: Count { filter: None },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[],
    },
    Measure {
        id: "tx_failed_count",
        label: "Failed transactions",
        description: "Transactions whose status is failed.",
        table: Transactions,
        kind: Count { filter: Some(Pred::EqStr("status", "failed")) },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[],
    },
    Measure {
        id: "tx_failure_ratio",
        label: "Transaction failure ratio",
        description: "Failed transactions over all transactions, from 0 to 1.",
        table: Transactions,
        kind: Ratio { numerator: Pred::EqStr("status", "failed"), denominator: None },
        unit: Unit::Ratio,
        requires_dimension: None,
        warnings: &["NULL for a group with no transactions."],
    },
    Measure {
        id: "fee_total_lamports",
        label: "Total fees",
        description: "The sum of transaction fees, in lamports, as an exact decimal string.",
        table: Transactions,
        kind: SumDecimal { column: "fee", filter: None },
        unit: Unit::Lamports,
        requires_dimension: None,
        warnings: &["Returned as a decimal string: the total can exceed what a JSON number holds exactly."],
    },
    Measure {
        id: "fee_avg_lamports",
        label: "Average fee",
        description: "The mean transaction fee, in lamports.",
        table: Transactions,
        kind: Avg { column: "fee" },
        unit: Unit::Lamports,
        requires_dimension: None,
        warnings: &[],
    },
    Measure {
        id: "compute_units_avg",
        label: "Average compute units",
        description: "The mean compute units a transaction consumed.",
        table: Transactions,
        kind: Avg { column: "compute_units_consumed" },
        unit: Unit::ComputeUnits,
        requires_dimension: None,
        warnings: &["Transactions the RPC reported no compute units for are left out of the average."],
    },
    Measure {
        id: "ix_count",
        label: "Instructions",
        description: "Instructions, top-level and inner.",
        table: Instructions,
        kind: Count { filter: None },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[],
    },
    Measure {
        id: "ix_cpi_count",
        label: "Inner instructions",
        description: "Instructions invoked by another program (cross-program invocations).",
        table: Instructions,
        kind: Count { filter: Some(Pred::IsNotNull("inner_instruction_index")) },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &["Counts every inner instruction; how deeply it was nested is not recorded."],
    },
    Measure {
        id: "ix_undecoded_count",
        label: "Undecoded instructions",
        description: "Instructions PRISM did not decode into a named instruction type.",
        table: Instructions,
        kind: Count { filter: Some(Pred::IsNull("instruction_type")) },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &["PRISM only decodes programs it has a decoder for; an undecoded instruction is not an error."],
    },
    Measure {
        id: "program_count",
        label: "Distinct programs",
        description: "Distinct programs invoked, top-level or inner.",
        table: Instructions,
        kind: CountDistinct { column: "program_id" },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[DISTINCT_NOT_ADDITIVE],
    },
    Measure {
        id: "transfer_count",
        label: "Token transfers",
        description: "SPL Token and Token-2022 transfers.",
        table: TokenTransfers,
        kind: Count { filter: None },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[],
    },
    Measure {
        id: "transfer_unattributed_count",
        label: "Unattributed token transfers",
        description: "Token transfers whose mint PRISM could not resolve.",
        table: TokenTransfers,
        kind: Count { filter: Some(Pred::IsNull("mint_token_address")) },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &["Typically a plain Transfer whose token account was closed before PRISM resolved it."],
    },
    Measure {
        id: "mint_count",
        label: "Distinct mints",
        description: "Distinct mints transferred.",
        table: TokenTransfers,
        kind: CountDistinct { column: "mint_token_address" },
        unit: Unit::Count,
        requires_dimension: None,
        warnings: &[DISTINCT_NOT_ADDITIVE, UNKNOWN_MINT_EXCLUDED],
    },
    Measure {
        id: "transfer_volume_raw",
        label: "Raw transfer volume",
        description: "The sum of transferred amounts in the mint's base units, as an exact decimal string.",
        table: TokenTransfers,
        kind: SumDecimal { column: "amount_raw", filter: Some(Pred::IsNotNull("mint_token_address")) },
        unit: Unit::RawTokenAmount,
        requires_dimension: Some("mint"),
        warnings: &[
            "Raw base units: the mint's decimals are not applied.",
            "Only meaningful per mint: group by or filter on mint.",
            UNKNOWN_MINT_EXCLUDED,
        ],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    fn m(id: &str) -> &'static Measure {
        MEASURES.iter().find(|m| m.id == id).unwrap()
    }

    #[test]
    fn predicates_render() {
        assert_eq!(Pred::IsNull("a").sql(), "a IS NULL");
        assert_eq!(Pred::IsNotNull("a").sql(), "a IS NOT NULL");
        assert_eq!(Pred::EqStr("a", "x").sql(), "a = 'x'");
        assert_eq!(Pred::EqBool("a", true).sql(), "a = TRUE");
        assert_eq!(Pred::EqBool("a", false).sql(), "a = FALSE");
        assert_eq!(Pred::IsNull("a").negated_sql(), "a IS NOT NULL");
        assert_eq!(Pred::IsNotNull("a").negated_sql(), "a IS NULL");
        assert_eq!(Pred::EqStr("a", "x").negated_sql(), "(a = 'x') IS NOT TRUE");
    }

    #[test]
    fn select_expressions() {
        assert_eq!(m("tx_count").select_expr(), "COUNT(*)");
        assert_eq!(m("tx_failed_count").select_expr(), "COUNT(*)");
        assert_eq!(m("tx_failed_count").filter(), Some(&Pred::EqStr("status", "failed")));
        assert_eq!(m("program_count").select_expr(), "COUNT(DISTINCT program_id)");
        assert_eq!(m("fee_total_lamports").select_expr(), "CAST(SUM(CAST(fee AS DECIMAL(38, 0))) AS VARCHAR)");
        assert_eq!(m("fee_avg_lamports").select_expr(), "AVG(fee)");
        assert_eq!(
            m("tx_failure_ratio").select_expr(),
            "CAST(COUNT(CASE WHEN status = 'failed' THEN 1 END) AS DOUBLE) / NULLIF(COUNT(*), 0)"
        );
        assert_eq!(m("tx_failure_ratio").filter(), None);
        assert_eq!(m("transfer_volume_raw").filter(), Some(&Pred::IsNotNull("mint_token_address")));
    }

    #[test]
    fn conditions_fold_into_the_aggregate() {
        let nn = Pred::IsNotNull("mint_token_address");
        let failed = Pred::EqStr("status", "failed");
        assert_eq!(m("tx_count").aggregate_sql(&[failed]), "COUNT(CASE WHEN status = 'failed' THEN 1 END)");
        assert_eq!(
            m("transfer_volume_raw").aggregate_sql(&[nn]),
            "CAST(SUM(CASE WHEN mint_token_address IS NOT NULL THEN CAST(amount_raw AS DECIMAL(38, 0)) END) AS VARCHAR)"
        );
        assert_eq!(
            m("mint_count").aggregate_sql(&[nn]),
            "COUNT(DISTINCT CASE WHEN mint_token_address IS NOT NULL THEN mint_token_address END)"
        );
        assert_eq!(m("fee_avg_lamports").aggregate_sql(&[failed]), "AVG(CASE WHEN status = 'failed' THEN fee END)");
        assert_eq!(
            m("tx_failure_ratio").aggregate_sql(&[Pred::IsNotNull("err")]),
            "CAST(COUNT(CASE WHEN err IS NOT NULL AND status = 'failed' THEN 1 END) AS DOUBLE) \
             / NULLIF(COUNT(CASE WHEN err IS NOT NULL THEN 1 END), 0)"
        );
        // No condition: exactly the select expression.
        for measure in MEASURES {
            assert_eq!(measure.aggregate_sql(&[]), measure.select_expr(), "{}", measure.id);
        }
    }

    #[test]
    fn decimal_sums_sort_as_numbers() {
        assert_eq!(m("fee_total_lamports").sort_sql(&[]), "SUM(CAST(fee AS DECIMAL(38, 0)))");
        assert_eq!(
            m("transfer_volume_raw").sort_sql(&[Pred::IsNotNull("mint_token_address")]),
            "SUM(CASE WHEN mint_token_address IS NOT NULL THEN CAST(amount_raw AS DECIMAL(38, 0)) END)"
        );
        // Everything else already sorts as what it selects.
        for measure in MEASURES.iter().filter(|m| !matches!(m.kind, SumDecimal { .. })) {
            assert_eq!(measure.sort_sql(&[]), measure.select_expr(), "{}", measure.id);
        }
    }

    #[test]
    fn a_ratio_with_a_denominator() {
        let r = Measure {
            kind: Ratio { numerator: Pred::EqStr("status", "failed"), denominator: Some(Pred::IsNotNull("err")) },
            ..*m("tx_failure_ratio")
        };
        assert!(r.select_expr().ends_with("/ NULLIF(COUNT(CASE WHEN err IS NOT NULL THEN 1 END), 0)"));
        assert_eq!(r.columns(), vec!["err", "status"]);
    }

    #[test]
    fn columns_are_listed() {
        assert_eq!(m("tx_count").columns(), Vec::<&str>::new());
        assert_eq!(m("transfer_volume_raw").columns(), vec!["amount_raw", "mint_token_address"]);
    }
}
