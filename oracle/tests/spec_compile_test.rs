//! `spec/compile.rs`: every accepted spec compiles to exactly one safe,
//! parseable `SELECT`, and the statement follows the compilation rules:
//! select order, the window first, raw-column filters, measure conditions
//! hoisted or folded, group-by in full, the deterministic order, `NULLS
//! LAST`, `LIMIT limit + 1`, and catalog warnings.

mod spec_support;

use oracle::catalog::{Catalog, DIMENSIONS, MEASURES, Measure, Pred, Table, Unit};
use oracle::spec::compile::*;
use oracle::spec::guard::{ValidFilter, ValidSpec, validate};
use oracle::spec::*;
use spec_support::{KEY_A, KEY_B, assert_safe_select, catalog, compile_at, filter, group, limits, order, spec};

const W: u64 = 1_000_000;

fn sql_of(s: &QuerySpec) -> String {
    compile_at(&catalog(), s, W).1.sql
}

// ---------------------------------------------------------------------------
// Safety, for every accepted spec
// ---------------------------------------------------------------------------

#[test]
fn every_accepted_spec_compiles_to_one_safe_select() {
    let c = catalog();
    let specs = spec_support::accepted_specs(&c);
    // Every measure and every dimension is exercised.
    for m in c.measures() {
        assert!(specs.iter().any(|(_, s)| s.measures.iter().any(|x| x == m.id)), "{}", m.id);
    }
    for d in c.dimensions() {
        assert!(specs.iter().any(|(_, s)| s.group_by.iter().any(|g| g.dimension == d.id)), "{}", d.id);
        if d.filterable {
            assert!(specs.iter().any(|(_, s)| s.filters.iter().any(|f| f.dimension == d.id)), "{}", d.id);
        }
    }
    // At a far snapshot, and at one where `last_slots` is clipped at slot 0.
    let mut compiled = 0;
    for w in [W, 1_299, 50] {
        for (label, s) in &specs {
            if matches!(s.window, Window::Range { to_slot, .. } if to_slot > w) {
                continue;
            }
            let (valid, out) = compile_at(&c, s, w);
            // The compiler's own self-check passed, or compile would have failed.
            assert_eq!(self_check(&out.sql, valid.table, out.columns.len()), Ok(()), "{label}");
            assert_safe_select(&c, &valid, &out);
            compiled += 1;
        }
    }
    assert!(compiled > 900, "{compiled}");
}

#[test]
fn compiling_is_deterministic_and_equivalent_specs_compile_alike() {
    let a = QuerySpec {
        group_by: vec![group("mint")],
        filters: vec![
            filter("token_program", FilterOp::In, &[KEY_B, KEY_A]),
            filter("source_owner", FilterOp::Eq, &[KEY_A]),
        ],
        ..spec(&["transfer_count", "transfer_volume_raw"])
    };
    let b = QuerySpec {
        filters: vec![
            filter("source_owner", FilterOp::In, &[KEY_A]),
            filter("token_program", FilterOp::In, &[KEY_A, KEY_B]),
        ],
        ..a.clone()
    };
    assert_eq!(sql_of(&a), sql_of(&a));
    assert_eq!(sql_of(&a), sql_of(&b));
}

#[test]
fn a_forged_valid_spec_still_cannot_inject() {
    let c = catalog();
    let base = validate(&QuerySpec { group_by: vec![group("mint")], ..spec(&["transfer_count"]) }, &c, &limits()).unwrap();
    let window = SlotWindow::new(1, 10);
    assert!(compile(&base, &c, window).is_ok());

    // Values that skipped the guard are re-checked by the catalog.
    for hostile in ["x' OR '1'='1", "unattributed", "1; DROP TABLE solana.slots"] {
        let forged = ValidSpec { filters: vec![ValidFilter { dimension: "mint", values: vec![hostile.into()] }], ..base.clone() };
        match compile(&forged, &c, window) {
            Err(SpecError::Internal(_)) => {}
            other => panic!("{hostile}: {other:?}"),
        }
    }
    let internal = |v: &ValidSpec, w: SlotWindow| matches!(compile(v, &c, w), Err(SpecError::Internal(_)));
    assert!(internal(&ValidSpec { measures: vec!["nope"], ..base.clone() }, window));
    assert!(internal(&ValidSpec { measures: vec!["tx_count"], ..base.clone() }, window), "a measure off the table");
    assert!(internal(&ValidSpec { measures: vec![], ..base.clone() }, window));
    assert!(internal(&base, SlotWindow { from_slot: 10, to_slot: 1, slots: 1 }));
    // A bucketed spec over a window that was not snapped.
    let bucketed = validate(
        &QuerySpec { group_by: vec![group("slot_bucket")], ..spec(&["transfer_count"]) },
        &c,
        &limits(),
    )
    .unwrap();
    assert!(internal(&bucketed, SlotWindow::new(5, 24)));
    assert!(compile(&bucketed, &c, SlotWindow::new(10, 29)).is_ok());
}

// ---------------------------------------------------------------------------
// The statement, rule by rule
// ---------------------------------------------------------------------------

#[test]
fn the_design_example_compiles_to_exactly_this() {
    let s = QuerySpec {
        group_by: vec![group("mint")],
        filters: vec![filter("token_program", FilterOp::In, &[KEY_A, KEY_B])],
        window: Window::LastSlots(20_000),
        order_by: vec![order("transfer_count", Direction::Desc)],
        limit: Some(10),
        ..spec(&["transfer_count", "transfer_volume_raw"])
    };
    let (_, out) = compile_at(&catalog(), &s, W);
    let mint = "COALESCE(mint_token_address, 'unattributed')";
    assert_eq!(
        out.sql,
        format!(
            "SELECT {mint} AS \"mint\", COUNT(*) AS \"transfer_count\", \
             CAST(SUM(CASE WHEN mint_token_address IS NOT NULL THEN CAST(amount_raw AS DECIMAL(38, 0)) END) AS VARCHAR) \
             AS \"transfer_volume_raw\" \
             FROM solana.token_transfers \
             WHERE slot >= 980001 AND slot <= 1000000 \
             AND ((spl_token_program_id = '{KEY_A}') OR (spl_token_program_id = '{KEY_B}')) \
             GROUP BY {mint} \
             ORDER BY COUNT(*) DESC NULLS LAST, {mint} ASC NULLS LAST \
             LIMIT 11"
        )
    );
    assert_eq!(out.window, SlotWindow::new(980_001, 1_000_000));
    let c = catalog();
    let volume = c.measure("transfer_volume_raw").unwrap();
    let mint_dim = c.dimension("mint").unwrap();
    assert_eq!(out.warnings, volume.warnings.iter().chain(mint_dim.warnings).map(|w| w.to_string()).collect::<Vec<_>>());
    assert_eq!(
        out.columns,
        vec![
            ColumnInfo { name: "mint", role: Role::Dimension, value_type: ValueType::Text, nullable: false, unit: None, bucket_width: None },
            ColumnInfo { name: "transfer_count", role: Role::Measure, value_type: ValueType::Integer, nullable: false, unit: Some(Unit::Count), bucket_width: None },
            ColumnInfo { name: "transfer_volume_raw", role: Role::Measure, value_type: ValueType::Decimal, nullable: true, unit: Some(Unit::RawTokenAmount), bucket_width: None },
        ]
    );
}

#[test]
fn the_select_list_is_dimensions_then_measures_in_request_order() {
    let s = QuerySpec {
        group_by: vec![group("tx_status"), group("slot_bucket"), group("slot")],
        ..spec(&["fee_avg_lamports", "tx_count", "tx_failure_ratio"])
    };
    let (_, out) = compile_at(&catalog(), &s, W);
    let names: Vec<_> = out.columns.iter().map(|c| (c.name, c.role)).collect();
    assert_eq!(
        names,
        [
            ("tx_status", Role::Dimension),
            ("slot_bucket", Role::Dimension),
            ("slot", Role::Dimension),
            ("fee_avg_lamports", Role::Measure),
            ("tx_count", Role::Measure),
            ("tx_failure_ratio", Role::Measure),
        ]
    );
    assert!(out.sql.starts_with("SELECT status AS \"tx_status\", CAST(slot AS BIGINT) / 10 AS \"slot_bucket\", slot AS \"slot\", AVG(fee)"), "{}", out.sql);
    let bucket = &out.columns[1];
    assert_eq!((bucket.value_type, bucket.nullable, bucket.bucket_width), (ValueType::Integer, false, Some(10)));
    assert_eq!((out.columns[2].value_type, out.columns[2].nullable), (ValueType::Integer, false));
    assert_eq!((out.columns[3].value_type, out.columns[3].nullable), (ValueType::Float, true));
    assert_eq!((out.columns[5].value_type, out.columns[5].unit), (ValueType::Float, Some(Unit::Ratio)));
}

#[test]
fn filters_test_the_raw_column_after_the_window() {
    let cases = [
        ("transfer_count", "mint", vec![KEY_A], format!("mint_token_address = '{KEY_A}'")),
        ("transfer_count", "source_owner", vec![KEY_A], format!("source_owner = '{KEY_A}'")),
        ("slot_count", "leader", vec![KEY_A], format!("leader = '{KEY_A}'")),
        ("tx_count", "tx_status", vec!["failed"], "status = 'failed'".to_owned()),
        ("program_count", "ix_level", vec!["top_level"], "inner_instruction_index IS NULL".to_owned()),
        ("program_count", "ix_decoded", vec!["decoded"], "instruction_type IS NOT NULL".to_owned()),
        ("tx_count", "slot", vec!["10..=20", "5"], "((slot >= 10 AND slot <= 20) OR (slot = 5))".to_owned()),
    ];
    for (m, d, values, expected) in cases {
        let s = QuerySpec { filters: vec![filter(d, FilterOp::In, &values)], ..spec(&[m]) };
        let sql = sql_of(&s);
        let w = format!("WHERE slot >= {} AND slot <= {W}", W - 399);
        assert!(sql.contains(&format!("{w} AND {expected}")), "{d}: {sql}");
        assert!(!sql.contains("COALESCE"), "a filter tests the expression: {sql}");
    }
}

#[test]
fn shared_measure_conditions_are_hoisted_and_differing_ones_folded() {
    // One filtered measure: its filter is a WHERE condition.
    let sql = sql_of(&spec(&["tx_failed_count"]));
    assert!(sql.contains("SELECT COUNT(*) AS \"tx_failed_count\""), "{sql}");
    assert!(sql.contains(&format!("slot <= {W} AND status = 'failed' LIMIT")), "{sql}");

    // With an unfiltered count, the filter would drop rows tx_count needs.
    let sql = sql_of(&spec(&["tx_count", "tx_failed_count"]));
    assert!(sql.contains("COUNT(*) AS \"tx_count\", COUNT(CASE WHEN status = 'failed' THEN 1 END) AS \"tx_failed_count\""), "{sql}");
    assert!(!sql.contains("AND status"), "{sql}");

    // No conditions anywhere: nothing to hoist.
    let sql = sql_of(&spec(&["tx_count", "fee_total_lamports"]));
    assert!(sql.contains(&format!("slot <= {W} LIMIT")), "{sql}");

    // The per-mint volume alone: rows with no mint are excluded in WHERE, so
    // no `unattributed` group appears at all.
    let s = QuerySpec { group_by: vec![group("mint")], ..spec(&["transfer_volume_raw"]) };
    let sql = sql_of(&s);
    assert!(sql.contains("CAST(SUM(CAST(amount_raw AS DECIMAL(38, 0))) AS VARCHAR) AS \"transfer_volume_raw\""), "{sql}");
    assert!(sql.contains(&format!("slot <= {W} AND mint_token_address IS NOT NULL GROUP BY")), "{sql}");

    // Two measures with the same conditions share them.
    let c = catalog();
    let mut measures: Vec<Measure> = MEASURES.to_vec();
    let failed_fees = Measure {
        id: "failed_fee_total",
        kind: oracle::catalog::MeasureKind::SumDecimal { column: "fee", filter: Some(Pred::EqStr("status", "failed")) },
        ..*c.measure("fee_total_lamports").unwrap()
    };
    measures.push(failed_fees);
    let c = Catalog::from_definitions(measures, DIMENSIONS.to_vec(), vec![]).unwrap();
    let (_, out) = compile_at(&c, &spec(&["tx_failed_count", "failed_fee_total"]), W);
    assert!(out.sql.contains("COUNT(*) AS \"tx_failed_count\", CAST(SUM(CAST(fee AS DECIMAL(38, 0))) AS VARCHAR)"), "{}", out.sql);
    assert!(out.sql.contains("AND status = 'failed' LIMIT"), "{}", out.sql);
}

#[test]
fn a_measure_that_requires_a_dimension_excludes_its_null_rows() {
    let c = catalog();
    // The built-in: the measure's own filter already says so; not doubled.
    let volume = c.measure("transfer_volume_raw").unwrap();
    assert_eq!(row_conditions(volume, Table::TokenTransfers, &c), [Pred::IsNotNull("mint_token_address")]);
    assert_eq!(row_conditions(c.measure("transfer_count").unwrap(), Table::TokenTransfers, &c), []);

    // Any measure that requires a nullable dimension gets the exclusion.
    let per_owner = Measure {
        id: "per_owner_transfers",
        requires_dimension: Some("source_owner"),
        ..*c.measure("transfer_count").unwrap()
    };
    let per_status = Measure { id: "per_status_txs", requires_dimension: Some("tx_status"), ..*c.measure("tx_count").unwrap() };
    let mut measures: Vec<Measure> = MEASURES.to_vec();
    measures.extend([per_owner, per_status]);
    let c = Catalog::from_definitions(measures, DIMENSIONS.to_vec(), vec![]).unwrap();
    assert_eq!(
        row_conditions(c.measure("per_owner_transfers").unwrap(), Table::TokenTransfers, &c),
        [Pred::IsNotNull("source_owner")]
    );
    // `status` is never null: nothing to exclude.
    assert_eq!(row_conditions(c.measure("per_status_txs").unwrap(), Table::Transactions, &c), []);

    let s = QuerySpec { group_by: vec![group("source_owner")], ..spec(&["transfer_count", "per_owner_transfers"]) };
    let (_, out) = compile_at(&c, &s, W);
    assert!(out.sql.contains("COUNT(CASE WHEN source_owner IS NOT NULL THEN 1 END) AS \"per_owner_transfers\""), "{}", out.sql);
}

#[test]
fn group_by_repeats_the_expressions_in_full() {
    let s = QuerySpec { group_by: vec![group("source_owner"), group("slot_bucket")], ..spec(&["transfer_count"]) };
    let sql = sql_of(&s);
    assert!(sql.contains("GROUP BY COALESCE(source_owner, 'unknown'), CAST(slot AS BIGINT) / 10 ORDER BY"), "{sql}");
    assert!(!sql.contains("GROUP BY \"") && !sql.contains("GROUP BY source_owner"), "{sql}");
    // Ungrouped: no GROUP BY and, unordered, no ORDER BY.
    let sql = sql_of(&spec(&["transfer_count"]));
    assert!(!sql.contains("GROUP BY") && !sql.contains("ORDER BY"), "{sql}");
}

#[test]
fn the_order_ends_with_every_group_by_dimension_ascending_nulls_last() {
    let mint = "COALESCE(mint_token_address, 'unattributed')";
    let owner = "COALESCE(source_owner, 'unknown')";
    let base = || QuerySpec { group_by: vec![group("mint"), group("source_owner")], ..spec(&["transfer_count"]) };
    let order_of = |s: &QuerySpec| {
        let sql = sql_of(s);
        let start = sql.find(" ORDER BY ").unwrap() + 10;
        let end = sql.rfind(" LIMIT ").unwrap();
        sql[start..end].to_owned()
    };

    // Nothing requested: the dimensions, in group-by order.
    assert_eq!(order_of(&base()), format!("{mint} ASC NULLS LAST, {owner} ASC NULLS LAST"));
    // A measure first, then the tie-breakers.
    assert_eq!(
        order_of(&QuerySpec { order_by: vec![order("transfer_count", Direction::Desc)], ..base() }),
        format!("COUNT(*) DESC NULLS LAST, {mint} ASC NULLS LAST, {owner} ASC NULLS LAST")
    );
    // A dimension ordered explicitly is not repeated.
    assert_eq!(
        order_of(&QuerySpec { order_by: vec![order("source_owner", Direction::Desc)], ..base() }),
        format!("{owner} DESC NULLS LAST, {mint} ASC NULLS LAST")
    );
    // A decimal sum sorts as a number, never as its text.
    let s = QuerySpec {
        group_by: vec![group("mint")],
        order_by: vec![order("transfer_volume_raw", Direction::Desc)],
        ..spec(&["transfer_count", "transfer_volume_raw"])
    };
    assert_eq!(
        order_of(&s),
        format!(
            "SUM(CASE WHEN mint_token_address IS NOT NULL THEN CAST(amount_raw AS DECIMAL(38, 0)) END) DESC NULLS LAST, \
             {mint} ASC NULLS LAST"
        )
    );
    // Ungrouped but ordered: the key alone.
    let s = QuerySpec { order_by: vec![order("fee_avg_lamports", Direction::Asc)], ..spec(&["fee_avg_lamports"]) };
    assert_eq!(order_of(&s), "AVG(fee) ASC NULLS LAST");
}

#[test]
fn the_limit_asks_for_one_more_row() {
    for (limit, asked) in [(Some(1), 2u64), (Some(10), 11), (None, 1_001), (Some(1_000), 1_001)] {
        let sql = sql_of(&QuerySpec { limit, ..spec(&["tx_count"]) });
        assert!(sql.ends_with(&format!(" LIMIT {asked}")), "{sql}");
    }
}

#[test]
fn warnings_are_catalog_text_measures_then_group_by_dimensions_each_once() {
    let c = catalog();
    let s = QuerySpec {
        group_by: vec![group("slot_bucket"), group("mint")],
        // A filter-only dimension adds nothing: its caveat is about grouping.
        filters: vec![filter("source_owner", FilterOp::Eq, &[KEY_A])],
        ..spec(&["mint_count", "transfer_volume_raw", "transfer_count"])
    };
    let (_, out) = compile_at(&c, &s, W);
    let catalog_text: Vec<&str> = c
        .measures()
        .iter()
        .flat_map(|m| m.warnings.iter())
        .chain(c.dimensions().iter().flat_map(|d| d.warnings.iter()))
        .copied()
        .collect();
    for w in &out.warnings {
        assert!(catalog_text.contains(&w.as_str()), "not catalog text: {w}");
    }
    let m = |id| c.measure(id).unwrap().warnings.to_vec();
    let d = |id| c.dimension(id).unwrap().warnings.to_vec();
    let mut expected: Vec<&str> = Vec::new();
    for w in m("mint_count").into_iter().chain(m("transfer_volume_raw")).chain(d("slot_bucket")).chain(d("mint")) {
        if !expected.contains(&w) {
            expected.push(w);
        }
    }
    assert_eq!(out.warnings, expected);
    // The unknown-mint caveat is on both measures and appears once.
    let shared = "Transfers whose mint PRISM could not resolve are not counted.";
    assert_eq!(out.warnings.iter().filter(|w| *w == shared).count(), 1);
    assert!(!out.warnings.iter().any(|w| w.contains("source owner")), "{:?}", out.warnings);
    // The bucket caveat describes the snap, which is what spec/ does.
    assert!(out.warnings.iter().any(|w| w.contains("snapped inward")), "{:?}", out.warnings);
    // No caveats, no warnings.
    assert!(compile_at(&c, &spec(&["tx_count"]), W).1.warnings.is_empty());
}
