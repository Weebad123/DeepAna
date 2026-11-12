//! The catalog without LENS: definition consistency, startup validation and
//! entity lookups.
//!
//! * every definition points at real columns of the right type, checked
//!   independently of `Catalog::new`'s own validation;
//! * every validation rule fires, with the error it should, in order;
//! * every built-in and extra entity is found by every form of every key,
//!   reverse lookups are exact, and `Ambiguous` never comes back;
//! * the built-in addresses are the SDK's own constants.

use std::collections::BTreeSet;
use std::sync::Arc;

use oracle::catalog::*;

fn catalog() -> Catalog {
    Catalog::new(vec![]).unwrap()
}

/// A valid key no built-in uses: `[n; 32]` in base58.
fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

fn def(name: &str, aliases: &[&str], address: &str) -> EntityDef {
    EntityDef {
        name: name.to_owned(),
        aliases: aliases.iter().map(|a| (*a).to_owned()).collect(),
        address: address.to_owned(),
        kind: EntityKind::Program,
    }
}

fn measure(id: &str) -> Measure {
    *MEASURES.iter().find(|m| m.id == id).unwrap()
}

fn dimension(id: &str) -> Dimension {
    *DIMENSIONS.iter().find(|d| d.id == id).unwrap()
}

/// The built-ins with `m` in place of the measure of the same id (or added).
fn with_measure(m: Measure) -> Result<Catalog, CatalogError> {
    let mut ms: Vec<Measure> = MEASURES.iter().copied().filter(|x| x.id != m.id).collect();
    ms.push(m);
    Catalog::from_definitions(ms, DIMENSIONS.to_vec(), vec![])
}

fn with_dimension(d: Dimension) -> Result<Catalog, CatalogError> {
    let mut ds: Vec<Dimension> = DIMENSIONS.iter().copied().filter(|x| x.id != d.id).collect();
    ds.push(d);
    Catalog::from_definitions(MEASURES.to_vec(), ds, vec![])
}

fn with_extras(extras: Vec<EntityDef>) -> Result<Catalog, CatalogError> {
    Catalog::new(extras)
}

#[track_caller]
fn assert_bad_definition(r: Result<Catalog, CatalogError>, item: &str, reason_has: &str) {
    match r {
        Err(CatalogError::BadDefinition { item: i, reason }) => {
            assert_eq!(i, item);
            assert!(reason.contains(reason_has), "{reason:?} lacks {reason_has:?}");
        }
        other => panic!("expected BadDefinition for {item}, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The built-in set
// ---------------------------------------------------------------------------

#[test]
fn the_catalog_is_send_sync_and_shareable() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Catalog>();
    let shared = Arc::new(catalog());
    let other = Arc::clone(&shared);
    std::thread::spawn(move || assert!(other.measure("tx_count").is_some())).join().unwrap();
}

#[test]
fn the_measure_set_is_exactly_the_specified_one_sorted_by_id() {
    let c = catalog();
    let ids: Vec<&str> = c.measures().iter().map(|m| m.id).collect();
    assert_eq!(
        ids,
        [
            "compute_units_avg",
            "fee_avg_lamports",
            "fee_total_lamports",
            "ix_cpi_count",
            "ix_undecoded_count",
            "mint_count",
            "program_count",
            "slot_count",
            "transfer_count",
            "transfer_unattributed_count",
            "transfer_volume_raw",
            "tx_count",
            "tx_failed_count",
            "tx_failure_ratio",
        ]
    );
    for m in c.measures() {
        assert_eq!(c.measure(m.id), Some(m));
    }
    assert_eq!(c.measure("nope"), None);
    assert_eq!(c.measure(""), None);
}

#[test]
fn dimensions_are_sorted_and_found_by_id() {
    let c = catalog();
    let ids: Vec<&str> = c.dimensions().iter().map(|d| d.id).collect();
    let mut sorted = ids.clone();
    sorted.sort_unstable();
    assert_eq!(ids, sorted);
    assert_eq!(ids.len(), DIMENSIONS.len());
    for d in c.dimensions() {
        assert_eq!(c.dimension(d.id), Some(d));
    }
    assert_eq!(c.dimension("hour"), None, "decision 8: no time dimensions");
    assert_eq!(c.dimension("day"), None);
}

#[test]
fn the_specified_kinds_and_units() {
    use MeasureKind::*;
    let c = catalog();
    let kind = |id| c.measure(id).unwrap().kind;
    let unit = |id| c.measure(id).unwrap().unit;
    assert_eq!(kind("slot_count"), Count { filter: None });
    assert_eq!(kind("tx_failed_count"), Count { filter: Some(Pred::EqStr("status", "failed")) });
    assert_eq!(kind("tx_failure_ratio"), Ratio { numerator: Pred::EqStr("status", "failed"), denominator: None });
    assert_eq!(kind("program_count"), CountDistinct { column: "program_id" });
    assert_eq!(kind("mint_count"), CountDistinct { column: "mint_token_address" });
    assert_eq!(kind("fee_avg_lamports"), Avg { column: "fee" });
    assert_eq!(kind("ix_undecoded_count"), Count { filter: Some(Pred::IsNull("instruction_type")) });
    assert_eq!(unit("fee_total_lamports"), Unit::Lamports);
    assert_eq!(unit("compute_units_avg"), Unit::ComputeUnits);
    assert_eq!(unit("tx_failure_ratio"), Unit::Ratio);
    assert_eq!(unit("transfer_volume_raw"), Unit::RawTokenAmount);
    assert_eq!(c.measure("transfer_volume_raw").unwrap().requires_dimension, Some("mint"));
    // Decision 5.
    for m in c.measures() {
        if matches!(m.kind, Count { .. }) {
            assert_eq!(m.select_expr(), "COUNT(*)", "{}", m.id);
        }
    }
}

#[test]
fn only_slot_bucket_is_parameterised() {
    let c = catalog();
    let p: Vec<&str> = c.dimensions().iter().filter(|d| d.parameterised).map(|d| d.id).collect();
    assert_eq!(p, ["slot_bucket"]);
    let b = c.dimension("slot_bucket").unwrap();
    assert!(!b.filterable);
    assert_eq!(b.table, DimTable::Any);
    assert_eq!(b.values, ValueRule::BucketWidth);
}

// ---------------------------------------------------------------------------
// Definition consistency, checked independently of `Catalog::new`
// ---------------------------------------------------------------------------

fn col(c: &Catalog, table: Table, name: &str) -> Column {
    *c.table_columns(table)
        .iter()
        .find(|col| col.name == name)
        .unwrap_or_else(|| panic!("{table}.{name} does not exist"))
}

#[track_caller]
fn assert_pred_fits(c: &Catalog, table: Table, p: &Pred) {
    let column = col(c, table, p.column());
    match p {
        Pred::IsNull(_) | Pred::IsNotNull(_) => assert!(column.nullable, "{table}.{} is never NULL", column.name),
        Pred::EqStr(..) => assert_eq!(column.ty, ColType::Utf8),
        Pred::EqBool(..) => assert_eq!(column.ty, ColType::Bool),
    }
}

#[test]
fn every_measure_points_at_real_columns_of_the_right_type() {
    use MeasureKind::*;
    let c = catalog();
    for m in c.measures() {
        for name in m.columns() {
            col(&c, m.table, name);
        }
        match m.kind {
            Count { filter } => {
                filter.iter().for_each(|p| assert_pred_fits(&c, m.table, p));
                assert_eq!(m.unit, Unit::Count, "{}", m.id);
            }
            CountDistinct { column } => {
                col(&c, m.table, column);
                assert_eq!(m.unit, Unit::Count, "{}", m.id);
            }
            SumDecimal { column, filter } => {
                assert!(col(&c, m.table, column).ty.is_integer(), "{}", m.id);
                filter.iter().for_each(|p| assert_pred_fits(&c, m.table, p));
            }
            Avg { column } => assert!(col(&c, m.table, column).ty.is_integer(), "{}", m.id),
            Ratio { numerator, denominator } => {
                assert_pred_fits(&c, m.table, &numerator);
                denominator.iter().for_each(|p| assert_pred_fits(&c, m.table, p));
                assert_eq!(m.unit, Unit::Ratio, "{}", m.id);
            }
        }
        assert!(!m.label.is_empty() && !m.description.is_empty(), "{}", m.id);
        assert!(m.warnings.iter().all(|w| !w.trim().is_empty()), "{}", m.id);
        if let Some(d) = m.requires_dimension {
            assert!(c.compatible(m.id, d), "{} requires {d}", m.id);
            assert!(c.dimension(d).unwrap().filterable);
        }
    }
}

#[test]
fn every_dimension_points_at_real_columns_of_the_right_type_on_every_table_it_is_on() {
    let c = catalog();
    for d in c.dimensions() {
        let tables = match d.table {
            DimTable::One(t) => vec![t],
            DimTable::Any => Table::ALL.to_vec(),
        };
        for t in tables {
            let column = col(&c, t, d.raw_column());
            match (d.expr, d.values) {
                (DimExpr::SlotBucket(_), ValueRule::BucketWidth) | (DimExpr::Column(_), ValueRule::SlotRange) => {
                    assert_eq!((column.ty, column.nullable), (ColType::U64, false), "{}", d.id);
                }
                (DimExpr::Column(_), ValueRule::Base58Key | ValueRule::Enum(_)) => {
                    assert_eq!(column.ty, ColType::Utf8, "{}", d.id);
                }
                (DimExpr::Coalesce(_, lit), ValueRule::Base58Key) => {
                    assert_eq!(column.ty, ColType::Utf8, "{}", d.id);
                    assert!(column.nullable, "{}: COALESCE on a non-null column", d.id);
                    assert!(!is_base58_key(lit), "{}", d.id);
                }
                (DimExpr::Case { when, then, otherwise }, ValueRule::Enum(values)) => {
                    assert_pred_fits(&c, t, &when);
                    let mut labels = [then, otherwise];
                    labels.sort_unstable();
                    assert_eq!(values, labels, "{}", d.id);
                }
                other => panic!("{}: unexpected shape {other:?}", d.id),
            }
        }
    }
}

#[test]
fn any_table_dimensions_only_use_columns_every_table_has() {
    let c = catalog();
    for d in c.dimensions().iter().filter(|d| d.table == DimTable::Any) {
        for t in Table::ALL {
            assert!(c.table_columns(t).iter().any(|col| col.name == d.raw_column()), "{} on {t}", d.id);
        }
    }
}

#[test]
fn compatibility_follows_the_tables() {
    let c = catalog();
    for m in c.measures() {
        for d in c.dimensions() {
            let expected = match d.table {
                DimTable::Any => true,
                DimTable::One(t) => t == m.table,
            };
            assert_eq!(c.compatible(m.id, d.id), expected, "{} × {}", m.id, d.id);
        }
    }
    assert!(c.compatible("tx_count", "slot_bucket"));
    assert!(c.compatible("transfer_volume_raw", "mint"));
    assert!(!c.compatible("tx_count", "mint"));
    assert!(!c.compatible("transfer_count", "program"));
    assert!(!c.compatible("nope", "slot"));
    assert!(!c.compatible("tx_count", "nope"));
}

#[test]
fn raw_token_amounts_are_never_summed_across_mints() {
    let c = catalog();
    for m in c.measures().iter().filter(|m| m.unit == Unit::RawTokenAmount) {
        assert!(matches!(m.kind, MeasureKind::SumDecimal { .. }), "{}", m.id);
        let d = c.dimension(m.requires_dimension.expect("requires the mint")).unwrap();
        assert_eq!(d.raw_column(), "mint_token_address");
        assert_eq!(m.filter(), Some(&Pred::IsNotNull("mint_token_address")));
        assert!(m.warnings.iter().any(|w| w.contains("decimals")), "{}", m.id);
    }
    // Nothing else reads amount_raw.
    for m in c.measures().iter().filter(|m| m.unit != Unit::RawTokenAmount) {
        assert!(!m.columns().contains(&"amount_raw"), "{}", m.id);
    }
}

#[test]
fn rendered_sql_is_built_from_identifiers_and_safe_literals_only() {
    let c = catalog();
    let safe = |s: &str| s.chars().all(|ch| ch.is_ascii_alphanumeric() || " _(),*'=/".contains(ch));
    for m in c.measures() {
        assert!(safe(&m.select_expr()), "{}: {}", m.id, m.select_expr());
        if let Some(p) = m.filter() {
            assert!(safe(&p.sql()), "{}", m.id);
        }
    }
    for d in c.dimensions() {
        let param = d.parameterised.then_some(150);
        assert!(safe(&d.select_expr(param)), "{}", d.id);
    }
}

#[test]
fn table_columns_mirror_the_constants() {
    let c = catalog();
    assert_eq!(c.table_columns(Table::Slots), &SLOTS_COLUMNS);
    assert_eq!(c.table_columns(Table::Transactions), &TRANSACTIONS_COLUMNS);
    assert_eq!(c.table_columns(Table::Instructions), &INSTRUCTIONS_COLUMNS);
    assert_eq!(c.table_columns(Table::TokenTransfers), &TOKEN_TRANSFERS_COLUMNS);
    for t in Table::ALL {
        assert_eq!(c.table_columns(t)[0], Column { name: "slot", ty: ColType::U64, nullable: false });
    }
}

#[test]
fn limits() {
    let l = catalog().limits();
    assert_eq!(l.max_bucket_width_slots, MAX_BUCKET_WIDTH_SLOTS);
    assert_eq!(l.max_slot, i64::MAX as u64);
    assert_eq!((l.max_entity_aliases, l.max_entity_name_len, l.max_extra_entities), (8, 64, 1024));
}

#[test]
fn the_catalog_is_deterministic() {
    let a = catalog();
    let b = catalog();
    assert_eq!(a.measures(), b.measures());
    assert_eq!(a.dimensions(), b.dimensions());
    assert_eq!(a.entities(), b.entities());
    assert_eq!(serde_json::to_string(a.measures()).unwrap(), serde_json::to_string(b.measures()).unwrap());

    // Input order changes nothing.
    let mut ms = MEASURES.to_vec();
    ms.reverse();
    let mut ds = DIMENSIONS.to_vec();
    ds.reverse();
    let extras = vec![def("Zeta", &[], &key(1)), def("Alpha", &[], &key(2))];
    let mut rev = extras.clone();
    rev.reverse();
    let x = Catalog::from_definitions(ms, ds, extras).unwrap();
    let y = Catalog::new(rev).unwrap();
    assert_eq!(x.measures(), a.measures());
    assert_eq!(x.dimensions(), a.dimensions());
    assert_eq!(x.entities(), y.entities());
}

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------

#[test]
fn duplicate_and_malformed_ids() {
    let mut ms = MEASURES.to_vec();
    ms.push(measure("tx_count"));
    assert_eq!(
        Catalog::from_definitions(ms, DIMENSIONS.to_vec(), vec![]).unwrap_err(),
        CatalogError::DuplicateId { kind: "measure", id: "tx_count".into() }
    );
    let mut ds = DIMENSIONS.to_vec();
    ds.push(dimension("mint"));
    assert_eq!(
        Catalog::from_definitions(MEASURES.to_vec(), ds, vec![]).unwrap_err(),
        CatalogError::DuplicateId { kind: "dimension", id: "mint".into() }
    );
    // A measure and a dimension may share an id: they are separate namespaces.
    assert!(with_dimension(Dimension { id: "tx_count", ..dimension("tx_status") }).is_ok());

    for id in ["", "Tx", "1x", "a-b", "a b"] {
        assert!(matches!(with_measure(Measure { id, ..measure("tx_count") }), Err(CatalogError::BadDefinition { .. })));
        assert!(matches!(
            with_dimension(Dimension { id, ..dimension("tx_status") }),
            Err(CatalogError::BadDefinition { .. })
        ));
    }
}

#[test]
fn unknown_columns() {
    use MeasureKind::*;
    let err = with_measure(Measure { kind: CountDistinct { column: "nope" }, ..measure("program_count") });
    assert_eq!(
        err.unwrap_err(),
        CatalogError::UnknownColumn { item: "measure program_count".into(), column: "instructions.nope".into() }
    );
    // A column of another table.
    let err = with_measure(Measure { kind: Avg { column: "fee" }, ..measure("compute_units_avg") });
    assert!(err.is_ok(), "fee is on transactions");
    let err = with_measure(Measure { kind: Avg { column: "amount_raw" }, ..measure("compute_units_avg") });
    assert!(matches!(err, Err(CatalogError::UnknownColumn { column, .. }) if column == "transactions.amount_raw"));
    let err = with_measure(Measure { kind: Count { filter: Some(Pred::IsNull("nope")) }, ..measure("tx_count") });
    assert!(matches!(err, Err(CatalogError::UnknownColumn { .. })));

    let err = with_dimension(Dimension { expr: DimExpr::Column("nope"), ..dimension("program") });
    assert!(matches!(err, Err(CatalogError::UnknownColumn { .. })));
    // An `Any` dimension on a column only one table has.
    let err = with_dimension(Dimension { table: DimTable::Any, ..dimension("program") });
    assert_eq!(
        err.unwrap_err(),
        CatalogError::UnknownColumn { item: "dimension program".into(), column: "slots.program_id".into() }
    );
}

#[test]
fn type_mismatches() {
    use MeasureKind::*;
    let err = with_measure(Measure { kind: SumDecimal { column: "status", filter: None }, ..measure("fee_total_lamports") });
    assert_eq!(
        err.unwrap_err(),
        CatalogError::TypeMismatch {
            item: "measure fee_total_lamports".into(),
            column: "transactions.status".into(),
            expected: "an integer (U8, U32, U64, I64)".into(),
            found: "Utf8".into(),
        }
    );
    let err = with_measure(Measure { kind: Avg { column: "blockhash" }, ..measure("fee_avg_lamports") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { .. })));
    let err = with_measure(Measure { kind: Count { filter: Some(Pred::EqStr("fee", "x")) }, ..measure("tx_count") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { expected, .. }) if expected == "Utf8"));
    let err = with_measure(Measure { kind: Count { filter: Some(Pred::EqBool("status", true)) }, ..measure("tx_count") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { expected, .. }) if expected == "Bool"));

    let err = with_dimension(Dimension { expr: DimExpr::Column("fee"), ..dimension("tx_status") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { .. })));
    let err = with_dimension(Dimension { expr: DimExpr::SlotBucket("blockhash"), table: DimTable::One(Table::Slots), ..dimension("slot_bucket") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { .. })));
    let err = with_dimension(Dimension { expr: DimExpr::Column("fee"), values: ValueRule::Bool, ..dimension("tx_status") });
    assert!(matches!(err, Err(CatalogError::TypeMismatch { expected, .. }) if expected == "Bool"));
}

#[test]
fn bad_measure_definitions() {
    use MeasureKind::*;
    let m = "measure tx_count";
    assert_bad_definition(
        with_measure(Measure { kind: Count { filter: Some(Pred::IsNull("fee")) }, ..measure("tx_count") }),
        m,
        "never NULL",
    );
    assert_bad_definition(
        with_measure(Measure { kind: Count { filter: Some(Pred::EqStr("status", "fail'ed")) }, ..measure("tx_count") }),
        m,
        "literal",
    );
    assert_bad_definition(with_measure(Measure { unit: Unit::Lamports, ..measure("tx_count") }), m, "unit");
    assert_bad_definition(with_measure(Measure { label: " ", ..measure("tx_count") }), m, "label");
    assert_bad_definition(with_measure(Measure { description: "", ..measure("tx_count") }), m, "description");
    assert_bad_definition(with_measure(Measure { warnings: &[""], ..measure("tx_count") }), m, "warning");
    assert_bad_definition(
        with_measure(Measure { requires_dimension: Some("nope"), ..measure("tx_count") }),
        m,
        "unknown dimension",
    );
    assert_bad_definition(
        with_measure(Measure { requires_dimension: Some("mint"), ..measure("tx_count") }),
        m,
        "not on transactions",
    );
    assert_bad_definition(
        with_measure(Measure { requires_dimension: Some("slot_bucket"), ..measure("tx_count") }),
        m,
        "cannot be filtered",
    );

    let r = "measure tx_failure_ratio";
    let same = Pred::EqStr("status", "failed");
    assert_bad_definition(
        with_measure(Measure { kind: Ratio { numerator: same, denominator: Some(same) }, ..measure("tx_failure_ratio") }),
        r,
        "constant",
    );
    assert_bad_definition(with_measure(Measure { unit: Unit::Count, ..measure("tx_failure_ratio") }), r, "unit");
    assert_bad_definition(
        with_measure(Measure { unit: Unit::RawTokenAmount, ..measure("fee_avg_lamports") }),
        "measure fee_avg_lamports",
        "unit",
    );
}

#[test]
fn a_raw_token_sum_must_be_per_mint() {
    use MeasureKind::*;
    let v = "measure transfer_volume_raw";
    assert_bad_definition(
        with_measure(Measure { requires_dimension: None, ..measure("transfer_volume_raw") }),
        v,
        "require the dimension",
    );
    assert_bad_definition(
        with_measure(Measure {
            kind: SumDecimal { column: "amount_raw", filter: None },
            ..measure("transfer_volume_raw")
        }),
        v,
        "IS NOT NULL",
    );
    assert_bad_definition(with_measure(Measure { warnings: &[], ..measure("transfer_volume_raw") }), v, "decimals");
    // Required by a dimension on another column, the filter must follow it.
    assert_bad_definition(
        with_measure(Measure { requires_dimension: Some("source_owner"), ..measure("transfer_volume_raw") }),
        v,
        "source_owner IS NOT NULL",
    );
}

#[test]
fn bad_dimension_definitions() {
    let i = |id: &str| format!("dimension {id}");
    assert_bad_definition(
        with_dimension(Dimension { expr: DimExpr::Coalesce("program_id", "x"), ..dimension("program") }),
        &i("program"),
        "never NULL",
    );
    assert_bad_definition(
        with_dimension(Dimension { expr: DimExpr::Coalesce("mint_token_address", "no'pe"), ..dimension("mint") }),
        &i("mint"),
        "placeholder",
    );
    assert_bad_definition(
        with_dimension(Dimension {
            expr: DimExpr::Coalesce("mint_token_address", "11111111111111111111111111111111"),
            ..dimension("mint")
        }),
        &i("mint"),
        "mistaken for an address",
    );
    assert_bad_definition(
        with_dimension(Dimension { values: ValueRule::Enum(&["inner", "other"]), ..dimension("ix_level") }),
        &i("ix_level"),
        "two distinct labels",
    );
    assert_bad_definition(
        with_dimension(Dimension { values: ValueRule::Enum(&["success", "failed"]), ..dimension("tx_status") }),
        &i("tx_status"),
        "sorted",
    );
    assert_bad_definition(
        with_dimension(Dimension { values: ValueRule::Enum(&[]), ..dimension("tx_status") }),
        &i("tx_status"),
        "at least one",
    );
    assert_bad_definition(
        with_dimension(Dimension { parameterised: true, ..dimension("program") }),
        &i("program"),
        "parameterised",
    );
    assert_bad_definition(
        with_dimension(Dimension { filterable: true, ..dimension("slot_bucket") }),
        &i("slot_bucket"),
        "cannot be filtered",
    );
    assert_bad_definition(
        with_dimension(Dimension { values: ValueRule::BucketWidth, ..dimension("slot") }),
        &i("slot"),
        "bucket width",
    );
    assert_bad_definition(
        with_dimension(Dimension { expr: DimExpr::Column("block_height"), table: DimTable::One(Table::Slots), ..dimension("slot") }),
        &i("slot"),
        "non-null",
    );
    assert_bad_definition(with_dimension(Dimension { label: "", ..dimension("mint") }), &i("mint"), "label");
}

#[test]
fn checks_run_in_the_documented_order() {
    use MeasureKind::*;
    // Ids before definitions before entities.
    let mut ms = MEASURES.to_vec();
    ms.push(measure("tx_count"));
    ms.push(Measure { id: "broken", kind: Avg { column: "nope" }, ..measure("fee_avg_lamports") });
    let err = Catalog::from_definitions(ms, DIMENSIONS.to_vec(), vec![def("", &[], "bad")]).unwrap_err();
    assert!(matches!(err, CatalogError::DuplicateId { kind: "measure", .. }), "{err:?}");

    // Dimensions before measures.
    let mut ms = MEASURES.to_vec();
    ms.push(Measure { id: "broken", kind: Avg { column: "nope" }, ..measure("fee_avg_lamports") });
    let mut ds = DIMENSIONS.to_vec();
    ds.push(Dimension { id: "broken", expr: DimExpr::Column("nope"), ..dimension("program") });
    let err = Catalog::from_definitions(ms.clone(), ds, vec![]).unwrap_err();
    assert!(matches!(err, CatalogError::UnknownColumn { ref item, .. } if item == "dimension broken"), "{err:?}");

    // Measures before entities.
    let err = Catalog::from_definitions(ms, DIMENSIONS.to_vec(), vec![def("", &[], "bad")]).unwrap_err();
    assert!(matches!(err, CatalogError::UnknownColumn { ref item, .. } if item == "measure broken"), "{err:?}");

    // Within one kind, by id, whatever the input order.
    let mut ms = MEASURES.to_vec();
    ms.push(Measure { id: "zz_broken", kind: Avg { column: "nope" }, ..measure("fee_avg_lamports") });
    ms.push(Measure { id: "aa_broken", kind: Avg { column: "nope" }, ..measure("fee_avg_lamports") });
    let err = Catalog::from_definitions(ms, DIMENSIONS.to_vec(), vec![]).unwrap_err();
    assert!(matches!(err, CatalogError::UnknownColumn { ref item, .. } if item == "measure aa_broken"), "{err:?}");
}

// ---------------------------------------------------------------------------
// Entities: validation
// ---------------------------------------------------------------------------

#[test]
fn bad_extra_entities() {
    for d in [
        def("", &[], &key(1)),
        def("a'b", &[], &key(1)),
        def("x", &[], "not base58"),
        def("x", &[], &bs58::encode([1u8; 31]).into_string()),
        def("x", &[], &bs58::encode([1u8; 33]).into_string()),
        def("x", &[" X "], &key(1)),
        def("x", &["a", "b", "c", "d", "e", "f", "g", "h", "i"], &key(1)),
        def("11111111111111111111111111111111", &[], &key(1)),
    ] {
        assert!(matches!(with_extras(vec![d.clone()]), Err(CatalogError::BadEntity { .. })), "{d:?}");
    }
    let many: Vec<EntityDef> = (0..=MAX_EXTRA_ENTITIES).map(|i| def(&format!("e{i}"), &[], &key(1))).collect();
    assert!(matches!(with_extras(many), Err(CatalogError::BadEntity { .. })));
}

#[test]
fn as_many_extras_as_the_limit_allows() {
    let extras: Vec<EntityDef> = (0..MAX_EXTRA_ENTITIES)
        .map(|i| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64 + 1).to_be_bytes());
            bytes[31] = 0xAB;
            def(&format!("extra {i}"), &[&format!("x{i}")], &bs58::encode(bytes).into_string())
        })
        .collect();
    let c = with_extras(extras).unwrap();
    assert_eq!(c.entities().len(), BUILTIN_ENTITIES.len() + MAX_EXTRA_ENTITIES);
    assert!(matches!(c.entity("Extra 17"), EntityLookup::Found(e) if e.slug == "extra-17"));
    assert!(matches!(c.entity("x1023"), EntityLookup::Found(e) if e.name == "extra 1023"));
}

#[test]
fn an_extra_that_collides_with_a_built_in_is_an_error_not_an_override() {
    let conflict = |r: Result<Catalog, CatalogError>| match r {
        Err(CatalogError::EntityConflict { key, existing, new }) => (key, existing, new),
        other => panic!("expected a conflict, got {other:?}"),
    };
    // Its name, normalised.
    assert_eq!(
        conflict(with_extras(vec![def("system_PROGRAM", &[], &key(1))])),
        ("system program".into(), "System Program".into(), "system_PROGRAM".into())
    );
    // A built-in's alias.
    assert_eq!(
        conflict(with_extras(vec![def("Mine", &["ATA"], &key(1))])),
        ("ata".into(), "Associated Token Account".into(), "Mine".into())
    );
    // Punctuation does not dodge it: "A.T.A" is "a t a", a different key.
    assert!(with_extras(vec![def("Mine", &["A.T.A"], &key(1))]).is_ok());
    // Its address.
    assert_eq!(
        conflict(with_extras(vec![def("Mine", &[], "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")])),
        ("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into(), "SPL Token".into(), "Mine".into())
    );
}

#[test]
fn two_extras_may_not_share_a_key_or_an_address() {
    let r = with_extras(vec![def("One", &["shared"], &key(1)), def("Two", &["Shared"], &key(2))]);
    assert_eq!(
        r.unwrap_err(),
        CatalogError::EntityConflict { key: "shared".into(), existing: "One".into(), new: "Two".into() }
    );
    let r = with_extras(vec![def("One", &[], &key(1)), def("Two", &[], &key(1))]);
    assert_eq!(r.unwrap_err(), CatalogError::EntityConflict { key: key(1), existing: "One".into(), new: "Two".into() });
    // A name and another's alias.
    let r = with_extras(vec![def("One", &["two"], &key(1)), def("Two", &[], &key(2))]);
    assert!(matches!(r, Err(CatalogError::EntityConflict { key, .. }) if key == "two"));
}

#[test]
fn a_key_may_not_be_another_entitys_address_lowercased() {
    // `[7; 32]` has an `L`, so lowercased it is no longer base58 and passes
    // as a name. It must still not shadow the address, in either order.
    let addr = key(7);
    assert!(addr.contains('L'));
    let lower = addr.to_lowercase();
    assert!(!is_base58_key(&lower));
    for extras in [
        vec![def("Owner", &[], &addr), def("Squatter", &[&lower], &key(8))],
        vec![def("Squatter", &[&lower], &key(8)), def("Owner", &[], &addr)],
    ] {
        assert!(matches!(with_extras(extras), Err(CatalogError::EntityConflict { key, .. }) if key == lower));
    }
}

// ---------------------------------------------------------------------------
// Entities: lookups
// ---------------------------------------------------------------------------

/// Spellings of `s` that normalise to the same key.
fn variants(s: &str) -> Vec<String> {
    vec![
        s.to_owned(),
        s.to_uppercase(),
        s.to_lowercase(),
        format!("  {s}\t"),
        s.replace(' ', "-"),
        s.replace(' ', "_"),
        s.replace(' ', "."),
        s.replace(' ', "  "),
        s.replace(' ', " - "),
    ]
}

#[track_caller]
fn assert_found(c: &Catalog, query: &str, address: &str) {
    match c.entity(query) {
        EntityLookup::Found(e) => assert_eq!(e.address, address, "{query:?}"),
        other => panic!("{query:?}: {other:?}"),
    }
}

fn extras() -> Vec<EntityDef> {
    vec![
        EntityDef { kind: EntityKind::Mint, ..def("Devnet USDC", &["usdc", "usd coin"], &key(3)) },
        EntityDef { kind: EntityKind::Account, ..def("Treasury", &["main.treasury"], &key(4)) },
        def("my_program-v2", &[], &key(5)),
    ]
}

#[test]
fn every_built_in_is_found_by_every_key_in_every_spelling() {
    let c = catalog();
    for b in BUILTIN_ENTITIES {
        for s in std::iter::once(b.name).chain(b.aliases.iter().copied()) {
            for v in variants(s) {
                assert_found(&c, &v, b.address);
            }
        }
        assert_found(&c, b.address, b.address);
        let e = c.entities().into_iter().find(|e| e.address == b.address).unwrap();
        assert_found(&c, &e.slug, b.address);
        assert_eq!((e.name.as_str(), e.kind), (b.name, b.kind));
    }
}

#[test]
fn extras_are_found_like_built_ins() {
    let c = with_extras(extras()).unwrap();
    for d in extras() {
        for s in std::iter::once(&d.name).chain(&d.aliases) {
            for v in variants(s) {
                assert_found(&c, &v, &d.address);
            }
        }
        assert_found(&c, &d.address, &d.address);
        assert_eq!(c.entity_name(&d.address), Some(d.name.as_str()));
    }
    let e = c.entities().into_iter().find(|e| e.address == key(5)).unwrap();
    assert_eq!((e.name.as_str(), e.slug.as_str()), ("my_program-v2", "my-program-v2"));
    assert_found(&c, "My Program V2", &key(5));
    assert!(matches!(c.entity("Devnet USDC"), EntityLookup::Found(e) if e.kind == EntityKind::Mint));
    // Extras are not in a plain catalog.
    assert_eq!(catalog().entity("usdc"), EntityLookup::NotFound);
}

#[test]
fn unknown_names_are_not_found() {
    let c = with_extras(extras()).unwrap();
    for q in ["", "   ", "-_.", "spl", "token", "system programs", "s y s t e m", "usdcc", "unknown", "unattributed"] {
        assert_eq!(c.entity(q), EntityLookup::NotFound, "{q:?}");
    }
    // An address nobody owns.
    assert_eq!(c.entity(&key(200)), EntityLookup::NotFound);
}

#[test]
fn addresses_are_exact() {
    let c = with_extras(extras()).unwrap();
    for e in c.entities() {
        assert_eq!(c.entity_name(&e.address), Some(e.name.as_str()));
        if e.address.chars().any(|ch| ch.is_ascii_uppercase()) {
            let lower = e.address.to_lowercase();
            assert_eq!(c.entity_name(&lower), None, "{}", e.name);
            assert_eq!(c.entity(&lower), EntityLookup::NotFound, "{}: lowercased", e.name);
        }
        if e.address.chars().any(|ch| ch.is_ascii_lowercase()) {
            assert_eq!(c.entity_name(&e.address.to_uppercase()), None, "{}", e.name);
        }
        assert_eq!(c.entity_name(&format!(" {}", e.address)), None);
        assert_eq!(c.entity_name(&format!("{} ", e.address)), None);
        assert_eq!(c.entity_name(&e.name), None, "names are not addresses");
        assert_eq!(c.entity(&format!("{} ", e.address)), EntityLookup::NotFound, "{}", e.name);
    }
    assert_eq!(c.entity_name(""), None);
    assert_eq!(c.entity_name(&key(200)), None);
    assert_eq!(c.entity_name("not an address"), None);
}

#[test]
fn ambiguous_never_comes_from_one_catalog() {
    let c = with_extras(extras()).unwrap();
    let mut queries: BTreeSet<String> = BTreeSet::new();
    for e in c.entities() {
        for s in std::iter::once(&e.name).chain(&e.aliases).chain([&e.slug, &e.address]) {
            queries.extend(variants(s));
        }
        queries.insert(e.address.to_lowercase());
        queries.insert(e.address.to_uppercase());
    }
    queries.extend(["", " ", "x", "system", "1"].map(String::from));
    for q in &queries {
        assert!(!matches!(c.entity(q), EntityLookup::Ambiguous(_)), "{q:?} is ambiguous");
    }
    assert!(queries.len() > 500, "{}", queries.len());
}

#[test]
fn entities_are_sorted_by_slug_and_slugs_are_unique() {
    let c = with_extras(extras()).unwrap();
    let slugs: Vec<&str> = c.entities().iter().map(|e| e.slug.as_str()).collect();
    let mut sorted = slugs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(slugs, sorted);
    for e in c.entities() {
        assert_eq!(e.slug, normalize(&e.name).unwrap().replace(' ', "-"));
        assert!(e.slug.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'), "{}", e.slug);
    }
    assert_eq!(c.entities().len(), BUILTIN_ENTITIES.len() + 3);
}

#[test]
fn normalisation_rules() {
    assert_eq!(normalize("  SPL\tToken--2022  ").as_deref(), Some("spl token 2022"));
    assert_eq!(normalize("main.treasury").as_deref(), Some("main treasury"));
    assert_eq!(normalize("A_B").as_deref(), Some("a b"));
    assert_eq!(normalize("\n"), None);
    assert_eq!(normalize("._-"), None);
}

// ---------------------------------------------------------------------------
// Entities: the built-in set
// ---------------------------------------------------------------------------

#[test]
fn built_in_addresses_are_the_sdks_constants() {
    let sdk: Vec<(&str, String)> = vec![
        ("System Program", solana_sdk_ids::system_program::ID.to_string()),
        ("SPL Token", spl_token_interface::ID.to_string()),
        ("SPL Token-2022", spl_token_2022_interface::ID.to_string()),
        ("Associated Token Account", spl_associated_token_account_interface::program::ID.to_string()),
        ("Compute Budget", solana_sdk_ids::compute_budget::ID.to_string()),
        ("Vote", solana_sdk_ids::vote::ID.to_string()),
        ("Memo", spl_memo_interface::v3::ID.to_string()),
        ("Stake", solana_sdk_ids::stake::ID.to_string()),
        ("Config", solana_sdk_ids::config::ID.to_string()),
        ("BPF Upgradeable Loader", solana_sdk_ids::bpf_loader_upgradeable::ID.to_string()),
        ("Address Lookup Table", solana_sdk_ids::address_lookup_table::ID.to_string()),
        ("Ed25519 Program", solana_sdk_ids::ed25519_program::ID.to_string()),
        ("Secp256k1 Program", solana_sdk_ids::secp256k1_program::ID.to_string()),
        ("Wrapped SOL", spl_token_interface::native_mint::ID.to_string()),
        ("Wrapped SOL Token-2022", spl_token_2022_interface::native_mint::ID.to_string()),
        ("Clock Sysvar", solana_sdk_ids::sysvar::clock::ID.to_string()),
        ("Rent Sysvar", solana_sdk_ids::sysvar::rent::ID.to_string()),
        ("Incinerator", solana_sdk_ids::incinerator::ID.to_string()),
    ];
    assert_eq!(sdk.len(), BUILTIN_ENTITIES.len(), "every built-in is checked");
    for (name, address) in sdk {
        let b = BUILTIN_ENTITIES.iter().find(|b| b.name == name).unwrap_or_else(|| panic!("{name}"));
        assert_eq!(b.address, address, "{name}");
        assert!(is_base58_key(b.address));
    }
}

#[test]
fn built_in_kinds() {
    let c = catalog();
    let kind = |name| match c.entity(name) {
        EntityLookup::Found(e) => e.kind,
        other => panic!("{other:?}"),
    };
    assert_eq!(kind("spl token"), EntityKind::Program);
    assert_eq!(kind("wsol"), EntityKind::Mint);
    assert_eq!(kind("clock sysvar"), EntityKind::Account);
}

#[test]
fn digest_program_names_agree_with_the_built_ins() {
    let c = catalog();
    for (address, name) in oracle::digest::PROGRAM_NAMES {
        assert_eq!(c.entity_name(address), Some(name), "{address}");
        assert!(matches!(c.entity(name), EntityLookup::Found(e) if e.kind == EntityKind::Program));
    }
}
