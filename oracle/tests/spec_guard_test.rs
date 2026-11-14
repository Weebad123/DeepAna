//! `spec/guard.rs`: every rule rejects what it should, with the field path
//! it should, and accepts its nearest valid neighbour. Also: the order the
//! rules run in, how much of a bad value an error repeats, the normal form,
//! and the wire format (unknown fields, the two window shapes).

mod spec_support;

use oracle::catalog::{Catalog, DIMENSIONS, Dimension, MAX_BUCKET_WIDTH_SLOTS, MAX_SLOT, MEASURES, Measure, Table};
use oracle::spec::guard::*;
use oracle::spec::*;
use serde_json::json;
use spec_support::{KEY_A, KEY_B, catalog, filter, group, limits, order, spec};

const MAX_SPAN: u64 = 216_000;
const MAX_ROWS: u32 = 1_000;

fn check(s: &QuerySpec) -> Result<ValidSpec, SpecError> {
    validate(s, &catalog(), &limits())
}

/// The reason `s` is rejected at `field`; panics on any other outcome.
#[track_caller]
fn rejected_at(s: &QuerySpec, field: &str) -> String {
    match check(s) {
        Err(SpecError::Invalid { field: got, reason }) => {
            assert_eq!(got, field, "rejected at the wrong field: {reason}");
            reason
        }
        other => panic!("expected Invalid at {field}, got {other:?}"),
    }
}

#[track_caller]
fn rejected_with(s: &QuerySpec, field: &str, fragment: &str) {
    let reason = rejected_at(s, field);
    assert!(reason.contains(fragment), "{field}: {reason:?} does not mention {fragment:?}");
}

#[track_caller]
fn accepted(s: &QuerySpec) -> ValidSpec {
    check(s).unwrap_or_else(|e| panic!("rejected: {e}\n{s:#?}"))
}

fn transfers() -> QuerySpec {
    spec(&["transfer_count"])
}

fn leak(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

/// The built-ins plus nine more transaction counts, nine more `Any`-table
/// slot dimensions, and a measure named like the `program` dimension, to
/// reach limits the built-ins cannot.
fn wide_catalog() -> Catalog {
    let tx_count = *MEASURES.iter().find(|m| m.id == "tx_count").unwrap();
    let slot = *DIMENSIONS.iter().find(|d| d.id == "slot").unwrap();
    let mut measures: Vec<Measure> = MEASURES.to_vec();
    measures.extend((0..9).map(|i| Measure { id: leak(format!("extra_count_{i}")), ..tx_count }));
    measures.push(Measure { id: "program", table: Table::Instructions, ..tx_count });
    let mut dimensions: Vec<Dimension> = DIMENSIONS.to_vec();
    dimensions.extend((0..9).map(|i| Dimension { id: leak(format!("extra_slot_{i}")), ..slot }));
    Catalog::from_definitions(measures, dimensions, vec![]).unwrap()
}

// ---------------------------------------------------------------------------
// The example from the design
// ---------------------------------------------------------------------------

#[test]
fn the_design_example_names_dimensions_the_catalog_does_not_have() {
    // Verbatim. `mint_address` is called `mint` in the catalog, and there is
    // no `transfer_kind`: token_transfers has no instruction-type column.
    let s: QuerySpec = serde_json::from_value(json!({
        "measures": ["transfer_count", "transfer_volume_raw"],
        "group_by": [{ "dimension": "mint_address" }],
        "filters": [{ "dimension": "transfer_kind", "op": "in", "values": ["Transfer", "TransferChecked"] }],
        "window": { "last_slots": 20000 },
        "order_by": [{ "target": "transfer_count", "direction": "desc" }],
        "limit": 10,
        "as_of_slot": null
    }))
    .unwrap();
    rejected_with(&s, "group_by[0].dimension", "unknown dimension \"mint_address\"");

    let fixed = QuerySpec { group_by: vec![group("mint")], ..s.clone() };
    rejected_with(&fixed, "filters[0].dimension", "unknown dimension \"transfer_kind\"");
}

#[test]
fn the_design_example_with_catalog_names_is_accepted() {
    let s: QuerySpec = serde_json::from_value(json!({
        "measures": ["transfer_count", "transfer_volume_raw"],
        "group_by": [{ "dimension": "mint" }],
        "filters": [{ "dimension": "token_program", "op": "in", "values": [KEY_A, KEY_B] }],
        "window": { "last_slots": 20000 },
        "order_by": [{ "target": "transfer_count", "direction": "desc" }],
        "limit": 10,
        "as_of_slot": null
    }))
    .unwrap();
    let v = accepted(&s);
    assert_eq!(v.table, Table::TokenTransfers);
    assert_eq!(v.measures, ["transfer_count", "transfer_volume_raw"]);
    assert_eq!(v.group_by, [ValidGroupBy { dimension: "mint", bucket_width: None }]);
    assert_eq!(v.filters, [ValidFilter { dimension: "token_program", values: vec![KEY_A.into(), KEY_B.into()] }]);
    assert_eq!(v.order_by, [ValidOrder { key: OrderKey::Measure(0), direction: Direction::Desc }]);
    assert_eq!((v.limit, v.window, v.as_of_slot), (10, Window::LastSlots(20_000), None));
}

// ---------------------------------------------------------------------------
// Rules 1-2: measures
// ---------------------------------------------------------------------------

#[test]
fn rule_1_measure_count() {
    rejected_with(&spec(&[]), "measures", "at least one");
    let c = wide_catalog();
    let ids: Vec<String> = (0..9).map(|i| format!("extra_count_{i}")).collect();
    let mut s = spec(&[]);
    s.measures = ids[..8].to_vec();
    assert_eq!(validate(&s, &c, &limits()).unwrap().measures.len(), MAX_MEASURES);
    s.measures = ids.clone();
    match validate(&s, &c, &limits()) {
        Err(SpecError::Invalid { field, reason }) => assert_eq!((field.as_str(), reason.contains("at most 8")), ("measures", true)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn rule_2_measures_are_known_distinct_and_on_one_table() {
    rejected_with(&spec(&["tx_count", "nope"]), "measures[1]", "unknown measure \"nope\"");
    rejected_with(&spec(&["TX_COUNT"]), "measures[0]", "unknown measure");
    rejected_with(&spec(&["tx_count", "tx_count"]), "measures[1]", "requested twice");
    rejected_with(&spec(&["tx_count", "transfer_count"]), "measures[1]", "reads token_transfers, but tx_count reads transactions");
    rejected_with(&spec(&["slot_count", "tx_count"]), "measures[1]", "one table");

    let v = accepted(&spec(&["tx_count", "tx_failed_count", "fee_total_lamports", "tx_failure_ratio"]));
    assert_eq!(v.table, Table::Transactions);
    for t in Table::ALL {
        let ms: Vec<&str> = MEASURES.iter().filter(|m| m.table == t).map(|m| m.id).collect();
        let mut s = spec(&ms);
        if t == Table::TokenTransfers {
            s.group_by = vec![group("mint")];
        }
        assert_eq!(accepted(&s).table, t);
    }
}

// ---------------------------------------------------------------------------
// Rules 3-5: group_by
// ---------------------------------------------------------------------------

#[test]
fn rule_3_group_by_count() {
    let four = QuerySpec {
        group_by: ["program", "ix_level", "ix_decoded", "slot"].map(group).to_vec(),
        ..spec(&["program_count"])
    };
    assert_eq!(accepted(&four).group_by.len(), MAX_GROUP_BY);
    let mut five = four.clone();
    five.group_by.push(group("slot_bucket"));
    rejected_with(&five, "group_by", "at most 4");
}

#[test]
fn rule_4_group_by_dimensions_are_known_on_the_table_distinct_and_not_a_measure_name() {
    let by = |dims: &[&str]| QuerySpec { group_by: dims.iter().map(|d| group(d)).collect(), ..transfers() };
    rejected_with(&by(&["mint", "nope"]), "group_by[1].dimension", "unknown dimension \"nope\"");
    rejected_with(&by(&["program"]), "group_by[0].dimension", "not on token_transfers");
    rejected_with(&by(&["tx_status"]), "group_by[0].dimension", "not on token_transfers");
    rejected_with(&by(&["mint", "mint"]), "group_by[1].dimension", "grouped by twice");
    accepted(&by(&["mint", "source_owner", "slot", "token_program"]));
    // `Any`-table dimensions group every table.
    for m in ["slot_count", "tx_count", "program_count", "transfer_count"] {
        accepted(&QuerySpec { group_by: vec![group("slot"), group("slot_bucket")], ..spec(&[m]) });
    }

    let c = wide_catalog();
    let clash = QuerySpec { group_by: vec![group("program")], ..spec(&["program"]) };
    match validate(&clash, &c, &limits()) {
        Err(SpecError::Invalid { field, reason }) => {
            assert_eq!(field, "group_by[0].dimension");
            assert!(reason.contains("same name as a requested measure"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
    let fine = QuerySpec { group_by: vec![group("program")], ..spec(&["ix_cpi_count"]) };
    validate(&fine, &c, &limits()).unwrap();
}

#[test]
fn rule_5_only_slot_bucket_takes_a_width_and_it_must_be_in_range() {
    let bucket = |w: Option<u64>| QuerySpec {
        group_by: vec![GroupBy { dimension: "slot_bucket".into(), bucket_width: w }],
        window: Window::LastSlots(MAX_SPAN),
        ..transfers()
    };
    rejected_with(&bucket(None), "group_by[0].bucket_width", "needs a width");
    rejected_with(&bucket(Some(0)), "group_by[0].bucket_width", "from 1 to 216000");
    rejected_with(&bucket(Some(MAX_BUCKET_WIDTH_SLOTS + 1)), "group_by[0].bucket_width", "from 1 to 216000");
    assert_eq!(accepted(&bucket(Some(1))).bucket_width(), Some(1));
    assert_eq!(accepted(&bucket(Some(150))).bucket_width(), Some(150));

    let mint = QuerySpec { group_by: vec![GroupBy { dimension: "mint".into(), bucket_width: Some(5) }], ..transfers() };
    rejected_with(&mint, "group_by[0].bucket_width", "takes no parameter");
}

// ---------------------------------------------------------------------------
// Rules 6-9: filters
// ---------------------------------------------------------------------------

#[test]
fn rule_6_filter_count() {
    let c = wide_catalog();
    let mut s = spec(&["tx_count"]);
    s.filters = (0..8).map(|i| filter(&format!("extra_slot_{i}"), FilterOp::Eq, &["5"])).collect();
    assert_eq!(validate(&s, &c, &limits()).unwrap().filters.len(), MAX_FILTERS);
    s.filters.push(filter("extra_slot_8", FilterOp::Eq, &["5"]));
    match validate(&s, &c, &limits()) {
        Err(SpecError::Invalid { field, .. }) => assert_eq!(field, "filters"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn rule_7_filter_dimensions_are_known_on_the_table_filterable_and_used_once() {
    let on = |f: Vec<Filter>| QuerySpec { filters: f, ..transfers() };
    rejected_with(&on(vec![filter("nope", FilterOp::Eq, &[KEY_A])]), "filters[0].dimension", "unknown dimension");
    rejected_with(&on(vec![filter("program", FilterOp::Eq, &[KEY_A])]), "filters[0].dimension", "not on token_transfers");
    rejected_with(&on(vec![filter("slot_bucket", FilterOp::Eq, &["1"])]), "filters[0].dimension", "cannot be filtered on");
    rejected_with(
        &on(vec![filter("mint", FilterOp::Eq, &[KEY_A]), filter("source_owner", FilterOp::Eq, &[KEY_A]), filter("mint", FilterOp::Eq, &[KEY_B])]),
        "filters[2].dimension",
        "already filtered by filters[0]",
    );
    accepted(&on(vec![
        filter("mint", FilterOp::Eq, &[KEY_A]),
        filter("source_owner", FilterOp::In, &[KEY_A, KEY_B]),
        filter("destination_owner", FilterOp::Eq, &[KEY_B]),
        filter("token_program", FilterOp::Eq, &[KEY_A]),
        filter("slot", FilterOp::In, &["5", "7..=9"]),
    ]));
}

#[test]
fn rule_8_eq_takes_one_value_and_in_takes_up_to_the_cap() {
    let on = |op, values: Vec<String>| QuerySpec {
        filters: vec![Filter { dimension: "slot".into(), op, values }],
        ..transfers()
    };
    let n = |k: usize| (0..k).map(|i| i.to_string()).collect::<Vec<_>>();
    rejected_with(&on(FilterOp::Eq, n(0)), "filters[0].values", "exactly one value, got 0");
    rejected_with(&on(FilterOp::Eq, n(2)), "filters[0].values", "exactly one value, got 2");
    rejected_with(&on(FilterOp::In, n(0)), "filters[0].values", "1 to 64 values, got 0");
    rejected_with(&on(FilterOp::In, n(MAX_FILTER_VALUES + 1)), "filters[0].values", "got 65");
    accepted(&on(FilterOp::Eq, n(1)));
    accepted(&on(FilterOp::In, n(1)));
    assert_eq!(accepted(&on(FilterOp::In, n(MAX_FILTER_VALUES))).filters[0].values.len(), MAX_FILTER_VALUES);
}

#[test]
fn rule_9_every_value_passes_its_dimension_rule_once() {
    let on = |dim: &str, values: &[&str]| QuerySpec { filters: vec![filter(dim, FilterOp::In, values)], ..transfers() };
    let tx = |values: &[&str]| QuerySpec { filters: vec![filter("tx_status", FilterOp::In, values)], ..spec(&["tx_count"]) };

    // Base58 keys.
    for bad in ["", "abc", "unattributed", "unknown", "0OIl0OIl0OIl0OIl0OIl0OIl0OIl0OIl", &format!("{KEY_A}'"), &format!(" {KEY_A}")] {
        rejected_with(&on("mint", &[KEY_A, bad]), "filters[0].values[1]", "32-byte base58 address");
    }
    rejected_with(&on("mint", &["' OR '1'='1"]), "filters[0].values[0]", "base58");
    accepted(&on("mint", &[KEY_A, KEY_B]));
    // Enum members: exact, lower case.
    for bad in ["FAILED", "failed ", "fail", "failed' OR '1'='1", ""] {
        rejected_with(&tx(&[bad]), "filters[0].values[0]", "one of: failed, success");
    }
    accepted(&tx(&["failed", "success"]));
    // Slots.
    for bad in ["-1", "+1", "1.0", "20..=10", "1..2", "9223372036854775808", " 1", ""] {
        rejected_with(&on("slot", &[bad]), "filters[0].values[0]", "a slot N or a range");
    }
    accepted(&on("slot", &["0", "9223372036854775807", "10..=20"]));
    // Listed once.
    rejected_with(&on("mint", &[KEY_A, KEY_B, KEY_A]), "filters[0].values[2]", "listed twice");
    rejected_with(&tx(&["failed", "failed"]), "filters[0].values[1]", "listed twice");
}

// ---------------------------------------------------------------------------
// Rule 10: required dimensions
// ---------------------------------------------------------------------------

#[test]
fn rule_10_a_per_mint_measure_needs_the_mint_grouped_or_pinned_to_one_value() {
    let volume = || spec(&["transfer_count", "transfer_volume_raw"]);
    rejected_with(&volume(), "measures[1]", "only meaningful per mint");
    rejected_with(
        &QuerySpec { filters: vec![filter("mint", FilterOp::In, &[KEY_A, KEY_B])], ..volume() },
        "measures[1]",
        "filter mint to exactly one value",
    );
    // Grouping by something else does not help.
    rejected_with(&QuerySpec { group_by: vec![group("source_owner")], ..volume() }, "measures[1]", "per mint");

    accepted(&QuerySpec { group_by: vec![group("mint")], ..volume() });
    accepted(&QuerySpec { filters: vec![filter("mint", FilterOp::Eq, &[KEY_A])], ..volume() });
    accepted(&QuerySpec { filters: vec![filter("mint", FilterOp::In, &[KEY_A])], ..volume() });
    accepted(&QuerySpec {
        group_by: vec![group("mint")],
        filters: vec![filter("mint", FilterOp::In, &[KEY_A, KEY_B])],
        ..volume()
    });
}

// ---------------------------------------------------------------------------
// Rules 11-12: the window
// ---------------------------------------------------------------------------

#[test]
fn rule_11_the_window_is_bounded() {
    let w = |window| QuerySpec { window, ..transfers() };
    let range = |from_slot, to_slot| w(Window::Range { from_slot, to_slot });

    rejected_with(&w(Window::LastSlots(0)), "window.last_slots", "outside 1..=216000");
    rejected_with(&w(Window::LastSlots(MAX_SPAN + 1)), "window.last_slots", "outside 1..=216000");
    accepted(&w(Window::LastSlots(1)));
    accepted(&w(Window::LastSlots(MAX_SPAN)));

    rejected_with(&range(MAX_SLOT + 1, MAX_SLOT + 1), "window.from_slot", "largest slot");
    rejected_with(&range(5, MAX_SLOT + 1), "window.to_slot", "largest slot");
    rejected_with(&range(10, 9), "window.to_slot", "below from_slot");
    rejected_with(&range(0, MAX_SPAN), "window", "covers 216001 slots, at most 216000");
    accepted(&range(0, MAX_SPAN - 1));
    accepted(&range(7, 7));
    accepted(&range(MAX_SLOT - 10, MAX_SLOT));

    // The span is the configured one.
    let tight = SpecLimits { max_slot_span: 150, ..limits() };
    assert!(validate(&w(Window::LastSlots(150)), &catalog(), &tight).is_ok());
    assert!(validate(&w(Window::LastSlots(151)), &catalog(), &tight).is_err());
}

#[test]
fn rule_12_a_bucketed_window_always_holds_a_whole_bucket() {
    let bucketed = |window, width| QuerySpec {
        group_by: vec![GroupBy { dimension: "slot_bucket".into(), bucket_width: Some(width) }],
        window,
        ..transfers()
    };
    // last_slots: at least 2w - 1, whatever W turns out to be.
    rejected_with(&bucketed(Window::LastSlots(298), 150), "group_by[0].bucket_width", "at least 2 * 150 - 1 = 299");
    rejected_with(&bucketed(Window::LastSlots(150), 150), "group_by[0].bucket_width", "299");
    accepted(&bucketed(Window::LastSlots(299), 150));
    accepted(&bucketed(Window::LastSlots(1), 1));
    // A range: its snap is known now.
    let range = |from_slot, to_slot| Window::Range { from_slot, to_slot };
    rejected_with(&bucketed(range(5, 14), 10), "group_by[0].bucket_width", "5..=14 holds no whole bucket of 10");
    rejected_with(&bucketed(range(1, 10), 10), "group_by[0].bucket_width", "no whole bucket");
    accepted(&bucketed(range(10, 19), 10));
    accepted(&bucketed(range(5, 19), 10));
    // The bucket is checked after the window itself.
    rejected_at(&bucketed(range(10, 5), 10), "window.to_slot");
}

// ---------------------------------------------------------------------------
// Rules 13-15
// ---------------------------------------------------------------------------

#[test]
fn rule_13_order_targets_are_requested_measures_or_group_by_dimensions_once() {
    let ordered = |orders: Vec<Order>| QuerySpec { group_by: vec![group("mint")], order_by: orders, ..transfers() };
    rejected_with(&ordered(vec![order("nope", Direction::Asc)]), "order_by[0].target", "\"nope\" is not a measure or group-by dimension");
    // A real measure that is not requested, a real dimension that is not grouped.
    rejected_with(&ordered(vec![order("mint_count", Direction::Asc)]), "order_by[0].target", "not a measure or group-by");
    rejected_with(&ordered(vec![order("source_owner", Direction::Asc)]), "order_by[0].target", "not a measure or group-by");
    rejected_with(
        &ordered(vec![order("transfer_count", Direction::Asc), order("transfer_count", Direction::Desc)]),
        "order_by[1].target",
        "ordered by twice",
    );
    let too_many = ordered((0..=MAX_ORDER_BY).map(|_| order("mint", Direction::Asc)).collect());
    rejected_with(&too_many, "order_by", "at most 12");

    let v = accepted(&ordered(vec![order("mint", Direction::Desc), order("transfer_count", Direction::Asc)]));
    assert_eq!(
        v.order_by,
        [
            ValidOrder { key: OrderKey::GroupBy(0), direction: Direction::Desc },
            ValidOrder { key: OrderKey::Measure(0), direction: Direction::Asc },
        ]
    );
}

#[test]
fn rule_14_the_limit_defaults_to_and_is_capped_by_max_rows() {
    let l = |limit| QuerySpec { limit, ..transfers() };
    rejected_with(&l(Some(0)), "limit", "at least 1");
    rejected_with(&l(Some(MAX_ROWS + 1)), "limit", "above the row cap, 1000");
    assert_eq!(accepted(&l(Some(1))).limit, 1);
    assert_eq!(accepted(&l(Some(MAX_ROWS))).limit, MAX_ROWS);
    assert_eq!(accepted(&l(None)).limit, MAX_ROWS);
    let small = SpecLimits { max_rows: 5, ..limits() };
    assert_eq!(validate(&l(None), &catalog(), &small).unwrap().limit, 5);
    assert!(validate(&l(Some(6)), &catalog(), &small).is_err());
}

#[test]
fn rule_15_as_of_slot_fits_a_slot() {
    let pinned = |as_of_slot| QuerySpec { as_of_slot, ..transfers() };
    rejected_with(&pinned(Some(MAX_SLOT + 1)), "as_of_slot", "largest slot");
    rejected_with(&pinned(Some(u64::MAX)), "as_of_slot", "largest slot");
    assert_eq!(accepted(&pinned(Some(MAX_SLOT))).as_of_slot, Some(MAX_SLOT));
    assert_eq!(accepted(&pinned(Some(0))).as_of_slot, Some(0));
}

// ---------------------------------------------------------------------------
// Order, echo, normal form
// ---------------------------------------------------------------------------

#[test]
fn the_first_failing_rule_is_reported() {
    // Every part wrong: the measure is reported.
    let all_wrong = QuerySpec {
        measures: vec!["nope".into()],
        group_by: vec![group("nope")],
        filters: vec![filter("nope", FilterOp::Eq, &["x"])],
        window: Window::LastSlots(0),
        order_by: vec![order("nope", Direction::Asc)],
        limit: Some(0),
        as_of_slot: Some(u64::MAX),
    };
    rejected_at(&all_wrong, "measures[0]");
    let s = QuerySpec { measures: vec!["transfer_count".into()], ..all_wrong };
    rejected_at(&s, "group_by[0].dimension");
    let s = QuerySpec { group_by: vec![], ..s };
    rejected_at(&s, "filters[0].dimension");
    let s = QuerySpec { filters: vec![], ..s };
    rejected_at(&s, "window.last_slots");
    let s = QuerySpec { window: Window::LastSlots(10), ..s };
    rejected_at(&s, "order_by[0].target");
    let s = QuerySpec { order_by: vec![], ..s };
    rejected_at(&s, "limit");
    let s = QuerySpec { limit: None, ..s };
    rejected_at(&s, "as_of_slot");
    accepted(&QuerySpec { as_of_slot: None, ..s });
}

#[test]
fn an_error_repeats_at_most_32_characters_of_the_value() {
    let long = format!("{}{}", "a".repeat(32), "SECRET".repeat(200));
    let cases = [
        (spec(&[&long]), "measures[0]"),
        (QuerySpec { group_by: vec![group(&long)], ..transfers() }, "group_by[0].dimension"),
        (QuerySpec { filters: vec![filter(&long, FilterOp::Eq, &["1"])], ..transfers() }, "filters[0].dimension"),
        (QuerySpec { filters: vec![filter("mint", FilterOp::Eq, &[&long])], ..transfers() }, "filters[0].values[0]"),
        (QuerySpec { order_by: vec![order(&long, Direction::Asc)], ..transfers() }, "order_by[0].target"),
    ];
    for (s, field) in cases {
        let reason = rejected_at(&s, field);
        assert!(reason.contains(&format!("\"{}\"…", "a".repeat(32))), "{field}: {reason}");
        assert!(!reason.contains("SECRET"), "{field}: {reason}");
    }
    // Control characters are escaped, not repeated raw.
    let reason = rejected_at(&spec(&["a\nb\u{1b}[2J"]), "measures[0]");
    assert!(!reason.contains('\n') && !reason.contains('\u{1b}'), "{reason:?}");
}

#[test]
fn the_normal_form_holds_catalog_ids_and_sorted_filters() {
    let a = QuerySpec {
        group_by: vec![group("source_owner")],
        filters: vec![
            filter("token_program", FilterOp::In, &[KEY_B, KEY_A]),
            filter("mint", FilterOp::Eq, &[KEY_A]),
        ],
        ..transfers()
    };
    let b = QuerySpec {
        filters: vec![
            filter("mint", FilterOp::In, &[KEY_A]),
            filter("token_program", FilterOp::In, &[KEY_A, KEY_B]),
        ],
        ..a.clone()
    };
    let c = catalog();
    let (va, vb) = (validate(&a, &c, &limits()).unwrap(), validate(&b, &c, &limits()).unwrap());
    assert_eq!(va, vb, "eq and a one-value in, and any value or filter order, are the same filter");
    assert_eq!(va.filters.iter().map(|f| f.dimension).collect::<Vec<_>>(), ["mint", "token_program"]);
    assert_eq!(va.filters[1].values, [KEY_A, KEY_B]);
    // The ids are the catalog's own strings, not the client's.
    assert!(std::ptr::eq(va.measures[0], c.measure("transfer_count").unwrap().id));
    assert!(!std::ptr::eq(va.measures[0].as_ptr(), a.measures[0].as_ptr()));
    assert!(std::ptr::eq(va.group_by[0].dimension, c.dimension("source_owner").unwrap().id));
}

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

#[test]
fn the_wire_format_refuses_unknown_fields_and_free_text() {
    let base = json!({ "measures": ["tx_count"], "window": { "last_slots": 10 } });
    let parsed: QuerySpec = serde_json::from_value(base.clone()).unwrap();
    assert_eq!(parsed, QuerySpec { limit: None, window: Window::LastSlots(10), ..spec(&["tx_count"]) });

    let with = |key: &str, value: serde_json::Value| {
        let mut v = base.clone();
        v[key] = value;
        serde_json::from_value::<QuerySpec>(v)
    };
    for (key, value) in [
        ("sql", json!("SELECT 1")),
        ("having", json!("COUNT(*) > 1")),
        ("join", json!("solana.slots")),
        ("expression", json!("fee * 2")),
        ("question", json!("top mints")),
    ] {
        assert!(with(key, value).is_err(), "{key} accepted");
    }
    assert!(with("group_by", json!([{ "dimension": "slot", "expr": "slot % 2" }])).is_err());
    assert!(with("filters", json!([{ "dimension": "slot", "op": "like", "values": ["1%"] }])).is_err());
    assert!(with("filters", json!([{ "dimension": "slot", "op": "eq", "values": ["1"], "raw": "1=1" }])).is_err());
    assert!(with("order_by", json!([{ "target": "tx_count" }])).is_err(), "a direction is required");
    assert!(with("order_by", json!([{ "target": "tx_count", "direction": "sideways" }])).is_err());
    assert!(with("limit", json!(-1)).is_err());

    // The window is mandatory and has exactly two shapes.
    assert!(serde_json::from_value::<QuerySpec>(json!({ "measures": ["tx_count"] })).is_err());
    for window in [
        json!({}),
        json!({ "last_slots": 10, "from_slot": 1, "to_slot": 2 }),
        json!({ "from_slot": 1 }),
        json!({ "to_slot": 1 }),
        json!({ "last_hours": 1 }),
        json!({ "last_slots": "10" }),
    ] {
        assert!(with("window", window.clone()).is_err(), "{window}");
    }
    assert_eq!(with("window", json!({ "from_slot": 1, "to_slot": 2 })).unwrap().window, Window::Range { from_slot: 1, to_slot: 2 });

    // Round trip.
    let full = QuerySpec {
        group_by: vec![group("slot_bucket"), group("mint")],
        filters: vec![filter("mint", FilterOp::In, &[KEY_A])],
        order_by: vec![order("transfer_count", Direction::Desc)],
        as_of_slot: Some(7),
        window: Window::Range { from_slot: 10, to_slot: 1_000 },
        ..transfers()
    };
    let text = serde_json::to_string(&full).unwrap();
    assert_eq!(serde_json::from_str::<QuerySpec>(&text).unwrap(), full);
}

#[test]
fn every_generated_spec_is_accepted() {
    let c = catalog();
    let specs = spec_support::accepted_specs(&c);
    assert!(specs.len() > 300, "{}", specs.len());
    for (label, s) in &specs {
        if let Err(e) = validate(s, &c, &limits()) {
            panic!("{label}: {e}");
        }
    }
}
