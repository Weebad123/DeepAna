//! Tests for `prism::types::event`.

use prism::types::event::{ChangeEvent, SkipReason};
use prism::types::udm::DeepAnaSlot;

fn slot_row() -> DeepAnaSlot {
    DeepAnaSlot {
        slot: 250_000_000,
        blockhash: "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".into(),
        parent_slot: 249_999_999,
        block_time: Some(1_700_000_000),
        block_height: Some(230_000_000),
        transaction_count: 1_234,
        leader: None,
    }
}

fn paths() -> Vec<String> {
    vec![
        "solana_slots/slot=250000000/part-0.parquet".into(),
        "solana_transactions/slot=250000000/part-0.parquet".into(),
    ]
}

// ------------------------------------------------------------------ SkipReason

#[test]
fn skip_reason_round_trips_display_and_parse() {
    for r in SkipReason::ALL {
        assert_eq!(r.to_string().parse::<SkipReason>().unwrap(), r);
        assert_eq!(serde_json::to_string(&r).unwrap(), format!("\"{r}\""));
    }
    assert_eq!(
        "validator_skipped".parse::<SkipReason>().unwrap(),
        SkipReason::ValidatorSkipped
    );
    assert_eq!(
        "rpc_unavailable".parse::<SkipReason>().unwrap(),
        SkipReason::RpcUnavailable
    );
    assert_eq!(
        "below_cursor".parse::<SkipReason>().unwrap(),
        SkipReason::BelowCursor
    );
    assert!("ValidatorSkipped".parse::<SkipReason>().is_err());
}

#[test]
fn only_rpc_unavailable_is_retryable() {
    assert!(SkipReason::RpcUnavailable.is_retryable());
    assert!(!SkipReason::ValidatorSkipped.is_retryable());
    assert!(!SkipReason::BelowCursor.is_retryable());
}

// ------------------------------------------------------------- ChangeEvent::New

#[test]
fn new_from_slot_copies_header_fields() {
    let ev = ChangeEvent::new_from_slot(&slot_row(), paths());
    match &ev {
        ChangeEvent::New {
            slot,
            blockhash,
            parent_slot,
            block_time,
            transaction_count,
            parquet_paths,
        } => {
            assert_eq!(*slot, 250_000_000);
            assert_eq!(blockhash, "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU");
            assert_eq!(*parent_slot, 249_999_999);
            assert_eq!(*block_time, Some(1_700_000_000));
            assert_eq!(*transaction_count, 1_234);
            assert_eq!(*parquet_paths, paths());
        }
        other => panic!("expected New, got {other:?}"),
    }
    assert!(ev.is_new());
    assert!(!ev.is_skip());
    assert_eq!(ev.kind(), "new");
    assert_eq!(ev.slot(), 250_000_000);
    assert_eq!(ev.parquet_paths(), paths().as_slice());
    assert_eq!(ev.skip_reason(), None);
    assert_eq!(ev.block_datetime().unwrap().timestamp(), 1_700_000_000);
}

#[test]
fn from_slot_ref_has_no_paths() {
    let ev = ChangeEvent::from(&slot_row());
    assert!(ev.is_new());
    assert!(ev.parquet_paths().is_empty());
}

#[test]
fn new_event_json_wire_format() {
    let ev = ChangeEvent::new_from_slot(&slot_row(), paths());
    let json = ev.to_json();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["event"], "new");
    assert_eq!(v["slot"], 250_000_000u64);
    assert_eq!(
        v["blockhash"],
        "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"
    );
    assert_eq!(v["parent_slot"], 249_999_999u64);
    assert_eq!(v["block_time"], 1_700_000_000i64);
    assert_eq!(v["transaction_count"], 1_234);
    assert_eq!(v["parquet_paths"].as_array().unwrap().len(), 2);
    assert!(!json.contains('\n'), "wire format must be a single line");
    assert_eq!(ChangeEvent::from_json(&json).unwrap(), ev);
}

#[test]
fn new_event_without_block_time_serialises_null() {
    let mut row = slot_row();
    row.block_time = None;
    let ev = ChangeEvent::new_from_slot(&row, vec![]);
    let v: serde_json::Value = serde_json::from_str(&ev.to_json()).unwrap();
    assert!(v["block_time"].is_null());
    assert_eq!(ev.block_datetime(), None);
}

#[test]
fn new_event_display_is_compact() {
    let ev = ChangeEvent::new_from_slot(&slot_row(), paths());
    assert_eq!(
        ev.to_string(),
        "new slot=250000000 blockhash=7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU txs=1234 files=2"
    );
}

// ------------------------------------------------------------ ChangeEvent::Skip

#[test]
fn skip_event_accessors() {
    let ev = ChangeEvent::skip(42, SkipReason::ValidatorSkipped);
    assert!(ev.is_skip());
    assert!(!ev.is_new());
    assert_eq!(ev.kind(), "skip");
    assert_eq!(ev.slot(), 42);
    assert_eq!(ev.skip_reason(), Some(SkipReason::ValidatorSkipped));
    assert!(ev.parquet_paths().is_empty());
    assert_eq!(ev.block_datetime(), None);
    assert_eq!(ev.to_string(), "skip slot=42 reason=validator_skipped");
}

#[test]
fn skip_event_json_wire_format() {
    let ev = ChangeEvent::skip(42, SkipReason::RpcUnavailable);
    let json = ev.to_json();
    assert_eq!(
        json,
        r#"{"event":"skip","slot":42,"reason":"rpc_unavailable"}"#
    );
    assert_eq!(ChangeEvent::from_json(&json).unwrap(), ev);
}

// ----------------------------------------------------------- ChangeEvent::Final

#[test]
fn final_event_accessors() {
    let ev = ChangeEvent::finalized(250_000_000, "abc", 249_999_968);
    assert!(ev.is_final());
    assert!(!ev.is_new());
    assert!(!ev.is_skip());
    assert_eq!(ev.kind(), "final");
    assert_eq!(ev.slot(), 250_000_000);
    assert_eq!(ev.skip_reason(), None);
    assert!(ev.parquet_paths().is_empty());
    assert_eq!(ev.block_datetime(), None);
    assert_eq!(
        ev.to_string(),
        "final slot=250000000 blockhash=abc previous=249999968"
    );
}

#[test]
fn final_event_json_wire_format() {
    let ev = ChangeEvent::finalized(42, "abc", 10);
    let json = ev.to_json();
    assert_eq!(
        json,
        r#"{"event":"final","slot":42,"blockhash":"abc","previous_slot":10}"#
    );
    assert_eq!(ChangeEvent::from_json(&json).unwrap(), ev);
}

// ------------------------------------------------------------------- Stream-level

#[test]
fn parses_a_jsonl_stream_in_order() {
    let events = vec![
        ChangeEvent::skip(99, SkipReason::BelowCursor),
        ChangeEvent::new_from_slot(&slot_row(), paths()),
        ChangeEvent::skip(250_000_001, SkipReason::ValidatorSkipped),
    ];
    let stream: String = events.iter().map(|e| e.to_json() + "\n").collect();
    let parsed: Vec<ChangeEvent> = stream
        .lines()
        .map(ChangeEvent::from_json)
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(parsed, events);
    let slots: Vec<u64> = parsed.iter().map(ChangeEvent::slot).collect();
    assert_eq!(slots, vec![99, 250_000_000, 250_000_001]);
}

#[test]
fn unknown_event_kind_is_rejected() {
    let err = ChangeEvent::from_json(r#"{"event":"reorg","slot":1}"#).unwrap_err();
    assert!(err.to_string().contains("reorg"), "{err}");
    assert!(ChangeEvent::from_json("garbage").is_err());
}
