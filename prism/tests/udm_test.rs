//! Tests for `prism::types::udm`.

use arrow::array::{Array, AsArray};
use arrow::datatypes::{
    DataType, TimeUnit, TimestampSecondType, UInt8Type, UInt32Type, UInt64Type,
};
use prism::types::udm::*;

fn slot_row() -> DeepAnaSlot {
    DeepAnaSlot {
        slot: 250_000_000,
        blockhash: "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".into(),
        parent_slot: 249_999_999,
        block_time: Some(1_700_000_000),
        block_height: Some(230_000_000),
        transaction_count: 1_234,
        leader: Some("DeepAnaLeader1111111111111111111111111111111".into()),
    }
}

fn tx_row() -> DeepAnaTransaction {
    DeepAnaTransaction {
        slot: 250_000_000,
        blockhash: "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU".into(),
        transaction_index: 7,
        tx_signature: "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW".into(),
        fee: 5_000,
        status: TransactionStatus::Failed,
        err: Some("InstructionError(0, Custom(1))".into()),
        accounts: vec!["payer".into(), "programX".into()],
        log_messages: vec!["Program log: hi".into()],
        compute_units_consumed: Some(1_500),
    }
}

fn ix_row() -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot: 250_000_000,
        tx_signature: "sig".into(),
        instruction_index: 2,
        inner_instruction_index: Some(0),
        program_id: spl_token::id().to_string(),
        accounts: vec!["src".into(), "dst".into(), "owner".into()],
        data_base58: bs58::encode([3u8, 1, 0, 0, 0, 0, 0, 0, 0]).into_string(),
        instruction_type: Some("transfer".into()),
        decoded_data: Some(serde_json::json!({ "amount": 1 })),
    }
}

fn transfer_row() -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        slot: 250_000_000,
        tx_signature: "sig".into(),
        instruction_index: 2,
        inner_instruction_index: None,
        spl_token_program_id: spl_token_2022::id().to_string(),
        source_token_account: "src".into(),
        destination_token_account: "dst".into(),
        source_owner: Some("alice".into()),
        destination_owner: Some("alice".into()),
        mint_token_address: Some("USDC".into()),
        amount_raw: 1_500_000,
        decimals: Some(6),
        block_time: Some(1_700_000_000),
    }
}

// ----------------------------------------------------------------- DeepAnaSlot

#[test]
fn slot_batch_matches_schema() {
    let batch = DeepAnaSlot::to_record_batch(&[slot_row(), DeepAnaSlot::default()]).unwrap();
    assert_eq!(batch.schema(), DeepAnaSlot::schema());
    assert_eq!(batch.num_rows(), 2);
    let bt = batch.column(3).as_primitive::<TimestampSecondType>();
    assert_eq!(bt.value(0), 1_700_000_000);
    assert!(bt.is_null(1));
}

#[test]
fn slot_block_time_is_utc_seconds() {
    assert_eq!(
        DeepAnaSlot::schema().field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Second, Some(UDM_TIMEZONE.into()))
    );
    assert_eq!(
        slot_row().block_datetime().unwrap().timestamp(),
        1_700_000_000
    );
    assert_eq!(DeepAnaSlot::default().block_datetime(), None);
}

#[test]
fn slot_helpers() {
    assert!(DeepAnaSlot::default().is_genesis());
    assert!(!slot_row().is_genesis());
    assert_eq!(UdmRow::slot(&slot_row()), 250_000_000);
}

// ---------------------------------------------------------- DeepAnaTransaction

#[test]
fn transaction_batch_encodes_lists_as_json() {
    let batch = DeepAnaTransaction::to_record_batch(&[tx_row()]).unwrap();
    assert_eq!(batch.schema(), DeepAnaTransaction::schema());
    let accounts = batch.column(7).as_string::<i32>();
    assert_eq!(accounts.value(0), r#"["payer","programX"]"#);
    let logs = batch.column(8).as_string::<i32>();
    assert_eq!(logs.value(0), r#"["Program log: hi"]"#);
    let status = batch.column(5).as_string::<i32>();
    assert_eq!(status.value(0), "failed");
    let decoded: Vec<String> = from_json_string(accounts.value(0)).unwrap();
    assert_eq!(decoded, tx_row().accounts);
}

#[test]
fn transaction_helpers() {
    let tx = tx_row();
    assert_eq!(tx.fee_payer(), Some("payer"));
    assert!(!tx.is_success());
    assert_eq!(tx.accounts_json(), r#"["payer","programX"]"#);
    assert_eq!(DeepAnaTransaction::default().fee_payer(), None);
}

#[test]
fn transaction_status_round_trips() {
    for s in [TransactionStatus::Success, TransactionStatus::Failed] {
        assert_eq!(s.to_string().parse::<TransactionStatus>().unwrap(), s);
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, format!("\"{s}\""));
    }
    assert!("bogus".parse::<TransactionStatus>().is_err());
    assert_eq!(
        TransactionStatus::from_err(None),
        TransactionStatus::Success
    );
    assert_eq!(
        TransactionStatus::from_err(Some("x")),
        TransactionStatus::Failed
    );
    assert_eq!(TransactionStatus::default(), TransactionStatus::Success);
}

// ---------------------------------------------------------- DeepAnaInstruction

#[test]
fn instruction_batch_matches_schema() {
    let row = ix_row();
    let batch = DeepAnaInstruction::to_record_batch(&[row]).unwrap();
    assert_eq!(batch.schema(), DeepAnaInstruction::schema());
    let inner = batch.column(3).as_primitive::<UInt32Type>();
    assert_eq!(inner.value(0), 0);
    let decoded = batch.column(8).as_string::<i32>();
    assert_eq!(decoded.value(0), r#"{"amount":1}"#);
}

#[test]
fn instruction_helpers() {
    let row = ix_row();
    assert!(row.is_inner());
    assert!(!row.is_top_level());
    assert!(row.is_decoded());
    assert_eq!(row.data_bytes().unwrap(), vec![3u8, 1, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(row.decoded_data_json().as_deref(), Some(r#"{"amount":1}"#));

    let top = DeepAnaInstruction {
        inner_instruction_index: None,
        ..row
    };
    assert!(top.is_top_level());
    assert_eq!(DeepAnaInstruction::default().decoded_data_json(), None);
}

#[test]
fn instruction_top_level_has_null_inner_index() {
    let mut row = ix_row();
    row.inner_instruction_index = None;
    row.decoded_data = None;
    let batch = DeepAnaInstruction::to_record_batch(&[row]).unwrap();
    assert!(batch.column(3).is_null(0));
    assert!(batch.column(8).is_null(0));
}

// -------------------------------------------------------- DeepAnaTokenTransfer

#[test]
fn token_transfer_batch_matches_schema() {
    let batch = DeepAnaTokenTransfer::to_record_batch(&[transfer_row()]).unwrap();
    assert_eq!(batch.schema(), DeepAnaTokenTransfer::schema());
    assert_eq!(
        batch.column(10).as_primitive::<UInt64Type>().value(0),
        1_500_000
    );
    assert_eq!(batch.column(11).as_primitive::<UInt8Type>().value(0), 6);
    assert!(batch.column(3).is_null(0));
    assert_eq!(
        batch
            .column(12)
            .as_primitive::<TimestampSecondType>()
            .value(0),
        1_700_000_000
    );
}

#[test]
fn token_transfer_helpers() {
    let row = transfer_row();
    assert_eq!(row.amount_ui(), Some(1.5));
    assert!(row.is_token_2022());
    assert!(row.is_self_transfer());
    assert_eq!(row.block_datetime().unwrap().timestamp(), 1_700_000_000);

    let legacy = DeepAnaTokenTransfer {
        spl_token_program_id: spl_token::id().to_string(),
        destination_owner: Some("bob".into()),
        decimals: None,
        ..row
    };
    assert!(!legacy.is_token_2022());
    assert!(!legacy.is_self_transfer());
    assert_eq!(legacy.amount_ui(), None);
}

// ------------------------------------------------------------------- Cross-table

#[test]
fn empty_batches_keep_schema() {
    let s = DeepAnaSlot::to_record_batch(&[]).unwrap();
    let t = DeepAnaTransaction::to_record_batch(&[]).unwrap();
    let i = DeepAnaInstruction::to_record_batch(&[]).unwrap();
    let k = DeepAnaTokenTransfer::to_record_batch(&[]).unwrap();
    for (batch, schema) in [
        (&s, DeepAnaSlot::schema()),
        (&t, DeepAnaTransaction::schema()),
        (&i, DeepAnaInstruction::schema()),
        (&k, DeepAnaTokenTransfer::schema()),
    ] {
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }
}

#[test]
fn every_table_starts_with_slot_partition_key() {
    for schema in [
        DeepAnaSlot::schema(),
        DeepAnaTransaction::schema(),
        DeepAnaInstruction::schema(),
        DeepAnaTokenTransfer::schema(),
    ] {
        let f = schema.field(0);
        assert_eq!(f.name(), "slot");
        assert_eq!(f.data_type(), &DataType::UInt64);
        assert!(!f.is_nullable());
    }
}

#[test]
fn rows_round_trip_through_serde() {
    let tx = tx_row();
    let back: DeepAnaTransaction =
        serde_json::from_str(&serde_json::to_string(&tx).unwrap()).unwrap();
    assert_eq!(back, tx);
    let ix = ix_row();
    let back: DeepAnaInstruction =
        serde_json::from_str(&serde_json::to_string(&ix).unwrap()).unwrap();
    assert_eq!(back, ix);
    let tr = transfer_row();
    let back: DeepAnaTokenTransfer =
        serde_json::from_str(&serde_json::to_string(&tr).unwrap()).unwrap();
    assert_eq!(back, tr);
}

#[test]
fn json_string_helpers_round_trip() {
    let v = vec!["a".to_string(), "b".to_string()];
    let s = to_json_string(&v);
    assert_eq!(s, r#"["a","b"]"#);
    let back: Vec<String> = from_json_string(&s).unwrap();
    assert_eq!(back, v);
    assert!(from_json_string::<Vec<String>>("not json").is_err());
}

#[test]
fn table_names_are_distinct() {
    let names = [
        DeepAnaSlot::TABLE_NAME,
        DeepAnaTransaction::TABLE_NAME,
        DeepAnaInstruction::TABLE_NAME,
        DeepAnaTokenTransfer::TABLE_NAME,
    ];
    let set: std::collections::HashSet<_> = names.iter().collect();
    assert_eq!(set.len(), names.len());
}
