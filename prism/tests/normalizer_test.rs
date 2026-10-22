//! Tests for `prism::normalizer`.
//!
//! Transactions are built as real `VersionedTransaction`s and encoded with
//! the SDK's own encoder, so the bytes the normalizer reads are byte for byte
//! what an RPC node sends. Metas are written as the RPC's JSON.
//!
//! [`mixed_block`] is the main fixture: legacy, v0 and failed transactions,
//! with and without CPIs and meta, across every encoding the adapter can ask
//! for. The instruction-count tests check the rows the normalizer produces
//! against a count taken straight from that raw block with the SDK's decoder,
//! not with the normalizer's own code.

use std::collections::HashSet;

use prism::adapter::{ChainBlock, ChainBlockData};
use prism::normalizer::{
    InstructionNormalizer, NormalizedBlock, Normalizer, NormalizerMetricsSnapshot,
    SlotNormalizer, TransactionNormalizer,
};
use prism::types::error::PrismError;
use prism::types::udm::{DeepAnaInstruction, TransactionStatus, UdmRow};
use serde_json::{Value, json};
use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{Hash, Message, MessageHeader, VersionedMessage, v0, v1};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{
    Encodable, EncodableWithMeta, EncodedConfirmedBlock, EncodedTransaction,
    EncodedTransactionWithStatusMeta, UiInnerInstructions, UiTransactionEncoding,
    UiTransactionStatusMeta,
};

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

const SLOT: u64 = 250_000_000;
const BLOCKHASH: &str = "5Zc6D1tGkKJzvUsqKArbRBXPFvqzW4CQRUq5V6pXp6Yk";
const PARENT_HASH: &str = "4sGjMW1sUnHzSxGspuhpqLDx6wiyjNtZAMdL4VZHirAn";

fn key(n: u8) -> Pubkey {
    Pubkey::new_from_array([n; 32])
}

fn payer() -> Pubkey {
    key(1)
}
fn alice() -> Pubkey {
    key(2)
}
fn source() -> Pubkey {
    key(4)
}
fn destination() -> Pubkey {
    key(5)
}
fn authority() -> Pubkey {
    key(6)
}
fn system_program() -> Pubkey {
    key(10)
}
fn memo_program() -> Pubkey {
    key(11)
}
fn dex_program() -> Pubkey {
    key(20)
}
fn token_program() -> Pubkey {
    key(21)
}
fn lookup_table() -> Pubkey {
    key(29)
}
fn loaded_writable_a() -> Pubkey {
    key(30)
}
fn loaded_writable_b() -> Pubkey {
    key(31)
}
fn loaded_readonly() -> Pubkey {
    key(32)
}
fn vote_program() -> Pubkey {
    key(40)
}
fn leader() -> Pubkey {
    key(99)
}

fn s(key: Pubkey) -> String {
    key.to_string()
}

fn signature(seed: u8) -> Signature {
    Signature::from([seed; 64])
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn ix(program_id_index: u8, accounts: &[u8], data: &[u8]) -> CompiledInstruction {
    CompiledInstruction {
        program_id_index,
        accounts: accounts.to_vec(),
        data: data.to_vec(),
    }
}

fn header() -> MessageHeader {
    MessageHeader {
        num_required_signatures: 1,
        num_readonly_signed_accounts: 0,
        num_readonly_unsigned_accounts: 0,
    }
}

fn legacy_tx(seed: u8, keys: Vec<Pubkey>, instructions: Vec<CompiledInstruction>) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![signature(seed)],
        message: VersionedMessage::Legacy(Message {
            header: header(),
            account_keys: keys,
            recent_blockhash: Hash::new_from_array([7; 32]),
            instructions,
        }),
    }
}

/// A v0 transaction loading two writable and one readonly address from
/// [`lookup_table`].
fn v0_tx(seed: u8, keys: Vec<Pubkey>, instructions: Vec<CompiledInstruction>) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![signature(seed)],
        message: VersionedMessage::V0(v0::Message {
            header: header(),
            account_keys: keys,
            recent_blockhash: Hash::new_from_array([7; 32]),
            instructions,
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: lookup_table(),
                writable_indexes: vec![0, 1],
                readonly_indexes: vec![0],
            }],
        }),
    }
}

/// What the RPC reports for [`v0_tx`]'s lookups.
fn v0_loaded_addresses() -> Value {
    json!({
        "writable": [s(loaded_writable_a()), s(loaded_writable_b())],
        "readonly": [s(loaded_readonly())],
    })
}

fn v1_tx(seed: u8, keys: Vec<Pubkey>, instructions: Vec<CompiledInstruction>) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![signature(seed)],
        message: VersionedMessage::V1(v1::Message {
            header: header(),
            config: v1::TransactionConfig::default(),
            lifetime_specifier: Hash::new_from_array([7; 32]),
            account_keys: keys,
            instructions,
        }),
    }
}

fn encode(tx: &VersionedTransaction, encoding: UiTransactionEncoding) -> EncodedTransaction {
    match encoding {
        // Raw JSON message, the shape `json` blocks carry.
        UiTransactionEncoding::Json => tx.json_encode(),
        encoding => tx.encode(encoding),
    }
}

/// A status meta in the RPC's JSON, defaults overridden by `overrides`.
fn meta(overrides: Value) -> UiTransactionStatusMeta {
    let mut meta = json!({
        "err": null,
        "status": {"Ok": null},
        "fee": 5_000,
        "preBalances": [],
        "postBalances": [],
        "innerInstructions": [],
        "logMessages": ["Program log: ok"],
        "loadedAddresses": {"writable": [], "readonly": []},
        "computeUnitsConsumed": 1_000,
    });
    for (field, value) in overrides.as_object().expect("overrides are an object") {
        meta[field] = value.clone();
    }
    serde_json::from_value(meta).expect("test meta JSON is valid")
}

/// One inner instruction in the RPC's JSON: program index, account indexes,
/// raw data and stack height.
fn inner(program_id_index: u8, accounts: &[u8], data: &[u8], stack_height: u32) -> Value {
    json!({
        "programIdIndex": program_id_index,
        "accounts": accounts,
        "data": bs58::encode(data).into_string(),
        "stackHeight": stack_height,
    })
}

fn group(index: u8, instructions: Vec<Value>) -> Value {
    json!({"index": index, "instructions": instructions})
}

fn with_meta(transaction: EncodedTransaction, meta: Option<UiTransactionStatusMeta>) -> EncodedTransactionWithStatusMeta {
    EncodedTransactionWithStatusMeta {
        transaction,
        meta,
        version: None,
    }
}

fn raw_block(transactions: Vec<EncodedTransactionWithStatusMeta>) -> EncodedConfirmedBlock {
    serde_json::from_value(json!({
        "previousBlockhash": PARENT_HASH,
        "blockhash": BLOCKHASH,
        "parentSlot": SLOT - 1,
        "transactions": transactions,
        "rewards": [
            {"pubkey": s(key(98)), "lamports": 10, "postBalance": 10, "rewardType": "Rent", "commission": null},
            {"pubkey": s(leader()), "lamports": 25_000, "postBalance": 1_000_000, "rewardType": "Fee", "commission": null},
        ],
        "blockTime": 1_700_000_000,
        "blockHeight": 230_000_000,
    }))
    .expect("test block JSON is valid")
}

fn chain_block(raw: EncodedConfirmedBlock) -> ChainBlock {
    ChainBlock {
        slot_or_height: SLOT,
        block_id: raw.blockhash.clone(),
        parent_id: raw.previous_blockhash.clone(),
        timestamp: raw.block_time,
        raw: ChainBlockData::Solana(Box::new(raw)),
    }
}

// ---------------------------------------------------------------------------
// The mixed block
// ---------------------------------------------------------------------------

/// 0: legacy, base64, two plain instructions, no CPIs.
fn tx0() -> EncodedTransactionWithStatusMeta {
    let tx = legacy_tx(
        0xA0,
        vec![payer(), alice(), system_program(), memo_program()],
        vec![ix(2, &[0, 1], &[2, 0, 0, 0, 64, 66, 15, 0]), ix(3, &[0], b"hello")],
    );
    with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta(json!({}))))
}

/// 1: legacy, base58, a swap through a DEX: instruction 0 makes three calls
/// (one nested a level deeper), instruction 1 makes one.
fn tx1() -> EncodedTransactionWithStatusMeta {
    let tx = legacy_tx(
        0xA1,
        vec![payer(), source(), destination(), dex_program(), token_program(), authority()],
        vec![ix(3, &[0, 1, 2, 4], &[9, 1]), ix(4, &[1, 2, 5], &[3, 100, 0, 0, 0, 0, 0, 0, 0])],
    );
    let meta = meta(json!({
        "fee": 7_500,
        "computeUnitsConsumed": 48_210,
        "logMessages": ["Program DEX invoke [1]", "Program TOKEN invoke [2]", "Program DEX success"],
        "innerInstructions": [
            group(0, vec![
                inner(4, &[1, 2, 5], &[3, 1, 0, 0, 0, 0, 0, 0, 0], 2),
                inner(3, &[0], &[42], 2),
                inner(4, &[2, 1, 5], &[3, 2, 0, 0, 0, 0, 0, 0, 0], 3),
            ]),
            group(1, vec![inner(4, &[1], &[7], 2)]),
        ],
    }));
    with_meta(encode(&tx, UiTransactionEncoding::Base58), Some(meta))
}

/// 2: v0, base64, accounts loaded from a lookup table, used by the top-level
/// instruction and its calls.
fn tx2() -> EncodedTransactionWithStatusMeta {
    let tx = v0_tx(
        0xA2,
        vec![payer(), dex_program(), token_program()],
        vec![ix(1, &[0, 3, 4, 5], &[1])],
    );
    let meta = meta(json!({
        "loadedAddresses": v0_loaded_addresses(),
        "innerInstructions": [group(0, vec![
            inner(2, &[3, 4, 0], &[3, 5, 0, 0, 0, 0, 0, 0, 0], 2),
            inner(2, &[4, 3, 0], &[3, 6, 0, 0, 0, 0, 0, 0, 0], 2),
        ])],
    }));
    with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta))
}

/// 3: legacy, json, failed after its first call.
fn tx3() -> EncodedTransactionWithStatusMeta {
    let tx = legacy_tx(
        0xA3,
        vec![payer(), alice(), dex_program(), token_program()],
        vec![ix(2, &[0, 1], &[5])],
    );
    let err = json!({"InstructionError": [0, {"Custom": 6001}]});
    let meta = meta(json!({
        "err": err,
        "status": {"Err": err},
        "innerInstructions": [group(0, vec![inner(3, &[1, 0], &[3], 2)])],
    }));
    with_meta(encode(&tx, UiTransactionEncoding::Json), Some(meta))
}

/// 4: legacy, the RPC's old `binary` (base58) encoding, no meta.
fn tx4() -> EncodedTransactionWithStatusMeta {
    let tx = legacy_tx(0xA4, vec![payer(), vote_program()], vec![ix(1, &[0], &[2])]);
    with_meta(encode(&tx, UiTransactionEncoding::Binary), None)
}

fn mixed_block() -> ChainBlock {
    chain_block(raw_block(vec![tx0(), tx1(), tx2(), tx3(), tx4()]))
}

/// Per transaction, (top-level, inner) instruction counts read straight from
/// the raw block: the SDK's decoder for the message, the meta for the calls.
fn expected_counts(block: &ChainBlock) -> Vec<(usize, usize)> {
    let ChainBlockData::Solana(raw) = &block.raw;
    raw.transactions
        .iter()
        .map(|tx| {
            let top_level = match &tx.transaction {
                EncodedTransaction::Json(ui) => match &ui.message {
                    solana_transaction_status::UiMessage::Raw(raw) => raw.instructions.len(),
                    solana_transaction_status::UiMessage::Parsed(parsed) => parsed.instructions.len(),
                },
                encoded => encoded.decode().expect("SDK decodes the fixture").message.instructions().len(),
            };
            let inner = tx
                .meta
                .as_ref()
                .and_then(|meta| Option::<&Vec<UiInnerInstructions>>::from(meta.inner_instructions.as_ref()))
                .map_or(0, |groups| groups.iter().map(|group| group.instructions.len()).sum());
            (top_level, inner)
        })
        .collect()
}

fn normalize(block: &ChainBlock) -> NormalizedBlock {
    Normalizer::new().normalize(block).expect("block normalizes")
}

fn rows_of<'a>(block: &'a NormalizedBlock, signature: &str) -> Vec<&'a DeepAnaInstruction> {
    block.instructions.iter().filter(|row| row.tx_signature == signature).collect()
}

fn positions(rows: &[&DeepAnaInstruction]) -> Vec<(u32, Option<u32>)> {
    rows.iter().map(|row| (row.instruction_index, row.inner_instruction_index)).collect()
}

fn assert_deserialize_error(err: &PrismError, slot: u64, needle: &str) {
    let message = err.to_string();
    assert_eq!(err.kind(), "deserialize_error", "{message}");
    assert_eq!(err.slot(), Some(slot), "{message}");
    assert!(message.contains(needle), "{message:?} does not mention {needle:?}");
}

// ---------------------------------------------------------------------------
// SlotNormalizer
// ---------------------------------------------------------------------------

#[test]
fn slot_row_maps_every_header_field() {
    let raw = raw_block(vec![tx0(), tx4()]);

    let row = SlotNormalizer::new().normalize(SLOT, &raw).unwrap();

    assert_eq!(row.slot, SLOT);
    assert_eq!(row.blockhash, BLOCKHASH);
    assert_eq!(row.parent_slot, SLOT - 1);
    assert_eq!(row.block_time, Some(1_700_000_000));
    assert_eq!(row.block_height, Some(230_000_000));
    assert_eq!(row.transaction_count, 2);
    assert_eq!(row.leader, Some(s(leader())), "the leader is the fee reward's pubkey");
}

#[test]
fn slot_row_tolerates_missing_optional_fields() {
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": PARENT_HASH,
        "blockhash": BLOCKHASH,
        "parentSlot": 9,
        "transactions": [],
        "rewards": [],
        "blockTime": null,
        "blockHeight": null,
    }))
    .unwrap();

    let row = SlotNormalizer::new().normalize(10, &raw).unwrap();

    assert_eq!(row.block_time, None);
    assert_eq!(row.block_height, None);
    assert_eq!(row.leader, None);
    assert_eq!(row.transaction_count, 0);
}

#[test]
fn slot_row_has_no_leader_without_a_fee_reward() {
    let mut raw = raw_block(vec![]);
    raw.rewards.retain(|reward| reward.reward_type != Some(solana_transaction_status::RewardType::Fee));

    let row = SlotNormalizer::new().normalize(SLOT, &raw).unwrap();

    assert_eq!(row.leader, None);
}

#[test]
fn empty_blockhash_is_a_malformed_block() {
    let mut raw = raw_block(vec![]);
    raw.blockhash.clear();

    let err = SlotNormalizer::new().normalize(SLOT, &raw).unwrap_err();

    assert_deserialize_error(&err, SLOT, "blockhash is empty");
}

// ---------------------------------------------------------------------------
// TransactionNormalizer
// ---------------------------------------------------------------------------

#[test]
fn transaction_row_maps_signature_meta_and_position() {
    let row = TransactionNormalizer::new().normalize(SLOT, BLOCKHASH, &tx1(), 1).unwrap();

    assert_eq!(row.slot, SLOT);
    assert_eq!(row.blockhash, BLOCKHASH);
    assert_eq!(row.transaction_index, 1);
    assert_eq!(row.tx_signature, signature(0xA1).to_string());
    assert_eq!(row.fee, 7_500);
    assert_eq!(row.status, TransactionStatus::Success);
    assert_eq!(row.err, None);
    assert_eq!(
        row.accounts,
        [payer(), source(), destination(), dex_program(), token_program(), authority()].map(s)
    );
    assert_eq!(row.fee_payer(), Some(s(payer()).as_str()));
    assert_eq!(row.log_messages.len(), 3);
    assert_eq!(row.compute_units_consumed, Some(48_210));
}

#[test]
fn every_encoding_of_a_transaction_gives_the_same_row() {
    let tx = legacy_tx(
        0xB0,
        vec![payer(), alice(), system_program()],
        vec![ix(2, &[0, 1], &[2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])],
    );
    let normalizer = TransactionNormalizer::new();
    let rows: Vec<_> = [
        UiTransactionEncoding::Base58,
        UiTransactionEncoding::Base64,
        UiTransactionEncoding::Binary,
        UiTransactionEncoding::Json,
    ]
    .into_iter()
    .map(|encoding| {
        let encoded = with_meta(encode(&tx, encoding), Some(meta(json!({}))));
        normalizer.normalize_with_message(SLOT, BLOCKHASH, &encoded, 0).unwrap()
    })
    .collect();

    for (row, decoded) in &rows[1..] {
        assert_eq!(row, &rows[0].0);
        assert_eq!(decoded, &rows[0].1);
    }
    assert_eq!(rows[0].1.instructions, tx.message.instructions());
}

#[test]
fn failed_transaction_keeps_the_rpc_error_as_json() {
    let row = TransactionNormalizer::new().normalize(SLOT, BLOCKHASH, &tx3(), 3).unwrap();

    assert_eq!(row.status, TransactionStatus::Failed);
    assert!(!row.is_success());
    let err: Value = serde_json::from_str(row.err.as_deref().expect("failed row has an error")).unwrap();
    assert_eq!(err, json!({"InstructionError": [0, {"Custom": 6001}]}));
}

#[test]
fn transaction_without_meta_gets_defaults() {
    let row = TransactionNormalizer::new().normalize(SLOT, BLOCKHASH, &tx4(), 4).unwrap();

    assert_eq!(row.tx_signature, signature(0xA4).to_string());
    assert_eq!(row.fee, 0);
    assert_eq!(row.status, TransactionStatus::Success);
    assert_eq!(row.err, None);
    assert!(row.log_messages.is_empty());
    assert_eq!(row.compute_units_consumed, None);
    assert_eq!(row.accounts, [s(payer()), s(vote_program())]);
}

#[test]
fn absent_logs_and_compute_units_are_empty_not_errors() {
    let mut meta = meta(json!({}));
    meta.log_messages = None.into();
    meta.compute_units_consumed = None.into();
    let tx = legacy_tx(0xB1, vec![payer(), system_program()], vec![ix(1, &[0], &[])]);

    let row = TransactionNormalizer::new()
        .normalize(SLOT, BLOCKHASH, &with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta)), 0)
        .unwrap();

    assert!(row.log_messages.is_empty());
    assert_eq!(row.compute_units_consumed, None);
}

#[test]
fn v0_transaction_appends_loaded_writable_then_readonly_keys() {
    let decoded = TransactionNormalizer::new().decode(SLOT, &tx2(), 2).unwrap();

    assert_eq!(
        decoded.account_keys,
        [payer(), dex_program(), token_program(), loaded_writable_a(), loaded_writable_b(), loaded_readonly()]
    );
    let row = TransactionNormalizer::new().normalize(SLOT, BLOCKHASH, &tx2(), 2).unwrap();
    assert_eq!(row.accounts, decoded.account_keys.iter().map(ToString::to_string).collect::<Vec<_>>());
}

#[test]
fn v0_transaction_in_json_reads_its_lookups_too() {
    let tx = v0_tx(0xB2, vec![payer(), dex_program()], vec![ix(1, &[0, 2, 3, 4], &[1])]);
    let meta = meta(json!({"loadedAddresses": v0_loaded_addresses()}));
    let normalizer = TransactionNormalizer::new();

    let json = normalizer
        .decode(SLOT, &with_meta(encode(&tx, UiTransactionEncoding::Json), Some(meta.clone())), 0)
        .unwrap();
    let binary = normalizer
        .decode(SLOT, &with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta)), 0)
        .unwrap();

    assert_eq!(json, binary);
    assert_eq!(json.account_keys.len(), 5);
}

#[test]
fn v0_lookups_without_loaded_addresses_are_rejected() {
    let tx = v0_tx(0xB3, vec![payer(), dex_program()], vec![ix(1, &[0], &[])]);
    let mut meta = meta(json!({}));
    meta.loaded_addresses = None.into();

    let err = TransactionNormalizer::new()
        .decode(SLOT, &with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta)), 7)
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "transaction 7");
}

#[test]
fn loaded_address_count_must_match_the_lookups() {
    let tx = v0_tx(0xB4, vec![payer(), dex_program()], vec![ix(1, &[0], &[])]);
    let meta = meta(json!({"loadedAddresses": {"writable": [s(loaded_writable_a())], "readonly": []}}));

    let err = TransactionNormalizer::new()
        .decode(SLOT, &with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta)), 0)
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "looks up 3 addresses but the meta loaded 1");
}

#[test]
fn legacy_transaction_with_loaded_addresses_is_rejected() {
    let tx = legacy_tx(0xB5, vec![payer(), system_program()], vec![ix(1, &[0], &[])]);
    let meta = meta(json!({"loadedAddresses": v0_loaded_addresses()}));

    let err = TransactionNormalizer::new()
        .decode(SLOT, &with_meta(encode(&tx, UiTransactionEncoding::Base58), Some(meta)), 0)
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "looks up 0 addresses but the meta loaded 3");
}

/// v1 transactions put their signatures after the message, which `bincode`
/// cannot read; the SDK's decoder can.
#[test]
fn v1_transaction_decodes() {
    let tx = v1_tx(0xB6, vec![payer(), system_program()], vec![ix(1, &[0], &[4, 2])]);

    let row = TransactionNormalizer::new()
        .normalize_with_message(SLOT, BLOCKHASH, &with_meta(encode(&tx, UiTransactionEncoding::Base64), Some(meta(json!({})))), 0)
        .unwrap();

    assert_eq!(row.0.tx_signature, signature(0xB6).to_string());
    assert_eq!(row.1.account_keys, [payer(), system_program()]);
    assert_eq!(row.1.instructions, [ix(1, &[0], &[4, 2])]);
}

#[test]
fn invalid_base58_is_reported_as_such() {
    let encoded = with_meta(
        EncodedTransaction::Binary("0OIl".into(), solana_transaction_status::TransactionBinaryEncoding::Base58),
        None,
    );

    let err = TransactionNormalizer::new().decode(SLOT, &encoded, 3).unwrap_err();

    assert_deserialize_error(&err, SLOT, "transaction 3");
    assert!(err_text(&encoded).contains("invalid base58"));
}

#[test]
fn invalid_base64_is_reported_as_such() {
    let encoded = with_meta(
        EncodedTransaction::Binary("not base64!".into(), solana_transaction_status::TransactionBinaryEncoding::Base64),
        None,
    );

    assert!(err_text(&encoded).contains("invalid base64"));
}

#[test]
fn bytes_that_are_not_a_transaction_are_rejected() {
    use base64::Engine;
    let blob = base64::prelude::BASE64_STANDARD.encode([0xFFu8; 16]);
    let encoded = with_meta(
        EncodedTransaction::Binary(blob, solana_transaction_status::TransactionBinaryEncoding::Base64),
        None,
    );

    assert!(err_text(&encoded).contains("not a valid transaction"));
}

#[test]
fn json_parsed_and_accounts_encodings_are_rejected() {
    let parsed: EncodedTransactionWithStatusMeta = serde_json::from_value(json!({
        "transaction": {
            "signatures": [signature(1).to_string()],
            "message": {
                "accountKeys": [{"pubkey": s(payer()), "writable": true, "signer": true, "source": "transaction"}],
                "recentBlockhash": BLOCKHASH,
                "instructions": [],
            },
        },
        "meta": null,
    }))
    .unwrap();
    assert!(err_text(&parsed).contains("jsonParsed"));

    let accounts: EncodedTransactionWithStatusMeta = serde_json::from_value(json!({
        "transaction": {"signatures": [signature(1).to_string()], "accountKeys": []},
        "meta": null,
    }))
    .unwrap();
    assert!(err_text(&accounts).contains("`accounts`"));
}

fn err_text(tx: &EncodedTransactionWithStatusMeta) -> String {
    TransactionNormalizer::new().decode(SLOT, tx, 0).unwrap_err().to_string()
}

// ---------------------------------------------------------------------------
// InstructionNormalizer
// ---------------------------------------------------------------------------

fn inner_groups(groups: Value) -> Vec<UiInnerInstructions> {
    serde_json::from_value(groups).expect("test inner instruction JSON is valid")
}

#[test]
fn top_level_instructions_resolve_program_accounts_and_data() {
    let keys = [payer(), alice(), system_program(), memo_program()];
    let instructions = [ix(2, &[0, 1], &[2, 0, 0, 0]), ix(3, &[], b"")];

    let rows = InstructionNormalizer::new()
        .normalize("sig", SLOT, &keys, &instructions, None)
        .unwrap();

    assert_eq!(rows.len(), 2);
    let first = &rows[0];
    assert_eq!(first.slot, SLOT);
    assert_eq!(first.tx_signature, "sig");
    assert_eq!((first.instruction_index, first.inner_instruction_index), (0, None));
    assert!(first.is_top_level());
    assert_eq!(first.program_id, s(system_program()));
    assert_eq!(first.accounts, [s(payer()), s(alice())]);
    assert_eq!(first.data_bytes().unwrap(), [2, 0, 0, 0]);
    assert_eq!((first.instruction_type.as_ref(), first.decoded_data.as_ref()), (None, None));

    assert_eq!(rows[1].instruction_index, 1);
    assert_eq!(rows[1].program_id, s(memo_program()));
    assert!(rows[1].accounts.is_empty());
    assert_eq!(rows[1].data_base58, "");
}

#[test]
fn rows_are_top_level_first_then_inner_grouped_by_parent() {
    let keys = [payer(), source(), destination(), dex_program(), token_program()];
    let instructions = [ix(3, &[0], &[1]), ix(4, &[1], &[2]), ix(3, &[0], &[3])];
    let groups = inner_groups(json!([
        group(0, vec![inner(4, &[1, 2], &[10], 2), inner(4, &[2, 1], &[11], 3)]),
        group(2, vec![inner(4, &[1], &[30], 2)]),
    ]));

    let rows = InstructionNormalizer::new()
        .normalize("sig", SLOT, &keys, &instructions, Some(&groups))
        .unwrap();

    let rows: Vec<_> = rows.iter().collect();
    assert_eq!(
        positions(&rows),
        [(0, None), (1, None), (2, None), (0, Some(0)), (0, Some(1)), (2, Some(0))]
    );
    let data: Vec<_> = rows.iter().map(|row| row.data_bytes().unwrap()[0]).collect();
    assert_eq!(data, [1, 2, 3, 10, 11, 30], "each row carries its own instruction's data");
    assert_eq!(rows[4].accounts, [s(destination()), s(source())]);
    assert!(rows[3..].iter().all(|row| row.is_inner() && row.program_id == s(token_program())));
}

#[test]
fn inner_groups_out_of_order_are_put_in_parent_order() {
    let keys = [payer(), dex_program()];
    let instructions = [ix(1, &[0], &[]), ix(1, &[0], &[]), ix(1, &[0], &[])];
    let groups = inner_groups(json!([
        group(2, vec![inner(1, &[0], &[2], 2)]),
        group(0, vec![inner(1, &[0], &[0], 2), inner(1, &[0], &[1], 2)]),
    ]));

    let rows = InstructionNormalizer::new()
        .normalize("sig", SLOT, &keys, &instructions, Some(&groups))
        .unwrap();

    let rows: Vec<_> = rows.iter().collect();
    assert_eq!(
        positions(&rows)[3..],
        [(0, Some(0)), (0, Some(1)), (2, Some(0))]
    );
}

#[test]
fn no_inner_instructions_and_an_empty_list_are_the_same() {
    let keys = [payer(), dex_program()];
    let instructions = [ix(1, &[0], &[5])];
    let normalizer = InstructionNormalizer::new();

    let none = normalizer.normalize("sig", SLOT, &keys, &instructions, None).unwrap();
    let empty = normalizer.normalize("sig", SLOT, &keys, &instructions, Some(&[])).unwrap();

    assert_eq!(none, empty);
    assert_eq!(none.len(), 1);
}

#[test]
fn no_instructions_give_no_rows() {
    let rows = InstructionNormalizer::new().normalize("sig", SLOT, &[payer()], &[], None).unwrap();

    assert!(rows.is_empty());
}

#[test]
fn inner_instructions_resolve_loaded_accounts() {
    let decoded = TransactionNormalizer::new().decode(SLOT, &tx2(), 2).unwrap();
    let tx2 = tx2();
    let groups = prism::normalizer::transaction::inner_instructions(tx2.meta.as_ref());

    let rows = InstructionNormalizer::new()
        .normalize(&decoded.signature, SLOT, &decoded.account_keys, &decoded.instructions, groups)
        .unwrap();

    assert_eq!(
        rows[0].accounts,
        [payer(), loaded_writable_a(), loaded_writable_b(), loaded_readonly()].map(s)
    );
    assert_eq!(rows[1].accounts, [loaded_writable_a(), loaded_writable_b(), payer()].map(s));
    assert_eq!(rows[2].accounts, [loaded_writable_b(), loaded_writable_a(), payer()].map(s));
}

#[test]
fn program_index_out_of_range_is_rejected() {
    let err = InstructionNormalizer::new()
        .normalize("sig", SLOT, &[payer()], &[ix(1, &[0], &[])], None)
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "instruction 0 of transaction sig");
}

#[test]
fn inner_account_index_out_of_range_is_rejected() {
    let groups = inner_groups(json!([group(0, vec![inner(1, &[0], &[], 2), inner(1, &[9], &[], 2)])]));

    let err = InstructionNormalizer::new()
        .normalize("sig", SLOT, &[payer(), dex_program()], &[ix(1, &[0], &[])], Some(&groups))
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "instruction 0.1 of transaction sig");
    assert!(err.to_string().contains("references account 9, but the transaction has 2"));
}

#[test]
fn inner_group_of_a_missing_instruction_is_rejected() {
    let groups = inner_groups(json!([group(1, vec![inner(1, &[0], &[], 2)])]));

    let err = InstructionNormalizer::new()
        .normalize("sig", SLOT, &[payer(), dex_program()], &[ix(1, &[0], &[])], Some(&groups))
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "has only 1 instructions");
}

#[test]
fn two_inner_groups_for_one_instruction_are_rejected() {
    let groups = inner_groups(json!([
        group(0, vec![inner(1, &[0], &[], 2)]),
        group(0, vec![inner(1, &[0], &[], 2)]),
    ]));

    let err = InstructionNormalizer::new()
        .normalize("sig", SLOT, &[payer(), dex_program()], &[ix(1, &[0], &[])], Some(&groups))
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "two groups of inner instructions");
}

#[test]
fn parsed_inner_instruction_is_rejected() {
    let groups = inner_groups(json!([group(0, vec![json!({
        "program": "spl-token",
        "programId": s(token_program()),
        "parsed": {"type": "transfer", "info": {}},
        "stackHeight": 2,
    })])]));

    let err = InstructionNormalizer::new()
        .normalize("sig", SLOT, &[payer(), dex_program()], &[ix(1, &[0], &[])], Some(&groups))
        .unwrap_err();

    assert_deserialize_error(&err, SLOT, "instruction 0.0");
}

// ---------------------------------------------------------------------------
// Normalizer: the whole block
// ---------------------------------------------------------------------------

#[test]
fn fixture_covers_what_it_claims() {
    // Guards the other block tests: if the fixture drifted, they would
    // still pass while testing less.
    let counts = expected_counts(&mixed_block());
    assert_eq!(counts, [(2, 0), (2, 4), (1, 2), (1, 1), (1, 0)]);
}

#[test]
fn instruction_rows_match_the_raw_block_count() {
    let block = mixed_block();
    let expected = expected_counts(&block);
    let normalized = normalize(&block);

    let top_level: usize = expected.iter().map(|(top, _)| top).sum();
    let inner: usize = expected.iter().map(|(_, inner)| inner).sum();
    assert_eq!(normalized.instructions.len(), top_level + inner);
    assert_eq!(normalized.instructions.iter().filter(|row| row.is_top_level()).count(), top_level);
    assert_eq!(normalized.instructions.iter().filter(|row| row.is_inner()).count(), inner);
}

#[test]
fn instruction_rows_match_the_raw_count_per_transaction() {
    let block = mixed_block();
    let normalized = normalize(&block);

    for (tx, (top_level, inner)) in normalized.transactions.iter().zip(expected_counts(&block)) {
        let rows = rows_of(&normalized, &tx.tx_signature);
        assert_eq!(rows.len(), top_level + inner, "transaction {}", tx.transaction_index);
        assert_eq!(rows.iter().filter(|row| row.is_inner()).count(), inner, "transaction {}", tx.transaction_index);
    }
}

#[test]
fn block_produces_one_row_per_transaction_in_block_order() {
    let normalized = normalize(&mixed_block());

    assert_eq!(normalized.slot.transaction_count, 5);
    assert_eq!(normalized.transactions.len(), 5);
    for (index, tx) in normalized.transactions.iter().enumerate() {
        assert_eq!(tx.transaction_index, index as u32);
        assert_eq!(tx.slot, SLOT);
        assert_eq!(tx.blockhash, BLOCKHASH);
        assert_eq!(tx.tx_signature, signature(0xA0 + index as u8).to_string());
    }
    let statuses: Vec<_> = normalized.transactions.iter().map(|tx| tx.status).collect();
    use TransactionStatus::{Failed, Success};
    assert_eq!(statuses, [Success, Success, Success, Failed, Success]);
}

#[test]
fn instructions_are_grouped_by_transaction_in_block_order() {
    let normalized = normalize(&mixed_block());

    let order: Vec<&str> = normalized.instructions.iter().map(|row| row.tx_signature.as_str()).collect();
    let mut runs = order.clone();
    runs.dedup();
    let signatures: Vec<&str> = normalized
        .transactions
        .iter()
        .map(|tx| tx.tx_signature.as_str())
        .collect();
    assert_eq!(runs, signatures, "one contiguous run of rows per transaction, in block order");
    assert!(normalized.instructions.iter().all(|row| row.slot == SLOT));
}

#[test]
fn swap_transaction_flattens_into_the_documented_order() {
    let normalized = normalize(&mixed_block());

    let rows = rows_of(&normalized, &signature(0xA1).to_string());

    assert_eq!(
        positions(&rows),
        [(0, None), (1, None), (0, Some(0)), (0, Some(1)), (0, Some(2)), (1, Some(0))]
    );
    let programs: Vec<_> = rows.iter().map(|row| row.program_id.clone()).collect();
    assert_eq!(
        programs,
        [dex_program(), token_program(), token_program(), dex_program(), token_program(), token_program()].map(s)
    );
}

#[test]
fn instruction_positions_are_unique_within_each_transaction() {
    let normalized = normalize(&mixed_block());

    let mut seen = HashSet::new();
    for row in &normalized.instructions {
        let key = (&row.tx_signature, row.instruction_index, row.inner_instruction_index);
        assert!(seen.insert(key), "duplicate position {key:?}");
    }
}

#[test]
fn every_instruction_row_belongs_to_a_transaction_row() {
    let normalized = normalize(&mixed_block());

    let signatures: HashSet<_> = normalized.transactions.iter().map(|tx| &tx.tx_signature).collect();
    assert!(normalized.instructions.iter().all(|row| signatures.contains(&row.tx_signature)));
    // ...and every program and account named by a row is one its
    // transaction can reference.
    for tx in &normalized.transactions {
        for row in rows_of(&normalized, &tx.tx_signature) {
            assert!(tx.accounts.contains(&row.program_id));
            assert!(row.accounts.iter().all(|account| tx.accounts.contains(account)));
        }
    }
}

#[test]
fn inner_rows_point_at_an_existing_top_level_row() {
    let normalized = normalize(&mixed_block());

    for row in normalized.instructions.iter().filter(|row| row.is_inner()) {
        assert!(
            normalized.instructions.iter().any(|parent| parent.tx_signature == row.tx_signature
                && parent.is_top_level()
                && parent.instruction_index == row.instruction_index),
            "orphan inner row {row:?}"
        );
    }
}

#[test]
fn decoder_columns_are_left_empty() {
    let normalized = normalize(&mixed_block());

    assert!(normalized.instructions.iter().all(|row| !row.is_decoded() && row.decoded_data.is_none()));
}

#[test]
fn rows_convert_to_record_batches_of_the_same_size() {
    let normalized = normalize(&mixed_block());

    let instructions = DeepAnaInstruction::to_record_batch(&normalized.instructions).unwrap();
    let transactions = UdmRow::to_record_batch(&normalized.transactions).unwrap();
    let slots = UdmRow::to_record_batch(std::slice::from_ref(&normalized.slot)).unwrap();

    assert_eq!(instructions.num_rows(), normalized.instructions.len());
    assert_eq!(transactions.num_rows(), 5);
    assert_eq!(slots.num_rows(), 1);
}

#[test]
fn empty_block_has_a_slot_row_and_nothing_else() {
    let normalized = normalize(&chain_block(raw_block(vec![])));

    assert_eq!(normalized.slot.slot, SLOT);
    assert_eq!(normalized.slot.transaction_count, 0);
    assert!(normalized.transactions.is_empty());
    assert!(normalized.instructions.is_empty());
}

#[test]
fn one_bad_transaction_fails_the_whole_block() {
    let mut bad = tx2();
    bad.meta.as_mut().unwrap().loaded_addresses = None.into();
    let block = chain_block(raw_block(vec![tx0(), tx1(), bad, tx3()]));

    let err = Normalizer::new().normalize(&block).unwrap_err();

    assert_deserialize_error(&err, SLOT, "transaction 2");
}

#[test]
fn metrics_count_rows_of_normalized_blocks() {
    let normalizer = Normalizer::new();
    let block = mixed_block();

    normalizer.normalize(&block).unwrap();
    normalizer.normalize(&chain_block(raw_block(vec![]))).unwrap();

    assert_eq!(
        normalizer.metrics().snapshot(),
        NormalizerMetricsSnapshot {
            blocks_normalized: 2,
            blocks_failed: 0,
            transactions_normalized: 5,
            top_level_instructions: 7,
            inner_instructions: 7,
            last_normalized_slot: SLOT,
        }
    );
    assert_eq!(normalizer.metrics().snapshot().instructions_normalized(), 14);
}

#[test]
fn metrics_count_a_failed_block_without_its_rows() {
    let normalizer = Normalizer::new();
    let mut raw = raw_block(vec![tx0(), tx1()]);
    raw.blockhash.clear();

    normalizer.normalize(&chain_block(raw)).unwrap_err();

    let snapshot = normalizer.metrics().snapshot();
    assert_eq!(snapshot.blocks_failed, 1);
    assert_eq!(snapshot.blocks_normalized, 0);
    assert_eq!(snapshot.transactions_normalized, 0);
    assert_eq!(snapshot.instructions_normalized(), 0);
}

#[test]
fn clones_share_their_metrics() {
    let normalizer = Normalizer::new();
    let clone = normalizer.clone();

    clone.normalize(&mixed_block()).unwrap();

    assert_eq!(normalizer.metrics().snapshot().blocks_normalized, 1);
}
