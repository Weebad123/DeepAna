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
//!
//! The `live_*` tests do the same against real Solana devnet blocks fetched
//! through the Helius SDK, and print the normalized rows side by side with
//! the raw block they came from. Each block is checked against two sources
//! that share no code with the normalizer: the `base64` block decoded by the
//! SDK with its meta read field by field, and the same block fetched again as
//! `jsonParsed`, in which the RPC node resolves every account (lookup tables
//! included) and every instruction's program itself. They need
//! `HELIUS_API_KEY` and are skipped (with a note) without it. To see the
//! output:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test normalizer_test live_ -- --nocapture --test-threads=1
//! ```
//!
//! The side-by-side test also writes the report for its whole block to
//! `target/tmp/normalizer-devnet-slot-<slot>.txt`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::Path;

use helius::types::Cluster;
use prism::adapter::config::{SolanaAdapterConfig, TransactionEncoding};
use prism::adapter::solana::SolanaAdapter;
use prism::adapter::{ChainAdapter, ChainBlock, ChainBlockData, CommitmentLevel};
use prism::ingestor::{
    HELIUS_API_KEY_ENV, VOTE_PROGRAM_ID, helius_api_key_from_env, helius_solana_adapter,
};
use prism::normalizer::{
    InstructionNormalizer, NormalizedBlock, Normalizer, NormalizerMetricsSnapshot,
    SlotNormalizer, TransactionNormalizer,
};
use prism::types::error::PrismError;
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTransaction, TransactionStatus, UdmRow,
};
use serde_json::{Value, json};
use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{Hash, Message, MessageHeader, VersionedMessage, v0, v1};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{
    Encodable, EncodableWithMeta, EncodedConfirmedBlock, EncodedTransaction,
    EncodedTransactionWithStatusMeta, RewardType, UiInnerInstructions, UiInstruction,
    UiLoadedAddresses, UiMessage, UiParsedInstruction, UiTransactionEncoding,
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

// ---------------------------------------------------------------------------
// Live: Solana devnet
// ---------------------------------------------------------------------------

/// Confirmed slots this far below the tip have settled, so `getBlock`
/// answers for them and a second fetch returns the same block.
const TIP_MARGIN: u64 = 10;

/// How many slots below the tip the live tests search for a suitable block.
const SCAN_DEPTH: u64 = 120;

/// Width of the left (raw) column of the side-by-side report.
const COLUMN: usize = 60;

/// The two views of devnet the live tests compare against.
struct Live {
    /// Blocks exactly as the pipeline fetches them: `base64`.
    binary: SolanaAdapter,
    /// The same blocks as `jsonParsed`: every account and program resolved
    /// by the RPC node.
    parsed: SolanaAdapter,
}

/// The devnet adapters, or `None` (with a note) when no key is configured.
async fn live() -> Option<Live> {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return None;
    };
    let binary = helius_solana_adapter(&key, Cluster::Devnet).await.expect("helius adapter");
    let parsed = SolanaAdapter::new(SolanaAdapterConfig {
        encoding: TransactionEncoding::JsonParsed,
        // Leaves the free tier's quota to the main adapter.
        requests_per_second: 10,
        ..binary.config().clone()
    })
    .expect("jsonParsed adapter");
    // One probe up front, so an unreachable devnet fails here with its cause
    // rather than deep inside each test.
    if let Err(err) = binary.get_tip(CommitmentLevel::Confirmed).await {
        panic!("{}", unreachable_devnet(&err).replace(&key, "<HELIUS_API_KEY>"));
    }
    Some(Live { binary, parsed })
}

/// Why devnet could not be reached. A TLS-inspecting proxy otherwise shows
/// up only as a nested `InvalidCertificate(UnknownIssuer)`.
fn unreachable_devnet(err: &PrismError) -> String {
    let detail = format!("{err:?}");
    if !detail.contains("InvalidCertificate") {
        return format!("cannot reach devnet: {err}\n{detail}");
    }
    format!(
        "cannot reach devnet: the TLS certificate presented for the RPC host is signed by an \
         issuer this client does not trust.\n\
         Something on this network (a TLS-inspecting proxy or firewall) is intercepting HTTPS; \
         `openssl s_client -connect devnet.helius-rpc.com:443` shows the issuer.\n\
         The client trusts only the Mozilla roots compiled into it (reqwest \
         `rustls-tls-webpki-roots`), so adding a CA to the OS store does not help. Run from a \
         network without interception, or have the RPC host exempted from inspection.\n\n\
         {detail}"
    )
}

impl Live {
    /// Up to `limit` blocks that `keep` accepts, newest first, from the
    /// [`SCAN_DEPTH`] slots below the settled tip.
    async fn blocks(&self, limit: usize, keep: impl Fn(&ChainBlock) -> bool) -> Vec<ChainBlock> {
        let tip = self.binary.get_tip(CommitmentLevel::Confirmed).await.expect("devnet tip");
        let top = tip.slot_or_height - TIP_MARGIN;
        let mut found = Vec::new();
        for slot in (top - SCAN_DEPTH..=top).rev() {
            match self.binary.get_block(slot).await {
                Ok(block) if keep(&block) => {
                    found.push(block);
                    if found.len() == limit {
                        break;
                    }
                }
                Ok(_) | Err(PrismError::SlotSkipped { .. }) => {}
                Err(err) if err.is_retryable() => eprintln!("slot {slot}: {err}"),
                Err(err) => panic!("slot {slot}: {err}"),
            }
        }
        found
    }

    /// `block` fetched again as `jsonParsed`.
    async fn parsed(&self, block: &ChainBlock) -> EncodedConfirmedBlock {
        let parsed = self.parsed.get_block(block.slot_or_height).await.expect("jsonParsed block");
        let ChainBlockData::Solana(raw) = parsed.raw;
        *raw
    }
}

fn raw_of(block: &ChainBlock) -> &EncodedConfirmedBlock {
    let ChainBlockData::Solana(raw) = &block.raw;
    raw
}

/// A block with at least one cross-program invocation.
fn has_cpis(block: &ChainBlock) -> bool {
    raw_of(block).transactions.iter().any(|tx| RawTx::read(tx).inner() > 0)
}

/// A block with at least one transaction that loads lookup-table addresses.
fn has_lookups(block: &ChainBlock) -> bool {
    raw_of(block).transactions.iter().any(|tx| RawTx::read(tx).loaded() > 0)
}

fn has_transactions(block: &ChainBlock) -> bool {
    !raw_of(block).transactions.is_empty()
}

/// Instruction rows by transaction signature, in row order.
fn rows_by_signature(block: &NormalizedBlock) -> HashMap<&str, Vec<&DeepAnaInstruction>> {
    let mut rows: HashMap<&str, Vec<&DeepAnaInstruction>> = HashMap::new();
    for row in &block.instructions {
        rows.entry(row.tx_signature.as_str()).or_default().push(row);
    }
    rows
}

// --- The raw view: what the block itself says ------------------------------

/// One instruction as the block holds it, its indexes unresolved.
struct RawIx {
    /// Index of the top-level instruction that made this call; `None` for a
    /// top-level instruction.
    parent: Option<u8>,
    stack_height: Option<u32>,
    program_id_index: u8,
    accounts: Vec<u8>,
    /// Base58, as the RPC sends inner instruction data.
    data: String,
}

/// One transaction as the block holds it: decoded with the SDK, its meta
/// read field by field. Nothing here goes through the normalizer.
struct RawTx {
    signature: String,
    version: &'static str,
    static_keys: Vec<String>,
    lookup_tables: usize,
    loaded_writable: Vec<String>,
    loaded_readonly: Vec<String>,
    fee: Option<u64>,
    err: Option<Value>,
    compute_units: Option<u64>,
    log_lines: Option<usize>,
    /// Top-level instructions in message order, then each inner group as
    /// `meta.innerInstructions` lists it.
    instructions: Vec<RawIx>,
}

impl RawTx {
    fn read(tx: &EncodedTransactionWithStatusMeta) -> Self {
        let decoded = tx.transaction.decode().expect("the SDK decodes every devnet transaction");
        let meta = tx.meta.as_ref();
        let loaded: Option<&UiLoadedAddresses> =
            meta.and_then(|meta| meta.loaded_addresses.as_ref().into());
        let groups: Option<&Vec<UiInnerInstructions>> =
            meta.and_then(|meta| meta.inner_instructions.as_ref().into());

        let mut instructions: Vec<RawIx> = decoded
            .message
            .instructions()
            .iter()
            .map(|ix| RawIx {
                parent: None,
                stack_height: None,
                program_id_index: ix.program_id_index,
                accounts: ix.accounts.clone(),
                data: bs58::encode(&ix.data).into_string(),
            })
            .collect();
        for group in groups.into_iter().flatten() {
            for ix in &group.instructions {
                let UiInstruction::Compiled(ix) = ix else {
                    panic!("a base64 block lists its inner instructions compiled");
                };
                instructions.push(RawIx {
                    parent: Some(group.index),
                    stack_height: ix.stack_height,
                    program_id_index: ix.program_id_index,
                    accounts: ix.accounts.clone(),
                    data: ix.data.clone(),
                });
            }
        }

        Self {
            signature: decoded.signatures[0].to_string(),
            version: match decoded.message {
                VersionedMessage::Legacy(_) => "legacy",
                VersionedMessage::V0(_) => "v0",
                VersionedMessage::V1(_) => "v1",
            },
            static_keys: decoded.message.static_account_keys().iter().map(ToString::to_string).collect(),
            lookup_tables: decoded.message.address_table_lookups().map_or(0, <[_]>::len),
            loaded_writable: loaded.map(|loaded| loaded.writable.clone()).unwrap_or_default(),
            loaded_readonly: loaded.map(|loaded| loaded.readonly.clone()).unwrap_or_default(),
            fee: meta.map(|meta| meta.fee),
            err: meta
                .and_then(|meta| meta.err.as_ref())
                .map(|err| serde_json::to_value(err).expect("RPC errors serialize")),
            compute_units: meta.and_then(|meta| meta.compute_units_consumed.clone().into()),
            log_lines: meta
                .and_then(|meta| Option::<&Vec<String>>::from(meta.log_messages.as_ref()))
                .map(Vec::len),
            instructions,
        }
    }

    /// Every key an index can point at, in the order the runtime numbers
    /// them: static, loaded writable, loaded readonly.
    fn keys(&self) -> Vec<String> {
        [&self.static_keys, &self.loaded_writable, &self.loaded_readonly]
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    fn loaded(&self) -> usize {
        self.loaded_writable.len() + self.loaded_readonly.len()
    }

    fn is_vote(&self) -> bool {
        self.static_keys.iter().any(|key| key == VOTE_PROGRAM_ID)
    }

    fn top_level(&self) -> usize {
        self.instructions.iter().filter(|ix| ix.parent.is_none()).count()
    }

    fn inner(&self) -> usize {
        self.instructions.len() - self.top_level()
    }

    /// The `(instruction_index, inner_instruction_index)` each raw
    /// instruction must come out at, in raw order.
    fn positions(&self) -> Vec<(u32, Option<u32>)> {
        let mut next_top = 0;
        let mut next_inner: HashMap<u8, u32> = HashMap::new();
        self.instructions
            .iter()
            .map(|ix| match ix.parent {
                None => {
                    next_top += 1;
                    (next_top - 1, None)
                }
                Some(parent) => {
                    let inner = next_inner.entry(parent).or_default();
                    *inner += 1;
                    (u32::from(parent), Some(*inner - 1))
                }
            })
            .collect()
    }
}

/// The normalized rows of one transaction say exactly what the raw block
/// says, field by field and instruction by instruction.
fn assert_rows_match_raw(
    index: usize,
    raw: &RawTx,
    row: &DeepAnaTransaction,
    ix_rows: &[&DeepAnaInstruction],
) {
    let at = format!("tx {index} ({})", raw.signature);
    assert_eq!(row.transaction_index as usize, index, "{at}");
    assert_eq!(row.tx_signature, raw.signature, "{at}");
    assert_eq!(row.fee, raw.fee.unwrap_or(0), "{at}");
    let err = row.err.as_deref().map(|err| serde_json::from_str::<Value>(err).unwrap());
    assert_eq!(err, raw.err, "{at}");
    assert_eq!(row.status.is_success(), raw.err.is_none(), "{at}");
    assert_eq!(row.compute_units_consumed, raw.compute_units, "{at}");
    assert_eq!(row.log_messages.len(), raw.log_lines.unwrap_or(0), "{at}");
    let keys = raw.keys();
    assert_eq!(row.accounts, keys, "{at}");

    assert_eq!(ix_rows.len(), raw.instructions.len(), "{at}: one row per raw instruction");
    for ((ix_row, ix), position) in ix_rows.iter().zip(&raw.instructions).zip(raw.positions()) {
        let at = format!("{at} instruction {position:?}");
        assert_eq!((ix_row.instruction_index, ix_row.inner_instruction_index), position, "{at}");
        assert_eq!(ix_row.program_id, keys[usize::from(ix.program_id_index)], "{at}");
        let accounts: Vec<_> =
            ix.accounts.iter().map(|&account| keys[usize::from(account)].clone()).collect();
        assert_eq!(ix_row.accounts, accounts, "{at}");
        assert_eq!(ix_row.data_base58, ix.data, "{at}");
        assert!(!ix_row.is_decoded() && ix_row.decoded_data.is_none(), "{at}");
    }
}

// --- The node's view: `jsonParsed` ------------------------------------------

/// What the node resolved for one instruction of a `jsonParsed` block.
struct NodeIx {
    program_id: String,
    /// Accounts and data, for instructions the node left partially decoded.
    /// A parsed instruction (system, token, vote, ...) carries neither.
    accounts_and_data: Option<(Vec<String>, String)>,
}

/// The node's full account key list and its instructions, flattened in the
/// same order as the raw view.
fn node_view(tx: &EncodedTransactionWithStatusMeta) -> (String, Vec<String>, Vec<NodeIx>) {
    let EncodedTransaction::Json(ui) = &tx.transaction else {
        panic!("a jsonParsed block holds JSON transactions");
    };
    let UiMessage::Parsed(message) = &ui.message else {
        panic!("a jsonParsed block holds parsed messages");
    };
    let keys: Vec<String> = message.account_keys.iter().map(|key| key.pubkey.clone()).collect();
    let node_ix = |ix: &UiInstruction| match ix {
        UiInstruction::Parsed(UiParsedInstruction::Parsed(ix)) => NodeIx {
            program_id: ix.program_id.clone(),
            accounts_and_data: None,
        },
        UiInstruction::Parsed(UiParsedInstruction::PartiallyDecoded(ix)) => NodeIx {
            program_id: ix.program_id.clone(),
            accounts_and_data: Some((ix.accounts.clone(), ix.data.clone())),
        },
        UiInstruction::Compiled(ix) => NodeIx {
            program_id: keys[usize::from(ix.program_id_index)].clone(),
            accounts_and_data: Some((
                ix.accounts.iter().map(|&account| keys[usize::from(account)].clone()).collect(),
                ix.data.clone(),
            )),
        },
    };
    let mut instructions: Vec<NodeIx> = message.instructions.iter().map(node_ix).collect();
    let groups: Option<&Vec<UiInnerInstructions>> =
        tx.meta.as_ref().and_then(|meta| meta.inner_instructions.as_ref().into());
    for group in groups.into_iter().flatten() {
        instructions.extend(group.instructions.iter().map(node_ix));
    }
    (ui.signatures[0].clone(), keys, instructions)
}

/// Checks made against the node's view.
#[derive(Debug, Default)]
struct NodeChecks {
    transactions: usize,
    with_lookups: usize,
    loaded_accounts: usize,
    programs: usize,
    /// Instructions whose accounts and data were compared too.
    accounts_and_data: usize,
}

/// The normalized rows of one transaction agree with what the node resolved.
fn assert_rows_match_node(
    tx: &EncodedTransactionWithStatusMeta,
    row: &DeepAnaTransaction,
    ix_rows: &[&DeepAnaInstruction],
    checks: &mut NodeChecks,
) {
    let (signature, keys, instructions) = node_view(tx);
    let at = format!("tx {} ({signature})", row.transaction_index);
    assert_eq!(row.tx_signature, signature, "{at}: the two fetches disagree on the block");
    assert_eq!(row.accounts, keys, "{at}: account keys differ from the node's resolution");
    assert_eq!(ix_rows.len(), instructions.len(), "{at}: instruction count differs from the node's");
    for (ix_row, node) in ix_rows.iter().zip(&instructions) {
        let at = format!("{at} instruction ({}, {:?})", ix_row.instruction_index, ix_row.inner_instruction_index);
        assert_eq!(ix_row.program_id, node.program_id, "{at}");
        checks.programs += 1;
        if let Some((accounts, data)) = &node.accounts_and_data {
            assert_eq!(&ix_row.accounts, accounts, "{at}");
            assert_eq!(&ix_row.data_base58, data, "{at}");
            checks.accounts_and_data += 1;
        }
    }
    checks.transactions += 1;
    let static_keys = message_static_key_count(tx);
    if keys.len() > static_keys {
        checks.with_lookups += 1;
        checks.loaded_accounts += keys.len() - static_keys;
    }
}

/// How many of the node's keys are the message's own (not loaded).
fn message_static_key_count(tx: &EncodedTransactionWithStatusMeta) -> usize {
    let EncodedTransaction::Json(ui) = &tx.transaction else { unreachable!() };
    let UiMessage::Parsed(message) = &ui.message else { unreachable!() };
    message
        .account_keys
        .iter()
        .filter(|key| {
            serde_json::to_value(&key.source).is_ok_and(|source| source != json!("lookupTable"))
        })
        .count()
}

// --- The side-by-side report -------------------------------------------------

/// `left │ right`, the left side padded (or cut) to [`COLUMN`].
fn pair(out: &mut String, left: impl AsRef<str>, right: impl AsRef<str>) {
    let left = left.as_ref();
    let left = if left.chars().count() > COLUMN {
        format!("{}…", left.chars().take(COLUMN - 1).collect::<String>())
    } else {
        left.to_owned()
    };
    writeln!(out, "  {left:<COLUMN$} │ {}", right.as_ref()).unwrap();
}

/// `Abcd…wxyz` for a base58 key or signature.
fn short(value: &str) -> String {
    if value.len() <= 11 {
        value.to_owned()
    } else {
        format!("{}…{}", &value[..4], &value[value.len() - 4..])
    }
}

fn short_data(data: &str) -> String {
    match data.len() {
        0 => String::from("∅"),
        1..=12 => data.to_owned(),
        _ => format!("{}…", &data[..11]),
    }
}

/// At most `max` items, then `+n` for the rest.
fn list<T: ToString>(items: &[T], max: usize, show: impl Fn(&T) -> String) -> String {
    let mut shown: Vec<String> = items.iter().take(max).map(show).collect();
    if items.len() > max {
        shown.push(format!("+{}", items.len() - max));
    }
    format!("[{}]", shown.join(", "))
}

fn opt<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| String::from("—"), |value| value.to_string())
}

fn report_slot(out: &mut String, block: &ChainBlock, row: &DeepAnaSlot) {
    let raw = raw_of(block);
    let fee_reward = raw.rewards.iter().find(|reward| reward.reward_type == Some(RewardType::Fee));
    writeln!(out, "\n━━ slot {} ━━", block.slot_or_height).unwrap();
    pair(out, "RAW BLOCK HEADER  (getBlock)", "NORMALIZED  (DeepAnaSlot)");
    pair(out, format!("slot (requested)  {}", block.slot_or_height), format!("slot               {}", row.slot));
    pair(out, format!("blockhash         {}", raw.blockhash), format!("blockhash          {}", row.blockhash));
    pair(out, format!("parentSlot        {}", raw.parent_slot), format!("parent_slot        {}", row.parent_slot));
    pair(out, format!("blockTime         {}", opt(raw.block_time)), format!("block_time         {}", opt(row.block_time)));
    pair(out, format!("blockHeight       {}", opt(raw.block_height)), format!("block_height       {}", opt(row.block_height)));
    pair(out, format!("transactions      {}", raw.transactions.len()), format!("transaction_count  {}", row.transaction_count));
    pair(
        out,
        format!("rewards[Fee]      {}", opt(fee_reward.map(|reward| reward.pubkey.clone()))),
        format!("leader             {}", opt(row.leader.clone())),
    );
}

fn report_transaction(
    out: &mut String,
    index: usize,
    raw: &RawTx,
    row: &DeepAnaTransaction,
    ix_rows: &[&DeepAnaInstruction],
) {
    let vote = if raw.is_vote() { " · vote" } else { "" };
    writeln!(out, "\n── tx {index} · {} · {}{vote}", raw.signature, raw.version).unwrap();
    pair(out, "RAW BLOCK  (base64 transaction + meta)", "NORMALIZED  (DeepAnaTransaction)");
    pair(out, format!("signature[0]    {}", short(&raw.signature)), format!("tx_signature            {}", short(&row.tx_signature)));
    pair(out, format!("position        {index}"), format!("transaction_index       {}", row.transaction_index));
    pair(out, format!("meta.fee        {}", opt(raw.fee)), format!("fee                     {}", row.fee));
    pair(
        out,
        format!("meta.err        {}", raw.err.as_ref().map_or_else(|| String::from("null"), Value::to_string)),
        format!("status / err            {} / {}", row.status, row.err.as_deref().unwrap_or("—")),
    );
    pair(out, format!("meta.computeUnitsConsumed  {}", opt(raw.compute_units)), format!("compute_units_consumed  {}", opt(row.compute_units_consumed)));
    pair(out, format!("meta.logMessages  {} lines", opt(raw.log_lines)), format!("log_messages            {} lines", row.log_messages.len()));
    pair(
        out,
        format!(
            "account keys    {} static + {} loaded ({} lookup tables)",
            raw.static_keys.len(),
            raw.loaded(),
            raw.lookup_tables
        ),
        format!("accounts                {}", row.accounts.len()),
    );
    let inner_rows = ix_rows.iter().filter(|ix| ix.is_inner()).count();
    pair(
        out,
        format!("instructions    {} top-level + {} inner", raw.top_level(), raw.inner()),
        format!("instruction rows        {} top-level + {inner_rows} inner", ix_rows.len() - inner_rows),
    );
    pair(out, "  ix          program   accounts (indexes)        data", "  (ix, inner)  program_id   accounts                         data");
    for (ix, ix_row) in raw.instructions.iter().zip(ix_rows) {
        let at = match ix.parent {
            None => String::from("top"),
            Some(parent) => format!("↳{parent} h{}", opt(ix.stack_height)),
        };
        pair(
            out,
            format!(
                "  {at:<10}  #{:<7}  {:<24}  {}",
                ix.program_id_index,
                list(&ix.accounts, 6, u8::to_string),
                short_data(&ix.data)
            ),
            format!(
                "  ({}, {})  {:<11}  {:<31}  {}",
                ix_row.instruction_index,
                opt(ix_row.inner_instruction_index),
                short(&ix_row.program_id),
                list(&ix_row.accounts, 3, |key| short(key)),
                short_data(&ix_row.data_base58)
            ),
        );
    }
    if raw.instructions.len() != ix_rows.len() {
        pair(out, format!("!! {} raw instructions", raw.instructions.len()), format!("!! {} rows", ix_rows.len()));
    }
}

// --- Tests --------------------------------------------------------------------

/// A real devnet block with CPIs, normalized and laid out side by side with
/// the raw block: every transaction and every instruction checked field by
/// field. Prints the non-vote transactions (and one vote); the full block
/// goes to a report file.
#[tokio::test]
async fn live_devnet_block_normalizes_side_by_side_with_the_raw_block() {
    const PRINTED_NON_VOTES: usize = 12;
    let Some(live) = live().await else { return };
    let block = match live.blocks(1, has_cpis).await.pop() {
        Some(block) => block,
        None => live.blocks(1, has_transactions).await.pop().expect("a devnet block with transactions"),
    };
    let raw = raw_of(&block);
    let normalizer = Normalizer::new();
    let normalized = normalizer.normalize(&block).expect("the devnet block normalizes");
    let rows = rows_by_signature(&normalized);
    let raw_txs: Vec<RawTx> = raw.transactions.iter().map(RawTx::read).collect();

    let mut full = String::new();
    let mut printed = String::new();
    report_slot(&mut full, &block, &normalized.slot);
    printed.push_str(&full);
    let (mut non_votes, mut votes) = (0, 0);
    for (index, (raw_tx, row)) in raw_txs.iter().zip(&normalized.transactions).enumerate() {
        let ix_rows = rows.get(row.tx_signature.as_str()).map_or(&[][..], Vec::as_slice);
        let mut section = String::new();
        report_transaction(&mut section, index, raw_tx, row, ix_rows);
        full.push_str(&section);
        let shown = if raw_tx.is_vote() { &mut votes } else { &mut non_votes };
        if *shown < if raw_tx.is_vote() { 1 } else { PRINTED_NON_VOTES } {
            printed.push_str(&section);
        }
        *shown += 1;
    }
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("normalizer-devnet-slot-{}.txt", block.slot_or_height));
    std::fs::write(&path, &full).expect("write the report");
    let m = normalizer.metrics().snapshot();
    println!("{printed}");
    println!(
        "\nslot {}: {} transactions ({non_votes} non-vote, {votes} vote) → {} transaction rows, \
         {} instruction rows ({} top-level + {} inner)\nfull side-by-side report: {}",
        block.slot_or_height,
        raw.transactions.len(),
        normalized.transactions.len(),
        normalized.instructions.len(),
        m.top_level_instructions,
        m.inner_instructions,
        path.display(),
    );

    assert_eq!(normalized.slot.slot, block.slot_or_height);
    assert_eq!(normalized.slot.blockhash, raw.blockhash);
    assert_eq!(normalized.slot.parent_slot, raw.parent_slot);
    assert_eq!(normalized.slot.block_time, raw.block_time);
    assert_eq!(normalized.slot.block_height, raw.block_height);
    assert_eq!(normalized.slot.transaction_count as usize, raw.transactions.len());
    assert_eq!(normalized.transactions.len(), raw_txs.len());
    for (index, (raw_tx, row)) in raw_txs.iter().zip(&normalized.transactions).enumerate() {
        let ix_rows = rows.get(row.tx_signature.as_str()).map_or(&[][..], Vec::as_slice);
        assert_rows_match_raw(index, raw_tx, row, ix_rows);
    }
}

/// Over a run of recent blocks, the instruction rows per transaction and per
/// block are exactly the raw block's top-level plus inner instructions.
#[tokio::test]
async fn live_instruction_rows_match_raw_counts_across_recent_blocks() {
    const BLOCKS: usize = 8;
    let Some(live) = live().await else { return };
    let blocks = live.blocks(BLOCKS, has_transactions).await;
    assert!(blocks.len() >= BLOCKS / 2, "only {} devnet blocks with transactions found", blocks.len());
    let normalizer = Normalizer::new();

    println!(
        "\n  {:>11}  {:>5}  {:>5}  │ {:>8}  {:>9}  │ {:>8}  {:>10}  {:>6}",
        "slot", "txs", "votes", "raw top", "raw inner", "rows top", "rows inner", "rows"
    );
    let (mut all_txs, mut all_top, mut all_inner) = (0, 0, 0);
    for block in &blocks {
        let normalized = normalizer.normalize(block).expect("the devnet block normalizes");
        let rows = rows_by_signature(&normalized);
        let raw_txs: Vec<RawTx> = raw_of(block).transactions.iter().map(RawTx::read).collect();
        assert_eq!(normalized.transactions.len(), raw_txs.len(), "slot {}", block.slot_or_height);

        for (raw_tx, row) in raw_txs.iter().zip(&normalized.transactions) {
            let ix_rows = rows.get(row.tx_signature.as_str()).map_or(&[][..], Vec::as_slice);
            let at = format!("slot {} tx {}", block.slot_or_height, raw_tx.signature);
            assert_eq!(row.tx_signature, raw_tx.signature, "{at}");
            assert_eq!(ix_rows.iter().filter(|ix| ix.is_top_level()).count(), raw_tx.top_level(), "{at}");
            assert_eq!(ix_rows.iter().filter(|ix| ix.is_inner()).count(), raw_tx.inner(), "{at}");
        }
        let raw_top: usize = raw_txs.iter().map(RawTx::top_level).sum();
        let raw_inner: usize = raw_txs.iter().map(RawTx::inner).sum();
        let rows_inner = normalized.instructions.iter().filter(|ix| ix.is_inner()).count();
        let rows_top = normalized.instructions.len() - rows_inner;
        println!(
            "  {:>11}  {:>5}  {:>5}  │ {raw_top:>8}  {raw_inner:>9}  │ {rows_top:>8}  {rows_inner:>10}  {:>6}",
            block.slot_or_height,
            raw_txs.len(),
            raw_txs.iter().filter(|tx| tx.is_vote()).count(),
            normalized.instructions.len(),
        );
        assert_eq!((rows_top, rows_inner), (raw_top, raw_inner), "slot {}", block.slot_or_height);
        all_txs += raw_txs.len();
        all_top += raw_top;
        all_inner += raw_inner;
    }

    let m = normalizer.metrics().snapshot();
    println!(
        "  {:>11}  {all_txs:>5}  {:>5}  │ {all_top:>8}  {all_inner:>9}  │ {:>8}  {:>10}  {:>6}",
        "total",
        "",
        m.top_level_instructions,
        m.inner_instructions,
        m.instructions_normalized()
    );
    assert_eq!(m.blocks_normalized, blocks.len() as u64);
    assert_eq!(m.transactions_normalized, all_txs as u64);
    assert_eq!(m.top_level_instructions, all_top as u64);
    assert_eq!(m.inner_instructions, all_inner as u64);
}

/// The same blocks fetched as `jsonParsed`, where the node resolves every
/// account and program itself: the normalized rows must agree with it,
/// including the accounts of v0 transactions loaded from lookup tables.
#[tokio::test]
async fn live_rows_agree_with_the_nodes_own_account_resolution() {
    const BLOCKS: usize = 3;
    let Some(live) = live().await else { return };
    let blocks = live.blocks(BLOCKS, has_cpis).await;
    let blocks = if blocks.is_empty() { live.blocks(BLOCKS, has_transactions).await } else { blocks };
    assert!(!blocks.is_empty(), "no devnet block with transactions found");

    let mut total = NodeChecks::default();
    for block in &blocks {
        let normalized = Normalizer::new().normalize(block).expect("the devnet block normalizes");
        let parsed = live.parsed(block).await;
        assert_eq!(parsed.blockhash, normalized.slot.blockhash, "slot {} was refetched from another fork", block.slot_or_height);
        assert_eq!(parsed.transactions.len(), normalized.transactions.len(), "slot {}", block.slot_or_height);
        let rows = rows_by_signature(&normalized);

        let mut checks = NodeChecks::default();
        for (tx, row) in parsed.transactions.iter().zip(&normalized.transactions) {
            let ix_rows = rows.get(row.tx_signature.as_str()).map_or(&[][..], Vec::as_slice);
            assert_rows_match_node(tx, row, ix_rows, &mut checks);
        }
        println!(
            "slot {}: {} transactions agree with the node ({} with lookup tables, {} loaded accounts) | \
             {} instruction programs, {} with accounts and data",
            block.slot_or_height,
            checks.transactions,
            checks.with_lookups,
            checks.loaded_accounts,
            checks.programs,
            checks.accounts_and_data,
        );
        total.transactions += checks.transactions;
        total.with_lookups += checks.with_lookups;
        total.programs += checks.programs;
        total.accounts_and_data += checks.accounts_and_data;
    }
    assert!(total.programs > 0, "nothing was compared");
}

/// v0 transactions that load accounts from lookup tables, side by side: the
/// loaded keys must follow the static ones (writable, then readonly), and
/// the node must resolve them the same way. Devnet traffic varies, so when
/// no such transaction is found nearby the test says so and passes.
#[tokio::test]
async fn live_versioned_transactions_resolve_lookup_table_accounts() {
    const PRINTED: usize = 3;
    let Some(live) = live().await else { return };
    let Some(block) = live.blocks(1, has_lookups).await.pop() else {
        eprintln!("no v0 transaction with lookup tables in the last {SCAN_DEPTH} devnet slots; nothing to check");
        return;
    };
    let normalized = Normalizer::new().normalize(&block).expect("the devnet block normalizes");
    let parsed = live.parsed(&block).await;
    assert_eq!(parsed.blockhash, normalized.slot.blockhash, "slot {} was refetched from another fork", block.slot_or_height);
    let rows = rows_by_signature(&normalized);

    let mut out = String::new();
    let mut checked = 0;
    let mut checks = NodeChecks::default();
    for (index, (tx, row)) in raw_of(&block).transactions.iter().zip(&normalized.transactions).enumerate() {
        let raw = RawTx::read(tx);
        if raw.loaded() == 0 {
            continue;
        }
        let ix_rows = rows.get(row.tx_signature.as_str()).map_or(&[][..], Vec::as_slice);
        if checked < PRINTED {
            report_transaction(&mut out, index, &raw, row, ix_rows);
            let loaded_used = ix_rows
                .iter()
                .flat_map(|ix| &ix.accounts)
                .filter(|key| row.accounts[raw.static_keys.len()..].contains(key))
                .count();
            writeln!(
                out,
                "  loaded writable {} · loaded readonly {} · {loaded_used} instruction account slots point at loaded keys",
                list(&raw.loaded_writable, 4, |key| short(key)),
                list(&raw.loaded_readonly, 4, |key| short(key)),
            )
            .unwrap();
        }
        assert_eq!(raw.version, "v0", "tx {index}");
        assert_eq!(row.accounts[..raw.static_keys.len()], raw.static_keys[..], "tx {index}");
        let loaded: Vec<String> = raw.loaded_writable.iter().chain(&raw.loaded_readonly).cloned().collect();
        assert_eq!(row.accounts[raw.static_keys.len()..], loaded[..], "tx {index}");
        assert_rows_match_raw(index, &raw, row, ix_rows);
        assert_rows_match_node(&parsed.transactions[index], row, ix_rows, &mut checks);
        checked += 1;
    }
    println!("{out}");
    println!(
        "\nslot {}: {checked} v0 transactions with lookup tables match the raw block and the node \
         ({} loaded accounts)",
        block.slot_or_height, checks.loaded_accounts
    );
    assert!(checked > 0);
}
