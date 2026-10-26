//! Tests for `prism::decoder`.
//!
//! SPL Token instructions are built with the token programs' own instruction
//! builders (`spl-token-interface`, `spl-token-2022-interface`), so the bytes
//! and account order the decoder reads are exactly what a wallet sends.
//! Anchor instructions are built from `tests/fixtures/vault_idl.json`, an
//! Anchor 0.30+ IDL that uses every IDL type the decoder reads, with the
//! bodies Borsh-encoded by `borsh`'s derive from mirror Rust types: the
//! encoder and the decoder share no code.
//!
//! The `*_decoding_rate` tests measure how much of a corpus each decoder
//! decodes and print a report. The offline ones run on a synthetic corpus;
//! the `live_*` ones run on recent Solana devnet blocks fetched through the
//! Helius SDK, normalized and decoded exactly as the pipeline does. The
//! live tests need `HELIUS_API_KEY` and are skipped (with a note) without
//! it:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test decoder_test decoding_rate -- --nocapture --test-threads=1
//! ```
//!
//! Optional knobs for the live tests:
//! * `PRISM_DECODE_RATE_BLOCKS`: blocks to scan (default 20).
//! * `PRISM_ANCHOR_IDLS`: comma-separated paths of Anchor 0.30+ IDL JSON
//!   files to decode with. Their programs must decode 100%.
//! * Without it (or in addition), the Anchor test looks for on-chain IDLs
//!   (the legacy `anchor:idl` account) of every program in the scanned
//!   blocks and reports their rate without asserting it: an on-chain IDL may
//!   be stale relative to the deployed program.

// `Transfer` is deprecated in favour of `TransferChecked` but still sent.
#![allow(deprecated)]

use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;

use borsh::BorshSerialize;
use helius::types::Cluster;
use prism::adapter::solana::SolanaAdapter;
use prism::adapter::{ChainAdapter, ChainBlock, CommitmentLevel};
use prism::decoder::anchor::EVENT_TYPE_PREFIX;
use prism::decoder::discriminant::{EVENT_IX_TAG, IDL_IX_TAG};
use prism::decoder::spl_token::{SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
use prism::decoder::{
    AnchorDecoder, DecodeResult, Decoder, DecoderRegistry, Discriminant, RegistryMetricsSnapshot,
    SplTokenDecoder,
};
use prism::ingestor::{HELIUS_API_KEY_ENV, helius_api_key_from_env, helius_solana_adapter};
use prism::normalizer::{InstructionNormalizer, NormalizedBlock, Normalizer};
use prism::types::error::PrismError;
use prism::types::udm::{
    DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022_interface::extension::ExtensionType;
use spl_token_2022_interface::extension::transfer_fee::instruction as transfer_fee;
use spl_token_2022_interface::instruction as token_2022;
use spl_token_2022_interface::instruction::AuthorityType;
use spl_token_interface::instruction as token;

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const SLOT: u64 = 250_000_000;
const SIG: &str = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";
const BLOCK_TIME: i64 = 1_700_000_000;
const VAULT_IDL: &str = include_str!("fixtures/vault_idl.json");
const VAULT_PROGRAM_ID: &str = "Fg6PaFpoGXkYsidMpWTK6W2BeZ7FEfcYkg476zPFsLnS";

fn key(n: u8) -> Pubkey {
    Pubkey::new_from_array([n; 32])
}
fn source() -> Pubkey {
    key(1)
}
fn destination() -> Pubkey {
    key(2)
}
fn owner() -> Pubkey {
    key(3)
}
fn mint() -> Pubkey {
    key(4)
}

fn spl_id() -> Pubkey {
    spl_token_interface::id()
}
fn token_2022_id() -> Pubkey {
    spl_token_2022_interface::id()
}

/// An instruction row as the normalizer makes it.
fn row(program_id: &Pubkey, accounts: Vec<String>, data: &[u8]) -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot: SLOT,
        tx_signature: SIG.to_owned(),
        instruction_index: 0,
        inner_instruction_index: None,
        program_id: program_id.to_string(),
        accounts,
        data_base58: bs58::encode(data).into_string(),
        instruction_type: None,
        decoded_data: None,
    }
}

/// The row of an instruction built by a token program's builder.
macro_rules! row_of {
    ($ix:expr) => {{
        let ix = $ix.expect("the builder accepts its arguments");
        row(
            &ix.program_id,
            ix.accounts.iter().map(|meta| meta.pubkey.to_string()).collect(),
            &ix.data,
        )
    }};
}

fn spl_decode(ix: &DeepAnaInstruction) -> Result<DecodeResult, PrismError> {
    let decoder = SplTokenDecoder::for_program(&ix.program_id).expect("a token program");
    let data = ix.data_bytes().unwrap();
    assert!(decoder.can_decode(&ix.program_id, &data), "decoder declines {ix:?}");
    decoder.decode(ix, &ix.accounts)
}

fn vault() -> AnchorDecoder {
    AnchorDecoder::from_json(VAULT_IDL).expect("the vault IDL loads")
}

fn vault_id() -> Pubkey {
    Pubkey::from_str(VAULT_PROGRAM_ID).unwrap()
}

/// An Anchor instruction: `discriminant ‖ borsh(body)`.
fn anchor_data(discriminant: &[u8], body: &impl BorshSerialize) -> Vec<u8> {
    let mut data = discriminant.to_vec();
    data.extend(borsh::to_vec(body).unwrap());
    data
}

fn anchor_row(data: &[u8]) -> DeepAnaInstruction {
    row(&vault_id(), vec![key(10).to_string()], data)
}

fn sha_prefix(preimage: &str) -> [u8; 8] {
    Sha256::digest(preimage.as_bytes())[..8].try_into().unwrap()
}

fn decode_reason(err: &PrismError) -> &str {
    match err {
        PrismError::DecodeError { reason, .. } => reason,
        other => panic!("expected a DecodeError, got {other:?}"),
    }
}

/// Small deterministic PRNG for the robustness tests (no `rand` dependency).
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

// Mirror types of the vault IDL, encoded by borsh's derive.

#[derive(BorshSerialize)]
struct Deposit {
    amount: u64,
    memo: Option<String>,
}

#[derive(BorshSerialize)]
#[allow(dead_code)]
enum Mode {
    Off,
    Fixed { rate: u64 },
    Range(u32, u32),
}

#[derive(BorshSerialize)]
struct Config {
    fee_bps: u16,
    admins: Vec<[u8; 32]>,
    mode: Mode,
    limits: [u64; 3],
    ratio: f64,
    small: i8,
    medium: i32,
    signed: i64,
    big: u128,
    big_negative: i128,
    flag: bool,
    payload: Vec<u8>,
    wide: [u8; 32],
    wide_negative: [u8; 32],
    pair: (u8, u16),
}

fn config(mode: Mode) -> Config {
    let mut wide = [0u8; 32];
    wide[..16].copy_from_slice(&u128::MAX.to_le_bytes());
    wide[16] = 1; // 2^128 + (2^128 - 1) = 2^129 - 1
    Config {
        fee_bps: 30,
        admins: vec![owner().to_bytes(), mint().to_bytes()],
        mode,
        limits: [1, 2, u64::MAX],
        ratio: 0.25,
        small: -8,
        medium: -70_000,
        signed: i64::MIN,
        big: u128::MAX,
        big_negative: i128::MIN,
        flag: true,
        payload: vec![0xde, 0xad, 0xbe, 0xef],
        wide,
        wide_negative: [0xff; 32],
        pair: (7, 65_535),
    }
}

#[derive(BorshSerialize)]
struct Wrap {
    items: [u32; 2],
    extra: Option<u32>,
    id: [u8; 4],
}

#[derive(BorshSerialize)]
struct DepositEvent {
    user: [u8; 32],
    amount: u64,
}

const DEPOSIT: [u8; 8] = [242, 35, 198, 137, 82, 225, 242, 182];
const CONFIGURE: [u8; 8] = [245, 7, 108, 117, 95, 196, 54, 217];
const WRAP: [u8; 8] = [178, 40, 10, 189, 228, 129, 186, 140];
const INITIALIZE: [u8; 8] = [175, 175, 109, 31, 13, 152, 155, 237];
const DEPOSIT_EVENT: [u8; 8] = [120, 248, 61, 83, 31, 142, 107, 144];

fn event_data(event: &DepositEvent) -> Vec<u8> {
    let mut data = EVENT_IX_TAG.to_bytes().to_vec();
    data.extend(DEPOSIT_EVENT);
    data.extend(borsh::to_vec(event).unwrap());
    data
}

// ---------------------------------------------------------------------------
// Discriminant
// ---------------------------------------------------------------------------

#[test]
fn instruction_discriminant_is_the_global_namespace_hash() {
    // Known value: every Anchor program's `initialize`.
    assert_eq!(Discriminant::for_instruction("initialize").to_bytes(), INITIALIZE);
    assert_eq!(Discriminant::for_instruction("deposit").to_bytes(), sha_prefix("global:deposit"));
    assert_eq!(Discriminant::for_instruction("deposit").to_string(), "f223c68952e1f2b6");
}

#[test]
fn instruction_discriminant_hashes_the_snake_case_name() {
    for (camel, snake) in [
        ("initializePool", "initialize_pool"),
        ("swapV2", "swap_v2"),
        ("createATA", "create_ata"),
        ("set_fee2Rate", "set_fee2_rate"),
    ] {
        assert_eq!(
            Discriminant::for_instruction(camel),
            Discriminant::for_instruction(snake),
            "{camel}"
        );
        assert_eq!(
            Discriminant::for_instruction(snake).to_bytes(),
            sha_prefix(&format!("global:{snake}"))
        );
    }
}

#[test]
fn account_and_event_discriminants_keep_the_pascal_case_name() {
    assert_eq!(Discriminant::for_account("Vault").to_bytes(), sha_prefix("account:Vault"));
    assert_eq!(Discriminant::for_event("DepositEvent").to_bytes(), DEPOSIT_EVENT);
    assert_eq!(
        Discriminant::namespaced("custom", "Name").to_bytes(),
        sha_prefix("custom:Name")
    );
}

#[test]
fn reserved_tags_match_anchor_and_are_the_reversed_hash() {
    assert_eq!(EVENT_IX_TAG.as_bytes().as_slice(), anchor_lang::event::EVENT_IX_TAG_LE);
    assert_eq!(IDL_IX_TAG.as_bytes().as_slice(), anchor_lang::idl::IDL_IX_TAG_LE);
    let mut reversed = sha_prefix("anchor:event");
    reversed.reverse();
    assert_eq!(EVENT_IX_TAG.to_bytes(), reversed);
    assert_eq!(EVENT_IX_TAG.to_string(), "e445a52e51cb9a1d");
}

#[test]
fn discriminant_is_read_from_the_front_of_the_data() {
    let mut data = INITIALIZE.to_vec();
    data.extend([1, 2, 3]);
    let d = Discriminant::from_data(&data).unwrap();
    assert_eq!(d.to_bytes(), INITIALIZE);
    assert!(d.matches(&data));
    assert!(!d.matches(&data[1..]));
    assert_eq!(Discriminant::from_data(&INITIALIZE[..7]), None);
    assert_eq!(Discriminant::from_data(&[]), None);
    assert!(Discriminant::try_from(&data[..]).is_err(), "exactly 8 bytes");
    assert_eq!(Discriminant::try_from(&data[..8]).unwrap(), d);
    assert_eq!(Discriminant::from(INITIALIZE), d);
    assert_eq!(format!("{d:?}"), "Discriminant(afaf6d1f0d989bed)");
}

// ---------------------------------------------------------------------------
// SplTokenDecoder: transfers
// ---------------------------------------------------------------------------

#[test]
fn program_id_constants_match_the_token_programs() {
    assert_eq!(SPL_TOKEN_PROGRAM_ID, spl_id().to_string());
    assert_eq!(TOKEN_2022_PROGRAM_ID, token_2022_id().to_string());
    assert_eq!(SplTokenDecoder::spl_token().program_id(), SPL_TOKEN_PROGRAM_ID);
    assert_eq!(SplTokenDecoder::token_2022().program_id(), TOKEN_2022_PROGRAM_ID);
    assert!(SplTokenDecoder::token_2022().is_token_2022());
    assert_eq!(SplTokenDecoder::for_program(VAULT_PROGRAM_ID), None);
}

#[test]
fn transfer_becomes_a_transfer_row_without_mint() {
    let ix = row_of!(token::transfer(&spl_id(), &source(), &destination(), &owner(), &[], 1_500));
    let result = spl_decode(&ix).unwrap();

    assert_eq!(result.instruction_type, "Transfer");
    assert_eq!(result.decoded_data, json!({ "amount": 1_500 }));
    assert_eq!(result.decoded_data_json(), r#"{"amount":1500}"#);
    assert_eq!(
        result.token_transfer,
        Some(DeepAnaTokenTransfer {
            slot: SLOT,
            tx_signature: SIG.to_owned(),
            instruction_index: 0,
            inner_instruction_index: None,
            spl_token_program_id: SPL_TOKEN_PROGRAM_ID.to_owned(),
            source_token_account: source().to_string(),
            destination_token_account: destination().to_string(),
            source_owner: None,
            destination_owner: None,
            mint_token_address: None,
            amount_raw: 1_500,
            decimals: None,
            block_time: None,
        })
    );
}

#[test]
fn transfer_checked_takes_mint_and_decimals_from_the_instruction() {
    let ix = row_of!(token::transfer_checked(
        &spl_id(),
        &source(),
        &mint(),
        &destination(),
        &owner(),
        &[],
        u64::MAX,
        9
    ));
    let result = spl_decode(&ix).unwrap();

    assert_eq!(result.instruction_type, "TransferChecked");
    assert_eq!(result.decoded_data, json!({ "amount": u64::MAX, "decimals": 9 }));
    let transfer = result.token_transfer.unwrap();
    assert_eq!(transfer.source_token_account, source().to_string());
    assert_eq!(transfer.destination_token_account, destination().to_string());
    assert_eq!(transfer.mint_token_address, Some(mint().to_string()));
    assert_eq!(transfer.decimals, Some(9));
    assert_eq!(transfer.amount_raw, u64::MAX);
    assert!(!transfer.is_token_2022());
}

#[test]
fn multisig_transfer_reads_the_same_positions() {
    let signers = [key(20), key(21)];
    let refs: Vec<&Pubkey> = signers.iter().collect();
    let ix = row_of!(token::transfer(&spl_id(), &source(), &destination(), &owner(), &refs, 5));
    assert_eq!(ix.accounts.len(), 5);
    let transfer = spl_decode(&ix).unwrap().token_transfer.unwrap();
    assert_eq!(transfer.source_token_account, source().to_string());
    assert_eq!(transfer.destination_token_account, destination().to_string());
}

#[test]
fn token_2022_transfers_carry_the_token_2022_program() {
    let ix = row_of!(token_2022::transfer_checked(
        &token_2022_id(),
        &source(),
        &mint(),
        &destination(),
        &owner(),
        &[],
        42,
        6
    ));
    let transfer = spl_decode(&ix).unwrap().token_transfer.unwrap();
    assert_eq!(transfer.spl_token_program_id, TOKEN_2022_PROGRAM_ID);
    assert!(transfer.is_token_2022());
    assert_eq!(transfer.mint_token_address, Some(mint().to_string()));
}

#[test]
fn transfer_checked_with_fee_is_a_transfer() {
    let ix = row_of!(transfer_fee::transfer_checked_with_fee(
        &token_2022_id(),
        &source(),
        &mint(),
        &destination(),
        &owner(),
        &[],
        10_000,
        2,
        25
    ));
    let result = spl_decode(&ix).unwrap();
    assert_eq!(result.instruction_type, "TransferCheckedWithFee");
    assert_eq!(result.decoded_data, json!({ "amount": 10_000, "decimals": 2, "fee": 25 }));
    let transfer = result.token_transfer.unwrap();
    assert_eq!(transfer.amount_raw, 10_000);
    assert_eq!(transfer.mint_token_address, Some(mint().to_string()));
    assert_eq!(transfer.destination_token_account, destination().to_string());
}

#[test]
fn transfer_row_keeps_the_instruction_position() {
    let mut ix = row_of!(token::transfer(&spl_id(), &source(), &destination(), &owner(), &[], 1));
    ix.instruction_index = 3;
    ix.inner_instruction_index = Some(7);
    let transfer = spl_decode(&ix).unwrap().token_transfer.unwrap();
    assert_eq!((transfer.instruction_index, transfer.inner_instruction_index), (3, Some(7)));
    assert_eq!(transfer.slot, SLOT);
    assert_eq!(transfer.tx_signature, SIG);
}

#[test]
fn transfer_with_too_few_accounts_fails() {
    let mut ix = row_of!(token::transfer_checked(
        &spl_id(),
        &source(),
        &mint(),
        &destination(),
        &owner(),
        &[],
        1,
        0
    ));
    ix.accounts.truncate(3);
    let err = spl_decode(&ix).unwrap_err();
    assert!(decode_reason(&err).contains("at least 4 accounts"), "{err}");
}

// ---------------------------------------------------------------------------
// SplTokenDecoder: every other instruction
// ---------------------------------------------------------------------------

/// `(instruction, expected type, expected fields)` for every SPL Token
/// instruction that is not a transfer, built by the program's own builders.
fn non_transfer_cases(program: &Pubkey) -> Vec<(DeepAnaInstruction, &'static str, Value)> {
    let p = program;
    let (a, m, o, d) = (source(), mint(), owner(), destination());
    vec![
        (row_of!(token_2022::initialize_mint(p, &m, &o, Some(&d), 6)), "InitializeMint",
            json!({ "decimals": 6, "mint_authority": o.to_string(), "freeze_authority": d.to_string() })),
        (row_of!(token_2022::initialize_mint2(p, &m, &o, None, 0)), "InitializeMint2",
            json!({ "decimals": 0, "mint_authority": o.to_string(), "freeze_authority": null })),
        (row_of!(token_2022::initialize_account(p, &a, &m, &o)), "InitializeAccount", json!({})),
        (row_of!(token_2022::initialize_account2(p, &a, &m, &o)), "InitializeAccount2",
            json!({ "owner": o.to_string() })),
        (row_of!(token_2022::initialize_account3(p, &a, &m, &o)), "InitializeAccount3",
            json!({ "owner": o.to_string() })),
        (row_of!(token_2022::initialize_multisig(p, &a, &[&key(20), &key(21)], 2)), "InitializeMultisig",
            json!({ "m": 2 })),
        (row_of!(token_2022::initialize_multisig2(p, &a, &[&key(20)], 1)), "InitializeMultisig2",
            json!({ "m": 1 })),
        (row_of!(token_2022::approve(p, &a, &d, &o, &[], 77)), "Approve", json!({ "amount": 77 })),
        (row_of!(token_2022::approve_checked(p, &a, &m, &d, &o, &[], 77, 3)), "ApproveChecked",
            json!({ "amount": 77, "decimals": 3 })),
        (row_of!(token_2022::revoke(p, &a, &o, &[])), "Revoke", json!({})),
        (row_of!(token_2022::set_authority(p, &a, Some(&d), AuthorityType::CloseAccount, &o, &[])),
            "SetAuthority", json!({ "authority_type": "CloseAccount", "new_authority": d.to_string() })),
        (row_of!(token_2022::set_authority(p, &m, None, AuthorityType::MintTokens, &o, &[])),
            "SetAuthority", json!({ "authority_type": "MintTokens", "new_authority": null })),
        (row_of!(token_2022::mint_to(p, &m, &a, &o, &[], 1_000)), "MintTo", json!({ "amount": 1_000 })),
        (row_of!(token_2022::mint_to_checked(p, &m, &a, &o, &[], 1_000, 6)), "MintToChecked",
            json!({ "amount": 1_000, "decimals": 6 })),
        (row_of!(token_2022::burn(p, &a, &m, &o, &[], 9)), "Burn", json!({ "amount": 9 })),
        (row_of!(token_2022::burn_checked(p, &a, &m, &o, &[], 9, 6)), "BurnChecked",
            json!({ "amount": 9, "decimals": 6 })),
        (row_of!(token_2022::close_account(p, &a, &d, &o, &[])), "CloseAccount", json!({})),
        (row_of!(token_2022::freeze_account(p, &a, &m, &o, &[])), "FreezeAccount", json!({})),
        (row_of!(token_2022::thaw_account(p, &a, &m, &o, &[])), "ThawAccount", json!({})),
        (row_of!(token_2022::sync_native(p, &a)), "SyncNative", json!({})),
        (row_of!(token_2022::get_account_data_size(p, &m, &[])), "GetAccountDataSize",
            json!({ "extension_types": [] })),
        (row_of!(token_2022::initialize_immutable_owner(p, &a)), "InitializeImmutableOwner", json!({})),
        (row_of!(token_2022::amount_to_ui_amount(p, &m, 12_345)), "AmountToUiAmount",
            json!({ "amount": 12_345 })),
        (row_of!(token_2022::ui_amount_to_amount(p, &m, "123.45")), "UiAmountToAmount",
            json!({ "ui_amount": "123.45" })),
    ]
}

/// Token-2022 only: extension instructions.
fn token_2022_only_cases() -> Vec<(DeepAnaInstruction, &'static str, Value)> {
    let p = &token_2022_id();
    let (a, m, o, d) = (source(), mint(), owner(), destination());
    vec![
        (row_of!(token_2022::initialize_mint_close_authority(p, &m, Some(&o))),
            "InitializeMintCloseAuthority", json!({ "close_authority": o.to_string() })),
        (row_of!(token_2022::reallocate(p, &a, &o, &o, &[], &[ExtensionType::MemoTransfer])),
            "Reallocate", json!({ "extension_types": ["MemoTransfer"] })),
        (row_of!(token_2022::get_account_data_size(p, &m, &[ExtensionType::ImmutableOwner])),
            "GetAccountDataSize", json!({ "extension_types": ["ImmutableOwner"] })),
        (row_of!(token_2022::create_native_mint(p, &o)), "CreateNativeMint", json!({})),
        (row_of!(token_2022::initialize_non_transferable_mint(p, &m)), "InitializeNonTransferableMint", json!({})),
        (row_of!(token_2022::initialize_permanent_delegate(p, &m, &d)), "InitializePermanentDelegate",
            json!({ "delegate": d.to_string() })),
        (row_of!(token_2022::withdraw_excess_lamports(p, &a, &d, &o, &[])), "WithdrawExcessLamports", json!({})),
        (row_of!(transfer_fee::initialize_transfer_fee_config(p, &m, Some(&o), None, 50, 5_000)),
            "InitializeTransferFeeConfig", json!({
                "transfer_fee_config_authority": o.to_string(),
                "withdraw_withheld_authority": null,
                "transfer_fee_basis_points": 50,
                "maximum_fee": 5_000,
            })),
        (row_of!(transfer_fee::set_transfer_fee(p, &m, &o, &[], 10, 99)), "SetTransferFee",
            json!({ "transfer_fee_basis_points": 10, "maximum_fee": 99 })),
        (row_of!(transfer_fee::harvest_withheld_tokens_to_mint(p, &m, &[&a])), "HarvestWithheldTokensToMint", json!({})),
        (row_of!(transfer_fee::withdraw_withheld_tokens_from_accounts(p, &m, &d, &o, &[], &[&a, &a])),
            "WithdrawWithheldTokensFromAccounts", json!({ "num_token_accounts": 2 })),
        (row_of!(spl_token_2022_interface::extension::memo_transfer::instruction::enable_required_transfer_memos(p, &a, &o, &[])),
            "MemoTransferExtension", json!({ "extension_instruction_tag": 0 })),
    ]
}

#[test]
fn every_spl_token_instruction_decodes_to_its_type_and_fields() {
    for (ix, expected_type, expected_fields) in non_transfer_cases(&spl_id()) {
        let result = spl_decode(&ix).unwrap_or_else(|err| panic!("{expected_type}: {err}"));
        assert_eq!(result.instruction_type, expected_type);
        assert_eq!(result.decoded_data, expected_fields, "{expected_type}");
        assert_eq!(result.token_transfer, None, "{expected_type} is not a transfer");
    }
}

#[test]
fn every_token_2022_instruction_decodes_to_its_type_and_fields() {
    let cases = non_transfer_cases(&token_2022_id()).into_iter().chain(token_2022_only_cases());
    for (ix, expected_type, expected_fields) in cases {
        let result = spl_decode(&ix).unwrap_or_else(|err| panic!("{expected_type}: {err}"));
        assert_eq!(result.instruction_type, expected_type);
        assert_eq!(result.decoded_data, expected_fields, "{expected_type}");
        assert_eq!(result.token_transfer, None, "{expected_type} is not a transfer");
    }
}

#[test]
fn spl_token_declines_token_2022_extensions_and_unknown_tags() {
    let spl = SplTokenDecoder::spl_token();
    let t22 = SplTokenDecoder::token_2022();
    // 25 = InitializeMintCloseAuthority, Token-2022 only.
    assert!(!spl.can_decode(SPL_TOKEN_PROGRAM_ID, &[25, 0]));
    assert!(t22.can_decode(TOKEN_2022_PROGRAM_ID, &[25, 0]));
    // p-token additions are accepted for SPL Token.
    for tag in [38, 45, 255] {
        assert!(spl.can_decode(SPL_TOKEN_PROGRAM_ID, &[tag]), "tag {tag}");
    }
    // Tags no program knows.
    for tag in [47, 100, 254] {
        assert!(!spl.can_decode(SPL_TOKEN_PROGRAM_ID, &[tag]), "tag {tag}");
        assert!(!t22.can_decode(TOKEN_2022_PROGRAM_ID, &[tag]), "tag {tag}");
    }
    assert!(!spl.can_decode(SPL_TOKEN_PROGRAM_ID, &[]));
    // Wrong program.
    assert!(!spl.can_decode(TOKEN_2022_PROGRAM_ID, &[3]));
    assert!(!t22.can_decode(SPL_TOKEN_PROGRAM_ID, &[3]));
    assert!(!spl.can_decode(VAULT_PROGRAM_ID, &[3]));
}

#[test]
fn malformed_token_instructions_fail_with_a_decode_error() {
    let accounts = vec![source().to_string(), destination().to_string(), owner().to_string()];
    for (data, why) in [
        (vec![], "empty"),
        (vec![3], "Transfer without amount"),
        (vec![3, 1, 2, 3], "Transfer with a truncated amount"),
        (vec![12, 1, 0, 0, 0, 0, 0, 0, 0], "TransferChecked without decimals"),
        (vec![0, 6], "InitializeMint without authority"),
        (vec![6, 9, 0], "SetAuthority with an unknown authority type"),
        (vec![200], "unknown tag"),
    ] {
        let ix = row(&spl_id(), accounts.clone(), &data);
        let err = SplTokenDecoder::spl_token().decode(&ix, &accounts).unwrap_err();
        assert!(matches!(err, PrismError::DecodeError { .. }), "{why}: {err:?}");
    }

    let mut ix = row(&spl_id(), accounts.clone(), &[3]);
    ix.data_base58 = "0OIl".to_owned(); // not base58
    let err = SplTokenDecoder::spl_token().decode(&ix, &accounts).unwrap_err();
    assert!(decode_reason(&err).contains("base58"), "{err}");

    let ix = row(&token_2022_id(), accounts.clone(), &[3, 1, 0, 0, 0, 0, 0, 0, 0]);
    let err = SplTokenDecoder::spl_token().decode(&ix, &accounts).unwrap_err();
    assert!(decode_reason(&err).contains("not a"), "wrong program: {err}");
}

#[test]
fn decode_error_names_the_program_and_inner_position() {
    let mut ix = row(&spl_id(), vec![], &[3]);
    ix.instruction_index = 2;
    ix.inner_instruction_index = Some(5);
    match spl_decode(&ix).unwrap_err() {
        PrismError::DecodeError {
            program_id,
            instruction_index,
            reason,
        } => {
            assert_eq!(program_id, SPL_TOKEN_PROGRAM_ID);
            assert_eq!(instruction_index, 2);
            assert!(reason.starts_with("inner instruction 5:"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn token_decoder_never_panics_on_arbitrary_data() {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let accounts: Vec<String> = (0..6).map(|n| key(n).to_string()).collect();
    for decoder in [SplTokenDecoder::spl_token(), SplTokenDecoder::token_2022()] {
        let program = Pubkey::from_str(decoder.program_id()).unwrap();
        for tag in 0..=255u8 {
            for len in [0, 1, 2, 8, 9, 10, 17, 33, 34, 35, 67, 80] {
                let mut data = vec![tag];
                data.extend(rng.bytes(len));
                for n_accounts in [0, 2, 6] {
                    let ix = row(&program, accounts[..n_accounts].to_vec(), &data);
                    // Either outcome is fine; panicking is not.
                    let _ = decoder.decode(&ix, &accounts);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AnchorDecoder
// ---------------------------------------------------------------------------

#[test]
fn idl_discriminants_are_precomputed_at_construction() {
    let decoder = vault();
    assert_eq!(decoder.program_id(), VAULT_PROGRAM_ID);
    assert_eq!(decoder.program_name(), "vault");
    let map = decoder.discriminant_map();
    // Six instructions; `ping` has a 1-byte custom discriminator and is not
    // in the 8-byte map; `withdraw` has none in the IDL and gets the
    // computed one.
    assert_eq!(map.len(), 5);
    for name in ["initialize", "deposit", "configure", "wrap", "withdraw"] {
        let d = Discriminant::for_instruction(name);
        assert_eq!(map.get(&d).map(|ix| ix.name.as_str()), Some(name));
    }
}

#[test]
fn anchor_instruction_with_no_args_decodes_to_an_empty_object() {
    let result = vault().decode(&anchor_row(&INITIALIZE), &[]).unwrap();
    assert_eq!(result.instruction_type, "initialize");
    assert_eq!(result.decoded_data, json!({}));
    assert_eq!(result.token_transfer, None);
}

#[test]
fn anchor_instruction_args_decode_by_name() {
    let decoder = vault();
    let data = anchor_data(&DEPOSIT, &Deposit { amount: 5_000_000, memo: Some("rent".into()) });
    let result = decoder.decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!(result.instruction_type, "deposit");
    assert_eq!(result.decoded_data, json!({ "amount": 5_000_000, "memo": "rent" }));
    assert_eq!(result.decoded_data_json(), r#"{"amount":5000000,"memo":"rent"}"#);

    let data = anchor_data(&DEPOSIT, &Deposit { amount: 0, memo: None });
    let result = decoder.decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!(result.decoded_data, json!({ "amount": 0, "memo": null }));
}

#[test]
fn every_idl_type_decodes_to_its_json_form() {
    let data = anchor_data(&CONFIGURE, &config(Mode::Fixed { rate: 12 }));
    let result = vault().decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!(result.instruction_type, "configure");
    assert_eq!(
        result.decoded_data,
        json!({ "config": {
            "fee_bps": 30,
            "admins": [owner().to_string(), mint().to_string()],
            "mode": { "Fixed": { "rate": 12 } },
            "limits": [1, 2, u64::MAX],
            "ratio": 0.25,
            "small": -8,
            "medium": -70_000,
            "signed": i64::MIN,
            "big": u128::MAX.to_string(),
            "big_negative": i128::MIN.to_string(),
            "flag": true,
            "payload": "3q2+7w==",
            "wide": "680564733841876926926749214863536422911",
            "wide_negative": "-1",
            "pair": [7, 65_535],
        }})
    );
}

#[test]
fn enum_variants_decode_by_shape() {
    let decoder = vault();
    for (mode, expected) in [
        (Mode::Off, json!("Off")),
        (Mode::Fixed { rate: 1 }, json!({ "Fixed": { "rate": 1 } })),
        (Mode::Range(3, 9), json!({ "Range": [3, 9] })),
    ] {
        let data = anchor_data(&CONFIGURE, &config(mode));
        let result = decoder.decode(&anchor_row(&data), &[]).unwrap();
        assert_eq!(result.decoded_data["config"]["mode"], expected);
    }
}

#[test]
fn generic_and_alias_types_are_resolved() {
    let data = anchor_data(&WRAP, &Wrap { items: [10, 20], extra: Some(30), id: [1, 2, 3, 4] });
    let result = vault().decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!(
        result.decoded_data,
        json!({ "wrapped": { "items": [10, 20], "extra": 30 }, "id": [1, 2, 3, 4] })
    );
}

#[test]
fn custom_and_computed_discriminators_route_correctly() {
    let decoder = vault();
    // `withdraw`: empty discriminator in the IDL, standard hash used.
    let data = anchor_data(&sha_prefix("global:withdraw"), &77u64);
    let result = decoder.decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!((result.instruction_type.as_str(), &result.decoded_data), ("withdraw", &json!({ "amount": 77 })));
    // `ping`: 1-byte custom discriminator.
    let result = decoder.decode(&anchor_row(&[7, 200]), &[]).unwrap();
    assert_eq!((result.instruction_type.as_str(), &result.decoded_data), ("ping", &json!({ "n": 200 })));
}

#[test]
fn emit_cpi_events_decode_from_the_self_invocation() {
    let event = DepositEvent { user: owner().to_bytes(), amount: 99 };
    let result = vault().decode(&anchor_row(&event_data(&event)), &[]).unwrap();
    assert_eq!(result.instruction_type, format!("{EVENT_TYPE_PREFIX}DepositEvent"));
    assert_eq!(result.decoded_data, json!({ "user": owner().to_string(), "amount": 99 }));
    assert_eq!(result.token_transfer, None);
}

#[test]
fn anchor_can_decode_checks_program_and_discriminant() {
    let decoder = vault();
    let deposit = anchor_data(&DEPOSIT, &Deposit { amount: 1, memo: None });
    assert!(decoder.can_decode(VAULT_PROGRAM_ID, &deposit));
    assert!(!decoder.can_decode(SPL_TOKEN_PROGRAM_ID, &deposit), "other program");
    assert!(!decoder.can_decode(VAULT_PROGRAM_ID, &sha_prefix("global:unknown")));
    assert!(!decoder.can_decode(VAULT_PROGRAM_ID, &DEPOSIT[..5]), "too short");
    assert!(!decoder.can_decode(VAULT_PROGRAM_ID, &[]));
    // The event tag alone, or with an unknown event, is not claimed.
    assert!(!decoder.can_decode(VAULT_PROGRAM_ID, EVENT_IX_TAG.as_bytes()));
    let mut unknown_event = EVENT_IX_TAG.to_bytes().to_vec();
    unknown_event.extend(sha_prefix("event:Other"));
    assert!(!decoder.can_decode(VAULT_PROGRAM_ID, &unknown_event));
    assert_eq!(decoder.instruction_for(&deposit).map(|ix| ix.name.as_str()), Some("deposit"));
}

#[test]
fn malformed_anchor_instructions_fail_with_a_decode_error() {
    let decoder = vault();
    let mut truncated = anchor_data(&DEPOSIT, &Deposit { amount: 1, memo: Some("x".into()) });
    truncated.pop();
    let mut bad_option = DEPOSIT.to_vec();
    bad_option.extend(1u64.to_le_bytes());
    bad_option.push(2);
    let mut bad_bool = CONFIGURE.to_vec();
    let mut body = borsh::to_vec(&config(Mode::Off)).unwrap();
    // fee_bps(2) + admins(4 + 64) + mode(1) + limits(24) + ratio(8) + small(1)
    // + medium(4) + signed(8) + big(16) + big_negative(16) = flag offset.
    body[2 + 4 + 64 + 1 + 24 + 8 + 1 + 4 + 8 + 16 + 16] = 2;
    bad_bool.extend(body);
    let mut bad_enum = CONFIGURE.to_vec();
    let mut body = borsh::to_vec(&config(Mode::Off)).unwrap();
    body[2 + 4 + 64] = 3; // Mode has 3 variants
    bad_enum.extend(body);
    let mut bad_utf8 = DEPOSIT.to_vec();
    bad_utf8.extend(1u64.to_le_bytes());
    bad_utf8.extend([1, 2, 0, 0, 0, 0xff, 0xfe]);

    for (data, expect) in [
        (DEPOSIT.to_vec(), "field `amount`"),
        (truncated, "field `memo`"),
        (bad_option, "option tag 2"),
        (bad_bool, "field `flag`"),
        (bad_enum, "got tag 3"),
        (bad_utf8, "field `memo`"),
        (sha_prefix("global:unknown").to_vec(), "unknown discriminant"),
        (vec![1, 2, 3], "data too short"),
    ] {
        let err = decoder.decode(&anchor_row(&data), &[]).unwrap_err();
        assert!(decode_reason(&err).contains(expect), "expected {expect:?}: {err}");
    }
}

#[test]
fn trailing_bytes_are_ignored_like_anchor_does() {
    let mut data = anchor_data(&DEPOSIT, &Deposit { amount: 3, memo: None });
    data.extend([9, 9, 9]);
    let result = vault().decode(&anchor_row(&data), &[]).unwrap();
    assert_eq!(result.decoded_data, json!({ "amount": 3, "memo": null }));
}

#[test]
fn anchor_decoder_never_panics_on_arbitrary_bodies() {
    let decoder = vault();
    let mut rng = XorShift(0xDEAD_BEEF_CAFE_F00D);
    let prefixes: Vec<Vec<u8>> = vec![
        INITIALIZE.to_vec(),
        DEPOSIT.to_vec(),
        CONFIGURE.to_vec(),
        WRAP.to_vec(),
        sha_prefix("global:withdraw").to_vec(),
        vec![7],
        [EVENT_IX_TAG.to_bytes(), DEPOSIT_EVENT].concat(),
    ];
    for prefix in &prefixes {
        for len in 0..200 {
            let data = [prefix.clone(), rng.bytes(len)].concat();
            let _ = decoder.decode(&anchor_row(&data), &[]);
        }
    }
}

#[test]
fn bad_idls_are_rejected_at_construction() {
    let idl: Value = serde_json::from_str(VAULT_IDL).unwrap();
    let with = |edit: &dyn Fn(&mut Value)| {
        let mut idl = idl.clone();
        edit(&mut idl);
        AnchorDecoder::from_json(&idl.to_string())
    };

    let err = with(&|idl| idl["address"] = json!("not-a-key")).unwrap_err();
    assert!(matches!(err, PrismError::ConfigError { .. }), "{err:?}");

    let err = with(&|idl| idl["instructions"][1]["discriminator"] = json!(INITIALIZE)).unwrap_err();
    assert!(err.to_string().contains("share the discriminator"), "{err}");

    let err = with(&|idl| {
        idl["instructions"][1]["args"][0]["type"] = json!({ "defined": { "name": "Missing" } })
    })
    .unwrap_err();
    assert!(err.to_string().contains("undefined type `Missing`"), "{err}");

    let err = with(&|idl| idl["types"].as_array_mut().unwrap().retain(|t| t["name"] != "DepositEvent"))
        .unwrap_err();
    assert!(err.to_string().contains("no type definition"), "{err}");

    // A pre-0.30 IDL (no `address`, no discriminators) is not accepted.
    let legacy = r#"{"version":"0.1.0","name":"old","instructions":[{"name":"init","accounts":[],"args":[]}]}"#;
    let err = AnchorDecoder::from_json(legacy).unwrap_err();
    assert!(err.to_string().contains("not an Anchor 0.30+ IDL"), "{err}");
}

#[test]
fn self_referencing_types_are_bounded() {
    let mut idl: Value = serde_json::from_str(VAULT_IDL).unwrap();
    idl["types"].as_array_mut().unwrap().push(json!({
        "name": "Node",
        "type": { "kind": "struct", "fields": [
            { "name": "next", "type": { "option": { "defined": { "name": "Node" } } } }
        ]}
    }));
    idl["instructions"][4]["args"] = json!([{ "name": "list", "type": { "defined": { "name": "Node" } } }]);
    let decoder = AnchorDecoder::from_json(&idl.to_string()).expect("recursive types are valid");

    let chain = |n: usize| [sha_prefix("global:withdraw").to_vec(), vec![1; n], vec![0]].concat();
    let ok = decoder.decode(&anchor_row(&chain(3)), &[]).unwrap();
    assert_eq!(ok.decoded_data, json!({ "list": { "next": { "next": { "next": { "next": null } } } } }));
    let err = decoder.decode(&anchor_row(&chain(10_000)), &[]).unwrap_err();
    assert!(decode_reason(&err).contains("nest deeper"), "{err}");
}

// ---------------------------------------------------------------------------
// DecoderRegistry
// ---------------------------------------------------------------------------

fn full_registry() -> DecoderRegistry {
    let mut registry = DecoderRegistry::with_spl_token();
    registry.register(Box::new(vault()));
    registry
}

fn assert_metrics_add_up(m: &RegistryMetricsSnapshot) {
    assert_eq!(
        m.total_decode_attempts,
        m.successful_decodes + m.no_decoder_found + m.decode_errors,
        "{m:?}"
    );
}

#[test]
fn registry_routes_by_program_id() {
    let registry = full_registry();
    assert_eq!(registry.len(), 3);
    assert!(registry.has_decoder(SPL_TOKEN_PROGRAM_ID));
    assert!(registry.has_decoder(TOKEN_2022_PROGRAM_ID));
    assert!(registry.has_decoder(VAULT_PROGRAM_ID));

    let spl = row_of!(token::transfer(&spl_id(), &source(), &destination(), &owner(), &[], 1));
    let anchor = anchor_row(&INITIALIZE);
    assert_eq!(registry.decode(&spl, &[]).unwrap().unwrap().instruction_type, "Transfer");
    assert_eq!(registry.decode(&anchor, &[]).unwrap().unwrap().instruction_type, "initialize");

    let m = registry.metrics().snapshot();
    assert_eq!((m.total_decode_attempts, m.successful_decodes, m.token_transfers_produced), (2, 2, 1));
    assert_metrics_add_up(&m);
}

#[test]
fn registry_counts_misses_and_errors_separately() {
    let registry = full_registry();
    let system = row(&Pubkey::default(), vec![], &[2, 0, 0, 0]);
    let unknown_tag = row(&spl_id(), vec![], &[200]);
    let unknown_discriminant = anchor_row(&sha_prefix("global:nope"));
    let truncated = row(&spl_id(), vec![], &[3, 1]);

    assert_eq!(registry.decode(&system, &[]).unwrap(), None);
    assert_eq!(registry.decode(&unknown_tag, &[]).unwrap(), None);
    assert_eq!(registry.decode(&unknown_discriminant, &[]).unwrap(), None);
    assert!(registry.decode(&truncated, &[]).is_err());

    let m = registry.metrics().snapshot();
    assert_eq!(m.no_decoder_found, 3);
    assert_eq!(m.decode_errors, 1);
    assert_eq!(m.successful_decodes, 0);
    assert_eq!(m.decode_success_rate(), Some(0.0));
    assert_metrics_add_up(&m);
}

/// A decoder that claims only data starting with `prefix`.
struct Prefixed {
    program_id: String,
    prefix: u8,
    name: &'static str,
}

impl Decoder for Prefixed {
    fn program_id(&self) -> &str {
        &self.program_id
    }
    fn can_decode(&self, program_id: &str, data: &[u8]) -> bool {
        program_id == self.program_id && data.first() == Some(&self.prefix)
    }
    fn decode(&self, _: &DeepAnaInstruction, keys: &[String]) -> Result<DecodeResult, PrismError> {
        Ok(DecodeResult {
            instruction_type: self.name.to_owned(),
            decoded_data: json!({ "keys": keys.len() }),
            token_transfer: None,
        })
    }
}

#[test]
fn several_decoders_can_share_a_program_and_the_first_match_wins() {
    let program = key(50);
    let mut registry = DecoderRegistry::new();
    for (prefix, name) in [(1, "first"), (2, "second"), (1, "shadowed")] {
        registry.register(Box::new(Prefixed { program_id: program.to_string(), prefix, name }));
    }
    assert_eq!(registry.len(), 3);
    let keys = vec!["a".to_owned(), "b".to_owned()];
    let decode = |data: &[u8]| registry.decode(&row(&program, vec![], data), &keys).unwrap();
    assert_eq!(decode(&[1]).unwrap().instruction_type, "first");
    assert_eq!(decode(&[2]).unwrap().instruction_type, "second");
    assert_eq!(decode(&[2]).unwrap().decoded_data, json!({ "keys": 2 }), "account_keys passed through");
    assert_eq!(decode(&[3]), None);
}

#[test]
fn registry_is_shareable_across_threads() {
    let registry = Arc::new(full_registry());
    let ix = row_of!(token::transfer(&spl_id(), &source(), &destination(), &owner(), &[], 1));
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let (registry, ix) = (Arc::clone(&registry), ix.clone());
            scope.spawn(move || {
                for _ in 0..250 {
                    registry.decode(&ix, &[]).unwrap().unwrap();
                }
            });
        }
    });
    let m = registry.metrics().snapshot();
    assert_eq!((m.successful_decodes, m.token_transfers_produced), (2_000, 2_000));
}

/// A block as the normalizer produces it: one transaction whose top-level
/// instruction calls a vault program that CPIs into SPL Token.
fn normalized_block() -> NormalizedBlock {
    let keys = vec![owner(), source(), destination(), mint(), vault_id(), spl_id(), Pubkey::default()];
    let index = |k: Pubkey| keys.iter().position(|x| *x == k).unwrap() as u8;
    let deposit = anchor_data(&DEPOSIT, &Deposit { amount: 500, memo: None });
    let top_level = vec![
        CompiledInstruction::new_from_raw_parts(index(vault_id()), deposit, vec![index(source()), index(owner())]),
        CompiledInstruction::new_from_raw_parts(index(Pubkey::default()), vec![2, 0, 0, 0], vec![0, 1]),
    ];
    let transfer = token::transfer_checked(&spl_id(), &source(), &mint(), &destination(), &owner(), &[], 500, 6).unwrap();
    let inner_json = json!([{ "index": 0, "instructions": [{
        "programIdIndex": index(spl_id()),
        "accounts": [index(source()), index(mint()), index(destination()), index(owner())],
        "data": bs58::encode(&transfer.data).into_string(),
        "stackHeight": 2,
    }]}]);
    let inner: Vec<solana_transaction_status::UiInnerInstructions> = serde_json::from_value(inner_json).unwrap();
    let instructions = InstructionNormalizer::new()
        .normalize(SIG, SLOT, &keys, &top_level, Some(&inner))
        .unwrap();
    NormalizedBlock {
        slot: DeepAnaSlot {
            slot: SLOT,
            blockhash: "hash".into(),
            parent_slot: SLOT - 1,
            block_time: Some(BLOCK_TIME),
            block_height: Some(1),
            transaction_count: 1,
            leader: None,
        },
        transactions: vec![DeepAnaTransaction {
            slot: SLOT,
            tx_signature: SIG.into(),
            accounts: keys.iter().map(ToString::to_string).collect(),
            status: TransactionStatus::Success,
            ..Default::default()
        }],
        instructions,
    }
}

#[test]
fn decode_block_fills_rows_and_returns_transfers_with_block_time() {
    let registry = full_registry();
    let mut block = normalized_block();
    let transfers = registry.decode_block(&mut block);

    let [deposit, system, transfer_ix] = block.instructions.as_slice() else {
        panic!("{:#?}", block.instructions)
    };
    assert_eq!(deposit.instruction_type.as_deref(), Some("deposit"));
    assert_eq!(deposit.decoded_data, Some(json!({ "amount": 500, "memo": null })));
    assert!(!system.is_decoded(), "no decoder for the system program");
    assert_eq!(transfer_ix.inner_instruction_index, Some(0));
    assert_eq!(transfer_ix.instruction_type.as_deref(), Some("TransferChecked"));

    assert_eq!(
        transfers,
        vec![DeepAnaTokenTransfer {
            slot: SLOT,
            tx_signature: SIG.into(),
            instruction_index: 0,
            inner_instruction_index: Some(0),
            spl_token_program_id: SPL_TOKEN_PROGRAM_ID.into(),
            source_token_account: source().to_string(),
            destination_token_account: destination().to_string(),
            source_owner: None,
            destination_owner: None,
            mint_token_address: Some(mint().to_string()),
            amount_raw: 500,
            decimals: Some(6),
            block_time: Some(BLOCK_TIME),
        }]
    );
    let m = registry.metrics().snapshot();
    assert_eq!((m.successful_decodes, m.no_decoder_found, m.token_transfers_produced), (2, 1, 1));
}

#[test]
fn decode_block_leaves_failing_instructions_undecoded() {
    let registry = full_registry();
    let mut block = normalized_block();
    block.instructions[2].data_base58 = bs58::encode([12u8, 1]).into_string(); // truncated
    let transfers = registry.decode_block(&mut block);
    assert!(transfers.is_empty());
    assert!(!block.instructions[2].is_decoded());
    assert!(block.instructions[0].is_decoded(), "the rest of the block is still decoded");
    assert_eq!(registry.metrics().snapshot().decode_errors, 1);
}

// ---------------------------------------------------------------------------
// Decoding rate: report
// ---------------------------------------------------------------------------

/// Per-program decoding tally.
#[derive(Debug, Default, Clone)]
struct Tally {
    seen: u64,
    decoded: u64,
    /// Claimed by a decoder that then failed.
    errors: u64,
    /// No decoder recognised the instruction.
    unclaimed: u64,
    transfers: u64,
    /// Up to a few `(signature, reason)` failures to print.
    samples: Vec<String>,
}

impl Tally {
    fn record(&mut self, outcome: &Result<Option<DecodeResult>, PrismError>, ix: &DeepAnaInstruction) {
        self.seen += 1;
        match outcome {
            Ok(Some(result)) => {
                self.decoded += 1;
                self.transfers += u64::from(result.token_transfer.is_some());
            }
            Ok(None) => {
                self.unclaimed += 1;
                self.sample(ix, "no decoder recognised it".into());
            }
            Err(err) => {
                self.errors += 1;
                self.sample(ix, err.to_string());
            }
        }
    }

    fn sample(&mut self, ix: &DeepAnaInstruction, why: String) {
        if self.samples.len() < 5 {
            let at = match ix.inner_instruction_index {
                None => ix.instruction_index.to_string(),
                Some(inner) => format!("{}.{inner}", ix.instruction_index),
            };
            self.samples.push(format!("{} ix {at} data {}: {why}", ix.tx_signature, ix.data_base58));
        }
    }

    /// `decoded / seen`, in percent.
    fn rate(&self) -> f64 {
        if self.seen == 0 { 100.0 } else { 100.0 * self.decoded as f64 / self.seen as f64 }
    }
}

fn print_rate_table(title: &str, rows: &BTreeMap<String, Tally>) {
    println!("\n  {title}");
    println!(
        "  {:<60} {:>8} {:>8} {:>7} {:>9} {:>9} {:>8}",
        "program", "seen", "decoded", "errors", "unclaimed", "transfers", "rate"
    );
    let mut total = Tally::default();
    for (program, t) in rows {
        println!(
            "  {program:<60} {:>8} {:>8} {:>7} {:>9} {:>9} {:>7.2}%",
            t.seen, t.decoded, t.errors, t.unclaimed, t.transfers, t.rate()
        );
        total.seen += t.seen;
        total.decoded += t.decoded;
        total.errors += t.errors;
        total.unclaimed += t.unclaimed;
        total.transfers += t.transfers;
    }
    println!(
        "  {:<60} {:>8} {:>8} {:>7} {:>9} {:>9} {:>7.2}%",
        "total", total.seen, total.decoded, total.errors, total.unclaimed, total.transfers, total.rate()
    );
    for (program, t) in rows {
        for sample in &t.samples {
            println!("    {program}: {sample}");
        }
    }
}

// ---------------------------------------------------------------------------
// Decoding rate: offline corpus
// ---------------------------------------------------------------------------

#[test]
fn spl_token_decoding_rate_on_builder_corpus() {
    let registry = DecoderRegistry::with_spl_token();
    let mut corpus: Vec<DeepAnaInstruction> = Vec::new();
    let mut expected_transfers = 0;
    for program in [spl_id(), token_2022_id()] {
        corpus.extend(non_transfer_cases(&program).into_iter().map(|(ix, _, _)| ix));
        for amount in [0, 1, 999, u64::MAX] {
            corpus.push(row_of!(token_2022::transfer(&program, &source(), &destination(), &owner(), &[], amount)));
            corpus.push(row_of!(token_2022::transfer_checked(
                &program, &source(), &mint(), &destination(), &owner(), &[], amount, 9
            )));
            expected_transfers += 2;
        }
    }
    corpus.extend(token_2022_only_cases().into_iter().map(|(ix, _, _)| ix));
    corpus.push(row_of!(transfer_fee::transfer_checked_with_fee(
        &token_2022_id(), &source(), &mint(), &destination(), &owner(), &[], 5, 0, 0
    )));
    expected_transfers += 1;

    let mut rows: BTreeMap<String, Tally> = BTreeMap::new();
    for ix in &corpus {
        let outcome = registry.decode(ix, &[]);
        rows.entry(ix.program_id.clone()).or_default().record(&outcome, ix);
    }
    print_rate_table("SPL Token decoding rate (builder corpus)", &rows);

    let m = registry.metrics().snapshot();
    assert_metrics_add_up(&m);
    assert_eq!(m.successful_decodes, corpus.len() as u64, "every instruction decodes: {rows:#?}");
    assert_eq!(m.token_transfers_produced, expected_transfers);
    for (program, t) in &rows {
        assert_eq!(t.rate(), 100.0, "{program}");
    }
}

#[test]
fn anchor_decoding_rate_on_idl_corpus() {
    let registry = full_registry();
    let mut rng = XorShift(42);
    let mut corpus = Vec::new();
    for i in 0..500u64 {
        let amount = rng.next();
        let data = match i % 7 {
            0 => INITIALIZE.to_vec(),
            1 => anchor_data(&DEPOSIT, &Deposit {
                amount,
                memo: amount.is_multiple_of(2).then(|| format!("memo #{i}")),
            }),
            2 => anchor_data(&CONFIGURE, &config(match amount % 3 {
                0 => Mode::Off,
                1 => Mode::Fixed { rate: amount },
                _ => Mode::Range(i as u32, amount as u32),
            })),
            3 => anchor_data(&WRAP, &Wrap { items: [i as u32, 0], extra: None, id: [0; 4] }),
            4 => anchor_data(&sha_prefix("global:withdraw"), &amount),
            5 => vec![7, i as u8],
            _ => event_data(&DepositEvent { user: key(i as u8).to_bytes(), amount }),
        };
        corpus.push(anchor_row(&data));
    }
    // Instructions outside the IDL: counted as unclaimed, not as failures.
    let foreign: Vec<DeepAnaInstruction> = (0..50)
        .map(|i| anchor_row(&sha_prefix(&format!("global:other_{i}"))))
        .collect();

    let mut idl_rows: BTreeMap<String, Tally> = BTreeMap::new();
    for ix in &corpus {
        let outcome = registry.decode(ix, &[]);
        idl_rows.entry(format!("{} (in IDL)", ix.program_id)).or_default().record(&outcome, ix);
    }
    let mut all_rows = idl_rows.clone();
    for ix in &foreign {
        let outcome = registry.decode(ix, &[]);
        all_rows.entry(format!("{} (not in IDL)", ix.program_id)).or_default().record(&outcome, ix);
    }
    print_rate_table("Anchor decoding rate (IDL corpus)", &all_rows);

    let m = registry.metrics().snapshot();
    assert_metrics_add_up(&m);
    assert_eq!(m.decode_errors, 0);
    assert_eq!(m.successful_decodes, corpus.len() as u64);
    assert_eq!(m.no_decoder_found, foreign.len() as u64);
    assert_eq!(m.decode_success_rate(), Some(1.0));
    assert_eq!(m.token_transfers_produced, 0, "Anchor never produces transfers");
    for t in idl_rows.values() {
        assert_eq!(t.rate(), 100.0);
    }
}

// ---------------------------------------------------------------------------
// Decoding rate: live devnet
// ---------------------------------------------------------------------------

/// Slots below the confirmed tip to start from, so every block exists.
const TIP_MARGIN: u64 = 30;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

async fn live_adapter() -> Option<SolanaAdapter> {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return None;
    };
    let adapter = helius_solana_adapter(&key, Cluster::Devnet).await.expect("helius adapter");
    if let Err(err) = adapter.get_tip(CommitmentLevel::Confirmed).await {
        panic!("cannot reach devnet: {}", err.to_string().replace(&key, "<HELIUS_API_KEY>"));
    }
    Some(adapter)
}

/// Up to `limit` recent devnet blocks that have transactions, normalized.
async fn live_blocks(adapter: &SolanaAdapter, limit: usize) -> Vec<NormalizedBlock> {
    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.expect("devnet tip");
    let top = tip.slot_or_height - TIP_MARGIN;
    let normalizer = Normalizer::new();
    let mut found = Vec::new();
    for slot in (top.saturating_sub(limit as u64 * 4)..=top).rev() {
        let block: ChainBlock = match adapter.get_block(slot).await {
            Ok(block) => block,
            Err(PrismError::SlotSkipped { .. }) => continue,
            Err(err) if err.is_retryable() => {
                eprintln!("slot {slot}: {err}");
                continue;
            }
            Err(err) => panic!("slot {slot}: {err}"),
        };
        let normalized = normalizer.normalize(&block).expect("the devnet block normalizes");
        if !normalized.transactions.is_empty() {
            found.push(normalized);
            if found.len() == limit {
                break;
            }
        }
    }
    found
}

fn successful_signatures(blocks: &[NormalizedBlock]) -> std::collections::HashSet<String> {
    blocks
        .iter()
        .flat_map(|b| &b.transactions)
        .filter(|tx| tx.is_success())
        .map(|tx| tx.tx_signature.clone())
        .collect()
}

/// Decodes every instruction of `blocks` whose program `include` accepts,
/// tallied per program, split by whether the transaction succeeded.
fn tally_blocks(
    registry: &DecoderRegistry,
    blocks: &[NormalizedBlock],
    include: impl Fn(&str) -> bool,
    label: impl Fn(&str) -> String,
) -> (BTreeMap<String, Tally>, BTreeMap<String, Tally>) {
    let succeeded = successful_signatures(blocks);
    let (mut ok_rows, mut failed_rows) = (BTreeMap::new(), BTreeMap::new());
    for block in blocks {
        let accounts: HashMap<&str, &[String]> = block
            .transactions
            .iter()
            .map(|tx| (tx.tx_signature.as_str(), tx.accounts.as_slice()))
            .collect();
        for ix in block.instructions.iter().filter(|ix| include(&ix.program_id)) {
            let keys = accounts.get(ix.tx_signature.as_str()).copied().unwrap_or_default();
            let outcome = registry.decode(ix, keys);
            let rows = if succeeded.contains(&ix.tx_signature) { &mut ok_rows } else { &mut failed_rows };
            rows.entry(label(&ix.program_id)).or_insert_with(Tally::default).record(&outcome, ix);
        }
    }
    (ok_rows, failed_rows)
}

/// The SPL Token decoding rate on recent devnet blocks.
///
/// Every token instruction in a *successful* transaction was executed by
/// the token program, so it is well-formed by definition: the decoder must
/// decode all of them. Failed transactions are reported separately, since
/// their instructions may be malformed on purpose.
#[tokio::test]
async fn live_spl_token_decoding_rate() {
    let Some(adapter) = live_adapter().await else { return };
    let limit = env_usize("PRISM_DECODE_RATE_BLOCKS", 20);
    let blocks = live_blocks(&adapter, limit).await;
    assert!(!blocks.is_empty(), "no devnet blocks with transactions found");

    let registry = DecoderRegistry::with_spl_token();
    let label = |p: &str| match p {
        SPL_TOKEN_PROGRAM_ID => format!("SPL Token ({p})"),
        _ => format!("Token-2022 ({p})"),
    };
    let is_token = |p: &str| p == SPL_TOKEN_PROGRAM_ID || p == TOKEN_2022_PROGRAM_ID;
    let (ok_rows, failed_rows) = tally_blocks(&registry, &blocks, is_token, label);

    let txs: usize = blocks.iter().map(|b| b.transactions.len()).sum();
    let ixs: usize = blocks.iter().map(|b| b.instructions.len()).sum();
    println!(
        "\n  scanned {} devnet blocks (slots {}..={}): {txs} transactions, {ixs} instructions",
        blocks.len(),
        blocks.last().unwrap().slot.slot,
        blocks[0].slot.slot
    );
    print_rate_table("SPL Token decoding rate: successful transactions", &ok_rows);
    print_rate_table("SPL Token decoding rate: failed transactions (informational)", &failed_rows);

    // Spot-check the transfer rows against the instructions they came from.
    let mut checked = 0;
    for block in &blocks {
        for ix in block.instructions.iter().filter(|ix| is_token(&ix.program_id)) {
            if let Ok(Some(DecodeResult { token_transfer: Some(t), .. })) = registry.decode(ix, &[]) {
                assert_eq!(t.source_token_account, ix.accounts[0]);
                assert!(ix.accounts.contains(&t.destination_token_account));
                assert_eq!(t.spl_token_program_id, ix.program_id);
                checked += 1;
            }
        }
    }
    println!("\n  transfer rows cross-checked against their instructions: {checked}");

    let m = registry.metrics().snapshot();
    assert_metrics_add_up(&m);
    let seen: u64 = ok_rows.values().map(|t| t.seen).sum();
    if seen == 0 {
        eprintln!("note: no token instructions in the scanned blocks; raise PRISM_DECODE_RATE_BLOCKS");
    }
    for (program, t) in &ok_rows {
        assert_eq!(
            t.decoded, t.seen,
            "{program}: every token instruction of a successful transaction must decode: {:#?}",
            t.samples
        );
    }
}

/// Anchor IDLs to decode with: from `PRISM_ANCHOR_IDLS` (asserted), and
/// from on-chain IDL accounts of programs seen in `blocks` (reported).
async fn live_anchor_decoders(
    adapter: &SolanaAdapter,
    blocks: &[NormalizedBlock],
) -> (Vec<AnchorDecoder>, Vec<AnchorDecoder>) {
    let from_files: Vec<AnchorDecoder> = std::env::var("PRISM_ANCHOR_IDLS")
        .unwrap_or_default()
        .split(',')
        .filter(|path| !path.trim().is_empty())
        .map(|path| {
            let json = std::fs::read_to_string(path.trim()).unwrap_or_else(|e| panic!("{path}: {e}"));
            AnchorDecoder::from_json(&json).unwrap_or_else(|e| panic!("{path}: {e}"))
        })
        .collect();

    let mut programs: Vec<String> = blocks
        .iter()
        .flat_map(|b| &b.instructions)
        .map(|ix| ix.program_id.clone())
        .filter(|p| !from_files.iter().any(|d| d.program_id() == p))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    programs.truncate(env_usize("PRISM_ANCHOR_MAX_PROGRAMS", 300));

    let client = solana_client::nonblocking::rpc_client::RpcClient::new(adapter.config().rpc_url.clone());
    let mut discovered = Vec::new();
    let (mut no_idl, mut legacy_idl) = (0, 0);
    for chunk in programs.chunks(100) {
        let ids: Vec<Pubkey> = chunk.iter().filter_map(|p| Pubkey::from_str(p).ok()).collect();
        let idl_addresses: Vec<Pubkey> = ids
            .iter()
            .map(|id| {
                let signer = Pubkey::find_program_address(&[], id).0;
                Pubkey::create_with_seed(&signer, "anchor:idl", id).unwrap()
            })
            .collect();
        let accounts = match client.get_multiple_accounts(&idl_addresses).await {
            Ok(accounts) => accounts,
            Err(err) => {
                eprintln!("IDL lookup failed: {err}");
                continue;
            }
        };
        for account in accounts.into_iter() {
            let Some(account) = account else {
                no_idl += 1;
                continue;
            };
            // 8 discriminator ‖ 32 authority ‖ u32 length ‖ zlib(JSON).
            let Some(len) = account.data.get(40..44).map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
            else {
                continue;
            };
            let Some(compressed) = account.data.get(44..44 + len) else { continue };
            let mut json = String::new();
            use std::io::Read as _;
            if flate2::read::ZlibDecoder::new(compressed).read_to_string(&mut json).is_err() {
                continue;
            }
            match AnchorDecoder::from_json(&json) {
                Ok(decoder) => discovered.push(decoder),
                Err(_) => legacy_idl += 1,
            }
        }
    }
    println!(
        "\n  on-chain IDLs: {} programs checked, {} usable (Anchor 0.30+), {legacy_idl} pre-0.30 \
         or invalid, {no_idl} without an IDL account",
        programs.len(),
        discovered.len()
    );
    (from_files, discovered)
}

/// The Anchor decoding rate on recent devnet blocks, for every program an
/// IDL is available for. See the module docs for where IDLs come from.
#[tokio::test]
async fn live_anchor_decoding_rate() {
    let Some(adapter) = live_adapter().await else { return };
    let limit = env_usize("PRISM_DECODE_RATE_BLOCKS", 20);
    let blocks = live_blocks(&adapter, limit).await;
    assert!(!blocks.is_empty(), "no devnet blocks with transactions found");
    let (from_files, discovered) = live_anchor_decoders(&adapter, &blocks).await;
    if from_files.is_empty() && discovered.is_empty() {
        eprintln!(
            "note: no Anchor IDL available for the programs in the scanned blocks; set \
             PRISM_ANCHOR_IDLS=path/to/idl.json (for a program active on devnet) or raise \
             PRISM_DECODE_RATE_BLOCKS"
        );
        return;
    }

    let mut names: HashMap<String, String> = HashMap::new();
    let mut asserted: Vec<String> = Vec::new();
    let mut registry = DecoderRegistry::new();
    for decoder in from_files {
        asserted.push(decoder.program_id().to_owned());
        names.insert(decoder.program_id().to_owned(), format!("{} [file]", decoder.program_name()));
        registry.register(Box::new(decoder));
    }
    for decoder in discovered {
        names.insert(decoder.program_id().to_owned(), format!("{} [on-chain]", decoder.program_name()));
        registry.register(Box::new(decoder));
    }
    let label = |p: &str| format!("{} {}", names[p], &p[..8]);
    let (ok_rows, failed_rows) = tally_blocks(&registry, &blocks, |p| names.contains_key(p), label);

    print_rate_table("Anchor decoding rate: successful transactions", &ok_rows);
    print_rate_table("Anchor decoding rate: failed transactions (informational)", &failed_rows);
    let m = registry.metrics().snapshot();
    assert_metrics_add_up(&m);
    if let Some(rate) = m.decode_success_rate() {
        println!("\n  of the instructions an IDL recognised, decoded: {:.2}%", 100.0 * rate);
    }
    assert_eq!(m.token_transfers_produced, 0, "Anchor never produces transfers");

    for program in &asserted {
        if let Some(t) = ok_rows.get(&label(program)) {
            assert_eq!(
                t.decoded, t.seen,
                "{program}: instructions of successful transactions must decode with the given IDL: {:#?}",
                t.samples
            );
        } else {
            eprintln!("note: {program} has no instructions in the scanned blocks");
        }
    }
}
