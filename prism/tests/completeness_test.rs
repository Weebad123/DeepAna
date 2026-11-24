//! Completeness and exactness of transaction and token data.
//!
//! The main test runs one block through the real [`Ingestor`] (normalizer,
//! decoder, account resolver, Parquet writer) and reads the Parquet files
//! back. The block mixes what real blocks hold: legacy and v0 transactions,
//! `json` and `base64` encodings, token accounts and the token program
//! reachable only through an address lookup table, transfers made by CPIs,
//! a plain `Transfer` that names no mint, Token-2022 with a transfer fee, an
//! account closed and one created inside a transaction, a failed transaction
//! and a vote.
//!
//! What is asserted:
//!
//! * **every transaction** of the block is stored exactly once, with every
//!   column equal to what the block says (full key list included);
//! * **every token transfer** of every successful transaction is stored
//!   exactly once, with every column equal to an expectation written out
//!   independently of the code (mint, decimals and both owners included,
//!   without a single RPC request);
//! * **on-chain verification**: for each successful transaction, the token
//!   movements the stored rows describe add up, account by account, to the
//!   balance changes the chain reported in `preTokenBalances` /
//!   `postTokenBalances`. A transfer missing, doubled or with a wrong amount
//!   or account would break the equation;
//! * a failed transaction is stored, but contributes no transfer: it moved
//!   nothing.
//!
//! Further tests cover the normalizer's reading of the token balances and
//! the resolver's rules for using them, branch by branch.

#![allow(deprecated)] // `Transfer` is deprecated in favour of `TransferChecked`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{TimestampSecondType, UInt8Type, UInt32Type, UInt64Type};
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prism::account_resolver::{AccountResolver, TokenAccountIndex, TokenAccountMeta};
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::commitment::{CommitmentTracker, FinalityWatermark};
use prism::decoder::spl_token::{SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
use prism::ingestor::{Ingestor, IngestorConfig, executed_transfers};
use prism::normalizer::{Normalizer, TokenBalanceChange, TokenBalanceSide, token_balance_changes};
use prism::slot_processor::SlotFetcherConfig;
use prism::storage::{PartitionKey, TableKind};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::ChangeEvent;
use prism::types::udm::{DeepAnaTokenTransfer, DeepAnaTransaction, TransactionStatus};
use serde_json::{Value, json};
use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::message::{Hash, Message, MessageHeader, VersionedMessage, v0};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status::{
    Encodable, EncodableWithMeta, EncodedConfirmedBlock, UiTransactionEncoding, UiTransactionStatusMeta,
};
use spl_token_2022_interface::extension::transfer_fee::instruction::transfer_checked_with_fee;
use spl_token_2022_interface::instruction as token;

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

const SLOT: u64 = 300_000_000;
const BLOCK_TIME: i64 = 1_750_000_000;
const VOTE_PROGRAM_ID: &str = "Vote111111111111111111111111111111111111111";
const ATA_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

fn key(n: u8) -> Pubkey {
    Pubkey::new_from_array([n; 32])
}

fn program(id: &str) -> Pubkey {
    Pubkey::from_str(id).unwrap()
}

fn spl() -> Pubkey {
    program(SPL_TOKEN_PROGRAM_ID)
}

fn t22() -> Pubkey {
    program(TOKEN_2022_PROGRAM_ID)
}

// Wallets.
fn alice() -> Pubkey {
    key(1)
}
fn bob() -> Pubkey {
    key(2)
}
fn carol() -> Pubkey {
    key(3)
}
fn dave() -> Pubkey {
    key(4)
}
fn validator() -> Pubkey {
    key(5)
}

// Mints: an SPL Token mint with 6 decimals, a Token-2022 mint with 9 and a
// transfer fee.
fn usdc() -> Pubkey {
    key(10)
}
fn feecoin() -> Pubkey {
    key(11)
}

/// A token account in the fixture: who owns it, for which mint, under which
/// program.
#[derive(Debug, Clone, Copy)]
struct TokenAccount {
    address: Pubkey,
    owner: Pubkey,
    mint: Pubkey,
    program: fn() -> Pubkey,
    decimals: u8,
}

fn usdc_of(address: u8, owner: Pubkey) -> TokenAccount {
    TokenAccount { address: key(address), owner, mint: usdc(), program: spl, decimals: 6 }
}

fn feecoin_of(address: u8, owner: Pubkey) -> TokenAccount {
    TokenAccount { address: key(address), owner, mint: feecoin(), program: t22, decimals: 9 }
}

fn a1() -> TokenAccount {
    usdc_of(20, alice())
}
fn b1() -> TokenAccount {
    usdc_of(21, bob())
}
fn c1() -> TokenAccount {
    usdc_of(22, carol())
}
fn d1() -> TokenAccount {
    usdc_of(23, dave())
}
/// Alice's second USDC account, emptied into `a1` and closed in tx 5.
fn e1() -> TokenAccount {
    usdc_of(24, alice())
}
fn a2() -> TokenAccount {
    feecoin_of(30, alice())
}
fn b2() -> TokenAccount {
    feecoin_of(31, bob())
}

fn lookup_table() -> Pubkey {
    key(40)
}
fn dex() -> Pubkey {
    key(41)
}

fn s(key: Pubkey) -> String {
    key.to_string()
}

fn sig(seed: u8) -> Signature {
    Signature::from([seed; 64])
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn header(signers: u8) -> MessageHeader {
    MessageHeader { num_required_signatures: signers, num_readonly_signed_accounts: 0, num_readonly_unsigned_accounts: 0 }
}

fn compiled(keys: &[Pubkey], ix: &solana_sdk::instruction::Instruction) -> CompiledInstruction {
    let index = |k: &Pubkey| keys.iter().position(|x| x == k).unwrap_or_else(|| panic!("{k} not in keys")) as u8;
    CompiledInstruction {
        program_id_index: index(&ix.program_id),
        accounts: ix.accounts.iter().map(|meta| index(&meta.pubkey)).collect(),
        data: ix.data.clone(),
    }
}

fn legacy(seed: u8, keys: Vec<Pubkey>, instructions: Vec<CompiledInstruction>) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![sig(seed)],
        message: VersionedMessage::Legacy(Message {
            header: header(1),
            account_keys: keys,
            recent_blockhash: Hash::new_from_array([7; 32]),
            instructions,
        }),
    }
}

/// A v0 transaction whose `writable` and `readonly` keys come from
/// [`lookup_table`] (indexes 0.. and 100.. in the table).
fn versioned(
    seed: u8,
    static_keys: Vec<Pubkey>,
    writable: usize,
    readonly: usize,
    instructions: Vec<CompiledInstruction>,
) -> VersionedTransaction {
    VersionedTransaction {
        signatures: vec![sig(seed)],
        message: VersionedMessage::V0(v0::Message {
            header: header(1),
            account_keys: static_keys,
            recent_blockhash: Hash::new_from_array([7; 32]),
            instructions,
            address_table_lookups: vec![MessageAddressTableLookup {
                account_key: lookup_table(),
                writable_indexes: (0..writable as u8).collect(),
                readonly_indexes: (100..100 + readonly as u8).collect(),
            }],
        }),
    }
}

/// One `preTokenBalances` / `postTokenBalances` entry.
fn balance(full_keys: &[Pubkey], account: TokenAccount, amount: u64) -> Value {
    let index = full_keys.iter().position(|k| *k == account.address).expect("token account in keys");
    let ui = amount as f64 / 10f64.powi(i32::from(account.decimals));
    json!({
        "accountIndex": index,
        "mint": s(account.mint),
        "owner": s(account.owner),
        "programId": s((account.program)()),
        "uiTokenAmount": {
            "amount": amount.to_string(),
            "decimals": account.decimals,
            "uiAmount": ui,
            "uiAmountString": ui.to_string(),
        },
    })
}

/// The token balances of `changes`: (account, amount before, amount after),
/// `None` for an account that did not exist before / no longer exists after.
fn balances(full_keys: &[Pubkey], changes: &[(TokenAccount, Option<u64>, Option<u64>)]) -> (Value, Value) {
    let pre = changes.iter().filter_map(|(a, pre, _)| pre.map(|amount| balance(full_keys, *a, amount))).collect();
    let post = changes.iter().filter_map(|(a, _, post)| post.map(|amount| balance(full_keys, *a, amount))).collect();
    (Value::Array(pre), Value::Array(post))
}

struct Meta {
    fee: u64,
    err: Option<Value>,
    loaded: Option<(Vec<Pubkey>, Vec<Pubkey>)>,
    inner: Value,
    token_balances: (Value, Value),
    logs: Vec<String>,
    compute_units: u64,
}

impl Meta {
    fn ok(token_balances: (Value, Value)) -> Self {
        Self {
            fee: 5_000,
            err: None,
            loaded: None,
            inner: json!([]),
            token_balances,
            logs: vec!["Program log: ok".to_owned()],
            compute_units: 4_000,
        }
    }

    fn json(&self) -> Value {
        let (writable, readonly) = self.loaded.clone().unwrap_or_default();
        json!({
            "err": self.err,
            "status": match &self.err { Some(err) => json!({"Err": err}), None => json!({"Ok": null}) },
            "fee": self.fee,
            "preBalances": [], "postBalances": [],
            "innerInstructions": self.inner,
            "logMessages": self.logs,
            "preTokenBalances": self.token_balances.0,
            "postTokenBalances": self.token_balances.1,
            "loadedAddresses": {
                "writable": writable.iter().map(|k| s(*k)).collect::<Vec<_>>(),
                "readonly": readonly.iter().map(|k| s(*k)).collect::<Vec<_>>(),
            },
            "computeUnitsConsumed": self.compute_units,
        })
    }
}

/// One transaction of the fixture with what its rows must look like.
struct Fixture {
    tx: VersionedTransaction,
    encoding: UiTransactionEncoding,
    meta: Meta,
    /// Static keys then loaded writable then loaded readonly.
    full_keys: Vec<Pubkey>,
}

impl Fixture {
    fn json(&self) -> Value {
        let transaction = match self.encoding {
            UiTransactionEncoding::Json => self.tx.json_encode(),
            encoding => self.tx.encode(encoding),
        };
        let version = match self.tx.message {
            VersionedMessage::Legacy(_) => json!("legacy"),
            _ => json!(0),
        };
        json!({"transaction": transaction, "meta": self.meta.json(), "version": version})
    }

    fn signature(&self) -> String {
        self.tx.signatures[0].to_string()
    }
}

fn inner_ix(keys: &[Pubkey], ix: &solana_sdk::instruction::Instruction, stack_height: u32) -> Value {
    let c = compiled(keys, ix);
    json!({
        "programIdIndex": c.program_id_index,
        "accounts": c.accounts,
        "data": bs58::encode(&c.data).into_string(),
        "stackHeight": stack_height,
    })
}

/// The block: eight transactions, see each for what it exercises.
fn fixtures() -> Vec<Fixture> {
    let mut out = Vec::new();

    // 0. Legacy, json: TransferChecked a1 -> b1, 100 USDC base units.
    {
        let keys = vec![alice(), a1().address, usdc(), b1().address, spl()];
        let ix = token::transfer_checked(&spl(), &a1().address, &usdc(), &b1().address, &alice(), &[], 100, 6).unwrap();
        out.push(Fixture {
            tx: legacy(0x10, keys.clone(), vec![compiled(&keys, &ix)]),
            encoding: UiTransactionEncoding::Json,
            meta: Meta::ok(balances(&keys, &[(a1(), Some(1_000), Some(900)), (b1(), Some(50), Some(150))])),
            full_keys: keys,
        });
    }

    // 1. v0, base64: a plain Transfer b1 -> c1 of 30 (no mint in the
    //    instruction). Both token accounts are reachable only through the
    //    lookup table (a program a transaction invokes itself must be a
    //    static key, so the token program is one); c1 is created first by
    //    the ATA program (undecoded), so it has no pre balance.
    {
        let static_keys = vec![bob(), program(ATA_PROGRAM_ID), spl()];
        let writable = vec![b1().address, c1().address];
        let readonly = vec![];
        let full: Vec<Pubkey> = static_keys.iter().chain(&writable).chain(&readonly).copied().collect();
        let create = CompiledInstruction { program_id_index: 1, accounts: vec![0, 4], data: vec![1] };
        let ix = token::transfer(&spl(), &b1().address, &c1().address, &bob(), &[], 30).unwrap();
        let mut meta = Meta::ok(balances(&full, &[(b1(), Some(150), Some(120)), (c1(), None, Some(30))]));
        meta.loaded = Some((writable.clone(), readonly.clone()));
        meta.fee = 7_500;
        meta.compute_units = 21_000;
        out.push(Fixture {
            tx: versioned(0x11, static_keys, writable.len(), readonly.len(), vec![create, compiled(&full, &ix)]),
            encoding: UiTransactionEncoding::Base64,
            meta,
            full_keys: full,
        });
    }

    // 2. v0, base64: a swap on `dex` whose two CPIs move tokens: a plain
    //    Transfer c1 -> d1 of 10 USDC and a Token-2022 TransferChecked
    //    a2 -> b2 of 500 FEECOIN. Every token account, the mint and both
    //    token programs (invoked by CPI only) come from the table.
    {
        let static_keys = vec![carol(), dex()];
        let writable = vec![c1().address, d1().address, a2().address, b2().address];
        let readonly = vec![feecoin(), spl(), t22()];
        let full: Vec<Pubkey> = static_keys.iter().chain(&writable).chain(&readonly).copied().collect();
        let swap = CompiledInstruction { program_id_index: 1, accounts: vec![2, 3, 4, 5], data: vec![9, 9] };
        let usdc_leg = token::transfer(&spl(), &c1().address, &d1().address, &carol(), &[], 10).unwrap();
        let fee_leg = token::transfer_checked(&t22(), &a2().address, &feecoin(), &b2().address, &carol(), &[], 500, 9).unwrap();
        let mut meta = Meta::ok(balances(
            &full,
            &[
                (c1(), Some(30), Some(20)),
                (d1(), Some(5), Some(15)),
                (a2(), Some(1_000), Some(500)),
                (b2(), Some(0), Some(500)),
            ],
        ));
        meta.loaded = Some((writable.clone(), readonly.clone()));
        meta.inner = json!([{ "index": 0, "instructions": [inner_ix(&full, &usdc_leg, 2), inner_ix(&full, &fee_leg, 2)] }]);
        out.push(Fixture {
            tx: versioned(0x12, static_keys, writable.len(), readonly.len(), vec![swap]),
            encoding: UiTransactionEncoding::Base64,
            meta,
            full_keys: full,
        });
    }

    // 3. Legacy, json, FAILED: a TransferChecked a1 -> b1 of 999 that ran
    //    out of funds. Balances unchanged; no transfer may be stored.
    {
        let keys = vec![alice(), a1().address, usdc(), b1().address, spl()];
        let ix = token::transfer_checked(&spl(), &a1().address, &usdc(), &b1().address, &alice(), &[], 999_999, 6).unwrap();
        let mut meta = Meta::ok(balances(&keys, &[(a1(), Some(900), Some(900)), (b1(), Some(150), Some(150))]));
        meta.err = Some(json!({"InstructionError": [0, {"Custom": 1}]}));
        meta.logs = vec!["Program log: Error: insufficient funds".to_owned()];
        out.push(Fixture {
            tx: legacy(0x13, keys.clone(), vec![compiled(&keys, &ix)]),
            encoding: UiTransactionEncoding::Json,
            meta,
            full_keys: keys,
        });
    }

    // 4. Legacy, base64: Token-2022 TransferCheckedWithFee b2 -> a2 of 200,
    //    fee 2. The destination is credited 198; 2 stay withheld.
    {
        let keys = vec![bob(), b2().address, feecoin(), a2().address, t22()];
        let ix = transfer_checked_with_fee(&t22(), &b2().address, &feecoin(), &a2().address, &bob(), &[], 200, 9, 2).unwrap();
        out.push(Fixture {
            tx: legacy(0x14, keys.clone(), vec![compiled(&keys, &ix)]),
            encoding: UiTransactionEncoding::Base64,
            meta: Meta::ok(balances(&keys, &[(b2(), Some(500), Some(300)), (a2(), Some(500), Some(698))])),
            full_keys: keys,
        });
    }

    // 5. Legacy, json: Alice empties e1 into a1 with a plain Transfer of 40,
    //    then closes e1. e1 has no post balance: owner and mint come from
    //    its pre balance alone.
    {
        let keys = vec![alice(), e1().address, a1().address, spl()];
        let transfer = token::transfer(&spl(), &e1().address, &a1().address, &alice(), &[], 40).unwrap();
        let close = token::close_account(&spl(), &e1().address, &alice(), &alice(), &[]).unwrap();
        out.push(Fixture {
            tx: legacy(0x15, keys.clone(), vec![compiled(&keys, &transfer), compiled(&keys, &close)]),
            encoding: UiTransactionEncoding::Json,
            meta: Meta::ok(balances(&keys, &[(e1(), Some(40), None), (a1(), Some(900), Some(940))])),
            full_keys: keys,
        });
    }

    // 6. A vote: no token balances at all.
    {
        let keys = vec![validator(), key(6), program(VOTE_PROGRAM_ID)];
        let vote = CompiledInstruction { program_id_index: 2, accounts: vec![1, 0], data: vec![2, 0, 0, 0] };
        let mut meta = Meta::ok((json!([]), json!([])));
        meta.compute_units = 2_100;
        out.push(Fixture {
            tx: legacy(0x16, keys.clone(), vec![vote]),
            encoding: UiTransactionEncoding::Base64,
            meta,
            full_keys: keys,
        });
    }

    // 7. v0, json: TransferChecked d1 -> a1 of 15 with both token accounts
    //    and the mint loaded, read from the raw JSON message's lookups.
    {
        let static_keys = vec![dave(), spl()];
        let writable = vec![d1().address, a1().address];
        let readonly = vec![usdc()];
        let full: Vec<Pubkey> = static_keys.iter().chain(&writable).chain(&readonly).copied().collect();
        let ix = token::transfer_checked(&spl(), &d1().address, &usdc(), &a1().address, &dave(), &[], 15, 6).unwrap();
        let mut meta = Meta::ok(balances(&full, &[(d1(), Some(15), Some(0)), (a1(), Some(940), Some(955))]));
        meta.loaded = Some((writable.clone(), readonly.clone()));
        out.push(Fixture {
            tx: versioned(0x17, static_keys, writable.len(), readonly.len(), vec![compiled(&full, &ix)]),
            encoding: UiTransactionEncoding::Json,
            meta,
            full_keys: full,
        });
    }

    out
}

fn block_of(slot: u64, fixtures: &[Fixture]) -> ChainBlock {
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": format!("hash-{}", slot - 1),
        "blockhash": format!("hash-{slot}"),
        "parentSlot": slot - 1,
        "transactions": fixtures.iter().map(Fixture::json).collect::<Vec<_>>(),
        "rewards": [],
        "blockTime": BLOCK_TIME,
        "blockHeight": slot,
    }))
    .expect("fixture block JSON is valid");
    ChainBlock {
        slot_or_height: slot,
        block_id: raw.blockhash.clone(),
        parent_id: raw.previous_blockhash.clone(),
        timestamp: raw.block_time,
        raw: ChainBlockData::Solana(Box::new(raw)),
    }
}

// ---------------------------------------------------------------------------
// The expectation, written out by hand
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
fn expected_transfer(
    seed: u8,
    instruction_index: u32,
    inner: Option<u32>,
    from: TokenAccount,
    to: TokenAccount,
    amount: u64,
) -> DeepAnaTokenTransfer {
    assert_eq!(from.mint, to.mint);
    DeepAnaTokenTransfer {
        slot: SLOT,
        tx_signature: sig(seed).to_string(),
        instruction_index,
        inner_instruction_index: inner,
        spl_token_program_id: s((from.program)()),
        source_token_account: s(from.address),
        destination_token_account: s(to.address),
        source_owner: Some(s(from.owner)),
        destination_owner: Some(s(to.owner)),
        mint_token_address: Some(s(from.mint)),
        amount_raw: amount,
        decimals: Some(from.decimals),
        block_time: Some(BLOCK_TIME),
    }
}

/// Every token transfer the block makes, in block / instruction order.
fn expected_transfers() -> Vec<DeepAnaTokenTransfer> {
    vec![
        expected_transfer(0x10, 0, None, a1(), b1(), 100),
        expected_transfer(0x11, 1, None, b1(), c1(), 30),
        expected_transfer(0x12, 0, Some(0), c1(), d1(), 10),
        expected_transfer(0x12, 0, Some(1), a2(), b2(), 500),
        // 0x13 failed: nothing.
        expected_transfer(0x14, 0, None, b2(), a2(), 200),
        expected_transfer(0x15, 0, None, e1(), a1(), 40),
        expected_transfer(0x17, 0, None, d1(), a1(), 15),
    ]
}

// ---------------------------------------------------------------------------
// A chain serving fixed blocks
// ---------------------------------------------------------------------------

#[derive(Default)]
struct BlockChain {
    blocks: Mutex<HashMap<u64, ChainBlock>>,
    account_requests: AtomicUsize,
}

#[async_trait]
impl ChainAdapter for BlockChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-fixture"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        let tip = self.blocks.lock().unwrap().keys().copied().max().unwrap_or(0);
        Ok(ChainTip { slot_or_height: tip, block_id: format!("hash-{tip}"), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        self.blocks.lock().unwrap().get(&slot).cloned().ok_or_else(|| PrismError::slot_skipped(slot))
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        self.account_requests.fetch_add(1, Ordering::SeqCst);
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingChangeLog {
    events: Mutex<Vec<ChangeEvent>>,
}

impl ChangeLog for RecordingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        let mut events = self.events.lock().unwrap();
        events.push(event);
        Ok(events.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-completeness-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Ingest `blocks` (slots `from..=to`) into a fresh directory; returns it,
/// the chain and the ingestor.
async fn ingest(blocks: Vec<ChainBlock>) -> (PathBuf, Arc<BlockChain>, Ingestor) {
    let dir = temp_dir();
    let chain = Arc::new(BlockChain::default());
    let (from, to) = (blocks[0].slot_or_height, blocks.last().unwrap().slot_or_height);
    chain.blocks.lock().unwrap().extend(blocks.into_iter().map(|b| (b.slot_or_height, b)));
    let config = IngestorConfig {
        start_slot: Some(from),
        end_slot: Some(to),
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_secs(3_600),
        fetcher: SlotFetcherConfig { max_retries: 1, base_delay_ms: 1, max_delay_ms: 2, request_timeout_ms: 5_000 },
        ..IngestorConfig::new(&dir)
    };
    let ingestor = Ingestor::new(
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        Arc::new(CommitmentTracker::from_watermark(Arc::new(FinalityWatermark::new()))),
        Arc::new(RecordingChangeLog::default()),
        config,
    )
    .unwrap();
    ingestor.run().await.unwrap();
    (dir, chain, ingestor)
}

// ---------------------------------------------------------------------------
// Reading Parquet back
// ---------------------------------------------------------------------------

fn read_table(dir: &Path, table: TableKind) -> Vec<RecordBatch> {
    let mut files = Vec::new();
    let mut pending = vec![PartitionKey::table_root(dir, table)];
    while let Some(d) = pending.pop() {
        let Ok(entries) = fs::read_dir(&d) else { continue };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
        .into_iter()
        .flat_map(|path| {
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn str_col(batch: &RecordBatch, name: &str, i: usize) -> Option<String> {
    let col = batch.column_by_name(name).unwrap().as_string::<i32>();
    col.is_valid(i).then(|| col.value(i).to_owned())
}

fn u64_col(batch: &RecordBatch, name: &str, i: usize) -> Option<u64> {
    let col = batch.column_by_name(name).unwrap().as_primitive::<UInt64Type>();
    col.is_valid(i).then(|| col.value(i))
}

fn u32_col(batch: &RecordBatch, name: &str, i: usize) -> Option<u32> {
    let col = batch.column_by_name(name).unwrap().as_primitive::<UInt32Type>();
    col.is_valid(i).then(|| col.value(i))
}

fn stored_transfers(dir: &Path) -> Vec<DeepAnaTokenTransfer> {
    let mut rows = Vec::new();
    for b in read_table(dir, TableKind::TokenTransfers) {
        let decimals = b.column_by_name("decimals").unwrap().as_primitive::<UInt8Type>();
        let times = b.column_by_name("block_time").unwrap().as_primitive::<TimestampSecondType>();
        for i in 0..b.num_rows() {
            rows.push(DeepAnaTokenTransfer {
                slot: u64_col(&b, "slot", i).unwrap(),
                tx_signature: str_col(&b, "tx_signature", i).unwrap(),
                instruction_index: u32_col(&b, "instruction_index", i).unwrap(),
                inner_instruction_index: u32_col(&b, "inner_instruction_index", i),
                spl_token_program_id: str_col(&b, "spl_token_program_id", i).unwrap(),
                source_token_account: str_col(&b, "source_token_account", i).unwrap(),
                destination_token_account: str_col(&b, "destination_token_account", i).unwrap(),
                source_owner: str_col(&b, "source_owner", i),
                destination_owner: str_col(&b, "destination_owner", i),
                mint_token_address: str_col(&b, "mint_token_address", i),
                amount_raw: u64_col(&b, "amount_raw", i).unwrap(),
                decimals: decimals.is_valid(i).then(|| decimals.value(i)),
                block_time: times.is_valid(i).then(|| times.value(i)),
            });
        }
    }
    rows
}

fn stored_transactions(dir: &Path) -> Vec<DeepAnaTransaction> {
    let mut rows = Vec::new();
    for b in read_table(dir, TableKind::Transactions) {
        for i in 0..b.num_rows() {
            rows.push(DeepAnaTransaction {
                slot: u64_col(&b, "slot", i).unwrap(),
                blockhash: str_col(&b, "blockhash", i).unwrap(),
                transaction_index: u32_col(&b, "transaction_index", i).unwrap(),
                tx_signature: str_col(&b, "tx_signature", i).unwrap(),
                fee: u64_col(&b, "fee", i).unwrap(),
                status: str_col(&b, "status", i).unwrap().parse().unwrap(),
                err: str_col(&b, "err", i),
                accounts: serde_json::from_str(&str_col(&b, "accounts", i).unwrap()).unwrap(),
                log_messages: serde_json::from_str(&str_col(&b, "log_messages", i).unwrap()).unwrap(),
                compute_units_consumed: u64_col(&b, "compute_units_consumed", i),
            });
        }
    }
    rows
}

/// `(tx_signature, instruction_index, inner_instruction_index)` → decoded
/// data, for every stored instruction row.
fn stored_instructions(dir: &Path) -> BTreeMap<(String, u32, Option<u32>), (Option<String>, Option<Value>)> {
    let mut rows = BTreeMap::new();
    for b in read_table(dir, TableKind::Instructions) {
        for i in 0..b.num_rows() {
            let key = (
                str_col(&b, "tx_signature", i).unwrap(),
                u32_col(&b, "instruction_index", i).unwrap(),
                u32_col(&b, "inner_instruction_index", i),
            );
            let value = (
                str_col(&b, "instruction_type", i),
                str_col(&b, "decoded_data", i).map(|d| serde_json::from_str(&d).unwrap()),
            );
            assert!(rows.insert(key.clone(), value).is_none(), "instruction {key:?} stored twice");
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// The on-chain audit
// ---------------------------------------------------------------------------

/// For every successful transaction of `fixtures`: the change of every token
/// account's balance that the stored transfer rows imply, next to the change
/// the chain reported. Returns the disagreements, as text.
///
/// A transfer debits its source by `amount_raw` and credits its destination
/// with `amount_raw` minus the withheld fee of a `TransferCheckedWithFee`
/// (read from the stored instruction row, not from the fixture). An account
/// the chain reports and no row touches must not have changed.
fn audit(
    fixtures: &[Fixture],
    transfers: &[DeepAnaTokenTransfer],
    instructions: &BTreeMap<(String, u32, Option<u32>), (Option<String>, Option<Value>)>,
) -> Vec<String> {
    let mut problems = Vec::new();
    for fixture in fixtures.iter().filter(|f| f.meta.err.is_none()) {
        let signature = fixture.signature();
        let mut implied: BTreeMap<String, i128> = BTreeMap::new();
        for t in transfers.iter().filter(|t| t.tx_signature == signature) {
            let ix_key = (t.tx_signature.clone(), t.instruction_index, t.inner_instruction_index);
            let withheld = match instructions.get(&ix_key) {
                Some((Some(kind), Some(data))) if kind == "TransferCheckedWithFee" => data["fee"].as_u64().unwrap(),
                Some(_) => 0,
                None => {
                    problems.push(format!("{ix_key:?}: transfer without an instruction row"));
                    0
                }
            };
            *implied.entry(t.source_token_account.clone()).or_default() -= i128::from(t.amount_raw);
            *implied.entry(t.destination_token_account.clone()).or_default() +=
                i128::from(t.amount_raw) - i128::from(withheld);
        }
        // What the chain reported, read straight from the meta JSON.
        let meta: UiTransactionStatusMeta = serde_json::from_value(fixture.meta.json()).unwrap();
        let keys: Vec<String> = fixture.full_keys.iter().map(|k| s(*k)).collect();
        let reported: BTreeMap<String, i128> = token_balance_changes(SLOT, &signature, &keys, Some(&meta))
            .unwrap()
            .into_iter()
            .map(|c| (c.account.clone(), c.delta()))
            .collect();
        for account in implied.keys().chain(reported.keys()).collect::<std::collections::BTreeSet<_>>() {
            let (rows, chain) = (implied.get(account).copied().unwrap_or(0), reported.get(account).copied().unwrap_or(0));
            if rows != chain {
                problems.push(format!("tx {signature}, account {account}: rows imply {rows}, the chain reported {chain}"));
            }
        }
    }
    problems
}

// ---------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_transaction_and_every_token_transfer_is_stored_exactly_and_balances_reconcile() {
    let fixtures = fixtures();
    let (dir, chain, ingestor) = ingest(vec![block_of(SLOT, &fixtures)]).await;

    // Transactions: all of them, once each, every column as the block says.
    let stored = stored_transactions(&dir);
    assert_eq!(stored.len(), fixtures.len(), "one row per transaction");
    let signatures: HashSet<&str> = stored.iter().map(|t| t.tx_signature.as_str()).collect();
    assert_eq!(signatures.len(), fixtures.len(), "no transaction twice");
    for (index, (row, fixture)) in stored.iter().zip(&fixtures).enumerate() {
        let failed = fixture.meta.err.is_some();
        let expected = DeepAnaTransaction {
            slot: SLOT,
            blockhash: format!("hash-{SLOT}"),
            transaction_index: index as u32,
            tx_signature: fixture.signature(),
            fee: fixture.meta.fee,
            status: if failed { TransactionStatus::Failed } else { TransactionStatus::Success },
            err: fixture.meta.err.as_ref().map(|e| serde_json::to_string(e).unwrap()),
            accounts: fixture.full_keys.iter().map(|k| s(*k)).collect(),
            log_messages: fixture.meta.logs.clone(),
            compute_units_consumed: Some(fixture.meta.compute_units),
        };
        assert_eq!(*row, expected, "transaction {index}");
    }

    // Token transfers: exactly the expected ones, every column.
    let transfers = stored_transfers(&dir);
    assert_eq!(transfers, expected_transfers());
    assert!(
        !transfers.iter().any(|t| t.tx_signature == sig(0x13).to_string()),
        "the failed transaction moved nothing"
    );

    // Instruction rows: every top-level and inner instruction, once.
    let instructions = stored_instructions(&dir);
    let expected_count: usize = fixtures
        .iter()
        .map(|f| {
            f.tx.message.instructions().len()
                + f.meta.inner.as_array().unwrap().iter().map(|g| g["instructions"].as_array().unwrap().len()).sum::<usize>()
        })
        .sum();
    assert_eq!(instructions.len(), expected_count);
    let failed_ix = &instructions[&(sig(0x13).to_string(), 0, None)];
    assert_eq!(failed_ix.0.as_deref(), Some("TransferChecked"), "a failed transaction's instructions are still decoded");

    // On chain: the rows add up to the balance changes the chain reported.
    let problems = audit(&fixtures, &transfers, &instructions);
    assert!(problems.is_empty(), "{problems:#?}");

    // All of it from the transactions' own metadata: not one RPC request.
    assert_eq!(chain.account_requests.load(Ordering::SeqCst), 0);
    let m = ingestor.resolver_metrics().snapshot();
    assert_eq!(m.total_transfers_processed, 7);
    assert_eq!((m.fully_enriched, m.partially_enriched), (7, 0));
    assert_eq!(m.filled_from_metadata, 7);
    assert_eq!(m.mint_fetches_attempted, 0);
}

#[tokio::test]
async fn the_audit_catches_a_missing_a_doubled_and_a_wrong_transfer() {
    // The audit itself must have teeth, or the test above proves nothing.
    let fixtures = fixtures();
    let (dir, _, _) = ingest(vec![block_of(SLOT, &fixtures)]).await;
    let transfers = stored_transfers(&dir);
    let instructions = stored_instructions(&dir);
    assert!(audit(&fixtures, &transfers, &instructions).is_empty());

    let mut missing = transfers.clone();
    missing.remove(2);
    assert!(!audit(&fixtures, &missing, &instructions).is_empty(), "a missing CPI transfer");

    let mut doubled = transfers.clone();
    doubled.push(transfers[0].clone());
    assert!(!audit(&fixtures, &doubled, &instructions).is_empty(), "a doubled transfer");

    let mut wrong = transfers.clone();
    wrong[4].amount_raw -= 2; // as if the fee had been taken off the amount
    assert!(!audit(&fixtures, &wrong, &instructions).is_empty(), "a wrong amount");

    let mut swapped = transfers;
    swapped[1].destination_token_account = s(d1().address);
    assert!(!audit(&fixtures, &swapped, &instructions).is_empty(), "a wrong account");
}

#[tokio::test]
async fn completeness_holds_across_several_blocks_and_flushes() {
    // The same fixture in three consecutive slots, signatures made unique
    // per slot, with a skipped slot in between.
    let blocks: Vec<ChainBlock> = [SLOT, SLOT + 2, SLOT + 3]
        .into_iter()
        .map(|slot| {
            let mut fixtures = fixtures();
            for f in &mut fixtures {
                let mut bytes = [0u8; 64];
                bytes.copy_from_slice(f.tx.signatures[0].as_ref());
                bytes[63] = (slot - SLOT) as u8;
                f.tx.signatures[0] = Signature::from(bytes);
            }
            block_of(slot, &fixtures)
        })
        .collect();
    let (dir, _, _) = ingest(blocks).await;
    let transfers = stored_transfers(&dir);
    assert_eq!(transfers.len(), 3 * expected_transfers().len());
    let unique: HashSet<_> = transfers
        .iter()
        .map(|t| (t.tx_signature.clone(), t.instruction_index, t.inner_instruction_index))
        .collect();
    assert_eq!(unique.len(), transfers.len(), "each transfer once");
    assert!(transfers.iter().all(|t| t.mint_token_address.is_some()
        && t.decimals.is_some()
        && t.source_owner.is_some()
        && t.destination_owner.is_some()));
    assert_eq!(stored_transactions(&dir).len(), 3 * fixtures().len());
}

// ---------------------------------------------------------------------------
// Normalizer: token balances
// ---------------------------------------------------------------------------

fn normalize(fixtures: &[Fixture]) -> prism::normalizer::NormalizedBlock {
    Normalizer::new().normalize(&block_of(SLOT, fixtures)).unwrap()
}

#[test]
fn token_balances_resolve_against_the_full_key_list_of_v0_transactions() {
    let fixtures = fixtures();
    let block = normalize(&fixtures);
    // Transaction 2: every token account is loaded from the lookup table.
    let tx2: Vec<&TokenBalanceChange> =
        block.token_balances.iter().filter(|c| c.tx_signature == sig(0x12).to_string()).collect();
    let accounts: Vec<&str> = tx2.iter().map(|c| c.account.as_str()).collect();
    assert_eq!(accounts, [s(c1().address), s(d1().address), s(a2().address), s(b2().address)]);
    assert_eq!(tx2.iter().map(|c| c.account_index).collect::<Vec<_>>(), [2, 3, 4, 5], "indexes into the loaded keys");
    assert_eq!(tx2[2].mint(), Some(s(feecoin()).as_str()));
    assert_eq!(tx2[2].program_id(), Some(TOKEN_2022_PROGRAM_ID));
    assert_eq!((tx2[2].decimals(), tx2[2].delta()), (Some(9), -500));
}

#[test]
fn token_balances_mark_created_and_closed_accounts() {
    let block = normalize(&fixtures());
    let find = |seed: u8, account: TokenAccount| {
        block
            .token_balances
            .iter()
            .find(|c| c.tx_signature == sig(seed).to_string() && c.account == s(account.address))
            .unwrap()
    };
    let created = find(0x11, c1());
    assert!(created.pre.is_none() && created.post.is_some());
    assert_eq!((created.owner(), created.delta()), (Some(s(carol()).as_str()), 30));
    let closed = find(0x15, e1());
    assert!(closed.pre.is_some() && closed.post.is_none());
    assert_eq!((closed.owner(), closed.mint(), closed.delta()), (Some(s(alice()).as_str()), Some(s(usdc()).as_str()), -40));
    assert!(block.token_balances.iter().all(|c| c.tx_signature != sig(0x16).to_string()), "the vote has none");
}

fn side(mint: Pubkey, owner: Pubkey, amount: u64) -> TokenBalanceSide {
    TokenBalanceSide { mint: s(mint), owner: Some(s(owner)), program_id: Some(SPL_TOKEN_PROGRAM_ID.into()), decimals: 6, amount }
}

#[test]
fn disagreeing_sides_withhold_the_ambiguous_value() {
    let mut change = TokenBalanceChange {
        slot: SLOT,
        tx_signature: "sig".into(),
        account_index: 1,
        account: s(a1().address),
        pre: Some(side(usdc(), alice(), 10)),
        post: Some(side(usdc(), bob(), 10)),
    };
    assert_eq!(change.owner(), None, "owner changed within the transaction");
    assert_eq!(change.mint(), Some(s(usdc()).as_str()));
    change.post = Some(side(feecoin(), alice(), 10));
    assert_eq!(change.mint(), None, "closed and re-created for another mint");
    assert_eq!(change.owner(), Some(s(alice()).as_str()));
    change.post.as_mut().unwrap().owner = None;
    assert_eq!(change.owner(), None, "one side does not name an owner");
}

fn meta_with(pre: Value, post: Value) -> UiTransactionStatusMeta {
    serde_json::from_value(json!({
        "err": null, "status": {"Ok": null}, "fee": 0, "preBalances": [], "postBalances": [],
        "preTokenBalances": pre, "postTokenBalances": post,
    }))
    .unwrap()
}

fn raw_balance(index: u8, amount: &str) -> Value {
    json!({"accountIndex": index, "mint": s(usdc()), "uiTokenAmount": {"amount": amount, "decimals": 6, "uiAmount": null, "uiAmountString": "0"}})
}

#[test]
fn malformed_token_balances_fail_rather_than_mislabel() {
    let keys = vec![s(alice()), s(a1().address)];
    let read = |pre: Value, post: Value| token_balance_changes(SLOT, "sig", &keys, Some(&meta_with(pre, post)));

    let err = read(json!([raw_balance(2, "1")]), json!([])).unwrap_err();
    assert!(err.to_string().contains("references account 2, but the transaction has 2 account keys"), "{err}");
    let err = read(json!([raw_balance(1, "1"), raw_balance(1, "2")]), json!([])).unwrap_err();
    assert!(err.to_string().contains("lists account 1 twice"), "{err}");
    let err = read(json!([]), json!([raw_balance(1, "-5")])).unwrap_err();
    assert!(err.to_string().contains("is not a u64"), "{err}");
    assert_eq!(err.slot(), Some(SLOT));

    // Absent fields (very old blocks) are no entries, not errors; so is no meta.
    let old: UiTransactionStatusMeta = serde_json::from_value(json!({
        "err": null, "status": {"Ok": null}, "fee": 0, "preBalances": [], "postBalances": [],
    }))
    .unwrap();
    assert!(token_balance_changes(SLOT, "sig", &keys, Some(&old)).unwrap().is_empty());
    assert!(token_balance_changes(SLOT, "sig", &keys, None).unwrap().is_empty());
    // Owner and program id may be missing too.
    let only = read(json!([raw_balance(1, "7")]), json!([])).unwrap();
    assert_eq!((only[0].owner(), only[0].program_id(), only[0].delta()), (None, None, -7));
}

#[test]
fn a_bad_token_balance_fails_the_whole_block() {
    let mut fixtures = fixtures();
    fixtures[4].meta.token_balances.1[0]["accountIndex"] = json!(99);
    let err = Normalizer::new().normalize(&block_of(SLOT, &fixtures)).unwrap_err();
    assert_eq!(err.kind(), "deserialize_error");
    assert!(err.to_string().contains(&sig(0x14).to_string()), "names the transaction: {err}");
}

// ---------------------------------------------------------------------------
// Account resolver: rules for using the metadata
// ---------------------------------------------------------------------------

fn bare_transfer(from: TokenAccount, to: TokenAccount, checked: bool) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        slot: SLOT,
        tx_signature: "tx".into(),
        spl_token_program_id: s((from.program)()),
        source_token_account: s(from.address),
        destination_token_account: s(to.address),
        mint_token_address: checked.then(|| s(from.mint)),
        decimals: checked.then_some(from.decimals),
        amount_raw: 1,
        ..DeepAnaTokenTransfer::default()
    }
}

fn meta_of(account: TokenAccount) -> TokenAccountMeta {
    TokenAccountMeta {
        mint: Some(s(account.mint)),
        owner: Some(s(account.owner)),
        program_id: Some(s((account.program)())),
        decimals: Some(account.decimals),
    }
}

fn index_of(entries: &[(TokenAccount, TokenAccountMeta)]) -> TokenAccountIndex {
    let mut index = TokenAccountIndex::new();
    for (account, meta) in entries {
        index.insert("tx", &s(account.address), meta.clone());
    }
    index
}

/// Serves one USDC mint with 6 decimals and counts requests.
#[derive(Default)]
struct MintChain {
    requests: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl ChainAdapter for MintChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }
    fn chain_id(&self) -> &str {
        "solana-mints"
    }
    async fn get_tip(&self, _: CommitmentLevel) -> PrismResult<ChainTip> {
        unimplemented!()
    }
    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        Err(PrismError::slot_skipped(slot))
    }
    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        use solana_sdk::program_pack::Pack;
        self.requests.lock().unwrap().push(addresses.to_vec());
        Ok(addresses
            .iter()
            .map(|a| {
                (*a == s(usdc())).then(|| {
                    let mut data = vec![0; spl_token_2022_interface::state::Mint::LEN];
                    let mint = spl_token_2022_interface::state::Mint { decimals: 6, is_initialized: true, ..Default::default() };
                    spl_token_2022_interface::state::Mint::pack_into_slice(&mint, &mut data);
                    AccountData { owner_program: SPL_TOKEN_PROGRAM_ID.into(), lamports: 1, data, ..AccountData::default() }
                })
            })
            .collect())
    }
    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn resolver() -> (Arc<MintChain>, AccountResolver) {
    let chain = Arc::new(MintChain::default());
    let resolver = AccountResolver::new(Arc::clone(&chain) as Arc<dyn ChainAdapter>);
    (chain, resolver)
}

#[tokio::test]
async fn a_plain_transfer_gets_mint_decimals_and_owners_without_a_request() {
    let (chain, mut resolver) = resolver();
    let index = index_of(&[(a1(), meta_of(a1())), (b1(), meta_of(b1()))]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), false)], &index).await.unwrap();
    let t = &out[0];
    assert_eq!(t.mint_token_address, Some(s(usdc())));
    assert_eq!(t.decimals, Some(6));
    assert_eq!((t.source_owner.clone(), t.destination_owner.clone()), (Some(s(alice())), Some(s(bob()))));
    assert!(chain.requests.lock().unwrap().is_empty());
    let m = resolver.metrics().snapshot();
    assert_eq!((m.filled_from_metadata, m.fully_enriched, m.mint_fetches_attempted), (1, 1, 0));
}

#[tokio::test]
async fn one_side_of_metadata_is_enough_but_two_disagreeing_mints_are_not() {
    let (_, mut resolver) = resolver();
    // Only the destination is known (the source was, say, not reported).
    let index = index_of(&[(b1(), meta_of(b1()))]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), false)], &index).await.unwrap();
    assert_eq!((out[0].mint_token_address.clone(), out[0].decimals), (Some(s(usdc())), Some(6)));
    assert_eq!((out[0].source_owner.clone(), out[0].destination_owner.clone()), (None, Some(s(bob()))));

    // The two sides name different mints: no mint is guessed; owners stay.
    let mut other = meta_of(b1());
    other.mint = Some(s(key(99)));
    let index = index_of(&[(a1(), meta_of(a1())), (b1(), other)]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), false)], &index).await.unwrap();
    assert_eq!((out[0].mint_token_address.clone(), out[0].decimals), (None, None));
    assert_eq!(out[0].source_owner, Some(s(alice())));
}

#[tokio::test]
async fn metadata_of_another_token_program_is_ignored() {
    let (_, mut resolver) = resolver();
    let mut foreign = meta_of(a1());
    foreign.program_id = Some(TOKEN_2022_PROGRAM_ID.into());
    let index = index_of(&[(a1(), foreign.clone()), (b1(), foreign)]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), false)], &index).await.unwrap();
    assert_eq!(out[0], bare_transfer(a1(), b1(), false), "nothing filled");
    assert_eq!(resolver.metrics().snapshot().filled_from_metadata, 0);

    // No program id (older RPC answers) cannot contradict, and is used.
    let mut unnamed = meta_of(a1());
    unnamed.program_id = None;
    let index = index_of(&[(a1(), unnamed)]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), false)], &index).await.unwrap();
    assert_eq!(out[0].mint_token_address, Some(s(usdc())));
}

#[tokio::test]
async fn the_instructions_mint_wins_over_contradicting_metadata() {
    let (chain, mut resolver) = resolver();
    let mut wrong = meta_of(a1());
    wrong.mint = Some(s(key(99)));
    wrong.decimals = Some(2);
    let index = index_of(&[(a1(), wrong)]);
    let out = resolver.resolve_with_metadata(vec![bare_transfer(a1(), b1(), true)], &index).await.unwrap();
    assert_eq!(out[0].mint_token_address, Some(s(usdc())), "kept");
    assert_eq!(out[0].decimals, Some(6), "the metadata's 2 are not used; the mint account says 6");
    assert_eq!(out[0].source_owner, Some(s(alice())), "owners are facts about the account");
    assert_eq!(chain.requests.lock().unwrap().len(), 1, "decimals came from the mint account");
}

#[tokio::test]
async fn the_chains_decimals_replace_the_instructions() {
    let (chain, mut resolver) = resolver();
    let mut transfer = bare_transfer(a1(), b1(), true);
    transfer.decimals = Some(3);
    let index = index_of(&[(a1(), meta_of(a1()))]);
    let out = resolver.resolve_with_metadata(vec![transfer], &index).await.unwrap();
    assert_eq!(out[0].decimals, Some(6));
    assert!(chain.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn without_metadata_the_rpc_path_is_unchanged() {
    let (chain, mut resolver) = resolver();
    let out = resolver
        .resolve_with_metadata(
            vec![bare_transfer(a1(), b1(), true), bare_transfer(a1(), b1(), false)],
            &TokenAccountIndex::new(),
        )
        .await
        .unwrap();
    assert_eq!(out[0].decimals, Some(6));
    assert_eq!(out[1], bare_transfer(a1(), b1(), false), "a plain transfer stays partial");
    assert_eq!(*chain.requests.lock().unwrap(), vec![vec![s(usdc())]]);
    let m = resolver.metrics().snapshot();
    assert_eq!((m.filled_from_metadata, m.fully_enriched, m.partially_enriched), (0, 1, 1));
}

#[test]
fn transfers_of_failed_transactions_are_dropped() {
    let ok = DeepAnaTransaction { tx_signature: "ok".into(), status: TransactionStatus::Success, ..Default::default() };
    let failed = DeepAnaTransaction { tx_signature: "bad".into(), status: TransactionStatus::Failed, ..Default::default() };
    let transfer = |sig: &str| DeepAnaTokenTransfer { tx_signature: sig.into(), ..Default::default() };
    let kept = executed_transfers(vec![transfer("ok"), transfer("bad"), transfer("ok")], &[ok, failed]);
    assert_eq!(kept, vec![transfer("ok"), transfer("ok")]);
}

#[test]
fn the_index_is_keyed_by_transaction_and_account() {
    let block = normalize(&fixtures());
    let index = TokenAccountIndex::from_balances(&block.token_balances);
    assert_eq!(index.len(), block.token_balances.len());
    // a1 appears in several transactions; each keeps its own view.
    let in_tx0 = index.get(&sig(0x10).to_string(), &s(a1().address)).unwrap();
    assert_eq!(in_tx0.owner, Some(s(alice())));
    assert!(index.get(&sig(0x16).to_string(), &s(a1().address)).is_none());
    assert!(index.get("unknown", &s(a1().address)).is_none());
}
