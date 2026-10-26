//! Tests for `prism::account_resolver`.
//!
//! The resolver reads mints through a [`MockAdapter`] that serves accounts
//! from a map and records every `get_accounts` call, so the tests can check
//! how many RPC round-trips a batch costs and which addresses each asked
//! for. Mint accounts are packed with the token programs' own `Pack` and
//! extension code: the encoder and the resolver share no code.
//!
//! Transfers come either hand-built (to reach every branch) or out of the
//! real [`SplTokenDecoder`] fed instructions from the token programs'
//! builders, so the resolver is tested on exactly what the decoder emits.
//!
//! `live_resolver_enriches_devnet_transfers` runs the decoder and the
//! resolver on recent devnet blocks. It needs `HELIUS_API_KEY` and is
//! skipped without it:
//!
//! ```text
//! HELIUS_API_KEY=... cargo test -p prism --test account_resolver_test live -- --nocapture
//! ```

#![allow(deprecated)] // `Transfer` is deprecated in favour of `TransferChecked`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use helius::types::Cluster;
use prism::account_resolver::{
    AccountResolver, DEFAULT_MISS_TTL, MintInfo, ResolverMetricsSnapshot, TokenProgram,
};
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::decoder::spl_token::{SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};
use prism::decoder::{Decoder, DecoderRegistry, SplTokenDecoder};
use prism::ingestor::{HELIUS_API_KEY_ENV, helius_api_key_from_env, helius_solana_adapter};
use prism::normalizer::Normalizer;
use prism::types::error::{PrismError, PrismResult};
use prism::types::udm::{DeepAnaInstruction, DeepAnaTokenTransfer};
use solana_sdk::program_pack::Pack;
use solana_sdk::pubkey::Pubkey;
use spl_token_2022_interface::extension::mint_close_authority::MintCloseAuthority;
use spl_token_2022_interface::extension::{
    BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
};
use spl_token_2022_interface::instruction as token_2022;
use spl_token_2022_interface::state::{Account, AccountState, Mint};

// ---------------------------------------------------------------------------
// Mock adapter
// ---------------------------------------------------------------------------

/// Serves accounts from a map; an address not in it does not exist.
#[derive(Default)]
struct MockAdapter {
    accounts: Mutex<HashMap<String, AccountData>>,
    /// The addresses of every `get_accounts` call, in call order.
    calls: Mutex<Vec<Vec<String>>>,
    /// When set, `get_accounts` fails (after recording the call).
    down: Mutex<bool>,
    /// When set, `get_accounts` answers with one account too few.
    short_answer: Mutex<bool>,
}

impl MockAdapter {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn put(&self, address: &str, account: AccountData) {
        self.accounts.lock().unwrap().insert(address.to_owned(), account);
    }

    fn set_down(&self, down: bool) {
        *self.down.lock().unwrap() = down;
    }

    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ChainAdapter for MockAdapter {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-test"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        Ok(ChainTip { slot_or_height: 0, block_id: String::new(), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        Err(PrismError::SlotSkipped { slot })
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        self.calls.lock().unwrap().push(addresses.to_vec());
        if *self.down.lock().unwrap() {
            return Err(PrismError::RateLimitError { retry_after_ms: None });
        }
        let accounts = self.accounts.lock().unwrap();
        let mut answer: Vec<Option<AccountData>> =
            addresses.iter().map(|address| accounts.get(address).cloned()).collect();
        if *self.short_answer.lock().unwrap() {
            answer.pop();
        }
        Ok(answer)
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const SIG: &str = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW";

fn key(n: u8) -> String {
    Pubkey::new_from_array([n; 32]).to_string()
}

/// Mint addresses used across the tests.
fn usdc() -> String {
    key(10)
}
fn wsol() -> String {
    key(11)
}
fn pyusd() -> String {
    key(12)
}

fn mint_state(decimals: u8) -> Mint {
    Mint { decimals, is_initialized: true, supply: 1_000_000, ..Mint::default() }
}

/// An SPL Token mint account.
fn standard_mint(decimals: u8) -> AccountData {
    let mut data = vec![0; Mint::LEN];
    Mint::pack_into_slice(&mint_state(decimals), &mut data);
    AccountData { owner_program: SPL_TOKEN_PROGRAM_ID.to_owned(), lamports: 1, data, ..AccountData::default() }
}

/// A Token-2022 mint account with an extension.
fn token_2022_mint(decimals: u8) -> AccountData {
    let len = ExtensionType::try_calculate_account_len::<Mint>(&[ExtensionType::MintCloseAuthority]).unwrap();
    let mut data = vec![0; len];
    let mut state = StateWithExtensionsMut::<Mint>::unpack_uninitialized(&mut data).unwrap();
    state.base = mint_state(decimals);
    state.pack_base();
    state.init_extension::<MintCloseAuthority>(true).unwrap();
    state.init_account_type().unwrap();
    AccountData { owner_program: TOKEN_2022_PROGRAM_ID.to_owned(), lamports: 1, data, ..AccountData::default() }
}

/// An SPL Token *token account*: owned by the token program, not a mint.
fn token_account() -> AccountData {
    let mut data = vec![0; Account::LEN];
    let account = Account {
        mint: Pubkey::new_unique(),
        owner: Pubkey::new_unique(),
        amount: 1,
        state: AccountState::Initialized,
        ..Account::default()
    };
    Account::pack_into_slice(&account, &mut data);
    AccountData { owner_program: SPL_TOKEN_PROGRAM_ID.to_owned(), lamports: 1, data, ..AccountData::default() }
}

/// A transfer as the decoder emits it for `TransferChecked`: mint and
/// decimals from the instruction.
fn checked(index: u32, program: &str, mint: &str, decimals: u8) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer {
        slot: 1,
        tx_signature: SIG.to_owned(),
        instruction_index: index,
        inner_instruction_index: None,
        spl_token_program_id: program.to_owned(),
        source_token_account: key(1),
        destination_token_account: key(2),
        source_owner: None,
        destination_owner: None,
        mint_token_address: Some(mint.to_owned()),
        amount_raw: 1_000 + u64::from(index),
        decimals: Some(decimals),
        block_time: Some(1_700_000_000),
    }
}

/// A transfer as the decoder emits it for plain `Transfer`: no mint, no
/// decimals.
fn plain(index: u32, program: &str) -> DeepAnaTokenTransfer {
    DeepAnaTokenTransfer { mint_token_address: None, decimals: None, ..checked(index, program, "", 0) }
}

/// A resolver over `adapter`, seen as the trait object the pipeline uses.
fn resolver(adapter: &Arc<MockAdapter>) -> AccountResolver {
    AccountResolver::new(Arc::clone(adapter) as Arc<dyn ChainAdapter>)
}

fn assert_metrics_add_up(m: &ResolverMetricsSnapshot) {
    assert_eq!(m.total_transfers_processed, m.fully_enriched + m.partially_enriched, "{m:?}");
    assert_eq!(m.mint_fetches_attempted, m.mint_fetches_succeeded + m.mint_fetches_failed, "{m:?}");
}

// ---------------------------------------------------------------------------
// Enrichment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transfer_checked_comes_out_fully_enriched() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    let mut resolver = resolver(&adapter);

    let out = resolver.resolve(vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6)]).await.unwrap();

    assert_eq!(out, vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6)], "nothing else changes");
    assert_eq!(out[0].amount_ui(), Some(1_000.0 / 1e6));
    assert_eq!(
        resolver.mint_cache().peek(&usdc()),
        Some(&MintInfo { decimals: 6, symbol: None, name: None, token_program: TokenProgram::Standard })
    );
    let m = resolver.metrics().snapshot();
    assert_eq!(
        m,
        ResolverMetricsSnapshot {
            total_transfers_processed: 1,
            fully_enriched: 1,
            partially_enriched: 0,
            mint_fetches_attempted: 1,
            mint_fetches_succeeded: 1,
            mint_fetches_failed: 0,
            cache_hits: 0,
        }
    );
}

#[tokio::test]
async fn plain_transfer_stays_partial_and_costs_no_rpc() {
    let adapter = MockAdapter::new();
    let mut resolver = resolver(&adapter);

    let input = vec![plain(0, SPL_TOKEN_PROGRAM_ID), plain(1, TOKEN_2022_PROGRAM_ID)];
    let out = resolver.resolve(input.clone()).await.unwrap();

    assert_eq!(out, input);
    assert!(adapter.calls().is_empty(), "no mint to fetch");
    let m = resolver.metrics().snapshot();
    assert_eq!((m.fully_enriched, m.partially_enriched, m.mint_fetches_attempted), (0, 2, 0));
}

#[tokio::test]
async fn token_2022_mints_with_extensions_resolve() {
    let adapter = MockAdapter::new();
    adapter.put(&pyusd(), token_2022_mint(6));
    let mut resolver = resolver(&adapter);

    let mut transfer = checked(0, TOKEN_2022_PROGRAM_ID, &pyusd(), 6);
    transfer.decimals = None;
    let out = resolver.resolve(vec![transfer]).await.unwrap();

    assert_eq!(out[0].decimals, Some(6));
    assert_eq!(resolver.mint_cache().peek(&pyusd()).unwrap().token_program, TokenProgram::Token2022);
    assert_eq!(resolver.metrics().snapshot().fully_enriched, 1);
}

#[tokio::test]
async fn a_known_mint_without_decimals_gets_them() {
    let adapter = MockAdapter::new();
    adapter.put(&wsol(), standard_mint(9));
    let mut resolver = resolver(&adapter);

    let mut transfer = checked(0, SPL_TOKEN_PROGRAM_ID, &wsol(), 0);
    transfer.decimals = None;
    let out = resolver.resolve(vec![transfer]).await.unwrap();

    assert_eq!(out[0].decimals, Some(9));
    let m = resolver.metrics().snapshot();
    assert_eq!((m.fully_enriched, m.partially_enriched), (1, 0));
}

/// The chain is the authority: a failed `TransferChecked` can carry
/// decimals that do not match its mint.
#[tokio::test]
async fn the_mints_decimals_override_the_instructions() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    let mut resolver = resolver(&adapter);

    let out = resolver.resolve(vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 9)]).await.unwrap();

    assert_eq!(out[0].decimals, Some(6));
    assert_eq!(resolver.mint_cache().peek(&usdc()).unwrap().decimals, 6, "the cache holds the chain's value");
}

/// A Token-2022 mint cannot be the mint of an SPL Token transfer (and the
/// reverse): its metadata is not applied.
#[tokio::test]
async fn a_mint_of_the_other_token_program_is_not_applied() {
    let adapter = MockAdapter::new();
    adapter.put(&pyusd(), token_2022_mint(2));
    let mut resolver = resolver(&adapter);

    let mut no_decimals = checked(0, SPL_TOKEN_PROGRAM_ID, &pyusd(), 0);
    no_decimals.decimals = None;
    let with_decimals = checked(1, SPL_TOKEN_PROGRAM_ID, &pyusd(), 6);
    let out = resolver.resolve(vec![no_decimals.clone(), with_decimals.clone()]).await.unwrap();

    assert_eq!(out, vec![no_decimals, with_decimals], "both left as they came");
    let m = resolver.metrics().snapshot();
    assert_eq!((m.fully_enriched, m.partially_enriched), (1, 1));
}

#[tokio::test]
async fn order_and_every_other_field_are_preserved() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    adapter.put(&wsol(), standard_mint(9));
    let mut resolver = resolver(&adapter);

    let mut input = vec![
        checked(0, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
        plain(1, SPL_TOKEN_PROGRAM_ID),
        checked(2, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
    ];
    input[1].inner_instruction_index = Some(3);
    input[2].source_owner = Some(key(7));
    let out = resolver.resolve(input.clone()).await.unwrap();

    assert_eq!(out, input);
}

#[tokio::test]
async fn an_empty_batch_is_a_no_op() {
    let adapter = MockAdapter::new();
    let mut resolver = resolver(&adapter);
    assert!(resolver.resolve(Vec::new()).await.unwrap().is_empty());
    assert!(adapter.calls().is_empty());
    assert_eq!(resolver.metrics().snapshot(), ResolverMetricsSnapshot::default());
}

// ---------------------------------------------------------------------------
// Fetching and caching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_mints_are_deduplicated_into_one_batch() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    adapter.put(&wsol(), standard_mint(9));
    adapter.put(&pyusd(), token_2022_mint(6));
    let mut resolver = resolver(&adapter);

    let input = vec![
        checked(0, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
        checked(1, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
        plain(2, SPL_TOKEN_PROGRAM_ID),
        checked(3, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
        checked(4, TOKEN_2022_PROGRAM_ID, &pyusd(), 6),
        checked(5, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
        checked(6, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
    ];
    resolver.resolve(input).await.unwrap();

    assert_eq!(adapter.calls(), vec![vec![wsol(), usdc(), pyusd()]], "one call, each mint once, first-seen order");
    assert_eq!(resolver.mint_cache().fetch_count(), 3);
    let m = resolver.metrics().snapshot();
    assert_eq!((m.mint_fetches_attempted, m.mint_fetches_succeeded), (3, 3));
    assert_eq!((m.fully_enriched, m.partially_enriched), (6, 1));
    assert_eq!(m.cache_hits, 0, "nothing was cached when the batch came in");
    assert_metrics_add_up(&m);
}

#[tokio::test]
async fn cached_mints_are_not_fetched_again() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    adapter.put(&wsol(), standard_mint(9));
    let mut resolver = resolver(&adapter);

    resolver.resolve(vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6)]).await.unwrap();
    let out = resolver
        .resolve(vec![
            checked(1, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
            checked(2, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
            checked(3, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
        ])
        .await
        .unwrap();
    resolver.resolve(vec![checked(4, SPL_TOKEN_PROGRAM_ID, &wsol(), 9)]).await.unwrap();

    assert_eq!(adapter.calls(), vec![vec![usdc()], vec![wsol()]], "only the new mint is fetched");
    assert!(out.iter().all(|t| t.decimals.is_some()));
    let m = resolver.metrics().snapshot();
    assert_eq!(m.cache_hits, 3, "usdc twice in the second batch, wsol in the third");
    assert_eq!(m.mint_fetches_attempted, 2);
    assert_eq!((resolver.mint_cache().hit_count(), resolver.mint_cache().miss_count()), (3, 2));
    assert_metrics_add_up(&m);
}

#[tokio::test]
async fn preloaded_mints_cost_no_rpc() {
    let adapter = MockAdapter::new();
    let mut resolver = resolver(&adapter);
    resolver.mint_cache_mut().insert(
        usdc(),
        MintInfo { decimals: 6, symbol: Some("USDC".into()), name: None, token_program: TokenProgram::Standard },
    );

    let mut transfer = checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 0);
    transfer.decimals = None;
    let out = resolver.resolve(vec![transfer]).await.unwrap();

    assert_eq!(out[0].decimals, Some(6));
    assert!(adapter.calls().is_empty());
    assert_eq!(resolver.metrics().snapshot().cache_hits, 1);
}

#[tokio::test]
async fn non_mints_are_remembered_as_misses() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), token_account()); // exists, but is not a mint
    let mut resolver = resolver(&adapter); // wsol: no such account

    let input = vec![
        checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6),
        checked(1, SPL_TOKEN_PROGRAM_ID, &wsol(), 9),
    ];
    let out = resolver.resolve(input.clone()).await.unwrap();
    resolver.resolve(input.clone()).await.unwrap();

    assert_eq!(out, input, "the instruction's decimals are kept");
    assert_eq!(adapter.calls().len(), 1, "misses are not refetched within the TTL");
    let cache = resolver.mint_cache();
    assert!(cache.is_recently_missing(&usdc()) && cache.is_recently_missing(&wsol()));
    assert!(cache.is_empty());
    let m = resolver.metrics().snapshot();
    assert_eq!((m.mint_fetches_attempted, m.mint_fetches_failed), (2, 2));
    assert_metrics_add_up(&m);
}

#[tokio::test]
async fn misses_are_fetched_again_after_the_ttl() {
    let adapter = MockAdapter::new();
    let mut resolver = AccountResolver::with_miss_ttl(Arc::clone(&adapter) as Arc<dyn ChainAdapter>, Duration::ZERO);
    let mut transfer = checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 0);
    transfer.decimals = None;

    let first = resolver.resolve(vec![transfer.clone()]).await.unwrap();
    assert_eq!(first[0].decimals, None);
    // The mint appears; with a zero TTL the miss has already expired.
    adapter.put(&usdc(), standard_mint(6));
    let second = resolver.resolve(vec![transfer]).await.unwrap();

    assert_eq!(second[0].decimals, Some(6));
    assert_eq!(adapter.calls().len(), 2);
    assert_eq!(resolver.mint_cache().missing_len(), 0, "the insert cleared the miss");
    let m = resolver.metrics().snapshot();
    assert_eq!((m.partially_enriched, m.fully_enriched), (1, 1));
}

#[tokio::test]
async fn the_default_miss_ttl_is_used() {
    let adapter = MockAdapter::new();
    let resolver = resolver(&adapter);
    assert_eq!(resolver.mint_cache().miss_ttl(), DEFAULT_MISS_TTL);
}

// ---------------------------------------------------------------------------
// Failures
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_rpc_failure_leaves_the_batch_as_it_is_and_retries_next_time() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    adapter.set_down(true);
    let mut resolver = resolver(&adapter);
    let mut transfer = checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 0);
    transfer.decimals = None;

    let out = resolver.resolve(vec![transfer.clone()]).await.expect("an RPC failure does not fail the batch");
    assert_eq!(out, vec![transfer.clone()]);
    assert!(!resolver.mint_cache().is_recently_missing(&usdc()), "a failure is not a miss");

    adapter.set_down(false);
    let out = resolver.resolve(vec![transfer]).await.unwrap();
    assert_eq!(out[0].decimals, Some(6));
    assert_eq!(adapter.calls().len(), 2);

    let m = resolver.metrics().snapshot();
    assert_eq!((m.mint_fetches_attempted, m.mint_fetches_succeeded, m.mint_fetches_failed), (2, 1, 1));
    assert_eq!((m.partially_enriched, m.fully_enriched), (1, 1));
    assert_metrics_add_up(&m);
}

#[tokio::test]
async fn an_adapter_answering_the_wrong_length_is_an_error() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    *adapter.short_answer.lock().unwrap() = true;
    let mut resolver = resolver(&adapter);

    let err = resolver.resolve(vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6)]).await.unwrap_err();

    assert!(matches!(err, PrismError::AccountResolveError { .. }), "{err:?}");
    assert!(err.to_string().contains("asked the adapter for 1 accounts, got 0"), "{err}");
    assert!(resolver.mint_cache().is_empty());
    let m = resolver.metrics().snapshot();
    assert_eq!((m.mint_fetches_attempted, m.mint_fetches_failed), (1, 1));
}

#[tokio::test]
async fn metrics_are_shared_through_the_arc() {
    let adapter = MockAdapter::new();
    adapter.put(&usdc(), standard_mint(6));
    let mut resolver = resolver(&adapter);
    let metrics = Arc::clone(resolver.metrics());

    resolver.resolve(vec![checked(0, SPL_TOKEN_PROGRAM_ID, &usdc(), 6)]).await.unwrap();

    assert_eq!(metrics.snapshot(), resolver.metrics().snapshot());
    assert_eq!(metrics.snapshot().total_transfers_processed, 1);
}

// ---------------------------------------------------------------------------
// Decoder → resolver
// ---------------------------------------------------------------------------

/// An instruction row as the normalizer makes it.
fn row(ix: &solana_sdk::instruction::Instruction, index: u32) -> DeepAnaInstruction {
    DeepAnaInstruction {
        slot: 1,
        tx_signature: SIG.to_owned(),
        instruction_index: index,
        inner_instruction_index: None,
        program_id: ix.program_id.to_string(),
        accounts: ix.accounts.iter().map(|meta| meta.pubkey.to_string()).collect(),
        data_base58: bs58::encode(&ix.data).into_string(),
        instruction_type: None,
        decoded_data: None,
    }
}

/// The transfers the real decoder emits for instructions built with the
/// token programs' builders, resolved against their mints.
#[tokio::test]
async fn decoder_output_resolves() {
    let spl = spl_token_interface::id();
    let t22 = spl_token_2022_interface::id();
    let (src, dst, owner) = (Pubkey::new_unique(), Pubkey::new_unique(), Pubkey::new_unique());
    let (usdc_mint, pyusd_mint) = (Pubkey::new_unique(), Pubkey::new_unique());

    let adapter = MockAdapter::new();
    adapter.put(&usdc_mint.to_string(), standard_mint(6));
    adapter.put(&pyusd_mint.to_string(), token_2022_mint(6));

    let instructions = [
        token_2022::transfer_checked(&spl, &src, &usdc_mint, &dst, &owner, &[], 2_500_000, 6).unwrap(),
        token_2022::transfer(&spl, &src, &dst, &owner, &[], 7).unwrap(),
        token_2022::transfer_checked(&t22, &src, &pyusd_mint, &dst, &owner, &[], 1_000_000, 6).unwrap(),
        token_2022::transfer_checked(&spl, &src, &usdc_mint, &dst, &owner, &[], 1, 6).unwrap(),
    ];
    let transfers: Vec<DeepAnaTokenTransfer> = instructions
        .iter()
        .enumerate()
        .map(|(i, ix)| {
            let row = row(ix, i as u32);
            let decoder = SplTokenDecoder::for_program(&row.program_id).unwrap();
            decoder.decode(&row, &row.accounts).unwrap().token_transfer.expect("a transfer")
        })
        .collect();

    let mut resolver = resolver(&adapter);
    let out = resolver.resolve(transfers).await.unwrap();

    assert_eq!(adapter.calls(), vec![vec![usdc_mint.to_string(), pyusd_mint.to_string()]]);
    assert_eq!(out[0].amount_ui(), Some(2.5));
    assert_eq!((out[1].mint_token_address.as_deref(), out[1].decimals), (None, None), "plain Transfer: partial");
    assert_eq!(out[2].amount_ui(), Some(1.0));
    assert_eq!(out[3].decimals, Some(6));
    let m = resolver.metrics().snapshot();
    assert_eq!((m.fully_enriched, m.partially_enriched), (3, 1));
    assert_metrics_add_up(&m);
}

// ---------------------------------------------------------------------------
// Live devnet
// ---------------------------------------------------------------------------

/// Slots below the confirmed tip to start from, so every block exists.
const TIP_MARGIN: u64 = 30;

/// Decodes and resolves the token transfers of recent devnet blocks.
///
/// A `TransferChecked` of a *successful* transaction names a real mint and
/// carries its decimals (the token program checked them), so every one
/// must come out fully enriched, with the decimals unchanged.
#[tokio::test]
async fn live_resolver_enriches_devnet_transfers() {
    let Ok(key) = helius_api_key_from_env() else {
        eprintln!("skipping: {HELIUS_API_KEY_ENV} is not set");
        return;
    };
    let adapter = Arc::new(helius_solana_adapter(&key, Cluster::Devnet).await.expect("helius adapter"));
    let tip = adapter.get_tip(CommitmentLevel::Confirmed).await.expect("devnet tip");
    let limit: usize = std::env::var("PRISM_RESOLVE_BLOCKS").ok().and_then(|v| v.parse().ok()).unwrap_or(20);

    let registry = DecoderRegistry::with_spl_token();
    let normalizer = Normalizer::new();
    let (mut transfers, mut succeeded) = (Vec::new(), std::collections::HashSet::new());
    let (mut blocks, top) = (0, tip.slot_or_height - TIP_MARGIN);
    for slot in (top.saturating_sub(limit as u64 * 4)..=top).rev() {
        let block = match adapter.get_block(slot).await {
            Ok(block) => block,
            Err(PrismError::SlotSkipped { .. }) => continue,
            Err(err) if err.is_retryable() => continue,
            Err(err) => panic!("slot {slot}: {err}"),
        };
        let mut normalized = normalizer.normalize(&block).expect("the devnet block normalizes");
        succeeded.extend(normalized.transactions.iter().filter(|tx| tx.is_success()).map(|tx| tx.tx_signature.clone()));
        transfers.extend(registry.decode_block(&mut normalized));
        blocks += 1;
        if blocks == limit {
            break;
        }
    }

    let input = transfers.clone();
    let mut resolver = AccountResolver::new(adapter as Arc<dyn ChainAdapter>);
    let out = resolver.resolve(transfers).await.expect("resolve");
    let m = resolver.metrics().snapshot();
    println!(
        "\n  {blocks} devnet blocks: {} transfers, {} fully / {} partially enriched; \
         {} mints fetched ({} ok, {} failed), {} cache hits",
        m.total_transfers_processed,
        m.fully_enriched,
        m.partially_enriched,
        m.mint_fetches_attempted,
        m.mint_fetches_succeeded,
        m.mint_fetches_failed,
        m.cache_hits
    );
    assert_metrics_add_up(&m);
    assert_eq!(out.len(), input.len());

    let mints: std::collections::HashSet<&str> =
        input.iter().filter_map(|t| t.mint_token_address.as_deref()).collect();
    assert_eq!(m.mint_fetches_attempted, mints.len() as u64, "each mint fetched once");

    for (before, after) in input.iter().zip(&out) {
        if before.mint_token_address.is_none() || !succeeded.contains(&before.tx_signature) {
            continue;
        }
        assert!(after.decimals.is_some(), "{after:?}");
        assert_eq!(after.decimals, before.decimals, "the token program checked these: {after:?}");
    }
    if input.is_empty() {
        eprintln!("note: no token transfers in the scanned blocks; raise PRISM_RESOLVE_BLOCKS");
    }
}
