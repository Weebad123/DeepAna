//! Tests for `prism::metadata`.
//!
//! What is checked, and how:
//!
//! - **Derivation is right.** `metadata_pda` is compared with three real
//!   mainnet metadata accounts (USDC, BONK, JUP; fixtures in
//!   `tests/fixtures/metadata/`, fetched with `getAccountInfo`). The program
//!   only ever creates a metadata account at the PDA, so an account at that
//!   address, owned by the program, naming the mint, proves the address. It
//!   is also re-derived independently with SHA-256 for many random mints.
//! - **Parsing is right.** The real accounts parse to their known name,
//!   symbol and URI. Strings are kept exactly (only trailing NUL padding
//!   goes), at every cap boundary.
//! - **Every malformed account is rejected for the right reason**: a table
//!   of hand-built accounts, one per rule and per precedence pair, plus
//!   exhaustive sweeps (every truncation of a real account, every wrong key
//!   byte, every single-bit flip of the mint field), and the stats of a live
//!   enricher counting each reason once.
//! - **The enricher cannot slow ingestion.** `hint` is timed against an RPC
//!   that never answers and against a task that is never polled, and a full
//!   ingestor run with such an enricher attached must finish and write every
//!   slot as if it were absent.
//! - **A mint is written once, across restarts**: repeated hints in one run,
//!   a restart over the same directory, leftover temp files, an unreadable
//!   file (disabled, file untouched).
//! - **Metadata survives rewinds**: `ParquetWriter::rollback(0)` leaves the
//!   table byte-identical, and a live ingestor rewind re-ingests every slot
//!   without a second row for any mint.
//! - The failure table: queue full, transient and permanent RPC errors with
//!   backoff and exhaustion, the negative cache and its TTL, repeated flush
//!   failures disabling the enricher, shutdown flushing and dropping.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{AsArray, RecordBatch};
use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use base64::Engine;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prism::adapter::{
    AccountData, ChainAdapter, ChainBlock, ChainBlockData, ChainFamily, ChainTip, CommitmentLevel,
};
use prism::changelog::ChangeLog;
use prism::commitment::{CommitmentTracker, FinalityWatermark};
use prism::decoder::spl_token::SPL_TOKEN_PROGRAM_ID;
use prism::ingestor::{Ingestor, IngestorConfig};
use prism::metadata::parse::{HEAD_LEN, MAX_NAME_LEN, MAX_SYMBOL_LEN, MAX_URI_LEN, METADATA_V1_KEY};
use prism::metadata::writer::{FILE_PREFIX, TABLE_DIR, parse_file_name, table_root};
use prism::metadata::{
    METADATA_PROGRAM, METADATA_PROGRAM_ID, MetadataConfig, MetadataEnricher, MetadataHandle,
    MetadataState, MetadataStats, MetadataWriter, Reject, metadata_pda, parse_metadata,
};
use prism::metrics::{Metrics, Sources, render_prometheus};
use prism::reconcile::Rewind;
use prism::slot_processor::SlotFetcherConfig;
use prism::storage::{ParquetWriter, PartitionKey, Storage, TableKind};
use prism::types::error::{PrismError, PrismResult};
use prism::types::event::ChangeEvent;
use prism::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTokenMetadata, DeepAnaTokenTransfer, UdmRow};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use solana_client::client_error::ClientError;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::EncodedConfirmedBlock;
use tokio_util::sync::CancellationToken;

// ===========================================================================
// Helpers
// ===========================================================================

/// A fresh, unique directory path under the system temp dir. Not created.
fn temp_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "prism-metadata-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// Waits up to 10 s for `done`.
async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
}

/// The mint of a deterministic test token.
fn mint(n: u64) -> Pubkey {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&n.to_le_bytes());
    bytes[31] = 0xA5;
    Pubkey::new_from_array(bytes)
}

fn transient_error() -> PrismError {
    PrismError::rpc(
        None,
        "getMultipleAccounts",
        ClientError::from(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out")),
    )
}

/// Encodes a `MetadataV1` account head followed by `trailer`.
fn encode_metadata(mint: &Pubkey, name: &[u8], symbol: &[u8], uri: &[u8]) -> Vec<u8> {
    let mut data = vec![METADATA_V1_KEY];
    data.extend_from_slice(&[9u8; 32]);
    data.extend_from_slice(mint.as_ref());
    for s in [name, symbol, uri] {
        data.extend_from_slice(&(s.len() as u32).to_le_bytes());
        data.extend_from_slice(s);
    }
    // seller_fee_basis_points and the rest: ignored by the parser.
    data.extend_from_slice(&[0xEE; 16]);
    data
}

/// `s` padded with NULs to `len` bytes, the way the program stores strings.
fn padded(s: &str, len: usize) -> Vec<u8> {
    let mut bytes = s.as_bytes().to_vec();
    bytes.resize(len, 0);
    bytes
}

fn metadata_account(data: Vec<u8>) -> AccountData {
    AccountData {
        owner_program: METADATA_PROGRAM_ID.to_owned(),
        lamports: 5_616_720,
        data,
        executable: false,
        rent_epoch: u64::MAX,
    }
}

fn valid_account(mint: &Pubkey, name: &str, symbol: &str, uri: &str) -> AccountData {
    metadata_account(encode_metadata(
        mint,
        &padded(name, MAX_NAME_LEN),
        &padded(symbol, MAX_SYMBOL_LEN),
        &padded(uri, MAX_URI_LEN),
    ))
}

/// A real mainnet metadata account.
struct Fixture {
    address: &'static str,
    mint: &'static str,
    name: &'static str,
    symbol: &'static str,
    uri: &'static str,
    account: AccountData,
}

fn fixture(file: &str) -> AccountData {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/metadata").join(file);
    let json: Value = serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    let value = &json["result"]["value"];
    AccountData {
        owner_program: value["owner"].as_str().unwrap().to_owned(),
        lamports: value["lamports"].as_u64().unwrap(),
        data: base64::engine::general_purpose::STANDARD
            .decode(value["data"][0].as_str().unwrap())
            .unwrap(),
        executable: value["executable"].as_bool().unwrap(),
        rent_epoch: value["rentEpoch"].as_u64().unwrap_or(0),
    }
}

fn fixtures() -> Vec<Fixture> {
    vec![
        Fixture {
            address: "5x38Kp4hvdomTCnCrAny4UtMUt5rQBdB6px2K1Ui45Wq",
            mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            name: "USD Coin",
            symbol: "USDC",
            uri: "",
            account: fixture("usdc_metadata_account.json"),
        },
        Fixture {
            address: "FDZZbyY9XGpL3CNKUZxLk3wFTTQYL3TkDiDzqxrizcPN",
            mint: "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
            name: "Bonk",
            symbol: "Bonk",
            uri: "https://arweave.net/QPC6FYdUn-3V8ytFNuoCS85S2tHAuiDblh6u3CIZLsw",
            account: fixture("bonk_metadata_account.json"),
        },
        Fixture {
            address: "5pddDLA4taryBwRYGdtKmS9qkwssXD8vHECeNbCZnwUy",
            mint: "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
            name: "Jupiter",
            symbol: "JUP",
            uri: "https://static.jup.ag/jup/metadata.json",
            account: fixture("jup_metadata_account.json"),
        },
    ]
}

/// Every `token_metadata` row on disk, across all files.
fn read_metadata_table(data_dir: &Path) -> Vec<DeepAnaTokenMetadata> {
    let mut rows = Vec::new();
    for path in metadata_files(data_dir) {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch: RecordBatch = batch.unwrap();
            assert_eq!(batch.schema().fields(), DeepAnaTokenMetadata::schema().fields());
            let s = |name: &str| batch.column_by_name(name).unwrap().as_string::<i32>().clone();
            let (mints, accounts, names, symbols, uris) =
                (s("mint_address"), s("metadata_account"), s("name"), s("symbol"), s("uri"));
            let slots = batch.column_by_name("observed_slot").unwrap().as_primitive::<UInt64Type>().clone();
            for i in 0..batch.num_rows() {
                rows.push(DeepAnaTokenMetadata {
                    mint_address: mints.value(i).to_owned(),
                    metadata_account: accounts.value(i).to_owned(),
                    name: names.value(i).to_owned(),
                    symbol: symbols.value(i).to_owned(),
                    uri: uris.value(i).to_owned(),
                    observed_slot: slots.value(i),
                });
            }
        }
    }
    rows
}

/// Every `.parquet` file of the table, sorted.
fn metadata_files(data_dir: &Path) -> Vec<PathBuf> {
    all_files(&table_root(data_dir))
        .into_iter()
        .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
        .collect()
}

fn all_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = fs::read_dir(&dir) else { continue };
        for entry in entries {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

/// Mint → number of rows naming it.
fn mint_counts(rows: &[DeepAnaTokenMetadata]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row.mint_address.clone()).or_insert(0) += 1;
    }
    counts
}

// ===========================================================================
// Fake chain for the enricher
// ===========================================================================

/// Serves metadata accounts by address and records every request.
#[derive(Default)]
struct MetaChain {
    accounts: Mutex<HashMap<String, AccountData>>,
    /// Served one per request before any success.
    failures: Mutex<VecDeque<PrismError>>,
    /// Requests never answer.
    hang: AtomicBool,
    /// Each request takes this long.
    delay_ms: AtomicU64,
    calls: Mutex<Vec<(Instant, Vec<String>)>>,
}

impl MetaChain {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn put(&self, mint: &Pubkey, account: AccountData) {
        self.accounts.lock().unwrap().insert(metadata_pda(mint).to_string(), account);
    }

    fn put_valid(&self, mint: &Pubkey, name: &str) {
        self.put(mint, valid_account(mint, name, &name[..name.len().min(4)], "https://x.test/m.json"));
    }

    fn fail_next(&self, errors: impl IntoIterator<Item = PrismError>) {
        self.failures.lock().unwrap().extend(errors);
    }

    fn calls(&self) -> Vec<(Instant, Vec<String>)> {
        self.calls.lock().unwrap().clone()
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }

    /// Every address ever requested.
    fn requested(&self) -> HashSet<String> {
        self.calls().into_iter().flat_map(|(_, a)| a).collect()
    }
}

#[async_trait]
impl ChainAdapter for MetaChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-metadata-test"
    }

    async fn get_tip(&self, _: CommitmentLevel) -> PrismResult<ChainTip> {
        Err(PrismError::config("get_tip", "not served"))
    }

    async fn get_block(&self, _: u64) -> PrismResult<ChainBlock> {
        Err(PrismError::config("get_block", "not served"))
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        self.calls.lock().unwrap().push((Instant::now(), addresses.to_vec()));
        if self.hang.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        if let Some(err) = self.failures.lock().unwrap().pop_front() {
            return Err(err);
        }
        let accounts = self.accounts.lock().unwrap();
        Ok(addresses.iter().map(|a| accounts.get(a).cloned()).collect())
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

/// Small intervals so a test runs in milliseconds.
fn fast_config() -> MetadataConfig {
    MetadataConfig {
        queue_capacity: 1_000,
        batch_max_mints: 10,
        batch_window: Duration::from_millis(20),
        min_request_interval: Duration::from_millis(10),
        negative_ttl: Duration::from_secs(3_600),
        flush_interval: Duration::from_millis(50),
        flush_max_rows: 1_000,
        max_flush_failures: 10,
        retry_base_delay: Duration::from_millis(20),
        retry_max_delay: Duration::from_millis(80),
        max_fetch_attempts: 5,
        ..MetadataConfig::default()
    }
}

fn start(config: MetadataConfig, chain: &Arc<MetaChain>, dir: &Path) -> MetadataHandle {
    MetadataEnricher::start(
        config,
        Arc::clone(chain) as Arc<dyn ChainAdapter>,
        dir,
        Metrics::new(),
        CancellationToken::new(),
    )
}

// ===========================================================================
// Derivation
// ===========================================================================

#[test]
fn pda_matches_real_mainnet_metadata_accounts() {
    for f in fixtures() {
        let mint = Pubkey::from_str(f.mint).unwrap();
        assert_eq!(metadata_pda(&mint).to_string(), f.address, "{}", f.name);
        // The chain put an account owned by the program, for this very mint,
        // at that address: only the program can, and only at the PDA.
        assert_eq!(f.account.owner_program, METADATA_PROGRAM_ID);
        assert_eq!(&f.account.data[33..65], mint.as_ref());
    }
}

/// `find_program_address` re-done by hand: the first bump from 255 down
/// whose SHA-256 is off the curve.
fn manual_pda(mint: &Pubkey) -> Pubkey {
    for bump in (0..=255u8).rev() {
        let mut hasher = Sha256::new();
        hasher.update(b"metadata");
        hasher.update(METADATA_PROGRAM.as_ref());
        hasher.update(mint.as_ref());
        hasher.update([bump]);
        hasher.update(METADATA_PROGRAM.as_ref());
        hasher.update(b"ProgramDerivedAddress");
        let candidate = Pubkey::new_from_array(hasher.finalize().into());
        if !candidate.is_on_curve() {
            return candidate;
        }
    }
    panic!("no bump");
}

#[test]
fn pda_matches_an_independent_derivation_for_many_mints() {
    for n in 0..300 {
        let m = if n % 2 == 0 { mint(n) } else { Pubkey::new_unique() };
        assert_eq!(metadata_pda(&m), manual_pda(&m), "mint {m}");
    }
}

#[test]
fn pda_depends_on_the_mint_and_is_never_a_wallet_address() {
    let derived: HashSet<Pubkey> = (0..500).map(|n| metadata_pda(&mint(n))).collect();
    assert_eq!(derived.len(), 500);
    assert!(derived.iter().all(|pda| !pda.is_on_curve()));
    assert_eq!(METADATA_PROGRAM.to_string(), "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
}

// ===========================================================================
// Parsing: valid accounts
// ===========================================================================

#[test]
fn real_accounts_parse_to_their_known_metadata() {
    for f in fixtures() {
        let mint = Pubkey::from_str(f.mint).unwrap();
        let meta = parse_metadata(&f.account, &mint).unwrap_or_else(|r| panic!("{}: {r}", f.name));
        assert_eq!(meta.mint, mint);
        assert_eq!(meta.name, f.name);
        assert_eq!(meta.symbol, f.symbol);
        assert_eq!(meta.uri, f.uri);
        assert_eq!(meta.update_authority, Pubkey::new_from_array(f.account.data[1..33].try_into().unwrap()));
    }
}

#[test]
fn strings_are_kept_exactly_except_trailing_nul_padding() {
    let m = mint(1);
    for (name, symbol, uri) in [
        ("  Spaced  Name ", " s ", " uri "),
        ("MiXeD cAsE", "AbC", "HTTPS://X"),
        ("Café ☕ 🚀", "€$", "ipfs://ü"),
        ("tab\there\nnewline", "a\u{0}b", "x\u{0}\u{0}y"),
        ("", "", ""),
    ] {
        let account = valid_account(&m, name, symbol, uri);
        let meta = parse_metadata(&account, &m).unwrap();
        assert_eq!((meta.name.as_str(), meta.symbol.as_str(), meta.uri.as_str()), (name, symbol, uri));
    }
    // Unpadded strings come back as stored too.
    let account = metadata_account(encode_metadata(&m, b"N ", b" S", b"U"));
    let meta = parse_metadata(&account, &m).unwrap();
    assert_eq!((meta.name.as_str(), meta.symbol.as_str(), meta.uri.as_str()), ("N ", " S", "U"));
}

#[test]
fn every_cap_is_inclusive() {
    let m = mint(2);
    let at_cap = metadata_account(encode_metadata(
        &m,
        &[b'n'; MAX_NAME_LEN],
        &[b's'; MAX_SYMBOL_LEN],
        &[b'u'; MAX_URI_LEN],
    ));
    let meta = parse_metadata(&at_cap, &m).unwrap();
    assert_eq!((meta.name.len(), meta.symbol.len(), meta.uri.len()), (32, 10, 200));
    // A multi-byte character that ends exactly at the cap is fine.
    let name = "é".repeat(16);
    let meta = parse_metadata(&metadata_account(encode_metadata(&m, name.as_bytes(), b"", b"")), &m).unwrap();
    assert_eq!(meta.name, name);
}

#[test]
fn bytes_after_the_uri_are_ignored() {
    let m = mint(3);
    let mut data = encode_metadata(&m, b"A", b"B", b"C");
    let end = HEAD_LEN + 4 + 1 + 4 + 1 + 4 + 1;
    data.truncate(end);
    assert!(parse_metadata(&metadata_account(data.clone()), &m).is_ok(), "nothing after uri");
    data.extend_from_slice(&[0xFF; 600]);
    assert!(parse_metadata(&metadata_account(data), &m).is_ok(), "garbage after uri");
}

// ===========================================================================
// Parsing: every malformed account, for the right reason
// ===========================================================================

#[test]
fn each_rule_rejects_for_its_own_reason() {
    let m = mint(10);
    let other = mint(11);
    let valid = encode_metadata(&m, b"Name", b"SYM", b"https://u");
    let with = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut d = valid.clone();
        f(&mut d);
        metadata_account(d)
    };
    let owned_by = |owner: &str| AccountData { owner_program: owner.to_owned(), ..metadata_account(valid.clone()) };
    let name_len_at = HEAD_LEN;
    let symbol_len_at = HEAD_LEN + 4 + 4;
    let uri_len_at = symbol_len_at + 4 + 3;
    let set_len = |d: &mut Vec<u8>, at: usize, len: u32| d[at..at + 4].copy_from_slice(&len.to_le_bytes());

    let cases: Vec<(&str, AccountData, Reject)> = vec![
        // 1. owner
        ("owned by SPL Token", owned_by(SPL_TOKEN_PROGRAM_ID), Reject::WrongOwner),
        ("owned by the system program", owned_by("11111111111111111111111111111111"), Reject::WrongOwner),
        ("no owner", owned_by(""), Reject::WrongOwner),
        ("owner differs by case", owned_by(&METADATA_PROGRAM_ID.to_uppercase()), Reject::WrongOwner),
        // 2. head length
        ("empty data", metadata_account(vec![]), Reject::Truncated),
        ("key byte only", metadata_account(vec![METADATA_V1_KEY]), Reject::Truncated),
        ("one byte short of the head", with(&|d| d.truncate(HEAD_LEN - 1)), Reject::Truncated),
        // 3. key
        ("uninitialized key", with(&|d| d[0] = 0), Reject::WrongKey),
        ("edition key", with(&|d| d[0] = 1), Reject::WrongKey),
        ("master edition v2 key", with(&|d| d[0] = 6), Reject::WrongKey),
        ("key 255", with(&|d| d[0] = 255), Reject::WrongKey),
        // 4. mint
        ("another mint", with(&|d| d[33..65].copy_from_slice(other.as_ref())), Reject::MintMismatch),
        ("zero mint", with(&|d| d[33..65].fill(0)), Reject::MintMismatch),
        // 5. strings: missing prefix
        ("head only, no name prefix", with(&|d| d.truncate(HEAD_LEN)), Reject::Truncated),
        ("half a name prefix", with(&|d| d.truncate(HEAD_LEN + 2)), Reject::Truncated),
        // 5. strings: too long
        ("name 33 bytes", with(&|d| set_len(d, name_len_at, 33)), Reject::TooLong),
        ("name u32::MAX", with(&|d| set_len(d, name_len_at, u32::MAX)), Reject::TooLong),
        ("symbol 11 bytes", with(&|d| set_len(d, symbol_len_at, 11)), Reject::TooLong),
        ("uri 201 bytes", with(&|d| set_len(d, uri_len_at, 201)), Reject::TooLong),
        // 5. strings: bytes missing
        ("name runs past the end", with(&|d| d.truncate(name_len_at + 4 + 2)), Reject::Truncated),
        ("uri runs past the end", with(&|d| d.truncate(uri_len_at + 4 + 3)), Reject::Truncated),
        ("uri prefix missing", with(&|d| d.truncate(uri_len_at + 1)), Reject::Truncated),
        ("name claims 20 of 9 bytes left", metadata_account({
            let mut d = valid.clone();
            set_len(&mut d, name_len_at, 20);
            d.truncate(name_len_at + 4 + 9);
            d
        }), Reject::Truncated),
        // 5. strings: UTF-8
        ("0xFF in name", with(&|d| d[name_len_at + 4] = 0xFF), Reject::BadUtf8),
        ("lone continuation byte in symbol", with(&|d| d[symbol_len_at + 4] = 0x80), Reject::BadUtf8),
        ("overlong NUL in uri", with(&|d| d[uri_len_at + 4..uri_len_at + 6].copy_from_slice(&[0xC0, 0x80])), Reject::BadUtf8),
        ("UTF-16 surrogate in name", with(&|d| d[name_len_at + 4..name_len_at + 7].copy_from_slice(&[0xED, 0xA0, 0x80])), Reject::BadUtf8),
        ("cut multi-byte char before padding", metadata_account(encode_metadata(&m, &[b'a', 0xC3, 0, 0], b"", b"")), Reject::BadUtf8),
        // Precedence: the earlier rule wins.
        ("wrong owner beats garbage", AccountData { data: vec![0xFF; 3], ..owned_by(SPL_TOKEN_PROGRAM_ID) }, Reject::WrongOwner),
        ("wrong key beats wrong mint", with(&|d| { d[0] = 6; d[33..65].fill(1) }), Reject::WrongKey),
        ("wrong mint beats bad strings", with(&|d| { d[33..65].fill(1); set_len(d, name_len_at, u32::MAX) }), Reject::MintMismatch),
        ("too long beats truncated", with(&|d| { set_len(d, name_len_at, 33); d.truncate(name_len_at + 5) }), Reject::TooLong),
        ("bad name beats bad symbol", with(&|d| { d[name_len_at + 4] = 0xFF; set_len(d, symbol_len_at, 99) }), Reject::BadUtf8),
    ];
    // Every reason is exercised.
    let reasons: BTreeSet<Reject> = cases.iter().map(|(_, _, r)| *r).collect();
    assert_eq!(reasons.len(), Reject::COUNT);
    for (what, account, expected) in cases {
        assert_eq!(parse_metadata(&account, &m), Err(expected), "{what}");
    }
}

#[test]
fn every_truncation_of_a_real_account_is_truncated() {
    for f in fixtures() {
        let mint = Pubkey::from_str(f.mint).unwrap();
        let end_of_uri = HEAD_LEN + 3 * 4 + MAX_NAME_LEN + MAX_SYMBOL_LEN + MAX_URI_LEN;
        for len in 0..end_of_uri {
            let cut = AccountData { data: f.account.data[..len].to_vec(), ..f.account.clone() };
            assert_eq!(parse_metadata(&cut, &mint), Err(Reject::Truncated), "{} cut to {len}", f.name);
        }
        let exact = AccountData { data: f.account.data[..end_of_uri].to_vec(), ..f.account.clone() };
        assert!(parse_metadata(&exact, &mint).is_ok(), "{} cut after the uri", f.name);
    }
}

#[test]
fn every_wrong_key_byte_is_wrong_key() {
    let f = &fixtures()[0];
    let mint = Pubkey::from_str(f.mint).unwrap();
    for key in (0..=255u8).filter(|k| *k != METADATA_V1_KEY) {
        let mut account = f.account.clone();
        account.data[0] = key;
        assert_eq!(parse_metadata(&account, &mint), Err(Reject::WrongKey), "key {key}");
    }
}

#[test]
fn every_bit_flip_in_the_mint_field_is_a_mismatch() {
    let f = &fixtures()[1];
    let bonk = Pubkey::from_str(f.mint).unwrap();
    for bit in 0..256 {
        let mut account = f.account.clone();
        account.data[33 + bit / 8] ^= 1 << (bit % 8);
        assert_eq!(parse_metadata(&account, &bonk), Err(Reject::MintMismatch), "bit {bit}");
    }
    // And the right account asked for the wrong mint.
    assert_eq!(parse_metadata(&f.account, &mint(99)), Err(Reject::MintMismatch));
}

#[tokio::test]
async fn a_live_enricher_counts_each_rejection_by_reason_and_writes_only_valid_rows() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let good = mint(100);
    chain.put_valid(&good, "Good");
    let bad: Vec<(Pubkey, AccountData, Reject)> = vec![
        (mint(101), AccountData { owner_program: SPL_TOKEN_PROGRAM_ID.into(), ..valid_account(&mint(101), "x", "x", "x") }, Reject::WrongOwner),
        (mint(102), metadata_account(vec![METADATA_V1_KEY; 10]), Reject::Truncated),
        (mint(103), { let mut a = valid_account(&mint(103), "x", "x", "x"); a.data[0] = 6; a }, Reject::WrongKey),
        (mint(104), valid_account(&mint(999), "x", "x", "x"), Reject::MintMismatch),
        (mint(105), metadata_account(encode_metadata(&mint(105), &[b'n'; 33], b"", b"")), Reject::TooLong),
        (mint(106), metadata_account(encode_metadata(&mint(106), &[0xFF], b"", b"")), Reject::BadUtf8),
    ];
    for (m, account, _) in &bad {
        chain.put(m, account.clone());
    }
    let missing = mint(107);

    let handle = start(fast_config(), &chain, &dir);
    handle.hint(good, 10);
    handle.hint(missing, 11);
    for (m, _, _) in &bad {
        handle.hint(*m, 12);
    }
    wait_for("every mint looked up", || {
        let s = handle.stats();
        s.not_found + s.rejected.values().sum::<u64>() + handle.stats().known_mints == 8
    })
    .await;
    handle.shutdown().await;

    let stats = handle.stats();
    for reason in Reject::ALL {
        assert_eq!(stats.rejected[reason.as_str()], 1, "{reason}");
    }
    assert_eq!(stats.not_found, 1);
    assert_eq!(stats.accounts_fetched, 7);
    assert_eq!(stats.written, 1);
    let rows = read_metadata_table(&dir);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].mint_address, good.to_string());
    assert_eq!(rows[0].metadata_account, metadata_pda(&good).to_string());
    assert_eq!((rows[0].name.as_str(), rows[0].symbol.as_str()), ("Good", "Good"));
    assert_eq!(rows[0].observed_slot, 10);
}

// ===========================================================================
// The enricher cannot slow ingestion
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hint_returns_at_once_while_the_rpc_never_answers() {
    let chain = MetaChain::new();
    chain.hang.store(true, Ordering::SeqCst);
    let dir = temp_dir();
    let config = MetadataConfig { queue_capacity: 64, ..fast_config() };
    let handle = start(config, &chain, &dir);
    // Let the task get stuck in its first request.
    for n in 0..20 {
        handle.hint(mint(n), 1);
    }
    wait_for("the first request to hang", || chain.call_count() == 1).await;

    const HINTS: u64 = 200_000;
    let mut slowest = Duration::ZERO;
    let started = Instant::now();
    for n in 0..HINTS {
        let one = Instant::now();
        handle.hint(mint(1_000 + n), n);
        slowest = slowest.max(one.elapsed());
    }
    let total = started.elapsed();

    // ~ tens of milliseconds in practice; the bound leaves room for a busy
    // machine but not for a single wait on the RPC or the task.
    assert!(total < Duration::from_secs(2), "{HINTS} hints took {total:?}");
    assert!(slowest < Duration::from_millis(250), "slowest hint took {slowest:?}");
    let stats = handle.stats();
    assert_eq!(stats.hints_received, HINTS + 20);
    assert!(stats.hints_dropped_queue_full >= HINTS - 64, "{stats:?}");
    assert_eq!(chain.call_count(), 1, "nothing else was requested");

    // Shutdown does not wait on the hung request either.
    tokio::time::timeout(Duration::from_secs(2), handle.shutdown()).await.expect("shutdown is prompt");
    assert_eq!(handle.state(), MetadataState::Draining);
}

#[tokio::test(flavor = "current_thread")]
async fn hint_needs_nothing_from_the_task_or_the_runtime() {
    // On a single-threaded runtime the task cannot run until this test
    // yields, which it never does while hinting: every hint must still
    // return, and the overflow be dropped, not waited for.
    let chain = MetaChain::new();
    let dir = temp_dir();
    let config = MetadataConfig { queue_capacity: 16, ..fast_config() };
    let handle = start(config, &chain, &dir);
    let started = Instant::now();
    for n in 0..10_000 {
        handle.hint(mint(n), n);
    }
    assert!(started.elapsed() < Duration::from_secs(1));
    let stats = handle.stats();
    assert_eq!(stats.hints_received, 10_000);
    assert_eq!(stats.hints_dropped_queue_full, 10_000 - 16);
    assert_eq!(chain.call_count(), 0);
    handle.shutdown().await;
}

// -- A chain that ingestion can run on --------------------------------------

/// Serves one block per slot holding one `TransferChecked` of a mint unique
/// to that slot. Never has a mint account (decimals come from the
/// instruction).
struct TransferChain {
    tip: u64,
}

fn transfer_mint(slot: u64) -> Pubkey {
    mint(50_000 + slot)
}

fn transfer_block(slot: u64) -> ChainBlock {
    let mut data = vec![12u8];
    data.extend(1_000u64.to_le_bytes());
    data.push(6);
    let tx = json!({
        "transaction": {
            "signatures": [format!("xfer-{slot}")],
            "message": {
                "header": {"numRequiredSignatures": 1, "numReadonlySignedAccounts": 0, "numReadonlyUnsignedAccounts": 2},
                "accountKeys": [
                    "11111111111111111111111111111112",
                    "Src1111111111111111111111111111111111111111",
                    transfer_mint(slot).to_string(),
                    "Dst1111111111111111111111111111111111111111",
                    SPL_TOKEN_PROGRAM_ID,
                ],
                "recentBlockhash": "11111111111111111111111111111111",
                "instructions": [{"programIdIndex": 4, "accounts": [1, 2, 3, 0], "data": bs58::encode(data).into_string(), "stackHeight": null}],
            }
        },
        "meta": {
            "err": null, "status": {"Ok": null}, "fee": 5_000,
            "preBalances": [], "postBalances": [], "computeUnitsConsumed": 1_000,
        },
    });
    let raw: EncodedConfirmedBlock = serde_json::from_value(json!({
        "previousBlockhash": format!("hash-{}", slot.saturating_sub(1)),
        "blockhash": format!("hash-{slot}"),
        "parentSlot": slot.saturating_sub(1),
        "transactions": [tx],
        "rewards": [],
        "blockTime": 1_700_000_000 + slot as i64,
        "blockHeight": slot,
    }))
    .unwrap();
    ChainBlock {
        slot_or_height: slot,
        block_id: format!("hash-{slot}"),
        parent_id: format!("hash-{}", slot.saturating_sub(1)),
        timestamp: raw.block_time,
        raw: ChainBlockData::Solana(Box::new(raw)),
    }
}

#[async_trait]
impl ChainAdapter for TransferChain {
    fn chain_family(&self) -> ChainFamily {
        ChainFamily::Solana
    }

    fn chain_id(&self) -> &str {
        "solana-transfer-test"
    }

    async fn get_tip(&self, commitment: CommitmentLevel) -> PrismResult<ChainTip> {
        Ok(ChainTip { slot_or_height: self.tip, block_id: format!("hash-{}", self.tip), commitment })
    }

    async fn get_block(&self, slot: u64) -> PrismResult<ChainBlock> {
        Ok(transfer_block(slot))
    }

    async fn get_accounts(&self, addresses: &[String]) -> PrismResult<Vec<Option<AccountData>>> {
        Ok(vec![None; addresses.len()])
    }

    async fn health_check(&self) -> PrismResult<()> {
        Ok(())
    }
}

#[derive(Default)]
struct CountingChangeLog {
    new_events: Mutex<Vec<u64>>,
}

impl ChangeLog for CountingChangeLog {
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64> {
        let mut news = self.new_events.lock().unwrap();
        if let ChangeEvent::New { slot, .. } = event {
            news.push(slot);
        }
        Ok(news.len() as u64)
    }

    fn flush(&self) -> PrismResult<()> {
        Ok(())
    }
}

fn ingest_config(dir: &Path) -> IngestorConfig {
    IngestorConfig {
        tip_poll_interval: Duration::from_millis(5),
        flush_interval: Duration::from_millis(20),
        flush_max_slots: 10,
        fetcher: SlotFetcherConfig { max_retries: 2, base_delay_ms: 1, max_delay_ms: 2, request_timeout_ms: 1_000 },
        ..IngestorConfig::new(dir)
    }
}

fn new_ingestor(tip: u64, config: IngestorConfig, changelog: &Arc<CountingChangeLog>) -> Ingestor {
    Ingestor::new(
        Arc::new(TransferChain { tip }),
        Arc::new(CommitmentTracker::from_watermark(Arc::new(FinalityWatermark::new()))),
        Arc::clone(changelog) as Arc<dyn ChangeLog>,
        config,
    )
    .unwrap()
}

fn table_slots(dir: &Path, table: TableKind) -> Vec<u64> {
    let mut slots = Vec::new();
    for path in all_files(&PartitionKey::table_root(dir, table)) {
        if path.extension().is_none_or(|e| e != "parquet") {
            continue;
        }
        for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap().build().unwrap() {
            let batch = batch.unwrap();
            slots.extend(batch.column_by_name("slot").unwrap().as_primitive::<UInt64Type>().values().iter());
        }
    }
    slots.sort_unstable();
    slots
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingestion_finishes_unhindered_with_a_stuck_enricher() {
    const LAST: u64 = 300;
    let range = |dir: &Path| IngestorConfig { start_slot: Some(1), end_slot: Some(LAST), ..ingest_config(dir) };

    // Baseline: no enricher.
    let base_dir = temp_dir();
    let changelog = Arc::new(CountingChangeLog::default());
    let started = Instant::now();
    new_ingestor(LAST, range(&base_dir), &changelog).run().await.unwrap();
    let baseline = started.elapsed();

    // Same run, with an enricher whose RPC never answers and whose queue
    // overflows almost at once.
    let dir = temp_dir();
    let chain = MetaChain::new();
    chain.hang.store(true, Ordering::SeqCst);
    let config = MetadataConfig { queue_capacity: 8, batch_max_mints: 4, ..fast_config() };
    let handle = start(config, &chain, &dir);
    let changelog = Arc::new(CountingChangeLog::default());
    let ingestor = new_ingestor(LAST, range(&dir), &changelog).with_metadata(handle.clone());
    let started = Instant::now();
    let report = tokio::time::timeout(Duration::from_secs(30), ingestor.run())
        .await
        .expect("ingestion must not wait on the enricher")
        .unwrap();
    let with_enricher = started.elapsed();

    assert_eq!(report.slots_processed, LAST);
    let expected: Vec<u64> = (1..=LAST).collect();
    assert_eq!(table_slots(&dir, TableKind::Slots), expected);
    assert_eq!(table_slots(&dir, TableKind::TokenTransfers), expected);
    // Every flushed mint was hinted, once; most were dropped, none waited.
    let stats = handle.stats();
    assert_eq!(stats.hints_received, LAST);
    assert!(stats.hints_dropped_queue_full > 0, "{stats:?}");
    assert_eq!(chain.call_count(), 1);
    // Generous: a hung enricher must not turn into a hung or crawling
    // pipeline. Both runs are dominated by the same fetch/flush work.
    assert!(
        with_enricher < baseline * 4 + Duration::from_secs(2),
        "baseline {baseline:?}, with a stuck enricher {with_enricher:?}"
    );
    tokio::time::timeout(Duration::from_secs(2), handle.shutdown()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_flushed_transfer_mint_is_written_at_its_slot() {
    let dir = temp_dir();
    let chain = MetaChain::new();
    for slot in 1..=20 {
        chain.put_valid(&transfer_mint(slot), &format!("T{slot}"));
    }
    let handle = start(fast_config(), &chain, &dir);
    let changelog = Arc::new(CountingChangeLog::default());
    let config = IngestorConfig { start_slot: Some(1), end_slot: Some(20), ..ingest_config(&dir) };
    new_ingestor(20, config, &changelog).with_metadata(handle.clone()).run().await.unwrap();
    wait_for("every mint written", || handle.stats().known_mints == 20).await;
    handle.shutdown().await;

    let rows = read_metadata_table(&dir);
    assert_eq!(rows.len(), 20);
    for row in rows {
        let slot = row.name[1..].parse::<u64>().unwrap();
        assert_eq!(row.mint_address, transfer_mint(slot).to_string());
        assert_eq!(row.observed_slot, slot, "observed at the transfer's slot");
    }
}

// ===========================================================================
// Write-once, across restarts
// ===========================================================================

#[tokio::test]
async fn repeated_hints_in_one_run_write_one_row() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(1);
    chain.put_valid(&m, "Once");
    // A long flush interval: the row is still buffered for the repeats.
    let handle = start(MetadataConfig { flush_interval: Duration::from_secs(3_600), ..fast_config() }, &chain, &dir);
    handle.hint(m, 5);
    wait_for("the mint buffered", || handle.stats().known_mints == 1).await;
    for slot in 6..56 {
        handle.hint(m, slot);
    }
    wait_for("the repeats seen", || handle.stats().hints_known == 50).await;
    handle.shutdown().await;

    let rows = read_metadata_table(&dir);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].observed_slot, 5, "the snapshot is never updated");
    assert_eq!(chain.call_count(), 1);
}

#[tokio::test]
async fn hints_for_a_mint_already_queued_merge_into_one_request() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(2);
    chain.put_valid(&m, "Merge");
    let handle = start(MetadataConfig { batch_window: Duration::from_millis(100), ..fast_config() }, &chain, &dir);
    for slot in [7, 9, 8] {
        handle.hint(m, slot);
    }
    wait_for("written", || handle.stats().known_mints == 1).await;
    handle.shutdown().await;
    assert_eq!(chain.requested().len(), 1);
    assert_eq!(chain.call_count(), 1);
    assert_eq!(read_metadata_table(&dir)[0].observed_slot, 9, "the latest sighting before the read");
}

#[tokio::test]
async fn a_restart_never_writes_a_known_mint_again() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let first: Vec<Pubkey> = (0..25).map(mint).collect();
    for (i, m) in first.iter().enumerate() {
        chain.put_valid(m, &format!("First{i}"));
    }

    // Run 1.
    let handle = start(fast_config(), &chain, &dir);
    for m in &first {
        handle.hint(*m, 100);
    }
    wait_for("run 1 buffered", || handle.stats().known_mints == 25).await;
    handle.shutdown().await;
    assert_eq!(handle.stats().written, 25);
    let files_after_run_1 = metadata_files(&dir);
    assert!(!files_after_run_1.is_empty());

    // The chain's metadata changes; the stored snapshot must not.
    for m in &first {
        chain.put_valid(m, "Changed");
    }
    let new_mint = mint(500);
    chain.put_valid(&new_mint, "Second");
    let requested_before = chain.requested();

    // Run 2, same directory.
    let handle = start(fast_config(), &chain, &dir);
    // Hints sent while the startup scan runs simply wait in the queue.
    assert_eq!(handle.state(), MetadataState::Running);
    for round in 0..3 {
        for m in &first {
            handle.hint(*m, 200 + round);
        }
    }
    handle.hint(new_mint, 300);
    wait_for("run 2 sees all hints", || {
        let s = handle.stats();
        s.hints_known == 75 && s.known_mints == 26
    })
    .await;
    handle.shutdown().await;

    let rows = read_metadata_table(&dir);
    let counts = mint_counts(&rows);
    assert_eq!(counts.len(), 26);
    assert!(counts.values().all(|n| *n == 1), "{counts:?}");
    assert!(rows.iter().all(|r| r.name != "Changed"));
    // Run 2 asked the RPC for the new mint only.
    let asked_in_run_2: HashSet<String> = chain.requested().difference(&requested_before).cloned().collect();
    assert_eq!(asked_in_run_2, HashSet::from([metadata_pda(&new_mint).to_string()]));
    // Run 1's files are untouched.
    for path in files_after_run_1 {
        assert!(path.exists());
    }

    // Run 3: the scan alone knows every mint.
    let (writer, report) = MetadataWriter::open(&dir, 1_000).unwrap();
    assert_eq!((writer.known_len(), report.rows, report.duplicate_rows), (26, 26, 0));
}

#[tokio::test]
async fn files_are_named_dated_and_never_slot_ranges() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.put_valid(&mint(1), "Named");
    let handle = start(fast_config(), &chain, &dir);
    handle.hint(mint(1), 1);
    wait_for("written", || handle.stats().files_written == 1).await;
    handle.shutdown().await;

    let files = metadata_files(&dir);
    assert_eq!(files.len(), 1);
    let path = &files[0];
    let name = path.file_name().unwrap().to_str().unwrap();
    let (ms, _seq) = parse_file_name(name).expect("metadata_{unix_ms}_{seq}.parquet");
    assert!(name.starts_with(&format!("{FILE_PREFIX}_")));
    let written_at = chrono::DateTime::from_timestamp_millis(ms).unwrap();
    assert!((chrono::Utc::now() - written_at).num_seconds().abs() < 60);
    let date_dir = path.parent().unwrap();
    assert_eq!(date_dir.file_name().unwrap().to_str().unwrap(), format!("date={}", written_at.format("%Y-%m-%d")));
    assert_eq!(date_dir.parent().unwrap(), dir.join("solana").join(TABLE_DIR));
    assert_eq!(PartitionKey::parse_file_name(name), None);
    // No temp file is left behind.
    assert_eq!(all_files(&table_root(&dir)), files);
}

#[test]
fn two_flushes_in_one_millisecond_get_distinct_files() {
    let dir = temp_dir();
    let (mut writer, _) = MetadataWriter::open(&dir, 10).unwrap();
    let now = chrono::Utc::now();
    let mut paths = Vec::new();
    for n in 0..3 {
        let m = mint(n);
        let row = DeepAnaTokenMetadata { mint_address: m.to_string(), ..Default::default() };
        writer.buffer(m, row);
        paths.push(writer.flush_at(now).unwrap().unwrap().path);
    }
    let unique: HashSet<_> = paths.iter().collect();
    assert_eq!(unique.len(), 3);
    assert_eq!(read_metadata_table(&dir).len(), 3);
}

#[test]
fn the_writer_itself_refuses_a_known_mint_and_a_full_set() {
    use prism::metadata::BufferOutcome;
    let dir = temp_dir();
    let (mut writer, _) = MetadataWriter::open(&dir, 2).unwrap();
    let row = |m: Pubkey| DeepAnaTokenMetadata { mint_address: m.to_string(), ..Default::default() };
    assert_eq!(writer.buffer(mint(1), row(mint(1))), BufferOutcome::Buffered);
    assert_eq!(writer.buffer(mint(1), row(mint(1))), BufferOutcome::Known, "known while buffered");
    writer.flush().unwrap();
    assert_eq!(writer.buffer(mint(1), row(mint(1))), BufferOutcome::Known, "known once on disk");
    assert_eq!(writer.buffer(mint(2), row(mint(2))), BufferOutcome::Buffered);
    assert_eq!(writer.buffer(mint(3), row(mint(3))), BufferOutcome::Full);
    assert!(!writer.is_known(&mint(3)));
    writer.flush().unwrap();
    assert_eq!(mint_counts(&read_metadata_table(&dir)).len(), 2);
}

#[tokio::test]
async fn a_full_known_set_drops_new_mints_without_fetching_them() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    for n in 0..3 {
        chain.put_valid(&mint(n), "Cap");
    }
    let handle = start(MetadataConfig { max_known_mints: 2, ..fast_config() }, &chain, &dir);
    handle.hint(mint(0), 1);
    handle.hint(mint(1), 1);
    wait_for("two known", || handle.stats().known_mints == 2).await;
    handle.hint(mint(2), 1);
    wait_for("refused", || handle.stats().known_set_full == 1).await;
    handle.shutdown().await;
    assert!(!chain.requested().contains(&metadata_pda(&mint(2)).to_string()));
    assert_eq!(read_metadata_table(&dir).len(), 2);
}

#[tokio::test]
async fn leftover_temp_files_are_removed_and_their_mints_fetched_again() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(7);
    chain.put_valid(&m, "Torn");
    // A crash mid-write leaves only a `.tmp`.
    let date_dir = table_root(&dir).join("date=2026-01-01");
    fs::create_dir_all(&date_dir).unwrap();
    let torn = date_dir.join("metadata_1767225600000_0.parquet.tmp");
    fs::write(&torn, b"PAR1 half a file").unwrap();

    let handle = start(fast_config(), &chain, &dir);
    handle.hint(m, 1);
    wait_for("written", || handle.stats().files_written == 1).await;
    handle.shutdown().await;
    assert!(!torn.exists());
    assert_eq!(handle.state(), MetadataState::Draining);
    assert_eq!(mint_counts(&read_metadata_table(&dir)).get(&m.to_string()), Some(&1));
}

#[tokio::test]
async fn an_unreadable_file_at_startup_disables_the_enricher_and_is_left_alone() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let date_dir = table_root(&dir).join("date=2026-01-01");
    fs::create_dir_all(&date_dir).unwrap();
    let corrupt = date_dir.join("metadata_1767225600000_0.parquet");
    fs::write(&corrupt, b"not parquet at all").unwrap();

    let handle = start(fast_config(), &chain, &dir);
    wait_for("disabled", || matches!(handle.state(), MetadataState::Disabled { .. })).await;
    let MetadataState::Disabled { reason } = handle.state() else { unreachable!() };
    assert!(reason.contains("metadata_1767225600000_0.parquet"), "{reason}");
    handle.hint(mint(1), 1);
    assert_eq!(handle.stats().hints_dropped_closed, 1);
    handle.shutdown().await;
    assert_eq!(chain.call_count(), 0);
    assert_eq!(fs::read(&corrupt).unwrap(), b"not parquet at all");
    assert!(matches!(handle.state(), MetadataState::Disabled { .. }), "shutdown keeps the reason");
}

#[tokio::test]
async fn a_file_with_a_foreign_schema_also_disables_it() {
    let dir = temp_dir();
    // A slots-table file dropped into the metadata directory.
    let date_dir = table_root(&dir).join("date=2026-01-01");
    fs::create_dir_all(&date_dir).unwrap();
    let batch = DeepAnaSlot::to_record_batch(&[DeepAnaSlot { slot: 1, ..Default::default() }]).unwrap();
    let file = File::create(date_dir.join("metadata_1_0.parquet")).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let err = MetadataWriter::open(&dir, 10).unwrap_err();
    assert!(err.to_string().contains("schema"), "{err}");
}

// ===========================================================================
// Rewinds
// ===========================================================================

#[tokio::test]
async fn rollback_to_slot_zero_leaves_the_metadata_table_byte_identical() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    for n in 0..5 {
        chain.put_valid(&mint(n), "Kept");
    }
    let handle = start(fast_config(), &chain, &dir);
    for n in 0..5 {
        handle.hint(mint(n), 1_000 + n);
    }
    wait_for("written", || handle.stats().written == 5).await;
    handle.shutdown().await;

    // Ingestion files next to it, some below and some above the point.
    let mut writer = ParquetWriter::new(&dir);
    for slot in [10, 11, 12] {
        let s = DeepAnaSlot { slot, blockhash: format!("h{slot}"), block_time: Some(1_700_000_000), ..Default::default() };
        let t = DeepAnaTokenTransfer { slot, mint_token_address: Some(mint(0).to_string()), ..Default::default() };
        writer.append(&s, &[], &[] as &[DeepAnaInstruction], &[t]).unwrap();
    }
    writer.flush().unwrap();

    let before: Vec<(PathBuf, Vec<u8>)> = metadata_files(&dir).into_iter().map(|p| { let b = fs::read(&p).unwrap(); (p, b) }).collect();
    assert!(!before.is_empty());
    writer.rollback(0).unwrap();
    let after: Vec<(PathBuf, Vec<u8>)> = metadata_files(&dir).into_iter().map(|p| { let b = fs::read(&p).unwrap(); (p, b) }).collect();
    assert_eq!(before, after);
    assert!(table_slots(&dir, TableKind::Slots).is_empty(), "ingestion files were rolled back");
    assert_eq!(read_metadata_table(&dir).len(), 5);
}

#[tokio::test]
async fn a_file_named_like_a_slot_range_inside_the_table_survives_rollback_too() {
    // Even a name rollback would parse as a slot range is out of its reach
    // inside the metadata directory.
    let dir = temp_dir();
    let lookalike = table_root(&dir).join("date=2026-01-01").join("transfers_5_9.parquet");
    fs::create_dir_all(lookalike.parent().unwrap()).unwrap();
    fs::write(&lookalike, b"x").unwrap();
    ParquetWriter::new(&dir).rollback(0).unwrap();
    assert!(lookalike.exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_live_rewind_reingests_without_a_second_row_for_any_mint() {
    // The cursor refuses to rewind below slot 2, so start at 10 and rewind
    // to 11: slots 11..=40 are deleted and ingested again.
    const FIRST: u64 = 10;
    const TIP: u64 = 40;
    const FROM: u64 = FIRST + 1;
    let slots = TIP - FIRST + 1;
    let redone = TIP - FROM + 1;
    let dir = temp_dir();
    let chain = MetaChain::new();
    for slot in FIRST..=TIP {
        chain.put_valid(&transfer_mint(slot), &format!("T{slot}"));
    }
    let handle = start(fast_config(), &chain, &dir);
    let changelog = Arc::new(CountingChangeLog::default());
    let config = IngestorConfig { start_slot: Some(FIRST), ..ingest_config(&dir) };
    let ingestor = Arc::new(new_ingestor(TIP, config, &changelog).with_metadata(handle.clone()));
    let run = tokio::spawn({
        let ingestor = Arc::clone(&ingestor);
        async move { ingestor.run().await }
    });

    wait_for("every slot flushed", || ingestor.cursor().state().last_flush_slot == TIP).await;
    wait_for("every mint written", || handle.stats().written == slots).await;
    let files_before: BTreeSet<PathBuf> = metadata_files(&dir).into_iter().collect();
    let hints_before = handle.stats().hints_received;
    assert_eq!(handle.stats().hints_known, 0);

    // The reconciler's rewind: every file from FROM is deleted and those
    // slots ingested again.
    ingestor.rewinder().rewind_to(FROM).await.unwrap();
    wait_for("every rewound slot re-announced", || {
        changelog.new_events.lock().unwrap().len() as u64 == slots + redone
    })
    .await;
    wait_for("the re-ingested mints hinted", || handle.stats().hints_received == hints_before + redone).await;
    wait_for("and found known", || handle.stats().hints_known == redone).await;

    ingestor.shutdown();
    let report = run.await.unwrap().unwrap();
    handle.shutdown().await;
    assert_eq!(report.rewinds, 1);

    let expected: Vec<u64> = (FIRST..=TIP).collect();
    assert_eq!(table_slots(&dir, TableKind::Slots), expected, "re-ingested once");
    assert_eq!(table_slots(&dir, TableKind::TokenTransfers), expected);
    let rows = read_metadata_table(&dir);
    let counts = mint_counts(&rows);
    assert_eq!(counts.len() as u64, slots);
    assert!(counts.values().all(|n| *n == 1), "{counts:?}");
    let files_after: BTreeSet<PathBuf> = metadata_files(&dir).into_iter().collect();
    assert_eq!(files_before, files_after, "no metadata file deleted or added");
    let stats = handle.stats();
    assert_eq!(stats.written, slots);
    // The rewind cost no request: every re-hinted mint was already known.
    assert_eq!(stats.fetch_batches, chain.call_count() as u64);
    let pdas_requested = chain.requested().len() as u64;
    assert_eq!(pdas_requested, slots);
}

// ===========================================================================
// Failure handling
// ===========================================================================

#[tokio::test]
async fn transient_errors_back_off_and_then_succeed() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(1);
    chain.put_valid(&m, "Retry");
    chain.fail_next([transient_error(), PrismError::rate_limited(Some(150)), transient_error()]);
    let handle = start(fast_config(), &chain, &dir);
    handle.hint(m, 1);
    wait_for("written after retries", || handle.stats().known_mints == 1).await;
    handle.shutdown().await;

    let stats = handle.stats();
    assert_eq!(stats.fetch_errors["transport"], 2);
    assert_eq!(stats.fetch_errors["rate_limited"], 1);
    assert_eq!(stats.exhausted, 0);
    assert_eq!(stats.written, 1);
    let calls = chain.calls();
    assert_eq!(calls.len(), 4);
    let gaps: Vec<Duration> = calls.windows(2).map(|w| w[1].0 - w[0].0).collect();
    // Backoff 20 ms, then the server's 150 ms (longer than the 40 ms
    // backoff), then 80 ms.
    assert!(gaps[0] >= Duration::from_millis(20), "{gaps:?}");
    assert!(gaps[1] >= Duration::from_millis(150), "retry-after honoured: {gaps:?}");
    assert!(gaps[2] >= Duration::from_millis(80), "{gaps:?}");
}

#[tokio::test]
async fn mints_are_exhausted_after_the_attempt_budget_and_can_be_hinted_again() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(1);
    chain.put_valid(&m, "Later");
    chain.fail_next((0..3).map(|_| transient_error()));
    let config = MetadataConfig { max_fetch_attempts: 3, ..fast_config() };
    let handle = start(config, &chain, &dir);
    handle.hint(m, 1);
    wait_for("exhausted", || handle.stats().exhausted == 1).await;
    assert_eq!(chain.call_count(), 3);
    assert_eq!(handle.stats().known_mints, 0);
    // Not negative-cached: the next hint tries again, and the RPC is back.
    handle.hint(m, 2);
    wait_for("written", || handle.stats().known_mints == 1).await;
    handle.shutdown().await;
    assert_eq!(handle.stats().negative_cache_hits, 0);
    assert_eq!(read_metadata_table(&dir).len(), 1);
}

#[tokio::test]
async fn a_permanent_error_drops_the_batch_at_once() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.fail_next([PrismError::deserialize(None, "getMultipleAccounts response", "garbage")]);
    let handle = start(fast_config(), &chain, &dir);
    for n in 0..3 {
        handle.hint(mint(n), 1);
    }
    wait_for("exhausted", || handle.stats().exhausted == 3).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    handle.shutdown().await;
    assert_eq!(chain.call_count(), 1, "not retried");
    assert_eq!(handle.stats().fetch_errors["invalid_response"], 1);
}

#[tokio::test]
async fn negative_results_are_cached_for_the_ttl_in_memory_only() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let missing = mint(1);
    let rejected = mint(2);
    chain.put(&rejected, metadata_account(vec![0; 3]));
    let config = MetadataConfig { negative_ttl: Duration::from_millis(300), ..fast_config() };
    let handle = start(config.clone(), &chain, &dir);
    handle.hint(missing, 1);
    handle.hint(rejected, 1);
    wait_for("looked up", || chain.call_count() == 1 && handle.stats().not_found == 1).await;

    // Within the TTL: not asked again.
    for _ in 0..5 {
        handle.hint(missing, 2);
        handle.hint(rejected, 2);
    }
    wait_for("cache hits", || handle.stats().negative_cache_hits == 10).await;
    assert_eq!(chain.call_count(), 1);

    // After it: asked again.
    tokio::time::sleep(Duration::from_millis(350)).await;
    handle.hint(missing, 3);
    wait_for("asked again after the TTL", || chain.call_count() == 2).await;
    handle.shutdown().await;

    // A restart forgets the cache: asked again at once.
    let handle = start(MetadataConfig { negative_ttl: Duration::from_secs(3_600), ..config }, &chain, &dir);
    handle.hint(missing, 4);
    wait_for("asked again after a restart", || chain.call_count() == 3).await;
    handle.shutdown().await;
    assert!(metadata_files(&dir).is_empty());
}

#[tokio::test]
async fn batches_close_at_the_size_cap_or_the_window_and_requests_are_spaced() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let config = MetadataConfig {
        batch_max_mints: 5,
        batch_window: Duration::from_millis(300),
        min_request_interval: Duration::from_millis(100),
        ..fast_config()
    };
    let handle = start(config, &chain, &dir);
    let started = Instant::now();
    // 12 mints: two full batches at once, two left for the window.
    for n in 0..12 {
        handle.hint(mint(n), 1);
    }
    wait_for("three requests", || chain.call_count() == 3).await;
    handle.shutdown().await;

    let calls = chain.calls();
    let sizes: Vec<usize> = calls.iter().map(|(_, a)| a.len()).collect();
    assert_eq!(sizes, [5, 5, 2]);
    assert!(calls[0].0 - started < Duration::from_millis(250), "a full batch does not wait for the window");
    assert!(calls[1].0 - calls[0].0 >= Duration::from_millis(100), "spaced by min_request_interval");
    assert!(calls[2].0 - started >= Duration::from_millis(300), "a partial batch waits for the window");
    // Every PDA requested is the derived one.
    let expected: HashSet<String> = (0..12).map(|n| metadata_pda(&mint(n)).to_string()).collect();
    assert_eq!(chain.requested(), expected);
}

#[tokio::test]
async fn the_default_rate_is_at_most_one_request_per_second() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let config = MetadataConfig { batch_max_mints: 1, ..MetadataConfig::default() };
    let handle = start(config, &chain, &dir);
    for n in 0..3 {
        handle.hint(mint(n), 1);
    }
    wait_for("three requests", || chain.call_count() == 3).await;
    handle.shutdown().await;
    let calls = chain.calls();
    for pair in calls.windows(2) {
        assert!(pair[1].0 - pair[0].0 >= Duration::from_millis(990), "{:?}", pair[1].0 - pair[0].0);
    }
}

#[tokio::test]
async fn ten_failed_flushes_in_a_row_disable_the_enricher() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    let m = mint(1);
    chain.put_valid(&m, "Blocked");
    let config = MetadataConfig { flush_interval: Duration::from_millis(10), ..fast_config() };
    let handle = start(config, &chain, &dir);
    // Wait for the scan, then make the table directory impossible to create.
    tokio::time::sleep(Duration::from_millis(50)).await;
    fs::create_dir_all(dir.join("solana")).unwrap();
    fs::write(table_root(&dir), b"in the way").unwrap();

    handle.hint(m, 1);
    wait_for("disabled", || matches!(handle.state(), MetadataState::Disabled { .. })).await;
    let stats = handle.stats();
    assert_eq!(stats.write_failures, 10);
    assert_eq!(stats.files_written, 0);
    let MetadataState::Disabled { reason } = handle.state() else { unreachable!() };
    assert!(reason.contains("10 token metadata flushes failed"), "{reason}");
    handle.hint(mint(2), 1);
    assert_eq!(handle.stats().hints_dropped_closed, 1);
    handle.shutdown().await;

    // Unblocked, a restart fetches and writes the mint that was lost.
    fs::remove_file(table_root(&dir)).unwrap();
    let handle = start(fast_config(), &chain, &dir);
    handle.hint(m, 2);
    wait_for("written after restart", || handle.stats().written == 1).await;
    handle.shutdown().await;
    assert_eq!(read_metadata_table(&dir).len(), 1);
}

#[tokio::test]
async fn a_failed_flush_is_retried_and_keeps_its_rows() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.put_valid(&mint(1), "Kept");
    let config = MetadataConfig { flush_interval: Duration::from_millis(30), ..fast_config() };
    let handle = start(config, &chain, &dir);
    tokio::time::sleep(Duration::from_millis(50)).await;
    fs::create_dir_all(dir.join("solana")).unwrap();
    fs::write(table_root(&dir), b"in the way").unwrap();
    handle.hint(mint(1), 1);
    wait_for("a flush failed", || handle.stats().write_failures >= 1).await;
    fs::remove_file(table_root(&dir)).unwrap();
    wait_for("written on a later attempt", || handle.stats().written == 1).await;
    handle.shutdown().await;
    assert_eq!(handle.state(), MetadataState::Draining);
    assert_eq!(read_metadata_table(&dir).len(), 1);
}

#[tokio::test]
async fn shutdown_flushes_buffered_rows_and_drops_pending_hints() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.put_valid(&mint(1), "Buffered");
    let config = MetadataConfig {
        flush_interval: Duration::from_secs(3_600),
        batch_window: Duration::from_secs(3_600),
        batch_max_mints: 1,
        min_request_interval: Duration::from_secs(3_600),
        ..fast_config()
    };
    let handle = start(config, &chain, &dir);
    handle.hint(mint(1), 1);
    wait_for("buffered", || handle.stats().known_mints == 1).await;
    // These wait behind the hour-long request spacing.
    for n in 2..50 {
        handle.hint(mint(n), 1);
    }
    tokio::time::timeout(Duration::from_secs(2), handle.shutdown()).await.expect("prompt");

    assert_eq!(handle.state(), MetadataState::Draining);
    assert_eq!(chain.call_count(), 1, "pending hints were dropped, not fetched");
    let rows = read_metadata_table(&dir);
    assert_eq!(rows.len(), 1, "the buffered row was flushed");
    assert_eq!(rows[0].name, "Buffered");
    handle.hint(mint(99), 1);
    assert_eq!(handle.stats().hints_dropped_closed, 1);
    // Idempotent.
    handle.shutdown().await;
}

#[tokio::test]
async fn the_shutdown_token_stops_it_too_and_every_clone_waits() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.put_valid(&mint(1), "Token");
    let token = CancellationToken::new();
    let config = MetadataConfig { flush_interval: Duration::from_secs(3_600), ..fast_config() };
    let handle = MetadataEnricher::start(config, Arc::clone(&chain) as Arc<dyn ChainAdapter>, &dir, Metrics::new(), token.clone());
    handle.hint(mint(1), 1);
    wait_for("buffered", || handle.stats().known_mints == 1).await;
    let clone = handle.clone();
    token.cancel();
    tokio::time::timeout(Duration::from_secs(2), clone.shutdown()).await.unwrap();
    assert_eq!(read_metadata_table(&dir).len(), 1, "flushed before shutdown returned");
}

#[test]
fn start_never_fails() {
    // No runtime.
    let chain = MetaChain::new();
    let handle = MetadataEnricher::start(
        MetadataConfig::default(),
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        temp_dir(),
        Metrics::new(),
        CancellationToken::new(),
    );
    assert!(matches!(handle.state(), MetadataState::Disabled { reason } if reason.contains("runtime")));
    handle.hint(mint(1), 1);
    assert_eq!(handle.stats().hints_dropped_closed, 1);

    // Invalid config.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    rt.block_on(async {
        let handle = start(MetadataConfig { batch_max_mints: 0, ..fast_config() }, &chain, &temp_dir());
        assert!(matches!(handle.state(), MetadataState::Disabled { reason } if reason.contains("batch_max_mints")));
        handle.shutdown().await;
    });
}

#[tokio::test]
async fn stats_and_rpc_calls_reach_the_metrics() {
    let chain = MetaChain::new();
    let dir = temp_dir();
    chain.put_valid(&mint(1), "Metric");
    chain.fail_next([transient_error()]);
    let metrics = Metrics::new();
    let handle = MetadataEnricher::start(
        fast_config(),
        Arc::clone(&chain) as Arc<dyn ChainAdapter>,
        &dir,
        Arc::clone(&metrics),
        CancellationToken::new(),
    );
    handle.hint(mint(1), 1);
    wait_for("written", || handle.stats().written == 1).await;
    handle.shutdown().await;

    let snapshot = metrics.snapshot(&Sources { metadata: Some(handle.stats()), ..Default::default() });
    let rpc = &snapshot.rpc["getMultipleAccounts"];
    assert_eq!(rpc.requests, 2);
    assert_eq!(rpc.errors["transport"], 1);
    assert_eq!(rpc.latency_us.count, 2);
    let counters = snapshot.sources.metadata.as_ref().unwrap();
    assert_eq!(counters["written"], 1);
    assert_eq!(counters["fetch_errors_transport"], 1);
    let text = render_prometheus(&snapshot);
    assert!(text.contains("prism_metadata_written_total 1\n"), "{text}");
    assert!(text.contains("prism_metadata_rejected_bad_utf8_total 0\n"));

    let json = serde_json::to_value(handle.stats()).unwrap();
    assert_eq!(json["state"]["state"], "draining");
    assert_eq!(json["rejected"]["too_long"], 0);
    let _: MetadataStats = handle.stats();
}
