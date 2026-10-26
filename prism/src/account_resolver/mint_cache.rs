//! [`MintCache`]: what PRISM knows about mints, so a mint is fetched from the
//! RPC once rather than once per transfer.
//!
//! The cache holds two kinds of entries:
//! * **hits**: a mint address with its [`MintInfo`]. A mint's decimals and
//!   owning program never change, so these never expire.
//! * **misses**: an address that was fetched and turned out not to be a mint
//!   (no such account, or an account that is not a mint). These are
//!   remembered for a TTL so that every transfer naming the address does not
//!   trigger another fetch, then forgotten, since the address may become a
//!   mint later.
//!
//! An RPC failure is neither: the address is simply asked for again next
//! time.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use spl_token_2022_interface::extension::StateWithExtensions;
use spl_token_2022_interface::state::Mint;

use crate::adapter::AccountData;
use crate::decoder::spl_token::{SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID};

/// How long a miss is remembered by default.
pub const DEFAULT_MISS_TTL: Duration = Duration::from_secs(10 * 60);

/// The program that owns a mint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TokenProgram {
    /// SPL Token, `TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`.
    Standard,
    /// Token-2022, `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`.
    Token2022,
}

impl TokenProgram {
    /// The token program with base58 id `program_id`, if it is one.
    pub fn from_program_id(program_id: &str) -> Option<Self> {
        match program_id {
            SPL_TOKEN_PROGRAM_ID => Some(Self::Standard),
            TOKEN_2022_PROGRAM_ID => Some(Self::Token2022),
            _ => None,
        }
    }

    /// Base58 id of the program.
    pub const fn program_id(self) -> &'static str {
        match self {
            Self::Standard => SPL_TOKEN_PROGRAM_ID,
            Self::Token2022 => TOKEN_2022_PROGRAM_ID,
        }
    }
}

/// What PRISM keeps about a mint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MintInfo {
    pub decimals: u8,
    /// Not stored on chain by SPL Token (it lives in Metaplex or Token-2022
    /// metadata); `None` for now.
    pub symbol: Option<String>,
    /// Same as `symbol`.
    pub name: Option<String>,
    pub token_program: TokenProgram,
}

impl MintInfo {
    /// The mint stored in `account`, or `None` when the account is not an
    /// initialized mint of SPL Token or Token-2022.
    pub fn from_account(account: &AccountData) -> Option<Self> {
        let token_program = TokenProgram::from_program_id(&account.owner_program)?;
        let mint = StateWithExtensions::<Mint>::unpack(&account.data).ok()?;
        if !mint.base.is_initialized {
            return None;
        }
        Some(Self {
            decimals: mint.base.decimals,
            symbol: None,
            name: None,
            token_program,
        })
    }
}

/// In-memory store of mint metadata, keyed by base58 mint address.
#[derive(Debug)]
pub struct MintCache {
    cache: HashMap<String, MintInfo>,
    /// When each known non-mint address was found missing.
    miss_log: HashMap<String, Instant>,
    miss_ttl: Duration,
    hit_count: u64,
    miss_count: u64,
    fetch_count: u64,
}

impl Default for MintCache {
    fn default() -> Self {
        Self::new(DEFAULT_MISS_TTL)
    }
}

impl MintCache {
    /// An empty cache that remembers misses for `miss_ttl`.
    pub fn new(miss_ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            miss_log: HashMap::new(),
            miss_ttl,
            hit_count: 0,
            miss_count: 0,
            fetch_count: 0,
        }
    }

    /// The mint `address`, counted as a hit or a miss.
    pub fn get(&mut self, address: &str) -> Option<&MintInfo> {
        let info = self.cache.get(address);
        if info.is_some() {
            self.hit_count += 1;
        } else {
            self.miss_count += 1;
        }
        info
    }

    /// The mint `address`, without touching the counters.
    pub fn peek(&self, address: &str) -> Option<&MintInfo> {
        self.cache.get(address)
    }

    pub fn contains(&self, address: &str) -> bool {
        self.cache.contains_key(address)
    }

    /// Stores `info` for `address`, replacing any earlier entry and clearing
    /// a recorded miss.
    pub fn insert(&mut self, address: impl Into<String>, info: MintInfo) {
        let address = address.into();
        self.miss_log.remove(&address);
        self.cache.insert(address, info);
    }

    /// Records that `address` is not a mint, as of now.
    pub fn mark_missing(&mut self, address: impl Into<String>) {
        self.mark_missing_at(address, Instant::now());
    }

    /// Records that `address` is not a mint, as of `at`.
    pub fn mark_missing_at(&mut self, address: impl Into<String>, at: Instant) {
        self.miss_log.insert(address.into(), at);
    }

    /// Whether `address` was found not to be a mint less than the miss TTL
    /// ago.
    pub fn is_recently_missing(&self, address: &str) -> bool {
        self.is_recently_missing_at(address, Instant::now())
    }

    /// Whether, at `now`, `address` was found not to be a mint less than the
    /// miss TTL ago.
    pub fn is_recently_missing_at(&self, address: &str, now: Instant) -> bool {
        self.miss_log
            .get(address)
            .is_some_and(|&at| now.saturating_duration_since(at) < self.miss_ttl)
    }

    /// Whether `address` should be fetched: neither cached nor recently
    /// missing.
    pub fn needs_fetch(&self, address: &str) -> bool {
        self.needs_fetch_at(address, Instant::now())
    }

    pub fn needs_fetch_at(&self, address: &str, now: Instant) -> bool {
        !self.contains(address) && !self.is_recently_missing_at(address, now)
    }

    /// Drops misses older than the TTL, as of `now`; returns how many.
    pub fn prune_misses_at(&mut self, now: Instant) -> usize {
        let ttl = self.miss_ttl;
        let before = self.miss_log.len();
        self.miss_log
            .retain(|_, &mut at| now.saturating_duration_since(at) < ttl);
        before - self.miss_log.len()
    }

    /// Counts `n` addresses requested from the RPC.
    pub fn record_fetches(&mut self, n: usize) {
        self.fetch_count += n as u64;
    }

    /// Number of mints cached.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Number of addresses in the miss log, expired ones included until
    /// pruned.
    pub fn missing_len(&self) -> usize {
        self.miss_log.len()
    }

    pub fn miss_ttl(&self) -> Duration {
        self.miss_ttl
    }

    /// Lookups through [`Self::get`] that found the mint.
    pub fn hit_count(&self) -> u64 {
        self.hit_count
    }

    /// Lookups through [`Self::get`] that did not.
    pub fn miss_count(&self) -> u64 {
        self.miss_count
    }

    /// Addresses requested from the RPC.
    pub fn fetch_count(&self) -> u64 {
        self.fetch_count
    }
}

#[cfg(test)]
mod tests {
    use solana_sdk::program_pack::Pack;
    use solana_sdk::pubkey::Pubkey;
    use spl_token_2022_interface::extension::mint_close_authority::MintCloseAuthority;
    use spl_token_2022_interface::extension::{
        BaseStateWithExtensionsMut, ExtensionType, StateWithExtensionsMut,
    };
    use spl_token_2022_interface::state::Account;

    use super::*;

    const MINT_A: &str = "So11111111111111111111111111111111111111112";
    const MINT_B: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    fn info(decimals: u8) -> MintInfo {
        MintInfo { decimals, symbol: None, name: None, token_program: TokenProgram::Standard }
    }

    fn mint(decimals: u8, is_initialized: bool) -> Mint {
        Mint { decimals, is_initialized, supply: 1_000, ..Mint::default() }
    }

    fn account(owner: &str, data: Vec<u8>) -> AccountData {
        AccountData { owner_program: owner.to_owned(), lamports: 1, data, ..AccountData::default() }
    }

    /// An 82-byte base mint, as both token programs store it.
    fn packed_mint(decimals: u8, is_initialized: bool) -> Vec<u8> {
        let mut data = vec![0; Mint::LEN];
        Mint::pack_into_slice(&mint(decimals, is_initialized), &mut data);
        data
    }

    /// A Token-2022 mint with an extension, so longer than the base mint.
    fn extended_mint(decimals: u8) -> Vec<u8> {
        let len = ExtensionType::try_calculate_account_len::<Mint>(&[ExtensionType::MintCloseAuthority])
            .unwrap();
        let mut data = vec![0; len];
        let mut state = StateWithExtensionsMut::<Mint>::unpack_uninitialized(&mut data).unwrap();
        state.base = mint(decimals, true);
        state.pack_base();
        state.init_extension::<MintCloseAuthority>(true).unwrap();
        state.init_account_type().unwrap();
        data
    }

    fn packed_token_account() -> Vec<u8> {
        let mut data = vec![0; Account::LEN];
        let token_account = Account {
            mint: Pubkey::new_unique(),
            owner: Pubkey::new_unique(),
            amount: 5,
            state: spl_token_2022_interface::state::AccountState::Initialized,
            ..Account::default()
        };
        Account::pack_into_slice(&token_account, &mut data);
        data
    }

    #[test]
    fn token_program_round_trips_its_id() {
        for program in [TokenProgram::Standard, TokenProgram::Token2022] {
            assert_eq!(TokenProgram::from_program_id(program.program_id()), Some(program));
        }
        assert_eq!(TokenProgram::Standard.program_id(), "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
        assert_eq!(TokenProgram::Token2022.program_id(), "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
        assert_eq!(TokenProgram::from_program_id("11111111111111111111111111111111"), None);
    }

    #[test]
    fn mint_info_reads_standard_and_token_2022_mints() {
        let standard = MintInfo::from_account(&account(SPL_TOKEN_PROGRAM_ID, packed_mint(6, true)));
        assert_eq!(standard, Some(info(6)));

        let base_2022 = MintInfo::from_account(&account(TOKEN_2022_PROGRAM_ID, packed_mint(0, true)));
        assert_eq!(base_2022.map(|i| (i.decimals, i.token_program)), Some((0, TokenProgram::Token2022)));

        let extended = MintInfo::from_account(&account(TOKEN_2022_PROGRAM_ID, extended_mint(9))).unwrap();
        assert_eq!((extended.decimals, extended.token_program), (9, TokenProgram::Token2022));
        assert_eq!((extended.symbol, extended.name), (None, None));
    }

    #[test]
    fn mint_info_rejects_what_is_not_an_initialized_mint() {
        let cases = [
            ("uninitialized mint", account(SPL_TOKEN_PROGRAM_ID, packed_mint(6, false))),
            ("not owned by a token program", account("11111111111111111111111111111111", packed_mint(6, true))),
            ("token account", account(SPL_TOKEN_PROGRAM_ID, packed_token_account())),
            ("token-2022 token account", account(TOKEN_2022_PROGRAM_ID, packed_token_account())),
            ("truncated", account(SPL_TOKEN_PROGRAM_ID, packed_mint(6, true)[..81].to_vec())),
            ("empty", account(SPL_TOKEN_PROGRAM_ID, Vec::new())),
        ];
        for (what, account) in cases {
            assert_eq!(MintInfo::from_account(&account), None, "{what}");
        }
    }

    #[test]
    fn get_counts_hits_and_misses_peek_does_not() {
        let mut cache = MintCache::default();
        assert!(cache.get(MINT_A).is_none());
        cache.insert(MINT_A, info(9));
        assert_eq!(cache.get(MINT_A), Some(&info(9)));
        assert_eq!(cache.get(MINT_A).map(|i| i.decimals), Some(9));
        assert_eq!(cache.peek(MINT_A), Some(&info(9)));
        assert!(cache.peek(MINT_B).is_none());
        assert_eq!((cache.hit_count(), cache.miss_count()), (2, 1));
        assert_eq!((cache.len(), cache.is_empty()), (1, false));
    }

    #[test]
    fn insert_replaces_and_clears_a_recorded_miss() {
        let mut cache = MintCache::default();
        cache.mark_missing(MINT_A);
        assert!(cache.is_recently_missing(MINT_A));
        assert!(!cache.needs_fetch(MINT_A));
        cache.insert(MINT_A, info(6));
        cache.insert(MINT_A, info(8));
        assert!(!cache.is_recently_missing(MINT_A));
        assert_eq!(cache.missing_len(), 0);
        assert_eq!(cache.peek(MINT_A).map(|i| i.decimals), Some(8));
        assert!(!cache.needs_fetch(MINT_A), "cached");
    }

    #[test]
    fn misses_expire_after_the_ttl() {
        let ttl = Duration::from_secs(60);
        let mut cache = MintCache::new(ttl);
        let t0 = Instant::now();
        cache.mark_missing_at(MINT_A, t0);
        assert!(cache.is_recently_missing_at(MINT_A, t0));
        assert!(cache.is_recently_missing_at(MINT_A, t0 + ttl - Duration::from_millis(1)));
        assert!(!cache.is_recently_missing_at(MINT_A, t0 + ttl), "expires at exactly the TTL");
        assert!(cache.needs_fetch_at(MINT_A, t0 + ttl));
        assert!(!cache.is_recently_missing_at(MINT_B, t0), "never marked");
        assert!(cache.needs_fetch_at(MINT_B, t0));
        // A clock reading before the miss does not underflow.
        cache.mark_missing_at(MINT_B, t0 + ttl);
        assert!(cache.is_recently_missing_at(MINT_B, t0));
    }

    #[test]
    fn prune_drops_only_expired_misses() {
        let ttl = Duration::from_secs(60);
        let mut cache = MintCache::new(ttl);
        let t0 = Instant::now();
        cache.mark_missing_at(MINT_A, t0);
        cache.mark_missing_at(MINT_B, t0 + Duration::from_secs(30));
        assert_eq!(cache.prune_misses_at(t0 + ttl), 1);
        assert_eq!(cache.missing_len(), 1);
        assert!(cache.is_recently_missing_at(MINT_B, t0 + ttl));
        assert_eq!(cache.prune_misses_at(t0 + 2 * ttl), 1);
        assert_eq!(cache.missing_len(), 0);
    }

    #[test]
    fn fetches_are_counted() {
        let mut cache = MintCache::default();
        assert_eq!(cache.miss_ttl(), DEFAULT_MISS_TTL);
        cache.record_fetches(3);
        cache.record_fetches(0);
        cache.record_fetches(2);
        assert_eq!(cache.fetch_count(), 5);
    }
}
