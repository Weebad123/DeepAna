//! The deterministic chain generator.
//!
//! [`generate`] is a pure function of its [`GenSpec`]. It is stable by
//! construction, so the same spec gives the same chain on every machine,
//! build and run:
//!
//! - Its only randomness is [`SplitMix64`], written here, seeded with
//!   `spec.seed`, with a **fixed order of draws** (below). No `rand`, no
//!   hashing of `HashMap`s, no floats, no clock, no environment.
//! - Every bounded draw is [`SplitMix64::below`], one draw each, so the
//!   number of draws never depends on a rejection loop.
//! - Every value in the chain is a function of the spec and that sequence:
//!   addresses and hashes are draws, everything else is integer arithmetic
//!   on draws and the spec.
//! - Keys are unique by construction, not by luck: each address carries a
//!   tag and an index in its last 8 bytes, each blockhash its slot, each
//!   signature a running transaction counter.
//!
//! Changing anything that changes the output, including the order of
//! draws, is a new [`GENERATOR_VERSION`]. The version is part of the set
//! hash, so that also means a new `fixtures.lock`. Golden digests in the
//! tests catch an output change that forgot to bump it.
//!
//! # What a chain holds
//!
//! - Slot numbers `start_slot .. start_slot + slots`, each skipped with
//!   probability `skip_percent`. A produced slot's `parent_slot` is the
//!   last produced slot, or `start_slot - 1` for the first one.
//! - 1 to `max_tx_per_slot` transactions per produced slot, each failed
//!   with probability `fail_percent` (its last top-level instruction fails).
//! - 1 to 3 top-level instructions per transaction: SPL Token or Token-2022
//!   instructions, a router program that calls 1 or 2 token instructions as
//!   inner (CPI) instructions, System transfers and memos.
//! - A token instruction is a transfer with probability `transfer_percent`,
//!   and gets a token transfer row. A transfer has no mint with probability
//!   `mint_null_percent` (a plain `Transfer` whose mint PRISM could not
//!   resolve); otherwise it is a `Transfer` or a `TransferChecked` with the
//!   mint and its decimals. The four mints have decimals 0, 2, 6 and 9.
//!
//! Every generated chain passes [`check_consistency`](super::check_consistency),
//! and is clean for ORACLE's decoding checks too: every token instruction is
//! decoded, every decoded transfer has its row, every checked transfer has
//! its mint and decimals, and each mint has one decimals value.
//!
//! # Order of draws
//!
//! `key` is 3 draws (24 bytes, little-endian) followed by a tag byte and a
//! 7-byte index. `signature` is 7 draws (56 bytes) followed by the 8-byte
//! number of transactions generated before it.
//!
//! ```text
//! setup:
//!   validators[0..4]      key(tag 1, v)
//!   wallets[0..8]         key(tag 2, w)
//!   mints[0..4]           key(tag 3, m)
//!   token accounts, for m in 0..4, for j in 0..4:
//!                         key(tag 4, 4m + j), then owner = below(8)
//!   router program        key(tag 5, 0)
//! for each slot number s, in increasing order:
//!   skipped               chance(skip_percent)      -- if skipped, next slot
//!   blockhash             key(tag 6, s)
//!   leader                below(4)
//!   n_tx                  1 + below(max_tx_per_slot)
//!   for each transaction:
//!     signature           signature()
//!     payer               below(8)
//!     fee                 5000 + below(100000)
//!     failed              chance(fail_percent)
//!     cu_known            below(10) != 0
//!     cu                  150 + below(1400000)      -- drawn even if unknown
//!     n_top               1 + below(3)
//!     for each top-level instruction:
//!       kind              below(4): 0 token, 1 router, 2 system, 3 memo
//!       token:            token_ix()
//!       router:           data = next_u64(); n_inner = 1 + below(2);
//!                         token_ix() for each inner instruction
//!       system:           to = below(8); lamports = 1 + below(10^10)
//!       memo:             (no draws)
//! token_ix():
//!   token_2022            below(4) == 0
//!   is_transfer           chance(transfer_percent)
//!   mint                  below(4)
//!   src, dst              below(4), below(4)        -- accounts of that mint
//!   amount                1 + below(10^12)
//!   if is_transfer:
//!     mint_null           chance(mint_null_percent)
//!     checked             below(2) == 1             -- only if not mint_null
//!     owners_known        below(2) == 0
//!   else:
//!     op                  below(3): 0 Approve, 1 MintTo, 2 Burn
//! ```

use super::model_cases::{
    Chain, InstructionRow, SPL_TOKEN_PROGRAM_ID, SlotRow, TOKEN_2022_PROGRAM_ID,
    TokenTransferRow, TransactionRow, TxStatus,
};
use super::{FieldIssue, StrictValue};

/// The generator's version. Part of the set hash.
pub const GENERATOR_VERSION: u32 = 1;

/// Most slot numbers in one spec.
pub const MAX_GEN_SLOTS: u32 = 10_000;
/// Most transactions per produced slot.
pub const MAX_GEN_TX_PER_SLOT: u32 = 64;
/// `slots * max_tx_per_slot` is at most this, which bounds the chain to
/// that many transactions and about nine times as many instructions.
pub const MAX_GEN_TRANSACTIONS: u64 = 20_000;
/// Largest `start_slot`; keeps every slot number below 2^56 (it is part of
/// the blockhash) and `block_time` far from overflow.
pub const MAX_GEN_START_SLOT: u64 = 1 << 48;

/// `block_time` of slot 0; a slot is 0.4 s.
const BASE_BLOCK_TIME: i64 = 1_600_000_000;

const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";
const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

const VALIDATORS: usize = 4;
const WALLETS: usize = 8;
const MINT_DECIMALS: [u8; 4] = [0, 2, 6, 9];
const MINTS: usize = MINT_DECIMALS.len();
const ACCOUNTS_PER_MINT: usize = 4;

const TAG_VALIDATOR: u8 = 1;
const TAG_WALLET: u8 = 2;
const TAG_MINT: u8 = 3;
const TAG_TOKEN_ACCOUNT: u8 = 4;
const TAG_ROUTER: u8 = 5;
const TAG_BLOCKHASH: u8 = 6;

// ---------------------------------------------------------------------------
// PRNG
// ---------------------------------------------------------------------------

/// SplitMix64 (Steele, Lea and Flood, 2014), exactly as in the reference
/// implementation. The state is the seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        SplitMix64 { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`, from exactly one draw: the high 64 bits of
    /// `draw * n`. `n` must be at least 1.
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0, "below(0)");
        ((u128::from(self.next_u64()) * u128::from(n)) >> 64) as u64
    }

    /// `true` with probability `percent`/100, from exactly one draw.
    pub fn chance(&mut self, percent: u8) -> bool {
        self.below(100) < u64::from(percent)
    }

    fn index(&mut self, n: usize) -> usize {
        self.below(n as u64) as usize
    }
}

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// What to generate. Every field is required in a model case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GenSpec {
    pub seed: u64,
    /// First slot number; at least 1, since its parent is `start_slot - 1`.
    pub start_slot: u64,
    /// Slot numbers, produced or skipped.
    pub slots: u32,
    pub skip_percent: u8,
    pub max_tx_per_slot: u32,
    pub fail_percent: u8,
    pub transfer_percent: u8,
    pub mint_null_percent: u8,
}

impl GenSpec {
    /// The bounds above. Fields are named as in a model case
    /// (`chain.generated.<field>`).
    pub fn validate(&self) -> Result<(), FieldIssue> {
        let f = |k: &str| format!("chain.generated.{k}");
        let range = |k: &str, v: u64, lo: u64, hi: u64, why: &str| {
            if v < lo {
                Err(FieldIssue::new(f(k), format!("must be at least {lo}{why}, not {v}")))
            } else if v > hi {
                Err(FieldIssue::new(f(k), format!("must be at most {hi}, not {v}")))
            } else {
                Ok(())
            }
        };
        range(
            "start_slot",
            self.start_slot,
            1,
            MAX_GEN_START_SLOT,
            " (the first slot's parent is start_slot - 1)",
        )?;
        range("slots", self.slots.into(), 1, MAX_GEN_SLOTS.into(), "")?;
        for (k, p) in [
            ("skip_percent", self.skip_percent),
            ("fail_percent", self.fail_percent),
            ("transfer_percent", self.transfer_percent),
            ("mint_null_percent", self.mint_null_percent),
        ] {
            if p > 100 {
                return Err(FieldIssue::new(f(k), format!("must be 0 to 100, not {p}")));
            }
        }
        range(
            "max_tx_per_slot",
            self.max_tx_per_slot.into(),
            1,
            MAX_GEN_TX_PER_SLOT.into(),
            "",
        )?;
        let total = u64::from(self.slots) * u64::from(self.max_tx_per_slot);
        if total > MAX_GEN_TRANSACTIONS {
            return Err(FieldIssue::new(
                f("max_tx_per_slot"),
                format!(
                    "slots * max_tx_per_slot is {total}, more than {MAX_GEN_TRANSACTIONS}"
                ),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Generator
// ---------------------------------------------------------------------------

fn b58(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

/// 3 draws, then `tag`, then the low 7 bytes of `index`.
fn key(rng: &mut SplitMix64, tag: u8, index: u64) -> String {
    let mut k = [0u8; 32];
    for i in 0..3 {
        k[8 * i..8 * i + 8].copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    k[24] = tag;
    k[25..32].copy_from_slice(&index.to_le_bytes()[..7]);
    b58(&k)
}

/// 7 draws, then `counter`.
fn signature(rng: &mut SplitMix64, counter: u64) -> String {
    let mut s = [0u8; 64];
    for i in 0..7 {
        s[8 * i..8 * i + 8].copy_from_slice(&rng.next_u64().to_le_bytes());
    }
    s[56..64].copy_from_slice(&counter.to_le_bytes());
    b58(&s)
}

fn tag_and_amount(tag: u8, amount: u64) -> Vec<u8> {
    let mut d = vec![tag];
    d.extend_from_slice(&amount.to_le_bytes());
    d
}

fn amount_json(amount: u64, decimals: Option<u8>) -> StrictValue {
    let mut m = std::collections::BTreeMap::new();
    m.insert("amount".to_string(), StrictValue::Int(amount.into()));
    if let Some(d) = decimals {
        m.insert("decimals".to_string(), StrictValue::Int(d.into()));
    }
    StrictValue::Object(m)
}

struct World {
    validators: Vec<String>,
    wallets: Vec<String>,
    mints: Vec<String>,
    /// `(address, owner wallet index)`, `ACCOUNTS_PER_MINT` per mint.
    token_accounts: Vec<(String, usize)>,
    router: String,
}

impl World {
    fn new(rng: &mut SplitMix64) -> World {
        let validators = (0..VALIDATORS as u64)
            .map(|i| key(rng, TAG_VALIDATOR, i))
            .collect();
        let wallets = (0..WALLETS as u64).map(|i| key(rng, TAG_WALLET, i)).collect();
        let mints = (0..MINTS as u64).map(|i| key(rng, TAG_MINT, i)).collect();
        let mut token_accounts = Vec::with_capacity(MINTS * ACCOUNTS_PER_MINT);
        for i in 0..(MINTS * ACCOUNTS_PER_MINT) as u64 {
            let addr = key(rng, TAG_TOKEN_ACCOUNT, i);
            let owner = rng.index(WALLETS);
            token_accounts.push((addr, owner));
        }
        let router = key(rng, TAG_ROUTER, 0);
        World {
            validators,
            wallets,
            mints,
            token_accounts,
            router,
        }
    }

    fn account(&self, mint: usize, j: usize) -> &(String, usize) {
        &self.token_accounts[mint * ACCOUNTS_PER_MINT + j]
    }
}

/// One generated instruction, before it is placed in a transaction.
struct Ix {
    program_id: String,
    accounts: Vec<String>,
    data: Vec<u8>,
    instruction_type: Option<String>,
    decoded_data: Option<StrictValue>,
    transfer: Option<Transfer>,
}

struct Transfer {
    source: String,
    destination: String,
    source_owner: Option<String>,
    destination_owner: Option<String>,
    mint: Option<String>,
    amount: u64,
    decimals: Option<u8>,
}

/// `token_ix()` in the order of draws.
fn token_ix(rng: &mut SplitMix64, w: &World, spec: &GenSpec) -> Ix {
    let program_id = if rng.below(4) == 0 {
        TOKEN_2022_PROGRAM_ID
    } else {
        SPL_TOKEN_PROGRAM_ID
    }
    .to_string();
    let is_transfer = rng.chance(spec.transfer_percent);
    let m = rng.index(MINTS);
    let (src, src_owner) = w.account(m, rng.index(ACCOUNTS_PER_MINT)).clone();
    let (dst, dst_owner) = w.account(m, rng.index(ACCOUNTS_PER_MINT)).clone();
    let amount = 1 + rng.below(1_000_000_000_000);
    let mint = w.mints[m].clone();
    let decimals = MINT_DECIMALS[m];

    if is_transfer {
        let mint_null = rng.chance(spec.mint_null_percent);
        let checked = !mint_null && rng.below(2) == 1;
        let owners_known = rng.below(2) == 0;
        let authority = w.wallets[src_owner].clone();
        let (ty, accounts, data, decoded) = if checked {
            let mut data = tag_and_amount(12, amount);
            data.push(decimals);
            (
                "TransferChecked",
                vec![src.clone(), mint.clone(), dst.clone(), authority],
                data,
                amount_json(amount, Some(decimals)),
            )
        } else {
            (
                "Transfer",
                vec![src.clone(), dst.clone(), authority],
                tag_and_amount(3, amount),
                amount_json(amount, None),
            )
        };
        let owner = |i: usize| owners_known.then(|| w.wallets[i].clone());
        Ix {
            program_id,
            accounts,
            data,
            instruction_type: Some(ty.into()),
            decoded_data: Some(decoded),
            transfer: Some(Transfer {
                source: src,
                destination: dst,
                source_owner: owner(src_owner),
                destination_owner: owner(dst_owner),
                mint: (!mint_null).then_some(mint),
                amount,
                decimals: (!mint_null).then_some(decimals),
            }),
        }
    } else {
        let (ty, tag, accounts) = match rng.below(3) {
            0 => ("Approve", 4, vec![src, w.wallets[dst_owner].clone(), w.wallets[src_owner].clone()]),
            1 => ("MintTo", 7, vec![mint, dst, w.wallets[0].clone()]),
            _ => ("Burn", 8, vec![src, mint, w.wallets[src_owner].clone()]),
        };
        Ix {
            program_id,
            accounts,
            data: tag_and_amount(tag, amount),
            instruction_type: Some(ty.into()),
            decoded_data: Some(amount_json(amount, None)),
            transfer: None,
        }
    }
}

/// A top-level instruction and its inner instructions.
fn top_level_ix(
    rng: &mut SplitMix64,
    w: &World,
    spec: &GenSpec,
    payer: &str,
    memo: String,
) -> (Ix, Vec<Ix>) {
    match rng.below(4) {
        0 => (token_ix(rng, w, spec), Vec::new()),
        1 => {
            let data = rng.next_u64().to_le_bytes().to_vec();
            let n_inner = 1 + rng.below(2);
            let inner = (0..n_inner).map(|_| token_ix(rng, w, spec)).collect();
            let router = Ix {
                program_id: w.router.clone(),
                accounts: vec![payer.to_string()],
                data,
                instruction_type: None,
                decoded_data: None,
                transfer: None,
            };
            (router, inner)
        }
        2 => {
            let to = w.wallets[rng.index(WALLETS)].clone();
            let lamports = 1 + rng.below(10_000_000_000);
            let mut data = vec![2, 0, 0, 0];
            data.extend_from_slice(&lamports.to_le_bytes());
            let ix = Ix {
                program_id: SYSTEM_PROGRAM_ID.into(),
                accounts: vec![payer.to_string(), to],
                data,
                instruction_type: None,
                decoded_data: None,
                transfer: None,
            };
            (ix, Vec::new())
        }
        _ => {
            let ix = Ix {
                program_id: MEMO_PROGRAM_ID.into(),
                accounts: Vec::new(),
                data: memo.into_bytes(),
                instruction_type: None,
                decoded_data: None,
                transfer: None,
            };
            (ix, Vec::new())
        }
    }
}

/// Generate the chain `spec` describes. Pure: the same spec always gives
/// the same chain. `spec` must pass [`GenSpec::validate`]; a spec outside
/// the bounds panics rather than produce a chain no fixture can name.
pub fn generate(spec: &GenSpec) -> Chain {
    if let Err(e) = spec.validate() {
        panic!("generate: invalid spec: {e}");
    }
    let mut rng = SplitMix64::new(spec.seed);
    let w = World::new(&mut rng);
    let mut chain = Chain::default();
    let mut parent = spec.start_slot - 1;
    let mut produced: u64 = 0;
    let mut tx_counter: u64 = 0;
    let height0 = spec.start_slot - spec.start_slot / 10;

    for s in spec.start_slot..spec.start_slot + u64::from(spec.slots) {
        if rng.chance(spec.skip_percent) {
            continue;
        }
        let blockhash = key(&mut rng, TAG_BLOCKHASH, s);
        let leader = w.validators[rng.index(VALIDATORS)].clone();
        let n_tx = 1 + rng.below(spec.max_tx_per_slot.into());
        // 0.4 s per slot; s < 2^49, so this cannot overflow.
        let block_time = BASE_BLOCK_TIME + (s * 2 / 5) as i64;

        for t in 0..n_tx {
            let tx_signature = signature(&mut rng, tx_counter);
            tx_counter += 1;
            let payer = w.wallets[rng.index(WALLETS)].clone();
            let fee = 5000 + rng.below(100_000);
            let failed = rng.chance(spec.fail_percent);
            let cu_known = rng.below(10) != 0;
            let cu = 150 + rng.below(1_400_000);
            let n_top = 1 + rng.below(3);

            let mut accounts = vec![payer.clone()];
            let mut logs = Vec::new();
            // Message order: the payer, then every address in instruction
            // order, each once.
            fn add(accounts: &mut Vec<String>, a: &String) {
                if !accounts.contains(a) {
                    accounts.push(a.clone());
                }
            }
            for i in 0..n_top {
                let memo = format!("gen {s} {t} {i}");
                let (top, inner) = top_level_ix(&mut rng, &w, spec, &payer, memo);
                let fails_here = failed && i == n_top - 1;
                logs.push(format!("Program {} invoke [1]", top.program_id));
                for x in std::iter::once(&top).chain(&inner) {
                    for a in &x.accounts {
                        add(&mut accounts, a);
                    }
                    add(&mut accounts, &x.program_id);
                }
                for x in &inner {
                    logs.push(format!("Program {} invoke [2]", x.program_id));
                    logs.push(format!("Program {} success", x.program_id));
                }
                logs.push(if fails_here {
                    format!("Program {} failed: custom program error: 0x1", top.program_id)
                } else {
                    format!("Program {} success", top.program_id)
                });

                let index = i as u32;
                let rows = std::iter::once((None, top))
                    .chain(inner.into_iter().enumerate().map(|(k, x)| (Some(k as u32), x)));
                for (inner_index, x) in rows {
                    if let Some(tr) = &x.transfer {
                        chain.token_transfers.push(TokenTransferRow {
                            slot: s,
                            tx_signature: tx_signature.clone(),
                            instruction_index: index,
                            inner_instruction_index: inner_index,
                            spl_token_program_id: x.program_id.clone(),
                            source_token_account: tr.source.clone(),
                            destination_token_account: tr.destination.clone(),
                            source_owner: tr.source_owner.clone(),
                            destination_owner: tr.destination_owner.clone(),
                            mint_token_address: tr.mint.clone(),
                            amount_raw: tr.amount,
                            decimals: tr.decimals,
                            block_time: Some(block_time),
                        });
                    }
                    chain.instructions.push(InstructionRow {
                        slot: s,
                        tx_signature: tx_signature.clone(),
                        instruction_index: index,
                        inner_instruction_index: inner_index,
                        program_id: x.program_id,
                        accounts: x.accounts,
                        data_base58: b58(&x.data),
                        instruction_type: x.instruction_type,
                        decoded_data: x.decoded_data,
                    });
                }
            }

            chain.transactions.push(TransactionRow {
                slot: s,
                blockhash: blockhash.clone(),
                transaction_index: t as u32,
                tx_signature,
                fee,
                status: if failed {
                    TxStatus::Failed
                } else {
                    TxStatus::Success
                },
                err: failed.then(|| format!("InstructionError({}, Custom(1))", n_top - 1)),
                accounts,
                log_messages: logs,
                compute_units_consumed: cu_known.then_some(cu),
            });
        }

        chain.slots.push(SlotRow {
            slot: s,
            blockhash,
            parent_slot: parent,
            block_time: Some(block_time),
            block_height: Some(height0 + produced),
            transaction_count: n_tx as u32,
            leader: Some(leader),
        });
        parent = s;
        produced += 1;
    }
    chain
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::check_consistency;
    use super::*;

    const EXAMPLE: GenSpec = GenSpec {
        seed: 7,
        start_slot: 1000,
        slots: 500,
        skip_percent: 5,
        max_tx_per_slot: 8,
        fail_percent: 10,
        transfer_percent: 30,
        mint_null_percent: 20,
    };

    #[test]
    fn splitmix64_reference_vectors() {
        // Computed outside Rust from the reference algorithm.
        let first4 = |seed| {
            let mut r = SplitMix64::new(seed);
            [r.next_u64(), r.next_u64(), r.next_u64(), r.next_u64()]
        };
        assert_eq!(
            first4(0),
            [0xe220a8397b1dcdaf, 0x6e789e6aa1b965f4, 0x06c45d188009454f, 0xf88bb8a8724c81ec]
        );
        assert_eq!(
            first4(7),
            [0x63cbe1e459320dd7, 0x044c3cd7f43c661c, 0xe6984080bab12a02, 0x953aeb70673e29cb]
        );
        assert_eq!(
            first4(u64::MAX),
            [0xe4d971771b652c20, 0xe99ff867dbf682c9, 0x382ff84cb27281e9, 0x6d1db36ccba982d2]
        );
    }

    #[test]
    fn below_and_chance_take_one_draw_and_stay_in_range() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(1);
        for n in [1u64, 2, 3, 7, 100, u64::MAX] {
            let x = a.below(n);
            assert!(x < n);
            let raw = b.next_u64();
            assert_eq!(x, ((u128::from(raw) * u128::from(n)) >> 64) as u64);
        }
        let mut r = SplitMix64::new(9);
        assert!((0..1000).all(|_| !r.chance(0)));
        assert!((0..1000).all(|_| r.chance(100)));
        assert_eq!(a, b, "same number of draws");
        // A rough sanity check of the distribution.
        let mut r = SplitMix64::new(3);
        let hits = (0..10_000).filter(|_| r.chance(30)).count();
        assert!((2_700..3_300).contains(&hits), "{hits}");
    }

    #[test]
    fn spec_bounds() {
        assert_eq!(EXAMPLE.validate(), Ok(()));
        let field = |s: GenSpec| s.validate().unwrap_err().field;
        assert_eq!(field(GenSpec { start_slot: 0, ..EXAMPLE }), "chain.generated.start_slot");
        assert_eq!(
            field(GenSpec { start_slot: MAX_GEN_START_SLOT + 1, ..EXAMPLE }),
            "chain.generated.start_slot"
        );
        assert_eq!(field(GenSpec { slots: 0, ..EXAMPLE }), "chain.generated.slots");
        assert_eq!(field(GenSpec { slots: MAX_GEN_SLOTS + 1, ..EXAMPLE }), "chain.generated.slots");
        assert_eq!(field(GenSpec { max_tx_per_slot: 0, ..EXAMPLE }), "chain.generated.max_tx_per_slot");
        assert_eq!(field(GenSpec { mint_null_percent: 101, ..EXAMPLE }), "chain.generated.mint_null_percent");
        let e = GenSpec { slots: 10_000, max_tx_per_slot: 3, ..EXAMPLE }.validate().unwrap_err();
        assert!(e.reason.contains("30000, more than 20000"), "{e}");
        assert_eq!(GenSpec { slots: 10_000, max_tx_per_slot: 2, ..EXAMPLE }.validate(), Ok(()));
        let edge = GenSpec { start_slot: MAX_GEN_START_SLOT, slots: 1, skip_percent: 0, ..EXAMPLE };
        assert_eq!(generate(&edge).slots[0].slot, MAX_GEN_START_SLOT);
    }

    #[test]
    #[should_panic(expected = "invalid spec")]
    fn generate_refuses_an_invalid_spec() {
        generate(&GenSpec { start_slot: 0, ..EXAMPLE });
    }

    #[test]
    fn generated_chains_are_consistent_and_have_every_feature() {
        let c = generate(&EXAMPLE);
        assert_eq!(check_consistency(&c), Ok(()));
        let slots: Vec<u64> = c.slots.iter().map(|s| s.slot).collect();
        assert!(slots.len() < 500, "some slots are skipped");
        assert!(slots.len() > 400, "but not most");
        assert!(slots.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(c.slots[0].parent_slot, slots[0] - 1);
        for w in c.slots.windows(2) {
            assert_eq!(w[1].parent_slot, w[0].slot, "parent is the last produced slot");
        }
        assert!(c.slots.windows(2).any(|w| w[1].slot - w[0].slot > 1), "a gap exists");
        assert!(c.slots.iter().all(|s| (1..=8).contains(&s.transaction_count)));
        assert!(c.transactions.iter().any(|t| t.status == TxStatus::Failed));
        assert!(c.transactions.iter().any(|t| t.status == TxStatus::Success));
        assert!(c.instructions.iter().any(|x| x.inner_instruction_index.is_some()), "CPI");
        assert!(!c.token_transfers.is_empty());
        assert!(c.token_transfers.iter().any(|t| t.mint_token_address.is_none()));
        assert!(c.token_transfers.iter().any(|t| t.inner_instruction_index.is_some()));
        assert!(c.token_transfers.iter().any(|t| t.spl_token_program_id == TOKEN_2022_PROGRAM_ID));
        let mut decimals: Vec<u8> = c.token_transfers.iter().filter_map(|t| t.decimals).collect();
        decimals.sort();
        decimals.dedup();
        assert_eq!(decimals, [0, 2, 6, 9], "several decimals values");
        // Every token instruction is decoded; every decoded transfer has a row.
        let tokens = [SPL_TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
        for x in c.instructions.iter().filter(|x| tokens.contains(&x.program_id.as_str())) {
            assert!(x.instruction_type.is_some());
            let is_transfer = matches!(x.instruction_type.as_deref(), Some("Transfer" | "TransferChecked"));
            let has_row = c.token_transfers.iter().any(|t| t.key() == x.key());
            assert_eq!(is_transfer, has_row, "{:?}", x.key());
            if x.instruction_type.as_deref() == Some("TransferChecked") {
                let t = c.token_transfers.iter().find(|t| t.key() == x.key()).unwrap();
                assert!(t.mint_token_address.is_some() && t.decimals.is_some());
            }
        }
    }

    #[test]
    fn percentages_at_their_extremes() {
        let none = generate(&GenSpec {
            skip_percent: 0,
            fail_percent: 0,
            transfer_percent: 0,
            ..EXAMPLE
        });
        assert_eq!(none.slots.len(), 500);
        assert!(none.transactions.iter().all(|t| t.status == TxStatus::Success && t.err.is_none()));
        assert!(none.token_transfers.is_empty());
        let all = generate(&GenSpec {
            skip_percent: 0,
            fail_percent: 100,
            transfer_percent: 100,
            mint_null_percent: 100,
            ..EXAMPLE
        });
        assert!(all.transactions.iter().all(|t| t.status == TxStatus::Failed));
        assert!(all.token_transfers.iter().all(|t| t.mint_token_address.is_none() && t.decimals.is_none()));
        let skipped = generate(&GenSpec { skip_percent: 100, ..EXAMPLE });
        assert_eq!(skipped, Chain::default());
        assert_eq!(check_consistency(&skipped), Ok(()));
    }

    #[test]
    fn generation_is_pure() {
        let a = generate(&EXAMPLE);
        let b = generate(&EXAMPLE);
        assert_eq!(a, b);
        assert_eq!(a.canonical_bytes(), b.canonical_bytes());
        // Each spec field matters.
        let variants = [
            GenSpec { seed: 8, ..EXAMPLE },
            GenSpec { start_slot: 1001, ..EXAMPLE },
            GenSpec { slots: 499, ..EXAMPLE },
            GenSpec { skip_percent: 6, ..EXAMPLE },
            GenSpec { max_tx_per_slot: 7, ..EXAMPLE },
            GenSpec { fail_percent: 11, ..EXAMPLE },
            GenSpec { transfer_percent: 31, ..EXAMPLE },
            GenSpec { mint_null_percent: 21, ..EXAMPLE },
        ];
        for v in variants {
            assert_ne!(generate(&v).digest(), a.digest(), "{v:?}");
        }
    }
}
