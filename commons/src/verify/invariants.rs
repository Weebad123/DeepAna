//! Stage 3: properties no fixture author wrote down.
//!
//! # Decoders
//!
//! Every input comes from the IDL (read by `static_checks.rs`, not by
//! PRISM) and one [`SplitMix64`] seeded with the first 8 bytes of the
//! artifact id (little-endian), drawn in a fixed order, so the run is
//! deterministic and differs per artifact. For each instruction, in IDL
//! order, [`ROUNDS`] times: generate a random value of its argument types,
//! Borsh-encode it **here**, and work out the JSON PRISM must produce
//! **here**, from the mapping PRISM documents (integers up to 64 bits as
//! numbers, 128- and 256-bit as decimal strings, `pubkey` base58, `bytes`
//! base64, `Option` as the value or `null`, structs as objects or arrays,
//! enums as the variant name or `{ "<Variant>": fields }`).
//!
//! | Check                           | Property                                                          |
//! |---------------------------------|-------------------------------------------------------------------|
//! | `invariant.roundtrip`           | `discriminator ‖ borsh(value)` decodes to the instruction's name and exactly the JSON worked out here |
//! | `invariant.trailing_bytes`      | appending bytes changes nothing (Anchor ignores trailing data on chain) |
//! | `invariant.truncation`          | any strictly shorter body is a decode error, never a decode nor unclaimed |
//! | `invariant.unknown_discriminator` | data that starts with no instruction's discriminator, or is shorter than 8 bytes, is not claimed |
//! | `invariant.foreign_program`     | every input sent to another program is not claimed                |
//! | `invariant.deterministic`       | every input decodes the same way twice                            |
//! | `invariant.no_panic`            | no input panics                                                   |
//!
//! Every call's canonical outcome, in order, forms the `fuzz` digest.
//!
//! # Models
//!
//! For every case:
//!
//! | Check                          | Property                                                          |
//! |--------------------------------|-------------------------------------------------------------------|
//! | `invariant.row_order`          | shuffling the rows of every input table (seeded) does not change the result multiset (nor the order, when the model orders) |
//! | `invariant.bound_independence` | removing every row outside the window from every table does not change the result: the semantic proof of the guard |
//! | `invariant.partition_stable`   | for generated chains, 2, 3 and 8 partitions give what 1 gives     |
//! | `invariant.output_types`       | every column is boolean, integer or string, at most 10,000 rows and 32 columns |
//!
//! `invariant.bound_independence` can only fail a model if a case's chain
//! has rows outside its window, so a model needs, **for each table it
//! reads**, a case where that table does (`invariant.bound_not_exercised`).
//! Every result, in order, forms the `invariants` digest.

use std::collections::BTreeMap;

use crate::config::echo;
use crate::fixtures::{
    ChainRef, DecoderCase, FixtureSet, ModelCase, SplitMix64, StrictValue, Table,
};
use crate::manifest::ArtifactId;

use super::Deadline;
use super::backend::{LoadedDecoder, Outcome};
use super::digest;
use super::engine::TypedResult;
use super::golden::{Call, StageRun, call, engine_check, outcome_of, run_contained};
use super::report::{CheckResult, Status};
use super::sql_guard::SqlInfo;
use super::static_checks::{
    EVENT_IX_TAG, Fields, GenericArg, IDL_IX_TAG, Idl, IdlInstruction, Len, Ty, TypeBody, substitute,
};

pub const CHECK_ROUNDTRIP: &str = "invariant.roundtrip";
pub const CHECK_TRAILING: &str = "invariant.trailing_bytes";
pub const CHECK_TRUNCATION: &str = "invariant.truncation";
pub const CHECK_UNKNOWN: &str = "invariant.unknown_discriminator";
pub const CHECK_FOREIGN: &str = "invariant.foreign_program";
pub const CHECK_DETERMINISTIC: &str = "invariant.deterministic";
pub const CHECK_NO_PANIC: &str = "invariant.no_panic";

pub const CHECK_ROW_ORDER: &str = "invariant.row_order";
pub const CHECK_BOUND_INDEPENDENCE: &str = "invariant.bound_independence";
pub const CHECK_PARTITION_STABLE: &str = "invariant.partition_stable";
pub const CHECK_OUTPUT_TYPES: &str = "invariant.output_types";
pub const CHECK_BOUND_NOT_EXERCISED: &str = "invariant.bound_not_exercised";

/// The key of the model invariants digest.
pub const INVARIANTS_KEY: &str = "invariants";

/// Random values per instruction.
pub const ROUNDS: usize = 32;
/// Random unclaimed inputs per run.
pub const UNKNOWN_ROUNDS: usize = 64;
/// Partition settings the model invariants try on generated chains.
pub const EXTRA_PARTITIONS: [usize; 3] = [2, 3, 8];

const MAX_DEPTH: usize = 6;
const MAX_VEC: u64 = 3;
const MAX_TEXT: u64 = 16;
/// Largest generated instruction body; an on-chain instruction is smaller.
const MAX_BODY: usize = 4096;

/// The fuzz seed: the first 8 bytes of the artifact id, little-endian.
pub fn seed(id: &ArtifactId) -> u64 {
    u64::from_le_bytes(id.0[..8].try_into().expect("8 bytes"))
}

// ---------------------------------------------------------------------------
// Results, one per check and subject
// ---------------------------------------------------------------------------

/// Collects failures per `(check, subject)` and keeps the first reason, so
/// a report has one line per property, not one per input.
#[derive(Default)]
struct Tally {
    order: Vec<(&'static str, String)>,
    first_failure: BTreeMap<(&'static str, String), (Status, String)>,
}

impl Tally {
    fn ran(&mut self, check: &'static str, subject: &str) {
        let key = (check, subject.to_string());
        if !self.order.contains(&key) {
            self.order.push(key);
        }
    }

    fn fail(&mut self, check: &'static str, subject: &str, reason: String) {
        self.ran(check, subject);
        self.first_failure
            .entry((check, subject.to_string()))
            .or_insert((Status::Fail, reason));
    }

    fn check(&mut self, check: &'static str, subject: &str, r: Result<(), String>) {
        match r {
            Ok(()) => self.ran(check, subject),
            Err(e) => self.fail(check, subject, e),
        }
    }

    fn into_checks(self) -> Vec<CheckResult> {
        self.order
            .into_iter()
            .map(|key| match self.first_failure.get(&key) {
                None => CheckResult::pass(key.0, &key.1),
                Some((Status::Error, r)) => CheckResult::error(key.0, &key.1, r),
                Some((_, r)) => CheckResult::fail(key.0, &key.1, r),
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Decoders
// ---------------------------------------------------------------------------

struct Fuzz<'a> {
    loaded: &'a dyn LoadedDecoder,
    program_id: [u8; 32],
    tally: Tally,
    outcomes: Vec<StrictValue>,
}

impl Fuzz<'_> {
    /// One input, run twice: determinism and panics are checked on every
    /// call, and the outcome goes into the digest.
    fn run(&mut self, program_id: &[u8; 32], data: &[u8], accounts: &[[u8; 32]], what: &str) -> Call {
        let a = call(self.loaded, program_id, data, accounts);
        let b = call(self.loaded, program_id, data, accounts);
        let shown = || format!("{what} ({} bytes: {})", data.len(), crate::manifest::to_hex(&data[..data.len().min(48)]));
        self.tally.check(
            CHECK_NO_PANIC,
            "",
            if a.outcome.is_none() { Err(format!("the decoder panicked on {}", shown())) } else { Ok(()) },
        );
        self.tally.check(
            CHECK_DETERMINISTIC,
            "",
            if a == b { Ok(()) } else { Err(format!("{} gave {} then {}", shown(), a.describe(), b.describe())) },
        );
        self.outcomes.push(a.canonical());
        a
    }
}

fn foreign(program_id: &[u8; 32]) -> [u8; 32] {
    let mut p = *program_id;
    p[31] ^= 0x01;
    p
}

fn random_accounts(rng: &mut SplitMix64) -> Vec<[u8; 32]> {
    (0..rng.below(4))
        .map(|_| {
            let mut a = [0u8; 32];
            fill(rng, &mut a);
            a
        })
        .collect()
}

fn fill(rng: &mut SplitMix64, out: &mut [u8]) {
    for chunk in out.chunks_mut(8) {
        let b = rng.next_u64().to_le_bytes();
        chunk.copy_from_slice(&b[..chunk.len()]);
    }
}

/// Every decoder invariant (module docs).
pub fn decoders(
    loaded: &dyn LoadedDecoder,
    idl: &Idl,
    program_id: &[u8; 32],
    cases: &[DecoderCase],
    seed: u64,
    deadline: &Deadline,
) -> StageRun {
    let mut rng = SplitMix64::new(seed);
    let mut f = Fuzz {
        loaded,
        program_id: *program_id,
        tally: Tally::default(),
        outcomes: Vec::new(),
    };
    let mut run = StageRun::default();
    let pid = f.program_id;
    let other = foreign(&pid);

    for ix in &idl.instructions {
        for _ in 0..ROUNDS {
            if deadline.expired() {
                break;
            }
            let accounts = random_accounts(&mut rng);
            let mut body = Vec::new();
            let expected = match (Gen { rng: &mut rng, idl }).args(ix, &mut body) {
                Ok(v) => v,
                Err(e) => {
                    f.tally.fail(CHECK_ROUNDTRIP, &ix.name, format!("cannot build arguments: {e}"));
                    break;
                }
            };
            let mut data = ix.discriminator.to_vec();
            data.extend_from_slice(&body);

            // Round trip.
            let c = f.run(&pid, &data, &accounts, "a generated instruction");
            let r = match (&c.outcome, &c.strict) {
                (Some(Outcome::Decoded { instruction_type, .. }), Ok(v)) if instruction_type == &ix.name && v == &expected => Ok(()),
                (Some(Outcome::Decoded { instruction_type, .. }), _) if instruction_type != &ix.name => Err(format!(
                    "decoded as '{}', expected '{}'",
                    echo(instruction_type),
                    ix.name
                )),
                (Some(Outcome::Decoded { .. }), Ok(v)) => Err(format!(
                    "{}: {}",
                    crate::manifest::to_hex(&data[..data.len().min(48)]),
                    crate::fixtures::first_difference(&expected, v).map_or_else(String::new, |d| d.to_string())
                )),
                _ => Err(format!(
                    "{} bytes {}: expected a decode, got {}",
                    data.len(),
                    crate::manifest::to_hex(&data[..data.len().min(48)]),
                    c.describe()
                )),
            };
            f.tally.check(CHECK_ROUNDTRIP, &ix.name, r);

            // Trailing bytes.
            let extra = 1 + rng.below(16) as usize;
            let mut longer = data.clone();
            let mut tail = vec![0u8; extra];
            fill(&mut rng, &mut tail);
            longer.extend_from_slice(&tail);
            let t = f.run(&pid, &longer, &accounts, "an instruction with trailing bytes");
            f.tally.check(
                CHECK_TRAILING,
                &ix.name,
                if t.outcome == c.outcome { Ok(()) } else { Err(format!("{extra} trailing bytes turned {} into {}", c.describe(), t.describe())) },
            );

            // Truncation.
            if !body.is_empty() {
                let keep = rng.below(body.len() as u64) as usize;
                let short = &data[..8 + keep];
                let s = f.run(&pid, short, &accounts, "a truncated instruction");
                f.tally.check(
                    CHECK_TRUNCATION,
                    &ix.name,
                    match s.outcome {
                        Some(Outcome::DecodeError(_)) => Ok(()),
                        _ => Err(format!(
                            "the body cut from {} to {keep} bytes gave {}, expected a decode error",
                            body.len(),
                            s.describe()
                        )),
                    },
                );
            }

            // Another program.
            let o = f.run(&other, &data, &accounts, "an instruction for another program");
            f.tally.check(
                CHECK_FOREIGN,
                &ix.name,
                if o.outcome == Some(Outcome::NotClaimed) { Ok(()) } else { Err(format!("sent to another program it gave {}", o.describe())) },
            );
        }
    }

    // Unknown discriminators.
    let known: Vec<[u8; 8]> = idl.instructions.iter().map(|i| i.discriminator).chain([EVENT_IX_TAG, IDL_IX_TAG]).collect();
    for _ in 0..UNKNOWN_ROUNDS {
        if deadline.expired() {
            break;
        }
        let len = rng.below(41) as usize;
        let mut data = vec![0u8; len];
        fill(&mut rng, &mut data);
        while data.len() >= 8 && known.iter().any(|k| data.starts_with(k)) {
            data[0] = data[0].wrapping_add(1);
        }
        let accounts = random_accounts(&mut rng);
        let u = f.run(&pid, &data, &accounts, "an unknown instruction");
        f.tally.check(
            CHECK_UNKNOWN,
            "",
            if u.outcome == Some(Outcome::NotClaimed) {
                Ok(())
            } else {
                Err(format!(
                    "{} bytes {} gave {}, expected not claimed",
                    data.len(),
                    crate::manifest::to_hex(&data),
                    u.describe()
                ))
            },
        );
    }

    // Fixture inputs, sent elsewhere.
    for case in cases {
        let o = f.run(&other, &case.data, &case.accounts, "a fixture case for another program");
        f.tally.check(
            CHECK_FOREIGN,
            &format!("case {}", case.name),
            if o.outcome == Some(Outcome::NotClaimed) { Ok(()) } else { Err(format!("sent to another program it gave {}", o.describe())) },
        );
    }

    run.checks = f.tally.into_checks();
    run.out_of_time(deadline);
    run.digests.insert(digest::FUZZ_KEY.to_string(), digest::fuzz_digest(&f.outcomes));
    run
}

// ---------------------------------------------------------------------------
// Values: Borsh bytes and the JSON PRISM must make of them
// ---------------------------------------------------------------------------

struct Gen<'a> {
    rng: &'a mut SplitMix64,
    idl: &'a Idl,
}

type Env = BTreeMap<String, GenericArg>;

impl Gen<'_> {
    fn args(&mut self, ix: &IdlInstruction, out: &mut Vec<u8>) -> Result<StrictValue, String> {
        let mut m = BTreeMap::new();
        for (name, ty) in &ix.args {
            let v = self.value(ty, &Env::new(), 0, out)?;
            m.insert(name.clone(), v);
        }
        if out.len() > MAX_BODY {
            return Err(format!("the arguments take more than {MAX_BODY} bytes"));
        }
        Ok(StrictValue::Object(m))
    }

    fn bytes(&mut self, n: usize, out: &mut Vec<u8>) -> Vec<u8> {
        let mut b = vec![0u8; n];
        fill(self.rng, &mut b);
        out.extend_from_slice(&b);
        b
    }

    fn value(&mut self, ty: &Ty, env: &Env, depth: usize, out: &mut Vec<u8>) -> Result<StrictValue, String> {
        if depth > 64 || out.len() > MAX_BODY {
            return Err("the types nest too deeply or grow too large".into());
        }
        let int = |b: &[u8], signed: bool| -> StrictValue {
            let mut wide = [if signed && b[b.len() - 1] & 0x80 != 0 { 0xff } else { 0 }; 16];
            wide[..b.len()].copy_from_slice(b);
            StrictValue::Int(i128::from_le_bytes(wide))
        };
        Ok(match ty {
            Ty::Bool => {
                let b = self.rng.below(2) as u8;
                out.push(b);
                StrictValue::Bool(b == 1)
            }
            Ty::U8 => int(&self.bytes(1, out), false),
            Ty::I8 => int(&self.bytes(1, out), true),
            Ty::U16 => int(&self.bytes(2, out), false),
            Ty::I16 => int(&self.bytes(2, out), true),
            Ty::U32 => int(&self.bytes(4, out), false),
            Ty::I32 => int(&self.bytes(4, out), true),
            Ty::U64 => int(&self.bytes(8, out), false),
            Ty::I64 => int(&self.bytes(8, out), true),
            Ty::U128 => StrictValue::Str(le_decimal(&self.bytes(16, out), false)),
            Ty::I128 => StrictValue::Str(le_decimal(&self.bytes(16, out), true)),
            Ty::U256 => StrictValue::Str(le_decimal(&self.bytes(32, out), false)),
            Ty::I256 => StrictValue::Str(le_decimal(&self.bytes(32, out), true)),
            Ty::F32 | Ty::F64 => return Err("floats cannot be pinned".into()),
            Ty::Str => {
                let n = self.rng.below(MAX_TEXT + 1) as usize;
                let s: String = (0..n).map(|_| char::from(0x20 + self.rng.below(95) as u8)).collect();
                out.extend_from_slice(&(n as u32).to_le_bytes());
                out.extend_from_slice(s.as_bytes());
                StrictValue::Str(s)
            }
            Ty::Bytes => {
                let n = self.rng.below(MAX_TEXT + 1) as usize;
                out.extend_from_slice(&(n as u32).to_le_bytes());
                let b = self.bytes(n, out);
                StrictValue::Str(base64(&b))
            }
            Ty::Pubkey => StrictValue::Str(bs58::encode(self.bytes(32, out)).into_string()),
            Ty::Option(inner) => {
                if depth >= MAX_DEPTH || self.rng.below(2) == 0 {
                    out.push(0);
                    StrictValue::Null
                } else {
                    out.push(1);
                    self.value(inner, env, depth + 1, out)?
                }
            }
            Ty::Vec(inner) => {
                let start = out.len();
                let n = if depth >= MAX_DEPTH { 0 } else { self.rng.below(MAX_VEC + 1) };
                out.extend_from_slice(&(n as u32).to_le_bytes());
                let mut items = Vec::new();
                for _ in 0..n {
                    let before = out.len();
                    items.push(self.value(inner, env, depth + 1, out)?);
                    if out.len() == before {
                        // PRISM refuses a non-empty vec of zero-sized
                        // elements; only the empty one is decodable.
                        out.truncate(start);
                        out.extend_from_slice(&0u32.to_le_bytes());
                        return Ok(StrictValue::Array(Vec::new()));
                    }
                }
                StrictValue::Array(items)
            }
            Ty::Array(inner, len) => {
                let n = match len {
                    Len::Value(n) => *n,
                    Len::Generic(g) => match env.get(g) {
                        Some(GenericArg::Const(v)) => v.parse().map_err(|_| format!("length {} is not a number", echo(v)))?,
                        _ => return Err(format!("length {} is unbound", echo(g))),
                    },
                };
                if n > MAX_BODY {
                    return Err(format!("an array of {n} elements is too large"));
                }
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.value(inner, env, depth + 1, out)?);
                }
                StrictValue::Array(items)
            }
            Ty::Generic(g) => match env.get(g) {
                Some(GenericArg::Type(t)) => {
                    let t = t.clone();
                    self.value(&t, &Env::new(), depth + 1, out)?
                }
                _ => return Err(format!("generic {} is unbound", echo(g))),
            },
            Ty::Defined { name, generics } => {
                let def = self.idl.types.get(name).ok_or_else(|| format!("type {} is undefined", echo(name)))?;
                let mut inner = Env::new();
                for (p, a) in def.generics.iter().zip(generics) {
                    let bound = match a {
                        GenericArg::Type(t) => GenericArg::Type(substitute(t, env).ok_or("unbound generic argument")?),
                        GenericArg::Const(v) => match env.get(v) {
                            Some(GenericArg::Const(outer)) => GenericArg::Const(outer.clone()),
                            _ => GenericArg::Const(v.clone()),
                        },
                    };
                    inner.insert(p.name().to_string(), bound);
                }
                match &def.body {
                    TypeBody::Struct(fields) => self.fields(fields, &inner, depth + 1, out)?,
                    TypeBody::Alias(t) => self.value(t, &inner, depth + 1, out)?,
                    TypeBody::Enum(variants) => {
                        if variants.is_empty() || variants.len() > 256 {
                            return Err(format!("enum {} has {} variants", echo(name), variants.len()));
                        }
                        // Past the depth limit, prefer a variant without
                        // fields so recursive enums terminate.
                        let pick = if depth >= MAX_DEPTH {
                            variants.iter().position(|(_, f)| *f == Fields::Unit).unwrap_or(0)
                        } else {
                            self.rng.below(variants.len() as u64) as usize
                        };
                        out.push(pick as u8);
                        let (vname, fields) = &variants[pick];
                        match fields {
                            Fields::Unit => StrictValue::Str(vname.clone()),
                            f => {
                                let v = self.fields(f, &inner, depth + 1, out)?;
                                StrictValue::Object([(vname.clone(), v)].into_iter().collect())
                            }
                        }
                    }
                }
            }
        })
    }

    fn fields(&mut self, f: &Fields, env: &Env, depth: usize, out: &mut Vec<u8>) -> Result<StrictValue, String> {
        Ok(match f {
            Fields::Unit => StrictValue::Object(BTreeMap::new()),
            Fields::Named(fs) => {
                let mut m = BTreeMap::new();
                for (n, t) in fs {
                    let v = self.value(t, env, depth, out)?;
                    m.insert(n.clone(), v);
                }
                StrictValue::Object(m)
            }
            Fields::Tuple(ts) => StrictValue::Array(
                ts.iter()
                    .map(|t| self.value(t, env, depth, out))
                    .collect::<Result<_, _>>()?,
            ),
        })
    }
}

/// A little-endian integer of any width in decimal; `signed` reads two's
/// complement. Written here, independently of PRISM's.
pub fn le_decimal(bytes: &[u8], signed: bool) -> String {
    let negative = signed && bytes.last().is_some_and(|b| b & 0x80 != 0);
    // Big-endian magnitude.
    let mut mag: Vec<u8> = bytes.iter().rev().copied().collect();
    if negative {
        for b in mag.iter_mut() {
            *b = !*b;
        }
        for b in mag.iter_mut().rev() {
            let (s, carry) = b.overflowing_add(1);
            *b = s;
            if !carry {
                break;
            }
        }
    }
    let mut digits = Vec::new();
    while mag.iter().any(|&b| b != 0) {
        let mut rem = 0u32;
        for b in mag.iter_mut() {
            let cur = (rem << 8) | u32::from(*b);
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    if digits.is_empty() {
        digits.push(b'0');
    }
    if negative {
        digits.push(b'-');
    }
    digits.reverse();
    String::from_utf8(digits).expect("ASCII")
}

/// Standard base64 with padding.
pub fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(A[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

fn table_index(t: Table) -> usize {
    Table::ALL.iter().position(|x| *x == t).expect("one of ALL")
}

/// Every model invariant (module docs).
pub fn models(
    fixtures: &FixtureSet,
    cases: &[ModelCase],
    sql: &str,
    info: &SqlInfo,
    seed: u64,
    deadline: &Deadline,
) -> StageRun {
    let mut rng = SplitMix64::new(seed);
    let mut run = StageRun::default();
    let mut outcomes = Vec::new();
    let mut exercised = [false; 4];
    let ordered = info.ordered;
    let same = |base: &TypedResult, other: &Result<TypedResult, (Option<digest::ModelFailure>, String)>| match other {
        Ok(r) => base.same_as(r, ordered),
        Err((_, m)) => Err(format!("it failed: {m}")),
    };
    for case in cases {
        if run.out_of_time(deadline) {
            break;
        }
        let chain = match fixtures.resolve_chain(&case.chain) {
            Ok(c) => c,
            Err(e) => {
                run.checks.push(CheckResult::error(CHECK_OUTPUT_TYPES, &case.name, e.to_string()));
                continue;
            }
        };
        let outside = chain.rows_outside(case.from_slot, case.to_slot);
        for (i, n) in outside.iter().enumerate() {
            exercised[i] |= *n > 0;
        }
        let (from, to) = (case.from_slot, case.to_slot);
        let base_r = run_contained(&chain, sql, from, to, 1);
        outcomes.push(outcome_of(&base_r, ordered));
        let base = match &base_r {
            Ok(b) => {
                run.checks.push(CheckResult::pass(CHECK_OUTPUT_TYPES, &case.name));
                b.clone()
            }
            Err((f, m)) => {
                run.checks.push(engine_check(CHECK_OUTPUT_TYPES, &case.name, *f, m));
                continue;
            }
        };

        // Row order.
        let mut shuffled = (*chain).clone();
        rng.shuffle(&mut shuffled.slots);
        rng.shuffle(&mut shuffled.transactions);
        rng.shuffle(&mut shuffled.instructions);
        rng.shuffle(&mut shuffled.token_transfers);
        let r = run_contained(&shuffled, sql, from, to, 1);
        outcomes.push(outcome_of(&r, ordered));
        run.checks.push(CheckResult::from_result(
            CHECK_ROW_ORDER,
            &case.name,
            same(&base, &r).map_err(|d| format!("shuffling the input rows changes the result: {d}")),
        ));

        // Bound independence.
        let r = run_contained(&chain.within(from, to), sql, from, to, 1);
        outcomes.push(outcome_of(&r, ordered));
        run.checks.push(CheckResult::from_result(
            CHECK_BOUND_INDEPENDENCE,
            &case.name,
            same(&base, &r).map_err(|d| {
                format!("removing the rows outside slots {from}..={to} changes the result, so the model reads outside its window: {d}")
            }),
        ));

        // Partitions, for generated chains.
        if matches!(case.chain, ChainRef::Generated(_)) {
            let mut verdict = Ok(());
            for p in EXTRA_PARTITIONS {
                let r = run_contained(&chain, sql, from, to, p);
                outcomes.push(outcome_of(&r, ordered));
                if verdict.is_ok() {
                    verdict = same(&base, &r).map_err(|d| format!("{p} partitions differ from 1: {d}"));
                }
            }
            run.checks.push(CheckResult::from_result(CHECK_PARTITION_STABLE, &case.name, verdict));
        }
    }
    for t in &info.tables {
        let ok = exercised[table_index(*t)];
        run.checks.push(CheckResult::from_result(
            CHECK_BOUND_NOT_EXERCISED,
            &format!("solana.{}", t.as_str()),
            if ok {
                Ok(())
            } else {
                Err(format!(
                    "no case's chain has {} rows outside its window, so nothing shows that the model \
                     ignores them; add a case whose window leaves some out",
                    t.as_str()
                ))
            },
        ));
    }
    run.digests.insert(INVARIANTS_KEY.to_string(), digest::fuzz_digest(&outcomes));
    run
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn le_decimal_matches_native_integers() {
        let mut r = SplitMix64::new(11);
        for _ in 0..500 {
            let x = (u128::from(r.next_u64()) << 64) | u128::from(r.next_u64());
            assert_eq!(le_decimal(&x.to_le_bytes(), false), x.to_string());
            assert_eq!(le_decimal(&x.to_le_bytes(), true), (x as i128).to_string());
            let y = r.next_u64();
            assert_eq!(le_decimal(&y.to_le_bytes(), false), y.to_string());
        }
        assert_eq!(le_decimal(&[0; 32], true), "0");
        assert_eq!(le_decimal(&[0xff; 32], true), "-1");
        assert_eq!(
            le_decimal(&[0xff; 32], false),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        let mut min = [0u8; 32];
        min[31] = 0x80;
        assert_eq!(
            le_decimal(&min, true),
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
        );
    }

    #[test]
    fn base64_matches_rfc_4648_vectors() {
        for (i, o) in [("", ""), ("f", "Zg=="), ("fo", "Zm8="), ("foo", "Zm9v"), ("foob", "Zm9vYg=="), ("fooba", "Zm9vYmE="), ("foobar", "Zm9vYmFy")] {
            assert_eq!(base64(i.as_bytes()), o);
        }
        assert_eq!(base64(&[0xff, 0xfe, 0xfd]), "//79");
    }

    #[test]
    fn the_seed_is_the_first_8_id_bytes_little_endian() {
        let mut id = [0u8; 32];
        id[0] = 1;
        id[7] = 0x80;
        id[8] = 0xff;
        assert_eq!(seed(&ArtifactId(id)), 0x8000_0000_0000_0001);
    }
}
