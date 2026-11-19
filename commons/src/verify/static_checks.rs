//! Stage 1: checks on the content alone, before anything is run on
//! fixtures. Runs on a loaded [`Artifact`], so the manifest, the file
//! rules and the content hash have already passed.
//!
//! # Decoders (`idl.json`)
//!
//! The IDL is read here independently of PRISM, into [`Idl`], which the
//! fuzz stage also uses to build instructions. Then PRISM must agree.
//!
//! | Check                        | The IDL must                                                       |
//! |------------------------------|--------------------------------------------------------------------|
//! | `static.idl_json`            | be strict JSON (`canon.rs`): one spelling, no duplicate keys, an object |
//! | `static.idl_shape`           | be the Anchor 0.30+ format: only the known top-level keys, `metadata.spec` `"0.1.0"`, `instructions` and `types` arrays |
//! | `static.idl_address`         | have `address` equal to the manifest's `program_id`                |
//! | `static.idl_instructions`    | list at least one instruction; each with a name a fixture can spell (`[A-Za-z][A-Za-z0-9_]{0,63}`), unique, and an explicit 8-byte discriminator, unique, that is neither of Anchor's reserved `emit_cpi!` and IDL-account prefixes |
//! | `static.idl_types`           | give every argument a type this build reads, name only defined types, bind generics fully, and reach no `f32`/`f64` from any argument (fixtures cannot pin a float) |
//! | `static.idl_loads`           | load in PRISM's decoder                                            |
//! | `static.idl_backend_agrees`  | give PRISM the same instructions and discriminators as read here   |
//!
//! # Models (`model.sql`)
//!
//! | Check              | The SQL must                                                     |
//! |--------------------|------------------------------------------------------------------|
//! | `static.sql_text`  | be printable ASCII, tabs and newlines (no look-alike or invisible characters a reviewer would miss) |
//! | `static.sql_*`     | pass [`sql_guard::analyse`] (see `sql_guard.rs`)                 |

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::config::echo;
use crate::fixtures::{ParseLimits, StrictValue, parse_strict};
use crate::manifest::{Artifact, Kind};

use super::backend::{DecoderBackend, InstructionInfo};
use super::report::{CheckResult, excerpt};
use super::sql_guard::{self, GUARD_CHECKS, SqlInfo};

pub const CHECK_IDL_JSON: &str = "static.idl_json";
pub const CHECK_IDL_SHAPE: &str = "static.idl_shape";
pub const CHECK_IDL_ADDRESS: &str = "static.idl_address";
pub const CHECK_IDL_INSTRUCTIONS: &str = "static.idl_instructions";
pub const CHECK_IDL_TYPES: &str = "static.idl_types";
pub const CHECK_IDL_LOADS: &str = "static.idl_loads";
pub const CHECK_IDL_BACKEND: &str = "static.idl_backend_agrees";
pub const CHECK_SQL_TEXT: &str = "static.sql_text";

/// The only IDL spec version this build reads.
pub const IDL_SPEC: &str = "0.1.0";
/// `emit_cpi!` self-invocations start with this (Anchor's `EVENT_IX_TAG`,
/// little-endian).
pub const EVENT_IX_TAG: [u8; 8] = 0x1d9a_cb51_2ea5_45e4_u64.to_le_bytes();
/// The legacy IDL-account instructions start with this (`IDL_IX_TAG`).
pub const IDL_IX_TAG: [u8; 8] = 0x0a69_e9a7_78bc_f440_u64.to_le_bytes();
/// Longest instruction name: a fixture's `instruction_type` limit.
pub const MAX_INSTRUCTION_NAME: usize = 64;

const TOP_KEYS: [&str; 9] = [
    "address", "metadata", "docs", "instructions", "accounts", "events", "errors", "types", "constants",
];

// ---------------------------------------------------------------------------
// The IDL, read independently
// ---------------------------------------------------------------------------

/// An IDL type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Ty {
    Bool,
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    U128,
    I128,
    U256,
    I256,
    F32,
    F64,
    Bytes,
    Str,
    Pubkey,
    Option(Box<Ty>),
    Vec(Box<Ty>),
    Array(Box<Ty>, Len),
    Defined { name: String, generics: Vec<GenericArg> },
    Generic(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Len {
    Value(usize),
    Generic(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericArg {
    Type(Ty),
    Const(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenericParam {
    Type(String),
    Const(String),
}

impl GenericParam {
    pub fn name(&self) -> &str {
        match self {
            GenericParam::Type(n) | GenericParam::Const(n) => n,
        }
    }
}

/// The fields of a struct or an enum variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fields {
    /// No `fields` key: a unit struct (`{}`) or a unit variant (its name).
    Unit,
    Named(Vec<(String, Ty)>),
    Tuple(Vec<Ty>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeBody {
    Struct(Fields),
    Enum(Vec<(String, Fields)>),
    Alias(Ty),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeDef {
    pub generics: Vec<GenericParam>,
    /// `serialization` is absent or `"borsh"`.
    pub borsh: bool,
    pub body: TypeBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdlInstruction {
    pub name: String,
    pub discriminator: [u8; 8],
    pub args: Vec<(String, Ty)>,
}

/// What `verify/` needs of an IDL: its address, its instructions, its types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Idl {
    pub address: String,
    /// In IDL order.
    pub instructions: Vec<IdlInstruction>,
    pub types: BTreeMap<String, TypeDef>,
}

impl Idl {
    /// `(name, discriminator)` of every instruction, sorted by name: what
    /// the backend must report.
    pub fn infos(&self) -> Vec<InstructionInfo> {
        let mut v: Vec<InstructionInfo> = self
            .instructions
            .iter()
            .map(|i| InstructionInfo {
                name: i.name.clone(),
                discriminator: i.discriminator,
            })
            .collect();
        v.sort();
        v
    }
}

/// A problem with one check, at a path in the IDL.
#[derive(Debug)]
struct IdlIssue {
    check: &'static str,
    reason: String,
}

fn issue(check: &'static str, at: &str, reason: impl AsRef<str>) -> IdlIssue {
    let at = if at.is_empty() { "top level" } else { at };
    IdlIssue {
        check,
        reason: format!("{at}: {}", reason.as_ref()),
    }
}

fn obj<'a>(v: &'a StrictValue, check: &'static str, at: &str) -> Result<&'a BTreeMap<String, StrictValue>, IdlIssue> {
    match v {
        StrictValue::Object(m) => Ok(m),
        other => Err(issue(check, at, format!("must be an object, not {}", other.type_name()))),
    }
}

fn arr<'a>(v: &'a StrictValue, check: &'static str, at: &str) -> Result<&'a [StrictValue], IdlIssue> {
    match v {
        StrictValue::Array(a) => Ok(a),
        other => Err(issue(check, at, format!("must be an array, not {}", other.type_name()))),
    }
}

fn string<'a>(v: &'a StrictValue, check: &'static str, at: &str) -> Result<&'a str, IdlIssue> {
    match v {
        StrictValue::Str(s) => Ok(s),
        other => Err(issue(check, at, format!("must be a string, not {}", other.type_name()))),
    }
}

fn get<'a>(m: &'a BTreeMap<String, StrictValue>, key: &str, check: &'static str, at: &str) -> Result<&'a StrictValue, IdlIssue> {
    m.get(key).ok_or_else(|| issue(check, &join(at, key), "is missing"))
}

fn join(at: &str, key: &str) -> String {
    crate::fixtures::join_key(at, key)
}

fn idx(at: &str, i: usize) -> String {
    crate::fixtures::join_index(at, i)
}

fn parse_ty(v: &StrictValue, at: &str) -> Result<Ty, IdlIssue> {
    let c = CHECK_IDL_TYPES;
    match v {
        StrictValue::Str(s) => Ok(match s.as_str() {
            "bool" => Ty::Bool,
            "u8" => Ty::U8,
            "i8" => Ty::I8,
            "u16" => Ty::U16,
            "i16" => Ty::I16,
            "u32" => Ty::U32,
            "i32" => Ty::I32,
            "u64" => Ty::U64,
            "i64" => Ty::I64,
            "u128" => Ty::U128,
            "i128" => Ty::I128,
            "u256" => Ty::U256,
            "i256" => Ty::I256,
            "f32" => Ty::F32,
            "f64" => Ty::F64,
            "bytes" => Ty::Bytes,
            "string" => Ty::Str,
            "pubkey" => Ty::Pubkey,
            other => return Err(issue(c, at, format!("'{}' is not an IDL type", echo(other)))),
        }),
        StrictValue::Object(m) if m.len() == 1 => {
            let (k, inner) = m.iter().next().expect("one entry");
            let here = join(at, k);
            match k.as_str() {
                "option" => Ok(Ty::Option(Box::new(parse_ty(inner, &here)?))),
                "vec" => Ok(Ty::Vec(Box::new(parse_ty(inner, &here)?))),
                "array" => match arr(inner, c, &here)? {
                    [t, n] => {
                        let len = match n {
                            StrictValue::Int(n) => Len::Value(
                                usize::try_from(*n).map_err(|_| issue(c, &idx(&here, 1), "is not an array length"))?,
                            ),
                            StrictValue::Object(g) if g.len() == 1 && g.contains_key("generic") => {
                                Len::Generic(string(&g["generic"], c, &idx(&here, 1))?.to_string())
                            }
                            _ => return Err(issue(c, &idx(&here, 1), "must be a length or {\"generic\": name}")),
                        };
                        Ok(Ty::Array(Box::new(parse_ty(t, &idx(&here, 0))?), len))
                    }
                    _ => Err(issue(c, &here, "must be [type, length]")),
                },
                "defined" => {
                    let d = obj(inner, c, &here)?;
                    if let Some(k) = d.keys().find(|k| *k != "name" && *k != "generics") {
                        return Err(issue(c, &here, format!("unknown key '{}'", echo(k))));
                    }
                    let name = string(get(d, "name", c, &here)?, c, &join(&here, "name"))?.to_string();
                    let mut generics = Vec::new();
                    if let Some(g) = d.get("generics") {
                        for (i, a) in arr(g, c, &join(&here, "generics"))?.iter().enumerate() {
                            let ga = idx(&join(&here, "generics"), i);
                            let a = obj(a, c, &ga)?;
                            match string(get(a, "kind", c, &ga)?, c, &join(&ga, "kind"))? {
                                "type" => generics.push(GenericArg::Type(parse_ty(get(a, "type", c, &ga)?, &join(&ga, "type"))?)),
                                "const" => generics.push(GenericArg::Const(
                                    string(get(a, "value", c, &ga)?, c, &join(&ga, "value"))?.to_string(),
                                )),
                                other => return Err(issue(c, &join(&ga, "kind"), format!("'{}' is not a generic kind", echo(other)))),
                            }
                        }
                    }
                    Ok(Ty::Defined { name, generics })
                }
                "generic" => Ok(Ty::Generic(string(inner, c, &here)?.to_string())),
                other => Err(issue(c, at, format!("'{}' is not an IDL type", echo(other)))),
            }
        }
        other => Err(issue(c, at, format!("is not an IDL type ({})", other.type_name()))),
    }
}

fn parse_fields(v: Option<&StrictValue>, at: &str) -> Result<Fields, IdlIssue> {
    let c = CHECK_IDL_TYPES;
    let Some(v) = v else { return Ok(Fields::Unit) };
    let items = arr(v, c, at)?;
    let named = items.iter().all(|f| matches!(f, StrictValue::Object(m) if m.contains_key("name") && m.contains_key("type")));
    if named {
        let mut out = Vec::new();
        for (i, f) in items.iter().enumerate() {
            let fa = idx(at, i);
            let m = obj(f, c, &fa)?;
            out.push((
                string(&m["name"], c, &join(&fa, "name"))?.to_string(),
                parse_ty(&m["type"], &join(&fa, "type"))?,
            ));
        }
        Ok(Fields::Named(out))
    } else {
        Ok(Fields::Tuple(
            items
                .iter()
                .enumerate()
                .map(|(i, t)| parse_ty(t, &idx(at, i)))
                .collect::<Result<_, _>>()?,
        ))
    }
}

fn parse_typedef(v: &StrictValue, at: &str) -> Result<(String, TypeDef), IdlIssue> {
    let c = CHECK_IDL_TYPES;
    let m = obj(v, c, at)?;
    let name = string(get(m, "name", c, at)?, c, &join(at, "name"))?.to_string();
    let borsh = match m.get("serialization") {
        None => true,
        Some(s) => string(s, c, &join(at, "serialization"))? == "borsh",
    };
    let mut generics = Vec::new();
    if let Some(g) = m.get("generics") {
        for (i, p) in arr(g, c, &join(at, "generics"))?.iter().enumerate() {
            let pa = idx(&join(at, "generics"), i);
            let p = obj(p, c, &pa)?;
            let n = string(get(p, "name", c, &pa)?, c, &join(&pa, "name"))?.to_string();
            match string(get(p, "kind", c, &pa)?, c, &join(&pa, "kind"))? {
                "type" => generics.push(GenericParam::Type(n)),
                "const" => generics.push(GenericParam::Const(n)),
                other => return Err(issue(c, &join(&pa, "kind"), format!("'{}' is not a generic kind", echo(other)))),
            }
        }
    }
    let ta = join(at, "type");
    let t = obj(get(m, "type", c, at)?, c, &ta)?;
    let body = match string(get(t, "kind", c, &ta)?, c, &join(&ta, "kind"))? {
        "struct" => TypeBody::Struct(parse_fields(t.get("fields"), &join(&ta, "fields"))?),
        "enum" => {
            let va = join(&ta, "variants");
            let mut variants = Vec::new();
            for (i, v) in arr(get(t, "variants", c, &ta)?, c, &va)?.iter().enumerate() {
                let vi = idx(&va, i);
                let v = obj(v, c, &vi)?;
                let vn = string(get(v, "name", c, &vi)?, c, &join(&vi, "name"))?.to_string();
                variants.push((vn, parse_fields(v.get("fields"), &join(&vi, "fields"))?));
            }
            TypeBody::Enum(variants)
        }
        "type" => TypeBody::Alias(parse_ty(get(t, "alias", c, &ta)?, &join(&ta, "alias"))?),
        other => return Err(issue(c, &join(&ta, "kind"), format!("'{}' is not a type kind", echo(other)))),
    };
    Ok((name, TypeDef { generics, borsh, body }))
}

fn is_instruction_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_INSTRUCTION_NAME
        && s.as_bytes()[0].is_ascii_alphabetic()
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Read `bytes` as an IDL, in check order; the first problem of each kind
/// stops the reading.
fn read_idl(bytes: &[u8], program_id: &[u8; 32]) -> Result<Idl, IdlIssue> {
    let v = parse_strict(bytes, &ParseLimits::DEFAULT)
        .map_err(|e| IdlIssue { check: CHECK_IDL_JSON, reason: e.to_string() })?;
    let top = obj(&v, CHECK_IDL_JSON, "")?;

    // Shape.
    let s = CHECK_IDL_SHAPE;
    if let Some(k) = top.keys().find(|k| !TOP_KEYS.contains(&k.as_str())) {
        return Err(issue(s, "", format!("unknown key '{}' (expected {})", echo(k), TOP_KEYS.join(", "))));
    }
    let meta = obj(get(top, "metadata", s, "")?, s, "metadata")?;
    let spec = string(get(meta, "spec", s, "metadata")?, s, "metadata.spec")?;
    if spec != IDL_SPEC {
        return Err(issue(
            s,
            "metadata.spec",
            format!("is '{}'; this build reads Anchor 0.30+ IDLs, spec {IDL_SPEC}", echo(spec)),
        ));
    }
    let instructions = arr(get(top, "instructions", s, "")?, s, "instructions")?;
    let types = match top.get("types") {
        Some(t) => arr(t, s, "types")?,
        None => &[],
    };

    // Address.
    let address = string(get(top, "address", CHECK_IDL_ADDRESS, "")?, CHECK_IDL_ADDRESS, "address")?;
    let expected = bs58::encode(program_id).into_string();
    if address != expected {
        return Err(issue(
            CHECK_IDL_ADDRESS,
            "address",
            format!("is '{}' but the manifest's program_id is {expected}", echo(address)),
        ));
    }

    // Instructions.
    let c = CHECK_IDL_INSTRUCTIONS;
    if instructions.is_empty() {
        return Err(issue(c, "instructions", "is empty; a decoder must decode something"));
    }
    let mut names = BTreeSet::new();
    let mut discs: BTreeMap<[u8; 8], String> = BTreeMap::new();
    let mut raw_args = Vec::new();
    let mut parsed = Vec::new();
    for (i, ix) in instructions.iter().enumerate() {
        let at = idx("instructions", i);
        let m = obj(ix, c, &at)?;
        let name = string(get(m, "name", c, &at)?, c, &join(&at, "name"))?;
        if !is_instruction_name(name) {
            return Err(issue(
                c,
                &join(&at, "name"),
                format!(
                    "'{}' is not an instruction name a fixture can spell ([A-Za-z][A-Za-z0-9_], at most {MAX_INSTRUCTION_NAME})",
                    echo(name)
                ),
            ));
        }
        if !names.insert(name.to_string()) {
            return Err(issue(c, &join(&at, "name"), format!("'{name}' appears twice")));
        }
        let da = join(&at, "discriminator");
        let d = arr(get(m, "discriminator", c, &at)?, c, &da)?;
        let bytes: Vec<u8> = d
            .iter()
            .map(|b| match b {
                StrictValue::Int(n) => u8::try_from(*n).ok(),
                _ => None,
            })
            .collect::<Option<_>>()
            .ok_or_else(|| issue(c, &da, "must be an array of bytes"))?;
        let disc: [u8; 8] = bytes.as_slice().try_into().map_err(|_| {
            issue(
                c,
                &da,
                format!("has {} bytes; every instruction needs an explicit 8-byte discriminator", bytes.len()),
            )
        })?;
        if disc == EVENT_IX_TAG || disc == IDL_IX_TAG {
            return Err(issue(c, &da, "is one of Anchor's reserved prefixes (emit_cpi! or the IDL account)"));
        }
        if let Some(other) = discs.insert(disc, name.to_string()) {
            return Err(issue(c, &da, format!("is also the discriminator of '{other}'")));
        }
        raw_args.push((at.clone(), m.get("args")));
        parsed.push((name.to_string(), disc));
    }

    // Types.
    let mut defs = BTreeMap::new();
    for (i, t) in types.iter().enumerate() {
        let (name, def) = parse_typedef(t, &idx("types", i))?;
        if defs.insert(name.clone(), def).is_some() {
            return Err(issue(CHECK_IDL_TYPES, &idx("types", i), format!("type '{}' is defined twice", echo(&name))));
        }
    }
    let mut out = Vec::new();
    for ((name, disc), (at, args)) in parsed.into_iter().zip(raw_args) {
        let aa = join(&at, "args");
        let args = match args {
            Some(a) => arr(a, CHECK_IDL_TYPES, &aa)?,
            None => return Err(issue(CHECK_IDL_TYPES, &aa, "is missing")),
        };
        let mut list = Vec::new();
        for (j, a) in args.iter().enumerate() {
            let ai = idx(&aa, j);
            let m = obj(a, CHECK_IDL_TYPES, &ai)?;
            let an = string(get(m, "name", CHECK_IDL_TYPES, &ai)?, CHECK_IDL_TYPES, &join(&ai, "name"))?;
            let ty = parse_ty(get(m, "type", CHECK_IDL_TYPES, &ai)?, &join(&ai, "type"))?;
            check_reachable(&ty, &defs, &BTreeMap::new(), &mut BTreeSet::new(), &join(&ai, "type"))?;
            list.push((an.to_string(), ty));
        }
        out.push(IdlInstruction { name, discriminator: disc, args: list });
    }
    Ok(Idl {
        address: address.to_string(),
        instructions: out,
        types: defs,
    })
}

/// Every type reachable from `ty` is defined, fully bound, and not a float.
fn check_reachable(
    ty: &Ty,
    defs: &BTreeMap<String, TypeDef>,
    env: &BTreeMap<String, GenericArg>,
    visiting: &mut BTreeSet<String>,
    at: &str,
) -> Result<(), IdlIssue> {
    let c = CHECK_IDL_TYPES;
    match ty {
        Ty::F32 | Ty::F64 => Err(issue(c, at, "reaches a float (f32/f64); a fixture cannot pin a float, so no case could pass")),
        Ty::Option(t) | Ty::Vec(t) => check_reachable(t, defs, env, visiting, at),
        Ty::Array(t, len) => {
            if let Len::Generic(n) = len
                && !matches!(env.get(n), Some(GenericArg::Const(v)) if v.parse::<usize>().is_ok())
            {
                return Err(issue(c, at, format!("array length '{}' is not bound to a number", echo(n))));
            }
            check_reachable(t, defs, env, visiting, at)
        }
        Ty::Generic(n) => match env.get(n) {
            Some(GenericArg::Type(t)) => check_reachable(t, defs, &BTreeMap::new(), visiting, at),
            _ => Err(issue(c, at, format!("generic '{}' is not bound to a type", echo(n)))),
        },
        Ty::Defined { name, generics } => {
            let def = defs.get(name).ok_or_else(|| issue(c, at, format!("names undefined type '{}'", echo(name))))?;
            if !def.borsh {
                return Err(issue(c, at, format!("type '{}' is not borsh-serialised", echo(name))));
            }
            if def.generics.len() != generics.len() {
                return Err(issue(
                    c,
                    at,
                    format!("type '{}' takes {} generic arguments, got {}", echo(name), def.generics.len(), generics.len()),
                ));
            }
            // Bind the arguments in the caller's scope.
            let mut inner = BTreeMap::new();
            for (p, a) in def.generics.iter().zip(generics) {
                let bound = match (p, a) {
                    (GenericParam::Type(_), GenericArg::Type(t)) => GenericArg::Type(substitute(t, env).ok_or_else(|| {
                        issue(c, at, format!("an argument of '{}' is not bound", echo(name)))
                    })?),
                    (GenericParam::Const(_), GenericArg::Const(v)) => match env.get(v) {
                        Some(GenericArg::Const(outer)) => GenericArg::Const(outer.clone()),
                        _ => GenericArg::Const(v.clone()),
                    },
                    _ => return Err(issue(c, at, format!("generic '{}' of '{}' has the wrong kind", echo(p.name()), echo(name)))),
                };
                inner.insert(p.name().to_string(), bound);
            }
            let key = format!("{name}{inner:?}");
            if !visiting.insert(key.clone()) {
                return Ok(());
            }
            let r = match &def.body {
                TypeBody::Struct(f) => check_fields(f, defs, &inner, visiting, at),
                TypeBody::Enum(vs) => vs.iter().try_for_each(|(_, f)| check_fields(f, defs, &inner, visiting, at)),
                TypeBody::Alias(t) => check_reachable(t, defs, &inner, visiting, at),
            };
            visiting.remove(&key);
            r
        }
        _ => Ok(()),
    }
}

fn check_fields(
    f: &Fields,
    defs: &BTreeMap<String, TypeDef>,
    env: &BTreeMap<String, GenericArg>,
    visiting: &mut BTreeSet<String>,
    at: &str,
) -> Result<(), IdlIssue> {
    match f {
        Fields::Unit => Ok(()),
        Fields::Named(fs) => fs.iter().try_for_each(|(_, t)| check_reachable(t, defs, env, visiting, at)),
        Fields::Tuple(ts) => ts.iter().try_for_each(|t| check_reachable(t, defs, env, visiting, at)),
    }
}

/// `ty` with every generic in `env` replaced; `None` if one is unbound.
pub fn substitute(ty: &Ty, env: &BTreeMap<String, GenericArg>) -> Option<Ty> {
    Some(match ty {
        Ty::Generic(n) => match env.get(n)? {
            GenericArg::Type(t) => t.clone(),
            GenericArg::Const(_) => return None,
        },
        Ty::Option(t) => Ty::Option(Box::new(substitute(t, env)?)),
        Ty::Vec(t) => Ty::Vec(Box::new(substitute(t, env)?)),
        Ty::Array(t, len) => {
            let len = match len {
                Len::Generic(n) => match env.get(n)? {
                    GenericArg::Const(v) => Len::Value(v.parse().ok()?),
                    GenericArg::Type(_) => return None,
                },
                l => l.clone(),
            };
            Ty::Array(Box::new(substitute(t, env)?), len)
        }
        Ty::Defined { name, generics } => Ty::Defined {
            name: name.clone(),
            generics: generics
                .iter()
                .map(|g| {
                    Some(match g {
                        GenericArg::Type(t) => GenericArg::Type(substitute(t, env)?),
                        GenericArg::Const(v) => match env.get(v) {
                            Some(GenericArg::Const(outer)) => GenericArg::Const(outer.clone()),
                            _ => GenericArg::Const(v.clone()),
                        },
                    })
                })
                .collect::<Option<_>>()?,
        },
        other => other.clone(),
    })
}

// ---------------------------------------------------------------------------
// The stage
// ---------------------------------------------------------------------------

/// What the static stage learned, for the stages after it.
#[derive(Debug, Clone)]
pub enum Learned {
    Decoder { idl: Idl, program_id: [u8; 32] },
    Model { sql: String, info: SqlInfo },
}

/// Run every static check on `artifact`. `learned` is `Some` exactly when
/// every check passed.
pub fn run(artifact: &Artifact, backend: &dyn DecoderBackend) -> (Vec<CheckResult>, Option<Learned>) {
    match artifact.manifest.kind {
        Kind::Decoder => decoder(artifact, backend),
        Kind::Model => model(artifact),
    }
}

const IDL_CHECKS: [&str; 7] = [
    CHECK_IDL_JSON,
    CHECK_IDL_SHAPE,
    CHECK_IDL_ADDRESS,
    CHECK_IDL_INSTRUCTIONS,
    CHECK_IDL_TYPES,
    CHECK_IDL_LOADS,
    CHECK_IDL_BACKEND,
];

fn decoder(artifact: &Artifact, backend: &dyn DecoderBackend) -> (Vec<CheckResult>, Option<Learned>) {
    let Some(program_id) = artifact.manifest.program_id else {
        // manifest::load guarantees it; a decoder without one is a bug.
        return (
            vec![CheckResult::error(CHECK_IDL_ADDRESS, "", "a decoder artifact has no program_id")],
            None,
        );
    };
    // Checks run in IDL_CHECKS order; the first failure ends the stage and
    // the checks after it are not reported, since they could not run.
    let mut out = Vec::new();
    let pass_up_to = |out: &mut Vec<CheckResult>, upto: &str| {
        for c in IDL_CHECKS.iter().take_while(|c| **c != upto) {
            if !out.iter().any(|r: &CheckResult| r.id == *c) {
                out.push(CheckResult::pass(c, ""));
            }
        }
    };
    let idl = match read_idl(&artifact.content, &program_id) {
        Ok(idl) => idl,
        Err(i) => {
            pass_up_to(&mut out, i.check);
            out.push(CheckResult::fail(i.check, "", i.reason));
            return (out, None);
        }
    };
    pass_up_to(&mut out, CHECK_IDL_LOADS);
    let loaded = catch_unwind(AssertUnwindSafe(|| backend.load(&artifact.content, &program_id)));
    let loaded = match loaded {
        Ok(Ok(l)) => l,
        Ok(Err(e)) => {
            out.push(CheckResult::fail(CHECK_IDL_LOADS, "", format!("PRISM's decoder rejects the IDL: {}", echo(&e.0))));
            return (out, None);
        }
        Err(_) => {
            out.push(CheckResult::fail(CHECK_IDL_LOADS, "", "PRISM's decoder panicked loading the IDL"));
            return (out, None);
        }
    };
    out.push(CheckResult::pass(CHECK_IDL_LOADS, ""));
    let theirs = catch_unwind(AssertUnwindSafe(|| loaded.instructions())).unwrap_or_default();
    let ours = idl.infos();
    if theirs != ours {
        let show = |v: &[InstructionInfo]| {
            v.iter()
                .map(|i| format!("{}={}", echo(&i.name), crate::manifest::to_hex(&i.discriminator)))
                .collect::<Vec<_>>()
                .join(", ")
        };
        out.push(CheckResult::fail(
            CHECK_IDL_BACKEND,
            "",
            format!("PRISM reads [{}] but the IDL says [{}]", show(&theirs), show(&ours)),
        ));
        return (out, None);
    }
    out.push(CheckResult::pass(CHECK_IDL_BACKEND, ""));
    (out, Some(Learned::Decoder { idl, program_id }))
}

fn model(artifact: &Artifact) -> (Vec<CheckResult>, Option<Learned>) {
    let mut out = Vec::new();
    let text = String::from_utf8_lossy(&artifact.content).into_owned();
    if let Some((at, c)) = text
        .char_indices()
        .find(|(_, c)| !(matches!(c, ' '..='~') || *c == '\n' || *c == '\t'))
    {
        let line = 1 + text[..at].matches('\n').count() as u64;
        out.push(
            CheckResult::fail(
                CHECK_SQL_TEXT,
                "",
                format!(
                    "line {line} holds U+{:04X}; model SQL is printable ASCII, tabs and newlines only",
                    c as u32
                ),
            )
            .with_excerpt(excerpt(&artifact.content, line)),
        );
        return (out, None);
    }
    out.push(CheckResult::pass(CHECK_SQL_TEXT, ""));
    match sql_guard::analyse(&text) {
        Ok(info) => {
            out.extend(GUARD_CHECKS.iter().map(|c| CheckResult::pass(c, "")));
            (out, Some(Learned::Model { sql: text, info }))
        }
        Err(issues) => {
            for c in GUARD_CHECKS {
                let mine: Vec<_> = issues.iter().filter(|i| i.check == c).collect();
                if mine.is_empty() {
                    out.push(CheckResult::pass(c, ""));
                }
                for i in mine {
                    let r = CheckResult::fail(c, "", &i.reason);
                    out.push(r.with_excerpt(i.line.and_then(|l| excerpt(&artifact.content, l))));
                }
            }
            (out, None)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const ADDRESS: &str = "Fg6PaFpoGXkYsidMpWTK6W2BeZ7FEfcYkg476zPFsLnS";

    fn pid() -> [u8; 32] {
        bs58::decode(ADDRESS).into_vec().unwrap().try_into().unwrap()
    }

    fn idl_with(instructions: &str, types: &str) -> String {
        format!(
            r#"{{"address":"{ADDRESS}","metadata":{{"name":"t","version":"0.1.0","spec":"0.1.0"}},"instructions":[{instructions}],"types":[{types}]}}"#
        )
    }

    const DEPOSIT: &str = r#"{"name":"deposit","discriminator":[242,35,198,137,82,225,242,182],"accounts":[],"args":[{"name":"amount","type":"u64"},{"name":"memo","type":{"option":"string"}}]}"#;

    #[track_caller]
    fn read_err(src: &str) -> (String, String) {
        match read_idl(src.as_bytes(), &pid()) {
            Ok(_) => panic!("accepted: {src}"),
            Err(i) => (i.check.to_string(), i.reason),
        }
    }

    #[test]
    fn reads_the_types_and_instructions() {
        let idl = read_idl(idl_with(DEPOSIT, "").as_bytes(), &pid()).unwrap();
        assert_eq!(idl.instructions[0].args[1], ("memo".to_string(), Ty::Option(Box::new(Ty::Str))));
        assert_eq!(idl.infos()[0].discriminator, [242, 35, 198, 137, 82, 225, 242, 182]);
        let vault = include_str!("../../../prism/tests/fixtures/vault_idl.json");
        // The PRISM test IDL is a stress test: it has a float, custom and
        // missing discriminators. Each is reported, one at a time.
        let (check, reason) = match read_idl(vault.as_bytes(), &pid()) {
            Err(i) => (i.check, i.reason),
            Ok(_) => panic!(),
        };
        assert_eq!(check, CHECK_IDL_INSTRUCTIONS, "{reason}");
        assert!(reason.contains("0 bytes"), "{reason}");
    }

    #[test]
    fn each_rule_names_its_check() {
        for (src, check, part) in [
            ("[1]".to_string(), CHECK_IDL_JSON, "must be an object"),
            ("{\"a\":1,\"a\":1}".to_string(), CHECK_IDL_JSON, "duplicate key"),
            (idl_with(DEPOSIT, "").replace("\"types\"", "\"extra\""), CHECK_IDL_SHAPE, "unknown key 'extra'"),
            (idl_with(DEPOSIT, "").replace("\"spec\":\"0.1.0\"", "\"spec\":\"0.0.9\""), CHECK_IDL_SHAPE, "spec 0.1.0"),
            (idl_with(DEPOSIT, "").replace(ADDRESS, "11111111111111111111111111111111"), CHECK_IDL_ADDRESS, "program_id is"),
            (idl_with("", ""), CHECK_IDL_INSTRUCTIONS, "is empty"),
            (idl_with(&format!("{DEPOSIT},{DEPOSIT}"), ""), CHECK_IDL_INSTRUCTIONS, "appears twice"),
            (idl_with(&DEPOSIT.replace("deposit", "de-posit"), ""), CHECK_IDL_INSTRUCTIONS, "fixture can spell"),
            (idl_with(&DEPOSIT.replace("242,35,198,137,82,225,242,182", "7"), ""), CHECK_IDL_INSTRUCTIONS, "1 bytes"),
            (
                idl_with(&DEPOSIT.replace("242,35,198,137,82,225,242,182", "228,69,165,46,81,203,154,29"), ""),
                CHECK_IDL_INSTRUCTIONS,
                "reserved",
            ),
            (
                idl_with(&format!("{DEPOSIT},{}", DEPOSIT.replace("\"deposit\"", "\"other\"")), ""),
                CHECK_IDL_INSTRUCTIONS,
                "also the discriminator of 'deposit'",
            ),
            (idl_with(&DEPOSIT.replace("\"u64\"", "\"u512\""), ""), CHECK_IDL_TYPES, "'u512' is not an IDL type"),
            (idl_with(&DEPOSIT.replace("\"u64\"", "\"f64\""), ""), CHECK_IDL_TYPES, "float"),
            (
                idl_with(&DEPOSIT.replace("\"u64\"", r#"{"defined":{"name":"Missing"}}"#), ""),
                CHECK_IDL_TYPES,
                "undefined type 'Missing'",
            ),
            (
                idl_with(
                    &DEPOSIT.replace("\"u64\"", r#"{"defined":{"name":"P"}}"#),
                    r#"{"name":"P","type":{"kind":"struct","fields":[{"name":"r","type":{"vec":"f32"}}]}}"#,
                ),
                CHECK_IDL_TYPES,
                "float",
            ),
            (
                idl_with(
                    &DEPOSIT.replace("\"u64\"", r#"{"defined":{"name":"W","generics":[{"kind":"type","type":"f64"}]}}"#),
                    r#"{"name":"W","generics":[{"kind":"type","name":"T"}],"type":{"kind":"struct","fields":[{"name":"x","type":{"generic":"T"}}]}}"#,
                ),
                CHECK_IDL_TYPES,
                "float",
            ),
        ] {
            let (c, reason) = read_err(&src);
            assert_eq!(c, check, "{src}: {reason}");
            assert!(reason.contains(part), "{src}: expected {part:?} in {reason}");
        }
    }

    #[test]
    fn recursive_and_generic_types_are_followed_once() {
        let src = idl_with(
            &DEPOSIT.replace("\"u64\"", r#"{"defined":{"name":"Node"}}"#),
            r#"{"name":"Node","type":{"kind":"struct","fields":[{"name":"next","type":{"option":{"defined":{"name":"Node"}}}}]}},
               {"name":"W","generics":[{"kind":"type","name":"T"},{"kind":"const","name":"N","type":"usize"}],
                "type":{"kind":"struct","fields":[{"name":"x","type":{"array":[{"generic":"T"},{"generic":"N"}]}}]}}"#,
        );
        read_idl(src.as_bytes(), &pid()).unwrap();
    }
}
