//! Pinned golden data: the evidence `verify/` checks contributions against.
//!
//! Nothing here runs a decoder or a SQL model. A fixture is evidence, and a
//! test a contributor can change together with the thing under test proves
//! nothing, so most of this module keeps the evidence honest:
//!
//! - **One pinned hash covers the whole set.** `fixtures.lock` at the repo
//!   root holds the [set hash](set_listing) of every file under
//!   `fixtures/` (and of the generator version). Changing, adding, removing
//!   or renaming any fixture without updating the lock fails the load.
//! - **No record mode, ever.** Nothing in COMMONS writes a fixture or the
//!   lock. Expectations are written by a person and reviewed.
//! - **Every case states where its expectation came from** ([`Provenance`]).
//! - **Every program needs positive and negative cases**: at least one
//!   `decoded` case and at least one `not_claimed` or `decode_error` case.
//! - **Expectations are exact.** Integers only, no tolerances, no wildcards
//!   (see `canon.rs`).
//! - **Duplicate keys are an error at every depth.**
//! - **The generator is stable by construction** (see `chain_gen.rs`), and
//!   its version is part of the set hash.
//! - **Expected rows only come with explicit chains.** A model case over a
//!   generated chain has no `expect`.
//! - **Every chain is internally consistent** (see `model_cases.rs`).
//! - **Files are read the way `manifest/` reads them**: no symlinks, only
//!   regular files, size checked before and while reading, the same text
//!   rules as artifact content (UTF-8, no BOM, no `\r`, exactly one trailing
//!   newline), and names that are lowercase, digits and single hyphens.
//! - **The whole set is loaded once, and it is immutable.** A
//!   [`FixtureSet`] has no mutators; explicit chains are shared as
//!   `Arc<Chain>`.
//!
//! Every problem is a [`CommonsError::FixtureError`] (exit code 1), except
//! I/O failures ([`CommonsError::IoError`]) and bugs
//! ([`CommonsError::Internal`]).
//!
//! # Layout
//!
//! ```text
//! fixtures.lock                                  one line: the set hash
//! fixtures/decoders/<program_id>/<case>.json     decoder_cases.rs
//! fixtures/chains/<chain-id>.json                model_cases.rs
//! fixtures/models/<model-name>/<case>.json       model_cases.rs
//! ```
//!
//! Nothing else is allowed: no other file or directory, no hidden entry, no
//! extension but `.json`. Any of the three directories may be absent or
//! empty, and an empty set is legal, but a program or model directory must
//! hold at least one case. The file name without `.json` equals the file's
//! `case` (or `id`), and the directory name equals its `program_id` (or
//! `model`).

mod canon;
mod chain_gen;
mod decoder_cases;
mod model_cases;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, Metadata};
use std::io;
use std::path::Path;
use std::sync::Arc;

use crate::config::{CommonsConfig, CommonsError, FIXTURES_DIR, echo};
use crate::manifest::{
    Entry, check_content, check_description, check_name, io_err, list_dir_with, parse_sha256_hex,
    read_regular_file_with, sha256, to_hex,
};

pub use canon::{
    CanonIssue, Difference, ParseLimits, StrictValue, canonical_bytes, canonical_string,
    first_difference, join_index, join_key, parse_strict,
};
pub use chain_gen::{
    GENERATOR_VERSION, GenSpec, MAX_GEN_SLOTS, MAX_GEN_START_SLOT, MAX_GEN_TRANSACTIONS,
    MAX_GEN_TX_PER_SLOT, SplitMix64, generate,
};
pub use decoder_cases::{
    DecoderCase, DecoderExpect, MAX_DECODER_ACCOUNTS, MAX_INSTRUCTION_DATA_BYTES,
    MAX_INSTRUCTION_TYPE_LEN, parse_decoder_case,
};
pub use model_cases::{
    Cell, Chain, ChainRef, Column, ColumnType, InstructionRow, MAX_EXPECT_COLUMNS,
    MAX_EXPECT_ROWS, MAX_ROWS_PER_TABLE, ModelCase, ModelExpect, SPL_TOKEN_PROGRAM_ID, SlotRow,
    TOKEN_2022_PROGRAM_ID, Table, TokenTransferRow, TransactionRow, TxStatus, check_consistency,
    parse_chain_file, parse_model_case,
};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The only `schema_version` a fixture file may have.
pub const FIXTURE_SCHEMA_VERSION: u32 = 1;
/// At the repo root, next to `fixtures/`.
pub const LOCK_FILE: &str = "fixtures.lock";
pub const DECODERS_DIR: &str = "decoders";
pub const CHAINS_DIR: &str = "chains";
pub const MODELS_DIR: &str = "models";
/// The only extension a fixture file may have.
pub const FIXTURE_EXT: &str = ".json";

/// Per-file limits, in bytes.
pub const MAX_DECODER_CASE_BYTES: u64 = 64 * 1024;
pub const MAX_CHAIN_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_MODEL_CASE_BYTES: u64 = 1024 * 1024;
/// 64 hex digits and a newline, with room to report a wrong one.
pub const MAX_LOCK_BYTES: u64 = 128;
/// Most fixture files in one set.
pub const MAX_FIXTURE_FILES: usize = 10_000;
/// Most bytes in all fixture files together.
pub const MAX_SET_BYTES: u64 = 256 * 1024 * 1024;

/// First line of the [set listing](set_listing). Changing the listing
/// format means changing this line.
pub const SET_LISTING_HEADER: &str = "commons-fixtures v1";

/// `field` of errors about a whole file (reading, text rules, JSON syntax at
/// the top level) and about directories.
pub const FIELD_FILE: &str = "file";
pub const FIELD_DIRECTORY: &str = "directory";
pub const FIELD_LOCK: &str = "lock";
/// `field` of errors about the set as a whole (counts, coverage).
pub const FIELD_SET: &str = "set";

// ---------------------------------------------------------------------------
// Field issues
// ---------------------------------------------------------------------------

/// One problem inside one fixture file, before it is tied to the file's
/// path. `field` is a path into the document (`expect.data`,
/// `slots[3].blockhash`) or [`FIELD_FILE`]. `reason` has contributor text
/// already echoed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldIssue {
    pub field: String,
    pub reason: String,
}

impl FieldIssue {
    pub(crate) fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        FieldIssue {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub(crate) fn into_error(self, path: &str) -> CommonsError {
        fixture_err(path, &self.field, self.reason)
    }
}

impl std::fmt::Display for FieldIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

impl From<CanonIssue> for FieldIssue {
    fn from(i: CanonIssue) -> Self {
        let field = if i.path.is_empty() {
            FIELD_FILE.to_string()
        } else {
            i.path
        };
        FieldIssue::new(field, format!("{} at byte {}", i.reason, i.offset))
    }
}

pub(crate) fn fixture_err(path: &str, field: &str, reason: impl Into<String>) -> CommonsError {
    CommonsError::FixtureError {
        path: path.to_string(),
        field: field.to_string(),
        reason: reason.into(),
    }
}

// ---------------------------------------------------------------------------
// Shared field rules
// ---------------------------------------------------------------------------

/// An object being read field by field. `at` is its own path (`""` at the
/// top of a file).
pub(crate) struct Obj<'a> {
    map: &'a BTreeMap<String, StrictValue>,
    at: String,
}

impl<'a> Obj<'a> {
    pub(crate) fn of(v: &'a StrictValue, at: &str) -> Result<Self, FieldIssue> {
        match v {
            StrictValue::Object(map) => Ok(Obj {
                map,
                at: at.to_string(),
            }),
            other => Err(FieldIssue::new(
                if at.is_empty() { FIELD_FILE } else { at },
                format!("must be an object, not {}", other.type_name()),
            )),
        }
    }

    pub(crate) fn path(&self, key: &str) -> String {
        join_key(&self.at, key)
    }

    /// Every key must be in `allowed`. The first unknown key (bytewise)
    /// is reported.
    pub(crate) fn only(&self, allowed: &[&str], what: &str) -> Result<(), FieldIssue> {
        match self.map.keys().find(|k| !allowed.contains(&k.as_str())) {
            None => Ok(()),
            Some(k) => Err(FieldIssue::new(
                if self.at.is_empty() { FIELD_FILE } else { &self.at },
                format!(
                    "unknown {what} '{}' (expected {})",
                    echo(k),
                    allowed.join(", ")
                ),
            )),
        }
    }

    pub(crate) fn opt(&self, key: &str) -> Option<&'a StrictValue> {
        self.map.get(key)
    }

    pub(crate) fn req(&self, key: &str) -> Result<&'a StrictValue, FieldIssue> {
        self.map
            .get(key)
            .ok_or_else(|| FieldIssue::new(self.path(key), "is missing"))
    }

    pub(crate) fn str(&self, key: &str) -> Result<&'a str, FieldIssue> {
        want_str(self.req(key)?, &self.path(key))
    }

    pub(crate) fn int<T: TryFrom<i128>>(&self, key: &str, ty: &str) -> Result<T, FieldIssue> {
        want_int(self.req(key)?, &self.path(key), ty)
    }

    pub(crate) fn bool(&self, key: &str) -> Result<bool, FieldIssue> {
        match self.req(key)? {
            StrictValue::Bool(b) => Ok(*b),
            other => Err(FieldIssue::new(
                self.path(key),
                format!("must be true or false, not {}", other.type_name()),
            )),
        }
    }

    pub(crate) fn array(&self, key: &str, max: usize) -> Result<&'a [StrictValue], FieldIssue> {
        want_array(self.req(key)?, &self.path(key), max)
    }

    /// `schema_version` is checked before any other key, so that a newer
    /// file says so instead of complaining about fields this build has
    /// never heard of.
    pub(crate) fn schema_version(&self) -> Result<(), FieldIssue> {
        let v: u64 = self.int("schema_version", "a schema version")?;
        if v != u64::from(FIXTURE_SCHEMA_VERSION) {
            return Err(FieldIssue::new(
                "schema_version",
                format!("{v} is not supported (this build supports {FIXTURE_SCHEMA_VERSION})"),
            ));
        }
        Ok(())
    }
}

pub(crate) fn want_str<'a>(v: &'a StrictValue, field: &str) -> Result<&'a str, FieldIssue> {
    match v {
        StrictValue::Str(s) => Ok(s),
        other => Err(FieldIssue::new(
            field,
            format!("must be a string, not {}", other.type_name()),
        )),
    }
}

/// An integer that fits `T`. `ty` names the range in the error, e.g.
/// "a u64".
pub(crate) fn want_int<T: TryFrom<i128>>(
    v: &StrictValue,
    field: &str,
    ty: &str,
) -> Result<T, FieldIssue> {
    match v {
        StrictValue::Int(n) => T::try_from(*n)
            .map_err(|_| FieldIssue::new(field, format!("{n} does not fit in {ty}"))),
        other => Err(FieldIssue::new(
            field,
            format!("must be an integer, not {}", other.type_name()),
        )),
    }
}

pub(crate) fn want_array<'a>(
    v: &'a StrictValue,
    field: &str,
    max: usize,
) -> Result<&'a [StrictValue], FieldIssue> {
    match v {
        StrictValue::Array(items) if items.len() <= max => Ok(items),
        StrictValue::Array(items) => Err(FieldIssue::new(
            field,
            format!("has {} elements, more than the limit of {max}", items.len()),
        )),
        other => Err(FieldIssue::new(
            field,
            format!("must be an array, not {}", other.type_name()),
        )),
    }
}

/// A case name, chain id or model name: the manifest's name rules
/// (lowercase letters, digits and single hyphens, starting with a letter).
pub(crate) fn want_name<'a>(v: &'a StrictValue, field: &str) -> Result<&'a str, FieldIssue> {
    let s = want_str(v, field)?;
    check_name(s).map_err(|r| FieldIssue::new(field, r))?;
    Ok(s)
}

/// Base58 that decodes to exactly `len` bytes (any length if `None`) and
/// re-encodes to the same string. `what` names the value in errors.
pub(crate) fn base58_exact(s: &str, len: Option<usize>, what: &str) -> Result<Vec<u8>, String> {
    let bad = |why: String| format!("'{}' is not {what}: {why}", echo(s));
    // n bytes are at most ~1.37 n base58 characters; 2n + 2 bounds the
    // work before decoding.
    if let Some(n) = len
        && s.len() > 2 * n + 2
    {
        return Err(bad(format!("too long for {n} bytes")));
    }
    let bytes = bs58::decode(s)
        .into_vec()
        .map_err(|_| bad("not valid base58".into()))?;
    if let Some(n) = len
        && bytes.len() != n
    {
        return Err(bad(format!("decodes to {} bytes, expected {n}", bytes.len())));
    }
    if bs58::encode(&bytes).into_string() != s {
        return Err(bad("not in canonical base58 form".into()));
    }
    Ok(bytes)
}

/// A 32-byte account address (pubkey, program id, mint, blockhash).
pub(crate) fn parse_key(s: &str, what: &str) -> Result<[u8; 32], String> {
    let v = base58_exact(s, Some(32), what)?;
    Ok(v.try_into().expect("length checked"))
}

pub(crate) fn want_key(v: &StrictValue, field: &str, what: &str) -> Result<[u8; 32], FieldIssue> {
    parse_key(want_str(v, field)?, what).map_err(|r| FieldIssue::new(field, r))
}

// ---------------------------------------------------------------------------
// Provenance
// ---------------------------------------------------------------------------

/// What an expectation was taken from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProvenanceKind {
    /// Read off a real transaction: `cluster` is required and `reference`
    /// is its signature.
    OnchainTransaction,
    /// Worked out by a person; `reference` says where (a document, an issue).
    HandComputed,
    /// Derived from the program's source; `reference` says which (repo and
    /// commit, a file and line).
    ProgramSource,
}

impl ProvenanceKind {
    pub const ALL: [ProvenanceKind; 3] = [
        ProvenanceKind::OnchainTransaction,
        ProvenanceKind::HandComputed,
        ProvenanceKind::ProgramSource,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ProvenanceKind::OnchainTransaction => "onchain_transaction",
            ProvenanceKind::HandComputed => "hand_computed",
            ProvenanceKind::ProgramSource => "program_source",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Cluster {
    MainnetBeta,
    Devnet,
    Testnet,
}

impl Cluster {
    pub const ALL: [Cluster; 3] = [Cluster::MainnetBeta, Cluster::Devnet, Cluster::Testnet];

    pub fn as_str(self) -> &'static str {
        match self {
            Cluster::MainnetBeta => "mainnet-beta",
            Cluster::Devnet => "devnet",
            Cluster::Testnet => "testnet",
        }
    }
}

/// Where a case's expectation came from. `reference` and `note` are
/// printable ASCII, 1 to 256 characters, no leading or trailing space.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Provenance {
    pub kind: ProvenanceKind,
    /// `Some` exactly for [`ProvenanceKind::OnchainTransaction`].
    pub cluster: Option<Cluster>,
    pub reference: String,
    pub note: String,
}

pub(crate) fn parse_provenance(v: &StrictValue, at: &str) -> Result<Provenance, FieldIssue> {
    let o = Obj::of(v, at)?;
    o.only(&["kind", "cluster", "reference", "note"], "provenance field")?;
    let kind_s = o.str("kind")?;
    let kind = ProvenanceKind::ALL
        .into_iter()
        .find(|k| k.as_str() == kind_s)
        .ok_or_else(|| {
            FieldIssue::new(
                o.path("kind"),
                format!(
                    "'{}' is not a provenance kind (expected onchain_transaction, \
                     hand_computed or program_source)",
                    echo(kind_s)
                ),
            )
        })?;
    let cluster = match (kind, o.opt("cluster")) {
        (ProvenanceKind::OnchainTransaction, None) => {
            return Err(FieldIssue::new(
                o.path("cluster"),
                "is required for an onchain_transaction",
            ));
        }
        (ProvenanceKind::OnchainTransaction, Some(c)) => {
            let c = want_str(c, &o.path("cluster"))?;
            Some(
                Cluster::ALL
                    .into_iter()
                    .find(|k| k.as_str() == c)
                    .ok_or_else(|| {
                        FieldIssue::new(
                            o.path("cluster"),
                            format!(
                                "'{}' is not a cluster (expected mainnet-beta, devnet or testnet)",
                                echo(c)
                            ),
                        )
                    })?,
            )
        }
        (_, Some(_)) => {
            return Err(FieldIssue::new(
                o.path("cluster"),
                format!("is only allowed for an onchain_transaction, not {kind_s}"),
            ));
        }
        (_, None) => None,
    };
    let reference = o.str("reference")?;
    check_description(reference).map_err(|r| FieldIssue::new(o.path("reference"), r))?;
    if kind == ProvenanceKind::OnchainTransaction {
        base58_exact(reference, Some(64), "a transaction signature")
            .map_err(|r| FieldIssue::new(o.path("reference"), r))?;
    }
    let note = o.str("note")?;
    check_description(note).map_err(|r| FieldIssue::new(o.path("note"), r))?;
    Ok(Provenance {
        kind,
        cluster,
        reference: reference.to_string(),
        note: note.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Set hash
// ---------------------------------------------------------------------------

/// The text the set hash is the SHA-256 of:
///
/// ```text
/// commons-fixtures v1
/// generator <GENERATOR_VERSION>
/// <sha256 hex of the file's bytes>  fixtures/<relative path>     one line per file
/// ```
///
/// Files are sorted bytewise by path, exactly as `LC_ALL=C sort` sorts
/// them; each line is what `sha256sum` prints. So from the repo root:
///
/// ```text
/// { printf 'commons-fixtures v1\ngenerator 1\n'
///   find fixtures -type f | LC_ALL=C sort | xargs -r sha256sum; } | sha256sum
/// ```
///
/// The bytes hashed are the raw file bytes as on disk, never a parsed or
/// re-serialised form. Paths are relative to the repo root, so the hash
/// does not depend on where the repository is checked out.
///
/// `files` are `(path, bytes)` pairs in any order. Paths must be unique.
pub fn set_listing(files: &[(&str, &[u8])], generator_version: u32) -> Vec<u8> {
    let mut sorted: Vec<&(&str, &[u8])> = files.iter().collect();
    sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut out = format!("{SET_LISTING_HEADER}\ngenerator {generator_version}\n").into_bytes();
    for (path, bytes) in sorted {
        out.extend_from_slice(to_hex(&sha256(bytes)).as_bytes());
        out.extend_from_slice(b"  ");
        out.extend_from_slice(path.as_bytes());
        out.push(b'\n');
    }
    out
}

/// SHA-256 of [`set_listing`].
pub fn set_hash(files: &[(&str, &[u8])], generator_version: u32) -> [u8; 32] {
    sha256(&set_listing(files, generator_version))
}

// ---------------------------------------------------------------------------
// The fixture set
// ---------------------------------------------------------------------------

/// How many of each thing a set holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FixtureCounts {
    /// Files under `fixtures/`, all kinds.
    pub files: usize,
    /// Program directories under `decoders/`.
    pub programs: usize,
    pub decoder_cases: usize,
    pub chains: usize,
    /// Model directories under `models/`.
    pub models: usize,
    pub model_cases: usize,
}

/// A loaded, validated, immutable fixture set. See [`FixtureSet::load`].
#[derive(Debug, PartialEq, Eq)]
pub struct FixtureSet {
    set_hash: [u8; 32],
    files: usize,
    /// Each list sorted by case name.
    decoders: BTreeMap<[u8; 32], Vec<DecoderCase>>,
    chains: BTreeMap<String, Arc<Chain>>,
    /// Each list sorted by case name.
    models: BTreeMap<String, Vec<ModelCase>>,
}

impl FixtureSet {
    /// Load the set under `config.fixtures_dir`, stopping at the first
    /// error:
    ///
    /// 1. `fixtures_dir` is present in the config (only `verify` has one),
    ///    and is a real directory.
    /// 2. **Walk** `decoders/`, `chains/` and `models/` with the layout
    ///    rules (module docs), in name order. (`directory`)
    /// 3. **Read** every file without following symlinks, with its per-file
    ///    limit and the text rules. (`file`)
    /// 4. **Set hash** of the raw bytes ([`set_listing`]).
    /// 5. **Lock**: `fixtures.lock`, read the same way, is one line of 64
    ///    lowercase hex digits equal to the set hash. (`lock`)
    /// 6. **Parse** every file into its typed form with the field rules,
    ///    then check the names against the directories and files, then
    ///    that every program has positive and negative cases.
    /// 7. **Resolve references**: every `explicit` chain id exists, and
    ///    every chain, explicit or generated, passes
    ///    [`check_consistency`].
    ///
    /// Files are visited in [set listing](set_listing) order, so the error
    /// reported never depends on the filesystem.
    pub fn load(config: &CommonsConfig) -> Result<FixtureSet, CommonsError> {
        // 1. The directory.
        let Some(dir) = &config.fixtures_dir else {
            return Err(CommonsError::Internal(
                "fixture set loaded without a fixtures directory (only verify has one)".into(),
            ));
        };
        real_dir(dir, FIXTURES_DIR)?;

        // 2. Walk.
        let mut found = walk(dir)?;
        found.sort_by(|a, b| a.rel.as_bytes().cmp(b.rel.as_bytes()));

        // 3. Read.
        let mut files = Vec::with_capacity(found.len());
        let mut total: u64 = 0;
        for f in found {
            let bytes = read_text_file(&f.path, &f.meta, f.kind.max_bytes(), &f.rel)?;
            total += bytes.len() as u64;
            if total > MAX_SET_BYTES {
                return Err(fixture_err(
                    FIXTURES_DIR,
                    FIELD_SET,
                    format!("all fixture files together are more than {MAX_SET_BYTES} bytes"),
                ));
            }
            files.push(RawFile { found: f, bytes });
        }

        // 4. Set hash.
        let pairs: Vec<(&str, &[u8])> = files
            .iter()
            .map(|f| (f.found.rel.as_str(), f.bytes.as_slice()))
            .collect();
        let actual = set_hash(&pairs, GENERATOR_VERSION);

        // 5. Lock.
        let pinned = read_lock(&config.repo_root.join(LOCK_FILE))?;
        if pinned != actual {
            return Err(fixture_err(
                LOCK_FILE,
                FIELD_LOCK,
                format!(
                    "pins {} but the fixture set hashes to {}; a change to fixtures/ must \
                     update {LOCK_FILE} in the same reviewed change",
                    to_hex(&pinned),
                    to_hex(&actual)
                ),
            ));
        }

        // 6. Parse, then names, then coverage.
        let mut parsed = Vec::with_capacity(files.len());
        for f in &files {
            parsed.push(parse_file(f)?);
        }
        for (f, p) in files.iter().zip(&parsed) {
            check_names(&f.found, p)?;
        }
        let mut set = FixtureSet {
            set_hash: actual,
            files: files.len(),
            decoders: BTreeMap::new(),
            chains: BTreeMap::new(),
            models: BTreeMap::new(),
        };
        for p in parsed {
            match p {
                Parsed::Decoder(c) => set.decoders.entry(c.program_id).or_default().push(c),
                Parsed::Chain(id, chain) => {
                    set.chains.insert(id, Arc::new(chain));
                }
                Parsed::Model(c) => set.models.entry(c.model.clone()).or_default().push(c),
            }
        }
        // Listing order within a directory is name order already; sort
        // anyway, so the lookups' order is a property of this function.
        for cases in set.decoders.values_mut() {
            cases.sort_by(|a, b| a.name.cmp(&b.name));
        }
        for cases in set.models.values_mut() {
            cases.sort_by(|a, b| a.name.cmp(&b.name));
        }
        for (pid, cases) in &set.decoders {
            check_coverage(pid, cases)?;
        }

        // 7. References and consistency.
        for c in set.models.values().flatten() {
            if let ChainRef::Explicit(id) = &c.chain
                && !set.chains.contains_key(id)
            {
                return Err(fixture_err(
                    &model_case_path(&c.model, &c.name),
                    "chain.explicit",
                    format!("no chain '{id}' in fixtures/{CHAINS_DIR}/"),
                ));
            }
        }
        for (id, chain) in &set.chains {
            check_consistency(chain).map_err(|i| i.into_error(&chain_path(id)))?;
        }
        let mut specs_seen = BTreeSet::new();
        for c in set.models.values().flatten() {
            if let ChainRef::Generated(spec) = &c.chain
                && specs_seen.insert(*spec)
            {
                check_consistency(&generate(spec)).map_err(|i| {
                    CommonsError::Internal(format!(
                        "generator v{GENERATOR_VERSION} produced an inconsistent chain for \
                         {}: {i}",
                        model_case_path(&c.model, &c.name)
                    ))
                })?;
            }
        }
        Ok(set)
    }

    /// The hash `fixtures.lock` pins, and that this set has.
    pub fn set_hash(&self) -> [u8; 32] {
        self.set_hash
    }

    /// The version of the chain generator generated chains come from. It
    /// is part of the set hash.
    pub fn generator_version(&self) -> u32 {
        GENERATOR_VERSION
    }

    /// The cases of one program, sorted by case name; empty if none.
    pub fn decoder_cases(&self, program_id: &[u8; 32]) -> &[DecoderCase] {
        self.decoders.get(program_id).map_or(&[], Vec::as_slice)
    }

    /// The cases of one model, sorted by case name; empty if none.
    pub fn model_cases(&self, model: &str) -> &[ModelCase] {
        self.models.get(model).map_or(&[], Vec::as_slice)
    }

    /// Every program with decoder cases, in byte order.
    pub fn programs(&self) -> impl Iterator<Item = &[u8; 32]> {
        self.decoders.keys()
    }

    /// Every model with cases, in name order.
    pub fn models(&self) -> impl Iterator<Item = &str> {
        self.models.keys().map(String::as_str)
    }

    /// Every explicit chain id, in name order.
    pub fn chain_ids(&self) -> impl Iterator<Item = &str> {
        self.chains.keys().map(String::as_str)
    }

    /// The chain a case runs over. An explicit chain is shared; a generated
    /// one is built now, every time, so it is always a fresh pure function
    /// of its spec.
    pub fn resolve_chain(&self, chain: &ChainRef) -> Result<Arc<Chain>, CommonsError> {
        match chain {
            ChainRef::Explicit(id) => self.chains.get(id).cloned().ok_or_else(|| {
                fixture_err(
                    &format!("{FIXTURES_DIR}/{CHAINS_DIR}/{}{FIXTURE_EXT}", echo(id)),
                    "chain.explicit",
                    "no such chain in the fixture set",
                )
            }),
            ChainRef::Generated(spec) => {
                spec.validate()
                    .map_err(|i| fixture_err(FIXTURES_DIR, &i.field, i.reason))?;
                Ok(Arc::new(generate(spec)))
            }
        }
    }

    pub fn counts(&self) -> FixtureCounts {
        FixtureCounts {
            files: self.files,
            programs: self.decoders.len(),
            decoder_cases: self.decoders.values().map(Vec::len).sum(),
            chains: self.chains.len(),
            models: self.models.len(),
            model_cases: self.models.values().map(Vec::len).sum(),
        }
    }
}

// ---------------------------------------------------------------------------
// Walk
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileKind {
    DecoderCase,
    Chain,
    ModelCase,
}

impl FileKind {
    fn max_bytes(self) -> u64 {
        match self {
            FileKind::DecoderCase => MAX_DECODER_CASE_BYTES,
            FileKind::Chain => MAX_CHAIN_BYTES,
            FileKind::ModelCase => MAX_MODEL_CASE_BYTES,
        }
    }
}

/// A file the walk accepted.
struct Found {
    /// `fixtures/...`, built from names that passed the layout rules.
    rel: String,
    path: std::path::PathBuf,
    meta: Metadata,
    kind: FileKind,
    /// The program id or model name of the directory, if any.
    dir_name: Option<String>,
    /// The file name without `.json`.
    stem: String,
}

struct RawFile {
    found: Found,
    bytes: Vec<u8>,
}

fn chain_path(id: &str) -> String {
    format!("{FIXTURES_DIR}/{CHAINS_DIR}/{id}{FIXTURE_EXT}")
}

fn model_case_path(model: &str, case: &str) -> String {
    format!("{FIXTURES_DIR}/{MODELS_DIR}/{model}/{case}{FIXTURE_EXT}")
}

/// `path` exists and is a real directory, not a symlink.
fn real_dir(path: &Path, label: &str) -> Result<(), CommonsError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(fixture_err(label, FIELD_DIRECTORY, "does not exist"));
        }
        Err(e) => return Err(io_err(path, e)),
    };
    if meta.file_type().is_symlink() {
        return Err(fixture_err(label, FIELD_DIRECTORY, "is a symlink"));
    }
    if !meta.is_dir() {
        return Err(fixture_err(label, FIELD_DIRECTORY, "is not a directory"));
    }
    Ok(())
}

/// The entries of `dir`, sorted by name, each a UTF-8 name that is not
/// hidden and not a symlink. `label` is `dir` relative to the repo root.
fn entries(dir: &Path, label: &str) -> Result<Vec<(String, Entry)>, CommonsError> {
    let mut out = Vec::new();
    for e in list_dir_with(dir, |r| fixture_err(label, FIELD_DIRECTORY, r))? {
        let shown = echo(&e.name.to_string_lossy());
        let bad = |reason: &str| fixture_err(&format!("{label}/{shown}"), FIELD_DIRECTORY, reason);
        let Some(name) = e.name.to_str() else {
            return Err(bad("name is not valid UTF-8"));
        };
        if name.starts_with('.') {
            return Err(bad("hidden entries are not allowed"));
        }
        if e.file_type.is_symlink() {
            return Err(bad("symlinks are not allowed"));
        }
        out.push((name.to_string(), e));
    }
    Ok(out)
}

/// A directory that must hold only `<name>.json` regular files, at least
/// one. Returns them in name order.
fn case_files(
    dir: &Path,
    label: &str,
    kind: FileKind,
    dir_name: Option<&str>,
    out: &mut Vec<Found>,
) -> Result<(), CommonsError> {
    let list = entries(dir, label)?;
    if list.is_empty() && dir_name.is_some() {
        return Err(fixture_err(label, FIELD_DIRECTORY, "holds no cases"));
    }
    for (name, e) in list {
        let rel = format!("{label}/{}", echo(&name));
        let bad = |reason: String| fixture_err(&rel, FIELD_DIRECTORY, reason);
        if !e.file_type.is_file() {
            return Err(bad(format!(
                "only {FIXTURE_EXT} files are allowed here, not directories or other entries"
            )));
        }
        let Some(stem) = name.strip_suffix(FIXTURE_EXT) else {
            return Err(bad(format!("must have the {FIXTURE_EXT} extension")));
        };
        check_name(stem).map_err(|r| bad(format!("file name: {r}")))?;
        if out.len() == MAX_FIXTURE_FILES {
            return Err(fixture_err(
                FIXTURES_DIR,
                FIELD_SET,
                format!("more than {MAX_FIXTURE_FILES} fixture files"),
            ));
        }
        let meta = fs::symlink_metadata(&e.path).map_err(|err| io_err(&e.path, err))?;
        out.push(Found {
            rel,
            path: e.path,
            meta,
            kind,
            dir_name: dir_name.map(str::to_string),
            stem: stem.to_string(),
        });
    }
    Ok(())
}

fn walk(root: &Path) -> Result<Vec<Found>, CommonsError> {
    let mut out = Vec::new();
    for (name, e) in entries(root, FIXTURES_DIR)? {
        let label = format!("{FIXTURES_DIR}/{}", echo(&name));
        let known = [DECODERS_DIR, CHAINS_DIR, MODELS_DIR].contains(&name.as_str());
        if !known || !e.file_type.is_dir() {
            return Err(fixture_err(
                &label,
                FIELD_DIRECTORY,
                format!(
                    "only the directories {DECODERS_DIR}/, {CHAINS_DIR}/ and {MODELS_DIR}/ \
                     are allowed in {FIXTURES_DIR}/"
                ),
            ));
        }
        match name.as_str() {
            CHAINS_DIR => case_files(&e.path, &label, FileKind::Chain, None, &mut out)?,
            DECODERS_DIR | MODELS_DIR => {
                for (sub, se) in entries(&e.path, &label)? {
                    let sub_label = format!("{label}/{}", echo(&sub));
                    let bad = |r: String| fixture_err(&sub_label, FIELD_DIRECTORY, r);
                    if !se.file_type.is_dir() {
                        return Err(bad(format!(
                            "only {} directories are allowed here",
                            if name == DECODERS_DIR { "program" } else { "model" }
                        )));
                    }
                    let kind = if name == DECODERS_DIR {
                        parse_key(&sub, "a program address")
                            .map_err(|r| bad(format!("directory name: {r}")))?;
                        FileKind::DecoderCase
                    } else {
                        check_name(&sub).map_err(|r| bad(format!("directory name: {r}")))?;
                        FileKind::ModelCase
                    };
                    case_files(&se.path, &sub_label, kind, Some(&sub), &mut out)?;
                }
            }
            _ => unreachable!("checked above"),
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Read and lock
// ---------------------------------------------------------------------------

/// Read a regular file the way `manifest/` does, then apply the text rules.
fn read_text_file(
    path: &Path,
    meta: &Metadata,
    max_bytes: u64,
    label: &str,
) -> Result<Vec<u8>, CommonsError> {
    let bytes = read_regular_file_with(path, meta, max_bytes, |r| {
        fixture_err(label, FIELD_FILE, r)
    })?;
    check_content(&bytes, max_bytes).map_err(|r| fixture_err(label, FIELD_FILE, r))?;
    Ok(bytes)
}

/// `fixtures.lock`: a regular file (not a symlink) holding exactly 64
/// lowercase hex digits and one `\n`.
fn read_lock(path: &Path) -> Result<[u8; 32], CommonsError> {
    let err = |r: &str| fixture_err(LOCK_FILE, FIELD_LOCK, r);
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(err(
                "is missing; every fixture set, even an empty one, is pinned by a lock",
            ));
        }
        Err(e) => return Err(io_err(path, e)),
    };
    if meta.file_type().is_symlink() {
        return Err(err("is a symlink"));
    }
    if !meta.is_file() {
        return Err(err("is not a regular file"));
    }
    let bytes = read_regular_file_with(path, &meta, MAX_LOCK_BYTES, |r| {
        fixture_err(LOCK_FILE, FIELD_LOCK, r)
    })?;
    check_content(&bytes, MAX_LOCK_BYTES).map_err(|r| fixture_err(LOCK_FILE, FIELD_LOCK, r))?;
    let line = std::str::from_utf8(&bytes[..bytes.len() - 1]).expect("checked UTF-8");
    parse_sha256_hex(line).map_err(|r| {
        fixture_err(
            LOCK_FILE,
            FIELD_LOCK,
            format!("must be one line holding the set hash: {r}"),
        )
    })
}

// ---------------------------------------------------------------------------
// Parse and names
// ---------------------------------------------------------------------------

enum Parsed {
    Decoder(DecoderCase),
    Chain(String, Chain),
    Model(ModelCase),
}

fn parse_file(f: &RawFile) -> Result<Parsed, CommonsError> {
    let rel = &f.found.rel;
    let limits = ParseLimits::DEFAULT;
    let value =
        parse_strict(&f.bytes, &limits).map_err(|i| FieldIssue::from(i).into_error(rel))?;
    let issue = |i: FieldIssue| i.into_error(rel);
    Ok(match f.found.kind {
        FileKind::DecoderCase => Parsed::Decoder(parse_decoder_case(&value).map_err(issue)?),
        FileKind::Chain => {
            let (id, chain) = parse_chain_file(&value).map_err(issue)?;
            Parsed::Chain(id, chain)
        }
        FileKind::ModelCase => Parsed::Model(parse_model_case(&value).map_err(issue)?),
    })
}

fn check_names(f: &Found, p: &Parsed) -> Result<(), CommonsError> {
    let mismatch = |field: &str, inside: &str, outside: &str, what: &str| {
        fixture_err(
            &f.rel,
            field,
            format!("is '{inside}' but the {what} is '{outside}'"),
        )
    };
    let dir = f.dir_name.as_deref().unwrap_or_default();
    match p {
        Parsed::Decoder(c) => {
            let pid = bs58::encode(c.program_id).into_string();
            if pid != dir {
                return Err(mismatch("program_id", &pid, dir, "directory name"));
            }
            if c.name != f.stem {
                return Err(mismatch("case", &c.name, &f.stem, "file name"));
            }
        }
        Parsed::Chain(id, _) => {
            if *id != f.stem {
                return Err(mismatch("id", id, &f.stem, "file name"));
            }
        }
        Parsed::Model(c) => {
            if c.model != dir {
                return Err(mismatch("model", &c.model, dir, "directory name"));
            }
            if c.name != f.stem {
                return Err(mismatch("case", &c.name, &f.stem, "file name"));
            }
        }
    }
    Ok(())
}

/// Every program has at least one `decoded` case and at least one
/// `not_claimed` or `decode_error` case.
fn check_coverage(pid: &[u8; 32], cases: &[DecoderCase]) -> Result<(), CommonsError> {
    let label = format!(
        "{FIXTURES_DIR}/{DECODERS_DIR}/{}",
        bs58::encode(pid).into_string()
    );
    let positive = cases
        .iter()
        .any(|c| matches!(c.expect, DecoderExpect::Decoded { .. }));
    let negative = cases
        .iter()
        .any(|c| !matches!(c.expect, DecoderExpect::Decoded { .. }));
    if !positive {
        return Err(fixture_err(
            &label,
            FIELD_SET,
            "has no positive case; every program needs at least one case with result 'decoded'",
        ));
    }
    if !negative {
        return Err(fixture_err(
            &label,
            FIELD_SET,
            "has no negative case; every program needs at least one case with result \
             'not_claimed' or 'decode_error'",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SIG: &str =
        "2Ana1pUpv2ZbMVkwF5FXapYeBEjdxDatLn7nvJkhgTSXbs59SyZSx866bXirPgj8QQVB57uxHJBG1YFvkRbFj4T";

    fn prov(src: &str) -> Result<Provenance, FieldIssue> {
        parse_provenance(
            &parse_strict(src.as_bytes(), &ParseLimits::DEFAULT).unwrap(),
            "provenance",
        )
    }

    #[test]
    fn provenance_rules() {
        let p = prov(&format!(
            r#"{{"kind":"onchain_transaction","cluster":"devnet","reference":"{SIG}","note":"ix 2"}}"#
        ))
        .unwrap();
        assert_eq!(p.kind, ProvenanceKind::OnchainTransaction);
        assert_eq!(p.cluster, Some(Cluster::Devnet));
        let p = prov(r#"{"kind":"hand_computed","reference":"worked on paper","note":"n"}"#).unwrap();
        assert_eq!((p.kind, p.cluster), (ProvenanceKind::HandComputed, None));

        for (src, field, part) in [
            (r#"{"kind":"guess","reference":"r","note":"n"}"#, "provenance.kind", "not a provenance kind"),
            (r#"{"kind":"onchain_transaction","reference":"r","note":"n"}"#, "provenance.cluster", "required"),
            (r#"{"kind":"hand_computed","cluster":"devnet","reference":"r","note":"n"}"#, "provenance.cluster", "only allowed"),
            (r#"{"kind":"onchain_transaction","cluster":"localnet","reference":"r","note":"n"}"#, "provenance.cluster", "not a cluster"),
            (r#"{"kind":"onchain_transaction","cluster":"devnet","reference":"abc","note":"n"}"#, "provenance.reference", "transaction signature"),
            (r#"{"kind":"hand_computed","reference":"","note":"n"}"#, "provenance.reference", "empty"),
            (r#"{"kind":"hand_computed","reference":"r","note":" padded"}"#, "provenance.note", "space"),
            (r#"{"kind":"hand_computed","reference":"r","note":"café"}"#, "provenance.note", "printable ASCII"),
            (r#"{"kind":"hand_computed","reference":"r"}"#, "provenance.note", "missing"),
            (r#"{"kind":"hand_computed","reference":"r","note":"n","by":"me"}"#, "provenance", "unknown provenance field 'by'"),
            (r#""hand_computed""#, "provenance", "must be an object"),
        ] {
            let e = prov(src).expect_err(src);
            assert_eq!(e.field, field, "{src}: {e}");
            assert!(e.reason.contains(part), "{src}: expected {part:?} in {e}");
        }
    }

    #[test]
    fn base58_is_exact_length_and_canonical() {
        let tok = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
        assert_eq!(bs58::encode(parse_key(tok, "k").unwrap()).into_string(), tok);
        assert_eq!(parse_key(&"1".repeat(32), "k").unwrap(), [0; 32]);
        assert!(parse_key(&"1".repeat(31), "k").unwrap_err().contains("31 bytes"));
        assert!(parse_key(&format!("{tok}1"), "k").is_err());
        assert!(parse_key("0OIl", "k").unwrap_err().contains("not valid base58"));
        assert!(parse_key(&"z".repeat(200), "k").unwrap_err().contains("too long"));
        assert_eq!(base58_exact(SIG, Some(64), "s").unwrap().len(), 64);
        assert_eq!(base58_exact("", None, "d").unwrap(), Vec::<u8>::new());
        assert_eq!(base58_exact("2", None, "d").unwrap(), vec![1]);
    }

    #[test]
    fn set_listing_format_is_exact() {
        let listing = set_listing(
            &[("fixtures/models/m/b.json", b"b\n"), ("fixtures/chains/a.json", b"a\n")],
            7,
        );
        let a = to_hex(&sha256(b"a\n"));
        let b = to_hex(&sha256(b"b\n"));
        assert_eq!(
            String::from_utf8(listing).unwrap(),
            format!(
                "commons-fixtures v1\ngenerator 7\n{a}  fixtures/chains/a.json\n{b}  fixtures/models/m/b.json\n"
            )
        );
        assert_eq!(
            String::from_utf8(set_listing(&[], 1)).unwrap(),
            "commons-fixtures v1\ngenerator 1\n"
        );
    }

    #[test]
    fn set_listing_sorts_bytewise_like_lc_all_c_sort() {
        // '-' (0x2d) sorts before '/' (0x2f), so "a-b/" comes before "a/".
        let listing = String::from_utf8(set_listing(
            &[
                ("fixtures/models/a/x.json", b"1\n"),
                ("fixtures/models/a-b/x.json", b"2\n"),
                ("fixtures/chains/z.json", b"3\n"),
            ],
            1,
        ))
        .unwrap();
        let paths: Vec<&str> = listing.lines().skip(2).map(|l| &l[66..]).collect();
        assert_eq!(
            paths,
            ["fixtures/chains/z.json", "fixtures/models/a-b/x.json", "fixtures/models/a/x.json"]
        );
    }

    #[test]
    fn set_hash_depends_on_every_byte_path_and_the_generator_version() {
        let base: &[(&str, &[u8])] = &[("fixtures/chains/a.json", b"{}\n")];
        let h = set_hash(base, 1);
        assert_eq!(h, set_hash(base, 1), "pure");
        assert_ne!(h, set_hash(&[("fixtures/chains/b.json", b"{}\n")], 1), "path");
        assert_ne!(h, set_hash(&[("fixtures/chains/a.json", b"{ }\n")], 1), "bytes");
        assert_ne!(h, set_hash(base, 2), "generator version");
        assert_ne!(h, set_hash(&[], 1), "removal");
        assert_ne!(
            h,
            set_hash(&[base[0], ("fixtures/chains/c.json", b"{}\n")], 1),
            "addition"
        );
    }

    #[test]
    fn field_issue_from_canon_issue() {
        let i: FieldIssue = parse_strict(b"{\"a\":01}", &ParseLimits::DEFAULT).unwrap_err().into();
        assert_eq!(i.field, "a");
        assert_eq!(i.reason, "leading zeros are not allowed at byte 5");
        let inner: FieldIssue = parse_strict(b"[", &ParseLimits::DEFAULT).unwrap_err().into();
        assert_eq!(inner.field, "[0]", "a syntax error inside a container names where");
        let i: FieldIssue = parse_strict(b"{", &ParseLimits::DEFAULT).unwrap_err().into();
        assert_eq!(i.field, FIELD_FILE);
        let e = i.into_error("fixtures/chains/x.json");
        assert_eq!(e.exit_code(), crate::config::EXIT_ARTIFACT);
        assert!(e.to_string().starts_with("fixture fixtures/chains/x.json: field file: "), "{e}");
    }
}
