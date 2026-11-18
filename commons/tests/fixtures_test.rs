//! Filesystem-level tests for `commons::fixtures`.
//!
//! Three properties get their own sections, because they are what makes a
//! fixture evidence:
//!
//! 1. **An accepted set is exactly what its files and its lock say.** The
//!    set hash is recomputed here independently (and pinned from
//!    `sha256sum`, outside Rust); any change to any file, without the lock,
//!    fails; the typed data is what the files hold.
//! 2. **The same fixture always gives the same hash and the same typed
//!    data**: twice, in another directory, written in another order.
//! 3. **The generator produces the same chain everywhere**: pinned digests,
//!    a value computed outside Rust, every path to a chain, every thread.
//!
//! `tests/data/sample/` is a complete fixture set (`fixtures.lock` and
//! `fixtures/`). Its hash was computed from the repository copy with
//!
//! ```text
//! cd commons/tests/data/sample
//! { printf 'commons-fixtures v1\ngenerator 1\n'
//!   find fixtures -type f | LC_ALL=C sort | xargs -r sha256sum; } | sha256sum
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use commons::config::{
    CommonsConfig, CommonsError, DEFAULT_MAX_ARTIFACT_BYTES, EXIT_ARTIFACT, EXIT_INTERNAL,
    LogLevel, OutputFormat,
};
use commons::fixtures::*;
use sha2::{Digest, Sha256};
use tempfile::TempDir;

const SAMPLE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/data/sample");
/// `sha256sum` of the sample's listing (module docs).
const SAMPLE_SET_HASH: &str = "99722a9ebaed9d5b85ec63f7f1308d723bf278963cad2c245ae79008c06ae46f";
/// `printf 'commons-fixtures v1\ngenerator 1\n' | sha256sum`.
const EMPTY_SET_HASH: &str = "c85f2e6f7238e89136c471ffd4ad9530a5d6bd1a5eb118bae2835e0bace3471a";

const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const PROGRAM_DIR: &str = "fixtures/decoders/TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const CHAIN_FILE: &str = "fixtures/chains/two-mints-one-failed.json";
const MODEL_DIR: &str = "fixtures/models/daily-active-programs";

/// Every file of the sample, in listing order.
const SAMPLE_FILES: [&str; 6] = [
    "fixtures/chains/two-mints-one-failed.json",
    "fixtures/decoders/TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA/transfer-basic.json",
    "fixtures/decoders/TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA/transfer-checked.json",
    "fixtures/decoders/TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA/truncated-transfer.json",
    "fixtures/models/daily-active-programs/generated-smoke.json",
    "fixtures/models/daily-active-programs/two-mints.json",
];

/// The generated chain of `generated-smoke.json`, and of the design notes.
const EXAMPLE_SPEC: GenSpec = GenSpec {
    seed: 7,
    start_slot: 1000,
    slots: 500,
    skip_percent: 5,
    max_tx_per_slot: 8,
    fail_percent: 10,
    transfer_percent: 30,
    mint_null_percent: 20,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct Repo {
    _tmp: TempDir,
    root: PathBuf,
    config: CommonsConfig,
}

/// A repo with an empty `fixtures/` and no lock.
fn repo() -> Repo {
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    fs::create_dir(root.join("fixtures")).unwrap();
    let config = CommonsConfig {
        repo_root: root.clone(),
        artifacts_dir: root.join("artifacts"),
        fixtures_dir: Some(root.join("fixtures")),
        format: OutputFormat::Text,
        log_level: LogLevel::Info,
        max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        lens_url: None,
        verify_timeout: Duration::from_secs(60),
    };
    Repo {
        _tmp: tmp,
        root,
        config,
    }
}

fn copy_tree(from: &Path, to: &Path, reverse: bool) {
    let mut entries: Vec<_> = fs::read_dir(from).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name());
    if reverse {
        entries.reverse();
    }
    for e in entries {
        let dest = to.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            fs::create_dir_all(&dest).unwrap();
            copy_tree(&e.path(), &dest, reverse);
        } else {
            fs::copy(e.path(), dest).unwrap();
        }
    }
}

/// A copy of `tests/data/sample`, lock included.
fn sample() -> Repo {
    sample_in_order(false)
}

/// The sample, with files created in name order or in reverse.
fn sample_in_order(reverse: bool) -> Repo {
    let r = repo();
    copy_tree(Path::new(SAMPLE_DIR), &r.root, reverse);
    r
}

impl Repo {
    fn path(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    fn write(&self, rel: &str, bytes: impl AsRef<[u8]>) {
        let p = self.path(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, bytes).unwrap();
    }

    fn read(&self, rel: &str) -> Vec<u8> {
        fs::read(self.path(rel)).unwrap()
    }

    fn read_str(&self, rel: &str) -> String {
        String::from_utf8(self.read(rel)).unwrap()
    }

    fn remove(&self, rel: &str) {
        fs::remove_file(self.path(rel)).unwrap();
    }

    fn lock_with(&self, hex: &str) {
        fs::write(self.path(LOCK_FILE), format!("{hex}\n")).unwrap();
    }

    /// Pin whatever is on disk now, computed independently. Returns the hash.
    fn relock(&self) -> String {
        let h = independent_set_hash(&self.root);
        self.lock_with(&h);
        h
    }

    fn load(&self) -> Result<FixtureSet, CommonsError> {
        FixtureSet::load(&self.config)
    }

    #[track_caller]
    fn loaded(&self) -> FixtureSet {
        self.load().unwrap_or_else(|e| panic!("load failed: {e}"))
    }

    /// Edit one file with a string replacement (which must apply).
    fn edit(&self, rel: &str, from: &str, to: &str) {
        let s = self.read_str(rel);
        assert!(s.contains(from), "{rel} does not contain {from:?}");
        self.write(rel, s.replacen(from, to, 1));
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The set hash, computed here from the filesystem without any code from
/// `commons`: every regular file below `fixtures/`, relative path with
/// `/`, sorted bytewise, one `sha256sum` line each, after the header.
fn independent_set_hash(root: &Path) -> String {
    fn walk(dir: &Path, rel: &str, out: &mut Vec<(String, Vec<u8>)>) {
        for e in fs::read_dir(dir).unwrap() {
            let e = e.unwrap();
            let name = e.file_name().into_string().unwrap();
            let r = format!("{rel}/{name}");
            let t = e.file_type().unwrap();
            if t.is_dir() {
                walk(&e.path(), &r, out);
            } else if t.is_file() {
                out.push((r, fs::read(e.path()).unwrap()));
            }
        }
    }
    let mut files = Vec::new();
    walk(&root.join("fixtures"), "fixtures", &mut files);
    files.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut listing = String::from("commons-fixtures v1\ngenerator 1\n");
    for (path, bytes) in &files {
        listing.push_str(&format!("{}  {path}\n", hex(&Sha256::digest(bytes))));
    }
    hex(&Sha256::digest(listing.as_bytes()))
}

#[track_caller]
fn fixture_error(r: Result<FixtureSet, CommonsError>) -> (String, String, String) {
    match r {
        Err(CommonsError::FixtureError {
            path,
            field,
            reason,
        }) => (path, field, reason),
        Err(other) => panic!("expected FixtureError, got {other:?}"),
        Ok(_) => panic!("expected FixtureError, but the set loaded"),
    }
}

#[track_caller]
fn assert_fixture_error(
    r: Result<FixtureSet, CommonsError>,
    path: &str,
    field: &str,
    reason_part: &str,
) {
    let (p, f, reason) = fixture_error(r);
    assert_eq!((p.as_str(), f.as_str()), (path, field), "{reason}");
    assert!(
        reason.contains(reason_part),
        "expected {reason_part:?} in: {reason}"
    );
}

fn key(n: u8) -> String {
    bs58::encode([n; 32]).into_string()
}

fn token_id() -> [u8; 32] {
    bs58::decode(TOKEN).into_vec().unwrap().try_into().unwrap()
}

/// A minimal decoder case file.
fn decoder_case(program: &str, case: &str, result: &str) -> String {
    let expect = if result == "decoded" {
        r#"{"result": "decoded", "instruction_type": "x", "data": null}"#.to_string()
    } else {
        format!(r#"{{"result": "{result}"}}"#)
    };
    format!(
        r#"{{"schema_version": 1, "program_id": "{program}", "case": "{case}",
"provenance": {{"kind": "hand_computed", "reference": "test", "note": "test"}},
"instruction_data_hex": "", "accounts": [], "expect": {expect}}}
"#
    )
}

/// A chain file holding `chain` under `id`, in canonical form.
fn chain_file(id: &str, chain: &Chain) -> String {
    let StrictValue::Object(mut m) = chain.to_strict() else {
        unreachable!()
    };
    m.insert("schema_version".into(), StrictValue::Int(1));
    m.insert("id".into(), StrictValue::Str(id.into()));
    format!("{}\n", canonical_string(&StrictValue::Object(m)))
}

// ===========================================================================
// 1. An accepted set is exactly what its files and its lock say
// ===========================================================================

#[test]
fn the_sample_set_hash_is_the_sha256sum_golden() {
    let r = sample();
    let set = r.loaded();
    assert_eq!(hex(&set.set_hash()), SAMPLE_SET_HASH);
    assert_eq!(independent_set_hash(&r.root), SAMPLE_SET_HASH);
    assert_eq!(r.read_str(LOCK_FILE), format!("{SAMPLE_SET_HASH}\n"));
    // The library's listing function agrees with the independent one.
    let files: Vec<(String, Vec<u8>)> = SAMPLE_FILES
        .iter()
        .map(|p| (p.to_string(), r.read(p)))
        .collect();
    let pairs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect();
    assert_eq!(hex(&commons::fixtures::set_hash(&pairs, 1)), SAMPLE_SET_HASH);
}

#[test]
fn the_empty_set_is_legal_and_pinned_like_any_other() {
    let r = repo();
    assert_fixture_error(r.load(), LOCK_FILE, "lock", "is missing");
    r.lock_with(EMPTY_SET_HASH);
    let set = r.loaded();
    assert_eq!(hex(&set.set_hash()), EMPTY_SET_HASH);
    assert_eq!(set.counts(), FixtureCounts::default());
    // Empty top-level directories hold no evidence and change nothing.
    for d in [DECODERS_DIR, CHAINS_DIR, MODELS_DIR] {
        fs::create_dir(r.root.join("fixtures").join(d)).unwrap();
    }
    assert_eq!(hex(&r.loaded().set_hash()), EMPTY_SET_HASH);
}

#[test]
fn the_typed_data_is_exactly_what_the_files_hold() {
    let set = sample().loaded();
    assert_eq!(
        set.counts(),
        FixtureCounts {
            files: 6,
            programs: 1,
            decoder_cases: 3,
            chains: 1,
            models: 1,
            model_cases: 2,
        }
    );
    assert_eq!(set.programs().collect::<Vec<_>>(), [&token_id()]);
    assert_eq!(set.models().collect::<Vec<_>>(), ["daily-active-programs"]);
    assert_eq!(set.chain_ids().collect::<Vec<_>>(), ["two-mints-one-failed"]);
    assert_eq!(set.generator_version(), GENERATOR_VERSION);

    // Decoder cases, sorted by case name.
    let cases = set.decoder_cases(&token_id());
    let names: Vec<&str> = cases.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["transfer-basic", "transfer-checked", "truncated-transfer"]);
    let basic = &cases[0];
    assert_eq!(basic.program_id, token_id());
    assert_eq!(basic.data, [0x03, 0xe8, 0x03, 0, 0, 0, 0, 0, 0]);
    assert_eq!(basic.accounts, [[0x11; 32], [0x12; 32], [0x21; 32]]);
    assert_eq!(basic.provenance.kind, ProvenanceKind::ProgramSource);
    assert_eq!(
        basic.provenance.reference,
        "spl-token program/src/instruction.rs, Transfer = 3"
    );
    assert_eq!(
        basic.expect,
        DecoderExpect::Decoded {
            instruction_type: "transfer".into(),
            data: parse_strict(br#"{"amount":1000}"#, &ParseLimits::DEFAULT).unwrap(),
        }
    );
    let checked = &cases[1];
    assert_eq!(checked.data, [0x0c, 0xf4, 0x01, 0, 0, 0, 0, 0, 0, 9]);
    assert_eq!(checked.accounts[1], [0x32; 32]);
    assert_eq!(cases[2].expect, DecoderExpect::DecodeError);
    assert_eq!(cases[2].data, [0x03, 0xe8, 0x03]);
    assert!(set.decoder_cases(&[0u8; 32]).is_empty());

    // The explicit chain.
    let chain = set
        .resolve_chain(&ChainRef::Explicit("two-mints-one-failed".into()))
        .unwrap();
    assert_eq!(chain.row_counts(), [2, 3, 4, 3]);
    assert_eq!(chain.slots[0].slot, 100);
    assert_eq!(chain.slots[0].parent_slot, 99);
    assert_eq!(chain.slots[0].leader.as_deref(), Some(key(0x61).as_str()));
    assert_eq!(chain.slots[1].leader, None);
    assert_eq!(chain.slots[1].parent_slot, 100, "slot 101 was skipped");
    assert_eq!(chain.transactions[1].status, TxStatus::Failed);
    assert_eq!(
        chain.transactions[1].err.as_deref(),
        Some("InstructionError(0, Custom(1))")
    );
    assert_eq!(chain.transactions[1].compute_units_consumed, None);
    assert_eq!(chain.instructions[2].inner_instruction_index, Some(0));
    assert_eq!(
        chain.instructions[2].instruction_type.as_deref(),
        Some("TransferChecked")
    );
    let t = &chain.token_transfers;
    assert_eq!(
        (t[0].amount_raw, t[0].decimals, t[0].mint_token_address.clone()),
        (1000, Some(6), Some(key(0x31)))
    );
    assert_eq!((t[1].amount_raw, t[1].decimals), (500, Some(9)));
    assert_eq!((t[2].mint_token_address.as_deref(), t[2].decimals), (None, None));

    // Model cases, sorted by case name.
    let cases = set.model_cases("daily-active-programs");
    let names: Vec<&str> = cases.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["generated-smoke", "two-mints"]);
    assert_eq!(cases[0].chain, ChainRef::Generated(EXAMPLE_SPEC));
    assert_eq!(cases[0].expect, None);
    assert_eq!((cases[0].from_slot, cases[0].to_slot), (1000, 1499));
    let m = &cases[1];
    assert_eq!(m.chain, ChainRef::Explicit("two-mints-one-failed".into()));
    assert_eq!((m.from_slot, m.to_slot), (100, 102));
    let e = m.expect.as_ref().unwrap();
    assert_eq!(e.columns, ["program_id", "n"]);
    assert!(!e.ordered);
    assert_eq!(
        e.rows,
        vec![
            vec![Cell::Str(TOKEN.into()), Cell::Int(3)],
            vec![Cell::Str(key(0x41)), Cell::Int(1)],
        ]
    );
    assert!(set.model_cases("no-such-model").is_empty());
}

/// Replace one alphanumeric byte with another alphanumeric byte: the text
/// rules still hold, so only the lock can catch it.
fn flip(b: u8) -> u8 {
    match b {
        b'0'..=b'8' | b'a'..=b'y' | b'A'..=b'Y' => b + 1,
        b'9' => b'0',
        b'z' => b'a',
        b'Z' => b'A',
        other => other,
    }
}

#[test]
fn any_changed_byte_in_any_file_fails_the_lock() {
    let r = sample();
    let mut checked = 0;
    for rel in SAMPLE_FILES {
        let original = r.read(rel);
        let positions: Vec<usize> = (0..original.len())
            .filter(|&i| original[i].is_ascii_alphanumeric())
            .collect();
        // First, last and every 29th alphanumeric byte of every file.
        let mut picks: Vec<usize> = positions.iter().copied().step_by(29).collect();
        picks.push(*positions.last().unwrap());
        for i in picks {
            let mut changed = original.clone();
            changed[i] = flip(changed[i]);
            r.write(rel, &changed);
            let (path, field, reason) = fixture_error(r.load());
            assert_eq!((path.as_str(), field.as_str()), (LOCK_FILE, "lock"), "{rel}@{i}");
            let actual = independent_set_hash(&r.root);
            assert!(
                reason.contains(&format!("pins {SAMPLE_SET_HASH} but the fixture set hashes to {actual}")),
                "{rel}@{i}: {reason}"
            );
            checked += 1;
        }
        r.write(rel, &original);
    }
    assert!(checked > 100, "{checked}");
    // Restored byte for byte, it loads again with the pinned hash.
    assert_eq!(hex(&r.loaded().set_hash()), SAMPLE_SET_HASH);
}

#[test]
fn adding_removing_renaming_or_moving_any_file_fails_the_lock() {
    let lock_fails = |r: &Repo, what: &str| {
        let (path, field, _) = fixture_error(r.load());
        assert_eq!((path.as_str(), field.as_str()), (LOCK_FILE, "lock"), "{what}");
    };

    for rel in SAMPLE_FILES {
        let r = sample();
        r.remove(rel);
        lock_fails(&r, &format!("removing {rel}"));
    }

    let r = sample();
    r.write(
        &format!("{PROGRAM_DIR}/another-negative.json"),
        decoder_case(TOKEN, "another-negative", "not_claimed"),
    );
    lock_fails(&r, "adding a valid case");

    // A rename that keeps the file valid (case field renamed too).
    let r = sample();
    let from = format!("{PROGRAM_DIR}/transfer-basic.json");
    let body = r.read_str(&from).replace("\"transfer-basic\"", "\"transfer-plain\"");
    r.remove(&from);
    r.write(&format!("{PROGRAM_DIR}/transfer-plain.json"), body);
    lock_fails(&r, "renaming a case");

    // Moving a model case to another model directory (model field too).
    let r = sample();
    let from = format!("{MODEL_DIR}/two-mints.json");
    let body = r
        .read_str(&from)
        .replace("\"daily-active-programs\"", "\"active-programs\"");
    r.remove(&from);
    r.write("fixtures/models/active-programs/two-mints.json", body);
    lock_fails(&r, "moving a case");

    // Swapping the contents of two files.
    let r = sample();
    let a = r.read(SAMPLE_FILES[1]);
    let b = r.read(SAMPLE_FILES[2]);
    r.write(SAMPLE_FILES[1], b);
    r.write(SAMPLE_FILES[2], a);
    lock_fails(&r, "swapping two files");
}

#[test]
fn only_a_lock_update_makes_a_changed_set_load_and_then_it_is_that_set() {
    let r = sample();
    r.edit(
        &format!("{PROGRAM_DIR}/transfer-basic.json"),
        "\"amount\": 1000",
        "\"amount\": 1001",
    );
    assert!(r.load().is_err());
    let new_hash = r.relock();
    assert_ne!(new_hash, SAMPLE_SET_HASH);
    let set = r.loaded();
    assert_eq!(hex(&set.set_hash()), new_hash);
    let DecoderExpect::Decoded { data, .. } = &set.decoder_cases(&token_id())[0].expect else {
        panic!()
    };
    assert_eq!(
        canonical_string(data),
        r#"{"amount":1001}"#,
        "the loaded data is the new file, not the old one"
    );
    // Going back needs the old lock back.
    r.edit(
        &format!("{PROGRAM_DIR}/transfer-basic.json"),
        "\"amount\": 1001",
        "\"amount\": 1000",
    );
    assert!(r.load().is_err());
    r.lock_with(SAMPLE_SET_HASH);
    assert_eq!(hex(&r.loaded().set_hash()), SAMPLE_SET_HASH);
}

#[test]
fn the_generator_version_is_part_of_what_the_lock_pins() {
    let r = sample();
    let set = r.loaded();
    assert_eq!(set.generator_version(), 1);
    let files: Vec<(String, Vec<u8>)> = SAMPLE_FILES
        .iter()
        .map(|p| (p.to_string(), r.read(p)))
        .collect();
    let pairs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_slice()))
        .collect();
    assert_eq!(
        commons::fixtures::set_hash(&pairs, GENERATOR_VERSION),
        set.set_hash()
    );
    assert_ne!(
        commons::fixtures::set_hash(&pairs, GENERATOR_VERSION + 1),
        set.set_hash(),
        "a new generator version needs a new lock"
    );
}

#[test]
fn lock_file_rules() {
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (b"".to_vec(), "is empty"),
        (b"\n".to_vec(), "64 lowercase hex"),
        (SAMPLE_SET_HASH.as_bytes().to_vec(), "does not end with a newline"),
        (format!("{SAMPLE_SET_HASH}\n\n").into_bytes(), "more than one newline"),
        (format!("{SAMPLE_SET_HASH}\r\n").into_bytes(), "carriage return"),
        (format!("{}\n", SAMPLE_SET_HASH.to_uppercase()).into_bytes(), "64 lowercase hex"),
        (format!("{SAMPLE_SET_HASH} \n").into_bytes(), "64 lowercase hex"),
        (format!(" {SAMPLE_SET_HASH}\n").into_bytes(), "64 lowercase hex"),
        (format!("sha256:{SAMPLE_SET_HASH}\n").into_bytes(), "64 lowercase hex"),
        (format!("{SAMPLE_SET_HASH}\n{SAMPLE_SET_HASH}\n").into_bytes(), "more than the limit"),
        (format!("\u{FEFF}{SAMPLE_SET_HASH}\n").into_bytes(), "byte-order mark"),
        (format!("{EMPTY_SET_HASH}\n").into_bytes(), "but the fixture set hashes to"),
    ];
    for (bytes, part) in cases {
        let r = sample();
        fs::write(r.path(LOCK_FILE), &bytes).unwrap();
        assert_fixture_error(r.load(), LOCK_FILE, "lock", part);
    }

    let r = sample();
    r.remove(LOCK_FILE);
    assert_fixture_error(r.load(), LOCK_FILE, "lock", "is missing");
    fs::create_dir(r.path(LOCK_FILE)).unwrap();
    assert_fixture_error(r.load(), LOCK_FILE, "lock", "not a regular file");

    #[cfg(unix)]
    {
        let r = sample();
        let elsewhere = r.path("real.lock");
        fs::rename(r.path(LOCK_FILE), &elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, r.path(LOCK_FILE)).unwrap();
        assert_fixture_error(r.load(), LOCK_FILE, "lock", "is a symlink");
    }
}

// ===========================================================================
// 2. The same fixture always gives the same hash and the same typed data
// ===========================================================================

#[test]
fn loading_the_same_files_twice_gives_identical_sets() {
    let r = sample();
    let a = r.loaded();
    let b = r.loaded();
    assert_eq!(a, b);
    assert_eq!(a.set_hash(), b.set_hash());
    assert_eq!(a.counts(), b.counts());
    for model in a.models() {
        assert_eq!(a.model_cases(model), b.model_cases(model));
    }
}

#[test]
fn the_same_files_anywhere_in_any_creation_order_give_the_same_set() {
    let a = sample_in_order(false);
    let b = sample_in_order(true);
    // A third copy, deeper and with a different directory name.
    let c = {
        let tmp = tempfile::tempdir().unwrap();
        let root = fs::canonicalize(tmp.path()).unwrap().join("deeper").join("checkout");
        fs::create_dir_all(&root).unwrap();
        copy_tree(Path::new(SAMPLE_DIR), &root, true);
        let mut config = a.config.clone();
        config.repo_root = root.clone();
        config.fixtures_dir = Some(root.join("fixtures"));
        (tmp, config)
    };
    let sa = a.loaded();
    let sb = b.loaded();
    let sc = FixtureSet::load(&c.1).unwrap();
    assert_ne!(a.root, b.root);
    assert_eq!(sa, sb);
    assert_eq!(sa, sc);
    assert_eq!(hex(&sa.set_hash()), SAMPLE_SET_HASH);
    assert_eq!(sb.set_hash(), sc.set_hash());
}

#[test]
fn metadata_is_not_evidence() {
    let r = sample();
    let before = r.loaded();
    // Rewriting a file with the same bytes changes its mtime and inode.
    for rel in SAMPLE_FILES {
        let b = r.read(rel);
        r.remove(rel);
        r.write(rel, b);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = r.path(SAMPLE_FILES[0]);
        fs::set_permissions(&p, fs::Permissions::from_mode(0o444)).unwrap();
    }
    assert_eq!(r.loaded(), before);
}

#[test]
fn formatting_changes_the_hash_but_never_the_typed_data() {
    let r = sample();
    let before = r.loaded();
    // Re-write every file canonically: compact, keys sorted.
    for rel in SAMPLE_FILES {
        let v = parse_strict(&r.read(rel), &ParseLimits::DEFAULT).unwrap();
        r.write(rel, format!("{}\n", canonical_string(&v)));
    }
    assert!(r.load().is_err(), "the bytes changed, so the lock fails");
    let new_hash = r.relock();
    assert_ne!(new_hash, SAMPLE_SET_HASH, "the hash covers bytes, not meaning");
    let after = r.loaded();
    assert_eq!(after.counts(), before.counts());
    assert_eq!(
        after.decoder_cases(&token_id()),
        before.decoder_cases(&token_id())
    );
    for m in before.models() {
        assert_eq!(after.model_cases(m), before.model_cases(m));
    }
    let id = ChainRef::Explicit("two-mints-one-failed".into());
    assert_eq!(
        after.resolve_chain(&id).unwrap(),
        before.resolve_chain(&id).unwrap()
    );
}

#[test]
fn the_typed_chain_is_the_files_value_exactly() {
    let r = sample();
    let set = r.loaded();
    let chain = set
        .resolve_chain(&ChainRef::Explicit("two-mints-one-failed".into()))
        .unwrap();
    // The file's value, re-built from the typed chain, is the file's value.
    let file = parse_strict(&r.read(CHAIN_FILE), &ParseLimits::DEFAULT).unwrap();
    let rebuilt =
        parse_strict(chain_file("two-mints-one-failed", &chain).as_bytes(), &ParseLimits::DEFAULT)
            .unwrap();
    assert_eq!(first_difference(&file, &rebuilt), None);
    assert_eq!(file, rebuilt);
    // And so is every decoder case's expected data.
    for c in set.decoder_cases(&token_id()) {
        let rel = format!("{PROGRAM_DIR}/{}.json", c.name);
        let StrictValue::Object(f) = parse_strict(&r.read(&rel), &ParseLimits::DEFAULT).unwrap()
        else {
            panic!()
        };
        let StrictValue::Object(expect) = &f["expect"] else { panic!() };
        match &c.expect {
            DecoderExpect::Decoded { data, .. } => assert_eq!(&expect["data"], data),
            other => assert_eq!(expect["result"], StrictValue::Str(other.result_str().into())),
        }
    }
}

// ===========================================================================
// 3. The generator produces the same chain everywhere
// ===========================================================================

/// Row counts and SHA-256 of the canonical bytes of `generate(spec)`.
///
/// Computed twice, independently: by this crate, and by a separate Python
/// implementation written only from the order of draws documented in
/// `chain_gen.rs` (with `json.dumps(sort_keys=True, separators=(',', ':'),
/// ensure_ascii=False)` as the canonical form). Both agree on all four.
///
/// A change here is a change of generator output, which needs a new
/// GENERATOR_VERSION and so a new fixtures.lock.
const GOLDEN_DIGESTS: [(GenSpec, [usize; 4], &str); 4] = [
    (
        EXAMPLE_SPEC,
        [474, 2100, 5730, 791],
        "e6a89cf5f75aa3bd9a63a9a4370c22825592c9352a85db379c80069d43c55264",
    ),
    (
        GenSpec {
            seed: 0,
            start_slot: 1,
            slots: 50,
            skip_percent: 0,
            max_tx_per_slot: 4,
            fail_percent: 0,
            transfer_percent: 100,
            mint_null_percent: 0,
        },
        [50, 118, 337, 167],
        "b51be7d7a23f22067593493bd15f490d39914c440d00efb28fd034663115ee91",
    ),
    (
        GenSpec {
            seed: u64::MAX,
            start_slot: 1 << 48,
            slots: 200,
            skip_percent: 50,
            max_tx_per_slot: 16,
            fail_percent: 50,
            transfer_percent: 50,
            mint_null_percent: 50,
        },
        [90, 744, 2078, 470],
        "1d0a3b2eaa7d6b492c1e563136d44ca0b9d848670f74f9f2e75046503cb3872e",
    ),
    (
        GenSpec {
            seed: 42,
            start_slot: 7,
            slots: 1,
            skip_percent: 0,
            max_tx_per_slot: 1,
            fail_percent: 0,
            transfer_percent: 0,
            mint_null_percent: 0,
        },
        [1, 1, 1, 0],
        "3b3bfc62a3e219759c649f911d829d3aa7353f85206ae1d5fd03f7915268222c",
    ),
];

#[test]
fn generator_output_is_pinned_by_digest() {
    for (spec, counts, digest) in GOLDEN_DIGESTS {
        let chain = generate(&spec);
        assert_eq!(chain.row_counts(), counts, "{spec:?}");
        assert_eq!(hex(&chain.digest()), digest, "{spec:?}");
        assert_eq!(check_consistency(&chain), Ok(()), "{spec:?}");
    }
}

#[test]
fn generator_first_values_match_a_computation_outside_rust() {
    // From a Python SplitMix64 following the documented order of draws:
    // 115 setup draws, then slot 1000's skip draw (not skipped at 5%),
    // then three draws for its blockhash, tag 6, slot 1000.
    let chain = generate(&EXAMPLE_SPEC);
    assert_eq!(chain.slots[0].slot, 1000);
    assert_eq!(chain.slots[0].parent_slot, 999);
    assert_eq!(
        chain.slots[0].blockhash,
        "EQV257QGtD9hX4Sjmm8eGQsbRufuBp6gbDw1FVmxZbdZ"
    );
    let raw = bs58::decode(&chain.slots[0].blockhash).into_vec().unwrap();
    assert_eq!(raw[24], 6, "blockhash tag");
    assert_eq!(&raw[25..], &1000u64.to_le_bytes()[..7], "blockhash carries its slot");
    // Pure arithmetic values.
    assert_eq!(chain.slots[0].block_time, Some(1_600_000_000 + 400));
    assert_eq!(chain.slots[0].block_height, Some(900));
}

#[test]
fn every_path_to_a_generated_chain_gives_the_same_chain() {
    let set = sample().loaded();
    let spec = EXAMPLE_SPEC;
    let direct = generate(&spec);
    let via_set = set.resolve_chain(&ChainRef::Generated(spec)).unwrap();
    let via_case = set
        .resolve_chain(&set.model_cases("daily-active-programs")[0].chain)
        .unwrap();
    assert_eq!(*via_set, direct);
    assert_eq!(*via_case, direct);
    // Built on demand, not cached: a fresh value each call.
    let again = set.resolve_chain(&ChainRef::Generated(spec)).unwrap();
    assert!(!Arc::ptr_eq(&via_set, &again));
    assert_eq!(via_set, again);
    // A different set (the empty one) generates the same chain too.
    let empty = repo();
    empty.lock_with(EMPTY_SET_HASH);
    assert_eq!(
        *empty.loaded().resolve_chain(&ChainRef::Generated(spec)).unwrap(),
        direct
    );
}

#[test]
fn a_generated_chain_written_as_an_explicit_chain_loads_back_identical() {
    let spec = GenSpec {
        seed: 3,
        start_slot: 500,
        slots: 40,
        skip_percent: 10,
        max_tx_per_slot: 4,
        fail_percent: 20,
        transfer_percent: 60,
        mint_null_percent: 25,
    };
    let chain = generate(&spec);
    assert!(chain.row_counts().iter().all(|&n| n <= MAX_ROWS_PER_TABLE));
    let r = repo();
    r.write("fixtures/chains/frozen.json", chain_file("frozen", &chain));
    r.relock();
    let set = r.loaded();
    let loaded = set.resolve_chain(&ChainRef::Explicit("frozen".into())).unwrap();
    assert_eq!(*loaded, chain, "the generator's output meets every explicit chain rule");
    assert_eq!(loaded.digest(), generate(&spec).digest());
}

#[test]
fn the_generator_gives_the_same_chain_on_every_thread() {
    let expected = generate(&EXAMPLE_SPEC).digest();
    let digests: Vec<[u8; 32]> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..4)
            .map(|_| s.spawn(|| generate(&EXAMPLE_SPEC).digest()))
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    assert!(digests.iter().all(|d| *d == expected));
}

// ===========================================================================
// The writer is the authority for row columns
// ===========================================================================

#[test]
fn row_columns_are_exactly_prisms_writer_schemas() {
    use arrow_schema::{DataType, TimeUnit};
    use prism::storage::TableKind;
    use prism::storage::writer::schema_for;

    for (table, kind) in Table::ALL.into_iter().zip(TableKind::ALL) {
        assert_eq!(table.as_str(), kind.as_str());
        let schema = schema_for(kind);
        let writer: Vec<(String, DataType, bool)> = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone(), f.is_nullable()))
            .collect();
        let ours: Vec<(String, DataType, bool)> = table
            .columns()
            .iter()
            .map(|c| {
                let ty = match c.ty {
                    ColumnType::UInt64 => DataType::UInt64,
                    ColumnType::UInt32 => DataType::UInt32,
                    ColumnType::UInt8 => DataType::UInt8,
                    ColumnType::TimestampSecond => {
                        DataType::Timestamp(TimeUnit::Second, Some("UTC".into()))
                    }
                    ColumnType::Utf8 | ColumnType::JsonText => DataType::Utf8,
                };
                (c.name.to_string(), ty, c.nullable)
            })
            .collect();
        assert_eq!(ours, writer, "{}", table.as_str());
    }
}

// ===========================================================================
// Layout rules
// ===========================================================================

#[test]
fn only_the_three_directories_are_allowed_at_the_top() {
    for (entry, is_dir) in [
        ("README.md", false),
        ("decoders.json", false),
        ("other", true),
        ("Decoders", true),
    ] {
        let r = sample();
        let p = r.path(&format!("fixtures/{entry}"));
        if is_dir {
            fs::create_dir(&p).unwrap();
        } else {
            fs::write(&p, "x\n").unwrap();
        }
        assert_fixture_error(
            r.load(),
            &format!("fixtures/{entry}"),
            "directory",
            "only the directories decoders/, chains/ and models/",
        );
    }
    // A file named like a directory.
    let r = repo();
    fs::write(r.path("fixtures/chains"), "x\n").unwrap();
    assert_fixture_error(r.load(), "fixtures/chains", "directory", "only the directories");
}

#[test]
fn hidden_entries_are_rejected_at_every_depth() {
    for rel in [
        "fixtures/.git",
        "fixtures/chains/.keep",
        &format!("{PROGRAM_DIR}/.case.json"),
        "fixtures/models/.hidden",
        &format!("{MODEL_DIR}/.DS_Store"),
    ] {
        let r = sample();
        r.write(rel, "x\n");
        assert_fixture_error(r.load(), rel, "directory", "hidden entries are not allowed");
    }
}

#[cfg(unix)]
#[test]
fn symlinks_are_rejected_everywhere() {
    use std::os::unix::fs::symlink;
    let target = |r: &Repo| r.path(SAMPLE_FILES[1]);

    let r = sample();
    symlink(target(&r), r.path(&format!("{PROGRAM_DIR}/linked.json"))).unwrap();
    assert_fixture_error(
        r.load(),
        &format!("{PROGRAM_DIR}/linked.json"),
        "directory",
        "symlinks are not allowed",
    );

    let r = sample();
    symlink(r.path(PROGRAM_DIR), r.path("fixtures/decoders/11111111111111111111111111111111"))
        .unwrap();
    assert_fixture_error(
        r.load(),
        "fixtures/decoders/11111111111111111111111111111111",
        "directory",
        "symlinks",
    );

    let r = sample();
    let elsewhere = r.path("models-real");
    fs::rename(r.path("fixtures/models"), &elsewhere).unwrap();
    symlink(&elsewhere, r.path("fixtures/models")).unwrap();
    assert_fixture_error(r.load(), "fixtures/models", "directory", "symlinks");

    let r = sample();
    let elsewhere = r.path("fixtures-real");
    fs::rename(r.path("fixtures"), &elsewhere).unwrap();
    symlink(&elsewhere, r.path("fixtures")).unwrap();
    assert_fixture_error(r.load(), "fixtures", "directory", "is a symlink");
}

#[test]
fn files_and_directories_must_sit_at_the_right_depth() {
    let cases = [
        ("fixtures/decoders/case.json", "only program directories"),
        ("fixtures/models/case.json", "only model directories"),
        (&format!("{PROGRAM_DIR}/sub/case.json") as &str, "only .json files are allowed here"),
        (&format!("{MODEL_DIR}/sub/case.json"), "only .json files are allowed here"),
        ("fixtures/chains/sub/x.json", "only .json files are allowed here"),
    ];
    for (rel, part) in cases {
        let r = sample();
        r.write(rel, "{}\n");
        let (path, field, reason) = fixture_error(r.load());
        assert_eq!(field, "directory", "{rel}: {reason}");
        assert!(rel.starts_with(&path), "{rel} vs {path}");
        assert!(reason.contains(part), "{rel}: expected {part:?} in {reason}");
    }
}

#[test]
fn names_follow_the_rules() {
    for (rel, part) in [
        (&format!("{PROGRAM_DIR}/case.JSON") as &str, "must have the .json extension"),
        (&format!("{PROGRAM_DIR}/case"), "must have the .json extension"),
        (&format!("{PROGRAM_DIR}/case.json.bak"), "must have the .json extension"),
        (&format!("{PROGRAM_DIR}/Case.json"), "only a-z"),
        (&format!("{PROGRAM_DIR}/a.b.json"), "only a-z"),
        (&format!("{PROGRAM_DIR}/a--b.json"), "--"),
        ("fixtures/chains/two_mints.json", "only a-z"),
        ("fixtures/models/Daily/x.json", "directory name"),
        ("fixtures/decoders/Tokenkeg/x.json", "not a program address"),
        ("fixtures/decoders/1111111111111111111111111111111/x.json", "31 bytes"),
        ("fixtures/decoders/0okenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA/x.json", "not valid base58"),
    ] {
        let r = sample();
        r.write(rel, "{}\n");
        let (_, field, reason) = fixture_error(r.load());
        assert_eq!(field, "directory", "{rel}: {reason}");
        assert!(reason.contains(part), "{rel}: expected {part:?} in {reason}");
    }
}

#[test]
fn program_and_model_directories_must_hold_cases() {
    let r = sample();
    fs::create_dir(r.path("fixtures/decoders/11111111111111111111111111111111")).unwrap();
    assert_fixture_error(
        r.load(),
        "fixtures/decoders/11111111111111111111111111111111",
        "directory",
        "holds no cases",
    );
    let r = sample();
    fs::create_dir(r.path("fixtures/models/empty-model")).unwrap();
    assert_fixture_error(r.load(), "fixtures/models/empty-model", "directory", "holds no cases");
}

#[test]
fn the_fixtures_directory_must_be_configured_and_real() {
    let r = sample();
    let mut config = r.config.clone();
    config.fixtures_dir = None;
    let e = FixtureSet::load(&config).unwrap_err();
    assert!(matches!(e, CommonsError::Internal(_)), "{e:?}");
    assert_eq!(e.exit_code(), EXIT_INTERNAL);

    let r = sample();
    fs::rename(r.path("fixtures"), r.path("gone")).unwrap();
    assert_fixture_error(r.load(), "fixtures", "directory", "does not exist");
    fs::write(r.path("fixtures"), "x\n").unwrap();
    assert_fixture_error(r.load(), "fixtures", "directory", "not a directory");
}

// ===========================================================================
// File rules
// ===========================================================================

#[test]
fn files_are_read_with_the_text_rules() {
    let rel = &format!("{PROGRAM_DIR}/transfer-basic.json");
    let good = sample().read_str(rel);
    let cases: Vec<(Vec<u8>, &str)> = vec![
        (good.replace('\n', "\r\n").into_bytes(), "carriage return"),
        ([b"\xEF\xBB\xBF".as_slice(), good.as_bytes()].concat(), "byte-order mark"),
        (good.trim_end().as_bytes().to_vec(), "does not end with a newline"),
        (format!("{good}\n").into_bytes(), "more than one newline"),
        ([good.as_bytes(), b"\xff\n"].concat(), "not valid UTF-8"),
        (Vec::new(), "is empty"),
    ];
    for (bytes, part) in cases {
        let r = sample();
        r.write(rel, &bytes);
        assert_fixture_error(r.load(), rel, "file", part);
    }
}

#[test]
fn per_file_limits_are_exact() {
    let r = repo();
    // A decoder case padded with spaces to exactly the limit loads...
    let pid = key(9);
    let neg = format!("fixtures/decoders/{pid}/neg.json");
    r.write(&neg, decoder_case(&pid, "neg", "not_claimed"));
    let body = decoder_case(&pid, "pos", "decoded");
    let pad = MAX_DECODER_CASE_BYTES as usize - body.len();
    let at_limit = format!("{}{}\n", body.trim_end(), " ".repeat(pad));
    assert_eq!(at_limit.len() as u64, MAX_DECODER_CASE_BYTES);
    let pos = format!("fixtures/decoders/{pid}/pos.json");
    r.write(&pos, &at_limit);
    r.relock();
    assert_eq!(r.loaded().counts().decoder_cases, 2);
    // ...one more byte does not, and that is found before the lock.
    r.write(&pos, format!(" {at_limit}"));
    assert_fixture_error(r.load(), &pos, "file", "65537 bytes, more than the limit of 65536");
}

// ===========================================================================
// Parse, names, coverage, references, consistency
// ===========================================================================

#[test]
fn parse_errors_name_the_file_and_field() {
    let rel = format!("{PROGRAM_DIR}/transfer-basic.json");
    for (from, to, field, part) in [
        ("\"amount\": 1000 }", "\"amount\": 1000, \"amount\": 1000 }", "expect.data", "duplicate key 'amount'"),
        ("\"amount\": 1000", "\"amount\": 1000.0", "expect.data.amount", "only integers"),
        ("\"amount\": 1000", "\"amount\": 1e3", "expect.data.amount", "only integers"),
        ("\"schema_version\": 1", "\"schema_version\": 2", "schema_version", "not supported"),
        ("\"case\": \"transfer-basic\",", "\"case\": \"transfer-basic\", \"author\": \"me\",", "file", "unknown field 'author'"),
        ("\"kind\": \"program_source\"", "\"kind\": \"vibes\"", "provenance.kind", "not a provenance kind"),
        ("\"03e803000000000000\"", "\"03E8\"", "instruction_data_hex", "not lowercase hex"),
    ] {
        let r = sample();
        r.edit(&rel, from, to);
        r.relock();
        assert_fixture_error(r.load(), &rel, field, part);
    }
}

#[test]
fn names_inside_must_match_names_outside() {
    let r = sample();
    r.edit(&format!("{PROGRAM_DIR}/transfer-basic.json"), "\"case\": \"transfer-basic\"", "\"case\": \"transfer-other\"");
    r.relock();
    assert_fixture_error(
        r.load(),
        &format!("{PROGRAM_DIR}/transfer-basic.json"),
        "case",
        "is 'transfer-other' but the file name is 'transfer-basic'",
    );

    let r = sample();
    let other = key(5);
    r.edit(&format!("{PROGRAM_DIR}/transfer-basic.json"), &format!("\"program_id\": \"{TOKEN}\""), &format!("\"program_id\": \"{other}\""));
    r.relock();
    assert_fixture_error(
        r.load(),
        &format!("{PROGRAM_DIR}/transfer-basic.json"),
        "program_id",
        "but the directory name is 'Tokenkeg",
    );

    let r = sample();
    r.edit(CHAIN_FILE, "\"id\": \"two-mints-one-failed\"", "\"id\": \"two-mints\"");
    r.relock();
    assert_fixture_error(r.load(), CHAIN_FILE, "id", "is 'two-mints' but the file name");

    let r = sample();
    r.edit(&format!("{MODEL_DIR}/two-mints.json"), "\"model\": \"daily-active-programs\"", "\"model\": \"daily-programs\"");
    r.relock();
    assert_fixture_error(r.load(), &format!("{MODEL_DIR}/two-mints.json"), "model", "but the directory name");
}

#[test]
fn every_program_needs_positive_and_negative_cases() {
    let r = sample();
    r.remove(&format!("{PROGRAM_DIR}/truncated-transfer.json"));
    r.relock();
    assert_fixture_error(r.load(), PROGRAM_DIR, "set", "has no negative case");

    let r = sample();
    r.remove(&format!("{PROGRAM_DIR}/transfer-basic.json"));
    r.remove(&format!("{PROGRAM_DIR}/transfer-checked.json"));
    r.relock();
    assert_fixture_error(r.load(), PROGRAM_DIR, "set", "has no positive case");

    // not_claimed counts as negative just like decode_error.
    let r = sample();
    r.remove(&format!("{PROGRAM_DIR}/truncated-transfer.json"));
    r.write(&format!("{PROGRAM_DIR}/foreign.json"), decoder_case(TOKEN, "foreign", "not_claimed"));
    r.relock();
    assert_eq!(r.loaded().decoder_cases(&token_id()).len(), 3);
}

#[test]
fn explicit_chain_references_must_resolve() {
    let r = sample();
    r.remove(CHAIN_FILE);
    r.relock();
    assert_fixture_error(
        r.load(),
        &format!("{MODEL_DIR}/two-mints.json"),
        "chain.explicit",
        "no chain 'two-mints-one-failed'",
    );
    let set = sample().loaded();
    let (path, field, _) = match set.resolve_chain(&ChainRef::Explicit("nope".into())) {
        Err(CommonsError::FixtureError { path, field, reason }) => (path, field, reason),
        other => panic!("{other:?}"),
    };
    assert_eq!((path.as_str(), field.as_str()), ("fixtures/chains/nope.json", "chain.explicit"));
    let bad = GenSpec { slots: 0, ..EXAMPLE_SPEC };
    assert!(matches!(
        set.resolve_chain(&ChainRef::Generated(bad)),
        Err(CommonsError::FixtureError { .. })
    ));
}

#[test]
fn explicit_chains_must_be_consistent() {
    for (from, to, field, part) in [
        ("\"transaction_count\": 2", "\"transaction_count\": 3", "slots[0]", "transaction_count is 3"),
        ("\"parent_slot\": 100", "\"parent_slot\": 101", "slots[1]", "not a slot of the chain"),
        ("\"parent_slot\": 100", "\"parent_slot\": 102", "slots[1]", "not smaller than slot 102"),
    ] {
        let r = sample();
        r.edit(CHAIN_FILE, from, to);
        r.relock();
        assert_fixture_error(r.load(), CHAIN_FILE, field, part);
    }
    // The same mint with two decimals values is inconsistent.
    let r = sample();
    let mint = |k: String| format!("\"mint_token_address\": \"{k}\"");
    r.edit(CHAIN_FILE, &mint(key(0x32)), &mint(key(0x31)));
    r.relock();
    assert_fixture_error(r.load(), CHAIN_FILE, "token_transfers[1]", "has decimals 9 here but 6");
}

#[test]
fn expected_rows_with_a_generated_chain_are_rejected() {
    let r = sample();
    r.edit(
        &format!("{MODEL_DIR}/generated-smoke.json"),
        "\"window\": { \"from_slot\": 1000, \"to_slot\": 1499 }",
        "\"window\": { \"from_slot\": 1000, \"to_slot\": 1499 },\n  \"expect\": { \"columns\": [\"n\"], \"ordered\": true, \"rows\": [[1]] }",
    );
    r.relock();
    assert_fixture_error(
        r.load(),
        &format!("{MODEL_DIR}/generated-smoke.json"),
        "expect",
        "only come with explicit chains",
    );
}

// ===========================================================================
// Order of errors, echo, exit codes, immutability
// ===========================================================================

#[test]
fn the_first_error_wins_in_load_order() {
    // Walk beats read beats lock beats parse.
    let r = sample();
    r.write(&format!("{PROGRAM_DIR}/transfer-basic.json"), "not json\r\n");
    r.write("fixtures/README.md", "x\n");
    r.lock_with(EMPTY_SET_HASH);
    let (path, ..) = fixture_error(r.load());
    assert_eq!(path, "fixtures/README.md", "walk first");
    r.remove("fixtures/README.md");
    let (_, field, _) = fixture_error(r.load());
    assert_eq!(field, "file", "then the text rules");
    r.write(&format!("{PROGRAM_DIR}/transfer-basic.json"), "not json\n");
    let (path, ..) = fixture_error(r.load());
    assert_eq!(path, LOCK_FILE, "then the lock, before any parsing");
    r.relock();
    let (path, field, _) = fixture_error(r.load());
    assert_eq!((path.as_str(), field.as_str()), (SAMPLE_FILES[1], "file"), "then parsing");

    // Parsing goes in listing order: the chain file sorts first.
    let r = sample();
    r.write(&format!("{PROGRAM_DIR}/transfer-basic.json"), "[]\n");
    r.write(CHAIN_FILE, "[]\n");
    r.write(&format!("{MODEL_DIR}/two-mints.json"), "[]\n");
    r.relock();
    let (path, ..) = fixture_error(r.load());
    assert_eq!(path, CHAIN_FILE);

    // Text rules go in listing order too.
    let r = sample();
    r.write(&format!("{MODEL_DIR}/two-mints.json"), "x");
    r.write(&format!("{PROGRAM_DIR}/transfer-checked.json"), "x");
    let (path, ..) = fixture_error(r.load());
    assert_eq!(path, format!("{PROGRAM_DIR}/transfer-checked.json"));
}

#[test]
fn contributor_names_are_echoed_in_errors() {
    let r = sample();
    r.write("fixtures/evil\u{1b}[2J\u{202E}.json", "x\n");
    let e = r.load().unwrap_err();
    let msg = e.to_string();
    assert!(!msg.contains('\u{1b}') && !msg.contains('\u{202E}'), "{msg:?}");
    assert!(msg.contains("\\u{001B}"), "{msg}");
}

#[test]
fn fixture_errors_exit_with_code_1() {
    let r = sample();
    r.lock_with(EMPTY_SET_HASH);
    let e = r.load().unwrap_err();
    assert_eq!(e.exit_code(), EXIT_ARTIFACT);
    assert!(
        e.to_string().starts_with("fixture fixtures.lock: field lock: pins "),
        "{e}"
    );
}

#[test]
fn a_loaded_set_is_shared_and_immutable() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<FixtureSet>();
    assert_send_sync::<Arc<Chain>>();

    let set = sample().loaded();
    let id = ChainRef::Explicit("two-mints-one-failed".into());
    let a = set.resolve_chain(&id).unwrap();
    let b = set.resolve_chain(&id).unwrap();
    assert!(Arc::ptr_eq(&a, &b), "explicit chains are loaded once and shared");

    // Shared across threads, every thread sees the same set.
    let set = Arc::new(set);
    let hashes: Vec<[u8; 32]> = std::thread::scope(|s| {
        (0..4)
            .map(|_| {
                let set = Arc::clone(&set);
                s.spawn(move || set.set_hash())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect()
    });
    assert!(hashes.iter().all(|h| hex(h) == SAMPLE_SET_HASH));
}

#[test]
fn loading_never_writes() {
    let r = sample();
    let before = independent_set_hash(&r.root);
    let lock = r.read(LOCK_FILE);
    let _ = r.loaded();
    r.lock_with(EMPTY_SET_HASH);
    let _ = r.load().unwrap_err();
    assert_eq!(independent_set_hash(&r.root), before);
    assert_eq!(r.read_str(LOCK_FILE), format!("{EMPTY_SET_HASH}\n"), "a failing load does not fix the lock");
    fs::write(r.path(LOCK_FILE), lock).unwrap();
    let mut names: Vec<String> = fs::read_dir(&r.root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["fixtures", LOCK_FILE], "nothing new at the repo root");
}

