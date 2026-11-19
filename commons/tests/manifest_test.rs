//! Filesystem-level tests for `commons::manifest`: `load`, `discover` and
//! `check_unique` against real directory trees.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use commons::config::{
    ARTIFACTS_DIR, CommonsConfig, CommonsError, DEFAULT_MAX_ARTIFACT_BYTES, EXIT_ARTIFACT,
    EXIT_INTERNAL, LogLevel, OutputFormat,
};
use commons::manifest::*;
use tempfile::TempDir;

const PID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const IDL: &[u8] = b"{\"version\":\"0.1.0\",\"name\":\"spl_token\",\"instructions\":[]}\n";
const SQL: &[u8] = b"SELECT slot, count(*) FROM transactions GROUP BY slot;\n";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

struct Repo {
    _tmp: TempDir,
    art: PathBuf,
    config: CommonsConfig,
}

fn repo() -> Repo {
    let tmp = tempfile::tempdir().unwrap();
    let root = fs::canonicalize(tmp.path()).unwrap();
    let art = root.join(ARTIFACTS_DIR);
    fs::create_dir(&art).unwrap();
    let config = CommonsConfig {
        repo_root: root,
        artifacts_dir: art.clone(),
        fixtures_dir: None,
        format: OutputFormat::Text,
        log_level: LogLevel::Info,
        max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
        lens_url: None,
        verify_timeout: Duration::from_secs(60),
    };
    Repo {
        _tmp: tmp,
        art,
        config,
    }
}

fn default_content(kind: &str) -> &'static [u8] {
    if kind == "decoder" { IDL } else { SQL }
}

fn content_name(kind: &str) -> &'static str {
    content_file_name(kind.parse().unwrap())
}

/// A pretty-printed manifest. `program_id` is included for decoders.
fn manifest_json(kind: &str, name: &str, version: &str, content: &[u8]) -> String {
    let pid = if kind == "decoder" {
        format!(",\n  \"program_id\": \"{PID}\"")
    } else {
        String::new()
    };
    format!(
        "{{\n  \"schema_version\": 1,\n  \"kind\": \"{kind}\",\n  \"name\": \"{name}\",\n  \
         \"version\": \"{version}\",\n  \"license\": \"Apache-2.0\",\n  \
         \"description\": \"Test artifact.\",\n  \"content_sha256\": \"{}\"{pid}\n}}\n",
        to_hex(&content_sha256(content))
    )
}

fn version_dir(r: &Repo, status: &str, kind: &str, name: &str, version: &str) -> PathBuf {
    r.art.join(status).join(kind).join(name).join(version)
}

/// Write a directory with exactly these files.
fn write_files(dir: &Path, files: &[(&str, &[u8])]) -> PathBuf {
    fs::create_dir_all(dir).unwrap();
    for (name, bytes) in files {
        fs::write(dir.join(name), bytes).unwrap();
    }
    dir.to_path_buf()
}

/// A valid artifact with default content.
fn artifact(r: &Repo, status: &str, kind: &str, name: &str, version: &str) -> PathBuf {
    let content = default_content(kind);
    artifact_with(
        r,
        status,
        kind,
        name,
        version,
        &manifest_json(kind, name, version, content),
        content,
    )
}

/// An artifact whose path is built from `status/kind/name/version` and whose
/// files are the given manifest and content (named for the path's kind).
fn artifact_with(
    r: &Repo,
    status: &str,
    kind: &str,
    name: &str,
    version: &str,
    manifest: &str,
    content: &[u8],
) -> PathBuf {
    write_files(
        &version_dir(r, status, kind, name, version),
        &[
            (MANIFEST_FILE, manifest.as_bytes()),
            (content_name(kind), content),
        ],
    )
}

/// An artifact with valid manifest for `content`, whatever the content is.
fn decoder_with_content(r: &Repo, content: &[u8]) -> PathBuf {
    artifact_with(
        r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &manifest_json("decoder", "spl-token", "1.0.0", content),
        content,
    )
}

/// The `(artifact, field, reason)` of a `ManifestError`.
#[track_caller]
fn merr<T: std::fmt::Debug>(res: Result<T, CommonsError>) -> (String, String, String) {
    match res {
        Err(CommonsError::ManifestError {
            artifact,
            field,
            reason,
        }) => (artifact, field, reason),
        other => panic!("expected ManifestError, got {other:?}"),
    }
}

#[track_caller]
fn assert_merr<T: std::fmt::Debug>(res: Result<T, CommonsError>, field: &str, part: &str) {
    let (artifact, f, reason) = merr(res);
    assert_eq!(f, field, "artifact {artifact}: {reason}");
    assert!(
        reason.contains(part),
        "expected {part:?} in reason for {field}: {reason}"
    );
}

#[cfg(unix)]
fn symlink(target: &Path, link: &Path) {
    std::os::unix::fs::symlink(target, link).unwrap();
}

/// Discover everything under `artifacts/`.
fn discover_root(r: &Repo) -> Result<Vec<PathBuf>, CommonsError> {
    discover(std::slice::from_ref(&r.art), &r.config)
}

fn rel(r: &Repo, paths: &[PathBuf]) -> Vec<String> {
    paths
        .iter()
        .map(|p| {
            p.strip_prefix(&r.art)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// load: the happy path
// ---------------------------------------------------------------------------

#[test]
fn loads_a_decoder() {
    let r = repo();
    let dir = artifact(&r, "verified", "decoder", "spl-token", "1.2.3");
    let a = load(&dir, &r.config).unwrap();
    assert_eq!(a.dir, dir);
    assert_eq!(a.status, Status::Verified);
    assert_eq!(a.manifest.kind, Kind::Decoder);
    assert_eq!(a.manifest.name, "spl-token");
    assert_eq!(
        a.manifest.version,
        Version {
            major: 1,
            minor: 2,
            patch: 3
        }
    );
    assert_eq!(a.manifest.content_sha256, content_sha256(IDL));
    assert_eq!(
        a.manifest
            .program_id
            .map(|p| bs58::encode(p).into_string())
            .as_deref(),
        Some(PID)
    );
    assert_eq!(a.content, IDL, "content is kept as raw bytes");
    assert_eq!(a.artifact_id, a.manifest.artifact_id());
    assert_eq!(a.artifact_id.0, sha256(&a.manifest.canonical_bytes()));
    assert_eq!(a.label(), "verified/decoder/spl-token/1.2.3");
}

#[test]
fn loads_a_model() {
    let r = repo();
    let dir = artifact(&r, "official", "model", "daily-volume", "0.1.0");
    let a = load(&dir, &r.config).unwrap();
    assert_eq!(a.status, Status::Official);
    assert_eq!(a.manifest.kind, Kind::Model);
    assert_eq!(a.manifest.program_id, None);
    assert_eq!(a.content, SQL);
}

#[test]
fn content_file_names_are_fixed_by_kind() {
    assert_eq!(content_file_name(Kind::Decoder), "idl.json");
    assert_eq!(content_file_name(Kind::Model), "model.sql");
}

// ---------------------------------------------------------------------------
// Identity stability
// ---------------------------------------------------------------------------

#[test]
fn identity_excludes_status_so_promotion_keeps_the_id() {
    let r = repo();
    let exp = artifact(&r, "experimental", "decoder", "spl-token", "1.0.0");
    let before = load(&exp, &r.config).unwrap();

    // Promote by moving the directory, as a merged pull request would.
    let ver = version_dir(&r, "verified", "decoder", "spl-token", "1.0.0");
    fs::create_dir_all(ver.parent().unwrap()).unwrap();
    fs::rename(&exp, &ver).unwrap();
    let after = load(&ver, &r.config).unwrap();
    assert_eq!(after.status, Status::Verified);
    assert_eq!(after.artifact_id, before.artifact_id);

    let off = version_dir(&r, "official", "decoder", "spl-token", "1.0.0");
    fs::create_dir_all(off.parent().unwrap()).unwrap();
    fs::rename(&ver, &off).unwrap();
    assert_eq!(
        load(&off, &r.config).unwrap().artifact_id,
        before.artifact_id
    );
}

#[test]
fn identity_ignores_manifest_formatting_and_repo_location() {
    let a = repo();
    let b = repo();
    let da = artifact(&a, "experimental", "decoder", "spl-token", "1.0.0");
    // Same fields, compact, keys reversed, CRLF line endings.
    let compact = format!(
        "{{\"program_id\":\"{PID}\",\"content_sha256\":\"{}\",\"description\":\"Test artifact.\",\r\n\
         \"license\":\"Apache-2.0\",\"version\":\"1.0.0\",\"name\":\"spl-token\",\r\n\
         \"kind\":\"decoder\",\"schema_version\":1}}",
        to_hex(&content_sha256(IDL))
    );
    let db = artifact_with(
        &b,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &compact,
        IDL,
    );
    let ia = load(&da, &a.config).unwrap().artifact_id;
    let ib = load(&db, &b.config).unwrap().artifact_id;
    assert_eq!(ia, ib);
    // Loading twice gives the same id.
    assert_eq!(load(&da, &a.config).unwrap().artifact_id, ia);
}

#[test]
fn identity_pinned_for_a_known_artifact() {
    // A full artifact whose id is fixed forever. If this test fails, every
    // published id has changed.
    let r = repo();
    let dir = artifact(&r, "experimental", "model", "daily-volume", "1.0.0");
    let a = load(&dir, &r.config).unwrap();
    let canonical = String::from_utf8(a.manifest.canonical_bytes()).unwrap();
    assert_eq!(
        canonical,
        format!(
            "{{\"schema_version\":1,\"kind\":\"model\",\"name\":\"daily-volume\",\
             \"version\":\"1.0.0\",\"license\":\"Apache-2.0\",\"description\":\"Test artifact.\",\
             \"content_sha256\":\"{}\"}}",
            to_hex(&content_sha256(SQL))
        )
    );
    assert_eq!(
        to_hex(&content_sha256(SQL)),
        "5d0635e4c049faf140441608b682f1bf371e7aebeb50d3ad0a7e9fb69e03cce4"
    );
    assert_eq!(
        a.artifact_id.to_string(),
        "03acf1aaabaeba6214d0a8aa9df0d057c36d104f3bdb190125ebecc912ddb112"
    );
}

#[test]
fn identity_differs_between_artifacts() {
    let r = repo();
    let ids: Vec<ArtifactId> = [
        artifact(&r, "experimental", "decoder", "a", "1.0.0"),
        artifact(&r, "experimental", "decoder", "a", "1.0.1"),
        artifact(&r, "experimental", "decoder", "b", "1.0.0"),
        artifact(&r, "experimental", "model", "a", "1.0.0"),
    ]
    .iter()
    .map(|d| load(d, &r.config).unwrap().artifact_id)
    .collect();
    for i in 0..ids.len() {
        for j in i + 1..ids.len() {
            assert_ne!(ids[i], ids[j], "{i} vs {j}");
        }
    }
}

// ---------------------------------------------------------------------------
// load step 1: identity from the path
// ---------------------------------------------------------------------------

#[test]
fn path_must_be_status_kind_name_version() {
    let r = repo();
    let ok = artifact(&r, "experimental", "decoder", "spl-token", "1.0.0");

    for (p, part) in [
        (r.art.clone(), "0 level(s)"),
        (r.art.join("experimental"), "1 level(s)"),
        (ok.parent().unwrap().to_path_buf(), "3 level(s)"),
        (ok.join("deeper"), "5 level(s)"),
        (r.art.join("draft/decoder/spl-token/1.0.0"), "not a status"),
        (
            r.art.join("Experimental/decoder/spl-token/1.0.0"),
            "not a status",
        ),
        (
            r.art.join("experimental/decoders/spl-token/1.0.0"),
            "not a kind",
        ),
        (
            r.art.join("experimental/decoder/Spl-Token/1.0.0"),
            "not a valid name",
        ),
        (
            r.art.join("experimental/decoder/spl_token/1.0.0"),
            "not a valid name",
        ),
        (
            r.art.join("experimental/decoder/spl-token/1.0"),
            "not a version",
        ),
        (
            r.art.join("experimental/decoder/spl-token/01.0.0"),
            "leading zeros",
        ),
        (
            r.art.join("experimental/decoder/spl-token/v1.0.0"),
            "not a version",
        ),
        (
            r.art.join("experimental/decoder/spl-token/1.0.0-rc1"),
            "not a version",
        ),
        (r.art.join("experimental/decoder/.hidden/1.0.0"), "hidden"),
        (
            r.art.join("experimental/decoder/x/../spl-token/1.0.0"),
            "'.' or '..'",
        ),
        (
            r.config.repo_root.join("elsewhere/decoder/spl-token/1.0.0"),
            "not inside",
        ),
    ] {
        assert_merr(load(&p, &r.config), FIELD_PATH, part);
    }
}

// ---------------------------------------------------------------------------
// load step 2: the directory
// ---------------------------------------------------------------------------

#[test]
fn directory_must_exist_and_be_real() {
    let r = repo();
    assert_merr(
        load(
            &version_dir(&r, "experimental", "decoder", "nope", "1.0.0"),
            &r.config,
        ),
        FIELD_DIRECTORY,
        "does not exist",
    );
    let file = version_dir(&r, "experimental", "decoder", "file", "1.0.0");
    fs::create_dir_all(file.parent().unwrap()).unwrap();
    fs::write(&file, b"x").unwrap();
    assert_merr(load(&file, &r.config), FIELD_DIRECTORY, "not a directory");
}

#[cfg(unix)]
#[test]
fn symlinked_version_or_ancestor_directories_are_rejected() {
    let r = repo();
    let outside = tempfile::tempdir().unwrap();

    // A perfectly valid artifact outside the repo, linked in as a version dir.
    let target = write_files(
        &outside.path().join("v"),
        &[
            (
                MANIFEST_FILE,
                manifest_json("decoder", "spl-token", "1.0.0", IDL).as_bytes(),
            ),
            ("idl.json", IDL),
        ],
    );
    let link = version_dir(&r, "experimental", "decoder", "spl-token", "1.0.0");
    fs::create_dir_all(link.parent().unwrap()).unwrap();
    symlink(&target, &link);
    assert_merr(load(&link, &r.config), FIELD_DIRECTORY, "is a symlink");

    // A symlinked name directory: the version dir under it is real.
    let real_name = outside.path().join("name");
    write_files(
        &real_name.join("1.0.0"),
        &[
            (
                MANIFEST_FILE,
                manifest_json("decoder", "linked", "1.0.0", IDL).as_bytes(),
            ),
            ("idl.json", IDL),
        ],
    );
    symlink(&real_name, &r.art.join("experimental/decoder/linked"));
    assert_merr(
        load(
            &version_dir(&r, "experimental", "decoder", "linked", "1.0.0"),
            &r.config,
        ),
        FIELD_DIRECTORY,
        "is a symlink",
    );

    // A symlinked status directory.
    let real_status = outside.path().join("status");
    write_files(
        &real_status.join("decoder/x/1.0.0"),
        &[
            (
                MANIFEST_FILE,
                manifest_json("decoder", "x", "1.0.0", IDL).as_bytes(),
            ),
            ("idl.json", IDL),
        ],
    );
    symlink(&real_status, &r.art.join("verified"));
    assert_merr(
        load(
            &version_dir(&r, "verified", "decoder", "x", "1.0.0"),
            &r.config,
        ),
        FIELD_DIRECTORY,
        "is a symlink",
    );
}

// ---------------------------------------------------------------------------
// load step 3: the entries
// ---------------------------------------------------------------------------

#[test]
fn exactly_two_files() {
    let r = repo();
    let m = manifest_json("decoder", "spl-token", "1.0.0", IDL);
    let base = |r: &Repo| version_dir(r, "experimental", "decoder", "spl-token", "1.0.0");
    type Files<'a> = Vec<(&'a str, &'a [u8])>;
    let cases: Vec<(Files, &str)> = vec![
        (
            vec![
                (MANIFEST_FILE, m.as_bytes()),
                ("idl.json", IDL),
                ("README.md", b"x\n"),
            ],
            "unexpected entry 'README.md'",
        ),
        (
            vec![
                (MANIFEST_FILE, m.as_bytes()),
                ("idl.json", IDL),
                (".DS_Store", b"x"),
            ],
            "unexpected entry '.DS_Store'",
        ),
        (
            vec![
                (MANIFEST_FILE, m.as_bytes()),
                ("idl.json", IDL),
                ("idl.json.bak", IDL),
            ],
            "unexpected entry 'idl.json.bak'",
        ),
        (
            vec![(MANIFEST_FILE, m.as_bytes()), ("model.sql", SQL)],
            "unexpected entry 'model.sql'",
        ),
        (
            vec![(MANIFEST_FILE, m.as_bytes()), ("IDL.json", IDL)],
            "unexpected entry 'IDL.json'",
        ),
        (
            vec![("Manifest.json", m.as_bytes()), ("idl.json", IDL)],
            "unexpected entry 'Manifest.json'",
        ),
        (vec![("idl.json", IDL)], "missing manifest.json"),
        (vec![(MANIFEST_FILE, m.as_bytes())], "missing idl.json"),
        (vec![], "missing manifest.json"),
    ];
    for (files, part) in cases {
        let r2 = repo();
        let dir = write_files(&base(&r2), &files);
        assert_merr(load(&dir, &r2.config), FIELD_DIRECTORY, part);
    }
    // A subdirectory is an unexpected entry too.
    let dir = decoder_with_content(&r, IDL);
    fs::create_dir(dir.join("sub")).unwrap();
    assert_merr(
        load(&dir, &r.config),
        FIELD_DIRECTORY,
        "unexpected entry 'sub'",
    );
}

#[test]
fn expected_names_must_be_regular_files() {
    let r = repo();
    let dir = write_files(
        &version_dir(&r, "experimental", "decoder", "spl-token", "1.0.0"),
        &[("idl.json", IDL)],
    );
    fs::create_dir(dir.join(MANIFEST_FILE)).unwrap();
    assert_merr(
        load(&dir, &r.config),
        FIELD_DIRECTORY,
        "'manifest.json' is not a regular file",
    );
}

#[cfg(unix)]
#[test]
fn symlinked_files_are_rejected_even_when_their_target_is_valid() {
    let outside = tempfile::tempdir().unwrap();
    let real_idl = outside.path().join("idl.json");
    fs::write(&real_idl, IDL).unwrap();
    let real_manifest = outside.path().join("manifest.json");
    fs::write(
        &real_manifest,
        manifest_json("decoder", "spl-token", "1.0.0", IDL),
    )
    .unwrap();

    let r = repo();
    let dir = version_dir(&r, "experimental", "decoder", "spl-token", "1.0.0");
    write_files(
        &dir,
        &[(
            MANIFEST_FILE,
            manifest_json("decoder", "spl-token", "1.0.0", IDL).as_bytes(),
        )],
    );
    symlink(&real_idl, &dir.join("idl.json"));
    assert_merr(
        load(&dir, &r.config),
        FIELD_DIRECTORY,
        "'idl.json' is a symlink",
    );

    let r = repo();
    let dir = version_dir(&r, "experimental", "decoder", "spl-token", "1.0.0");
    write_files(&dir, &[("idl.json", IDL)]);
    symlink(&real_manifest, &dir.join(MANIFEST_FILE));
    assert_merr(
        load(&dir, &r.config),
        FIELD_DIRECTORY,
        "'manifest.json' is a symlink",
    );

    // A symlink to /etc/passwd-like files in the repo is no better.
    let r = repo();
    let dir = decoder_with_content(&r, IDL);
    fs::remove_file(dir.join("idl.json")).unwrap();
    symlink(Path::new("/dev/zero"), &dir.join("idl.json"));
    assert_merr(load(&dir, &r.config), FIELD_DIRECTORY, "is a symlink");
}

#[test]
fn entry_errors_do_not_depend_on_creation_order() {
    let mut reasons = Vec::new();
    for order in [["zzz.txt", "aaa.txt"], ["aaa.txt", "zzz.txt"]] {
        let r = repo();
        let dir = decoder_with_content(&r, IDL);
        for name in order {
            fs::write(dir.join(name), b"x").unwrap();
        }
        reasons.push(merr(load(&dir, &r.config)).2);
    }
    assert_eq!(reasons[0], reasons[1]);
    assert!(reasons[0].contains("'aaa.txt'"), "{}", reasons[0]);
}

// ---------------------------------------------------------------------------
// load step 4: the manifest
// ---------------------------------------------------------------------------

#[test]
fn manifest_errors_carry_their_field() {
    let r = repo();
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        "{\"kind\":",
        IDL,
    );
    assert_merr(
        load(&dir, &r.config),
        FIELD_MANIFEST,
        "unexpected end of file at byte 8",
    );

    let r = repo();
    let with_status = manifest_json("decoder", "spl-token", "1.0.0", IDL).replacen(
        '{',
        "{\"status\": \"official\",",
        1,
    );
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &with_status,
        IDL,
    );
    assert_merr(
        load(&dir, &r.config),
        FIELD_MANIFEST,
        "status is the directory",
    );

    let r = repo();
    let with_author = manifest_json("decoder", "spl-token", "1.0.0", IDL).replacen(
        '{',
        "{\"author\": \"alice\",",
        1,
    );
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &with_author,
        IDL,
    );
    assert_merr(load(&dir, &r.config), FIELD_MANIFEST, "git author");
}

#[test]
fn manifest_size_limit_is_exact() {
    let base = manifest_json("decoder", "spl-token", "1.0.0", IDL);
    let at_max = format!(
        "{base}{}",
        " ".repeat(MAX_MANIFEST_BYTES as usize - base.len())
    );
    assert_eq!(at_max.len() as u64, MAX_MANIFEST_BYTES);

    let r = repo();
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &at_max,
        IDL,
    );
    load(&dir, &r.config).unwrap();

    let r = repo();
    let over = format!("{at_max} ");
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &over,
        IDL,
    );
    assert_merr(
        load(&dir, &r.config),
        FIELD_MANIFEST,
        "16385 bytes, more than the limit of 16384",
    );
}

// ---------------------------------------------------------------------------
// load step 5: path versus manifest
// ---------------------------------------------------------------------------

#[test]
fn manifest_must_match_its_path_exactly() {
    for (kind, name, version, field, part) in [
        (
            "model",
            "spl-token",
            "1.0.0",
            "manifest.kind",
            "is 'model' but the path says 'decoder'",
        ),
        (
            "decoder",
            "spl-token2",
            "1.0.0",
            "manifest.name",
            "is 'spl-token2' but the path says 'spl-token'",
        ),
        (
            "decoder",
            "spl",
            "1.0.0",
            "manifest.name",
            "but the path says 'spl-token'",
        ),
        (
            "decoder",
            "spl-token",
            "1.0.1",
            "manifest.version",
            "is '1.0.1' but the path says '1.0.0'",
        ),
        (
            "decoder",
            "spl-token",
            "10.0.0",
            "manifest.version",
            "but the path says '1.0.0'",
        ),
    ] {
        let r = repo();
        let m = manifest_json(kind, name, version, IDL);
        let dir = artifact_with(&r, "experimental", "decoder", "spl-token", "1.0.0", &m, IDL);
        assert_merr(load(&dir, &r.config), field, part);
    }
}

// ---------------------------------------------------------------------------
// load step 6: the content
// ---------------------------------------------------------------------------

#[test]
fn content_must_be_canonical_text() {
    for (content, part) in [
        (&b""[..], "is empty"),
        (b"\xEF\xBB\xBF{}\n", "byte-order mark"),
        (b"{\"a\":\"\xff\"}\n", "not valid UTF-8"),
        (b"{}\r\n", "carriage return"),
        (b"{\n}\r", "carriage return (\\r) on line 2"),
        (b"{}", "does not end with a newline"),
        (b"{}\n\n", "more than one newline"),
    ] {
        let r = repo();
        // The manifest hash matches, so the content rule is what fails.
        let dir = decoder_with_content(&r, content);
        assert_merr(load(&dir, &r.config), FIELD_CONTENT, part);
    }
}

#[test]
fn content_size_limit_follows_config_exactly() {
    let mut r = repo();
    r.config.max_artifact_bytes = 64;

    let at_max = [&b"x".repeat(63)[..], b"\n"].concat();
    let dir = decoder_with_content(&r, &at_max);
    assert_eq!(load(&dir, &r.config).unwrap().content.len(), 64);

    let mut r = repo();
    r.config.max_artifact_bytes = 64;
    let over = [&b"x".repeat(64)[..], b"\n"].concat();
    let dir = decoder_with_content(&r, &over);
    assert_merr(
        load(&dir, &r.config),
        FIELD_CONTENT,
        "65 bytes, more than the limit of 64",
    );
}

#[test]
fn huge_content_is_rejected_from_metadata_without_reading_it() {
    let r = repo();
    let dir = decoder_with_content(&r, IDL);
    // Sparse: 64 GiB on paper, nothing on disk.
    let f = fs::OpenOptions::new()
        .write(true)
        .open(dir.join("idl.json"))
        .unwrap();
    f.set_len(64 << 30).unwrap();
    drop(f);
    let started = std::time::Instant::now();
    assert_merr(
        load(&dir, &r.config),
        FIELD_CONTENT,
        &format!("{} bytes", 64u64 << 30),
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// load steps 7 and 8: the hash and kind rules
// ---------------------------------------------------------------------------

#[test]
fn content_hash_must_match_raw_bytes() {
    let r = repo();
    let m = manifest_json("decoder", "spl-token", "1.0.0", b"{}\n");
    let dir = artifact_with(&r, "experimental", "decoder", "spl-token", "1.0.0", &m, IDL);
    let (_, field, reason) = merr(load(&dir, &r.config));
    assert_eq!(field, "manifest.content_sha256");
    assert!(
        reason.contains(&to_hex(&content_sha256(b"{}\n"))),
        "{reason}"
    );
    assert!(reason.contains(&to_hex(&content_sha256(IDL))), "{reason}");

    // Semantically identical JSON with different bytes is a different hash:
    // content is never re-serialised.
    let r = repo();
    let reformatted =
        b"{ \"version\": \"0.1.0\", \"name\": \"spl_token\", \"instructions\": [] }\n";
    let m = manifest_json("decoder", "spl-token", "1.0.0", IDL);
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &m,
        reformatted,
    );
    assert_merr(
        load(&dir, &r.config),
        "manifest.content_sha256",
        "hashes to",
    );
}

#[test]
fn kind_rules_for_program_id() {
    let r = repo();
    let m = manifest_json("decoder", "spl-token", "1.0.0", IDL)
        .replace(&format!(",\n  \"program_id\": \"{PID}\""), "");
    let dir = artifact_with(&r, "experimental", "decoder", "spl-token", "1.0.0", &m, IDL);
    assert_merr(
        load(&dir, &r.config),
        "manifest.program_id",
        "required for a decoder",
    );

    let r = repo();
    let m = manifest_json("model", "vol", "1.0.0", SQL).replacen(
        "\n}",
        &format!(",\n  \"program_id\": \"{PID}\"\n}}"),
        1,
    );
    let dir = artifact_with(&r, "experimental", "model", "vol", "1.0.0", &m, SQL);
    assert_merr(
        load(&dir, &r.config),
        "manifest.program_id",
        "not allowed for a model",
    );
}

// ---------------------------------------------------------------------------
// load: first error wins, in step order
// ---------------------------------------------------------------------------

#[test]
fn first_error_wins_in_step_order() {
    let bad_content: &[u8] = b"a\r\n";
    let bad_manifest = "not json";

    // 1 beats everything: bad version in path, bad everything else.
    let r = repo();
    let dir = write_files(
        &r.art.join("experimental/decoder/spl-token/1.0"),
        &[(MANIFEST_FILE, bad_manifest.as_bytes()), ("junk", b"")],
    );
    assert_merr(load(&dir, &r.config), FIELD_PATH, "not a version");

    // 3 beats 4: an unexpected entry next to a broken manifest.
    let r = repo();
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        bad_manifest,
        bad_content,
    );
    fs::write(dir.join("extra"), b"").unwrap();
    assert_merr(load(&dir, &r.config), FIELD_DIRECTORY, "unexpected entry");

    // 4 beats 6: broken manifest and broken content.
    let r = repo();
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        bad_manifest,
        bad_content,
    );
    assert_merr(load(&dir, &r.config), FIELD_MANIFEST, "");

    // 5 beats 6: name mismatch and broken content.
    let r = repo();
    let m = manifest_json("decoder", "other", "1.0.0", bad_content);
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &m,
        bad_content,
    );
    assert_merr(load(&dir, &r.config), "manifest.name", "");

    // 6 beats 7: CRLF content whose LF twin is what the manifest hashed.
    let r = repo();
    let m = manifest_json("decoder", "spl-token", "1.0.0", b"a\n");
    let dir = artifact_with(
        &r,
        "experimental",
        "decoder",
        "spl-token",
        "1.0.0",
        &m,
        bad_content,
    );
    assert_merr(load(&dir, &r.config), FIELD_CONTENT, "carriage return");

    // 7 beats 8: wrong hash and a model with a program_id.
    let r = repo();
    let m = manifest_json("model", "vol", "1.0.0", b"other\n").replacen(
        "\n}",
        &format!(",\n  \"program_id\": \"{PID}\"\n}}"),
        1,
    );
    let dir = artifact_with(&r, "experimental", "model", "vol", "1.0.0", &m, SQL);
    assert_merr(load(&dir, &r.config), "manifest.content_sha256", "");
}

// ---------------------------------------------------------------------------
// load: error reporting
// ---------------------------------------------------------------------------

#[test]
fn errors_name_the_artifact_and_echo_hostile_paths() {
    let r = repo();
    let dir = decoder_with_content(&r, b"x");
    let (artifact, _, _) = merr(load(&dir, &r.config));
    assert_eq!(artifact, "experimental/decoder/spl-token/1.0.0");

    let evil = r
        .art
        .join("experimental/decoder/evil\u{1b}[2J\u{202E}/1.0.0");
    fs::create_dir_all(&evil).unwrap();
    let e = load(&evil, &r.config).unwrap_err();
    assert_eq!(e.exit_code(), EXIT_ARTIFACT);
    let msg = e.to_string();
    assert!(
        !msg.contains('\u{1b}') && !msg.contains('\u{202E}'),
        "{msg:?}"
    );
    assert!(msg.contains("\\u{001B}"), "{msg}");
}

#[cfg(unix)]
#[test]
fn unreadable_directory_is_an_io_error() {
    use std::os::unix::fs::PermissionsExt;
    let r = repo();
    let dir = decoder_with_content(&r, IDL);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o000)).unwrap();
    let readable_anyway = fs::read_dir(&dir).is_ok(); // running as root
    let res = load(&dir, &r.config);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
    if readable_anyway {
        eprintln!("skipping: permissions are not enforced for this user");
        return;
    }
    let e = res.unwrap_err();
    assert!(matches!(e, CommonsError::IoError { .. }), "{e:?}");
    assert_eq!(e.exit_code(), EXIT_INTERNAL);
}

#[cfg(unix)]
#[test]
fn unreadable_content_is_an_io_error() {
    use std::os::unix::fs::PermissionsExt;
    let r = repo();
    let dir = decoder_with_content(&r, IDL);
    let f = dir.join("idl.json");
    fs::set_permissions(&f, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&f).is_ok() {
        eprintln!("skipping: permissions are not enforced for this user");
        return;
    }
    let e = load(&dir, &r.config).unwrap_err();
    assert!(matches!(e, CommonsError::IoError { .. }), "{e:?}");
    assert_eq!(e.exit_code(), EXIT_INTERNAL);
}

// ---------------------------------------------------------------------------
// discover
// ---------------------------------------------------------------------------

fn sample_tree(r: &Repo) {
    artifact(r, "experimental", "decoder", "spl-token", "1.0.0");
    artifact(r, "experimental", "decoder", "spl-token", "1.1.0");
    artifact(r, "experimental", "model", "daily-volume", "0.1.0");
    artifact(r, "verified", "decoder", "token-2022", "2.0.0");
    artifact(r, "official", "model", "fees", "1.0.0");
}

#[test]
fn discovers_from_every_level() {
    let r = repo();
    sample_tree(&r);
    fs::write(r.art.join(ARTIFACTS_README), b"# Artifacts\n").unwrap();

    let d = |p: &str| {
        let path = if p.is_empty() {
            r.art.clone()
        } else {
            r.art.join(p)
        };
        rel(&r, &discover(&[path], &r.config).unwrap())
    };
    assert_eq!(
        d(""),
        [
            "experimental/decoder/spl-token/1.0.0",
            "experimental/decoder/spl-token/1.1.0",
            "experimental/model/daily-volume/0.1.0",
            "official/model/fees/1.0.0",
            "verified/decoder/token-2022/2.0.0",
        ]
    );
    assert_eq!(
        d("experimental"),
        [
            "experimental/decoder/spl-token/1.0.0",
            "experimental/decoder/spl-token/1.1.0",
            "experimental/model/daily-volume/0.1.0",
        ]
    );
    assert_eq!(
        d("experimental/decoder"),
        [
            "experimental/decoder/spl-token/1.0.0",
            "experimental/decoder/spl-token/1.1.0"
        ]
    );
    assert_eq!(
        d("experimental/decoder/spl-token"),
        [
            "experimental/decoder/spl-token/1.0.0",
            "experimental/decoder/spl-token/1.1.0"
        ]
    );
    assert_eq!(
        d("experimental/decoder/spl-token/1.1.0"),
        ["experimental/decoder/spl-token/1.1.0"]
    );
}

#[test]
fn discovery_sorts_by_path_components_and_dedups() {
    // "foo" sorts before "foo-bar" by component; as strings, "foo-bar/" < "foo/".
    let names = ["foo-bar", "foo", "a"];
    let versions = ["1.9.0", "1.10.0"];
    let mut results = Vec::new();
    for rev in [false, true] {
        let r = repo();
        let mut order: Vec<(&str, &str)> = names
            .iter()
            .flat_map(|n| versions.iter().map(move |v| (*n, *v)))
            .collect();
        if rev {
            order.reverse();
        }
        for (n, v) in order {
            artifact(&r, "experimental", "decoder", n, v);
        }
        let paths = [r.art.clone(), r.art.join("experimental"), r.art.clone()];
        results.push(rel(&r, &discover(&paths, &r.config).unwrap()));
    }
    assert_eq!(results[0], results[1], "independent of creation order");
    assert_eq!(
        results[0],
        [
            "experimental/decoder/a/1.10.0",
            "experimental/decoder/a/1.9.0",
            "experimental/decoder/foo/1.10.0",
            "experimental/decoder/foo/1.9.0",
            "experimental/decoder/foo-bar/1.10.0",
            "experimental/decoder/foo-bar/1.9.0",
        ]
    );
}

#[test]
fn empty_trees_discover_nothing() {
    let r = repo();
    assert!(discover_root(&r).unwrap().is_empty());
    fs::create_dir_all(r.art.join("experimental/decoder/x")).unwrap();
    assert!(discover_root(&r).unwrap().is_empty());
}

#[test]
fn version_directories_are_not_entered() {
    let r = repo();
    let dir = artifact(&r, "experimental", "decoder", "spl-token", "1.0.0");
    fs::create_dir(dir.join(".git")).unwrap();
    fs::write(dir.join("stray"), b"").unwrap();
    // discover lists it; load is what rejects the contents.
    assert_eq!(discover_root(&r).unwrap(), std::slice::from_ref(&dir));
    assert_merr(load(&dir, &r.config), FIELD_DIRECTORY, "unexpected entry");
}

#[test]
fn readme_is_allowed_only_directly_in_artifacts() {
    let r = repo();
    sample_tree(&r);
    fs::write(r.art.join(ARTIFACTS_README), b"# hi\n").unwrap();
    assert_eq!(discover_root(&r).unwrap().len(), 5);

    for p in [
        "experimental/README.md",
        "experimental/decoder/README.md",
        "experimental/decoder/spl-token/README.md",
    ] {
        let r = repo();
        sample_tree(&r);
        fs::write(r.art.join(p), b"# hi\n").unwrap();
        let (artifact, field, reason) = merr(discover_root(&r));
        assert_eq!(
            (artifact.as_str(), field.as_str()),
            (p, FIELD_DIRECTORY),
            "{reason}"
        );
        assert!(reason.contains("only"), "{reason}");
    }

    let r = repo();
    fs::create_dir(r.art.join(ARTIFACTS_README)).unwrap();
    assert_merr(discover_root(&r), FIELD_DIRECTORY, "must be a regular file");

    let r = repo();
    fs::write(r.art.join("readme.md"), b"").unwrap();
    assert_merr(
        discover_root(&r),
        FIELD_DIRECTORY,
        "only status directories",
    );
}

#[test]
fn files_and_unknown_names_at_tree_levels_are_rejected() {
    for (p, is_dir, part) in [
        ("notes.txt", false, "only status directories"),
        ("draft", true, "not a status"),
        ("Verified", true, "not a status"),
        ("experimental/decoder.txt", false, "only kind directories"),
        ("experimental/decoders", true, "not a kind"),
        ("experimental/decoder/file", false, "only name directories"),
        (
            "experimental/decoder/spl-token/1.0.0.tar",
            false,
            "only version directories",
        ),
    ] {
        let r = repo();
        sample_tree(&r);
        let full = r.art.join(p);
        if is_dir {
            fs::create_dir_all(&full).unwrap();
        } else {
            fs::write(&full, b"").unwrap();
        }
        let (artifact, field, reason) = merr(discover_root(&r));
        assert_eq!(field, FIELD_DIRECTORY, "{p}: {reason}");
        assert_eq!(artifact, p);
        assert!(reason.contains(part), "{p}: expected {part:?} in {reason}");
    }
}

#[test]
fn hidden_entries_are_rejected_at_every_level() {
    for p in [
        ".gitkeep",
        ".git",
        "experimental/.gitkeep",
        "experimental/decoder/.gitkeep",
        "experimental/decoder/spl-token/.hidden",
    ] {
        let r = repo();
        sample_tree(&r);
        fs::write(r.art.join(p), b"").unwrap_or_else(|_| fs::create_dir(r.art.join(p)).unwrap());
        let (artifact, field, reason) = merr(discover_root(&r));
        assert_eq!((artifact.as_str(), field.as_str()), (p, FIELD_DIRECTORY));
        assert!(reason.contains("hidden"), "{p}: {reason}");
    }
}

#[cfg(unix)]
#[test]
fn symlinks_are_rejected_at_every_level() {
    let outside = tempfile::tempdir().unwrap();
    let target = outside.path();
    for p in [
        "official",
        "README.md",
        "verified/model",
        "experimental/decoder/linked",
        "experimental/decoder/spl-token/9.9.9",
    ] {
        let r = repo();
        sample_tree(&r);
        // Replace a real directory with a link, or add a new one.
        let _ = fs::remove_dir_all(r.art.join(p));
        symlink(target, &r.art.join(p));
        let (artifact, field, reason) = merr(discover_root(&r));
        assert_eq!((artifact.as_str(), field.as_str()), (p, FIELD_DIRECTORY));
        assert!(reason.contains("symlink"), "{p}: {reason}");
    }

    // A symlink loop is reported, not followed.
    let r = repo();
    sample_tree(&r);
    symlink(Path::new(".."), &r.art.join("experimental/decoder/loop"));
    assert_merr(discover_root(&r), FIELD_DIRECTORY, "symlink");

    // A dangling symlink is reported too.
    let r = repo();
    symlink(Path::new("/nonexistent/target"), &r.art.join("verified"));
    assert_merr(discover_root(&r), FIELD_DIRECTORY, "symlink");

    // And a symlink passed as the path argument.
    let r = repo();
    sample_tree(&r);
    symlink(
        &r.art.join("experimental/model"),
        &r.art.join("verified/model"),
    );
    assert_merr(
        discover(&[r.art.join("verified/model")], &r.config),
        FIELD_DIRECTORY,
        "is a symlink",
    );
}

#[test]
fn path_arguments_must_be_at_a_valid_depth() {
    let r = repo();
    sample_tree(&r);
    let v = r.art.join("experimental/decoder/spl-token/1.0.0");
    fs::create_dir(v.join("deeper")).unwrap();
    for (p, part) in [
        (v.join("deeper"), "deeper than a version directory"),
        (r.art.join("draft"), "not a status"),
        (r.art.join("experimental/decoders"), "not a kind"),
        (r.art.join(".hidden"), "hidden"),
        (r.config.repo_root.clone(), "not inside"),
        (r.art.join("experimental/../verified"), "'.' or '..'"),
    ] {
        assert_merr(discover(&[p], &r.config), FIELD_PATH, part);
    }
    // A path that does not exist (deleted after config validated it).
    assert_merr(
        discover(&[r.art.join("verified/model")], &r.config),
        FIELD_DIRECTORY,
        "does not exist",
    );
}

#[test]
fn entries_per_directory_limit_is_exact() {
    let r = repo();
    let kind = r.art.join("experimental/decoder");
    for i in 0..MAX_ENTRIES_PER_DIR {
        fs::create_dir_all(kind.join(format!("n{i}"))).unwrap();
    }
    assert!(discover_root(&r).unwrap().is_empty());
    fs::create_dir(kind.join("one-more")).unwrap();
    let (artifact, field, reason) = merr(discover_root(&r));
    assert_eq!(artifact, "experimental/decoder");
    assert_eq!(field, FIELD_DIRECTORY);
    assert_eq!(
        reason,
        format!("has more than {MAX_ENTRIES_PER_DIR} entries")
    );
}

#[test]
fn version_directory_limit_is_exact() {
    let r = repo();
    let kind = r.art.join("experimental/decoder");
    let per_name = MAX_ENTRIES_PER_DIR;
    let mut made = 0;
    'outer: for n in 0.. {
        for v in 0..per_name {
            if made == MAX_VERSION_DIRS {
                break 'outer;
            }
            fs::create_dir_all(kind.join(format!("n{n}")).join(format!("0.0.{v}"))).unwrap();
            made += 1;
        }
    }
    assert_eq!(discover_root(&r).unwrap().len(), MAX_VERSION_DIRS);
    fs::create_dir_all(kind.join("zz/0.0.0")).unwrap();
    assert_merr(
        discover_root(&r),
        FIELD_DIRECTORY,
        &format!("more than {MAX_VERSION_DIRS} version directories"),
    );
}

// ---------------------------------------------------------------------------
// check_unique
// ---------------------------------------------------------------------------

fn load_all(r: &Repo) -> Vec<Artifact> {
    discover_root(r)
        .unwrap()
        .iter()
        .map(|d| load(d, &r.config).unwrap())
        .collect()
}

#[test]
fn unique_triples_pass() {
    let r = repo();
    sample_tree(&r);
    // Same name and version under different kinds is fine.
    artifact(&r, "experimental", "model", "spl-token", "1.0.0");
    // Same name, different version, different status is fine.
    artifact(&r, "official", "decoder", "spl-token", "0.9.0");
    let all = load_all(&r);
    assert_eq!(all.len(), 7);
    check_unique(&all).unwrap();
    check_unique(&[]).unwrap();
}

#[test]
fn a_copied_promotion_is_a_duplicate_naming_both_locations() {
    let r = repo();
    sample_tree(&r);
    artifact(&r, "official", "decoder", "spl-token", "1.1.0"); // copy of experimental
    let mut all = load_all(&r);

    let e = check_unique(&all).unwrap_err();
    assert_eq!(e.exit_code(), EXIT_ARTIFACT);
    let (artifact, field, reason) = merr::<()>(Err(e));
    assert_eq!(field, FIELD_PATH);
    assert_eq!(artifact, "official/decoder/spl-token/1.1.0");
    assert!(
        reason.contains("'experimental/decoder/spl-token/1.1.0'"),
        "{reason}"
    );
    assert!(
        reason.contains("'official/decoder/spl-token/1.1.0'"),
        "{reason}"
    );
    assert!(
        reason.contains("move the directory, not copy it"),
        "{reason}"
    );

    // The report is the same whatever the input order.
    all.reverse();
    let (_, _, reason2) = merr(check_unique(&all));
    assert_eq!(reason, reason2);
}

#[test]
fn duplicates_report_the_smallest_triple_first() {
    let r = repo();
    artifact(&r, "experimental", "model", "z", "1.0.0");
    artifact(&r, "verified", "model", "z", "1.0.0");
    artifact(&r, "experimental", "decoder", "b", "1.0.0");
    artifact(&r, "official", "decoder", "b", "1.0.0");
    let (_, _, reason) = merr(check_unique(&load_all(&r)));
    assert!(reason.starts_with("decoder/b/1.0.0"), "{reason}");
}

#[test]
fn the_same_artifact_twice_is_a_duplicate() {
    let r = repo();
    let a = load(
        &artifact(&r, "verified", "decoder", "x", "1.0.0"),
        &r.config,
    )
    .unwrap();
    assert_merr(
        check_unique(&[a.clone(), a]),
        FIELD_PATH,
        "listed more than once",
    );
}

#[test]
fn end_to_end_discover_load_unique() {
    let r = repo();
    sample_tree(&r);
    fs::write(r.art.join(ARTIFACTS_README), b"# Artifacts\n").unwrap();
    let all = load_all(&r);
    check_unique(&all).unwrap();
    let labels: Vec<String> = all.iter().map(Artifact::label).collect();
    assert_eq!(
        labels,
        [
            "experimental/decoder/spl-token/1.0.0",
            "experimental/decoder/spl-token/1.1.0",
            "experimental/model/daily-volume/0.1.0",
            "official/model/fees/1.0.0",
            "verified/decoder/token-2022/2.0.0",
        ]
    );
    for a in &all {
        assert_eq!(a.manifest.content_sha256, content_sha256(&a.content));
        assert_eq!(a.artifact_id, a.manifest.artifact_id());
    }
}

// ---------------------------------------------------------------------------
// check_unique_paths
// ---------------------------------------------------------------------------

#[test]
fn unique_paths_pass_on_a_clean_tree_and_an_empty_one() {
    let r = repo();
    check_unique_paths(&r.art).unwrap();
    sample_tree(&r);
    fs::write(r.art.join(ARTIFACTS_README), b"# Artifacts\n").unwrap();
    artifact(&r, "experimental", "model", "spl-token", "1.0.0");
    check_unique_paths(&r.art).unwrap();
}

#[test]
fn unique_paths_catch_a_duplicate_outside_the_verified_paths() {
    let r = repo();
    sample_tree(&r);
    let new = artifact(&r, "official", "decoder", "spl-token", "1.1.0");
    // Verifying only the new directory passes check_unique...
    let only_new = vec![load(&new, &r.config).unwrap()];
    check_unique(&only_new).unwrap();
    // ...but the tree as a whole holds the triple twice.
    let (artifact, field, reason) = merr(check_unique_paths(&r.art));
    assert_eq!(field, FIELD_PATH);
    assert_eq!(artifact, "official/decoder/spl-token/1.1.0");
    assert!(reason.contains("'experimental/decoder/spl-token/1.1.0'"), "{reason}");
    assert!(reason.contains("move the directory, not copy it"), "{reason}");
}

#[test]
fn unique_paths_read_no_manifest() {
    let r = repo();
    // A broken manifest elsewhere is not this check's business...
    write_files(
        &version_dir(&r, "experimental", "model", "broken", "1.0.0"),
        &[(MANIFEST_FILE, b"not json"), ("model.sql", SQL)],
    );
    check_unique_paths(&r.art).unwrap();
    // ...but a version directory that names no identity is.
    fs::create_dir_all(r.art.join("experimental/model/broken/v1")).unwrap();
    assert_merr(check_unique_paths(&r.art), FIELD_PATH, "not a version");
}

#[cfg(unix)]
#[test]
fn unique_paths_follow_the_discover_rules() {
    let r = repo();
    sample_tree(&r);
    std::os::unix::fs::symlink(
        r.art.join("official"),
        r.art.join("experimental").join("model").join("link"),
    )
    .unwrap();
    assert_merr(check_unique_paths(&r.art), FIELD_DIRECTORY, "symlinks are not allowed");
}
