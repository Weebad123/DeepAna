//! Artifact manifests, validation and canonical identity.
//!
//! An artifact lives at `artifacts/<status>/<kind>/<name>/<version>/` and
//! holds exactly two regular files: `manifest.json` and one content file
//! whose name is fixed by kind ([`content_file_name`]).
//!
//! - **Status is the directory**, not a manifest field, and it is not part
//!   of the identity: promoting an artifact (moving its directory) keeps its
//!   [`ArtifactId`].
//! - **The author is not in the manifest.** It is the git author.
//! - **Content is hashed as raw bytes**, and those bytes must already be
//!   canonical text (see [`check_content`]).
//! - **The identity** is SHA-256 of a hand-written canonical form of the
//!   validated manifest (see `canonical.rs`).
//! - **Bytes, not meaning.** Nothing inside an artifact is executed or parsed
//!   as code here.
//! - **Strict, first error wins**, in a fixed order (see [`load`]).
//!
//! This module depends only on `config.rs`.

mod canonical;
mod validate;

use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

use crate::config::{CommonsConfig, CommonsError, echo};

pub use canonical::{content_sha256, sha256, to_hex};
pub use validate::{
    ARTIFACTS_README, FIELD_CONTENT, FIELD_DIRECTORY, FIELD_MANIFEST, FIELD_PATH, MANIFEST_FILE,
    MAX_DESCRIPTION_LEN, MAX_ENTRIES_PER_DIR, MAX_LICENSE_LEN, MAX_MANIFEST_BYTES, MAX_NAME_LEN,
    MAX_VERSION_DIRS, ManifestIssue, SCHEMA_VERSION, check_content,
};

use validate::{manifest_err, real_dir};

// Shared with `fixtures/`, which reads files and names the way this module does.
pub(crate) use validate::{
    Entry, check_description, check_name, io_err, list_dir_with, parse_sha256_hex,
    read_regular_file_with,
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Where an artifact sits in review. Ordered by promotion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Status {
    Experimental,
    Verified,
    Official,
}

impl Status {
    pub const ALL: [Status; 3] = [Status::Experimental, Status::Verified, Status::Official];

    pub fn as_str(self) -> &'static str {
        match self {
            Status::Experimental => "experimental",
            Status::Verified => "verified",
            Status::Official => "official",
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Status {
    type Err = String;

    /// Exact, lowercase match only.
    fn from_str(s: &str) -> Result<Self, String> {
        Status::ALL
            .into_iter()
            .find(|st| st.as_str() == s)
            .ok_or_else(|| {
                format!(
                    "'{}' is not a status (expected experimental, verified or official)",
                    echo(s)
                )
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    /// Anchor IDL JSON.
    Decoder,
    /// Read-only SQL.
    Model,
}

impl Kind {
    pub const ALL: [Kind; 2] = [Kind::Decoder, Kind::Model];

    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Decoder => "decoder",
            Kind::Model => "model",
        }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = String;

    /// Exact, lowercase match only.
    fn from_str(s: &str) -> Result<Self, String> {
        Kind::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| format!("'{}' is not a kind (expected decoder or model)", echo(s)))
    }
}

/// The content file of an artifact of `kind`.
pub fn content_file_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Decoder => "idl.json",
        Kind::Model => "model.sql",
    }
}

/// `MAJOR.MINOR.PATCH`. [`FromStr`] is strict (no leading zeros, no
/// pre-release or build suffix), so `Display` always gives back the input.
/// Ordered numerically.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl FromStr for Version {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        validate::parse_version(s)
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// SHA-256 of the canonical manifest bytes. Displayed as lowercase hex.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactId(pub [u8; 32]);

/// A validated `manifest.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub schema_version: u32,
    pub kind: Kind,
    pub name: String,
    pub version: Version,
    pub license: String,
    pub description: String,
    pub content_sha256: [u8; 32],
    pub program_id: Option<[u8; 32]>,
}

/// A fully validated artifact directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    pub dir: PathBuf,
    pub status: Status,
    pub manifest: Manifest,
    /// Raw content bytes, exactly as on disk.
    pub content: Vec<u8>,
    pub artifact_id: ArtifactId,
}

impl Artifact {
    /// `<status>/<kind>/<name>/<version>`, built from validated values.
    pub fn label(&self) -> String {
        format!(
            "{}/{}/{}/{}",
            self.status, self.manifest.kind, self.manifest.name, self.manifest.version
        )
    }
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// How a path is named in errors: relative to `artifacts/` if it is inside
/// it, otherwise in full. Always echoed.
fn label_for(path: &Path, artifacts_dir: &Path) -> String {
    match path.strip_prefix(artifacts_dir) {
        Ok(rel) if rel.as_os_str().is_empty() => crate::config::ARTIFACTS_DIR.to_string(),
        Ok(rel) => echo(&rel.to_string_lossy()),
        Err(_) => echo(&path.to_string_lossy()),
    }
}

/// The components of `path` below `artifacts_dir`, each a plain UTF-8 name
/// that is not hidden. Purely lexical.
fn relative_components<'a>(path: &'a Path, artifacts_dir: &Path) -> Result<Vec<&'a str>, String> {
    let rel = path.strip_prefix(artifacts_dir).map_err(|_| {
        format!(
            "'{}' is not inside the artifacts directory '{}'",
            echo(&path.to_string_lossy()),
            echo(&artifacts_dir.to_string_lossy())
        )
    })?;
    let mut out = Vec::new();
    for c in rel.components() {
        let Component::Normal(os) = c else {
            return Err("must be a plain path without '.' or '..' components".into());
        };
        let s = os
            .to_str()
            .ok_or_else(|| format!("'{}' is not valid UTF-8", echo(&os.to_string_lossy())))?;
        if s.starts_with('.') {
            return Err(format!("'{}' is a hidden entry", echo(s)));
        }
        out.push(s);
    }
    Ok(out)
}

/// What an artifact's path says it is.
struct PathIdentity {
    status: Status,
    kind: Kind,
    name: String,
    version: Version,
}

fn path_identity(dir: &Path, artifacts_dir: &Path) -> Result<PathIdentity, String> {
    let c = relative_components(dir, artifacts_dir)?;
    let [status, kind, name, version] = c.as_slice() else {
        return Err(format!(
            "an artifact must be at artifacts/<status>/<kind>/<name>/<version>, \
             but this path is {} level(s) below artifacts/",
            c.len()
        ));
    };
    let status: Status = status.parse()?;
    let kind: Kind = kind.parse()?;
    validate::check_name(name)?;
    let version: Version = version.parse()?;
    Ok(PathIdentity {
        status,
        kind,
        name: name.to_string(),
        version,
    })
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Load and validate one artifact directory. Stops at the first failure:
///
/// 1. **Identity from the path**: `artifacts/<status>/<kind>/<name>/<version>`. (`path`)
/// 2. **The directory**: it and every directory between it and `artifacts/`
///    are real directories, not symlinks. (`directory`)
/// 3. **The entries**: exactly `manifest.json` and the content file for the
///    path's kind, both regular files. (`directory`)
/// 4. **The manifest**: [`Manifest::parse`]. (`manifest`, `manifest.<key>`)
/// 5. **Path versus manifest**: `kind`, `name`, `version` equal the path's.
/// 6. **The content**: [`check_content`]. (`content`)
/// 7. **The hash**: SHA-256 of the raw content equals `content_sha256`.
/// 8. **Kind rules**: `program_id` required for decoders, absent for models.
/// 9. **The identity**: canonical bytes, which must parse back to the same
///    manifest (a failure here is a bug: [`CommonsError::Internal`]).
///
/// A file that cannot be read at all is an [`CommonsError::IoError`].
pub fn load(dir: &Path, config: &CommonsConfig) -> Result<Artifact, CommonsError> {
    let root = &config.artifacts_dir;
    let label = label_for(dir, root);
    let err = |field: &str, reason: String| manifest_err(&label, field, reason);

    // 1. Identity from the path.
    let id = path_identity(dir, root).map_err(|r| err(FIELD_PATH, r))?;

    // 2. The directory, and every directory above it up to artifacts/.
    let mut p = root.clone();
    for c in dir
        .strip_prefix(root)
        .expect("checked in step 1")
        .components()
    {
        p.push(c);
        real_dir(&p, &label)?;
    }

    // 3. The entries.
    let files = validate::artifact_files(dir, id.kind, &label)?;

    // 4. The manifest.
    let (mpath, mmeta) = &files.manifest;
    let bytes =
        validate::read_regular_file(mpath, mmeta, MAX_MANIFEST_BYTES, &label, FIELD_MANIFEST)?;
    let manifest = Manifest::parse(&bytes).map_err(|i| i.into_error(&label))?;

    // 5. Path versus manifest.
    if manifest.kind != id.kind {
        return Err(err(
            "manifest.kind",
            format!("is '{}' but the path says '{}'", manifest.kind, id.kind),
        ));
    }
    if manifest.name != id.name {
        return Err(err(
            "manifest.name",
            format!("is '{}' but the path says '{}'", manifest.name, id.name),
        ));
    }
    if manifest.version != id.version {
        return Err(err(
            "manifest.version",
            format!(
                "is '{}' but the path says '{}'",
                manifest.version, id.version
            ),
        ));
    }

    // 6. The content.
    let (cpath, cmeta) = &files.content;
    if cmeta.len() == 0 {
        return Err(err(FIELD_CONTENT, "is empty".into()));
    }
    let content = validate::read_regular_file(
        cpath,
        cmeta,
        config.max_artifact_bytes,
        &label,
        FIELD_CONTENT,
    )?;
    check_content(&content, config.max_artifact_bytes).map_err(|r| err(FIELD_CONTENT, r))?;

    // 7. The hash.
    let actual = content_sha256(&content);
    if actual != manifest.content_sha256 {
        return Err(err(
            "manifest.content_sha256",
            format!(
                "is {} but {} hashes to {}",
                to_hex(&manifest.content_sha256),
                content_file_name(id.kind),
                to_hex(&actual)
            ),
        ));
    }

    // 8. Kind rules.
    validate::check_kind_rules(&manifest).map_err(|i| i.into_error(&label))?;

    // 9. The identity.
    let canonical = manifest.canonical_bytes();
    if Manifest::parse(&canonical).as_ref() != Ok(&manifest) {
        return Err(CommonsError::Internal(format!(
            "artifact {label}: canonical manifest does not parse back to the same manifest"
        )));
    }
    let artifact_id = ArtifactId(sha256(&canonical));

    Ok(Artifact {
        dir: dir.to_path_buf(),
        status: id.status,
        manifest,
        content,
        artifact_id,
    })
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Depth below `artifacts/` of a version directory.
const VERSION_DEPTH: usize = 4;

/// What lives at each depth below `artifacts/` (index = depth - 1).
const LEVELS: [&str; VERSION_DEPTH] = ["status", "kind", "name", "version"];

/// Expand `paths` (each a directory inside `artifacts/`: the root, a status,
/// kind, name or version directory) into the sorted, de-duplicated list of
/// version directories below them.
///
/// Only directories are allowed at every level, except `README.md` directly
/// in `artifacts/`. Symlinks and hidden entries are errors anywhere. Status
/// and kind directories must have known names; name and version directories
/// are checked by [`load`]. Version directories are not entered.
///
/// Sorted by path components, never by filesystem order.
pub fn discover(paths: &[PathBuf], config: &CommonsConfig) -> Result<Vec<PathBuf>, CommonsError> {
    let root = &config.artifacts_dir;
    let mut out = Vec::new();
    for path in paths {
        let label = label_for(path, root);
        let comps =
            relative_components(path, root).map_err(|r| manifest_err(&label, FIELD_PATH, r))?;
        if comps.len() > VERSION_DEPTH {
            return Err(manifest_err(
                &label,
                FIELD_PATH,
                "is deeper than a version directory \
                 (artifacts/<status>/<kind>/<name>/<version>)",
            ));
        }
        if let Some(s) = comps.first() {
            s.parse::<Status>()
                .map_err(|r| manifest_err(&label, FIELD_PATH, r))?;
        }
        if let Some(k) = comps.get(1) {
            k.parse::<Kind>()
                .map_err(|r| manifest_err(&label, FIELD_PATH, r))?;
        }
        real_dir(root, &label)?;
        let mut p = root.clone();
        for c in &comps {
            p.push(c);
            real_dir(&p, &label)?;
        }
        walk(path, comps.len(), root, &mut out)?;
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn walk(dir: &Path, depth: usize, root: &Path, out: &mut Vec<PathBuf>) -> Result<(), CommonsError> {
    if depth == VERSION_DEPTH {
        if out.len() == MAX_VERSION_DIRS {
            return Err(manifest_err(
                &label_for(dir, root),
                FIELD_DIRECTORY,
                format!("more than {MAX_VERSION_DIRS} version directories"),
            ));
        }
        out.push(dir.to_path_buf());
        return Ok(());
    }
    let level = LEVELS[depth];
    for e in validate::list_dir(dir, &label_for(dir, root))? {
        let label = label_for(&e.path, root);
        let bad = |reason: String| manifest_err(&label, FIELD_DIRECTORY, reason);
        let Some(name) = e.name.to_str() else {
            return Err(bad("name is not valid UTF-8".into()));
        };
        if name.starts_with('.') {
            return Err(bad("hidden entries are not allowed".into()));
        }
        if e.file_type.is_symlink() {
            return Err(bad("symlinks are not allowed".into()));
        }
        if depth == 0 && name == ARTIFACTS_README {
            if !e.file_type.is_file() {
                return Err(bad(format!("{ARTIFACTS_README} must be a regular file")));
            }
            continue;
        }
        if !e.file_type.is_dir() {
            return Err(bad(format!(
                "only {level} directories are allowed here{}",
                if depth == 0 {
                    format!(" (and {ARTIFACTS_README})")
                } else {
                    String::new()
                }
            )));
        }
        match depth {
            0 => {
                name.parse::<Status>().map_err(bad)?;
            }
            1 => {
                name.parse::<Kind>().map_err(bad)?;
            }
            _ => {}
        }
        walk(&e.path, depth + 1, root, out)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Uniqueness
// ---------------------------------------------------------------------------

/// `(kind, name, version)` must be unique across all three status
/// directories. The same triple under two statuses is a promotion done as a
/// copy instead of a move. The reported pair does not depend on input order.
pub fn check_unique(artifacts: &[Artifact]) -> Result<(), CommonsError> {
    let key = |a: &Artifact| (a.manifest.kind, a.manifest.name.clone(), a.manifest.version);
    let mut sorted: Vec<&Artifact> = artifacts.iter().collect();
    sorted.sort_by(|a, b| {
        key(a)
            .cmp(&key(b))
            .then(a.status.cmp(&b.status))
            .then_with(|| a.dir.cmp(&b.dir))
    });
    for pair in sorted.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        if key(a) != key(b) {
            continue;
        }
        let triple = format!(
            "{}/{}/{}",
            a.manifest.kind, a.manifest.name, a.manifest.version
        );
        let reason = if a.status == b.status {
            format!("{triple} is listed more than once")
        } else {
            format!(
                "{triple} exists in both '{}' and '{}'; promotion must move the directory, \
                 not copy it",
                a.label(),
                b.label()
            )
        };
        return Err(manifest_err(&b.label(), FIELD_PATH, reason));
    }
    Ok(())
}
