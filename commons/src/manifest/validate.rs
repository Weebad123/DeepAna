//! Field rules, content-file rules and directory rules.
//!
//! Everything here checks bytes, not meaning: a decoder's IDL is never
//! parsed as JSON and a model's SQL is never parsed as SQL.
//!
//! `manifest.json` is read by a small hand-written reader instead of a
//! general JSON library, so the accepted subset is exactly what is listed
//! below and nothing a library happens to tolerate:
//!
//! - UTF-8 without a byte-order mark, at most [`MAX_MANIFEST_BYTES`].
//! - One flat object. Values are strings or non-negative integers; `null`,
//!   booleans, arrays, nested objects, fractions, exponents, signs and
//!   leading zeros are rejected.
//! - Strings may escape only `"` and `\`. Every string field is printable
//!   ASCII, so any other escape would only spell a character that either
//!   is rejected anyway or needed no escape. Each value has one spelling.
//! - No duplicate keys, no unknown keys, no comments, no trailing commas,
//!   nothing after the object but whitespace (space, tab, LF, CR).
//!
//! Whitespace and key order in the file are free: the identity comes from
//! the canonical form (see `canonical.rs`), not the file.
//!
//! The first error wins, and the order is fixed: syntax (in document
//! order), duplicate keys, `schema_version`, unknown keys, then each field
//! in canonical order.

use std::collections::HashSet;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, FileType, Metadata};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use crate::config::{CommonsError, echo};

use super::{Kind, Manifest, Version, content_file_name};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The only `schema_version` this build understands.
pub const SCHEMA_VERSION: u32 = 1;
/// Name of the manifest file in every artifact directory.
pub const MANIFEST_FILE: &str = "manifest.json";
/// The one non-directory allowed directly in `artifacts/`.
pub const ARTIFACTS_README: &str = "README.md";

/// Largest accepted `manifest.json`, in bytes.
pub const MAX_MANIFEST_BYTES: u64 = 16 * 1024;
pub const MAX_NAME_LEN: usize = 64;
pub const MAX_LICENSE_LEN: usize = 64;
pub const MAX_DESCRIPTION_LEN: usize = 256;
/// Most entries read from any one directory.
pub const MAX_ENTRIES_PER_DIR: usize = 1_000;
/// Most version directories [`super::discover`] returns in total.
pub const MAX_VERSION_DIRS: usize = 10_000;

/// Stable `field` identifiers for [`CommonsError::ManifestError`].
pub const FIELD_PATH: &str = "path";
pub const FIELD_DIRECTORY: &str = "directory";
pub const FIELD_MANIFEST: &str = "manifest";
pub const FIELD_CONTENT: &str = "content";

/// Manifest keys, in canonical order.
pub(crate) const MANIFEST_KEYS: [&str; 8] = [
    "schema_version",
    "kind",
    "name",
    "version",
    "license",
    "description",
    "content_sha256",
    "program_id",
];

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// One problem with a manifest, before it is tied to an artifact path.
/// `reason` already has contributor-derived text passed through [`echo`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestIssue {
    /// `manifest` for syntax and unknown keys, `manifest.<key>` otherwise.
    pub field: String,
    pub reason: String,
}

impl ManifestIssue {
    fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        ManifestIssue {
            field: field.into(),
            reason: reason.into(),
        }
    }

    fn key(key: &str, reason: impl Into<String>) -> Self {
        Self::new(format!("{FIELD_MANIFEST}.{key}"), reason)
    }

    pub fn into_error(self, artifact: &str) -> CommonsError {
        manifest_err(artifact, &self.field, self.reason)
    }
}

impl fmt::Display for ManifestIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.field, self.reason)
    }
}

impl std::error::Error for ManifestIssue {}

pub(crate) fn manifest_err(artifact: &str, field: &str, reason: impl Into<String>) -> CommonsError {
    CommonsError::ManifestError {
        artifact: artifact.to_string(),
        field: field.to_string(),
        reason: reason.into(),
    }
}

pub(crate) fn io_err(path: &Path, source: io::Error) -> CommonsError {
    CommonsError::IoError {
        path: path.to_path_buf(),
        source,
    }
}

// ---------------------------------------------------------------------------
// Field rules (pure)
// ---------------------------------------------------------------------------

/// `MAJOR.MINOR.PATCH`: ASCII digits, no leading zeros, each fits `u32`.
/// No `v` prefix, pre-release or build metadata.
pub(crate) fn parse_version(s: &str) -> Result<Version, String> {
    let bad = |why: &str| {
        format!(
            "'{}' is not a version: {why} (expected MAJOR.MINOR.PATCH, e.g. 1.0.0)",
            echo(s)
        )
    };
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return Err(bad("needs exactly three dot-separated numbers"));
    }
    let mut n = [0u32; 3];
    for (slot, p) in n.iter_mut().zip(&parts) {
        if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad("each part must be ASCII digits only"));
        }
        if p.len() > 1 && p.starts_with('0') {
            return Err(bad("leading zeros are not allowed"));
        }
        *slot = p
            .parse()
            .map_err(|_| bad("a part is larger than 4294967295"))?;
    }
    Ok(Version {
        major: n[0],
        minor: n[1],
        patch: n[2],
    })
}

/// Lowercase letters, digits and single hyphens; starts with a letter,
/// does not end with a hyphen; 1 to [`MAX_NAME_LEN`] chars.
pub(crate) fn check_name(s: &str) -> Result<(), String> {
    let bad = |why: &str| format!("'{}' is not a valid name: {why}", echo(s));
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > MAX_NAME_LEN {
        return Err(bad(&format!("longer than {MAX_NAME_LEN} characters")));
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(bad("only a-z, 0-9 and '-' are allowed"));
    }
    if !s.as_bytes()[0].is_ascii_lowercase() {
        return Err(bad("must start with a letter a-z"));
    }
    if s.ends_with('-') {
        return Err(bad("must not end with '-'"));
    }
    if s.contains("--") {
        return Err(bad("must not contain '--'"));
    }
    Ok(())
}

/// One SPDX-identifier-shaped token: letters, digits, `.`, `-`, `+`,
/// starting with a letter or digit; 1 to [`MAX_LICENSE_LEN`] chars.
/// The shape is checked, not whether the licence exists.
pub(crate) fn check_license(s: &str) -> Result<(), String> {
    let bad = |why: &str| format!("'{}' is not a valid license identifier: {why}", echo(s));
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > MAX_LICENSE_LEN {
        return Err(bad(&format!("longer than {MAX_LICENSE_LEN} characters")));
    }
    if !s
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+'))
    {
        return Err(bad("only A-Z, a-z, 0-9, '.', '-' and '+' are allowed"));
    }
    if !s.as_bytes()[0].is_ascii_alphanumeric() {
        return Err(bad("must start with a letter or digit"));
    }
    Ok(())
}

/// Printable ASCII (0x20..=0x7E) only, no leading or trailing space,
/// 1 to [`MAX_DESCRIPTION_LEN`] chars.
pub(crate) fn check_description(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > MAX_DESCRIPTION_LEN {
        return Err(format!(
            "is {} bytes, longer than {MAX_DESCRIPTION_LEN}",
            s.len()
        ));
    }
    if let Some((i, c)) = s.char_indices().find(|&(_, c)| !(' '..='~').contains(&c)) {
        return Err(format!(
            "only printable ASCII is allowed; found '{}' (U+{:04X}) at byte {i}",
            echo(&c.to_string()),
            c as u32
        ));
    }
    if s.starts_with(' ') || s.ends_with(' ') {
        return Err("must not start or end with a space".into());
    }
    Ok(())
}

/// Exactly 64 lowercase hex digits.
pub(crate) fn parse_sha256_hex(s: &str) -> Result<[u8; 32], String> {
    let bad = || format!("'{}' is not 64 lowercase hex digits", echo(s));
    let b = s.as_bytes();
    if b.len() != 64 {
        return Err(bad());
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        let hi = nibble(b[2 * i]).ok_or_else(bad)?;
        let lo = nibble(b[2 * i + 1]).ok_or_else(bad)?;
        *o = (hi << 4) | lo;
    }
    Ok(out)
}

/// A base58 Solana address: decodes to exactly 32 bytes and re-encodes to
/// the same string.
pub(crate) fn parse_program_id(s: &str) -> Result<[u8; 32], String> {
    let bad = |why: &str| format!("'{}' is not a program address: {why}", echo(s));
    // 32 bytes encode to 32..=44 base58 chars.
    if !(32..=44).contains(&s.len()) {
        return Err(bad("must be 32 to 44 base58 characters"));
    }
    let bytes = bs58::decode(s)
        .into_vec()
        .map_err(|_| bad("not valid base58"))?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| bad(&format!("decodes to {} bytes, expected 32", bytes.len())))?;
    if bs58::encode(arr).into_string() != s {
        return Err(bad("not in canonical base58 form"));
    }
    Ok(arr)
}

/// Rules that depend on the kind: a decoder names the program it decodes, a
/// model has no program.
pub(crate) fn check_kind_rules(m: &Manifest) -> Result<(), ManifestIssue> {
    match (m.kind, &m.program_id) {
        (Kind::Decoder, None) => Err(ManifestIssue::key(
            "program_id",
            "is required for a decoder",
        )),
        (Kind::Model, Some(_)) => Err(ManifestIssue::key(
            "program_id",
            "is not allowed for a model",
        )),
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Content-file rules (pure)
// ---------------------------------------------------------------------------

/// Text rules shared by every content file, in this order:
/// not empty; at most `max_bytes`; no BOM; valid UTF-8; no `\r`; ends in
/// `\n`; exactly one trailing `\n`.
pub fn check_content(bytes: &[u8], max_bytes: u64) -> Result<(), String> {
    if bytes.is_empty() {
        return Err("is empty".into());
    }
    if bytes.len() as u64 > max_bytes {
        return Err(format!(
            "is {} bytes, more than the limit of {max_bytes}",
            bytes.len()
        ));
    }
    if bytes.starts_with(UTF8_BOM) {
        return Err("starts with a UTF-8 byte-order mark".into());
    }
    if let Err(e) = std::str::from_utf8(bytes) {
        return Err(format!(
            "is not valid UTF-8 (first bad byte at offset {})",
            e.valid_up_to()
        ));
    }
    if let Some(pos) = bytes.iter().position(|&b| b == b'\r') {
        let line = 1 + bytes[..pos].iter().filter(|&&b| b == b'\n').count();
        return Err(format!(
            "contains a carriage return (\\r) on line {line}; only \\n line endings are allowed"
        ));
    }
    if bytes.last() != Some(&b'\n') {
        return Err("does not end with a newline (\\n)".into());
    }
    if bytes.len() >= 2 && bytes[bytes.len() - 2] == b'\n' {
        return Err("ends with more than one newline; exactly one is required".into());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// manifest.json reader
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Raw {
    Str(String),
    Int(u64),
}

/// The `field` for a problem with the value of `key`: `manifest.<key>` for
/// known keys, `manifest` otherwise (unknown keys are echoed in the reason).
fn field_for(key: &str) -> String {
    if MANIFEST_KEYS.contains(&key) {
        format!("{FIELD_MANIFEST}.{key}")
    } else {
        FIELD_MANIFEST.to_string()
    }
}

struct Reader<'a> {
    b: &'a [u8],
    i: usize,
}

impl<'a> Reader<'a> {
    fn syntax(&self, reason: &str) -> ManifestIssue {
        ManifestIssue::new(FIELD_MANIFEST, format!("{reason} at byte {}", self.i))
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn expect(&mut self, c: u8, what: &str) -> Result<(), ManifestIssue> {
        if self.peek() == Some(c) {
            self.i += 1;
            Ok(())
        } else {
            Err(self.syntax(&format!("expected {what}")))
        }
    }

    fn object(&mut self) -> Result<Vec<(String, Raw)>, ManifestIssue> {
        let mut entries = Vec::new();
        self.ws();
        self.expect(b'{', "'{' (the manifest must be a JSON object)")?;
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
        } else {
            loop {
                if self.peek() != Some(b'"') {
                    return Err(self.syntax("expected a key in double quotes"));
                }
                let key = self.string()?;
                self.ws();
                self.expect(b':', "':' after a key")?;
                self.ws();
                let value = self.value(&key)?;
                entries.push((key, value));
                self.ws();
                match self.peek() {
                    Some(b',') => {
                        self.i += 1;
                        self.ws();
                    }
                    Some(b'}') => {
                        self.i += 1;
                        break;
                    }
                    _ => return Err(self.syntax("expected ',' or '}'")),
                }
            }
        }
        self.ws();
        if self.i != self.b.len() {
            return Err(self.syntax("unexpected data after the object"));
        }
        Ok(entries)
    }

    fn value(&mut self, key: &str) -> Result<Raw, ManifestIssue> {
        let at = self.i;
        let found = match self.peek() {
            None => return Err(self.syntax("unexpected end of file")),
            Some(b'"') => return self.string().map(Raw::Str),
            Some(b'0'..=b'9') => return self.integer(key),
            Some(b'-') => "a negative number",
            Some(b'n') => "null",
            Some(b't' | b'f') => "a boolean",
            Some(b'[') => "an array",
            Some(b'{') => "an object",
            Some(_) => "an invalid token",
        };
        Err(ManifestIssue::new(
            field_for(key),
            format!(
                "value of '{}' must be a string or a non-negative integer, found {found} at byte {at}",
                echo(key)
            ),
        ))
    }

    /// Digits only, no leading zero, fits `u64`; no fraction or exponent.
    fn integer(&mut self, key: &str) -> Result<Raw, ManifestIssue> {
        let start = self.i;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        let digits = &self.b[start..self.i];
        let bad = |why: &str| ManifestIssue::new(field_for(key), format!("{why} at byte {start}"));
        if digits.len() > 1 && digits[0] == b'0' {
            return Err(bad("leading zeros are not allowed"));
        }
        if matches!(self.peek(), Some(b'.' | b'e' | b'E')) {
            return Err(bad("only integers are allowed (no fraction or exponent)"));
        }
        // Digits are ASCII, so this is valid UTF-8.
        std::str::from_utf8(digits)
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Raw::Int)
            .ok_or_else(|| bad("integer is out of range"))
    }

    /// A string starting at the current `"`. Escapes: `\"` and `\\` only.
    fn string(&mut self) -> Result<String, ManifestIssue> {
        self.i += 1; // opening quote
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => return Err(self.syntax("unterminated string")),
                Some(b'"') => {
                    self.i += 1;
                    break;
                }
                Some(b'\\') => {
                    self.i += 1;
                    match self.peek() {
                        Some(c @ (b'"' | b'\\')) => {
                            out.push(c);
                            self.i += 1;
                        }
                        _ => {
                            return Err(self
                                .syntax("escape not allowed (only \\\" and \\\\ may be escaped)"));
                        }
                    }
                }
                Some(c) if c < 0x20 => {
                    return Err(self.syntax("control character inside a string"));
                }
                Some(c) => {
                    out.push(c);
                    self.i += 1;
                }
            }
        }
        // The input is valid UTF-8 and we only split it at ASCII bytes.
        String::from_utf8(out).map_err(|_| self.syntax("invalid UTF-8 in string"))
    }
}

impl Manifest {
    /// Parse and validate `manifest.json` bytes. Does not check the kind
    /// rules (see [`super::load`]); everything else about the manifest alone
    /// is checked here.
    pub fn parse(bytes: &[u8]) -> Result<Manifest, ManifestIssue> {
        if bytes.is_empty() {
            return Err(ManifestIssue::new(FIELD_MANIFEST, "is empty"));
        }
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(ManifestIssue::new(
                FIELD_MANIFEST,
                format!(
                    "is {} bytes, more than the limit of {MAX_MANIFEST_BYTES}",
                    bytes.len()
                ),
            ));
        }
        if bytes.starts_with(UTF8_BOM) {
            return Err(ManifestIssue::new(
                FIELD_MANIFEST,
                "starts with a UTF-8 byte-order mark",
            ));
        }
        if let Err(e) = std::str::from_utf8(bytes) {
            return Err(ManifestIssue::new(
                FIELD_MANIFEST,
                format!(
                    "is not valid UTF-8 (first bad byte at offset {})",
                    e.valid_up_to()
                ),
            ));
        }

        let entries = Reader { b: bytes, i: 0 }.object()?;

        let mut seen = HashSet::new();
        for (k, _) in &entries {
            if !seen.insert(k.as_str()) {
                return Err(ManifestIssue::new(
                    field_for(k),
                    format!("duplicate key '{}'", echo(k)),
                ));
            }
        }
        let get = |key: &str| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v);

        // schema_version first: a newer manifest should say so, not complain
        // about fields this build has never heard of.
        let schema_version = match get("schema_version") {
            None => return Err(ManifestIssue::key("schema_version", "is missing")),
            Some(Raw::Str(_)) => {
                return Err(ManifestIssue::key(
                    "schema_version",
                    "must be an integer, not a string",
                ));
            }
            Some(Raw::Int(n)) => *n,
        };
        if schema_version != u64::from(SCHEMA_VERSION) {
            return Err(ManifestIssue::key(
                "schema_version",
                format!("{schema_version} is not supported (this build supports {SCHEMA_VERSION})"),
            ));
        }

        for (k, _) in &entries {
            if MANIFEST_KEYS.contains(&k.as_str()) {
                continue;
            }
            let hint = match k.as_str() {
                "status" => {
                    ": status is the directory the artifact lives in \
                     (experimental, verified or official), not a manifest field"
                }
                "author" | "authors" => {
                    ": the author is the git author of the pull request, not a manifest field"
                }
                _ => "",
            };
            return Err(ManifestIssue::new(
                FIELD_MANIFEST,
                format!("unknown field '{}'{hint}", echo(k)),
            ));
        }

        let string = |key: &'static str| -> Result<&str, ManifestIssue> {
            match get(key) {
                None => Err(ManifestIssue::key(key, "is missing")),
                Some(Raw::Int(_)) => Err(ManifestIssue::key(key, "must be a string, not a number")),
                Some(Raw::Str(s)) => Ok(s.as_str()),
            }
        };

        let kind_s = string("kind")?;
        let kind: Kind = kind_s.parse().map_err(|r| ManifestIssue::key("kind", r))?;
        let name = string("name")?;
        check_name(name).map_err(|r| ManifestIssue::key("name", r))?;
        let version =
            parse_version(string("version")?).map_err(|r| ManifestIssue::key("version", r))?;
        let license = string("license")?;
        check_license(license).map_err(|r| ManifestIssue::key("license", r))?;
        let description = string("description")?;
        check_description(description).map_err(|r| ManifestIssue::key("description", r))?;
        let content_sha256 = parse_sha256_hex(string("content_sha256")?)
            .map_err(|r| ManifestIssue::key("content_sha256", r))?;
        let program_id = match get("program_id") {
            None => None,
            Some(_) => Some(
                parse_program_id(string("program_id")?)
                    .map_err(|r| ManifestIssue::key("program_id", r))?,
            ),
        };

        Ok(Manifest {
            schema_version: SCHEMA_VERSION,
            kind,
            name: name.to_string(),
            version,
            license: license.to_string(),
            description: description.to_string(),
            content_sha256,
            program_id,
        })
    }
}

// ---------------------------------------------------------------------------
// Directory and file rules (I/O)
// ---------------------------------------------------------------------------

/// One directory entry. `file_type` does not follow symlinks.
pub(crate) struct Entry {
    pub name: OsString,
    pub path: PathBuf,
    pub file_type: FileType,
}

/// `path` must exist and be a real directory, not a symlink.
pub(crate) fn real_dir(path: &Path, artifact: &str) -> Result<(), CommonsError> {
    let meta = match fs::symlink_metadata(path) {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Err(manifest_err(
                artifact,
                FIELD_DIRECTORY,
                format!("'{}' does not exist", echo(&path.to_string_lossy())),
            ));
        }
        Err(e) => return Err(io_err(path, e)),
    };
    if meta.file_type().is_symlink() {
        return Err(manifest_err(
            artifact,
            FIELD_DIRECTORY,
            format!("'{}' is a symlink", echo(&path.to_string_lossy())),
        ));
    }
    if !meta.is_dir() {
        return Err(manifest_err(
            artifact,
            FIELD_DIRECTORY,
            format!("'{}' is not a directory", echo(&path.to_string_lossy())),
        ));
    }
    Ok(())
}

/// All entries of `dir`, sorted by name so that errors do not depend on the
/// order the filesystem returns them in. At most [`MAX_ENTRIES_PER_DIR`].
pub(crate) fn list_dir(dir: &Path, artifact: &str) -> Result<Vec<Entry>, CommonsError> {
    list_dir_with(dir, |reason| manifest_err(artifact, FIELD_DIRECTORY, reason))
}

/// [`list_dir`] with the caller's error for a rule violation (I/O failures
/// are always [`CommonsError::IoError`]). Shared with `fixtures/`, so both
/// modules read directories the same way.
pub(crate) fn list_dir_with(
    dir: &Path,
    rule_err: impl Fn(String) -> CommonsError,
) -> Result<Vec<Entry>, CommonsError> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir).map_err(|e| io_err(dir, e))? {
        let e = e.map_err(|e| io_err(dir, e))?;
        if out.len() == MAX_ENTRIES_PER_DIR {
            return Err(rule_err(format!(
                "has more than {MAX_ENTRIES_PER_DIR} entries"
            )));
        }
        let file_type = e.file_type().map_err(|err| io_err(&e.path(), err))?;
        out.push(Entry {
            name: e.file_name(),
            path: e.path(),
            file_type,
        });
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// The two files of an artifact directory, with their `lstat` metadata.
pub(crate) struct ArtifactFiles {
    pub manifest: (PathBuf, Metadata),
    pub content: (PathBuf, Metadata),
}

/// An artifact directory holds exactly `manifest.json` and the content file
/// for `kind`, both regular files. Entries are checked in name order, then
/// the manifest's presence, then the content file's.
pub(crate) fn artifact_files(
    dir: &Path,
    kind: Kind,
    artifact: &str,
) -> Result<ArtifactFiles, CommonsError> {
    let content_name = content_file_name(kind);
    let mut manifest = None;
    let mut content = None;
    for e in list_dir(dir, artifact)? {
        let shown = echo(&e.name.to_string_lossy());
        let slot = match e.name.to_str() {
            Some(MANIFEST_FILE) => &mut manifest,
            Some(n) if n == content_name => &mut content,
            _ => {
                return Err(manifest_err(
                    artifact,
                    FIELD_DIRECTORY,
                    format!(
                        "unexpected entry '{shown}': a {kind} artifact directory holds only \
                         {MANIFEST_FILE} and {content_name}"
                    ),
                ));
            }
        };
        let meta = fs::symlink_metadata(&e.path).map_err(|err| io_err(&e.path, err))?;
        if meta.file_type().is_symlink() {
            return Err(manifest_err(
                artifact,
                FIELD_DIRECTORY,
                format!("'{shown}' is a symlink"),
            ));
        }
        if !meta.is_file() {
            return Err(manifest_err(
                artifact,
                FIELD_DIRECTORY,
                format!("'{shown}' is not a regular file"),
            ));
        }
        *slot = Some((e.path, meta));
    }
    let manifest = manifest.ok_or_else(|| {
        manifest_err(
            artifact,
            FIELD_DIRECTORY,
            format!("missing {MANIFEST_FILE}"),
        )
    })?;
    let content = content.ok_or_else(|| {
        manifest_err(artifact, FIELD_DIRECTORY, format!("missing {content_name}"))
    })?;
    Ok(ArtifactFiles { manifest, content })
}

#[cfg(unix)]
fn same_file(a: &Metadata, b: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    a.dev() == b.dev() && a.ino() == b.ino()
}

#[cfg(not(unix))]
fn same_file(_: &Metadata, _: &Metadata) -> bool {
    true
}

/// Read a regular file previously `lstat`ed as `lmeta`, reading at most
/// `max_bytes` (the size on disk is checked before and while reading). The
/// opened file must still be the same regular file.
pub(crate) fn read_regular_file(
    path: &Path,
    lmeta: &Metadata,
    max_bytes: u64,
    artifact: &str,
    field: &str,
) -> Result<Vec<u8>, CommonsError> {
    read_regular_file_with(path, lmeta, max_bytes, |reason| {
        manifest_err(artifact, field, reason)
    })
}

/// [`read_regular_file`] with the caller's error for a rule violation (I/O
/// failures are always [`CommonsError::IoError`]). Shared with `fixtures/`,
/// so both modules read files the same way.
pub(crate) fn read_regular_file_with(
    path: &Path,
    lmeta: &Metadata,
    max_bytes: u64,
    rule_err: impl Fn(String) -> CommonsError,
) -> Result<Vec<u8>, CommonsError> {
    let too_large = |n: u64| rule_err(format!("is {n} bytes, more than the limit of {max_bytes}"));
    if lmeta.len() > max_bytes {
        return Err(too_large(lmeta.len()));
    }
    let file = File::open(path).map_err(|e| io_err(path, e))?;
    let fmeta = file.metadata().map_err(|e| io_err(path, e))?;
    if !fmeta.is_file() || !same_file(lmeta, &fmeta) {
        return Err(rule_err("file was replaced while being read".into()));
    }
    let mut buf = Vec::with_capacity(lmeta.len() as usize);
    file.take(max_bytes + 1)
        .read_to_end(&mut buf)
        .map_err(|e| io_err(path, e))?;
    if buf.len() as u64 > max_bytes {
        return Err(too_large(buf.len() as u64));
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SHA: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    const PID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    /// A valid decoder manifest as compact JSON, one line per field so that
    /// tests can edit single fields.
    fn fields() -> Vec<(String, String)> {
        [
            ("schema_version", "1"),
            ("kind", "\"decoder\""),
            ("name", "\"spl-token\""),
            ("version", "\"1.0.0\""),
            ("license", "\"Apache-2.0\""),
            ("description", "\"Decodes SPL Token instructions.\""),
            ("content_sha256", &format!("\"{SHA}\"")),
            ("program_id", &format!("\"{PID}\"")),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
    }

    fn render(f: &[(String, String)]) -> String {
        let body: Vec<String> = f.iter().map(|(k, v)| format!("  \"{k}\": {v}")).collect();
        format!("{{\n{}\n}}\n", body.join(",\n"))
    }

    fn with(key: &str, raw_value: &str) -> String {
        let mut f = fields();
        match f.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = raw_value.to_string(),
            None => f.push((key.to_string(), raw_value.to_string())),
        }
        render(&f)
    }

    fn without(key: &str) -> String {
        let mut f = fields();
        f.retain(|(k, _)| k != key);
        render(&f)
    }

    fn issue(src: impl AsRef<[u8]>) -> ManifestIssue {
        match Manifest::parse(src.as_ref()) {
            Ok(m) => panic!("expected an error, parsed {m:?}"),
            Err(e) => e,
        }
    }

    #[track_caller]
    fn assert_issue(src: impl AsRef<[u8]>, field: &str, reason_part: &str) {
        let e = issue(src);
        assert_eq!(e.field, field, "{e}");
        assert!(
            e.reason.contains(reason_part),
            "expected {reason_part:?} in: {e}"
        );
    }

    // -- Version ------------------------------------------------------------

    #[test]
    fn version_accepts_strict_semver_core() {
        for (s, v) in [
            ("0.0.0", (0, 0, 0)),
            ("1.0.0", (1, 0, 0)),
            ("1.2.3", (1, 2, 3)),
            ("10.20.30", (10, 20, 30)),
            ("4294967295.0.4294967295", (u32::MAX, 0, u32::MAX)),
        ] {
            let got = parse_version(s).unwrap();
            assert_eq!((got.major, got.minor, got.patch), v, "{s}");
            assert_eq!(got.to_string(), s, "Display round-trips");
            assert_eq!(s.parse::<Version>().unwrap(), got, "FromStr agrees");
        }
    }

    #[test]
    fn version_rejects_everything_else() {
        for bad in [
            "",
            "1",
            "1.0",
            "1.0.0.0",
            "01.0.0",
            "1.00.0",
            "1.0.00",
            "1..0",
            ".1.0",
            "1.0.",
            "v1.0.0",
            "V1.0.0",
            "1.0.0-rc.1",
            "1.0.0+build",
            "+1.0.0",
            "-1.0.0",
            " 1.0.0",
            "1.0.0 ",
            "1.0.0\n",
            "1. 0.0",
            "1.0x0.0",
            "0x1.0.0",
            "1_0.0.0",
            "1e3.0.0",
            "4294967296.0.0",
            "99999999999999999999.0.0",
            "١.٠.٠",
            "１.0.0",
            "1,0,0",
            "1/0/0",
        ] {
            assert!(parse_version(bad).is_err(), "{bad:?} must be rejected");
            assert!(bad.parse::<Version>().is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn version_orders_numerically_not_lexically() {
        let v = |s: &str| s.parse::<Version>().unwrap();
        assert!(v("1.10.0") > v("1.9.0"));
        assert!(v("2.0.0") > v("1.99.99"));
        assert!(v("1.0.10") > v("1.0.2"));
        let mut all = [v("10.0.0"), v("1.10.0"), v("1.2.0"), v("1.0.0"), v("9.9.9")];
        all.sort();
        let shown: Vec<String> = all.iter().map(ToString::to_string).collect();
        assert_eq!(shown, ["1.0.0", "1.2.0", "1.10.0", "9.9.9", "10.0.0"]);
    }

    // -- Field rules ----------------------------------------------------------

    #[test]
    fn name_rules() {
        let max = "a".repeat(MAX_NAME_LEN);
        for ok in ["a", "spl-token", "a1", "token-2022", "x-y-z", max.as_str()] {
            assert!(check_name(ok).is_ok(), "{ok:?}");
        }
        let long = "a".repeat(MAX_NAME_LEN + 1);
        for bad in [
            "",
            "-a",
            "a-",
            "a--b",
            "1abc",
            "Spl-token",
            "spl_token",
            "spl.token",
            "spl token",
            "spl/token",
            "..",
            ".",
            "é",
            "spl-tokеn",
            /* Cyrillic e */ "a\0",
            long.as_str(),
        ] {
            assert!(check_name(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn license_rules() {
        let max = "A".repeat(MAX_LICENSE_LEN);
        for ok in [
            "MIT",
            "Apache-2.0",
            "GPL-3.0+",
            "LicenseRef-Custom",
            "0BSD",
            max.as_str(),
        ] {
            assert!(check_license(ok).is_ok(), "{ok:?}");
        }
        let long = "A".repeat(MAX_LICENSE_LEN + 1);
        for bad in [
            "",
            "MIT OR Apache-2.0",
            "(MIT)",
            "-MIT",
            ".MIT",
            "MIT\n",
            "MIT/X11",
            "Ünlicense",
            long.as_str(),
        ] {
            assert!(check_license(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn description_is_printable_ascii_only() {
        let max = "x".repeat(MAX_DESCRIPTION_LEN);
        for ok in [
            "x",
            "Decodes SPL Token instructions.",
            "a \"b\" \\c ~!",
            max.as_str(),
        ] {
            assert!(check_description(ok).is_ok(), "{ok:?}");
        }
        let long = "x".repeat(MAX_DESCRIPTION_LEN + 1);
        for bad in [
            "",
            " lead",
            "trail ",
            "tab\there",
            "new\nline",
            "cr\rhere",
            "del\x7f",
            "nul\0",
            "café",
            "bidi\u{202E}txt",
            "zero\u{200B}width",
            "bom\u{FEFF}",
            "emoji 🙂",
            "nbsp\u{00A0}x",
            long.as_str(),
        ] {
            assert!(check_description(bad).is_err(), "{bad:?} must be rejected");
        }
        // Every byte value: exactly 0x20..=0x7E pass.
        let allowed = (0u8..=0x7f)
            .filter(|&b| check_description(&format!("a{}a", b as char)).is_ok())
            .count();
        assert_eq!(allowed, 0x7e - 0x20 + 1);
    }

    #[test]
    fn sha256_hex_is_64_lowercase_digits() {
        assert!(parse_sha256_hex(SHA).is_ok());
        for bad in [
            "",
            &SHA[..63],
            &format!("{SHA}0"),
            &SHA.to_uppercase(),
            &format!("{}A", &SHA[..63]),
            &format!("0x{}", &SHA[..62]),
            &format!("{} ", &SHA[..63]),
            &format!("{}g", &SHA[..63]),
        ] {
            assert!(parse_sha256_hex(bad).is_err(), "{bad:?} must be rejected");
        }
        let all_ff = "f".repeat(64);
        assert_eq!(parse_sha256_hex(&all_ff).unwrap(), [0xff; 32]);
    }

    #[test]
    fn program_id_is_a_canonical_32_byte_base58_address() {
        let pk = parse_program_id(PID).unwrap();
        assert_eq!(bs58::encode(pk).into_string(), PID);
        // The all-zero key is 32 '1's.
        assert_eq!(parse_program_id(&"1".repeat(32)).unwrap(), [0u8; 32]);
        for bad in [
            "",
            "Tokenkeg",
            &format!("{PID}1"),         // 33 bytes
            &PID[1..],                  // too few bytes
            &"1".repeat(31),            // 31 zero bytes
            &format!("0{}", &PID[1..]), // '0' is not base58
            &format!("O{}", &PID[1..]), // 'O'
            &format!("I{}", &PID[1..]), // 'I'
            &format!("l{}", &PID[1..]), // 'l'
            &format!(" {}", &PID[1..]),
            &"z".repeat(45),
        ] {
            assert!(parse_program_id(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn kind_rules() {
        let decoder = Manifest::parse(render(&fields()).as_bytes()).unwrap();
        assert!(check_kind_rules(&decoder).is_ok());
        let mut no_pid = decoder.clone();
        no_pid.program_id = None;
        let e = check_kind_rules(&no_pid).unwrap_err();
        assert_eq!(e.field, "manifest.program_id");
        assert!(e.reason.contains("required for a decoder"));

        let mut model = decoder.clone();
        model.kind = Kind::Model;
        let e = check_kind_rules(&model).unwrap_err();
        assert_eq!(e.field, "manifest.program_id");
        assert!(e.reason.contains("not allowed for a model"));
        model.program_id = None;
        assert!(check_kind_rules(&model).is_ok());
    }

    // -- Content rules -------------------------------------------------------

    #[test]
    fn content_accepts_canonical_text() {
        for ok in [
            &b"x\n"[..],
            b"\n",
            b"line 1\nline 2\n",
            "héllo wörld\n".as_bytes(),
            b"a\n\nb\n",
        ] {
            assert_eq!(check_content(ok, 1024), Ok(()), "{ok:?}");
        }
    }

    #[test]
    fn content_rejections_name_the_rule() {
        let cases: &[(&[u8], &str)] = &[
            (b"", "is empty"),
            (b"\xEF\xBB\xBFx\n", "byte-order mark"),
            (
                b"ok\n\xff\n",
                "not valid UTF-8 (first bad byte at offset 3)",
            ),
            (b"\xc3\x28\n", "not valid UTF-8"),
            (b"\xed\xa0\x80\n", "not valid UTF-8"), // UTF-16 surrogate
            (b"\xc0\xaf\n", "not valid UTF-8"),     // overlong '/'
            (b"a\r\nb\r\n", "carriage return (\\r) on line 1"),
            (b"a\nb\nc\rd\n", "carriage return (\\r) on line 3"),
            (b"a\r", "carriage return"),
            (b"no newline", "does not end with a newline"),
            (b"two\n\n", "more than one newline"),
            (b"\n\n", "more than one newline"),
            (b"many\n\n\n\n", "more than one newline"),
        ];
        for (bytes, part) in cases {
            let e = check_content(bytes, 1024).unwrap_err();
            assert!(e.contains(part), "{bytes:?}: expected {part:?} in {e:?}");
        }
    }

    #[test]
    fn content_size_boundary_is_exact() {
        let max = 64u64;
        let at_max = [&b"a".repeat(max as usize - 1)[..], b"\n"].concat();
        assert_eq!(at_max.len() as u64, max);
        assert_eq!(check_content(&at_max, max), Ok(()));
        let over = [&b"a".repeat(max as usize)[..], b"\n"].concat();
        let e = check_content(&over, max).unwrap_err();
        assert!(e.contains("65 bytes, more than the limit of 64"), "{e}");
    }

    #[test]
    fn content_rules_apply_in_fixed_order() {
        // Each input breaks several rules; the earliest in the list wins.
        let cases: &[(&[u8], &str)] = &[
            (b"\xEF\xBB\xBF\xff\r\n\n", "byte-order mark"),
            (b"\xff\r\n\n", "not valid UTF-8"),
            (b"a\r\n\n", "carriage return"),
        ];
        for (bytes, part) in cases {
            let e = check_content(bytes, 1024).unwrap_err();
            assert!(e.contains(part), "{bytes:?}: expected {part:?} in {e:?}");
        }
        let e = check_content(b"\xEF\xBB\xBFtoo long\r", 4).unwrap_err();
        assert!(e.contains("more than the limit"), "{e}");
    }

    // -- Manifest::parse ------------------------------------------------------

    #[test]
    fn parses_the_example() {
        let m = Manifest::parse(render(&fields()).as_bytes()).unwrap();
        assert_eq!(m.schema_version, 1);
        assert_eq!(m.kind, Kind::Decoder);
        assert_eq!(m.name, "spl-token");
        assert_eq!(
            m.version,
            Version {
                major: 1,
                minor: 0,
                patch: 0
            }
        );
        assert_eq!(m.license, "Apache-2.0");
        assert_eq!(m.description, "Decodes SPL Token instructions.");
        assert_eq!(m.content_sha256, parse_sha256_hex(SHA).unwrap());
        assert_eq!(m.program_id, Some(parse_program_id(PID).unwrap()));
    }

    #[test]
    fn program_id_is_optional_at_parse_time() {
        let m = Manifest::parse(without("program_id").as_bytes()).unwrap();
        assert_eq!(m.program_id, None);
    }

    #[test]
    fn rejects_bad_encodings_and_sizes() {
        assert_issue("", "manifest", "is empty");
        let mut bom = UTF8_BOM.to_vec();
        bom.extend_from_slice(render(&fields()).as_bytes());
        assert_issue(&bom, "manifest", "byte-order mark");
        assert_issue(b"{\"name\":\"\xff\"}", "manifest", "not valid UTF-8");
        // UTF-16 is not UTF-8.
        let utf16: Vec<u8> = "{}".encode_utf16().flat_map(|u| u.to_le_bytes()).collect();
        assert_issue(&utf16, "manifest", "");

        // Size boundary: padding with whitespace up to exactly the limit is
        // fine, one byte more is not.
        let base = render(&fields());
        let pad = MAX_MANIFEST_BYTES as usize - base.len();
        let at_max = format!("{base}{}", " ".repeat(pad));
        assert_eq!(at_max.len() as u64, MAX_MANIFEST_BYTES);
        assert!(Manifest::parse(at_max.as_bytes()).is_ok());
        let over = format!("{at_max} ");
        assert_issue(over, "manifest", "more than the limit of 16384");
    }

    #[test]
    fn rejects_json_syntax_leniencies() {
        let good = render(&fields());
        let trailing_comma = good.replacen("\"\n}", "\",\n}", 1);
        let cases: Vec<(String, &str)> = vec![
            ("[]".into(), "expected '{'"),
            ("\"str\"".into(), "expected '{'"),
            ("{".into(), "expected a key"),
            (trailing_comma, "expected a key"),
            (format!("{good}{{}}"), "unexpected data after the object"),
            (format!("{good}x"), "unexpected data after the object"),
            (format!("// c\n{good}"), "expected '{'"),
            (
                good.replacen("\n  \"kind\"", "\n  /* c */ \"kind\"", 1),
                "expected a key",
            ),
            (good.replacen("\"kind\"", "'kind'", 1), "expected a key"),
            (good.replacen("\"kind\"", "kind", 1), "expected a key"),
            (good.replacen("\"kind\":", "\"kind\"=", 1), "expected ':'"),
            (
                good.replacen(",\n  \"kind\"", "\n  \"kind\"", 1),
                "expected ',' or '}'",
            ),
            (
                good.replacen("\"decoder\"", "\"deco", 1).replace("}\n", ""),
                "",
            ),
            (
                good.replacen("\"decoder\"", "\"deco\\u0064er\"", 1),
                "escape not allowed",
            ),
            (
                good.replacen("spl-token\"", "spl-token\\n\"", 1),
                "escape not allowed",
            ),
            (
                good.replacen("spl-token\"", "spl\\/token\"", 1),
                "escape not allowed",
            ),
            (
                good.replacen("spl-token\"", "spl\ttoken\"", 1),
                "control character",
            ),
            (
                good.replacen("1.0.0\"", "1.0.0\u{1}\"", 1),
                "control character",
            ),
            (good.replace("\n", "\u{0B}"), "expected"), // vertical tab is not whitespace
            (good.replace("\n", "\u{A0}"), "expected"), // nor is NBSP
        ];
        for (src, part) in cases {
            let e = issue(&src);
            assert_eq!(e.field, "manifest", "{src:?}: {e}");
            assert!(e.reason.contains(part), "{src:?}: expected {part:?} in {e}");
        }
        // Syntax errors report a byte offset.
        assert_eq!(
            issue("{\"a\" 1}").reason,
            "expected ':' after a key at byte 5"
        );
    }

    #[test]
    fn rejects_non_string_non_integer_values() {
        for (key, v, part) in [
            ("program_id", "null", "found null"),
            ("description", "true", "found a boolean"),
            ("name", "[\"a\"]", "found an array"),
            ("license", "{\"id\":\"MIT\"}", "found an object"),
            ("schema_version", "-1", "found a negative number"),
            ("schema_version", "-0", "found a negative number"),
            ("schema_version", "1.0", "only integers"),
            ("schema_version", "1e0", "only integers"),
            ("schema_version", "1E0", "only integers"),
            ("schema_version", "01", "leading zeros"),
            ("schema_version", "99999999999999999999999", "out of range"),
            ("schema_version", "NaN", "invalid token"),
            ("schema_version", "Infinity", "invalid token"),
        ] {
            assert_issue(with(key, v), &format!("manifest.{key}"), part);
        }
    }

    #[test]
    fn rejects_wrong_scalar_types() {
        assert_issue(
            with("schema_version", "\"1\""),
            "manifest.schema_version",
            "not a string",
        );
        assert_issue(with("name", "7"), "manifest.name", "not a number");
        assert_issue(with("version", "100"), "manifest.version", "not a number");
        assert_issue(
            with("program_id", "0"),
            "manifest.program_id",
            "not a number",
        );
    }

    #[test]
    fn rejects_unsupported_schema_versions() {
        assert_issue(
            with("schema_version", "0"),
            "manifest.schema_version",
            "0 is not supported",
        );
        assert_issue(
            with("schema_version", "2"),
            "manifest.schema_version",
            "2 is not supported",
        );
        assert_issue(
            without("schema_version"),
            "manifest.schema_version",
            "is missing",
        );
    }

    #[test]
    fn rejects_duplicate_keys_even_with_equal_values() {
        let mut f = fields();
        f.push(("name".into(), "\"spl-token\"".into()));
        assert_issue(render(&f), "manifest.name", "duplicate key 'name'");
        let mut f = fields();
        f.push(("extra".into(), "1".into()));
        f.push(("extra".into(), "1".into()));
        assert_issue(render(&f), "manifest", "duplicate key 'extra'");
    }

    #[test]
    fn rejects_unknown_fields_including_status_and_author() {
        assert_issue(
            with("status", "\"verified\""),
            "manifest",
            "status is the directory",
        );
        assert_issue(with("author", "\"alice\""), "manifest", "git author");
        assert_issue(with("Name", "\"x\""), "manifest", "unknown field 'Name'");
        assert_issue(with("name ", "\"x\""), "manifest", "unknown field 'name '");
        assert_issue(with("", "\"x\""), "manifest", "unknown field ''");
        // Unknown keys are echoed safely.
        let e = issue(with("evil\\\\\u{202E}", "1"));
        assert!(!e.reason.contains('\u{202E}'), "{e}");
    }

    #[test]
    fn every_required_field_is_required() {
        for key in MANIFEST_KEYS.iter().filter(|&&k| k != "program_id") {
            assert_issue(without(key), &format!("manifest.{key}"), "is missing");
        }
    }

    #[test]
    fn field_values_are_checked() {
        for (key, v, part) in [
            ("kind", "\"Decoder\"", "decoder or model"),
            ("kind", "\"decoders\"", "decoder or model"),
            ("kind", "\"\"", "decoder or model"),
            ("name", "\"SPL\"", "only a-z"),
            ("version", "\"1.0\"", "not a version"),
            ("version", "\"v1.0.0\"", "not a version"),
            ("license", "\"MIT OR Apache-2.0\"", "only A-Z"),
            ("description", "\"caf\u{e9}\"", "printable ASCII"),
            ("description", "\" padded \"", "start or end with a space"),
            (
                "content_sha256",
                &format!("\"{}\"", SHA.to_uppercase()),
                "lowercase hex",
            ),
            ("program_id", "\"not-base58!\"", "not a program address"),
        ] {
            assert_issue(with(key, v), &format!("manifest.{key}"), part);
        }
    }

    #[test]
    fn first_error_wins_in_a_fixed_order() {
        // Syntax beats everything, in document order.
        assert_issue("{\"status\": 1, \"name\" 2}", "manifest", "expected ':'");
        // Duplicate keys beat schema_version.
        assert_issue(
            "{\"schema_version\": 2, \"schema_version\": 2}",
            "manifest.schema_version",
            "duplicate",
        );
        // schema_version beats unknown keys: a newer manifest says so.
        let mut f = fields();
        f[0].1 = "2".into();
        f.push(("future_field".into(), "1".into()));
        assert_issue(render(&f), "manifest.schema_version", "2 is not supported");
        // Unknown keys beat field values.
        let mut f = fields();
        f[2].1 = "\"BAD\"".into();
        f.push(("status".into(), "\"official\"".into()));
        assert_issue(render(&f), "manifest", "unknown field 'status'");
        // Fields go in canonical order, whatever the order in the file.
        let mut f = fields();
        f[3].1 = "\"1.0\"".into(); // bad version
        f[2].1 = "\"BAD\"".into(); // bad name, comes first canonically
        f.reverse();
        assert_issue(render(&f), "manifest.name", "");
        let mut f = fields();
        f[7].1 = "\"bad\"".into(); // bad program_id
        f[6].1 = "\"bad\"".into(); // bad content_sha256, earlier
        assert_issue(render(&f), "manifest.content_sha256", "");
    }

    #[test]
    fn issue_display_and_error_conversion() {
        let e = issue(without("kind"));
        assert_eq!(e.to_string(), "manifest.kind: is missing");
        let ce = e.into_error("experimental/decoder/x/1.0.0");
        assert_eq!(ce.exit_code(), crate::config::EXIT_ARTIFACT);
        assert_eq!(
            ce.to_string(),
            "artifact experimental/decoder/x/1.0.0: manifest field manifest.kind: is missing"
        );
    }
}
