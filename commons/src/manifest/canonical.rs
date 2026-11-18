//! Canonical manifest bytes, the artifact id and the content hash.
//!
//! The canonical form is written by hand, never by a serialiser, so that its
//! bytes are fixed by this file alone:
//!
//! ```text
//! {"schema_version":1,"kind":"decoder","name":"spl-token","version":"1.0.0","license":"Apache-2.0","description":"...","content_sha256":"<64 hex>","program_id":"<base58>"}
//! ```
//!
//! - Keys in the order above. `program_id` is left out when absent.
//! - No whitespace outside strings and no trailing newline.
//! - Strings escape only `"` and `\`. A validated manifest holds nothing
//!   else that needs escaping (every string field is printable ASCII).
//! - `content_sha256` is lowercase hex; `program_id` is base58.
//!
//! It is built from the validated [`Manifest`], never copied from the file,
//! so whitespace, key order or line endings in `manifest.json` never change
//! the identity. Status is not part of the manifest, so it is not part of
//! the identity either: promoting an artifact keeps its id.

use std::fmt;

use sha2::{Digest, Sha256};

use super::{ArtifactId, Manifest};

/// SHA-256 of `bytes`.
pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// The content hash: SHA-256 of the raw content bytes, exactly as on disk.
/// Never computed from a re-serialisation of the content.
pub fn content_sha256(content: &[u8]) -> [u8; 32] {
    sha256(content)
}

/// Lowercase hex.
pub fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

impl Manifest {
    /// The canonical bytes of this manifest (see the module docs).
    ///
    /// Defined for manifests that passed [`Manifest::parse`]. The output is
    /// always valid JSON, but for a hand-built manifest with invalid fields
    /// it will not parse back, which [`super::load`] treats as a bug.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.description.len());
        out.extend_from_slice(b"{\"schema_version\":");
        out.extend_from_slice(self.schema_version.to_string().as_bytes());
        push_field(&mut out, "kind", self.kind.as_str());
        push_field(&mut out, "name", &self.name);
        push_field(&mut out, "version", &self.version.to_string());
        push_field(&mut out, "license", &self.license);
        push_field(&mut out, "description", &self.description);
        push_field(&mut out, "content_sha256", &to_hex(&self.content_sha256));
        if let Some(pid) = &self.program_id {
            push_field(&mut out, "program_id", &bs58::encode(pid).into_string());
        }
        out.push(b'}');
        out
    }

    /// The artifact id: SHA-256 of [`Manifest::canonical_bytes`].
    pub fn artifact_id(&self) -> ArtifactId {
        ArtifactId(sha256(&self.canonical_bytes()))
    }
}

/// `,"key":"value"`
fn push_field(out: &mut Vec<u8>, key: &str, value: &str) {
    out.extend_from_slice(b",\"");
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":");
    push_string(out, value);
}

fn push_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for &b in s.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            // Unreachable for a validated manifest; keeps the output valid JSON.
            0x00..=0x1f => out.extend_from_slice(format!("\\u{b:04x}").as_bytes()),
            _ => out.push(b),
        }
    }
    out.push(b'"');
}

impl ArtifactId {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        to_hex(&self.0)
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArtifactId({})", self.to_hex())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::{Kind, Version};
    use super::*;

    const TEST_SHA: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
    const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

    /// The example manifest from the design notes.
    const EXAMPLE_JSON: &str = r#"{
  "schema_version": 1,
  "kind": "decoder",
  "name": "spl-token",
  "version": "1.0.0",
  "license": "Apache-2.0",
  "description": "Decodes SPL Token instructions.",
  "content_sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
}
"#;

    /// Exact canonical bytes of [`EXAMPLE_JSON`]. Changing this breaks every
    /// published artifact id.
    const EXAMPLE_CANONICAL: &str = concat!(
        r#"{"schema_version":1,"kind":"decoder","name":"spl-token","version":"1.0.0","#,
        r#""license":"Apache-2.0","description":"Decodes SPL Token instructions.","#,
        r#""content_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","#,
        r#""program_id":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}"#
    );

    /// `printf '%s' "$EXAMPLE_CANONICAL" | sha256sum`, computed outside Rust.
    const EXAMPLE_ID: &str = "658a6e4a5b05c897eec2c91bddebb5f4db6bd97611e47c7c2eaa7383181b004f";

    fn example() -> Manifest {
        Manifest::parse(EXAMPLE_JSON.as_bytes()).unwrap()
    }

    fn hex32(s: &str) -> [u8; 32] {
        let mut out = [0u8; 32];
        for (i, b) in out.iter_mut().enumerate() {
            *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap();
        }
        out
    }

    #[test]
    fn sha256_known_vectors() {
        assert_eq!(to_hex(&content_sha256(b"test")), TEST_SHA);
        assert_eq!(
            to_hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_is_lowercase_two_digits_per_byte() {
        assert_eq!(to_hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
        assert_eq!(to_hex(&[]), "");
    }

    #[test]
    fn canonical_bytes_golden() {
        let m = example();
        assert_eq!(
            String::from_utf8(m.canonical_bytes()).unwrap(),
            EXAMPLE_CANONICAL
        );
        assert_eq!(m.content_sha256, hex32(TEST_SHA));
        assert_eq!(
            bs58::encode(m.program_id.unwrap()).into_string(),
            TOKEN_PROGRAM
        );
    }

    #[test]
    fn artifact_id_golden() {
        let m = example();
        assert_eq!(m.artifact_id().to_string(), EXAMPLE_ID);
        assert_eq!(m.artifact_id().0, sha256(EXAMPLE_CANONICAL.as_bytes()));
        assert_eq!(m.artifact_id().as_bytes(), &hex32(EXAMPLE_ID));
    }

    #[test]
    fn artifact_id_display_and_debug() {
        let id = example().artifact_id();
        let s = id.to_string();
        assert_eq!(s.len(), 64);
        assert!(
            s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        );
        assert_eq!(format!("{id:?}"), format!("ArtifactId({s})"));
    }

    #[test]
    fn canonical_bytes_are_compact_ascii_and_reparse_to_the_same_manifest() {
        let m = example();
        let c = m.canonical_bytes();
        assert!(
            c.iter().all(|&b| (0x20..=0x7e).contains(&b)),
            "ASCII printable only"
        );
        assert!(!c.ends_with(b"\n"), "no trailing newline");
        assert_eq!(Manifest::parse(&c).unwrap(), m);
        // A fixpoint: canonicalising the canonical form changes nothing.
        assert_eq!(Manifest::parse(&c).unwrap().canonical_bytes(), c);
        // Stable across calls.
        assert_eq!(m.canonical_bytes(), c);
    }

    #[test]
    fn model_without_program_id_omits_the_key() {
        let m = Manifest {
            schema_version: 1,
            kind: Kind::Model,
            name: "daily-volume".into(),
            version: Version {
                major: 2,
                minor: 10,
                patch: 0,
            },
            license: "MIT".into(),
            description: "Daily volume.".into(),
            content_sha256: hex32(TEST_SHA),
            program_id: None,
        };
        let expected = concat!(
            r#"{"schema_version":1,"kind":"model","name":"daily-volume","version":"2.10.0","#,
            r#""license":"MIT","description":"Daily volume.","#,
            r#""content_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"}"#
        );
        assert_eq!(String::from_utf8(m.canonical_bytes()).unwrap(), expected);
        assert_eq!(Manifest::parse(expected.as_bytes()).unwrap(), m);
    }

    #[test]
    fn quotes_and_backslashes_are_escaped_and_round_trip() {
        let mut m = example();
        m.description = r#"Says "hi" via C:\path\."#.into();
        let c = String::from_utf8(m.canonical_bytes()).unwrap();
        assert!(
            c.contains(r#""description":"Says \"hi\" via C:\\path\\.""#),
            "{c}"
        );
        assert_eq!(Manifest::parse(c.as_bytes()).unwrap(), m);
    }

    #[test]
    fn identity_ignores_manifest_formatting_and_key_order() {
        let compact = EXAMPLE_CANONICAL;
        let reordered = r#"{"program_id":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA","content_sha256":"9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08","description":"Decodes SPL Token instructions.","license":"Apache-2.0","version":"1.0.0","name":"spl-token","kind":"decoder","schema_version":1}"#;
        let crlf_tabs = EXAMPLE_JSON.replace('\n', "\r\n").replace("  ", "\t");
        let expected = example().artifact_id();
        for (what, src) in [
            ("pretty", EXAMPLE_JSON),
            ("compact", compact),
            ("reordered", reordered),
            ("crlf+tabs", crlf_tabs.as_str()),
        ] {
            let m = Manifest::parse(src.as_bytes()).unwrap_or_else(|e| panic!("{what}: {e}"));
            assert_eq!(m.artifact_id(), expected, "{what}");
            assert_eq!(m.canonical_bytes(), compact.as_bytes(), "{what}");
        }
    }

    #[test]
    fn identity_changes_when_any_field_changes() {
        let base = example();
        let mut variants: Vec<(&str, Manifest)> = Vec::new();
        let mut v = |what, f: &dyn Fn(&mut Manifest)| {
            let mut m = base.clone();
            f(&mut m);
            variants.push((what, m));
        };
        v("kind", &|m| {
            m.kind = Kind::Model;
            m.program_id = None;
        });
        v("name", &|m| m.name = "spl-token2".into());
        v("major", &|m| m.version.major = 2);
        v("minor", &|m| m.version.minor = 1);
        v("patch", &|m| m.version.patch = 1);
        v("license", &|m| m.license = "MIT".into());
        v("description", &|m| m.description.push('!'));
        v("content", &|m| m.content_sha256[31] ^= 1);
        v("program_id", &|m| m.program_id.as_mut().unwrap()[0] ^= 1);
        v("no program_id", &|m| m.program_id = None);

        let mut ids = vec![base.artifact_id()];
        for (what, m) in &variants {
            let id = m.artifact_id();
            assert!(!ids.contains(&id), "changing {what} must change the id");
            ids.push(id);
        }
        assert_eq!(ids.len(), 11);
    }

    #[test]
    fn version_fields_cannot_collide_through_formatting() {
        // 1.10.0 vs 11.0.0 vs 1.1.00 style ambiguities: the dots are fixed.
        let mut a = example();
        a.version = Version {
            major: 1,
            minor: 10,
            patch: 0,
        };
        let mut b = example();
        b.version = Version {
            major: 11,
            minor: 0,
            patch: 0,
        };
        assert_ne!(a.canonical_bytes(), b.canonical_bytes());
        assert_ne!(a.artifact_id(), b.artifact_id());
    }
}
