//! Strict JSON values, canonical bytes and exact comparison.
//!
//! Every fixture file is read by [`parse_strict`], a small hand-written
//! reader whose accepted subset is exactly this, and nothing a general JSON
//! library happens to tolerate:
//!
//! - UTF-8 without a byte-order mark. Whitespace is space, tab, LF and CR.
//! - `null`, `true`, `false`, integers, strings, arrays and objects.
//! - **Integers only**, in `i128`: an optional `-`, then digits with no
//!   leading zero. Fractions, exponents and `-0` are rejected, so every
//!   number has one spelling and nothing is ever rounded.
//! - **Strings escape only `"` and `\`.** Control characters (C0, DEL, C1)
//!   are rejected, raw or otherwise, so every string has one spelling.
//! - **Duplicate keys are an error at every depth**, even with equal values.
//! - No comments, no trailing commas, nothing after the value but
//!   whitespace. Depth, container size and string length are bounded by
//!   [`ParseLimits`].
//!
//! [`canonical_bytes`] writes a value back with object keys sorted bytewise,
//! no whitespace, integers in plain decimal and only `"` and `\` escaped.
//! For every value [`parse_strict`] accepts, the canonical bytes parse back
//! to the same value, and canonicalising them again changes nothing.
//!
//! For strings without control characters (all a strict value can hold),
//! the canonical form of an array of strings or of any object is
//! byte-for-byte what `serde_json::to_string` writes for the same value
//! without `preserve_order`, which is how PRISM stores its JSON columns.
//!
//! Comparison is exact: [`StrictValue`]'s `==` never coerces (`1` is not
//! `"1"`, `0` is not `false`) and [`first_difference`] names the first path
//! where two values differ.
//!
//! Two additions serve `verify/`. [`canonical_cmp`] (and [`sort_canonical`])
//! is the one written-down order of a multiset of values, bytewise on
//! canonical bytes, which is what digests of unordered rows use. And
//! [`json_quote`] / [`json_ascii`] write JSON that is valid and pure ASCII
//! for **any** string, including ones a fixture could never hold; they are
//! for report output and request bodies, never for fixtures or digests.

use std::collections::BTreeMap;
use std::fmt;

use crate::config::echo;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A JSON value as [`parse_strict`] accepts it. Objects are sorted maps, so
/// two values are equal exactly when their canonical bytes are.
///
/// `Ord` is structural (variant order first, then contents); it exists so
/// that values can be sorted for multiset comparison, not to mean anything.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StrictValue {
    Null,
    Bool(bool),
    Int(i128),
    Str(String),
    Array(Vec<StrictValue>),
    Object(BTreeMap<String, StrictValue>),
}

impl StrictValue {
    /// The JSON type name, for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            StrictValue::Null => "null",
            StrictValue::Bool(_) => "a boolean",
            StrictValue::Int(_) => "an integer",
            StrictValue::Str(_) => "a string",
            StrictValue::Array(_) => "an array",
            StrictValue::Object(_) => "an object",
        }
    }

    /// An array of strings.
    pub fn str_array<S: AsRef<str>>(items: &[S]) -> StrictValue {
        StrictValue::Array(
            items
                .iter()
                .map(|s| StrictValue::Str(s.as_ref().to_string()))
                .collect(),
        )
    }

    /// `Some(s)` as a string, `None` as null.
    pub fn opt_str(s: Option<&str>) -> StrictValue {
        s.map_or(StrictValue::Null, |s| StrictValue::Str(s.to_string()))
    }

    /// `Some(n)` as an integer, `None` as null.
    pub fn opt_int(n: Option<impl Into<i128>>) -> StrictValue {
        n.map_or(StrictValue::Null, |n| StrictValue::Int(n.into()))
    }
}

/// Bounds for [`parse_strict`]. The file size is bounded before parsing, by
/// whoever reads the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseLimits {
    /// Most nested arrays and objects; the outermost container is depth 1.
    pub max_depth: usize,
    /// Most elements in one array, or entries in one object.
    pub max_items: usize,
    /// Longest string or key, in bytes.
    pub max_string_bytes: usize,
}

impl ParseLimits {
    /// Limits that every fixture file kind stays well inside.
    pub const DEFAULT: ParseLimits = ParseLimits {
        max_depth: 32,
        max_items: 10_000,
        max_string_bytes: 64 * 1024,
    };
}

impl Default for ParseLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Why a document is not a strict value. `path` is where in the document
/// the problem is (`""` for the top level, then `.key` and `[index]`),
/// `offset` the byte it was found at. Contributor text is already echoed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonIssue {
    pub path: String,
    pub offset: usize,
    pub reason: String,
}

impl fmt::Display for CanonIssue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let at = if self.path.is_empty() {
            "top level"
        } else {
            &self.path
        };
        write!(f, "{at}: {} (at byte {})", self.reason, self.offset)
    }
}

impl std::error::Error for CanonIssue {}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// `prefix.key`, or `prefix["key"]` when the key is not a plain identifier.
/// The key is echoed.
pub fn join_key(prefix: &str, key: &str) -> String {
    let plain = !key.is_empty()
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    match (prefix.is_empty(), plain) {
        (true, true) => key.to_string(),
        (false, true) => format!("{prefix}.{key}"),
        (_, false) => format!("{prefix}[\"{}\"]", echo(key)),
    }
}

/// `prefix[index]`.
pub fn join_index(prefix: &str, index: usize) -> String {
    format!("{prefix}[{index}]")
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

const UTF8_BOM: &[u8] = b"\xEF\xBB\xBF";

/// Parse `bytes` as one strict JSON value (see the module docs).
pub fn parse_strict(bytes: &[u8], limits: &ParseLimits) -> Result<StrictValue, CanonIssue> {
    let top = |reason: &str| CanonIssue {
        path: String::new(),
        offset: 0,
        reason: reason.to_string(),
    };
    if bytes.starts_with(UTF8_BOM) {
        return Err(top("starts with a UTF-8 byte-order mark"));
    }
    if let Err(e) = std::str::from_utf8(bytes) {
        return Err(CanonIssue {
            path: String::new(),
            offset: e.valid_up_to(),
            reason: "is not valid UTF-8".into(),
        });
    }
    let mut p = Parser {
        b: bytes,
        i: 0,
        limits,
        path: Vec::new(),
    };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != p.b.len() {
        return Err(p.issue("unexpected data after the value"));
    }
    Ok(v)
}

enum Seg {
    Key(String),
    Index(usize),
}

struct Parser<'a> {
    b: &'a [u8],
    i: usize,
    limits: &'a ParseLimits,
    path: Vec<Seg>,
}

impl Parser<'_> {
    fn path_string(&self) -> String {
        let mut s = String::new();
        for seg in &self.path {
            s = match seg {
                Seg::Key(k) => join_key(&s, k),
                Seg::Index(i) => join_index(&s, *i),
            };
        }
        s
    }

    fn issue_at(&self, offset: usize, reason: impl Into<String>) -> CanonIssue {
        CanonIssue {
            path: self.path_string(),
            offset,
            reason: reason.into(),
        }
    }

    fn issue(&self, reason: impl Into<String>) -> CanonIssue {
        self.issue_at(self.i, reason)
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.i += 1;
        }
    }

    fn literal(&mut self, word: &[u8], v: StrictValue) -> Result<StrictValue, CanonIssue> {
        if self.b[self.i..].starts_with(word) {
            self.i += word.len();
            Ok(v)
        } else {
            Err(self.issue("invalid token"))
        }
    }

    /// `depth` is the number of containers already open around this value.
    fn value(&mut self, depth: usize) -> Result<StrictValue, CanonIssue> {
        match self.peek() {
            None => Err(self.issue("unexpected end of input")),
            Some(b'n') => self.literal(b"null", StrictValue::Null),
            Some(b't') => self.literal(b"true", StrictValue::Bool(true)),
            Some(b'f') => self.literal(b"false", StrictValue::Bool(false)),
            Some(b'"') => self.string().map(StrictValue::Str),
            Some(b'-' | b'0'..=b'9') => self.integer(),
            Some(b'[') => self.array(depth + 1),
            Some(b'{') => self.object(depth + 1),
            Some(_) => Err(self.issue("invalid token")),
        }
    }

    fn enter(&self, depth: usize) -> Result<(), CanonIssue> {
        if depth > self.limits.max_depth {
            return Err(self.issue(format!(
                "nested deeper than {} levels",
                self.limits.max_depth
            )));
        }
        Ok(())
    }

    fn array(&mut self, depth: usize) -> Result<StrictValue, CanonIssue> {
        self.enter(depth)?;
        self.i += 1; // '['
        let mut items = Vec::new();
        self.ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(StrictValue::Array(items));
        }
        loop {
            if items.len() == self.limits.max_items {
                return Err(self.issue(format!(
                    "array has more than {} elements",
                    self.limits.max_items
                )));
            }
            self.path.push(Seg::Index(items.len()));
            let v = self.value(depth)?;
            self.path.pop();
            items.push(v);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                    self.ws();
                }
                Some(b']') => {
                    self.i += 1;
                    return Ok(StrictValue::Array(items));
                }
                _ => return Err(self.issue("expected ',' or ']'")),
            }
        }
    }

    fn object(&mut self, depth: usize) -> Result<StrictValue, CanonIssue> {
        self.enter(depth)?;
        self.i += 1; // '{'
        let mut map = BTreeMap::new();
        self.ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(StrictValue::Object(map));
        }
        loop {
            if map.len() == self.limits.max_items {
                return Err(self.issue(format!(
                    "object has more than {} entries",
                    self.limits.max_items
                )));
            }
            if self.peek() != Some(b'"') {
                return Err(self.issue("expected a key in double quotes"));
            }
            let key_at = self.i;
            let key = self.string()?;
            if map.contains_key(&key) {
                return Err(self.issue_at(key_at, format!("duplicate key '{}'", echo(&key))));
            }
            self.ws();
            if self.peek() != Some(b':') {
                return Err(self.issue("expected ':' after a key"));
            }
            self.i += 1;
            self.ws();
            self.path.push(Seg::Key(key.clone()));
            let v = self.value(depth)?;
            self.path.pop();
            map.insert(key, v);
            self.ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                    self.ws();
                }
                Some(b'}') => {
                    self.i += 1;
                    return Ok(StrictValue::Object(map));
                }
                _ => return Err(self.issue("expected ',' or '}'")),
            }
        }
    }

    /// `-`? digits, no leading zero, no `-0`, no fraction or exponent,
    /// fits `i128`.
    fn integer(&mut self) -> Result<StrictValue, CanonIssue> {
        let start = self.i;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.i += 1;
        }
        let digits_at = self.i;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.i += 1;
        }
        let digits = &self.b[digits_at..self.i];
        let bad = |why: &str| self.issue_at(start, why.to_string());
        if digits.is_empty() {
            return Err(bad("expected digits after '-'"));
        }
        if digits.len() > 1 && digits[0] == b'0' {
            return Err(bad("leading zeros are not allowed"));
        }
        if negative && digits == b"0" {
            return Err(bad("-0 is not allowed; write 0"));
        }
        if matches!(self.peek(), Some(b'.' | b'e' | b'E')) {
            return Err(bad("only integers are allowed (no fraction or exponent)"));
        }
        // ASCII only, so valid UTF-8.
        let text = std::str::from_utf8(&self.b[start..self.i]).expect("ASCII");
        text.parse::<i128>()
            .map(StrictValue::Int)
            .map_err(|_| bad("integer does not fit in 128 bits"))
    }

    /// A string starting at the current `"`. Escapes: `\"` and `\\` only.
    fn string(&mut self) -> Result<String, CanonIssue> {
        let start = self.i;
        self.i += 1; // opening quote
        let mut out = Vec::new();
        loop {
            match self.peek() {
                None => return Err(self.issue_at(start, "unterminated string")),
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
                            return Err(self.issue(
                                "escape not allowed (only \\\" and \\\\ may be escaped)",
                            ));
                        }
                    }
                }
                Some(c) if c < 0x20 => {
                    return Err(self.issue("control character inside a string"));
                }
                Some(c) => {
                    out.push(c);
                    self.i += 1;
                }
            }
            if out.len() > self.limits.max_string_bytes {
                return Err(self.issue_at(
                    start,
                    format!(
                        "string is longer than {} bytes",
                        self.limits.max_string_bytes
                    ),
                ));
            }
        }
        // The input is valid UTF-8 and was only split at ASCII bytes.
        let s = String::from_utf8(out).map_err(|_| self.issue_at(start, "invalid UTF-8"))?;
        if let Some(c) = s.chars().find(|c| c.is_control()) {
            return Err(self.issue_at(
                start,
                format!("control character U+{:04X} inside a string", c as u32),
            ));
        }
        Ok(s)
    }
}

// ---------------------------------------------------------------------------
// Canonical bytes
// ---------------------------------------------------------------------------

/// Compact JSON: object keys sorted bytewise, no whitespace, integers in
/// plain decimal, strings escaping only `"` and `\`. No trailing newline.
///
/// Defined for values without control characters in their strings, which
/// is every value [`parse_strict`] returns.
pub fn canonical_bytes(v: &StrictValue) -> Vec<u8> {
    let mut out = Vec::new();
    write_canonical(&mut out, v);
    out
}

fn write_canonical(out: &mut Vec<u8>, v: &StrictValue) {
    match v {
        StrictValue::Null => out.extend_from_slice(b"null"),
        StrictValue::Bool(true) => out.extend_from_slice(b"true"),
        StrictValue::Bool(false) => out.extend_from_slice(b"false"),
        StrictValue::Int(n) => out.extend_from_slice(n.to_string().as_bytes()),
        StrictValue::Str(s) => write_string(out, s),
        StrictValue::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(out, item);
            }
            out.push(b']');
        }
        StrictValue::Object(map) => {
            // BTreeMap<String, _> iterates in bytewise key order.
            out.push(b'{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_string(out, k);
                out.push(b':');
                write_canonical(out, item);
            }
            out.push(b'}');
        }
    }
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    out.push(b'"');
    for &b in s.as_bytes() {
        match b {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            _ => out.push(b),
        }
    }
    out.push(b'"');
}

/// [`canonical_bytes`] as a `String` (it is always valid UTF-8).
pub fn canonical_string(v: &StrictValue) -> String {
    String::from_utf8(canonical_bytes(v)).expect("canonical bytes are UTF-8")
}

// ---------------------------------------------------------------------------
// Canonical order
// ---------------------------------------------------------------------------

/// The order of values in a multiset whenever its order is written down (a
/// digest, a report): bytewise by [`canonical_bytes`].
///
/// Unlike the structural `Ord` of [`StrictValue`], this order is defined by
/// the bytes alone, so anyone holding the canonical forms can reproduce it
/// (`LC_ALL=C sort` over one canonical value per line gives the same order
/// for values whose strings hold no newline). Equal values compare equal.
///
/// [`canonical_bytes`] is injective for every value, including strings
/// with control characters (only `"` and `\` are escaped, and each has one
/// escape), so two different values never tie.
pub fn canonical_cmp(a: &StrictValue, b: &StrictValue) -> std::cmp::Ordering {
    canonical_bytes(a).cmp(&canonical_bytes(b))
}

/// Sort `values` into [`canonical_cmp`] order. Each value is serialised
/// once.
pub fn sort_canonical(values: &mut [StrictValue]) {
    values.sort_by_cached_key(canonical_bytes);
}

// ---------------------------------------------------------------------------
// ASCII JSON
// ---------------------------------------------------------------------------

/// `s` as a JSON string literal, for text that may hold anything: report
/// output and request bodies, not fixtures.
///
/// The result is printable ASCII and valid JSON for every input: `"` and
/// `\` are escaped as `\"` and `\\`, and every character outside
/// U+0020..=U+007E as `\uXXXX` (a surrogate pair above U+FFFF). A JSON
/// reader gives back exactly `s`; a terminal shows nothing but ASCII.
pub fn json_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    push_json_quoted(&mut out, s);
    out
}

fn push_json_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            ' '..='~' => out.push(c),
            _ => {
                let mut units = [0u16; 2];
                for u in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{u:04x}"));
                }
            }
        }
    }
    out.push('"');
}

/// `v` as compact JSON in the canonical layout (sorted keys, no
/// whitespace), with every string written by [`json_quote`]. Equal to
/// [`canonical_string`] for values whose strings are printable ASCII;
/// unlike it, valid JSON and pure ASCII for every value.
pub fn json_ascii(v: &StrictValue) -> String {
    let mut out = String::new();
    write_json_ascii(&mut out, v);
    out
}

fn write_json_ascii(out: &mut String, v: &StrictValue) {
    match v {
        StrictValue::Null => out.push_str("null"),
        StrictValue::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        StrictValue::Int(n) => out.push_str(&n.to_string()),
        StrictValue::Str(s) => push_json_quoted(out, s),
        StrictValue::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_ascii(out, item);
            }
            out.push(']');
        }
        StrictValue::Object(map) => {
            out.push('{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_json_quoted(out, k);
                out.push(':');
                write_json_ascii(out, item);
            }
            out.push('}');
        }
    }
}

// ---------------------------------------------------------------------------
// Exact comparison
// ---------------------------------------------------------------------------

/// Where two values first differ. `expected` and `actual` are the canonical
/// forms of the two sub-values (echoed, so possibly truncated).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub path: String,
    pub expected: String,
    pub actual: String,
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let at = if self.path.is_empty() {
            "top level"
        } else {
            &self.path
        };
        write!(
            f,
            "{at}: expected {} but got {}",
            self.expected, self.actual
        )
    }
}

/// `None` exactly when `expected == actual`. Otherwise the first difference
/// in a fixed order: array elements by index (then a length difference),
/// object keys in bytewise order. No coercion of any kind.
pub fn first_difference(expected: &StrictValue, actual: &StrictValue) -> Option<Difference> {
    diff_at("", expected, actual)
}

fn diff_at(path: &str, e: &StrictValue, a: &StrictValue) -> Option<Difference> {
    let here = |e: &StrictValue, a: &StrictValue| Difference {
        path: path.to_string(),
        expected: echo(&canonical_string(e)),
        actual: echo(&canonical_string(a)),
    };
    match (e, a) {
        (StrictValue::Array(xs), StrictValue::Array(ys)) => {
            for (i, (x, y)) in xs.iter().zip(ys).enumerate() {
                if let Some(d) = diff_at(&join_index(path, i), x, y) {
                    return Some(d);
                }
            }
            (xs.len() != ys.len()).then(|| Difference {
                path: path.to_string(),
                expected: format!("an array of {} elements", xs.len()),
                actual: format!("{} elements", ys.len()),
            })
        }
        (StrictValue::Object(xm), StrictValue::Object(ym)) => {
            let mut keys: Vec<&String> = xm.keys().chain(ym.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                let p = join_key(path, k);
                match (xm.get(k), ym.get(k)) {
                    (Some(x), Some(y)) => {
                        if let Some(d) = diff_at(&p, x, y) {
                            return Some(d);
                        }
                    }
                    (Some(x), None) => {
                        return Some(Difference {
                            path: p,
                            expected: echo(&canonical_string(x)),
                            actual: "no such key".into(),
                        });
                    }
                    (None, Some(y)) => {
                        return Some(Difference {
                            path: p,
                            expected: "no such key".into(),
                            actual: echo(&canonical_string(y)),
                        });
                    }
                    (None, None) => unreachable!("key came from one of the maps"),
                }
            }
            None
        }
        _ => (e != a).then(|| here(e, a)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<StrictValue, CanonIssue> {
        parse_strict(s.as_bytes(), &ParseLimits::DEFAULT)
    }

    fn ok(s: &str) -> StrictValue {
        parse(s).unwrap_or_else(|e| panic!("{s:?}: {e}"))
    }

    #[track_caller]
    fn bad(s: &str, part: &str) -> CanonIssue {
        let e = parse(s).expect_err(s);
        assert!(e.reason.contains(part), "{s:?}: expected {part:?} in {e}");
        e
    }

    fn obj(entries: &[(&str, StrictValue)]) -> StrictValue {
        StrictValue::Object(
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
        )
    }

    #[test]
    fn parses_every_kind_of_value() {
        assert_eq!(ok("null"), StrictValue::Null);
        assert_eq!(ok(" true "), StrictValue::Bool(true));
        assert_eq!(ok("\tfalse\r\n"), StrictValue::Bool(false));
        assert_eq!(ok("0"), StrictValue::Int(0));
        assert_eq!(ok("-17"), StrictValue::Int(-17));
        assert_eq!(ok("\"a\\\"b\\\\c é 🙂\""), StrictValue::Str("a\"b\\c é 🙂".into()));
        assert_eq!(ok("[]"), StrictValue::Array(vec![]));
        assert_eq!(ok("{}"), StrictValue::Object(BTreeMap::new()));
        assert_eq!(
            ok(r#"{"b":[1,{"c":null}],"a":"x"}"#),
            obj(&[
                ("a", StrictValue::Str("x".into())),
                (
                    "b",
                    StrictValue::Array(vec![
                        StrictValue::Int(1),
                        obj(&[("c", StrictValue::Null)])
                    ])
                ),
            ])
        );
    }

    #[test]
    fn integers_cover_i128_exactly() {
        let max = i128::MAX.to_string();
        let min = i128::MIN.to_string();
        assert_eq!(ok(&max), StrictValue::Int(i128::MAX));
        assert_eq!(ok(&min), StrictValue::Int(i128::MIN));
        assert_eq!(ok(&u64::MAX.to_string()), StrictValue::Int(u64::MAX as i128));
        bad("170141183460469231731687303715884105728", "128 bits");
        bad("-170141183460469231731687303715884105729", "128 bits");
    }

    #[test]
    fn rejects_every_number_that_is_not_one_plain_integer() {
        for (s, part) in [
            ("01", "leading zeros"),
            ("-01", "leading zeros"),
            ("00", "leading zeros"),
            ("-0", "-0 is not allowed"),
            ("1.0", "only integers"),
            ("1.5", "only integers"),
            ("1e3", "only integers"),
            ("1E3", "only integers"),
            ("-", "expected digits"),
            ("+1", "invalid token"),
            (".5", "invalid token"),
            ("NaN", "invalid token"),
            ("Infinity", "invalid token"),
            ("-Infinity", "expected digits"),
            ("0x10", "unexpected data"),
        ] {
            bad(s, part);
        }
    }

    #[test]
    fn strings_have_exactly_one_spelling() {
        for (s, part) in [
            (r#""\n""#, "escape not allowed"),
            (r#""\u0041""#, "escape not allowed"),
            (r#""\/""#, "escape not allowed"),
            (r#""\t""#, "escape not allowed"),
            ("\"a\tb\"", "control character"),
            ("\"a\nb\"", "control character"),
            ("\"a\u{7f}b\"", "U+007F"),
            ("\"a\u{85}b\"", "U+0085"),
            ("\"abc", "unterminated"),
            ("'abc'", "invalid token"),
        ] {
            bad(s, part);
        }
    }

    #[test]
    fn rejects_syntax_leniencies() {
        for (s, part) in [
            ("", "unexpected end"),
            ("   ", "unexpected end"),
            ("[1,]", "invalid token"),
            ("{\"a\":1,}", "expected a key"),
            ("[1 2]", "expected ',' or ']'"),
            ("{\"a\" 1}", "expected ':'"),
            ("{\"a\":1 \"b\":2}", "expected ',' or '}'"),
            ("{a:1}", "expected a key"),
            ("[1]]", "unexpected data"),
            ("1 2", "unexpected data"),
            ("// c\n1", "invalid token"),
            ("[1 /* c */]", "expected ',' or ']'"),
            ("nul", "invalid token"),
            ("True", "invalid token"),
            ("\u{0B}1", "invalid token"), // vertical tab is not whitespace
            ("\u{A0}1", "invalid token"), // nor is NBSP
        ] {
            bad(s, part);
        }
        let e = parse_strict(b"\xEF\xBB\xBF1", &ParseLimits::DEFAULT).unwrap_err();
        assert!(e.reason.contains("byte-order mark"), "{e}");
        let e = parse_strict(b"\"\xff\"", &ParseLimits::DEFAULT).unwrap_err();
        assert!(e.reason.contains("UTF-8"), "{e}");
        assert_eq!(e.offset, 1);
    }

    #[test]
    fn duplicate_keys_are_an_error_at_every_depth() {
        let e = bad(r#"{"a":1,"a":1}"#, "duplicate key 'a'");
        assert_eq!(e.path, "");
        assert_eq!(e.offset, 7);
        let e = bad(r#"{"x":{"y":[{"k":1,"j":2,"k":1}]}}"#, "duplicate key 'k'");
        assert_eq!(e.path, "x.y[0]");
        let e = bad(r#"[[[{"":0,"":0}]]]"#, "duplicate key ''");
        assert_eq!(e.path, "[0][0][0]");
        // Different values, same key.
        bad(r#"{"a":1,"b":{"c":true,"c":false}}"#, "duplicate key 'c'");
        // Keys that only look alike are different keys.
        assert!(parse(r#"{"a":1,"A":1,"a ":1}"#).is_ok());
    }

    #[test]
    fn issues_name_the_path_and_echo_keys() {
        let e = bad(r#"{"slots":[{"slot":1},{"slot":1.5}]}"#, "only integers");
        assert_eq!(e.path, "slots[1].slot");
        assert_eq!(e.to_string(), "slots[1].slot: only integers are allowed (no fraction or exponent) (at byte 29)");
        let e = bad("{\"we\u{202E}ird key\":[1.0]}", "only integers");
        assert!(!e.path.contains('\u{202E}'), "{}", e.path);
        assert!(e.path.starts_with("[\"we\\u{202E}ird key\"]"), "{}", e.path);
    }

    #[test]
    fn limits_are_exact() {
        let limits = ParseLimits {
            max_depth: 3,
            max_items: 2,
            max_string_bytes: 4,
        };
        let p = |s: &str| parse_strict(s.as_bytes(), &limits);
        assert!(p("[[[1]]]").is_ok());
        assert!(p("[[[[1]]]]").unwrap_err().reason.contains("deeper than 3"));
        assert!(p("{\"a\":{\"b\":{}}}").is_ok());
        assert!(p("{\"a\":{\"b\":{\"c\":{}}}}").is_err());
        assert!(p("[1,2]").is_ok());
        assert!(p("[1,2,3]").unwrap_err().reason.contains("more than 2 elements"));
        assert!(p("{\"a\":1,\"b\":2,\"c\":3}").unwrap_err().reason.contains("more than 2 entries"));
        assert!(p("\"abcd\"").is_ok());
        assert!(p("\"abcde\"").unwrap_err().reason.contains("longer than 4 bytes"));
        // Bytes, not chars, and escapes count once.
        assert!(p("\"éé\"").is_ok());
        assert!(p("\"ééé\"").is_err());
        assert!(p("\"\\\\\\\\\\\\\\\\\"").is_ok());
        // Keys obey the same limit.
        assert!(p("{\"abcde\":1}").is_err());
        // Deep nesting is rejected, not a stack overflow.
        let deep = format!("{}{}", "[".repeat(100_000), "]".repeat(100_000));
        assert!(parse(&deep).unwrap_err().reason.contains("deeper than"));
    }

    #[test]
    fn canonical_bytes_golden() {
        let v = ok("{ \"b\" : [ 1 , -2 , true , null ] ,\n \"a\" : \"q\\\"\\\\\" , \"B\": {} }");
        assert_eq!(
            canonical_string(&v),
            r#"{"B":{},"a":"q\"\\","b":[1,-2,true,null]}"#
        );
        assert_eq!(canonical_string(&StrictValue::Int(i128::MIN)), i128::MIN.to_string());
        assert_eq!(canonical_string(&ok("\"é🙂\"")), "\"é🙂\"");
    }

    #[test]
    fn keys_sort_bytewise_not_by_locale_or_char() {
        let v = ok(r#"{"b":1,"a":1,"B":1,"_":1,"a-b":1,"a.b":1,"é":1,"z":1,"10":1,"9":1}"#);
        assert_eq!(
            canonical_string(&v),
            r#"{"10":1,"9":1,"B":1,"_":1,"a":1,"a-b":1,"a.b":1,"b":1,"z":1,"é":1}"#
        );
    }

    #[test]
    fn canonical_form_is_a_fixpoint_and_reparses_to_the_same_value() {
        for src in [
            "null",
            "[]",
            "{}",
            "[[],{},[{}]]",
            r#"{"z":[1,2,{"y":"x","x":"y"}],"a":-170141183460469231731687303715884105728}"#,
            "\"\\\\\\\"\"",
            r#"{"accounts":["a","b"],"n":0}"#,
        ] {
            let v = ok(src);
            let c = canonical_bytes(&v);
            let back = parse_strict(&c, &ParseLimits::DEFAULT).unwrap();
            assert_eq!(back, v, "{src}");
            assert_eq!(canonical_bytes(&back), c, "{src}");
            assert!(!c.contains(&b' ') || src.contains(' '), "no added whitespace");
        }
    }

    #[test]
    fn formatting_and_key_order_never_change_the_canonical_form() {
        let a = ok("{\"x\":1,\"y\":[true,false]}");
        let b = ok("{\n  \"y\": [ true,\tfalse ],\r\n  \"x\": 1\n}\n");
        assert_eq!(a, b);
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
    }

    #[test]
    fn canonical_matches_serde_json_for_prisms_json_columns() {
        // PRISM writes `accounts`, `log_messages` and `decoded_data` with
        // `serde_json::to_string`; serde_json is not a dependency here, so
        // these are its outputs for the same values, written down.
        let accounts = StrictValue::str_array(&["Abc", "q\"uote", "back\\slash", "ü/ber"]);
        assert_eq!(
            canonical_string(&accounts),
            r#"["Abc","q\"uote","back\\slash","ü/ber"]"#
        );
        let decoded = ok(r#"{"mint":"M","amount":1000,"decimals":6,"extra":{"b":null,"a":[]}}"#);
        assert_eq!(
            canonical_string(&decoded),
            r#"{"amount":1000,"decimals":6,"extra":{"a":[],"b":null},"mint":"M"}"#
        );
    }

    #[test]
    fn comparison_is_exact_and_never_coerces() {
        let pairs = [
            ("1", "\"1\""),
            ("0", "false"),
            ("1", "true"),
            ("null", "0"),
            ("null", "\"\""),
            ("[]", "{}"),
            ("[1]", "1"),
            ("\"a\"", "\"A\""),
            ("\"a\"", "\"a \""),
            ("10", "100"),
            ("[1,2]", "[2,1]"),
            ("{\"a\":1}", "{\"a\":1,\"b\":null}"),
        ];
        for (x, y) in pairs {
            let (a, b) = (ok(x), ok(y));
            assert_ne!(a, b, "{x} vs {y}");
            assert!(first_difference(&a, &b).is_some(), "{x} vs {y}");
            assert!(first_difference(&b, &a).is_some(), "{y} vs {x}");
        }
        let v = ok(r#"{"a":[1,{"b":"c"}]}"#);
        assert_eq!(first_difference(&v, &v.clone()), None);
    }

    #[test]
    fn first_difference_names_the_first_path_in_a_fixed_order() {
        let e = ok(r#"{"a":[1,2,3],"b":{"x":1,"y":2},"c":1}"#);
        let d = first_difference(&e, &ok(r#"{"a":[1,9,8],"b":{"x":0},"c":2}"#)).unwrap();
        assert_eq!(d.path, "a[1]");
        assert_eq!((d.expected.as_str(), d.actual.as_str()), ("2", "9"));
        let d = first_difference(&e, &ok(r#"{"a":[1,2,3],"b":{"x":1},"c":1}"#)).unwrap();
        assert_eq!(d.path, "b.y");
        assert_eq!(d.actual, "no such key");
        let d = first_difference(&e, &ok(r#"{"a":[1,2],"b":{"x":1,"y":2},"c":1}"#)).unwrap();
        assert_eq!(d.path, "a");
        assert_eq!(d.to_string(), "a: expected an array of 3 elements but got 2 elements");
        let d = first_difference(&e, &ok(r#"{"a":[1,2,3],"b":{"x":1,"y":2},"c":1,"d":0}"#)).unwrap();
        assert_eq!((d.path.as_str(), d.expected.as_str()), ("d", "no such key"));
        let d = first_difference(&ok("1"), &ok("\"1\"")).unwrap();
        assert_eq!(d.to_string(), "top level: expected 1 but got \"1\"");
    }

    #[test]
    fn canonical_order_is_bytewise_on_canonical_bytes() {
        let mut v: Vec<StrictValue> = ["[2]", "\"b\"", "10", "9", "null", "[10]", "\"B\"", "{}", "true"]
            .iter()
            .map(|s| ok(s))
            .collect();
        sort_canonical(&mut v);
        let shown: Vec<String> = v.iter().map(canonical_string).collect();
        // '"' 0x22 < '1' < '9' < '[' 0x5b < 'n' < 't' < '{'.
        assert_eq!(shown, ["\"B\"", "\"b\"", "10", "9", "[10]", "[2]", "null", "true", "{}"]);
        // The structural Ord would put null first: the two orders differ on
        // purpose, and only this one is written down.
        assert_eq!(canonical_cmp(&ok("10"), &ok("9")), std::cmp::Ordering::Less);
        assert!(StrictValue::Int(10) > StrictValue::Int(9));
        // Different values never tie, even with characters a fixture cannot
        // hold; equal values do.
        let raw = |s: &str| StrictValue::Str(s.to_string());
        assert_ne!(canonical_cmp(&raw("a\nb"), &raw("a\\nb")), std::cmp::Ordering::Equal);
        assert_ne!(canonical_cmp(&raw("\""), &raw("\\\"")), std::cmp::Ordering::Equal);
        assert_eq!(canonical_cmp(&ok("{\"a\":[1]}"), &ok("{ \"a\" : [1] }")), std::cmp::Ordering::Equal);
    }

    #[test]
    fn json_quote_is_ascii_valid_json_and_reads_back_exactly() {
        for s in [
            "",
            "plain",
            "q\"uote and back\\slash",
            "line\nfeed\r\ttab\u{0}nul",
            "\u{7f}\u{85}\u{a0}",
            "é ü 漢字",
            "bidi \u{202E}rev\u{200B}",
            "astral 🙂 \u{10FFFF}",
            "\u{1b}[31mred",
        ] {
            let q = json_quote(s);
            assert!(q.bytes().all(|b| (0x20..=0x7e).contains(&b)), "{q}");
            let back: String = serde_json::from_str(&q).unwrap_or_else(|e| panic!("{q}: {e}"));
            assert_eq!(back, s);
        }
        assert_eq!(json_quote("a\"b\\c\n"), r#""a\"b\\c\u000a""#);
        assert_eq!(json_quote("🙂"), r#""\ud83d\ude42""#, "a surrogate pair");
    }

    #[test]
    fn json_ascii_is_canonical_for_ascii_and_valid_for_anything() {
        let v = ok(r#"{"b":[1,-2,true,null],"a":"q\"\\","B":{}}"#);
        assert_eq!(json_ascii(&v), canonical_string(&v));
        let odd = StrictValue::Object(
            [("k\n".to_string(), StrictValue::str_array(&["é", "\u{1}"]))]
                .into_iter()
                .collect(),
        );
        let text = json_ascii(&odd);
        assert!(text.is_ascii());
        let back: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(back, serde_json::json!({"k\n": ["é", "\u{1}"]}));
    }
}
