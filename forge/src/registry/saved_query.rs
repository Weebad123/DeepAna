//! [`SavedQuery`]: a named, parameterized LENS query, its validation, and
//! the typed binding that turns HTTP query-string values into SQL literals.
//!
//! # SQL templates
//!
//! A query's SQL is a template. `{{name}}` marks where a parameter goes:
//!
//! ```text
//! SELECT * FROM transfers WHERE amount >= {{min_amount}} AND mint = {{mint}}
//! ```
//!
//! [`validate_template`] checks a template, and alert SQL goes through the
//! same function (see [`super::alert`]):
//!
//! * it is non-blank, at most [`MAX_SQL_BYTES`] bytes, and has no NUL byte,
//! * it is a single statement: one trailing `;` is allowed, nothing after it,
//! * every string literal, quoted identifier and block comment is closed,
//! * every placeholder is **bare**. It must sit in plain SQL, not inside a
//!   string, quoted identifier or comment, and it must not touch an
//!   identifier character, a quote or a `.`. Binding supplies the quoting,
//!   so `'{{mint}}'` would double-quote and `col{{n}}` would paste into a
//!   different identifier. Both are rejected.
//!
//! # Binding
//!
//! Every query-string value arrives as a string. [`SavedQuery::bind`] parses
//! each against its [`ParamKind`] and renders a literal: integers in
//! canonical decimal (negative ones parenthesized, so `x -{{n}}` cannot turn
//! into a `--` comment), strings single-quoted with `'` doubled. A supplied
//! value is never spliced in raw.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::RegistryError;

/// Largest SQL template accepted, in bytes.
pub const MAX_SQL_BYTES: usize = 64 * 1024;
/// Longest saved-query or alert name.
pub const MAX_NAME_LEN: usize = 64;
/// Most parameters one saved query may declare.
pub const MAX_PARAMS: usize = 32;
/// Longest string value (supplied or default) a parameter accepts, in bytes.
pub const MAX_STRING_PARAM_BYTES: usize = 4 * 1024;

/// A named, parameterized query, as stored in the registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedQuery {
    pub name: String,
    pub sql: String,
    pub params: Vec<ParamSpec>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds.
    pub updated_at: i64,
}

/// One declared parameter of a [`SavedQuery`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParamSpec {
    pub name: String,
    pub kind: ParamKind,
    pub required: bool,
    /// Used when the parameter is not supplied. It must be set exactly when
    /// `required` is false, because an absent optional parameter still has
    /// to render as something. Stored as text and parsed like a supplied
    /// value.
    #[serde(default)]
    pub default: Option<String>,
}

/// The type a parameter's value is parsed as before it is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParamKind {
    String,
    U64,
    I64,
}

/// What a client submits to create a saved query; the registry adds the
/// timestamps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSavedQuery {
    pub name: String,
    pub sql: String,
    #[serde(default)]
    pub params: Vec<ParamSpec>,
}

impl NewSavedQuery {
    /// Check the name, the SQL and the parameter list together.
    pub fn validate(&self) -> Result<(), RegistryError> {
        validate_name("name", &self.name)?;
        let template = validate_template("sql", &self.sql)?;
        validate_params(&self.params, &template.placeholder_names())
    }
}

impl SavedQuery {
    /// Render the SQL with `supplied` bound to the declared parameters.
    ///
    /// Fails with [`RegistryError::Invalid`] when a supplied key is not a
    /// declared parameter (most likely a typo), a required parameter is
    /// missing, or a value does not parse as its kind. The `field` of the
    /// error is the parameter's name.
    pub fn bind(&self, supplied: &HashMap<String, String>) -> Result<String, RegistryError> {
        if let Some(unknown) = supplied
            .keys()
            .filter(|key| !self.params.iter().any(|p| &p.name == *key))
            .min()
        {
            return Err(RegistryError::invalid(unknown.clone(), "is not a parameter of this query"));
        }

        let mut literals = HashMap::with_capacity(self.params.len());
        for param in &self.params {
            let raw = match (supplied.get(&param.name), &param.default) {
                (Some(value), _) => value.as_str(),
                (None, Some(default)) => default.as_str(),
                (None, None) => return Err(RegistryError::invalid(param.name.clone(), "is required")),
            };
            literals.insert(param.name.as_str(), render_literal(&param.name, param.kind, raw)?);
        }

        // Stored SQL was validated on the way in; re-parsing keeps the
        // stored form the single source of truth.
        let template = validate_template("sql", &self.sql)?;
        template.render(|name| {
            literals
                .get(name)
                .cloned()
                .ok_or_else(|| RegistryError::invalid(name.to_owned(), "placeholder has no declared parameter"))
        })
    }
}

// ---------------------------------------------------------------------------
// Names and parameters
// ---------------------------------------------------------------------------

/// A saved-query, alert or parameter name: an ASCII letter, then letters,
/// digits, `_` or `-`, at most [`MAX_NAME_LEN`] in all.
pub(crate) fn validate_name(field: &str, name: &str) -> Result<(), RegistryError> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(RegistryError::invalid(field, format!("must be 1 to {MAX_NAME_LEN} characters")));
    }
    if !name.as_bytes()[0].is_ascii_alphabetic() {
        return Err(RegistryError::invalid(field, "must start with an ASCII letter"));
    }
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(RegistryError::invalid(field, "may contain only ASCII letters, digits, '_' and '-'"));
    }
    Ok(())
}

fn validate_params(params: &[ParamSpec], placeholders: &HashSet<&str>) -> Result<(), RegistryError> {
    if params.len() > MAX_PARAMS {
        return Err(RegistryError::invalid("params", format!("at most {MAX_PARAMS} parameters are allowed")));
    }
    let mut seen = HashSet::new();
    for param in params {
        let field = format!("params.{}", param.name);
        if !is_placeholder_name(&param.name) {
            return Err(RegistryError::invalid(
                field,
                "parameter names are an ASCII letter or '_', then letters, digits or '_'",
            ));
        }
        if !seen.insert(param.name.as_str()) {
            return Err(RegistryError::invalid(field, "is declared twice"));
        }
        match (param.required, &param.default) {
            (true, Some(_)) => return Err(RegistryError::invalid(field, "a required parameter cannot have a default")),
            (false, None) => return Err(RegistryError::invalid(field, "an optional parameter needs a default")),
            (_, Some(default)) => {
                render_literal(&field, param.kind, default)?;
            }
            (true, None) => {}
        }
        if !placeholders.contains(param.name.as_str()) {
            return Err(RegistryError::invalid(field, "is declared but never used in the SQL"));
        }
    }
    let mut undeclared: Vec<_> = placeholders.iter().filter(|name| !seen.contains(*name)).collect();
    undeclared.sort();
    if let Some(name) = undeclared.first() {
        return Err(RegistryError::invalid("sql", format!("placeholder {{{{{name}}}}} has no declared parameter")));
    }
    Ok(())
}

/// `raw` as a single-quoted SQL string literal, every `'` doubled.
///
/// The one escaping routine for string literals: saved-query parameters use
/// it, and so does ORACLE's `profile` module. Callers still validate their
/// input first; this is the second line of defence, not the only one.
pub fn quote_sql_string(raw: &str) -> String {
    format!("'{}'", raw.replace('\'', "''"))
}

/// Parse `raw` as `kind` and render it as a SQL literal.
fn render_literal(field: &str, kind: ParamKind, raw: &str) -> Result<String, RegistryError> {
    match kind {
        ParamKind::String => {
            if raw.len() > MAX_STRING_PARAM_BYTES {
                return Err(RegistryError::invalid(field, format!("is longer than {MAX_STRING_PARAM_BYTES} bytes")));
            }
            if raw.contains('\0') {
                return Err(RegistryError::invalid(field, "contains a NUL byte"));
            }
            Ok(quote_sql_string(raw))
        }
        // Only plain digits: Rust's parsers also take a leading '+'.
        ParamKind::U64 => {
            if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
                return Err(RegistryError::invalid(field, format!("{raw:?} is not an unsigned integer")));
            }
            raw.parse::<u64>()
                .map(|value| value.to_string())
                .map_err(|_| RegistryError::invalid(field, format!("{raw:?} is out of range for u64")))
        }
        ParamKind::I64 => {
            let digits = raw.strip_prefix('-').unwrap_or(raw);
            if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                return Err(RegistryError::invalid(field, format!("{raw:?} is not an integer")));
            }
            let value = raw
                .parse::<i64>()
                .map_err(|_| RegistryError::invalid(field, format!("{raw:?} is out of range for i64")))?;
            Ok(match value {
                // The positive half of i64::MIN does not fit in an i64, and
                // some SQL parsers read `-N` as `-(N)`.
                i64::MIN => "(-9223372036854775807 - 1)".to_owned(),
                v if v < 0 => format!("({v})"),
                v => v.to_string(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

/// A validated SQL template, split at its placeholders.
#[derive(Debug)]
pub(crate) struct Template<'a> {
    segments: Vec<Segment<'a>>,
}

#[derive(Debug, PartialEq, Eq)]
enum Segment<'a> {
    Text(&'a str),
    Placeholder(&'a str),
}

impl<'a> Template<'a> {
    pub(crate) fn placeholder_names(&self) -> HashSet<&'a str> {
        self.segments
            .iter()
            .filter_map(|segment| match segment {
                Segment::Placeholder(name) => Some(*name),
                Segment::Text(_) => None,
            })
            .collect()
    }

    /// Concatenate the text with each placeholder replaced by `value(name)`.
    pub(crate) fn render(
        &self,
        mut value: impl FnMut(&str) -> Result<String, RegistryError>,
    ) -> Result<String, RegistryError> {
        let mut out = String::new();
        for segment in &self.segments {
            match segment {
                Segment::Text(text) => out.push_str(text),
                Segment::Placeholder(name) => out.push_str(&value(name)?),
            }
        }
        Ok(out)
    }
}

fn is_placeholder_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b) if b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && name.len() <= MAX_NAME_LEN
}

/// A byte that may not touch a placeholder (see the module docs).
const fn sticks_to_placeholder(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'\'' | b'"' | b'`' | b'.' | b'$')
}

/// If `{{name}}` starts at `at`, its name and the offset just past `}}`.
fn placeholder_at(sql: &str, at: usize) -> Option<(&str, usize)> {
    let rest = sql.get(at..)?.strip_prefix("{{")?;
    let end = rest.find("}}")?;
    let name = &rest[..end];
    is_placeholder_name(name).then_some((name, at + 2 + end + 2))
}

/// Check `sql` (see the module docs) and split it at its placeholders.
/// Errors are reported against `field`.
pub(crate) fn validate_template<'a>(field: &str, sql: &'a str) -> Result<Template<'a>, RegistryError> {
    if sql.trim().is_empty() {
        return Err(RegistryError::invalid(field, "must not be blank"));
    }
    if sql.len() > MAX_SQL_BYTES {
        return Err(RegistryError::invalid(field, format!("is longer than {MAX_SQL_BYTES} bytes")));
    }
    if sql.contains('\0') {
        return Err(RegistryError::invalid(field, "contains a NUL byte"));
    }

    let bytes = sql.as_bytes();
    let mut segments = Vec::new();
    let mut text_start = 0;
    let mut terminated = false;
    let mut i = 0;
    // Every index below is at an ASCII byte, so slicing `sql` is safe.
    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();
        match b {
            b'\'' | b'"' => {
                let what = if b == b'\'' { "string literal" } else { "quoted identifier" };
                let end = quoted_end(bytes, i, b)
                    .ok_or_else(|| RegistryError::invalid(field, format!("has an unterminated {what}")))?;
                reject_placeholder_in(field, sql, i, end, what)?;
                if terminated {
                    return Err(single_statement(field));
                }
                i = end;
            }
            b'-' if next == Some(b'-') => {
                let end = sql[i..].find('\n').map_or(bytes.len(), |n| i + n);
                reject_placeholder_in(field, sql, i, end, "comment")?;
                i = end;
            }
            b'/' if next == Some(b'*') => {
                let end = sql[i + 2..]
                    .find("*/")
                    .map(|n| i + 2 + n + 2)
                    .ok_or_else(|| RegistryError::invalid(field, "has an unterminated block comment"))?;
                reject_placeholder_in(field, sql, i, end, "comment")?;
                i = end;
            }
            b';' => {
                if terminated {
                    return Err(single_statement(field));
                }
                terminated = true;
                i += 1;
            }
            b'{' if next == Some(b'{') => {
                let Some((name, end)) = placeholder_at(sql, i) else {
                    return Err(RegistryError::invalid(
                        field,
                        format!("has a malformed placeholder at byte {i}; write {{{{name}}}} with no spaces"),
                    ));
                };
                if terminated {
                    return Err(single_statement(field));
                }
                let before = i.checked_sub(1).map(|j| bytes[j]);
                let after = bytes.get(end).copied();
                if before.is_some_and(sticks_to_placeholder) || after.is_some_and(sticks_to_placeholder) {
                    return Err(RegistryError::invalid(
                        field,
                        format!(
                            "placeholder {{{{{name}}}}} must be bare: separate it from identifiers, quotes and '.' \
                             (binding adds any quoting)"
                        ),
                    ));
                }
                if text_start < i {
                    segments.push(Segment::Text(&sql[text_start..i]));
                }
                segments.push(Segment::Placeholder(name));
                text_start = end;
                i = end;
            }
            _ if b.is_ascii_whitespace() => i += 1,
            _ => {
                if terminated {
                    return Err(single_statement(field));
                }
                i += 1;
            }
        }
    }
    if text_start < bytes.len() {
        segments.push(Segment::Text(&sql[text_start..]));
    }
    Ok(Template { segments })
}

fn single_statement(field: &str) -> RegistryError {
    RegistryError::invalid(field, "must be a single statement; nothing may follow a ';'")
}

/// Offset just past the closing `quote` of the quoted run opening at
/// `start`, where a doubled quote is an escaped one.
fn quoted_end(bytes: &[u8], start: usize, quote: u8) -> Option<usize> {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

fn reject_placeholder_in(field: &str, sql: &str, start: usize, end: usize, what: &str) -> Result<(), RegistryError> {
    let region = &sql[start..end];
    let mut from = 0;
    while let Some(n) = region[from..].find("{{") {
        if let Some((name, _)) = placeholder_at(region, from + n) {
            return Err(RegistryError::invalid(
                field,
                format!("placeholder {{{{{name}}}}} is inside a {what}; write it bare, binding adds any quoting"),
            ));
        }
        from += n + 2;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn param(name: &str, kind: ParamKind, default: Option<&str>) -> ParamSpec {
        ParamSpec {
            name: name.to_owned(),
            kind,
            required: default.is_none(),
            default: default.map(str::to_owned),
        }
    }

    fn query(sql: &str, params: Vec<ParamSpec>) -> SavedQuery {
        let new = NewSavedQuery {
            name: "q".to_owned(),
            sql: sql.to_owned(),
            params,
        };
        new.validate().unwrap();
        SavedQuery {
            name: new.name,
            sql: new.sql,
            params: new.params,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn supplied(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    fn field(err: RegistryError) -> String {
        match err {
            RegistryError::Invalid { field, .. } => field,
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn template_splits_at_placeholders() {
        let t = validate_template("sql", "SELECT {{a}} + {{b}}, {{a}}").unwrap();
        assert_eq!(
            t.segments,
            vec![
                Segment::Text("SELECT "),
                Segment::Placeholder("a"),
                Segment::Text(" + "),
                Segment::Placeholder("b"),
                Segment::Text(", "),
                Segment::Placeholder("a"),
            ]
        );
        assert_eq!(t.placeholder_names(), HashSet::from(["a", "b"]));
    }

    #[test]
    fn template_allows_one_trailing_semicolon_and_trailing_comments() {
        for ok in ["SELECT 1;", "SELECT 1 ;  \n", "SELECT 1; -- done", "SELECT 1; /* x */", "SELECT ';' AS s"] {
            assert!(validate_template("sql", ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn template_rejects_multiple_statements() {
        for bad in ["SELECT 1; SELECT 2", "SELECT 1;;", "SELECT 1; 'x'", "SELECT 1; {{a}}", "DELETE FROM t; SELECT 1"] {
            let err = validate_template("sql", bad).unwrap_err();
            assert!(err.to_string().contains("single statement"), "{bad}: {err}");
        }
    }

    #[test]
    fn template_rejects_blank_oversized_and_unterminated_sql() {
        let huge = format!("SELECT '{}'", "x".repeat(MAX_SQL_BYTES));
        for bad in ["", "   \n", "SELECT 'oops", "SELECT \"col", "SELECT 1 /* open", "SELECT '\0'", huge.as_str()] {
            assert_eq!(field(validate_template("sql", bad).unwrap_err()), "sql", "{bad:.40}");
        }
        let at_cap = format!("SELECT '{}'", "x".repeat(MAX_SQL_BYTES - 9));
        assert_eq!(at_cap.len(), MAX_SQL_BYTES);
        assert!(validate_template("sql", &at_cap).is_ok());
    }

    #[test]
    fn template_handles_escaped_quotes() {
        let t = validate_template("sql", "SELECT 'it''s; fine', \"a\"\"b\" FROM t WHERE x = {{x}}").unwrap();
        assert_eq!(t.placeholder_names(), HashSet::from(["x"]));
    }

    #[test]
    fn placeholders_must_be_bare() {
        for bad in [
            "SELECT * FROM t WHERE m = '{{mint}}'",
            "SELECT * FROM t WHERE \"{{col}}\" = 1",
            "SELECT 1 -- {{x}}",
            "SELECT 1 /* {{x}} */",
            "SELECT col{{n}} FROM t",
            "SELECT {{n}}col FROM t",
            "SELECT t.{{n}} FROM t",
            "SELECT 1.{{n}}",
            "SELECT '{{a}}'{{a}}",
        ] {
            assert_eq!(field(validate_template("sql", bad).unwrap_err()), "sql", "{bad}");
        }
        for ok in ["SELECT ({{n}})", "SELECT x-{{n}}", "SELECT {{n}},{{m}}", "IN ({{a}}, {{b}})"] {
            assert!(validate_template("sql", ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn malformed_placeholders_are_rejected() {
        for bad in ["SELECT {{ a }}", "SELECT {{}}", "SELECT {{1a}}", "SELECT {{a", "SELECT {{a-b}}"] {
            assert!(validate_template("sql", bad).is_err(), "{bad}");
        }
        // Text that only looks like braces in strings and comments is left alone.
        assert!(validate_template("sql", "SELECT '{{ not a placeholder' -- {{ }}").is_ok());
    }

    #[test]
    fn names_follow_the_naming_rule() {
        for ok in ["a", "whale-transfers", "Q_1", &"a".repeat(MAX_NAME_LEN)] {
            assert!(validate_name("name", ok).is_ok(), "{ok}");
        }
        for bad in ["", "1abc", "_a", "a b", "a/b", "é", &"a".repeat(MAX_NAME_LEN + 1)] {
            assert!(validate_name("name", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn params_must_match_placeholders_exactly() {
        let new = |sql: &str, params| NewSavedQuery {
            name: "q".into(),
            sql: sql.into(),
            params,
        };
        let err = new("SELECT {{a}}", vec![]).validate().unwrap_err();
        assert!(err.to_string().contains("{{a}} has no declared parameter"), "{err}");

        let err = new("SELECT 1", vec![param("a", ParamKind::U64, None)]).validate().unwrap_err();
        assert_eq!(field(err), "params.a");

        let dup = vec![param("a", ParamKind::U64, None), param("a", ParamKind::I64, None)];
        assert_eq!(field(new("SELECT {{a}}", dup).validate().unwrap_err()), "params.a");
    }

    #[test]
    fn required_and_default_are_mutually_exclusive() {
        let mut p = param("a", ParamKind::U64, None);
        p.required = false;
        let err = NewSavedQuery { name: "q".into(), sql: "SELECT {{a}}".into(), params: vec![p.clone()] }
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("needs a default"), "{err}");

        p.required = true;
        p.default = Some("1".into());
        let err = NewSavedQuery { name: "q".into(), sql: "SELECT {{a}}".into(), params: vec![p] }
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("cannot have a default"), "{err}");
    }

    #[test]
    fn defaults_must_parse_as_their_kind() {
        let p = param("a", ParamKind::U64, Some("-1"));
        let err = NewSavedQuery { name: "q".into(), sql: "SELECT {{a}}".into(), params: vec![p] }
            .validate()
            .unwrap_err();
        assert_eq!(field(err), "params.a");
    }

    #[test]
    fn bind_renders_typed_literals() {
        let q = query(
            "SELECT * FROM t WHERE m = {{mint}} AND a >= {{min}} AND d > {{delta}}",
            vec![
                param("mint", ParamKind::String, None),
                param("min", ParamKind::U64, Some("0")),
                param("delta", ParamKind::I64, Some("-5")),
            ],
        );
        assert_eq!(
            q.bind(&supplied(&[("mint", "So1'; DROP TABLE t; --")])).unwrap(),
            "SELECT * FROM t WHERE m = 'So1''; DROP TABLE t; --' AND a >= 0 AND d > (-5)"
        );
        assert_eq!(
            q.bind(&supplied(&[("mint", "x"), ("min", "007"), ("delta", "3")])).unwrap(),
            "SELECT * FROM t WHERE m = 'x' AND a >= 7 AND d > 3"
        );
    }

    #[test]
    fn bind_reports_missing_unknown_and_malformed_values() {
        let q = query("SELECT {{n}}", vec![param("n", ParamKind::U64, None)]);
        assert_eq!(field(q.bind(&supplied(&[])).unwrap_err()), "n");
        assert_eq!(field(q.bind(&supplied(&[("n", "1"), ("typo", "2")])).unwrap_err()), "typo");
        for bad in ["", "-1", "+1", "1.0", "1e3", " 1", "0x1", "18446744073709551616"] {
            assert_eq!(field(q.bind(&supplied(&[("n", bad)])).unwrap_err()), "n", "{bad:?}");
        }
        assert_eq!(q.bind(&supplied(&[("n", "18446744073709551615")])).unwrap(), "SELECT 18446744073709551615");
    }

    #[test]
    fn negative_integers_cannot_form_a_comment() {
        let q = query("SELECT x -{{n}}", vec![param("n", ParamKind::I64, None)]);
        assert_eq!(q.bind(&supplied(&[("n", "-1")])).unwrap(), "SELECT x -(-1)");
        assert_eq!(
            q.bind(&supplied(&[("n", "-9223372036854775808")])).unwrap(),
            "SELECT x -(-9223372036854775807 - 1)"
        );
        for bad in ["--1", "- 1", "+1", "9223372036854775808"] {
            assert!(q.bind(&supplied(&[("n", bad)])).is_err(), "{bad}");
        }
    }

    #[test]
    fn string_values_are_capped_and_nul_free() {
        let q = query("SELECT {{s}}", vec![param("s", ParamKind::String, None)]);
        assert!(q.bind(&supplied(&[("s", &"x".repeat(MAX_STRING_PARAM_BYTES))])).is_ok());
        assert!(q.bind(&supplied(&[("s", &"x".repeat(MAX_STRING_PARAM_BYTES + 1))])).is_err());
        assert!(q.bind(&supplied(&[("s", "a\0b")])).is_err());
        assert_eq!(q.bind(&supplied(&[("s", "")])).unwrap(), "SELECT ''");
    }

    #[test]
    fn param_kind_serializes_lowercase() {
        let p: ParamSpec = serde_json::from_str(r#"{"name":"n","kind":"u64","required":true}"#).unwrap();
        assert_eq!(p, param("n", ParamKind::U64, None));
        assert_eq!(serde_json::to_string(&ParamKind::I64).unwrap(), r#""i64""#);
    }
}
