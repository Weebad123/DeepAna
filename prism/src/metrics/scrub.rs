//! Secret registry and error-text scrubbing.
//!
//! Every piece of free text PRISM exposes (the halt reason, the last error)
//! goes through [`clean`] first. Error messages from the RPC client can carry
//! the endpoint URL, and the endpoint URL can carry an API key, so the text is
//! treated as hostile until it has been scrubbed.
//!
//! Two layers:
//!
//! 1. **A registry** of known secrets. Call [`register_secret`] at startup,
//!    before the first request, with every credential PRISM was configured
//!    with (RPC URL, API key, ...). Each registered value, and its
//!    percent-encoded form, is replaced wherever it appears.
//! 2. **Patterns** for secrets nobody registered: any URL, `name=value` /
//!    `name: value` for well-known credential names, and `Bearer <token>`.
//!
//! `clean` steps, in order:
//!
//! 1. cut to [`MAX_INPUT_BYTES`] at a character boundary, before any scanning;
//! 2. replace control characters with a space;
//! 3. replace registered secrets (raw and percent-encoded) with `<redacted>`;
//! 4. replace URLs (`scheme://` up to whitespace or one of `"'<>)`) with `<url>`;
//! 5. replace credential `name=value` / `name: value` / `"name": "value"` /
//!    `Bearer <token>` with `name=<redacted>`;
//! 6. cap at [`MAX_OUTPUT_CHARS`] characters, `...` included.
//!
//! Control characters are replaced *before* the patterns run, not after: a
//! newline or NUL next to the separator (`token:\nabc`, `token\0=abc`) would
//! otherwise stop the credential pattern from matching and the value would
//! survive. Replacing them one-for-one with a space keeps every other step's
//! result the same.

use std::sync::RwLock;

/// Input is cut to this many bytes before scanning, so a huge error body
/// cannot make scrubbing expensive.
pub const MAX_INPUT_BYTES: usize = 64 * 1024;

/// Output length cap, in characters, truncation marker included.
pub const MAX_OUTPUT_CHARS: usize = 200;

/// Registered secrets shorter than this are ignored: redacting every `a` or
/// `42` in an error message would destroy it and protect nothing.
pub const MIN_SECRET_LEN: usize = 4;

const REDACTED: &str = "<redacted>";
const URL_MARKER: &str = "<url>";
const TRUNCATION_MARKER: &str = "...";

/// Credential names matched by step 4, lowercase. Longer names first so that
/// `access_token` is preferred over a shorter overlap.
const SECRET_NAMES: [&str; 12] = [
    "authorization",
    "access_token",
    "signature",
    "password",
    "api-key",
    "api_key",
    "apikey",
    "secret",
    "token",
    "auth",
    "key",
    "sig",
];

/// Every pattern a registered secret is replaced by: the raw value and its
/// percent-encoded forms (upper- and lowercase hex).
static SECRETS: RwLock<Vec<String>> = RwLock::new(Vec::new());

/// Register a secret to be redacted from every scrubbed text.
///
/// Call at startup, before any request is made. Values shorter than
/// [`MIN_SECRET_LEN`] are ignored; registering the same value twice is
/// harmless.
pub fn register_secret(secret: &str) {
    let secret = secret.trim();
    if secret.len() < MIN_SECRET_LEN {
        return;
    }
    let mut forms = vec![secret.to_owned()];
    let upper = percent_encode(secret, false);
    let lower = percent_encode(secret, true);
    forms.push(upper);
    forms.push(lower);

    let mut registry = SECRETS.write().unwrap_or_else(|e| e.into_inner());
    for form in forms {
        if !registry.contains(&form) {
            registry.push(form);
        }
    }
    // Longest first: a secret that contains another must be replaced whole.
    registry.sort_by_key(|s| std::cmp::Reverse(s.len()));
}

/// Scrub `input` for exposure: secrets removed, control characters replaced,
/// length capped. See the module docs for the exact steps.
pub fn clean(input: &str) -> String {
    let cut = &input[..input.floor_char_boundary(MAX_INPUT_BYTES)];
    let text: String = cut
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    let text = redact_registered(&text);
    let text = redact_urls(&text);
    let text = redact_named(&text);
    cap(text)
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

fn redact_registered(text: &str) -> String {
    let registry = SECRETS.read().unwrap_or_else(|e| e.into_inner());
    let mut out = text.to_owned();
    for secret in registry.iter() {
        if out.contains(secret.as_str()) {
            out = out.replace(secret.as_str(), REDACTED);
        }
    }
    out
}

/// Replace `scheme://...` with `<url>`. A scheme is an ASCII letter followed
/// by letters, digits, `+`, `-` or `.`.
fn redact_urls(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut search = 0;
    while let Some(rel) = text[search..].find("://") {
        let sep = search + rel;
        // Walk back over the scheme.
        let mut start = sep;
        while start > 0 && is_scheme_byte(bytes[start - 1]) {
            start -= 1;
        }
        // The scheme must start with a letter; skip leading digits etc.
        while start < sep && !bytes[start].is_ascii_alphabetic() {
            start += 1;
        }
        if start == sep || start < copied {
            search = sep + 3;
            continue;
        }
        let mut end = sep + 3;
        while end < bytes.len() && !is_url_end(bytes[end]) {
            end += 1;
        }
        out.push_str(&text[copied..start]);
        out.push_str(URL_MARKER);
        copied = end;
        search = end;
    }
    out.push_str(&text[copied..]);
    out
}

/// Replace `name=value`, `name: value` and `Bearer <token>` with
/// `name=<redacted>`, keeping the name as written.
fn redact_named(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut copied = 0;
    let mut i = 0;
    while i < bytes.len() {
        if !bytes[i].is_ascii_alphabetic() || (i > 0 && is_word_byte(bytes[i - 1])) {
            i += 1;
            continue;
        }
        if let Some((name_end, value_end)) = match_named(bytes, i) {
            out.push_str(&text[copied..i]);
            out.push_str(&text[i..name_end]);
            out.push('=');
            out.push_str(REDACTED);
            copied = value_end;
            i = value_end;
        } else {
            i += 1;
        }
    }
    out.push_str(&text[copied..]);
    out
}

/// At `start` (a word start), match a credential assignment. Returns the end
/// of the name and the end of the value.
fn match_named(bytes: &[u8], start: usize) -> Option<(usize, usize)> {
    // `Bearer <token>`.
    if let Some(name_end) = match_word(bytes, start, "bearer") {
        let value_start = skip_blanks(bytes, name_end);
        if value_start > name_end {
            let value_end = trim_embedded(bytes, value_start, scan_value(bytes, value_start));
            if value_end > value_start {
                return Some((name_end, value_end));
            }
        }
        return None;
    }

    for name in SECRET_NAMES {
        let Some((name_end, sep_end)) = assignment_at(bytes, start, name) else {
            continue;
        };
        let value_start = skip_blanks(bytes, sep_end);
        let mut value_end = trim_embedded(bytes, value_start, scan_value(bytes, value_start));
        if value_end == value_start {
            continue;
        }
        // `Authorization: Bearer abc` / `Basic abc`: the scheme word is not
        // the secret, the token after it is.
        let value = &bytes[value_start..value_end];
        if value.eq_ignore_ascii_case(b"bearer") || value.eq_ignore_ascii_case(b"basic") {
            let token_start = skip_blanks(bytes, value_end);
            if token_start > value_end {
                value_end = scan_value(bytes, token_start).max(value_end);
            }
        }
        return Some((name_end, value_end));
    }
    None
}

/// `name` at `start`, then an optional closing quote (JSON keys:
/// `"api_key": "..."`), blanks, and `=` or `:`. Returns the end of the name
/// and the position just past the separator.
fn assignment_at(bytes: &[u8], start: usize, name: &str) -> Option<(usize, usize)> {
    let name_end = match_word(bytes, start, name)?;
    let mut sep = name_end;
    if sep < bytes.len() && matches!(bytes[sep], b'"' | b'\'') {
        sep += 1;
    }
    let sep = skip_blanks(bytes, sep);
    (sep < bytes.len() && matches!(bytes[sep], b'=' | b':')).then_some((name_end, sep + 1))
}

/// End an unquoted value where another credential assignment starts inside
/// it, so `password: a:token=b` redacts both values instead of swallowing
/// the second name into the first value and leaving its value behind.
fn trim_embedded(bytes: &[u8], value_start: usize, value_end: usize) -> usize {
    if value_start >= value_end || matches!(bytes[value_start], b'"' | b'\'') {
        return value_end;
    }
    for j in value_start + 1..value_end {
        if is_word_byte(bytes[j - 1]) || !bytes[j].is_ascii_alphabetic() {
            continue;
        }
        if match_word(bytes, j, "bearer").is_some()
            || SECRET_NAMES.iter().any(|name| assignment_at(bytes, j, name).is_some())
        {
            return j;
        }
    }
    value_end
}

/// Case-insensitive match of `word` at `start`, ending at a word boundary.
fn match_word(bytes: &[u8], start: usize, word: &str) -> Option<usize> {
    let end = start + word.len();
    if end > bytes.len() || !bytes[start..end].eq_ignore_ascii_case(word.as_bytes()) {
        return None;
    }
    if end < bytes.len() && is_word_byte(bytes[end]) {
        return None;
    }
    Some(end)
}

fn skip_blanks(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t') {
        i += 1;
    }
    i
}

/// A value runs to whitespace, a separator or a quote.
fn scan_value(bytes: &[u8], mut i: usize) -> usize {
    // A quoted value runs to its closing quote.
    if i < bytes.len() && matches!(bytes[i], b'"' | b'\'') {
        let quote = bytes[i];
        let mut j = i + 1;
        while j < bytes.len() && bytes[j] != quote {
            j += 1;
        }
        return (j + 1).min(bytes.len());
    }
    while i < bytes.len()
        && !bytes[i].is_ascii_whitespace()
        && !matches!(bytes[i], b'&' | b';' | b',' | b'"' | b'\'' | b'<' | b'>' | b')')
    {
        i += 1;
    }
    i
}

fn cap(text: String) -> String {
    if text.chars().count() <= MAX_OUTPUT_CHARS {
        return text;
    }
    let keep = MAX_OUTPUT_CHARS - TRUNCATION_MARKER.chars().count();
    let mut out: String = text.chars().take(keep).collect();
    out.push_str(TRUNCATION_MARKER);
    out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fn is_scheme_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'+' | b'-' | b'.')
}

const fn is_url_end(b: u8) -> bool {
    b.is_ascii_whitespace() || matches!(b, b'"' | b'\'' | b'<' | b'>' | b')')
}

const fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')
}

/// Percent-encode everything outside the RFC 3986 unreserved set.
fn percent_encode(s: &str, lowercase: bool) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else if lowercase {
            out.push_str(&format!("%{b:02x}"));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_secret_and_encoded_form_are_redacted() {
        register_secret("s3cr3t/key+value");
        let out = clean("got s3cr3t/key+value and s3cr3t%2Fkey%2Bvalue and s3cr3t%2fkey%2bvalue");
        assert!(!out.contains("s3cr3t"), "{out}");
        assert_eq!(out.matches(REDACTED).count(), 3);
    }

    #[test]
    fn short_secret_is_ignored() {
        register_secret("ab");
        assert_eq!(clean("abc ab"), "abc ab");
    }

    #[test]
    fn urls_are_replaced() {
        let out = clean("error sending request for url (https://rpc.example.com/?api-key=abc): timed out");
        assert_eq!(out, "error sending request for url (<url>): timed out");
        assert_eq!(clean("\"wss://x.y/z\" end"), "\"<url>\" end");
        assert_eq!(clean("not a ://url"), "not a ://url");
    }

    #[test]
    fn named_credentials_are_redacted() {
        assert_eq!(clean("api-key=abc123 rest"), "api-key=<redacted> rest");
        assert_eq!(clean("API_KEY: abc123"), "API_KEY=<redacted>");
        assert_eq!(clean("a token=x&sig=y"), "a token=<redacted>&sig=<redacted>");
        assert_eq!(clean("Authorization: Bearer abc.def"), "Authorization=<redacted>");
        assert_eq!(clean("header Bearer abc.def ok"), "header Bearer=<redacted> ok");
        assert_eq!(clean("password=\"a b c\" end"), "password=<redacted> end");
    }

    #[test]
    fn names_only_match_whole_words() {
        assert_eq!(clean("monkey=banana"), "monkey=banana");
        assert_eq!(clean("keyboard: fine"), "keyboard: fine");
        assert_eq!(clean("the key is lost"), "the key is lost");
    }

    #[test]
    fn control_characters_become_spaces() {
        assert_eq!(clean("a\nb\tc\u{1b}[31m"), "a b c [31m");
    }

    #[test]
    fn output_is_capped_at_a_char_boundary() {
        let out = clean(&"é".repeat(500));
        assert_eq!(out.chars().count(), MAX_OUTPUT_CHARS);
        assert!(out.ends_with(TRUNCATION_MARKER));
        assert_eq!(clean(&"x".repeat(200)).len(), 200);
    }

    #[test]
    fn huge_input_is_cut_before_scanning() {
        let input = format!("{}{}", "€".repeat(MAX_INPUT_BYTES), "token=abc");
        let out = clean(&input);
        assert_eq!(out.chars().count(), MAX_OUTPUT_CHARS);
    }
}
