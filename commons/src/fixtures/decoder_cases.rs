//! Decoder cases: one instruction and what a decoder must say about it.
//!
//! ```json
//! {
//!   "schema_version": 1,
//!   "program_id": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
//!   "case": "transfer-basic",
//!   "provenance": { "kind": "onchain_transaction", "cluster": "devnet",
//!                   "reference": "<transaction signature>", "note": "instruction 2" },
//!   "instruction_data_hex": "03e803000000000000",
//!   "accounts": ["<source>", "<destination>", "<owner>"],
//!   "expect": { "result": "decoded", "instruction_type": "transfer", "data": { "amount": 1000 } }
//! }
//! ```
//!
//! - Every key is required and no other key is allowed.
//! - `instruction_data_hex` is lowercase hex, an even number of digits, at
//!   most [`MAX_INSTRUCTION_DATA_BYTES`] bytes (empty is allowed: some
//!   instructions carry no data).
//! - `accounts` are base58 addresses, at most [`MAX_DECODER_ACCOUNTS`].
//! - `expect.result` is `decoded`, `not_claimed` (the decoder must decline
//!   the instruction) or `decode_error` (the decoder claims it and must
//!   fail). Only `decoded` has, and requires, `instruction_type` and `data`.
//! - `expect.data` is any strict JSON value, compared exactly.
//!
//! `verify/` builds the instruction the decoder sees from these fields, with
//! fixed placeholders for everything else.

use crate::config::echo;

use super::{
    FieldIssue, Obj, Provenance, StrictValue, parse_provenance, want_key, want_name, want_str,
};

/// Solana's packet size bounds instruction data well below this.
pub const MAX_INSTRUCTION_DATA_BYTES: usize = 1232;
/// An instruction indexes its accounts with a `u8`.
pub const MAX_DECODER_ACCOUNTS: usize = 256;
pub const MAX_INSTRUCTION_TYPE_LEN: usize = 64;

/// What a decoder must say about one instruction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DecoderExpect {
    /// The decoder claims the instruction and decodes it to exactly this.
    Decoded {
        instruction_type: String,
        data: StrictValue,
    },
    /// The decoder must not claim the instruction.
    NotClaimed,
    /// The decoder claims the instruction and must fail to decode it.
    DecodeError,
}

impl DecoderExpect {
    pub fn result_str(&self) -> &'static str {
        match self {
            DecoderExpect::Decoded { .. } => "decoded",
            DecoderExpect::NotClaimed => "not_claimed",
            DecoderExpect::DecodeError => "decode_error",
        }
    }
}

/// A validated decoder case.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecoderCase {
    pub program_id: [u8; 32],
    /// The `case` field, equal to the file name without `.json`.
    pub name: String,
    pub provenance: Provenance,
    /// Instruction data, decoded from `instruction_data_hex`.
    pub data: Vec<u8>,
    pub accounts: Vec<[u8; 32]>,
    pub expect: DecoderExpect,
}

const KEYS: [&str; 7] = [
    "schema_version",
    "program_id",
    "case",
    "provenance",
    "instruction_data_hex",
    "accounts",
    "expect",
];

/// Parse a decoder case file. Fields are checked in the order of the
/// example above, after `schema_version` and unknown keys. Names against
/// the directory and file are checked by the loader.
pub fn parse_decoder_case(v: &StrictValue) -> Result<DecoderCase, FieldIssue> {
    let o = Obj::of(v, "")?;
    o.schema_version()?;
    o.only(&KEYS, "field")?;
    let program_id = want_key(o.req("program_id")?, "program_id", "a program address")?;
    let name = want_name(o.req("case")?, "case")?.to_string();
    let provenance = parse_provenance(o.req("provenance")?, "provenance")?;
    let data = parse_hex(o.str("instruction_data_hex")?)
        .map_err(|r| FieldIssue::new("instruction_data_hex", r))?;
    let accounts = o
        .array("accounts", MAX_DECODER_ACCOUNTS)?
        .iter()
        .enumerate()
        .map(|(i, a)| want_key(a, &format!("accounts[{i}]"), "an account address"))
        .collect::<Result<Vec<_>, _>>()?;
    let expect = parse_expect(o.req("expect")?)?;
    Ok(DecoderCase {
        program_id,
        name,
        provenance,
        data,
        accounts,
        expect,
    })
}

/// Lowercase hex, even length, at most [`MAX_INSTRUCTION_DATA_BYTES`].
fn parse_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() > 2 * MAX_INSTRUCTION_DATA_BYTES {
        return Err(format!(
            "is {} hex digits, more than {MAX_INSTRUCTION_DATA_BYTES} bytes",
            s.len()
        ));
    }
    if !s.len().is_multiple_of(2) {
        return Err("must have an even number of hex digits".into());
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    s.as_bytes()
        .chunks(2)
        .enumerate()
        .map(|(i, pair)| match (nibble(pair[0]), nibble(pair[1])) {
            (Some(hi), Some(lo)) => Ok((hi << 4) | lo),
            _ => Err(format!(
                "'{}' at digit {} is not lowercase hex",
                echo(&String::from_utf8_lossy(pair)),
                2 * i
            )),
        })
        .collect()
}

/// An instruction type: ASCII letters, digits and `_`, starting with a
/// letter, 1 to [`MAX_INSTRUCTION_TYPE_LEN`] characters.
fn check_instruction_type(s: &str) -> Result<(), String> {
    let bad = |why: &str| format!("'{}' is not an instruction type: {why}", echo(s));
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > MAX_INSTRUCTION_TYPE_LEN {
        return Err(bad(&format!(
            "longer than {MAX_INSTRUCTION_TYPE_LEN} characters"
        )));
    }
    if !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(bad("only A-Z, a-z, 0-9 and '_' are allowed"));
    }
    if !s.as_bytes()[0].is_ascii_alphabetic() {
        return Err(bad("must start with a letter"));
    }
    Ok(())
}

fn parse_expect(v: &StrictValue) -> Result<DecoderExpect, FieldIssue> {
    let o = Obj::of(v, "expect")?;
    let result = want_str(o.req("result")?, "expect.result")?;
    match result {
        "decoded" => {
            o.only(&["result", "instruction_type", "data"], "expect field")?;
            let t = o.str("instruction_type")?;
            check_instruction_type(t).map_err(|r| FieldIssue::new("expect.instruction_type", r))?;
            Ok(DecoderExpect::Decoded {
                instruction_type: t.to_string(),
                data: o.req("data")?.clone(),
            })
        }
        "not_claimed" | "decode_error" => {
            o.only(&["result"], "expect field").map_err(|i| {
                FieldIssue::new(i.field, format!("{} (a '{result}' case has only 'result')", i.reason))
            })?;
            Ok(if result == "not_claimed" {
                DecoderExpect::NotClaimed
            } else {
                DecoderExpect::DecodeError
            })
        }
        other => Err(FieldIssue::new(
            "expect.result",
            format!(
                "'{}' is not a result (expected decoded, not_claimed or decode_error)",
                echo(other)
            ),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::{ParseLimits, ProvenanceKind, parse_strict};
    use super::*;

    const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
    const A: &str = "11111111111111111111111111111111";
    const B: &str = "SysvarRent111111111111111111111111111111111";

    fn render(fields: &[(String, String)]) -> String {
        let body: Vec<String> = fields.iter().map(|(k, v)| format!("\"{k}\": {v}")).collect();
        format!("{{{}}}\n", body.join(",\n"))
    }

    fn fields() -> Vec<(String, String)> {
        [
            ("schema_version", "1".into()),
            ("program_id", format!("\"{TOKEN}\"")),
            ("case", "\"transfer-basic\"".into()),
            (
                "provenance",
                r#"{"kind":"program_source","reference":"spl token instruction.rs","note":"tag 3"}"#
                    .into(),
            ),
            ("instruction_data_hex", "\"03e803000000000000\"".into()),
            ("accounts", format!("[\"{A}\", \"{B}\", \"{A}\"]")),
            (
                "expect",
                r#"{"result":"decoded","instruction_type":"transfer","data":{"amount":1000}}"#.into(),
            ),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    fn with(key: &str, v: &str) -> String {
        let mut f = fields();
        match f.iter_mut().find(|(k, _)| k == key) {
            Some(slot) => slot.1 = v.to_string(),
            None => f.push((key.to_string(), v.to_string())),
        }
        render(&f)
    }

    fn without(key: &str) -> String {
        let mut f = fields();
        f.retain(|(k, _)| k != key);
        render(&f)
    }

    fn parse(src: &str) -> Result<DecoderCase, FieldIssue> {
        parse_decoder_case(&parse_strict(src.as_bytes(), &ParseLimits::DEFAULT).unwrap())
    }

    #[track_caller]
    fn bad(src: &str, field: &str, part: &str) {
        let e = parse(src).expect_err(src);
        assert_eq!(e.field, field, "{e}");
        assert!(e.reason.contains(part), "expected {part:?} in {e}");
    }

    #[test]
    fn parses_the_example_into_typed_data() {
        let c = parse(&render(&fields())).unwrap();
        assert_eq!(bs58::encode(c.program_id).into_string(), TOKEN);
        assert_eq!(c.name, "transfer-basic");
        assert_eq!(c.provenance.kind, ProvenanceKind::ProgramSource);
        assert_eq!(c.data, [0x03, 0xe8, 0x03, 0, 0, 0, 0, 0, 0]);
        assert_eq!(c.accounts.len(), 3);
        assert_eq!(c.accounts[0], [0u8; 32]);
        assert_eq!(c.accounts[0], c.accounts[2], "repeats are kept");
        let DecoderExpect::Decoded { instruction_type, data } = &c.expect else {
            panic!("{:?}", c.expect)
        };
        assert_eq!(instruction_type, "transfer");
        assert_eq!(
            data,
            &parse_strict(br#"{"amount":1000}"#, &ParseLimits::DEFAULT).unwrap()
        );
    }

    #[test]
    fn negative_results_have_only_result() {
        let c = parse(&with("expect", r#"{"result":"not_claimed"}"#)).unwrap();
        assert_eq!(c.expect, DecoderExpect::NotClaimed);
        assert_eq!(c.expect.result_str(), "not_claimed");
        let c = parse(&with("expect", r#"{"result":"decode_error"}"#)).unwrap();
        assert_eq!(c.expect, DecoderExpect::DecodeError);
        bad(
            &with("expect", r#"{"result":"decode_error","data":null}"#),
            "expect",
            "has only 'result'",
        );
        bad(
            &with("expect", r#"{"result":"not_claimed","instruction_type":"x"}"#),
            "expect",
            "unknown expect field 'instruction_type'",
        );
        bad(&with("expect", r#"{"result":"decoded","data":1}"#), "expect.instruction_type", "missing");
        bad(&with("expect", r#"{"result":"decoded","instruction_type":"t"}"#), "expect.data", "missing");
        bad(&with("expect", r#"{"result":"ok"}"#), "expect.result", "not a result");
        bad(&with("expect", r#"{"result":"Decoded"}"#), "expect.result", "not a result");
        bad(&with("expect", r#"{}"#), "expect.result", "missing");
        bad(&with("expect", r#""decoded""#), "expect", "must be an object");
    }

    #[test]
    fn hex_rules() {
        assert_eq!(parse(&with("instruction_data_hex", "\"\"")).unwrap().data, Vec::<u8>::new());
        let max = format!("\"{}\"", "ab".repeat(MAX_INSTRUCTION_DATA_BYTES));
        assert_eq!(parse(&with("instruction_data_hex", &max)).unwrap().data.len(), 1232);
        let over = format!("\"{}\"", "ab".repeat(MAX_INSTRUCTION_DATA_BYTES + 1));
        bad(&with("instruction_data_hex", &over), "instruction_data_hex", "more than 1232 bytes");
        bad(&with("instruction_data_hex", "\"abc\""), "instruction_data_hex", "even number");
        bad(&with("instruction_data_hex", "\"03E8\""), "instruction_data_hex", "'E8' at digit 2");
        bad(&with("instruction_data_hex", "\"0x03\""), "instruction_data_hex", "not lowercase hex");
        bad(&with("instruction_data_hex", "\"03 e8 \""), "instruction_data_hex", "' e' at digit 2");
        bad(&with("instruction_data_hex", "[3]"), "instruction_data_hex", "must be a string");
    }

    #[test]
    fn field_rules() {
        bad(&with("program_id", "\"Tokenkeg\""), "program_id", "not a program address");
        bad(&with("case", "\"Transfer\""), "case", "only a-z");
        bad(&with("case", "\"\""), "case", "empty");
        bad(&with("accounts", "[\"x\"]"), "accounts[0]", "not an account address");
        bad(&with("accounts", &format!("[\"{A}\", 1]")), "accounts[1]", "must be a string");
        let many = format!("[{}]", vec![format!("\"{A}\""); MAX_DECODER_ACCOUNTS + 1].join(","));
        bad(&with("accounts", &many), "accounts", "more than the limit of 256");
        bad(
            &with("expect", r#"{"result":"decoded","instruction_type":"9x","data":1}"#),
            "expect.instruction_type",
            "start with a letter",
        );
        bad(
            &with("expect", r#"{"result":"decoded","instruction_type":"a-b","data":1}"#),
            "expect.instruction_type",
            "only A-Z",
        );
        bad(&with("provenance", r#"{"kind":"x"}"#), "provenance.kind", "not a provenance kind");
    }

    #[test]
    fn keys_schema_version_first_then_unknown_then_fields_in_order() {
        bad(&without("schema_version"), "schema_version", "missing");
        bad(&with("schema_version", "2"), "schema_version", "2 is not supported");
        bad(&with("schema_version", "\"1\""), "schema_version", "must be an integer");
        let mut f = fields();
        f[0].1 = "2".into();
        f.push(("future".into(), "1".into()));
        bad(&render(&f), "schema_version", "not supported");
        bad(&with("note", "\"x\""), "file", "unknown field 'note'");
        for k in KEYS.iter().skip(1) {
            bad(&without(k), k, "missing");
        }
        // program_id is checked before case.
        let mut f = fields();
        f[1].1 = "\"bad\"".into();
        f[2].1 = "\"BAD\"".into();
        bad(&render(&f), "program_id", "");
    }

    #[test]
    fn same_bytes_give_the_same_case_and_formatting_does_not_matter() {
        let a = parse(&render(&fields())).unwrap();
        let mut f = fields();
        f.reverse();
        let pretty = render(&f).replace(",\n", ",\n    ");
        assert_eq!(parse(&pretty).unwrap(), a);
        assert_eq!(parse(&render(&fields())).unwrap(), a);
    }
}
