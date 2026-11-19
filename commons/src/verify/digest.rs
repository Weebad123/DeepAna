//! Canonical outcomes, per-case digests and the run digest.
//!
//! **Digests cover outputs, and nothing else.** No path, time, artifact id,
//! status, verifier version, error wording or machine detail goes in, so
//! the same artifact over the same fixtures has the same digests on every
//! machine and every run, and a digest changes exactly when an output does.
//!
//! Everything is built from `canon.rs`: an outcome is a [`StrictValue`],
//! and what is hashed is its [`canonical_bytes`], which are injective (two
//! different outcomes never share bytes).
//!
//! # Canonical outcomes
//!
//! A decoder case (and every fuzz input):
//!
//! ```text
//! {"result":"decoded","instruction_type":"<t>","data":<strict JSON>}
//! {"result":"decoded","instruction_type":"<t>","data_json":"<raw text>"}   output is not strict JSON
//! {"result":"not_claimed"}
//! {"result":"decode_error"}          the message is not an output: wording changes between versions
//! {"result":"panic"}
//! ```
//!
//! A model case:
//!
//! ```text
//! {"columns":[...],"ordered":<bool>,"rows":[[...],...]}   rows sorted canonically unless ordered
//! {"error":"plan"|"execution"|"output"|"panic"}
//! ```
//!
//! # Digests
//!
//! ```text
//! case digest = sha256("commons-verify-case v1\n" ‖ <kind> ‖ "\n" ‖ canonical_bytes(outcome))
//! fuzz digest = sha256("commons-verify-fuzz v1\n" ‖ for each outcome, in order: canonical_bytes ‖ "\n")
//! run digest  = sha256("commons-verify-run v1\n"  ‖ for each key, bytewise: "<key> <hex digest>\n")
//! ```
//!
//! `<kind>` is `decoder` or `model`. Keys are `decoder/<case>`,
//! `model/<case>` and `fuzz`.

use std::collections::BTreeMap;

use crate::fixtures::{Cell, StrictValue, canonical_bytes, sort_canonical};
use crate::manifest::{sha256, to_hex};

use super::backend::Outcome;

pub const CASE_DOMAIN: &str = "commons-verify-case v1";
pub const FUZZ_DOMAIN: &str = "commons-verify-fuzz v1";
pub const RUN_DOMAIN: &str = "commons-verify-run v1";
/// The key of the fuzz digest in [`Digests`].
pub const FUZZ_KEY: &str = "fuzz";

/// What kind of case a digest is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CaseKind {
    Decoder,
    Model,
}

impl CaseKind {
    pub fn as_str(self) -> &'static str {
        match self {
            CaseKind::Decoder => "decoder",
            CaseKind::Model => "model",
        }
    }

    /// `decoder/<case>` or `model/<case>`.
    pub fn key(self, case: &str) -> String {
        format!("{}/{case}", self.as_str())
    }
}

/// Why a model case has no result. Only the kind is an output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelFailure {
    Plan,
    Execution,
    Output,
    Panic,
}

impl ModelFailure {
    pub fn as_str(self) -> &'static str {
        match self {
            ModelFailure::Plan => "plan",
            ModelFailure::Execution => "execution",
            ModelFailure::Output => "output",
            ModelFailure::Panic => "panic",
        }
    }
}

fn object(entries: Vec<(&str, StrictValue)>) -> StrictValue {
    StrictValue::Object(
        entries
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

/// The canonical outcome of one decoder call; `None` is a call that
/// panicked. `strict_data` is the decoder's JSON as a strict value, when it
/// is one (the caller has already tried [`parse_strict`](crate::fixtures::parse_strict)).
pub fn decoder_outcome(outcome: Option<&Outcome>, strict_data: Option<&StrictValue>) -> StrictValue {
    let result = |r: &str| ("result", StrictValue::Str(r.to_string()));
    match outcome {
        None => object(vec![result("panic")]),
        Some(Outcome::NotClaimed) => object(vec![result("not_claimed")]),
        Some(Outcome::DecodeError(_)) => object(vec![result("decode_error")]),
        Some(Outcome::Decoded {
            instruction_type,
            data_json,
        }) => {
            let data = match strict_data {
                Some(v) => ("data", v.clone()),
                None => ("data_json", StrictValue::Str(data_json.clone())),
            };
            object(vec![
                result("decoded"),
                ("instruction_type", StrictValue::Str(instruction_type.clone())),
                data,
            ])
        }
    }
}

/// The canonical outcome of a model that produced `columns` and `rows`.
/// Unless `ordered`, rows are a multiset and are written in canonical
/// order, so the engine's row order never reaches the digest.
pub fn model_outcome(columns: &[String], rows: &[Vec<Cell>], ordered: bool) -> StrictValue {
    let mut rows: Vec<StrictValue> = rows
        .iter()
        .map(|r| StrictValue::Array(r.iter().map(Cell::to_strict).collect()))
        .collect();
    if !ordered {
        sort_canonical(&mut rows);
    }
    object(vec![
        ("columns", StrictValue::str_array(columns)),
        ("ordered", StrictValue::Bool(ordered)),
        ("rows", StrictValue::Array(rows)),
    ])
}

/// The canonical outcome of a model that produced no result.
pub fn model_failure(kind: ModelFailure) -> StrictValue {
    object(vec![("error", StrictValue::Str(kind.as_str().to_string()))])
}

/// See the module docs.
pub fn case_digest(kind: CaseKind, outcome: &StrictValue) -> [u8; 32] {
    let mut bytes = format!("{CASE_DOMAIN}\n{}\n", kind.as_str()).into_bytes();
    bytes.extend_from_slice(&canonical_bytes(outcome));
    sha256(&bytes)
}

/// See the module docs. `outcomes` are in the order the stage produced
/// them, which is fixed by the IDL and the seed.
pub fn fuzz_digest(outcomes: &[StrictValue]) -> [u8; 32] {
    let mut bytes = format!("{FUZZ_DOMAIN}\n").into_bytes();
    for o in outcomes {
        bytes.extend_from_slice(&canonical_bytes(o));
        bytes.push(b'\n');
    }
    sha256(&bytes)
}

/// The digests of one run of one artifact, by key.
pub type Digests = BTreeMap<String, [u8; 32]>;

/// The text the run digest hashes (see the module docs).
pub fn run_listing(digests: &Digests) -> String {
    let mut out = format!("{RUN_DOMAIN}\n");
    // BTreeMap<String, _> iterates in bytewise key order.
    for (key, d) in digests {
        out.push_str(key);
        out.push(' ');
        out.push_str(&to_hex(d));
        out.push('\n');
    }
    out
}

/// SHA-256 of [`run_listing`].
pub fn run_digest(digests: &Digests) -> [u8; 32] {
    sha256(run_listing(digests).as_bytes())
}

/// The keys whose digests differ between two runs (present in one only, or
/// different), in key order.
pub fn differing_keys(a: &Digests, b: &Digests) -> Vec<String> {
    let mut keys: Vec<&String> = a.keys().chain(b.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|k| a.get(*k) != b.get(*k))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{ParseLimits, parse_strict};

    fn sv(s: &str) -> StrictValue {
        parse_strict(s.as_bytes(), &ParseLimits::DEFAULT).unwrap()
    }

    fn decoded(t: &str, json: &str) -> Outcome {
        Outcome::Decoded {
            instruction_type: t.into(),
            data_json: json.into(),
        }
    }

    /// Computed outside Rust:
    /// `printf 'commons-verify-case v1\ndecoder\n{"data":{"amount":1000},"instruction_type":"deposit","result":"decoded"}' | sha256sum`
    const DEPOSIT_DIGEST: &str = "49722a7400997295452c37e3908d9a0a0ce4caa28151b3b815ebef5d2533d22f";

    #[test]
    fn decoder_outcomes_are_canonical_and_exclude_messages() {
        let ok = decoded("deposit", r#"{"amount":1000}"#);
        let v = decoder_outcome(Some(&ok), Some(&sv(r#"{"amount":1000}"#)));
        assert_eq!(
            crate::fixtures::canonical_string(&v),
            r#"{"data":{"amount":1000},"instruction_type":"deposit","result":"decoded"}"#
        );
        let a = decoder_outcome(Some(&Outcome::DecodeError("short".into())), None);
        let b = decoder_outcome(Some(&Outcome::DecodeError("too short!".into())), None);
        assert_eq!(a, b, "error wording is not an output");
        assert_eq!(crate::fixtures::canonical_string(&a), r#"{"result":"decode_error"}"#);
        let raw = decoder_outcome(Some(&decoded("t", "{\"x\":1.5}")), None);
        assert_eq!(
            crate::fixtures::canonical_string(&raw),
            r#"{"data_json":"{\"x\":1.5}","instruction_type":"t","result":"decoded"}"#
        );
        assert_ne!(decoder_outcome(None, None), decoder_outcome(Some(&Outcome::NotClaimed), None));
    }

    #[test]
    fn case_digest_matches_a_computation_outside_rust() {
        let v = decoder_outcome(
            Some(&decoded("deposit", "{}")),
            Some(&sv(r#"{"amount":1000}"#)),
        );
        assert_eq!(to_hex(&case_digest(CaseKind::Decoder, &v)), DEPOSIT_DIGEST);
        assert_ne!(case_digest(CaseKind::Model, &v), case_digest(CaseKind::Decoder, &v), "kind is bound");
    }

    #[test]
    fn model_outcomes_ignore_row_order_unless_ordered() {
        let cols = vec!["k".to_string(), "n".to_string()];
        let r1 = vec![vec![Cell::Str("b".into()), Cell::Int(1)], vec![Cell::Str("a".into()), Cell::Int(2)]];
        let mut r2 = r1.clone();
        r2.reverse();
        assert_eq!(model_outcome(&cols, &r1, false), model_outcome(&cols, &r2, false));
        assert_ne!(model_outcome(&cols, &r1, true), model_outcome(&cols, &r2, true));
        assert_ne!(model_outcome(&cols, &r1, false), model_outcome(&cols, &r1, true));
        assert_eq!(
            crate::fixtures::canonical_string(&model_outcome(&cols, &r1, false)),
            r#"{"columns":["k","n"],"ordered":false,"rows":[["a",2],["b",1]]}"#
        );
        // Multiplicity is an output.
        let mut r3 = r1.clone();
        r3.push(r1[0].clone());
        assert_ne!(model_outcome(&cols, &r1, false), model_outcome(&cols, &r3, false));
        // No coercion: 1 is not "1", null is not 0.
        let one = |c: Cell| model_outcome(&cols[..1], &[vec![c]], false);
        assert_ne!(one(Cell::Int(1)), one(Cell::Str("1".into())));
        assert_ne!(one(Cell::Null), one(Cell::Int(0)));
        assert_ne!(one(Cell::Bool(false)), one(Cell::Int(0)));
    }

    #[test]
    fn run_digest_is_over_sorted_keys_and_outputs_only() {
        let mut d = Digests::new();
        d.insert("model/b".into(), [2; 32]);
        d.insert("decoder/a".into(), [1; 32]);
        let listing = run_listing(&d);
        assert_eq!(
            listing,
            format!(
                "commons-verify-run v1\ndecoder/a {}\nmodel/b {}\n",
                "01".repeat(32),
                "02".repeat(32)
            )
        );
        let mut e = d.clone();
        assert_eq!(run_digest(&d), run_digest(&e));
        e.insert("fuzz".into(), [3; 32]);
        assert_ne!(run_digest(&d), run_digest(&e));
        assert_eq!(differing_keys(&d, &e), ["fuzz"]);
        e.insert("decoder/a".into(), [9; 32]);
        assert_eq!(differing_keys(&d, &e), ["decoder/a", "fuzz"]);
        assert!(differing_keys(&d, &d).is_empty());
    }

    #[test]
    fn fuzz_digest_depends_on_order_and_every_outcome() {
        let a = decoder_outcome(Some(&Outcome::NotClaimed), None);
        let b = decoder_outcome(Some(&Outcome::DecodeError(String::new())), None);
        let ab = fuzz_digest(&[a.clone(), b.clone()]);
        assert_eq!(ab, fuzz_digest(&[a.clone(), b.clone()]));
        assert_ne!(ab, fuzz_digest(&[b.clone(), a.clone()]));
        assert_ne!(ab, fuzz_digest(std::slice::from_ref(&a)));
        assert_ne!(fuzz_digest(&[]), ab);
    }
}
