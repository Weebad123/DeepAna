//! Read-time decoding of instructions PRISM stored undecoded.
//!
//! PRISM decodes SPL Token, Token-2022 and the Anchor programs it has an IDL
//! for; every other instruction reaches Parquet with `instruction_type`
//! null. A transaction report would then show the most common instructions
//! of all, a compute budget setting or a SOL transfer, as opaque base58.
//!
//! [`decode`] closes that gap for four programs whose layouts are fixed by
//! the runtime, without touching PRISM or the rows already written:
//!
//! | Program                  | Layout                                              |
//! |--------------------------|-----------------------------------------------------|
//! | System                   | `u32` LE tag, then the variant's fields             |
//! | Compute Budget           | `u8` tag, then the variant's fields                 |
//! | Memo (v1 and v2)         | the data is the memo, UTF-8                         |
//! | Associated Token Account | empty or a `u8` tag; the accounts carry the meaning |
//!
//! It is a fallback, never an override: the report uses it only for a row
//! PRISM left undecoded, and labels the result `decoded_by: "oracle"`.
//! Names follow PRISM's convention (the variant's name, `Transfer`).
//!
//! 64-bit amounts are JSON strings: a lamport amount can exceed 2^53, which
//! a JavaScript client would round.
//!
//! Anything that does not match exactly (an unknown tag, data too short for
//! its variant) is `None`: the instruction stays undecoded instead of being
//! shown half-read.

use serde_json::{Map, Value, json};

pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
pub const COMPUTE_BUDGET_PROGRAM: &str = "ComputeBudget111111111111111111111111111111";
pub const MEMO_PROGRAM: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
pub const MEMO_V1_PROGRAM: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";
pub const ASSOCIATED_TOKEN_PROGRAM: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

/// Every program [`decode`] knows.
pub const DECODED_PROGRAMS: [&str; 5] =
    [SYSTEM_PROGRAM, COMPUTE_BUDGET_PROGRAM, MEMO_PROGRAM, MEMO_V1_PROGRAM, ASSOCIATED_TOKEN_PROGRAM];

/// An instruction ORACLE decoded itself.
#[derive(Debug, Clone, PartialEq)]
pub struct Decoded {
    pub instruction_type: &'static str,
    pub data: Value,
}

pub fn is_memo_program(program_id: &str) -> bool {
    program_id == MEMO_PROGRAM || program_id == MEMO_V1_PROGRAM
}

/// Decode `data` of an instruction of `program_id` called with `accounts`.
/// See the [module docs](self).
pub fn decode(program_id: &str, data: &[u8], accounts: &[String]) -> Option<Decoded> {
    match program_id {
        SYSTEM_PROGRAM => system(data, accounts),
        COMPUTE_BUDGET_PROGRAM => compute_budget(data),
        MEMO_PROGRAM | MEMO_V1_PROGRAM => memo(data),
        ASSOCIATED_TOKEN_PROGRAM => associated_token(data, accounts),
        _ => None,
    }
}

/// The memo an instruction of a memo program carries, if it is UTF-8.
pub fn memo_of(program_id: &str, data: &[u8]) -> Option<String> {
    if !is_memo_program(program_id) {
        return None;
    }
    std::str::from_utf8(data).ok().map(str::to_owned)
}

// -- reading ----------------------------------------------------------------

/// A cursor over instruction data. Every read fails past the end.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u32(&mut self) -> Option<u32> {
        self.take(4).map(|b| u32::from_le_bytes(b.try_into().expect("4 bytes")))
    }

    fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|b| u64::from_le_bytes(b.try_into().expect("8 bytes")))
    }

    fn pubkey(&mut self) -> Option<String> {
        self.take(32).map(|b| bs58::encode(b).into_string())
    }
}

/// `fields` plus the named accounts that are present.
fn with_accounts(mut fields: Map<String, Value>, names: &[&str], accounts: &[String]) -> Value {
    for (name, account) in names.iter().zip(accounts) {
        fields.insert((*name).to_owned(), Value::from(account.as_str()));
    }
    Value::Object(fields)
}

fn fields(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => Map::new(),
    }
}

// -- programs ---------------------------------------------------------------

fn system(data: &[u8], accounts: &[String]) -> Option<Decoded> {
    let mut r = Reader(data);
    let (name, own, account_names): (&'static str, Value, &[&str]) = match r.u32()? {
        0 => {
            let (lamports, space, owner) = (r.u64()?, r.u64()?, r.pubkey()?);
            (
                "CreateAccount",
                json!({ "lamports": lamports.to_string(), "space": space.to_string(), "owner": owner }),
                &["funder", "new_account"],
            )
        }
        1 => ("Assign", json!({ "owner": r.pubkey()? }), &["account"]),
        2 => ("Transfer", json!({ "lamports": r.u64()?.to_string() }), &["source", "destination"]),
        // The seeded variants carry a length-prefixed string; their accounts
        // say enough.
        3 => ("CreateAccountWithSeed", json!({}), &["funder", "new_account", "base"]),
        4 => ("AdvanceNonceAccount", json!({}), &["nonce_account", "recent_blockhashes_sysvar", "nonce_authority"]),
        5 => (
            "WithdrawNonceAccount",
            json!({ "lamports": r.u64()?.to_string() }),
            &["nonce_account", "destination", "recent_blockhashes_sysvar", "rent_sysvar", "nonce_authority"],
        ),
        6 => ("InitializeNonceAccount", json!({ "authority": r.pubkey()? }), &["nonce_account"]),
        7 => ("AuthorizeNonceAccount", json!({ "new_authority": r.pubkey()? }), &["nonce_account", "nonce_authority"]),
        8 => ("Allocate", json!({ "space": r.u64()?.to_string() }), &["account"]),
        9 => ("AllocateWithSeed", json!({}), &["account", "base"]),
        10 => ("AssignWithSeed", json!({}), &["account", "base"]),
        11 => ("TransferWithSeed", json!({ "lamports": r.u64()?.to_string() }), &["source", "base", "destination"]),
        12 => ("UpgradeNonceAccount", json!({}), &["nonce_account"]),
        _ => return None,
    };
    Some(Decoded { instruction_type: name, data: with_accounts(fields(own), account_names, accounts) })
}

fn compute_budget(data: &[u8]) -> Option<Decoded> {
    let mut r = Reader(data);
    let (name, data) = match r.u8()? {
        0 => ("RequestUnits", json!({ "units": r.u32()?, "additional_fee": r.u32()? })),
        1 => ("RequestHeapFrame", json!({ "bytes": r.u32()? })),
        2 => ("SetComputeUnitLimit", json!({ "units": r.u32()? })),
        3 => ("SetComputeUnitPrice", json!({ "micro_lamports": r.u64()?.to_string() })),
        4 => ("SetLoadedAccountsDataSizeLimit", json!({ "bytes": r.u32()? })),
        _ => return None,
    };
    Some(Decoded { instruction_type: name, data })
}

fn memo(data: &[u8]) -> Option<Decoded> {
    let text = std::str::from_utf8(data).ok()?;
    Some(Decoded { instruction_type: "Memo", data: json!({ "memo": text }) })
}

fn associated_token(data: &[u8], accounts: &[String]) -> Option<Decoded> {
    let (name, names): (&'static str, &[&str]) = match data {
        [] | [0] => ("Create", &["funder", "associated_account", "wallet", "mint"]),
        [1] => ("CreateIdempotent", &["funder", "associated_account", "wallet", "mint"]),
        [2] => (
            "RecoverNested",
            &["nested_account", "nested_mint", "destination_account", "owner_account", "owner_mint", "wallet"],
        ),
        _ => return None,
    };
    Some(Decoded { instruction_type: name, data: with_accounts(Map::new(), names, accounts) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accounts(n: u8) -> Vec<String> {
        (1..=n).map(|i| bs58::encode([i; 32]).into_string()).collect()
    }

    fn sys(tag: u32, rest: &[u8]) -> Vec<u8> {
        [tag.to_le_bytes().as_slice(), rest].concat()
    }

    #[test]
    fn a_sol_transfer_names_its_accounts_and_keeps_the_amount_exact() {
        let a = accounts(2);
        let d = decode(SYSTEM_PROGRAM, &sys(2, &u64::MAX.to_le_bytes()), &a).unwrap();
        assert_eq!(d.instruction_type, "Transfer");
        assert_eq!(d.data, json!({ "lamports": "18446744073709551615", "source": a[0], "destination": a[1] }));
    }

    #[test]
    fn create_account_reads_every_field() {
        let owner = [9u8; 32];
        let rest = [1_000u64.to_le_bytes().as_slice(), &165u64.to_le_bytes(), &owner].concat();
        let a = accounts(2);
        let d = decode(SYSTEM_PROGRAM, &sys(0, &rest), &a).unwrap();
        assert_eq!(d.instruction_type, "CreateAccount");
        assert_eq!(
            d.data,
            json!({
                "lamports": "1000", "space": "165", "owner": bs58::encode(owner).into_string(),
                "funder": a[0], "new_account": a[1],
            })
        );
    }

    #[test]
    fn every_system_variant_has_a_name() {
        let filler = [0u8; 64];
        let names: Vec<&str> =
            (0..=12).map(|tag| decode(SYSTEM_PROGRAM, &sys(tag, &filler), &[]).unwrap().instruction_type).collect();
        assert_eq!(names[4], "AdvanceNonceAccount");
        assert_eq!(names[12], "UpgradeNonceAccount");
        let unique: std::collections::BTreeSet<_> = names.iter().collect();
        assert_eq!(unique.len(), 13);
        assert_eq!(decode(SYSTEM_PROGRAM, &sys(13, &filler), &[]), None);
    }

    #[test]
    fn compute_budget_settings() {
        let d = |data: &[u8]| decode(COMPUTE_BUDGET_PROGRAM, data, &[]).map(|d| (d.instruction_type, d.data));
        assert_eq!(
            d(&[[2].as_slice(), &200_000u32.to_le_bytes()].concat()),
            Some(("SetComputeUnitLimit", json!({ "units": 200_000 })))
        );
        assert_eq!(
            d(&[[3].as_slice(), &u64::MAX.to_le_bytes()].concat()),
            Some(("SetComputeUnitPrice", json!({ "micro_lamports": "18446744073709551615" })))
        );
        assert_eq!(d(&[[1].as_slice(), &65_536u32.to_le_bytes()].concat()).unwrap().0, "RequestHeapFrame");
        assert_eq!(d(&[[4].as_slice(), &1u32.to_le_bytes()].concat()).unwrap().0, "SetLoadedAccountsDataSizeLimit");
        assert_eq!(d(&[0, 1, 0, 0, 0, 2, 0, 0, 0]).unwrap().1, json!({ "units": 1, "additional_fee": 2 }));
        assert_eq!(d(&[5]), None);
    }

    #[test]
    fn data_too_short_for_its_variant_stays_undecoded() {
        assert_eq!(decode(SYSTEM_PROGRAM, &[], &[]), None);
        assert_eq!(decode(SYSTEM_PROGRAM, &[2, 0, 0], &[]), None);
        assert_eq!(decode(SYSTEM_PROGRAM, &sys(2, &[1, 2, 3]), &[]), None);
        assert_eq!(decode(SYSTEM_PROGRAM, &sys(1, &[0; 31]), &[]), None);
        assert_eq!(decode(COMPUTE_BUDGET_PROGRAM, &[], &[]), None);
        assert_eq!(decode(COMPUTE_BUDGET_PROGRAM, &[2, 1], &[]), None);
    }

    #[test]
    fn a_memo_is_its_text_in_both_versions() {
        for program in [MEMO_PROGRAM, MEMO_V1_PROGRAM] {
            let d = decode(program, "gm ☀".as_bytes(), &[]).unwrap();
            assert_eq!((d.instruction_type, d.data), ("Memo", json!({ "memo": "gm ☀" })));
            assert_eq!(memo_of(program, b"gm").as_deref(), Some("gm"));
            assert_eq!(decode(program, &[0xff, 0xfe], &[]), None);
            assert_eq!(memo_of(program, &[0xff]), None);
        }
        assert_eq!(memo_of(SYSTEM_PROGRAM, b"gm"), None);
    }

    #[test]
    fn associated_token_accounts() {
        let a = accounts(4);
        for (data, name) in [(&[][..], "Create"), (&[0][..], "Create"), (&[1][..], "CreateIdempotent")] {
            let d = decode(ASSOCIATED_TOKEN_PROGRAM, data, &a).unwrap();
            assert_eq!(d.instruction_type, name);
            assert_eq!(d.data, json!({ "funder": a[0], "associated_account": a[1], "wallet": a[2], "mint": a[3] }));
        }
        assert_eq!(decode(ASSOCIATED_TOKEN_PROGRAM, &[2], &[]).unwrap().instruction_type, "RecoverNested");
        assert_eq!(decode(ASSOCIATED_TOKEN_PROGRAM, &[3], &a), None);
        assert_eq!(decode(ASSOCIATED_TOKEN_PROGRAM, &[0, 0], &a), None);
    }

    #[test]
    fn missing_accounts_are_left_out_and_other_programs_are_not_guessed() {
        let d = decode(SYSTEM_PROGRAM, &sys(2, &5u64.to_le_bytes()), &accounts(1)).unwrap();
        assert_eq!(d.data, json!({ "lamports": "5", "source": accounts(1)[0] }));
        assert_eq!(decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", &[3, 0, 0, 0], &[]), None);
    }
}
