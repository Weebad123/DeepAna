//! [`SystemDecoder`]: decodes System program instructions: native SOL
//! transfers, account creation, allocation, assignment and durable nonces.
//!
//! System instruction data is a bincode-encoded enum: a little-endian `u32`
//! variant tag, then the variant's fields in order. Integers are
//! little-endian `u64`s, a pubkey is its 32 raw bytes and a string is a
//! little-endian `u64` byte length followed by that many UTF-8 bytes. The
//! layout is read by hand with bounds checks on every field, so no input can
//! make the decoder panic; bytes after the last field are ignored, as the
//! runtime's own `limited_deserialize` ignores them.
//!
//! | Tag | Instruction              | Fields                                    | Accounts (by position)                                       |
//! |-----|--------------------------|-------------------------------------------|--------------------------------------------------------------|
//! | 0   | `CreateAccount`          | lamports, space, owner                    | source, new_account                                          |
//! | 1   | `Assign`                 | owner                                     | account                                                      |
//! | 2   | `Transfer`               | lamports                                  | source, destination                                          |
//! | 3   | `CreateAccountWithSeed`  | base, seed, lamports, space, owner        | source, new_account, [base_account]                          |
//! | 4   | `AdvanceNonceAccount`    |                                           | nonce_account, recent_blockhashes_sysvar, [nonce_authority]  |
//! | 5   | `WithdrawNonceAccount`   | lamports                                  | nonce_account, destination, recent_blockhashes_sysvar, rent_sysvar, [nonce_authority] |
//! | 6   | `InitializeNonceAccount` | authorized                                | nonce_account, recent_blockhashes_sysvar, rent_sysvar        |
//! | 7   | `AuthorizeNonceAccount`  | authorized                                | nonce_account, [nonce_authority]                             |
//! | 8   | `Allocate`               | space                                     | account                                                      |
//! | 9   | `AllocateWithSeed`       | base, seed, space, owner                  | account, [base_account]                                      |
//! | 10  | `AssignWithSeed`         | base, seed, owner                         | account, [base_account]                                      |
//! | 11  | `TransferWithSeed`       | lamports, from_seed, from_owner           | source, source_base, destination                             |
//! | 12  | `UpgradeNonceAccount`    |                                           | nonce_account                                                |
//!
//! The decoded data holds the instruction's fields at the top level and its
//! accounts, by role, under `"accounts"`:
//!
//! ```json
//! { "lamports": 5000, "accounts": { "source": "...", "destination": "..." } }
//! ```
//!
//! Accounts in brackets are optional: the program finds them through the
//! transaction's signers rather than by position, so they may be absent and
//! are `null` when they are. Any other missing account is a decode error.
//! Accounts past the last role are ignored.
//!
//! Native SOL transfers produce no [`DeepAnaTokenTransfer`](crate::types::udm::DeepAnaTokenTransfer)
//! row: that table is for token accounts and mints. The transfer is visible
//! in the instruction row's `instruction_type` and `decoded_data`.

use serde_json::{Map, Value, json};
use solana_sdk::pubkey::Pubkey;

use super::{DecodeResult, Decoder, decode_error, instruction_data};
use crate::types::error::PrismResult;
use crate::types::udm::DeepAnaInstruction;

/// Base58 id of the System program.
pub const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

/// Highest variant tag this decoder knows (`UpgradeNonceAccount`).
const LAST_TAG: u32 = 12;

/// Decodes System program instructions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemDecoder;

impl SystemDecoder {
    pub const fn new() -> Self {
        Self
    }
}

impl Decoder for SystemDecoder {
    fn program_id(&self) -> &str {
        SYSTEM_PROGRAM_ID
    }

    fn can_decode(&self, program_id: &str, data: &[u8]) -> bool {
        program_id == SYSTEM_PROGRAM_ID && read_tag(data).is_some_and(|tag| tag <= LAST_TAG)
    }

    fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        _account_keys: &[String],
    ) -> PrismResult<DecodeResult> {
        if instruction.program_id != SYSTEM_PROGRAM_ID {
            return Err(decode_error(
                instruction,
                format!("not a {SYSTEM_PROGRAM_ID} instruction"),
            ));
        }
        let data = instruction_data(instruction)?;
        let described = describe(&data).map_err(|reason| decode_error(instruction, reason))?;
        let accounts = described
            .roles
            .resolve(&instruction.accounts)
            .map_err(|reason| decode_error(instruction, format!("{}: {reason}", described.instruction_type)))?;
        let mut fields = described.fields;
        fields.insert("accounts".to_owned(), Value::Object(accounts));
        Ok(DecodeResult {
            instruction_type: described.instruction_type.to_owned(),
            decoded_data: Value::Object(fields),
            token_transfer: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Account roles
// ---------------------------------------------------------------------------

/// The role of each account position of an instruction. The first
/// `required` must be present; the rest are `null` when absent.
struct Roles {
    names: &'static [&'static str],
    required: usize,
}

impl Roles {
    const fn new(names: &'static [&'static str], required: usize) -> Self {
        Self { names, required }
    }

    /// `accounts` keyed by role name.
    fn resolve(&self, accounts: &[String]) -> Result<Map<String, Value>, String> {
        if accounts.len() < self.required {
            return Err(format!(
                "needs at least {} accounts ({}), the instruction has {}",
                self.required,
                self.names.iter().take(self.required).copied().collect::<Vec<_>>().join(", "),
                accounts.len()
            ));
        }
        Ok(self
            .names
            .iter()
            .enumerate()
            .map(|(at, name)| {
                let key = accounts.get(at).map_or(Value::Null, |key| Value::String(key.clone()));
                ((*name).to_owned(), key)
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Data → type, fields, roles
// ---------------------------------------------------------------------------

/// A decoded instruction, before it is tied to its accounts.
struct Described {
    instruction_type: &'static str,
    fields: Map<String, Value>,
    roles: Roles,
}

fn described(instruction_type: &'static str, fields: Value, roles: Roles) -> Described {
    let Value::Object(fields) = fields else {
        unreachable!("fields are always built with json!({{...}})")
    };
    Described {
        instruction_type,
        fields,
        roles,
    }
}

/// The variant tag at the front of `data`, if there are four bytes.
fn read_tag(data: &[u8]) -> Option<u32> {
    let bytes: [u8; 4] = data.get(..4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

/// Type name, JSON fields and account roles of the instruction `data`
/// encodes.
fn describe(data: &[u8]) -> Result<Described, String> {
    let mut r = Reader::new(data);
    let tag = r.u32("instruction tag")?;
    Ok(match tag {
        0 => described(
            "CreateAccount",
            json!({
                "lamports": r.u64("lamports")?,
                "space": r.u64("space")?,
                "owner": r.pubkey("owner")?,
            }),
            Roles::new(&["source", "new_account"], 2),
        ),
        1 => described(
            "Assign",
            json!({ "owner": r.pubkey("owner")? }),
            Roles::new(&["account"], 1),
        ),
        2 => described(
            "Transfer",
            json!({ "lamports": r.u64("lamports")? }),
            Roles::new(&["source", "destination"], 2),
        ),
        3 => described(
            "CreateAccountWithSeed",
            json!({
                "base": r.pubkey("base")?,
                "seed": r.string("seed")?,
                "lamports": r.u64("lamports")?,
                "space": r.u64("space")?,
                "owner": r.pubkey("owner")?,
            }),
            Roles::new(&["source", "new_account", "base_account"], 2),
        ),
        4 => described(
            "AdvanceNonceAccount",
            json!({}),
            Roles::new(&["nonce_account", "recent_blockhashes_sysvar", "nonce_authority"], 2),
        ),
        5 => described(
            "WithdrawNonceAccount",
            json!({ "lamports": r.u64("lamports")? }),
            Roles::new(
                &[
                    "nonce_account",
                    "destination",
                    "recent_blockhashes_sysvar",
                    "rent_sysvar",
                    "nonce_authority",
                ],
                4,
            ),
        ),
        6 => described(
            "InitializeNonceAccount",
            json!({ "authorized": r.pubkey("authorized")? }),
            Roles::new(&["nonce_account", "recent_blockhashes_sysvar", "rent_sysvar"], 3),
        ),
        7 => described(
            "AuthorizeNonceAccount",
            json!({ "authorized": r.pubkey("authorized")? }),
            Roles::new(&["nonce_account", "nonce_authority"], 1),
        ),
        8 => described(
            "Allocate",
            json!({ "space": r.u64("space")? }),
            Roles::new(&["account"], 1),
        ),
        9 => described(
            "AllocateWithSeed",
            json!({
                "base": r.pubkey("base")?,
                "seed": r.string("seed")?,
                "space": r.u64("space")?,
                "owner": r.pubkey("owner")?,
            }),
            Roles::new(&["account", "base_account"], 1),
        ),
        10 => described(
            "AssignWithSeed",
            json!({
                "base": r.pubkey("base")?,
                "seed": r.string("seed")?,
                "owner": r.pubkey("owner")?,
            }),
            Roles::new(&["account", "base_account"], 1),
        ),
        11 => described(
            "TransferWithSeed",
            json!({
                "lamports": r.u64("lamports")?,
                "from_seed": r.string("from_seed")?,
                "from_owner": r.pubkey("from_owner")?,
            }),
            Roles::new(&["source", "source_base", "destination"], 3),
        ),
        12 => described(
            "UpgradeNonceAccount",
            json!({}),
            Roles::new(&["nonce_account"], 1),
        ),
        other => return Err(format!("unknown instruction tag {other}")),
    })
}

/// Reads bincode fields from the front of the data, failing (never
/// panicking) when the data runs out.
struct Reader<'a> {
    data: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, at: 0 }
    }

    fn take(&mut self, len: usize, field: &str) -> Result<&'a [u8], String> {
        let bytes = self
            .at
            .checked_add(len)
            .and_then(|end| self.data.get(self.at..end))
            .ok_or_else(|| {
                format!(
                    "data too short for {field}: needs {len} bytes at offset {}, has {}",
                    self.at,
                    self.data.len().saturating_sub(self.at)
                )
            })?;
        self.at += len;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self, field: &str) -> Result<[u8; N], String> {
        let bytes = self.take(N, field)?;
        bytes
            .try_into()
            .map_err(|_| format!("data too short for {field}"))
    }

    fn u32(&mut self, field: &str) -> Result<u32, String> {
        self.array(field).map(u32::from_le_bytes)
    }

    fn u64(&mut self, field: &str) -> Result<u64, String> {
        self.array(field).map(u64::from_le_bytes)
    }

    fn pubkey(&mut self, field: &str) -> Result<Value, String> {
        let bytes = self.array::<32>(field)?;
        Ok(Value::String(Pubkey::new_from_array(bytes).to_string()))
    }

    fn string(&mut self, field: &str) -> Result<Value, String> {
        let len = self.u64(field)?;
        let len = usize::try_from(len).map_err(|_| format!("{field} length {len} does not fit in memory"))?;
        let bytes = self.take(len, field)?;
        let text = std::str::from_utf8(bytes).map_err(|err| format!("{field} is not valid UTF-8: {err}"))?;
        Ok(Value::String(text.to_owned()))
    }
}
