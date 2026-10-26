//! [`SplTokenDecoder`]: decodes SPL Token and Token-2022 instructions, and
//! turns transfers into [`DeepAnaTokenTransfer`] rows.
//!
//! Token instructions are not Anchor: the first byte is the instruction tag
//! and the rest is a fixed little-endian layout per tag, which
//! `TokenInstruction::unpack` reads. Token-2022 keeps tags `0..=24` exactly
//! as SPL Token has them and adds its extensions from `25` on; an extension
//! instruction carries a second tag byte selecting the extension's own
//! instruction.
//!
//! | Instruction              | Accounts (by position)                          | Transfer row |
//! |--------------------------|-------------------------------------------------|--------------|
//! | `Transfer`               | source, destination, authority, signers...      | yes, without mint and decimals |
//! | `TransferChecked`        | source, mint, destination, authority, signers... | yes, with mint and decimals |
//! | `TransferCheckedWithFee` | as `TransferChecked` (Token-2022 transfer fee)  | yes, with mint and decimals |
//! | everything else          | n/a                                             | no           |
//!
//! Mints and burns are decoded but produce no transfer row: the transfer
//! table needs both a source and a destination account, and a mint or burn
//! has only one side.
//!
//! The decoder is built for one program; register [`SplTokenDecoder::spl_token`]
//! and [`SplTokenDecoder::token_2022`] side by side to cover both.

// `TokenInstruction::Transfer` is deprecated in favour of `TransferChecked`
// but still sent, so it has to be decoded.
#![allow(deprecated)]

use serde_json::{Map, Value, json};
use solana_sdk::pubkey::Pubkey;
use spl_token_2022_interface::extension::transfer_fee::instruction::TransferFeeInstruction;
use spl_token_2022_interface::instruction::TokenInstruction;

use super::{DecodeResult, Decoder, decode_error, instruction_data};
use crate::types::error::PrismResult;
use crate::types::udm::{DeepAnaInstruction, DeepAnaTokenTransfer};

/// Base58 id of the SPL Token program.
pub const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
/// Base58 id of the Token-2022 program.
pub const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// Highest tag of the original SPL Token instruction set (`UiAmountToAmount`).
const SPL_TOKEN_LAST_TAG: u8 = 24;
/// Instructions the SPL Token program gained when it was replaced by its
/// optimised rewrite (p-token, SIMD-0266), numbered as in Token-2022:
/// `WithdrawExcessLamports`, `UnwrapLamports` and `Batch`.
const SPL_TOKEN_EXTRA_TAGS: [u8; 3] = [38, 45, 255];
/// Highest single-instruction tag of Token-2022 (`PermissionedBurnExtension`).
const TOKEN_2022_LAST_TAG: u8 = 46;
/// `Batch`: several token instructions packed into one.
const BATCH_TAG: u8 = 255;
/// Tag of `TransferFeeExtension`, whose sub-instructions include
/// `TransferCheckedWithFee`.
const TRANSFER_FEE_EXTENSION_TAG: u8 = 26;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokenProgram {
    SplToken,
    Token2022,
}

/// Decodes the instructions of one token program: SPL Token or Token-2022.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SplTokenDecoder {
    program: TokenProgram,
}

impl SplTokenDecoder {
    /// A decoder for the SPL Token program.
    pub const fn spl_token() -> Self {
        Self {
            program: TokenProgram::SplToken,
        }
    }

    /// A decoder for the Token-2022 program.
    pub const fn token_2022() -> Self {
        Self {
            program: TokenProgram::Token2022,
        }
    }

    /// The decoder for `program_id`, if it is one of the two token programs.
    pub fn for_program(program_id: &str) -> Option<Self> {
        match program_id {
            SPL_TOKEN_PROGRAM_ID => Some(Self::spl_token()),
            TOKEN_2022_PROGRAM_ID => Some(Self::token_2022()),
            _ => None,
        }
    }

    pub const fn is_token_2022(&self) -> bool {
        matches!(self.program, TokenProgram::Token2022)
    }

    /// Whether this decoder's program knows instruction tag `tag`.
    fn knows_tag(&self, tag: u8) -> bool {
        match self.program {
            TokenProgram::SplToken => tag <= SPL_TOKEN_LAST_TAG || SPL_TOKEN_EXTRA_TAGS.contains(&tag),
            TokenProgram::Token2022 => tag <= TOKEN_2022_LAST_TAG || tag == BATCH_TAG,
        }
    }

    /// The instruction `data` encodes, read with the unpacker of this
    /// decoder's program. The SPL Token layout is a strict subset of the
    /// Token-2022 one, so once SPL Token's own unpacker has accepted the
    /// data, both programs share the Token-2022 representation.
    fn unpack<'a>(&self, data: &'a [u8]) -> Result<TokenInstruction<'a>, String> {
        let tag = *data.first().ok_or("instruction data is empty")?;
        if !self.knows_tag(tag) {
            return Err(format!("unknown instruction tag {tag}"));
        }
        if self.program == TokenProgram::SplToken && tag <= SPL_TOKEN_LAST_TAG {
            spl_token_interface::instruction::TokenInstruction::unpack(data)
                .map_err(|err| format!("invalid SPL Token instruction (tag {tag}): {err}"))?;
        }
        TokenInstruction::unpack(data)
            .map_err(|err| format!("invalid token instruction (tag {tag}): {err}"))
    }
}

impl Decoder for SplTokenDecoder {
    fn program_id(&self) -> &str {
        match self.program {
            TokenProgram::SplToken => SPL_TOKEN_PROGRAM_ID,
            TokenProgram::Token2022 => TOKEN_2022_PROGRAM_ID,
        }
    }

    fn can_decode(&self, program_id: &str, data: &[u8]) -> bool {
        program_id == self.program_id() && data.first().is_some_and(|&tag| self.knows_tag(tag))
    }

    fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        _account_keys: &[String],
    ) -> PrismResult<DecodeResult> {
        if instruction.program_id != self.program_id() {
            return Err(decode_error(
                instruction,
                format!("not a {} instruction", self.program_id()),
            ));
        }
        let data = instruction_data(instruction)?;
        let token_instruction = self
            .unpack(&data)
            .map_err(|reason| decode_error(instruction, reason))?;
        let decoded = describe(token_instruction, &data).map_err(|reason| decode_error(instruction, reason))?;
        let token_transfer = decoded
            .transfer
            .map(|transfer| transfer.row(instruction))
            .transpose()?;
        Ok(DecodeResult {
            instruction_type: decoded.instruction_type.to_owned(),
            decoded_data: Value::Object(decoded.fields),
            token_transfer,
        })
    }
}

// ---------------------------------------------------------------------------
// Instruction → type, fields, transfer
// ---------------------------------------------------------------------------

/// A decoded instruction, before it is tied to its row.
struct Described {
    instruction_type: &'static str,
    fields: Map<String, Value>,
    transfer: Option<Transfer>,
}

/// A token movement and where its accounts sit in the instruction.
struct Transfer {
    amount: u64,
    /// `None` for a plain `Transfer`, whose instruction has no mint.
    checked: Option<Checked>,
}

/// What a checked transfer adds: the mint account and the decimals.
struct Checked {
    decimals: u8,
}

impl Transfer {
    const fn unchecked(amount: u64) -> Self {
        Self {
            amount,
            checked: None,
        }
    }

    const fn checked(amount: u64, decimals: u8) -> Self {
        Self {
            amount,
            checked: Some(Checked { decimals }),
        }
    }

    /// The transfer row of `instruction`. Accounts are
    /// `[source, destination, authority]` for `Transfer` and
    /// `[source, mint, destination, authority]` for the checked variants.
    fn row(&self, instruction: &DeepAnaInstruction) -> PrismResult<DeepAnaTokenTransfer> {
        let (destination_at, required) = match self.checked {
            None => (1, 3),
            Some(_) => (2, 4),
        };
        let accounts = &instruction.accounts;
        if accounts.len() < required {
            return Err(decode_error(
                instruction,
                format!(
                    "transfer needs at least {required} accounts, the instruction has {}",
                    accounts.len()
                ),
            ));
        }
        Ok(DeepAnaTokenTransfer {
            slot: instruction.slot,
            tx_signature: instruction.tx_signature.clone(),
            instruction_index: instruction.instruction_index,
            inner_instruction_index: instruction.inner_instruction_index,
            spl_token_program_id: instruction.program_id.clone(),
            source_token_account: accounts[0].clone(),
            destination_token_account: accounts[destination_at].clone(),
            source_owner: None,
            destination_owner: None,
            mint_token_address: self.checked.as_ref().map(|_| accounts[1].clone()),
            amount_raw: self.amount,
            decimals: self.checked.as_ref().map(|checked| checked.decimals),
            block_time: None,
        })
    }
}

fn described(instruction_type: &'static str, fields: Value) -> Described {
    let Value::Object(fields) = fields else {
        unreachable!("fields are always built with json!({{...}})")
    };
    Described {
        instruction_type,
        fields,
        transfer: None,
    }
}

fn with_transfer(mut described: Described, transfer: Transfer) -> Described {
    described.transfer = Some(transfer);
    described
}

fn pubkey(key: impl Into<Pubkey>) -> Value {
    Value::String(key.into().to_string())
}

/// A `COption<Pubkey>` (or any `Option`-like of a key) as a key or `null`.
fn optional_pubkey(key: impl Into<Option<Pubkey>>) -> Value {
    key.into().map_or(Value::Null, pubkey)
}

fn debug_list<T: std::fmt::Debug>(items: &[T]) -> Value {
    items.iter().map(|item| Value::String(format!("{item:?}"))).collect()
}

/// Type name, JSON fields and transfer of `ix`. `data` is the raw
/// instruction, for the extension instructions `unpack` does not open.
fn describe(ix: TokenInstruction<'_>, data: &[u8]) -> Result<Described, String> {
    use TokenInstruction as T;
    Ok(match ix {
        T::InitializeMint {
            decimals,
            mint_authority,
            freeze_authority,
        } => described(
            "InitializeMint",
            json!({
                "decimals": decimals,
                "mint_authority": pubkey(mint_authority),
                "freeze_authority": optional_pubkey(freeze_authority),
            }),
        ),
        T::InitializeMint2 {
            decimals,
            mint_authority,
            freeze_authority,
        } => described(
            "InitializeMint2",
            json!({
                "decimals": decimals,
                "mint_authority": pubkey(mint_authority),
                "freeze_authority": optional_pubkey(freeze_authority),
            }),
        ),
        T::InitializeAccount => described("InitializeAccount", json!({})),
        T::InitializeAccount2 { owner } => {
            described("InitializeAccount2", json!({ "owner": pubkey(owner) }))
        }
        T::InitializeAccount3 { owner } => {
            described("InitializeAccount3", json!({ "owner": pubkey(owner) }))
        }
        T::InitializeMultisig { m } => described("InitializeMultisig", json!({ "m": m })),
        T::InitializeMultisig2 { m } => described("InitializeMultisig2", json!({ "m": m })),
        T::Transfer { amount } => with_transfer(
            described("Transfer", json!({ "amount": amount })),
            Transfer::unchecked(amount),
        ),
        T::TransferChecked { amount, decimals } => with_transfer(
            described("TransferChecked", json!({ "amount": amount, "decimals": decimals })),
            Transfer::checked(amount, decimals),
        ),
        T::Approve { amount } => described("Approve", json!({ "amount": amount })),
        T::ApproveChecked { amount, decimals } => described(
            "ApproveChecked",
            json!({ "amount": amount, "decimals": decimals }),
        ),
        T::Revoke => described("Revoke", json!({})),
        T::SetAuthority {
            authority_type,
            new_authority,
        } => described(
            "SetAuthority",
            json!({
                "authority_type": format!("{authority_type:?}"),
                "new_authority": optional_pubkey(new_authority),
            }),
        ),
        T::MintTo { amount } => described("MintTo", json!({ "amount": amount })),
        T::MintToChecked { amount, decimals } => described(
            "MintToChecked",
            json!({ "amount": amount, "decimals": decimals }),
        ),
        T::Burn { amount } => described("Burn", json!({ "amount": amount })),
        T::BurnChecked { amount, decimals } => described(
            "BurnChecked",
            json!({ "amount": amount, "decimals": decimals }),
        ),
        T::CloseAccount => described("CloseAccount", json!({})),
        T::FreezeAccount => described("FreezeAccount", json!({})),
        T::ThawAccount => described("ThawAccount", json!({})),
        T::SyncNative => described("SyncNative", json!({})),
        T::GetAccountDataSize { extension_types } => described(
            "GetAccountDataSize",
            json!({ "extension_types": debug_list(&extension_types) }),
        ),
        T::InitializeImmutableOwner => described("InitializeImmutableOwner", json!({})),
        T::AmountToUiAmount { amount } => {
            described("AmountToUiAmount", json!({ "amount": amount }))
        }
        T::UiAmountToAmount { ui_amount } => {
            described("UiAmountToAmount", json!({ "ui_amount": ui_amount }))
        }
        T::InitializeMintCloseAuthority { close_authority } => described(
            "InitializeMintCloseAuthority",
            json!({ "close_authority": optional_pubkey(close_authority) }),
        ),
        T::Reallocate { extension_types } => described(
            "Reallocate",
            json!({ "extension_types": debug_list(&extension_types) }),
        ),
        T::CreateNativeMint => described("CreateNativeMint", json!({})),
        T::InitializeNonTransferableMint => described("InitializeNonTransferableMint", json!({})),
        T::InitializePermanentDelegate { delegate } => described(
            "InitializePermanentDelegate",
            json!({ "delegate": pubkey(delegate) }),
        ),
        T::WithdrawExcessLamports => described("WithdrawExcessLamports", json!({})),
        T::UnwrapLamports { amount } => described(
            "UnwrapLamports",
            json!({ "amount": Option::<u64>::from(amount) }),
        ),
        T::Batch { data } => described("Batch", json!({ "data_len": data.len() })),
        T::TransferFeeExtension => describe_transfer_fee(data)?,
        T::ConfidentialTransferExtension => extension("ConfidentialTransferExtension", data),
        T::DefaultAccountStateExtension => extension("DefaultAccountStateExtension", data),
        T::MemoTransferExtension => extension("MemoTransferExtension", data),
        T::InterestBearingMintExtension => extension("InterestBearingMintExtension", data),
        T::CpiGuardExtension => extension("CpiGuardExtension", data),
        T::TransferHookExtension => extension("TransferHookExtension", data),
        T::ConfidentialTransferFeeExtension => {
            extension("ConfidentialTransferFeeExtension", data)
        }
        T::MetadataPointerExtension => extension("MetadataPointerExtension", data),
        T::GroupPointerExtension => extension("GroupPointerExtension", data),
        T::GroupMemberPointerExtension => extension("GroupMemberPointerExtension", data),
        T::ConfidentialMintBurnExtension => extension("ConfidentialMintBurnExtension", data),
        T::ScaledUiAmountExtension => extension("ScaledUiAmountExtension", data),
        T::PausableExtension => extension("PausableExtension", data),
        T::PermissionedBurnExtension => extension("PermissionedBurnExtension", data),
    })
}

/// An extension instruction this decoder does not open: named after the
/// extension, with the extension's own instruction tag.
fn extension(instruction_type: &'static str, data: &[u8]) -> Described {
    described(
        instruction_type,
        json!({ "extension_instruction_tag": data.get(1) }),
    )
}

/// `TransferFeeExtension`: opened, because one of its instructions,
/// `TransferCheckedWithFee`, moves tokens.
fn describe_transfer_fee(data: &[u8]) -> Result<Described, String> {
    debug_assert_eq!(data.first(), Some(&TRANSFER_FEE_EXTENSION_TAG));
    let ix = TransferFeeInstruction::unpack(&data[1..])
        .map_err(|err| format!("invalid transfer fee instruction: {err}"))?;
    use TransferFeeInstruction as F;
    Ok(match ix {
        F::InitializeTransferFeeConfig {
            transfer_fee_config_authority,
            withdraw_withheld_authority,
            transfer_fee_basis_points,
            maximum_fee,
        } => described(
            "InitializeTransferFeeConfig",
            json!({
                "transfer_fee_config_authority": optional_pubkey(transfer_fee_config_authority),
                "withdraw_withheld_authority": optional_pubkey(withdraw_withheld_authority),
                "transfer_fee_basis_points": transfer_fee_basis_points,
                "maximum_fee": maximum_fee,
            }),
        ),
        F::TransferCheckedWithFee {
            amount,
            decimals,
            fee,
        } => with_transfer(
            described(
                "TransferCheckedWithFee",
                json!({ "amount": amount, "decimals": decimals, "fee": fee }),
            ),
            Transfer::checked(amount, decimals),
        ),
        F::WithdrawWithheldTokensFromMint => described("WithdrawWithheldTokensFromMint", json!({})),
        F::WithdrawWithheldTokensFromAccounts { num_token_accounts } => described(
            "WithdrawWithheldTokensFromAccounts",
            json!({ "num_token_accounts": num_token_accounts }),
        ),
        F::HarvestWithheldTokensToMint => described("HarvestWithheldTokensToMint", json!({})),
        F::SetTransferFee {
            transfer_fee_basis_points,
            maximum_fee,
        } => described(
            "SetTransferFee",
            json!({
                "transfer_fee_basis_points": transfer_fee_basis_points,
                "maximum_fee": maximum_fee,
            }),
        ),
    })
}
