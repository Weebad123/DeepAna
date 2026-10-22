//! A transaction's instruction tree → flat [`DeepAnaInstruction`] rows.
//!
//! A transaction runs its top-level instructions in message order; each may
//! invoke other programs (CPIs), which the RPC reports in
//! `meta.innerInstructions`: one group per top-level instruction that made
//! calls, keyed by that instruction's index, holding every call it made in
//! execution order (nested calls included, flattened, their depth only in
//! `stackHeight`).
//!
//! Each instruction becomes one row whose place in the tree is its key:
//!
//! | Row                            | `instruction_index` | `inner_instruction_index` |
//! |--------------------------------|---------------------|---------------------------|
//! | top-level instruction `i`      | `i`                 | `None`                    |
//! | call `j` made by instruction `i` | `i`               | `Some(j)`                 |
//!
//! so `(tx_signature, instruction_index, inner_instruction_index)` is unique
//! and any instruction can be queried as a plain row.
//!
//! Rows come out in a fixed order: every top-level instruction in message
//! order, then the inner instructions grouped by their top-level
//! instruction, groups in top-level order and calls in execution order:
//!
//! ```text
//!   (0, -) (1, -) (2, -) │ (0, 0) (0, 1) │ (2, 0)
//!   ──── top-level ───── │ ─ under 0 ─── │ under 2
//! ```

use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{UiInnerInstructions, UiInstruction};

use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::DeepAnaInstruction;

/// Flattens a transaction's instruction tree into rows. Stateless.
#[derive(Debug, Clone, Copy, Default)]
pub struct InstructionNormalizer;

impl InstructionNormalizer {
    pub const fn new() -> Self {
        Self
    }

    /// The rows of one transaction's instructions, in the order described in
    /// the [module docs](self). Account indexes are resolved against
    /// `account_keys`, which must be the full list (static then loaded keys).
    /// The decoder columns are left empty for the decoder to fill.
    ///
    /// Fails with [`PrismError::DeserializeError`] when an instruction points
    /// past `account_keys`, an inner group names a top-level instruction that
    /// does not exist or appears twice, or an inner instruction came parsed
    /// (`jsonParsed`) rather than compiled.
    pub fn normalize(
        &self,
        tx_signature: &str,
        slot: u64,
        account_keys: &[Pubkey],
        instructions: &[CompiledInstruction],
        inner_instructions: Option<&[UiInnerInstructions]>,
    ) -> PrismResult<Vec<DeepAnaInstruction>> {
        let walker = Walker {
            tx_signature,
            slot,
            keys: account_keys.iter().map(ToString::to_string).collect(),
        };
        let mut groups: Vec<&UiInnerInstructions> =
            inner_instructions.unwrap_or_default().iter().collect();
        // The RPC sends groups in order already; sorting makes it a guarantee.
        groups.sort_by_key(|group| group.index);

        let inner_count: usize = groups.iter().map(|group| group.instructions.len()).sum();
        let mut rows = Vec::with_capacity(instructions.len() + inner_count);

        for (i, ix) in instructions.iter().enumerate() {
            let position = Position::top_level(walker.index(i)?);
            rows.push(walker.row(
                position,
                ix.program_id_index,
                &ix.accounts,
                bs58::encode(&ix.data).into_string(),
            )?);
        }

        let mut previous_parent = None;
        for group in groups {
            let parent = u32::from(group.index);
            if usize::from(group.index) >= instructions.len() {
                return Err(walker.error(
                    Position::top_level(parent),
                    format!(
                        "has inner instructions but the transaction has only {} instructions",
                        instructions.len()
                    ),
                ));
            }
            if previous_parent == Some(parent) {
                return Err(walker.error(
                    Position::top_level(parent),
                    "has two groups of inner instructions",
                ));
            }
            previous_parent = Some(parent);
            for (j, inner) in group.instructions.iter().enumerate() {
                let position = Position::inner(parent, walker.index(j)?);
                let UiInstruction::Compiled(ix) = inner else {
                    return Err(walker.error(
                        position,
                        "came parsed and no longer carries its raw data; \
                         fetch blocks as base58, base64 or json",
                    ));
                };
                rows.push(walker.row(position, ix.program_id_index, &ix.accounts, ix.data.clone())?);
            }
        }
        Ok(rows)
    }
}

/// Where an instruction sits in the tree.
#[derive(Debug, Clone, Copy)]
struct Position {
    instruction_index: u32,
    inner_instruction_index: Option<u32>,
}

impl Position {
    const fn top_level(instruction_index: u32) -> Self {
        Self {
            instruction_index,
            inner_instruction_index: None,
        }
    }

    const fn inner(instruction_index: u32, inner_instruction_index: u32) -> Self {
        Self {
            instruction_index,
            inner_instruction_index: Some(inner_instruction_index),
        }
    }
}

/// What every row of one transaction shares.
struct Walker<'a> {
    tx_signature: &'a str,
    slot: u64,
    /// `account_keys` as base58, converted once per transaction.
    keys: Vec<String>,
}

impl Walker<'_> {
    fn row(
        &self,
        position: Position,
        program_id_index: u8,
        accounts: &[u8],
        data_base58: String,
    ) -> PrismResult<DeepAnaInstruction> {
        Ok(DeepAnaInstruction {
            slot: self.slot,
            tx_signature: self.tx_signature.to_owned(),
            instruction_index: position.instruction_index,
            inner_instruction_index: position.inner_instruction_index,
            program_id: self.key(position, program_id_index)?,
            accounts: accounts
                .iter()
                .map(|&account| self.key(position, account))
                .collect::<PrismResult<_>>()?,
            data_base58,
            instruction_type: None,
            decoded_data: None,
        })
    }

    fn key(&self, position: Position, index: u8) -> PrismResult<String> {
        self.keys.get(usize::from(index)).cloned().ok_or_else(|| {
            self.error(
                position,
                format!(
                    "references account {index}, but the transaction has {} account keys",
                    self.keys.len()
                ),
            )
        })
    }

    /// A position in an instruction list as a row index.
    fn index(&self, index: usize) -> PrismResult<u32> {
        u32::try_from(index).map_err(|_| {
            PrismError::deserialize(
                Some(self.slot),
                format!("transaction {}", self.tx_signature),
                format!("instruction position {index} does not fit a u32"),
            )
        })
    }

    fn error(&self, position: Position, reason: impl Into<String>) -> PrismError {
        let at = match position.inner_instruction_index {
            None => position.instruction_index.to_string(),
            Some(inner) => format!("{}.{inner}", position.instruction_index),
        };
        PrismError::deserialize(
            Some(self.slot),
            format!("instruction {at} of transaction {}", self.tx_signature),
            reason.into(),
        )
    }
}
