//! The normalizer: turns a raw [`ChainBlock`] into UDM rows.
//!
//! Only the slot row is produced so far, straight from the block header.
//! Transaction and instruction rows come empty until [`slot`],
//! [`transaction`] and [`instruction`] are built; the [`NormalizedBlock`]
//! shape is already final, so callers do not change when they are.

use crate::adapter::{ChainBlock, ChainBlockData};
use crate::types::error::PrismResult;
use crate::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTransaction};

pub mod slot;
pub mod transaction;
pub mod instruction;

/// The UDM rows of one block.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NormalizedBlock {
    pub slot: DeepAnaSlot,
    pub transactions: Vec<DeepAnaTransaction>,
    pub instructions: Vec<DeepAnaInstruction>,
}

/// Maps raw blocks onto the UDM. Stateless and cheap to share.
#[derive(Debug, Clone, Copy, Default)]
pub struct Normalizer;

impl Normalizer {
    pub const fn new() -> Self {
        Self
    }

    /// The UDM rows of `block`.
    pub fn normalize(&self, block: &ChainBlock) -> PrismResult<NormalizedBlock> {
        let ChainBlockData::Solana(raw) = &block.raw;
        Ok(NormalizedBlock {
            slot: DeepAnaSlot {
                slot: block.slot_or_height,
                blockhash: block.block_id.clone(),
                parent_slot: raw.parent_slot,
                block_time: block.timestamp,
                block_height: raw.block_height,
                transaction_count: u32::try_from(raw.transactions.len()).unwrap_or(u32::MAX),
                leader: None,
            },
            transactions: Vec::new(),
            instructions: Vec::new(),
        })
    }
}
