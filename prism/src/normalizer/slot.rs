//! Block header → [`DeepAnaSlot`].
//!
//! | `DeepAnaSlot`       | Taken from                                              |
//! |---------------------|---------------------------------------------------------|
//! | `slot`              | the slot the block was fetched for (not in the payload) |
//! | `blockhash`         | `blockhash`                                             |
//! | `parent_slot`       | `parentSlot`                                            |
//! | `block_time`        | `blockTime`, `None` for old slots                       |
//! | `block_height`      | `blockHeight`, `None` when the RPC omits it             |
//! | `transaction_count` | length of `transactions`                                |
//! | `leader`            | the `fee` reward's pubkey, `None` without rewards       |
//!
//! `previousBlockhash` has no column in the slot table; it travels as
//! [`ChainBlock::parent_id`](crate::adapter::ChainBlock::parent_id).

use solana_transaction_status::{EncodedConfirmedBlock, RewardType};

use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::DeepAnaSlot;

/// Maps the header of an [`EncodedConfirmedBlock`] onto a [`DeepAnaSlot`].
/// Stateless.
#[derive(Debug, Clone, Copy, Default)]
pub struct SlotNormalizer;

impl SlotNormalizer {
    pub const fn new() -> Self {
        Self
    }

    /// The slot row of `block`, which the RPC served for `slot`.
    ///
    /// Optional fields the RPC left out stay `None`. The one error is a
    /// [`PrismError::DeserializeError`] for an empty blockhash: every block
    /// has one, so its absence means the response is malformed.
    pub fn normalize(&self, slot: u64, block: &EncodedConfirmedBlock) -> PrismResult<DeepAnaSlot> {
        if block.blockhash.is_empty() {
            return Err(PrismError::deserialize(
                Some(slot),
                "block header",
                "the blockhash is empty",
            ));
        }
        Ok(DeepAnaSlot {
            slot,
            blockhash: block.blockhash.clone(),
            parent_slot: block.parent_slot,
            block_time: block.block_time,
            block_height: block.block_height,
            transaction_count: u32::try_from(block.transactions.len()).unwrap_or(u32::MAX),
            leader: leader(block),
        })
    }
}

/// The validator that produced the block: the one credited with its fee
/// reward. `None` when rewards were not requested or the block paid none.
fn leader(block: &EncodedConfirmedBlock) -> Option<String> {
    block
        .rewards
        .iter()
        .find(|reward| reward.reward_type == Some(RewardType::Fee))
        .map(|reward| reward.pubkey.clone())
}
