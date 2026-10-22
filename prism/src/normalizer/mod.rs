//! The normalizer: turns a raw [`ChainBlock`] into UDM rows.
//!
//! [`Normalizer`] sits between the slot processor and the decoder. It takes
//! the block apart in three steps, one sub-normalizer each:
//!
//! ```text
//!   ChainBlock ──▶ SlotNormalizer ─────────▶ DeepAnaSlot
//!        │
//!        └─ each tx ─▶ TransactionNormalizer ──▶ DeepAnaTransaction
//!                            │ (decoded message)
//!                            └──▶ InstructionNormalizer ──▶ Vec<DeepAnaInstruction>
//! ```
//!
//! | File              | Holds                                                    |
//! |-------------------|----------------------------------------------------------|
//! | `mod.rs`          | [`Normalizer`], [`NormalizedBlock`], [`NormalizerMetrics`] |
//! | [`slot`]          | [`SlotNormalizer`]: block header → slot row              |
//! | [`transaction`]   | [`TransactionNormalizer`]: one transaction → its row     |
//! | [`instruction`]   | [`InstructionNormalizer`]: instruction tree → flat rows  |
//!
//! A block is normalized whole or not at all: the first transaction that
//! cannot be read fails the block, so no caller ever sees a block with rows
//! missing. Every transaction is kept, failed ones and votes included;
//! filtering is left to the stages downstream.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::adapter::{ChainBlock, ChainBlockData};
use crate::types::error::PrismResult;
use crate::types::udm::{DeepAnaInstruction, DeepAnaSlot, DeepAnaTransaction};

pub mod instruction;
pub mod slot;
pub mod transaction;

pub use instruction::InstructionNormalizer;
pub use slot::SlotNormalizer;
pub use transaction::{DecodedTransaction, TransactionNormalizer};

/// The UDM rows of one block.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NormalizedBlock {
    pub slot: DeepAnaSlot,
    /// In block order.
    pub transactions: Vec<DeepAnaTransaction>,
    /// Transaction by transaction in block order; within one transaction, in
    /// the order [`InstructionNormalizer`] describes.
    pub instructions: Vec<DeepAnaInstruction>,
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters a [`Normalizer`] keeps. Rows are counted only for blocks that
/// normalized in full; every counter except `last_normalized_slot` only ever
/// goes up.
#[derive(Debug, Default)]
pub struct NormalizerMetrics {
    blocks_normalized: AtomicU64,
    blocks_failed: AtomicU64,
    transactions_normalized: AtomicU64,
    top_level_instructions: AtomicU64,
    inner_instructions: AtomicU64,
    last_normalized_slot: AtomicU64,
}

/// A point-in-time copy of [`NormalizerMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NormalizerMetricsSnapshot {
    pub blocks_normalized: u64,
    /// Blocks that could not be read; they produced no rows.
    pub blocks_failed: u64,
    pub transactions_normalized: u64,
    pub top_level_instructions: u64,
    /// Instructions invoked by other instructions (CPIs).
    pub inner_instructions: u64,
    /// Most recently normalized slot; 0 before the first.
    pub last_normalized_slot: u64,
}

impl NormalizerMetricsSnapshot {
    /// Instruction rows produced, top-level and inner.
    pub const fn instructions_normalized(&self) -> u64 {
        self.top_level_instructions + self.inner_instructions
    }
}

impl NormalizerMetrics {
    pub fn snapshot(&self) -> NormalizerMetricsSnapshot {
        NormalizerMetricsSnapshot {
            blocks_normalized: self.blocks_normalized.load(Ordering::Relaxed),
            blocks_failed: self.blocks_failed.load(Ordering::Relaxed),
            transactions_normalized: self.transactions_normalized.load(Ordering::Relaxed),
            top_level_instructions: self.top_level_instructions.load(Ordering::Relaxed),
            inner_instructions: self.inner_instructions.load(Ordering::Relaxed),
            last_normalized_slot: self.last_normalized_slot.load(Ordering::Relaxed),
        }
    }

    fn record_block(&self, block: &NormalizedBlock) {
        let inner = block.instructions.iter().filter(|ix| ix.is_inner()).count() as u64;
        let top_level = block.instructions.len() as u64 - inner;
        self.transactions_normalized
            .fetch_add(block.transactions.len() as u64, Ordering::Relaxed);
        self.top_level_instructions.fetch_add(top_level, Ordering::Relaxed);
        self.inner_instructions.fetch_add(inner, Ordering::Relaxed);
        self.blocks_normalized.fetch_add(1, Ordering::Relaxed);
        self.last_normalized_slot.store(block.slot.slot, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.blocks_failed.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Normalizer
// ---------------------------------------------------------------------------

/// Maps raw blocks onto the UDM. Cheap to clone; clones share their metrics.
#[derive(Debug, Clone, Default)]
pub struct Normalizer {
    slot_normalizer: SlotNormalizer,
    transaction_normalizer: TransactionNormalizer,
    instruction_normalizer: InstructionNormalizer,
    metrics: Arc<NormalizerMetrics>,
}

impl Normalizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// The normalizer's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<NormalizerMetrics> {
        &self.metrics
    }

    /// The UDM rows of `block`.
    ///
    /// Fails with [`PrismError::DeserializeError`] when the block header or
    /// any of its transactions cannot be read; see the sub-normalizers for
    /// the exact cases.
    ///
    /// [`PrismError::DeserializeError`]: crate::types::error::PrismError::DeserializeError
    pub fn normalize(&self, block: &ChainBlock) -> PrismResult<NormalizedBlock> {
        let result = self.normalize_block(block);
        match &result {
            Ok(normalized) => self.metrics.record_block(normalized),
            Err(_) => self.metrics.record_failure(),
        }
        result
    }

    fn normalize_block(&self, block: &ChainBlock) -> PrismResult<NormalizedBlock> {
        let ChainBlockData::Solana(raw) = &block.raw;
        let slot_number = block.slot_or_height;
        let slot = self.slot_normalizer.normalize(slot_number, raw)?;

        let mut transactions = Vec::with_capacity(raw.transactions.len());
        let mut instructions = Vec::new();
        for (index, tx_with_meta) in raw.transactions.iter().enumerate() {
            let (row, decoded) = self.transaction_normalizer.normalize_with_message(
                slot_number,
                &slot.blockhash,
                tx_with_meta,
                index,
            )?;
            instructions.extend(self.instruction_normalizer.normalize(
                &row.tx_signature,
                slot_number,
                &decoded.account_keys,
                &decoded.instructions,
                transaction::inner_instructions(tx_with_meta.meta.as_ref()),
            )?);
            transactions.push(row);
        }
        Ok(NormalizedBlock {
            slot,
            transactions,
            instructions,
        })
    }
}
