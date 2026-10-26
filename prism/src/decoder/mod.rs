//! The decoder: gives raw [`DeepAnaInstruction`] rows a type and a payload,
//! and turns token movements into [`DeepAnaTokenTransfer`] rows.
//!
//! [`DecoderRegistry`] sits after the normalizer. For every instruction it
//! looks up the decoders registered for the instruction's program, asks each
//! in turn whether it [can decode](Decoder::can_decode) the data, and hands
//! the instruction to the first that says yes:
//!
//! ```text
//!   DeepAnaInstruction ──▶ DecoderRegistry ──program_id──▶ [Decoder, ...]
//!                                                              │ first can_decode
//!                                                              ▼
//!                         DecodeResult { instruction_type, decoded_data, token_transfer }
//! ```
//!
//! | File              | Holds                                                         |
//! |-------------------|---------------------------------------------------------------|
//! | `mod.rs`          | [`Decoder`], [`DecodeResult`], [`DecoderRegistry`], [`RegistryMetrics`] |
//! | [`discriminant`]  | [`Discriminant`]: the 8-byte Anchor instruction prefix         |
//! | [`spl_token`]     | [`SplTokenDecoder`]: SPL Token and Token-2022                  |
//! | [`anchor`]        | [`AnchorDecoder`]: any Anchor program, given its IDL           |
//!
//! Most programs on chain have no decoder registered; their instructions are
//! counted as [`no_decoder_found`](RegistryMetricsSnapshot::no_decoder_found)
//! and left undecoded. A decoder that claims an instruction and then fails
//! on it is a different matter, counted as a
//! [`decode_error`](RegistryMetricsSnapshot::decode_errors): either the data
//! is malformed (possible in failed transactions) or the decoder is wrong.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::Value;

use crate::normalizer::NormalizedBlock;
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::{DeepAnaInstruction, DeepAnaTokenTransfer, to_json_string};

pub mod anchor;
pub mod discriminant;
pub mod spl_token;

pub use anchor::AnchorDecoder;
pub use discriminant::Discriminant;
pub use spl_token::SplTokenDecoder;

// ---------------------------------------------------------------------------
// Decoder
// ---------------------------------------------------------------------------

/// What a decoder makes of one instruction.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodeResult {
    /// Goes to [`DeepAnaInstruction::instruction_type`]: the SPL variant
    /// name (`"TransferChecked"`) or the IDL instruction name
    /// (`"initialize_pool"`).
    pub instruction_type: String,
    /// Goes to [`DeepAnaInstruction::decoded_data`]: the instruction's
    /// arguments as a JSON object.
    pub decoded_data: Value,
    /// The token movement the instruction makes, for the instructions that
    /// move tokens between accounts. `block_time` is left `None`: the
    /// instruction row does not carry it ([`DecoderRegistry::decode_block`]
    /// fills it in), and owners, mint and decimals that are not in the
    /// instruction are left for the account resolver.
    pub token_transfer: Option<DeepAnaTokenTransfer>,
}

impl DecodeResult {
    /// `decoded_data` as it is stored in Parquet.
    pub fn decoded_data_json(&self) -> String {
        to_json_string(&self.decoded_data)
    }
}

/// Decodes the instructions of one program.
pub trait Decoder: Send + Sync {
    /// Base58 id of the program this decoder handles; the registry files the
    /// decoder under it.
    fn program_id(&self) -> &str;

    /// Whether this decoder recognises `data` sent to `program_id`. Cheap:
    /// it looks at the instruction tag only and does not validate the body.
    fn can_decode(&self, program_id: &str, data: &[u8]) -> bool;

    /// Decodes `instruction`.
    ///
    /// The instruction's accounts are read by position from
    /// [`DeepAnaInstruction::accounts`], which the normalizer has already
    /// resolved from indexes into pubkeys. `account_keys` is the whole
    /// transaction's account list (static then loaded), for decoders that
    /// need context beyond the instruction itself.
    ///
    /// Fails with [`PrismError::DecodeError`] when the data does not parse
    /// or the instruction has fewer accounts than it requires.
    fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        account_keys: &[String],
    ) -> Result<DecodeResult, PrismError>;
}

/// A [`PrismError::DecodeError`] for `instruction`. The error type stores a
/// `u8` index; an inner instruction's position is added to the reason.
pub(crate) fn decode_error(instruction: &DeepAnaInstruction, reason: impl Into<String>) -> PrismError {
    let reason = reason.into();
    let reason = match instruction.inner_instruction_index {
        None => reason,
        Some(inner) => format!("inner instruction {inner}: {reason}"),
    };
    PrismError::decode(
        &instruction.program_id,
        u8::try_from(instruction.instruction_index).unwrap_or(u8::MAX),
        reason,
    )
}

/// The instruction's data bytes, or a decode error when `data_base58` is not
/// valid base58.
pub(crate) fn instruction_data(instruction: &DeepAnaInstruction) -> PrismResult<Vec<u8>> {
    instruction
        .data_bytes()
        .map_err(|err| decode_error(instruction, format!("data is not valid base58: {err}")))
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters a [`DecoderRegistry`] keeps. Every decode attempt lands in
/// exactly one of `successful_decodes`, `no_decoder_found` and
/// `decode_errors`, so those three always add up to `total_decode_attempts`.
#[derive(Debug, Default)]
pub struct RegistryMetrics {
    total_decode_attempts: AtomicU64,
    successful_decodes: AtomicU64,
    no_decoder_found: AtomicU64,
    decode_errors: AtomicU64,
    token_transfers_produced: AtomicU64,
}

/// A point-in-time copy of [`RegistryMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegistryMetricsSnapshot {
    pub total_decode_attempts: u64,
    pub successful_decodes: u64,
    /// No decoder registered for the program, or none recognised the data.
    /// Expected to be most instructions.
    pub no_decoder_found: u64,
    /// A decoder claimed the instruction and then failed on it.
    pub decode_errors: u64,
    pub token_transfers_produced: u64,
}

impl RegistryMetricsSnapshot {
    /// Of the instructions some decoder claimed, the share it decoded:
    /// `successful / (successful + errors)`. `None` before any was claimed.
    pub fn decode_success_rate(&self) -> Option<f64> {
        let claimed = self.successful_decodes + self.decode_errors;
        (claimed > 0).then(|| self.successful_decodes as f64 / claimed as f64)
    }
}

impl RegistryMetrics {
    pub fn snapshot(&self) -> RegistryMetricsSnapshot {
        RegistryMetricsSnapshot {
            total_decode_attempts: self.total_decode_attempts.load(Ordering::Relaxed),
            successful_decodes: self.successful_decodes.load(Ordering::Relaxed),
            no_decoder_found: self.no_decoder_found.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
            token_transfers_produced: self.token_transfers_produced.load(Ordering::Relaxed),
        }
    }

    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// Routes instructions to the decoders of their program.
#[derive(Default)]
pub struct DecoderRegistry {
    /// Keyed by base58 program id. Several decoders may share a program;
    /// they are asked in registration order.
    decoders: HashMap<String, Vec<Box<dyn Decoder>>>,
    metrics: Arc<RegistryMetrics>,
}

impl std::fmt::Debug for DecoderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut programs: Vec<(&str, usize)> =
            self.decoders.iter().map(|(id, list)| (id.as_str(), list.len())).collect();
        programs.sort_unstable();
        f.debug_struct("DecoderRegistry")
            .field("decoders", &programs)
            .field("metrics", &self.metrics.snapshot())
            .finish()
    }
}

impl DecoderRegistry {
    /// An empty registry: every instruction is `no_decoder_found`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A registry with decoders for SPL Token and Token-2022.
    pub fn with_spl_token() -> Self {
        let mut registry = Self::new();
        registry.register(Box::new(SplTokenDecoder::spl_token()));
        registry.register(Box::new(SplTokenDecoder::token_2022()));
        registry
    }

    /// Adds `decoder` under its [`Decoder::program_id`], after any decoders
    /// already registered for that program.
    pub fn register(&mut self, decoder: Box<dyn Decoder>) -> &mut Self {
        self.decoders
            .entry(decoder.program_id().to_owned())
            .or_default()
            .push(decoder);
        self
    }

    /// Whether any decoder is registered for `program_id`.
    pub fn has_decoder(&self, program_id: &str) -> bool {
        self.decoders.contains_key(program_id)
    }

    /// The programs with at least one decoder, in no particular order.
    pub fn program_ids(&self) -> impl Iterator<Item = &str> {
        self.decoders.keys().map(String::as_str)
    }

    /// Number of registered decoders, across all programs.
    pub fn len(&self) -> usize {
        self.decoders.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.decoders.is_empty()
    }

    /// The registry's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<RegistryMetrics> {
        &self.metrics
    }

    /// Decodes one instruction with the first of its program's decoders
    /// that recognises it. `Ok(None)` when no decoder does.
    ///
    /// Fails with [`PrismError::DecodeError`] when the instruction's data is
    /// not base58, or the decoder that claimed it failed.
    pub fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        account_keys: &[String],
    ) -> PrismResult<Option<DecodeResult>> {
        let metrics = &self.metrics;
        RegistryMetrics::bump(&metrics.total_decode_attempts);
        let Some(candidates) = self.decoders.get(&instruction.program_id) else {
            RegistryMetrics::bump(&metrics.no_decoder_found);
            return Ok(None);
        };
        let data = instruction_data(instruction).inspect_err(|_| {
            RegistryMetrics::bump(&metrics.decode_errors);
        })?;
        let Some(decoder) = candidates
            .iter()
            .find(|decoder| decoder.can_decode(&instruction.program_id, &data))
        else {
            RegistryMetrics::bump(&metrics.no_decoder_found);
            return Ok(None);
        };
        match decoder.decode(instruction, account_keys) {
            Ok(result) => {
                RegistryMetrics::bump(&metrics.successful_decodes);
                if result.token_transfer.is_some() {
                    RegistryMetrics::bump(&metrics.token_transfers_produced);
                }
                Ok(Some(result))
            }
            Err(err) => {
                RegistryMetrics::bump(&metrics.decode_errors);
                Err(err)
            }
        }
    }

    /// Decodes every instruction of `block` in place, filling
    /// `instruction_type` and `decoded_data`, and returns the token
    /// transfers found, in instruction order, with the block's
    /// `block_time`.
    ///
    /// One instruction failing does not fail the block: it is logged,
    /// counted and left undecoded.
    pub fn decode_block(&self, block: &mut NormalizedBlock) -> Vec<DeepAnaTokenTransfer> {
        let NormalizedBlock {
            slot,
            transactions,
            instructions,
        } = block;
        let accounts_by_signature: HashMap<&str, &[String]> = transactions
            .iter()
            .map(|tx| (tx.tx_signature.as_str(), tx.accounts.as_slice()))
            .collect();

        let mut transfers = Vec::new();
        for instruction in instructions.iter_mut() {
            let account_keys = accounts_by_signature
                .get(instruction.tx_signature.as_str())
                .copied()
                .unwrap_or_default();
            match self.decode(instruction, account_keys) {
                Ok(Some(result)) => {
                    instruction.instruction_type = Some(result.instruction_type);
                    instruction.decoded_data = Some(result.decoded_data);
                    if let Some(mut transfer) = result.token_transfer {
                        transfer.block_time = slot.block_time;
                        transfers.push(transfer);
                    }
                }
                Ok(None) => {}
                Err(err) => tracing::debug!(
                    slot = instruction.slot,
                    tx = %instruction.tx_signature,
                    "instruction left undecoded: {err}"
                ),
            }
        }
        transfers
    }
}
