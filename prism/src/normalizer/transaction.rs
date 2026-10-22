//! One transaction of a block → [`DeepAnaTransaction`].
//!
//! A transaction arrives in two halves: the transaction itself, encoded the
//! way the adapter asked for it, and its status meta (fee, error, logs, inner
//! instructions, ...), which is always JSON. [`TransactionNormalizer::decode`]
//! takes the first half apart into a [`DecodedTransaction`];
//! [`TransactionNormalizer::normalize`] joins it with the meta into a row.
//!
//! | Encoding            | How the transaction is read                              |
//! |---------------------|----------------------------------------------------------|
//! | `base58` / `base64` | the SDK's decoder (every version, sanitized)             |
//! | `json`              | the raw message, field by field                          |
//! | `jsonParsed`        | rejected: parsed instructions no longer carry their data |
//!
//! # Versioned transactions
//!
//! A v0 transaction can reference accounts it does not list itself, by index
//! into on-chain address lookup tables. Instead of fetching those tables, the
//! normalizer takes the addresses the RPC already resolved into
//! `meta.loadedAddresses`. The full key list is the static keys, then the
//! loaded writable keys, then the loaded readonly keys: the order the runtime
//! numbers them in, so instruction account indexes point straight into it.
//! The number of loaded keys must match what the message's lookups ask for;
//! a mismatch is an error rather than a set of silently wrong accounts.
//!
//! | `DeepAnaTransaction`     | Taken from                                    |
//! |--------------------------|-----------------------------------------------|
//! | `slot`, `blockhash`      | the containing block                          |
//! | `transaction_index`      | position in the block                         |
//! | `tx_signature`           | first signature                               |
//! | `fee`                    | `meta.fee`, 0 without meta                    |
//! | `status`, `err`          | `meta.err`, as the RPC's JSON; success without meta |
//! | `accounts`               | the full key list above                       |
//! | `log_messages`           | `meta.logMessages`, empty when absent         |
//! | `compute_units_consumed` | `meta.computeUnitsConsumed`                   |

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use solana_sdk::message::compiled_instruction::CompiledInstruction;
use solana_sdk::message::v0::MessageAddressTableLookup;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, TransactionBinaryEncoding,
    UiAddressTableLookup, UiInnerInstructions, UiLoadedAddresses, UiMessage, UiTransaction,
    UiTransactionStatusMeta,
};

use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::{DeepAnaTransaction, TransactionStatus, to_json_string};

/// The parts of a transaction the instruction rows are built from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedTransaction {
    /// First signature, base58.
    pub signature: String,
    /// Every account an instruction can reference by index: static keys,
    /// then loaded writable, then loaded readonly.
    pub account_keys: Vec<Pubkey>,
    /// Top-level instructions, in message order.
    pub instructions: Vec<CompiledInstruction>,
}

/// Maps one transaction of a block onto a [`DeepAnaTransaction`]. Stateless.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionNormalizer;

impl TransactionNormalizer {
    pub const fn new() -> Self {
        Self
    }

    /// The row of the transaction at position `index` of the block at `slot`.
    ///
    /// Fails with [`PrismError::DeserializeError`] when the transaction
    /// cannot be decoded; see [`Self::decode`].
    pub fn normalize(
        &self,
        slot: u64,
        blockhash: &str,
        tx_with_meta: &EncodedTransactionWithStatusMeta,
        index: usize,
    ) -> PrismResult<DeepAnaTransaction> {
        self.normalize_with_message(slot, blockhash, tx_with_meta, index)
            .map(|(row, _)| row)
    }

    /// Like [`Self::normalize`], also handing back the decoded transaction
    /// so the instructions can be walked without decoding it a second time.
    pub fn normalize_with_message(
        &self,
        slot: u64,
        blockhash: &str,
        tx_with_meta: &EncodedTransactionWithStatusMeta,
        index: usize,
    ) -> PrismResult<(DeepAnaTransaction, DecodedTransaction)> {
        let decoded = self.decode(slot, tx_with_meta, index)?;
        let transaction_index = u32::try_from(index)
            .map_err(|_| decode_error(slot, index, "the index does not fit a u32"))?;
        let meta = tx_with_meta.meta.as_ref();
        let err = meta.and_then(|meta| meta.err.as_ref()).map(to_json_string);
        let row = DeepAnaTransaction {
            slot,
            blockhash: blockhash.to_owned(),
            transaction_index,
            tx_signature: decoded.signature.clone(),
            fee: meta.map_or(0, |meta| meta.fee),
            status: TransactionStatus::from_err(err.as_deref()),
            err,
            accounts: decoded.account_keys.iter().map(ToString::to_string).collect(),
            log_messages: meta
                .and_then(|meta| Option::from(meta.log_messages.clone()))
                .unwrap_or_default(),
            compute_units_consumed: meta
                .and_then(|meta| Option::from(meta.compute_units_consumed.clone())),
        };
        Ok((row, decoded))
    }

    /// Signature, full account key list and top-level instructions of the
    /// transaction at position `index` of the block at `slot`.
    ///
    /// Fails with [`PrismError::DeserializeError`] when the transaction is
    /// not valid in its encoding, uses an encoding without raw instructions
    /// (`jsonParsed`, `accounts`), or loads a different number of addresses
    /// than its lookups ask for.
    pub fn decode(
        &self,
        slot: u64,
        tx_with_meta: &EncodedTransactionWithStatusMeta,
        index: usize,
    ) -> PrismResult<DecodedTransaction> {
        let message = match &tx_with_meta.transaction {
            encoded @ EncodedTransaction::LegacyBinary(blob) => {
                decode_binary(slot, index, encoded, blob, TransactionBinaryEncoding::Base58)?
            }
            encoded @ EncodedTransaction::Binary(blob, encoding) => {
                decode_binary(slot, index, encoded, blob, *encoding)?
            }
            EncodedTransaction::Json(ui) => decode_json(slot, index, ui)?,
            EncodedTransaction::Accounts(_) => {
                return Err(decode_error(
                    slot,
                    index,
                    "fetched with transaction details `accounts`, which carry no instructions",
                ));
            }
        };
        let account_keys = full_account_keys(
            slot,
            index,
            message.static_keys,
            message.lookup_count,
            tx_with_meta.meta.as_ref(),
        )?;
        Ok(DecodedTransaction {
            signature: message.signature,
            account_keys,
            instructions: message.instructions,
        })
    }
}

/// The inner instructions the meta lists, `None` when it lists none.
pub fn inner_instructions(meta: Option<&UiTransactionStatusMeta>) -> Option<&[UiInnerInstructions]> {
    meta.and_then(|meta| Option::<&Vec<_>>::from(meta.inner_instructions.as_ref()))
        .map(Vec::as_slice)
}

/// A message before its lookup-table keys are appended.
struct RawMessage {
    signature: String,
    static_keys: Vec<Pubkey>,
    /// How many keys the message's address lookups load.
    lookup_count: usize,
    instructions: Vec<CompiledInstruction>,
}

/// Reads a binary-encoded transaction with the SDK's own decoder, the one
/// that knows every wire format: v1 transactions are not laid out the way
/// `bincode` would read them. The decoder only says yes or no, so on a no
/// the text layer is checked again to say which step failed.
fn decode_binary(
    slot: u64,
    index: usize,
    encoded: &EncodedTransaction,
    blob: &str,
    encoding: TransactionBinaryEncoding,
) -> PrismResult<RawMessage> {
    let Some(tx) = encoded.decode() else {
        let reason = match encoding {
            TransactionBinaryEncoding::Base58 => bs58::decode(blob)
                .into_vec()
                .err()
                .map(|err| format!("invalid base58: {err}")),
            TransactionBinaryEncoding::Base64 => BASE64_STANDARD
                .decode(blob)
                .err()
                .map(|err| format!("invalid base64: {err}")),
        };
        return Err(decode_error(
            slot,
            index,
            reason.unwrap_or_else(|| String::from("the bytes are not a valid transaction")),
        ));
    };
    let signature = tx
        .signatures
        .first()
        .ok_or_else(|| decode_error(slot, index, "no signature"))?
        .to_string();
    Ok(RawMessage {
        signature,
        static_keys: tx.message.static_account_keys().to_vec(),
        lookup_count: tx.message.address_table_lookups().map_or(0, binary_lookup_count),
        instructions: tx.message.instructions().to_vec(),
    })
}

fn decode_json(slot: u64, index: usize, ui: &UiTransaction) -> PrismResult<RawMessage> {
    let UiMessage::Raw(raw) = &ui.message else {
        return Err(decode_error(
            slot,
            index,
            "fetched as `jsonParsed`, whose instructions do not carry their raw data; \
             fetch blocks as base58, base64 or json",
        ));
    };
    let signature = ui
        .signatures
        .first()
        .ok_or_else(|| decode_error(slot, index, "no signature"))?
        .clone();
    let instructions = raw
        .instructions
        .iter()
        .map(|ix| {
            let data = bs58::decode(&ix.data).into_vec().map_err(|err| {
                decode_error(slot, index, format!("instruction data is not base58: {err}"))
            })?;
            Ok(CompiledInstruction {
                program_id_index: ix.program_id_index,
                accounts: ix.accounts.clone(),
                data,
            })
        })
        .collect::<PrismResult<_>>()?;
    Ok(RawMessage {
        signature,
        static_keys: parse_keys(slot, index, &raw.account_keys)?,
        lookup_count: raw.address_table_lookups.as_deref().map_or(0, json_lookup_count),
        instructions,
    })
}

/// `static_keys` followed by the addresses the RPC loaded from lookup tables.
fn full_account_keys(
    slot: u64,
    index: usize,
    mut keys: Vec<Pubkey>,
    lookup_count: usize,
    meta: Option<&UiTransactionStatusMeta>,
) -> PrismResult<Vec<Pubkey>> {
    let loaded: Option<&UiLoadedAddresses> =
        meta.and_then(|meta| meta.loaded_addresses.as_ref().into());
    let loaded_count = loaded.map_or(0, |loaded| loaded.writable.len() + loaded.readonly.len());
    if loaded_count != lookup_count {
        return Err(decode_error(
            slot,
            index,
            format!(
                "the message looks up {lookup_count} addresses but the meta loaded {loaded_count}"
            ),
        ));
    }
    if let Some(loaded) = loaded {
        keys.extend(parse_keys(slot, index, &loaded.writable)?);
        keys.extend(parse_keys(slot, index, &loaded.readonly)?);
    }
    Ok(keys)
}

fn binary_lookup_count(lookups: &[MessageAddressTableLookup]) -> usize {
    lookups
        .iter()
        .map(|lookup| lookup.writable_indexes.len() + lookup.readonly_indexes.len())
        .sum()
}

fn json_lookup_count(lookups: &[UiAddressTableLookup]) -> usize {
    lookups
        .iter()
        .map(|lookup| lookup.writable_indexes.len() + lookup.readonly_indexes.len())
        .sum()
}

fn parse_keys(slot: u64, index: usize, keys: &[String]) -> PrismResult<Vec<Pubkey>> {
    keys.iter()
        .map(|key| {
            key.parse()
                .map_err(|err| decode_error(slot, index, format!("account key {key:?}: {err}")))
        })
        .collect()
}

fn decode_error(slot: u64, index: usize, reason: impl Into<String>) -> PrismError {
    PrismError::deserialize(Some(slot), format!("transaction {index}"), reason.into())
}
