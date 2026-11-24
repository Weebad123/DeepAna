//! [`TokenAccountIndex`]: what each transaction's own metadata says about the
//! token accounts it touched, looked up by `(signature, account)`.
//!
//! Built from the [`TokenBalanceChange`]s the normalizer reads out of
//! `meta.preTokenBalances` / `meta.postTokenBalances`. Those come from the
//! node reading the accounts as the transaction ran, so they answer, without
//! a request and as of that very transaction:
//!
//! * the **mint** of a token account, which a plain `Transfer` does not name;
//! * the mint's **decimals**;
//! * the account's **owner**, which no transfer instruction names.
//!
//! A value is only offered when the metadata is unambiguous: an account whose
//! pre and post entries disagree (closed and re-created for another mint, or
//! handed to a new owner within the transaction) has that value withheld
//! rather than guessed. See [`TokenBalanceChange::mint`] and friends.

use std::collections::HashMap;

use crate::normalizer::TokenBalanceChange;

/// What one transaction's metadata says about one token account.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TokenAccountMeta {
    /// Base58 mint, when unambiguous.
    pub mint: Option<String>,
    /// Base58 owner, when unambiguous.
    pub owner: Option<String>,
    /// Base58 id of the owning token program, when the RPC reported it.
    pub program_id: Option<String>,
    /// Decimals of `mint`, when unambiguous.
    pub decimals: Option<u8>,
}

impl TokenAccountMeta {
    /// The metadata of `change`.
    pub fn from_change(change: &TokenBalanceChange) -> Self {
        Self {
            mint: change.mint().map(str::to_owned),
            owner: change.owner().map(str::to_owned),
            program_id: change.program_id().map(str::to_owned),
            decimals: change.decimals(),
        }
    }

    /// Whether this account can belong to `program_id`: it names that
    /// program, or names none (blocks from before the RPC reported it).
    pub fn belongs_to(&self, program_id: &str) -> bool {
        self.program_id.as_deref().is_none_or(|id| id == program_id)
    }
}

/// Token account metadata of a batch of transactions, keyed by transaction
/// signature, then account address.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenAccountIndex {
    by_transaction: HashMap<String, HashMap<String, TokenAccountMeta>>,
}

impl TokenAccountIndex {
    /// An empty index: every lookup misses.
    pub fn new() -> Self {
        Self::default()
    }

    /// The index of `changes`, e.g. a slot's
    /// [`token_balances`](crate::slot_processor::ProcessedSlot::token_balances).
    pub fn from_balances<'a>(changes: impl IntoIterator<Item = &'a TokenBalanceChange>) -> Self {
        let mut index = Self::new();
        for change in changes {
            index.insert(&change.tx_signature, &change.account, TokenAccountMeta::from_change(change));
        }
        index
    }

    /// Records `meta` for `account` in transaction `tx_signature`, replacing
    /// any earlier entry.
    pub fn insert(&mut self, tx_signature: &str, account: &str, meta: TokenAccountMeta) {
        self.by_transaction
            .entry(tx_signature.to_owned())
            .or_default()
            .insert(account.to_owned(), meta);
    }

    /// What transaction `tx_signature`'s metadata says about `account`.
    pub fn get(&self, tx_signature: &str, account: &str) -> Option<&TokenAccountMeta> {
        self.by_transaction.get(tx_signature)?.get(account)
    }

    /// Accounts indexed, across all transactions.
    pub fn len(&self) -> usize {
        self.by_transaction.values().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.by_transaction.is_empty()
    }
}
