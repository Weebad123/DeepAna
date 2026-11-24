//! A transaction's token balances → [`TokenBalanceChange`]s.
//!
//! For every token account a transaction touches, the RPC reports the
//! account's state before (`meta.preTokenBalances`) and after
//! (`meta.postTokenBalances`) execution: its mint, owner, owning token
//! program, the mint's decimals and the raw amount. The node reads those from
//! the accounts themselves, so they are the chain's own word on what each
//! token account *is*, with no extra request.
//!
//! An entry names its account by `accountIndex`, an index into the
//! transaction's **full** key list: the static keys, then the addresses a v0
//! transaction loaded from lookup tables (writable, then readonly). That is
//! the list [`TransactionNormalizer`](super::TransactionNormalizer) builds,
//! so a token account that only a lookup table names resolves like any other.
//!
//! | `TokenBalanceChange` | Taken from                                             |
//! |----------------------|--------------------------------------------------------|
//! | `account_index`      | `accountIndex`                                         |
//! | `account`            | the full key list at `accountIndex`                    |
//! | `pre`                | the `preTokenBalances` entry, `None` without one       |
//! | `post`               | the `postTokenBalances` entry, `None` without one      |
//!
//! An account without a `pre` entry was created by the transaction; one
//! without a `post` entry was closed by it. Entries come out ordered by
//! `account_index`.
//!
//! The entries are read as strictly as the rest of the transaction: an index
//! past the key list, an account listed twice on one side, or an amount that
//! is not a `u64` fail the block (a [`PrismError::DeserializeError`]) rather
//! than attach a balance to the wrong account. A meta without token balances
//! (absent, or a block old enough to predate them) yields no entries.
//!
//! These values are not a UDM table: they travel with the slot to the
//! [account resolver](crate::account_resolver), which fills the token
//! transfer rows from them.

use std::collections::BTreeMap;

use solana_transaction_status::option_serializer::OptionSerializer;
use solana_transaction_status::{UiTransactionStatusMeta, UiTransactionTokenBalance};

use crate::types::error::{PrismError, PrismResult};

/// One side (before or after execution) of a token account's balance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalanceSide {
    /// Base58 mint of the account.
    pub mint: String,
    /// Base58 owner of the account. `None` when the RPC omitted it (blocks
    /// from before owners were recorded).
    pub owner: Option<String>,
    /// Base58 id of the token program that owns the account. `None` when the
    /// RPC omitted it.
    pub program_id: Option<String>,
    /// Decimals of the mint.
    pub decimals: u8,
    /// Raw amount, in base units.
    pub amount: u64,
}

/// What a transaction's meta says about one token account it touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenBalanceChange {
    pub slot: u64,
    /// Base58 signature of the transaction.
    pub tx_signature: String,
    /// Index of the account in the transaction's full key list.
    pub account_index: u8,
    /// Base58 address of the token account.
    pub account: String,
    /// State before execution; `None` when the account did not exist yet.
    pub pre: Option<TokenBalanceSide>,
    /// State after execution; `None` when the transaction closed it.
    pub post: Option<TokenBalanceSide>,
}

impl TokenBalanceChange {
    /// The account's mint, when every side present names the same one.
    pub fn mint(&self) -> Option<&str> {
        self.agreed(|side| Some(side.mint.as_str()))
    }

    /// The account's owner, when every side present names the same one. An
    /// owner changed inside the transaction (`SetAuthority`) is ambiguous for
    /// any one instruction, so it is `None`.
    pub fn owner(&self) -> Option<&str> {
        self.agreed(|side| side.owner.as_deref())
    }

    /// The token program owning the account, when every side present that
    /// names one names the same one.
    pub fn program_id(&self) -> Option<&str> {
        let mut named = self.sides().filter_map(|side| side.program_id.as_deref());
        let first = named.next()?;
        named.all(|other| other == first).then_some(first)
    }

    /// The mint's decimals, when every side present agrees.
    pub fn decimals(&self) -> Option<u8> {
        let mut sides = self.sides().map(|side| side.decimals);
        let first = sides.next()?;
        sides.all(|other| other == first).then_some(first)
    }

    /// Raw amount before execution; 0 for an account that did not exist.
    pub fn pre_amount(&self) -> u64 {
        self.pre.as_ref().map_or(0, |side| side.amount)
    }

    /// Raw amount after execution; 0 for an account that was closed.
    pub fn post_amount(&self) -> u64 {
        self.post.as_ref().map_or(0, |side| side.amount)
    }

    /// `post_amount - pre_amount`: what the transaction did to the balance.
    pub fn delta(&self) -> i128 {
        i128::from(self.post_amount()) - i128::from(self.pre_amount())
    }

    fn sides(&self) -> impl Iterator<Item = &TokenBalanceSide> {
        self.pre.iter().chain(self.post.iter())
    }

    /// `field` of every side present, if they all have it and agree.
    fn agreed<'a>(&'a self, field: impl Fn(&'a TokenBalanceSide) -> Option<&'a str>) -> Option<&'a str> {
        let mut values = self.sides().map(field);
        let first = values.next()??;
        values.all(|other| other == Some(first)).then_some(first)
    }
}

/// The token balance changes of one transaction, ordered by account index.
/// `account_keys` must be the full key list, as base58.
///
/// Fails with [`PrismError::DeserializeError`] as the [module docs](self)
/// describe.
pub fn token_balance_changes(
    slot: u64,
    tx_signature: &str,
    account_keys: &[String],
    meta: Option<&UiTransactionStatusMeta>,
) -> PrismResult<Vec<TokenBalanceChange>> {
    let Some(meta) = meta else {
        return Ok(Vec::new());
    };
    let mut by_index: BTreeMap<u8, (Option<TokenBalanceSide>, Option<TokenBalanceSide>)> = BTreeMap::new();
    for (side_name, entries, is_pre) in [
        ("preTokenBalances", &meta.pre_token_balances, true),
        ("postTokenBalances", &meta.post_token_balances, false),
    ] {
        for entry in listed(entries) {
            let side = read_side(slot, tx_signature, side_name, entry, account_keys.len())?;
            let slot_entry = by_index.entry(entry.account_index).or_default();
            let target = if is_pre { &mut slot_entry.0 } else { &mut slot_entry.1 };
            if target.is_some() {
                return Err(balance_error(
                    slot,
                    tx_signature,
                    format!("{side_name} lists account {} twice", entry.account_index),
                ));
            }
            *target = Some(side);
        }
    }
    Ok(by_index
        .into_iter()
        .map(|(account_index, (pre, post))| TokenBalanceChange {
            slot,
            tx_signature: tx_signature.to_owned(),
            account_index,
            account: account_keys[usize::from(account_index)].clone(),
            pre,
            post,
        })
        .collect())
}

fn listed(entries: &OptionSerializer<Vec<UiTransactionTokenBalance>>) -> &[UiTransactionTokenBalance] {
    Option::<&Vec<_>>::from(entries.as_ref()).map_or(&[], Vec::as_slice)
}

fn read_side(
    slot: u64,
    tx_signature: &str,
    side_name: &str,
    entry: &UiTransactionTokenBalance,
    key_count: usize,
) -> PrismResult<TokenBalanceSide> {
    if usize::from(entry.account_index) >= key_count {
        return Err(balance_error(
            slot,
            tx_signature,
            format!(
                "{side_name} references account {}, but the transaction has {key_count} account keys",
                entry.account_index
            ),
        ));
    }
    let amount = entry.ui_token_amount.amount.parse::<u64>().map_err(|err| {
        balance_error(
            slot,
            tx_signature,
            format!(
                "{side_name} amount {:?} of account {} is not a u64: {err}",
                entry.ui_token_amount.amount, entry.account_index
            ),
        )
    })?;
    Ok(TokenBalanceSide {
        mint: entry.mint.clone(),
        owner: Option::from(entry.owner.clone()),
        program_id: Option::from(entry.program_id.clone()),
        decimals: entry.ui_token_amount.decimals,
        amount,
    })
}

fn balance_error(slot: u64, tx_signature: &str, reason: String) -> PrismError {
    PrismError::deserialize(Some(slot), format!("token balances of transaction {tx_signature}"), reason)
}
