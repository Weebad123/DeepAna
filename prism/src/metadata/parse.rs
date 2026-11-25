//! Metaplex metadata account bytes → validated [`TokenMetadata`], or the one
//! [`Reject`] reason that disqualified them.
//!
//! The layout read is the head of the Token Metadata program's `MetadataV1`
//! account (Borsh, little-endian); everything after `uri` is ignored:
//!
//! ```text
//! offset  size        field
//! 0       1           key               4 = MetadataV1
//! 1       32          update_authority
//! 33      32          mint
//! 65      4 + n       name              u32 length, then n bytes, n <= 32
//! ..      4 + n       symbol            u32 length, then n bytes, n <= 10
//! ..      4 + n       uri               u32 length, then n bytes, n <= 200
//! ```
//!
//! An account is accepted only if **all** of these hold, checked in this
//! order (the first failure is the reason reported):
//!
//! 1. it is owned by the metadata program ([`Reject::WrongOwner`]);
//! 2. it is long enough for the fixed head ([`Reject::Truncated`]);
//! 3. its key byte is `MetadataV1` ([`Reject::WrongKey`]);
//! 4. its mint field is the mint it was fetched for ([`Reject::MintMismatch`]);
//! 5. for each of name, symbol, uri: the length fits its cap
//!    ([`Reject::TooLong`], checked before the bytes are touched, so a huge
//!    length never allocates), the bytes are all there
//!    ([`Reject::Truncated`]) and they are UTF-8 ([`Reject::BadUtf8`]).
//!
//! Strings are kept exactly as stored, minus the trailing NUL bytes the
//! program pads them with (USDC's name is stored as `"USD Coin"` followed by
//! 24 NULs). Nothing else is touched: no whitespace trimming, no case or
//! Unicode normalisation, interior NULs kept.

use std::fmt;

use serde::Serialize;
use solana_sdk::pubkey::Pubkey;

use super::pda::METADATA_PROGRAM_ID;
use crate::adapter::AccountData;

/// Key byte of a `MetadataV1` account.
pub const METADATA_V1_KEY: u8 = 4;
/// Longest name the program stores, in bytes.
pub const MAX_NAME_LEN: usize = 32;
/// Longest symbol the program stores, in bytes.
pub const MAX_SYMBOL_LEN: usize = 10;
/// Longest URI the program stores, in bytes.
pub const MAX_URI_LEN: usize = 200;

/// Offset of `update_authority`.
const UPDATE_AUTHORITY_OFFSET: usize = 1;
/// Offset of `mint`.
const MINT_OFFSET: usize = 33;
/// Offset of the first string (`name`'s length prefix): the fixed head's size.
pub const HEAD_LEN: usize = 65;

/// What a valid metadata account says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenMetadata {
    pub update_authority: Pubkey,
    pub mint: Pubkey,
    pub name: String,
    pub symbol: String,
    pub uri: String,
}

/// Why an account was not accepted as a mint's metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Reject {
    /// Not owned by the metadata program.
    WrongOwner,
    /// Shorter than the fixed head, or a string runs past the end.
    Truncated,
    /// The key byte is not `MetadataV1` (an edition, a master edition, an
    /// uninitialised account, ...).
    WrongKey,
    /// The account's mint field names another mint.
    MintMismatch,
    /// A string's length prefix exceeds its cap.
    TooLong,
    /// A string is not valid UTF-8.
    BadUtf8,
}

impl Reject {
    pub const ALL: [Reject; 6] = [
        Self::WrongOwner,
        Self::Truncated,
        Self::WrongKey,
        Self::MintMismatch,
        Self::TooLong,
        Self::BadUtf8,
    ];
    pub const COUNT: usize = Self::ALL.len();

    /// Stable `snake_case` label, for stats and logs.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WrongOwner => "wrong_owner",
            Self::Truncated => "truncated",
            Self::WrongKey => "wrong_key",
            Self::MintMismatch => "mint_mismatch",
            Self::TooLong => "too_long",
            Self::BadUtf8 => "bad_utf8",
        }
    }

    pub(crate) const fn idx(self) -> usize {
        self as usize
    }
}

impl fmt::Display for Reject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for Reject {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// Validate `account` as the metadata of `mint`. See the module docs for
/// the rules and their order.
pub fn parse_metadata(account: &AccountData, mint: &Pubkey) -> Result<TokenMetadata, Reject> {
    if account.owner_program != METADATA_PROGRAM_ID {
        return Err(Reject::WrongOwner);
    }
    let data = account.data.as_slice();
    if data.len() < HEAD_LEN {
        return Err(Reject::Truncated);
    }
    if data[0] != METADATA_V1_KEY {
        return Err(Reject::WrongKey);
    }
    let stored_mint = pubkey_at(data, MINT_OFFSET);
    if stored_mint != *mint {
        return Err(Reject::MintMismatch);
    }
    let mut cursor = HEAD_LEN;
    let name = read_string(data, &mut cursor, MAX_NAME_LEN)?;
    let symbol = read_string(data, &mut cursor, MAX_SYMBOL_LEN)?;
    let uri = read_string(data, &mut cursor, MAX_URI_LEN)?;
    Ok(TokenMetadata {
        update_authority: pubkey_at(data, UPDATE_AUTHORITY_OFFSET),
        mint: stored_mint,
        name,
        symbol,
        uri,
    })
}

/// The 32 bytes at `offset` as a pubkey. The caller has checked the length.
fn pubkey_at(data: &[u8], offset: usize) -> Pubkey {
    let bytes: [u8; 32] = data[offset..offset + 32]
        .try_into()
        .expect("caller checked the head length");
    Pubkey::new_from_array(bytes)
}

/// A Borsh string at `*cursor`, capped at `cap` bytes, with its trailing
/// NUL padding removed. Moves the cursor past it.
fn read_string(data: &[u8], cursor: &mut usize, cap: usize) -> Result<String, Reject> {
    let prefix = data.get(*cursor..*cursor + 4).ok_or(Reject::Truncated)?;
    let len = u32::from_le_bytes(prefix.try_into().expect("4 bytes")) as usize;
    if len > cap {
        return Err(Reject::TooLong);
    }
    let start = *cursor + 4;
    let bytes = data.get(start..start + len).ok_or(Reject::Truncated)?;
    let text = std::str::from_utf8(bytes).map_err(|_| Reject::BadUtf8)?;
    *cursor = start + len;
    Ok(text.trim_end_matches('\0').to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(data: Vec<u8>) -> AccountData {
        AccountData {
            owner_program: METADATA_PROGRAM_ID.to_owned(),
            lamports: 1,
            data,
            executable: false,
            rent_epoch: 0,
        }
    }

    fn encode(mint: &Pubkey, name: &[u8], symbol: &[u8], uri: &[u8]) -> Vec<u8> {
        let mut data = vec![METADATA_V1_KEY];
        data.extend_from_slice(&[7u8; 32]);
        data.extend_from_slice(mint.as_ref());
        for s in [name, symbol, uri] {
            data.extend_from_slice(&(s.len() as u32).to_le_bytes());
            data.extend_from_slice(s);
        }
        data
    }

    #[test]
    fn minimal_valid_account_parses() {
        let mint = Pubkey::new_unique();
        let meta = parse_metadata(&account(encode(&mint, b"A", b"B", b"C")), &mint).unwrap();
        assert_eq!((meta.name.as_str(), meta.symbol.as_str(), meta.uri.as_str()), ("A", "B", "C"));
        assert_eq!(meta.mint, mint);
        assert_eq!(meta.update_authority, Pubkey::new_from_array([7; 32]));
    }

    #[test]
    fn length_is_checked_before_the_bytes() {
        let mint = Pubkey::new_unique();
        let mut data = encode(&mint, b"", b"", b"");
        data[HEAD_LEN..HEAD_LEN + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(parse_metadata(&account(data), &mint), Err(Reject::TooLong));
    }

    #[test]
    fn labels_are_unique() {
        let labels: std::collections::HashSet<_> = Reject::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(labels.len(), Reject::COUNT);
        for (i, r) in Reject::ALL.iter().enumerate() {
            assert_eq!(r.idx(), i);
        }
    }
}
