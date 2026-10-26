//! [`Discriminant`]: the 8-byte prefix Anchor uses to tell which instruction,
//! account type or event a blob of bytes is.
//!
//! Solana itself has no notion of "which instruction": the runtime hands a
//! program an opaque `data: &[u8]` and the program decides how to read it.
//! Anchor's convention is to prefix every payload with the first 8 bytes of
//! a SHA-256 over a namespaced name, followed by the Borsh-encoded body:
//!
//! ```text
//!   instruction data = sha256("global:<instruction_name>")[..8] ‖ borsh(args)
//!   account data     = sha256("account:<AccountName>")[..8]     ‖ borsh(account)
//!   event data       = sha256("event:<EventName>")[..8]         ‖ borsh(event)
//! ```
//!
//! The instruction name is the snake_case name of the Rust handler
//! (`initialize_pool`), account and event names are the PascalCase struct
//! names. The generated `#[program]` dispatcher reads `data[..8]`, matches it
//! against the discriminants of its handlers and deserializes `data[8..]`
//! into that handler's arguments; anything else fails with
//! `InstructionFallbackNotFound`.
//!
//! Since Anchor 0.30 the IDL stores each discriminant explicitly (the
//! `discriminator` field), and programs may override it with
//! `#[instruction(discriminator = ...)]`, in which case it can be any
//! non-empty length. [`Discriminant`] models the standard 8-byte form; the
//! Anchor decoder handles the custom lengths separately.
//!
//! Two reserved prefixes are not program instructions at all:
//! * [`EVENT_IX_TAG`]: `emit_cpi!` makes the program invoke itself with
//!   `EVENT_IX_TAG ‖ event discriminant ‖ borsh(event)`, so events survive
//!   in the inner instructions even when logs are truncated.
//! * [`IDL_IX_TAG`]: the (legacy) instructions that manage the on-chain IDL
//!   account.

use std::fmt;

use sha2::{Digest, Sha256};

/// Prefix of the self-invocation `emit_cpi!` uses to record an event:
/// `e4 45 a5 2e 51 cb 9a 1d`. Anchor defines it as the `u64`
/// `0x1d9acb512ea545e4` (the bytes of `sha256("anchor:event")[..8]`) and
/// writes it little-endian, so on chain the hash prefix appears reversed.
pub const EVENT_IX_TAG: Discriminant = Discriminant(0x1d9a_cb51_2ea5_45e4_u64.to_le_bytes());

/// Prefix of the legacy instructions that manage a program's on-chain IDL
/// account: `40 f4 bc 78 a7 e9 69 0a`, i.e. `sha256("anchor:idl")[..8]`
/// reversed, for the same reason as [`EVENT_IX_TAG`].
pub const IDL_IX_TAG: Discriminant = Discriminant(0x0a69_e9a7_78bc_f440_u64.to_le_bytes());

/// The 8-byte Anchor discriminant at the start of instruction, account and
/// event data.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Discriminant([u8; Discriminant::LEN]);

impl Discriminant {
    /// Length of a standard Anchor discriminant.
    pub const LEN: usize = 8;

    pub const fn new(bytes: [u8; Self::LEN]) -> Self {
        Self(bytes)
    }

    /// The discriminant `data` starts with, or `None` when `data` is shorter
    /// than [`Self::LEN`].
    pub fn from_data(data: &[u8]) -> Option<Self> {
        data.first_chunk::<{ Self::LEN }>().copied().map(Self)
    }

    /// `sha256("<namespace>:<name>")[..8]`, the derivation every Anchor
    /// discriminant follows.
    pub fn namespaced(namespace: &str, name: &str) -> Self {
        let digest = Sha256::new()
            .chain_update(namespace.as_bytes())
            .chain_update(b":")
            .chain_update(name.as_bytes())
            .finalize();
        let mut bytes = [0; Self::LEN];
        bytes.copy_from_slice(&digest[..Self::LEN]);
        Self(bytes)
    }

    /// The discriminant of the instruction handler `name`:
    /// `sha256("global:<snake_case name>")[..8]`. Accepts the camelCase
    /// names of pre-0.30 IDLs as well, since Anchor always hashes the
    /// snake_case Rust name.
    pub fn for_instruction(name: &str) -> Self {
        Self::namespaced("global", &to_snake_case(name))
    }

    /// The discriminant of the account type `name` (PascalCase, as written).
    pub fn for_account(name: &str) -> Self {
        Self::namespaced("account", name)
    }

    /// The discriminant of the event `name` (PascalCase, as written).
    pub fn for_event(name: &str) -> Self {
        Self::namespaced("event", name)
    }

    pub const fn as_bytes(&self) -> &[u8; Self::LEN] {
        &self.0
    }

    pub const fn to_bytes(self) -> [u8; Self::LEN] {
        self.0
    }

    /// Whether `data` starts with this discriminant.
    pub fn matches(&self, data: &[u8]) -> bool {
        data.starts_with(&self.0)
    }
}

impl From<[u8; Discriminant::LEN]> for Discriminant {
    fn from(bytes: [u8; Discriminant::LEN]) -> Self {
        Self(bytes)
    }
}

impl TryFrom<&[u8]> for Discriminant {
    type Error = std::array::TryFromSliceError;

    /// Exactly [`Discriminant::LEN`] bytes; use [`Discriminant::from_data`]
    /// to take the prefix of a longer payload.
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        <[u8; Self::LEN]>::try_from(bytes).map(Self)
    }
}

impl AsRef<[u8]> for Discriminant {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Lowercase hex, the form Anchor's tooling prints.
impl fmt::Display for Discriminant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl fmt::Debug for Discriminant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Discriminant({self})")
    }
}

/// `initializePool` → `initialize_pool`; snake_case input is returned as is.
/// An underscore goes before an uppercase letter that follows a lowercase
/// letter or a digit, so acronyms stay together (`createATA` → `create_ata`).
fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    let mut previous: Option<char> = None;
    for c in name.chars() {
        if c.is_uppercase() && previous.is_some_and(|p| p.is_lowercase() || p.is_ascii_digit()) {
            out.push('_');
        }
        out.extend(c.to_lowercase());
        previous = Some(c);
    }
    out
}
