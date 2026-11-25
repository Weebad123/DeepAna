//! Where a mint's Metaplex metadata lives.
//!
//! The Token Metadata program keeps one metadata account per mint, at the
//! program-derived address of the seeds
//! `["metadata", METADATA_PROGRAM, mint]` under that same program. Nothing
//! else may own an account at that address, so the address alone names the
//! mint's metadata; [`parse`](super::parse) still checks the owner and the
//! mint stored inside.

use solana_sdk::pubkey::Pubkey;

/// Base58 id of the Metaplex Token Metadata program.
pub const METADATA_PROGRAM_ID: &str = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s";

/// [`METADATA_PROGRAM_ID`] as a [`Pubkey`].
pub const METADATA_PROGRAM: Pubkey = Pubkey::from_str_const(METADATA_PROGRAM_ID);

/// First seed of every metadata PDA.
pub const METADATA_SEED: &[u8] = b"metadata";

/// The address of `mint`'s metadata account. Pure: no request is made.
pub fn metadata_pda(mint: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[METADATA_SEED, METADATA_PROGRAM.as_ref(), mint.as_ref()],
        &METADATA_PROGRAM,
    )
    .0
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn program_constant_matches_its_base58_id() {
        assert_eq!(METADATA_PROGRAM.to_string(), METADATA_PROGRAM_ID);
    }

    #[test]
    fn usdc_metadata_account_is_the_one_on_mainnet() {
        // Read from mainnet: an account owned by the metadata program whose
        // mint field is USDC lives at this address.
        let usdc = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v").unwrap();
        assert_eq!(
            metadata_pda(&usdc).to_string(),
            "5x38Kp4hvdomTCnCrAny4UtMUt5rQBdB6px2K1Ui45Wq"
        );
    }

    #[test]
    fn derivation_is_deterministic_and_off_curve() {
        let mint = Pubkey::new_unique();
        let pda = metadata_pda(&mint);
        assert_eq!(pda, metadata_pda(&mint));
        assert!(!pda.is_on_curve());
        assert_ne!(pda, metadata_pda(&Pubkey::new_unique()));
    }
}
