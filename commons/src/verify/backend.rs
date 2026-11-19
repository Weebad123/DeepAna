//! [`DecoderBackend`]: how `verify/` runs a decoder, and [`PrismBackend`],
//! the adapter over PRISM's.
//!
//! A decoder artifact is an Anchor IDL: data, interpreted by PRISM's own
//! [`AnchorDecoder`]. Nothing contributed is executed. This file is the
//! only place in `verify/` that names a PRISM type; the rest of the module
//! sees [`LoadedDecoder`] and [`Outcome`].
//!
//! [`PrismBackend`] runs every instruction through a [`DecoderRegistry`]
//! holding just the loaded decoder, which is how PRISM dispatches in
//! production: look the program up, ask [`Decoder::can_decode`], then
//! decode. The instruction is built from the case's data and accounts with
//! fixed placeholders for everything else (slot 0, an all-zero signature,
//! top-level index 0), and the transaction's account list is the
//! instruction's.
//!
//! `decoded_data` comes back as the exact text PRISM writes to Parquet
//! ([`DecodeResult::decoded_data_json`]).

use prism::decoder::{AnchorDecoder, Decoder, DecoderRegistry, Discriminant};
use prism::types::udm::DeepAnaInstruction;

/// One instruction a decoder knows.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstructionInfo {
    /// The IDL name, which is also the decoded `instruction_type`.
    pub name: String,
    /// The 8 bytes instruction data starts with.
    pub discriminator: [u8; 8],
}

/// What a decoder makes of one instruction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Outcome {
    /// The decoder does not claim the instruction.
    NotClaimed,
    /// Claimed and decoded. `data_json` is the JSON text PRISM stores.
    Decoded {
        instruction_type: String,
        data_json: String,
    },
    /// Claimed, and decoding failed. The message is for people only.
    DecodeError(String),
}

/// A decoder could not be built from an IDL. The message may hold
/// contributor text and is echoed by whoever shows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError(pub String);

impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BackendError {}

/// Builds decoders from IDLs. Shared by every artifact of a run.
pub trait DecoderBackend: Send + Sync {
    /// A decoder for `program_id` from `idl`, the raw content bytes. Each
    /// call builds a new decoder that shares no state with any other.
    fn load(&self, idl: &[u8], program_id: &[u8; 32]) -> Result<Box<dyn LoadedDecoder>, BackendError>;
}

/// One loaded decoder.
pub trait LoadedDecoder: Send {
    /// The instructions it claims, with 8-byte discriminators, sorted by
    /// name. Instructions with any other discriminator length are left out.
    fn instructions(&self) -> Vec<InstructionInfo>;

    /// Decode one instruction sent to `program_id`.
    fn run(&self, program_id: &[u8; 32], data: &[u8], accounts: &[[u8; 32]]) -> Outcome;
}

// ---------------------------------------------------------------------------
// PRISM
// ---------------------------------------------------------------------------

/// [`DecoderBackend`] over PRISM's [`AnchorDecoder`].
#[derive(Debug, Clone, Copy, Default)]
pub struct PrismBackend;

/// The signature placeholder every instruction carries: 64 zero bytes.
fn placeholder_signature() -> String {
    bs58::encode([0u8; 64]).into_string()
}

impl DecoderBackend for PrismBackend {
    fn load(&self, idl: &[u8], program_id: &[u8; 32]) -> Result<Box<dyn LoadedDecoder>, BackendError> {
        let text = std::str::from_utf8(idl).map_err(|_| BackendError("the IDL is not UTF-8".into()))?;
        let decoder = AnchorDecoder::from_json(text).map_err(|e| BackendError(e.to_string()))?;
        let expected = bs58::encode(program_id).into_string();
        if decoder.program_id() != expected {
            return Err(BackendError(format!(
                "the IDL is for {} but the manifest's program_id is {expected}",
                decoder.program_id()
            )));
        }
        let mut instructions: Vec<InstructionInfo> = decoder
            .idl()
            .instructions
            .iter()
            .filter_map(|ix| {
                let bytes = if ix.discriminator.is_empty() {
                    Discriminant::for_instruction(&ix.name).to_bytes()
                } else {
                    ix.discriminator.as_slice().try_into().ok()?
                };
                Some(InstructionInfo {
                    name: ix.name.clone(),
                    discriminator: bytes,
                })
            })
            .collect();
        instructions.sort();
        let mut registry = DecoderRegistry::new();
        registry.register(Box::new(decoder));
        Ok(Box::new(PrismLoaded {
            registry,
            instructions,
        }))
    }
}

struct PrismLoaded {
    registry: DecoderRegistry,
    instructions: Vec<InstructionInfo>,
}

impl LoadedDecoder for PrismLoaded {
    fn instructions(&self) -> Vec<InstructionInfo> {
        self.instructions.clone()
    }

    fn run(&self, program_id: &[u8; 32], data: &[u8], accounts: &[[u8; 32]]) -> Outcome {
        let accounts: Vec<String> = accounts.iter().map(|a| bs58::encode(a).into_string()).collect();
        let instruction = DeepAnaInstruction {
            slot: 0,
            tx_signature: placeholder_signature(),
            instruction_index: 0,
            inner_instruction_index: None,
            program_id: bs58::encode(program_id).into_string(),
            accounts: accounts.clone(),
            data_base58: bs58::encode(data).into_string(),
            instruction_type: None,
            decoded_data: None,
        };
        match self.registry.decode(&instruction, &accounts) {
            Ok(None) => Outcome::NotClaimed,
            Ok(Some(result)) => Outcome::Decoded {
                data_json: result.decoded_data_json(),
                instruction_type: result.instruction_type,
            },
            Err(e) => Outcome::DecodeError(e.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const ADDRESS: &str = "Fg6PaFpoGXkYsidMpWTK6W2BeZ7FEfcYkg476zPFsLnS";

    fn idl() -> String {
        format!(
            r#"{{"address":"{ADDRESS}","metadata":{{"name":"t","version":"0.1.0","spec":"0.1.0"}},
            "instructions":[
              {{"name":"deposit","discriminator":[242,35,198,137,82,225,242,182],"accounts":[],
                "args":[{{"name":"amount","type":"u64"}}]}},
              {{"name":"ping","discriminator":[7],"accounts":[],"args":[]}}]}}"#
        )
    }

    fn pid() -> [u8; 32] {
        bs58::decode(ADDRESS).into_vec().unwrap().try_into().unwrap()
    }

    #[test]
    fn prism_backend_decodes_claims_and_refuses() {
        let d = PrismBackend.load(idl().as_bytes(), &pid()).unwrap();
        assert_eq!(
            d.instructions(),
            [InstructionInfo {
                name: "deposit".into(),
                discriminator: [242, 35, 198, 137, 82, 225, 242, 182]
            }],
            "the 1-byte custom discriminator is left out"
        );
        let mut data = vec![242, 35, 198, 137, 82, 225, 242, 182];
        data.extend_from_slice(&1000u64.to_le_bytes());
        assert_eq!(
            d.run(&pid(), &data, &[[1; 32]]),
            Outcome::Decoded {
                instruction_type: "deposit".into(),
                data_json: r#"{"amount":1000}"#.into()
            }
        );
        assert!(matches!(d.run(&pid(), &data[..10], &[]), Outcome::DecodeError(_)));
        assert_eq!(d.run(&pid(), &[1, 2, 3, 4, 5, 6, 7, 8, 9], &[]), Outcome::NotClaimed);
        assert_eq!(d.run(&[9; 32], &data, &[]), Outcome::NotClaimed, "another program");
    }

    #[test]
    fn load_checks_the_program_id_and_the_idl() {
        let e = PrismBackend.load(idl().as_bytes(), &[3; 32]).err().unwrap();
        assert!(e.0.contains("manifest's program_id"), "{e}");
        assert!(PrismBackend.load(b"{}", &pid()).is_err());
        assert!(PrismBackend.load(b"\xff", &pid()).is_err());
    }
}
