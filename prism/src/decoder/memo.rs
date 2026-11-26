//! [`MemoDecoder`]: decodes SPL Memo instructions into their text.
//!
//! A memo instruction's data is the memo itself, UTF-8, with no tag and no
//! length prefix; the program rejects anything that is not valid UTF-8.
//! Memo v2 also requires every account passed to the instruction to sign,
//! and those accounts are the memo's signers. Memo v1 ignores its accounts;
//! they are reported the same way.
//!
//! | Program | Id                                              |
//! |---------|-------------------------------------------------|
//! | Memo v2 | `MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr`   |
//! | Memo v1 | `Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo`   |
//!
//! Every instruction is typed `Memo`, with decoded data:
//!
//! ```json
//! { "memo": "thanks for lunch", "version": "v2", "accounts": { "signers": ["..."] } }
//! ```
//!
//! Data that is not UTF-8 (possible in a failed transaction) is a decode
//! error. The decoder is built for one program; register [`MemoDecoder::v2`]
//! and [`MemoDecoder::v1`] side by side to cover both.

use serde_json::{Value, json};

use super::{DecodeResult, Decoder, decode_error, instruction_data};
use crate::types::error::PrismResult;
use crate::types::udm::DeepAnaInstruction;

/// Base58 id of the SPL Memo program (v2), the one in use today.
pub const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
/// Base58 id of the original SPL Memo program (v1).
pub const MEMO_V1_PROGRAM_ID: &str = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo";

/// `instruction_type` of every memo instruction.
pub const MEMO_INSTRUCTION_TYPE: &str = "Memo";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemoProgram {
    V1,
    V2,
}

/// Decodes the instructions of one memo program: v2 or v1.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoDecoder {
    program: MemoProgram,
}

impl MemoDecoder {
    /// A decoder for the Memo v2 program.
    pub const fn v2() -> Self {
        Self {
            program: MemoProgram::V2,
        }
    }

    /// A decoder for the Memo v1 program.
    pub const fn v1() -> Self {
        Self {
            program: MemoProgram::V1,
        }
    }

    /// The decoder for `program_id`, if it is one of the two memo programs.
    pub fn for_program(program_id: &str) -> Option<Self> {
        match program_id {
            MEMO_PROGRAM_ID => Some(Self::v2()),
            MEMO_V1_PROGRAM_ID => Some(Self::v1()),
            _ => None,
        }
    }

    const fn version(&self) -> &'static str {
        match self.program {
            MemoProgram::V1 => "v1",
            MemoProgram::V2 => "v2",
        }
    }
}

impl Decoder for MemoDecoder {
    fn program_id(&self) -> &str {
        match self.program {
            MemoProgram::V1 => MEMO_V1_PROGRAM_ID,
            MemoProgram::V2 => MEMO_PROGRAM_ID,
        }
    }

    /// Every instruction of the program is a memo, empty ones included.
    fn can_decode(&self, program_id: &str, _data: &[u8]) -> bool {
        program_id == self.program_id()
    }

    fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        _account_keys: &[String],
    ) -> PrismResult<DecodeResult> {
        if instruction.program_id != self.program_id() {
            return Err(decode_error(
                instruction,
                format!("not a {} instruction", self.program_id()),
            ));
        }
        let data = instruction_data(instruction)?;
        let memo = String::from_utf8(data)
            .map_err(|err| decode_error(instruction, format!("memo is not valid UTF-8: {}", err.utf8_error())))?;
        Ok(DecodeResult {
            instruction_type: MEMO_INSTRUCTION_TYPE.to_owned(),
            decoded_data: json!({
                "memo": memo,
                "version": self.version(),
                "accounts": { "signers": Value::from(instruction.accounts.clone()) },
            }),
            token_transfer: None,
        })
    }
}
