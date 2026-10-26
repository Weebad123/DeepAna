//! [`AnchorDecoder`]: decodes the instructions of any Anchor program from its
//! IDL.
//!
//! An Anchor instruction is `discriminant ‖ borsh(args)` (see
//! [`discriminant`](super::discriminant)). The decoder reads the IDL once,
//! at construction, into a map from discriminant to instruction; decoding is
//! then a lookup on the first bytes followed by a walk of the Borsh body
//! guided by the instruction's argument types:
//!
//! ```text
//!   data ─▶ data[..8] ─▶ discriminant_map ─▶ IdlInstruction { name, args }
//!                                                    │
//!          data[8..] ─▶ borsh, field by field ◀──────┘ ─▶ { "arg": value, ... }
//! ```
//!
//! IDLs in the Anchor 0.30+ format (`anchor-lang-idl-spec`) are supported;
//! they carry every instruction's discriminant explicitly, including custom
//! ones of any length. An instruction without one gets the standard
//! `sha256("global:<name>")[..8]`.
//!
//! Events emitted with `emit_cpi!` are decoded too: the program invokes
//! itself with `EVENT_IX_TAG ‖ event discriminant ‖ borsh(event)`, and such
//! an instruction comes out with type `event:<EventName>`. The event is
//! looked up in the IDL's `events`, then among its type definitions (an IDL
//! may define an event struct without listing it as an event).
//!
//! An IDL can be stale, most often the one uploaded to the on-chain
//! `anchor:idl` account: the deployed program has instructions and events
//! the IDL does not describe. Two kinds of instruction are recognised
//! regardless, since Anchor itself fixes their prefix:
//! * an `emit_cpi!` self-invocation whose event the IDL does not describe
//!   comes out as [`UNDESCRIBED_EVENT_TYPE`], with the event discriminant
//!   and the undecoded body;
//! * the instructions that manage the legacy on-chain IDL account
//!   ([`IDL_IX_TAG`]), which every Anchor program before 1.0 accepts but no
//!   IDL lists, come out as `idl:<operation>`.
//!
//! An ordinary instruction the IDL does not describe stays unclaimed: its
//! argument layout is not known, so nothing can be decoded from it.
//!
//! Borsh values map to JSON as follows. Integers up to 64 bits are JSON
//! numbers; 128- and 256-bit integers are decimal strings, since JSON
//! readers commonly lose precision past 2^53. `pubkey` is base58, `bytes` is
//! base64, an `Option` is the value or `null`, `Vec` and arrays are arrays,
//! a struct with named fields is an object (tuple fields: an array), and an
//! enum is the variant name when the variant has no fields, otherwise
//! `{ "<Variant>": fields }`.
//!
//! The decoder never produces token transfers: an Anchor program moves
//! tokens by calling the token program, and that inner instruction is
//! decoded by [`SplTokenDecoder`](super::SplTokenDecoder).

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use anchor_lang_idl_spec::{
    Idl, IdlArrayLen, IdlDefinedFields, IdlField, IdlGenericArg, IdlInstruction,
    IdlSerialization, IdlType, IdlTypeDef, IdlTypeDefGeneric, IdlTypeDefTy,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use borsh::BorshDeserialize;
use serde_json::{Map, Number, Value};
use solana_sdk::pubkey::Pubkey;

use super::discriminant::{Discriminant, EVENT_IX_TAG, IDL_IX_TAG};
use super::{DecodeResult, Decoder, decode_error, instruction_data};
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::DeepAnaInstruction;

/// Prefix of the `instruction_type` of an `emit_cpi!` event.
pub const EVENT_TYPE_PREFIX: &str = "event:";

/// `instruction_type` of an `emit_cpi!` event the IDL does not describe.
/// Its `decoded_data` is `{ "discriminator": <hex>, "data": <base64> }`.
pub const UNDESCRIBED_EVENT_TYPE: &str = "event:<undescribed>";

/// Prefix of the `instruction_type` of a legacy IDL-account instruction.
pub const IDL_TYPE_PREFIX: &str = "idl:";

/// Deepest nesting of types the decoder follows. Real IDLs stay in single
/// digits; the limit stops a self-referencing type from recursing forever.
const MAX_TYPE_DEPTH: usize = 64;

/// What the first bytes of an instruction select.
#[derive(Debug, Clone, Copy)]
enum Target<'a> {
    Instruction(&'a IdlInstruction),
    /// An `emit_cpi!` self-invocation carrying the event of this type.
    Event(&'a str),
    /// An `emit_cpi!` self-invocation carrying an event the IDL does not
    /// describe.
    UndescribedEvent(Discriminant),
    /// An instruction managing the legacy on-chain IDL account.
    IdlAccount,
}

/// Decodes the instructions of one Anchor program, given its IDL.
#[derive(Debug, Clone)]
pub struct AnchorDecoder {
    program_id: String,
    idl: Idl,
    /// Instructions with a standard 8-byte discriminant. Precomputed at
    /// construction.
    discriminant_map: HashMap<Discriminant, IdlInstruction>,
    /// Instructions whose custom discriminant is not 8 bytes long, longest
    /// first so that the most specific prefix wins.
    custom_discriminants: Vec<(Vec<u8>, IdlInstruction)>,
    /// Type names of events by their 8-byte discriminant, for `emit_cpi!`
    /// instructions: the IDL's events, then any other struct whose
    /// `sha256("event:<Name>")[..8]` is not taken.
    events: HashMap<Discriminant, String>,
    /// The IDL's type definitions by name.
    types: HashMap<String, IdlTypeDef>,
}

impl AnchorDecoder {
    /// A decoder for the program `idl` describes; the program id is the
    /// IDL's `address`.
    ///
    /// Fails with [`PrismError::ConfigError`] when the address is not a
    /// pubkey, two instructions share a discriminant, or an instruction or
    /// event refers to a type the IDL does not define.
    pub fn new(idl: Idl) -> PrismResult<Self> {
        Pubkey::from_str(&idl.address).map_err(|err| {
            PrismError::config("idl.address", format!("`{}` is not a pubkey: {err}", idl.address))
        })?;
        let types: HashMap<String, IdlTypeDef> =
            idl.types.iter().map(|def| (def.name.clone(), def.clone())).collect();

        let mut discriminant_map = HashMap::new();
        let mut custom_discriminants: Vec<(Vec<u8>, IdlInstruction)> = Vec::new();
        let mut seen: HashMap<Vec<u8>, &str> = HashMap::new();
        for instruction in &idl.instructions {
            let bytes = if instruction.discriminator.is_empty() {
                Discriminant::for_instruction(&instruction.name).to_bytes().to_vec()
            } else {
                instruction.discriminator.clone()
            };
            if let Some(other) = seen.insert(bytes.clone(), &instruction.name) {
                return Err(PrismError::config(
                    "idl.instructions",
                    format!(
                        "`{other}` and `{}` share the discriminator {bytes:?}",
                        instruction.name
                    ),
                ));
            }
            for arg in &instruction.args {
                check_type(&types, &arg.ty, &format!("argument `{}` of `{}`", arg.name, instruction.name))?;
            }
            match Discriminant::try_from(bytes.as_slice()) {
                Ok(discriminant) => {
                    discriminant_map.insert(discriminant, instruction.clone());
                }
                Err(_) => custom_discriminants.push((bytes, instruction.clone())),
            }
        }
        custom_discriminants.sort_by_key(|(bytes, _)| std::cmp::Reverse(bytes.len()));

        let mut events = HashMap::new();
        for event in &idl.events {
            if !types.contains_key(&event.name) {
                return Err(PrismError::config(
                    "idl.events",
                    format!("event `{}` has no type definition", event.name),
                ));
            }
            let discriminant = if event.discriminator.is_empty() {
                Discriminant::for_event(&event.name)
            } else {
                Discriminant::try_from(event.discriminator.as_slice()).map_err(|_| {
                    PrismError::config(
                        "idl.events",
                        format!(
                            "event `{}` has a {}-byte discriminator; only 8-byte event \
                             discriminators are supported",
                            event.name,
                            event.discriminator.len()
                        ),
                    )
                })?
            };
            events.insert(discriminant, event.name.clone());
        }
        for def in &idl.types {
            if matches!(def.ty, IdlTypeDefTy::Struct { .. }) && def.generics.is_empty() {
                events
                    .entry(Discriminant::for_event(&def.name))
                    .or_insert_with(|| def.name.clone());
            }
        }

        Ok(Self {
            program_id: idl.address.clone(),
            idl,
            discriminant_map,
            custom_discriminants,
            events,
            types,
        })
    }

    /// A decoder from the IDL's JSON, as `anchor build` writes it to
    /// `target/idl/<program>.json`.
    ///
    /// Fails with [`PrismError::ConfigError`] when the JSON is not an Anchor
    /// 0.30+ IDL, or for any reason [`Self::new`] does.
    pub fn from_json(json: &str) -> PrismResult<Self> {
        let idl: Idl = serde_json::from_str(json)
            .map_err(|err| PrismError::config("idl", format!("not an Anchor 0.30+ IDL: {err}")))?;
        Self::new(idl)
    }

    pub fn idl(&self) -> &Idl {
        &self.idl
    }

    /// Name of the program, from the IDL metadata.
    pub fn program_name(&self) -> &str {
        &self.idl.metadata.name
    }

    /// The precomputed discriminant → instruction map (8-byte
    /// discriminants only).
    pub fn discriminant_map(&self) -> &HashMap<Discriminant, IdlInstruction> {
        &self.discriminant_map
    }

    /// The IDL instruction `data` calls, if any.
    pub fn instruction_for(&self, data: &[u8]) -> Option<&IdlInstruction> {
        match self.target(data)? {
            Target::Instruction(instruction) => Some(instruction),
            Target::Event(_) | Target::UndescribedEvent(_) | Target::IdlAccount => None,
        }
    }

    /// What the first bytes of `data` select. An 8-byte instruction
    /// discriminant is tried first, then `emit_cpi!` events, then the IDL
    /// account instructions, then custom discriminants, longest first.
    fn target(&self, data: &[u8]) -> Option<Target<'_>> {
        let discriminant = Discriminant::from_data(data);
        if let Some(instruction) = discriminant.and_then(|d| self.discriminant_map.get(&d)) {
            return Some(Target::Instruction(instruction));
        }
        if discriminant == Some(EVENT_IX_TAG)
            && let Some(event) = Discriminant::from_data(&data[Discriminant::LEN..])
        {
            return Some(match self.events.get(&event) {
                Some(name) => Target::Event(name),
                None => Target::UndescribedEvent(event),
            });
        }
        if discriminant == Some(IDL_IX_TAG) && data.len() > Discriminant::LEN {
            return Some(Target::IdlAccount);
        }
        self.custom_discriminants
            .iter()
            .find(|(prefix, _)| data.starts_with(prefix))
            .map(|(_, instruction)| Target::Instruction(instruction))
    }

    fn decode_target(&self, target: Target<'_>, data: &[u8]) -> Result<(String, Value), String> {
        let mut reader = BorshReader {
            data,
            types: &self.types,
        };
        match target {
            Target::Instruction(instruction) => {
                let skip = if instruction.discriminator.is_empty() {
                    Discriminant::LEN
                } else {
                    instruction.discriminator.len()
                };
                reader.data = &data[skip..];
                let args = reader
                    .read_named_fields(&instruction.args, &Generics::new(), 0)
                    .map_err(|err| format!("instruction `{}`: {err}", instruction.name))?;
                Ok((instruction.name.clone(), Value::Object(args)))
            }
            Target::Event(name) => {
                reader.data = &data[2 * Discriminant::LEN..];
                let ty = IdlType::Defined {
                    name: name.to_owned(),
                    generics: Vec::new(),
                };
                let fields = reader
                    .read(&ty, &Generics::new(), 0)
                    .map_err(|err| format!("event `{name}`: {err}"))?;
                Ok((format!("{EVENT_TYPE_PREFIX}{name}"), fields))
            }
            Target::UndescribedEvent(discriminant) => {
                let mut fields = Map::new();
                fields.insert("discriminator".to_owned(), discriminant.to_string().into());
                fields.insert(
                    "data".to_owned(),
                    BASE64.encode(&data[2 * Discriminant::LEN..]).into(),
                );
                Ok((UNDESCRIBED_EVENT_TYPE.to_owned(), Value::Object(fields)))
            }
            Target::IdlAccount => {
                reader.data = &data[Discriminant::LEN..];
                read_idl_account_instruction(&mut reader)
                    .map(|(operation, fields)| (format!("{IDL_TYPE_PREFIX}{operation}"), fields))
                    .map_err(|err| format!("IDL account instruction: {err}"))
            }
        }
    }
}

impl Decoder for AnchorDecoder {
    fn program_id(&self) -> &str {
        &self.program_id
    }

    fn can_decode(&self, program_id: &str, data: &[u8]) -> bool {
        program_id == self.program_id && self.target(data).is_some()
    }

    fn decode(
        &self,
        instruction: &DeepAnaInstruction,
        _account_keys: &[String],
    ) -> PrismResult<DecodeResult> {
        if instruction.program_id != self.program_id {
            return Err(decode_error(
                instruction,
                format!("not an instruction of {}", self.program_id),
            ));
        }
        let data = instruction_data(instruction)?;
        let target = self.target(&data).ok_or_else(|| {
            let reason = match Discriminant::from_data(&data) {
                Some(discriminant) => format!("unknown discriminant {discriminant}"),
                None => format!("data too short: {} bytes", data.len()),
            };
            decode_error(instruction, reason)
        })?;
        let (instruction_type, decoded_data) = self
            .decode_target(target, &data)
            .map_err(|reason| decode_error(instruction, reason))?;
        Ok(DecodeResult {
            instruction_type,
            decoded_data,
            token_transfer: None,
        })
    }
}

/// The body of a legacy IDL-account instruction, Anchor's
/// `anchor_lang::idl::IdlInstruction` enum: the operation and its fields.
fn read_idl_account_instruction(reader: &mut BorshReader<'_, '_>) -> Result<(&'static str, Value), String> {
    let mut fields = Map::new();
    let operation = match reader.borsh::<u8>("operation tag")? {
        0 => {
            fields.insert("data_len".to_owned(), reader.borsh::<u64>("data_len")?.into());
            "create"
        }
        1 => "create_buffer",
        2 => {
            let bytes = reader.borsh::<Vec<u8>>("data")?;
            fields.insert("data".to_owned(), BASE64.encode(bytes).into());
            "write"
        }
        3 => "set_buffer",
        4 => {
            let key: [u8; 32] = reader.borsh("new_authority")?;
            fields.insert("new_authority".to_owned(), Pubkey::new_from_array(key).to_string().into());
            "set_authority"
        }
        5 => "close",
        6 => {
            fields.insert("data_len".to_owned(), reader.borsh::<u64>("data_len")?.into());
            "resize"
        }
        tag => return Err(format!("unknown operation tag {tag}")),
    };
    Ok((operation, Value::Object(fields)))
}

// ---------------------------------------------------------------------------
// IDL checks
// ---------------------------------------------------------------------------

/// Fails when `ty`, or any type it reaches, names a type `types` does not
/// define.
fn check_type(types: &HashMap<String, IdlTypeDef>, ty: &IdlType, at: &str) -> PrismResult<()> {
    let mut visited = HashSet::new();
    check_type_inner(types, ty, at, &mut visited)
}

fn check_type_inner<'a>(
    types: &'a HashMap<String, IdlTypeDef>,
    ty: &'a IdlType,
    at: &str,
    visited: &mut HashSet<&'a str>,
) -> PrismResult<()> {
    match ty {
        IdlType::Option(inner) | IdlType::Vec(inner) | IdlType::Array(inner, _) => {
            check_type_inner(types, inner, at, visited)
        }
        IdlType::Defined { name, generics } => {
            for generic in generics {
                if let IdlGenericArg::Type { ty } = generic {
                    check_type_inner(types, ty, at, visited)?;
                }
            }
            let def = types.get(name).ok_or_else(|| {
                PrismError::config("idl.types", format!("{at} refers to undefined type `{name}`"))
            })?;
            if !visited.insert(name) {
                return Ok(());
            }
            let fields: Vec<&IdlType> = match &def.ty {
                IdlTypeDefTy::Struct { fields } => defined_field_types(fields.as_ref()),
                IdlTypeDefTy::Enum { variants } => variants
                    .iter()
                    .flat_map(|variant| defined_field_types(variant.fields.as_ref()))
                    .collect(),
                IdlTypeDefTy::Type { alias } => vec![alias],
            };
            fields
                .into_iter()
                .try_for_each(|field| check_type_inner(types, field, at, visited))
        }
        _ => Ok(()),
    }
}

fn defined_field_types(fields: Option<&IdlDefinedFields>) -> Vec<&IdlType> {
    match fields {
        None => Vec::new(),
        Some(IdlDefinedFields::Named(fields)) => fields.iter().map(|field| &field.ty).collect(),
        Some(IdlDefinedFields::Tuple(types)) => types.iter().collect(),
    }
}

// ---------------------------------------------------------------------------
// Borsh → JSON
// ---------------------------------------------------------------------------

/// What a generic parameter of a defined type is bound to.
#[derive(Debug, Clone)]
enum Bound {
    Type(IdlType),
    Const(String),
}

type Generics = HashMap<String, Bound>;

/// Reads Borsh values off the front of `data`, guided by IDL types.
struct BorshReader<'a, 't> {
    data: &'a [u8],
    types: &'t HashMap<String, IdlTypeDef>,
}

impl BorshReader<'_, '_> {
    /// One Borsh value via the `borsh` crate, with the field's context on
    /// failure.
    fn borsh<T: BorshDeserialize>(&mut self, what: &str) -> Result<T, String> {
        T::deserialize(&mut self.data).map_err(|err| format!("{what}: {err}"))
    }

    fn take(&mut self, len: usize, what: &str) -> Result<&[u8], String> {
        if self.data.len() < len {
            return Err(format!(
                "{what}: needs {len} bytes, {} left",
                self.data.len()
            ));
        }
        let (head, rest) = self.data.split_at(len);
        self.data = rest;
        Ok(head)
    }

    fn read_named_fields(
        &mut self,
        fields: &[IdlField],
        generics: &Generics,
        depth: usize,
    ) -> Result<Map<String, Value>, String> {
        let mut out = Map::new();
        for field in fields {
            let value = self
                .read(&field.ty, generics, depth)
                .map_err(|err| format!("field `{}`: {err}", field.name))?;
            out.insert(field.name.clone(), value);
        }
        Ok(out)
    }

    fn read_fields(
        &mut self,
        fields: Option<&IdlDefinedFields>,
        generics: &Generics,
        depth: usize,
    ) -> Result<Value, String> {
        Ok(match fields {
            None => Value::Object(Map::new()),
            Some(IdlDefinedFields::Named(fields)) => {
                Value::Object(self.read_named_fields(fields, generics, depth)?)
            }
            Some(IdlDefinedFields::Tuple(types)) => types
                .iter()
                .enumerate()
                .map(|(i, ty)| {
                    self.read(ty, generics, depth)
                        .map_err(|err| format!("field {i}: {err}"))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into(),
        })
    }

    /// The next value, of type `ty`, as JSON.
    fn read(&mut self, ty: &IdlType, generics: &Generics, depth: usize) -> Result<Value, String> {
        if depth > MAX_TYPE_DEPTH {
            return Err(format!("types nest deeper than {MAX_TYPE_DEPTH} levels"));
        }
        let depth = depth + 1;
        Ok(match ty {
            IdlType::Bool => Value::Bool(self.borsh("bool")?),
            IdlType::U8 => self.borsh::<u8>("u8")?.into(),
            IdlType::I8 => self.borsh::<i8>("i8")?.into(),
            IdlType::U16 => self.borsh::<u16>("u16")?.into(),
            IdlType::I16 => self.borsh::<i16>("i16")?.into(),
            IdlType::U32 => self.borsh::<u32>("u32")?.into(),
            IdlType::I32 => self.borsh::<i32>("i32")?.into(),
            IdlType::U64 => self.borsh::<u64>("u64")?.into(),
            IdlType::I64 => self.borsh::<i64>("i64")?.into(),
            IdlType::U128 => self.borsh::<u128>("u128")?.to_string().into(),
            IdlType::I128 => self.borsh::<i128>("i128")?.to_string().into(),
            IdlType::F32 => float(f64::from(self.borsh::<f32>("f32")?)),
            IdlType::F64 => float(self.borsh::<f64>("f64")?),
            IdlType::U256 => le_to_decimal(self.take(32, "u256")?, false).into(),
            IdlType::I256 => le_to_decimal(self.take(32, "i256")?, true).into(),
            IdlType::String => Value::String(self.borsh("string")?),
            IdlType::Bytes => BASE64.encode(self.borsh::<Vec<u8>>("bytes")?).into(),
            IdlType::Pubkey => {
                let bytes: [u8; 32] = self.borsh("pubkey")?;
                Pubkey::new_from_array(bytes).to_string().into()
            }
            IdlType::Option(inner) => match self.borsh::<u8>("option tag")? {
                0 => Value::Null,
                1 => self.read(inner, generics, depth)?,
                tag => return Err(format!("option tag {tag} is neither 0 nor 1")),
            },
            IdlType::Vec(inner) => {
                let len = self.borsh::<u32>("vec length")?;
                let mut items = Vec::new();
                for i in 0..len {
                    let before = self.data.len();
                    items.push(
                        self.read(inner, generics, depth)
                            .map_err(|err| format!("element {i} of {len}: {err}"))?,
                    );
                    if self.data.len() == before {
                        return Err("vec of zero-sized elements".to_owned());
                    }
                }
                Value::Array(items)
            }
            IdlType::Array(inner, len) => {
                let len = array_len(len, generics)?;
                (0..len)
                    .map(|i| {
                        self.read(inner, generics, depth)
                            .map_err(|err| format!("element {i} of {len}: {err}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into()
            }
            IdlType::Defined {
                name,
                generics: args,
            } => self.read_defined(name, args, generics, depth)?,
            IdlType::Generic(name) => match generics.get(name) {
                Some(Bound::Type(bound)) => {
                    let bound = bound.clone();
                    self.read(&bound, &Generics::new(), depth)?
                }
                Some(Bound::Const(_)) => {
                    return Err(format!("const generic `{name}` used as a type"));
                }
                None => return Err(format!("unbound generic `{name}`")),
            },
            other => return Err(format!("unsupported IDL type {other:?}")),
        })
    }

    fn read_defined(
        &mut self,
        name: &str,
        args: &[IdlGenericArg],
        outer: &Generics,
        depth: usize,
    ) -> Result<Value, String> {
        let def = self
            .types
            .get(name)
            .ok_or_else(|| format!("undefined type `{name}`"))?;
        if def.serialization != IdlSerialization::Borsh {
            return Err(format!(
                "type `{name}` uses {:?} serialization; only borsh is supported",
                def.serialization
            ));
        }
        let generics = bind_generics(name, &def.generics, args, outer)?;
        match &def.ty {
            IdlTypeDefTy::Struct { fields } => self.read_fields(fields.as_ref(), &generics, depth),
            IdlTypeDefTy::Enum { variants } => {
                let tag = self.borsh::<u8>("enum tag")?;
                let variant = variants.get(usize::from(tag)).ok_or_else(|| {
                    format!("enum `{name}` has {} variants, got tag {tag}", variants.len())
                })?;
                Ok(match &variant.fields {
                    None => Value::String(variant.name.clone()),
                    Some(fields) => {
                        let value = self
                            .read_fields(Some(fields), &generics, depth)
                            .map_err(|err| format!("variant `{}`: {err}", variant.name))?;
                        let mut object = Map::new();
                        object.insert(variant.name.clone(), value);
                        Value::Object(object)
                    }
                })
            }
            IdlTypeDefTy::Type { alias } => self.read(alias, &generics, depth),
        }
    }
}

/// Binds the generic parameters of type `name` to `args`, resolved in the
/// caller's scope `outer`, so the type body can be read without it.
fn bind_generics(
    name: &str,
    params: &[IdlTypeDefGeneric],
    args: &[IdlGenericArg],
    outer: &Generics,
) -> Result<Generics, String> {
    if params.len() != args.len() {
        return Err(format!(
            "type `{name}` takes {} generic arguments, got {}",
            params.len(),
            args.len()
        ));
    }
    params
        .iter()
        .zip(args)
        .map(|(param, arg)| {
            let bound = match arg {
                IdlGenericArg::Type { ty } => Bound::Type(substitute(ty, outer)?),
                IdlGenericArg::Const { value } => Bound::Const(value.clone()),
            };
            let param = match param {
                IdlTypeDefGeneric::Type { name } | IdlTypeDefGeneric::Const { name, .. } => name,
            };
            Ok((param.clone(), bound))
        })
        .collect()
}

/// `ty` with every generic parameter of `generics` replaced by what it is
/// bound to.
fn substitute(ty: &IdlType, generics: &Generics) -> Result<IdlType, String> {
    let sub = |inner: &IdlType| substitute(inner, generics).map(Box::new);
    Ok(match ty {
        IdlType::Generic(name) => match generics.get(name) {
            Some(Bound::Type(bound)) => bound.clone(),
            Some(Bound::Const(_)) => return Err(format!("const generic `{name}` used as a type")),
            None => return Err(format!("unbound generic `{name}`")),
        },
        IdlType::Option(inner) => IdlType::Option(sub(inner)?),
        IdlType::Vec(inner) => IdlType::Vec(sub(inner)?),
        IdlType::Array(inner, len) => {
            IdlType::Array(sub(inner)?, IdlArrayLen::Value(array_len(len, generics)?))
        }
        IdlType::Defined { name, generics: args } => IdlType::Defined {
            name: name.clone(),
            generics: args
                .iter()
                .map(|arg| {
                    Ok(match arg {
                        IdlGenericArg::Type { ty } => IdlGenericArg::Type {
                            ty: substitute(ty, generics)?,
                        },
                        IdlGenericArg::Const { value } => match generics.get(value) {
                            Some(Bound::Const(bound)) => IdlGenericArg::Const {
                                value: bound.clone(),
                            },
                            _ => arg.clone(),
                        },
                    })
                })
                .collect::<Result<_, String>>()?,
        },
        other => other.clone(),
    })
}

fn array_len(len: &IdlArrayLen, generics: &Generics) -> Result<usize, String> {
    match len {
        IdlArrayLen::Value(len) => Ok(*len),
        IdlArrayLen::Generic(name) => match generics.get(name) {
            Some(Bound::Const(value)) => value
                .parse()
                .map_err(|_| format!("array length `{name}` = `{value}` is not a number")),
            _ => Err(format!("array length `{name}` is not bound")),
        },
    }
}

/// A float as a JSON number; `null` for infinities, which JSON cannot hold
/// (Borsh already rejects NaN).
fn float(value: f64) -> Value {
    Number::from_f64(value).map_or(Value::Null, Value::Number)
}

/// A little-endian integer of any width as a decimal string; `signed` reads
/// it as two's complement.
fn le_to_decimal(bytes: &[u8], signed: bool) -> String {
    let mut limbs: Vec<u32> = bytes
        .chunks(4)
        .map(|chunk| {
            let mut limb = [0; 4];
            limb[..chunk.len()].copy_from_slice(chunk);
            u32::from_le_bytes(limb)
        })
        .collect();
    let negative = signed && bytes.last().is_some_and(|&top| top & 0x80 != 0);
    if negative {
        // Two's complement: invert and add one.
        let mut carry = true;
        for limb in &mut limbs {
            let (sum, overflow) = (!*limb).overflowing_add(u32::from(carry));
            *limb = sum;
            carry = overflow;
        }
    }

    const CHUNK: u64 = 1_000_000_000;
    let mut chunks = Vec::new();
    while limbs.iter().any(|&limb| limb != 0) {
        let mut remainder = 0u64;
        for limb in limbs.iter_mut().rev() {
            let current = (remainder << 32) | u64::from(*limb);
            *limb = (current / CHUNK) as u32;
            remainder = current % CHUNK;
        }
        chunks.push(remainder);
    }
    let Some((most, rest)) = chunks.split_last() else {
        return "0".to_owned();
    };
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    out.push_str(&most.to_string());
    for chunk in rest.iter().rev() {
        out.push_str(&format!("{chunk:09}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::le_to_decimal;

    #[test]
    fn decimal_strings_of_wide_integers() {
        assert_eq!(le_to_decimal(&[0; 32], false), "0");
        assert_eq!(le_to_decimal(&[0; 32], true), "0");
        assert_eq!(le_to_decimal(&u128::MAX.to_le_bytes(), false), u128::MAX.to_string());
        assert_eq!(le_to_decimal(&i128::MIN.to_le_bytes(), true), i128::MIN.to_string());
        assert_eq!(le_to_decimal(&(-1i128).to_le_bytes(), true), "-1");
        assert_eq!(
            le_to_decimal(&[0xff; 32], false),
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
        assert_eq!(le_to_decimal(&[0xff; 32], true), "-1");
        let mut i256_min = [0u8; 32];
        i256_min[31] = 0x80;
        assert_eq!(
            le_to_decimal(&i256_min, true),
            "-57896044618658097711785492504343953926634992332820282019728792003956564819968"
        );
    }
}
