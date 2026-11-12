//! Entities: names and aliases for addresses, both ways.
//!
//! The built-in set holds only addresses that are the same on every cluster
//! (native programs, the SPL programs, the wrapped SOL mints, a few sysvars),
//! so it is right on devnet, testnet and mainnet alike. A host adds its own
//! with [`EntityDef`]s; they are validated with the built-ins, and an extra
//! that collides with a built-in is an error, never an override. To change a
//! built-in, change [`BUILTIN_ENTITIES`].
//!
//! # Keys
//!
//! An entity is found by its [normalised](normalize) name, any normalised
//! alias, or its exact address. Within one catalog every key belongs to one
//! entity:
//!
//! * no two entities share a key or an address;
//! * no key is itself a 32-byte base58 key, so a pasted address never hits
//!   a name;
//! * no key equals another entity's address normalised, so `entity(address)`
//!   and `entity(name)` cannot disagree.
//!
//! So [`EntityLookup::Ambiguous`] never comes from a [`Catalog`]; it exists
//! for callers that merge entities from several sources.
//!
//! [`Catalog`]: super::Catalog

use std::collections::HashMap;

use serde::Serialize;

use super::{CatalogError, MAX_ENTITY_ALIASES, MAX_ENTITY_NAME_LEN, MAX_EXTRA_ENTITIES, is_base58_key};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityKind {
    Program,
    Mint,
    Account,
}

/// An entity as a host supplies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityDef {
    pub name: String,
    pub aliases: Vec<String>,
    pub address: String,
    pub kind: EntityKind,
}

/// A validated entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entity {
    /// As given, trimmed.
    pub name: String,
    /// The normalised name with spaces as `-`: `spl-token-2022`.
    pub slug: String,
    /// As given, trimmed.
    pub aliases: Vec<String>,
    pub address: String,
    pub kind: EntityKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntityLookup<'a> {
    Found(&'a Entity),
    /// More than one entity, sorted by slug. Never from one validated
    /// catalog; see the module docs.
    Ambiguous(Vec<&'a Entity>),
    NotFound,
}

/// Trim, lowercase, turn `-`, `_` and `.` into spaces, and collapse runs of
/// whitespace. `None` when nothing is left.
pub fn normalize(s: &str) -> Option<String> {
    let replaced: String =
        s.to_lowercase().chars().map(|c| if matches!(c, '-' | '_' | '.') { ' ' } else { c }).collect();
    let joined = replaced.split_whitespace().collect::<Vec<_>>().join(" ");
    (!joined.is_empty()).then_some(joined)
}

/// A built-in entity, as a constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinEntity {
    pub name: &'static str,
    pub aliases: &'static [&'static str],
    pub address: &'static str,
    pub kind: EntityKind,
}

const fn builtin(
    name: &'static str,
    aliases: &'static [&'static str],
    address: &'static str,
    kind: EntityKind,
) -> BuiltinEntity {
    BuiltinEntity { name, aliases, address, kind }
}

use EntityKind::{Account, Mint, Program};

/// Addresses that are the same on every cluster. The names of the programs
/// `digest/` labels match its `PROGRAM_NAMES`.
pub const BUILTIN_ENTITIES: &[BuiltinEntity] = &[
    builtin("System Program", &["system"], "11111111111111111111111111111111", Program),
    builtin(
        "SPL Token",
        &["token program", "spl token program", "tokenkeg"],
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        Program,
    ),
    builtin(
        "SPL Token-2022",
        &["token 2022", "token 2022 program", "token extensions"],
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
        Program,
    ),
    builtin(
        "Associated Token Account",
        &["ata", "associated token program", "associated token account program"],
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        Program,
    ),
    builtin("Compute Budget", &["compute budget program"], "ComputeBudget111111111111111111111111111111", Program),
    builtin("Vote", &["vote program"], "Vote111111111111111111111111111111111111111", Program),
    builtin("Memo", &["memo program", "spl memo"], "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr", Program),
    builtin("Stake", &["stake program"], "Stake11111111111111111111111111111111111111", Program),
    builtin("Config", &["config program"], "Config1111111111111111111111111111111111111", Program),
    builtin(
        "BPF Upgradeable Loader",
        &["bpf loader upgradeable", "upgradeable loader"],
        "BPFLoaderUpgradeab1e11111111111111111111111",
        Program,
    ),
    builtin(
        "Address Lookup Table",
        &["address lookup table program", "alt", "lookup table program"],
        "AddressLookupTab1e1111111111111111111111111",
        Program,
    ),
    builtin("Ed25519 Program", &["ed25519 sigverify"], "Ed25519SigVerify111111111111111111111111111", Program),
    builtin("Secp256k1 Program", &["keccak secp256k1"], "KeccakSecp256k11111111111111111111111111111", Program),
    builtin(
        "Wrapped SOL",
        &["wsol", "native mint", "wrapped sol mint"],
        "So11111111111111111111111111111111111111112",
        Mint,
    ),
    builtin(
        "Wrapped SOL Token-2022",
        &["token 2022 native mint", "wsol 2022"],
        "9pan9bMn5HatX4EJdBwg9VgCa7Uz5HL8N1m5D3NdXejP",
        Mint,
    ),
    builtin("Clock Sysvar", &["sysvar clock"], "SysvarC1ock11111111111111111111111111111111", Account),
    builtin("Rent Sysvar", &["sysvar rent"], "SysvarRent111111111111111111111111111111111", Account),
    builtin("Incinerator", &["burn address"], "1nc1nerator11111111111111111111111111111111", Account),
];

pub(crate) fn builtin_defs() -> Vec<EntityDef> {
    BUILTIN_ENTITIES
        .iter()
        .map(|b| EntityDef {
            name: b.name.to_owned(),
            aliases: b.aliases.iter().map(|a| (*a).to_owned()).collect(),
            address: b.address.to_owned(),
            kind: b.kind,
        })
        .collect()
}

/// A name or alias: 1 to [`MAX_ENTITY_NAME_LEN`] bytes once trimmed, ASCII
/// letters, digits, spaces, `-`, `_` and `.`, something left once
/// normalised, and not shaped like an address. Returns its key.
fn check_name(owner: &str, what: &str, s: &str) -> Result<String, CatalogError> {
    let err = |reason: String| CatalogError::BadEntity { name: owner.to_owned(), reason };
    let t = s.trim();
    if t.is_empty() || t.len() > MAX_ENTITY_NAME_LEN {
        return Err(err(format!("{what} must be 1 to {MAX_ENTITY_NAME_LEN} bytes")));
    }
    if !t.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b' ' | b'-' | b'_' | b'.')) {
        return Err(err(format!("{what} {t:?} may only hold ASCII letters, digits, spaces, '-', '_' and '.'")));
    }
    let key = normalize(t).ok_or_else(|| err(format!("{what} {t:?} has no letters or digits")))?;
    if is_base58_key(&key) {
        return Err(err(format!("{what} {t:?} looks like an address")));
    }
    Ok(key)
}

/// The validated entity and its keys (name first).
fn check_def(def: EntityDef) -> Result<(Entity, Vec<String>), CatalogError> {
    let name = def.name.trim().to_owned();
    let owner = if name.is_empty() { def.name.clone() } else { name.clone() };
    let err = |reason: String| CatalogError::BadEntity { name: owner.clone(), reason };
    let name_key = check_name(&owner, "the name", &def.name)?;
    if !is_base58_key(&def.address) {
        return Err(err("the address is not a 32-byte base58 key".to_owned()));
    }
    if def.aliases.len() > MAX_ENTITY_ALIASES {
        return Err(err(format!("at most {MAX_ENTITY_ALIASES} aliases")));
    }
    let mut keys = vec![name_key.clone()];
    let mut aliases = Vec::with_capacity(def.aliases.len());
    for alias in &def.aliases {
        let key = check_name(&owner, "an alias", alias)?;
        if keys.contains(&key) {
            return Err(err(format!("alias {:?} repeats its name or another alias", alias.trim())));
        }
        keys.push(key);
        aliases.push(alias.trim().to_owned());
    }
    let slug = name_key.replace(' ', "-");
    Ok((Entity { name, slug, aliases, address: def.address, kind: def.kind }, keys))
}

/// The validated entities and their keys.
#[derive(Debug)]
pub(crate) struct EntityIndex {
    /// Sorted by slug.
    entities: Vec<Entity>,
    /// Normalised name or alias → index.
    keys: HashMap<String, usize>,
    /// Exact address → index.
    by_address: HashMap<String, usize>,
}

impl EntityIndex {
    /// Built-ins first, then extras, each in order: the first bad entity or
    /// conflict is returned.
    pub(crate) fn build(builtins: Vec<EntityDef>, extras: Vec<EntityDef>) -> Result<Self, CatalogError> {
        if extras.len() > MAX_EXTRA_ENTITIES {
            return Err(CatalogError::BadEntity {
                name: format!("({} extra entities)", extras.len()),
                reason: format!("at most {MAX_EXTRA_ENTITIES} extra entities"),
            });
        }
        let mut entities: Vec<(Entity, Vec<String>)> = Vec::with_capacity(builtins.len() + extras.len());
        // Key / normalised address / exact address → owner's name.
        let mut keys: HashMap<String, String> = HashMap::new();
        let mut address_keys: HashMap<String, String> = HashMap::new();
        let mut addresses: HashMap<String, String> = HashMap::new();
        let conflict = |key: &str, existing: &str, new: &str| CatalogError::EntityConflict {
            key: key.to_owned(),
            existing: existing.to_owned(),
            new: new.to_owned(),
        };
        for def in builtins.into_iter().chain(extras) {
            let (entity, entity_keys) = check_def(def)?;
            let new = entity.name.as_str();
            if let Some(existing) = addresses.get(&entity.address) {
                return Err(conflict(&entity.address, existing, new));
            }
            let address_key = normalize(&entity.address).expect("a base58 key is not blank");
            for key in &entity_keys {
                if let Some(existing) = keys.get(key).or_else(|| address_keys.get(key)) {
                    return Err(conflict(key, existing, new));
                }
            }
            if let Some(existing) = keys.get(&address_key) {
                return Err(conflict(&address_key, existing, new));
            }
            for key in &entity_keys {
                keys.insert(key.clone(), new.to_owned());
            }
            address_keys.insert(address_key, new.to_owned());
            addresses.insert(entity.address.clone(), new.to_owned());
            entities.push((entity, entity_keys));
        }

        entities.sort_by(|a, b| a.0.slug.cmp(&b.0.slug));
        let mut index = Self { entities: Vec::new(), keys: HashMap::new(), by_address: HashMap::new() };
        for (i, (entity, entity_keys)) in entities.into_iter().enumerate() {
            for key in entity_keys {
                index.keys.insert(key, i);
            }
            index.by_address.insert(entity.address.clone(), i);
            index.entities.push(entity);
        }
        Ok(index)
    }

    pub(crate) fn lookup(&self, name: &str) -> EntityLookup<'_> {
        let mut hits: Vec<usize> = Vec::with_capacity(2);
        if let Some(&i) = normalize(name).and_then(|k| self.keys.get(&k)) {
            hits.push(i);
        }
        if let Some(&i) = self.by_address.get(name) {
            hits.push(i);
        }
        // Indices follow slug order, so sorting them sorts by slug.
        hits.sort_unstable();
        hits.dedup();
        match hits.as_slice() {
            [] => EntityLookup::NotFound,
            [i] => EntityLookup::Found(&self.entities[*i]),
            many => EntityLookup::Ambiguous(many.iter().map(|&i| &self.entities[i]).collect()),
        }
    }

    pub(crate) fn name_of(&self, address: &str) -> Option<&str> {
        self.by_address.get(address).map(|&i| self.entities[i].name.as_str())
    }

    pub(crate) fn all(&self) -> &[Entity] {
        &self.entities
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, aliases: &[&str], address: &str) -> EntityDef {
        EntityDef {
            name: name.to_owned(),
            aliases: aliases.iter().map(|a| (*a).to_owned()).collect(),
            address: address.to_owned(),
            kind: EntityKind::Program,
        }
    }

    /// A valid key no built-in uses.
    const A: &str = "US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx"; // [7; 32]

    #[test]
    fn normalisation() {
        assert_eq!(normalize("  SPL  Token-2022 ").as_deref(), Some("spl token 2022"));
        assert_eq!(normalize("a_b.c-d").as_deref(), Some("a b c d"));
        assert_eq!(normalize("A\t\n B").as_deref(), Some("a b"));
        assert_eq!(normalize(" -_. "), None);
        assert_eq!(normalize(""), None);
    }

    #[test]
    fn a_def_is_trimmed_and_slugged() {
        let (e, keys) = check_def(def("  My Program v2 ", &[" mp "], A)).unwrap();
        assert_eq!((e.name.as_str(), e.slug.as_str()), ("My Program v2", "my-program-v2"));
        assert_eq!(e.aliases, vec!["mp"]);
        assert_eq!(keys, vec!["my program v2", "mp"]);
    }

    #[test]
    fn bad_defs() {
        let cases = [
            def("", &[], A),
            def("   ", &[], A),
            def("---", &[], A),
            def("a'b", &[], A),
            def("Résumé", &[], A),
            def(&"a".repeat(65), &[], A),
            def("x", &[], "not-an-address"),
            def("x", &[], "1111111111111111111111111111111"),
            def("x", &["X"], A),
            def("x", &["y", "Y"], A),
            def("x", &["a"; 9], A),
            def("11111111111111111111111111111111", &[], A),
            def("x", &["11111111111111111111111111111111"], A),
        ];
        for d in cases {
            assert!(matches!(check_def(d.clone()), Err(CatalogError::BadEntity { .. })), "{d:?}");
        }
    }

    #[test]
    fn eight_aliases_are_allowed() {
        let aliases: Vec<String> = (0..8).map(|i| format!("alias {i}")).collect();
        let refs: Vec<&str> = aliases.iter().map(String::as_str).collect();
        assert!(check_def(def("x", &refs, A)).is_ok());
    }
}
