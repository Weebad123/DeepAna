//! The tables `intent/` reads a question with: phrase tables, structural and
//! filler words, units, the four templates and their example questions.
//!
//! Everything here is a constant, reviewed like code, and checked by
//! [`PhraseTables::validate`] when an [`Interpreter`](super::Interpreter) is
//! built. A mistake fails startup with an [`IntentError`], never a user.
//!
//! # Names
//!
//! Targets are catalog ids. The design names several dimensions the catalog
//! calls differently, and some it does not have:
//!
//! | design                          | catalog                         |
//! |---------------------------------|---------------------------------|
//! | `program_id`                    | `program`                       |
//! | `mint_address`                  | `mint`                          |
//! | `outcome`                       | `tx_status`                     |
//! | `depth`                         | `ix_level`                      |
//! | `source_token_account`          | `source_owner` (owners, not token accounts) |
//! | `destination_token_account`     | `destination_owner`             |
//! | `instruction_type`, `transfer_kind`, `finalized` | none: no phrase, no default measure |
//!
//! So `finalized slots` and `instruction types` are not in the tables, and
//! `decoded instructions` (`ix_decoded = decoded`) and `token programs`
//! (`token_program`) are, because the catalog has them.
//!
//! # Phrases
//!
//! A phrase is 1 to [`MAX_PHRASE_TOKENS`] words, matched exactly on the
//! ASCII-lowercased words of the question. A phrase may be in both tables
//! (`programs`, `mints`); the template decides by position. Within a table a
//! phrase has one target.
//!
//! `slot` and `slots` are window words only after a number (`last 1000
//! slots`, `per 200 slots`), so `slots` may also be a measure phrase. The
//! kind words (`mint`, `program`, …) act only directly before an address,
//! so they may also be dimension phrases.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use serde::Serialize;
use thiserror::Error;

use crate::catalog::{Catalog, EntityKind, EntityLookup};

/// Most words in one phrase, and in one entity name the interpreter tries.
pub const MAX_PHRASE_TOKENS: usize = 4;

// ---------------------------------------------------------------------------
// Templates
// ---------------------------------------------------------------------------

/// The four question shapes, in the fixed order ambiguity is reported in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TemplateId {
    TopN,
    ByDimension,
    OverTime,
    Total,
}

impl TemplateId {
    pub const ALL: [TemplateId; 4] = [Self::TopN, Self::ByDimension, Self::OverTime, Self::Total];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopN => "top_n",
            Self::ByDimension => "by_dimension",
            Self::OverTime => "over_time",
            Self::Total => "total",
        }
    }
}

impl fmt::Display for TemplateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A template's description and example questions. The shapes themselves
/// are code, in `mod.rs`; a template not listed here is never matched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TemplateDef {
    pub id: TemplateId,
    pub description: &'static str,
    /// Each must interpret as `Matched` with this template's id.
    pub examples: &'static [&'static str],
}

/// The built-in templates, in their fixed order.
///
/// The examples are checked under the interpreter's own slot length and
/// limits, so they are kept within what any reasonable deployment allows:
/// at most 10 rows or buckets, and windows of at most 2 hours, which is
/// 72 000 slots at the shortest slot (100 ms).
pub const TEMPLATES: &[TemplateDef] = &[
    TemplateDef {
        id: TemplateId::TopN,
        description: "The top N groups of one dimension, ranked by a measure (its default one if none is named).",
        examples: &[
            "top 10 mints by transfers in the last hour",
            "busiest programs",
            "top 5 leaders by slots in the last 2 hours",
            "most active senders",
            "mints by transfer volume",
        ],
    },
    TemplateDef {
        id: TemplateId::ByDimension,
        description: "One or more measures broken down by a dimension, largest first.",
        examples: &[
            "transfers by mint in the last hour",
            "transactions by status",
            "instructions per program for the last 30 minutes",
            "cpi instructions per program",
        ],
    },
    TemplateDef {
        id: TemplateId::OverTime,
        description: "One or more measures per slot bucket, oldest first.",
        examples: &[
            "failure rate over time in the last 2 hours",
            "transfers per 1000 slots",
            "average fee every 100 slots in the last 1000 slots",
        ],
    },
    TemplateDef {
        id: TemplateId::Total,
        description: "One or more measures over the whole window.",
        examples: &[
            "how many failed transactions in the last hour",
            "instructions for the vote program in the last 1000 slots",
            "total fees",
            "how many distinct mints",
            "wsol transfer volume",
        ],
    },
];

// ---------------------------------------------------------------------------
// Phrase tables
// ---------------------------------------------------------------------------

/// A fixed filter a measure phrase carries: `successful transactions` is
/// `tx_count` with `tx_status = success`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct ImpliedFilter {
    pub dimension: &'static str,
    pub value: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MeasurePhrase {
    pub phrase: &'static str,
    pub measure: &'static str,
    pub implied: Option<ImpliedFilter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DimensionPhrase {
    pub phrase: &'static str,
    pub dimension: &'static str,
}

/// What `top <dimension>` ranks by when no measure is named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DefaultMeasure {
    pub dimension: &'static str,
    pub measure: &'static str,
}

const fn m(phrase: &'static str, measure: &'static str) -> MeasurePhrase {
    MeasurePhrase { phrase, measure, implied: None }
}

const fn mf(
    phrase: &'static str,
    measure: &'static str,
    dimension: &'static str,
    value: &'static str,
) -> MeasurePhrase {
    MeasurePhrase { phrase, measure, implied: Some(ImpliedFilter { dimension, value }) }
}

const fn d(phrase: &'static str, dimension: &'static str) -> DimensionPhrase {
    DimensionPhrase { phrase, dimension }
}

/// The built-in measure phrases.
pub const MEASURE_PHRASES: &[MeasurePhrase] = &[
    m("slots", "slot_count"),
    m("blocks", "slot_count"),
    m("slot count", "slot_count"),
    m("transactions", "tx_count"),
    m("txs", "tx_count"),
    m("tx", "tx_count"),
    m("transaction count", "tx_count"),
    mf("successful transactions", "tx_count", "tx_status", "success"),
    mf("successful txs", "tx_count", "tx_status", "success"),
    m("failed transactions", "tx_failed_count"),
    m("failed txs", "tx_failed_count"),
    m("transaction failures", "tx_failed_count"),
    m("failure rate", "tx_failure_ratio"),
    m("failure ratio", "tx_failure_ratio"),
    m("transaction failure rate", "tx_failure_ratio"),
    m("error rate", "tx_failure_ratio"),
    m("total fees", "fee_total_lamports"),
    // Not "total" + "fees paid": the longest phrase wins, so this needs its own entry.
    m("total fees paid", "fee_total_lamports"),
    m("fees paid", "fee_total_lamports"),
    m("fees", "fee_total_lamports"),
    m("average fee", "fee_avg_lamports"),
    m("avg fee", "fee_avg_lamports"),
    m("mean fee", "fee_avg_lamports"),
    m("average compute units", "compute_units_avg"),
    m("avg compute units", "compute_units_avg"),
    m("instructions", "ix_count"),
    m("ixs", "ix_count"),
    m("instruction count", "ix_count"),
    mf("top-level instructions", "ix_count", "ix_level", "top_level"),
    mf("top level instructions", "ix_count", "ix_level", "top_level"),
    mf("decoded instructions", "ix_count", "ix_decoded", "decoded"),
    m("cpi instructions", "ix_cpi_count"),
    m("inner instructions", "ix_cpi_count"),
    m("cpis", "ix_cpi_count"),
    m("undecoded instructions", "ix_undecoded_count"),
    m("distinct programs", "program_count"),
    m("unique programs", "program_count"),
    m("programs", "program_count"),
    m("transfers", "transfer_count"),
    m("token transfers", "transfer_count"),
    m("transfer count", "transfer_count"),
    m("unattributed transfers", "transfer_unattributed_count"),
    m("transfers without a mint", "transfer_unattributed_count"),
    m("distinct mints", "mint_count"),
    m("unique mints", "mint_count"),
    m("mints", "mint_count"),
    m("volume", "transfer_volume_raw"),
    m("transfer volume", "transfer_volume_raw"),
    m("token volume", "transfer_volume_raw"),
];

/// The built-in dimension phrases. `token program` (singular) is missing on
/// purpose: it is an alias of SPL Token.
pub const DIMENSION_PHRASES: &[DimensionPhrase] = &[
    d("programs", "program"),
    d("program", "program"),
    d("mints", "mint"),
    d("mint", "mint"),
    d("tokens", "mint"),
    d("leaders", "leader"),
    d("leader", "leader"),
    d("validators", "leader"),
    d("status", "tx_status"),
    d("transaction status", "tx_status"),
    d("outcome", "tx_status"),
    d("level", "ix_level"),
    d("instruction level", "ix_level"),
    d("depth", "ix_level"),
    d("decoding", "ix_decoded"),
    d("token programs", "token_program"),
    d("senders", "source_owner"),
    d("sources", "source_owner"),
    d("source owners", "source_owner"),
    d("source owner", "source_owner"),
    d("recipients", "destination_owner"),
    d("receivers", "destination_owner"),
    d("destinations", "destination_owner"),
    d("destination owners", "destination_owner"),
    d("destination owner", "destination_owner"),
];

/// One entry per rankable dimension.
pub const DEFAULT_MEASURES: &[DefaultMeasure] = &[
    DefaultMeasure { dimension: "program", measure: "ix_count" },
    DefaultMeasure { dimension: "ix_level", measure: "ix_count" },
    DefaultMeasure { dimension: "ix_decoded", measure: "ix_count" },
    DefaultMeasure { dimension: "mint", measure: "transfer_count" },
    DefaultMeasure { dimension: "source_owner", measure: "transfer_count" },
    DefaultMeasure { dimension: "destination_owner", measure: "transfer_count" },
    DefaultMeasure { dimension: "token_program", measure: "transfer_count" },
    DefaultMeasure { dimension: "tx_status", measure: "tx_count" },
    DefaultMeasure { dimension: "leader", measure: "slot_count" },
];

// ---------------------------------------------------------------------------
// Structural words
// ---------------------------------------------------------------------------

/// Dropped, but only at the start of a question.
pub const LEAD_WORDS: &[&str] = &["how many", "how much", "number of", "count of", "total"];
pub const TOP_WORDS: &[&str] = &["top", "busiest", "most active", "most used"];
pub const BY_WORDS: &[&str] = &["by", "per", "grouped by", "broken down by", "split by", "for each"];
/// Over forms without a width. The forms with one are [`OVER_NUMBER_FORMS`].
pub const OVER_WORDS: &[&str] = &["over time", "per bucket", "by bucket"];
/// Introduce an entity at the end of a question.
pub const PREPOSITIONS: &[&str] = &["for", "of", "on", "involving", "with"];
/// Joins measures.
pub const AND_WORDS: &[&str] = &["and"];
/// Dropped anywhere. Inside a phrase (`transfers without a mint`) a filler
/// word is part of the phrase.
pub const FILLER_WORDS: &[&str] = &[
    "the", "please", "me", "us", "show", "tell", "give", "list", "what", "whats", "what's", "is", "are", "was", "were",
    "there", "which", "a", "an", "all",
];

/// `slot` or `slots`: a window or bucket unit after a number.
pub const SLOT_UNITS: &[&str] = &["slot", "slots"];

/// A time unit and its length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl TimeUnit {
    pub const ALL: [TimeUnit; 5] = [Self::Second, Self::Minute, Self::Hour, Self::Day, Self::Week];

    pub const fn millis(self) -> u64 {
        match self {
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
            Self::Week => 604_800_000,
        }
    }

    /// The words that name it.
    pub const fn words(self) -> &'static [&'static str] {
        match self {
            Self::Second => &["second", "seconds", "sec", "secs"],
            Self::Minute => &["minute", "minutes", "min", "mins"],
            Self::Hour => &["hour", "hours", "hr", "hrs"],
            Self::Day => &["day", "days"],
            Self::Week => &["week", "weeks"],
        }
    }

    pub const fn name(self, plural: bool) -> &'static str {
        match (self, plural) {
            (Self::Second, false) => "second",
            (Self::Second, true) => "seconds",
            (Self::Minute, false) => "minute",
            (Self::Minute, true) => "minutes",
            (Self::Hour, false) => "hour",
            (Self::Hour, true) => "hours",
            (Self::Day, false) => "day",
            (Self::Day, true) => "days",
            (Self::Week, false) => "week",
            (Self::Week, true) => "weeks",
        }
    }

    pub fn from_word(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|u| u.words().contains(&word))
    }
}

/// May open a `last …` window: `in the last hour`, `over the past 2 days`.
pub const WINDOW_LEADS: &[&str] = &["in", "over", "during", "for", "within"];
/// `last`, `past`, `previous`: a rolling window ending at the snapshot.
pub const LAST_WORDS: &[&str] = &["last", "past", "previous"];
/// Open a slot range: `from slot 5 to 9`, `between slots 5 and 9`.
pub const RANGE_LEADS: &[&str] = &["from", "between"];
/// Close a slot range: `slots 5 to 9`. `and` only after `between`.
pub const RANGE_SEPARATORS: &[&str] = &["to", "through", "until"];
/// Open a single-slot window: `in slot 5`.
pub const SINGLE_SLOT_LEADS: &[&str] = &["in", "at"];

/// Words of a calendar or of a unit longer than a week. Never converted.
pub const CALENDAR_WORDS: &[&str] = &[
    "yesterday",
    "today",
    "tonight",
    "tomorrow",
    "morning",
    "afternoon",
    "evening",
    "midnight",
    "noon",
    "weekend",
    "ago",
    "since",
    "hourly",
    "daily",
    "weekly",
    "monthly",
    "month",
    "months",
    "year",
    "years",
    "quarter",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];

/// A word that names an address's kind when directly before it: `mint
/// <address>`. Maps to the dimension it filters and the catalog kind an
/// address it names must have, if the catalog knows the address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindWord {
    pub word: &'static str,
    pub dimension: &'static str,
    pub kind: EntityKind,
}

pub const KIND_WORDS: &[KindWord] = &[
    KindWord { word: "mint", dimension: "mint", kind: EntityKind::Mint },
    KindWord { word: "program", dimension: "program", kind: EntityKind::Program },
    KindWord { word: "leader", dimension: "leader", kind: EntityKind::Account },
    KindWord { word: "source", dimension: "source_owner", kind: EntityKind::Account },
    KindWord { word: "destination", dimension: "destination_owner", kind: EntityKind::Account },
];

pub fn kind_word(word: &str) -> Option<&'static KindWord> {
    KIND_WORDS.iter().find(|k| k.word == word)
}

/// The dimension a catalog entity of `kind` filters. `None` for an
/// account: whether it is a source or a destination is unclear.
pub const fn entity_dimension(kind: EntityKind) -> Option<&'static str> {
    match kind {
        EntityKind::Program => Some("program"),
        EntityKind::Mint => Some("mint"),
        EntityKind::Account => None,
    }
}

/// One element of an over form with a width.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pat {
    /// Any one of these words.
    Word(&'static [&'static str]),
    /// The bucket width.
    Num,
}

/// `per N slots`, `every N slots`, `in buckets of N slots`, `in N slot buckets`.
pub const OVER_NUMBER_FORMS: &[&[Pat]] = &[
    &[Pat::Word(&["per"]), Pat::Num, Pat::Word(SLOT_UNITS)],
    &[Pat::Word(&["every"]), Pat::Num, Pat::Word(SLOT_UNITS)],
    &[Pat::Word(&["in"]), Pat::Word(&["buckets"]), Pat::Word(&["of"]), Pat::Num, Pat::Word(SLOT_UNITS)],
    &[Pat::Word(&["in"]), Pat::Num, Pat::Word(&["slot"]), Pat::Word(&["buckets"])],
];

/// What a structural word does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Structural {
    Lead,
    Top,
    By,
    Over,
    Prep,
    And,
    Filler,
}

/// Every structural phrase with its role, in a fixed order.
pub fn structural_words() -> Vec<(&'static str, Structural)> {
    let groups: [(&[&'static str], Structural); 7] = [
        (LEAD_WORDS, Structural::Lead),
        (TOP_WORDS, Structural::Top),
        (BY_WORDS, Structural::By),
        (OVER_WORDS, Structural::Over),
        (PREPOSITIONS, Structural::Prep),
        (AND_WORDS, Structural::And),
        (FILLER_WORDS, Structural::Filler),
    ];
    groups.iter().flat_map(|(words, role)| words.iter().map(move |w| (*w, *role))).collect()
}

/// Every word a phrase may not be, and what it is instead. Structural and
/// filler words, time units, window words and calendar words. Not the slot
/// units and not the kind words; see the module docs.
fn reserved_words() -> Vec<(&'static str, &'static str)> {
    let mut v: Vec<(&'static str, &'static str)> =
        structural_words().into_iter().map(|(w, _)| (w, "a structural or filler word")).collect();
    v.extend(TimeUnit::ALL.iter().flat_map(|u| u.words().iter().map(|w| (*w, "a time unit"))));
    for words in [WINDOW_LEADS, LAST_WORDS, RANGE_LEADS, RANGE_SEPARATORS, SINGLE_SLOT_LEADS] {
        v.extend(words.iter().map(|w| (*w, "a window word")));
    }
    for form in OVER_NUMBER_FORMS {
        for p in *form {
            if let Pat::Word(words) = p {
                v.extend(words.iter().filter(|w| !SLOT_UNITS.contains(w)).map(|w| (*w, "a bucket word")));
            }
        }
    }
    v.extend(CALENDAR_WORDS.iter().map(|w| (*w, "a calendar word")));
    v
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why an [`Interpreter`](super::Interpreter) could not be built. Startup
/// only: [`interpret`](super::Interpreter::interpret) never fails.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum IntentError {
    #[error("{table} phrase {phrase:?}: {reason}")]
    BadPhraseTable { table: &'static str, phrase: String, reason: String },
    #[error("phrase {phrase:?} clashes with {with}")]
    PhraseClash { phrase: String, with: String },
    #[error("implied filter of {phrase:?}: {reason}")]
    BadImpliedFilter { phrase: String, reason: String },
    #[error("default measure for dimension {dimension:?}: {reason}")]
    BadDefaultMeasure { dimension: String, reason: String },
    #[error("slot_millis {value} is outside 100..=2000")]
    BadSlotMillis { value: u32 },
    #[error("template {id} is listed twice")]
    DuplicateTemplate { id: TemplateId },
    #[error("template {template} example {example:?} does not interpret as {template}: {reason}")]
    BadExample { template: TemplateId, example: String, reason: String },
}

// ---------------------------------------------------------------------------
// The tables, together
// ---------------------------------------------------------------------------

/// Every table an interpreter reads. [`builtin`](Self::builtin) is what
/// ORACLE runs with; tests build others to show each startup check fires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhraseTables {
    pub measures: Vec<MeasurePhrase>,
    pub dimensions: Vec<DimensionPhrase>,
    pub default_measures: Vec<DefaultMeasure>,
    pub templates: Vec<TemplateDef>,
}

impl Default for PhraseTables {
    fn default() -> Self {
        Self::builtin()
    }
}

/// A phrase as the interpreter matches it: ASCII-lowercased words joined by
/// one space.
pub fn normalize_phrase(phrase: &str) -> String {
    phrase.split_ascii_whitespace().map(str::to_ascii_lowercase).collect::<Vec<_>>().join(" ")
}

/// Digits, optionally grouped by commas in threes (`1,000`), as a `u64`.
pub fn parse_number(word: &str) -> Option<u64> {
    let mut groups = word.split(',');
    let first = groups.next()?;
    if first.is_empty() || first.len() > 3 && word.contains(',') || !first.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut digits = first.to_owned();
    for g in groups {
        if g.len() != 3 || !g.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        digits.push_str(g);
    }
    digits.parse().ok()
}

/// Every contiguous run of words in `phrase`, longest first.
fn sub_spans(phrase: &str) -> Vec<String> {
    let words: Vec<&str> = phrase.split(' ').collect();
    let mut spans = Vec::new();
    for len in (1..=words.len()).rev() {
        for start in 0..=words.len() - len {
            spans.push(words[start..start + len].join(" "));
        }
    }
    spans
}

/// The entity `phrase`, or any run of its words, names.
fn entity_clash(catalog: &Catalog, phrase: &str) -> Option<String> {
    sub_spans(phrase).into_iter().find_map(|span| match catalog.entity(&span) {
        EntityLookup::Found(e) => Some(format!("entity {:?} (as {span:?})", e.name)),
        EntityLookup::Ambiguous(es) => Some(format!("entity {:?} (as {span:?})", es[0].name)),
        EntityLookup::NotFound => None,
    })
}

impl PhraseTables {
    pub fn builtin() -> Self {
        Self {
            measures: MEASURE_PHRASES.to_vec(),
            dimensions: DIMENSION_PHRASES.to_vec(),
            default_measures: DEFAULT_MEASURES.to_vec(),
            templates: TEMPLATES.to_vec(),
        }
    }

    /// The shape of one phrase: words, count, characters.
    fn check_shape(table: &'static str, phrase: &str) -> Result<String, IntentError> {
        let bad = |reason: String| IntentError::BadPhraseTable { table, phrase: phrase.to_owned(), reason };
        let norm = normalize_phrase(phrase);
        if norm.is_empty() {
            return Err(bad("the phrase is empty".to_owned()));
        }
        let words: Vec<&str> = norm.split(' ').collect();
        if words.len() > MAX_PHRASE_TOKENS {
            return Err(bad(format!("{} words, at most {MAX_PHRASE_TOKENS}", words.len())));
        }
        for w in &words {
            let ok_chars = w.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'\'');
            let edges = w.as_bytes().first().zip(w.as_bytes().last());
            let ok_edges = edges.is_some_and(|(a, z)| a.is_ascii_alphanumeric() && z.is_ascii_alphanumeric());
            if !ok_chars || !ok_edges {
                return Err(bad(format!(
                    "word {w:?} can never match: words are ASCII letters and digits, with '-' or '\\'' inside"
                )));
            }
            if parse_number(w).is_some() {
                return Err(bad(format!("word {w:?} is a number")));
            }
        }
        Ok(norm)
    }

    /// Duplicates, reserved words and entity names.
    fn check_clashes(
        catalog: &Catalog,
        reserved: &BTreeMap<&'static str, &'static str>,
        seen: &mut BTreeMap<String, String>,
        table: &'static str,
        phrase: &str,
        norm: &str,
        target: &str,
    ) -> Result<(), IntentError> {
        let clash = |with: String| IntentError::PhraseClash { phrase: phrase.to_owned(), with };
        if let Some(previous) = seen.get(norm) {
            return Err(clash(format!("the {table} phrase for {previous} (a phrase has one target per table)")));
        }
        if let Some(what) = reserved.get(norm) {
            return Err(clash((*what).to_owned()));
        }
        if let Some(entity) = entity_clash(catalog, norm) {
            return Err(clash(entity));
        }
        seen.insert(norm.to_owned(), target.to_owned());
        Ok(())
    }

    /// Every check in the order [`Interpreter::new`](super::Interpreter::new)
    /// documents, except the examples, which need the interpreter.
    pub(crate) fn validate(&self, catalog: &Catalog) -> Result<(), IntentError> {
        // Templates.
        let mut ids = BTreeSet::new();
        for t in &self.templates {
            if !ids.insert(t.id) {
                return Err(IntentError::DuplicateTemplate { id: t.id });
            }
        }

        // Structural words: distinct among themselves, and not entity names.
        let mut reserved: BTreeMap<&'static str, &'static str> = BTreeMap::new();
        let mut roles: BTreeMap<String, Structural> = BTreeMap::new();
        for (w, role) in structural_words() {
            if roles.insert(normalize_phrase(w), role).is_some() {
                return Err(IntentError::PhraseClash {
                    phrase: w.to_owned(),
                    with: "another structural word".to_owned(),
                });
            }
        }
        for (w, what) in reserved_words() {
            reserved.entry(w).or_insert(what);
        }
        let kind_words = KIND_WORDS.iter().map(|k| k.word);
        let slot_units = SLOT_UNITS.iter().copied();
        for w in reserved.keys().copied().chain(kind_words).chain(slot_units) {
            if let Some(entity) = entity_clash(catalog, w) {
                return Err(IntentError::PhraseClash { phrase: w.to_owned(), with: entity });
            }
        }

        // Measure phrases.
        let mut seen = BTreeMap::new();
        for p in &self.measures {
            let norm = Self::check_shape("measures", p.phrase)?;
            let Some(measure) = catalog.measure(p.measure) else {
                return Err(IntentError::BadPhraseTable {
                    table: "measures",
                    phrase: p.phrase.to_owned(),
                    reason: format!("unknown measure {:?}", p.measure),
                });
            };
            Self::check_clashes(catalog, &reserved, &mut seen, "measures", p.phrase, &norm, p.measure)?;
            if let Some(f) = p.implied {
                let bad = |reason: String| IntentError::BadImpliedFilter { phrase: p.phrase.to_owned(), reason };
                let Some(dim) = catalog.dimension(f.dimension) else {
                    return Err(bad(format!("unknown dimension {:?}", f.dimension)));
                };
                dim.filter_sql(f.value).map_err(|e| bad(format!("value {:?}: {e}", f.value)))?;
                if !dim.table.includes(measure.table) {
                    return Err(bad(format!(
                        "dimension {} is not on {}, the table {} reads",
                        dim.id, measure.table, measure.id
                    )));
                }
            }
        }

        // Dimension phrases.
        let mut seen = BTreeMap::new();
        for p in &self.dimensions {
            let norm = Self::check_shape("dimensions", p.phrase)?;
            let bad = |reason: String| IntentError::BadPhraseTable {
                table: "dimensions",
                phrase: p.phrase.to_owned(),
                reason,
            };
            let Some(dim) = catalog.dimension(p.dimension) else {
                return Err(bad(format!("unknown dimension {:?}", p.dimension)));
            };
            if dim.parameterised {
                return Err(bad(format!("dimension {} takes a parameter; bucket by time with an over form", dim.id)));
            }
            Self::check_clashes(catalog, &reserved, &mut seen, "dimensions", p.phrase, &norm, p.dimension)?;
        }

        // Default measures: each valid, at most one per dimension, and one
        // for every dimension a phrase names.
        let mut defaults = BTreeSet::new();
        for dm in &self.default_measures {
            let bad = |reason: String| IntentError::BadDefaultMeasure { dimension: dm.dimension.to_owned(), reason };
            if catalog.dimension(dm.dimension).is_none() {
                return Err(bad("unknown dimension".to_owned()));
            }
            if catalog.measure(dm.measure).is_none() {
                return Err(bad(format!("unknown measure {:?}", dm.measure)));
            }
            if !catalog.compatible(dm.measure, dm.dimension) {
                return Err(bad(format!("measure {} cannot be grouped by {}", dm.measure, dm.dimension)));
            }
            if !defaults.insert(dm.dimension) {
                return Err(bad("listed twice".to_owned()));
            }
        }
        for p in &self.dimensions {
            if !defaults.contains(p.dimension) {
                return Err(IntentError::BadDefaultMeasure {
                    dimension: p.dimension.to_owned(),
                    reason: format!("phrase {:?} names it, but it has no default measure", p.phrase),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numbers() {
        assert_eq!(parse_number("0"), Some(0));
        assert_eq!(parse_number("1000"), Some(1000));
        assert_eq!(parse_number("1,000"), Some(1000));
        assert_eq!(parse_number("12,345,678"), Some(12_345_678));
        for bad in ["", "1,00", "1000,000", ",100", "1,", "1.5", "-1", "+1", "1e3", "x", "99999999999999999999999"] {
            assert_eq!(parse_number(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn phrases_normalise() {
        assert_eq!(normalize_phrase("  Top  10 "), "top 10");
        assert_eq!(normalize_phrase(""), "");
    }

    #[test]
    fn sub_spans_are_longest_first() {
        assert_eq!(sub_spans("a b c"), ["a b c", "a b", "b c", "a", "b", "c"]);
    }

    #[test]
    fn units() {
        assert_eq!(TimeUnit::from_word("hrs"), Some(TimeUnit::Hour));
        assert_eq!(TimeUnit::from_word("slots"), None);
        assert_eq!(TimeUnit::Week.millis(), 7 * TimeUnit::Day.millis());
    }

    #[test]
    fn every_template_appears_once_in_order() {
        let ids: Vec<TemplateId> = TEMPLATES.iter().map(|t| t.id).collect();
        assert_eq!(ids, TemplateId::ALL);
        assert!(TEMPLATES.iter().all(|t| !t.examples.is_empty()));
    }
}
