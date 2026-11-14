//! Typed questions to structured queries.
//!
//! `catalog/` defines what can be asked, `spec/` validates, compiles and runs
//! a [`QuerySpec`]. `intent/` turns a short question, `top 10 mints by
//! transfers in the last hour`, into that `QuerySpec`, with fixed templates
//! and the catalog's names. It is pure and synchronous: no LENS call, no
//! store, no I/O.
//!
//! | File          | Holds                                                           |
//! |---------------|-----------------------------------------------------------------|
//! | `mod.rs`      | [`Interpreter`]: tokenise, extract, match, assemble, validate, suggest |
//! | [`templates`] | phrase tables, structural and filler words, units, the four templates and their examples |
//!
//! # Decisions
//!
//! * **Every word is accounted for.** A word that is not part of a window,
//!   an address, an entity, a phrase, a structural or a filler word is
//!   [`NoMatchReason::UnknownWord`]. `top 10 mints by transfers excluding
//!   wsol` does not quietly drop `excluding wsol`.
//! * **Exact matching** on ASCII-lowercased words. No model, no fuzzy
//!   matching, no typo correction. A miss returns example questions.
//! * **Four templates**, each with an optional entity and an optional window.
//! * **A [`Matched`] spec always passes [`guard::validate`]** under the
//!   interpreter's limits: intent runs the guard itself, and a refusal
//!   becomes [`Interpretation::Rejected`] carrying the guard's message.
//! * **Ambiguity is reported, not resolved**: readings that give different
//!   valid specs are all returned, in template order.
//! * **Time becomes slots here, and says so**: `slots = ceil(amount ×
//!   unit_ms / slot_millis)`, and every time window adds
//!   [`Assumption::TimeConverted`] naming the slots and `slot_millis`.
//! * **ASCII-only case folding, no Unicode normalisation.** Tokens keep their
//!   text; only the lowercase form is matched against phrases. Base58 keys
//!   are case-sensitive and are used exactly as typed.
//! * **An address needs a kind word**: `mint <key>`, `program <key>`,
//!   `leader <key>`, `source <key>`, `destination <key>`. A bare one is
//!   [`NoMatchReason::BareKey`].
//! * **One entity per question.** **No calendar words.**
//! * **The tables are validated at startup** ([`Interpreter::new`]), and
//!   every template example must interpret as itself.
//!
//! # The pipeline of [`Interpreter::interpret`]
//!
//! 1. **Validate the text**: not empty, at most [`MAX_TEXT_CHARS`]
//!    characters, no control characters except tab, CR and LF (which count
//!    as spaces), at most [`MAX_TOKENS`] words.
//! 2. **Tokenise**: split on whitespace, strip leading and trailing ASCII
//!    punctuation (`mints?` is `mints`; `top-level` keeps its hyphen).
//! 3. **Calendar words** anywhere: [`NoMatchReason::CalendarWord`].
//! 4. **Extract the window** (below). Two are [`NoMatchReason::TwoWindows`];
//!    a leftover `last`, `past` or `previous` is
//!    [`NoMatchReason::UnsupportedWindow`].
//! 5. **Extract keys**: a 32-byte base58 word right after a kind word.
//! 6. **Extract entities**: at each position, spans of up to 4 words,
//!    longest first, against [`Catalog::entity`]. An entity directly
//!    followed by its kind word takes it (`wsol mint`).
//! 7. **Type the rest**: at each position the longest of a bucket form with
//!    a width (`per 200 slots`), a phrase of up to 4 words (measure,
//!    dimension or structural), or a number. Filler is dropped.
//! 8. **Any word still untyped** is [`NoMatchReason::UnknownWord`]. Then
//!    more than one entity is [`NoMatchReason::TooManyEntities`], and a
//!    number not right after `top` is [`NoMatchReason::MisplacedNumber`].
//! 9. **Match templates** over the typed stream.
//! 10. **Assemble** each reading's spec, fill defaults, run the guard.
//! 11. **Check ambiguity** across the templates that matched.
//!
//! Each position tries a bounded number of spans (4 entity spans, 4 bucket
//! forms, 4 phrase spans) and each template is one left-to-right pass with
//! no backtracking, so the work is linear in the number of words, which is
//! itself capped.
//!
//! # Window forms
//!
//! | Form                                                     | Window                  |
//! |----------------------------------------------------------|-------------------------|
//! | `[in\|over\|during\|for\|within] [the] last\|past\|previous [N] <time unit>` | `LastSlots(ceil(N × unit / slot_millis))`, N = 1 if absent |
//! | `[in\|…] [the] last\|past\|previous [N] slot\|slots`     | `LastSlots(N)`          |
//! | `[from\|between] slot\|slots A to\|through\|until [slot\|slots] B` (`and` after `between`) | `Range { A, B }` |
//! | `in\|at slot N`                                          | `Range { N, N }`        |
//! | none                                                     | `LastSlots(min(DEFAULT_WINDOW_SLOTS, max_slot_span))`, with [`Assumption::DefaultWindow`] |
//!
//! Time units: seconds, minutes, hours, days and weeks. A zero amount is
//! passed through as `LastSlots(0)` and the guard refuses it, rather than
//! rounded up to one slot.
//!
//! # Templates
//!
//! After the window and filler are removed, leading [lead
//! words](templates::LEAD_WORDS) are dropped and the entity, if any, is
//! taken from the end (`… for <entity>`, after a preposition) or from
//! directly before the first measure or dimension phrase. The rest must fit:
//!
//! | id             | Shape                                                | Spec                                                  |
//! |----------------|------------------------------------------------------|-------------------------------------------------------|
//! | `top_n`        | `Top [N] Dimension [By Measure+]`, `Dimension By Measure+` | group by the dimension, order by the first measure descending, limit N or [`DEFAULT_TOP`] |
//! | `by_dimension` | `Measure+ By Dimension`                              | the same, limit [`DEFAULT_TOP`]                       |
//! | `over_time`    | `Measure+ Over`                                      | group by `slot_bucket`, ascending, limit = the buckets the window holds |
//! | `total`        | `Measure+`                                           | no grouping, no order, no limit                       |
//!
//! `Measure+` is one to [`MAX_QUESTION_MEASURES`] measure phrases,
//! optionally joined by `and`. `top_n` without a measure ranks by the
//! dimension's [default measure](templates::DEFAULT_MEASURES). An automatic
//! bucket width is `max(1, ceil(window_slots / min(50, result_cap)))`.
//!
//! A measure phrase with an [implied filter](templates::ImpliedFilter)
//! must be the question's only measure: its filter applies to the whole
//! query, so it would silently change any other measure.

pub mod templates;

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use serde::Serialize;

use crate::catalog::{Catalog, EntityKind, EntityLookup, MAX_BUCKET_WIDTH_SLOTS, is_base58_key};
use crate::config::DEFAULT_MAX_SLOT_SPAN;
use crate::spec::guard::{self, SpecLimits};
use crate::spec::{Direction, Filter, FilterOp, GroupBy, Order, QuerySpec, SlotWindow, SpecError, Window};

use templates::{
    FILLER_WORDS, KindWord, LAST_WORDS, MAX_PHRASE_TOKENS, OVER_NUMBER_FORMS, Pat, RANGE_LEADS, RANGE_SEPARATORS,
    SINGLE_SLOT_LEADS, SLOT_UNITS, Structural, WINDOW_LEADS, entity_dimension, kind_word, normalize_phrase,
    parse_number, structural_words,
};
pub use templates::{ImpliedFilter, IntentError, PhraseTables, TemplateId, TimeUnit};

/// Longest question, in characters.
pub const MAX_TEXT_CHARS: usize = 256;
/// Most words in a question.
pub const MAX_TOKENS: usize = 32;
/// Rows `top_n` and `by_dimension` return when no number is given (at most
/// the row cap).
pub const DEFAULT_TOP: u32 = 10;
/// The window when none is given (at most the slot span): about an hour at
/// 400 ms a slot.
pub const DEFAULT_WINDOW_SLOTS: u64 = 9_000;
/// Buckets an automatic bucket width aims for (at most the row cap).
pub const AUTO_BUCKETS: u64 = 50;
pub const MIN_SLOT_MILLIS: u32 = 100;
pub const MAX_SLOT_MILLIS: u32 = 2_000;
/// Most measures one question may name.
pub const MAX_QUESTION_MEASURES: usize = 4;
/// Example questions a [`Interpretation::NoMatch`] carries.
pub const MAX_SUGGESTIONS: usize = 4;
/// Most results [`Interpreter::suggest`] returns, whatever `limit` is.
pub const MAX_SUGGEST_LIMIT: usize = 50;

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// What a question means, or why it means nothing ORACLE will run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Interpretation {
    Matched(Box<Matched>),
    /// At least two readings with different valid specs, in template order.
    Ambiguous {
        candidates: Vec<Candidate>,
    },
    NoMatch {
        reason: NoMatchReason,
        suggestions: Vec<String>,
    },
    /// The question was understood, and the spec it means is refused.
    Rejected {
        code: RejectCode,
        message: String,
    },
    /// Not a question at all.
    Invalid {
        reason: InvalidReason,
    },
}

/// One reading of a question.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Matched {
    pub template_id: TemplateId,
    /// Passed [`guard::validate`] under the interpreter's limits.
    pub spec: QuerySpec,
    /// The spec in words, from catalog labels.
    pub understood_as: String,
    /// Everything filled in or converted, in a fixed order: the window, the
    /// measure, the limit, the bucket width.
    pub assumptions: Vec<Assumption>,
    /// The entity the question names, if any.
    pub entities: Vec<ResolvedEntity>,
}

/// An [`Interpretation::Ambiguous`] candidate.
pub type Candidate = Matched;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Assumption {
    /// No window was given.
    DefaultWindow { slots: u64 },
    /// A time window converted to slots: approximate, slots vary in length.
    TimeConverted { amount: u64, unit: TimeUnit, slots: u64, slot_millis: u32 },
    /// No row count was given.
    DefaultLimit { n: u32 },
    /// `top <dimension>` with no measure ranks by this one.
    DefaultMeasure { measure: &'static str },
    /// No bucket width was given.
    AutoBucketWidth { width: u64, buckets: u64 },
}

/// The entity a question names, and the filter it became.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolvedEntity {
    /// The words of the question that named it.
    pub text: String,
    /// The catalog's name, if the catalog knows the address.
    pub name: Option<String>,
    pub address: String,
    pub kind: EntityKind,
    /// The dimension it filters.
    pub dimension: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum NoMatchReason {
    UnknownWord {
        word: String,
    },
    CalendarWord {
        word: String,
    },
    /// `last` without a unit ORACLE understands.
    UnsupportedWindow {
        word: String,
    },
    TwoWindows,
    /// An address with no kind word before it.
    BareKey {
        key: String,
    },
    TooManyEntities {
        count: usize,
    },
    MisplacedNumber {
        number: String,
    },
    TooManyMeasures {
        count: usize,
        max: usize,
    },
    /// Every word is known, and they fit no template.
    NoTemplate,
}

/// The fixed hint for an address without a kind word.
pub const BARE_KEY_HINT: &str = "An address needs a kind word before it: mint <address>, program <address>, \
                                 leader <address>, source <address> or destination <address>.";

impl NoMatchReason {
    pub fn message(&self) -> String {
        match self {
            Self::UnknownWord { word } => {
                format!("{word:?} is not a word ORACLE understands here, and no word is ignored.")
            }
            Self::CalendarWord { word } => format!(
                "{word:?}: calendar words are not supported. Ask for a rolling window, such as \
                 \"in the last 24 hours\" or \"in the last 1000 slots\"."
            ),
            Self::UnsupportedWindow { word } => format!(
                "{word:?} does not start a window ORACLE understands. Use \"in the last N \
                 seconds|minutes|hours|days|weeks\", \"in the last N slots\" or \"from slot A to B\"."
            ),
            Self::TwoWindows => "A question can have only one window.".to_owned(),
            Self::BareKey { .. } => BARE_KEY_HINT.to_owned(),
            Self::TooManyEntities { count } => {
                format!("A question can name one program, mint or account for now; this one names {count}.")
            }
            Self::MisplacedNumber { number } => format!(
                "{number} is not understood where it is: a number may follow \"top\", or be part of a window \
                 or a bucket width (\"per 200 slots\")."
            ),
            Self::TooManyMeasures { count, max } => format!("{count} measures; a question can ask for at most {max}."),
            Self::NoTemplate => "Every word is known, but together they do not form a question ORACLE can answer. \
                                 See the examples."
                .to_owned(),
        }
    }
}

/// Why an understood question is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectCode {
    /// The measures read different tables.
    MixedTables,
    RepeatedMeasure,
    /// The measure needs a dimension grouped by or filtered to one value.
    NeedsDimension,
    /// The group-by dimension is not on the measure's table.
    DimensionWrongTable,
    /// The entity's dimension is not on the measure's table.
    EntityWrongTable,
    /// An implied filter and the entity filter the same dimension.
    FilterConflict,
    /// A measure phrase with an implied filter alongside other measures.
    ImpliedFilterNotAlone,
    /// An account entity: source or destination is unclear.
    AccountEntity,
    /// The kind word contradicts what the catalog knows the address to be.
    KindMismatch,
    Window,
    BucketWidth,
    /// More rows than the row cap: a large `top N`, or too many buckets.
    Limit,
    /// Any other guard refusal.
    Guard,
}

impl RejectCode {
    /// The fixed hint appended to the guard's message.
    pub const fn hint(self) -> Option<&'static str> {
        match self {
            Self::MixedTables => {
                Some("Ask about measures of one table at a time: slots, transactions, instructions or token transfers.")
            }
            Self::RepeatedMeasure => Some("Name each measure once."),
            Self::NeedsDimension => Some("Group by that dimension (\"by mint\"), or name one entity of it."),
            Self::DimensionWrongTable => Some(
                "Group by a dimension of the measure's own table: program, level or decoding for instructions; \
                 mint, token program, sender or recipient for transfers; status for transactions; leader for slots.",
            ),
            Self::EntityWrongTable => Some(
                "A program filters instructions, a mint filters token transfers, and a leader filters slots: \
                 ask about a measure of the entity's own table.",
            ),
            Self::FilterConflict => Some("The phrase already filters that dimension; drop one of the two."),
            Self::Window => Some("Ask for a shorter window."),
            Self::BucketWidth => Some("Use a bucket width from 1 slot to half the window."),
            Self::Limit => Some("Ask for fewer rows; over time, use a wider bucket or a shorter window."),
            Self::ImpliedFilterNotAlone | Self::AccountEntity | Self::KindMismatch | Self::Guard => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvalidReason {
    Empty,
    TooLong {
        max_chars: usize,
    },
    /// `position` is the character index.
    ControlCharacter {
        position: usize,
    },
    TooManyTokens {
        count: usize,
        max: usize,
    },
}

/// A template, for `/intent/templates`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TemplateInfo {
    pub id: TemplateId,
    pub description: &'static str,
    pub examples: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/// One word of the question.
#[derive(Debug)]
struct Tok<'a> {
    /// As typed, punctuation stripped.
    text: &'a str,
    /// ASCII-lowercased.
    lower: String,
}

/// What a typing-table entry is.
#[derive(Debug, Clone, Copy, Default)]
struct Entry {
    structural: Option<Structural>,
    /// Index into the measure phrases.
    measure: Option<usize>,
    dimension: Option<&'static str>,
}

/// A typed element of the question.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum T {
    Lead,
    Top,
    By,
    /// A bucket form, with its width if it gives one.
    Over(Option<u64>),
    Prep,
    And,
    /// The value, and the word's index.
    Number(u64, usize),
    /// Index into the entity list.
    Entity(usize),
    /// A measure phrase, a dimension phrase, or both.
    Phrase {
        measure: Option<usize>,
        dimension: Option<&'static str>,
    },
}

/// What claimed a word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mark {
    Free,
    /// The window, an entity's or a key's later words.
    Taken,
    /// The first word of entity `k`.
    Entity(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowHit {
    Slots(u64),
    Time { amount: u64, unit: TimeUnit },
    Range { from: u64, to: u64 },
}

/// Where an entity came from.
#[derive(Debug, Clone, Copy)]
enum Source {
    Catalog,
    Key(&'static KindWord),
}

#[derive(Debug, Clone)]
struct EntityRef {
    text: String,
    name: Option<String>,
    address: String,
    kind: EntityKind,
    source: Source,
}

/// One template's reading of the typed stream.
#[derive(Debug, Clone)]
struct Reading {
    template: TemplateId,
    dimension: Option<&'static str>,
    /// Indices into the measure phrases.
    measures: Vec<usize>,
    top: Option<u64>,
    width: Option<u64>,
}

type Rejection = (RejectCode, String);

/// Lookups one interpretation made, to pin the work bound.
#[derive(Debug, Default, Clone, Copy)]
struct Work {
    entity_lookups: usize,
    phrase_lookups: usize,
    form_checks: usize,
}

fn check_text(text: &str) -> Result<(), InvalidReason> {
    if text.chars().nth(MAX_TEXT_CHARS).is_some() {
        return Err(InvalidReason::TooLong { max_chars: MAX_TEXT_CHARS });
    }
    if let Some(position) = text.chars().position(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r')) {
        return Err(InvalidReason::ControlCharacter { position });
    }
    Ok(())
}

fn tokenise(text: &str) -> Vec<Tok<'_>> {
    text.split_whitespace()
        .map(|w| w.trim_matches(|c: char| c.is_ascii_punctuation()))
        .filter(|w| !w.is_empty())
        .map(|text| Tok { text, lower: text.to_ascii_lowercase() })
        .collect()
}

/// The window starting at `toks[0]`, and how many words it takes.
fn window_at(toks: &[Tok<'_>]) -> Option<(usize, WindowHit)> {
    let w = |k: usize| toks.get(k).map(|t| t.lower.as_str());
    let is = |k: usize, set: &[&str]| w(k).is_some_and(|x| set.contains(&x));

    // [lead] [the] last [N] unit
    let mut k = usize::from(is(0, WINDOW_LEADS));
    if w(k) == Some("the") {
        k += 1;
    }
    if is(k, LAST_WORDS) {
        k += 1;
        let amount = w(k).and_then(parse_number);
        if amount.is_some() {
            k += 1;
        }
        let amount = amount.unwrap_or(1);
        if is(k, SLOT_UNITS) {
            return Some((k + 1, WindowHit::Slots(amount)));
        }
        if let Some(unit) = w(k).and_then(TimeUnit::from_word) {
            return Some((k + 1, WindowHit::Time { amount, unit }));
        }
    }

    // [from|between] slot(s) A to [slot(s)] B
    let between = w(0) == Some("between");
    let mut k = usize::from(is(0, RANGE_LEADS));
    if is(k, SLOT_UNITS)
        && let Some(from) = w(k + 1).and_then(parse_number)
        && w(k + 2).is_some_and(|x| RANGE_SEPARATORS.contains(&x) || (between && x == "and"))
    {
        k += 3;
        if is(k, SLOT_UNITS) {
            k += 1;
        }
        if let Some(to) = w(k).and_then(parse_number) {
            return Some((k + 1, WindowHit::Range { from, to }));
        }
    }

    // in|at slot N
    if is(0, SINGLE_SLOT_LEADS)
        && w(1) == Some("slot")
        && let Some(n) = w(2).and_then(parse_number)
    {
        return Some((3, WindowHit::Range { from: n, to: n }));
    }
    None
}

/// The bucket width `form` reads at the start of `toks`.
fn form_at(form: &[Pat], toks: &[Tok<'_>]) -> Option<u64> {
    if toks.len() < form.len() {
        return None;
    }
    let mut width = None;
    for (p, t) in form.iter().zip(toks) {
        match p {
            Pat::Word(words) if words.contains(&t.lower.as_str()) => {}
            Pat::Num => width = Some(parse_number(&t.lower)?),
            Pat::Word(_) => return None,
        }
    }
    width
}

fn kind_name(kind: EntityKind) -> &'static str {
    match kind {
        EntityKind::Program => "program",
        EntityKind::Mint => "mint",
        EntityKind::Account => "account",
    }
}

fn summary(i: &Interpretation) -> String {
    match i {
        Interpretation::Matched(m) => format!("matched {}", m.template_id),
        Interpretation::Ambiguous { candidates } => format!(
            "ambiguous between {}",
            candidates.iter().map(|c| c.template_id.as_str()).collect::<Vec<_>>().join(", ")
        ),
        Interpretation::NoMatch { reason, .. } => format!("no match: {}", reason.message()),
        Interpretation::Rejected { code, message } => format!("rejected ({code:?}): {message}"),
        Interpretation::Invalid { reason } => format!("invalid: {reason:?}"),
    }
}

/// Words that say something, for ranking suggestions.
fn content_words(text: &str) -> BTreeSet<String> {
    tokenise(text).into_iter().map(|t| t.lower).filter(|w| !FILLER_WORDS.contains(&w.as_str())).collect()
}

// ---------------------------------------------------------------------------
// Interpreter
// ---------------------------------------------------------------------------

/// Turns questions into specs. Immutable and `Send + Sync`; share it.
pub struct Interpreter {
    catalog: Arc<Catalog>,
    slot_millis: u32,
    limits: SpecLimits,
    tables: PhraseTables,
    /// Normalised phrase → what it is.
    entries: HashMap<String, Entry>,
    /// Template, example, its content words; in template order.
    examples: Vec<(TemplateId, &'static str, BTreeSet<String>)>,
}

impl fmt::Debug for Interpreter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Interpreter")
            .field("slot_millis", &self.slot_millis)
            .field("limits", &self.limits)
            .field("phrases", &self.entries.len())
            .finish_non_exhaustive()
    }
}

impl Interpreter {
    /// The built-in tables, with `result_cap` as the row cap and the default
    /// slot span ([`DEFAULT_MAX_SLOT_SPAN`]).
    ///
    /// Checks, in order, returning the first failure:
    ///
    /// 1. `slot_millis` is within 100..=2000;
    /// 2. template ids are unique;
    /// 3. structural words are distinct and none is an entity name or alias;
    /// 4. each measure phrase: 1 to 4 well-formed words, a real measure,
    ///    unique in its table, not a reserved word, no run of its words an
    ///    entity name or alias; its implied filter names a real filterable
    ///    dimension of the measure's table with a valid value;
    /// 5. each dimension phrase: the same, for an unparameterised dimension;
    /// 6. each default measure exists, is compatible with its dimension, and
    ///    every dimension a phrase names has one;
    /// 7. every example of every template interprets as `Matched` with that
    ///    template's id, under these limits. The built-in examples need a
    ///    row cap of at least 10 and a slot span of at least 72 000 slots.
    pub fn new(catalog: Arc<Catalog>, slot_millis: u32, result_cap: u32) -> Result<Self, IntentError> {
        Self::with_limits(
            catalog,
            slot_millis,
            SpecLimits { max_rows: result_cap, max_slot_span: DEFAULT_MAX_SLOT_SPAN },
        )
    }

    /// With the runner's own limits, so that what intent matches is exactly
    /// what `spec/` accepts. Hosts with an `OracleConfig` pass
    /// [`SpecLimits::from_config`].
    pub fn with_limits(catalog: Arc<Catalog>, slot_millis: u32, limits: SpecLimits) -> Result<Self, IntentError> {
        Self::from_tables(catalog, slot_millis, limits, PhraseTables::builtin())
    }

    /// With other tables, under the same checks as [`new`](Self::new).
    pub fn from_tables(
        catalog: Arc<Catalog>,
        slot_millis: u32,
        limits: SpecLimits,
        tables: PhraseTables,
    ) -> Result<Self, IntentError> {
        if !(MIN_SLOT_MILLIS..=MAX_SLOT_MILLIS).contains(&slot_millis) {
            return Err(IntentError::BadSlotMillis { value: slot_millis });
        }
        tables.validate(&catalog)?;

        let mut entries: HashMap<String, Entry> = HashMap::new();
        for (w, role) in structural_words() {
            entries.entry(normalize_phrase(w)).or_default().structural = Some(role);
        }
        for (i, p) in tables.measures.iter().enumerate() {
            entries.entry(normalize_phrase(p.phrase)).or_default().measure = Some(i);
        }
        for p in &tables.dimensions {
            let id = catalog.dimension(p.dimension).expect("validated").id;
            entries.entry(normalize_phrase(p.phrase)).or_default().dimension = Some(id);
        }
        let examples = tables
            .templates
            .iter()
            .flat_map(|t| t.examples.iter().map(move |e| (t.id, *e, content_words(e))))
            .collect();

        let me = Self { catalog, slot_millis, limits, tables, entries, examples };
        for t in &me.tables.templates {
            for example in t.examples {
                match me.interpret(example) {
                    Interpretation::Matched(m) if m.template_id == t.id => {}
                    other => {
                        return Err(IntentError::BadExample {
                            template: t.id,
                            example: (*example).to_owned(),
                            reason: summary(&other),
                        });
                    }
                }
            }
        }
        Ok(me)
    }

    pub const fn slot_millis(&self) -> u32 {
        self.slot_millis
    }

    pub const fn limits(&self) -> SpecLimits {
        self.limits
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Every template in order, with its examples.
    pub fn templates(&self) -> Vec<TemplateInfo> {
        self.tables
            .templates
            .iter()
            .map(|t| TemplateInfo {
                id: t.id,
                description: t.description,
                examples: t.examples.iter().map(|e| (*e).to_owned()).collect(),
            })
            .collect()
    }

    /// Example questions that start with `prefix` (compared lowercased, with
    /// whitespace collapsed), then those with a word starting with it; in
    /// template order, at most `limit` (and [`MAX_SUGGEST_LIMIT`]).
    pub fn suggest(&self, prefix: &str, limit: usize) -> Vec<String> {
        let limit = limit.min(MAX_SUGGEST_LIMIT);
        if limit == 0 || prefix.chars().nth(MAX_TEXT_CHARS).is_some() {
            return Vec::new();
        }
        let p = normalize_phrase(prefix);
        let starts = self.examples.iter().filter(|(_, e, _)| e.starts_with(&p));
        let inside = self.examples.iter().filter(|(_, e, _)| !e.starts_with(&p) && e.contains(&format!(" {p}")));
        starts.chain(inside).take(limit).map(|(_, e, _)| (*e).to_owned()).collect()
    }

    /// What `text` means. Never panics, never fails; see [`Interpretation`].
    pub fn interpret(&self, text: &str) -> Interpretation {
        self.run(text, &mut Work::default())
    }

    fn run(&self, text: &str, work: &mut Work) -> Interpretation {
        if let Err(reason) = check_text(text) {
            return Interpretation::Invalid { reason };
        }
        let toks = tokenise(text);
        if toks.is_empty() {
            return Interpretation::Invalid { reason: InvalidReason::Empty };
        }
        if toks.len() > MAX_TOKENS {
            return Interpretation::Invalid {
                reason: InvalidReason::TooManyTokens { count: toks.len(), max: MAX_TOKENS },
            };
        }
        let result = match self.read(&toks, work) {
            Ok(i) => i,
            Err(reason) => Interpretation::NoMatch { suggestions: self.suggestions(&toks), reason },
        };
        // The linear bound the module docs promise, checked on every call in
        // debug builds.
        let n = toks.len();
        debug_assert!(
            work.entity_lookups <= MAX_PHRASE_TOKENS * n
                && work.phrase_lookups <= MAX_PHRASE_TOKENS * n
                && work.form_checks <= OVER_NUMBER_FORMS.len() * n,
            "work bound exceeded for {n} words: {work:?}"
        );
        result
    }

    /// Examples sharing the most words with the question, or the first of
    /// each template when none shares any.
    fn suggestions(&self, toks: &[Tok<'_>]) -> Vec<String> {
        let words: BTreeSet<&str> =
            toks.iter().map(|t| t.lower.as_str()).filter(|w| !FILLER_WORDS.contains(w)).collect();
        let mut scored: Vec<(usize, usize)> = self
            .examples
            .iter()
            .enumerate()
            .map(|(i, (_, _, ex))| (ex.iter().filter(|w| words.contains(w.as_str())).count(), i))
            .filter(|(score, _)| *score > 0)
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        if scored.is_empty() {
            let mut seen = BTreeSet::new();
            return self
                .examples
                .iter()
                .filter(|(t, _, _)| seen.insert(*t))
                .take(MAX_SUGGESTIONS)
                .map(|(_, e, _)| (*e).to_owned())
                .collect();
        }
        scored.iter().take(MAX_SUGGESTIONS).map(|&(_, i)| self.examples[i].1.to_owned()).collect()
    }

    /// Steps 3 to 11 of the pipeline.
    fn read(&self, toks: &[Tok<'_>], work: &mut Work) -> Result<Interpretation, NoMatchReason> {
        let n = toks.len();

        // Calendar words.
        if let Some(t) = toks.iter().find(|t| templates::CALENDAR_WORDS.contains(&t.lower.as_str())) {
            return Err(NoMatchReason::CalendarWord { word: t.text.to_owned() });
        }

        // The window.
        let mut marks = vec![Mark::Free; n];
        let mut windows = Vec::new();
        let mut i = 0;
        while i < n {
            match window_at(&toks[i..]) {
                Some((len, hit)) => {
                    marks[i..i + len].fill(Mark::Taken);
                    windows.push(hit);
                    i += len;
                }
                None => i += 1,
            }
        }
        if windows.len() > 1 {
            return Err(NoMatchReason::TwoWindows);
        }
        let window = windows.pop();
        if let Some(t) =
            toks.iter().zip(&marks).find(|(t, m)| **m == Mark::Free && LAST_WORDS.contains(&t.lower.as_str()))
        {
            return Err(NoMatchReason::UnsupportedWindow { word: t.0.text.to_owned() });
        }

        // Keys.
        let mut entities: Vec<EntityRef> = Vec::new();
        for i in 0..n {
            if marks[i] != Mark::Free || !is_base58_key(toks[i].text) {
                continue;
            }
            let kw = if i > 0 && marks[i - 1] == Mark::Free { kind_word(&toks[i - 1].lower) } else { None };
            let Some(kw) = kw else {
                return Err(NoMatchReason::BareKey { key: toks[i].text.to_owned() });
            };
            let address = toks[i].text.to_owned();
            marks[i - 1] = Mark::Entity(entities.len());
            marks[i] = Mark::Taken;
            entities.push(EntityRef {
                text: format!("{} {}", toks[i - 1].text, toks[i].text),
                name: self.catalog.entity_name(&address).map(str::to_owned),
                address,
                kind: kw.kind,
                source: Source::Key(kw),
            });
        }

        // Entities.
        let mut i = 0;
        while i < n {
            if marks[i] != Mark::Free {
                i += 1;
                continue;
            }
            let free = marks[i..].iter().take(MAX_PHRASE_TOKENS).take_while(|m| **m == Mark::Free).count();
            let mut found = None;
            for len in (1..=free).rev() {
                work.entity_lookups += 1;
                let span = toks[i..i + len].iter().map(|t| t.text).collect::<Vec<_>>().join(" ");
                if let EntityLookup::Found(e) = self.catalog.entity(&span) {
                    found = Some((len, e));
                    break;
                }
            }
            let Some((mut len, e)) = found else {
                i += 1;
                continue;
            };
            if i + len < n && marks[i + len] == Mark::Free && toks[i + len].lower == kind_name(e.kind) {
                len += 1;
            }
            marks[i] = Mark::Entity(entities.len());
            marks[i + 1..i + len].fill(Mark::Taken);
            entities.push(EntityRef {
                text: toks[i..i + len].iter().map(|t| t.text).collect::<Vec<_>>().join(" "),
                name: Some(e.name.clone()),
                address: e.address.clone(),
                kind: e.kind,
                source: Source::Catalog,
            });
            i += len;
        }

        // Typing.
        let mut typed: Vec<T> = Vec::with_capacity(n);
        let mut i = 0;
        while i < n {
            match marks[i] {
                Mark::Taken => {
                    i += 1;
                    continue;
                }
                Mark::Entity(k) => {
                    typed.push(T::Entity(k));
                    i += 1;
                    continue;
                }
                Mark::Free => {}
            }
            let free = marks[i..].iter().take_while(|m| **m == Mark::Free).count();
            let span = &toks[i..i + free];
            let mut best: Option<(usize, Option<T>)> = None;
            for form in OVER_NUMBER_FORMS {
                work.form_checks += 1;
                if best.is_none_or(|(l, _)| form.len() > l)
                    && let Some(width) = form_at(form, span)
                {
                    best = Some((form.len(), Some(T::Over(Some(width)))));
                }
            }
            for len in (1..=free.min(MAX_PHRASE_TOKENS)).rev() {
                if best.is_some_and(|(l, _)| l >= len) {
                    break;
                }
                work.phrase_lookups += 1;
                let key = span[..len].iter().map(|t| t.lower.as_str()).collect::<Vec<_>>().join(" ");
                if let Some(e) = self.entries.get(&key) {
                    let t = match e.structural {
                        Some(Structural::Lead) => Some(T::Lead),
                        Some(Structural::Top) => Some(T::Top),
                        Some(Structural::By) => Some(T::By),
                        Some(Structural::Over) => Some(T::Over(None)),
                        Some(Structural::Prep) => Some(T::Prep),
                        Some(Structural::And) => Some(T::And),
                        Some(Structural::Filler) => None,
                        None => Some(T::Phrase { measure: e.measure, dimension: e.dimension }),
                    };
                    best = Some((len, t));
                    break;
                }
            }
            if best.is_none()
                && let Some(v) = parse_number(&toks[i].lower)
            {
                best = Some((1, Some(T::Number(v, i))));
            }
            let Some((len, t)) = best else {
                return Err(NoMatchReason::UnknownWord { word: toks[i].text.to_owned() });
            };
            typed.extend(t);
            i += len;
        }

        // One entity, checked after typing so an unknown word is reported first.
        if entities.len() > 1 {
            return Err(NoMatchReason::TooManyEntities { count: entities.len() });
        }

        // Numbers only after `top`.
        for (j, t) in typed.iter().enumerate() {
            if let T::Number(_, w) = t
                && (j == 0 || typed[j - 1] != T::Top)
            {
                return Err(NoMatchReason::MisplacedNumber { number: toks[*w].text.to_owned() });
            }
        }

        // Leads, then the entity.
        let leads = typed.iter().take_while(|t| **t == T::Lead).count();
        let mut core: Vec<T> = typed[leads..].to_vec();
        let mut entity: Option<&EntityRef> = None;
        let len = core.len();
        if len >= 2
            && core[len - 2] == T::Prep
            && let T::Entity(k) = core[len - 1]
        {
            entity = Some(&entities[k]);
            core.truncate(len - 2);
        } else if let Some(p) = core.iter().position(|t| matches!(t, T::Phrase { .. }))
            && p > 0
            && let T::Entity(k) = core[p - 1]
        {
            entity = Some(&entities[k]);
            core.remove(p - 1);
        }

        // Templates, then assembly.
        let mut matched: Vec<Matched> = Vec::new();
        let mut rejection: Option<Rejection> = None;
        let mut too_many: Option<usize> = None;
        for t in &self.tables.templates {
            let Some(reading) = match_template(t.id, &core) else { continue };
            if reading.measures.len() > MAX_QUESTION_MEASURES {
                too_many.get_or_insert(reading.measures.len());
                continue;
            }
            match self.assemble(&reading, entity, window) {
                Ok(m) => {
                    if !matched.iter().any(|x| x.spec == m.spec) {
                        matched.push(m);
                    }
                }
                Err(r) => {
                    rejection.get_or_insert(r);
                }
            }
        }
        match matched.len() {
            0 => match (rejection, too_many) {
                (Some((code, message)), _) => Ok(Interpretation::Rejected { code, message }),
                (None, Some(count)) => Err(NoMatchReason::TooManyMeasures { count, max: MAX_QUESTION_MEASURES }),
                (None, None) => Err(NoMatchReason::NoTemplate),
            },
            1 => Ok(Interpretation::Matched(Box::new(matched.pop().expect("one")))),
            _ => Ok(Interpretation::Ambiguous { candidates: matched }),
        }
    }

    /// `amount` units in slots: `ceil(amount × unit_ms / slot_millis)`, at
    /// least 1 for a non-zero amount, saturating.
    fn to_slots(&self, amount: u64, unit: TimeUnit) -> u64 {
        if amount == 0 {
            return 0;
        }
        let ms = u128::from(amount) * u128::from(unit.millis());
        u64::try_from(ms.div_ceil(u128::from(self.slot_millis)).max(1)).unwrap_or(u64::MAX)
    }

    fn assemble(
        &self,
        r: &Reading,
        entity: Option<&EntityRef>,
        window: Option<WindowHit>,
    ) -> Result<Matched, Rejection> {
        let catalog = &self.catalog;
        let mut assumptions = Vec::new();

        // Window.
        let mut converted = None;
        let window = match window {
            None => {
                let slots = DEFAULT_WINDOW_SLOTS.min(self.limits.max_slot_span);
                assumptions.push(Assumption::DefaultWindow { slots });
                Window::LastSlots(slots)
            }
            Some(WindowHit::Slots(n)) => Window::LastSlots(n),
            Some(WindowHit::Time { amount, unit }) => {
                let slots = self.to_slots(amount, unit);
                assumptions.push(Assumption::TimeConverted { amount, unit, slots, slot_millis: self.slot_millis });
                converted = Some((amount, unit));
                Window::LastSlots(slots)
            }
            Some(WindowHit::Range { from, to }) => Window::Range { from_slot: from, to_slot: to },
        };

        // Measures and their implied filters.
        let phrases: Vec<_> = r.measures.iter().map(|&i| &self.tables.measures[i]).collect();
        let mut measures: Vec<&'static str> = phrases.iter().map(|p| p.measure).collect();
        let mut filters: Vec<Filter> = Vec::new();
        for p in &phrases {
            if let Some(f) = p.implied {
                if phrases.len() > 1 {
                    let others: Vec<&str> = phrases.iter().filter(|q| q.phrase != p.phrase).map(|q| q.phrase).collect();
                    return Err((
                        RejectCode::ImpliedFilterNotAlone,
                        format!(
                            "{:?} means {} where {} = {}, and that filter would also apply to {}. \
                             Ask about it on its own.",
                            p.phrase,
                            p.measure,
                            f.dimension,
                            f.value,
                            others.join(", ")
                        ),
                    ));
                }
                filters.push(Filter {
                    dimension: f.dimension.to_owned(),
                    op: FilterOp::Eq,
                    values: vec![f.value.to_owned()],
                });
            }
        }
        if measures.is_empty() {
            let dim = r.dimension.expect("only top_n has no measure, and it has a dimension");
            let dm = self.tables.default_measures.iter().find(|d| d.dimension == dim).expect("validated");
            measures.push(dm.measure);
            assumptions.push(Assumption::DefaultMeasure { measure: dm.measure });
        }

        // The entity.
        let mut resolved = Vec::new();
        let mut entity_filter = None;
        if let Some(e) = entity {
            let dimension = self.entity_dimension(e)?;
            entity_filter = Some(filters.len());
            filters.push(Filter { dimension: dimension.to_owned(), op: FilterOp::Eq, values: vec![e.address.clone()] });
            resolved.push(ResolvedEntity {
                text: e.text.clone(),
                name: e.name.clone(),
                address: e.address.clone(),
                kind: e.kind,
                dimension,
            });
        }

        // Shape.
        let (group_by, order_by, limit) = match r.template {
            TemplateId::TopN | TemplateId::ByDimension => {
                let dim = r.dimension.expect("these templates have a dimension");
                let limit = match r.top {
                    Some(n) => u32::try_from(n).unwrap_or(u32::MAX),
                    None => {
                        let n = DEFAULT_TOP.min(self.limits.max_rows);
                        assumptions.push(Assumption::DefaultLimit { n });
                        n
                    }
                };
                (
                    vec![GroupBy { dimension: dim.to_owned(), bucket_width: None }],
                    vec![Order { target: measures[0].to_owned(), direction: Direction::Desc }],
                    Some(limit),
                )
            }
            TemplateId::OverTime => {
                let span = match window {
                    Window::LastSlots(n) => n,
                    Window::Range { from_slot, to_slot } => to_slot.saturating_sub(from_slot).saturating_add(1),
                };
                let width = match r.width {
                    Some(w) => w,
                    None => {
                        let target = AUTO_BUCKETS.min(u64::from(self.limits.max_rows)).max(1);
                        span.div_ceil(target).clamp(1, MAX_BUCKET_WIDTH_SLOTS)
                    }
                };
                let buckets = match window {
                    _ if width == 0 => 1,
                    Window::LastSlots(n) => n / width,
                    Window::Range { from_slot, to_slot } if from_slot <= to_slot => {
                        SlotWindow::new(from_slot, to_slot).snap(width).map_or(0, |w| w.slots / width)
                    }
                    Window::Range { .. } => 1,
                }
                .max(1);
                if r.width.is_none() {
                    assumptions.push(Assumption::AutoBucketWidth { width, buckets });
                }
                (
                    vec![GroupBy { dimension: "slot_bucket".to_owned(), bucket_width: Some(width) }],
                    vec![Order { target: "slot_bucket".to_owned(), direction: Direction::Asc }],
                    Some(u32::try_from(buckets).unwrap_or(u32::MAX)),
                )
            }
            TemplateId::Total => (vec![], vec![], None),
        };

        let spec = QuerySpec {
            measures: measures.iter().map(|m| (*m).to_owned()).collect(),
            group_by,
            filters,
            window,
            order_by,
            limit,
            as_of_slot: None,
        };
        if let Err(e) = guard::validate(&spec, catalog, &self.limits) {
            let code = self.classify(&e, &spec, entity_filter);
            let message = match code.hint() {
                Some(hint) => format!("{e}. {hint}"),
                None => e.to_string(),
            };
            return Err((code, message));
        }

        let understood_as = self.describe(&spec, r.template, &resolved, converted);
        Ok(Matched { template_id: r.template, spec, understood_as, assumptions, entities: resolved })
    }

    /// The dimension an entity filters, or why it cannot be used.
    fn entity_dimension(&self, e: &EntityRef) -> Result<&'static str, Rejection> {
        match e.source {
            Source::Catalog => entity_dimension(e.kind).ok_or_else(|| {
                (
                    RejectCode::AccountEntity,
                    format!(
                        "{} is an account: whether it is the source or the destination of transfers is unclear. \
                         Ask with source <address> or destination <address>.",
                        e.name.as_deref().unwrap_or(&e.address)
                    ),
                )
            }),
            Source::Key(kw) => {
                if let EntityLookup::Found(known) = self.catalog.entity(&e.address)
                    && known.kind != kw.kind
                {
                    return Err((
                        RejectCode::KindMismatch,
                        format!(
                            "{} is {}, a {}, not a {}.",
                            e.address,
                            known.name,
                            kind_name(known.kind),
                            kind_name(kw.kind)
                        ),
                    ));
                }
                Ok(kw.dimension)
            }
        }
    }

    /// What kind of refusal a guard error is, from its field path and the
    /// spec intent built; never from the message text.
    fn classify(&self, e: &SpecError, spec: &QuerySpec, entity_filter: Option<usize>) -> RejectCode {
        let SpecError::Invalid { field, .. } = e else { return RejectCode::Guard };
        let index = |prefix: &str| -> Option<usize> { field.strip_prefix(prefix)?.split(']').next()?.parse().ok() };
        if field == "limit" {
            return RejectCode::Limit;
        }
        if field.starts_with("window") {
            return RejectCode::Window;
        }
        if field.ends_with(".bucket_width") {
            return RejectCode::BucketWidth;
        }
        if field.starts_with("group_by[") {
            return RejectCode::DimensionWrongTable;
        }
        if let Some(i) = index("filters[")
            && field.ends_with("].dimension")
        {
            let dim = &spec.filters[i].dimension;
            if spec.filters[..i].iter().any(|f| &f.dimension == dim) {
                return RejectCode::FilterConflict;
            }
            if entity_filter == Some(i) {
                return RejectCode::EntityWrongTable;
            }
            return RejectCode::Guard;
        }
        if let Some(i) = index("measures[") {
            let get = |k: usize| self.catalog.measure(&spec.measures[k]);
            let (Some(first), Some(this)) = (get(0), get(i)) else { return RejectCode::Guard };
            if first.table != this.table {
                return RejectCode::MixedTables;
            }
            if spec.measures[..i].contains(&spec.measures[i]) {
                return RejectCode::RepeatedMeasure;
            }
            if this.requires_dimension.is_some() {
                return RejectCode::NeedsDimension;
            }
        }
        RejectCode::Guard
    }

    /// The spec in words, from catalog labels.
    fn describe(
        &self,
        spec: &QuerySpec,
        template: TemplateId,
        entities: &[ResolvedEntity],
        converted: Option<(u64, TimeUnit)>,
    ) -> String {
        let label = |id: &str| self.catalog.measure(id).map_or_else(|| id.to_owned(), |m| m.label.to_owned());
        let dim_label = |id: &str| self.catalog.dimension(id).map_or_else(|| id.to_owned(), |d| d.label.to_owned());
        let measures = spec.measures.iter().map(|m| label(m)).collect::<Vec<_>>().join(" and ");
        let mut s = match template {
            TemplateId::TopN | TemplateId::ByDimension => format!(
                "{measures} by {}, top {} by {}",
                dim_label(&spec.group_by[0].dimension),
                spec.limit.unwrap_or(0),
                label(&spec.measures[0])
            ),
            TemplateId::OverTime => {
                format!("{measures} per {}-slot bucket, oldest first", spec.group_by[0].bucket_width.unwrap_or(0))
            }
            TemplateId::Total => measures,
        };
        for f in &spec.filters {
            match entities.iter().find(|e| e.dimension == f.dimension && f.values == [e.address.clone()]) {
                Some(e) => match &e.name {
                    Some(name) => s.push_str(&format!(", for {name}")),
                    None => s.push_str(&format!(", for {} {}", dim_label(&f.dimension).to_lowercase(), e.address)),
                },
                None => s.push_str(&format!(", where {} is {}", dim_label(&f.dimension).to_lowercase(), f.values[0])),
            }
        }
        match spec.window {
            Window::LastSlots(n) => {
                s.push_str(&format!(", over the last {n} slots"));
                if let Some((amount, unit)) = converted {
                    s.push_str(&format!(
                        " (about {amount} {} at {} ms a slot)",
                        unit.name(amount != 1),
                        self.slot_millis
                    ));
                }
            }
            Window::Range { from_slot, to_slot } if from_slot == to_slot => {
                s.push_str(&format!(", in slot {from_slot}"))
            }
            Window::Range { from_slot, to_slot } => s.push_str(&format!(", over slots {from_slot} to {to_slot}")),
        }
        s
    }

    /// The lookups one interpretation makes.
    #[cfg(test)]
    fn work(&self, text: &str) -> Work {
        let mut w = Work::default();
        self.run(text, &mut w);
        w
    }
}

/// `measures` from `start`: phrases with a measure reading, optionally
/// joined by `and`. Returns them and where they end.
fn parse_measures(core: &[T], start: usize) -> (Vec<usize>, usize) {
    let mut ms = Vec::new();
    let mut i = start;
    while let Some(T::Phrase { measure: Some(m), .. }) = core.get(i) {
        ms.push(*m);
        i += 1;
        if core.get(i) == Some(&T::And) && matches!(core.get(i + 1), Some(T::Phrase { measure: Some(_), .. })) {
            i += 1;
        }
    }
    (ms, i)
}

/// One left-to-right pass of template `id` over the typed stream.
fn match_template(id: TemplateId, core: &[T]) -> Option<Reading> {
    let reading = |dimension, measures, top, width| Reading { template: id, dimension, measures, top, width };
    let dim_at = |i: usize| match core.get(i) {
        Some(T::Phrase { dimension: Some(d), .. }) => Some(*d),
        _ => None,
    };
    match id {
        TemplateId::TopN => {
            if core.first() == Some(&T::Top) {
                let (top, i) = match core.get(1) {
                    Some(T::Number(n, _)) => (Some(*n), 2),
                    _ => (None, 1),
                };
                let d = dim_at(i)?;
                if i + 1 == core.len() {
                    return Some(reading(Some(d), vec![], top, None));
                }
                if core.get(i + 1) != Some(&T::By) {
                    return None;
                }
                let (ms, end) = parse_measures(core, i + 2);
                (!ms.is_empty() && end == core.len()).then(|| reading(Some(d), ms, top, None))
            } else {
                let d = dim_at(0)?;
                if core.get(1) != Some(&T::By) {
                    return None;
                }
                let (ms, end) = parse_measures(core, 2);
                (!ms.is_empty() && end == core.len()).then(|| reading(Some(d), ms, None, None))
            }
        }
        TemplateId::ByDimension => {
            let (ms, end) = parse_measures(core, 0);
            if ms.is_empty() || core.get(end) != Some(&T::By) || end + 2 != core.len() {
                return None;
            }
            dim_at(end + 1).map(|d| reading(Some(d), ms, None, None))
        }
        TemplateId::OverTime => {
            let (ms, end) = parse_measures(core, 0);
            match core.get(end) {
                Some(T::Over(width)) if !ms.is_empty() && end + 1 == core.len() => {
                    Some(reading(None, ms, None, *width))
                }
                _ => None,
            }
        }
        TemplateId::Total => {
            let (ms, end) = parse_measures(core, 0);
            (!ms.is_empty() && end == core.len()).then(|| reading(None, ms, None, None))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interpreter() -> Interpreter {
        Interpreter::new(Arc::new(Catalog::new(vec![]).unwrap()), 400, 1_000).unwrap()
    }

    #[test]
    fn tokens_keep_their_text_and_lose_edge_punctuation() {
        let toks = tokenise("  Top-level, \"Mints\"?  (wsol) ... ");
        let pairs: Vec<(&str, &str)> = toks.iter().map(|t| (t.text, t.lower.as_str())).collect();
        assert_eq!(pairs, [("Top-level", "top-level"), ("Mints", "mints"), ("wsol", "wsol")]);
    }

    #[test]
    fn window_forms() {
        let at = |s: &str| window_at(&tokenise(s));
        assert_eq!(at("in the last hour"), Some((4, WindowHit::Time { amount: 1, unit: TimeUnit::Hour })));
        assert_eq!(at("past 2 days"), Some((3, WindowHit::Time { amount: 2, unit: TimeUnit::Day })));
        assert_eq!(at("over the previous 1,000 slots"), Some((5, WindowHit::Slots(1000))));
        assert_eq!(at("from slot 5 to 9"), Some((5, WindowHit::Range { from: 5, to: 9 })));
        assert_eq!(at("between slots 5 and slot 9"), Some((6, WindowHit::Range { from: 5, to: 9 })));
        assert_eq!(at("slots 5 through 9"), Some((4, WindowHit::Range { from: 5, to: 9 })));
        assert_eq!(at("in slot 7"), Some((3, WindowHit::Range { from: 7, to: 7 })));
        assert_eq!(at("slots 5 and 9"), None, "`and` only after `between`");
        assert_eq!(at("last 10 transfers"), None);
        assert_eq!(at("per 200 slots"), None);
    }

    #[test]
    fn bucket_forms() {
        let w = |s: &str| OVER_NUMBER_FORMS.iter().find_map(|f| form_at(f, &tokenise(s)));
        assert_eq!(w("per 200 slots"), Some(200));
        assert_eq!(w("every 1 slot"), Some(1));
        assert_eq!(w("in buckets of 50 slots"), Some(50));
        assert_eq!(w("in 25 slot buckets"), Some(25));
        assert_eq!(w("per mint"), None);
    }

    /// The pipeline's work is linear in the words: per position, at most 4
    /// entity spans, every bucket form once and 4 phrase spans.
    #[test]
    fn work_is_bounded_by_the_word_count() {
        let i = interpreter();
        let forms = OVER_NUMBER_FORMS.len();
        let adversarial = [
            "a ".repeat(MAX_TOKENS),
            "the ".repeat(MAX_TOKENS),
            "spl ".repeat(MAX_TOKENS),
            "token ".repeat(MAX_TOKENS),
            "top ".repeat(MAX_TOKENS),
            "transfers and ".repeat(MAX_TOKENS / 2),
            "in the last ".repeat(MAX_TOKENS / 3),
            "per 1 ".repeat(MAX_TOKENS / 2),
            "slots 1 to ".repeat(MAX_TOKENS / 3),
            "top level ".repeat(MAX_TOKENS / 2),
        ];
        for text in &adversarial {
            let n = tokenise(text).len();
            let w = i.work(text);
            assert!(w.entity_lookups <= MAX_PHRASE_TOKENS * n, "{text:?}: {w:?}");
            assert!(w.phrase_lookups <= MAX_PHRASE_TOKENS * n, "{text:?}: {w:?}");
            assert!(w.form_checks <= forms * n, "{text:?}: {w:?}");
        }
    }

    #[test]
    fn time_converts_up_and_saturates() {
        let i = interpreter();
        assert_eq!(i.to_slots(1, TimeUnit::Hour), 9_000);
        assert_eq!(i.to_slots(1, TimeUnit::Second), 3, "2.5 slots round up");
        assert_eq!(i.to_slots(0, TimeUnit::Hour), 0);
        assert_eq!(i.to_slots(u64::MAX, TimeUnit::Week), u64::MAX);
    }
}
