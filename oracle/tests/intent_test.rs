//! `intent/`: startup validation, every rule of the pipeline, ambiguity,
//! conversions, suggestions, the wire shape, and the bounded work on
//! adversarial input.

use std::sync::Arc;
use std::time::{Duration, Instant};

use oracle::catalog::{Catalog, EntityDef, EntityKind, is_base58_key};
use oracle::intent::templates::{
    DefaultMeasure, DimensionPhrase, ImpliedFilter, MeasurePhrase, PhraseTables, TEMPLATES, TemplateDef,
};
use oracle::intent::*;
use oracle::spec::guard::{SpecLimits, validate};
use oracle::spec::{Filter, FilterOp, QuerySpec, Window};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const VOTE: &str = "Vote111111111111111111111111111111111111111";
const KEY7: &str = "US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx";

fn catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new(vec![]).unwrap())
}

fn limits() -> SpecLimits {
    SpecLimits { max_rows: 1_000, max_slot_span: 216_000 }
}

fn interpreter() -> Interpreter {
    Interpreter::new(catalog(), 400, 1_000).unwrap()
}

fn build(tables: PhraseTables) -> Result<Interpreter, IntentError> {
    Interpreter::from_tables(catalog(), 400, limits(), tables)
}

fn filter(dimension: &str, value: &str) -> Filter {
    Filter { dimension: dimension.to_owned(), op: FilterOp::Eq, values: vec![value.to_owned()] }
}

#[track_caller]
fn matched(i: &Interpreter, q: &str) -> Matched {
    match i.interpret(q) {
        Interpretation::Matched(m) => *m,
        other => panic!("{q:?}: expected Matched, got {other:?}"),
    }
}

#[track_caller]
fn rejected(i: &Interpreter, q: &str) -> (RejectCode, String) {
    match i.interpret(q) {
        Interpretation::Rejected { code, message } => (code, message),
        other => panic!("{q:?}: expected Rejected, got {other:?}"),
    }
}

#[track_caller]
fn no_match(i: &Interpreter, q: &str) -> (NoMatchReason, Vec<String>) {
    match i.interpret(q) {
        Interpretation::NoMatch { reason, suggestions } => (reason, suggestions),
        other => panic!("{q:?}: expected NoMatch, got {other:?}"),
    }
}

#[test]
fn an_interpreter_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Interpreter>();
    let i = Arc::new(interpreter());
    let other = Arc::clone(&i);
    std::thread::spawn(move || assert!(matches!(other.interpret("busiest programs"), Interpretation::Matched(_))))
        .join()
        .unwrap();
}

// ---------------------------------------------------------------------------
// Startup validation
// ---------------------------------------------------------------------------

fn with_measure(p: MeasurePhrase) -> PhraseTables {
    let mut t = PhraseTables::builtin();
    t.measures.push(p);
    t
}

fn with_dimension(p: DimensionPhrase) -> PhraseTables {
    let mut t = PhraseTables::builtin();
    t.dimensions.push(p);
    t
}

const fn mp(phrase: &'static str, measure: &'static str) -> MeasurePhrase {
    MeasurePhrase { phrase, measure, implied: None }
}

#[test]
fn the_built_in_tables_pass_at_every_valid_slot_length() {
    for ms in [100, 333, 400, 1_000, 2_000] {
        Interpreter::new(catalog(), ms, 1_000).unwrap_or_else(|e| panic!("{ms}: {e}"));
    }
    // The smallest limits the built-in examples are documented to need.
    Interpreter::with_limits(catalog(), 100, SpecLimits { max_rows: 10, max_slot_span: 72_000 }).unwrap();
}

#[test]
fn slot_millis_must_be_within_100_to_2000_and_is_checked_first() {
    for v in [0, 99, 2_001, u32::MAX] {
        assert_eq!(Interpreter::new(catalog(), v, 1_000).unwrap_err(), IntentError::BadSlotMillis { value: v });
    }
    let broken = with_measure(mp("", "tx_count"));
    assert_eq!(
        Interpreter::from_tables(catalog(), 99, limits(), broken).unwrap_err(),
        IntentError::BadSlotMillis { value: 99 }
    );
}

#[test]
fn template_ids_are_unique() {
    let mut t = PhraseTables::builtin();
    t.templates.push(TEMPLATES[0]);
    assert_eq!(build(t).unwrap_err(), IntentError::DuplicateTemplate { id: TemplateId::TopN });
}

#[test]
fn malformed_phrases_are_refused() {
    let bad_measure = |p: MeasurePhrase| match build(with_measure(p)).unwrap_err() {
        IntentError::BadPhraseTable { table: "measures", phrase, reason } => {
            assert_eq!(phrase, p.phrase);
            reason
        }
        other => panic!("{}: {other:?}", p.phrase),
    };
    assert!(bad_measure(mp("", "tx_count")).contains("empty"));
    assert!(bad_measure(mp("   ", "tx_count")).contains("empty"));
    assert!(bad_measure(mp("a b c d e", "tx_count")).contains("5 words"));
    assert!(bad_measure(mp("fees!", "tx_count")).contains("never match"));
    assert!(bad_measure(mp("-fees", "tx_count")).contains("never match"));
    assert!(bad_measure(mp("frais é", "tx_count")).contains("never match"));
    assert!(bad_measure(mp("10 fees", "tx_count")).contains("number"));
    assert!(bad_measure(mp("widgets", "nope")).contains("unknown measure"));

    let bad_dimension = |p: DimensionPhrase| match build(with_dimension(p)).unwrap_err() {
        IntentError::BadPhraseTable { table: "dimensions", reason, .. } => reason,
        other => panic!("{}: {other:?}", p.phrase),
    };
    assert!(bad_dimension(DimensionPhrase { phrase: "widgets", dimension: "nope" }).contains("unknown dimension"));
    assert!(bad_dimension(DimensionPhrase { phrase: "buckets of", dimension: "slot_bucket" }).contains("parameter"));
    // Four words is the most.
    let mut t = PhraseTables::builtin();
    t.measures.push(mp("fees paid by everyone", "fee_total_lamports"));
    build(t).unwrap();
}

#[test]
fn phrases_clash_with_their_own_table_reserved_words_and_entities() {
    let clash = |t: PhraseTables| match build(t).unwrap_err() {
        IntentError::PhraseClash { phrase, with } => (phrase, with),
        other => panic!("{other:?}"),
    };
    // Twice in one table, even as written differently.
    let (phrase, with) = clash(with_measure(mp("Fees", "tx_count")));
    assert_eq!(phrase, "Fees");
    assert!(with.contains("fee_total_lamports"), "{with}");
    let (_, with) = clash(with_dimension(DimensionPhrase { phrase: "mints", dimension: "token_program" }));
    assert!(with.contains("mint"), "{with}");
    // But once in each table is allowed: that is how `programs` works.
    assert!(PhraseTables::builtin().measures.iter().any(|p| p.phrase == "programs"));
    assert!(PhraseTables::builtin().dimensions.iter().any(|p| p.phrase == "programs"));

    for (word, what) in [
        ("top", "structural"),
        ("by", "structural"),
        ("over time", "structural"),
        ("the", "filler"),
        ("hours", "time unit"),
        ("last", "window"),
        ("buckets", "bucket"),
        ("yesterday", "calendar"),
    ] {
        let (phrase, with) = clash(with_measure(mp(word, "tx_count")));
        assert_eq!(phrase, word);
        assert!(with.contains(what), "{word}: {with}");
    }

    // A host entity named like a phrase, or like a word inside one.
    let entity =
        |name: &str| EntityDef { name: name.into(), aliases: vec![], address: KEY7.into(), kind: EntityKind::Program };
    let with_entity = |name: &str| Interpreter::new(Arc::new(Catalog::new(vec![entity(name)]).unwrap()), 400, 1_000);
    assert_eq!(
        with_entity("Transfers").unwrap_err(),
        IntentError::PhraseClash { phrase: "transfers".into(), with: "entity \"Transfers\" (as \"transfers\")".into() }
    );
    assert_eq!(
        with_entity("Token").unwrap_err(),
        IntentError::PhraseClash { phrase: "token transfers".into(), with: "entity \"Token\" (as \"token\")".into() }
    );
    assert!(matches!(with_entity("top").unwrap_err(), IntentError::PhraseClash { phrase, .. } if phrase == "top"));
    assert!(matches!(with_entity("mint").unwrap_err(), IntentError::PhraseClash { phrase, .. } if phrase == "mint"));
    with_entity("Jupiter").unwrap();
}

#[test]
fn implied_filters_must_be_real_valid_and_on_the_measures_table() {
    let implied = |dimension, value, measure| {
        build(with_measure(MeasurePhrase {
            phrase: "special things",
            measure,
            implied: Some(ImpliedFilter { dimension, value }),
        }))
    };
    let reason = |e: IntentError| match e {
        IntentError::BadImpliedFilter { phrase, reason } => {
            assert_eq!(phrase, "special things");
            reason
        }
        other => panic!("{other:?}"),
    };
    assert!(reason(implied("nope", "x", "tx_count").unwrap_err()).contains("unknown dimension"));
    assert!(reason(implied("tx_status", "maybe", "tx_count").unwrap_err()).contains("maybe"));
    assert!(reason(implied("program", "not-a-key", "ix_count").unwrap_err()).contains("base58"));
    assert!(reason(implied("tx_status", "success", "transfer_count").unwrap_err()).contains("not on token_transfers"));
    implied("tx_status", "failed", "fee_avg_lamports").unwrap();
}

#[test]
fn default_measures_must_exist_fit_and_cover_every_dimension_phrase() {
    let with_defaults = |f: &dyn Fn(&mut Vec<DefaultMeasure>)| {
        let mut t = PhraseTables::builtin();
        f(&mut t.default_measures);
        build(t)
    };
    let dimension_of = |e: IntentError| match e {
        IntentError::BadDefaultMeasure { dimension, reason } => (dimension, reason),
        other => panic!("{other:?}"),
    };
    let (d, r) = dimension_of(with_defaults(&|v| v.retain(|d| d.dimension != "program")).unwrap_err());
    assert_eq!(d, "program");
    assert!(r.contains("no default measure"), "{r}");
    let (d, r) = dimension_of(
        with_defaults(&|v| {
            v.iter_mut().filter(|d| d.dimension == "program").for_each(|d| d.measure = "transfer_count")
        })
        .unwrap_err(),
    );
    assert_eq!(d, "program");
    assert!(r.contains("cannot be grouped by"), "{r}");
    let (_, r) = dimension_of(with_defaults(&|v| v[0].measure = "nope").unwrap_err());
    assert!(r.contains("unknown measure"), "{r}");
    let (_, r) = dimension_of(with_defaults(&|v| v[0].dimension = "nope").unwrap_err());
    assert!(r.contains("unknown dimension"), "{r}");
    let (_, r) = dimension_of(with_defaults(&|v| v.push(v[0])).unwrap_err());
    assert!(r.contains("twice"), "{r}");
}

#[test]
fn every_example_must_interpret_as_its_own_template() {
    let mut t = PhraseTables::builtin();
    t.templates[3] = TemplateDef { examples: &["busiest programs"], ..t.templates[3] };
    match build(t).unwrap_err() {
        IntentError::BadExample { template, example, reason } => {
            assert_eq!((template, example.as_str()), (TemplateId::Total, "busiest programs"));
            assert!(reason.contains("matched top_n"), "{reason}");
        }
        other => panic!("{other:?}"),
    }
    // A phrase edit that breaks an example fails startup, not a user.
    let mut t = PhraseTables::builtin();
    t.measures.retain(|p| p.phrase != "failure rate");
    assert!(matches!(
        build(t).unwrap_err(),
        IntentError::BadExample { template: TemplateId::OverTime, example, .. } if example.starts_with("failure rate")
    ));
    // Examples are checked under the interpreter's own limits.
    match Interpreter::new(catalog(), 400, 5).unwrap_err() {
        IntentError::BadExample { reason, .. } => assert!(reason.contains("row cap"), "{reason}"),
        other => panic!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Invalid text
// ---------------------------------------------------------------------------

#[test]
fn invalid_text_is_told_apart_from_a_question_that_does_not_match() {
    let i = interpreter();
    let invalid = |q: &str| match i.interpret(q) {
        Interpretation::Invalid { reason } => reason,
        other => panic!("{q:?}: {other:?}"),
    };
    assert_eq!(invalid(""), InvalidReason::Empty);
    assert_eq!(invalid(" \t\n "), InvalidReason::Empty);
    assert_eq!(invalid("... ?? !!"), InvalidReason::Empty);
    assert_eq!(invalid("tx\u{1b}[31m"), InvalidReason::ControlCharacter { position: 2 });
    assert_eq!(invalid("\u{85}tx"), InvalidReason::ControlCharacter { position: 0 });
    assert_eq!(invalid(&"x".repeat(257)), InvalidReason::TooLong { max_chars: 256 });
    assert_eq!(invalid(&"é".repeat(257)), InvalidReason::TooLong { max_chars: 256 });
    assert_eq!(invalid(&"tx ".repeat(33)), InvalidReason::TooManyTokens { count: 33, max: 32 });
    // At the limits, and with whitespace control characters, it is a question.
    assert!(!matches!(i.interpret(&"é".repeat(256)), Interpretation::Invalid { .. }));
    assert!(!matches!(i.interpret(&"tx ".repeat(32)), Interpretation::Invalid { .. }));
    assert!(matches!(i.interpret("busiest\tprograms\r\n"), Interpretation::Matched(_)));
}

// ---------------------------------------------------------------------------
// Every word is accounted for; matching is exact
// ---------------------------------------------------------------------------

#[test]
fn no_word_is_ever_ignored() {
    let i = interpreter();
    for t in TEMPLATES {
        for e in t.examples {
            for q in [format!("{e} zzz"), format!("zzz {e}"), format!("{e} excluding wsol")] {
                let (reason, suggestions) = no_match(&i, &q);
                assert!(
                    matches!(&reason, NoMatchReason::UnknownWord { word } if word == "zzz" || word == "excluding"),
                    "{q}: {reason:?}"
                );
                assert!(!suggestions.is_empty());
            }
        }
    }
}

#[test]
fn matching_is_exact_no_typo_is_corrected_and_only_ascii_is_folded() {
    let i = interpreter();
    for (q, word) in [
        ("trasnfers by mint", "trasnfers"),
        ("busiest progarms", "progarms"),
        ("top 10 mintz", "mintz"),
        ("transférs by mint", "transférs"),
        ("ＴＯＰ programs", "ＴＯＰ"),
        ("TRANSFERS BY MİNT", "MİNT"),
    ] {
        assert_eq!(no_match(&i, q).0, NoMatchReason::UnknownWord { word: word.into() }, "{q}");
    }
    assert_eq!(matched(&i, "BuSiEsT PrOgRaMs").spec, matched(&i, "busiest programs").spec);
    assert_eq!(matched(&i, "busiest programs?!").spec, matched(&i, "busiest programs").spec);
}

// ---------------------------------------------------------------------------
// Keys and entities
// ---------------------------------------------------------------------------

#[test]
fn keys_keep_their_case_and_need_a_kind_word() {
    let i = interpreter();
    // The kind word folds; the key never does.
    let m = matched(&i, &format!("transfers for MINT {WSOL}"));
    assert_eq!(m.spec.filters, [filter("mint", WSOL)]);
    assert_eq!(m.entities[0].name.as_deref(), Some("Wrapped SOL"));

    // A key typed in another case is another key, or none, never WSOL.
    let lower = WSOL.to_lowercase();
    match i.interpret(&format!("transfers for mint {lower}")) {
        Interpretation::Matched(m) => {
            assert!(is_base58_key(&lower));
            assert_eq!(m.spec.filters, [filter("mint", &lower)]);
            assert_eq!(m.entities[0].name, None);
        }
        Interpretation::NoMatch { reason, .. } => {
            assert!(!is_base58_key(&lower));
            assert!(matches!(reason, NoMatchReason::UnknownWord { .. }), "{reason:?}");
        }
        other => panic!("{other:?}"),
    }

    // A bare key, wherever it is, gets the fixed hint.
    for q in [format!("transfers for {WSOL}"), format!("{WSOL} transfers"), KEY7.to_owned()] {
        let (reason, _) = no_match(&i, &q);
        assert!(matches!(&reason, NoMatchReason::BareKey { key } if key == WSOL || key == KEY7), "{q}");
        assert_eq!(reason.message(), BARE_KEY_HINT);
    }

    // Each kind word filters its own dimension.
    for (word, measure, dimension) in [
        ("mint", "transfers", "mint"),
        ("program", "instructions", "program"),
        ("leader", "slots", "leader"),
        ("source", "transfers", "source_owner"),
        ("destination", "transfers", "destination_owner"),
    ] {
        let m = matched(&i, &format!("{measure} for {word} {KEY7}"));
        assert_eq!(m.spec.filters, [filter(dimension, KEY7)], "{word}");
        assert_eq!(m.entities[0].dimension, dimension);
        // Before the measure too.
        assert_eq!(matched(&i, &format!("{word} {KEY7} {measure}")).spec, m.spec, "{word}");
    }
}

#[test]
fn an_entitys_dimension_comes_from_its_kind() {
    let i = interpreter();
    let m = matched(&i, "instructions for the vote program in the last 1000 slots");
    assert_eq!(m.spec.filters, [filter("program", VOTE)]);
    assert_eq!(m.entities[0].text, "vote program");
    assert_eq!(m.entities[0].kind, EntityKind::Program);
    // Its kind word after its name is part of it.
    assert_eq!(matched(&i, "wsol mint transfers").entities[0].text, "wsol mint");
    assert_eq!(matched(&i, "transfers for the wsol mint").spec, matched(&i, "transfers for wsol").spec);

    // An account is never guessed to be a source or a destination.
    let (code, message) = rejected(&i, "transfers involving the incinerator");
    assert_eq!(code, RejectCode::AccountEntity);
    assert!(message.contains("Incinerator") && message.contains("source <address>"), "{message}");

    // The kind word must agree with what the catalog knows.
    let (code, message) = rejected(&i, &format!("instructions for program {WSOL}"));
    assert_eq!(code, RejectCode::KindMismatch);
    assert!(message.contains("Wrapped SOL, a mint, not a program"), "{message}");

    // One entity per question.
    assert_eq!(no_match(&i, "transfers for wsol on spl token").0, NoMatchReason::TooManyEntities { count: 2 });
    assert_eq!(
        no_match(&i, &format!("transfers for mint {WSOL} on leader {KEY7}")).0,
        NoMatchReason::TooManyEntities { count: 2 }
    );
}

#[test]
fn an_entity_on_the_wrong_table_is_refused_with_the_guards_reason_and_a_fixed_hint() {
    let i = interpreter();
    let (code, message) = rejected(&i, "transactions for wsol");
    assert_eq!(code, RejectCode::EntityWrongTable);
    assert!(message.starts_with("invalid filters[0].dimension: dimension mint is not on transactions"), "{message}");
    assert!(message.ends_with(RejectCode::EntityWrongTable.hint().unwrap()), "{message}");
}

// ---------------------------------------------------------------------------
// Implied filters
// ---------------------------------------------------------------------------

#[test]
fn implied_filters_apply_alone_and_never_silently_to_other_measures() {
    let i = interpreter();
    let m = matched(&i, "successful transactions by status");
    assert_eq!(m.spec.filters, [filter("tx_status", "success")]);
    let (code, message) = rejected(&i, "successful transactions and average fee");
    assert_eq!(code, RejectCode::ImpliedFilterNotAlone);
    assert!(message.contains("tx_status = success") && message.contains("average fee"), "{message}");
}

#[test]
fn an_implied_filter_and_the_entity_on_one_dimension_are_refused() {
    let i = build(with_measure(MeasurePhrase {
        phrase: "voting instructions",
        measure: "ix_count",
        implied: Some(ImpliedFilter { dimension: "program", value: VOTE }),
    }))
    .unwrap();
    assert_eq!(matched(&i, "voting instructions").spec.filters, [filter("program", VOTE)]);
    let (code, message) = rejected(&i, "voting instructions for the system program");
    assert_eq!(code, RejectCode::FilterConflict);
    assert!(message.contains("already filtered"), "{message}");
}

// ---------------------------------------------------------------------------
// Ambiguity
// ---------------------------------------------------------------------------

#[test]
fn ambiguity_is_reported_with_every_candidate_in_template_order() {
    // `levels` in both tables, with different targets than `programs`, so
    // "programs by levels" has two valid, different readings.
    let mut t = with_measure(mp("levels", "ix_cpi_count"));
    t.dimensions.push(DimensionPhrase { phrase: "levels", dimension: "ix_level" });
    let i = build(t).unwrap();
    for _ in 0..3 {
        let Interpretation::Ambiguous { candidates } = i.interpret("programs by levels") else { panic!() };
        let readings: Vec<(TemplateId, Vec<String>, String)> = candidates
            .iter()
            .map(|c| (c.template_id, c.spec.measures.clone(), c.spec.group_by[0].dimension.clone()))
            .collect();
        assert_eq!(
            readings,
            [
                (TemplateId::TopN, vec!["ix_cpi_count".to_owned()], "program".to_owned()),
                (TemplateId::ByDimension, vec!["program_count".to_owned()], "ix_level".to_owned()),
            ]
        );
        for c in &candidates {
            validate(&c.spec, i.catalog(), &i.limits()).unwrap();
        }
    }
    // Two readings with the same spec are one answer.
    let m = matched(&interpreter(), "programs by programs");
    assert_eq!(m.spec.measures, ["program_count"]);
    // The other way round is ambiguous too: both readings are valid.
    assert!(
        matches!(i.interpret("levels by programs"), Interpretation::Ambiguous { candidates } if candidates.len() == 2)
    );

    // A reading the guard refuses is not a candidate: `calls` is a measure
    // on instructions and a dimension on token transfers, so of the two
    // readings of "programs by calls" only `top_n`'s is on one table.
    let mut t = with_measure(mp("calls", "ix_cpi_count"));
    t.dimensions.push(DimensionPhrase { phrase: "calls", dimension: "mint" });
    let i = build(t).unwrap();
    let m = matched(&i, "programs by calls");
    assert_eq!((m.template_id, m.spec.measures[0].as_str()), (TemplateId::TopN, "ix_cpi_count"));
    assert_eq!(m.spec.group_by[0].dimension, "program");
}

// ---------------------------------------------------------------------------
// Windows and time
// ---------------------------------------------------------------------------

#[test]
fn time_becomes_slots_rounding_up_and_always_says_so() {
    for (ms, q, slots) in [
        (400, "transfers in the last hour", 9_000),
        (400, "transfers in the last 90 seconds", 225),
        (400, "transfers in the last second", 3),
        (333, "transfers in the last hour", 10_811),
        (100, "transfers in the last 2 hours", 72_000),
        (2_000, "transfers in the last day", 43_200),
        (2_000, "transfers in the last week", 302_400),
    ] {
        let i =
            Interpreter::with_limits(catalog(), ms, SpecLimits { max_rows: 1_000, max_slot_span: 6_480_000 }).unwrap();
        let m = matched(&i, q);
        assert_eq!(m.spec.window, Window::LastSlots(slots), "{q} at {ms}");
        let [Assumption::TimeConverted { slots: s, slot_millis, .. }] = m.assumptions.as_slice() else {
            panic!("{q}: {:?}", m.assumptions)
        };
        assert_eq!((*s, *slot_millis), (slots, ms));
        assert!(m.understood_as.contains(&format!("at {ms} ms a slot")), "{}", m.understood_as);
    }
    let i = interpreter();
    assert!(matched(&i, "transfers in the last 1000 slots").assumptions.is_empty());
    assert!(matched(&i, "transfers from slot 5 to 900").assumptions.is_empty());
    assert_eq!(rejected(&i, "transfers in the last 0 minutes").0, RejectCode::Window);
}

#[test]
fn every_window_form_is_understood_and_two_windows_are_not() {
    let i = interpreter();
    for (q, w) in [
        ("transfers in the last 500 slots", Window::LastSlots(500)),
        ("transfers over the past 500 slots", Window::LastSlots(500)),
        ("transfers during the previous 500 slots", Window::LastSlots(500)),
        ("transfers within last 500 slots", Window::LastSlots(500)),
        ("transfers for the last slot", Window::LastSlots(1)),
        ("transfers last 2 mins", Window::LastSlots(300)),
        ("past hour transfers", Window::LastSlots(9_000)),
        ("transfers from slot 10 to 20", Window::Range { from_slot: 10, to_slot: 20 }),
        ("transfers from slot 10 to slot 20", Window::Range { from_slot: 10, to_slot: 20 }),
        ("transfers slots 10 through 20", Window::Range { from_slot: 10, to_slot: 20 }),
        ("transfers between slots 10 and 20", Window::Range { from_slot: 10, to_slot: 20 }),
        ("transfers at slot 10", Window::Range { from_slot: 10, to_slot: 10 }),
    ] {
        assert_eq!(matched(&i, q).spec.window, w, "{q}");
    }
    assert_eq!(no_match(&i, "transfers in the last hour from slot 1 to 5").0, NoMatchReason::TwoWindows);
    assert_eq!(no_match(&i, "transfers in the past").0, NoMatchReason::UnsupportedWindow { word: "past".into() });
    assert_eq!(no_match(&i, "transfers slots 10 and 20").0, NoMatchReason::MisplacedNumber { number: "10".into() });
    assert_eq!(no_match(&i, "what happened yesterday").0, NoMatchReason::CalendarWord { word: "yesterday".into() });
}

#[test]
fn the_default_window_and_limit_never_exceed_the_limits() {
    let mut t = PhraseTables::builtin();
    for template in &mut t.templates {
        template.examples = &[];
    }
    let i = Interpreter::from_tables(catalog(), 400, SpecLimits { max_rows: 4, max_slot_span: 150 }, t).unwrap();
    let m = matched(&i, "busiest programs");
    assert_eq!((m.spec.window, m.spec.limit), (Window::LastSlots(150), Some(4)));
    assert_eq!(
        m.assumptions,
        [
            Assumption::DefaultWindow { slots: 150 },
            Assumption::DefaultMeasure { measure: "ix_count" },
            Assumption::DefaultLimit { n: 4 },
        ]
    );
    // An automatic bucket width aims at no more buckets than the row cap.
    let m = matched(&i, "transfers over time");
    assert_eq!(m.spec.group_by[0].bucket_width, Some(38));
    assert_eq!(m.spec.limit, Some(3));
}

// ---------------------------------------------------------------------------
// Buckets
// ---------------------------------------------------------------------------

#[test]
fn bucket_widths_are_automatic_or_explicit_and_never_exceed_the_row_cap() {
    let i = interpreter();
    let m = matched(&i, "failure rate over time in the last 2 hours");
    assert_eq!(m.spec.group_by[0].bucket_width, Some(360));
    assert!(m.assumptions.contains(&Assumption::AutoBucketWidth { width: 360, buckets: 50 }));

    for (q, width, buckets) in [
        ("transfers per 200 slots", 200, 45),
        ("transfers every 1 slot in the last 1000 slots", 1, 1000),
        ("transfers in buckets of 300 slots from slot 0 to 999", 300, 3),
        ("transfers in 250 slot buckets from slot 100 to 999", 250, 3),
        ("transfers by bucket in the last 100 slots", 2, 50),
        ("transfers per bucket from slot 1 to 1", 1, 1),
    ] {
        let m = matched(&i, q);
        assert_eq!(m.spec.group_by[0].bucket_width, Some(width), "{q}");
        assert_eq!(m.spec.limit, Some(buckets), "{q}");
        assert_eq!(
            m.assumptions.iter().any(|a| matches!(a, Assumption::AutoBucketWidth { .. })),
            q.contains("bucket ") && !q.contains("buckets of") && !q.contains("slot buckets"),
            "{q}"
        );
    }

    // More buckets than the cap: the guard's own refusal.
    let (code, message) = rejected(&i, "transfers every 1 slot in the last 1001 slots");
    assert_eq!(code, RejectCode::Limit);
    assert!(message.contains("invalid limit: 1001 is above the row cap, 1000"), "{message}");
    // Too wide for the window.
    let (code, message) = rejected(&i, "transfers per 600 slots in the last 1000 slots");
    assert_eq!(code, RejectCode::BucketWidth);
    assert!(message.contains("2 * 600 - 1 = 1199"), "{message}");
}

#[test]
fn a_number_is_allowed_only_after_top_or_in_a_bucket_or_window() {
    let i = interpreter();
    assert_eq!(matched(&i, "top 7 mints").spec.limit, Some(7));
    assert_eq!(matched(&i, "top 1,000 mints").spec.limit, Some(1_000));
    for (q, n) in
        [("7 mints", "7"), ("mints 7", "7"), ("top mints 7", "7"), ("transfers by mint 3", "3"), ("top 7 7 mints", "7")]
    {
        assert_eq!(no_match(&i, q).0, NoMatchReason::MisplacedNumber { number: n.into() }, "{q}");
    }
}

// ---------------------------------------------------------------------------
// Suggestions and templates
// ---------------------------------------------------------------------------

#[test]
fn templates_are_listed_in_their_fixed_order() {
    let ts = interpreter().templates();
    assert_eq!(ts.iter().map(|t| t.id).collect::<Vec<_>>(), TemplateId::ALL);
    assert!(ts.iter().all(|t| !t.examples.is_empty() && !t.description.is_empty()));
    assert_eq!(ts[0].examples[0], "top 10 mints by transfers in the last hour");
}

#[test]
fn suggest_completes_a_prefix_from_the_examples() {
    let i = interpreter();
    assert_eq!(
        i.suggest("top", 10),
        ["top 10 mints by transfers in the last hour", "top 5 leaders by slots in the last 2 hours"]
    );
    assert_eq!(i.suggest("  TOP   10 ", 10), ["top 10 mints by transfers in the last hour"]);
    // Then examples with a word starting with it.
    let s = i.suggest("tra", 50);
    assert!(s[0].starts_with("transfers") && s.iter().any(|e| e == "mints by transfer volume"), "{s:?}");
    assert_eq!(i.suggest("", 3).len(), 3);
    assert!(i.suggest("", 0).is_empty());
    assert!(i.suggest("zzz", 5).is_empty());
    let all: usize = TEMPLATES.iter().map(|t| t.examples.len()).sum();
    assert_eq!(i.suggest("", usize::MAX).len(), all.min(MAX_SUGGEST_LIMIT));
    for e in i.suggest("", usize::MAX) {
        assert!(matches!(i.interpret(&e), Interpretation::Matched(_)), "{e}");
    }
}

#[test]
fn a_miss_suggests_the_closest_examples() {
    let i = interpreter();
    let (_, s) = no_match(&i, "top 10 mints by transfers excluding wsol");
    assert_eq!(s[0], "top 10 mints by transfers in the last hour");
    assert!(s.len() <= MAX_SUGGESTIONS);
    // Nothing in common: the first example of each template.
    let (_, s) = no_match(&i, "zzz");
    let firsts: Vec<&str> = TEMPLATES.iter().map(|t| t.examples[0]).collect();
    assert_eq!(s, firsts);
}

// ---------------------------------------------------------------------------
// The wire shape
// ---------------------------------------------------------------------------

#[test]
fn an_interpretation_serialises_with_a_spec_spec_accepts() {
    let i = interpreter();
    let got = i.interpret("top 10 mints by transfers for wsol in the last hour");
    let v = serde_json::to_value(&got).unwrap();
    assert_eq!(v["outcome"], "matched");
    assert_eq!(v["template_id"], "top_n");
    assert_eq!(v["assumptions"][0]["type"], "time_converted");
    assert_eq!(v["assumptions"][0]["unit"], "hour");
    assert_eq!(v["entities"][0]["kind"], "mint");
    let spec: QuerySpec = serde_json::from_value(v["spec"].clone()).unwrap();
    let Interpretation::Matched(m) = got else { panic!() };
    assert_eq!(spec, m.spec);
    validate(&spec, i.catalog(), &i.limits()).unwrap();

    let v = serde_json::to_value(i.interpret("transfers excluding wsol")).unwrap();
    assert_eq!((v["outcome"].as_str(), v["reason"]["type"].as_str()), (Some("no_match"), Some("unknown_word")));
    let v = serde_json::to_value(i.interpret("transactions for wsol")).unwrap();
    assert_eq!((v["outcome"].as_str(), v["code"].as_str()), (Some("rejected"), Some("entity_wrong_table")));
    let v = serde_json::to_value(i.interpret("")).unwrap();
    assert_eq!((v["outcome"].as_str(), v["reason"]["type"].as_str()), (Some("invalid"), Some("empty")));
}

// ---------------------------------------------------------------------------
// Bounded work
// ---------------------------------------------------------------------------

/// The longest legal inputs built to make each stage work hardest: every
/// position an entity prefix, a phrase prefix, a window or bucket prefix,
/// or a number. Each must take time proportional to its length, so a few
/// thousand runs finish well within the bound even unoptimised.
#[test]
fn adversarial_input_takes_bounded_time() {
    let i = interpreter();
    let words = |w: &str| {
        let unit: Vec<&str> = w.split(' ').collect();
        unit.iter().cycle().take(MAX_TOKENS).copied().collect::<Vec<_>>().join(" ")
    };
    let inputs = [
        words("spl token 2022 program"),
        words("associated token account program"),
        words("token"),
        words("top level"),
        words("transfers and"),
        words("in the last"),
        words("in buckets of"),
        words("per 1"),
        words("slots 1 to"),
        words("between slots 1 and"),
        words("how many"),
        words("1,000"),
        words("the"),
        format!("{} zzz", words("transfers by mint").rsplit_once(' ').unwrap().0),
        words(&format!("mint {KEY7}")),
        "x".repeat(MAX_TEXT_CHARS),
        format!("{} ", "a ".repeat(MAX_TEXT_CHARS / 2 - 1)),
    ];
    let runs = 300;
    let started = Instant::now();
    let mut slowest = Duration::ZERO;
    for input in &inputs {
        let t = Instant::now();
        for _ in 0..runs {
            std::hint::black_box(i.interpret(std::hint::black_box(input)));
        }
        slowest = slowest.max(t.elapsed() / runs);
    }
    let total = started.elapsed();
    println!("{} adversarial inputs x {runs}: {total:?}, slowest {slowest:?} per call", inputs.len());
    assert!(slowest < Duration::from_millis(20), "slowest call {slowest:?}");
    assert!(total < Duration::from_secs(30), "{total:?}");
}

#[test]
fn every_outcome_is_deterministic() {
    let a = interpreter();
    let b = interpreter();
    for q in ["busiest programs", "programs by programs", "transfers for wsol and spl token", "zzz", "volume", ""] {
        assert_eq!(a.interpret(q), b.interpret(q), "{q}");
        assert_eq!(a.interpret(q), a.interpret(q), "{q}");
    }
}

#[test]
fn a_matched_spec_is_always_accepted_by_the_guard() {
    let i = interpreter();
    let m = matched(&i, "top 10 programs by instructions and cpi instructions over the past 2 hours");
    let valid = validate(&m.spec, i.catalog(), &i.limits()).unwrap();
    assert_eq!(valid.measures, ["ix_count", "ix_cpi_count"]);
    assert_eq!(m.spec.as_of_slot, None);
}
