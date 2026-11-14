//! `intent/`: does every question mean exactly what the tables say, and do
//! real questions get the right answer or an honest refusal?
//!
//! * Every phrase in the built-in tables, alone and combined with every
//!   other, is interpreted and its spec compared with what the table says.
//! * The design's example table, row by row, spec by spec.
//! * A corpus of real questions with their expected outcome, scored: the
//!   score is printed and must be 100%.
//! * A generated sweep of several thousand questions checks the invariants:
//!   every matched spec passes the guard *and* compiles, ambiguity is real,
//!   conversions are always stated, and nothing is ever silently dropped.
//!
//! Failures are collected, not stopped at, so one run shows every miss.

use std::fmt::Write as _;
use std::sync::Arc;

use oracle::catalog::Catalog;
use oracle::intent::templates::{DIMENSION_PHRASES, MEASURE_PHRASES, TEMPLATES};
use oracle::intent::*;
use oracle::spec::compile::compile;
use oracle::spec::guard::{SpecLimits, validate};
use oracle::spec::{Direction, Filter, FilterOp, GroupBy, Order, QuerySpec, Window, resolve_window};

const WSOL: &str = "So11111111111111111111111111111111111111112";
const VOTE: &str = "Vote111111111111111111111111111111111111111";
const MEMO: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";
const TOKENKEG: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN_2022: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const SYSTEM: &str = "11111111111111111111111111111111";
/// A valid key no built-in entity uses: `[7; 32]`.
const KEY7: &str = "US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx";

const SLOT_MILLIS: u32 = 400;
const CAP: u32 = 1_000;
const SPAN: u64 = 216_000;
/// One hour at 400 ms.
const HOUR: u64 = 9_000;

fn catalog() -> Arc<Catalog> {
    Arc::new(Catalog::new(vec![]).unwrap())
}

fn interpreter() -> Interpreter {
    Interpreter::new(catalog(), SLOT_MILLIS, CAP).unwrap()
}

fn limits() -> SpecLimits {
    SpecLimits { max_rows: CAP, max_slot_span: SPAN }
}

fn filter(dimension: &str, value: &str) -> Filter {
    Filter { dimension: dimension.to_owned(), op: FilterOp::Eq, values: vec![value.to_owned()] }
}

fn strings(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| (*s).to_owned()).collect()
}

/// `spec` passes the guard and compiles to a statement, against a snapshot
/// far above every slot used here.
fn runs(spec: &QuerySpec, c: &Catalog) -> Result<(), String> {
    let valid = validate(spec, c, &limits()).map_err(|e| format!("guard: {e}"))?;
    let window = resolve_window(valid.window, 400_000_000).map_err(|e| format!("window: {e}"))?;
    let window = match valid.bucket_width() {
        Some(w) => window.snap(w).ok_or("no whole bucket")?,
        None => window,
    };
    compile(&valid, c, window).map(|_| ()).map_err(|e| format!("compile: {e}"))
}

fn outcome(i: &Interpretation) -> String {
    match i {
        Interpretation::Matched(m) => format!("Matched({}, {:?})", m.template_id, m.spec),
        Interpretation::Ambiguous { candidates } => {
            format!("Ambiguous({:?})", candidates.iter().map(|c| (c.template_id, &c.spec)).collect::<Vec<_>>())
        }
        Interpretation::NoMatch { reason, .. } => format!("NoMatch({reason:?})"),
        Interpretation::Rejected { code, message } => format!("Rejected({code:?}: {message})"),
        Interpretation::Invalid { reason } => format!("Invalid({reason:?})"),
    }
}

/// Collects every check, then reports the score and every miss.
#[derive(Default)]
struct Board {
    name: &'static str,
    total: usize,
    misses: Vec<String>,
}

impl Board {
    fn new(name: &'static str) -> Self {
        Self { name, ..Self::default() }
    }

    fn check(&mut self, question: &str, ok: bool, detail: impl FnOnce() -> String) {
        self.total += 1;
        if !ok {
            self.misses.push(format!("{question:?}: {}", detail()));
        }
    }

    #[track_caller]
    fn finish(self) {
        let right = self.total - self.misses.len();
        let pct = 100.0 * right as f64 / self.total.max(1) as f64;
        println!("{}: {right}/{} questions right ({pct:.1}%)", self.name, self.total);
        assert!(self.total > 0, "{}: nothing was checked", self.name);
        if !self.misses.is_empty() {
            let mut msg = format!("{}: {right}/{} right ({pct:.1}%), misses:\n", self.name, self.total);
            for m in &self.misses {
                let _ = writeln!(msg, "  {m}");
            }
            panic!("{msg}");
        }
    }
}

// ---------------------------------------------------------------------------
// Every phrase means exactly what its table says
// ---------------------------------------------------------------------------

/// The first plain phrase for `measure`: no implied filter.
fn phrase_for(measure: &str) -> &'static str {
    MEASURE_PHRASES.iter().find(|p| p.measure == measure && p.implied.is_none()).unwrap().phrase
}

fn implied(p: &templates::MeasurePhrase) -> Vec<Filter> {
    p.implied.map(|f| filter(f.dimension, f.value)).into_iter().collect()
}

#[test]
fn every_measure_phrase_means_exactly_its_measure() {
    let i = interpreter();
    let c = catalog();
    let mut board = Board::new("measure phrases");
    for p in MEASURE_PHRASES {
        let needs = c.measure(p.measure).unwrap().requires_dimension;
        for question in [p.phrase.to_owned(), format!("how many {}", p.phrase), format!("show me the {}", p.phrase)] {
            let got = i.interpret(&question);
            let ok = match (&got, needs) {
                (Interpretation::Matched(m), None) => {
                    m.template_id == TemplateId::Total
                        && m.spec.measures == [p.measure]
                        && m.spec.filters == implied(p)
                        && m.spec.group_by.is_empty()
                        && m.spec.order_by.is_empty()
                        && m.spec.limit.is_none()
                        && m.spec.window == Window::LastSlots(HOUR)
                        && m.assumptions == [Assumption::DefaultWindow { slots: HOUR }]
                }
                (Interpretation::Rejected { code: RejectCode::NeedsDimension, .. }, Some(_)) => true,
                _ => false,
            };
            board
                .check(&question, ok, || format!("expected {} with {:?}, got {}", p.measure, p.implied, outcome(&got)));
        }

        let question = format!("{} over time", p.phrase);
        let got = i.interpret(&question);
        let ok = match (&got, needs) {
            (Interpretation::Matched(m), None) => {
                m.template_id == TemplateId::OverTime
                    && m.spec.measures == [p.measure]
                    && m.spec.filters == implied(p)
                    && m.spec.group_by == [GroupBy { dimension: "slot_bucket".into(), bucket_width: Some(180) }]
                    && m.spec.order_by == [Order { target: "slot_bucket".into(), direction: Direction::Asc }]
                    && m.spec.limit == Some(50)
            }
            (Interpretation::Rejected { code: RejectCode::NeedsDimension, .. }, Some(_)) => true,
            _ => false,
        };
        board.check(&question, ok, || format!("expected {} per 180-slot bucket, got {}", p.measure, outcome(&got)));
    }
    board.finish();
}

#[test]
fn every_dimension_phrase_means_exactly_its_dimension() {
    let i = interpreter();
    let mut board = Board::new("dimension phrases");
    for p in DIMENSION_PHRASES {
        let default = templates::DEFAULT_MEASURES.iter().find(|d| d.dimension == p.dimension).unwrap().measure;
        let group = [GroupBy { dimension: p.dimension.to_owned(), bucket_width: None }];

        // No measure: the default one, said so.
        let question = format!("top {}", p.phrase);
        let got = i.interpret(&question);
        let ok = matches!(&got, Interpretation::Matched(m)
            if m.template_id == TemplateId::TopN
                && m.spec.measures == [default]
                && m.spec.group_by == group
                && m.spec.order_by == [Order { target: default.to_owned(), direction: Direction::Desc }]
                && m.spec.limit == Some(DEFAULT_TOP)
                && m.assumptions.contains(&Assumption::DefaultMeasure { measure: default })
                && m.assumptions.contains(&Assumption::DefaultLimit { n: DEFAULT_TOP }));
        board.check(&question, ok, || format!("expected {default} by {}, got {}", p.dimension, outcome(&got)));

        // An explicit count and measure: no measure or limit assumption.
        let question = format!("top 3 {} by {}", p.phrase, phrase_for(default));
        let got = i.interpret(&question);
        let ok = matches!(&got, Interpretation::Matched(m)
            if m.template_id == TemplateId::TopN
                && m.spec.measures == [default]
                && m.spec.group_by == group
                && m.spec.limit == Some(3)
                && m.assumptions == [Assumption::DefaultWindow { slots: HOUR }]);
        board.check(&question, ok, || format!("expected top 3 {} by {default}, got {}", p.dimension, outcome(&got)));
    }
    board.finish();
}

/// Every measure phrase by every dimension phrase: exactly that measure by
/// exactly that dimension, or the guard's refusal for the right reason.
#[test]
fn every_measure_by_every_dimension_means_exactly_that_or_is_refused_for_the_right_reason() {
    let i = interpreter();
    let c = catalog();
    let mut board = Board::new("measure by dimension");
    for mp in MEASURE_PHRASES {
        let m = c.measure(mp.measure).unwrap();
        for dp in DIMENSION_PHRASES {
            let question = format!("{} by {}", mp.phrase, dp.phrase);
            let got = i.interpret(&question);
            let on_table = c.compatible(mp.measure, dp.dimension);
            let pinned = m.requires_dimension.is_none_or(|r| r == dp.dimension);
            let want = QuerySpec {
                measures: strings(&[mp.measure]),
                group_by: vec![GroupBy { dimension: dp.dimension.to_owned(), bucket_width: None }],
                filters: implied(mp),
                window: Window::LastSlots(HOUR),
                order_by: vec![Order { target: mp.measure.to_owned(), direction: Direction::Desc }],
                limit: Some(DEFAULT_TOP),
                as_of_slot: None,
            };
            let ok = match &got {
                // `top_n` only when both words read both ways and give the same spec.
                Interpretation::Matched(x) => {
                    on_table
                        && pinned
                        && x.spec == want
                        && (x.template_id == TemplateId::ByDimension || x.template_id == TemplateId::TopN)
                }
                Interpretation::Rejected { code: RejectCode::DimensionWrongTable, .. } => !on_table,
                Interpretation::Rejected { code: RejectCode::NeedsDimension, .. } => on_table && !pinned,
                _ => false,
            };
            board
                .check(&question, ok, || format!("expected {} by {}, got {}", mp.measure, dp.dimension, outcome(&got)));
        }
    }
    board.finish();
}

// ---------------------------------------------------------------------------
// The design's example table
// ---------------------------------------------------------------------------

fn spec(
    measures: &[&str],
    group_by: Option<(&str, Option<u64>)>,
    filters: Vec<Filter>,
    window: Window,
    order_by: Option<(&str, Direction)>,
    limit: Option<u32>,
) -> QuerySpec {
    QuerySpec {
        measures: strings(measures),
        group_by: group_by.map(|(d, w)| GroupBy { dimension: d.to_owned(), bucket_width: w }).into_iter().collect(),
        filters,
        window,
        order_by: order_by.map(|(t, direction)| Order { target: t.to_owned(), direction }).into_iter().collect(),
        limit,
        as_of_slot: None,
    }
}

#[track_caller]
fn matched(i: &Interpreter, q: &str) -> Matched {
    match i.interpret(q) {
        Interpretation::Matched(m) => *m,
        other => panic!("{q:?}: expected Matched, got {}", outcome(&other)),
    }
}

fn hours(n: u64) -> Assumption {
    Assumption::TimeConverted { amount: n, unit: TimeUnit::Hour, slots: n * HOUR, slot_millis: SLOT_MILLIS }
}

#[test]
fn the_design_examples_give_exactly_the_specs_in_the_table() {
    use Direction::{Asc, Desc};
    let i = interpreter();
    let c = catalog();
    let rows: Vec<(&str, TemplateId, QuerySpec, Vec<Assumption>)> = vec![
        (
            "top 10 mints by transfers in the last hour",
            TemplateId::TopN,
            spec(
                &["transfer_count"],
                Some(("mint", None)),
                vec![],
                Window::LastSlots(HOUR),
                Some(("transfer_count", Desc)),
                Some(10),
            ),
            vec![hours(1)],
        ),
        (
            "busiest programs",
            TemplateId::TopN,
            spec(
                &["ix_count"],
                Some(("program", None)),
                vec![],
                Window::LastSlots(HOUR),
                Some(("ix_count", Desc)),
                Some(10),
            ),
            vec![
                Assumption::DefaultWindow { slots: HOUR },
                Assumption::DefaultMeasure { measure: "ix_count" },
                Assumption::DefaultLimit { n: 10 },
            ],
        ),
        (
            "transfers by mint in the last 6 hours",
            TemplateId::ByDimension,
            spec(
                &["transfer_count"],
                Some(("mint", None)),
                vec![],
                Window::LastSlots(6 * HOUR),
                Some(("transfer_count", Desc)),
                Some(10),
            ),
            vec![hours(6), Assumption::DefaultLimit { n: 10 }],
        ),
        (
            "how many failed transactions in the last day",
            TemplateId::Total,
            spec(&["tx_failed_count"], None, vec![], Window::LastSlots(24 * HOUR), None, None),
            vec![Assumption::TimeConverted {
                amount: 1,
                unit: TimeUnit::Day,
                slots: 216_000,
                slot_millis: SLOT_MILLIS,
            }],
        ),
        (
            "failure rate over time in the last 2 hours",
            TemplateId::OverTime,
            spec(
                &["tx_failure_ratio"],
                Some(("slot_bucket", Some(360))),
                vec![],
                Window::LastSlots(2 * HOUR),
                Some(("slot_bucket", Asc)),
                Some(50),
            ),
            vec![hours(2), Assumption::AutoBucketWidth { width: 360, buckets: 50 }],
        ),
        (
            "instructions for the vote program in the last 1000 slots",
            TemplateId::Total,
            spec(&["ix_count"], None, vec![filter("program", VOTE)], Window::LastSlots(1000), None, None),
            vec![],
        ),
        (
            "transfers per 200 slots",
            TemplateId::OverTime,
            spec(
                &["transfer_count"],
                Some(("slot_bucket", Some(200))),
                vec![],
                Window::LastSlots(HOUR),
                Some(("slot_bucket", Asc)),
                Some(45),
            ),
            vec![Assumption::DefaultWindow { slots: HOUR }],
        ),
    ];
    for (q, template, want, assumptions) in rows {
        let m = matched(&i, q);
        assert_eq!(m.template_id, template, "{q}");
        assert_eq!(m.spec, want, "{q}");
        assert_eq!(m.assumptions, assumptions, "{q}");
        runs(&m.spec, &c).unwrap_or_else(|e| panic!("{q}: {e}"));
    }

    // "SPL Token is a program, and transfers are filtered by mint."
    match i.interpret("transfer volume by mint for spl token") {
        Interpretation::Rejected { code: RejectCode::EntityWrongTable, message } => {
            assert!(message.contains("dimension program is not on token_transfers"), "{message}");
            assert!(message.contains(RejectCode::EntityWrongTable.hint().unwrap()), "{message}");
        }
        other => panic!("{}", outcome(&other)),
    }
    let no_match = |q: &str| match i.interpret(q) {
        Interpretation::NoMatch { reason, suggestions } => {
            assert!(!suggestions.is_empty(), "{q}");
            reason
        }
        other => panic!("{q}: {}", outcome(&other)),
    };
    assert_eq!(
        no_match("top 10 mints by transfers excluding wsol"),
        NoMatchReason::UnknownWord { word: "excluding".into() }
    );
    assert_eq!(no_match("what happened yesterday"), NoMatchReason::CalendarWord { word: "yesterday".into() });
}

// ---------------------------------------------------------------------------
// Real questions
// ---------------------------------------------------------------------------

/// What a real question should give.
enum Want {
    /// Template, measures, group-by (dimension, bucket width), filters,
    /// window, limit. Order is implied by the template.
    Spec(
        TemplateId,
        &'static [&'static str],
        Option<(&'static str, Option<u64>)>,
        &'static [(&'static str, &'static str)],
        Window,
        Option<u32>,
    ),
    Rejected(RejectCode),
    NoMatch(NoMatchReason),
    Invalid(InvalidReason),
}

fn want_spec(
    t: TemplateId,
    measures: &[&str],
    group: Option<(&str, Option<u64>)>,
    filters: &[(&str, &str)],
    window: Window,
    limit: Option<u32>,
) -> QuerySpec {
    let order = match (t, group) {
        (TemplateId::OverTime, _) => Some(("slot_bucket", Direction::Asc)),
        (TemplateId::Total, _) | (_, None) => None,
        _ => Some((measures[0], Direction::Desc)),
    };
    spec(measures, group, filters.iter().map(|(d, v)| filter(d, v)).collect(), window, order, limit)
}

fn real_questions() -> Vec<(String, Want)> {
    use TemplateId::{ByDimension, OverTime, TopN, Total};
    use Want::*;
    use Window::{LastSlots, Range};
    let long = "a".repeat(MAX_TEXT_CHARS + 1);
    let many = vec!["tx"; MAX_TOKENS + 1].join(" ");
    let v: Vec<(&str, Want)> = vec![
        // --- answered ---
        (
            "How many transactions in the last 10 minutes?",
            Spec(Total, &["tx_count"], None, &[], LastSlots(1_500), None),
        ),
        (
            "show me the top 5 programs by cpi instructions",
            Spec(TopN, &["ix_cpi_count"], Some(("program", None)), &[], LastSlots(HOUR), Some(5)),
        ),
        (
            "What is the failure rate in the past 30 minutes",
            Spec(Total, &["tx_failure_ratio"], None, &[], LastSlots(4_500), None),
        ),
        (
            "average fee over time",
            Spec(OverTime, &["fee_avg_lamports"], Some(("slot_bucket", Some(180))), &[], LastSlots(HOUR), Some(50)),
        ),
        ("total fees in the last 24 hours", Spec(Total, &["fee_total_lamports"], None, &[], LastSlots(216_000), None)),
        ("total fees paid", Spec(Total, &["fee_total_lamports"], None, &[], LastSlots(HOUR), None)),
        (
            "wsol transfers over time in the last hour",
            Spec(
                OverTime,
                &["transfer_count"],
                Some(("slot_bucket", Some(180))),
                &[("mint", WSOL)],
                LastSlots(HOUR),
                Some(50),
            ),
        ),
        (
            "top 10 tokens by volume",
            Spec(TopN, &["transfer_volume_raw"], Some(("mint", None)), &[], LastSlots(HOUR), Some(10)),
        ),
        (
            "transfers and volume by mint",
            Spec(
                ByDimension,
                &["transfer_count", "transfer_volume_raw"],
                Some(("mint", None)),
                &[],
                LastSlots(HOUR),
                Some(10),
            ),
        ),
        (
            "successful transactions in the last hour",
            Spec(Total, &["tx_count"], None, &[("tx_status", "success")], LastSlots(HOUR), None),
        ),
        (
            "top-level instructions by program",
            Spec(
                ByDimension,
                &["ix_count"],
                Some(("program", None)),
                &[("ix_level", "top_level")],
                LastSlots(HOUR),
                Some(10),
            ),
        ),
        (
            "top level instructions by program",
            Spec(
                ByDimension,
                &["ix_count"],
                Some(("program", None)),
                &[("ix_level", "top_level")],
                LastSlots(HOUR),
                Some(10),
            ),
        ),
        (
            "number of undecoded instructions for the memo program",
            Spec(Total, &["ix_undecoded_count"], None, &[("program", MEMO)], LastSlots(HOUR), None),
        ),
        ("spl token instructions", Spec(Total, &["ix_count"], None, &[("program", TOKENKEG)], LastSlots(HOUR), None)),
        (
            "token 2022 instructions",
            Spec(Total, &["ix_count"], None, &[("program", TOKEN_2022)], LastSlots(HOUR), None),
        ),
        (
            "how many distinct programs from slot 100 to 200",
            Spec(Total, &["program_count"], None, &[], Range { from_slot: 100, to_slot: 200 }, None),
        ),
        (
            "transactions in slot 250000000",
            Spec(Total, &["tx_count"], None, &[], Range { from_slot: 250_000_000, to_slot: 250_000_000 }, None),
        ),
        (
            "transfers between slots 1,000 and 2,000",
            Spec(Total, &["transfer_count"], None, &[], Range { from_slot: 1_000, to_slot: 2_000 }, None),
        ),
        (
            "transfers for mint So11111111111111111111111111111111111111112",
            Spec(Total, &["transfer_count"], None, &[("mint", WSOL)], LastSlots(HOUR), None),
        ),
        (
            "instructions for program Vote111111111111111111111111111111111111111",
            Spec(Total, &["ix_count"], None, &[("program", VOTE)], LastSlots(HOUR), None),
        ),
        (
            "slots for leader US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx",
            Spec(Total, &["slot_count"], None, &[("leader", KEY7)], LastSlots(HOUR), None),
        ),
        (
            "transfers for source US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx",
            Spec(Total, &["transfer_count"], None, &[("source_owner", KEY7)], LastSlots(HOUR), None),
        ),
        (
            "transfers for destination US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx",
            Spec(Total, &["transfer_count"], None, &[("destination_owner", KEY7)], LastSlots(HOUR), None),
        ),
        (
            "top recipients by volume for wsol",
            Spec(
                TopN,
                &["transfer_volume_raw"],
                Some(("destination_owner", None)),
                &[("mint", WSOL)],
                LastSlots(HOUR),
                Some(10),
            ),
        ),
        (
            "busiest leaders in the last 2 hours",
            Spec(TopN, &["slot_count"], Some(("leader", None)), &[], LastSlots(2 * HOUR), Some(10)),
        ),
        (
            "transactions by status in the last 1000 slots",
            Spec(ByDimension, &["tx_count"], Some(("tx_status", None)), &[], LastSlots(1_000), Some(10)),
        ),
        (
            "instructions by level",
            Spec(ByDimension, &["ix_count"], Some(("ix_level", None)), &[], LastSlots(HOUR), Some(10)),
        ),
        (
            "failed transactions, average fee and total fees",
            Spec(
                Total,
                &["tx_failed_count", "fee_avg_lamports", "fee_total_lamports"],
                None,
                &[],
                LastSlots(HOUR),
                None,
            ),
        ),
        (
            "transfers per 500 slots for wsol in the last hour",
            Spec(
                OverTime,
                &["transfer_count"],
                Some(("slot_bucket", Some(500))),
                &[("mint", WSOL)],
                LastSlots(HOUR),
                Some(18),
            ),
        ),
        (
            "transfers in buckets of 100 slots",
            Spec(OverTime, &["transfer_count"], Some(("slot_bucket", Some(100))), &[], LastSlots(HOUR), Some(90)),
        ),
        ("most used programs", Spec(TopN, &["ix_count"], Some(("program", None)), &[], LastSlots(HOUR), Some(10))),
        (
            "top 3 senders by transfers for wsol",
            Spec(TopN, &["transfer_count"], Some(("source_owner", None)), &[("mint", WSOL)], LastSlots(HOUR), Some(3)),
        ),
        (
            "TRANSFERS BY MINT",
            Spec(ByDimension, &["transfer_count"], Some(("mint", None)), &[], LastSlots(HOUR), Some(10)),
        ),
        (
            "how many unattributed transfers",
            Spec(Total, &["transfer_unattributed_count"], None, &[], LastSlots(HOUR), None),
        ),
        (
            "transfers without a mint in the last hour",
            Spec(Total, &["transfer_unattributed_count"], None, &[], LastSlots(HOUR), None),
        ),
        ("wsol mint transfers", Spec(Total, &["transfer_count"], None, &[("mint", WSOL)], LastSlots(HOUR), None)),
        (
            "top 10 programs by instructions and cpi instructions over the past 2 hours",
            Spec(TopN, &["ix_count", "ix_cpi_count"], Some(("program", None)), &[], LastSlots(2 * HOUR), Some(10)),
        ),
        ("how many slots in the last hour", Spec(Total, &["slot_count"], None, &[], LastSlots(HOUR), None)),
        (
            "decoded instructions by program",
            Spec(
                ByDimension,
                &["ix_count"],
                Some(("program", None)),
                &[("ix_decoded", "decoded")],
                LastSlots(HOUR),
                Some(10),
            ),
        ),
        (
            "inner instructions for the system program in the last 5 minutes",
            Spec(Total, &["ix_cpi_count"], None, &[("program", SYSTEM)], LastSlots(750), None),
        ),
        (
            "give me the error rate over time",
            Spec(OverTime, &["tx_failure_ratio"], Some(("slot_bucket", Some(180))), &[], LastSlots(HOUR), Some(50)),
        ),
        (
            "transfers by token programs",
            Spec(ByDimension, &["transfer_count"], Some(("token_program", None)), &[], LastSlots(HOUR), Some(10)),
        ),
        (
            "average compute units by status",
            Spec(ByDimension, &["compute_units_avg"], Some(("tx_status", None)), &[], LastSlots(HOUR), Some(10)),
        ),
        (
            "what are the busiest programs in the last day",
            Spec(TopN, &["ix_count"], Some(("program", None)), &[], LastSlots(216_000), Some(10)),
        ),
        // --- refused: understood, and the spec is refused ---
        ("transactions for wsol", Rejected(RejectCode::EntityWrongTable)),
        ("successful transactions for wsol", Rejected(RejectCode::EntityWrongTable)),
        ("mints by transfers for the token 2022 program", Rejected(RejectCode::EntityWrongTable)),
        ("transactions by mint", Rejected(RejectCode::DimensionWrongTable)),
        ("fees by program", Rejected(RejectCode::DimensionWrongTable)),
        ("transactions and transfers", Rejected(RejectCode::MixedTables)),
        ("volume", Rejected(RejectCode::NeedsDimension)),
        ("volume by senders", Rejected(RejectCode::NeedsDimension)),
        ("top 5000 mints", Rejected(RejectCode::Limit)),
        ("top 0 mints", Rejected(RejectCode::Limit)),
        ("transfers per 10 slots in the last day", Rejected(RejectCode::Limit)),
        ("transfers in the last 2 weeks", Rejected(RejectCode::Window)),
        ("busiest programs in the last week", Rejected(RejectCode::Window)),
        ("transactions in the last 0 hours", Rejected(RejectCode::Window)),
        ("transfers from slot 200 to 100", Rejected(RejectCode::Window)),
        ("transfers per 0 slots", Rejected(RejectCode::BucketWidth)),
        ("transfers per 5000 slots", Rejected(RejectCode::BucketWidth)),
        ("transfers for the burn address", Rejected(RejectCode::AccountEntity)),
        ("transfers for mint TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", Rejected(RejectCode::KindMismatch)),
        ("successful transactions and fees", Rejected(RejectCode::ImpliedFilterNotAlone)),
        ("transactions and txs", Rejected(RejectCode::RepeatedMeasure)),
        ("mints and programs", Rejected(RejectCode::MixedTables)),
        // --- refused: not understood ---
        ("transfers excluding wsol", NoMatch(NoMatchReason::UnknownWord { word: "excluding".into() })),
        ("trnasfers by mint", NoMatch(NoMatchReason::UnknownWord { word: "trnasfers".into() })),
        ("how many token transfers without a mint", NoMatch(NoMatchReason::UnknownWord { word: "without".into() })),
        ("how many transactions failed", NoMatch(NoMatchReason::UnknownWord { word: "failed".into() })),
        (
            "transfers for So11111111111111111111111111111111111111112",
            NoMatch(NoMatchReason::BareKey { key: WSOL.into() }),
        ),
        ("transfers for wsol and spl token", NoMatch(NoMatchReason::TooManyEntities { count: 2 })),
        ("transfers in the last hour in the last day", NoMatch(NoMatchReason::TwoWindows)),
        ("transfers in the last few hours", NoMatch(NoMatchReason::UnsupportedWindow { word: "last".into() })),
        ("transfers 10", NoMatch(NoMatchReason::MisplacedNumber { number: "10".into() })),
        ("transactions this month", NoMatch(NoMatchReason::CalendarWord { word: "month".into() })),
        ("transfers last tuesday", NoMatch(NoMatchReason::CalendarWord { word: "tuesday".into() })),
        ("average fee yesterday", NoMatch(NoMatchReason::CalendarWord { word: "yesterday".into() })),
        ("transfers since monday", NoMatch(NoMatchReason::CalendarWord { word: "since".into() })),
        (
            "slots, transactions, instructions, transfers and fees",
            NoMatch(NoMatchReason::TooManyMeasures { count: 5, max: 4 }),
        ),
        ("top transfers", NoMatch(NoMatchReason::NoTemplate)),
        ("by mint", NoMatch(NoMatchReason::NoTemplate)),
        ("how many", NoMatch(NoMatchReason::NoTemplate)),
        ("transfers for wsol by mint", NoMatch(NoMatchReason::NoTemplate)),
        ("mints over time by program", NoMatch(NoMatchReason::NoTemplate)),
        ("transfers total", NoMatch(NoMatchReason::NoTemplate)),
        // --- not questions ---
        ("", Invalid(InvalidReason::Empty)),
        ("   ", Invalid(InvalidReason::Empty)),
        ("?!", Invalid(InvalidReason::Empty)),
        ("transfers\u{7}", Invalid(InvalidReason::ControlCharacter { position: 9 })),
        ("transfers\u{0} by mint", Invalid(InvalidReason::ControlCharacter { position: 9 })),
    ];
    let mut v: Vec<(String, Want)> = v.into_iter().map(|(q, w)| (q.to_owned(), w)).collect();
    v.push((long, Want::Invalid(InvalidReason::TooLong { max_chars: MAX_TEXT_CHARS })));
    v.push((many, Want::Invalid(InvalidReason::TooManyTokens { count: MAX_TOKENS + 1, max: MAX_TOKENS })));
    v
}

#[test]
fn real_questions_get_the_right_answer_or_an_honest_refusal() {
    let i = interpreter();
    let c = catalog();
    let mut board = Board::new("real questions");
    let (mut answered, mut refused) = (0, 0);
    for (q, want) in real_questions() {
        let got = i.interpret(&q);
        let (ok, expected) = match &want {
            Want::Spec(t, ms, g, fs, w, l) => {
                answered += 1;
                let s = want_spec(*t, ms, *g, fs, *w, *l);
                let ok = matches!(&got, Interpretation::Matched(m) if m.template_id == *t && m.spec == s && runs(&m.spec, &c).is_ok());
                (ok, format!("Matched({t}, {s:?})"))
            }
            Want::Rejected(code) => {
                refused += 1;
                let ok = matches!(&got, Interpretation::Rejected { code: got, message } if got == code && !message.is_empty());
                (ok, format!("Rejected({code:?})"))
            }
            Want::NoMatch(reason) => {
                refused += 1;
                let ok = matches!(&got, Interpretation::NoMatch { reason: got, suggestions } if got == reason && !suggestions.is_empty());
                (ok, format!("NoMatch({reason:?})"))
            }
            Want::Invalid(reason) => {
                refused += 1;
                (
                    matches!(&got, Interpretation::Invalid { reason: got } if got == reason),
                    format!("Invalid({reason:?})"),
                )
            }
        };
        board.check(&q, ok, || format!("expected {expected}, got {}", outcome(&got)));
    }
    println!("real questions: {answered} should be answered, {refused} refused");
    board.finish();
}

/// Matched answers say how every default and conversion was filled in.
#[test]
fn real_answers_state_their_assumptions_and_entities() {
    let i = interpreter();
    let m = matched(&i, "wsol transfers over time in the last hour");
    assert_eq!(m.assumptions, [hours(1), Assumption::AutoBucketWidth { width: 180, buckets: 50 }]);
    assert_eq!(
        m.entities,
        [ResolvedEntity {
            text: "wsol".into(),
            name: Some("Wrapped SOL".into()),
            address: WSOL.into(),
            kind: oracle::catalog::EntityKind::Mint,
            dimension: "mint",
        }]
    );
    assert_eq!(
        m.understood_as,
        "Token transfers per 180-slot bucket, oldest first, for Wrapped SOL, over the last 9000 slots \
         (about 1 hour at 400 ms a slot)"
    );

    let m = matched(&i, "slots for leader US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx");
    assert_eq!(m.entities[0].name, None, "the catalog does not know this address");
    assert_eq!(m.entities[0].text, format!("leader {KEY7}"));
    assert!(m.understood_as.contains(&format!("for leader {KEY7}")), "{}", m.understood_as);

    let m = matched(&i, "top-level instructions by program from slot 5 to 9");
    assert_eq!(m.assumptions, [Assumption::DefaultLimit { n: 10 }], "a slot range needs no conversion");
    assert_eq!(
        m.understood_as,
        "Instructions by Program, top 10 by Instructions, where instruction level is top_level, over slots 5 to 9"
    );
}

// ---------------------------------------------------------------------------
// The generated sweep
// ---------------------------------------------------------------------------

#[test]
fn a_generated_sweep_keeps_every_invariant() {
    let i = interpreter();
    let c = catalog();
    let entities = [
        "",
        " for wsol",
        " for the vote program",
        " for mint So11111111111111111111111111111111111111112",
        " for leader US517G5965aydkZ46HS38QLi7UQiSojurfbQfKCELFx",
        " for the burn address",
    ];
    let windows = ["", " in the last hour", " in the last 1000 slots", " from slot 100 to 5000", " in the last 3 days"];
    let leads = ["", "how many ", "show me ", "total "];
    let mut questions: Vec<(String, &[TemplateId])> = Vec::new();
    use TemplateId::*;
    for (a, m) in MEASURE_PHRASES.iter().enumerate() {
        for (b, e) in entities.iter().enumerate() {
            for (k, w) in windows.iter().enumerate() {
                let lead = leads[(a + b + k) % leads.len()];
                questions.push((format!("{lead}{}{e}{w}", m.phrase), &[Total]));
                questions.push((format!("{} over time{e}{w}", m.phrase), &[OverTime]));
                questions.push((format!("{} per 100 slots{e}{w}", m.phrase), &[OverTime]));
            }
        }
        for (b, d) in DIMENSION_PHRASES.iter().enumerate() {
            let e = entities[(a + b) % entities.len()];
            let w = windows[(a * 7 + b) % windows.len()];
            questions.push((format!("top {} {} by {}{e}{w}", 1 + (a + b) % 20, d.phrase, m.phrase), &[TopN]));
            questions.push((format!("{} by {}{e}{w}", m.phrase, d.phrase), &[ByDimension, TopN]));
            questions.push((format!("{} by {}{e}{w}", d.phrase, m.phrase), &[TopN, ByDimension]));
        }
    }

    let mut board = Board::new("generated sweep");
    let mut counts = std::collections::BTreeMap::<&str, usize>::new();
    for (q, allowed) in &questions {
        let got = i.interpret(q);
        // Deterministic.
        let again = i.interpret(q);
        let mut problems: Vec<String> = Vec::new();
        if got != again {
            problems.push("two runs differ".into());
        }
        let check_matched = |m: &Matched, problems: &mut Vec<String>| {
            if let Err(e) = runs(&m.spec, &c) {
                problems.push(format!("{} does not run: {e}", m.template_id));
            }
            if !allowed.contains(&m.template_id) {
                problems.push(format!("template {} not in {allowed:?}", m.template_id));
            }
            let time = q.contains("hour") || q.contains("days");
            let converted = m.assumptions.iter().any(|a| matches!(a, Assumption::TimeConverted { .. }));
            if time != converted {
                problems.push(format!("time window {time}, conversion stated {converted}"));
            }
            let defaulted = m.assumptions.iter().any(|a| matches!(a, Assumption::DefaultWindow { .. }));
            if defaulted != (!q.contains(" in the ") && !q.contains(" from slot")) {
                problems.push("default window stated wrongly".into());
            }
            for e in &m.entities {
                if !m.spec.filters.contains(&filter(e.dimension, &e.address)) {
                    problems.push(format!("entity {} is not a filter", e.address));
                }
                if !q.contains(&e.text) {
                    problems.push(format!("entity text {:?} is not in the question", e.text));
                }
            }
            let has_entity = entities[1..].iter().any(|e| q.ends_with(e) || q.contains(&format!("{e} ")));
            if has_entity != (m.entities.len() == 1) {
                problems.push("entity dropped or invented".into());
            }
        };
        let kind = match &got {
            Interpretation::Matched(m) => {
                check_matched(m, &mut problems);
                "matched"
            }
            Interpretation::Ambiguous { candidates } => {
                if candidates.len() < 2 {
                    problems.push("fewer than two candidates".into());
                }
                for (k, a) in candidates.iter().enumerate() {
                    check_matched(a, &mut problems);
                    if candidates[..k].iter().any(|b| b.spec == a.spec) {
                        problems.push("two candidates with one spec".into());
                    }
                    if k > 0 && candidates[k - 1].template_id >= a.template_id {
                        problems.push("candidates out of template order".into());
                    }
                }
                "ambiguous"
            }
            Interpretation::Rejected { message, .. } => {
                if message.is_empty() {
                    problems.push("empty message".into());
                }
                "rejected"
            }
            Interpretation::NoMatch { .. } => {
                problems.push("every word here is known; it must not be NoMatch".into());
                "no_match"
            }
            Interpretation::Invalid { .. } => {
                problems.push("invalid".into());
                "invalid"
            }
        };
        *counts.entry(kind).or_default() += 1;
        board.check(q, problems.is_empty(), || format!("{}: {}", problems.join("; "), outcome(&got)));
    }
    println!("generated sweep outcomes: {counts:?}");
    assert!(counts["matched"] > 1_000 && counts["rejected"] > 1_000, "{counts:?}");
    board.finish();
}

/// The examples the service shows are questions that work.
#[test]
fn every_template_example_matches_its_template_and_runs() {
    let i = interpreter();
    let c = catalog();
    for t in TEMPLATES {
        for e in t.examples {
            let m = matched(&i, e);
            assert_eq!(m.template_id, t.id, "{e}");
            runs(&m.spec, &c).unwrap_or_else(|err| panic!("{e}: {err}"));
        }
    }
}
