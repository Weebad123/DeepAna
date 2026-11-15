//! Structured queries, typed questions, and what a client may ask about.
//!
//! * `POST /query`: a [`QuerySpec`] in, [`SpecRunner::run`]'s
//!   [`SpecAnswer`] out, unchanged.
//! * `POST /ask`: `{ "question": "...", "execute": true }` in, an
//!   [`AskResponse`] out. HTTP 200 whenever the question could be processed,
//!   whatever ORACLE made of it: the outcome is in `outcome`. Only a text
//!   that is not a question at all (empty, too long, control characters) is
//!   a 400 on `question`, and a failure while executing a matched spec is
//!   the same error `/query` would return. A matched spec runs through the
//!   same [`SpecRunner::run`] as `/query`, so `answer` is exactly what
//!   `POST /query` with `spec` returns. ORACLE never runs a guess: an
//!   ambiguous question returns its candidates, each with its spec, and runs
//!   none.
//! * `GET /suggest?prefix=&limit=`, `GET /templates`, `GET /catalog`:
//!   static listings, deterministic.
//!
//! [`SpecRunner::run`]: crate::spec::SpecRunner::run

use std::sync::Arc;

use axum::extract::State;
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiJson, StrictQuery};
use crate::AppState;
use crate::catalog::{DimTable, EntityKind, Limits, Table, Unit};
use crate::intent::{
    InvalidReason, Interpretation, Matched, NoMatchReason, RejectCode, ResolvedEntity, TemplateId, TemplateInfo,
};
use crate::spec::{QuerySpec, SpecAnswer};

/// Longest `/suggest` prefix, in characters.
pub const MAX_PREFIX_CHARS: usize = 100;
pub const MIN_SUGGEST_LIMIT: u64 = 1;
pub const MAX_SUGGEST_LIMIT: u64 = 10;
pub const DEFAULT_SUGGEST_LIMIT: u64 = 5;

// ---------------------------------------------------------------------------
// POST /query
// ---------------------------------------------------------------------------

/// `POST /query`.
pub async fn run_query(
    State(state): State<Arc<AppState>>,
    ApiJson(spec): ApiJson<QuerySpec>,
) -> Result<ApiJson<SpecAnswer>, ApiError> {
    Ok(ApiJson(state.specs.run(&spec, &state.shutdown).await?))
}

// ---------------------------------------------------------------------------
// POST /ask
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AskRequest {
    pub question: String,
    #[serde(default = "yes")]
    pub execute: bool,
}

const fn yes() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AskOutcome {
    Matched,
    Ambiguous,
    NoMatch,
    Rejected,
}

/// One reading of a question, as `/ask` shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Reading {
    pub template_id: TemplateId,
    /// Send this to `POST /query` to run the reading.
    pub spec: QuerySpec,
    pub understood_as: String,
    /// [`Assumption::text`](crate::intent::Assumption::text), in order.
    pub assumptions: Vec<String>,
    pub entities: Vec<ResolvedEntity>,
}

impl From<Matched> for Reading {
    fn from(m: Matched) -> Self {
        Self {
            template_id: m.template_id,
            spec: m.spec,
            understood_as: m.understood_as,
            assumptions: m.assumptions.iter().map(|a| a.text()).collect(),
            entities: m.entities,
        }
    }
}

/// The body of a 200 from `/ask`. Every key is always present, `null` or
/// empty when the outcome does not use it:
///
/// ```json
/// {
///   "question": "total fees in the last 1000 slots",
///   "outcome": "matched",
///   "executed": true,
///   "template_id": "total",
///   "understood_as": "…",
///   "spec": { "measures": ["fee_total_lamports"], … },
///   "assumptions": ["…"],
///   "entities": [],
///   "answer": { "columns": […], "rows": [[…]], … },
///   "candidates": [],
///   "message": null, "reason": null, "code": null, "hint": null,
///   "suggestions": []
/// }
/// ```
///
/// * `matched`: `template_id`, `understood_as`, `spec`, `assumptions` and
///   `entities` describe the one reading. `answer` is exactly what
///   `POST /query` with `spec` returns, or `null` with `executed: false`.
/// * `ambiguous`: `candidates`, each with its own `spec`; nothing is run.
/// * `no_match`: `message`, the structured `reason`, and `suggestions`.
/// * `rejected`: `message`, the `code`, and its fixed `hint` if any.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AskResponse {
    /// The question as received.
    pub question: String,
    pub outcome: AskOutcome,
    /// Whether `answer` holds the result of running `spec`.
    pub executed: bool,
    pub template_id: Option<TemplateId>,
    pub understood_as: Option<String>,
    /// Send this to `POST /query` to run the reading.
    pub spec: Option<QuerySpec>,
    /// [`Assumption::text`](crate::intent::Assumption::text), in order.
    pub assumptions: Vec<String>,
    pub entities: Vec<ResolvedEntity>,
    pub answer: Option<SpecAnswer>,
    pub candidates: Vec<Reading>,
    pub message: Option<String>,
    pub reason: Option<NoMatchReason>,
    pub code: Option<RejectCode>,
    pub hint: Option<&'static str>,
    pub suggestions: Vec<String>,
}

impl AskResponse {
    fn new(question: String, outcome: AskOutcome) -> Self {
        Self {
            question,
            outcome,
            executed: false,
            template_id: None,
            understood_as: None,
            spec: None,
            assumptions: Vec::new(),
            entities: Vec::new(),
            answer: None,
            candidates: Vec::new(),
            message: None,
            reason: None,
            code: None,
            hint: None,
            suggestions: Vec::new(),
        }
    }
}

fn invalid_question(reason: InvalidReason) -> ApiError {
    let reason = match reason {
        InvalidReason::Empty => "must not be empty".to_owned(),
        InvalidReason::TooLong { max_chars } => format!("must be at most {max_chars} characters"),
        InvalidReason::ControlCharacter { position } => format!("has a control character at character {position}"),
        InvalidReason::TooManyTokens { count, max } => format!("has {count} words; at most {max} are allowed"),
    };
    ApiError::invalid("question", reason)
}

/// `POST /ask`.
pub async fn ask(
    State(state): State<Arc<AppState>>,
    ApiJson(req): ApiJson<AskRequest>,
) -> Result<ApiJson<AskResponse>, ApiError> {
    let question = req.question;
    let response = match state.interpreter.interpret(&question) {
        Interpretation::Invalid { reason } => return Err(invalid_question(reason)),
        Interpretation::Matched(m) => {
            // The same call `POST /query` makes, so the answers are identical.
            let answer = if req.execute { Some(state.specs.run(&m.spec, &state.shutdown).await?) } else { None };
            let reading = Reading::from(*m);
            AskResponse {
                executed: answer.is_some(),
                template_id: Some(reading.template_id),
                understood_as: Some(reading.understood_as),
                spec: Some(reading.spec),
                assumptions: reading.assumptions,
                entities: reading.entities,
                answer,
                ..AskResponse::new(question, AskOutcome::Matched)
            }
        }
        Interpretation::Ambiguous { candidates } => AskResponse {
            candidates: candidates.into_iter().map(Reading::from).collect(),
            ..AskResponse::new(question, AskOutcome::Ambiguous)
        },
        Interpretation::NoMatch { reason, suggestions } => AskResponse {
            message: Some(reason.message()),
            reason: Some(reason),
            suggestions,
            ..AskResponse::new(question, AskOutcome::NoMatch)
        },
        Interpretation::Rejected { code, message } => AskResponse {
            message: Some(message),
            code: Some(code),
            hint: code.hint(),
            ..AskResponse::new(question, AskOutcome::Rejected)
        },
    };
    Ok(ApiJson(response))
}

// ---------------------------------------------------------------------------
// GET /suggest, GET /templates
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Suggestions {
    pub suggestions: Vec<String>,
}

/// `GET /suggest?prefix=&limit=`.
pub async fn suggest(State(state): State<Arc<AppState>>, mut q: StrictQuery) -> Result<ApiJson<Suggestions>, ApiError> {
    let prefix = q.take_str("prefix").unwrap_or_default();
    let limit = q.take_u64("limit", MIN_SUGGEST_LIMIT..=MAX_SUGGEST_LIMIT)?.unwrap_or(DEFAULT_SUGGEST_LIMIT);
    q.finish()?;
    let chars = prefix.chars().count();
    if chars > MAX_PREFIX_CHARS {
        return Err(ApiError::invalid("prefix", format!("must be at most {MAX_PREFIX_CHARS} characters, got {chars}")));
    }
    if prefix.chars().any(char::is_control) {
        return Err(ApiError::invalid("prefix", "must not contain control characters"));
    }
    Ok(ApiJson(Suggestions { suggestions: state.interpreter.suggest(&prefix, limit as usize) }))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Templates {
    pub templates: Vec<TemplateInfo>,
}

/// `GET /templates`.
pub async fn templates(State(state): State<Arc<AppState>>, q: StrictQuery) -> Result<ApiJson<Templates>, ApiError> {
    q.finish()?;
    Ok(ApiJson(Templates { templates: state.interpreter.templates() }))
}

// ---------------------------------------------------------------------------
// GET /catalog
// ---------------------------------------------------------------------------

/// The ids a client may put in a spec, and the limits it must respect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogView {
    /// Sorted by id.
    pub measures: Vec<MeasureView>,
    /// Sorted by id.
    pub dimensions: Vec<DimensionView>,
    /// Sorted by name.
    pub entities: Vec<EntityView>,
    pub limits: CatalogLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MeasureView {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub table: Table,
    pub unit: Unit,
    /// A dimension the spec must group by or filter to one value.
    pub requires_dimension: Option<&'static str>,
    /// Dimensions this measure can be grouped or filtered by, sorted.
    pub dimensions: Vec<&'static str>,
    pub warnings: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DimensionView {
    pub id: &'static str,
    pub label: &'static str,
    pub description: &'static str,
    pub tables: Vec<Table>,
    /// What a filter value must look like.
    pub values: String,
    pub filterable: bool,
    /// Takes a `bucket_width`.
    pub parameterised: bool,
    pub warnings: &'static [&'static str],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntityView {
    pub name: String,
    pub aliases: Vec<String>,
    pub address: String,
    pub kind: EntityKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogLimits {
    #[serde(flatten)]
    pub catalog: Limits,
    /// `limit` above this is refused.
    pub max_rows: u32,
    /// A window longer than this is refused.
    pub max_slot_span: u64,
}

/// `GET /catalog`.
pub async fn catalog(State(state): State<Arc<AppState>>, q: StrictQuery) -> Result<ApiJson<CatalogView>, ApiError> {
    q.finish()?;
    let c = &state.catalog;
    let mut measures: Vec<MeasureView> = c
        .measures()
        .iter()
        .map(|m| {
            let mut dimensions: Vec<&'static str> =
                c.dimensions().iter().filter(|d| c.compatible(m.id, d.id)).map(|d| d.id).collect();
            dimensions.sort_unstable();
            MeasureView {
                id: m.id,
                label: m.label,
                description: m.description,
                table: m.table,
                unit: m.unit,
                requires_dimension: m.requires_dimension,
                dimensions,
                warnings: m.warnings,
            }
        })
        .collect();
    measures.sort_by_key(|m| m.id);
    let mut dimensions: Vec<DimensionView> = c
        .dimensions()
        .iter()
        .map(|d| DimensionView {
            id: d.id,
            label: d.label,
            description: d.description,
            tables: match d.table {
                DimTable::Any => Table::ALL.to_vec(),
                t => t.tables(),
            },
            values: d.values.expected(),
            filterable: d.filterable,
            parameterised: d.parameterised,
            warnings: d.warnings,
        })
        .collect();
    dimensions.sort_by_key(|d| d.id);
    let mut entities: Vec<EntityView> = c
        .entities()
        .into_iter()
        .map(|e| EntityView { name: e.name.clone(), aliases: e.aliases.clone(), address: e.address.clone(), kind: e.kind })
        .collect();
    entities.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.address.cmp(&b.address)));
    let limits = state.specs.limits();
    Ok(ApiJson(CatalogView {
        measures,
        dimensions,
        entities,
        limits: CatalogLimits { catalog: c.limits(), max_rows: limits.max_rows, max_slot_span: limits.max_slot_span },
    }))
}
