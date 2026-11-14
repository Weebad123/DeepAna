//! Structured queries: where a client actually asks.
//!
//! A client sends a [`QuerySpec`]: measures, group-by dimensions, filters, a
//! window of slots, an order and a limit. Never SQL. ORACLE checks it
//! against the catalog, compiles it into exactly one safe `SELECT`, runs it
//! through LENS, and returns a [`SpecAnswer`] with the catalog's caveats
//! attached.
//!
//! | File          | Holds                                                       |
//! |---------------|-------------------------------------------------------------|
//! | `mod.rs`      | [`QuerySpec`], [`SpecRunner`], [`SpecAnswer`], [`SpecError`], windows |
//! | [`guard`]     | [`validate`](guard::validate): spec → [`ValidSpec`](guard::ValidSpec), pure |
//! | [`compile`]   | [`compile`](compile::compile): valid spec + window → [`Compiled`](compile::Compiled), pure |
//!
//! # Decisions
//!
//! * **One table per query**, no joins, no `HAVING`, no client-written
//!   expression and no free text. Unknown request fields are refused.
//! * **The window is mandatory and in slots**: the last `N` slots up to the
//!   snapshot, or an explicit inclusive range. "Last hour" is not
//!   translated into slots, and raw token amounts are not converted to UI
//!   units: both are deferred, so nothing in an answer is estimated.
//! * **The snapshot is `W`**, as in `profile/`, `quality/` and `digest/`:
//!   the smaller of `MAX(slot)` of `solana.slots` and of
//!   `solana.transactions`, the newest slot both cover. `as_of_slot` pins it
//!   and must not be above it.
//! * **Bucket grouping snaps the window inward** to whole buckets, so no
//!   bucket is partly outside. The answer reports the window before and
//!   after the snap.
//! * **A measure that requires a dimension excludes rows where that
//!   dimension's column is null**, and is refused unless the dimension is
//!   grouped by or filtered to one value.
//! * **Filters compile against the raw column**, so DataFusion can prune
//!   files with Parquet statistics.
//! * **Deterministic output**: every ordering ends with all group-by
//!   dimensions ascending, and every sort key is `NULLS LAST`.
//! * **`LIMIT` is requested as `limit + 1`**; the extra row, if it comes
//!   back, is dropped and sets [`SpecAnswer::has_more`].
//! * **Rows are arrays**, in the order of a separate `columns` list.
//! * **A result cut off by LENS's own row cap is flagged**
//!   ([`SpecAnswer::truncated`]), never hidden and never an error.
//! * **Warnings are catalog text**, never composed at run time.
//!
//! # The flow of [`SpecRunner::run`]
//!
//! 1. **Validate** ([`guard::validate`]). Nothing reaches LENS before that.
//! 2. Take a permit: at most [`MAX_CONCURRENT_SPECS`] specs run at once.
//! 3. **Snapshot**: [`MAX_SLOTS_SQL`] then [`MAX_TX_SLOT_SQL`]; `W` is the
//!    smaller, or `as_of_slot` if given and not above it.
//! 4. **Resolve the window** ([`resolve_window`]): `last_slots` ends at
//!    `W` (clipped at slot 0); a range must end at or below `W`. Then snap.
//! 5. **Compile and execute**: one statement, one LENS query.
//! 6. **Shape**: check every row has exactly the compiled columns with the
//!    right types, drop the `limit + 1`-th row, attach the warnings.
//!
//! Every successful run issues exactly [`SPEC_QUERIES`] (3) LENS queries.
//! The whole run races the shutdown token: dropping it at any `await` is
//! safe, since nothing is written anywhere and the permit is released.
//!
//! # LENS errors
//!
//! | LENS result                                          | `SpecError`   |
//! |------------------------------------------------------|---------------|
//! | `Unavailable`                                        | `Unavailable` |
//! | `Timeout`                                            | `Timeout`     |
//! | `BadQuery` naming a missing table                    | `NotReady`    |
//! | any other `BadQuery`, `Protocol`, a missing, renamed or mistyped column | `Internal`: the compiler produced something LENS rejects, a bug |
//! | `truncated: true`                                    | the answer, flagged |

pub mod compile;
pub mod guard;

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use forge::lens_client::{LensApi, LensCallError, LensResponse};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::catalog::Catalog;
use crate::config::OracleConfig;
pub use crate::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL};

use compile::{ColumnInfo, Compiled};
use guard::{SpecLimits, ValidSpec};

/// Specs running at once; further calls wait for a permit.
pub const MAX_CONCURRENT_SPECS: usize = 4;
/// LENS queries one successful run issues: two for the snapshot, one for
/// the answer.
pub const SPEC_QUERIES: u32 = 3;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// One structured query, as a client sends it.
///
/// ```json
/// {
///   "measures": ["transfer_count", "transfer_volume_raw"],
///   "group_by": [{ "dimension": "mint" }],
///   "filters": [{ "dimension": "token_program", "op": "in", "values": ["Tokenkeg…", "Tokenz…"] }],
///   "window": { "last_slots": 20000 },
///   "order_by": [{ "target": "transfer_count", "direction": "desc" }],
///   "limit": 10,
///   "as_of_slot": null
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySpec {
    pub measures: Vec<String>,
    #[serde(default)]
    pub group_by: Vec<GroupBy>,
    #[serde(default)]
    pub filters: Vec<Filter>,
    /// Mandatory: there is no default window.
    pub window: Window,
    #[serde(default)]
    pub order_by: Vec<Order>,
    /// Rows to return; `ORACLE_MAX_ROWS` if absent.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Pin the snapshot to this slot for a reproducible answer.
    #[serde(default)]
    pub as_of_slot: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupBy {
    pub dimension: String,
    /// `slot_bucket`'s width in slots; nothing else takes one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_width: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Filter {
    pub dimension: String,
    pub op: FilterOp,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    /// Exactly one value.
    Eq,
    /// Any of one or more values.
    In,
}

/// The slots a query reads, both ends inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawWindow", into = "RawWindow")]
pub enum Window {
    /// `{"last_slots": N}`: the `N` slots ending at the snapshot `W`.
    LastSlots(u64),
    /// `{"from_slot": A, "to_slot": B}`: slots `A..=B`, with `B <= W`.
    Range { from_slot: u64, to_slot: u64 },
}

/// The wire form of [`Window`]: exactly one of the two shapes.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawWindow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_slots: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    from_slot: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    to_slot: Option<u64>,
}

impl TryFrom<RawWindow> for Window {
    type Error = String;

    fn try_from(raw: RawWindow) -> Result<Self, Self::Error> {
        match (raw.last_slots, raw.from_slot, raw.to_slot) {
            (Some(n), None, None) => Ok(Self::LastSlots(n)),
            (None, Some(from_slot), Some(to_slot)) => Ok(Self::Range { from_slot, to_slot }),
            _ => Err("window must be {\"last_slots\": N} or {\"from_slot\": A, \"to_slot\": B}".to_owned()),
        }
    }
}

impl From<Window> for RawWindow {
    fn from(w: Window) -> Self {
        match w {
            Window::LastSlots(n) => Self { last_slots: Some(n), from_slot: None, to_slot: None },
            Window::Range { from_slot, to_slot } => {
                Self { last_slots: None, from_slot: Some(from_slot), to_slot: Some(to_slot) }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Order {
    /// A requested measure, or a group-by dimension.
    pub target: String,
    pub direction: Direction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

/// A resolved window: slots `from_slot..=to_slot`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SlotWindow {
    pub from_slot: u64,
    pub to_slot: u64,
    /// `to_slot - from_slot + 1`.
    pub slots: u64,
}

impl SlotWindow {
    /// # Panics
    ///
    /// If `from_slot > to_slot`.
    pub fn new(from_slot: u64, to_slot: u64) -> Self {
        assert!(from_slot <= to_slot, "window {from_slot}..={to_slot} is reversed");
        Self { from_slot, to_slot, slots: to_slot - from_slot + 1 }
    }

    /// Shrunk to the whole buckets of `width` inside it, bucket `k` being
    /// slots `k * width ..= k * width + width - 1`. `None` if it holds none.
    pub fn snap(self, width: u64) -> Option<Self> {
        if width == 0 {
            return None;
        }
        let from = self.from_slot.div_ceil(width).checked_mul(width)?;
        // One past the last whole bucket's end.
        let end = (self.to_slot.checked_add(1)? / width).checked_mul(width)?;
        (from < end).then(|| Self::new(from, end - 1))
    }
}

/// `window` against the snapshot `w`, before any snap. A range ending above
/// `w` is refused: its newest slots are not fully visible yet.
pub fn resolve_window(window: Window, w: u64) -> Result<SlotWindow, SpecError> {
    match window {
        Window::LastSlots(n) => Ok(SlotWindow::new(w.saturating_sub(n.saturating_sub(1)), w)),
        Window::Range { from_slot, to_slot } => {
            if to_slot > w {
                return Err(SpecError::invalid(
                    "window.to_slot",
                    format!("{to_slot} is above {w}, the newest fully visible slot"),
                ));
            }
            Ok(SlotWindow::new(from_slot, to_slot))
        }
    }
}

// ---------------------------------------------------------------------------
// Answer
// ---------------------------------------------------------------------------

/// The answer to one spec.
///
/// Every field is a pure function of the spec and the data at or below
/// `as_of_slot`, except `duration_ms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SpecAnswer {
    /// Dimensions first, then measures, each in the order requested.
    pub columns: Vec<ColumnInfo>,
    /// One array per row, in `columns` order.
    pub rows: Vec<Vec<Value>>,
    /// More rows exist than `rows` holds: either beyond `limit`, or cut by
    /// LENS (`truncated`).
    pub has_more: bool,
    /// LENS's own row cap cut the result short of `limit`: `rows` is not
    /// every row the spec asked for.
    pub truncated: bool,
    pub limit: u32,
    /// The snapshot slot `W`; pass it back as `as_of_slot` to reproduce.
    pub as_of_slot: u64,
    /// The window resolved against `W`, before snapping to buckets.
    pub requested_window: SlotWindow,
    /// The window the query read.
    pub window: SlotWindow,
    /// Catalog warnings of the measures, then the group-by dimensions.
    pub warnings: Vec<String>,
    /// The statement LENS ran.
    pub sql: String,
    pub queries_run: u32,
    pub duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why no answer was produced.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum SpecError {
    /// The spec is wrong. Nothing was sent to LENS, except when the window
    /// or `as_of_slot` is above the snapshot, which needs the snapshot to
    /// judge.
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },
    /// LENS has no data, or not the table, yet.
    #[error("LENS is not ready: {0}")]
    NotReady(String),
    /// A transient LENS failure; the same call may succeed later.
    #[error("LENS is unavailable: {0}")]
    Unavailable(String),
    #[error("LENS timed out: {0}")]
    Timeout(String),
    /// The shutdown token fired.
    #[error("the query was cancelled")]
    Cancelled,
    /// A statement of ours is wrong, or LENS answered in a shape we do not
    /// expect: a bug.
    #[error("internal error: {0}")]
    Internal(String),
}

impl SpecError {
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid { field: field.into(), reason: reason.into() }
    }

    /// Stable `snake_case` name of the variant.
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Invalid { .. } => "invalid",
            Self::NotReady(_) => "not_ready",
            Self::Unavailable(_) => "unavailable",
            Self::Timeout(_) => "timeout",
            Self::Cancelled => "cancelled",
            Self::Internal(_) => "internal",
        }
    }

    /// The HTTP status the API answers with.
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::NotReady(_) | Self::Unavailable(_) | Self::Cancelled => 503,
            Self::Timeout(_) => 504,
            Self::Internal(_) => 500,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::NotReady(_) | Self::Unavailable(_) | Self::Timeout(_))
    }
}

/// Whether a `BadQuery` message is DataFusion's "table '…' not found".
fn is_missing_table(message: &str) -> bool {
    message.contains("table '") && message.contains("' not found")
}

/// See the [module docs](self#lens-errors).
pub fn lens_error(err: LensCallError) -> SpecError {
    match &err {
        LensCallError::BadQuery { message } if is_missing_table(message) => SpecError::NotReady(message.clone()),
        LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } => {
            SpecError::Internal(format!("LENS refused a compiled statement: {err}"))
        }
        LensCallError::Timeout { message } => SpecError::Timeout(message.clone()),
        LensCallError::Unavailable { message } => SpecError::Unavailable(message.clone()),
    }
}

// ---------------------------------------------------------------------------
// Shaping
// ---------------------------------------------------------------------------

/// The rows of `response` as arrays in `columns` order, and whether more
/// rows exist than `limit`. Every row must carry exactly the compiled
/// columns, each holding what its [`ColumnInfo`] accepts.
pub fn shape(columns: &[ColumnInfo], limit: u32, response: &LensResponse) -> Result<(Vec<Vec<Value>>, bool), SpecError> {
    let asked = u64::from(limit) + 1;
    let n = response.rows.len() as u64;
    if n > asked {
        return Err(SpecError::Internal(format!("LENS returned {n} rows for LIMIT {asked}")));
    }
    let grouped = columns.iter().any(|c| c.role == compile::Role::Dimension);
    if !grouped && n != 1 && !(n == 0 && response.truncated) {
        return Err(SpecError::Internal(format!("an ungrouped aggregate returned {n} rows, expected 1")));
    }
    let mut rows = Vec::with_capacity(response.rows.len());
    for (i, row) in response.rows.iter().enumerate() {
        rows.push(shape_row(columns, row).map_err(|e| SpecError::Internal(format!("row {i}: {e}")))?);
    }
    let over = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    Ok((rows, over || response.truncated))
}

fn shape_row(columns: &[ColumnInfo], row: &Map<String, Value>) -> Result<Vec<Value>, String> {
    if let Some(extra) = row.keys().find(|k| !columns.iter().any(|c| c.name == k.as_str())) {
        return Err(format!("unexpected column {extra:?}"));
    }
    columns
        .iter()
        .map(|c| {
            let v = row.get(c.name).ok_or_else(|| format!("column {} is missing", c.name))?;
            if c.accepts(v) { Ok(v.clone()) } else { Err(format!("column {} = {v} is not {:?}", c.name, c.value_type)) }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Runs [`QuerySpec`]s. Cheap to clone; clones share the concurrency limit.
#[derive(Clone)]
pub struct SpecRunner {
    catalog: Arc<Catalog>,
    lens: Arc<dyn LensApi>,
    limits: SpecLimits,
    permits: Arc<Semaphore>,
}

impl fmt::Debug for SpecRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpecRunner")
            .field("limits", &self.limits)
            .field("available_permits", &self.permits.available_permits())
            .finish_non_exhaustive()
    }
}

/// One run's LENS queries, counted.
struct Session<'a> {
    lens: &'a dyn LensApi,
    queries: u32,
}

impl Session<'_> {
    async fn query(&mut self, sql: &str) -> Result<LensResponse, SpecError> {
        self.queries += 1;
        self.lens.query(sql).await.map_err(lens_error)
    }

    async fn max_slot(&mut self, sql: &str, table: &str) -> Result<u64, SpecError> {
        let response = self.query(sql).await?;
        let bad = |what: &str| SpecError::Internal(format!("snapshot query on {table} {what}"));
        if response.truncated {
            return Err(bad("was truncated"));
        }
        let [row] = response.rows.as_slice() else {
            return Err(bad(&format!("returned {} rows, expected 1", response.rows.len())));
        };
        match row.get("max_slot") {
            Some(Value::Null) => Err(SpecError::NotReady(format!("{table} has no rows yet"))),
            Some(v) => v.as_u64().ok_or_else(|| bad(&format!("returned max_slot = {v}"))),
            None => Err(bad("returned no max_slot column")),
        }
    }
}

impl SpecRunner {
    /// `config` supplies `max_rows` and `max_slot_span`.
    pub fn new(catalog: Arc<Catalog>, lens: Arc<dyn LensApi>, config: &OracleConfig) -> Self {
        Self::with_limits(catalog, lens, SpecLimits::from_config(config))
    }

    /// With explicit limits, for tests and hosts without an [`OracleConfig`].
    pub fn with_limits(catalog: Arc<Catalog>, lens: Arc<dyn LensApi>, limits: SpecLimits) -> Self {
        Self { catalog, lens, limits, permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SPECS)) }
    }

    pub const fn limits(&self) -> SpecLimits {
        self.limits
    }

    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }

    /// Validate `spec`, then answer it. See the [module docs](self).
    pub async fn run(&self, spec: &QuerySpec, shutdown: &CancellationToken) -> Result<SpecAnswer, SpecError> {
        let valid = guard::validate(spec, &self.catalog, &self.limits)?;
        tokio::select! {
            biased;
            () = shutdown.cancelled() => Err(SpecError::Cancelled),
            result = self.execute(valid) => result,
        }
    }

    async fn execute(&self, valid: ValidSpec) -> Result<SpecAnswer, SpecError> {
        let _permit =
            self.permits.acquire().await.map_err(|_| SpecError::Internal("the permit pool is closed".to_owned()))?;
        let started = Instant::now();
        let mut s = Session { lens: self.lens.as_ref(), queries: 0 };

        // Snapshot.
        let slots = s.max_slot(MAX_SLOTS_SQL, "solana.slots").await?;
        let txs = s.max_slot(MAX_TX_SLOT_SQL, "solana.transactions").await?;
        let visible = slots.min(txs);
        let w = match valid.as_of_slot {
            Some(pin) if pin > visible => {
                return Err(SpecError::invalid(
                    "as_of_slot",
                    format!("{pin} is above {visible}, the newest fully visible slot"),
                ));
            }
            Some(pin) => pin,
            None => visible,
        };
        if w > crate::catalog::MAX_SLOT {
            return Err(SpecError::Internal(format!("snapshot slot {w} does not fit in i64")));
        }

        // Window.
        let requested = resolve_window(valid.window, w)?;
        let window = match valid.bucket_width() {
            None => requested,
            Some(width) => requested.snap(width).ok_or_else(|| {
                SpecError::NotReady(format!(
                    "the window {}..={} holds no whole bucket of {width} slots yet",
                    requested.from_slot, requested.to_slot
                ))
            })?,
        };

        // Compile, execute, shape.
        let Compiled { sql, columns, warnings, window } = compile::compile(&valid, &self.catalog, window)?;
        let response = s.query(&sql).await?;
        let (rows, has_more) = shape(&columns, valid.limit, &response)?;
        debug_assert_eq!(s.queries, SPEC_QUERIES);

        Ok(SpecAnswer {
            columns,
            rows,
            has_more,
            truncated: response.truncated,
            limit: valid.limit,
            as_of_slot: w,
            requested_window: requested,
            window,
            warnings,
            sql,
            queries_run: s.queries,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapping_keeps_whole_buckets_only() {
        let w = |a, b| SlotWindow::new(a, b);
        assert_eq!(w(0, 299).snap(150), Some(w(0, 299)));
        assert_eq!(w(1, 300).snap(150), Some(w(150, 299)));
        assert_eq!(w(5, 14).snap(10), None);
        assert_eq!(w(10, 19).snap(10), Some(w(10, 19)));
        assert_eq!(w(7, 7).snap(1), Some(w(7, 7)));
        assert_eq!(w(0, 0).snap(2), None);
        assert_eq!(w(0, 5).snap(0), None);
        let max = crate::catalog::MAX_SLOT;
        // Near the top of the range: no overflow, and the partial last bucket is dropped.
        assert_eq!(w(max - 9, max).snap(10), None);
        assert_eq!(w(max - 17, max).snap(10), Some(w(max - 17, max - 8)));
        // A window of 2w - 1 slots holds a whole bucket however it is aligned.
        for width in 1..=12u64 {
            for from in 0..3 * width {
                assert!(w(from, from + 2 * width - 2).snap(width).is_some(), "{width} {from}");
            }
            // And 2w - 2 is not always enough: starting one past a boundary.
            if width >= 2 {
                assert_eq!(w(1, 2 * width - 2).snap(width), None, "{width}");
            }
        }
    }

    #[test]
    fn windows_resolve_against_the_snapshot() {
        assert_eq!(resolve_window(Window::LastSlots(100), 1_000).unwrap(), SlotWindow::new(901, 1_000));
        assert_eq!(resolve_window(Window::LastSlots(1), 1_000).unwrap(), SlotWindow::new(1_000, 1_000));
        assert_eq!(resolve_window(Window::LastSlots(5_000), 10).unwrap(), SlotWindow::new(0, 10));
        let range = Window::Range { from_slot: 10, to_slot: 20 };
        assert_eq!(resolve_window(range, 20).unwrap(), SlotWindow::new(10, 20));
        match resolve_window(range, 19) {
            Err(SpecError::Invalid { field, .. }) => assert_eq!(field, "window.to_slot"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn missing_tables_are_told_apart_from_other_rejections() {
        let bad = |m: &str| lens_error(LensCallError::BadQuery { message: m.to_owned() }).kind();
        assert_eq!(bad("table 'datafusion.solana.token_transfers' not found"), "not_ready");
        assert_eq!(bad("Schema error: No field named nope. Valid fields are solana.slots.slot."), "internal");
        assert_eq!(bad("sql parser error: Expected: end of statement"), "internal");
        assert_eq!(bad("field not found"), "internal");
    }
}
