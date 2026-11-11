//! Entity profiles: a fixed report about one mint, program or token account.
//!
//! A [`Profiler`] holds no state, schedules nothing and never touches the
//! store. It takes an identifier from outside, so validation comes first and
//! everything else is built around it.
//!
//! # The flow of [`Profiler::profile`]
//!
//! 1. **Validate** the request (see below). Nothing reaches LENS before that.
//! 2. Take a permit: at most [`MAX_CONCURRENT_PROFILES`] profiles run at
//!    once. A profile is the heaviest read ORACLE makes.
//! 3. **Snapshot.** [`MAX_SLOTS_SQL`] then [`MAX_TX_SLOT_SQL`]; the snapshot
//!    slot `W` is the smaller of the two, the newest slot both tables cover.
//!    `as_of_slot`, if given, replaces `W`, and is rejected if it is above
//!    it, so a profile never includes a slot that is not fully visible.
//! 4. **Window.** `to = W`, `from = W - (window_slots - 1)` (saturating at
//!    0), both inclusive. The series splits it into at most
//!    [`SERIES_BUCKETS`] buckets of `ceil(slots / SERIES_BUCKETS)` slots.
//! 5. **Queries**, one after another, never concurrently, every one bounded
//!    by `slot >= from AND slot <= to`. The number is fixed per kind, see
//!    [`Kind::queries`]. All of them are run even if the entity turns out to
//!    have no activity, so the cost of a profile does not depend on the data.
//! 6. **Assemble** the report and cross-check its sections: the series must
//!    add up to the summary and no top entry may exceed it. Every section
//!    reads the same `slot <= W` rows, so a disagreement means rows under
//!    the snapshot changed mid-profile, which is reported as `Unavailable`
//!    (retry) instead of being served.
//!
//! The whole call, including the wait for a permit, has a deadline of
//! [`PROFILE_DEADLINE_SECS`]. Dropping the future at any `await` is safe: no
//! state is written anywhere and the permit is released on drop.
//!
//! Any LENS failure fails the whole call. A partial profile would present
//! half a picture as a whole one.
//!
//! # Queries per kind
//!
//! Failed transactions are excluded from every count by joining to
//! `solana.transactions` on `(slot, tx_signature)` with `status = 'success'`;
//! the `coverage` section says how much that excluded.
//!
//! | Kind            | # | Query                                                    |
//! |-----------------|---|----------------------------------------------------------|
//! | `mint`          | 1 | summary: transfers, first/last slot, distinct sources and destinations, volume, decimals |
//! |                 | 2 | top sources (token accounts)                             |
//! |                 | 3 | top destinations                                         |
//! |                 | 4 | series                                                   |
//! |                 | 5 | coverage: transfers in failed tx; transfers without a mint |
//! | `token_account` | 1 | summary: in/out counts and volumes, mint, owner          |
//! |                 | 2 | top counterparties                                       |
//! |                 | 3 | series                                                   |
//! |                 | 4 | coverage: transfers in failed tx                         |
//! | `program`       | 1 | summary, failure share and coverage in one statement     |
//! |                 | 2 | top instruction types                                    |
//! |                 | 3 | series                                                   |
//!
//! Plus the two snapshot queries for every kind. Every top-N orders by
//! `n DESC, subject ASC`, so ties always break the same way and the same
//! inputs give the same report.
//!
//! # Labels that matter
//!
//! * A mint's numbers are a **lower bound**: a plain `Transfer` whose token
//!   account PRISM could not resolve has no mint and cannot be attributed.
//! * A program's **failure share** is "transactions that invoked this
//!   program and failed". The failure may come from another instruction of
//!   the same transaction, so it is not the program's failure rate.
//! * **Decode rate** only means something for programs PRISM has a decoder
//!   for.
//! * **Volumes** are decimal strings, never floats: a sum of `u64` amounts
//!   overflows `u64` and loses precision in `f64`. LENS sums them as
//!   `DECIMAL(38,0)` and returns text.
//!
//! # Input safety
//!
//! The `id` must be base58, 32 to 44 characters, and decode to exactly 32
//! bytes. It is then rendered with FORGE's [`quote_sql_string`], the same
//! routine saved queries use: base58 has no quote, but validation is not
//! the only defence. Every other literal in the SQL is an integer this
//! module computes, or a constant written here.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use forge::lens_client::{LensApi, LensCallError, LensResponse};
use forge::quote_sql_string;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::sync::Semaphore;

pub use crate::detect::{MAX_SLOTS_SQL, MAX_TX_SLOT_SQL};

/// `window_slots` when the request has none: about 5.5 hours.
pub const DEFAULT_WINDOW_SLOTS: u64 = 50_000;
pub const MIN_WINDOW_SLOTS: u64 = 1;
/// About 9 days at 400 ms per slot.
pub const MAX_WINDOW_SLOTS: u64 = 2_000_000;
/// Most buckets in the series.
pub const SERIES_BUCKETS: u64 = 24;
/// Entries in each top-N list.
pub const TOP_N: u64 = 10;
/// Profiles running at once; further calls wait for a permit.
pub const MAX_CONCURRENT_PROFILES: usize = 4;
/// Deadline of one whole profile, the wait for a permit included.
pub const PROFILE_DEADLINE_SECS: u64 = 120;
/// Shortest base58 text of a 32-byte key.
pub const MIN_ID_CHARS: usize = 32;
/// Longest base58 text of a 32-byte key.
pub const MAX_ID_CHARS: usize = 44;
/// Bytes an `id` must decode to.
pub const ID_BYTES: usize = 32;
/// Queries every profile runs to fix its snapshot.
pub const SNAPSHOT_QUERIES: u32 = 2;

const BASE58_ALPHABET: &str = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub const WARN_MINT_LOWER_BOUND: &str = "transfers without a mint cannot be attributed, so counts are a lower bound";
pub const WARN_MIXED_DECIMALS: &str = "transfers disagree on decimals, so volume_ui is omitted";
pub const WARN_NO_DECIMALS: &str = "decimals are unknown, so volume_ui is omitted";
pub const WARN_MULTIPLE_MINTS: &str =
    "transfers of more than one mint touch this account, so volumes add unlike units and volume_ui is omitted";
pub const WARN_FAILURE_SHARE: &str = "failure_share counts transactions that invoked this program and failed; \
     the failure may come from another instruction, so it is not the program's failure rate";
pub const WARN_DECODE_RATE: &str = "decode_rate is meaningful only for programs PRISM has a decoder for";
pub const WARN_NOTHING_DECODED: &str = "no instruction was decoded: PRISM most likely has no decoder for this program";
pub const WARN_WINDOW_CLIPPED: &str = "the window reaches before slot 0 and was clipped";
pub const WARN_NOT_FOUND: &str = "no successful activity in the window";

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// What is being profiled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Mint,
    Program,
    TokenAccount,
}

impl Kind {
    pub const ALL: [Kind; 3] = [Kind::Mint, Kind::Program, Kind::TokenAccount];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Mint => "mint",
            Self::Program => "program",
            Self::TokenAccount => "token_account",
        }
    }

    /// LENS queries one profile of this kind runs, snapshot included.
    pub const fn queries(self) -> u32 {
        SNAPSHOT_QUERIES
            + match self {
                Self::Mint => 5,
                Self::TokenAccount => 4,
                Self::Program => 3,
            }
    }
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Kind {
    type Err = ProfileError;

    /// For callers holding the kind as text (an HTTP path segment).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|k| k.as_str() == s)
            .ok_or_else(|| ProfileError::invalid("kind", "must be one of mint, program, token_account"))
    }
}

/// One profile request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileRequest {
    pub kind: Kind,
    pub id: String,
    /// Slots in the window, [`DEFAULT_WINDOW_SLOTS`] if `None`.
    #[serde(default)]
    pub window_slots: Option<u64>,
    /// Pin the snapshot to this slot for a reproducible report.
    #[serde(default)]
    pub as_of_slot: Option<u64>,
}

impl ProfileRequest {
    pub fn new(kind: Kind, id: impl Into<String>) -> Self {
        Self { kind, id: id.into(), window_slots: None, as_of_slot: None }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Why no profile was produced.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ProfileError {
    /// The request is wrong. Nothing was sent to LENS (except for an
    /// `as_of_slot` above the snapshot, which needs the snapshot to judge).
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },
    /// LENS has no data, or not the table, yet.
    #[error("LENS is not ready: {0}")]
    NotReady(String),
    /// A transient LENS failure; the same call may succeed later.
    #[error("LENS is unavailable: {0}")]
    Unavailable(String),
    /// LENS, or the profile deadline, ran out of time.
    #[error("profile timed out: {0}")]
    Timeout(String),
    /// LENS cut a result off, so the numbers cannot be trusted.
    #[error("LENS truncated a result")]
    Truncated,
    /// A query of ours is wrong, or LENS answered in a shape we do not
    /// expect: a bug.
    #[error("{0}")]
    Permanent(String),
}

impl ProfileError {
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
            Self::Truncated => "truncated",
            Self::Permanent(_) => "permanent",
        }
    }

    /// The HTTP status the API answers with.
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::NotReady(_) | Self::Unavailable(_) => 503,
            Self::Timeout(_) => 504,
            Self::Truncated | Self::Permanent(_) => 500,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::NotReady(_) | Self::Unavailable(_) | Self::Timeout(_))
    }
}

/// | `LensCallError`                                  | `ProfileError` |
/// |--------------------------------------------------|----------------|
/// | `BadQuery` naming a missing table ("not found")   | `NotReady`     |
/// | other `BadQuery`, `Protocol`                     | `Permanent`    |
/// | `Timeout`                                        | `Timeout`, advising a narrower window |
/// | `Unavailable`                                    | `Unavailable`  |
fn lens_error(err: LensCallError, window_slots: u64) -> ProfileError {
    match &err {
        LensCallError::BadQuery { message } if message.contains("not found") => ProfileError::NotReady(err.to_string()),
        LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } => ProfileError::Permanent(err.to_string()),
        LensCallError::Timeout { .. } => ProfileError::Timeout(format!(
            "{err}; busy entities may need a narrower window than window_slots = {window_slots}"
        )),
        LensCallError::Unavailable { .. } => ProfileError::Unavailable(err.to_string()),
    }
}

fn protocol(message: impl fmt::Display) -> ProfileError {
    ProfileError::Permanent(format!("unexpected LENS result: {message}"))
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

/// Check `id` is the base58 text of a 32-byte key.
///
/// The reason never quotes the input: it may be long or hostile.
pub fn validate_id(id: &str) -> Result<(), ProfileError> {
    let invalid = |reason: String| ProfileError::invalid("id", reason);
    if let Some((at, _)) = id.char_indices().find(|(_, c)| !BASE58_ALPHABET.contains(*c)) {
        return Err(invalid(format!("character at byte {at} is not in the base58 alphabet")));
    }
    // All ASCII from here, so bytes are characters.
    if !(MIN_ID_CHARS..=MAX_ID_CHARS).contains(&id.len()) {
        return Err(invalid(format!(
            "is {} characters, expected {MIN_ID_CHARS} to {MAX_ID_CHARS}",
            id.len()
        )));
    }
    let bytes = bs58::decode(id).into_vec().map_err(|e| invalid(format!("is not valid base58: {e}")))?;
    if bytes.len() != ID_BYTES {
        return Err(invalid(format!("decodes to {} bytes, expected {ID_BYTES}", bytes.len())));
    }
    Ok(())
}

/// A request that passed validation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Valid {
    kind: Kind,
    id: String,
    window_slots: u64,
    as_of_slot: Option<u64>,
}

fn validate(req: &ProfileRequest) -> Result<Valid, ProfileError> {
    validate_id(&req.id)?;
    let window_slots = req.window_slots.unwrap_or(DEFAULT_WINDOW_SLOTS);
    if !(MIN_WINDOW_SLOTS..=MAX_WINDOW_SLOTS).contains(&window_slots) {
        return Err(ProfileError::invalid(
            "window_slots",
            format!("{window_slots} is outside {MIN_WINDOW_SLOTS}..={MAX_WINDOW_SLOTS}"),
        ));
    }
    if let Some(slot) = req.as_of_slot
        && i64::try_from(slot).is_err()
    {
        return Err(ProfileError::invalid("as_of_slot", format!("{slot} does not fit in i64")));
    }
    Ok(Valid { kind: req.kind, id: req.id.clone(), window_slots, as_of_slot: req.as_of_slot })
}

// ---------------------------------------------------------------------------
// Window
// ---------------------------------------------------------------------------

/// The slots a profile covers, both ends inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    pub from_slot: u64,
    pub to_slot: u64,
    /// `to_slot - from_slot + 1`; fewer than asked for if clipped at 0.
    pub slots: u64,
}

impl Window {
    /// The `window_slots` slots ending at `to`.
    pub fn ending_at(to: u64, window_slots: u64) -> Self {
        let from = to.saturating_sub(window_slots.saturating_sub(1));
        Self { from_slot: from, to_slot: to, slots: to - from + 1 }
    }

    /// Slots per series bucket: `ceil(slots / SERIES_BUCKETS)`.
    pub const fn bucket_width(&self) -> u64 {
        self.slots.div_ceil(SERIES_BUCKETS)
    }

    /// Buckets in the series; at most [`SERIES_BUCKETS`].
    pub const fn buckets(&self) -> u64 {
        self.slots.div_ceil(self.bucket_width())
    }

    /// The empty series: every bucket, count 0.
    fn empty_series(&self) -> Vec<SeriesPoint> {
        let width = self.bucket_width();
        (0..self.buckets())
            .map(|b| {
                let from_slot = self.from_slot + b * width;
                SeriesPoint { from_slot, to_slot: (from_slot + width - 1).min(self.to_slot), count: 0 }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// The report.
///
/// Every field is a pure function of the request and the data at or below
/// `as_of_slot`, except `duration_ms`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Profile {
    pub kind: Kind,
    pub id: String,
    /// Whether the entity has any successful activity in the window.
    pub found: bool,
    /// The snapshot slot `W`; pass it back as `as_of_slot` to reproduce.
    pub as_of_slot: u64,
    pub window: Window,
    pub summary: Summary,
    /// Per kind: `top_sources` and `top_destinations` for a mint,
    /// `top_counterparties` for a token account, `top_instruction_types` for
    /// a program.
    #[serde(flatten)]
    pub top: BTreeMap<&'static str, Vec<TopEntry>>,
    pub series: Vec<SeriesPoint>,
    pub coverage: BTreeMap<&'static str, u64>,
    pub warnings: Vec<String>,
    pub queries_run: u32,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Summary {
    Mint(MintSummary),
    TokenAccount(TokenAccountSummary),
    Program(ProgramSummary),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MintSummary {
    pub transfers: u64,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
    pub distinct_sources: u64,
    pub distinct_destinations: u64,
    pub volume_raw: String,
    pub volume_ui: Option<String>,
    pub decimals: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TokenAccountSummary {
    pub transfers: u64,
    pub transfers_out: u64,
    pub transfers_in: u64,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
    /// The mint, if exactly one mint was seen.
    pub mint: Option<String>,
    pub distinct_mints: u64,
    /// The owner, if exactly one owner was seen.
    pub owner: Option<String>,
    pub distinct_owners: u64,
    pub volume_out_raw: String,
    pub volume_in_raw: String,
    pub volume_out_ui: Option<String>,
    pub volume_in_ui: Option<String>,
    pub decimals: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProgramSummary {
    /// Instructions, inner ones included, in successful transactions.
    pub invocations: u64,
    pub inner_invocations: u64,
    /// Successful transactions that invoked the program.
    pub transactions: u64,
    pub first_seen_slot: Option<u64>,
    pub last_seen_slot: Option<u64>,
    pub decoded_invocations: u64,
    /// `decoded_invocations / invocations`; see [`WARN_DECODE_RATE`].
    pub decode_rate: Option<f64>,
    /// Transactions that invoked the program, failed ones included.
    pub transactions_including_failed: u64,
    pub failed_transactions: u64,
    /// `failed_transactions / transactions_including_failed`; see
    /// [`WARN_FAILURE_SHARE`].
    pub failure_share: Option<f64>,
}

/// One row of a top-N list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TopEntry {
    /// `None` only for undecoded instructions in `top_instruction_types`.
    pub key: Option<String>,
    pub count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume_raw: Option<String>,
}

/// One series bucket, both ends inclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SeriesPoint {
    pub from_slot: u64,
    pub to_slot: u64,
    pub count: u64,
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

/// Statement builders. Each takes the quoted id literal and integers only.
pub mod sql {
    use super::{TOP_N, Window};

    /// `SUM(expr)` of `u64` amounts as exact decimal text.
    fn volume(expr: &str) -> String {
        format!("CAST(SUM(CAST({expr} AS DECIMAL(38,0))) AS VARCHAR)")
    }

    /// Token transfers joined to their transaction, both bounded by `w`.
    fn transfers_from(w: &Window) -> String {
        let (from, to) = (w.from_slot, w.to_slot);
        format!(
            "FROM solana.token_transfers x JOIN solana.transactions t \
               ON x.slot = t.slot AND x.tx_signature = t.tx_signature \
             WHERE x.slot >= {from} AND x.slot <= {to} AND t.slot >= {from} AND t.slot <= {to}"
        )
    }

    /// Instructions joined to their transaction, both bounded by `w`.
    fn instructions_from(w: &Window) -> String {
        let (from, to) = (w.from_slot, w.to_slot);
        format!(
            "FROM solana.instructions i JOIN solana.transactions t \
               ON i.slot = t.slot AND i.tx_signature = t.tx_signature \
             WHERE i.slot >= {from} AND i.slot <= {to} AND t.slot >= {from} AND t.slot <= {to}"
        )
    }

    fn bucket(column: &str, w: &Window) -> String {
        format!("(CAST({column} AS BIGINT) - {}) / {}", w.from_slot, w.bucket_width())
    }

    fn series(from: String, slot: &str, w: &Window) -> String {
        let bucket = bucket(slot, w);
        format!("SELECT {bucket} AS bucket, COUNT(*) AS n {from} GROUP BY {bucket} ORDER BY bucket")
    }

    fn top(from: String, key: &str, with_volume: bool) -> String {
        let volume = if with_volume { format!(", {} AS volume_raw", volume("x.amount_raw")) } else { String::new() };
        format!(
            "SELECT {key} AS subject, COUNT(*) AS n{volume} {from} GROUP BY {key} \
             ORDER BY n DESC, subject ASC LIMIT {TOP_N}"
        )
    }

    // --- mint ---

    fn mint_ok(id: &str, w: &Window) -> String {
        format!("{} AND t.status = 'success' AND x.mint_token_address = {id}", transfers_from(w))
    }

    pub fn mint_summary(id: &str, w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS transfers, MIN(x.slot) AS first_slot, MAX(x.slot) AS last_slot, \
               COUNT(DISTINCT x.source_token_account) AS sources, \
               COUNT(DISTINCT x.destination_token_account) AS destinations, \
               {} AS volume_raw, \
               MIN(x.decimals) AS min_decimals, MAX(x.decimals) AS max_decimals \
             {}",
            volume("x.amount_raw"),
            mint_ok(id, w)
        )
    }

    pub fn mint_top_sources(id: &str, w: &Window) -> String {
        top(mint_ok(id, w), "x.source_token_account", true)
    }

    pub fn mint_top_destinations(id: &str, w: &Window) -> String {
        top(mint_ok(id, w), "x.destination_token_account", true)
    }

    pub fn mint_series(id: &str, w: &Window) -> String {
        series(mint_ok(id, w), "x.slot", w)
    }

    pub fn mint_coverage(id: &str, w: &Window) -> String {
        format!(
            "SELECT COUNT(CASE WHEN t.status <> 'success' AND x.mint_token_address = {id} THEN 1 END) AS failed_excluded, \
               COUNT(CASE WHEN t.status = 'success' AND x.mint_token_address IS NULL THEN 1 END) AS unattributed \
             {} AND (x.mint_token_address = {id} OR x.mint_token_address IS NULL)",
            transfers_from(w)
        )
    }

    // --- token account ---

    fn touches(id: &str) -> String {
        format!("(x.source_token_account = {id} OR x.destination_token_account = {id})")
    }

    fn account_ok(id: &str, w: &Window) -> String {
        format!("{} AND t.status = 'success' AND {}", transfers_from(w), touches(id))
    }

    pub fn account_summary(id: &str, w: &Window) -> String {
        let owner = format!(
            "CASE WHEN x.source_token_account = {id} THEN x.source_owner ELSE x.destination_owner END"
        );
        format!(
            "SELECT COUNT(*) AS transfers, MIN(x.slot) AS first_slot, MAX(x.slot) AS last_slot, \
               COUNT(CASE WHEN x.source_token_account = {id} THEN 1 END) AS sent, \
               COUNT(CASE WHEN x.destination_token_account = {id} THEN 1 END) AS received, \
               {} AS volume_out_raw, {} AS volume_in_raw, \
               COUNT(DISTINCT x.mint_token_address) AS mints, MIN(x.mint_token_address) AS mint, \
               COUNT(DISTINCT {owner}) AS owners, MIN({owner}) AS owner, \
               MIN(x.decimals) AS min_decimals, MAX(x.decimals) AS max_decimals \
             {}",
            volume(&format!("CASE WHEN x.source_token_account = {id} THEN x.amount_raw END")),
            volume(&format!("CASE WHEN x.destination_token_account = {id} THEN x.amount_raw END")),
            account_ok(id, w)
        )
    }

    pub fn account_top_counterparties(id: &str, w: &Window) -> String {
        let key = format!(
            "CASE WHEN x.source_token_account = {id} THEN x.destination_token_account ELSE x.source_token_account END"
        );
        top(account_ok(id, w), &key, true)
    }

    pub fn account_series(id: &str, w: &Window) -> String {
        series(account_ok(id, w), "x.slot", w)
    }

    pub fn account_coverage(id: &str, w: &Window) -> String {
        format!(
            "SELECT COUNT(*) AS failed_excluded {} AND t.status <> 'success' AND {}",
            transfers_from(w),
            touches(id)
        )
    }

    // --- program ---

    fn program_all(id: &str, w: &Window) -> String {
        format!("{} AND i.program_id = {id}", instructions_from(w))
    }

    fn program_ok(id: &str, w: &Window) -> String {
        format!("{} AND t.status = 'success'", program_all(id, w))
    }

    pub fn program_summary(id: &str, w: &Window) -> String {
        const OK: &str = "t.status = 'success'";
        format!(
            "SELECT COUNT(CASE WHEN {OK} THEN 1 END) AS invocations, \
               COUNT(CASE WHEN {OK} AND i.inner_instruction_index IS NOT NULL THEN 1 END) AS inner_invocations, \
               COUNT(DISTINCT CASE WHEN {OK} THEN i.tx_signature END) AS transactions, \
               MIN(CASE WHEN {OK} THEN i.slot END) AS first_slot, \
               MAX(CASE WHEN {OK} THEN i.slot END) AS last_slot, \
               COUNT(CASE WHEN {OK} AND i.instruction_type IS NOT NULL THEN 1 END) AS decoded, \
               COUNT(DISTINCT i.tx_signature) AS transactions_all, \
               COUNT(DISTINCT CASE WHEN t.status <> 'success' THEN i.tx_signature END) AS failed_transactions, \
               COUNT(CASE WHEN t.status <> 'success' THEN 1 END) AS failed_excluded \
             {}",
            program_all(id, w)
        )
    }

    pub fn program_top_instruction_types(id: &str, w: &Window) -> String {
        top(program_ok(id, w), "COALESCE(i.instruction_type, '')", false)
    }

    pub fn program_series(id: &str, w: &Window) -> String {
        series(program_ok(id, w), "i.slot", w)
    }
}

// ---------------------------------------------------------------------------
// Row access
// ---------------------------------------------------------------------------

fn single_row<'a>(response: &'a LensResponse, what: &str) -> Result<&'a Map<String, Value>, ProfileError> {
    match response.rows.as_slice() {
        [row] => Ok(row),
        rows => Err(protocol(format!("{what} returned {} rows, expected 1", rows.len()))),
    }
}

fn opt_u64(row: &Map<String, Value>, column: &str) -> Result<Option<u64>, ProfileError> {
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| protocol(format!("{column} = {value} is not a u64"))),
        None => Err(protocol(format!("column {column} is missing"))),
    }
}

fn get_u64(row: &Map<String, Value>, column: &str) -> Result<u64, ProfileError> {
    opt_u64(row, column)?.ok_or_else(|| protocol(format!("{column} is null")))
}

fn opt_str(row: &Map<String, Value>, column: &str) -> Result<Option<String>, ProfileError> {
    match row.get(column) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(value) => Err(protocol(format!("{column} = {value} is not a string"))),
        None => Err(protocol(format!("column {column} is missing"))),
    }
}

/// A summed volume: decimal digits, `"0"` for the sum of nothing.
fn volume(row: &Map<String, Value>, column: &str) -> Result<String, ProfileError> {
    let raw = opt_str(row, column)?.unwrap_or_else(|| "0".to_owned());
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(protocol(format!("{column} = {raw:?} is not a non-negative integer")));
    }
    Ok(raw)
}

/// One decimals value for all rows, or the warning saying why not.
fn decimals(row: &Map<String, Value>) -> Result<Result<u8, &'static str>, ProfileError> {
    let (min, max) = (opt_u64(row, "min_decimals")?, opt_u64(row, "max_decimals")?);
    Ok(match (min, max) {
        (Some(a), Some(b)) if a == b => {
            Ok(u8::try_from(a).map_err(|_| protocol(format!("decimals {a} do not fit in u8")))?)
        }
        (Some(_), Some(_)) => Err(WARN_MIXED_DECIMALS),
        _ => Err(WARN_NO_DECIMALS),
    })
}

/// `raw / 10^decimals` as exact decimal text. `raw` is digits only.
pub fn ui_amount(raw: &str, decimals: u8) -> String {
    let raw = raw.trim_start_matches('0');
    let d = usize::from(decimals);
    if d == 0 {
        return if raw.is_empty() { "0".to_owned() } else { raw.to_owned() };
    }
    let padded = format!("{raw:0>width$}", width = d + 1);
    let (int, frac) = padded.split_at(padded.len() - d);
    format!("{int}.{frac}")
}

fn ratio(n: u64, d: u64) -> Option<f64> {
    (d > 0).then(|| n as f64 / d as f64)
}

/// Rows `subject, n[, volume_raw]` as top entries; `""` keys become `None`
/// when `empty_is_none`.
fn top_entries(
    response: &LensResponse,
    with_volume: bool,
    empty_is_none: bool,
    total: u64,
) -> Result<Vec<TopEntry>, ProfileError> {
    if response.rows.len() as u64 > TOP_N {
        return Err(protocol(format!("top list returned {} rows, limit {TOP_N}", response.rows.len())));
    }
    let mut out = Vec::with_capacity(response.rows.len());
    for row in &response.rows {
        let key = opt_str(row, "subject")?.filter(|k| !(empty_is_none && k.is_empty()));
        let count = get_u64(row, "n")?;
        if count == 0 || count > total {
            return Err(inconsistent(format!("a top entry counts {count} of {total}")));
        }
        let volume_raw = if with_volume { Some(volume(row, "volume_raw")?) } else { None };
        out.push(TopEntry { key, count, volume_raw });
    }
    Ok(out)
}

fn series(response: &LensResponse, window: &Window, total: u64) -> Result<Vec<SeriesPoint>, ProfileError> {
    let mut points = window.empty_series();
    for row in &response.rows {
        let bucket = get_u64(row, "bucket")?;
        let point = points
            .get_mut(usize::try_from(bucket).unwrap_or(usize::MAX))
            .ok_or_else(|| protocol(format!("bucket {bucket} outside 0..{}", window.buckets())))?;
        point.count = get_u64(row, "n")?;
    }
    let sum: u64 = points.iter().map(|p| p.count).sum();
    if sum != total {
        return Err(inconsistent(format!("the series adds up to {sum}, the summary says {total}")));
    }
    Ok(points)
}

/// Sections read the same snapshot yet disagree: rows at or below the
/// snapshot changed while the profile ran. Retrying gives a consistent one.
fn inconsistent(detail: String) -> ProfileError {
    ProfileError::Unavailable(format!("data under the snapshot changed during the profile ({detail}); retry"))
}

// ---------------------------------------------------------------------------
// Profiler
// ---------------------------------------------------------------------------

/// Builds [`Profile`]s. Cheap to clone; clones share the concurrency limit.
#[derive(Clone)]
pub struct Profiler {
    lens: Arc<dyn LensApi>,
    permits: Arc<Semaphore>,
    deadline: Duration,
}

impl fmt::Debug for Profiler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Profiler")
            .field("available_permits", &self.permits.available_permits())
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// One profile's queries, counted.
struct Session<'a> {
    lens: &'a dyn LensApi,
    window_slots: u64,
    queries: u32,
}

impl Session<'_> {
    async fn query(&mut self, sql: &str) -> Result<LensResponse, ProfileError> {
        self.queries += 1;
        let response = self.lens.query(sql).await.map_err(|e| lens_error(e, self.window_slots))?;
        if response.truncated {
            return Err(ProfileError::Truncated);
        }
        Ok(response)
    }

    async fn max_slot(&mut self, sql: &str, table: &str) -> Result<u64, ProfileError> {
        let response = self.query(sql).await?;
        opt_u64(single_row(&response, "snapshot query")?, "max_slot")?
            .ok_or_else(|| ProfileError::NotReady(format!("{table} has no rows yet")))
    }
}

/// What the per-kind part of a profile produces.
struct Body {
    found: bool,
    summary: Summary,
    top: BTreeMap<&'static str, Vec<TopEntry>>,
    series: Vec<SeriesPoint>,
    coverage: BTreeMap<&'static str, u64>,
    warnings: Vec<String>,
}

impl Profiler {
    pub fn new(lens: Arc<dyn LensApi>) -> Self {
        Self::with_limits(lens, MAX_CONCURRENT_PROFILES, Duration::from_secs(PROFILE_DEADLINE_SECS))
    }

    /// For tests: a different concurrency limit or deadline.
    pub fn with_limits(lens: Arc<dyn LensApi>, max_concurrent: usize, deadline: Duration) -> Self {
        Self { lens, permits: Arc::new(Semaphore::new(max_concurrent.max(1))), deadline }
    }

    /// Validate `req`, then build its profile. See the [module docs](self).
    pub async fn profile(&self, req: ProfileRequest) -> Result<Profile, ProfileError> {
        let valid = validate(&req)?;
        let started = Instant::now();
        match tokio::time::timeout(self.deadline, self.run(valid, started)).await {
            Ok(result) => result,
            Err(_) => Err(ProfileError::Timeout(format!(
                "the profile did not finish within {} s; narrow window_slots",
                self.deadline.as_secs()
            ))),
        }
    }

    async fn run(&self, valid: Valid, started: Instant) -> Result<Profile, ProfileError> {
        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|_| ProfileError::Unavailable("the profiler is shut down".to_owned()))?;
        let mut s = Session { lens: self.lens.as_ref(), window_slots: valid.window_slots, queries: 0 };

        // Snapshot.
        let slots = s.max_slot(MAX_SLOTS_SQL, "solana.slots").await?;
        let txs = s.max_slot(MAX_TX_SLOT_SQL, "solana.transactions").await?;
        let visible = slots.min(txs);
        let w = match valid.as_of_slot {
            Some(pin) if pin > visible => {
                return Err(ProfileError::invalid(
                    "as_of_slot",
                    format!("{pin} is above {visible}, the newest fully visible slot"),
                ));
            }
            Some(pin) => pin,
            None => visible,
        };
        if i64::try_from(w).is_err() {
            return Err(protocol(format!("snapshot slot {w} does not fit in i64")));
        }

        let window = Window::ending_at(w, valid.window_slots);
        let id = quote_sql_string(&valid.id);
        let mut body = match valid.kind {
            Kind::Mint => mint(&mut s, &id, &window).await?,
            Kind::TokenAccount => token_account(&mut s, &id, &window).await?,
            Kind::Program => program(&mut s, &id, &window).await?,
        };
        debug_assert_eq!(s.queries, valid.kind.queries());

        if window.slots < valid.window_slots {
            body.warnings.push(WARN_WINDOW_CLIPPED.to_owned());
        }
        if !body.found {
            body.warnings.push(WARN_NOT_FOUND.to_owned());
        }
        Ok(Profile {
            kind: valid.kind,
            id: valid.id,
            found: body.found,
            as_of_slot: w,
            window,
            summary: body.summary,
            top: body.top,
            series: body.series,
            coverage: body.coverage,
            warnings: body.warnings,
            queries_run: s.queries,
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        })
    }
}

async fn mint(s: &mut Session<'_>, id: &str, w: &Window) -> Result<Body, ProfileError> {
    let summary = s.query(&sql::mint_summary(id, w)).await?;
    let sources = s.query(&sql::mint_top_sources(id, w)).await?;
    let destinations = s.query(&sql::mint_top_destinations(id, w)).await?;
    let series_rows = s.query(&sql::mint_series(id, w)).await?;
    let coverage = s.query(&sql::mint_coverage(id, w)).await?;

    let row = single_row(&summary, "mint summary")?;
    let transfers = get_u64(row, "transfers")?;
    let volume_raw = volume(row, "volume_raw")?;
    let mut warnings = vec![WARN_MINT_LOWER_BOUND.to_owned()];
    let decimals = match decimals(row)? {
        Ok(d) => Some(d),
        Err(warning) => {
            if transfers > 0 {
                warnings.push(warning.to_owned());
            }
            None
        }
    };
    let summary = MintSummary {
        transfers,
        first_seen_slot: opt_u64(row, "first_slot")?,
        last_seen_slot: opt_u64(row, "last_slot")?,
        distinct_sources: get_u64(row, "sources")?,
        distinct_destinations: get_u64(row, "destinations")?,
        volume_ui: decimals.map(|d| ui_amount(&volume_raw, d)),
        volume_raw,
        decimals,
    };
    let top = BTreeMap::from([
        ("top_sources", top_entries(&sources, true, false, transfers)?),
        ("top_destinations", top_entries(&destinations, true, false, transfers)?),
    ]);
    let row = single_row(&coverage, "mint coverage")?;
    let coverage = BTreeMap::from([
        ("transfers_in_failed_tx_excluded", get_u64(row, "failed_excluded")?),
        ("unattributed_transfers", get_u64(row, "unattributed")?),
    ]);
    Ok(Body {
        found: transfers > 0,
        summary: Summary::Mint(summary),
        top,
        series: series(&series_rows, w, transfers)?,
        coverage,
        warnings,
    })
}

async fn token_account(s: &mut Session<'_>, id: &str, w: &Window) -> Result<Body, ProfileError> {
    let summary = s.query(&sql::account_summary(id, w)).await?;
    let counterparties = s.query(&sql::account_top_counterparties(id, w)).await?;
    let series_rows = s.query(&sql::account_series(id, w)).await?;
    let coverage = s.query(&sql::account_coverage(id, w)).await?;

    let row = single_row(&summary, "token account summary")?;
    let transfers = get_u64(row, "transfers")?;
    let distinct_mints = get_u64(row, "mints")?;
    let distinct_owners = get_u64(row, "owners")?;
    let (volume_out_raw, volume_in_raw) = (volume(row, "volume_out_raw")?, volume(row, "volume_in_raw")?);
    let mut warnings = Vec::new();
    let decimals = if distinct_mints > 1 {
        warnings.push(WARN_MULTIPLE_MINTS.to_owned());
        None
    } else {
        match decimals(row)? {
            Ok(d) => Some(d),
            Err(warning) => {
                if transfers > 0 {
                    warnings.push(warning.to_owned());
                }
                None
            }
        }
    };
    let summary = TokenAccountSummary {
        transfers,
        transfers_out: get_u64(row, "sent")?,
        transfers_in: get_u64(row, "received")?,
        first_seen_slot: opt_u64(row, "first_slot")?,
        last_seen_slot: opt_u64(row, "last_slot")?,
        mint: if distinct_mints == 1 { opt_str(row, "mint")? } else { None },
        distinct_mints,
        owner: if distinct_owners == 1 { opt_str(row, "owner")? } else { None },
        distinct_owners,
        volume_out_ui: decimals.map(|d| ui_amount(&volume_out_raw, d)),
        volume_in_ui: decimals.map(|d| ui_amount(&volume_in_raw, d)),
        volume_out_raw,
        volume_in_raw,
        decimals,
    };
    let top = BTreeMap::from([("top_counterparties", top_entries(&counterparties, true, false, transfers)?)]);
    let row = single_row(&coverage, "token account coverage")?;
    let coverage = BTreeMap::from([("transfers_in_failed_tx_excluded", get_u64(row, "failed_excluded")?)]);
    Ok(Body {
        found: transfers > 0,
        summary: Summary::TokenAccount(summary),
        top,
        series: series(&series_rows, w, transfers)?,
        coverage,
        warnings,
    })
}

async fn program(s: &mut Session<'_>, id: &str, w: &Window) -> Result<Body, ProfileError> {
    let summary = s.query(&sql::program_summary(id, w)).await?;
    let types = s.query(&sql::program_top_instruction_types(id, w)).await?;
    let series_rows = s.query(&sql::program_series(id, w)).await?;

    let row = single_row(&summary, "program summary")?;
    let invocations = get_u64(row, "invocations")?;
    let decoded = get_u64(row, "decoded")?;
    let transactions_all = get_u64(row, "transactions_all")?;
    let failed = get_u64(row, "failed_transactions")?;
    if failed > transactions_all || decoded > invocations {
        return Err(protocol(format!(
            "failed {failed} of {transactions_all} transactions, decoded {decoded} of {invocations}"
        )));
    }
    let mut warnings = vec![WARN_FAILURE_SHARE.to_owned(), WARN_DECODE_RATE.to_owned()];
    if invocations > 0 && decoded == 0 {
        warnings.push(WARN_NOTHING_DECODED.to_owned());
    }
    let summary = ProgramSummary {
        invocations,
        inner_invocations: get_u64(row, "inner_invocations")?,
        transactions: get_u64(row, "transactions")?,
        first_seen_slot: opt_u64(row, "first_slot")?,
        last_seen_slot: opt_u64(row, "last_slot")?,
        decoded_invocations: decoded,
        decode_rate: ratio(decoded, invocations),
        transactions_including_failed: transactions_all,
        failed_transactions: failed,
        failure_share: ratio(failed, transactions_all),
    };
    let top = BTreeMap::from([("top_instruction_types", top_entries(&types, false, true, invocations)?)]);
    let coverage = BTreeMap::from([("instructions_in_failed_tx_excluded", get_u64(row, "failed_excluded")?)]);
    Ok(Body {
        found: invocations > 0,
        summary: Summary::Program(summary),
        top,
        series: series(&series_rows, w, invocations)?,
        coverage,
        warnings,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SYSTEM: &str = "11111111111111111111111111111111";
    const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    #[test]
    fn real_keys_pass_and_near_misses_fail() {
        assert!(validate_id(SYSTEM).is_ok());
        assert!(validate_id(USDC).is_ok());
        for bad in [
            "",
            "short",
            &USDC[..31],
            &format!("{USDC}1"),          // 45 characters
            "0PjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // '0' is not base58
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1'",
            "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", // 44 chars, decodes to 33 bytes
            "1111111111111111111111111111111111111111111", // 43 zeros: 43 bytes
        ] {
            match validate_id(bad) {
                Err(ProfileError::Invalid { field, .. }) => assert_eq!(field, "id", "{bad}"),
                other => panic!("{bad:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn the_reason_never_echoes_the_input() {
        let hostile = "x'; DROP TABLE solana.slots; --";
        let err = validate_id(hostile).unwrap_err().to_string();
        assert!(!err.contains("DROP"), "{err}");
    }

    #[test]
    fn window_and_buckets() {
        let w = Window::ending_at(1_000, 100);
        assert_eq!((w.from_slot, w.to_slot, w.slots), (901, 1_000, 100));
        assert_eq!(w.bucket_width(), 5); // ceil(100 / 24)
        assert_eq!(w.buckets(), 20);
        let s = w.empty_series();
        assert_eq!(s.first().unwrap().from_slot, 901);
        assert_eq!(s.last().unwrap().to_slot, 1_000);
        for pair in s.windows(2) {
            assert_eq!(pair[0].to_slot + 1, pair[1].from_slot);
        }

        let clipped = Window::ending_at(10, 50_000);
        assert_eq!((clipped.from_slot, clipped.slots), (0, 11));
        let one = Window::ending_at(7, 1);
        assert_eq!((one.from_slot, one.to_slot, one.buckets()), (7, 7, 1));
        for slots in 1..=200 {
            let w = Window::ending_at(10_000, slots);
            assert!(w.buckets() <= SERIES_BUCKETS);
            let s = w.empty_series();
            assert_eq!(s.iter().map(|p| p.to_slot - p.from_slot + 1).sum::<u64>(), slots);
        }
    }

    #[test]
    fn ui_amounts_are_exact() {
        assert_eq!(ui_amount("184467440737095516150", 6), "184467440737095.516150");
        assert_eq!(ui_amount("5", 6), "0.000005");
        assert_eq!(ui_amount("0", 6), "0.000000");
        assert_eq!(ui_amount("0", 0), "0");
        assert_eq!(ui_amount("1000000", 6), "1.000000");
        assert_eq!(ui_amount("42", 0), "42");
    }

    #[test]
    fn lens_errors_map_to_profile_errors() {
        let m = |e| lens_error(e, 50_000).kind();
        let s = |x: &str| x.to_owned();
        assert_eq!(m(LensCallError::BadQuery { message: s("table 'solana.slots' not found") }), "not_ready");
        assert_eq!(m(LensCallError::BadQuery { message: s("syntax") }), "permanent");
        assert_eq!(m(LensCallError::Protocol { message: s("x") }), "permanent");
        assert_eq!(m(LensCallError::Unavailable { message: s("x") }), "unavailable");
        let timeout = lens_error(LensCallError::Timeout { message: s("x") }, 50_000);
        assert_eq!(timeout.kind(), "timeout");
        assert!(timeout.to_string().contains("narrower window"), "{timeout}");
    }

    #[test]
    fn kinds_parse_and_round_trip() {
        for k in Kind::ALL {
            assert_eq!(k.as_str().parse::<Kind>().unwrap(), k);
            assert_eq!(serde_json::to_value(k).unwrap(), Value::from(k.as_str()));
        }
        assert!(matches!("Mint".parse::<Kind>(), Err(ProfileError::Invalid { .. })));
        assert!(serde_json::from_str::<Kind>("\"wallet\"").is_err());
    }
}
