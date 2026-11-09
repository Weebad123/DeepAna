//! The detector set.
//!
//! A [`Detector`] turns a range of complete buckets into findings. It asks
//! LENS, scores what comes back, and returns [`NewAnomaly`] rows. It never
//! touches the store: the runner (`detect/mod.rs`) commits the findings and
//! advances progress in one transaction.
//!
//! # Buckets
//!
//! The time axis is the bucket: `bucket = slot / bucket_slots`, where
//! `bucket_slots = bucket_secs * 1000 / 400` (Solana's nominal 400 ms slot).
//! Bucket `b` covers slots `b * bucket_slots ..= (b + 1) * bucket_slots - 1`.
//! Counting slots instead of reading `block_time` keeps a bucket's bounds a
//! pure function of integers. Skipped slots and a nullable `block_time`
//! cannot move them, and every query can bound itself with `slot BETWEEN`.
//!
//! # SQL
//!
//! Every statement is built from integers this module computes (slot bounds,
//! `bucket_slots`, `TOP_K_PROGRAMS`) and string constants written here.
//! Nothing read from LENS or from a request is ever put into SQL. A mint or
//! program read from LENS only ever goes into a store parameter.
//!
//! # The set
//!
//! | id                     | kind              | value per bucket                         | missing when                 |
//! |------------------------|-------------------|------------------------------------------|------------------------------|
//! | `tx_count`             | series            | transactions                             | never: no rows means 0       |
//! | `tx_failure_rate`      | series            | failed / transactions                    | fewer than [`MIN_RATIO_DENOMINATOR`] transactions |
//! | `fee_per_tx`           | series            | mean fee, lamports                       | no transactions              |
//! | `transfer_count`       | series            | token transfers                          | never: no rows means 0       |
//! | `top_mint_share`       | concentration     | busiest mint's transfers / all transfers | fewer than [`MIN_RATIO_DENOMINATOR`] transfers |
//! | `first_seen_mint`      | first_seen        | a mint's first transfer                  | n/a                          |
//! | `first_seen_program`   | first_seen        | a program's first instruction            | n/a                          |
//! | `program_failure_rate` | program_failure   | per program: failed / transactions       | fewer than [`MIN_RATIO_DENOMINATOR`] transactions |
//!
//! A missing bucket is neither scored nor part of any history. A count
//! metric's empty bucket is a real 0, and a sudden 0 is exactly the kind of
//! drop worth flagging. A ratio over a handful of transactions is noise.
//!
//! # Scored anomalies
//!
//! A series finding stores `baseline` = the history's median and `scale` =
//! the effective scale it was divided by (`max(MAD, min_scale, rel_floor *
//! |median|)`, see `baseline.rs`), so `0.6745 * (value - baseline) / scale`
//! recomputed from the row is the stored `score`. The raw MAD is in
//! `detail.mad`.
//!
//! # Program failure rate
//!
//! This one is the most expensive. One statement joins instructions to
//! transactions on `(slot, tx_signature)`, counts distinct transactions per
//! bucket and program, and keeps the [`TOP_K_PROGRAMS`] busiest programs of
//! *each scored bucket*. Each kept program is its own series with its own
//! history. Choosing the top K per bucket rather than per chunk makes a
//! bucket's verdict independent of how the runner chunked the range, so a
//! live run and a backfill agree. Rust re-derives each bucket's top K from
//! the returned rows with the same tie-break (count descending, then program
//! id ascending).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use forge::lens_client::{LensApi, LensCallError, LensResponse};
use serde_json::{Map, Value, json};
use thiserror::Error;

use super::baseline::{Direction, ScoreParams, Verdict, score};
use crate::store::{AnomalyKind, MAX_SUBJECT_CHARS, NewAnomaly};

/// Nominal Solana slot time.
pub const SLOT_MS: u64 = 400;
/// Fewest observations behind a ratio before it counts as a value.
pub const MIN_RATIO_DENOMINATOR: u64 = 20;
/// Programs kept per scored bucket by `program_failure_rate`.
pub const TOP_K_PROGRAMS: u64 = 10;

/// Slots per bucket of `bucket_secs`.
pub const fn bucket_slots(bucket_secs: u64) -> u64 {
    bucket_secs * 1000 / SLOT_MS
}

/// First slot of `bucket`.
pub const fn bucket_start(bucket: u64, bucket_slots: u64) -> u64 {
    bucket.saturating_mul(bucket_slots)
}

/// Last slot of `bucket`.
pub const fn bucket_end(bucket: u64, bucket_slots: u64) -> u64 {
    bucket.saturating_add(1).saturating_mul(bucket_slots).saturating_sub(1)
}

// ---------------------------------------------------------------------------
// Trait and types
// ---------------------------------------------------------------------------

/// What one scan sees.
#[derive(Clone)]
pub struct ScanContext {
    pub lens: Arc<dyn LensApi>,
    pub bucket_slots: u64,
    /// Buckets of history before each scored bucket (`baseline_buckets`).
    pub history_buckets: u64,
    /// `threshold` and `min_history` from config. Each detector replaces
    /// `min_scale`, `rel_floor` and `direction` with its own.
    pub params: ScoreParams,
    /// First bucket to score, inclusive. At least `first_complete_bucket`.
    pub from_bucket: u64,
    /// Last bucket to score, inclusive. Complete.
    pub to_bucket: u64,
    /// The first bucket whose every slot is ingested. History never reaches
    /// before it: the bucket ingestion started in is partial.
    pub first_complete_bucket: u64,
}

impl std::fmt::Debug for ScanContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanContext")
            .field("bucket_slots", &self.bucket_slots)
            .field("history_buckets", &self.history_buckets)
            .field("params", &self.params)
            .field("from_bucket", &self.from_bucket)
            .field("to_bucket", &self.to_bucket)
            .field("first_complete_bucket", &self.first_complete_bucket)
            .finish_non_exhaustive()
    }
}

impl ScanContext {
    /// First bucket any history in this scan reaches back to.
    pub fn history_start(&self) -> u64 {
        self.from_bucket.saturating_sub(self.history_buckets).max(self.first_complete_bucket)
    }

    fn start_slot(&self, bucket: u64) -> u64 {
        bucket_start(bucket, self.bucket_slots)
    }

    fn end_slot(&self, bucket: u64) -> u64 {
        bucket_end(bucket, self.bucket_slots)
    }

    /// Run `sql`, refusing a truncated result.
    async fn query(&self, sql: &str) -> Result<LensResponse, ScanError> {
        let response = self.lens.query(sql).await?;
        if response.truncated {
            return Err(ScanError::Truncated);
        }
        Ok(response)
    }
}

/// What a scan found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ScanOutput {
    pub anomalies: Vec<NewAnomaly>,
}

/// Why a scan produced nothing. In every case progress stays where it was.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ScanError {
    /// LENS has no such table yet (PRISM has not written it, or LENS has not
    /// refreshed). Expected at startup.
    #[error("LENS is not ready: {0}")]
    NotReady(String),
    /// LENS timed out or could not be reached. Retried next tick.
    #[error("transient LENS failure: {0}")]
    Transient(String),
    /// LENS dropped rows past its row limit. The runner halves the chunk.
    #[error("LENS truncated the result")]
    Truncated,
    /// A bug or a protocol mismatch: retrying will not help.
    #[error("{0}")]
    Permanent(String),
}

impl ScanError {
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::NotReady(_) => "not_ready",
            Self::Transient(_) => "transient",
            Self::Truncated => "truncated",
            Self::Permanent(_) => "permanent",
        }
    }
}

/// | `LensCallError` | `ScanError` |
/// |---|---|
/// | `BadQuery` naming a missing table ("not found") | `NotReady` |
/// | other `BadQuery`, `Protocol` | `Permanent` |
/// | `Timeout`, `Unavailable` | `Transient` |
impl From<LensCallError> for ScanError {
    fn from(err: LensCallError) -> Self {
        match &err {
            LensCallError::BadQuery { message } if message.contains("not found") => Self::NotReady(err.to_string()),
            LensCallError::BadQuery { .. } | LensCallError::Protocol { .. } => Self::Permanent(err.to_string()),
            LensCallError::Timeout { .. } | LensCallError::Unavailable { .. } => Self::Transient(err.to_string()),
        }
    }
}

/// One detector. Its [`id`](Detector::id) is also its store metric.
#[async_trait]
pub trait Detector: Send + Sync {
    fn id(&self) -> &str;
    fn kind(&self) -> AnomalyKind;
    /// Score buckets `ctx.from_bucket ..= ctx.to_bucket`. Must return the
    /// same findings for the same data, however the range is chunked.
    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError>;
}

/// Every detector, in the fixed order the runner visits them.
pub fn default_detectors() -> Vec<Arc<dyn Detector>> {
    vec![
        Arc::new(SeriesDetector::new(SeriesMetric::TxCount)),
        Arc::new(SeriesDetector::new(SeriesMetric::TxFailureRate)),
        Arc::new(SeriesDetector::new(SeriesMetric::FeePerTx)),
        Arc::new(SeriesDetector::new(SeriesMetric::TransferCount)),
        Arc::new(TopMintShare),
        Arc::new(FirstSeen::mints()),
        Arc::new(FirstSeen::programs()),
        Arc::new(ProgramFailureRate),
    ]
}

// ---------------------------------------------------------------------------
// Row parsing
// ---------------------------------------------------------------------------

fn protocol(message: String) -> ScanError {
    ScanError::Permanent(format!("unexpected LENS result: {message}"))
}

fn get_u64(row: &Map<String, Value>, column: &str) -> Result<u64, ScanError> {
    match row.get(column) {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| protocol(format!("column {column} = {value} is not a non-negative integer"))),
        None => Err(protocol(format!("column {column} is missing"))),
    }
}

fn get_str<'a>(row: &'a Map<String, Value>, column: &str) -> Result<&'a str, ScanError> {
    match row.get(column) {
        Some(Value::String(text)) => Ok(text),
        Some(value) => Err(protocol(format!("column {column} = {value} is not a string"))),
        None => Err(protocol(format!("column {column} is missing"))),
    }
}

/// A row's bucket, checked to lie in `lo ..= hi`.
fn get_bucket(row: &Map<String, Value>, lo: u64, hi: u64) -> Result<u64, ScanError> {
    let bucket = get_u64(row, "bucket")?;
    if !(lo..=hi).contains(&bucket) {
        return Err(protocol(format!("bucket {bucket} outside the queried {lo}..={hi}")));
    }
    Ok(bucket)
}

/// Whether an entity read from LENS can be a store subject. One that cannot
/// is skipped with a warning instead of failing the whole commit forever.
fn usable_subject(subject: &str) -> bool {
    !subject.is_empty()
        && subject.chars().count() <= MAX_SUBJECT_CHARS
        && subject.trim() == subject
        && !subject.chars().any(char::is_control)
}

// ---------------------------------------------------------------------------
// Series scoring, shared
// ---------------------------------------------------------------------------

/// How a detector scores its series, on top of the config's params.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeriesScoring {
    pub min_scale: f64,
    pub rel_floor: f64,
    pub direction: Direction,
}

impl SeriesScoring {
    const COUNT: Self = Self { min_scale: 1.0, rel_floor: 0.05, direction: Direction::Both };
    const RATIO_UP: Self = Self { min_scale: 0.01, rel_floor: 0.0, direction: Direction::Up };

    pub fn params(&self, base: &ScoreParams) -> ScoreParams {
        ScoreParams { min_scale: self.min_scale, rel_floor: self.rel_floor, direction: self.direction, ..*base }
    }
}

/// One scored bucket of a series.
struct Flagged {
    bucket: u64,
    value: f64,
    z: f64,
    median: f64,
    mad: f64,
    scale: f64,
    direction: Direction,
    history_len: usize,
}

/// Score every bucket of `ctx.from_bucket ..= ctx.to_bucket` that has a
/// value in `values`, each against the values of the `history_buckets`
/// buckets before it (never before `first_complete_bucket`).
fn score_series(ctx: &ScanContext, values: &BTreeMap<u64, f64>, params: &ScoreParams) -> Vec<Flagged> {
    let mut flagged = Vec::new();
    for bucket in ctx.from_bucket..=ctx.to_bucket {
        let Some(&value) = values.get(&bucket) else { continue };
        let lo = bucket.saturating_sub(ctx.history_buckets).max(ctx.first_complete_bucket);
        let history: Vec<f64> = values.range(lo..bucket).map(|(_, v)| *v).collect();
        if let Verdict::Anomalous { z, median, mad, scale, direction } = score(&history, value, params) {
            flagged.push(Flagged { bucket, value, z, median, mad, scale, direction, history_len: history.len() });
        }
    }
    flagged
}

fn series_anomaly(
    ctx: &ScanContext,
    kind: AnomalyKind,
    subject: String,
    f: &Flagged,
    params: &ScoreParams,
    extra: Map<String, Value>,
) -> NewAnomaly {
    let mut detail = Map::new();
    detail.insert("mad".into(), json!(f.mad));
    detail.insert("direction".into(), json!(f.direction.as_str()));
    detail.insert("threshold".into(), json!(params.threshold));
    detail.insert("history".into(), json!(f.history_len));
    detail.extend(extra);
    NewAnomaly {
        subject,
        bucket: f.bucket,
        start_slot: ctx.start_slot(f.bucket),
        end_slot: ctx.end_slot(f.bucket),
        kind,
        value: f.value,
        baseline: Some(f.median),
        scale: Some(f.scale),
        score: f.z,
        detail: Some(Value::Object(detail)),
    }
}

// ---------------------------------------------------------------------------
// Global series: tx_count, tx_failure_rate, fee_per_tx, transfer_count
// ---------------------------------------------------------------------------

/// The global per-bucket series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeriesMetric {
    TxCount,
    TxFailureRate,
    FeePerTx,
    TransferCount,
}

impl SeriesMetric {
    pub const fn id(self) -> &'static str {
        match self {
            Self::TxCount => "tx_count",
            Self::TxFailureRate => "tx_failure_rate",
            Self::FeePerTx => "fee_per_tx",
            Self::TransferCount => "transfer_count",
        }
    }

    const fn scoring(self) -> SeriesScoring {
        match self {
            Self::TxCount | Self::FeePerTx | Self::TransferCount => SeriesScoring::COUNT,
            Self::TxFailureRate => SeriesScoring::RATIO_UP,
        }
    }

    /// One row per bucket with data: `bucket`, `n`, and `failed`, `fee` for
    /// the transaction metrics.
    fn sql(self, lo_slot: u64, hi_slot: u64, bucket_slots: u64) -> String {
        match self {
            Self::TxCount | Self::TxFailureRate | Self::FeePerTx => format!(
                "SELECT CAST(slot AS BIGINT) / {bucket_slots} AS bucket, COUNT(*) AS n, \
                 SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed, SUM(fee) AS fee \
                 FROM solana.transactions WHERE slot >= {lo_slot} AND slot <= {hi_slot} \
                 GROUP BY CAST(slot AS BIGINT) / {bucket_slots} ORDER BY bucket"
            ),
            Self::TransferCount => format!(
                "SELECT CAST(slot AS BIGINT) / {bucket_slots} AS bucket, COUNT(*) AS n \
                 FROM solana.token_transfers WHERE slot >= {lo_slot} AND slot <= {hi_slot} \
                 GROUP BY CAST(slot AS BIGINT) / {bucket_slots} ORDER BY bucket"
            ),
        }
    }

    /// Whether a bucket without rows is a 0 (counts) or missing (ratios).
    const fn zero_fills(self) -> bool {
        matches!(self, Self::TxCount | Self::TransferCount)
    }

    /// The bucket's value from its row; `None` = missing.
    fn value(self, row: &Map<String, Value>) -> Result<Option<f64>, ScanError> {
        let n = get_u64(row, "n")?;
        Ok(match self {
            Self::TxCount | Self::TransferCount => Some(n as f64),
            Self::TxFailureRate => {
                let failed = get_u64(row, "failed")?;
                (n >= MIN_RATIO_DENOMINATOR).then(|| failed as f64 / n as f64)
            }
            Self::FeePerTx => {
                let fee = get_u64(row, "fee")?;
                (n > 0).then(|| fee as f64 / n as f64)
            }
        })
    }
}

/// A global series detector.
#[derive(Debug, Clone, Copy)]
pub struct SeriesDetector {
    metric: SeriesMetric,
}

impl SeriesDetector {
    pub const fn new(metric: SeriesMetric) -> Self {
        Self { metric }
    }
}

#[async_trait]
impl Detector for SeriesDetector {
    fn id(&self) -> &str {
        self.metric.id()
    }

    fn kind(&self) -> AnomalyKind {
        AnomalyKind::Series
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        let lo = ctx.history_start();
        let sql = self.metric.sql(ctx.start_slot(lo), ctx.end_slot(ctx.to_bucket), ctx.bucket_slots);
        let response = ctx.query(&sql).await?;

        let mut values = BTreeMap::new();
        if self.metric.zero_fills() {
            values.extend((lo..=ctx.to_bucket).map(|b| (b, 0.0)));
        }
        let mut raw: HashMap<u64, Map<String, Value>> = HashMap::new();
        for row in &response.rows {
            let bucket = get_bucket(row, lo, ctx.to_bucket)?;
            match self.metric.value(row)? {
                Some(value) => values.insert(bucket, value),
                None => values.remove(&bucket),
            };
            raw.insert(bucket, row.clone());
        }

        let params = self.metric.scoring().params(&ctx.params);
        let anomalies = score_series(ctx, &values, &params)
            .iter()
            .map(|f| {
                // The counts the value came from, so the ratio can be checked.
                let extra = raw.get(&f.bucket).cloned().unwrap_or_default();
                let extra = extra.into_iter().filter(|(key, _)| key != "bucket").collect();
                series_anomaly(ctx, AnomalyKind::Series, String::new(), f, &params, extra)
            })
            .collect();
        Ok(ScanOutput { anomalies })
    }
}

// ---------------------------------------------------------------------------
// top_mint_share
// ---------------------------------------------------------------------------

/// The busiest mint's share of a bucket's token transfers.
#[derive(Debug, Clone, Copy)]
pub struct TopMintShare;

impl TopMintShare {
    pub const ID: &'static str = "top_mint_share";
    const SCORING: SeriesScoring = SeriesScoring::RATIO_UP;

    /// One row per bucket: `bucket`, `top` (the busiest mint's transfers),
    /// `total`.
    fn sql(lo_slot: u64, hi_slot: u64, bucket_slots: u64) -> String {
        format!(
            "WITH per AS (SELECT CAST(slot AS BIGINT) / {bucket_slots} AS bucket, mint_token_address AS mint, \
             COUNT(*) AS n FROM solana.token_transfers \
             WHERE slot >= {lo_slot} AND slot <= {hi_slot} AND mint_token_address IS NOT NULL \
             GROUP BY CAST(slot AS BIGINT) / {bucket_slots}, mint_token_address) \
             SELECT bucket, MAX(n) AS top, SUM(n) AS total FROM per GROUP BY bucket ORDER BY bucket"
        )
    }

    /// The busiest mint of one bucket, ties to the smallest address.
    fn top_mint_sql(lo_slot: u64, hi_slot: u64) -> String {
        format!(
            "SELECT mint_token_address AS mint, COUNT(*) AS n FROM solana.token_transfers \
             WHERE slot >= {lo_slot} AND slot <= {hi_slot} AND mint_token_address IS NOT NULL \
             GROUP BY mint_token_address ORDER BY n DESC, mint ASC LIMIT 1"
        )
    }
}

#[async_trait]
impl Detector for TopMintShare {
    fn id(&self) -> &str {
        Self::ID
    }

    fn kind(&self) -> AnomalyKind {
        AnomalyKind::Concentration
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        let lo = ctx.history_start();
        let sql = Self::sql(ctx.start_slot(lo), ctx.end_slot(ctx.to_bucket), ctx.bucket_slots);
        let response = ctx.query(&sql).await?;

        let mut values = BTreeMap::new();
        let mut counts = HashMap::new();
        for row in &response.rows {
            let bucket = get_bucket(row, lo, ctx.to_bucket)?;
            let top = get_u64(row, "top")?;
            let total = get_u64(row, "total")?;
            if top > total {
                return Err(protocol(format!("bucket {bucket}: top {top} above total {total}")));
            }
            if total >= MIN_RATIO_DENOMINATOR {
                values.insert(bucket, top as f64 / total as f64);
            }
            counts.insert(bucket, (top, total));
        }

        let params = Self::SCORING.params(&ctx.params);
        let mut anomalies = Vec::new();
        for f in score_series(ctx, &values, &params) {
            // Name the mint: one more query, built from the bucket's bounds.
            let follow_up = Self::top_mint_sql(ctx.start_slot(f.bucket), ctx.end_slot(f.bucket));
            let named = ctx.query(&follow_up).await?;
            let [row] = named.rows.as_slice() else {
                return Err(protocol(format!("bucket {}: top-mint query returned {} rows", f.bucket, named.rows.len())));
            };
            let mint = get_str(row, "mint")?;
            if !usable_subject(mint) {
                tracing::warn!(metric = Self::ID, bucket = f.bucket, "top mint is not a usable subject; skipped");
                continue;
            }
            let (top, total) = counts[&f.bucket];
            let mut extra = Map::new();
            extra.insert("top".into(), json!(top));
            extra.insert("total".into(), json!(total));
            anomalies.push(series_anomaly(ctx, AnomalyKind::Concentration, mint.to_owned(), &f, &params, extra));
        }
        Ok(ScanOutput { anomalies })
    }
}

// ---------------------------------------------------------------------------
// First seen
// ---------------------------------------------------------------------------

/// Entities whose first appearance in the ingested data falls in the range.
/// Stateless: each scan asks LENS for the minimum slot of every entity over
/// everything up to the range's end, and keeps those inside the range.
#[derive(Debug, Clone, Copy)]
pub struct FirstSeen {
    id: &'static str,
    table: &'static str,
    column: &'static str,
}

impl FirstSeen {
    pub const MINTS_ID: &'static str = "first_seen_mint";
    pub const PROGRAMS_ID: &'static str = "first_seen_program";

    pub const fn mints() -> Self {
        Self { id: Self::MINTS_ID, table: "solana.token_transfers", column: "mint_token_address" }
    }

    pub const fn programs() -> Self {
        Self { id: Self::PROGRAMS_ID, table: "solana.instructions", column: "program_id" }
    }

    fn sql(&self, from_slot: u64, to_slot: u64) -> String {
        let Self { table, column, .. } = self;
        format!(
            "SELECT {column} AS subject, MIN(slot) AS first_slot FROM {table} \
             WHERE slot <= {to_slot} AND {column} IS NOT NULL \
             GROUP BY {column} HAVING MIN(slot) >= {from_slot} ORDER BY first_slot, subject"
        )
    }
}

#[async_trait]
impl Detector for FirstSeen {
    fn id(&self) -> &str {
        self.id
    }

    fn kind(&self) -> AnomalyKind {
        AnomalyKind::FirstSeen
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        let from_slot = ctx.start_slot(ctx.from_bucket);
        let to_slot = ctx.end_slot(ctx.to_bucket);
        let response = ctx.query(&self.sql(from_slot, to_slot)).await?;
        let mut anomalies = Vec::with_capacity(response.rows.len());
        for row in &response.rows {
            let subject = get_str(row, "subject")?;
            let first_slot = get_u64(row, "first_slot")?;
            if !(from_slot..=to_slot).contains(&first_slot) {
                return Err(protocol(format!("first_slot {first_slot} outside {from_slot}..={to_slot}")));
            }
            if !usable_subject(subject) {
                tracing::warn!(metric = self.id, first_slot, "entity is not a usable subject; skipped");
                continue;
            }
            let bucket = first_slot / ctx.bucket_slots;
            anomalies.push(NewAnomaly {
                subject: subject.to_owned(),
                bucket,
                start_slot: ctx.start_slot(bucket),
                end_slot: ctx.end_slot(bucket),
                kind: AnomalyKind::FirstSeen,
                // One new entity; there is no baseline to score against.
                value: 1.0,
                baseline: None,
                scale: None,
                score: 1.0,
                detail: Some(json!({ "first_slot": first_slot })),
            });
        }
        Ok(ScanOutput { anomalies })
    }
}

// ---------------------------------------------------------------------------
// program_failure_rate
// ---------------------------------------------------------------------------

/// Per-program failure rate, for the busiest programs of each bucket.
#[derive(Debug, Clone, Copy)]
pub struct ProgramFailureRate;

impl ProgramFailureRate {
    pub const ID: &'static str = "program_failure_rate";
    const SCORING: SeriesScoring = SeriesScoring { min_scale: 0.02, rel_floor: 0.0, direction: Direction::Up };

    /// Rows `bucket, program, n, failed` from `lo_slot ..= hi_slot`, for every
    /// program that is among the top K of some bucket at or after
    /// `from_bucket`.
    fn sql(lo_slot: u64, hi_slot: u64, bucket_slots: u64, from_bucket: u64) -> String {
        format!(
            "WITH per AS (\
               SELECT CAST(i.slot AS BIGINT) / {bucket_slots} AS bucket, i.program_id AS program, \
                 COUNT(DISTINCT i.tx_signature) AS n, \
                 COUNT(DISTINCT CASE WHEN t.status = 'failed' THEN i.tx_signature END) AS failed \
               FROM solana.instructions i JOIN solana.transactions t \
                 ON i.slot = t.slot AND i.tx_signature = t.tx_signature \
               WHERE i.slot >= {lo_slot} AND i.slot <= {hi_slot} AND t.slot >= {lo_slot} AND t.slot <= {hi_slot} \
               GROUP BY CAST(i.slot AS BIGINT) / {bucket_slots}, i.program_id), \
             ranked AS (\
               SELECT program, ROW_NUMBER() OVER (PARTITION BY bucket ORDER BY n DESC, program ASC) AS rk \
               FROM per WHERE bucket >= {from_bucket}), \
             chosen AS (SELECT DISTINCT program FROM ranked WHERE rk <= {TOP_K_PROGRAMS}) \
             SELECT per.bucket AS bucket, per.program AS program, per.n AS n, per.failed AS failed \
             FROM per JOIN chosen ON per.program = chosen.program ORDER BY program, bucket"
        )
    }
}

#[async_trait]
impl Detector for ProgramFailureRate {
    fn id(&self) -> &str {
        Self::ID
    }

    fn kind(&self) -> AnomalyKind {
        AnomalyKind::ProgramFailure
    }

    async fn scan(&self, ctx: &ScanContext) -> Result<ScanOutput, ScanError> {
        let lo = ctx.history_start();
        let sql = Self::sql(ctx.start_slot(lo), ctx.end_slot(ctx.to_bucket), ctx.bucket_slots, ctx.from_bucket);
        let response = ctx.query(&sql).await?;

        // program -> bucket -> (n, failed)
        let mut series: BTreeMap<String, BTreeMap<u64, (u64, u64)>> = BTreeMap::new();
        for row in &response.rows {
            let bucket = get_bucket(row, lo, ctx.to_bucket)?;
            let program = get_str(row, "program")?;
            let n = get_u64(row, "n")?;
            let failed = get_u64(row, "failed")?;
            if failed > n {
                return Err(protocol(format!("bucket {bucket}: failed {failed} above n {n}")));
            }
            series.entry(program.to_owned()).or_default().insert(bucket, (n, failed));
        }

        // Each scored bucket's top K among the returned programs, which hold
        // every program of the true top K (see the module docs).
        let mut top_k: HashMap<u64, BTreeSet<&str>> = HashMap::new();
        for bucket in ctx.from_bucket..=ctx.to_bucket {
            let mut active: Vec<(&str, u64)> = series
                .iter()
                .filter_map(|(program, buckets)| buckets.get(&bucket).map(|(n, _)| (program.as_str(), *n)))
                .collect();
            active.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
            top_k.insert(bucket, active.into_iter().take(TOP_K_PROGRAMS as usize).map(|(p, _)| p).collect());
        }

        let params = Self::SCORING.params(&ctx.params);
        let mut anomalies = Vec::new();
        for (program, buckets) in &series {
            if !usable_subject(program) {
                tracing::warn!(metric = Self::ID, "program id is not a usable subject; skipped");
                continue;
            }
            let values: BTreeMap<u64, f64> = buckets
                .iter()
                .filter(|(_, (n, _))| *n >= MIN_RATIO_DENOMINATOR)
                .map(|(bucket, (n, failed))| (*bucket, *failed as f64 / *n as f64))
                .collect();
            for f in score_series(ctx, &values, &params) {
                if !top_k.get(&f.bucket).is_some_and(|set| set.contains(program.as_str())) {
                    continue;
                }
                let (n, failed) = buckets[&f.bucket];
                let mut extra = Map::new();
                extra.insert("n".into(), json!(n));
                extra.insert("failed".into(), json!(failed));
                anomalies.push(series_anomaly(ctx, AnomalyKind::ProgramFailure, program.clone(), &f, &params, extra));
            }
        }
        Ok(ScanOutput { anomalies })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_bounds_tile_the_slots() {
        let bs = bucket_slots(60);
        assert_eq!(bs, 150);
        assert_eq!(bucket_slots(300), 750);
        for b in [0u64, 1, 7, 1_000_000] {
            assert_eq!(bucket_end(b, bs) + 1, bucket_start(b + 1, bs));
            assert_eq!(bucket_start(b, bs) / bs, b);
            assert_eq!(bucket_end(b, bs) / bs, b);
        }
    }

    #[test]
    fn lens_errors_map_to_what_the_runner_can_do() {
        let map = |err| ScanError::from(err).kind();
        let msg = |m: &str| m.to_owned();
        assert_eq!(
            map(LensCallError::BadQuery { message: msg("query_plan_error: table 'datafusion.solana.slots' not found") }),
            "not_ready"
        );
        assert_eq!(map(LensCallError::BadQuery { message: msg("sql_error: syntax") }), "permanent");
        assert_eq!(map(LensCallError::Protocol { message: msg("x") }), "permanent");
        assert_eq!(map(LensCallError::Timeout { message: msg("x") }), "transient");
        assert_eq!(map(LensCallError::Unavailable { message: msg("x") }), "transient");
    }

    #[test]
    fn sql_holds_only_integers_and_constants() {
        // Every generator takes integers only, so this pins the shapes.
        let sqls = [
            SeriesMetric::TxCount.sql(150, 449, 150),
            SeriesMetric::TransferCount.sql(150, 449, 150),
            TopMintShare::sql(150, 449, 150),
            TopMintShare::top_mint_sql(300, 449),
            FirstSeen::mints().sql(300, 449),
            FirstSeen::programs().sql(300, 449),
            ProgramFailureRate::sql(150, 449, 150, 2),
        ];
        for sql in sqls {
            assert!(sql.contains("449"), "{sql}");
            // The only quoted literal anywhere is the status constant.
            let quoted: Vec<&str> = sql.split('\'').skip(1).step_by(2).collect();
            assert!(quoted.iter().all(|q| *q == "failed"), "{sql}");
            assert!(!sql.contains(';'), "{sql}");
        }
    }

    #[test]
    fn detector_ids_are_unique_valid_metrics() {
        let detectors = default_detectors();
        let ids: BTreeSet<&str> = detectors.iter().map(|d| d.id()).collect();
        assert_eq!(ids.len(), detectors.len());
        for id in ids {
            assert!(id.chars().all(|c| c.is_ascii_lowercase() || c == '_'), "{id}");
        }
    }

    #[test]
    fn subjects_from_lens_are_screened() {
        assert!(usable_subject("So11111111111111111111111111111111111111112"));
        assert!(!usable_subject(""));
        assert!(!usable_subject(" padded"));
        assert!(!usable_subject("bell\u{7}"));
        assert!(!usable_subject(&"x".repeat(MAX_SUBJECT_CHARS + 1)));
    }
}
