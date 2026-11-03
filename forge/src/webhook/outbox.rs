//! The outbox: every read and write on the `deliveries` table.
//!
//! The registry creates the table (see `registry/mod.rs`); this module is
//! the only code that touches its rows. A row is one fired slot range of one
//! alert, and its payload is posted, byte for byte, until it is delivered or
//! its attempts run out.
//!
//! # Lifecycle
//!
//! ```text
//! enqueue ─┬─ with a hold ──▶ held ── release_up_to ──┐
//!          └─ no hold ────────────────────────────────┴──▶ pending ◀──────────────┐
//!                                                             │                    │
//!                                                       begin_attempt              │
//!                                                             ▼                    │
//!                                                        (in flight) ─ non-2xx ────┘
//!                                                          │      │    (attempts left)
//!                                                         2xx     └─ non-2xx, last attempt ─▶ failed
//!                                                          ▼                                    │
//!                                                      delivered            pending ◀─ requeue ─┘
//! ```
//!
//! * `held`: waiting for its `release_after_slot` to be finalized. The hold
//!   is a row state, not evaluator memory, so it survives restarts.
//! * `pending`: due once `next_attempt_at` has passed.
//! * `delivered` and `failed` are terminal; only an operator's
//!   [`Outbox::requeue`] takes a `failed` row back to `pending`.
//!
//! # Rules
//!
//! * **One row per range.** `UNIQUE (alert_id, to_slot)`: re-evaluating a
//!   range after a crash returns [`Enqueued::Duplicate`] instead of a second
//!   row. [`Outbox::enqueue`] runs in the caller's transaction, so the row
//!   commits together with the alert's progress.
//! * **The attempt is recorded before the request is sent.**
//!   [`Outbox::begin_attempt`] increments `attempts` and moves
//!   `next_attempt_at` one lease into the future, in one transaction. A crash
//!   mid-send therefore costs one attempt, and the row is due again when the
//!   lease runs out. The lease is `visibility_timeout_secs`, which the config
//!   keeps above the request timeout.
//! * **Guarded outcomes.** [`Outbox::record_success`] and
//!   [`Outbox::record_failure`] only apply while the row is still `pending`
//!   *and* still on the attempt they report. A late outcome of an older
//!   attempt is [`Recorded::Stale`] and changes nothing.
//! * **Bounded attempts.** The failure of attempt `max_attempts` is
//!   terminal. So is a row whose final attempt was interrupted by a crash:
//!   the next claim marks it `failed` instead of sending it again.
//! * **No secrets at rest in errors.** `last_error` is stripped of the
//!   alert's secret, its webhook URL and that URL's path and query, and of
//!   control characters, then capped at [`MAX_ERROR_LEN`] bytes.
//! * **Checked conversions.** Slots are `u64` and SQLite integers `i64`;
//!   neither direction ever wraps.
//!
//! Times are unix **milliseconds**, supplied by the caller (the service's
//! clock), so every method here is deterministic under test.

use std::collections::HashSet;
use std::time::Duration;

use rusqlite::types::Value;
use rusqlite::{OptionalExtension, Row, Transaction, params, params_from_iter};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::config::{ForgeError, ForgeResult};
use crate::registry::{Db, RegistryError, slot_to_sql};

/// Every delivery id is this prefix and 32 lowercase hex digits.
pub const DELIVERY_ID_PREFIX: &str = "dlv_";
/// Longest `last_error` stored, in bytes.
pub const MAX_ERROR_LEN: usize = 512;
/// Most rows [`Outbox::list`] returns at once.
pub const MAX_LIST_LIMIT: usize = 1000;
/// What sensitive text in `last_error` is replaced with.
pub const REDACTED: &str = "<redacted>";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Where a delivery is in its lifecycle; see the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryStatus {
    Held,
    Pending,
    Delivered,
    Failed,
}

impl DeliveryStatus {
    pub const ALL: [Self; 4] = [Self::Held, Self::Pending, Self::Delivered, Self::Failed];

    /// The value stored in the `status` column.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Pending => "pending",
            Self::Delivered => "delivered",
            Self::Failed => "failed",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|status| status.as_str() == raw)
    }

    /// `delivered` or `failed`.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Delivered | Self::Failed)
    }
}

/// A delivery to enqueue: what the evaluator produced for one slot range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewDelivery {
    pub alert_id: String,
    /// The range's last slot. Unique per alert.
    pub to_slot: u64,
    /// Serialized JSON, posted byte for byte. Checked to parse, otherwise
    /// opaque.
    pub payload: String,
    /// `Some(to_slot)` for a `wait_for_final` alert: hold the row until this
    /// slot is finalized. `None` to make it due at once.
    pub release_after_slot: Option<u64>,
}

impl NewDelivery {
    pub fn validate(&self) -> Result<(), RegistryError> {
        if self.alert_id.is_empty() {
            return Err(RegistryError::invalid("alert_id", "must not be empty"));
        }
        // Sent as `application/json`; an evaluator bug that produced
        // anything else would only ever fail at the receiver.
        serde_json::from_str::<serde::de::IgnoredAny>(&self.payload)
            .map_err(|err| RegistryError::invalid("payload", format!("is not JSON: {err}")))?;
        Ok(())
    }
}

/// What [`Outbox::enqueue`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Enqueued {
    /// A new row.
    Inserted { delivery_id: String },
    /// The alert already has a row for this `to_slot`; nothing was written.
    Duplicate { delivery_id: String },
}

impl Enqueued {
    pub fn delivery_id(&self) -> &str {
        match self {
            Self::Inserted { delivery_id } | Self::Duplicate { delivery_id } => delivery_id,
        }
    }
}

/// A `deliveries` row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Delivery {
    pub delivery_id: String,
    pub alert_id: String,
    pub to_slot: u64,
    pub payload: String,
    pub status: DeliveryStatus,
    pub release_after_slot: Option<u64>,
    /// Attempts started, including one in flight.
    pub attempts: u32,
    /// Unix ms. For `pending`: when the row is next due.
    pub next_attempt_at: i64,
    pub last_attempt_at: Option<i64>,
    pub delivered_at: Option<i64>,
    pub last_status_code: Option<u16>,
    pub last_error: Option<String>,
    pub created_at: i64,
}

/// A claimed delivery, joined with what the dispatcher needs to send it.
#[derive(Clone, PartialEq, Eq)]
pub struct Attempt {
    pub delivery_id: String,
    pub alert_id: String,
    pub to_slot: u64,
    pub payload: String,
    /// 1 for the first attempt; already recorded in `attempts`.
    pub attempt: u32,
    pub webhook_url: String,
    pub secret: String,
}

impl std::fmt::Debug for Attempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Attempt")
            .field("delivery_id", &self.delivery_id)
            .field("alert_id", &self.alert_id)
            .field("to_slot", &self.to_slot)
            .field("payload_len", &self.payload.len())
            .field("attempt", &self.attempt)
            .field("webhook_url", &REDACTED)
            .field("secret", &REDACTED)
            .finish()
    }
}

/// What an outcome update did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    Delivered,
    /// Back to `pending`, due at this unix ms.
    Retrying { next_attempt_at: i64 },
    /// Attempts exhausted; terminal.
    Failed,
    /// The row has moved on (another attempt, or a terminal state); nothing
    /// was written.
    Stale,
}

/// Row counts per status, for `/health`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct OutboxDepth {
    pub held: u64,
    pub pending: u64,
    pub delivered: u64,
    pub failed: u64,
    /// How long the oldest `pending` row has existed, in ms.
    pub oldest_pending_age_ms: Option<u64>,
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

/// The `deliveries` table, through the registry's shared connection.
#[derive(Debug, Clone)]
pub struct Outbox {
    db: Db,
    max_attempts: u32,
    lease_ms: i64,
}

impl Outbox {
    /// `max_attempts` counts the first attempt; `lease` is how long a claimed
    /// row stays invisible to [`Outbox::begin_attempt`].
    pub fn new(db: Db, max_attempts: u32, lease: Duration) -> ForgeResult<Self> {
        if max_attempts == 0 {
            return Err(ForgeError::config("webhook_max_attempts", "must be at least 1"));
        }
        let lease_ms = i64::try_from(lease.as_millis())
            .ok()
            .filter(|ms| *ms > 0)
            .ok_or_else(|| ForgeError::config("visibility_timeout_secs", "lease must be 1 ms to i64::MAX ms"))?;
        Ok(Self {
            db,
            max_attempts,
            lease_ms,
        })
    }

    pub const fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Insert `new` inside the caller's transaction `tx`, as `held` when it
    /// has a `release_after_slot` and `pending` (due at `now_ms`) otherwise.
    ///
    /// A second row for the same `(alert_id, to_slot)` is not written:
    /// [`Enqueued::Duplicate`] names the existing one. An unknown alert fails
    /// the foreign key, and so the caller's transaction.
    pub fn enqueue(tx: &Transaction<'_>, new: &NewDelivery, now_ms: i64) -> Result<Enqueued, RegistryError> {
        new.validate()?;
        let to_slot = slot_to_sql("to_slot", new.to_slot)?;
        let release_after_slot = new
            .release_after_slot
            .map(|slot| slot_to_sql("release_after_slot", slot))
            .transpose()?;
        let status = match release_after_slot {
            Some(_) => DeliveryStatus::Held,
            None => DeliveryStatus::Pending,
        };
        let delivery_id = new_delivery_id();
        let inserted = tx.execute(
            "INSERT INTO deliveries (delivery_id, alert_id, to_slot, payload, status, release_after_slot,
                                     attempts, next_attempt_at, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)
             ON CONFLICT (alert_id, to_slot) DO NOTHING",
            params![
                delivery_id,
                new.alert_id,
                to_slot,
                new.payload,
                status.as_str(),
                release_after_slot,
                now_ms
            ],
        )?;
        if inserted == 1 {
            return Ok(Enqueued::Inserted { delivery_id });
        }
        let existing = tx.query_row(
            "SELECT delivery_id FROM deliveries WHERE alert_id = ?1 AND to_slot = ?2",
            params![new.alert_id, to_slot],
            |row| row.get(0),
        )?;
        Ok(Enqueued::Duplicate { delivery_id: existing })
    }

    /// Move every `held` row whose `release_after_slot <= final_slot` to
    /// `pending`, due at `now_ms`. Idempotent; returns how many moved.
    pub fn release_up_to(&self, final_slot: u64, now_ms: i64) -> ForgeResult<usize> {
        self.db.with_conn("release held deliveries", |conn| {
            let final_slot = slot_to_sql("final_slot", final_slot)?;
            Ok(conn.execute(
                "UPDATE deliveries SET status = 'pending', next_attempt_at = ?2
                 WHERE status = 'held' AND release_after_slot <= ?1",
                params![final_slot, now_ms],
            )?)
        })
    }

    /// Claim the earliest due `pending` row (oldest first on a tie) whose
    /// alert is not in `busy_alerts`: count the attempt, stamp
    /// `last_attempt_at`, and push `next_attempt_at` one lease out, all
    /// before returning it. `None` when nothing is due.
    ///
    /// A due row that has already used every attempt (its last one was
    /// interrupted) is marked `failed` on the way, and never returned.
    pub fn begin_attempt(&self, now_ms: i64, busy_alerts: &HashSet<String>) -> ForgeResult<Option<Attempt>> {
        let mut sql = String::from(
            "SELECT d.delivery_id, d.alert_id, d.to_slot, d.payload, d.attempts, a.webhook_url, a.secret
             FROM deliveries d JOIN alerts a ON a.id = d.alert_id
             WHERE d.status = 'pending' AND d.next_attempt_at <= ?1",
        );
        let mut values = vec![Value::Integer(now_ms)];
        if !busy_alerts.is_empty() {
            let slots: Vec<String> = (0..busy_alerts.len()).map(|i| format!("?{}", i + 2)).collect();
            sql.push_str(&format!(" AND d.alert_id NOT IN ({})", slots.join(", ")));
            values.extend(busy_alerts.iter().map(|id| Value::Text(id.clone())));
        }
        sql.push_str(" ORDER BY d.next_attempt_at, d.created_at, d.rowid LIMIT 1");

        self.db.transaction("claim delivery", |tx| {
            loop {
                let Some(claim) = tx
                    .query_row(&sql, params_from_iter(values.iter()), read_claim)
                    .optional()?
                else {
                    return Ok(None);
                };
                let (mut attempt, done) = claim?;
                if done >= self.max_attempts {
                    tx.execute(
                        "UPDATE deliveries SET status = 'failed', last_error = ?2
                         WHERE delivery_id = ?1 AND status = 'pending'",
                        params![
                            attempt.delivery_id,
                            format!("gave up after {done} attempts; the last was interrupted before its outcome was recorded")
                        ],
                    )?;
                    continue;
                }
                attempt.attempt = done + 1;
                tx.execute(
                    "UPDATE deliveries SET attempts = ?2, last_attempt_at = ?3, next_attempt_at = ?4
                     WHERE delivery_id = ?1",
                    params![attempt.delivery_id, attempt.attempt, now_ms, now_ms.saturating_add(self.lease_ms)],
                )?;
                return Ok(Some(attempt));
            }
        })
    }

    /// Attempt `attempt` got a 2xx: `pending` to `delivered`.
    pub fn record_success(&self, delivery_id: &str, attempt: u32, status_code: u16, now_ms: i64) -> ForgeResult<Recorded> {
        self.db.with_conn("record delivery success", |conn| {
            let changed = conn.execute(
                "UPDATE deliveries
                 SET status = 'delivered', delivered_at = ?3, last_status_code = ?4, last_error = NULL
                 WHERE delivery_id = ?1 AND status = 'pending' AND attempts = ?2",
                params![delivery_id, attempt, now_ms, status_code],
            )?;
            Ok(if changed == 1 { Recorded::Delivered } else { Recorded::Stale })
        })
    }

    /// Attempt `attempt` failed with `error` (and the response's
    /// `status_code`, if one came back). Due again at `retry_at_ms`, or
    /// `failed` if that was the last attempt.
    pub fn record_failure(
        &self,
        delivery_id: &str,
        attempt: u32,
        status_code: Option<u16>,
        error: &str,
        retry_at_ms: i64,
    ) -> ForgeResult<Recorded> {
        self.db.transaction("record delivery failure", |tx| {
            let target: Option<(String, String)> = tx
                .query_row(
                    "SELECT a.webhook_url, a.secret FROM deliveries d JOIN alerts a ON a.id = d.alert_id
                     WHERE d.delivery_id = ?1 AND d.status = 'pending' AND d.attempts = ?2",
                    params![delivery_id, attempt],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((webhook_url, secret)) = target else {
                return Ok(Recorded::Stale);
            };
            let error = clean_error(error, &webhook_url, &secret);
            let outcome = if attempt >= self.max_attempts {
                Recorded::Failed
            } else {
                Recorded::Retrying {
                    next_attempt_at: retry_at_ms,
                }
            };
            let (status, next) = match outcome {
                Recorded::Retrying { next_attempt_at } => (DeliveryStatus::Pending, Some(next_attempt_at)),
                _ => (DeliveryStatus::Failed, None),
            };
            tx.execute(
                "UPDATE deliveries
                 SET status = ?2, next_attempt_at = COALESCE(?3, next_attempt_at),
                     last_status_code = ?4, last_error = ?5
                 WHERE delivery_id = ?1",
                params![delivery_id, status.as_str(), next, status_code, error],
            )?;
            Ok(outcome)
        })
    }

    /// An operator retry: `failed` to `pending`, attempts reset, due at
    /// `now_ms`. Any other status is a [`RegistryError::Conflict`].
    pub fn requeue(&self, delivery_id: &str, now_ms: i64) -> ForgeResult<Delivery> {
        self.db.transaction("requeue delivery", |tx| {
            let changed = tx.execute(
                "UPDATE deliveries SET status = 'pending', attempts = 0, next_attempt_at = ?2
                 WHERE delivery_id = ?1 AND status = 'failed'",
                params![delivery_id, now_ms],
            )?;
            let delivery = select_delivery(tx, delivery_id)?;
            if changed == 0 {
                return Err(RegistryError::conflict(format!(
                    "delivery {delivery_id} is {}; only failed deliveries can be requeued",
                    delivery.status.as_str()
                )));
            }
            Ok(delivery)
        })
    }

    pub fn get(&self, delivery_id: &str) -> ForgeResult<Delivery> {
        self.db.with_conn("get delivery", |conn| select_delivery(conn, delivery_id))
    }

    /// Up to `limit` (1 to [`MAX_LIST_LIMIT`]) of an alert's deliveries,
    /// newest first (insertion order within one millisecond). An unknown
    /// alert has none.
    pub fn list(&self, alert_id: &str, limit: usize) -> ForgeResult<Vec<Delivery>> {
        self.db.with_conn("list deliveries", |conn| {
            if !(1..=MAX_LIST_LIMIT).contains(&limit) {
                return Err(RegistryError::invalid("limit", format!("must be 1 to {MAX_LIST_LIMIT}, got {limit}")));
            }
            let mut stmt = conn.prepare(&format!(
                "SELECT {DELIVERY_COLUMNS} FROM deliveries WHERE alert_id = ?1
                 ORDER BY created_at DESC, rowid DESC LIMIT ?2"
            ))?;
            let rows = stmt.query_map(params![alert_id, limit as i64], read_delivery)?;
            rows.map(|row| row?).collect()
        })
    }

    /// Rows per status, and the age of the oldest `pending` row at `now_ms`.
    pub fn depth(&self, now_ms: i64) -> ForgeResult<OutboxDepth> {
        self.db.with_conn("outbox depth", |conn| {
            let mut depth = OutboxDepth::default();
            let mut stmt = conn.prepare("SELECT status, COUNT(*) FROM deliveries GROUP BY status")?;
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                let status: String = row.get(0)?;
                let count = u64::try_from(row.get::<_, i64>(1)?).unwrap_or_default();
                match DeliveryStatus::parse(&status) {
                    Some(DeliveryStatus::Held) => depth.held = count,
                    Some(DeliveryStatus::Pending) => depth.pending = count,
                    Some(DeliveryStatus::Delivered) => depth.delivered = count,
                    Some(DeliveryStatus::Failed) => depth.failed = count,
                    None => return Err(RegistryError::storage(format!("unknown delivery status {status:?}"))),
                }
            }
            let oldest: Option<i64> =
                conn.query_row("SELECT MIN(created_at) FROM deliveries WHERE status = 'pending'", [], |row| row.get(0))?;
            depth.oldest_pending_age_ms = oldest.map(|created| u64::try_from(now_ms.saturating_sub(created)).unwrap_or(0));
            Ok(depth)
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// `dlv_` and a UUID v4 as 32 lowercase hex digits.
fn new_delivery_id() -> String {
    format!("{DELIVERY_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

/// `error` made safe to store: the alert's secret, its URL, and that URL's
/// path and query replaced by [`REDACTED`], control characters turned into
/// spaces, and the result capped at [`MAX_ERROR_LEN`] bytes.
pub(crate) fn clean_error(error: &str, webhook_url: &str, secret: &str) -> String {
    let mut text = redact(error, webhook_url, secret);
    text = text.chars().map(|c| if c.is_control() { ' ' } else { c }).collect();
    truncate(&mut text, MAX_ERROR_LEN);
    text
}

/// `text` with the secret, the URL (as given and normalized), and its path
/// and query replaced by [`REDACTED`]. Longest first, so a URL is redacted
/// whole rather than around a redacted path.
pub(crate) fn redact(text: &str, webhook_url: &str, secret: &str) -> String {
    let mut needles = vec![secret.to_owned(), webhook_url.to_owned()];
    if let Ok(url) = Url::parse(webhook_url) {
        needles.push(url.as_str().to_owned());
        needles.push(url.as_str().trim_end_matches('/').to_owned());
        if url.path().len() > 1 {
            needles.push(url.path().to_owned());
        }
        if let Some(query) = url.query().filter(|q| !q.is_empty()) {
            needles.push(query.to_owned());
        }
    }
    needles.retain(|needle| !needle.is_empty());
    needles.sort_by_key(|needle| std::cmp::Reverse(needle.len()));
    let mut text = text.to_owned();
    for needle in &needles {
        if text.contains(needle.as_str()) {
            text = text.replace(needle.as_str(), REDACTED);
        }
    }
    text
}

/// Cut `text` to at most `max` bytes, on a char boundary.
pub(crate) fn truncate(text: &mut String, max: usize) {
    if text.len() > max {
        let mut cut = max;
        while !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
    }
}

fn read_claim(row: &Row<'_>) -> rusqlite::Result<Result<(Attempt, u32), RegistryError>> {
    let delivery_id: String = row.get(0)?;
    let (Ok(to_slot), Ok(done)) = (u64::try_from(row.get::<_, i64>(2)?), u32::try_from(row.get::<_, i64>(4)?)) else {
        return Ok(Err(RegistryError::storage(format!("delivery {delivery_id} has an out-of-range to_slot or attempts"))));
    };
    Ok(Ok((
        Attempt {
            delivery_id,
            alert_id: row.get(1)?,
            to_slot,
            payload: row.get(3)?,
            attempt: 0,
            webhook_url: row.get(5)?,
            secret: row.get(6)?,
        },
        done,
    )))
}

const DELIVERY_COLUMNS: &str = "delivery_id, alert_id, to_slot, payload, status, release_after_slot, attempts, \
                                next_attempt_at, last_attempt_at, delivered_at, last_status_code, last_error, created_at";

fn select_delivery(conn: &rusqlite::Connection, delivery_id: &str) -> Result<Delivery, RegistryError> {
    conn.query_row(
        &format!("SELECT {DELIVERY_COLUMNS} FROM deliveries WHERE delivery_id = ?1"),
        [delivery_id],
        read_delivery,
    )
    .optional()?
    .ok_or_else(|| RegistryError::not_found("delivery", delivery_id))?
}

fn read_delivery(row: &Row<'_>) -> rusqlite::Result<Result<Delivery, RegistryError>> {
    let delivery_id: String = row.get(0)?;
    let corrupt = |what: &str| Ok(Err(RegistryError::storage(format!("delivery {delivery_id} has an unreadable {what}"))));
    let Ok(to_slot) = u64::try_from(row.get::<_, i64>(2)?) else {
        return corrupt("to_slot");
    };
    let status: String = row.get(4)?;
    let Some(status) = DeliveryStatus::parse(&status) else {
        return corrupt("status");
    };
    let Ok(release_after_slot) = row.get::<_, Option<i64>>(5)?.map(u64::try_from).transpose() else {
        return corrupt("release_after_slot");
    };
    let Ok(attempts) = u32::try_from(row.get::<_, i64>(6)?) else {
        return corrupt("attempts");
    };
    let Ok(last_status_code) = row.get::<_, Option<i64>>(10)?.map(u16::try_from).transpose() else {
        return corrupt("last_status_code");
    };
    Ok(Ok(Delivery {
        delivery_id,
        alert_id: row.get(1)?,
        to_slot,
        payload: row.get(3)?,
        status,
        release_after_slot,
        attempts,
        next_attempt_at: row.get(7)?,
        last_attempt_at: row.get(8)?,
        delivered_at: row.get(9)?,
        last_status_code,
        last_error: row.get(11)?,
        created_at: row.get(12)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "s3cr3t-s3cr3t-16";
    const URL: &str = "https://hooks.example.com/services/T0/B1/XyZ?token=abc123";
    const LEASE: Duration = Duration::from_secs(60);

    fn outbox(max_attempts: u32) -> Outbox {
        let db = Db::open_in_memory();
        db.with_conn("seed", |conn| {
            for id in ["a1", "a2"] {
                conn.execute(
                    "INSERT INTO alerts (id, name, sql, webhook_url, secret, wait_for_final, enabled, created_at, updated_at)
                     VALUES (?1, ?1, 'x', ?2, ?3, 0, 1, 0, 0)",
                    params![id, URL, SECRET],
                )?;
            }
            Ok(())
        })
        .unwrap();
        Outbox::new(db, max_attempts, LEASE).unwrap()
    }

    fn new(alert_id: &str, to_slot: u64, hold: bool) -> NewDelivery {
        NewDelivery {
            alert_id: alert_id.into(),
            to_slot,
            payload: format!(r#"{{"to_slot":{to_slot}}}"#),
            release_after_slot: hold.then_some(to_slot),
        }
    }

    fn enqueue(outbox: &Outbox, new: &NewDelivery, now: i64) -> Result<Enqueued, ForgeError> {
        outbox.db.transaction("enqueue", |tx| Outbox::enqueue(tx, new, now))
    }

    fn none() -> HashSet<String> {
        HashSet::new()
    }

    #[test]
    fn statuses_round_trip() {
        for status in DeliveryStatus::ALL {
            assert_eq!(DeliveryStatus::parse(status.as_str()), Some(status));
        }
        assert_eq!(DeliveryStatus::parse("Pending"), None);
        assert!(DeliveryStatus::Delivered.is_terminal() && DeliveryStatus::Failed.is_terminal());
        assert!(!DeliveryStatus::Held.is_terminal() && !DeliveryStatus::Pending.is_terminal());
    }

    #[test]
    fn construction_is_checked() {
        let db = Db::open_in_memory();
        assert!(Outbox::new(db.clone(), 0, LEASE).is_err());
        assert!(Outbox::new(db.clone(), 1, Duration::ZERO).is_err());
        assert!(Outbox::new(db.clone(), 1, Duration::MAX).is_err());
        assert!(Outbox::new(db, 1, Duration::from_millis(1)).is_ok());
    }

    #[test]
    fn enqueue_inserts_once_per_range() {
        let outbox = outbox(3);
        let first = enqueue(&outbox, &new("a1", 10, false), 5).unwrap();
        let Enqueued::Inserted { delivery_id } = &first else { panic!("{first:?}") };
        assert!(delivery_id.starts_with(DELIVERY_ID_PREFIX) && delivery_id.len() == 36, "{delivery_id}");

        let again = enqueue(&outbox, &new("a1", 10, false), 6).unwrap();
        assert_eq!(again, Enqueued::Duplicate { delivery_id: delivery_id.clone() });
        // Same slot, other alert: a separate range.
        assert!(matches!(enqueue(&outbox, &new("a2", 10, false), 6).unwrap(), Enqueued::Inserted { .. }));

        let row = outbox.get(delivery_id).unwrap();
        assert_eq!((row.status, row.attempts, row.next_attempt_at, row.created_at), (DeliveryStatus::Pending, 0, 5, 5));
    }

    #[test]
    fn enqueue_validates() {
        let outbox = outbox(3);
        let mut bad = new("a1", 1, false);
        bad.payload = "{not json".into();
        assert_eq!(enqueue(&outbox, &bad, 0).unwrap_err().as_registry_error().unwrap().http_status(), 400);
        assert!(enqueue(&outbox, &new("", 1, false), 0).is_err());
        assert!(enqueue(&outbox, &new("a1", u64::MAX, false), 0).is_err());
        let mut far = new("a1", 1, false);
        far.release_after_slot = Some(i64::MAX as u64 + 1);
        assert!(enqueue(&outbox, &far, 0).is_err());
        // Foreign key.
        assert_eq!(enqueue(&outbox, &new("nope", 1, false), 0).unwrap_err().as_registry_error().unwrap().http_status(), 500);
    }

    #[test]
    fn held_rows_wait_for_release() {
        let outbox = outbox(3);
        let held = enqueue(&outbox, &new("a1", 10, true), 0).unwrap();
        enqueue(&outbox, &new("a1", 20, true), 0).unwrap();
        assert_eq!(outbox.get(held.delivery_id()).unwrap().status, DeliveryStatus::Held);
        assert_eq!(outbox.begin_attempt(i64::MAX, &none()).unwrap(), None, "held rows are never claimed");

        assert_eq!(outbox.release_up_to(9, 100).unwrap(), 0);
        assert_eq!(outbox.release_up_to(10, 100).unwrap(), 1);
        assert_eq!(outbox.release_up_to(10, 200).unwrap(), 0, "idempotent");
        let row = outbox.get(held.delivery_id()).unwrap();
        assert_eq!((row.status, row.next_attempt_at), (DeliveryStatus::Pending, 100));
        assert_eq!(outbox.release_up_to(1_000, 300).unwrap(), 1);
        assert!(outbox.release_up_to(u64::MAX, 0).is_err());
    }

    #[test]
    fn begin_attempt_records_before_returning_and_leases() {
        let outbox = outbox(3);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        assert_eq!(outbox.begin_attempt(-1, &none()).unwrap(), None, "not yet due");

        let attempt = outbox.begin_attempt(10, &none()).unwrap().unwrap();
        assert_eq!((attempt.attempt, attempt.webhook_url.as_str(), attempt.secret.as_str()), (1, URL, SECRET));
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.attempts, row.last_attempt_at, row.next_attempt_at), (1, Some(10), 10 + 60_000));

        // Leased: invisible until the lease runs out.
        assert_eq!(outbox.begin_attempt(10 + 59_999, &none()).unwrap(), None);
        let again = outbox.begin_attempt(10 + 60_000, &none()).unwrap().unwrap();
        assert_eq!(again.attempt, 2);
    }

    #[test]
    fn begin_attempt_skips_busy_alerts_and_takes_the_oldest() {
        let outbox = outbox(3);
        enqueue(&outbox, &new("a1", 1, false), 5).unwrap();
        enqueue(&outbox, &new("a2", 1, false), 3).unwrap();
        enqueue(&outbox, &new("a1", 2, false), 1).unwrap();
        let first = outbox.begin_attempt(10, &none()).unwrap().unwrap();
        assert_eq!((first.alert_id.as_str(), first.to_slot), ("a1", 2));
        let busy = HashSet::from(["a1".to_owned()]);
        let second = outbox.begin_attempt(10, &busy).unwrap().unwrap();
        assert_eq!(second.alert_id, "a2");
        let busy = HashSet::from(["a1".to_owned(), "a2".to_owned()]);
        assert_eq!(outbox.begin_attempt(10, &busy).unwrap(), None);
    }

    #[test]
    fn outcomes_are_guarded_by_attempt() {
        let outbox = outbox(5);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        let first = outbox.begin_attempt(0, &none()).unwrap().unwrap();
        // The lease expires and attempt 2 is claimed while attempt 1 is
        // still out: attempt 1's outcome must not land.
        let second = outbox.begin_attempt(60_000, &none()).unwrap().unwrap();
        assert_eq!(outbox.record_success(&id, first.attempt, 200, 1).unwrap(), Recorded::Stale);
        assert_eq!(outbox.record_failure(&id, first.attempt, Some(500), "x", 9).unwrap(), Recorded::Stale);
        assert_eq!(outbox.get(&id).unwrap().last_status_code, None);

        assert_eq!(outbox.record_success(&id, second.attempt, 204, 70_000).unwrap(), Recorded::Delivered);
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.status, row.delivered_at, row.last_status_code), (DeliveryStatus::Delivered, Some(70_000), Some(204)));
        // Terminal: nothing moves it.
        assert_eq!(outbox.record_success(&id, second.attempt, 200, 1).unwrap(), Recorded::Stale);
        assert_eq!(outbox.record_failure(&id, second.attempt, None, "x", 1).unwrap(), Recorded::Stale);
        assert_eq!(outbox.get(&id).unwrap(), row);
    }

    #[test]
    fn failures_retry_until_the_last_attempt() {
        let outbox = outbox(2);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        let a = outbox.begin_attempt(0, &none()).unwrap().unwrap();
        assert_eq!(
            outbox.record_failure(&id, a.attempt, Some(503), "HTTP 503", 1_000).unwrap(),
            Recorded::Retrying { next_attempt_at: 1_000 }
        );
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.status, row.next_attempt_at, row.last_status_code), (DeliveryStatus::Pending, 1_000, Some(503)));
        assert_eq!(outbox.begin_attempt(999, &none()).unwrap(), None);

        let b = outbox.begin_attempt(1_000, &none()).unwrap().unwrap();
        assert_eq!(outbox.record_failure(&id, b.attempt, None, "connection refused", 2_000).unwrap(), Recorded::Failed);
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.status, row.attempts, row.last_status_code), (DeliveryStatus::Failed, 2, None));
        assert_eq!(row.last_error.as_deref(), Some("connection refused"));
        assert_eq!(outbox.begin_attempt(i64::MAX, &none()).unwrap(), None);
    }

    #[test]
    fn an_interrupted_last_attempt_is_failed_not_resent() {
        let outbox = outbox(1);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        outbox.begin_attempt(0, &none()).unwrap().unwrap();
        // The process dies here; no outcome is ever recorded. Another row
        // falls due as the lease ends; the exhausted row is older, so it is
        // visited first.
        enqueue(&outbox, &new("a1", 2, false), 60_000).unwrap();
        let next = outbox.begin_attempt(60_000, &none()).unwrap().unwrap();
        assert_eq!(next.to_slot, 2, "the exhausted row is skipped");
        let row = outbox.get(&id).unwrap();
        assert_eq!((row.status, row.attempts), (DeliveryStatus::Failed, 1));
        assert!(row.last_error.unwrap().contains("interrupted"));
    }

    #[test]
    fn requeue_only_failed_rows() {
        let outbox = outbox(1);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        let status = |err: ForgeError| err.as_registry_error().unwrap().http_status();
        assert_eq!(status(outbox.requeue(&id, 0).unwrap_err()), 409);
        assert_eq!(status(outbox.requeue("dlv_nope", 0).unwrap_err()), 404);

        let a = outbox.begin_attempt(0, &none()).unwrap().unwrap();
        outbox.record_failure(&id, a.attempt, Some(500), "boom", 0).unwrap();
        let row = outbox.requeue(&id, 42).unwrap();
        assert_eq!((row.status, row.attempts, row.next_attempt_at), (DeliveryStatus::Pending, 0, 42));
        assert_eq!(outbox.begin_attempt(42, &none()).unwrap().unwrap().attempt, 1);
    }

    #[test]
    fn last_error_never_holds_secrets_or_urls() {
        let outbox = outbox(3);
        let id = enqueue(&outbox, &new("a1", 1, false), 0).unwrap().delivery_id().to_owned();
        let a = outbox.begin_attempt(0, &none()).unwrap().unwrap();
        let nasty = format!(
            "error for {URL}: echoed {SECRET}; path /services/T0/B1/XyZ; query token=abc123\r\n{}",
            "x".repeat(2 * MAX_ERROR_LEN)
        );
        outbox.record_failure(&id, a.attempt, Some(400), &nasty, 1).unwrap();
        let stored = outbox.get(&id).unwrap().last_error.unwrap();
        for leak in [SECRET, URL, "hooks.example.com/services", "/services/T0/B1/XyZ", "token=abc123"] {
            assert!(!stored.contains(leak), "{leak:?} in {stored:?}");
        }
        assert!(stored.len() <= MAX_ERROR_LEN);
        assert!(!stored.chars().any(char::is_control), "{stored:?}");
    }

    #[test]
    fn truncate_respects_char_boundaries() {
        let mut text = "ééé".to_owned();
        truncate(&mut text, 3);
        assert_eq!(text, "é");
        let mut short = "ab".to_owned();
        truncate(&mut short, 3);
        assert_eq!(short, "ab");
    }

    #[test]
    fn ties_within_one_millisecond_keep_insertion_order() {
        let outbox = outbox(3);
        let ids: Vec<String> = (1..=20)
            .map(|slot| enqueue(&outbox, &new("a1", slot, false), 7).unwrap().delivery_id().to_owned())
            .collect();
        let listed: Vec<String> = outbox.list("a1", 20).unwrap().into_iter().map(|d| d.delivery_id).collect();
        assert_eq!(listed, ids.iter().rev().cloned().collect::<Vec<_>>());
        let first = outbox.begin_attempt(7, &none()).unwrap().unwrap();
        assert_eq!(first.delivery_id, ids[0]);
    }

    #[test]
    fn list_and_depth() {
        let outbox = outbox(1);
        for slot in 1..=3 {
            enqueue(&outbox, &new("a1", slot, false), slot as i64 * 10).unwrap();
        }
        enqueue(&outbox, &new("a2", 1, true), 0).unwrap();
        let listed = outbox.list("a1", 2).unwrap();
        assert_eq!(listed.iter().map(|d| d.to_slot).collect::<Vec<_>>(), [3, 2]);
        assert!(outbox.list("a1", 0).is_err());
        assert!(outbox.list("a1", MAX_LIST_LIMIT + 1).is_err());
        assert!(outbox.list("nope", 10).unwrap().is_empty());

        let a = outbox.begin_attempt(100, &none()).unwrap().unwrap();
        outbox.record_success(&a.delivery_id, a.attempt, 200, 100).unwrap();
        let b = outbox.begin_attempt(100, &none()).unwrap().unwrap();
        outbox.record_failure(&b.delivery_id, b.attempt, None, "x", 0).unwrap();
        assert_eq!(
            outbox.depth(100).unwrap(),
            OutboxDepth {
                held: 1,
                pending: 1,
                delivered: 1,
                failed: 1,
                oldest_pending_age_ms: Some(70)
            }
        );
        assert_eq!(Outbox::new(Db::open_in_memory(), 1, LEASE).unwrap().depth(0).unwrap(), OutboxDepth::default());
    }

    #[test]
    fn attempt_debug_redacts() {
        let outbox = outbox(1);
        enqueue(&outbox, &new("a1", 1, false), 0).unwrap();
        let a = outbox.begin_attempt(0, &none()).unwrap().unwrap();
        let shown = format!("{a:?}");
        assert!(!shown.contains(SECRET) && !shown.contains("hooks.example.com"), "{shown}");
    }
}
