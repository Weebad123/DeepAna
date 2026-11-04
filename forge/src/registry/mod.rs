//! The registry: one embedded SQLite file holding all of FORGE's state
//! (saved queries, alerts, the webhook outbox's deliveries, and the alert
//! engine's target slot).
//!
//! # One database, one connection
//!
//! An alert's `last_evaluated_slot` must advance in the **same transaction**
//! that inserts its outbox row. If the two writes could land separately, a
//! crash between them would either fire the alert again on restart (progress
//! lost, row kept) or never fire it at all (progress kept, row lost). So both
//! tables live in this one file behind one connection, shared through
//! [`Db`]. The registry owns the schema, `deliveries` included:
//! `webhook/outbox.rs` reads and writes that table but never creates it.
//!
//! # Opening
//!
//! [`Registry::open`]:
//!
//! 1. creates `config.db_path` if missing (its parent directory was checked
//!    by `config.rs`). On Unix the file is created mode `0600`, since alert
//!    secrets are stored in plaintext.
//! 2. takes an exclusive advisory lock on the sidecar `{db_path}.lock`, as
//!    PRISM's `LocalChangeLog` does on its file, so two FORGE processes never
//!    share one database,
//! 3. sets the connection pragmas (WAL, `synchronous = FULL`, foreign keys,
//!    a busy timeout),
//! 4. runs migrations.
//!
//! # Migrations
//!
//! `PRAGMA user_version` is the schema version. [`MIGRATIONS`]`[n]` takes the
//! schema from version `n` to `n + 1`. Each migration runs in its own
//! transaction together with the version bump, so a crash mid-migration
//! leaves the previous version intact. A database newer than this binary is
//! refused.
//!
//! # Errors
//!
//! Every public operation returns a [`ForgeError::RegistryError`] whose
//! source is a [`RegistryError`]. [`ForgeError::as_registry_error`] recovers
//! it, and [`RegistryError::http_status`] gives `api/` its status code
//! without string matching.

pub mod alert;
pub mod saved_query;

pub use alert::*;
pub use saved_query::*;

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, ErrorCode, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use thiserror::Error;
use tracing::info;

use crate::config::{BoxError, ForgeConfig, ForgeError, ForgeResult};

/// Suffix of the sidecar file [`Registry::open`] locks.
pub const LOCK_FILE_SUFFIX: &str = ".lock";

/// How long a statement waits on a lock held by another connection.
pub const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Schema migrations, oldest first. Never edit a released entry; append one.
pub const MIGRATIONS: &[&str] = &[
    // 1: saved queries, alerts, and the webhook outbox.
    r#"
    CREATE TABLE saved_queries (
        name        TEXT    PRIMARY KEY NOT NULL,
        sql         TEXT    NOT NULL,
        params_json TEXT    NOT NULL,
        created_at  INTEGER NOT NULL,
        updated_at  INTEGER NOT NULL
    ) STRICT;

    CREATE TABLE alerts (
        id                  TEXT    PRIMARY KEY NOT NULL,
        name                TEXT    NOT NULL UNIQUE,
        sql                 TEXT    NOT NULL,
        webhook_url         TEXT    NOT NULL,
        secret              TEXT    NOT NULL,
        wait_for_final      INTEGER NOT NULL CHECK (wait_for_final IN (0, 1)),
        enabled             INTEGER NOT NULL CHECK (enabled IN (0, 1)),
        last_evaluated_slot INTEGER          CHECK (last_evaluated_slot >= 0),
        created_at          INTEGER NOT NULL,
        updated_at          INTEGER NOT NULL
    ) STRICT;

    -- One row per fired range; see webhook/outbox.rs. Times are unix
    -- milliseconds (alerts and saved queries use seconds).
    CREATE TABLE deliveries (
        delivery_id        TEXT    PRIMARY KEY NOT NULL,
        alert_id           TEXT    NOT NULL REFERENCES alerts (id) ON DELETE CASCADE,
        to_slot            INTEGER NOT NULL CHECK (to_slot >= 0),
        payload            TEXT    NOT NULL,
        status             TEXT    NOT NULL CHECK (status IN ('held', 'pending', 'delivered', 'failed')),
        release_after_slot INTEGER          CHECK (release_after_slot >= 0),
        attempts           INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
        next_attempt_at    INTEGER NOT NULL,
        last_attempt_at    INTEGER,
        delivered_at       INTEGER,
        last_status_code   INTEGER          CHECK (last_status_code BETWEEN 100 AND 999),
        last_error         TEXT,
        created_at         INTEGER NOT NULL,
        -- A range fires at most once, however often it is re-evaluated.
        UNIQUE (alert_id, to_slot),
        CHECK (status <> 'held' OR release_after_slot IS NOT NULL),
        CHECK ((status = 'delivered') = (delivered_at IS NOT NULL))
    ) STRICT;

    -- The dispatcher's due query, and the per-alert history (which also
    -- serves the cascade from alerts).
    CREATE INDEX deliveries_by_status_due ON deliveries (status, next_attempt_at);
    CREATE INDEX deliveries_by_alert ON deliveries (alert_id, created_at);
    "#,
    // 2: the alert engine's target, so a restart can resume alerts that
    // were behind it without waiting for PRISM's next event.
    r#"
    CREATE TABLE engine_state (
        id          INTEGER PRIMARY KEY NOT NULL CHECK (id = 1),
        target_slot INTEGER NOT NULL CHECK (target_slot >= 0),
        updated_at  INTEGER NOT NULL
    ) STRICT;
    "#,
];

// ---------------------------------------------------------------------------
// RegistryError
// ---------------------------------------------------------------------------

/// Why a registry operation failed. Always carried as the source of a
/// [`ForgeError::RegistryError`].
#[derive(Debug, Error)]
pub enum RegistryError {
    /// Client input failed validation. HTTP 400.
    #[error("invalid {field}: {reason}")]
    Invalid { field: String, reason: String },

    /// No saved query or alert has that key. HTTP 404.
    #[error("{entity} {key:?} not found")]
    NotFound { entity: &'static str, key: String },

    /// The write clashes with current state: a duplicate name, or progress
    /// that would not move forward. HTTP 409.
    #[error("conflict: {reason}")]
    Conflict { reason: String },

    /// SQLite failed, or stored data is unreadable. HTTP 500.
    #[error("storage failure: {0}")]
    Storage(BoxError),
}

impl RegistryError {
    pub fn invalid(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Invalid {
            field: field.into(),
            reason: reason.into(),
        }
    }

    pub fn not_found(entity: &'static str, key: impl Into<String>) -> Self {
        Self::NotFound { entity, key: key.into() }
    }

    pub fn conflict(reason: impl Into<String>) -> Self {
        Self::Conflict { reason: reason.into() }
    }

    pub fn storage(source: impl Into<BoxError>) -> Self {
        Self::Storage(source.into())
    }

    /// The HTTP status `api/` answers with.
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::Invalid { .. } => 400,
            Self::NotFound { .. } => 404,
            Self::Conflict { .. } => 409,
            Self::Storage(_) => 500,
        }
    }
}

// Unlike `ForgeError`, a storage failure needs no context of its own (the
// enclosing `ForgeError::RegistryError` names the operation), so `?` on a
// rusqlite call inside a transaction closure is fine.
impl From<rusqlite::Error> for RegistryError {
    fn from(err: rusqlite::Error) -> Self {
        Self::storage(err)
    }
}

impl ForgeError {
    /// The [`RegistryError`] behind a [`ForgeError::RegistryError`].
    pub fn as_registry_error(&self) -> Option<&RegistryError> {
        match self {
            Self::RegistryError { source, .. } => source.downcast_ref(),
            _ => None,
        }
    }
}

fn is_unique_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _)
            if e.code == ErrorCode::ConstraintViolation
                && matches!(e.extended_code, rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE | rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY)
    )
}

/// Slots are `u64`; SQLite integers are `i64`. No real slot comes near the
/// boundary, but one must never wrap silently.
pub(crate) fn slot_to_sql(field: &str, slot: u64) -> Result<i64, RegistryError> {
    i64::try_from(slot).map_err(|_| RegistryError::invalid(field, format!("slot {slot} exceeds {}", i64::MAX)))
}

pub(crate) fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

// ---------------------------------------------------------------------------
// Db
// ---------------------------------------------------------------------------

/// The shared handle on FORGE's one SQLite connection. Cheap to clone;
/// `webhook/outbox.rs` holds one too.
#[derive(Clone)]
pub struct Db {
    conn: Arc<Mutex<Connection>>,
}

impl Db {
    fn new(conn: Connection) -> Self {
        Self {
            conn: Arc::new(Mutex::new(conn)),
        }
    }

    /// A migrated in-memory database, for unit tests elsewhere in the crate.
    #[cfg(test)]
    pub(crate) fn open_in_memory() -> Self {
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        migrate(&mut conn).unwrap();
        Self::new(conn)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // A panic mid-transaction drops its `Transaction`, which rolls back,
        // so the connection is still consistent.
        self.conn.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Run `f` on the connection outside any explicit transaction.
    pub fn with_conn<T>(
        &self,
        operation: &str,
        f: impl FnOnce(&Connection) -> Result<T, RegistryError>,
    ) -> ForgeResult<T> {
        f(&self.lock()).map_err(|err| ForgeError::registry(operation, err))
    }

    /// Run `f` in one `BEGIN IMMEDIATE` transaction: commit if it returns
    /// `Ok`, roll back if it returns `Err` or panics.
    ///
    /// This is how the evaluator advances an alert and enqueues its delivery
    /// together:
    ///
    /// ```ignore
    /// db.transaction("fire alert", |tx| {
    ///     Registry::advance_progress(tx, &alert.id, to_slot)?;
    ///     webhook.enqueue(tx, &delivery)?;
    ///     Ok(())
    /// })?;
    /// webhook.wake();
    /// ```
    ///
    /// `IMMEDIATE` takes the write lock up front, so the transaction cannot
    /// fail half-way with `SQLITE_BUSY` when it first writes.
    pub fn transaction<T>(
        &self,
        operation: &str,
        f: impl FnOnce(&Transaction<'_>) -> Result<T, RegistryError>,
    ) -> ForgeResult<T> {
        let mut conn = self.lock();
        let run = |conn: &mut Connection| {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let value = f(&tx)?;
            tx.commit()?;
            Ok(value)
        };
        run(&mut conn).map_err(|err: RegistryError| ForgeError::registry(operation, err))
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db").finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

/// FORGE's open database: saved queries, alerts and progress.
#[derive(Debug)]
pub struct Registry {
    db: Db,
    path: PathBuf,
    /// Exclusively locked until dropped.
    _lock: File,
}

impl Registry {
    /// Open (or create) `config.db_path()`; see the module docs.
    pub fn open(config: &ForgeConfig) -> ForgeResult<Self> {
        Self::open_path(config.db_path())
    }

    /// [`Registry::open`] on an explicit path. `path`'s parent must exist.
    pub fn open_path(path: impl AsRef<Path>) -> ForgeResult<Self> {
        let path = path.as_ref().to_path_buf();
        let fail = |operation: &str, err: RegistryError| {
            ForgeError::registry(format!("{operation} {}", path.display()), err)
        };

        let lock_path = sidecar_lock_path(&path);
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|err| ForgeError::io(lock_path.display().to_string(), err))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(fail(
                    "lock",
                    RegistryError::conflict(format!(
                        "another process holds {} (is a second FORGE running on this database?)",
                        lock_path.display()
                    )),
                ));
            }
            Err(TryLockError::Error(err)) => return Err(ForgeError::io(lock_path.display().to_string(), err)),
        }

        create_private_file(&path).map_err(|err| ForgeError::io(path.display().to_string(), err))?;
        let conn = Connection::open(&path).map_err(|err| fail("open", err.into()))?;
        configure(&conn).map_err(|err| fail("configure", err))?;
        let mut conn = conn;
        let version = migrate(&mut conn).map_err(|err| fail("migrate", err))?;
        info!(path = %path.display(), schema_version = version, "registry open");

        Ok(Self {
            db: Db::new(conn),
            path,
            _lock: lock,
        })
    }

    /// The shared connection handle, for `webhook/outbox.rs` and for
    /// transactions that span the registry and the outbox.
    pub fn db(&self) -> &Db {
        &self.db
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // -- saved queries ------------------------------------------------------

    /// Validate and store a new saved query. A taken name is a
    /// [`RegistryError::Conflict`].
    pub fn create_query(&self, new: NewSavedQuery) -> ForgeResult<SavedQuery> {
        self.db.with_conn("create query", |conn| {
            new.validate()?;
            let now = unix_now();
            let params_json = serde_json::to_string(&new.params).map_err(RegistryError::storage)?;
            conn.execute(
                "INSERT INTO saved_queries (name, sql, params_json, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?4)",
                params![new.name, new.sql, params_json, now],
            )
            .map_err(|err| {
                if is_unique_violation(&err) {
                    RegistryError::conflict(format!("a saved query named {:?} already exists", new.name))
                } else {
                    err.into()
                }
            })?;
            Ok(SavedQuery {
                name: new.name,
                sql: new.sql,
                params: new.params,
                created_at: now,
                updated_at: now,
            })
        })
    }

    pub fn get_query(&self, name: &str) -> ForgeResult<SavedQuery> {
        self.db.with_conn("get query", |conn| {
            conn.query_row(
                "SELECT name, sql, params_json, created_at, updated_at FROM saved_queries WHERE name = ?1",
                [name],
                read_query,
            )
            .optional()?
            .ok_or_else(|| RegistryError::not_found("saved query", name))?
        })
    }

    /// Every saved query, by name.
    pub fn list_queries(&self) -> ForgeResult<Vec<SavedQuery>> {
        self.db.with_conn("list queries", |conn| {
            let mut stmt =
                conn.prepare("SELECT name, sql, params_json, created_at, updated_at FROM saved_queries ORDER BY name")?;
            let rows = stmt.query_map([], read_query)?;
            rows.map(|row| row?).collect()
        })
    }

    pub fn delete_query(&self, name: &str) -> ForgeResult<()> {
        self.db.with_conn("delete query", |conn| {
            match conn.execute("DELETE FROM saved_queries WHERE name = ?1", [name])? {
                0 => Err(RegistryError::not_found("saved query", name)),
                _ => Ok(()),
            }
        })
    }

    // -- alerts -------------------------------------------------------------

    /// Validate and store a new alert under a fresh UUID v4. It starts
    /// enabled with no progress. A taken name is a
    /// [`RegistryError::Conflict`].
    pub fn create_alert(&self, new: NewAlert) -> ForgeResult<Alert> {
        self.db.with_conn("create alert", |conn| {
            new.validate()?;
            let now = unix_now();
            let alert = Alert {
                id: uuid::Uuid::new_v4().to_string(),
                name: new.name,
                sql: new.sql,
                webhook_url: new.webhook_url,
                secret: new.secret,
                wait_for_final: new.wait_for_final,
                enabled: true,
                last_evaluated_slot: None,
                created_at: now,
                updated_at: now,
            };
            conn.execute(
                "INSERT INTO alerts (id, name, sql, webhook_url, secret, wait_for_final, enabled,
                                     last_evaluated_slot, created_at, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?8)",
                params![
                    alert.id,
                    alert.name,
                    alert.sql,
                    alert.webhook_url,
                    alert.secret,
                    alert.wait_for_final,
                    alert.enabled,
                    now
                ],
            )
            .map_err(|err| name_conflict(err, &alert.name))?;
            Ok(alert)
        })
    }

    pub fn get_alert(&self, id: &str) -> ForgeResult<Alert> {
        self.db.with_conn("get alert", |conn| select_alert(conn, id))
    }

    /// Every alert, by name.
    pub fn list_alerts(&self) -> ForgeResult<Vec<Alert>> {
        self.db.with_conn("list alerts", |conn| {
            let mut stmt = conn.prepare(&format!("SELECT {ALERT_COLUMNS} FROM alerts ORDER BY name"))?;
            let rows = stmt.query_map([], read_alert)?;
            rows.map(|row| row?).collect()
        })
    }

    /// Apply the fields set in `update` and re-validate the result. Progress
    /// and `enabled` are untouched; see [`Registry::set_enabled`].
    pub fn update_alert(&self, id: &str, update: AlertUpdate) -> ForgeResult<Alert> {
        self.db.transaction("update alert", |tx| {
            let mut alert = select_alert(tx, id)?;
            update.apply_to(&mut alert)?;
            alert.updated_at = unix_now();
            tx.execute(
                "UPDATE alerts SET name = ?2, sql = ?3, webhook_url = ?4, secret = ?5, wait_for_final = ?6,
                                   updated_at = ?7
                 WHERE id = ?1",
                params![
                    alert.id,
                    alert.name,
                    alert.sql,
                    alert.webhook_url,
                    alert.secret,
                    alert.wait_for_final,
                    alert.updated_at
                ],
            )
            .map_err(|err| name_conflict(err, &alert.name))?;
            Ok(alert)
        })
    }

    /// Delete an alert and, by cascade, all its deliveries (held and pending
    /// ones are never sent).
    pub fn delete_alert(&self, id: &str) -> ForgeResult<()> {
        self.db.with_conn("delete alert", |conn| {
            match conn.execute("DELETE FROM alerts WHERE id = ?1", [id])? {
                0 => Err(RegistryError::not_found("alert", id)),
                _ => Ok(()),
            }
        })
    }

    // -- alert state --------------------------------------------------------

    /// Enable or disable an alert.
    ///
    /// Re-enabling a disabled alert clears `last_evaluated_slot`: otherwise
    /// an alert switched off for a day would evaluate the whole gap as one
    /// enormous range. Evaluation restarts from the next event instead.
    /// Enabling an alert that is already enabled keeps its progress, so a
    /// retried request cannot reset it.
    pub fn set_enabled(&self, id: &str, enabled: bool) -> ForgeResult<Alert> {
        self.db.transaction("set alert enabled", |tx| {
            let changed = tx.execute(
                "UPDATE alerts
                 SET last_evaluated_slot = CASE WHEN ?2 = 1 AND enabled = 0 THEN NULL ELSE last_evaluated_slot END,
                     enabled = ?2,
                     updated_at = ?3
                 WHERE id = ?1",
                params![id, enabled, unix_now()],
            )?;
            if changed == 0 {
                return Err(RegistryError::not_found("alert", id));
            }
            select_alert(tx, id)
        })
    }

    /// Record that alert `alert_id` has been evaluated through `to_slot`,
    /// inside the caller's transaction `tx`.
    ///
    /// Monotonic: the `UPDATE` only matches while the stored slot is NULL or
    /// below `to_slot`, so it holds across concurrent callers and connections
    /// with no read-then-write race. A `to_slot` at or behind the stored one
    /// is a [`RegistryError::Conflict`]. That is an error, not a silent
    /// no-op, so the `?` in the caller's closure rolls back the whole
    /// transaction, outbox row included, and a stale evaluation can never
    /// enqueue a duplicate delivery. An unknown id is
    /// [`RegistryError::NotFound`].
    pub fn advance_progress(tx: &Transaction<'_>, alert_id: &str, to_slot: u64) -> Result<(), RegistryError> {
        let slot = slot_to_sql("to_slot", to_slot)?;
        let changed = tx.execute(
            "UPDATE alerts SET last_evaluated_slot = ?2, updated_at = ?3
             WHERE id = ?1 AND (last_evaluated_slot IS NULL OR last_evaluated_slot < ?2)",
            params![alert_id, slot, unix_now()],
        )?;
        if changed == 1 {
            return Ok(());
        }
        let current: Option<Option<i64>> = tx
            .query_row("SELECT last_evaluated_slot FROM alerts WHERE id = ?1", [alert_id], |row| row.get(0))
            .optional()?;
        match current {
            None => Err(RegistryError::not_found("alert", alert_id)),
            Some(current) => Err(RegistryError::conflict(format!(
                "alert {alert_id} is already evaluated through slot {}; {to_slot} does not move it forward",
                current.unwrap_or_default()
            ))),
        }
    }

    // -- engine state -------------------------------------------------------

    /// The alert engine's target: the highest announced slot LENS has been
    /// seen to cover. `None` before the engine's first gate.
    pub fn engine_target(&self) -> ForgeResult<Option<u64>> {
        self.db.with_conn("read engine target", |conn| {
            let slot: Option<i64> = conn
                .query_row("SELECT target_slot FROM engine_state WHERE id = 1", [], |row| row.get(0))
                .optional()?;
            slot.map(|slot| {
                u64::try_from(slot).map_err(|_| RegistryError::storage(format!("negative engine target {slot}")))
            })
            .transpose()
        })
    }

    /// Raise the stored engine target to `slot`. Never lowers it.
    pub fn raise_engine_target(&self, slot: u64) -> ForgeResult<()> {
        self.db.with_conn("raise engine target", |conn| {
            let slot = slot_to_sql("target_slot", slot)?;
            conn.execute(
                "INSERT INTO engine_state (id, target_slot, updated_at) VALUES (1, ?1, ?2)
                 ON CONFLICT (id) DO UPDATE SET target_slot = excluded.target_slot, updated_at = excluded.updated_at
                 WHERE excluded.target_slot > engine_state.target_slot",
                params![slot, unix_now()],
            )?;
            Ok(())
        })
    }

    // -- maintenance --------------------------------------------------------

    /// The schema version (`PRAGMA user_version`).
    pub fn schema_version(&self) -> ForgeResult<u32> {
        self.db.with_conn("read schema version", |conn| Ok(user_version(conn)?))
    }

    /// The schema version and how many alerts and saved queries are stored,
    /// for `/health`. Counts only; nothing is read.
    pub fn stats(&self) -> ForgeResult<RegistryStats> {
        self.db.with_conn("read registry stats", |conn| {
            let count = |table: &str| -> Result<u64, RegistryError> {
                let n: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| row.get(0))?;
                Ok(u64::try_from(n).unwrap_or_default())
            };
            Ok(RegistryStats {
                schema_version: user_version(conn)?,
                alerts: count("alerts")?,
                saved_queries: count("saved_queries")?,
            })
        })
    }
}

/// What [`Registry::stats`] reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RegistryStats {
    pub schema_version: u32,
    pub alerts: u64,
    pub saved_queries: u64,
}

/// `{db_path}.lock`.
pub fn sidecar_lock_path(db_path: &Path) -> PathBuf {
    let mut name = db_path.as_os_str().to_owned();
    name.push(LOCK_FILE_SUFFIX);
    PathBuf::from(name)
}

fn create_private_file(path: &Path) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map(drop)
}

fn configure(conn: &Connection) -> Result<(), RegistryError> {
    // WAL lets the API read while the evaluator writes; FULL makes every
    // commit durable, which the outbox relies on.
    let mode: String = conn.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(RegistryError::storage(format!("journal_mode is {mode}, expected wal")));
    }
    conn.execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")?;
    conn.busy_timeout(BUSY_TIMEOUT)?;
    Ok(())
}

fn user_version(conn: &Connection) -> rusqlite::Result<u32> {
    conn.query_row("PRAGMA user_version", [], |row| row.get(0))
}

/// Apply every migration past the stored version; return the new version.
fn migrate(conn: &mut Connection) -> Result<u32, RegistryError> {
    let target = MIGRATIONS.len() as u32;
    let current = user_version(conn)?;
    if current > target {
        return Err(RegistryError::conflict(format!(
            "schema version {current} is newer than this FORGE supports ({target})"
        )));
    }
    for (index, sql) in MIGRATIONS.iter().enumerate().skip(current as usize) {
        let version = index as u32 + 1;
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(sql)?;
        // `user_version` is transactional: it commits with the migration.
        tx.pragma_update(None, "user_version", version)?;
        tx.commit()?;
        info!(version, "applied registry migration");
    }
    Ok(target)
}

fn name_conflict(err: rusqlite::Error, name: &str) -> RegistryError {
    if is_unique_violation(&err) {
        RegistryError::conflict(format!("an alert named {name:?} already exists"))
    } else {
        err.into()
    }
}

fn read_query(row: &Row<'_>) -> rusqlite::Result<Result<SavedQuery, RegistryError>> {
    let name: String = row.get(0)?;
    let params_json: String = row.get(2)?;
    let params = match serde_json::from_str(&params_json) {
        Ok(params) => params,
        Err(err) => {
            return Ok(Err(RegistryError::storage(format!(
                "saved query {name:?} has unreadable params_json: {err}"
            ))));
        }
    };
    Ok(Ok(SavedQuery {
        name,
        sql: row.get(1)?,
        params,
        created_at: row.get(3)?,
        updated_at: row.get(4)?,
    }))
}

const ALERT_COLUMNS: &str = "id, name, sql, webhook_url, secret, wait_for_final, enabled, last_evaluated_slot, \
                             created_at, updated_at";

fn select_alert(conn: &Connection, id: &str) -> Result<Alert, RegistryError> {
    conn.query_row(&format!("SELECT {ALERT_COLUMNS} FROM alerts WHERE id = ?1"), [id], read_alert)
        .optional()?
        .ok_or_else(|| RegistryError::not_found("alert", id))?
}

fn read_alert(row: &Row<'_>) -> rusqlite::Result<Result<Alert, RegistryError>> {
    let id: String = row.get(0)?;
    let last: Option<i64> = row.get(7)?;
    let last_evaluated_slot = match last.map(u64::try_from).transpose() {
        Ok(slot) => slot,
        Err(_) => {
            return Ok(Err(RegistryError::storage(format!("alert {id} has a negative last_evaluated_slot"))));
        }
    };
    Ok(Ok(Alert {
        id,
        name: row.get(1)?,
        sql: row.get(2)?,
        webhook_url: row.get(3)?,
        secret: row.get(4)?,
        wait_for_final: row.get(5)?,
        enabled: row.get(6)?,
        last_evaluated_slot,
        created_at: row.get(8)?,
        updated_at: row.get(9)?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_per_variant() {
        assert_eq!(RegistryError::invalid("f", "r").http_status(), 400);
        assert_eq!(RegistryError::not_found("alert", "x").http_status(), 404);
        assert_eq!(RegistryError::conflict("c").http_status(), 409);
        assert_eq!(RegistryError::storage("s").http_status(), 500);
    }

    #[test]
    fn registry_error_is_recoverable_from_forge_error() {
        let err = ForgeError::registry("get alert", RegistryError::not_found("alert", "abc"));
        assert!(matches!(err.as_registry_error(), Some(RegistryError::NotFound { entity: "alert", .. })));
        assert!(ForgeError::config("f", "r").as_registry_error().is_none());
        assert!(err.to_string().contains("get alert"), "{err}");
    }

    #[test]
    fn migrations_run_in_memory() {
        let mut conn = Connection::open_in_memory().unwrap();
        assert_eq!(migrate(&mut conn).unwrap(), MIGRATIONS.len() as u32);
        // Idempotent.
        assert_eq!(migrate(&mut conn).unwrap(), MIGRATIONS.len() as u32);
    }

    #[test]
    fn slots_beyond_i64_are_rejected() {
        assert_eq!(slot_to_sql("s", i64::MAX as u64).unwrap(), i64::MAX);
        assert!(slot_to_sql("s", i64::MAX as u64 + 1).is_err());
    }

    #[test]
    fn the_engine_target_only_rises() {
        let dir = std::env::temp_dir().join(format!("forge-engine-target-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let registry = Registry::open_path(dir.join("forge.db")).unwrap();
        assert_eq!(registry.engine_target().unwrap(), None);
        registry.raise_engine_target(100).unwrap();
        registry.raise_engine_target(90).unwrap();
        assert_eq!(registry.engine_target().unwrap(), Some(100));
        registry.raise_engine_target(120).unwrap();
        assert_eq!(registry.engine_target().unwrap(), Some(120));
        let stats = registry.stats().unwrap();
        assert_eq!(stats, RegistryStats { schema_version: MIGRATIONS.len() as u32, alerts: 0, saved_queries: 0 });
        assert!(registry.raise_engine_target(u64::MAX).is_err());
        drop(registry);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn lock_path_is_a_sidecar() {
        assert_eq!(sidecar_lock_path(Path::new("/d/forge.db")), PathBuf::from("/d/forge.db.lock"));
    }
}
