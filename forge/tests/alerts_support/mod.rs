//! Helpers for the alert engine tests: a fake LENS serving rows from an
//! in-memory dataset, a change log writer speaking PRISM's wire format, and
//! a harness wiring both to a real registry file, a real change log reader
//! and a running webhook service.
//!
//! The fake checks every query as it arrives, independently of the engine:
//! the range must start right after the alert's committed progress, and end
//! at or below both the highest slot announced in the change log and the
//! highest slot LENS can see. Breaches are recorded, not panicked on, so a
//! test (or a crash child) reports all of them.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use forge::{
    AlertEngine, ChangelogReader, CheckpointStore, EngineSettings, LensApi, LensCallError, LensHealth,
    LensHealthStatus, LensResponse, NewAlert, Registry, RetryPolicy, SystemClock, Visibility, WebhookService,
    WebhookSettings,
};
use serde_json::{Map, Value, json};

use crate::common::TempDir;
use crate::webhook_support::SECRET;

pub type Row = Map<String, Value>;

/// `SELECT slot, n FROM solana.{table} WHERE slot BETWEEN ... AND ...`.
pub fn alert_sql(table: &str) -> String {
    format!("SELECT slot, n FROM solana.{table} WHERE slot BETWEEN {{{{from_slot}}}} AND {{{{to_slot}}}}")
}

/// Fast, deterministic engine settings.
pub fn engine_settings() -> EngineSettings {
    EngineSettings {
        poll_interval: Duration::from_millis(5),
        batch_size: 1_000,
        gate_timeout: Duration::from_millis(150),
        max_range_slots: 1_000,
        payload_budget_bytes: 256 * 1024,
        retry_min: Duration::from_millis(5),
        retry_max: Duration::from_millis(20),
        permanent_retry: Duration::from_millis(40),
        missing_table_retries: 3,
    }
}

pub fn webhook_settings() -> WebhookSettings {
    WebhookSettings {
        max_attempts: 5,
        lease: Duration::from_secs(30),
        request_timeout: Duration::from_secs(2),
        retry: RetryPolicy {
            delays: vec![Duration::from_millis(20)],
            jitter_pct: 0,
        },
        max_in_flight: 4,
        tick: Duration::from_millis(20),
        shutdown_grace: Duration::from_secs(3),
    }
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

/// Rows of one table, by slot.
pub type Table = BTreeMap<u64, Vec<Row>>;

/// 0 to 3 rows per slot of `slots`, decided by a hash of the slot and
/// `seed`, each with a unique `n` near `u64::MAX` so precision loss shows.
pub fn dataset(slots: std::ops::RangeInclusive<u64>, seed: u64) -> Table {
    slots
        .map(|slot| {
            let mut h = slot.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seed.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
            h ^= h >> 29;
            let rows = (0..h % 4)
                .map(|i| row(slot, u64::MAX - (slot * 10 + i)))
                .collect();
            (slot, rows)
        })
        .collect()
}

pub fn row(slot: u64, n: u64) -> Row {
    serde_json::from_value(json!({ "slot": slot, "n": n })).unwrap()
}

/// Every `(slot, n)` of `table` with `from <= slot <= to`.
pub fn expected(table: &Table, from: u64, to: u64) -> Vec<(u64, u64)> {
    table
        .range(from..=to)
        .flat_map(|(_, rows)| rows.iter().map(key))
        .collect()
}

pub fn key(row: &Row) -> (u64, u64) {
    (row["slot"].as_u64().unwrap(), row["n"].as_u64().unwrap())
}

// ---------------------------------------------------------------------------
// FakeLens
// ---------------------------------------------------------------------------

/// One alert query, as the fake parsed it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub table: String,
    pub from: u64,
    pub to: u64,
    /// 1-based index among every call (gates and queries).
    pub call: usize,
    pub failed: bool,
}

type Fault = Box<dyn FnMut(&Query) -> Option<LensCallError> + Send>;
type Checker = Arc<dyn Fn(&Query) -> Option<String> + Send + Sync>;
type OnCall = Arc<dyn Fn(usize) + Send + Sync>;

#[derive(Default)]
struct Inner {
    tables: HashMap<String, Table>,
    unregistered: HashSet<String>,
    max_rows: Option<usize>,
    fault: Option<Fault>,
    checker: Option<Checker>,
    on_call: Option<OnCall>,
    log: Option<PathBuf>,
    queries: Vec<Query>,
    gates: Vec<u64>,
    violations: Vec<String>,
    calls: usize,
}

/// LENS, faked: rows come from in-memory tables, visibility from
/// [`FakeLens::show`].
#[derive(Default)]
pub struct FakeLens {
    inner: Mutex<Inner>,
    visible: AtomicU64,
    announced: AtomicU64,
}

impl FakeLens {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn table(&self, name: &str, table: Table) {
        self.inner.lock().unwrap().tables.insert(name.into(), table);
    }

    /// Make `slot` the highest one LENS sees. Never moves back.
    pub fn show(&self, slot: u64) {
        self.visible.fetch_max(slot, Ordering::SeqCst);
    }

    pub fn visible(&self) -> u64 {
        self.visible.load(Ordering::SeqCst)
    }

    /// Record that the change log has announced `slot`.
    pub fn announce(&self, slot: u64) {
        self.announced.fetch_max(slot, Ordering::SeqCst);
    }

    /// Answer queries on `table` with LENS's "not found" until
    /// [`Self::register`].
    pub fn unregister(&self, table: &str) {
        self.inner.lock().unwrap().unregistered.insert(table.into());
    }

    pub fn register(&self, table: &str) {
        self.inner.lock().unwrap().unregistered.remove(table);
    }

    /// Truncate results past `max_rows`, as LENS's `max_query_rows` does.
    pub fn max_rows(&self, max_rows: usize) {
        self.inner.lock().unwrap().max_rows = Some(max_rows);
    }

    /// Decide, per query, whether it fails and how.
    pub fn fault(&self, fault: impl FnMut(&Query) -> Option<LensCallError> + Send + 'static) {
        self.inner.lock().unwrap().fault = Some(Box::new(fault));
    }

    pub fn clear_fault(&self) {
        self.inner.lock().unwrap().fault = None;
    }

    /// Check every query; a `Some` is a violation.
    pub fn checker(&self, checker: impl Fn(&Query) -> Option<String> + Send + Sync + 'static) {
        self.inner.lock().unwrap().checker = Some(Arc::new(checker));
    }

    /// Called with each call's 1-based index before it is answered, outside
    /// the fake's lock: a crash child exits here, a shutdown test cancels.
    pub fn on_call(&self, on_call: impl Fn(usize) + Send + Sync + 'static) {
        self.inner.lock().unwrap().on_call = Some(Arc::new(on_call));
    }

    /// Append every query and violation to `path`, flushed before the query
    /// is answered, so a crashed process leaves them behind.
    pub fn log_to(&self, path: impl Into<PathBuf>) {
        self.inner.lock().unwrap().log = Some(path.into());
    }

    pub fn queries(&self) -> Vec<Query> {
        self.inner.lock().unwrap().queries.clone()
    }

    pub fn gates(&self) -> Vec<u64> {
        self.inner.lock().unwrap().gates.clone()
    }

    pub fn calls(&self) -> usize {
        self.inner.lock().unwrap().calls
    }

    pub fn violations(&self) -> Vec<String> {
        self.inner.lock().unwrap().violations.clone()
    }

    fn next_call(&self) -> usize {
        let (call, on_call) = {
            let mut inner = self.inner.lock().unwrap();
            inner.calls += 1;
            (inner.calls, inner.on_call.clone())
        };
        if let Some(on_call) = on_call {
            on_call(call);
        }
        call
    }

    fn record(inner: &mut Inner, line: String) {
        if let Some(path) = &inner.log {
            let mut file = OpenOptions::new().create(true).append(true).open(path).unwrap();
            file.write_all(format!("{line}\n").as_bytes()).unwrap();
            file.sync_data().unwrap();
        }
    }
}

#[async_trait]
impl LensApi for FakeLens {
    async fn query(&self, sql: &str) -> Result<LensResponse, LensCallError> {
        let call = self.next_call();
        let (table, from, to) = parse(sql);
        let mut query = Query { table, from, to, call, failed: false };

        let checker = self.inner.lock().unwrap().checker.clone();
        let mut violations = Vec::new();
        if from > to {
            violations.push(format!("empty range {from}..={to}"));
        }
        let (visible, announced) = (self.visible(), self.announced.load(Ordering::SeqCst));
        if to > visible {
            violations.push(format!("{} queried {from}..={to} past the visible slot {visible}", query.table));
        }
        if to > announced {
            violations.push(format!("{} queried {from}..={to} past the announced slot {announced}", query.table));
        }
        if let Some(violation) = checker.and_then(|check| check(&query)) {
            violations.push(violation);
        }

        let mut inner = self.inner.lock().unwrap();
        let mut answer = if inner.unregistered.contains(&query.table) {
            Err(LensCallError::BadQuery {
                message: format!("query_plan_error: table 'datafusion.solana.{}' not found", query.table),
            })
        } else {
            match inner.fault.as_mut().and_then(|fault| fault(&query)) {
                Some(err) => Err(err),
                None => Ok(()),
            }
        };
        if answer.is_ok() && !inner.tables.contains_key(&query.table) {
            answer = Err(LensCallError::BadQuery { message: format!("no such test table {}", query.table) });
        }
        query.failed = answer.is_err();
        for violation in violations {
            Self::record(&mut inner, format!("VIOLATION {violation}"));
            inner.violations.push(violation);
        }
        Self::record(&mut inner, format!("QUERY {} {} {} {}", query.table, query.from, query.to, query.failed));
        inner.queries.push(query.clone());
        answer?;

        // Only what LENS can see is returned.
        let mut rows: Vec<Row> = inner.tables[&query.table]
            .range(from..=to.min(visible))
            .flat_map(|(_, rows)| rows.iter().cloned())
            .collect();
        let truncated = inner.max_rows.is_some_and(|max| rows.len() > max);
        if let Some(max) = inner.max_rows {
            rows.truncate(max);
        }
        Ok(LensResponse {
            row_count: rows.len() as u64,
            rows,
            duration_ms: 0,
            truncated,
            sql: sql.to_owned(),
            round_trip_ms: 0,
        })
    }

    async fn wait_until_visible(&self, slot: u64, timeout: Duration) -> Visibility {
        self.next_call();
        self.inner.lock().unwrap().gates.push(slot);
        let started = Instant::now();
        loop {
            let max_slot = self.visible();
            if max_slot >= slot && max_slot > 0 {
                return Visibility::Visible { max_slot };
            }
            if started.elapsed() >= timeout {
                return Visibility::TimedOut {
                    last_seen: (max_slot > 0).then_some(max_slot),
                    last_error: None,
                };
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    async fn health(&self) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status: LensHealthStatus::Healthy, round_trip_ms: 0 })
    }
}

/// `(table, from, to)` of an [`alert_sql`] query.
fn parse(sql: &str) -> (String, u64, u64) {
    let after = |needle: &str| &sql[sql.find(needle).unwrap_or_else(|| panic!("{needle} in {sql}")) + needle.len()..];
    let table = after("FROM solana.").split_whitespace().next().unwrap().to_owned();
    let number = |text: &str| text.split_whitespace().next().unwrap().parse::<u64>().unwrap();
    (table, number(after("BETWEEN ")), number(after(" AND ")))
}

// ---------------------------------------------------------------------------
// Changelog
// ---------------------------------------------------------------------------

/// Appends PRISM envelopes to a change log file.
pub struct Changelog {
    path: PathBuf,
    next_id: u64,
}

impl Changelog {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), next_id: 1 }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Id of the last event appended to the current file; 0 for none.
    pub fn last_id(&self) -> u64 {
        self.next_id - 1
    }

    pub fn new_slot(&mut self, slot: u64) {
        self.append(json!({
            "event": "new", "slot": slot, "blockhash": format!("h{slot}"), "parent_slot": slot.saturating_sub(1),
            "block_time": 1_730_000_000 + slot as i64, "transaction_count": 3, "parquet_paths": []
        }));
    }

    pub fn skip(&mut self, slot: u64) {
        self.append(json!({ "event": "skip", "slot": slot, "reason": "validator_skipped" }));
    }

    pub fn finalized(&mut self, slot: u64) {
        self.append(json!({ "event": "final", "slot": slot, "blockhash": format!("h{slot}"), "previous_slot": 0 }));
    }

    /// An event kind FORGE does not know.
    pub fn unknown(&mut self) {
        self.append(json!({ "event": "reorg", "slot": 1 }));
    }

    /// Replace the file with a new, empty one (a new inode), as a PRISM
    /// reset does. Ids start over.
    pub fn replace(&mut self) {
        let temp = self.path.with_extension("new");
        fs::write(&temp, b"").unwrap();
        fs::rename(&temp, &self.path).unwrap();
        self.next_id = 1;
    }

    fn append(&mut self, event: Value) {
        let mut envelope = json!({ "id": self.next_id, "emitted_at": 1_730_000_000, "chain": "solana-devnet" });
        envelope.as_object_mut().unwrap().extend(event.as_object().unwrap().clone());
        self.next_id += 1;
        let mut file = OpenOptions::new().create(true).append(true).open(&self.path).unwrap();
        file.write_all(format!("{envelope}\n").as_bytes()).unwrap();
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// A registry file, a change log, a fake LENS and a webhook service, all in
/// one scratch directory.
pub struct Harness {
    /// The scratch directory; removed on drop when `temp` owns it.
    pub root: PathBuf,
    temp: Option<TempDir>,
    pub changelog: Changelog,
    pub lens: Arc<FakeLens>,
    pub registry: Arc<Registry>,
    pub webhook: Arc<WebhookService>,
    pub webhook_url: String,
    /// Which alert reads which table, for the fake's progress check.
    tables: Arc<Mutex<HashMap<String, String>>>,
}

/// What survives [`Harness::close`]: everything on disk, and the fake.
pub struct Closed {
    pub root: PathBuf,
    temp: Option<TempDir>,
    pub changelog: Changelog,
    pub lens: Arc<FakeLens>,
    pub webhook_url: String,
}

impl Closed {
    pub fn open(self) -> Harness {
        Harness::open(self.root, self.temp, self.lens, &self.webhook_url, self.changelog)
    }
}

impl Harness {
    /// A fresh scratch directory. Must run inside a Tokio runtime (the
    /// webhook service starts here).
    pub fn new(webhook_url: &str) -> Self {
        let temp = TempDir::new();
        let root = temp.path().to_path_buf();
        let changelog = Changelog::new(root.join("changelog.jsonl"));
        Self::open(root, Some(temp), FakeLens::new(), webhook_url, changelog)
    }

    /// The harness on the files under `root`, as after a restart. Each
    /// table's alert is found again from its SQL. `temp`, when given, is
    /// what removes `root` on drop; a crash child passes `None`.
    pub fn open(root: PathBuf, temp: Option<TempDir>, lens: Arc<FakeLens>, webhook_url: &str, changelog: Changelog) -> Self {
        let registry = Arc::new(open_registry(&root.join("forge.db")));
        let webhook = Arc::new(
            WebhookService::start_with(registry.db().clone(), webhook_settings(), Arc::new(SystemClock)).unwrap(),
        );
        let tables: Arc<Mutex<HashMap<String, String>>> = Arc::default();
        for alert in registry.list_alerts().unwrap() {
            tables.lock().unwrap().insert(table_of(&alert.sql), alert.id);
        }
        let harness = Self {
            root,
            temp,
            changelog,
            lens,
            registry,
            webhook,
            webhook_url: webhook_url.to_owned(),
            tables,
        };
        harness.check_progress();
        harness
    }

    /// Stop the webhook service and close the registry. Engines built from
    /// this harness must have been dropped.
    pub async fn close(self) -> Closed {
        let Self { root, temp, changelog, lens, registry, webhook, webhook_url, .. } = self;
        assert_eq!(Arc::strong_count(&registry), 1, "an engine still holds the registry");
        webhook.shutdown().await;
        drop(webhook);
        drop(registry);
        Closed { root, temp, changelog, lens, webhook_url }
    }

    /// [`Self::close`], then open it all again on the same files.
    pub async fn restart(self) -> Self {
        self.close().await.open()
    }

    /// Holds the registry weakly, so a restart can reopen it.
    fn check_progress(&self) {
        let registry = Arc::downgrade(&self.registry);
        let tables = self.tables.clone();
        self.lens.checker(move |query| {
            let id = tables.lock().unwrap().get(&query.table).cloned()?;
            let progress = registry.upgrade()?.get_alert(&id).ok()?.last_evaluated_slot;
            match progress {
                Some(progress) if query.from == progress + 1 => None,
                other => Some(format!(
                    "{} queried {}..={} with stored progress {other:?}",
                    query.table, query.from, query.to
                )),
            }
        });
    }

    /// An alert on `table`, with no progress.
    pub fn alert(&self, name: &str, table: &str, wait_for_final: bool) -> String {
        let id = self
            .registry
            .create_alert(NewAlert {
                name: name.into(),
                sql: alert_sql(table),
                webhook_url: self.webhook_url.clone(),
                secret: SECRET.into(),
                wait_for_final,
            })
            .unwrap()
            .id;
        self.tables.lock().unwrap().insert(table.into(), id.clone());
        id
    }

    /// An alert on `table` already evaluated through `progress`.
    pub fn alert_at(&self, name: &str, table: &str, progress: u64) -> String {
        let id = self.alert(name, table, false);
        self.set_progress(&id, progress);
        id
    }

    pub fn set_progress(&self, id: &str, progress: u64) {
        self.registry
            .db()
            .transaction("set progress", |tx| Registry::advance_progress(tx, id, progress))
            .unwrap();
    }

    pub fn progress(&self, id: &str) -> Option<u64> {
        self.registry.get_alert(id).unwrap().last_evaluated_slot
    }

    /// Announce `slots` as `New` events. LENS does not see them until
    /// [`FakeLens::show`].
    pub fn publish(&mut self, slots: impl IntoIterator<Item = u64>) {
        for slot in slots {
            self.changelog.new_slot(slot);
            self.lens.announce(slot);
        }
    }

    pub fn checkpoint_store(&self) -> CheckpointStore {
        CheckpointStore::new(self.root.join("forge.checkpoint.json"))
    }

    /// Id of the last event the change log checkpoint has committed.
    pub fn committed_id(&self) -> u64 {
        self.checkpoint_store().load().unwrap().map_or(0, |checkpoint| checkpoint.last_id)
    }

    /// A fresh engine: a new change log reader resuming from the checkpoint.
    pub fn engine(&self, settings: EngineSettings) -> Arc<AlertEngine> {
        let reader = ChangelogReader::open_path(self.changelog.path(), self.checkpoint_store()).unwrap();
        Arc::new(
            AlertEngine::with_settings(
                self.registry.clone(),
                reader,
                self.lens.clone(),
                self.webhook.clone(),
                settings,
            )
            .unwrap(),
        )
    }

    /// Every delivery of `alert_id`: `(to_slot, payload)`, by `to_slot`.
    pub fn deliveries(&self, alert_id: &str) -> Vec<(u64, Value)> {
        self.registry
            .db()
            .with_conn("deliveries", |conn| {
                let mut stmt =
                    conn.prepare("SELECT to_slot, payload FROM deliveries WHERE alert_id = ?1 ORDER BY to_slot")?;
                let rows = stmt
                    .query_map([alert_id], |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, String>(1)?)))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .unwrap()
            .into_iter()
            .map(|(to_slot, payload)| (to_slot, serde_json::from_str(&payload).unwrap()))
            .collect()
    }

    pub fn delivery_statuses(&self, alert_id: &str) -> Vec<String> {
        self.registry
            .db()
            .with_conn("statuses", |conn| {
                let mut stmt = conn.prepare("SELECT status FROM deliveries WHERE alert_id = ?1 ORDER BY to_slot")?;
                let rows = stmt.query_map([alert_id], |row| row.get(0))?.collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .unwrap()
    }
}

/// [`Registry::open_path`], retried for a moment on a lock conflict: a
/// crash test elsewhere in this binary forks children, and until a child
/// execs it holds a copy of every descriptor, a just-closed registry's
/// lock included.
fn open_registry(path: &Path) -> Registry {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Registry::open_path(path) {
            Ok(registry) => return registry,
            Err(err)
                if matches!(err.as_registry_error(), Some(forge::RegistryError::Conflict { .. }))
                    && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("cannot open {}: {err}", path.display()),
        }
    }
}

/// The table an [`alert_sql`] reads.
pub fn table_of(sql: &str) -> String {
    sql.split("FROM solana.").nth(1).unwrap().split_whitespace().next().unwrap().to_owned()
}

// ---------------------------------------------------------------------------
// The guarantee
// ---------------------------------------------------------------------------

/// Check that `deliveries` of one alert carry every row of `table` in
/// `from..=to` exactly once and nothing else: each delivery's rows inside
/// its own range, the ranges disjoint and inside `from..=to`, no row
/// truncated or omitted. Returns the breaches.
pub fn exactly_once(deliveries: &[(u64, Value)], table: &Table, from: u64, to: u64) -> Vec<String> {
    let mut breaches = Vec::new();
    let mut seen: HashMap<(u64, u64), usize> = HashMap::new();
    let mut ranges = Vec::new();
    for (to_slot, payload) in deliveries {
        let (lo, hi) = (payload["from_slot"].as_u64().unwrap(), payload["to_slot"].as_u64().unwrap());
        if hi != *to_slot {
            breaches.push(format!("delivery to_slot {to_slot} carries a payload for {lo}..={hi}"));
        }
        if lo < from || hi > to || lo > hi {
            breaches.push(format!("delivery range {lo}..={hi} outside {from}..={to}"));
        }
        if payload["truncated"] != json!(false) || payload["rows_omitted"] != json!(0) {
            breaches.push(format!("delivery {lo}..={hi} is incomplete: {payload}"));
        }
        let rows = payload["rows"].as_array().unwrap();
        if payload["row_count"].as_u64() != Some(rows.len() as u64) || rows.is_empty() {
            breaches.push(format!("delivery {lo}..={hi} has row_count {} and {} rows", payload["row_count"], rows.len()));
        }
        for row in rows {
            let (slot, n) = key(row.as_object().unwrap());
            if !(lo..=hi).contains(&slot) {
                breaches.push(format!("row of slot {slot} in delivery {lo}..={hi}"));
            }
            *seen.entry((slot, n)).or_default() += 1;
        }
        ranges.push((lo, hi));
    }
    ranges.sort();
    for pair in ranges.windows(2) {
        if pair[1].0 <= pair[0].1 {
            breaches.push(format!("delivery ranges {:?} and {:?} overlap", pair[0], pair[1]));
        }
    }
    let want = expected(table, from, to);
    for row in &want {
        match seen.get(row) {
            None => breaches.push(format!("row {row:?} never delivered")),
            Some(1) => {}
            Some(times) => breaches.push(format!("row {row:?} delivered {times} times")),
        }
    }
    let want: HashSet<_> = want.into_iter().collect();
    for row in seen.keys().filter(|row| !want.contains(row)) {
        breaches.push(format!("row {row:?} delivered but not expected"));
    }
    breaches
}

/// Poll `condition` every 5 ms for up to `secs` seconds.
pub async fn wait_for(what: &str, secs: u64, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
