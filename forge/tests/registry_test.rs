//! Registry integration tests: opening and migrating the database, CRUD on
//! saved queries and alerts, and progress tracking.

mod common;

use std::collections::HashMap;

use common::{Fixture, TempDir};
use forge::{
    AlertUpdate, ForgeError, MIGRATIONS, NewAlert, NewSavedQuery, ParamKind, ParamSpec, Registry, RegistryError,
    sidecar_lock_path,
};

const ALERT_SQL: &str = "SELECT * FROM transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}}";

fn open(dir: &TempDir) -> Registry {
    Registry::open_path(dir.path().join("forge.db")).unwrap()
}

fn new_alert(name: &str) -> NewAlert {
    NewAlert {
        name: name.into(),
        sql: ALERT_SQL.into(),
        webhook_url: "https://hooks.example.com/forge".into(),
        secret: "0123456789abcdef0123".into(),
        wait_for_final: false,
    }
}

fn new_query(name: &str) -> NewSavedQuery {
    NewSavedQuery {
        name: name.into(),
        sql: "SELECT * FROM transfers WHERE mint = {{mint}} LIMIT {{limit}}".into(),
        params: vec![
            ParamSpec {
                name: "mint".into(),
                kind: ParamKind::String,
                required: true,
                default: None,
            },
            ParamSpec {
                name: "limit".into(),
                kind: ParamKind::U64,
                required: false,
                default: Some("100".into()),
            },
        ],
    }
}

/// The inner error and its HTTP status, panicking on any other outcome.
fn registry_error<T: std::fmt::Debug>(result: Result<T, ForgeError>) -> (u16, String) {
    let err = result.expect_err("expected a registry error");
    let inner = err
        .as_registry_error()
        .unwrap_or_else(|| panic!("expected a RegistryError source, got {err:?}"));
    (inner.http_status(), inner.to_string())
}

fn advance(registry: &Registry, id: &str, slot: u64) -> Result<(), ForgeError> {
    registry
        .db()
        .transaction("advance", |tx| Registry::advance_progress(tx, id, slot))
}

// ---------------------------------------------------------------------------
// Opening
// ---------------------------------------------------------------------------

#[test]
fn open_from_config_creates_the_file_and_migrates() {
    let fixture = Fixture::new();
    let config = fixture.load(&[]).unwrap();
    let registry = Registry::open(&config).unwrap();
    assert!(config.db_path().is_file());
    assert_eq!(registry.schema_version().unwrap(), MIGRATIONS.len() as u32);
    assert_eq!(registry.path(), config.db_path());
}

#[test]
fn reopen_keeps_data_and_version() {
    let dir = TempDir::new();
    let id = {
        let registry = open(&dir);
        registry.create_query(new_query("q")).unwrap();
        registry.create_alert(new_alert("a")).unwrap().id
    };
    let registry = open(&dir);
    assert_eq!(registry.schema_version().unwrap(), 1);
    assert_eq!(registry.get_query("q").unwrap().params.len(), 2);
    assert_eq!(registry.get_alert(&id).unwrap().name, "a");
}

#[test]
fn a_second_open_is_refused_while_the_first_is_alive() {
    let dir = TempDir::new();
    let first = open(&dir);
    let (status, message) = registry_error(Registry::open_path(first.path()));
    assert_eq!(status, 409);
    assert!(message.contains("second FORGE"), "{message}");
    assert!(sidecar_lock_path(first.path()).exists());
    drop(first);
    open(&dir);
}

#[test]
fn a_database_from_a_newer_forge_is_refused() {
    let dir = TempDir::new();
    let path = dir.path().join("forge.db");
    drop(open(&dir));
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 99).unwrap();
    drop(conn);
    let (status, message) = registry_error(Registry::open_path(&path));
    assert_eq!(status, 409);
    assert!(message.contains("newer"), "{message}");
}

#[cfg(unix)]
#[test]
fn the_database_file_is_private() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new();
    let registry = open(&dir);
    let mode = std::fs::metadata(registry.path()).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "{mode:o}");
}

#[test]
fn connection_pragmas_are_set() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let (journal, fk, sync): (String, i64, i64) = registry
        .db()
        .with_conn("pragmas", |conn| {
            Ok((
                conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?,
                conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?,
                conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?,
            ))
        })
        .unwrap();
    assert_eq!(journal, "wal");
    assert_eq!(fk, 1);
    assert_eq!(sync, 2, "FULL");
}

#[test]
fn migration_one_creates_the_deliveries_table() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let tables: Vec<String> = registry
        .db()
        .with_conn("tables", |conn| {
            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")?;
            let names = stmt.query_map([], |r| r.get(0))?.collect::<Result<_, _>>()?;
            Ok(names)
        })
        .unwrap();
    assert_eq!(tables, ["alerts", "deliveries", "saved_queries"]);
}

// ---------------------------------------------------------------------------
// Saved queries
// ---------------------------------------------------------------------------

#[test]
fn saved_query_round_trip_and_bind() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let created = registry.create_query(new_query("by-mint")).unwrap();
    assert_eq!(created.created_at, created.updated_at);
    let fetched = registry.get_query("by-mint").unwrap();
    assert_eq!(fetched, created);

    let supplied = HashMap::from([("mint".to_owned(), "So1".to_owned())]);
    assert_eq!(
        fetched.bind(&supplied).unwrap(),
        "SELECT * FROM transfers WHERE mint = 'So1' LIMIT 100"
    );
}

#[test]
fn saved_query_errors_map_to_400_404_409() {
    let dir = TempDir::new();
    let registry = open(&dir);
    registry.create_query(new_query("q")).unwrap();

    assert_eq!(registry_error(registry.create_query(new_query("q"))).0, 409);
    assert_eq!(registry_error(registry.get_query("missing")).0, 404);
    assert_eq!(registry_error(registry.delete_query("missing")).0, 404);

    let mut bad = new_query("bad");
    bad.sql = "SELECT 1; DROP TABLE t".into();
    assert_eq!(registry_error(registry.create_query(bad)).0, 400);
    // A rejected create stores nothing.
    assert_eq!(registry_error(registry.get_query("bad")).0, 404);
}

#[test]
fn list_and_delete_queries() {
    let dir = TempDir::new();
    let registry = open(&dir);
    for name in ["b", "a", "c"] {
        registry.create_query(new_query(name)).unwrap();
    }
    let names: Vec<_> = registry.list_queries().unwrap().into_iter().map(|q| q.name).collect();
    assert_eq!(names, ["a", "b", "c"]);
    registry.delete_query("b").unwrap();
    assert_eq!(registry.list_queries().unwrap().len(), 2);
}

// ---------------------------------------------------------------------------
// Alerts
// ---------------------------------------------------------------------------

#[test]
fn alerts_get_generated_uuid_ids_and_start_enabled_without_progress() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let a = registry.create_alert(new_alert("a")).unwrap();
    let b = registry.create_alert(new_alert("b")).unwrap();
    assert_ne!(a.id, b.id);
    let parsed = uuid::Uuid::parse_str(&a.id).unwrap();
    assert_eq!(parsed.get_version_num(), 4);
    assert!(a.enabled);
    assert_eq!(a.last_evaluated_slot, None);
    assert_eq!(registry.get_alert(&a.id).unwrap(), a);
    assert_eq!(registry.list_alerts().unwrap(), vec![a, b]);
}

#[test]
fn the_secret_is_stored_as_given() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let a = registry.create_alert(new_alert("a")).unwrap();
    assert_eq!(registry.get_alert(&a.id).unwrap().secret, "0123456789abcdef0123");
}

#[test]
fn alert_errors_map_to_400_404_409() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let a = registry.create_alert(new_alert("a")).unwrap();
    registry.create_alert(new_alert("b")).unwrap();

    assert_eq!(registry_error(registry.create_alert(new_alert("a"))).0, 409);
    assert_eq!(registry_error(registry.get_alert("nope")).0, 404);
    assert_eq!(registry_error(registry.delete_alert("nope")).0, 404);
    assert_eq!(registry_error(registry.set_enabled("nope", true)).0, 404);
    assert_eq!(registry_error(registry.update_alert("nope", AlertUpdate::default())).0, 404);

    let rename = AlertUpdate {
        name: Some("b".into()),
        ..AlertUpdate::default()
    };
    assert_eq!(registry_error(registry.update_alert(&a.id, rename)).0, 409);

    let mut missing_from = new_alert("c");
    missing_from.sql = "SELECT * FROM t WHERE slot <= {{to_slot}}".into();
    let (status, message) = registry_error(registry.create_alert(missing_from));
    assert_eq!(status, 400);
    assert!(message.contains("from_slot"), "{message}");

    let mut creds = new_alert("c");
    creds.webhook_url = "https://user:pw@hooks.example.com".into();
    assert_eq!(registry_error(registry.create_alert(creds)).0, 400);
}

#[test]
fn update_alert_changes_only_given_fields_and_keeps_progress() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let a = registry.create_alert(new_alert("a")).unwrap();
    advance(&registry, &a.id, 42).unwrap();

    let updated = registry
        .update_alert(
            &a.id,
            AlertUpdate {
                webhook_url: Some("http://127.0.0.1:9000/hook".into()),
                wait_for_final: Some(true),
                ..AlertUpdate::default()
            },
        )
        .unwrap();
    assert_eq!(updated.webhook_url, "http://127.0.0.1:9000/hook");
    assert!(updated.wait_for_final);
    assert_eq!(updated.name, "a");
    assert_eq!(updated.last_evaluated_slot, Some(42));
    assert_eq!(registry.get_alert(&a.id).unwrap(), updated);

    // An invalid update changes nothing.
    let bad = AlertUpdate {
        name: Some("renamed".into()),
        sql: Some("SELECT 1".into()),
        ..AlertUpdate::default()
    };
    assert_eq!(registry_error(registry.update_alert(&a.id, bad)).0, 400);
    assert_eq!(registry.get_alert(&a.id).unwrap(), updated);
}

#[test]
fn deleting_an_alert_frees_its_name() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let a = registry.create_alert(new_alert("a")).unwrap();
    registry.delete_alert(&a.id).unwrap();
    assert_eq!(registry_error(registry.get_alert(&a.id)).0, 404);
    registry.create_alert(new_alert("a")).unwrap();
}

// ---------------------------------------------------------------------------
// Alert state
// ---------------------------------------------------------------------------

#[test]
fn advance_progress_only_moves_forward() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = registry.create_alert(new_alert("a")).unwrap().id;

    advance(&registry, &id, 10).unwrap();
    advance(&registry, &id, 11).unwrap();
    for stale in [11, 10, 0] {
        assert_eq!(registry_error(advance(&registry, &id, stale)).0, 409, "{stale}");
    }
    assert_eq!(registry.get_alert(&id).unwrap().last_evaluated_slot, Some(11));
    assert_eq!(registry_error(advance(&registry, "nope", 1)).0, 404);
    assert_eq!(registry_error(advance(&registry, &id, u64::MAX)).0, 400);
}

#[test]
fn advance_progress_guard_holds_across_connections() {
    // A second connection, as another process would have, advances first.
    // The guard lives in the UPDATE, so the registry's own stale write
    // still loses.
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = registry.create_alert(new_alert("a")).unwrap().id;

    let mut other = rusqlite::Connection::open(registry.path()).unwrap();
    let tx = other.transaction().unwrap();
    Registry::advance_progress(&tx, &id, 100).unwrap();
    tx.commit().unwrap();

    assert_eq!(registry_error(advance(&registry, &id, 50)).0, 409);
    assert_eq!(registry.get_alert(&id).unwrap().last_evaluated_slot, Some(100));
}

#[test]
fn re_enabling_clears_progress_but_a_repeated_enable_does_not() {
    let dir = TempDir::new();
    let registry = open(&dir);
    let id = registry.create_alert(new_alert("a")).unwrap().id;
    advance(&registry, &id, 500).unwrap();

    // Already enabled: progress kept.
    assert_eq!(registry.set_enabled(&id, true).unwrap().last_evaluated_slot, Some(500));

    let disabled = registry.set_enabled(&id, false).unwrap();
    assert!(!disabled.enabled);
    assert_eq!(disabled.last_evaluated_slot, Some(500));

    let enabled = registry.set_enabled(&id, true).unwrap();
    assert!(enabled.enabled);
    assert_eq!(enabled.last_evaluated_slot, None);
    assert_eq!(enabled.next_range(9_000, 9_010), Some((9_000, 9_010)));

    // After the reset, any slot moves it forward again.
    advance(&registry, &id, 1).unwrap();
}
