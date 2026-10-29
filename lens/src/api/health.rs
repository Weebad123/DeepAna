//! `GET /health`: is LENS up and serving data, and if not, why not.
//!
//! Always answers 200 with a [`HealthResponse`]; the verdict is in `status`,
//! so an operator gets the diagnosis even when LENS is unhealthy:
//!
//! ```json
//! {
//!   "status": "healthy",
//!   "chain": "solana",
//!   "data_dir": "/home/user/helios/data",
//!   "tables": {
//!     "solana.slots":           {"registered": true,  "file_count": 12, "dir_exists": true},
//!     "solana.token_transfers": {"registered": false, "file_count": 0,  "dir_exists": false}
//!   },
//!   "catalog": {"total_refreshes": 128, "last_refresh_duration_ms": 4, "tables_failed": 0},
//!   "engine": {"total_queries": 47, "successful_queries": 46, "failed_queries": 1,
//!              "timed_out_queries": 0, "success_rate": 0.979, "last_query_duration_ms": 83,
//!              "p50_duration_ms": 45, "p99_duration_ms": 312},
//!   "uptime_secs": 3847
//! }
//! ```
//!
//! # Status
//!
//! | `status`    | When                                                                     |
//! |-------------|--------------------------------------------------------------------------|
//! | `unhealthy` | No table is registered, or the table directories could not be read      |
//! | `degraded`  | A table has a directory but is not registered, or the last catalog refresh failed for a table |
//! | `healthy`   | Otherwise                                                                |
//!
//! A table whose directory does not exist yet is not a problem: PRISM only
//! creates it once it has a row for it (e.g. no token transfer seen yet).
//! Query failures do not affect the status either; most are bad SQL, which
//! says nothing about LENS.

use std::time::Instant;

use axum::Json;
use axum::extract::State;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use super::AppState;
use crate::catalog::{CatalogMetrics, TableStatus};
use crate::engine::EngineMetricsSnapshot;

/// The verdict of a health check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// The body of `GET /health`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub chain: String,
    pub data_dir: String,
    /// Keyed by `"solana.slots"`, ..., in `TableKind::ALL` order.
    #[serde(serialize_with = "tables_as_map")]
    pub tables: Vec<TableStatus>,
    /// Why the table directories could not be read; absent when they could.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub catalog: CatalogHealth,
    pub engine: EngineHealth,
    pub uptime_secs: u64,
}

/// The part of [`CatalogMetrics`] the health check reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct CatalogHealth {
    pub total_refreshes: u64,
    pub last_refresh_duration_ms: u64,
    pub tables_failed: u64,
}

impl From<CatalogMetrics> for CatalogHealth {
    fn from(metrics: CatalogMetrics) -> Self {
        Self {
            total_refreshes: metrics.total_refreshes,
            last_refresh_duration_ms: metrics.last_refresh_duration_ms,
            tables_failed: metrics.tables_failed,
        }
    }
}

/// [`EngineMetricsSnapshot`] as the health check reports it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct EngineHealth {
    pub total_queries: u64,
    pub successful_queries: u64,
    pub failed_queries: u64,
    pub timed_out_queries: u64,
    /// Rounded to 3 decimals.
    pub success_rate: f64,
    pub last_query_duration_ms: u64,
    pub p50_duration_ms: u64,
    pub p99_duration_ms: u64,
}

impl From<EngineMetricsSnapshot> for EngineHealth {
    fn from(snapshot: EngineMetricsSnapshot) -> Self {
        Self {
            total_queries: snapshot.total_queries,
            successful_queries: snapshot.successful_queries,
            failed_queries: snapshot.failed_queries,
            timed_out_queries: snapshot.timed_out_queries,
            success_rate: (snapshot.success_rate * 1_000.0).round() / 1_000.0,
            last_query_duration_ms: snapshot.last_query_duration_ms,
            p50_duration_ms: snapshot.p50_duration_ms,
            p99_duration_ms: snapshot.p99_duration_ms,
        }
    }
}

#[derive(Serialize)]
struct TableHealth {
    registered: bool,
    file_count: usize,
    dir_exists: bool,
}

fn tables_as_map<S: Serializer>(tables: &[TableStatus], serializer: S) -> Result<S::Ok, S::Error> {
    let mut map = serializer.serialize_map(Some(tables.len()))?;
    for table in tables {
        let health = TableHealth {
            registered: table.registered,
            file_count: table.file_count,
            dir_exists: table.dir_exists,
        };
        map.serialize_entry(&table.name, &health)?;
    }
    map.end()
}

/// Handler for `GET /health`.
pub async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    // Takes the catalog lock and walks every table directory: blocking work.
    let engine = state.engine.clone();
    let checked = tokio::task::spawn_blocking(move || (engine.table_status(), engine.catalog_metrics())).await;
    let (tables, error, catalog) = match checked {
        Ok((Ok(tables), catalog)) => (tables, None, catalog),
        Ok((Err(err), catalog)) => (Vec::new(), Some(err.to_string()), catalog),
        Err(join_err) => (Vec::new(), Some(format!("health check task failed: {join_err}")), CatalogMetrics::default()),
    };

    let status = compute_status(&tables, error.is_some(), &catalog);
    if status != HealthStatus::Healthy {
        tracing::warn!(?status, error = error.as_deref(), "health check is not healthy");
    }
    Json(HealthResponse {
        status,
        chain: state.config.chain().to_owned(),
        data_dir: state.config.data_dir().display().to_string(),
        tables,
        error,
        catalog: catalog.into(),
        engine: state.engine.metrics_snapshot().into(),
        uptime_secs: uptime_secs(state.start_time),
    })
}

/// The verdict for `tables`. See the [module docs](self).
pub fn compute_status(tables: &[TableStatus], unreadable: bool, catalog: &CatalogMetrics) -> HealthStatus {
    if unreadable || !tables.iter().any(|table| table.registered) {
        return HealthStatus::Unhealthy;
    }
    let stranded = tables.iter().any(|table| table.dir_exists && !table.registered);
    if stranded || catalog.tables_failed > 0 {
        return HealthStatus::Degraded;
    }
    HealthStatus::Healthy
}

fn uptime_secs(start_time: Instant) -> u64 {
    start_time.elapsed().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, dir_exists: bool, registered: bool) -> TableStatus {
        TableStatus {
            name: format!("solana.{name}"),
            dir_exists,
            file_count: usize::from(dir_exists) * 3,
            registered,
        }
    }

    fn all(dir_exists: bool, registered: bool) -> Vec<TableStatus> {
        ["slots", "transactions", "instructions", "token_transfers"]
            .into_iter()
            .map(|name| table(name, dir_exists, registered))
            .collect()
    }

    #[test]
    fn every_table_registered_is_healthy() {
        assert_eq!(compute_status(&all(true, true), false, &CatalogMetrics::default()), HealthStatus::Healthy);
    }

    #[test]
    fn a_table_prism_has_not_written_yet_is_still_healthy() {
        let mut tables = all(true, true);
        tables[3] = table("token_transfers", false, false);
        assert_eq!(compute_status(&tables, false, &CatalogMetrics::default()), HealthStatus::Healthy);
    }

    #[test]
    fn a_table_on_disk_but_not_registered_is_degraded() {
        let mut tables = all(true, true);
        tables[1] = table("transactions", true, false);
        assert_eq!(compute_status(&tables, false, &CatalogMetrics::default()), HealthStatus::Degraded);
    }

    #[test]
    fn a_failed_catalog_refresh_is_degraded() {
        let catalog = CatalogMetrics { tables_failed: 1, ..CatalogMetrics::default() };
        assert_eq!(compute_status(&all(true, true), false, &catalog), HealthStatus::Degraded);
    }

    #[test]
    fn nothing_registered_or_unreadable_dirs_is_unhealthy() {
        let none = CatalogMetrics::default();
        assert_eq!(compute_status(&all(false, false), false, &none), HealthStatus::Unhealthy);
        assert_eq!(compute_status(&all(true, false), false, &none), HealthStatus::Unhealthy);
        assert_eq!(compute_status(&[], false, &none), HealthStatus::Unhealthy);
        assert_eq!(compute_status(&all(true, true), true, &none), HealthStatus::Unhealthy);
    }

    #[test]
    fn tables_serialize_as_an_object_in_catalog_order() {
        #[derive(Serialize)]
        struct Wrapper(#[serde(serialize_with = "tables_as_map")] Vec<TableStatus>);
        let mut tables = all(true, true);
        tables[3] = table("token_transfers", false, false);
        let json = serde_json::to_string(&Wrapper(tables)).unwrap();
        assert_eq!(
            json,
            "{\"solana.slots\":{\"registered\":true,\"file_count\":3,\"dir_exists\":true},\
              \"solana.transactions\":{\"registered\":true,\"file_count\":3,\"dir_exists\":true},\
              \"solana.instructions\":{\"registered\":true,\"file_count\":3,\"dir_exists\":true},\
              \"solana.token_transfers\":{\"registered\":false,\"file_count\":0,\"dir_exists\":false}}"
        );
    }

    #[test]
    fn success_rate_is_rounded_to_three_decimals() {
        let snapshot = EngineMetricsSnapshot {
            total_queries: 47,
            successful_queries: 46,
            failed_queries: 1,
            timed_out_queries: 0,
            total_rows_returned: 0,
            success_rate: 46.0 / 47.0,
            last_query_duration_ms: 83,
            p50_duration_ms: 45,
            p99_duration_ms: 312,
        };
        assert_eq!(EngineHealth::from(snapshot).success_rate, 0.979);
    }
}
