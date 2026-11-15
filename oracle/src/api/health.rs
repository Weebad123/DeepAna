//! `GET /health`: is ORACLE doing its job, and if not, why not.
//!
//! Always 200; the verdict is in `status`, with a reason for anything short
//! of healthy:
//!
//! ```json
//! {
//!   "status": "healthy",
//!   "reasons": [],
//!   "uptime_secs": 3847,
//!   "lens": { "reachable": true, "max_slot": 295000999, "round_trip_ms": 4 },
//!   "detectors": { "watermark": 295000700, "complete_max": 1475002, "lens_ok": true,
//!                  "items": [{ "id": "tx_count", "enabled": true, "state": "UpToDate", "lag_buckets": 1,
//!                              "anomalies_total": 3, "last_error": null }] },
//!   "digest": { "state": "Idle", "last_window": [295000000, 295009999], "windows_built": 12, "lag_windows": 0,
//!               "last_error": null },
//!   "quality": { "available": true, "overall": "warn", "as_of_slot": 295000400, "age_secs": 120 },
//!   "store": { "available": true, "schema_version": 1, "anomalies": 41, "digests": 12 }
//! }
//! ```
//!
//! Every section is read from what already exists, and nothing here costs
//! real work: LENS gets one `MAX(slot)` query bounded by
//! [`LENS_PROBE_TIMEOUT`], the store one stats read bounded by
//! [`STORE_PROBE_TIMEOUT`], and quality is the auditor's last cached report
//! ([`Auditor::last`](crate::quality::Auditor::last)), never a new audit.
//!
//! | `status`    | When |
//! |-------------|------|
//! | `unhealthy` | LENS unreachable, or the store unreadable |
//! | `degraded`  | LENS has no data yet; a detector failing, retrying or ahead of the data; the digester failing or ahead of the data; the last quality report `fail` |
//! | `healthy`   | otherwise |

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::State;
use serde::Serialize;
use serde_json::Value;

use super::{ApiError, ApiJson};
use crate::AppState;
use crate::detect::{DetectorState, MAX_SLOTS_SQL};
use crate::digest::DigestState;
use crate::quality::Overall;

/// How long LENS may take to answer the probe before it counts as down.
pub const LENS_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// How long the store stats read may take.
pub const STORE_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub reasons: Vec<String>,
    pub uptime_secs: u64,
    pub lens: LensSection,
    pub detectors: DetectorsSection,
    pub digest: DigestSection,
    pub quality: QualitySection,
    pub store: StoreSection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LensSection {
    pub reachable: bool,
    /// `null` when LENS is down or has no slots yet.
    pub max_slot: Option<u64>,
    pub round_trip_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DetectorsSection {
    /// The newest slot both tables covered at the last tick.
    pub watermark: Option<u64>,
    pub complete_max: Option<u64>,
    /// Whether the last tick read LENS without error.
    pub lens_ok: bool,
    pub items: Vec<DetectorItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DetectorItem {
    pub id: String,
    pub enabled: bool,
    pub state: &'static str,
    /// Complete buckets not yet evaluated; `null` before the first run.
    pub lag_buckets: Option<u64>,
    /// Anomalies stored by this detector since the process started.
    pub anomalies_total: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DigestSection {
    pub state: &'static str,
    pub last_window: Option<[u64; 2]>,
    pub windows_built: u64,
    pub lag_windows: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QualitySection {
    pub available: bool,
    pub overall: Option<Overall>,
    pub as_of_slot: Option<u64>,
    pub age_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StoreSection {
    pub available: bool,
    pub schema_version: Option<u32>,
    pub anomalies: Option<u64>,
    pub digests: Option<u64>,
}

pub fn detector_state_name(state: &DetectorState) -> (&'static str, Option<String>) {
    match state {
        DetectorState::Pending => ("Pending", None),
        DetectorState::Disabled => ("Disabled", None),
        DetectorState::Baselined => ("Baselined", None),
        DetectorState::UpToDate => ("UpToDate", None),
        DetectorState::CatchingUp => ("CatchingUp", None),
        DetectorState::NotReady(m) => ("NotReady", Some(m.clone())),
        DetectorState::Retrying(m) => ("Retrying", Some(m.clone())),
        DetectorState::AheadOfData(m) => ("AheadOfData", Some(m.clone())),
        DetectorState::Failing(m) => ("Failing", Some(m.clone())),
    }
}

const fn digest_state_name(state: &DigestState) -> &'static str {
    match state {
        DigestState::Idle => "Idle",
        DigestState::Building => "Building",
        DigestState::Waiting { .. } => "Waiting",
        DigestState::NotReady => "NotReady",
        DigestState::AheadOfData => "AheadOfData",
        DigestState::Failing => "Failing",
    }
}

async fn probe_lens(state: &AppState) -> LensSection {
    let down = LensSection { reachable: false, max_slot: None, round_trip_ms: None };
    match tokio::time::timeout(LENS_PROBE_TIMEOUT, state.lens.query(MAX_SLOTS_SQL)).await {
        Ok(Ok(r)) => LensSection {
            reachable: true,
            max_slot: r.rows.first().and_then(|row| row.get("max_slot")).and_then(Value::as_u64),
            round_trip_ms: Some(r.round_trip_ms),
        },
        // LENS answered: it is up, just without the table.
        Ok(Err(forge::lens_client::LensCallError::BadQuery { .. })) => LensSection { reachable: true, ..down },
        Ok(Err(_)) | Err(_) => down,
    }
}

async fn probe_store(state: &AppState) -> StoreSection {
    let store = state.store.clone();
    let read = tokio::task::spawn_blocking(move || store.stats());
    match tokio::time::timeout(STORE_PROBE_TIMEOUT, read).await {
        Ok(Ok(Ok(stats))) => StoreSection {
            available: true,
            schema_version: Some(stats.schema_version),
            anomalies: Some(stats.anomalies),
            digests: Some(stats.digests),
        },
        _ => StoreSection { available: false, schema_version: None, anomalies: None, digests: None },
    }
}

/// `GET /health`.
pub async fn health(State(state): State<Arc<AppState>>) -> Result<ApiJson<HealthResponse>, ApiError> {
    let (lens, store) = tokio::join!(probe_lens(&state), probe_store(&state));

    let runner = state.detectors.status();
    let complete_max = runner.watermark.map(|w| w.complete_max);
    let items: Vec<DetectorItem> = runner
        .detectors
        .iter()
        .map(|d| {
            let (name, last_error) = detector_state_name(&d.state);
            DetectorItem {
                id: d.id.clone(),
                enabled: d.enabled,
                state: name,
                lag_buckets: complete_max.zip(d.progress).map(|(c, p)| c.saturating_sub(p)),
                anomalies_total: d.anomalies_inserted,
                last_error,
            }
        })
        .collect();
    let detectors = DetectorsSection {
        watermark: runner.watermark.map(|w| w.max_slot),
        complete_max,
        lens_ok: runner.tick_error.is_none(),
        items,
    };

    let ds = state.digests.status();
    let digest = DigestSection {
        state: digest_state_name(&ds.state),
        last_window: ds.last_window.map(|w| [w.start_slot, w.end_slot]),
        windows_built: ds.windows_built,
        lag_windows: ds.lag_windows,
        last_error: ds.last_error.clone(),
    };

    let now = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let quality = match state.auditor.last() {
        Some(r) => QualitySection {
            available: true,
            overall: Some(r.overall),
            as_of_slot: Some(r.as_of_slot),
            age_secs: Some(now.saturating_sub(r.generated_at)),
        },
        None => QualitySection { available: false, overall: None, as_of_slot: None, age_secs: None },
    };

    let mut unhealthy = Vec::new();
    let mut degraded = Vec::new();
    if !lens.reachable {
        unhealthy.push("LENS is unreachable".to_owned());
    } else if lens.max_slot.is_none() {
        degraded.push("LENS has no slots yet".to_owned());
    }
    if !store.available {
        unhealthy.push("the store cannot be read".to_owned());
    }
    for d in &detectors.items {
        if matches!(d.state, "Failing" | "Retrying" | "AheadOfData") {
            degraded.push(format!("detector {} is {}", d.id, d.state));
        }
    }
    if matches!(ds.state, DigestState::Failing | DigestState::AheadOfData) {
        degraded.push(format!("the digester is {}", digest.state));
    }
    if quality.overall == Some(Overall::Fail) {
        degraded.push("the last quality report failed".to_owned());
    }
    let status = if !unhealthy.is_empty() {
        HealthStatus::Unhealthy
    } else if !degraded.is_empty() {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    unhealthy.extend(degraded);

    Ok(ApiJson(HealthResponse {
        status,
        reasons: unhealthy,
        uptime_secs: state.started_at.elapsed().as_secs(),
        lens,
        detectors,
        digest,
        quality,
        store,
    }))
}
