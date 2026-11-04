//! `GET /health`: is FORGE doing its job, and if not, why not.
//!
//! Always answers 200 with a [`HealthResponse`], as LENS does; the verdict
//! is in `status`, with the reasons for anything short of healthy:
//!
//! ```json
//! {
//!   "status": "healthy",
//!   "reasons": [],
//!   "uptime_secs": 3847,
//!   "lens": { "reachable": true, "status": "healthy", "round_trip_ms": 4 },
//!   "changelog": { "bytes_behind": 0, "last_target_slot": 295000999 },
//!   "engine": { "events_processed": 120, "evaluations": 40, "matches": 3,
//!               "transient_failures": 0, "permanent_failures": 0, "failing_alerts": [] },
//!   "outbox": { "held": 0, "pending": 0, "delivered": 3, "failed": 0, "oldest_pending_age_secs": null },
//!   "registry": { "schema_version": 2, "alerts": 2, "saved_queries": 1 }
//! }
//! ```
//!
//! Every section is composed from what already exists: LENS's own
//! `/health` ([`crate::LensApi::health`]), the engine's
//! [`EngineStatus`] through its [`crate::EngineHandle`], the outbox's
//! [`OutboxDepth`], and [`Registry::stats`](crate::Registry::stats). LENS
//! gets [`LENS_HEALTH_TIMEOUT`] to answer, so a hung LENS cannot hang this
//! check. A section that could not be read is `null`, with a reason.
//!
//! # Status
//!
//! | `status`    | When                                                                     |
//! |-------------|--------------------------------------------------------------------------|
//! | `unhealthy` | LENS is unreachable or reports itself unhealthy (every query fails), or the registry or outbox cannot be read |
//! | `degraded`  | LENS reports itself degraded, an alert is failing, or the oldest pending delivery is older than [`STALE_PENDING_SECS`] |
//! | `healthy`   | Otherwise                                                                |
//!
//! `failed` deliveries do not count: they are terminal until an operator
//! replays them, and would otherwise pin the status for good.

use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use super::{AppState, blocking};
use crate::alerts::{EngineStatus, FailingAlert};
use crate::lens_client::{LensCallError, LensHealth, LensHealthStatus};
use crate::registry::RegistryStats;
use crate::webhook::OutboxDepth;

/// How long LENS's `/health` may take before LENS counts as unreachable.
pub const LENS_HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
/// A pending delivery older than this means the dispatcher is not keeping
/// up, or the receivers are down.
pub const STALE_PENDING_SECS: u64 = 300;

/// FORGE's verdict on itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// The body of `GET /health`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: HealthStatus,
    /// Why `status` is not `healthy`; empty when it is.
    pub reasons: Vec<String>,
    pub uptime_secs: u64,
    pub lens: LensSection,
    pub changelog: ChangelogSection,
    pub engine: EngineSection,
    /// `null` when the outbox could not be read.
    pub outbox: Option<OutboxSection>,
    /// `null` when the registry could not be read.
    pub registry: Option<RegistryStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LensSection {
    /// Whether LENS answered its `/health` in time.
    pub reachable: bool,
    /// LENS's verdict on itself; `null` when unreachable.
    pub status: Option<LensHealthStatus>,
    /// Measured by FORGE; `null` when unreachable.
    pub round_trip_ms: Option<u64>,
    /// Why LENS is unreachable; absent when it is reachable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangelogSection {
    /// Change log bytes not read yet, at the engine's last poll.
    pub bytes_behind: u64,
    /// The slot alerts are being brought up to; `null` before the first gate.
    pub last_target_slot: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineSection {
    pub events_processed: u64,
    pub evaluations: u64,
    pub matches: u64,
    pub transient_failures: u64,
    pub permanent_failures: u64,
    pub failing_alerts: Vec<FailingAlertView>,
}

/// [`FailingAlert`], for the health body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailingAlertView {
    pub id: String,
    /// `transient` or `permanent`.
    pub kind: String,
    pub consecutive: u32,
    /// Unix seconds of the first failure in the row.
    pub since: i64,
    pub last_error: String,
}

impl From<FailingAlert> for FailingAlertView {
    fn from(failing: FailingAlert) -> Self {
        Self {
            id: failing.id,
            kind: failing.kind.as_str().to_owned(),
            consecutive: failing.consecutive,
            since: failing.since,
            last_error: failing.last_error,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxSection {
    pub held: u64,
    pub pending: u64,
    pub delivered: u64,
    pub failed: u64,
    /// Age of the oldest `pending` delivery; `null` when none is pending.
    pub oldest_pending_age_secs: Option<u64>,
}

impl From<OutboxDepth> for OutboxSection {
    fn from(depth: OutboxDepth) -> Self {
        Self {
            held: depth.held,
            pending: depth.pending,
            delivered: depth.delivered,
            failed: depth.failed,
            oldest_pending_age_secs: depth.oldest_pending_age_ms.map(|ms| ms / 1_000),
        }
    }
}

/// Handler for `GET /health`.
pub async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let lens = async {
        match tokio::time::timeout(LENS_HEALTH_TIMEOUT, state.lens.health()).await {
            Ok(answer) => answer,
            Err(_) => Err(LensCallError::Timeout {
                message: format!("GET /health did not answer within {}s", LENS_HEALTH_TIMEOUT.as_secs()),
            }),
        }
    };
    let outbox = {
        let webhook = state.webhook.clone();
        blocking(move || webhook.depth())
    };
    let registry = {
        let registry = state.registry.clone();
        blocking(move || registry.stats())
    };
    let (lens, outbox, registry) = tokio::join!(lens, outbox, registry);
    let outbox = outbox.map(OutboxSection::from).map_err(|err| err.body.error);
    let registry = registry.map_err(|err| err.body.error);
    let engine = state.engine.status();

    let (status, reasons) = compute_status(&lens, &engine, outbox.as_ref(), registry.as_ref());
    if status != HealthStatus::Healthy {
        tracing::warn!(?status, ?reasons, "health check is not healthy");
    }
    Json(HealthResponse {
        status,
        reasons,
        uptime_secs: uptime_secs(state.start_time),
        lens: match lens {
            Ok(health) => LensSection {
                reachable: true,
                status: Some(health.status),
                round_trip_ms: Some(health.round_trip_ms),
                error: None,
            },
            Err(err) => LensSection { reachable: false, status: None, round_trip_ms: None, error: Some(err.to_string()) },
        },
        changelog: ChangelogSection {
            bytes_behind: engine.changelog_bytes_behind,
            last_target_slot: engine.target_slot,
        },
        outbox: outbox.ok(),
        registry: registry.ok(),
        engine: EngineSection {
            events_processed: engine.events_processed,
            evaluations: engine.evaluations,
            matches: engine.matches,
            transient_failures: engine.transient_failures,
            permanent_failures: engine.permanent_failures,
            failing_alerts: engine.failing_alerts.into_iter().map(FailingAlertView::from).collect(),
        },
    })
}

/// The verdict and its reasons. See the [module docs](self#status).
pub fn compute_status(
    lens: &Result<LensHealth, LensCallError>,
    engine: &EngineStatus,
    outbox: Result<&OutboxSection, &String>,
    registry: Result<&RegistryStats, &String>,
) -> (HealthStatus, Vec<String>) {
    let mut unhealthy = Vec::new();
    let mut degraded = Vec::new();
    match lens {
        Err(err) => unhealthy.push(format!("LENS is unreachable: {err}")),
        Ok(LensHealth { status: LensHealthStatus::Unhealthy, .. }) => {
            unhealthy.push("LENS reports itself unhealthy; every query will fail".to_owned());
        }
        Ok(LensHealth { status: LensHealthStatus::Degraded, .. }) => {
            degraded.push("LENS reports itself degraded".to_owned());
        }
        Ok(LensHealth { status: LensHealthStatus::Healthy, .. }) => {}
    }
    if let Err(err) = registry {
        unhealthy.push(format!("the registry cannot be read: {err}"));
    }
    match outbox {
        Err(err) => unhealthy.push(format!("the outbox cannot be read: {err}")),
        Ok(OutboxSection { oldest_pending_age_secs: Some(age), .. }) if *age > STALE_PENDING_SECS => {
            degraded.push(format!("the oldest pending delivery is {age}s old"));
        }
        Ok(_) => {}
    }
    if !engine.failing_alerts.is_empty() {
        let ids: Vec<&str> = engine.failing_alerts.iter().map(|failing| failing.id.as_str()).collect();
        degraded.push(format!("{} alert(s) failing: {}", ids.len(), ids.join(", ")));
    }
    let status = if !unhealthy.is_empty() {
        HealthStatus::Unhealthy
    } else if !degraded.is_empty() {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    };
    unhealthy.extend(degraded);
    (status, unhealthy)
}

fn uptime_secs(start_time: Instant) -> u64 {
    start_time.elapsed().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::FailureKind;

    fn lens(status: LensHealthStatus) -> Result<LensHealth, LensCallError> {
        Ok(LensHealth { status, round_trip_ms: 1 })
    }

    fn outbox(oldest: Option<u64>) -> OutboxSection {
        OutboxSection { held: 0, pending: 1, delivered: 0, failed: 9, oldest_pending_age_secs: oldest }
    }

    const STATS: RegistryStats = RegistryStats { schema_version: 2, alerts: 1, saved_queries: 0 };

    fn verdict(
        lens: Result<LensHealth, LensCallError>,
        engine: &EngineStatus,
        outbox: Result<&OutboxSection, &String>,
        registry: Result<&RegistryStats, &String>,
    ) -> HealthStatus {
        compute_status(&lens, engine, outbox, registry).0
    }

    #[test]
    fn everything_fine_is_healthy_with_no_reasons() {
        let (status, reasons) =
            compute_status(&lens(LensHealthStatus::Healthy), &EngineStatus::default(), Ok(&outbox(Some(3))), Ok(&STATS));
        assert_eq!(status, HealthStatus::Healthy);
        assert!(reasons.is_empty(), "{reasons:?}");
    }

    #[test]
    fn failed_deliveries_alone_do_not_degrade() {
        let ok = EngineStatus::default();
        assert_eq!(verdict(lens(LensHealthStatus::Healthy), &ok, Ok(&outbox(None)), Ok(&STATS)), HealthStatus::Healthy);
    }

    #[test]
    fn lens_down_or_unhealthy_and_unreadable_storage_are_unhealthy() {
        let ok = EngineStatus::default();
        let down = Err(LensCallError::Unavailable { message: "refused".into() });
        assert_eq!(verdict(down, &ok, Ok(&outbox(None)), Ok(&STATS)), HealthStatus::Unhealthy);
        assert_eq!(verdict(lens(LensHealthStatus::Unhealthy), &ok, Ok(&outbox(None)), Ok(&STATS)), HealthStatus::Unhealthy);
        let broken = "disk I/O error".to_owned();
        assert_eq!(verdict(lens(LensHealthStatus::Healthy), &ok, Err(&broken), Ok(&STATS)), HealthStatus::Unhealthy);
        assert_eq!(verdict(lens(LensHealthStatus::Healthy), &ok, Ok(&outbox(None)), Err(&broken)), HealthStatus::Unhealthy);
    }

    #[test]
    fn degraded_lens_failing_alerts_and_stale_deliveries_are_degraded() {
        let ok = EngineStatus::default();
        assert_eq!(verdict(lens(LensHealthStatus::Degraded), &ok, Ok(&outbox(None)), Ok(&STATS)), HealthStatus::Degraded);
        let stale = outbox(Some(STALE_PENDING_SECS + 1));
        assert_eq!(verdict(lens(LensHealthStatus::Healthy), &ok, Ok(&stale), Ok(&STATS)), HealthStatus::Degraded);
        let at_limit = outbox(Some(STALE_PENDING_SECS));
        assert_eq!(verdict(lens(LensHealthStatus::Healthy), &ok, Ok(&at_limit), Ok(&STATS)), HealthStatus::Healthy);

        let failing = EngineStatus {
            failing_alerts: vec![FailingAlert {
                id: "a1".into(),
                kind: FailureKind::Permanent,
                consecutive: 2,
                since: 0,
                last_error: "LENS rejected the query".into(),
            }],
            ..EngineStatus::default()
        };
        let (status, reasons) = compute_status(&lens(LensHealthStatus::Healthy), &failing, Ok(&outbox(None)), Ok(&STATS));
        assert_eq!(status, HealthStatus::Degraded);
        assert_eq!(reasons, ["1 alert(s) failing: a1"]);
    }

    #[test]
    fn unhealthy_reasons_come_first_and_all_are_kept() {
        let down = Err(LensCallError::Timeout { message: "slow".into() });
        let stale = outbox(Some(STALE_PENDING_SECS * 2));
        let (status, reasons) = compute_status(&down, &EngineStatus::default(), Ok(&stale), Ok(&STATS));
        assert_eq!(status, HealthStatus::Unhealthy);
        assert_eq!(reasons.len(), 2, "{reasons:?}");
        assert!(reasons[0].starts_with("LENS is unreachable"), "{reasons:?}");
    }

    #[test]
    fn outbox_ages_are_reported_in_whole_seconds() {
        let depth = OutboxDepth { held: 1, pending: 2, delivered: 3, failed: 4, oldest_pending_age_ms: Some(61_999) };
        assert_eq!(OutboxSection::from(depth).oldest_pending_age_secs, Some(61));
    }
}
