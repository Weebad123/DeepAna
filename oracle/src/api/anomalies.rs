//! Stored anomalies and the detectors that find them.
//!
//! * `GET /anomalies?metric=&subject=&since_bucket=&until_bucket=&limit=`:
//!   stored anomalies, newest bucket first ([`Store::list_anomalies`]).
//!   `metric` must be a detector id; `subject=` (empty) selects global
//!   series only; `since_bucket <= until_bucket`; `limit` is 1 to
//!   [`MAX_LIST_LIMIT`], default [`DEFAULT_LIST_LIMIT`].
//! * `GET /detectors`: [`RunnerHandle::status`], unchanged.
//! * `PATCH /detectors/{id}`: `{ "enabled": bool }`, required and strictly a
//!   JSON boolean. Returns the detector's new status. In memory only, as the
//!   runner documents.
//! * `POST /detectors/{id}/reset-progress`: no body. Forgets the detector's
//!   progress so its next turn baselines again, after any turn in flight
//!   ends. Returns the cleared progress and the new status.
//!
//! An unknown detector id is a 404. Both control routes are logged at
//! `info` with the request id: they change what ORACLE reports.
//!
//! [`Store::list_anomalies`]: crate::store::Store::list_anomalies
//! [`RunnerHandle::status`]: crate::detect::RunnerHandle::status

use std::sync::Arc;

use axum::Extension;
use axum::body::Bytes;
use axum::extract::State;
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiJson, ApiPath, RequestId, StrictQuery, blocking, unless_shutdown};
use crate::AppState;
use crate::catalog::MAX_SLOT;
use crate::detect::{DetectorStatus, RunnerStatus};
use crate::store::{Anomaly, AnomalyFilter, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT, MAX_SUBJECT_CHARS};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AnomalyList {
    pub anomalies: Vec<Anomaly>,
}

fn unknown_detector(id: &str) -> ApiError {
    // The id is the client's own text; bound what is echoed.
    let shown: String = id.chars().take(64).collect();
    ApiError::NotFound(format!("no detector {shown:?}"))
}

/// `GET /anomalies`.
pub async fn list_anomalies(
    State(state): State<Arc<AppState>>,
    mut q: StrictQuery,
) -> Result<ApiJson<AnomalyList>, ApiError> {
    let metric = q.take_str("metric");
    let subject = q.take_str("subject");
    let since_bucket = q.take_u64("since_bucket", 0..=MAX_SLOT)?;
    let until_bucket = q.take_u64("until_bucket", 0..=MAX_SLOT)?;
    let limit = q.take_u64("limit", 1..=MAX_LIST_LIMIT as u64)?.unwrap_or(DEFAULT_LIST_LIMIT as u64);
    q.finish()?;
    if let Some(m) = &metric
        && !state.detectors.ids().iter().any(|id| id == m)
    {
        return Err(ApiError::invalid("metric", format!("must be one of {}", state.detectors.ids().join(", "))));
    }
    if let Some(s) = &subject
        && s.chars().count() > MAX_SUBJECT_CHARS
    {
        return Err(ApiError::invalid("subject", format!("must be at most {MAX_SUBJECT_CHARS} characters")));
    }
    if let (Some(since), Some(until)) = (since_bucket, until_bucket)
        && since > until
    {
        return Err(ApiError::invalid("until_bucket", format!("{until} is before since_bucket {since}")));
    }
    let filter = AnomalyFilter { metric, subject, since_bucket, until_bucket, limit: limit as usize };
    let store = state.store.clone();
    let anomalies = blocking(move || store.list_anomalies(&filter)).await?;
    Ok(ApiJson(AnomalyList { anomalies }))
}

/// `GET /detectors`.
pub async fn list_detectors(State(state): State<Arc<AppState>>, q: StrictQuery) -> Result<ApiJson<RunnerStatus>, ApiError> {
    q.finish()?;
    Ok(ApiJson(state.detectors.status()))
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetEnabled {
    pub enabled: bool,
}

/// `PATCH /detectors/{id}`.
pub async fn set_enabled(
    State(state): State<Arc<AppState>>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiPath(id): ApiPath<String>,
    q: StrictQuery,
    ApiJson(body): ApiJson<SetEnabled>,
) -> Result<ApiJson<DetectorStatus>, ApiError> {
    q.finish()?;
    let status = state.detectors.set_enabled(&id, body.enabled).ok_or_else(|| unknown_detector(&id))?;
    tracing::info!(request_id, detector = %id, enabled = body.enabled, "detector enable state set over the API");
    Ok(ApiJson(status))
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResetResponse {
    pub id: String,
    /// The progress that was forgotten; `null` if there was none.
    pub cleared_progress: Option<u64>,
    pub detector: DetectorStatus,
}

/// `POST /detectors/{id}/reset-progress`.
pub async fn reset_progress(
    State(state): State<Arc<AppState>>,
    Extension(RequestId(request_id)): Extension<RequestId>,
    ApiPath(id): ApiPath<String>,
    q: StrictQuery,
    body: Bytes,
) -> Result<ApiJson<ResetResponse>, ApiError> {
    q.finish()?;
    if !body.is_empty() {
        return Err(ApiError::bad_request("this route takes no request body"));
    }
    if state.detectors.detector(&id).is_none() {
        return Err(unknown_detector(&id));
    }
    let handle = state.detectors.clone();
    let target = id.clone();
    let cleared = unless_shutdown(&state, async move { Ok(handle.reset_progress(&target).await?) }).await?;
    let detector = state.detectors.detector(&id).ok_or_else(|| unknown_detector(&id))?;
    tracing::info!(request_id, detector = %id, cleared = ?cleared, "detector progress reset over the API");
    Ok(ApiJson(ResetResponse { id, cleared_progress: cleared, detector }))
}
