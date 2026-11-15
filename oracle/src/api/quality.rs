//! `GET /quality?window_slots=&as_of_slot=&max_age_secs=`: pipeline health
//! checks.
//!
//! * With neither `window_slots` nor `as_of_slot`: [`Auditor::report`] with
//!   `max_age_secs` (default [`DEFAULT_MAX_AGE_SECS`], at most
//!   [`MAX_MAX_AGE_SECS`]): the cached default report if it is fresh enough,
//!   else a new audit that every concurrent caller shares.
//! * With either: [`Auditor::run`], a new audit for exactly that request,
//!   never cached. `max_age_secs` means nothing there and is a 400.
//!
//! The [`QualityReport`] is returned unchanged.
//!
//! [`Auditor::report`]: crate::quality::Auditor::report
//! [`Auditor::run`]: crate::quality::Auditor::run

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::response::{IntoResponse, Response};

use super::{ApiError, ApiJson, StrictQuery, unless_shutdown};
use crate::AppState;
use crate::quality::{AuditRequest, QualityReport};

pub const DEFAULT_MAX_AGE_SECS: u64 = 300;
pub const MAX_MAX_AGE_SECS: u64 = 86_400;

/// `GET /quality`.
pub async fn quality(State(state): State<Arc<AppState>>, mut q: StrictQuery) -> Result<Response, ApiError> {
    let window_slots = q.take_u64("window_slots", 0..=u64::MAX)?;
    let as_of_slot = q.take_u64("as_of_slot", 0..=u64::MAX)?;
    let max_age = q.take_u64("max_age_secs", 0..=MAX_MAX_AGE_SECS)?;
    q.finish()?;
    let auditor = Arc::clone(&state.auditor);
    if window_slots.is_none() && as_of_slot.is_none() {
        let max_age = Duration::from_secs(max_age.unwrap_or(DEFAULT_MAX_AGE_SECS));
        let report = unless_shutdown(&state, async move { Ok(auditor.report(max_age).await?) }).await?;
        return Ok(ApiJson::<&QualityReport>(&report).into_response());
    }
    if max_age.is_some() {
        return Err(ApiError::invalid(
            "max_age_secs",
            "applies only to the default report; drop it, or drop window_slots and as_of_slot",
        ));
    }
    let req = AuditRequest { window_slots, as_of_slot };
    let report = unless_shutdown(&state, async move { Ok(auditor.run(req).await?) }).await?;
    Ok(ApiJson(report).into_response())
}
