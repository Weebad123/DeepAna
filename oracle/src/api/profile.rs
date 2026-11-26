//! `GET /profile/{kind}/{id}?window_slots=&as_of_slot=`: an entity profile.
//!
//! `kind` is `mint`, `program` or `token_account`; `id` a base58 key. Both
//! are checked by `profile/` before anything reaches LENS. `window_slots`
//! and `as_of_slot` are optional, ASCII digits only; their ranges are the
//! profiler's, so a bad value is the profiler's own 400. The
//! [`Profile`](crate::profile::Profile) is returned unchanged.
//!
//! A profile runs several LENS queries and holds one of
//! [`MAX_CONCURRENT_PROFILES`](crate::profile::MAX_CONCURRENT_PROFILES)
//! permits; it is the heaviest GET. A result LENS truncated is a 502: the
//! numbers cannot be trusted, and a narrower window is the remedy.
//!
//! # Detail reports
//!
//! * `GET /profile/transaction/{signature}?slot=&window_slots=&as_of_slot=`:
//!   one transaction in full, a
//!   [`TransactionReport`](crate::profile::TransactionReport). `slot`, when
//!   the caller knows it, bounds the search to that slot; without it the
//!   last `window_slots` are searched. A signature that is not there is a
//!   200 with `found: false`, as an entity with no activity is.
//! * `GET /profile/slot/{slot}?after_index=&limit=&as_of_slot=`: one slot
//!   and a page of its transactions, a
//!   [`SlotReport`](crate::profile::SlotReport).
//!
//! Both are what a client opens from a row of `POST /drill`. They hold a
//! profile permit and answer with the same errors as a profile.

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::State;

use super::{ApiError, ApiJson, ApiPath, StrictQuery, parse_digits, unless_shutdown};
use crate::AppState;
use crate::profile::{Kind, Profile, ProfileRequest, SlotReport, SlotRequest, TransactionReport, TransactionRequest};

/// `GET /profile/{kind}/{id}`.
pub async fn profile(
    State(state): State<Arc<AppState>>,
    ApiPath((kind, id)): ApiPath<(String, String)>,
    mut q: StrictQuery,
) -> Result<ApiJson<Profile>, ApiError> {
    let window_slots = q.take_u64("window_slots", 0..=u64::MAX)?;
    let as_of_slot = q.take_u64("as_of_slot", 0..=u64::MAX)?;
    q.finish()?;
    let kind = Kind::from_str(&kind)?;
    let req = ProfileRequest { kind, id, window_slots, as_of_slot };
    let profiler = Arc::clone(&state.profiler);
    let profile = unless_shutdown(&state, async move { Ok(profiler.profile(req).await?) }).await?;
    Ok(ApiJson(profile))
}

/// `GET /profile/transaction/{signature}`.
pub async fn transaction(
    State(state): State<Arc<AppState>>,
    ApiPath(signature): ApiPath<String>,
    mut q: StrictQuery,
) -> Result<ApiJson<TransactionReport>, ApiError> {
    let slot = q.take_u64("slot", 0..=u64::MAX)?;
    let window_slots = q.take_u64("window_slots", 0..=u64::MAX)?;
    let as_of_slot = q.take_u64("as_of_slot", 0..=u64::MAX)?;
    q.finish()?;
    let req = TransactionRequest { signature, slot, window_slots, as_of_slot };
    let profiler = Arc::clone(&state.profiler);
    let report = unless_shutdown(&state, async move { Ok(profiler.transaction(req).await?) }).await?;
    Ok(ApiJson(report))
}

/// `GET /profile/slot/{slot}`.
pub async fn slot(
    State(state): State<Arc<AppState>>,
    ApiPath(slot): ApiPath<String>,
    mut q: StrictQuery,
) -> Result<ApiJson<SlotReport>, ApiError> {
    let after_index = q.take_u64("after_index", 0..=u64::from(u32::MAX))?.map(|i| i as u32);
    let limit = q.take_u64("limit", 0..=u64::MAX)?;
    let as_of_slot = q.take_u64("as_of_slot", 0..=u64::MAX)?;
    q.finish()?;
    let slot = parse_digits(&slot).ok_or_else(|| ApiError::invalid("slot", "must be an unsigned integer"))?;
    let req = SlotRequest { slot, after_index, limit, as_of_slot };
    let profiler = Arc::clone(&state.profiler);
    let report = unless_shutdown(&state, async move { Ok(profiler.slot(req).await?) }).await?;
    Ok(ApiJson(report))
}
