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

use std::str::FromStr;
use std::sync::Arc;

use axum::extract::State;

use super::{ApiError, ApiJson, ApiPath, StrictQuery, unless_shutdown};
use crate::AppState;
use crate::profile::{Kind, Profile, ProfileRequest};

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
