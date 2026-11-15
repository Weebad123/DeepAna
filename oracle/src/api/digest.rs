//! Digests.
//!
//! * `GET /digest`: the latest stored digest, 404 `NotFound` before the
//!   first one is built.
//! * `GET /digests?limit=`: recent stored digests, newest first, `limit` 1
//!   to [`MAX_DIGESTS_LIMIT`] (the default).
//! * `GET /digest/preview?window_start=`: [`DigestHandle::preview`], built
//!   now and not stored. One preview runs at a time; another is a 429
//!   `Busy` with `Retry-After: 5`.
//!
//! A stored digest is `{ "id", "start_slot", "end_slot", "created_at",
//! "body" }`, `body` being the stored JSON as the store returns it: the
//! store serialised it with `serde_json` and `serde_json` writes it back
//! the same, byte for byte. A body the store cannot read is a 500.
//!
//! [`DigestHandle::preview`]: crate::digest::DigestHandle::preview

use std::sync::Arc;

use axum::extract::State;
use serde::Serialize;

use super::{ApiError, ApiJson, StrictQuery, blocking, unless_shutdown};
use crate::AppState;
use crate::digest::{Digest, StoredDigest};

pub const MAX_DIGESTS_LIMIT: u64 = 10;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DigestList {
    pub digests: Vec<StoredDigest>,
}

/// `GET /digest`.
pub async fn latest(State(state): State<Arc<AppState>>, q: StrictQuery) -> Result<ApiJson<StoredDigest>, ApiError> {
    q.finish()?;
    let handle = state.digests.clone();
    blocking(move || handle.latest())
        .await?
        .map(ApiJson)
        .ok_or_else(|| ApiError::NotFound("no digest has been built yet".to_owned()))
}

/// `GET /digests`.
pub async fn list(State(state): State<Arc<AppState>>, mut q: StrictQuery) -> Result<ApiJson<DigestList>, ApiError> {
    let limit = q.take_u64("limit", 1..=MAX_DIGESTS_LIMIT)?.unwrap_or(MAX_DIGESTS_LIMIT);
    q.finish()?;
    let handle = state.digests.clone();
    let digests = blocking(move || handle.list(limit as usize)).await?;
    Ok(ApiJson(DigestList { digests }))
}

/// `GET /digest/preview`.
pub async fn preview(State(state): State<Arc<AppState>>, mut q: StrictQuery) -> Result<ApiJson<Digest>, ApiError> {
    let window_start = q.take_u64("window_start", 0..=u64::MAX)?;
    q.finish()?;
    let handle = state.digests.clone();
    let digest = unless_shutdown(&state, async move { Ok(handle.preview(window_start).await?) }).await?;
    Ok(ApiJson(digest))
}
