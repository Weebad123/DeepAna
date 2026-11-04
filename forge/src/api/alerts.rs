//! Alerts: create, read, update, delete; their delivery history; replaying a
//! failed delivery.
//!
//! # `POST /alerts`
//!
//! ```json
//! {"name": "whale-transfers",
//!  "sql": "SELECT * FROM solana.token_transfers WHERE slot BETWEEN {{from_slot}} AND {{to_slot}} AND amount_raw > 1000000000",
//!  "webhook_url": "https://hooks.example.com/forge",
//!  "secret": "optional, 16 to 512 bytes",
//!  "wait_for_final": false}
//! ```
//!
//! In this order, stopping at the first failure:
//!
//! 1. **Validate** with the registry's own checks ([`NewAlert::validate`]).
//!    Without a `secret`, FORGE generates one: 64 hex digits from two UUID
//!    v4s, 244 random bits.
//! 2. **Dry run**: LENS runs the SQL bound to the range `0..=0`. No row of
//!    slot 0 exists, so it matches nothing (whatever it returns is
//!    discarded), but DataFusion still resolves every table and column, so a
//!    typo fails here rather than on the alert's first evaluation.
//! 3. **Create** the alert in the registry (`409` for a taken name).
//! 4. **Seed** its progress at LENS's `MAX(slot)` of `solana.slots`
//!    ([`Registry::advance_progress`]), so it fires on rows from now on.
//!    With `solana.slots` still empty the alert is left unseeded and the
//!    engine baselines it at its next gate. If the engine got there first,
//!    its progress is kept. If the seed query fails, the alert is deleted
//!    again and the LENS error returned: either the whole request happened
//!    or none of it did.
//! 5. **Respond** `201`, `Location: /alerts/{id}`, with a [`CreatedAlert`]:
//!    the alert and its `secret`. This is the only response that ever
//!    carries the secret.
//!
//! # `PATCH /alerts/{id}`
//!
//! Any of `name`, `sql`, `webhook_url`, `secret`, `wait_for_final`,
//! `enabled`; at least one. Every field is validated before anything is
//! written, and a changed `sql` is dry-run as above. Progress is kept,
//! except that re-enabling a disabled alert clears it
//! ([`Registry::set_enabled`]); the engine then baselines it again.
//! A rotated secret is not echoed back.
//!
//! # `GET /alerts/{id}/deliveries?limit=&status=`
//!
//! Newest first. `limit` is 1 to [`MAX_LIST_LIMIT`], default
//! [`DEFAULT_HISTORY_LIMIT`]; `status` is `held`, `pending`, `delivered` or
//! `failed`, and filters before the limit. Metadata only: the payload is
//! not returned, only its size. Any other parameter is a `400`.
//!
//! # `POST /deliveries/{id}/replay`
//!
//! Only a `failed` delivery can be replayed (`409` otherwise). It goes back
//! to `pending` with its attempts reset, the dispatcher is woken, and the
//! answer is `202` with the delivery as it now stands: the send itself has
//! not happened yet.

use axum::Json;
use axum::extract::{RawQuery, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::{ApiError, ApiJson, ApiPath, AppState, blocking, parse_query_string};
use crate::lens_client::{VISIBILITY_SQL, max_slot_of};
use crate::registry::{Alert, AlertUpdate, NewAlert, Registry, RegistryError, bind_alert_sql};
use crate::webhook::{Delivery, DeliveryStatus, outbox::MAX_LIST_LIMIT};

/// Deliveries `GET /alerts/{id}/deliveries` returns without a `limit`.
pub const DEFAULT_HISTORY_LIMIT: usize = 50;

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// The body of `POST /alerts`.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateAlertRequest {
    pub name: String,
    pub sql: String,
    pub webhook_url: String,
    /// Generated when absent.
    #[serde(default)]
    pub secret: Option<String>,
    #[serde(default)]
    pub wait_for_final: bool,
}

impl std::fmt::Debug for CreateAlertRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateAlertRequest")
            .field("name", &self.name)
            .field("sql", &self.sql)
            .field("webhook_url", &self.webhook_url)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("wait_for_final", &self.wait_for_final)
            .finish()
    }
}

/// The body of `PATCH /alerts/{id}`: each field that is set replaces the
/// stored one.
#[derive(Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateAlertRequest {
    pub name: Option<String>,
    pub sql: Option<String>,
    pub webhook_url: Option<String>,
    pub secret: Option<String>,
    pub wait_for_final: Option<bool>,
    pub enabled: Option<bool>,
}

impl std::fmt::Debug for UpdateAlertRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpdateAlertRequest")
            .field("name", &self.name)
            .field("sql", &self.sql)
            .field("webhook_url", &self.webhook_url)
            .field("secret", &self.secret.as_ref().map(|_| "<redacted>"))
            .field("wait_for_final", &self.wait_for_final)
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl UpdateAlertRequest {
    /// The registry's part, and `enabled`.
    fn split(self) -> (AlertUpdate, Option<bool>) {
        let update = AlertUpdate {
            name: self.name,
            sql: self.sql,
            webhook_url: self.webhook_url,
            secret: self.secret,
            wait_for_final: self.wait_for_final,
        };
        (update, self.enabled)
    }
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// An alert as the API shows it. Has no secret field, so no response built
/// from it can carry the secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertView {
    pub id: String,
    pub name: String,
    pub sql: String,
    pub webhook_url: String,
    pub wait_for_final: bool,
    pub enabled: bool,
    pub last_evaluated_slot: Option<u64>,
    /// Unix seconds.
    pub created_at: i64,
    /// Unix seconds.
    pub updated_at: i64,
}

impl From<&Alert> for AlertView {
    fn from(alert: &Alert) -> Self {
        Self {
            id: alert.id.clone(),
            name: alert.name.clone(),
            sql: alert.sql.clone(),
            webhook_url: alert.webhook_url.clone(),
            wait_for_final: alert.wait_for_final,
            enabled: alert.enabled,
            last_evaluated_slot: alert.last_evaluated_slot,
            created_at: alert.created_at,
            updated_at: alert.updated_at,
        }
    }
}

/// The body of `POST /alerts`: the alert, and its secret this once.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedAlert {
    #[serde(flatten)]
    pub alert: AlertView,
    pub secret: String,
}

impl std::fmt::Debug for CreatedAlert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreatedAlert").field("alert", &self.alert).field("secret", &"<redacted>").finish()
    }
}

/// The body of `GET /alerts`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertList {
    /// By name.
    pub alerts: Vec<AlertView>,
}

/// One delivery's metadata, without its payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryView {
    pub delivery_id: String,
    pub alert_id: String,
    pub to_slot: u64,
    pub status: DeliveryStatus,
    pub release_after_slot: Option<u64>,
    pub attempts: u32,
    pub last_status_code: Option<u16>,
    /// Stored with the alert's secret and webhook URL already redacted.
    pub last_error: Option<String>,
    /// Size of the payload, in bytes.
    pub payload_bytes: usize,
    /// Unix milliseconds, as every delivery time.
    pub created_at: i64,
    pub next_attempt_at: i64,
    pub last_attempt_at: Option<i64>,
    pub delivered_at: Option<i64>,
}

impl From<Delivery> for DeliveryView {
    fn from(delivery: Delivery) -> Self {
        Self {
            payload_bytes: delivery.payload.len(),
            delivery_id: delivery.delivery_id,
            alert_id: delivery.alert_id,
            to_slot: delivery.to_slot,
            status: delivery.status,
            release_after_slot: delivery.release_after_slot,
            attempts: delivery.attempts,
            last_status_code: delivery.last_status_code,
            last_error: delivery.last_error,
            created_at: delivery.created_at,
            next_attempt_at: delivery.next_attempt_at,
            last_attempt_at: delivery.last_attempt_at,
            delivered_at: delivery.delivered_at,
        }
    }
}

/// The body of `GET /alerts/{id}/deliveries`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryHistory {
    pub alert_id: String,
    /// Newest first.
    pub deliveries: Vec<DeliveryView>,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Handler for `POST /alerts`. See the [module docs](self#post-alerts).
pub async fn create_alert(
    State(state): State<AppState>,
    ApiJson(request): ApiJson<CreateAlertRequest>,
) -> Result<Response, ApiError> {
    // 1. Validate.
    let new = NewAlert {
        name: request.name,
        sql: request.sql,
        webhook_url: request.webhook_url,
        secret: request.secret.unwrap_or_else(generate_secret),
        wait_for_final: request.wait_for_final,
    };
    new.validate().map_err(|err| ApiError::from_registry(&err))?;

    // 2. Dry run.
    let dry_run = new.bind_range(0, 0).map_err(|err| ApiError::from_registry(&err))?;
    dry_run_on_lens(&state, &dry_run).await?;

    // 3. Create.
    let secret = new.secret.clone();
    let registry = state.registry.clone();
    let alert = blocking(move || registry.create_alert(new)).await?;

    // 4. Seed, or undo the create.
    let seeded = match seed(&state, &alert.id).await {
        Ok(seeded) => seeded,
        Err(err) => {
            undo_create(&state, &alert.id).await;
            return Err(err);
        }
    };
    tracing::info!(alert_id = %alert.id, name = %alert.name, seeded, "alert created");

    // 5. Respond, with the alert as stored now: seeding moved it.
    let registry = state.registry.clone();
    let alert = blocking(move || registry.get_alert(&alert.id)).await?;
    let location = format!("/alerts/{}", alert.id);
    let body = CreatedAlert { alert: AlertView::from(&alert), secret };
    Ok((StatusCode::CREATED, [(header::LOCATION, location)], Json(body)).into_response())
}

/// Handler for `GET /alerts`.
pub async fn list_alerts(State(state): State<AppState>) -> Result<Json<AlertList>, ApiError> {
    let registry = state.registry.clone();
    let alerts = blocking(move || registry.list_alerts()).await?;
    Ok(Json(AlertList { alerts: alerts.iter().map(AlertView::from).collect() }))
}

/// Handler for `GET /alerts/{id}`.
pub async fn get_alert(State(state): State<AppState>, ApiPath(id): ApiPath<String>) -> Result<Json<AlertView>, ApiError> {
    let registry = state.registry.clone();
    let alert = blocking(move || registry.get_alert(&id)).await?;
    Ok(Json(AlertView::from(&alert)))
}

/// Handler for `PATCH /alerts/{id}`. See the
/// [module docs](self#patch-alertsid).
pub async fn update_alert(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    ApiJson(request): ApiJson<UpdateAlertRequest>,
) -> Result<Json<AlertView>, ApiError> {
    if request == UpdateAlertRequest::default() {
        return Err(ApiError::invalid(
            "body",
            "sets no field; send at least one of name, sql, webhook_url, secret, wait_for_final, enabled",
        ));
    }
    let registry = state.registry.clone();
    let current = blocking({
        let id = id.clone();
        move || registry.get_alert(&id)
    })
    .await?;

    let (update, enabled) = request.split();
    // Every field first, so nothing is written, and LENS is not asked, for
    // a request that is going to fail anyway.
    update.clone().apply_to(&mut current.clone()).map_err(|err| ApiError::from_registry(&err))?;
    if let Some(sql) = update.sql.as_ref().filter(|sql| **sql != current.sql) {
        let dry_run = bind_alert_sql(sql, 0, 0).map_err(|err| ApiError::from_registry(&err))?;
        dry_run_on_lens(&state, &dry_run).await?;
    }

    let registry = state.registry.clone();
    let alert = blocking(move || {
        let mut alert = current;
        if update != AlertUpdate::default() {
            alert = registry.update_alert(&id, update)?;
        }
        if let Some(enabled) = enabled {
            alert = registry.set_enabled(&id, enabled)?;
        }
        Ok(alert)
    })
    .await?;
    tracing::info!(alert_id = %alert.id, enabled = alert.enabled, "alert updated");
    Ok(Json(AlertView::from(&alert)))
}

/// Handler for `DELETE /alerts/{id}`. Its deliveries go with it, sent or
/// not.
pub async fn delete_alert(State(state): State<AppState>, ApiPath(id): ApiPath<String>) -> Result<StatusCode, ApiError> {
    let registry = state.registry.clone();
    let deleted = id.clone();
    blocking(move || registry.delete_alert(&deleted)).await?;
    tracing::info!(alert_id = %id, "alert deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Handler for `GET /alerts/{id}/deliveries`. See the
/// [module docs](self#get-alertsiddeliverieslimitstatus).
pub async fn list_deliveries(
    State(state): State<AppState>,
    ApiPath(id): ApiPath<String>,
    RawQuery(raw): RawQuery,
) -> Result<Json<DeliveryHistory>, ApiError> {
    let mut limit = DEFAULT_HISTORY_LIMIT;
    let mut status = None;
    for (key, value) in parse_query_string(raw.as_deref())? {
        match key.as_str() {
            "limit" => limit = parse_limit(&value)?,
            "status" => {
                let parsed = DeliveryStatus::parse(&value).ok_or_else(|| {
                    let all: Vec<&str> = DeliveryStatus::ALL.iter().map(|status| status.as_str()).collect();
                    ApiError::invalid("status", format!("{value:?} is not one of {}", all.join(", ")))
                })?;
                status = Some(parsed);
            }
            other => {
                return Err(ApiError::invalid_request(format!(
                    "unknown query parameter {other:?}; expected limit or status"
                )));
            }
        }
    }

    let registry = state.registry.clone();
    let webhook = state.webhook.clone();
    let deliveries = blocking(move || {
        // An unknown alert has no deliveries; say it does not exist instead.
        registry.get_alert(&id)?;
        Ok((id.clone(), webhook.history(&id, limit, status)?))
    })
    .await;
    let (alert_id, deliveries) = deliveries?;
    Ok(Json(DeliveryHistory { alert_id, deliveries: deliveries.into_iter().map(DeliveryView::from).collect() }))
}

/// Handler for `POST /deliveries/{id}/replay`.
pub async fn replay_delivery(State(state): State<AppState>, ApiPath(id): ApiPath<String>) -> Result<Response, ApiError> {
    let webhook = state.webhook.clone();
    let delivery = blocking(move || webhook.requeue(&id)).await?;
    tracing::info!(delivery_id = %delivery.delivery_id, alert_id = %delivery.alert_id, "delivery replayed");
    Ok((StatusCode::ACCEPTED, Json(DeliveryView::from(delivery))).into_response())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// 64 hex digits: two UUID v4s, 122 random bits each.
fn generate_secret() -> String {
    format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple())
}

/// Run `sql` on LENS for its errors; the rows are discarded.
async fn dry_run_on_lens(state: &AppState, sql: &str) -> Result<(), ApiError> {
    state.lens.query(sql).await.map(drop).map_err(|err| ApiError::from_lens(&err, sql))
}

/// Advance a new alert to LENS's `MAX(slot)`. `Some(slot)` when seeded,
/// `None` when LENS has no slot yet or the engine already moved it.
async fn seed(state: &AppState, alert_id: &str) -> Result<Option<u64>, ApiError> {
    let response = state.lens.query(VISIBILITY_SQL).await.map_err(|err| ApiError::from_lens(&err, VISIBILITY_SQL))?;
    let Some(slot) = max_slot_of(&response).map_err(|err| ApiError::from_lens(&err, VISIBILITY_SQL))? else {
        return Ok(None);
    };
    let registry = state.registry.clone();
    let alert_id = alert_id.to_owned();
    blocking(move || {
        registry.db().transaction("seed alert progress", |tx| match Registry::advance_progress(tx, &alert_id, slot) {
            Ok(()) => Ok(Some(slot)),
            // The engine baselined it in between, at or past `slot`.
            Err(RegistryError::Conflict { .. }) => Ok(None),
            Err(err) => Err(err),
        })
    })
    .await
}

/// Delete an alert whose creation failed half-way.
async fn undo_create(state: &AppState, alert_id: &str) {
    let registry = state.registry.clone();
    let id = alert_id.to_owned();
    if let Err(err) = blocking(move || registry.delete_alert(&id)).await {
        tracing::error!(alert_id, error = %err.body.error, "cannot delete an alert whose creation failed; delete it by hand");
    }
}

/// A `limit` query value: plain digits, 1 to [`MAX_LIST_LIMIT`].
fn parse_limit(raw: &str) -> Result<usize, ApiError> {
    let invalid = || ApiError::invalid("limit", format!("{raw:?} is not a number from 1 to {MAX_LIST_LIMIT}"));
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    raw.parse().ok().filter(|limit| (1..=MAX_LIST_LIMIT).contains(limit)).ok_or_else(invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_secrets_are_valid_and_distinct() {
        let (a, b) = (generate_secret(), generate_secret());
        assert_eq!(a.len(), 64);
        assert!(a.bytes().all(|byte| byte.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b);
        let new = NewAlert {
            name: "a".into(),
            sql: "SELECT {{from_slot}}, {{to_slot}}".into(),
            webhook_url: "https://h".into(),
            secret: a,
            wait_for_final: false,
        };
        new.validate().unwrap();
    }

    #[test]
    fn limits() {
        assert_eq!(parse_limit("1").unwrap(), 1);
        assert_eq!(parse_limit("0050").unwrap(), 50);
        assert_eq!(parse_limit(&MAX_LIST_LIMIT.to_string()).unwrap(), MAX_LIST_LIMIT);
        for bad in ["", "0", "-1", "+1", "1.0", " 1", "1001", "99999999999999999999999"] {
            assert_eq!(parse_limit(bad).unwrap_err().status, StatusCode::BAD_REQUEST, "{bad:?}");
        }
    }

    #[test]
    fn request_debug_output_redacts_the_secret() {
        let create = CreateAlertRequest {
            name: "a".into(),
            sql: "s".into(),
            webhook_url: "u".into(),
            secret: Some("hunter2hunter2hunter2".into()),
            wait_for_final: false,
        };
        assert!(!format!("{create:?}").contains("hunter2"));
        let update = UpdateAlertRequest { secret: Some("hunter2hunter2hunter2".into()), ..UpdateAlertRequest::default() };
        assert!(!format!("{update:?}").contains("hunter2"));
    }

    #[test]
    fn a_delivery_view_has_no_payload() {
        let delivery = Delivery {
            delivery_id: "dlv_1".into(),
            alert_id: "a".into(),
            to_slot: 9,
            payload: r#"{"rows":["sensitive"]}"#.into(),
            status: DeliveryStatus::Failed,
            release_after_slot: None,
            attempts: 5,
            next_attempt_at: 1,
            last_attempt_at: Some(2),
            delivered_at: None,
            last_status_code: Some(500),
            last_error: Some("HTTP 500".into()),
            created_at: 0,
        };
        let json = serde_json::to_value(DeliveryView::from(delivery)).unwrap();
        assert_eq!(json["payload_bytes"], 22);
        assert!(json.get("payload").is_none());
        assert!(!json.to_string().contains("sensitive"));
    }
}
