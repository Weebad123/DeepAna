//! The FORGE demo: one alert that fires on every 50th slot, and the webhook
//! it delivers to, `POST /demo/hook`, served by DeepAna itself.
//!
//! ```text
//!   dashboard "Create demo alert" ─► POST /api/demo/alert
//!        └─ in-process POST /api/forge/alerts {name, sql, webhook_url, secret}
//!   FORGE dispatcher ─► POST http://<listen>/demo/hook (signed)
//!        └─ verified with the secret, summarised, kept (last 50) and
//!           pushed to every dashboard as a "demo_delivery" event
//! ```
//!
//! # The secret
//!
//! Generated per process and never sent to a browser: the create route
//! strips it from FORGE's answer. It is not written to disk either, so on
//! startup [`DemoHook::sync`] gives an existing demo alert this process's
//! secret and webhook URL (the port may have changed too).
//!
//! # What is kept
//!
//! A summary per receipt, not the payload: the delivery id and attempt, the
//! range, the row count and the first few slots. A request whose signature
//! does not verify is answered 401 and recorded with only its headers and
//! the reason, none of its body.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use forge::{HEADER_ATTEMPT, HEADER_DELIVERY_ID, HEADER_SIGNATURE, HEADER_TIMESTAMP};
use serde_json::{Value, json};

use crate::config::FORGE_PREFIX;
use crate::stream::{self, Reply};

/// The demo alert's name in FORGE.
pub const DEMO_ALERT_NAME: &str = "deepana-demo";
/// Where FORGE delivers it.
pub const DEMO_HOOK_PATH: &str = "/demo/hook";
/// Fires once for every slot divisible by 50 that PRISM wrote.
pub const DEMO_ALERT_SQL: &str = "SELECT slot, transaction_count, block_time FROM solana.slots \
     WHERE slot >= {{from_slot}} AND slot <= {{to_slot}} AND slot % 50 = 0 ORDER BY slot";
/// Slots of a delivery's rows a receipt keeps.
pub const RECEIPT_SLOTS: usize = 10;

/// The demo's secret and webhook URL.
#[derive(Clone)]
pub struct DemoHook {
    secret: Arc<str>,
    webhook_url: Arc<str>,
}

impl std::fmt::Debug for DemoHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemoHook").field("webhook_url", &self.webhook_url).field("secret", &"<redacted>").finish()
    }
}

impl DemoHook {
    /// A fresh secret (64 hex digits, two UUID v4s, as FORGE generates its
    /// own) and `http://<listen>/demo/hook`.
    pub fn new(listen: SocketAddr) -> Self {
        let secret = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
        Self { secret: Arc::from(secret), webhook_url: Arc::from(format!("http://{listen}{DEMO_HOOK_PATH}")) }
    }

    pub fn webhook_url(&self) -> &str {
        &self.webhook_url
    }

    /// Check one `POST /demo/hook` request at `now_secs`. Returns the status
    /// to answer and the receipt to record.
    pub fn receive(&self, headers: &HeaderMap, body: &[u8], now_secs: i64) -> (StatusCode, Value) {
        let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_owned);
        let mut receipt = json!({
            "delivery_id": header(HEADER_DELIVERY_ID),
            "attempt": header(HEADER_ATTEMPT).and_then(|a| a.parse::<u64>().ok()),
            "received_at": now_secs,
            "bytes": body.len(),
        });
        let checked = (|| {
            let timestamp = header(HEADER_TIMESTAMP).ok_or("no X-Forge-Timestamp header")?;
            let timestamp: i64 = timestamp.parse().map_err(|_| "X-Forge-Timestamp is not a number")?;
            let signature = header(HEADER_SIGNATURE).ok_or("no X-Forge-Signature header")?;
            forge::signer::verify(
                self.secret.as_bytes(),
                timestamp,
                body,
                &signature,
                now_secs,
                forge::signer::DEFAULT_TOLERANCE,
            )
            .map_err(|err| match err {
                forge::VerifyError::Mismatch => "signature does not match this process's demo secret",
                forge::VerifyError::Malformed => "signature is malformed",
                forge::VerifyError::OutsideTolerance { .. } => "timestamp is outside the 300 s tolerance",
            })?;
            serde_json::from_slice::<Value>(body).map_err(|_| "body is not JSON")
        })();
        match checked {
            Ok(payload) => {
                receipt["verified"] = json!(true);
                receipt["alert"] = payload.pointer("/alert/name").cloned().unwrap_or(Value::Null);
                for key in ["from_slot", "to_slot", "row_count", "truncated", "rows_omitted"] {
                    receipt[key] = payload.get(key).cloned().unwrap_or(Value::Null);
                }
                let slots: Vec<Value> = payload
                    .get("rows")
                    .and_then(Value::as_array)
                    .map(|rows| rows.iter().filter_map(|r| r.get("slot").cloned()).take(RECEIPT_SLOTS).collect())
                    .unwrap_or_default();
                receipt["slots"] = json!(slots);
                (StatusCode::OK, receipt)
            }
            Err(reason) => {
                receipt["verified"] = json!(false);
                receipt["error"] = json!(reason);
                (StatusCode::UNAUTHORIZED, receipt)
            }
        }
    }

    /// Create the demo alert through FORGE's router. The answer is FORGE's,
    /// without the secret. A demo alert that already exists is adopted
    /// ([`Self::sync`]) and returned with 200.
    pub async fn create(&self, api: &Router) -> (StatusCode, Value) {
        let body = json!({
            "name": DEMO_ALERT_NAME,
            "sql": DEMO_ALERT_SQL,
            "webhook_url": &*self.webhook_url,
            "secret": &*self.secret,
            "wait_for_final": false,
        });
        match stream::post(api, format!("{FORGE_PREFIX}/alerts"), body).await {
            // FORGE's answer is the alert with `secret` beside its fields.
            Reply::Ok(created) => (StatusCode::CREATED, json!({ "alert": without_secret(created) })),
            Reply::Status(409, _) => match self.sync(api).await {
                Ok(Some(alert)) => (StatusCode::OK, json!({ "alert": alert })),
                Ok(None) => (StatusCode::CONFLICT, json!({ "error": "the demo alert's name is taken, but no such alert is listed" })),
                Err(error) => (StatusCode::BAD_GATEWAY, json!({ "error": error })),
            },
            Reply::Status(code, body) => {
                let status = StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_GATEWAY);
                let error = body.get("error").and_then(Value::as_str).map_or_else(|| format!("FORGE answered HTTP {code}"), str::to_owned);
                (status, json!({ "error": error }))
            }
            Reply::Failed(reason) => (StatusCode::BAD_GATEWAY, json!({ "error": reason })),
        }
    }

    /// Give an existing demo alert this process's secret and webhook URL.
    /// `Ok(None)` when there is no demo alert.
    pub async fn sync(&self, api: &Router) -> Result<Option<Value>, String> {
        let listed = match stream::get(api, format!("{FORGE_PREFIX}/alerts")).await {
            Reply::Ok(body) => body,
            other => return Err(other.error().unwrap_or_default()),
        };
        let Some(id) = listed
            .get("alerts")
            .and_then(Value::as_array)
            .and_then(|alerts| alerts.iter().find(|a| a.get("name").and_then(Value::as_str) == Some(DEMO_ALERT_NAME)))
            .and_then(|a| a.get("id")?.as_str().map(str::to_owned))
        else {
            return Ok(None);
        };
        let update = json!({ "secret": &*self.secret, "webhook_url": &*self.webhook_url });
        match stream::patch(api, format!("{FORGE_PREFIX}/alerts/{id}"), update).await {
            // A PATCH never echoes the secret; strip it anyway.
            Reply::Ok(alert) => Ok(Some(without_secret(alert))),
            other => Err(other.error().unwrap_or_default()),
        }
    }
}

fn without_secret(mut alert: Value) -> Value {
    if let Some(object) = alert.as_object_mut() {
        object.remove("secret");
    }
    alert
}

/// Unix seconds now.
pub fn now_secs() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn signed(hook: &DemoHook, body: &str, at: i64) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_DELIVERY_ID, HeaderValue::from_static("d-1"));
        headers.insert(HEADER_ATTEMPT, HeaderValue::from_static("1"));
        headers.insert(HEADER_TIMESTAMP, HeaderValue::from_str(&at.to_string()).unwrap());
        let signature = forge::signer::sign(hook.secret.as_bytes(), at, body.as_bytes());
        headers.insert(HEADER_SIGNATURE, HeaderValue::from_str(&signature).unwrap());
        headers
    }

    #[test]
    fn a_signed_delivery_is_summarised() {
        let hook = DemoHook::new("127.0.0.1:8099".parse().unwrap());
        assert_eq!(hook.webhook_url(), "http://127.0.0.1:8099/demo/hook");
        let body = r#"{"version":1,"alert":{"id":"a","name":"deepana-demo"},"from_slot":100,"to_slot":151,"evaluated_at":5,"row_count":2,"truncated":false,"rows_omitted":0,"rows":[{"slot":100},{"slot":150}]}"#;
        let (status, receipt) = hook.receive(&signed(&hook, body, 1_000), body.as_bytes(), 1_010);
        assert_eq!(status, StatusCode::OK);
        assert_eq!(receipt["verified"], true);
        assert_eq!(receipt["delivery_id"], "d-1");
        assert_eq!(receipt["alert"], "deepana-demo");
        assert_eq!(receipt["slots"], json!([100, 150]));
        assert_eq!(receipt["to_slot"], 151);
    }

    #[test]
    fn a_bad_signature_is_refused_and_its_body_not_kept() {
        let hook = DemoHook::new("127.0.0.1:8099".parse().unwrap());
        let other = DemoHook::new("127.0.0.1:8099".parse().unwrap());
        let body = r#"{"alert":{"name":"secret-stuff"},"rows":[]}"#;
        let (status, receipt) = hook.receive(&signed(&other, body, 1_000), body.as_bytes(), 1_000);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(receipt["verified"], false);
        assert!(receipt.get("alert").is_none(), "{receipt}");
        let (status, _) = hook.receive(&HeaderMap::new(), body.as_bytes(), 1_000);
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn the_secret_is_not_in_debug_output() {
        let hook = DemoHook::new("127.0.0.1:8099".parse().unwrap());
        assert!(!format!("{hook:?}").contains(&*hook.secret));
    }
}
