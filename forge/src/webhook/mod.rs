//! Webhooks: a durable outbox and the dispatcher that drains it.
//!
//! | File              | Holds                                                         |
//! |-------------------|---------------------------------------------------------------|
//! | `mod.rs`          | [`WebhookService`]: enqueue, release, wake, shutdown          |
//! | [`signer`]        | HMAC-SHA256 signing and verification                          |
//! | [`outbox`]        | [`Outbox`]: every read and write on the `deliveries` table     |
//! | [`dispatcher`]    | claims due deliveries, sends them, schedules retries          |
//!
//! # Delivery contract
//!
//! At least once, unordered. The body is the stored payload, byte for byte.
//!
//! ```text
//! POST <webhook_url>
//! Content-Type: application/json
//! User-Agent: helios-forge/<version>
//! X-Forge-Delivery-Id: dlv_<32 hex>      stable across retries; dedupe on it
//! X-Forge-Attempt: 3                     1-based
//! X-Forge-Timestamp: 1730000000          unix seconds, fresh per attempt
//! X-Forge-Signature: v1=<64 hex>         HMAC-SHA256(secret, "<timestamp>.<body>")
//! ```
//!
//! Any 2xx is success. Anything else, a timeout, or a dropped connection
//! is retried on the [`RetryPolicy`] until `webhook_max_attempts` attempts
//! have been made; redirects are not followed. A receiver checks a request
//! with [`signer::verify`].
//!
//! # Calling pattern
//!
//! The evaluator enqueues in the transaction that advances the alert, and
//! wakes the dispatcher only once that transaction has committed:
//!
//! ```ignore
//! let delivery = NewDelivery {
//!     alert_id: alert.id.clone(),
//!     to_slot,
//!     payload,
//!     release_after_slot: alert.wait_for_final.then_some(to_slot),
//! };
//! db.transaction("fire alert", |tx| {
//!     Registry::advance_progress(tx, &alert.id, to_slot)?;
//!     webhook.enqueue(tx, &delivery)?;
//!     Ok(())
//! })?;
//! webhook.wake();
//! ```
//!
//! Waking before the commit would let the dispatcher look for the row before
//! it exists. Forgetting to wake only delays the send to the next tick.
//!
//! For a `wait_for_final` alert the row is `held` until the change log's
//! `Final` event covers its `to_slot`; on every such event the engine calls
//! [`WebhookService::release_final`]. The hold lives in the database, so a
//! restart neither loses it nor sends early.
//!
//! # Configuration
//!
//! No settings of its own beyond [`ForgeConfig`]'s: `webhook_max_attempts`,
//! `webhook_timeout_secs` (per request), and `visibility_timeout_secs` (the
//! claim lease). The rest are the constants below, overridable through
//! [`WebhookService::start_with`] for tests and embedders.

pub mod dispatcher;
pub mod outbox;
pub mod signer;

pub use dispatcher::RetryPolicy;
pub use outbox::{Attempt, Delivery, DeliveryStatus, Enqueued, NewDelivery, Outbox, OutboxDepth, Recorded};
pub use signer::VerifyError;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::Transaction;
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;

use crate::config::{ForgeConfig, ForgeError, ForgeResult};
use crate::registry::{Db, RegistryError};
use dispatcher::Dispatcher;

pub const HEADER_DELIVERY_ID: &str = "X-Forge-Delivery-Id";
pub const HEADER_ATTEMPT: &str = "X-Forge-Attempt";
pub const HEADER_TIMESTAMP: &str = "X-Forge-Timestamp";
pub const HEADER_SIGNATURE: &str = "X-Forge-Signature";
pub const USER_AGENT_VALUE: &str = concat!("helios-forge/", env!("CARGO_PKG_VERSION"));

/// Sends running at once, across all alerts.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 8;
/// How often the dispatcher looks for due rows without being woken.
pub const DISPATCH_TICK: Duration = Duration::from_secs(1);
/// Shutdown waits one request timeout plus this for in-flight sends.
pub const SHUTDOWN_GRACE_MARGIN: Duration = Duration::from_secs(2);

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// Where the webhook service reads the time. Unix milliseconds.
pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> i64;
}

/// The system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
    }
}

// ---------------------------------------------------------------------------
// WebhookSettings
// ---------------------------------------------------------------------------

/// Everything the service runs on. [`WebhookSettings::from_config`] is what
/// FORGE uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookSettings {
    /// Attempts per delivery, the first included.
    pub max_attempts: u32,
    /// How long a claimed row stays invisible to the next claim.
    pub lease: Duration,
    /// Per request, connect to last byte.
    pub request_timeout: Duration,
    pub retry: RetryPolicy,
    pub max_in_flight: usize,
    pub tick: Duration,
    /// How long shutdown waits for in-flight sends.
    pub shutdown_grace: Duration,
}

impl WebhookSettings {
    pub fn from_config(config: &ForgeConfig) -> Self {
        Self {
            max_attempts: config.webhook_max_attempts(),
            lease: config.visibility_timeout(),
            request_timeout: config.webhook_timeout(),
            retry: RetryPolicy::default(),
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            tick: DISPATCH_TICK,
            shutdown_grace: config.webhook_timeout() + SHUTDOWN_GRACE_MARGIN,
        }
    }

    pub fn validate(&self) -> ForgeResult<()> {
        let invalid = |field: &str, reason: String| Err(ForgeError::config(field, reason));
        if self.max_attempts == 0 {
            return invalid("webhook_max_attempts", "must be at least 1".into());
        }
        if self.request_timeout.is_zero() {
            return invalid("webhook_timeout_secs", "must be greater than zero".into());
        }
        // As in config.rs: a lease that can run out mid-request would let
        // the row be claimed while its send is still in flight.
        if self.lease <= self.request_timeout {
            return invalid(
                "visibility_timeout_secs",
                format!("lease {:?} must be longer than the request timeout {:?}", self.lease, self.request_timeout),
            );
        }
        if let Err(reason) = self.retry.validate() {
            return invalid("webhook_retry", reason);
        }
        if self.max_in_flight == 0 {
            return invalid("webhook_max_in_flight", "must be at least 1".into());
        }
        if self.tick.is_zero() {
            return invalid("webhook_tick", "must be greater than zero".into());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WebhookService
// ---------------------------------------------------------------------------

/// The outbox and its running dispatcher.
pub struct WebhookService {
    outbox: Outbox,
    clock: Arc<dyn Clock>,
    wake: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl WebhookService {
    /// Start the dispatcher on `db` with [`WebhookSettings::from_config`]
    /// and the system clock. Must be called inside a Tokio runtime.
    pub fn start(config: &ForgeConfig, db: Db) -> ForgeResult<Self> {
        Self::start_with(db, WebhookSettings::from_config(config), Arc::new(SystemClock))
    }

    /// [`Self::start`] with explicit settings and clock.
    pub fn start_with(db: Db, settings: WebhookSettings, clock: Arc<dyn Clock>) -> ForgeResult<Self> {
        settings.validate()?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|err| ForgeError::webhook("dispatcher", format!("must be started inside a Tokio runtime: {err}")))?;
        let outbox = Outbox::new(db, settings.max_attempts, settings.lease)?;
        let http = dispatcher::build_client(settings.request_timeout)?;
        let wake = Arc::new(Notify::new());
        let (shutdown, shutdown_rx) = watch::channel(false);
        let dispatcher = Dispatcher::new(
            http,
            outbox.clone(),
            clock.clone(),
            settings.retry,
            settings.max_in_flight,
            settings.tick,
            settings.shutdown_grace,
            wake.clone(),
            shutdown_rx,
        );
        let task = runtime.spawn(dispatcher.run());
        Ok(Self {
            outbox,
            clock,
            wake,
            shutdown,
            task: Mutex::new(Some(task)),
        })
    }

    pub fn outbox(&self) -> &Outbox {
        &self.outbox
    }

    /// [`Outbox::enqueue`] at the service clock's time, in the caller's
    /// transaction. Call [`Self::wake`] after the commit.
    pub fn enqueue(&self, tx: &Transaction<'_>, new: &NewDelivery) -> Result<Enqueued, RegistryError> {
        Outbox::enqueue(tx, new, self.clock.now_ms())
    }

    /// Have the dispatcher look for due rows now. Cheap; a wake while it is
    /// busy is remembered for its next pass.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    /// Every slot at or below `final_slot` is finalized: release the rows
    /// held for them, then wake. Idempotent; returns how many moved.
    pub fn release_final(&self, final_slot: u64) -> ForgeResult<usize> {
        let released = self.outbox.release_up_to(final_slot, self.clock.now_ms())?;
        if released > 0 {
            self.wake();
        }
        Ok(released)
    }

    /// Retry a `failed` delivery from attempt 1, now.
    pub fn requeue(&self, delivery_id: &str) -> ForgeResult<Delivery> {
        let delivery = self.outbox.requeue(delivery_id, self.clock.now_ms())?;
        self.wake();
        Ok(delivery)
    }

    /// For `/health`.
    pub fn depth(&self) -> ForgeResult<OutboxDepth> {
        self.outbox.depth(self.clock.now_ms())
    }

    /// For the delivery history endpoint; see [`Outbox::list`].
    pub fn list(&self, alert_id: &str, limit: usize) -> ForgeResult<Vec<Delivery>> {
        self.outbox.list(alert_id, limit)
    }

    /// Stop claiming, let in-flight sends finish (or abandon them after the
    /// grace period), and return once the dispatcher has stopped. Calling it
    /// again returns at once.
    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        let task = self.task.lock().unwrap_or_else(PoisonError::into_inner).take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for WebhookService {
    /// Dropped without [`WebhookService::shutdown`]: the dispatcher still
    /// stops, in the background.
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
    }
}

impl std::fmt::Debug for WebhookService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookService").field("outbox", &self.outbox).finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> WebhookSettings {
        WebhookSettings {
            max_attempts: 3,
            lease: Duration::from_secs(20),
            request_timeout: Duration::from_secs(10),
            retry: RetryPolicy::default(),
            max_in_flight: 4,
            tick: DISPATCH_TICK,
            shutdown_grace: Duration::from_secs(12),
        }
    }

    #[test]
    fn settings_validation() {
        settings().validate().unwrap();
        let field = |s: WebhookSettings| s.validate().unwrap_err().config_field().unwrap().to_owned();
        assert_eq!(field(WebhookSettings { max_attempts: 0, ..settings() }), "webhook_max_attempts");
        assert_eq!(field(WebhookSettings { request_timeout: Duration::ZERO, ..settings() }), "webhook_timeout_secs");
        assert_eq!(field(WebhookSettings { lease: Duration::from_secs(10), ..settings() }), "visibility_timeout_secs");
        let retry = RetryPolicy { delays: vec![], jitter_pct: 0 };
        assert_eq!(field(WebhookSettings { retry, ..settings() }), "webhook_retry");
        assert_eq!(field(WebhookSettings { max_in_flight: 0, ..settings() }), "webhook_max_in_flight");
        assert_eq!(field(WebhookSettings { tick: Duration::ZERO, ..settings() }), "webhook_tick");
    }

    #[test]
    fn user_agent_names_the_version() {
        assert_eq!(USER_AGENT_VALUE, format!("helios-forge/{}", env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn start_outside_a_runtime_is_an_error() {
        let err = WebhookService::start_with(Db::open_in_memory(), settings(), Arc::new(SystemClock)).unwrap_err();
        assert_eq!(err.kind(), "webhook_error", "{err}");
    }

    #[test]
    fn system_clock_is_unix_ms() {
        let now = SystemClock.now_ms();
        assert!(now > 1_700_000_000_000 && now < 10_000_000_000_000, "{now}");
    }
}
