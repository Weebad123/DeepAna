//! The dispatcher: claims due deliveries, sends them, records the outcome.
//!
//! One background task runs [`Dispatcher::run`]:
//!
//! 1. Wait for a wake signal ([`super::WebhookService::wake`]), a finished
//!    send, or the 1 s tick, whichever comes first. The tick is what picks
//!    up retries as they fall due; the wake makes a fresh delivery go out
//!    at once instead of on the next tick.
//! 2. Claim due rows ([`Outbox::begin_attempt`]) until `max_in_flight`
//!    sends are running or nothing is due, and start a send for each.
//!    **At most one delivery per alert is in flight**: the claim skips
//!    every alert in the in-memory busy set, so a slow receiver gets its
//!    deliveries one at a time and holds up no other alert.
//! 3. Each send signs with a fresh timestamp ([`signer::sign`]), posts with
//!    the configured timeout and redirects off, and reads at most
//!    [`MAX_RESPONSE_EXCERPT`] bytes of a non-2xx body, for the error
//!    message only. A 2xx is [`Outbox::record_success`]; anything else is
//!    [`Outbox::record_failure`] with the next attempt
//!    [`RetryPolicy::delay`] from now.
//!
//! On shutdown it stops claiming, gives in-flight sends `shutdown_grace` to
//! finish (enough for one request timeout), then abandons the rest and
//! returns. An abandoned or crashed send already counted its attempt; its
//! row is due again once its lease runs out.
//!
//! The outbox is synchronous SQLite, so every call to it runs on the
//! blocking pool.

use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Response, redirect};
use tokio::sync::{Notify, watch};
use tokio::task::{Id, JoinError, JoinSet};
use tracing::{debug, info, warn};

use super::outbox::{Attempt, Outbox, Recorded, clean_error};
use super::{Clock, HEADER_ATTEMPT, HEADER_DELIVERY_ID, HEADER_SIGNATURE, HEADER_TIMESTAMP, USER_AGENT_VALUE, signer};
use crate::config::{ForgeError, ForgeResult};

/// Most bytes of a failed response's body kept for its error message.
pub const MAX_RESPONSE_EXCERPT: usize = 2048;
/// Largest `jitter_pct` [`RetryPolicy::validate`] accepts.
pub const MAX_JITTER_PCT: u8 = 100;

// ---------------------------------------------------------------------------
// RetryPolicy
// ---------------------------------------------------------------------------

/// How long to wait after a failed attempt.
///
/// After attempt `n` fails, the next is due `delays[n - 1]` later; past the
/// end of `delays` the last entry repeats. Each delay is then moved by a
/// random amount of up to `jitter_pct` percent either way, so receivers that
/// fail together do not retry together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    pub delays: Vec<Duration>,
    pub jitter_pct: u8,
}

impl Default for RetryPolicy {
    /// 10 s, 1 min, 5 min, 15 min, then hourly, ±20 %. With the default
    /// five attempts a delivery is given up on about 21 minutes after its
    /// first attempt.
    fn default() -> Self {
        Self {
            delays: [10, 60, 300, 900, 3_600].map(Duration::from_secs).to_vec(),
            jitter_pct: 20,
        }
    }
}

impl RetryPolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.delays.is_empty() {
            return Err("delays must not be empty".into());
        }
        if self.delays.iter().any(Duration::is_zero) {
            return Err("every delay must be greater than zero".into());
        }
        if self.jitter_pct > MAX_JITTER_PCT {
            return Err(format!("jitter_pct must be at most {MAX_JITTER_PCT}, got {}", self.jitter_pct));
        }
        Ok(())
    }

    /// The delay after failed attempt `attempt` (1-based), before jitter.
    pub fn base_delay(&self, attempt: u32) -> Duration {
        let index = (attempt.max(1) - 1) as usize;
        self.delays.get(index).or(self.delays.last()).copied().unwrap_or_default()
    }

    /// [`Self::base_delay`] moved by `jitter_pct` percent times `unit`,
    /// where `unit` is in `-1.0..=1.0` (clamped).
    pub fn delay(&self, attempt: u32, unit: f64) -> Duration {
        let factor = 1.0 + f64::from(self.jitter_pct) / 100.0 * unit.clamp(-1.0, 1.0);
        Duration::try_from_secs_f64(self.base_delay(attempt).as_secs_f64() * factor).unwrap_or(Duration::MAX)
    }
}

/// Uniform-ish in `-1.0..1.0`. Jitter needs spread, not unpredictability;
/// each `RandomState` is freshly keyed, so this needs no RNG crate.
fn random_unit() -> f64 {
    let bits = std::collections::hash_map::RandomState::new().build_hasher().finish();
    (bits >> 11) as f64 / (1u64 << 53) as f64 * 2.0 - 1.0
}

// ---------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------

/// The HTTP client every send uses: `timeout` per request, no redirects, no
/// proxy, no content decoding.
pub(crate) fn build_client(timeout: Duration) -> ForgeResult<Client> {
    Client::builder()
        .timeout(timeout)
        .redirect(redirect::Policy::none())
        .no_proxy()
        .no_gzip()
        .no_brotli()
        .no_deflate()
        .build()
        .map_err(|err| ForgeError::webhook("dispatcher", err))
}

/// What one send needs; shared by every send task.
struct Courier {
    http: Client,
    outbox: Outbox,
    clock: Arc<dyn Clock>,
    retry: RetryPolicy,
}

pub(crate) struct Dispatcher {
    courier: Arc<Courier>,
    max_in_flight: usize,
    tick: Duration,
    shutdown_grace: Duration,
    wake: Arc<Notify>,
    shutdown: watch::Receiver<bool>,
}

/// In-flight send tasks, and the alert each one is for.
#[derive(Default)]
struct InFlight {
    tasks: JoinSet<()>,
    alerts: HashMap<Id, String>,
}

impl InFlight {
    fn finished(&mut self, done: Result<(Id, ()), JoinError>) {
        let id = match done {
            Ok((id, ())) => id,
            Err(err) => {
                if err.is_panic() {
                    warn!(error = %err, "webhook send task panicked");
                }
                err.id()
            }
        };
        self.alerts.remove(&id);
    }

    fn busy_alerts(&self) -> HashSet<String> {
        self.alerts.values().cloned().collect()
    }
}

impl Dispatcher {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        http: Client,
        outbox: Outbox,
        clock: Arc<dyn Clock>,
        retry: RetryPolicy,
        max_in_flight: usize,
        tick: Duration,
        shutdown_grace: Duration,
        wake: Arc<Notify>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            courier: Arc::new(Courier {
                http,
                outbox,
                clock,
                retry,
            }),
            max_in_flight,
            tick,
            shutdown_grace,
            wake,
            shutdown,
        }
    }

    /// The loop described in the [module docs](self). Returns after shutdown.
    pub(crate) async fn run(mut self) {
        let mut in_flight = InFlight::default();
        loop {
            while let Some(done) = in_flight.tasks.try_join_next_with_id() {
                in_flight.finished(done);
            }
            if *self.shutdown.borrow() {
                break;
            }
            self.claim(&mut in_flight).await;
            tokio::select! {
                () = self.wake.notified() => {}
                () = tokio::time::sleep(self.tick) => {}
                Some(done) = in_flight.tasks.join_next_with_id(), if !in_flight.tasks.is_empty() => {
                    in_flight.finished(done);
                }
                changed = self.shutdown.changed() => {
                    // A dropped sender means the service is gone.
                    if changed.is_err() {
                        break;
                    }
                }
            }
        }

        let running = in_flight.tasks.len();
        if running > 0 {
            info!(running, "webhook dispatcher stopping; waiting for in-flight sends");
        }
        let drain = async {
            while let Some(done) = in_flight.tasks.join_next_with_id().await {
                in_flight.finished(done);
            }
        };
        if tokio::time::timeout(self.shutdown_grace, drain).await.is_err() {
            warn!(
                abandoned = in_flight.tasks.len(),
                "abandoning in-flight webhook sends; they are retried once their lease runs out"
            );
            in_flight.tasks.shutdown().await;
        }
        info!("webhook dispatcher stopped");
    }

    /// Claim and start sends until the in-flight cap or nothing is due.
    async fn claim(&self, in_flight: &mut InFlight) {
        while in_flight.tasks.len() < self.max_in_flight {
            let busy = in_flight.busy_alerts();
            let outbox = self.courier.outbox.clone();
            let now = self.courier.clock.now_ms();
            match tokio::task::spawn_blocking(move || outbox.begin_attempt(now, &busy)).await {
                Ok(Ok(Some(attempt))) => {
                    let alert_id = attempt.alert_id.clone();
                    let task = in_flight.tasks.spawn(deliver(self.courier.clone(), attempt));
                    in_flight.alerts.insert(task.id(), alert_id);
                }
                Ok(Ok(None)) => return,
                Ok(Err(err)) => {
                    warn!(error = %err, "cannot claim a webhook delivery");
                    return;
                }
                Err(err) => {
                    warn!(error = %err, "webhook claim task failed");
                    return;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sending
// ---------------------------------------------------------------------------

/// How one send ended, before it is recorded.
#[derive(Debug, PartialEq, Eq)]
enum Sent {
    Delivered(u16),
    /// `error` is already cleaned (see [`clean_error`]).
    Failed { status_code: Option<u16>, error: String },
}

/// Send `attempt` and record the outcome.
async fn deliver(courier: Arc<Courier>, attempt: Attempt) {
    let sent = courier.send(&attempt).await;
    let now = courier.clock.now_ms();
    let outbox = courier.outbox.clone();
    let (delivery_id, number) = (attempt.delivery_id.clone(), attempt.attempt);
    let recorded = match &sent {
        Sent::Delivered(code) => {
            let code = *code;
            tokio::task::spawn_blocking(move || outbox.record_success(&delivery_id, number, code, now)).await
        }
        Sent::Failed { status_code, error } => {
            let delay = courier.retry.delay(number, random_unit());
            let retry_at = now.saturating_add(i64::try_from(delay.as_millis()).unwrap_or(i64::MAX));
            let (status_code, error) = (*status_code, error.clone());
            tokio::task::spawn_blocking(move || outbox.record_failure(&delivery_id, number, status_code, &error, retry_at))
                .await
        }
    };

    let id = attempt.delivery_id.as_str();
    match (recorded, &sent) {
        (Ok(Ok(Recorded::Delivered)), Sent::Delivered(code)) => {
            info!(delivery_id = id, alert_id = %attempt.alert_id, attempt = number, status = code, "webhook delivered");
        }
        (Ok(Ok(Recorded::Retrying { next_attempt_at })), Sent::Failed { error, .. }) => {
            warn!(delivery_id = id, alert_id = %attempt.alert_id, attempt = number, next_attempt_at, %error, "webhook attempt failed; will retry");
        }
        (Ok(Ok(Recorded::Failed)), Sent::Failed { error, .. }) => {
            warn!(delivery_id = id, alert_id = %attempt.alert_id, attempt = number, %error, "webhook delivery failed; attempts exhausted");
        }
        (Ok(Ok(outcome)), _) => {
            debug!(delivery_id = id, attempt = number, ?outcome, "webhook outcome not recorded; the row has moved on");
        }
        (Ok(Err(err)), _) => {
            warn!(error = %ForgeError::webhook(id, err), "cannot record the webhook outcome; the attempt is retried after its lease");
        }
        (Err(err), _) => warn!(delivery_id = id, error = %err, "webhook record task failed"),
    }
}

impl Courier {
    async fn send(&self, attempt: &Attempt) -> Sent {
        let clean = |text: &str| clean_error(text, &attempt.webhook_url, &attempt.secret);
        // A fresh timestamp per attempt, so every attempt passes a
        // receiver's replay window however late it is.
        let timestamp = self.clock.now_ms().div_euclid(1000);
        let signature = signer::sign(attempt.secret.as_bytes(), timestamp, attempt.payload.as_bytes());
        let request = self
            .http
            .post(&attempt.webhook_url)
            .header(CONTENT_TYPE, "application/json")
            .header(USER_AGENT, USER_AGENT_VALUE)
            .header(HEADER_DELIVERY_ID, &attempt.delivery_id)
            .header(HEADER_ATTEMPT, attempt.attempt.to_string())
            .header(HEADER_TIMESTAMP, timestamp.to_string())
            .header(HEADER_SIGNATURE, signature)
            .body(attempt.payload.clone());

        let response = match request.send().await {
            Ok(response) => response,
            Err(err) => {
                return Sent::Failed {
                    status_code: None,
                    error: clean(&describe_transport(err)),
                };
            }
        };
        let status = response.status();
        if status.is_success() {
            return Sent::Delivered(status.as_u16());
        }
        let mut error = format!("HTTP {status}");
        if status.is_redirection() {
            error.push_str(" (redirects are not followed)");
        }
        let excerpt = read_excerpt(response).await;
        if !excerpt.is_empty() {
            error.push_str(": ");
            error.push_str(&excerpt);
        }
        Sent::Failed {
            status_code: Some(status.as_u16()),
            error: clean(&error),
        }
    }
}

/// Up to [`MAX_RESPONSE_EXCERPT`] bytes of the body, lossily decoded and
/// trimmed. A body that fails part-way yields what arrived.
async fn read_excerpt(mut response: Response) -> String {
    let mut body = Vec::new();
    while body.len() < MAX_RESPONSE_EXCERPT {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) | Err(_) => break,
        }
    }
    body.truncate(MAX_RESPONSE_EXCERPT);
    String::from_utf8_lossy(&body).trim().to_owned()
}

/// A send that got no response, as one line without the URL.
fn describe_transport(err: reqwest::Error) -> String {
    let what = if err.is_timeout() {
        "timed out"
    } else if err.is_connect() {
        "cannot connect"
    } else if err.is_builder() {
        "cannot build the request"
    } else {
        "request failed"
    };
    format!("{what}: {}", error_chain(&err.without_url()))
}

/// `err` and its causes, `": "`-separated, skipping a cause whose text the
/// line already holds.
fn error_chain(err: &(dyn StdError + 'static)) -> String {
    let mut line = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !line.contains(&text) {
            line.push_str(": ");
            line.push_str(&text);
        }
        source = cause.source();
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn default_policy_is_valid() {
        RetryPolicy::default().validate().unwrap();
    }

    #[test]
    fn validation() {
        let ok = RetryPolicy { delays: vec![secs(1)], jitter_pct: 100 };
        ok.validate().unwrap();
        assert!(RetryPolicy { delays: vec![], jitter_pct: 0 }.validate().is_err());
        assert!(RetryPolicy { delays: vec![secs(1), Duration::ZERO], jitter_pct: 0 }.validate().is_err());
        assert!(RetryPolicy { delays: vec![secs(1)], jitter_pct: 101 }.validate().is_err());
    }

    #[test]
    fn delays_follow_the_list_then_repeat_the_last() {
        let policy = RetryPolicy { delays: vec![secs(1), secs(5), secs(30)], jitter_pct: 0 };
        let got: Vec<_> = (1..=5).map(|n| policy.delay(n, random_unit())).collect();
        assert_eq!(got, [secs(1), secs(5), secs(30), secs(30), secs(30)]);
        // Attempt 0 does not exist; treated as the first.
        assert_eq!(policy.base_delay(0), secs(1));
        assert_eq!(policy.base_delay(u32::MAX), secs(30));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let policy = RetryPolicy { delays: vec![secs(100)], jitter_pct: 20 };
        assert_eq!(policy.delay(1, -1.0), secs(80));
        assert_eq!(policy.delay(1, 0.0), secs(100));
        assert_eq!(policy.delay(1, 1.0), secs(120));
        assert_eq!(policy.delay(1, 7.0), secs(120), "clamped");
        for _ in 0..1_000 {
            let d = policy.delay(1, random_unit());
            assert!((secs(80)..=secs(120)).contains(&d), "{d:?}");
        }
        let full = RetryPolicy { delays: vec![secs(1)], jitter_pct: 100 };
        assert_eq!(full.delay(1, -1.0), Duration::ZERO);
        let huge = RetryPolicy { delays: vec![Duration::MAX], jitter_pct: 100 };
        assert_eq!(huge.delay(1, 1.0), Duration::MAX, "saturates");
    }

    #[test]
    fn random_unit_spreads() {
        let samples: Vec<f64> = (0..1_000).map(|_| random_unit()).collect();
        assert!(samples.iter().all(|u| (-1.0..1.0).contains(u)));
        assert!(samples.iter().any(|u| *u < -0.5) && samples.iter().any(|u| *u > 0.5), "{samples:?}");
    }
}
