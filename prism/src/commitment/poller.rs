//! [`FinalityPoller`]: the background task that keeps the
//! [`FinalityWatermark`] in step with the chain.
//!
//! Every `poll_interval` it:
//!
//! 1. fetches the confirmed tip,
//! 2. fetches the finalized tip,
//! 3. offers the confirmed tip to the watermark,
//! 4. offers the finalized tip to the watermark,
//! 5. emits a [`ChangeEvent::Final`] if the finalized watermark has moved
//!    past what was last announced,
//! 6. logs the watermark and waits for the next tick.
//!
//! The poller favours accuracy over liveness. A tip that failed to fetch, or
//! came back at a commitment other than the one asked for, is dropped rather
//! than guessed at. A tip below the watermark is rejected by the watermark
//! itself. A `Final` event that fails to emit is not lost: announcing is
//! driven by the gap between the watermark and the last announced slot, so
//! the next poll emits one event covering the whole gap.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::finality_watermark::{Advance, FinalityWatermark};
use crate::adapter::{ChainAdapter, ChainTip, CommitmentLevel};
use crate::changelog::ChangeLog;
use crate::types::error::{PrismError, PrismResult};
use crate::types::event::ChangeEvent;

/// How often the chain is polled when no interval is given.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Polls the chain's confirmed and finalized tips into a [`FinalityWatermark`].
pub struct FinalityPoller {
    adapter: Arc<dyn ChainAdapter>,
    watermark: Arc<FinalityWatermark>,
    changelog: Arc<dyn ChangeLog>,
    poll_interval: Duration,
    shutdown: CancellationToken,
    /// Finalized slot of the last `Final` event that was emitted.
    announced: u64,
}

impl FinalityPoller {
    /// Fails with [`PrismError::ConfigError`] when `poll_interval` is zero.
    ///
    /// Announcing starts from the watermark's current finalized slot: a
    /// watermark that already holds a value is assumed to have been announced.
    pub fn new(
        adapter: Arc<dyn ChainAdapter>,
        watermark: Arc<FinalityWatermark>,
        changelog: Arc<dyn ChangeLog>,
        poll_interval: Duration,
        shutdown: CancellationToken,
    ) -> PrismResult<Self> {
        if poll_interval.is_zero() {
            return Err(PrismError::config("poll_interval", "must be greater than 0"));
        }
        let announced = watermark.finalized_slot();
        Ok(Self {
            adapter,
            watermark,
            changelog,
            poll_interval,
            shutdown,
            announced,
        })
    }

    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Poll until `shutdown` is cancelled. The first poll runs immediately.
    ///
    /// Cancellation interrupts a fetch in progress (the adapter may be deep in
    /// its retry backoff) but not an emit, so a `Final` event is never cut off
    /// half written.
    pub async fn run(mut self) {
        info!(
            chain_id = self.adapter.chain_id(),
            poll_interval_ms = self.poll_interval.as_millis() as u64,
            "finality poller started"
        );
        let mut ticker = tokio::time::interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = self.shutdown.cancelled() => break,
                _ = ticker.tick() => {}
            }
            let tips = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => break,
                tips = self.fetch_tips() => tips,
            };
            self.apply(tips).await;
        }
        info!(
            chain_id = self.adapter.chain_id(),
            "finality poller stopped"
        );
    }

    /// One iteration of the loop, without the wait.
    pub async fn poll_once(&mut self) {
        let tips = self.fetch_tips().await;
        self.apply(tips).await;
    }

    /// Steps 1 and 2. Confirmed is read first, so the finalized read is
    /// never older than the confirmed one.
    async fn fetch_tips(&self) -> (Option<ChainTip>, Option<ChainTip>) {
        let confirmed = self.fetch_tip(CommitmentLevel::Confirmed).await;
        let finalized = self.fetch_tip(CommitmentLevel::Finalized).await;
        (confirmed, finalized)
    }

    async fn fetch_tip(&self, commitment: CommitmentLevel) -> Option<ChainTip> {
        match self.adapter.get_tip(commitment).await {
            Ok(tip) if tip.commitment == commitment => Some(tip),
            Ok(tip) => {
                error!(
                    requested = %commitment,
                    returned = %tip.commitment,
                    slot = tip.slot_or_height,
                    "adapter returned a tip at the wrong commitment, ignoring it"
                );
                None
            }
            Err(err) => {
                warn!(%commitment, error = %err, "failed to fetch chain tip");
                None
            }
        }
    }

    /// Steps 3 to 6.
    async fn apply(&mut self, (confirmed, finalized): (Option<ChainTip>, Option<ChainTip>)) {
        if let Some(tip) = confirmed {
            let outcome = self
                .watermark
                .advance_confirmed(tip.slot_or_height, &tip.block_id);
            log_outcome(&tip, &outcome);
        }
        if let Some(tip) = finalized {
            let outcome = self
                .watermark
                .advance_finalized(tip.slot_or_height, &tip.block_id);
            log_outcome(&tip, &outcome);
        }
        self.announce().await;
        debug!(
            confirmed_slot = self.watermark.confirmed_slot(),
            finalized_slot = self.watermark.finalized_slot(),
            lag = self.watermark.lag(),
            announced_slot = self.announced,
            "finality watermark"
        );
    }

    /// Emit a `Final` event for everything finalized since the last one.
    async fn announce(&mut self) {
        let (slot, blockhash) = self.watermark.finalized_tip();
        if slot <= self.announced {
            return;
        }
        let event = ChangeEvent::finalized(slot, blockhash, self.announced);
        match self.changelog.emit(event).await {
            Ok(()) => self.announced = slot,
            Err(err) => warn!(
                slot,
                previous_slot = self.announced,
                error = %err,
                "failed to emit final event, retrying next poll"
            ),
        }
    }
}

fn log_outcome(tip: &ChainTip, outcome: &Advance) {
    match outcome {
        Advance::Advanced { .. } | Advance::Unchanged => {}
        // Expected now and then behind a load balancer: one node is behind.
        Advance::Stale { current } => debug!(
            commitment = %tip.commitment,
            slot = tip.slot_or_height,
            watermark = current,
            "ignoring tip below the watermark"
        ),
        Advance::Conflict { recorded } => {
            if tip.commitment == CommitmentLevel::Finalized {
                error!(
                    slot = tip.slot_or_height,
                    blockhash = %tip.block_id,
                    recorded = %recorded,
                    "finalized blockhash changed for an already finalized slot"
                );
            } else {
                warn!(
                    commitment = %tip.commitment,
                    slot = tip.slot_or_height,
                    blockhash = %tip.block_id,
                    recorded = %recorded,
                    "blockhash changed for the recorded tip slot"
                );
            }
        }
    }
}
