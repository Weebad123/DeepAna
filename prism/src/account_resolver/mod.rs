//! The account resolver: fills in what the decoder could not know about a
//! token transfer from the instruction alone.
//!
//! [`AccountResolver`] sits after the decoder. It takes the
//! [`DeepAnaTokenTransfer`] rows of a batch and completes them from two
//! sources, cheapest and most specific first:
//!
//! ```text
//!   transfers ─▶ 1. the transaction's own metadata (TokenAccountIndex)
//!                   mint, decimals, source / destination owner. No request.
//!                   │
//!                   ▼ only transfers whose decimals are still unknown
//!                2. mints not cached, not recently missing (deduplicated)
//!                   │ one ChainAdapter::get_accounts call
//!                   ▼
//!              MintCache ◀── MintInfo per mint, or a miss
//!                   │
//!                   ▼
//!   transfers with mint, decimals and owners
//! ```
//!
//! | File               | Holds                                                     |
//! |--------------------|-----------------------------------------------------------|
//! | `mod.rs`           | [`AccountResolver`], [`ResolverMetrics`]                   |
//! | [`mint_cache`]     | [`MintCache`], [`MintInfo`], [`TokenProgram`]              |
//! | [`token_metadata`] | [`TokenAccountIndex`], [`TokenAccountMeta`]                |
//!
//! Where each column comes from, in order of precedence:
//!
//! | Column              | Source                                                                  |
//! |---------------------|-------------------------------------------------------------------------|
//! | `mint_token_address`| the instruction (checked variants); else the metadata of the source account (and of the destination, which must agree) |
//! | `decimals`          | the metadata of whichever side holds that mint; else the mint account over RPC; else the instruction's |
//! | `source_owner`      | the metadata of the source account                                       |
//! | `destination_owner` | the metadata of the destination account                                  |
//!
//! Metadata is only used when it fits the transfer: an account whose metadata
//! names another token program than the one that ran the transfer is
//! ignored, and when the metadata's mint contradicts the mint the instruction
//! names, the metadata's mint and decimals are not used (owners are facts
//! about the accounts and still are). A transfer the metadata does not cover
//! (no token balances in the meta, e.g. very old blocks) is resolved over RPC
//! exactly as before: a `TransferChecked` comes out *fully enriched* (mint
//! and decimals known), a plain `Transfer` stays *partially enriched*.
//!
//! The chain is the authority on decimals: decimals a `TransferChecked`
//! carries are replaced by the chain's (metadata or mint account) when they
//! differ. The cache is filled from mint accounts only, never from
//! instruction data or metadata.
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::adapter::ChainAdapter;
use crate::types::error::{PrismError, PrismResult};
use crate::types::udm::DeepAnaTokenTransfer;

pub mod mint_cache;
pub mod token_metadata;

pub use mint_cache::{DEFAULT_MISS_TTL, MintCache, MintInfo, TokenProgram};
pub use token_metadata::{TokenAccountIndex, TokenAccountMeta};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Counters an [`AccountResolver`] keeps. Every processed transfer lands in
/// exactly one of `fully_enriched` and `partially_enriched`, and every mint
/// fetch attempted in exactly one of `mint_fetches_succeeded` and
/// `mint_fetches_failed`.
#[derive(Debug, Default)]
pub struct ResolverMetrics {
    total_transfers_processed: AtomicU64,
    fully_enriched: AtomicU64,
    partially_enriched: AtomicU64,
    mint_fetches_attempted: AtomicU64,
    mint_fetches_succeeded: AtomicU64,
    mint_fetches_failed: AtomicU64,
    cache_hits: AtomicU64,
    filled_from_metadata: AtomicU64,
}

/// A point-in-time copy of [`ResolverMetrics`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResolverMetricsSnapshot {
    pub total_transfers_processed: u64,
    /// Transfers that came out with both mint and decimals.
    pub fully_enriched: u64,
    /// Transfers that came out missing the mint or the decimals.
    pub partially_enriched: u64,
    /// Distinct mint addresses requested from the RPC.
    pub mint_fetches_attempted: u64,
    /// Requested addresses that were mints.
    pub mint_fetches_succeeded: u64,
    /// Requested addresses that were not mints, or whose request failed.
    pub mint_fetches_failed: u64,
    /// Transfers whose mint was already cached when the batch came in.
    pub cache_hits: u64,
    /// Transfers that got at least one column (mint, decimals, an owner)
    /// from their transaction's own metadata.
    pub filled_from_metadata: u64,
}

impl ResolverMetrics {
    pub fn snapshot(&self) -> ResolverMetricsSnapshot {
        ResolverMetricsSnapshot {
            total_transfers_processed: self.total_transfers_processed.load(Ordering::Relaxed),
            fully_enriched: self.fully_enriched.load(Ordering::Relaxed),
            partially_enriched: self.partially_enriched.load(Ordering::Relaxed),
            mint_fetches_attempted: self.mint_fetches_attempted.load(Ordering::Relaxed),
            mint_fetches_succeeded: self.mint_fetches_succeeded.load(Ordering::Relaxed),
            mint_fetches_failed: self.mint_fetches_failed.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            filled_from_metadata: self.filled_from_metadata.load(Ordering::Relaxed),
        }
    }

    fn add(counter: &AtomicU64, n: usize) {
        counter.fetch_add(n as u64, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

/// Enriches token transfers with the metadata of their mint.
pub struct AccountResolver {
    adapter: Arc<dyn ChainAdapter>,
    mint_cache: MintCache,
    metrics: Arc<ResolverMetrics>,
}

impl std::fmt::Debug for AccountResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountResolver")
            .field("chain_id", &self.adapter.chain_id())
            .field("mint_cache", &self.mint_cache)
            .field("metrics", &self.metrics.snapshot())
            .finish()
    }
}

impl AccountResolver {
    /// A resolver reading accounts through `adapter`, with an empty cache
    /// that remembers misses for [`DEFAULT_MISS_TTL`].
    pub fn new(adapter: Arc<dyn ChainAdapter>) -> Self {
        Self::with_miss_ttl(adapter, DEFAULT_MISS_TTL)
    }

    /// A resolver whose cache remembers misses for `miss_ttl`.
    pub fn with_miss_ttl(adapter: Arc<dyn ChainAdapter>, miss_ttl: Duration) -> Self {
        Self {
            adapter,
            mint_cache: MintCache::new(miss_ttl),
            metrics: Arc::default(),
        }
    }

    pub fn mint_cache(&self) -> &MintCache {
        &self.mint_cache
    }

    /// The cache, e.g. to preload mints known in advance.
    pub fn mint_cache_mut(&mut self) -> &mut MintCache {
        &mut self.mint_cache
    }

    /// The resolver's counters. Clone the `Arc` to read them from elsewhere.
    pub fn metrics(&self) -> &Arc<ResolverMetrics> {
        &self.metrics
    }

    /// `transfers`, in the same order, enriched with the metadata of their
    /// mints. The mints not yet known are fetched in a single
    /// [`ChainAdapter::get_accounts`] call, each address once.
    ///
    /// Same as [`Self::resolve_with_metadata`] with no metadata.
    pub async fn resolve(
        &mut self,
        transfers: Vec<DeepAnaTokenTransfer>,
    ) -> PrismResult<Vec<DeepAnaTokenTransfer>> {
        self.resolve_with_metadata(transfers, &TokenAccountIndex::new()).await
    }

    /// `transfers`, in the same order, completed first from `metadata` (their
    /// transactions' own token balances) and then, for those whose decimals
    /// are still unknown, from their mint accounts. See the
    /// [module docs](self) for which column comes from where.
    ///
    /// An RPC failure does not fail the batch: the adapter has already
    /// retried, so the affected transfers are returned as they are and
    /// their mints asked for again on the next call.
    ///
    /// Fails with [`PrismError::AccountResolveError`] only when the adapter
    /// breaks its contract by answering with a different number of
    /// accounts than it was asked for.
    pub async fn resolve_with_metadata(
        &mut self,
        mut transfers: Vec<DeepAnaTokenTransfer>,
        metadata: &TokenAccountIndex,
    ) -> PrismResult<Vec<DeepAnaTokenTransfer>> {
        let metrics = Arc::clone(&self.metrics);
        ResolverMetrics::add(&metrics.total_transfers_processed, transfers.len());

        // Step 1: the transactions' own metadata. A transfer whose decimals
        // it settled needs nothing from the RPC.
        let mut settled = Vec::with_capacity(transfers.len());
        let mut filled = 0;
        for transfer in &mut transfers {
            let outcome = apply_metadata(transfer, metadata);
            filled += usize::from(outcome.filled);
            settled.push(outcome.decimals_settled);
        }
        ResolverMetrics::add(&metrics.filled_from_metadata, filled);

        // Step 2: mints to fetch, deduplicated, in first-seen order; cache
        // hits counted per transfer before anything is fetched.
        let mut unknown: Vec<String> = Vec::new();
        let mut queued: HashSet<&str> = HashSet::new();
        let pending = transfers
            .iter()
            .zip(&settled)
            .filter(|(_, settled)| !**settled)
            .filter_map(|(t, _)| t.mint_token_address.as_deref());
        for mint in pending {
            if self.mint_cache.get(mint).is_some() {
                ResolverMetrics::add(&metrics.cache_hits, 1);
            } else if self.mint_cache.needs_fetch(mint) && queued.insert(mint) {
                unknown.push(mint.to_owned());
            }
        }

        if !unknown.is_empty() {
            self.fetch_mints(&unknown).await?;
        }

        let (mut full, mut partial) = (0, 0);
        for (transfer, settled) in transfers.iter_mut().zip(settled) {
            if !settled {
                self.enrich(transfer);
            }
            if transfer.mint_token_address.is_some() && transfer.decimals.is_some() {
                full += 1;
            } else {
                partial += 1;
            }
        }
        ResolverMetrics::add(&metrics.fully_enriched, full);
        ResolverMetrics::add(&metrics.partially_enriched, partial);
        Ok(transfers)
    }

    /// Fetches `mints` in one call and records each as a mint or a miss.
    async fn fetch_mints(&mut self, mints: &[String]) -> PrismResult<()> {
        let metrics = &self.metrics;
        self.mint_cache.record_fetches(mints.len());
        ResolverMetrics::add(&metrics.mint_fetches_attempted, mints.len());

        let accounts = match self.adapter.get_accounts(mints).await {
            Ok(accounts) => accounts,
            Err(err) => {
                ResolverMetrics::add(&metrics.mint_fetches_failed, mints.len());
                tracing::warn!(mints = mints.len(), "mint fetch failed, transfers left as they are: {err}");
                return Ok(());
            }
        };
        if accounts.len() != mints.len() {
            ResolverMetrics::add(&metrics.mint_fetches_failed, mints.len());
            return Err(PrismError::account_resolve(
                mints[0].clone(),
                format!(
                    "asked the adapter for {} accounts, got {}",
                    mints.len(),
                    accounts.len()
                ),
            ));
        }

        for (mint, account) in mints.iter().zip(accounts) {
            match account.as_ref().and_then(MintInfo::from_account) {
                Some(info) => {
                    self.mint_cache.insert(mint.clone(), info);
                    ResolverMetrics::add(&metrics.mint_fetches_succeeded, 1);
                }
                None => {
                    tracing::debug!(%mint, exists = account.is_some(), "not a mint");
                    self.mint_cache.mark_missing(mint.clone());
                    ResolverMetrics::add(&metrics.mint_fetches_failed, 1);
                }
            }
        }
        Ok(())
    }

    /// Fills `transfer` from the cached metadata of its mint. A mint owned
    /// by a different token program than the one that executed the
    /// transfer cannot be the transfer's mint, and is not used.
    fn enrich(&self, transfer: &mut DeepAnaTokenTransfer) {
        let Some(info) = transfer
            .mint_token_address
            .as_deref()
            .and_then(|mint| self.mint_cache.peek(mint))
        else {
            return;
        };
        if info.token_program.program_id() != transfer.spl_token_program_id {
            tracing::debug!(
                tx = %transfer.tx_signature,
                mint = ?transfer.mint_token_address,
                "mint belongs to another token program than the transfer"
            );
            return;
        }
        if transfer.decimals.is_some_and(|d| d != info.decimals) {
            tracing::debug!(
                tx = %transfer.tx_signature,
                instruction = ?transfer.decimals,
                mint = info.decimals,
                "instruction decimals differ from the mint's; using the mint's"
            );
        }
        transfer.decimals = Some(info.decimals);
    }
}

// ---------------------------------------------------------------------------
// Transaction metadata
// ---------------------------------------------------------------------------

/// What [`apply_metadata`] did to one transfer.
#[derive(Debug, Clone, Copy, Default)]
struct MetadataOutcome {
    /// At least one column was filled.
    filled: bool,
    /// The decimals now come from the chain (the metadata), so the mint
    /// account need not be read.
    decimals_settled: bool,
}

/// Step 1 of [`AccountResolver::resolve_with_metadata`]: fill `transfer`
/// from its transaction's metadata, as the module docs lay out. Never
/// overwrites a mint or owner that is already set.
fn apply_metadata(transfer: &mut DeepAnaTokenTransfer, metadata: &TokenAccountIndex) -> MetadataOutcome {
    let program = transfer.spl_token_program_id.as_str();
    let lookup = |account: &str| {
        metadata
            .get(&transfer.tx_signature, account)
            .filter(|meta| meta.belongs_to(program))
            .cloned()
    };
    let source = lookup(&transfer.source_token_account);
    let destination = lookup(&transfer.destination_token_account);
    let mut outcome = MetadataOutcome::default();
    if source.is_none() && destination.is_none() {
        return outcome;
    }

    let mut fill = |column: &mut Option<String>, value: Option<&String>| {
        if column.is_none()
            && let Some(value) = value
        {
            *column = Some(value.clone());
            outcome.filled = true;
        }
    };
    fill(&mut transfer.source_owner, source.as_ref().and_then(|m| m.owner.as_ref()));
    fill(&mut transfer.destination_owner, destination.as_ref().and_then(|m| m.owner.as_ref()));

    // The mint both sides agree on; one side alone is enough.
    let source_mint = source.as_ref().and_then(|m| m.mint.as_ref());
    let destination_mint = destination.as_ref().and_then(|m| m.mint.as_ref());
    let metadata_mint = match (source_mint, destination_mint) {
        (Some(a), Some(b)) if a != b => None,
        (a, b) => a.or(b),
    };
    match (&transfer.mint_token_address, metadata_mint) {
        (Some(named), Some(found)) if named != found => {
            tracing::debug!(
                tx = %transfer.tx_signature,
                instruction = %named,
                metadata = %found,
                "the transaction's metadata names another mint; its mint and decimals are not used"
            );
            return outcome;
        }
        (None, found) => fill(&mut transfer.mint_token_address, found),
        _ => {}
    }

    let Some(mint) = transfer.mint_token_address.as_deref() else {
        return outcome;
    };
    let decimals = [&source, &destination]
        .into_iter()
        .flatten()
        .filter(|meta| meta.mint.as_deref() == Some(mint))
        .find_map(|meta| meta.decimals);
    if let Some(decimals) = decimals {
        if transfer.decimals.is_some_and(|d| d != decimals) {
            tracing::debug!(
                tx = %transfer.tx_signature,
                instruction = ?transfer.decimals,
                metadata = decimals,
                "instruction decimals differ from the chain's; using the chain's"
            );
        }
        if transfer.decimals != Some(decimals) {
            transfer.decimals = Some(decimals);
            outcome.filled = true;
        }
        outcome.decimals_settled = true;
    }
    outcome
}
