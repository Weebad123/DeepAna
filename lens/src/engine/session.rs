//! Session: the DataFusion [`SessionContext`] every LENS query runs in.
//!
//! [`create_session`] builds the one context the engine owns for the life of
//! the process. The context holds the catalog registrations, the planner and
//! optimizer configuration, and the runtime (memory pool, spill files, file
//! listing cache) that every query shares.
//!
//! | Setting                                         | Value                      | Why                                                       |
//! |-------------------------------------------------|----------------------------|-----------------------------------------------------------|
//! | `execution.target_partitions`                   | available CPUs             | one partition per core for scans, joins, aggregations     |
//! | `execution.collect_statistics`                  | `true`                     | row counts and min/max from Parquet footers feed the planner |
//! | `execution.parquet.pushdown_filters`            | `true`                     | filters run while decoding, so rows that fail are never materialized |
//! | `execution.parquet.reorder_filters`             | `true`                     | cheapest, most selective pushed-down filter runs first    |
//! | `execution.parquet.bloom_filter_on_read`        | `true`                     | skip row groups for `tx_signature = '...'` lookups        |
//! | `execution.parquet.enable_page_index`           | `true`                     | skip pages, not just row groups                           |
//! | `execution.parquet.pruning`                     | `true`                     | skip row groups by min/max (`slot BETWEEN ...`)           |
//! | `catalog.information_schema`                    | `true`                     | `SHOW TABLES`, `information_schema.columns` for discovery |
//!
//! The Parquet options are set on the session before any table is
//! registered: [`PrismTable::listing_options`] copies them from the context,
//! so every table reads its files with them.
//!
//! # Memory
//!
//! Without a memory pool DataFusion lets a query allocate without bound, so
//! one large `ORDER BY` or `GROUP BY` can take the whole process down. The
//! runtime gets a [`FairSpillPool`] of [`SessionSettings::memory_limit_bytes`]:
//!
//! * The limit is for the whole process, shared by every query running at
//!   the same time, not per query.
//! * Operators that can spill (sort, hash aggregation, sort-merge join) split
//!   the pool fairly and spill to temp files under the OS temp directory
//!   when they reach their share, up to
//!   [`SessionSettings::max_spill_bytes`] on disk.
//! * An operator that cannot spill and asks for more than is left fails its
//!   query with `ResourcesExhausted`. The other queries and the process keep
//!   going.
//!
//! The pool is wrapped in a [`TrackConsumersPool`], so that error names the
//! operators holding the most memory.
//!
//! The memory LENS uses to keep a finished result before it is serialized
//! is not counted against the pool. `max_query_rows` limits that instead.
//!
//! [`PrismTable::listing_options`]: crate::catalog::PrismTable::listing_options

use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool, TrackConsumersPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::config::LensConfig;
use crate::error::{LensError, LensResult};

/// Default memory pool size: 2 GiB, for all queries combined.
pub const DEFAULT_MEMORY_LIMIT_BYTES: usize = 2 * 1024 * 1024 * 1024;

/// Smallest memory pool [`create_session`] accepts: 16 MiB. Below this,
/// even a scan of one Parquet row group cannot get its buffers.
pub const MIN_MEMORY_LIMIT_BYTES: usize = 16 * 1024 * 1024;

/// Default limit on the spill files of all running queries combined: 20 GiB.
pub const DEFAULT_MAX_SPILL_BYTES: u64 = 20 * 1024 * 1024 * 1024;

/// How many of the largest memory consumers a `ResourcesExhausted` error
/// lists.
const TRACKED_CONSUMERS: NonZeroUsize = NonZeroUsize::new(5).unwrap();

/// Runtime settings that are not in [`LensConfig`]. [`Default`] is what
/// [`create_session`] uses; tests and embedders can set lower limits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionSettings {
    /// Memory pool size, shared by every running query. At least
    /// [`MIN_MEMORY_LIMIT_BYTES`].
    pub memory_limit_bytes: usize,
    /// Most disk space spill files may use. 0 turns spilling off: an operator
    /// that runs out of memory then fails its query instead.
    pub max_spill_bytes: u64,
    /// Partitions a query is split into. At least 1.
    pub target_partitions: usize,
}

impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            memory_limit_bytes: DEFAULT_MEMORY_LIMIT_BYTES,
            max_spill_bytes: DEFAULT_MAX_SPILL_BYTES,
            target_partitions: std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
        }
    }
}

impl SessionSettings {
    fn validate(&self) -> LensResult<()> {
        if self.memory_limit_bytes < MIN_MEMORY_LIMIT_BYTES {
            return Err(LensError::config(
                "memory_limit_bytes",
                format!(
                    "{} is below the minimum of {MIN_MEMORY_LIMIT_BYTES} bytes",
                    self.memory_limit_bytes
                ),
            ));
        }
        if self.target_partitions == 0 {
            return Err(LensError::config("target_partitions", "must be at least 1"));
        }
        Ok(())
    }
}

/// The [`SessionContext`] LENS runs queries in, with
/// [`SessionSettings::default`]. No tables are registered yet.
pub fn create_session(config: &LensConfig) -> LensResult<SessionContext> {
    create_session_with(config, &SessionSettings::default())
}

/// [`create_session`] with explicit runtime settings.
///
/// Fails with [`LensError::ConfigError`] if `settings` is out of range or
/// the runtime cannot be built (e.g. the spill directory cannot be created).
pub fn create_session_with(
    config: &LensConfig,
    settings: &SessionSettings,
) -> LensResult<SessionContext> {
    settings.validate()?;

    let session_config = session_config(settings);

    let pool: Arc<dyn MemoryPool> = Arc::new(TrackConsumersPool::new(
        FairSpillPool::new(settings.memory_limit_bytes),
        TRACKED_CONSUMERS,
    ));
    let disk_manager = if settings.max_spill_bytes == 0 {
        DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled)
    } else {
        DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::OsTmpDirectory)
            .with_max_temp_directory_size(settings.max_spill_bytes)
    };
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool)
        .with_disk_manager_builder(disk_manager)
        .build_arc()
        .map_err(|err| LensError::config("datafusion_runtime", err.to_string()))?;

    tracing::info!(
        chain = %config.chain(),
        target_partitions = settings.target_partitions,
        memory_limit_bytes = settings.memory_limit_bytes,
        max_spill_bytes = settings.max_spill_bytes,
        "DataFusion session created"
    );
    Ok(SessionContext::new_with_config_rt(session_config, runtime))
}

/// The planner and reader options from the table at the top of this module.
fn session_config(settings: &SessionSettings) -> SessionConfig {
    let mut session_config = SessionConfig::new()
        .with_target_partitions(settings.target_partitions)
        .with_information_schema(true);
    let options = session_config.options_mut();
    options.execution.collect_statistics = true;
    let parquet = &mut options.execution.parquet;
    parquet.pushdown_filters = true;
    parquet.reorder_filters = true;
    parquet.bloom_filter_on_read = true;
    parquet.enable_page_index = true;
    parquet.pruning = true;
    session_config
}

#[cfg(test)]
mod tests {
    use datafusion::execution::memory_pool::MemoryLimit;

    use super::*;

    #[test]
    fn defaults_pass_validation() {
        let settings = SessionSettings::default();
        assert!(settings.validate().is_ok());
        assert!(settings.target_partitions >= 1);
        const { assert!(DEFAULT_MEMORY_LIMIT_BYTES >= MIN_MEMORY_LIMIT_BYTES) };
    }

    #[test]
    fn out_of_range_settings_name_the_field() {
        let too_small = SessionSettings {
            memory_limit_bytes: MIN_MEMORY_LIMIT_BYTES - 1,
            ..SessionSettings::default()
        };
        assert_eq!(too_small.validate().unwrap_err().config_field(), Some("memory_limit_bytes"));

        let no_partitions = SessionSettings { target_partitions: 0, ..SessionSettings::default() };
        assert_eq!(no_partitions.validate().unwrap_err().config_field(), Some("target_partitions"));
    }

    #[test]
    fn session_config_sets_every_documented_option() {
        let config = session_config(&SessionSettings { target_partitions: 3, ..Default::default() });
        let options = config.options();
        assert_eq!(options.execution.target_partitions, 3);
        assert!(options.execution.collect_statistics);
        assert!(options.catalog.information_schema);
        let parquet = &options.execution.parquet;
        assert!(parquet.pushdown_filters);
        assert!(parquet.reorder_filters);
        assert!(parquet.bloom_filter_on_read);
        assert!(parquet.enable_page_index);
        assert!(parquet.pruning);
    }

    #[test]
    fn memory_pool_is_bounded_by_the_configured_limit() {
        let pool = TrackConsumersPool::new(FairSpillPool::new(MIN_MEMORY_LIMIT_BYTES), TRACKED_CONSUMERS);
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(MIN_MEMORY_LIMIT_BYTES)));
    }
}
