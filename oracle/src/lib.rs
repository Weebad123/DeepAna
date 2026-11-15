pub mod api;
pub mod catalog;
pub mod config;
pub mod detect;
pub mod digest;
pub mod intent;
pub mod profile;
pub mod quality;
pub mod spec;
pub mod store;

use std::sync::Arc;
use std::time::Instant;

use forge::lens_client::LensApi;
use tokio_util::sync::CancellationToken;

pub use config::{BoxError, OracleConfig, OracleError, OracleResult};

use catalog::Catalog;
use detect::RunnerHandle;
use digest::DigestHandle;
use intent::Interpreter;
use profile::Profiler;
use quality::Auditor;
use spec::SpecRunner;
use store::Store;

/// Everything the HTTP API reads. Built by the host (`Oracle::start`, later)
/// once every module is up; `api/` only reads it and never builds a part.
///
/// Shared as `Arc<AppState>`; every field is itself cheap to share.
#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub catalog: Arc<Catalog>,
    pub interpreter: Arc<Interpreter>,
    pub specs: Arc<SpecRunner>,
    pub profiler: Arc<Profiler>,
    pub auditor: Arc<Auditor>,
    pub digests: DigestHandle,
    pub detectors: RunnerHandle,
    pub lens: Arc<dyn LensApi>,
    pub config: Arc<OracleConfig>,
    /// Fired when ORACLE is stopping: long requests end as `ShuttingDown`.
    pub shutdown: CancellationToken,
    /// When the process started, for `uptime_secs`.
    pub started_at: Instant,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("store", &self.store)
            .field("specs", &self.specs)
            .field("profiler", &self.profiler)
            .field("auditor", &self.auditor)
            .field("shutting_down", &self.shutdown.is_cancelled())
            .finish_non_exhaustive()
    }
}
