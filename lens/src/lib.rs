//! LENS: SQL over the Parquet tables PRISM writes, served over HTTP.
//!
//! ```text
//!   config ─► engine (session + catalog) ─► api ─► POST /query, GET /health
//!                                            └──► result (JSON / CSV)
//!   service: start(config, token) ─► Lens { router(), stopped(), wait() },
//!            optionally running PRISM in-process
//! ```

pub mod config;
pub mod error;
mod api;
mod catalog;
mod engine;
mod result;
mod service;

pub use api::*;
pub use catalog::*;
pub use config::LensConfig;
pub use engine::*;
pub use error::{BoxError, LensError, LensResult};
pub use result::*;
pub use service::{
    CATALOG_REFRESH_TASK, Lens, LensServiceConfig, PRISM_CLUSTER, PRISM_TASK, build_ingestor, create_table_dirs, start,
};
