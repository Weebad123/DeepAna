//! LENS: SQL over the Parquet tables PRISM writes, served over HTTP.
//!
//! ```text
//!   config ─► engine (session + catalog) ─► api ─► POST /query, GET /health
//!                                            └──► result (JSON / CSV)
//! ```

pub mod config;
pub mod error;
mod api;
mod catalog;
mod engine;
mod result;

pub use api::*;
pub use catalog::*;
pub use config::LensConfig;
pub use engine::*;
pub use error::{BoxError, LensError, LensResult};
pub use result::*;
