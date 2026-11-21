//! DeepAna: every service in one process, on one port, under one data
//! directory, with a live dashboard.
//!
//! ```text
//! cargo run -p deepana -- --parquet-directory <DIR> [--open]
//! ```
//!
//! | Module            | What it holds                                                    |
//! |-------------------|------------------------------------------------------------------|
//! | [`layout`]        | Every path under `<DIR>`                                         |
//! | [`config`]        | The port, and each service's config built from `<DIR>`           |
//! | [`app`]           | Starting the services, the router, fail-stop supervision         |
//! | [`stream`]        | The one poller and `GET /api/stream` (SSE)                       |
//! | [`commons_panel`] | `commons verify`, run in-process for the dashboard               |

pub mod app;
pub mod commons_panel;
pub mod config;
pub mod layout;
pub mod stream;
