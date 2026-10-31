//! The change log: where PRISM publishes its [`ChangeEvent`] stream.
//!
//! [`ChangeLog`] is the sink every producer writes to (the gap handler for
//! `Skip`, the ingestor's writer task for `New`, the finality poller for
//! `Final`). Implementations decide where the events go; [`LocalChangeLog`]
//! appends them to a JSON-lines file that FORGE tails.
//!
//! | File        | Holds                                                  |
//! |-------------|--------------------------------------------------------|
//! | `mod.rs`    | [`ChangeLog`], the sink trait                          |
//! | [`local`]   | [`LocalChangeLog`], the file-backed implementation      |
//!
//! Every recorded event gets an id from the log: `1` for the first, then
//! one more for each, with no gaps and in file order, however many producers
//! emit at once. A failed emit assigns no id.
//!
//! The trait is synchronous: an emit is one small append and a flush one
//! `fsync`, the same kind of blocking I/O the Parquet writer does.

use crate::types::error::PrismResult;
use crate::types::event::ChangeEvent;

pub mod local;

pub use local::{CHANGELOG_FILE_NAME, ChangeLogStats, LocalChangeLog};

/// A sink for [`ChangeEvent`]s.
pub trait ChangeLog: Send + Sync {
    /// Append `event` to the log and return the id it was recorded under.
    /// An `Err` means it was not recorded and no id was used up.
    ///
    /// A recorded event may still be lost to a power failure until the next
    /// [`Self::flush`].
    fn emit(&self, event: ChangeEvent) -> PrismResult<u64>;

    /// Make every event recorded so far durable (`fsync`).
    fn flush(&self) -> PrismResult<()>;
}
