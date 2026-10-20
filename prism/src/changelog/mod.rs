//! The change log: where PRISM publishes its [`ChangeEvent`] stream.
//!
//! [`ChangeLog`] is the sink every producer writes to (the slot processor for
//! `New` / `Skip`, the commitment tracker for `Final`). Implementations decide
//! where the events go; [`local`] will append them to a file.

use async_trait::async_trait;

use crate::types::error::PrismResult;
use crate::types::event::ChangeEvent;

pub mod local;

/// A sink for [`ChangeEvent`]s.
#[async_trait]
pub trait ChangeLog: Send + Sync {
    /// Append `event` to the log. Returns once the event is recorded; an
    /// `Err` means it was not, and the caller still owns delivering it.
    async fn emit(&self, event: ChangeEvent) -> PrismResult<()>;
}
