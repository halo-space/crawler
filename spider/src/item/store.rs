use std::future::Future;

use crate::{item, payload};

/// Persistence boundary for parsed Items.
///
/// A Store owns Item output and its own resources. It never claims, leases, or
/// settles Requests in a Scheduler.
pub trait Store: Send + Sync {
    fn open(&self) -> impl Future<Output = Result<(), item::Error>> + Send;

    fn close(&self) -> impl Future<Output = Result<(), item::Error>> + Send;

    /// Submit one complete Item Payload.
    ///
    /// Implementations must call [`payload::Payload::validate_store`] before
    /// mutating their backend. A returned error does not guarantee that the
    /// backend accepted no data, so implementations must accept an unchanged
    /// Payload being retried.
    fn submit(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), item::Error>> + Send;
}
