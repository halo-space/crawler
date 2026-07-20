use std::future::Future;
use std::sync::Arc;

use crate::{middleware, scheduler};

#[doc(hidden)]
pub trait Init<S>: Send + Sync
where
    S: scheduler::Scheduler + 'static,
{
    fn init(
        &self,
        scheduler: Arc<S>,
        registry: Arc<middleware::Registry>,
    ) -> impl Future<Output = Result<Output, crate::Error>> + Send;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Output {
    Start,
    Consume,
}

#[doc(hidden)]
pub struct NoInit;

impl<S> Init<S> for NoInit
where
    S: scheduler::Scheduler + 'static,
{
    async fn init(
        &self,
        _scheduler: Arc<S>,
        _registry: Arc<middleware::Registry>,
    ) -> Result<Output, crate::Error> {
        Ok(Output::Consume)
    }
}
