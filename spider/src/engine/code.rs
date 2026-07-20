use std::sync::Arc;

use crate::{middleware, scheduler};

#[doc(hidden)]
pub struct Init {
    seed: Option<(String, String)>,
}

impl Init {
    pub(crate) fn new(seed: Option<(String, String)>) -> Self {
        Self { seed }
    }
}

impl<S> super::init::Init<S> for Init
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
{
    async fn init(
        &self,
        scheduler: Arc<S>,
        _registry: Arc<middleware::Registry>,
    ) -> Result<super::init::Output, crate::Error> {
        let Some((task_id, trace_id)) = &self.seed else {
            return Ok(super::init::Output::Consume);
        };
        scheduler
            .init(
                trace_id.clone(),
                crate::trace::Snapshot::code(task_id.clone()),
                Vec::new(),
            )
            .await
            .map_err(crate::Error::Scheduler)?;
        Ok(super::init::Output::Start)
    }
}
