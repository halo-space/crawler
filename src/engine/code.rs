use std::sync::Arc;

use crate::scheduler;

#[doc(hidden)]
pub struct Init {
    seed: Option<(String, String, Option<crate::item::attachment::Config>)>,
}

impl Init {
    pub(crate) fn new(
        seed: Option<(String, String, Option<crate::item::attachment::Config>)>,
    ) -> Self {
        Self { seed }
    }
}

impl<S> super::init::Init<S> for Init
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
{
    async fn init(&self, scheduler: Arc<S>) -> Result<super::init::Output, crate::Error> {
        let Some((task_id, trace_id, attachment)) = &self.seed else {
            return Ok(super::init::Output::Consume);
        };
        let mut snapshot = crate::trace::Snapshot::code(task_id.clone());
        snapshot.attachment = attachment
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| crate::Error::message(error.to_string()))?;
        scheduler
            .init(trace_id.clone(), snapshot, Vec::new())
            .await
            .map_err(crate::Error::Scheduler)?;
        Ok(super::init::Output::Start)
    }
}
