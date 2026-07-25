use crate::Error;
use crate::svc::Context;
use crate::types::{Page, worker};

pub(crate) async fn list(
    context: &Context,
    query: &worker::List,
) -> Result<Page<worker::Summary>, Error> {
    context
        .store
        .workers(context.config.namespace(), query)
        .await
}

pub(crate) fn policy(context: &Context) -> worker::Policy {
    let policy = context.config.policy();
    worker::Policy {
        lease_timeout_ms: policy.lease_timeout_ms,
        lease_interval_ms: policy.lease_interval_ms,
        heartbeat_interval_ms: policy.heartbeat_interval_ms,
        max_response_bytes: context.config.max_api_bytes() as u64,
    }
}

pub(crate) async fn heartbeat(context: &Context, body: &worker::Heartbeat) -> Result<(), Error> {
    context
        .store
        .heartbeat(context.config.namespace(), body)
        .await
}
