use std::sync::Arc;

use crate::spider::tx::Context;
use crate::{middleware, net, scheduler};

pub(super) async fn handle<S>(
    requests: Vec<net::Request>,
    context: Option<&Context>,
    scheduler: Arc<S>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + Send,
{
    let mut ready = Vec::with_capacity(requests.len());
    for request in requests {
        let node = request.node_key().to_string();
        match registry
            .before_scheduler(request)
            .await
            .map_err(crate::Error::Middleware)?
        {
            middleware::registry::Output::Continue(request) => ready.push(request),
            middleware::registry::Output::Skip { middleware } => {
                if let Some(context) = context
                    && let Some(stats) = context.stats()
                {
                    stats.total(&node, 1);
                    super::record_skip(stats, &node, &middleware);
                }
            }
        }
    }

    if ready.is_empty() {
        return Ok(());
    }

    let payload = context
        .map(Context::payload)
        .unwrap_or_default()
        .requests(ready);

    scheduler
        .push(payload)
        .await
        .map_err(crate::Error::Scheduler)
}
