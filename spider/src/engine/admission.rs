use crate::spider::tx::Context;
use crate::{middleware, net, stats};

/// Applies Request middleware before work enters the Scheduler.
pub(super) async fn apply(
    requests: Vec<net::Request>,
    context: Option<&Context>,
    registry: &middleware::Registry,
) -> Result<Vec<net::Request>, crate::Error> {
    let mut accepted = Vec::with_capacity(requests.len());
    for request in requests {
        let node = request.node_key().to_string();
        match registry
            .before_scheduler(request)
            .await
            .map_err(crate::Error::Middleware)?
        {
            middleware::registry::Output::Continue(request) => accepted.push(request),
            middleware::registry::Output::Skip { middleware } => {
                if let Some(context) = context
                    && let Some(stats) = context.stats()
                {
                    stats.total(&node, 1);
                    record_skip(stats, &node, &middleware);
                }
            }
        }
    }
    Ok(accepted)
}

fn record_skip(stats: &stats::Delta, node: &str, middleware: &str) {
    match middleware {
        "dedup" => stats.dedup(node, 1),
        "validate" => stats.validate(node, 1),
        _ => stats.filter(node, 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::{Init as _, Scheduler as _};

    #[tokio::test]
    async fn init_failure_does_not_restore_an_observed_fingerprint() {
        let registry = middleware::Registry::new();
        let mut request = net::Request::follow("https://example.com/article").unwrap();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.middlewares.push(
            middleware::Spec::new("dedup")
                .hook("before_scheduler")
                .args(serde_json::json!({
                    "rules": {
                        "url": {
                            "key": ["$request.url"],
                            "ttl": -1
                        }
                    }
                })),
        );
        let scheduler = crate::scheduler::Memory::new("worker-1");

        let accepted = apply(vec![request.clone()], None, &registry).await.unwrap();
        let error = scheduler
            .init(
                request.trace_id.clone(),
                crate::trace::Snapshot::code("different-task"),
                accepted,
            )
            .await
            .unwrap_err();

        assert!(matches!(error, crate::scheduler::Error::Message(_)));
        assert!(scheduler.trace(&request.trace_id).await.unwrap().is_none());
        assert!(
            apply(vec![request], None, &registry)
                .await
                .unwrap()
                .is_empty()
        );
    }
}
