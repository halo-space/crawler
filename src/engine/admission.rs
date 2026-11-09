use crate::spider::tx::Context;
use crate::{middleware, net, stats};

/// Applies Request middleware before work enters the Scheduler.
pub(super) async fn apply(
    requests: Vec<net::Request>,
    context: Option<&Context>,
    registry: &middleware::Registry,
) -> Result<Vec<net::Request>, crate::Error> {
    for request in &requests {
        validate_identity(request, context)?;
    }

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

fn validate_identity(
    request: &net::Request,
    context: Option<&Context>,
) -> Result<(), crate::Error> {
    let Some(context) = context else {
        return Ok(());
    };
    if request.task_id != context.task_id() {
        return Err(crate::Error::Middleware(middleware::Error::Message(
            "Request task_id does not match its Tx context".to_string(),
        )));
    }
    if request.trace_id != context.trace_id() {
        return Err(crate::Error::Middleware(middleware::Error::Message(
            "Request trace_id does not match its Tx context".to_string(),
        )));
    }
    Ok(())
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
    use crate::middleware::{BoxFuture, Middleware, Next, Spec};

    struct ChangeIdentity {
        id: Option<&'static str>,
        task_id: Option<&'static str>,
        trace_id: Option<&'static str>,
    }

    impl Middleware for ChangeIdentity {
        fn order(&self, _hook: &str) -> i32 {
            200
        }

        fn before_scheduler<'a>(
            &'a self,
            mut request: net::Request,
            _spec: &'a Spec,
        ) -> BoxFuture<'a, Next<net::Request>> {
            Box::pin(async move {
                if let Some(id) = self.id {
                    request.id = id.to_string();
                }
                if let Some(task_id) = self.task_id {
                    request.task_id = task_id.to_string();
                }
                if let Some(trace_id) = self.trace_id {
                    request.trace_id = trace_id.to_string();
                }
                Ok(Next::Continue(request))
            })
        }
    }

    #[tokio::test]
    async fn rejects_wrong_owner_before_dedup_observes_the_fingerprint() {
        let registry = middleware::Registry::new();
        let mut request = net::Request::follow("https://example.com/article").unwrap();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.middlewares.push(
            middleware::Spec::new("dedup")
                .hook("before_scheduler")
                .args(serde_json::json!({
                    "key": ["$request.url"],
                    "ttl": -1
                })),
        );
        let mut wrong_owner = request.clone();
        wrong_owner.task_id = "task-2".to_string();
        let context = Context::trace("task-1", "trace-1");

        let error = apply(
            vec![request.clone(), wrong_owner],
            Some(&context),
            &registry,
        )
        .await
        .unwrap_err();
        assert!(matches!(error, crate::Error::Middleware(_)));

        assert_eq!(
            apply(vec![request.clone()], Some(&context), &registry)
                .await
                .unwrap()
                .len(),
            1
        );

        request.url = "https://example.com/other".to_string();
        request.trace_id = "trace-2".to_string();
        let error = apply(vec![request], Some(&context), &registry)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::Error::Middleware(_)));
    }

    #[tokio::test]
    async fn rejects_middleware_identity_changes_before_dedup() {
        let registry = middleware::Registry::new();
        registry.register(
            "change-task",
            ChangeIdentity {
                id: None,
                task_id: Some("other-task"),
                trace_id: None,
            },
        );
        registry.register(
            "change-trace",
            ChangeIdentity {
                id: None,
                task_id: None,
                trace_id: Some("other-trace"),
            },
        );
        let dedup = Spec::new("dedup")
            .hook("before_scheduler")
            .args(serde_json::json!({
                "key": ["$request.url"],
                "ttl": -1
            }));
        let mut request = net::Request::follow("https://example.com/article").unwrap();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.middlewares = vec![
            Spec::new("change-task").hook("before_scheduler"),
            dedup.clone(),
        ];
        let context = Context::trace("task-1", "trace-1");

        let error = apply(vec![request], Some(&context), &registry)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::Error::Middleware(_)));

        let mut valid = net::Request::follow("https://example.com/article").unwrap();
        valid.task_id = "task-1".to_string();
        valid.trace_id = "trace-1".to_string();
        valid.middlewares = vec![dedup];
        assert_eq!(
            apply(vec![valid], Some(&context), &registry)
                .await
                .unwrap()
                .len(),
            1
        );

        let mut changed_trace = net::Request::follow("https://example.com/other").unwrap();
        changed_trace.task_id = "task-1".to_string();
        changed_trace.trace_id = "trace-1".to_string();
        changed_trace.middlewares = vec![
            Spec::new("change-trace").hook("before_scheduler"),
            Spec::new("dedup")
                .hook("before_scheduler")
                .args(serde_json::json!({
                    "key": ["$request.url"],
                    "ttl": -1
                })),
        ];
        let error = apply(vec![changed_trace], Some(&context), &registry)
            .await
            .unwrap_err();
        assert!(matches!(error, crate::Error::Middleware(_)));
    }

    #[tokio::test]
    async fn rejects_middleware_identity_id_changes_before_dedup() {
        let registry = middleware::Registry::new();
        registry.register(
            "change-id",
            ChangeIdentity {
                id: Some("other-id"),
                task_id: None,
                trace_id: None,
            },
        );
        let dedup = Spec::new("dedup")
            .hook("before_scheduler")
            .args(serde_json::json!({
                "key": ["$request.url"],
                "ttl": -1
            }));
        let mut request = net::Request::follow("https://example.com/article").unwrap();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.middlewares = vec![
            Spec::new("change-id").hook("before_scheduler"),
            dedup.clone(),
        ];
        let context = Context::trace("task-1", "trace-1");

        let error = apply(vec![request], Some(&context), &registry)
            .await
            .unwrap_err();

        assert!(matches!(error, crate::Error::Middleware(_)));
        assert!(error.to_string().contains("Request id"));

        let mut valid = net::Request::follow("https://example.com/article").unwrap();
        valid.task_id = "task-1".to_string();
        valid.trace_id = "trace-1".to_string();
        valid.middlewares = vec![dedup];
        assert_eq!(
            apply(vec![valid], Some(&context), &registry)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
