use std::sync::Arc;

use crate::spider::tx::Context;
use crate::{middleware, net, scheduler};

pub(super) async fn handle<S>(
    mut requests: Vec<net::Request>,
    context: Option<&Context>,
    scheduler: Arc<S>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + Send,
{
    let ids = context
        .map(|context| context.assign_ids(&mut requests))
        .transpose()?;
    let requests = crate::engine::admission::apply(requests, context, registry.as_ref()).await?;
    if requests.is_empty() {
        if let Some(ids) = ids {
            ids.commit();
        }
        return Ok(());
    }

    let payload = context
        .map(Context::payload)
        .unwrap_or_default()
        .requests(requests);

    scheduler
        .push(payload)
        .await
        .map_err(crate::Error::Scheduler)?;
    if let Some(ids) = ids {
        ids.commit();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn retains_fingerprint_after_scheduler_push_failure() {
        let scheduler = Arc::new(crate::scheduler::Memory::new());
        let registry = Arc::new(crate::middleware::Registry::new());
        let mut request = crate::net::Request::follow("https://example.com/article").unwrap();
        request.middlewares.push(
            crate::middleware::Spec::new("dedup").args(serde_json::json!({
                "rules": {
                    "url": {
                        "key": ["$request.url"],
                        "ttl": -1
                    }
                }
            })),
        );
        request.version = 1;

        let error = handle(
            vec![request.clone()],
            None,
            scheduler.clone(),
            registry.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, crate::Error::Scheduler(_)));
        assert_eq!(scheduler.queued_len(), 0);

        request.version = 0;
        handle(vec![request], None, scheduler.clone(), registry)
            .await
            .unwrap();

        assert_eq!(scheduler.queued_len(), 0);
    }
}
