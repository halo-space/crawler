use std::sync::Arc;

use crate::spider::tx::Context;
use crate::{item, middleware, payload};

pub(super) async fn handle<O>(
    items: Vec<Box<dyn crate::item::Item>>,
    context: Option<&Context>,
    store: Arc<O>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    O: item::Store + Send,
{
    if let Some(stats) = context.and_then(Context::stats) {
        stats.total("items", items.len());
    }

    let mut ready = Vec::with_capacity(items.len());
    for item in items {
        let item = match registry
            .before_item(item)
            .await
            .map_err(crate::Error::Middleware)?
        {
            middleware::registry::Output::Continue(item) => item,
            middleware::registry::Output::Skip { middleware } => {
                if let Some(context) = context
                    && let Some(stats) = context.stats()
                {
                    super::record_skip(stats, "items", &middleware);
                }
                continue;
            }
        };

        ready.push(item);
    }

    if ready.is_empty() {
        return Ok(());
    }

    let mut payload = context
        .map(Context::payload)
        .unwrap_or_default()
        .items(ready);
    if payload.task_id.is_empty() {
        payload.task_id = "default".to_string();
    }
    let retry = retry_policy(&payload, &registry)?;
    let mut attempt = 0;
    loop {
        match store.submit(&payload).await {
            Ok(()) => {
                break;
            }
            Err(error) => {
                let submit_error = error.to_string();
                if let Some(delay) = retry.delay(attempt) {
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                for item in &payload.items {
                    if let Err(callback_error) =
                        registry.error_item(item.as_ref(), &submit_error).await
                    {
                        tracing::error!(
                            item_id = %item.id(),
                            store_error = %submit_error,
                            error = %callback_error,
                            "failed to run Item error middleware"
                        );
                    }
                }
                return Err(crate::Error::Item(error));
            }
        }
    }

    if let Some(stats) = context.and_then(Context::stats) {
        stats.done("items", payload.items.len());
    }

    Ok(())
}

fn retry_policy(
    payload: &payload::Payload,
    registry: &middleware::Registry,
) -> Result<crate::middleware::retry::Policy, crate::Error> {
    let mut policies = payload
        .items
        .iter()
        .map(|item| registry.retry_policy(item.middlewares(), "error_item"));
    let Some(first) = policies
        .next()
        .transpose()
        .map_err(crate::Error::Middleware)?
    else {
        return Ok(crate::middleware::retry::Policy::default());
    };
    for policy in policies {
        let policy = policy.map_err(crate::Error::Middleware)?;
        if policy != first {
            return Err(crate::Error::Middleware(
                crate::middleware::Error::InvalidConfig {
                    name: "retry".to_string(),
                    message: "all Items in one payload must use the same error_item retry policy"
                        .to_string(),
                },
            ));
        }
    }
    Ok(first)
}
