use std::path::PathBuf;
use std::sync::Arc;

use crate::spider::tx::Context;
use crate::{middleware, payload, scheduler};

pub(super) async fn handle<S>(
    items: Vec<Box<dyn crate::item::Item>>,
    context: Option<&Context>,
    scheduler: Arc<S>,
    registry: Arc<middleware::Registry>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + Send,
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
    let mut snapshot: Option<PathBuf> = None;
    let mut snapshot_error: Option<String> = None;
    loop {
        match scheduler.push_items(&payload).await {
            Ok(()) => {
                if let (Some(path), Some(snapshots)) = (snapshot.as_deref(), &snapshots) {
                    let _ = snapshots.remove(path).await;
                }
                break;
            }
            Err(error) => {
                let submit_error = error.to_string();
                if snapshot.is_none()
                    && snapshot_error.is_none()
                    && let Some(snapshots) = &snapshots
                {
                    match snapshots.write(&payload, &submit_error).await {
                        Ok(path) => snapshot = Some(path),
                        Err(error) => snapshot_error = Some(error.to_string()),
                    }
                }
                if let Some(delay) = retry.delay(attempt) {
                    attempt += 1;
                    tokio::time::sleep(delay).await;
                    continue;
                }
                let message = snapshot_error.as_ref().map_or_else(
                    || submit_error.clone(),
                    |snapshot_error| {
                        format!("{submit_error}; failure snapshot also failed: {snapshot_error}")
                    },
                );
                for item in &payload.items {
                    registry
                        .error_item(item.as_ref(), &message)
                        .await
                        .map_err(crate::Error::Middleware)?;
                }
                return if snapshot_error.is_some() {
                    Err(crate::item::Error::Message(message).into())
                } else {
                    Err(crate::Error::Scheduler(error))
                };
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
