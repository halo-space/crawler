use std::sync::Arc;

use crate::spider::tx::{Event, Reply};
use crate::{middleware, scheduler, stats};

mod item;
mod request;

/// 处理 Spider Tx 发出的 Request 或 Item。
pub(crate) async fn handle<S>(
    event: Event,
    scheduler: Arc<S>,
    registry: Arc<middleware::Registry>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
) where
    S: scheduler::Scheduler + Send,
{
    match event {
        Event::Requests {
            requests,
            context,
            reply,
            permit: _permit,
        } => respond(
            reply,
            request::handle(requests, context.as_ref(), scheduler, registry).await,
        ),
        Event::Items {
            items,
            context,
            reply,
            permit: _permit,
        } => respond(
            reply,
            item::handle(items, context.as_ref(), scheduler, registry, snapshots).await,
        ),
    }
}

fn respond(reply: Reply, result: Result<(), crate::Error>) {
    let _ = reply.send(result.map_err(|error| error.to_string()));
}

fn record_skip(stats: &stats::Delta, node: &str, middleware: &str) {
    match middleware {
        "dedup" => stats.dedup(node, 1),
        "validate" => stats.validate(node, 1),
        _ => stats.filter(node, 1),
    }
}
