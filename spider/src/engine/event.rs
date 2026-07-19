use std::sync::Arc;

use crate::spider::tx::Kind;
use crate::{middleware, scheduler, stats};

mod item;
mod request;

/// 处理 Spider Tx 发出的 Request 或 Item。
pub(crate) async fn handle<S>(
    event: Kind,
    scheduler: Arc<S>,
    registry: Arc<middleware::Registry>,
    snapshots: Option<Arc<crate::item::snapshot::Store>>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + Send,
{
    match event {
        Kind::Requests { requests, context } => {
            request::handle(requests, context.as_ref(), scheduler, registry).await
        }
        Kind::Items { items, context } => {
            item::handle(items, context.as_ref(), scheduler, registry, snapshots).await
        }
    }
}

fn record_skip(stats: &stats::Delta, node: &str, middleware: &str) {
    match middleware {
        "dedup" => stats.dedup(node, 1),
        "validate" => stats.validate(node, 1),
        _ => stats.filter(node, 1),
    }
}
