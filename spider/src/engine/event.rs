use std::sync::Arc;

use crate::spider::tx::Kind;
use crate::{middleware, scheduler, stats, trace};

mod item;
mod request;

/// 处理 Spider Tx 发出的 Request 或 Item。
pub(crate) async fn handle<S, O>(
    event: Kind,
    parent: Option<trace::RuntimeContext>,
    scheduler: Arc<S>,
    store: Arc<O>,
    registry: Arc<middleware::Registry>,
) -> Result<(), crate::Error>
where
    S: scheduler::Scheduler + Send,
    O: crate::item::Store + Send,
{
    let (name, count) = event.output();
    trace::output(
        name,
        count,
        parent,
        async move {
            match event {
                Kind::Requests { requests, context } => {
                    request::handle(requests, context.as_ref(), scheduler, registry).await
                }
                Kind::Items { items, context } => {
                    item::handle(items, context.as_ref(), store, registry).await
                }
            }
        },
        trace::error_class,
    )
    .await
}

fn record_skip(stats: &stats::Delta, node: &str, middleware: &str) {
    match middleware {
        "dedup" => stats.dedup(node, 1),
        "validate" => stats.validate(node, 1),
        _ => stats.filter(node, 1),
    }
}
