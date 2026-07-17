use std::future::Future;
use std::sync::Arc;

use crate::{net, payload, stats};

tokio::task_local! {
    static CURRENT: Context;
}

#[derive(Clone)]
pub(crate) struct Context {
    id: String,
    task_id: String,
    trace_id: String,
    version: i64,
    worker_id: String,
    node: String,
    stats: Option<Arc<stats::Delta>>,
}

impl Context {
    fn new(request: &net::Request, stats: Arc<stats::Delta>) -> Self {
        Self {
            id: request.id.clone(),
            task_id: request.task_id.clone(),
            trace_id: request.trace_id.clone(),
            version: request.version,
            worker_id: request.leased_by.clone(),
            node: request.node_key().to_string(),
            stats: Some(stats),
        }
    }

    pub(super) fn trace(task_id: impl Into<String>, trace_id: impl Into<String>) -> Self {
        Self {
            id: String::new(),
            task_id: task_id.into(),
            trace_id: trace_id.into(),
            version: 0,
            worker_id: String::new(),
            node: String::new(),
            stats: None,
        }
    }

    pub(crate) fn payload(&self) -> payload::Payload {
        payload::Payload {
            id: self.id.clone(),
            task_id: self.task_id.clone(),
            trace_id: self.trace_id.clone(),
            version: self.version,
            worker_id: self.worker_id.clone(),
            node: self.node.clone(),
            ..payload::Payload::new()
        }
    }

    pub(crate) fn stats(&self) -> Option<&stats::Delta> {
        self.stats.as_deref()
    }

    pub(super) fn task_id(&self) -> &str {
        &self.task_id
    }

    pub(super) fn trace_id(&self) -> &str {
        &self.trace_id
    }
}

pub(crate) async fn scope<F>(
    request: &net::Request,
    stats: Arc<stats::Delta>,
    future: F,
) -> F::Output
where
    F: Future,
{
    CURRENT.scope(Context::new(request, stats), future).await
}

pub(super) fn current() -> Option<Context> {
    CURRENT.try_with(Clone::clone).ok()
}
