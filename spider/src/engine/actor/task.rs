use std::collections::HashMap;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;

pub(super) type Id = tokio::task::Id;

/// Tracks one singleton control task by its actual handle.
pub(super) struct Task {
    handle: tokio::task::JoinHandle<()>,
}

impl Task {
    pub(super) fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self { handle }
    }

    pub(super) fn matches(&self, id: Id) -> bool {
        self.handle.id() == id
    }
}

/// Tracks concurrent tasks by their actual handles.
#[derive(Default)]
pub(super) struct Tasks {
    handles: HashMap<Id, tokio::task::JoinHandle<()>>,
}

impl Tasks {
    pub(super) fn insert(&mut self, handle: tokio::task::JoinHandle<()>) {
        self.handles.insert(handle.id(), handle);
    }

    pub(super) fn remove(&mut self, id: Id) -> bool {
        self.handles.remove(&id).is_some()
    }

    pub(super) fn len(&self) -> usize {
        self.handles.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }
}

/// Protects one Engine task by converting an unwind into a runtime error.
pub(super) async fn protect<T>(
    future: impl Future<Output = Result<T, crate::Error>> + Send,
) -> Result<T, crate::Error> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| Err(crate::Error::message(panic_message(payload))))
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        format!("engine task panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<String>() {
        format!("engine task panicked: {message}")
    } else {
        "engine task panicked".to_string()
    }
}
