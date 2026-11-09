use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;

#[derive(Clone)]
pub(super) struct Id(Arc<()>);

impl Id {
    pub(super) fn new() -> Self {
        Self(Arc::new(()))
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Tracks one singleton control task by its actual handle.
pub(super) struct Task {
    id: Id,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for Task {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Task {
    pub(super) fn new(id: Id, handle: tokio::task::JoinHandle<()>) -> Self {
        Self { id, handle }
    }

    pub(super) fn matches(&self, id: &Id) -> bool {
        self.id.matches(id)
    }

    pub(super) fn abort(self) {
        self.handle.abort();
    }
}

/// Tracks concurrent tasks by their actual handles.
#[derive(Default)]
pub(super) struct Tasks {
    tasks: Vec<Task>,
}

impl Tasks {
    pub(super) fn insert(&mut self, id: Id, handle: tokio::task::JoinHandle<()>) {
        self.tasks.push(Task::new(id, handle));
    }

    pub(super) fn remove(&mut self, id: &Id) -> bool {
        let Some(index) = self.tasks.iter().position(|task| task.matches(id)) else {
            return false;
        };
        self.tasks.swap_remove(index);
        true
    }

    pub(super) fn len(&self) -> usize {
        self.tasks.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
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

#[cfg(test)]
mod tests {
    use super::*;

    struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn dropping_a_tracked_task_aborts_its_future() {
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (dropped, dropped_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _notify = NotifyOnDrop(Some(dropped));
            let _ = started.send(());
            std::future::pending::<()>().await;
        });
        let task = Task::new(Id::new(), handle);
        started_rx.await.unwrap();

        drop(task);

        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("tracked task was detached instead of aborted")
            .unwrap();
    }
}
