use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(super) struct Activity {
    inner: Arc<State>,
}

#[derive(Default)]
struct State {
    producers: AtomicUsize,
    changed: Notify,
}

impl Activity {
    pub(super) fn register(&self) {
        self.inner.producers.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn unregister(&self) {
        if self.inner.producers.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.inner.changed.notify_waiters();
        }
    }

    pub(super) fn is_idle(&self) -> bool {
        self.inner.producers.load(Ordering::Acquire) == 0
    }

    pub(super) async fn wait(&self) {
        loop {
            let changed = self.inner.changed.notified();
            if self.is_idle() {
                return;
            }
            changed.await;
        }
    }
}

pub(super) struct Registration {
    activity: Activity,
}

impl Registration {
    pub(super) fn new(activity: Activity) -> Self {
        activity.register();
        Self { activity }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.activity.unregister();
    }
}
