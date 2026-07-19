use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Notify;

#[derive(Clone)]
pub(super) struct Capacity {
    state: Arc<State>,
}

struct State {
    active: AtomicUsize,
    max: AtomicUsize,
    changed: Notify,
}

impl Capacity {
    pub(super) fn new(max: usize) -> Self {
        Self {
            state: Arc::new(State {
                active: AtomicUsize::new(0),
                max: AtomicUsize::new(max),
                changed: Notify::new(),
            }),
        }
    }

    pub(super) fn set(&self, max: usize) {
        self.state.max.store(max, Ordering::Release);
        self.state.changed.notify_waiters();
    }

    pub(super) async fn acquire(&self) -> Permit {
        loop {
            let changed = self.state.changed.notified();
            let active = self.state.active.load(Ordering::Acquire);
            let max = self.state.max.load(Ordering::Acquire);
            if active < max
                && self
                    .state
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Permit {
                    capacity: self.clone(),
                };
            }
            changed.await;
        }
    }

    pub(super) fn is_idle(&self) -> bool {
        self.state.active.load(Ordering::Acquire) == 0
    }

    pub(super) async fn wait(&self) {
        loop {
            let changed = self.state.changed.notified();
            if self.is_idle() {
                return;
            }
            changed.await;
        }
    }
}

pub(crate) struct Permit {
    capacity: Capacity,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.capacity.state.active.fetch_sub(1, Ordering::AcqRel);
        self.capacity.state.changed.notify_one();
    }
}
