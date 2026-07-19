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

#[derive(Debug, Eq, PartialEq)]
enum Reservation {
    Acquired,
    Contended,
    Full,
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
            match self.reserve(active, max) {
                Reservation::Acquired => {
                    return Permit {
                        capacity: self.clone(),
                    };
                }
                Reservation::Contended => continue,
                Reservation::Full => changed.await,
            }
        }
    }

    fn reserve(&self, active: usize, max: usize) -> Reservation {
        if active >= max {
            return Reservation::Full;
        }
        if self
            .state
            .active
            .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            Reservation::Acquired
        } else {
            Reservation::Contended
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn stale_reservation_retries_while_capacity_remains() {
        let capacity = Capacity::new(2);
        let stale = capacity.state.active.load(Ordering::Acquire);
        capacity.state.active.store(1, Ordering::Release);

        assert_eq!(capacity.reserve(stale, 2), Reservation::Contended);
        let permit = tokio::time::timeout(Duration::from_millis(100), capacity.acquire())
            .await
            .expect("CAS contention must not wait for a release");

        assert_eq!(capacity.state.active.load(Ordering::Acquire), 2);
        drop(permit);
    }
}
