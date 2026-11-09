use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::Request;

use super::{Config, invalid};

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub struct Memory {
    state: Mutex<State>,
}

struct State {
    groups: HashMap<String, Arc<Slot>>,
    next_cleanup: Instant,
}

struct Slot {
    interval: Duration,
    next: AsyncMutex<Instant>,
}

impl Default for Memory {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::new(Instant::now())),
        }
    }
}

impl Middleware for Memory {
    fn order(&self, _hook: &str) -> i32 {
        200
    }

    fn before_download<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            let config = Config::from_spec(spec)?;
            let slot = self.group(&config.group(&request)?, config.interval())?;
            let mut next = slot.next.lock().await;
            let now = Instant::now();
            if *next > now {
                tokio::time::sleep(*next - now).await;
            }
            *next = Instant::now()
                .checked_add(slot.interval)
                .ok_or_else(|| invalid("qps exceeds the runtime clock range"))?;
            Ok(Next::Continue(request))
        })
    }
}

impl Memory {
    fn group(
        &self,
        group: &str,
        interval: Duration,
    ) -> Result<Arc<Slot>, crate::middleware::Error> {
        let now = Instant::now();
        let mut state = self.state();
        state.cleanup(now);
        if let Some(slot) = state.groups.get(group) {
            if slot.interval == interval {
                return Ok(Arc::clone(slot));
            }
            if !can_remove(slot, now) {
                return Err(invalid("group is already active with a different qps"));
            }
        }
        let slot = Arc::new(Slot {
            interval,
            next: AsyncMutex::new(now),
        });
        state.groups.insert(group.to_string(), Arc::clone(&slot));
        Ok(slot)
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl State {
    fn new(now: Instant) -> Self {
        Self {
            groups: HashMap::new(),
            next_cleanup: cleanup_deadline(now),
        }
    }

    fn cleanup(&mut self, now: Instant) {
        if now < self.next_cleanup {
            return;
        }
        self.groups.retain(|_, slot| !can_remove(slot, now));
        self.next_cleanup = cleanup_deadline(now);
    }
}

fn can_remove(slot: &Arc<Slot>, now: Instant) -> bool {
    if Arc::strong_count(slot) != 1 {
        return false;
    }
    let Ok(next) = slot.next.try_lock() else {
        return false;
    };
    *next <= now
}

fn cleanup_deadline(now: Instant) -> Instant {
    now.checked_add(CLEANUP_INTERVAL).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(interval: Duration, next: Instant) -> Arc<Slot> {
        Arc::new(Slot {
            interval,
            next: AsyncMutex::new(next),
        })
    }

    #[tokio::test]
    async fn limits_concurrent_requests_in_the_same_group() {
        let memory = Memory::default();
        let spec = Spec::new("rate_limit").args(serde_json::json!({
            "group": "api",
            "qps": 100.0
        }));
        let started = Instant::now();
        let (first, second) = tokio::join!(
            memory.before_download(Request::follow("https://example.com/1").unwrap(), &spec,),
            memory.before_download(Request::follow("https://example.com/2").unwrap(), &spec,)
        );
        first.unwrap();
        second.unwrap();
        assert!(started.elapsed() >= Duration::from_millis(8));
    }

    #[tokio::test]
    async fn rejects_conflicting_qps_without_changing_the_schedule() {
        let memory = Memory::default();
        let first = Spec::new("rate_limit").args(serde_json::json!({"group": "api", "qps": 1.0}));
        memory
            .before_download(Request::follow("https://example.com/1").unwrap(), &first)
            .await
            .unwrap();
        let slot = Arc::clone(memory.state().groups.get("api").unwrap());
        let scheduled = *slot.next.lock().await;
        let second = Spec::new("rate_limit").args(serde_json::json!({"group": "api", "qps": 2.0}));
        assert!(
            memory
                .before_download(Request::follow("https://example.com/2").unwrap(), &second)
                .await
                .is_err()
        );
        assert_eq!(*slot.next.lock().await, scheduled);
    }

    #[test]
    fn replaces_an_inactive_elapsed_group() {
        let memory = Memory::default();
        let now = Instant::now();
        memory.state().groups.insert(
            "api".to_string(),
            slot(
                Duration::from_secs(1),
                now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            ),
        );
        let current = memory.group("api", Duration::from_millis(500)).unwrap();
        assert_eq!(current.interval, Duration::from_millis(500));
    }
}
