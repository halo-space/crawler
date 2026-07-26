use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::Request;

use super::{Config, Fingerprint, Ttl, invalid};

#[derive(Default)]
pub struct Memory {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    values: HashMap<Fingerprint, Option<Instant>>,
    expirations: BinaryHeap<Reverse<(Instant, Fingerprint)>>,
}

impl Middleware for Memory {
    fn order(&self, _hook: &str) -> i32 {
        400
    }

    fn before_scheduler<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            if request.dont_filter {
                return Ok(Next::Continue(request));
            }
            let config = Config::from_spec(spec)?;
            let Some(fingerprint) = config.fingerprint(&request)? else {
                return Ok(Next::Continue(request));
            };
            if self.contains_or_insert(fingerprint, config.ttl())? {
                Ok(Next::Skip)
            } else {
                Ok(Next::Continue(request))
            }
        })
    }
}

impl Memory {
    fn contains_or_insert(
        &self,
        fingerprint: Fingerprint,
        ttl: Ttl,
    ) -> Result<bool, crate::middleware::Error> {
        let now = Instant::now();
        let expires = match ttl {
            Ttl::Permanent => None,
            Ttl::Finite(ttl) => Some(now.checked_add(ttl).ok_or_else(|| {
                invalid("ttl exceeds the runtime clock range; use -1 for permanent")
            })?),
            Ttl::Disabled => return Ok(false),
        };
        let mut state = self.state();
        state.remove_expired(now);
        if state.values.contains_key(&fingerprint) {
            return Ok(true);
        }
        state.values.insert(fingerprint.clone(), expires);
        if let Some(expires) = expires {
            state.expirations.push(Reverse((expires, fingerprint)));
        }
        Ok(false)
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl State {
    fn remove_expired(&mut self, now: Instant) {
        while self
            .expirations
            .peek()
            .is_some_and(|Reverse((expires, _))| *expires <= now)
        {
            let Some(Reverse((expires, fingerprint))) = self.expirations.pop() else {
                break;
            };
            if self.values.get(&fingerprint) == Some(&Some(expires)) {
                self.values.remove(&fingerprint);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::time::Duration;

    use super::*;

    fn spec(ttl: serde_json::Value) -> Spec {
        Spec::new("dedup").args(serde_json::json!({
            "key": ["$request.url"],
            "ttl": ttl
        }))
    }

    fn request(task: &str, node: &str, url: &str) -> Request {
        let mut request = Request::follow(url).unwrap().node(node);
        request.task_id = task.to_string();
        request
    }

    #[tokio::test]
    async fn shares_task_node_membership_across_traces() {
        let memory = Memory::default();
        let mut first = request("task", "detail", "https://example.com/article");
        first.trace_id = "trace-a".to_string();
        let mut second = first.clone();
        second.trace_id = "trace-b".to_string();

        assert!(matches!(
            memory
                .before_scheduler(first, &spec((-1).into()))
                .await
                .unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            memory
                .before_scheduler(second, &spec((-1).into()))
                .await
                .unwrap(),
            Next::Skip
        ));
    }

    #[tokio::test]
    async fn isolates_tasks_and_nodes() {
        let memory = Memory::default();
        for request in [
            request("task-a", "detail", "https://example.com/article"),
            request("task-b", "detail", "https://example.com/article"),
            request("task-a", "page", "https://example.com/article"),
        ] {
            assert!(matches!(
                memory
                    .before_scheduler(request, &spec((-1).into()))
                    .await
                    .unwrap(),
                Next::Continue(_)
            ));
        }
    }

    #[tokio::test]
    async fn spec_key_does_not_isolate_membership() {
        let memory = Memory::default();
        let first = spec((-1).into()).key("first");
        let second = spec((-1).into()).key("second");

        assert!(matches!(
            memory
                .before_scheduler(
                    request("task", "detail", "https://example.com/article"),
                    &first,
                )
                .await
                .unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            memory
                .before_scheduler(
                    request("task", "detail", "https://example.com/article"),
                    &second,
                )
                .await
                .unwrap(),
            Next::Skip
        ));
    }

    #[tokio::test]
    async fn zero_ttl_and_dont_filter_bypass_lookup_and_validation() {
        let memory = Memory::default();
        let zero = spec(0.into());
        for _ in 0..2 {
            assert!(matches!(
                memory
                    .before_scheduler(
                        request("task", "detail", "https://example.com/article"),
                        &zero,
                    )
                    .await
                    .unwrap(),
                Next::Continue(_)
            ));
        }
        assert!(memory.state().values.is_empty());

        let mut bypass = request("", "detail", "https://example.com/article");
        bypass.dont_filter = true;
        let missing = Spec::new("dedup").args(serde_json::json!({
            "key": ["$vals.missing"]
        }));
        assert!(matches!(
            memory.before_scheduler(bypass, &missing).await.unwrap(),
            Next::Continue(_)
        ));
    }

    #[tokio::test]
    async fn finite_ttl_expires() {
        let memory = Memory::default();
        let ttl = spec(10.into());
        let value = || request("task", "detail", "https://example.com/article");

        assert!(matches!(
            memory.before_scheduler(value(), &ttl).await.unwrap(),
            Next::Continue(_)
        ));
        assert!(matches!(
            memory.before_scheduler(value(), &ttl).await.unwrap(),
            Next::Skip
        ));
        tokio::time::sleep(Duration::from_millis(15)).await;
        assert!(matches!(
            memory.before_scheduler(value(), &ttl).await.unwrap(),
            Next::Continue(_)
        ));
    }

    #[test]
    fn deadline_overflow_does_not_mutate_state() {
        if Instant::now().checked_add(Duration::MAX).is_some() {
            return;
        }
        let memory = Memory::default();
        let fingerprint = Config::from_spec(&spec((-1).into()))
            .unwrap()
            .fingerprint(&request("task", "detail", "https://example.com/article"))
            .unwrap()
            .unwrap();

        assert!(
            memory
                .contains_or_insert(fingerprint, Ttl::Finite(Duration::MAX))
                .is_err()
        );
        let state = memory.state();
        assert!(state.values.is_empty());
        assert!(state.expirations.is_empty());
    }

    #[test]
    fn concurrent_identical_requests_have_one_winner() {
        let memory = Arc::new(Memory::default());
        let barrier = Arc::new(Barrier::new(8));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let memory = memory.clone();
            let barrier = barrier.clone();
            tasks.push(std::thread::spawn(move || {
                barrier.wait();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(memory.before_scheduler(
                    request("task", "detail", "https://example.com/article"),
                    &spec((-1).into()),
                ))
            }));
        }
        let continued = tasks
            .into_iter()
            .map(|task| task.join().unwrap().unwrap())
            .filter(|result| matches!(result, Next::Continue(_)))
            .count();
        assert_eq!(continued, 1);
    }
}
