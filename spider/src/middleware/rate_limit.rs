use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::Request;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub struct RateLimit {
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

impl Default for RateLimit {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::new(Instant::now())),
        }
    }
}

impl Middleware for RateLimit {
    fn order(&self, _hook: &str) -> i32 {
        200
    }

    fn before_download<'a>(
        &'a self,
        request: Request,
        spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Request>> {
        Box::pin(async move {
            let interval = interval(spec)?;
            let group = spec
                .args
                .get("group")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
                .or_else(|| request_host(&request))
                .ok_or_else(|| invalid_config("group is missing and URL has no host"))?;
            let slot = self.group(&group, interval)?;
            let mut next = slot.next.lock().await;
            let now = Instant::now();
            if *next > now {
                tokio::time::sleep(*next - now).await;
            }
            *next = Instant::now()
                .checked_add(slot.interval)
                .ok_or_else(|| invalid_config("qps exceeds the runtime clock range"))?;

            Ok(Next::Continue(request))
        })
    }
}

impl RateLimit {
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
                return Err(invalid_config(
                    "group is already active with a different qps",
                ));
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

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| hook != "before_download")
    {
        return Err(invalid_config("hook must be before_download"));
    }
    if spec.skip {
        return Ok(());
    }
    let Some(args) = spec.args.as_object() else {
        return Err(invalid_config("args must be an object"));
    };
    if args
        .keys()
        .any(|name| !matches!(name.as_str(), "qps" | "group"))
    {
        return Err(invalid_config("only qps and group are supported"));
    }
    interval(spec)?;
    if let Some(group) = args.get("group")
        && group.as_str().is_none_or(str::is_empty)
    {
        return Err(invalid_config("group must be a non-empty string"));
    }
    Ok(())
}

fn interval(spec: &Spec) -> Result<Duration, crate::middleware::Error> {
    let qps = spec
        .args
        .get("qps")
        .and_then(serde_json::Value::as_f64)
        .filter(|qps| qps.is_finite() && *qps > 0.0)
        .ok_or_else(|| invalid_config("qps must be greater than zero"))?;
    let interval = Duration::try_from_secs_f64(1.0 / qps)
        .map_err(|_| invalid_config("qps is outside the supported duration range"))?;
    if interval.is_zero() {
        return Err(invalid_config("qps exceeds the supported timer precision"));
    }
    if Instant::now().checked_add(interval).is_none() {
        return Err(invalid_config("qps exceeds the runtime clock range"));
    }
    Ok(interval)
}

fn request_host(request: &Request) -> Option<String> {
    url::Url::parse(&request.url)
        .ok()?
        .host_str()
        .map(ToOwned::to_owned)
}

fn invalid_config(message: &str) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "rate_limit".to_string(),
        message: message.to_string(),
    }
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
        let limiter = RateLimit::default();
        let spec = Spec::new("rate_limit").args(serde_json::json!({"group": "api", "qps": 100.0}));
        let first = Request::follow("https://example.com/1").unwrap();
        let second = Request::follow("https://example.com/2").unwrap();
        let started = Instant::now();

        let (first, second) = tokio::join!(
            limiter.before_download(first, &spec),
            limiter.before_download(second, &spec)
        );
        first.unwrap();
        second.unwrap();

        assert!(started.elapsed() >= Duration::from_millis(8));
    }

    #[tokio::test]
    async fn rejects_conflicting_qps_without_waiting_or_changing_the_schedule() {
        let limiter = RateLimit::default();
        let first_spec =
            Spec::new("rate_limit").args(serde_json::json!({"group": "api", "qps": 1.0}));
        limiter
            .before_download(
                Request::follow("https://example.com/1").unwrap(),
                &first_spec,
            )
            .await
            .unwrap();

        let slot = Arc::clone(limiter.state().groups.get("api").unwrap());
        let scheduled = *slot.next.lock().await;
        let conflicting_spec =
            Spec::new("rate_limit").args(serde_json::json!({"group": "api", "qps": 2.0}));
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            limiter.before_download(
                Request::follow("https://example.com/2").unwrap(),
                &conflicting_spec,
            ),
        )
        .await
        .expect("conflicting configuration must not wait for the group schedule");

        assert!(matches!(
            result,
            Err(crate::middleware::Error::InvalidConfig { name, message })
                if name == "rate_limit" && message.contains("different qps")
        ));
        assert_eq!(*slot.next.lock().await, scheduled);
    }

    #[test]
    fn replaces_an_inactive_elapsed_group_with_a_new_interval() {
        let limiter = RateLimit::default();
        let now = Instant::now();
        let old_interval = Duration::from_secs(1);
        let new_interval = Duration::from_millis(500);
        limiter.state().groups.insert(
            "api".to_string(),
            slot(
                old_interval,
                now.checked_sub(Duration::from_secs(1)).unwrap_or(now),
            ),
        );

        let current = limiter.group("api", new_interval).unwrap();

        assert_eq!(current.interval, new_interval);
        assert_eq!(limiter.state().groups.len(), 1);
    }

    #[test]
    fn cleanup_keeps_delayed_and_held_groups() {
        let limiter = RateLimit::default();
        let now = Instant::now();
        let elapsed = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        let delayed = now.checked_add(Duration::from_secs(1)).unwrap();
        let held = slot(Duration::from_millis(10), elapsed);
        {
            let mut state = limiter.state();
            state.groups.insert(
                "elapsed".to_string(),
                slot(Duration::from_millis(10), elapsed),
            );
            state.groups.insert(
                "delayed".to_string(),
                slot(Duration::from_millis(10), delayed),
            );
            state.groups.insert("held".to_string(), Arc::clone(&held));
            state.next_cleanup = elapsed;
        }

        limiter.group("current", Duration::from_millis(10)).unwrap();

        let state = limiter.state();
        assert!(!state.groups.contains_key("elapsed"));
        assert!(state.groups.contains_key("delayed"));
        assert!(state.groups.contains_key("held"));
    }

    #[test]
    fn full_cleanup_is_throttled_between_deadlines() {
        let limiter = RateLimit::default();
        let now = Instant::now();
        let elapsed = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        {
            let mut state = limiter.state();
            state.groups.insert(
                "stale".to_string(),
                slot(Duration::from_millis(10), elapsed),
            );
            state.next_cleanup = cleanup_deadline(now);
        }

        limiter.group("first", Duration::from_millis(10)).unwrap();
        assert!(limiter.state().groups.contains_key("stale"));

        limiter.state().next_cleanup = elapsed;
        limiter.group("second", Duration::from_millis(10)).unwrap();
        assert!(!limiter.state().groups.contains_key("stale"));
    }

    #[test]
    fn rejects_qps_outside_duration_range_without_panicking() {
        let too_small = Spec::new("rate_limit").args(serde_json::json!({"qps": f64::MIN_POSITIVE}));
        let too_large = Spec::new("rate_limit").args(serde_json::json!({"qps": f64::MAX}));

        assert!(check(&too_small).is_err());
        assert!(check(&too_large).is_err());
    }

    #[test]
    fn rejects_qps_outside_the_runtime_clock_range() {
        let qps = 1e-19;
        let duration = Duration::try_from_secs_f64(1.0 / qps).unwrap();
        if Instant::now().checked_add(duration).is_none() {
            let spec = Spec::new("rate_limit").args(serde_json::json!({"qps": qps}));
            assert!(check(&spec).is_err());
        }
    }
}
