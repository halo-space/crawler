use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::Mutex as AsyncMutex;

use crate::middleware::{BoxFuture, Middleware, Next, Spec};
use crate::net::Request;

#[derive(Default)]
pub struct RateLimit {
    groups: Mutex<HashMap<String, Arc<AsyncMutex<Instant>>>>,
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
            let slot = self.group(&group);
            let mut next = slot.lock().await;
            let now = Instant::now();
            if *next > now {
                tokio::time::sleep(*next - now).await;
            }
            *next = Instant::now() + interval;

            Ok(Next::Continue(request))
        })
    }
}

impl RateLimit {
    fn group(&self, group: &str) -> Arc<AsyncMutex<Instant>> {
        self.groups()
            .entry(group.to_string())
            .or_insert_with(|| Arc::new(AsyncMutex::new(Instant::now())))
            .clone()
    }

    fn groups(&self) -> MutexGuard<'_, HashMap<String, Arc<AsyncMutex<Instant>>>> {
        self.groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| hook != "before_download")
    {
        return Err(invalid_config("hook must be before_download"));
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

    #[test]
    fn rejects_qps_outside_duration_range_without_panicking() {
        let too_small = Spec::new("rate_limit").args(serde_json::json!({"qps": f64::MIN_POSITIVE}));
        let too_large = Spec::new("rate_limit").args(serde_json::json!({"qps": f64::MAX}));

        assert!(check(&too_small).is_err());
        assert!(check(&too_large).is_err());
    }
}
