use std::time::{Duration, Instant};

use crate::middleware::{Middleware, Spec};

#[derive(Default)]
pub struct Retry;

impl Middleware for Retry {
    fn order(&self, _hook: &str) -> i32 {
        100
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Policy {
    schedules: Vec<Schedule>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Schedule {
    count: usize,
    backoff: Vec<Duration>,
}

impl Policy {
    pub(crate) fn from_spec(spec: &Spec) -> Result<Self, crate::middleware::Error> {
        let count = match spec.args.get("count") {
            Some(value) => value
                .as_u64()
                .ok_or_else(|| invalid_config("count must be a non-negative integer"))?,
            None => 0,
        };
        let count = usize::try_from(count).map_err(|_| invalid_config("count is too large"))?;
        let backoff = match spec.args.get("backoff") {
            Some(value) => {
                let values = value
                    .as_array()
                    .ok_or_else(|| invalid_config("backoff must be an array"))?;
                values
                    .iter()
                    .map(|value| {
                        value.as_u64().ok_or_else(|| {
                            invalid_config("backoff must contain non-negative milliseconds")
                        })
                    })
                    .map(|value| value.map(Duration::from_millis))
                    .collect::<Result<Vec<_>, _>>()
            }
            None => Ok(Vec::new()),
        }?;
        let now = Instant::now();
        if backoff
            .iter()
            .any(|delay| now.checked_add(*delay).is_none())
        {
            return Err(invalid_config("backoff exceeds the runtime clock range"));
        }

        Ok(Self {
            schedules: vec![Schedule { count, backoff }],
        })
    }

    pub(crate) fn delay(&self, mut attempt: usize) -> Option<Duration> {
        for schedule in &self.schedules {
            if attempt < schedule.count {
                return Some(
                    schedule
                        .backoff
                        .get(attempt)
                        .or_else(|| schedule.backoff.last())
                        .copied()
                        .unwrap_or_default(),
                );
            }
            attempt -= schedule.count;
        }
        None
    }

    pub(crate) fn extend(&mut self, other: Self) {
        self.schedules.extend(other.schedules);
    }
}

pub(super) fn check(spec: &Spec) -> Result<(), crate::middleware::Error> {
    if spec
        .hook
        .as_deref()
        .is_some_and(|hook| !matches!(hook, "error_download" | "error_parse" | "error_item"))
    {
        return Err(invalid_config(
            "hook must be error_download, error_parse, or error_item",
        ));
    }
    if spec.skip {
        return Ok(());
    }
    if spec
        .args
        .as_object()
        .into_iter()
        .flatten()
        .any(|(name, _)| !matches!(name.as_str(), "count" | "backoff"))
    {
        return Err(invalid_config("only count and backoff are supported"));
    }
    Policy::from_spec(spec).map(|_| ())
}

fn invalid_config(message: &str) -> crate::middleware::Error {
    crate::middleware::Error::InvalidConfig {
        name: "retry".to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeats_last_backoff_value() {
        let spec = Spec::new("retry").args(serde_json::json!({"count": 3, "backoff": [10, 20]}));
        let policy = Policy::from_spec(&spec).unwrap();

        assert_eq!(policy.delay(0), Some(Duration::from_millis(10)));
        assert_eq!(policy.delay(1), Some(Duration::from_millis(20)));
        assert_eq!(policy.delay(2), Some(Duration::from_millis(20)));
        assert_eq!(policy.delay(3), None);
    }

    #[test]
    fn large_count_does_not_allocate_per_attempt() {
        let count = 1_000_000_000_000_u64;
        let spec = Spec::new("retry").args(serde_json::json!({
            "count": count,
            "backoff": [7]
        }));
        let policy = Policy::from_spec(&spec).unwrap();

        assert_eq!(policy.schedules.len(), 1);
        assert_eq!(
            policy.delay(999_999_999_999),
            Some(Duration::from_millis(7))
        );
        assert_eq!(policy.delay(1_000_000_000_000), None);
    }

    #[test]
    fn rejects_non_array_backoff() {
        let spec = Spec::new("retry").args(serde_json::json!({
            "count": 1,
            "backoff": 100
        }));

        assert!(Policy::from_spec(&spec).is_err());
        assert!(check(&spec).is_err());
    }

    #[test]
    fn checks_backoff_against_the_runtime_clock_range() {
        let delay = Duration::from_millis(u64::MAX);
        let overflows = Instant::now().checked_add(delay).is_none();
        let spec = Spec::new("retry").args(serde_json::json!({
            "count": 1,
            "backoff": [u64::MAX]
        }));

        let result = Policy::from_spec(&spec);

        assert_eq!(result.is_err(), overflows);
    }
}
