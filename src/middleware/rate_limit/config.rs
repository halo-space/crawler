use std::time::{Duration, Instant};

use crate::middleware::Spec;

use super::invalid;

#[derive(Clone, Debug)]
pub struct Config {
    group: Option<String>,
    interval: Duration,
}

impl Config {
    pub fn from_spec(spec: &Spec) -> Result<Self, crate::middleware::Error> {
        if spec
            .hook
            .as_deref()
            .is_some_and(|hook| hook != "before_download")
        {
            return Err(invalid("hook must be before_download"));
        }
        let args = spec
            .args
            .as_object()
            .ok_or_else(|| invalid("args must be an object"))?;
        if args
            .keys()
            .any(|name| !matches!(name.as_str(), "qps" | "group"))
        {
            return Err(invalid("only qps and group are supported"));
        }
        let qps = args
            .get("qps")
            .and_then(serde_json::Value::as_f64)
            .filter(|qps| qps.is_finite() && *qps > 0.0)
            .ok_or_else(|| invalid("qps must be greater than zero"))?;
        let interval = Duration::try_from_secs_f64(1.0 / qps)
            .map_err(|_| invalid("qps is outside the supported duration range"))?;
        if interval.is_zero() {
            return Err(invalid("qps exceeds the supported timer precision"));
        }
        if Instant::now().checked_add(interval).is_none() {
            return Err(invalid("qps exceeds the runtime clock range"));
        }
        let group = args
            .get("group")
            .map(|value| {
                value
                    .as_str()
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| invalid("group must be a non-empty string"))
            })
            .transpose()?;
        Ok(Self { group, interval })
    }

    pub fn group(&self, request: &crate::net::Request) -> Result<String, crate::middleware::Error> {
        self.group
            .clone()
            .or_else(|| {
                url::Url::parse(&request.url)
                    .ok()?
                    .host_str()
                    .map(ToOwned::to_owned)
            })
            .ok_or_else(|| invalid("group is missing and URL has no host"))
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_group_and_interval() {
        let config = Config::from_spec(&Spec::new("rate_limit").args(serde_json::json!({
            "group": "api",
            "qps": 2.0
        })))
        .unwrap();
        assert_eq!(config.interval(), Duration::from_millis(500));
        assert_eq!(
            config
                .group(&crate::net::Request::follow("https://example.com").unwrap())
                .unwrap(),
            "api"
        );
    }

    #[test]
    fn falls_back_to_url_host() {
        let config =
            Config::from_spec(&Spec::new("rate_limit").args(serde_json::json!({"qps": 1.0})))
                .unwrap();
        assert_eq!(
            config
                .group(&crate::net::Request::follow("https://EXAMPLE.com/path").unwrap())
                .unwrap(),
            "example.com"
        );
    }
}
