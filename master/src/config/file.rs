use std::path::Path;
use std::time::Duration;

use bytesize::ByteSize;
use serde::{Deserialize, Deserializer, de};

use super::{Api as RuntimeApi, Config, History as RuntimeHistory, Policy as RuntimePolicy};
use crate::Error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    bind: String,
    database_url: String,
    namespace: String,
    worker_token: String,
    control_token: String,
    #[serde(default)]
    policy: Policy,
    #[serde(default)]
    api: Api,
    #[serde(default = "super::default_cron_interval_ms")]
    cron_interval_ms: u64,
    #[serde(default = "super::default_dispatch_limit")]
    dispatch_limit: usize,
    #[serde(default = "super::default_recovery_limit")]
    recovery_limit: usize,
    #[serde(default)]
    history: History,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Api {
    #[serde(default = "default_api_size", deserialize_with = "deserialize_size")]
    max_size: ByteSize,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            max_size: default_api_size(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct History {
    #[serde(default = "default_history_ttl", with = "humantime_serde")]
    ttl: Duration,
    #[serde(default = "default_cleanup_limit")]
    cleanup_limit: usize,
}

impl Default for History {
    fn default() -> Self {
        Self {
            ttl: default_history_ttl(),
            cleanup_limit: default_cleanup_limit(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Policy {
    #[serde(default = "default_lease_timeout_ms")]
    lease_timeout_ms: i64,
    #[serde(default = "default_lease_interval_ms")]
    lease_interval_ms: i64,
    #[serde(default = "default_heartbeat_interval_ms")]
    heartbeat_interval_ms: i64,
}

impl Default for Policy {
    fn default() -> Self {
        let policy = RuntimePolicy::default();
        Self {
            lease_timeout_ms: policy.lease_timeout_ms,
            lease_interval_ms: policy.lease_interval_ms,
            heartbeat_interval_ms: policy.heartbeat_interval_ms,
        }
    }
}

impl Source {
    pub(super) fn read(path: impl AsRef<Path>) -> Result<Config, Error> {
        let path = path.as_ref();
        let source = std::fs::read_to_string(path).map_err(|error| {
            Error::Config(format!(
                "cannot read Master config {}: {error}",
                path.display()
            ))
        })?;
        let file: Self = serde_yaml::from_str(&source).map_err(|error| {
            Error::Config(format!(
                "cannot parse Master config {}: {error}",
                path.display()
            ))
        })?;
        file.into_config()
    }

    fn into_config(self) -> Result<Config, Error> {
        let bind = self
            .bind
            .parse()
            .map_err(|error| Error::Config(format!("invalid bind address: {error}")))?;
        let max_size = usize::try_from(self.api.max_size.as_u64()).map_err(|_| {
            Error::Config("API max size exceeds the platform size range".to_string())
        })?;
        let config = Config {
            bind,
            database_url: self.database_url,
            namespace: self.namespace,
            worker_token: self.worker_token,
            control_token: self.control_token,
            policy: RuntimePolicy {
                lease_timeout_ms: self.policy.lease_timeout_ms,
                lease_interval_ms: self.policy.lease_interval_ms,
                heartbeat_interval_ms: self.policy.heartbeat_interval_ms,
            },
            api: RuntimeApi { max_size },
            cron_interval: Duration::from_millis(self.cron_interval_ms),
            dispatch_limit: self.dispatch_limit,
            recovery_limit: self.recovery_limit,
            history: RuntimeHistory {
                ttl: self.history.ttl,
                cleanup_limit: self.history.cleanup_limit,
            },
        };
        config.validate()?;
        Ok(config)
    }
}

fn default_api_size() -> ByteSize {
    ByteSize::b(RuntimeApi::default().max_size as u64)
}

fn deserialize_size<'de, D>(deserializer: D) -> Result<ByteSize, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_yaml::Value::deserialize(deserializer)?;
    let serde_yaml::Value::String(value) = value else {
        return Err(de::Error::custom(
            "API max size must be a string with an explicit unit, such as \"64MiB\"",
        ));
    };
    if !value.chars().any(|value| value.is_ascii_alphabetic()) {
        return Err(de::Error::custom(
            "API max size must include an explicit unit, such as \"64MiB\"",
        ));
    }
    value.parse().map_err(de::Error::custom)
}

fn default_history_ttl() -> Duration {
    RuntimeHistory::default().ttl
}

fn default_cleanup_limit() -> usize {
    RuntimeHistory::default().cleanup_limit
}

fn default_lease_timeout_ms() -> i64 {
    RuntimePolicy::default().lease_timeout_ms
}

fn default_lease_interval_ms() -> i64 {
    RuntimePolicy::default().lease_interval_ms
}

fn default_heartbeat_interval_ms() -> i64 {
    RuntimePolicy::default().heartbeat_interval_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
bind: "127.0.0.1:8080"
database_url: "mysql://crawler"
namespace: "crawler"
worker_token: "worker-secret"
control_token: "control-secret"
"#;

    #[test]
    fn reads_nested_human_sizes_and_durations() {
        let source = format!(
            "{BASE}\napi:\n  max_size: \"64MiB\"\nhistory:\n  ttl: \"48h\"\n  cleanup_limit: 42\n"
        );
        let config = serde_yaml::from_str::<Source>(&source)
            .unwrap()
            .into_config()
            .unwrap();

        assert_eq!(config.api().max_size, 64 * 1024 * 1024);
        assert_eq!(config.history().ttl, Duration::from_secs(48 * 60 * 60));
        assert_eq!(config.history().cleanup_limit, 42);
    }

    #[test]
    fn defaults_nested_sections_as_a_unit() {
        let config = serde_yaml::from_str::<Source>(BASE)
            .unwrap()
            .into_config()
            .unwrap();
        let api = RuntimeApi::default();
        let history = RuntimeHistory::default();

        assert_eq!(config.api().max_size, api.max_size);
        assert_eq!(config.history().ttl, history.ttl);
        assert_eq!(config.history().cleanup_limit, history.cleanup_limit);
    }

    #[test]
    fn rejects_old_flat_and_unknown_nested_fields() {
        let old = format!("{BASE}\nmax_api_bytes: 67108864\n");
        assert!(serde_yaml::from_str::<Source>(&old).is_err());

        let unknown = format!("{BASE}\napi:\n  max_size: \"64MiB\"\n  bytes: 1\n");
        assert!(serde_yaml::from_str::<Source>(&unknown).is_err());
    }

    #[test]
    fn rejects_invalid_and_out_of_range_values() {
        let invalid_size = format!("{BASE}\napi:\n  max_size: \"64XB\"\n");
        assert!(serde_yaml::from_str::<Source>(&invalid_size).is_err());

        let ambiguous_size = format!("{BASE}\napi:\n  max_size: 67108864\n");
        assert!(serde_yaml::from_str::<Source>(&ambiguous_size).is_err());

        let unitless_size = format!("{BASE}\napi:\n  max_size: \"67108864\"\n");
        assert!(serde_yaml::from_str::<Source>(&unitless_size).is_err());

        let too_small = format!("{BASE}\napi:\n  max_size: \"1023B\"\n");
        assert!(
            serde_yaml::from_str::<Source>(&too_small)
                .unwrap()
                .into_config()
                .is_err()
        );

        let invalid_ttl = format!("{BASE}\nhistory:\n  ttl: \"forever\"\n");
        assert!(serde_yaml::from_str::<Source>(&invalid_ttl).is_err());

        let overflowing_ttl = format!("{BASE}\nhistory:\n  ttl: \"9223372036854775808ms\"\n");
        assert!(serde_yaml::from_str::<Source>(&overflowing_ttl).is_err());
    }
}
