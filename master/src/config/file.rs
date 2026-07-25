use std::path::Path;

use serde::Deserialize;

use super::{Config, Policy};
use crate::Error;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct File {
    bind: String,
    database_url: String,
    namespace: String,
    worker_token: String,
    control_token: String,
    #[serde(default)]
    policy: PolicyFile,
    #[serde(default = "super::default_max_api_bytes")]
    max_api_bytes: usize,
    #[serde(default = "super::default_cron_interval_ms")]
    cron_interval_ms: u64,
    #[serde(default = "super::default_dispatch_limit")]
    dispatch_limit: usize,
    #[serde(default = "super::default_recovery_limit")]
    recovery_limit: usize,
    #[serde(default = "super::default_history_retention_ms")]
    history_retention_ms: u64,
    #[serde(default = "super::default_cleanup_limit")]
    cleanup_limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(default = "default_lease_timeout_ms")]
    lease_timeout_ms: i64,
    #[serde(default = "default_lease_interval_ms")]
    lease_interval_ms: i64,
    #[serde(default = "default_heartbeat_interval_ms")]
    heartbeat_interval_ms: i64,
}

impl Default for PolicyFile {
    fn default() -> Self {
        let policy = Policy::default();
        Self {
            lease_timeout_ms: policy.lease_timeout_ms,
            lease_interval_ms: policy.lease_interval_ms,
            heartbeat_interval_ms: policy.heartbeat_interval_ms,
        }
    }
}

impl File {
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
        let bind = file
            .bind
            .parse()
            .map_err(|error| Error::Config(format!("invalid bind address: {error}")))?;
        let config = Config {
            bind,
            database_url: file.database_url,
            namespace: file.namespace,
            worker_token: file.worker_token,
            control_token: file.control_token,
            policy: Policy {
                lease_timeout_ms: file.policy.lease_timeout_ms,
                lease_interval_ms: file.policy.lease_interval_ms,
                heartbeat_interval_ms: file.policy.heartbeat_interval_ms,
            },
            max_api_bytes: file.max_api_bytes,
            cron_interval: std::time::Duration::from_millis(file.cron_interval_ms),
            dispatch_limit: file.dispatch_limit,
            recovery_limit: file.recovery_limit,
            history_retention: std::time::Duration::from_millis(file.history_retention_ms),
            cleanup_limit: file.cleanup_limit,
        };
        config.validate()?;
        Ok(config)
    }
}

fn default_lease_timeout_ms() -> i64 {
    Policy::default().lease_timeout_ms
}

fn default_lease_interval_ms() -> i64 {
    Policy::default().lease_interval_ms
}

fn default_heartbeat_interval_ms() -> i64 {
    Policy::default().heartbeat_interval_ms
}
