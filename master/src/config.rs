use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use axum::http::HeaderValue;

use crate::Error;

mod file;

const DEFAULT_MAX_API_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CRON_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_DISPATCH_LIMIT: usize = 64;
const DEFAULT_RECOVERY_LIMIT: usize = 128;
const DEFAULT_HISTORY_RETENTION: Duration = Duration::from_secs(2 * 24 * 60 * 60);
const DEFAULT_CLEANUP_LIMIT: usize = 1_000;
const MIN_OPERATION_RETENTION: Duration = Duration::from_secs(5 * 60 + 30);
const MAX_API_BYTES: usize = u32::MAX as usize;

const fn default_max_api_bytes() -> usize {
    DEFAULT_MAX_API_BYTES
}

const fn default_cron_interval_ms() -> u64 {
    DEFAULT_CRON_INTERVAL.as_millis() as u64
}

const fn default_dispatch_limit() -> usize {
    DEFAULT_DISPATCH_LIMIT
}

const fn default_recovery_limit() -> usize {
    DEFAULT_RECOVERY_LIMIT
}

const fn default_history_retention_ms() -> u64 {
    DEFAULT_HISTORY_RETENTION.as_millis() as u64
}

const fn default_cleanup_limit() -> usize {
    DEFAULT_CLEANUP_LIMIT
}

#[derive(Clone, Debug)]
pub struct Policy {
    pub lease_timeout_ms: i64,
    pub lease_interval_ms: i64,
    pub heartbeat_interval_ms: i64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            lease_timeout_ms: 30_000,
            lease_interval_ms: 10_000,
            heartbeat_interval_ms: 10_000,
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.lease_timeout_ms <= 0
            || self.lease_interval_ms <= 0
            || self.heartbeat_interval_ms <= 0
        {
            return Err(Error::Config(
                "lease and heartbeat intervals must be positive".to_string(),
            ));
        }
        if self.lease_interval_ms >= self.lease_timeout_ms {
            return Err(Error::Config(
                "lease interval must be shorter than lease timeout".to_string(),
            ));
        }
        if self.heartbeat_interval_ms >= self.lease_timeout_ms {
            return Err(Error::Config(
                "heartbeat interval must be shorter than lease timeout".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Config {
    bind: SocketAddr,
    database_url: String,
    namespace: String,
    worker_token: String,
    control_token: String,
    policy: Policy,
    max_api_bytes: usize,
    cron_interval: Duration,
    dispatch_limit: usize,
    recovery_limit: usize,
    history_retention: Duration,
    cleanup_limit: usize,
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("bind", &self.bind)
            .field("has_database_url", &!self.database_url.is_empty())
            .field("namespace", &self.namespace)
            .field("has_worker_token", &!self.worker_token.is_empty())
            .field("has_control_token", &!self.control_token.is_empty())
            .field("policy", &self.policy)
            .field("max_api_bytes", &self.max_api_bytes)
            .field("cron_interval", &self.cron_interval)
            .field("dispatch_limit", &self.dispatch_limit)
            .field("recovery_limit", &self.recovery_limit)
            .field("history_retention", &self.history_retention)
            .field("cleanup_limit", &self.cleanup_limit)
            .finish()
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        file::File::read(path)
    }

    pub fn new(
        bind: SocketAddr,
        database_url: impl Into<String>,
        namespace: impl Into<String>,
        worker_token: impl Into<String>,
        control_token: impl Into<String>,
    ) -> Result<Self, Error> {
        let config = Self {
            bind,
            database_url: database_url.into(),
            namespace: namespace.into(),
            worker_token: worker_token.into(),
            control_token: control_token.into(),
            policy: Policy::default(),
            max_api_bytes: DEFAULT_MAX_API_BYTES,
            cron_interval: DEFAULT_CRON_INTERVAL,
            dispatch_limit: DEFAULT_DISPATCH_LIMIT,
            recovery_limit: DEFAULT_RECOVERY_LIMIT,
            history_retention: DEFAULT_HISTORY_RETENTION,
            cleanup_limit: DEFAULT_CLEANUP_LIMIT,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_policy(mut self, policy: Policy) -> Result<Self, Error> {
        self.policy = policy;
        self.validate()?;
        Ok(self)
    }

    pub fn with_max_api_bytes(mut self, max_api_bytes: usize) -> Result<Self, Error> {
        self.max_api_bytes = max_api_bytes;
        self.validate()?;
        Ok(self)
    }

    pub fn with_cron_interval(mut self, cron_interval: Duration) -> Result<Self, Error> {
        self.cron_interval = cron_interval;
        self.validate()?;
        Ok(self)
    }

    pub fn with_dispatch_limit(mut self, dispatch_limit: usize) -> Result<Self, Error> {
        self.dispatch_limit = dispatch_limit;
        self.validate()?;
        Ok(self)
    }

    pub fn with_recovery_limit(mut self, recovery_limit: usize) -> Result<Self, Error> {
        self.recovery_limit = recovery_limit;
        self.validate()?;
        Ok(self)
    }

    pub fn with_history_retention(mut self, history_retention: Duration) -> Result<Self, Error> {
        self.history_retention = history_retention;
        self.validate()?;
        Ok(self)
    }

    pub fn with_cleanup_limit(mut self, cleanup_limit: usize) -> Result<Self, Error> {
        self.cleanup_limit = cleanup_limit;
        self.validate()?;
        Ok(self)
    }

    pub fn bind(&self) -> SocketAddr {
        self.bind
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub(crate) fn worker_token(&self) -> &str {
        &self.worker_token
    }

    pub(crate) fn control_token(&self) -> &str {
        &self.control_token
    }

    pub fn policy(&self) -> &Policy {
        &self.policy
    }

    pub fn max_api_bytes(&self) -> usize {
        self.max_api_bytes
    }

    pub fn cron_interval(&self) -> Duration {
        self.cron_interval
    }

    pub fn dispatch_limit(&self) -> usize {
        self.dispatch_limit
    }

    pub fn recovery_limit(&self) -> usize {
        self.recovery_limit
    }

    pub fn history_retention(&self) -> Duration {
        self.history_retention
    }

    pub fn cleanup_limit(&self) -> usize {
        self.cleanup_limit
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.namespace.trim().is_empty()
            || self.namespace.len() > 128
            || self
                .namespace
                .chars()
                .any(|value| value.is_control() || value.is_whitespace())
            || !header_value(&self.namespace)
        {
            return Err(Error::Config(
                "namespace must be a valid HTTP header value of at most 128 bytes without whitespace or control characters"
                    .to_string(),
            ));
        }
        validate_token(&self.worker_token, "worker")?;
        validate_token(&self.control_token, "control")?;
        if self.worker_token == self.control_token {
            return Err(Error::Config(
                "worker and control tokens must be different".to_string(),
            ));
        }
        if self.database_url.trim().is_empty() {
            return Err(Error::Config("database URL must not be empty".to_string()));
        }
        if self.max_api_bytes == 0 || self.max_api_bytes > MAX_API_BYTES {
            return Err(Error::Config(format!(
                "maximum API message size must be between 1 and {MAX_API_BYTES} bytes"
            )));
        }
        if self.cron_interval.is_zero() {
            return Err(Error::Config("Cron interval must be positive".to_string()));
        }
        if self.dispatch_limit == 0 {
            return Err(Error::Config("dispatch limit must be positive".to_string()));
        }
        if self.recovery_limit == 0 {
            return Err(Error::Config("recovery limit must be positive".to_string()));
        }
        if self.history_retention.is_zero() {
            return Err(Error::Config(
                "history retention must be positive".to_string(),
            ));
        }
        if self.cleanup_limit == 0 {
            return Err(Error::Config("cleanup limit must be positive".to_string()));
        }
        self.policy.validate()?;
        let lease_timeout = Duration::from_millis(self.policy.lease_timeout_ms as u64);
        if self.history_retention < lease_timeout.max(MIN_OPERATION_RETENTION) {
            return Err(Error::Config(
                "history retention must cover the lease timeout, five-minute idempotency window, and transport margin"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

fn header_value(value: &str) -> bool {
    HeaderValue::from_str(value).is_ok_and(|value| value.to_str().is_ok())
}

fn validate_token(token: &str, name: &str) -> Result<(), Error> {
    if token.trim().is_empty() {
        return Err(Error::Config(format!(
            "{name} token must be supplied explicitly; no default token is provided"
        )));
    }
    if !header_value(&format!("Bearer {token}")) {
        return Err(Error::Config(format!(
            "{name} token cannot be represented in an Authorization header"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config::new(
            "127.0.0.1:0".parse().unwrap(),
            "mysql://crawler",
            "crawler",
            "worker-secret",
            "control-secret",
        )
        .unwrap()
    }

    #[test]
    fn defaults_are_bounded_and_valid() {
        let config = config();

        assert_eq!(config.max_api_bytes(), DEFAULT_MAX_API_BYTES);
        assert_eq!(config.cron_interval(), DEFAULT_CRON_INTERVAL);
        assert_eq!(config.dispatch_limit(), DEFAULT_DISPATCH_LIMIT);
        assert_eq!(config.recovery_limit(), DEFAULT_RECOVERY_LIMIT);
        assert_eq!(config.history_retention(), DEFAULT_HISTORY_RETENTION);
        assert_eq!(config.cleanup_limit(), DEFAULT_CLEANUP_LIMIT);
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_runtime_bounds() {
        assert!(config().with_max_api_bytes(0).is_err());
        if let Some(too_large) = MAX_API_BYTES.checked_add(1) {
            assert!(config().with_max_api_bytes(too_large).is_err());
        }
        assert!(config().with_cron_interval(Duration::ZERO).is_err());
        assert!(config().with_dispatch_limit(0).is_err());
        assert!(config().with_recovery_limit(0).is_err());
        assert!(config().with_history_retention(Duration::ZERO).is_err());
        assert!(
            config()
                .with_history_retention(MIN_OPERATION_RETENTION - Duration::from_millis(1))
                .is_err()
        );
        assert!(
            config()
                .with_history_retention(MIN_OPERATION_RETENTION)
                .is_ok()
        );
        assert!(config().with_cleanup_limit(0).is_err());
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "bad namespace",
                "worker-secret",
                "control-secret",
            )
            .is_err()
        );
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "crawler",
                "令牌",
                "control",
            )
            .is_err()
        );
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "爬虫",
                "worker-secret",
                "control-secret",
            )
            .is_err()
        );
    }

    #[test]
    fn policy_rejects_heartbeat_at_the_lease_deadline() {
        let policy = Policy {
            lease_timeout_ms: 30_000,
            lease_interval_ms: 10_000,
            heartbeat_interval_ms: 30_000,
        };

        assert!(policy.validate().is_err());
    }

    #[test]
    fn policy_updates_validate_the_entire_configuration() {
        let config = config()
            .with_history_retention(MIN_OPERATION_RETENTION)
            .unwrap();
        assert!(
            config
                .with_policy(Policy {
                    lease_timeout_ms: MIN_OPERATION_RETENTION.as_millis() as i64 + 1,
                    lease_interval_ms: 1_000,
                    heartbeat_interval_ms: 1_000,
                })
                .is_err()
        );
    }

    #[test]
    fn credentials_must_be_distinct_valid_header_values() {
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "crawler",
                "shared",
                "shared",
            )
            .is_err()
        );
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "crawler",
                "worker\ninvalid",
                "control",
            )
            .is_err()
        );
        assert!(
            Config::new(
                "127.0.0.1:0".parse().unwrap(),
                "mysql://crawler",
                "crawler",
                "worker",
                "control\rinvalid",
            )
            .is_err()
        );
    }

    #[test]
    fn debug_redacts_credentials() {
        let debug = format!("{:?}", config());

        assert!(!debug.contains("worker-secret"));
        assert!(!debug.contains("control-secret"));
        assert!(!debug.contains("mysql://crawler"));
        assert!(debug.contains("has_worker_token: true"));
        assert!(debug.contains("has_control_token: true"));
    }

    #[test]
    fn file_configuration_loads_the_checked_in_runtime_shape() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("etc/master-api.yaml");
        let config = Config::from_file(path).unwrap();

        assert_eq!(config.bind(), "127.0.0.1:8080".parse().unwrap());
        assert_eq!(config.namespace(), "crawler");
        assert_eq!(config.max_api_bytes(), DEFAULT_MAX_API_BYTES);
        assert_eq!(config.dispatch_limit(), DEFAULT_DISPATCH_LIMIT);
        assert_eq!(config.recovery_limit(), DEFAULT_RECOVERY_LIMIT);
        config.validate().unwrap();
    }
}
