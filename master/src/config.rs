use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use axum::http::HeaderValue;

use crate::Error;

mod api;
mod file;
mod history;
mod policy;

pub use api::Api;
pub use history::History;
pub use policy::Policy;

const DEFAULT_CRON_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_DISPATCH_LIMIT: usize = 64;
const DEFAULT_RECOVERY_LIMIT: usize = 128;
const MIN_HISTORY_TTL: Duration = Duration::from_secs(5 * 60 + 30);

const fn default_cron_interval_ms() -> u64 {
    DEFAULT_CRON_INTERVAL.as_millis() as u64
}

const fn default_dispatch_limit() -> usize {
    DEFAULT_DISPATCH_LIMIT
}

const fn default_recovery_limit() -> usize {
    DEFAULT_RECOVERY_LIMIT
}

#[derive(Clone)]
pub struct Config {
    bind: SocketAddr,
    database_url: String,
    namespace: String,
    worker_token: String,
    control_token: String,
    policy: Policy,
    api: Api,
    cron_interval: Duration,
    dispatch_limit: usize,
    recovery_limit: usize,
    history: History,
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
            .field("api", &self.api)
            .field("cron_interval", &self.cron_interval)
            .field("dispatch_limit", &self.dispatch_limit)
            .field("recovery_limit", &self.recovery_limit)
            .field("history", &self.history)
            .finish()
    }
}

impl Config {
    pub fn from_file(path: impl AsRef<std::path::Path>) -> Result<Self, Error> {
        file::Source::read(path)
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
            api: Api::default(),
            cron_interval: DEFAULT_CRON_INTERVAL,
            dispatch_limit: DEFAULT_DISPATCH_LIMIT,
            recovery_limit: DEFAULT_RECOVERY_LIMIT,
            history: History::default(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn with_policy(mut self, policy: Policy) -> Result<Self, Error> {
        self.policy = policy;
        self.validate()?;
        Ok(self)
    }

    pub fn with_api(mut self, api: Api) -> Result<Self, Error> {
        self.api = api;
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

    pub fn with_history(mut self, history: History) -> Result<Self, Error> {
        self.history = history;
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

    pub fn api(&self) -> &Api {
        &self.api
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

    pub fn history(&self) -> &History {
        &self.history
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
        self.api.validate()?;
        if self.cron_interval.is_zero() {
            return Err(Error::Config("Cron interval must be positive".to_string()));
        }
        if self.dispatch_limit == 0 {
            return Err(Error::Config("dispatch limit must be positive".to_string()));
        }
        if self.recovery_limit == 0 {
            return Err(Error::Config("recovery limit must be positive".to_string()));
        }
        self.history.validate()?;
        self.policy.validate()?;
        let lease_timeout = Duration::from_millis(self.policy.lease_timeout_ms as u64);
        if self.history.ttl < lease_timeout.max(MIN_HISTORY_TTL) {
            return Err(Error::Config(
                "history TTL must cover the lease timeout, five-minute idempotency window, and transport margin"
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
        let api = Api::default();
        let history = History::default();

        assert_eq!(config.api().max_size, api.max_size);
        assert_eq!(config.cron_interval(), DEFAULT_CRON_INTERVAL);
        assert_eq!(config.dispatch_limit(), DEFAULT_DISPATCH_LIMIT);
        assert_eq!(config.recovery_limit(), DEFAULT_RECOVERY_LIMIT);
        assert_eq!(config.history().ttl, history.ttl);
        assert_eq!(config.history().cleanup_limit, history.cleanup_limit);
        config.validate().unwrap();
    }

    #[test]
    fn rejects_invalid_top_level_bounds_and_identity() {
        assert!(config().with_cron_interval(Duration::ZERO).is_err());
        assert!(config().with_dispatch_limit(0).is_err());
        assert!(config().with_recovery_limit(0).is_err());
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
    fn history_ttl_covers_the_policy_and_operation_window() {
        assert!(
            config()
                .with_history(History {
                    ttl: MIN_HISTORY_TTL - Duration::from_millis(1),
                    ..History::default()
                })
                .is_err()
        );
        let config = config()
            .with_history(History {
                ttl: MIN_HISTORY_TTL,
                ..History::default()
            })
            .unwrap();
        assert!(
            config
                .with_policy(Policy {
                    lease_timeout_ms: MIN_HISTORY_TTL.as_millis() as i64 + 1,
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
    fn checked_in_template_requires_explicit_credentials() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("etc/master-api.yaml");
        let error = Config::from_file(path).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("token must be supplied explicitly")
        );
    }
}
