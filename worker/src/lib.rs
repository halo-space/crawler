use std::path::PathBuf;
use std::time::Duration;

use contrib::scheduler::{api::Api, mysql::MySql, redis::Redis};
use halo::harbor::downloader::{self, Downloader};
use halo::harbor::item::{Jsonl, Map};
use halo::harbor::net::{self, Mode};
use halo::harbor::scheduler::{Init, Scheduler};
use halo::harbor::{Error as SpiderError, Memory, Spider, SpiderFactory, Tx, engine};

/// Selects the Scheduler used by the Worker process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SchedulerConfig {
    Memory,
    Redis {
        url: String,
        namespace: String,
    },
    MySql {
        dsn: String,
    },
    MasterApi {
        url: String,
        token: String,
        namespace: String,
    },
}

/// Configuration for one long-lived Harbor Worker process.
#[derive(Clone, Debug)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    pub worker_id: String,
    pub worker_host: String,
    pub worker_version: String,
    pub modes: Vec<Mode>,
    pub concurrency: usize,
    pub limit: usize,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub store_dir: PathBuf,
    pub browser: BrowserConfig,
}

impl Config {
    /// Loads the service configuration from environment variables.
    ///
    /// Scheduler-specific connection variables are required only for the selected backend.
    /// Worker identity and local runtime settings have safe defaults.
    pub fn from_env() -> Result<Self, Error> {
        let scheduler = SchedulerConfig::from_env()?;
        let worker_id = optional("HARBOR_WORKER_ID").unwrap_or_else(|| "worker-local".to_string());
        let worker_host = optional("HARBOR_WORKER_HOST")
            .or_else(|| optional("HOSTNAME"))
            .unwrap_or_else(|| "localhost".to_string());
        let worker_version = optional("HARBOR_WORKER_VERSION")
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let modes = parse_modes(optional("HARBOR_WORKER_MODES").as_deref().unwrap_or("http"))?;
        let concurrency = parse_positive("HARBOR_WORKER_CONCURRENCY", 5)?;
        let limit = parse_positive("HARBOR_WORKER_LIMIT", 5)?;
        let heartbeat_interval = parse_duration("HARBOR_HEARTBEAT_INTERVAL_MS", 10_000)?;
        let heartbeat_timeout = parse_duration("HARBOR_HEARTBEAT_TIMEOUT_MS", 30_000)?;
        if heartbeat_timeout <= heartbeat_interval {
            return Err(Error::Config(
                "HARBOR_HEARTBEAT_TIMEOUT_MS must be greater than HARBOR_HEARTBEAT_INTERVAL_MS"
                    .to_string(),
            ));
        }
        let store_dir = optional("HARBOR_ITEM_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/items"));

        Ok(Self {
            scheduler,
            worker_id,
            worker_host,
            worker_version,
            modes,
            concurrency,
            limit,
            heartbeat_interval,
            heartbeat_timeout,
            store_dir,
            browser: BrowserConfig::from_env()?,
        })
    }
}

impl SchedulerConfig {
    fn from_env() -> Result<Self, Error> {
        match optional("HARBOR_SCHEDULER").as_deref().unwrap_or("memory") {
            "memory" => Ok(Self::Memory),
            "redis" => Ok(Self::Redis {
                url: required("HARBOR_REDIS_URL")?,
                namespace: optional("HARBOR_REDIS_NAMESPACE")
                    .unwrap_or_else(|| "crawler".to_string()),
            }),
            "mysql" => Ok(Self::MySql {
                dsn: required("HARBOR_MYSQL_DSN")?,
            }),
            "master_api" => Ok(Self::MasterApi {
                url: required("HARBOR_MASTER_URL")?,
                token: required("HARBOR_TOKEN")?,
                namespace: optional("HARBOR_MASTER_NAMESPACE")
                    .unwrap_or_else(|| "default".to_string()),
            }),
            value => Err(Error::Config(format!(
                "HARBOR_SCHEDULER must be memory, redis, mysql, or master_api; received {value:?}"
            ))),
        }
    }
}

/// Browser-specific options kept local to the Worker process.
#[derive(Clone, Debug)]
pub struct BrowserConfig {
    pub webdriver_url: Option<String>,
    pub binary: Option<String>,
    pub headless: bool,
}

impl Default for BrowserConfig {
    fn default() -> Self {
        Self {
            webdriver_url: None,
            binary: None,
            headless: true,
        }
    }
}

impl BrowserConfig {
    fn from_env() -> Result<Self, Error> {
        Ok(Self {
            webdriver_url: optional("HARBOR_BROWSER_WEBDRIVER_URL"),
            binary: optional("HARBOR_BROWSER_BINARY"),
            headless: parse_bool("HARBOR_BROWSER_HEADLESS", true)?,
        })
    }
}

/// Starts a Worker with the Scheduler selected by [`Config::scheduler`].
pub async fn run(config: Config) -> Result<(), Error> {
    match config.scheduler.clone() {
        SchedulerConfig::Memory => {
            let scheduler = Memory::new().with_modes(config.modes.clone());
            config.with_scheduler(scheduler).run().await
        }
        SchedulerConfig::Redis { url, namespace } => {
            let scheduler = Redis::new(url)
                .map_err(config_error)?
                .with_namespace(namespace)
                .map_err(config_error)?
                .with_worker_id(&config.worker_id)
                .map_err(config_error)?
                .with_worker_host(&config.worker_host)
                .map_err(config_error)?
                .with_worker_version(&config.worker_version)
                .map_err(config_error)?
                .with_modes(config.modes.clone())
                .map_err(config_error)?
                .with_heartbeat(config.heartbeat_interval, config.heartbeat_timeout)
                .map_err(config_error)?;
            config.with_scheduler(scheduler).run().await
        }
        SchedulerConfig::MySql { dsn } => {
            let scheduler = MySql::new(dsn)
                .map_err(config_error)?
                .with_worker_id(&config.worker_id)
                .map_err(config_error)?
                .with_worker_host(&config.worker_host)
                .map_err(config_error)?
                .with_worker_version(&config.worker_version)
                .map_err(config_error)?
                .with_modes(config.modes.clone())
                .map_err(config_error)?
                .with_heartbeat(config.heartbeat_interval, config.heartbeat_timeout)
                .map_err(config_error)?;
            config.with_scheduler(scheduler).run().await
        }
        SchedulerConfig::MasterApi {
            url,
            token,
            namespace,
        } => {
            let scheduler = Api::new(url, token)
                .map_err(config_error)?
                .with_namespace(namespace)
                .map_err(config_error)?
                .with_worker_id(&config.worker_id)
                .map_err(config_error)?
                .with_worker_host(&config.worker_host)
                .map_err(config_error)?
                .with_worker_version(&config.worker_version)
                .map_err(config_error)?
                .with_modes(config.modes.clone())
                .map_err(config_error)?
                .with_limit(config.limit)
                .map_err(config_error)?;
            config.with_scheduler(scheduler).run().await
        }
    }
}

/// A Worker lifecycle bound to one Scheduler implementation.
pub struct Worker<S> {
    config: Config,
    scheduler: S,
}

impl Config {
    /// Binds a Scheduler implementation to this Worker configuration.
    ///
    /// Custom Scheduler implementations use this entry point and do not need to modify the
    /// Worker crate.
    pub fn with_scheduler<S>(self, scheduler: S) -> Worker<S> {
        Worker {
            config: self,
            scheduler,
        }
    }
}

impl<S> Worker<S>
where
    S: Scheduler + Init + 'static,
{
    pub async fn run(self) -> Result<(), Error> {
        let downloader = build_downloader(&self.config);
        let store = Jsonl::with_dir(self.config.store_dir);
        let mut runtime = engine::Engine::new()
            .with_scheduler(self.scheduler)
            .with_downloader(downloader)
            .with_spider(RulesWorkerFactory)
            .with_store(store)
            .build()
            .with_concurrency(self.config.concurrency)
            .with_claim_limit(self.config.limit);

        runtime.start().await.map_err(Error::Runtime)
    }
}

fn config_error(error: impl std::fmt::Display) -> Error {
    Error::Config(error.to_string())
}

fn build_downloader(config: &Config) -> Downloader {
    let mut browser = downloader::browser::Browser::new();
    if let Some(url) = &config.browser.webdriver_url {
        browser = browser.with_webdriver_url(url.clone());
    }
    if let Some(binary) = &config.browser.binary {
        browser = browser.with_binary(binary.clone());
    }
    browser = browser.with_headless(config.browser.headless);

    Downloader::new().with_browser(browser)
}

/// A Rules-only Spider implementation used by the service.
///
/// The actual graph and Item schema come from the Trace Snapshot restored by
/// the Scheduler. This local Spider only supplies the framework's stable
/// `Spider` boundary and the default Item forwarding function.
#[derive(Clone)]
pub struct RulesWorker {
    tx: Tx,
}

impl Spider for RulesWorker {
    type Item = Map;

    fn name(&self) -> &str {
        "harbor-worker"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: net::Response) -> Result<(), SpiderError> {
        Ok(())
    }
}

pub struct RulesWorkerFactory;

impl SpiderFactory for RulesWorkerFactory {
    type Spider = RulesWorker;

    fn build(self, tx: Tx) -> Self::Spider {
        RulesWorker { tx }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker configuration error: {0}")]
    Config(String),
    #[error("worker runtime failed: {0}")]
    Runtime(SpiderError),
}

fn required(name: &str) -> Result<String, Error> {
    optional(name).ok_or_else(|| Error::Config(format!("{name} must be set")))
}

fn optional(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_positive(name: &str, default: usize) -> Result<usize, Error> {
    let value = optional(name)
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|error| Error::Config(format!("{name} must be a positive integer: {error}")))?
        .unwrap_or(default);
    if value == 0 {
        return Err(Error::Config(format!("{name} must be positive")));
    }
    Ok(value)
}

fn parse_duration(name: &str, default_ms: u64) -> Result<Duration, Error> {
    let value = optional(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .map_err(|error| Error::Config(format!("{name} must be a non-negative integer: {error}")))?
        .unwrap_or(default_ms);
    if value == 0 {
        return Err(Error::Config(format!("{name} must be positive")));
    }
    Ok(Duration::from_millis(value))
}

fn parse_bool(name: &str, default: bool) -> Result<bool, Error> {
    match optional(name).as_deref() {
        None => Ok(default),
        Some("1" | "true" | "TRUE" | "yes" | "YES") => Ok(true),
        Some("0" | "false" | "FALSE" | "no" | "NO") => Ok(false),
        Some(value) => Err(Error::Config(format!(
            "{name} must be true or false, received {value}"
        ))),
    }
}

fn parse_modes(value: &str) -> Result<Vec<Mode>, Error> {
    let mut modes = Vec::new();
    for name in value
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let mode = match name {
            "http" => Mode::Http,
            "browser" => Mode::Browser,
            other => {
                return Err(Error::Config(format!(
                    "HARBOR_WORKER_MODES contains unsupported mode {other:?}"
                )));
            }
        };
        if !modes.contains(&mode) {
            modes.push(mode);
        }
    }
    if modes.is_empty() {
        return Err(Error::Config(
            "HARBOR_WORKER_MODES must contain at least one mode".to_string(),
        ));
    }
    Ok(modes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_modes_without_duplicates() {
        assert_eq!(
            parse_modes("browser,http,browser").unwrap(),
            vec![Mode::Browser, Mode::Http]
        );
    }

    #[test]
    fn rejects_unknown_modes() {
        assert!(parse_modes("http,api").is_err());
    }

    #[test]
    fn parses_boolean_defaults() {
        assert!(parse_bool("MISSING_HARBOR_FLAG", true).unwrap());
    }
}
