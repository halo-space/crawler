use std::path::PathBuf;

use contrib::scheduler::api::Api;
use harbor::downloader::{self, Downloader};
use harbor::item::{Jsonl, Map};
use harbor::net::{self, Mode};
use harbor::harbor::{Spider, SpiderFactory, Tx};
use harbor::{Error as SpiderError, engine};

/// Configuration for one long-lived Harbor Worker process.
#[derive(Clone, Debug)]
pub struct Config {
    pub master_url: String,
    pub token: String,
    pub worker_id: String,
    pub worker_host: String,
    pub worker_version: String,
    pub modes: Vec<Mode>,
    pub concurrency: usize,
    pub limit: usize,
    pub store_dir: PathBuf,
    pub browser: BrowserConfig,
}

impl Config {
    /// Loads the service configuration from environment variables.
    ///
    /// Required variables are `HARBOR_MASTER_URL`, `HARBOR_TOKEN`, and
    /// `HARBOR_WORKER_ID`. The remaining values have safe local defaults.
    pub fn from_env() -> Result<Self, Error> {
        let master_url = required("HARBOR_MASTER_URL")?;
        let token = required("HARBOR_TOKEN")?;
        let worker_id = required("HARBOR_WORKER_ID")?;
        let worker_host = optional("HARBOR_WORKER_HOST")
            .or_else(|| optional("HOSTNAME"))
            .unwrap_or_else(|| "localhost".to_string());
        let worker_version = optional("HARBOR_WORKER_VERSION")
            .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string());
        let modes = parse_modes(optional("HARBOR_WORKER_MODES").as_deref().unwrap_or("http"))?;
        let concurrency = parse_positive("HARBOR_WORKER_CONCURRENCY", 5)?;
        let limit = parse_positive("HARBOR_WORKER_LIMIT", 5)?;
        let store_dir = optional("HARBOR_ITEM_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data/items"));

        Ok(Self {
            master_url,
            token,
            worker_id,
            worker_host,
            worker_version,
            modes,
            concurrency,
            limit,
            store_dir,
            browser: BrowserConfig::from_env()?,
        })
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

/// Starts a Worker that consumes Requests from the Master API.
pub async fn run(config: Config) -> Result<(), Error> {
    let scheduler = Api::new(&config.master_url, config.token.clone())
        .map_err(|error| Error::Config(error.to_string()))?
        .with_worker_id(config.worker_id.clone())
        .map_err(|error| Error::Config(error.to_string()))?
        .with_worker_host(config.worker_host.clone())
        .map_err(|error| Error::Config(error.to_string()))?
        .with_worker_version(config.worker_version.clone())
        .map_err(|error| Error::Config(error.to_string()))?
        .with_modes(config.modes.clone())
        .map_err(|error| Error::Config(error.to_string()))?;
    let scheduler = scheduler
        .with_limit(config.limit)
        .map_err(|error| Error::Config(error.to_string()))?;

    let downloader = build_downloader(&config);
    let store = Jsonl::with_dir(config.store_dir);
    let mut runtime = engine::Engine::new()
        .with_scheduler(scheduler)
        .with_downloader(downloader)
        .with_spider(RulesWorkerFactory)
        .with_store(store)
        .build()
        .with_concurrency(config.concurrency)
        .with_claim_limit(config.limit);

    runtime.start().await.map_err(Error::Runtime)
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
