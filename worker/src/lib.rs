use std::any::Any;
use std::path::PathBuf;

use harbor::downloader::{self, Downloader};
use harbor::item::{Jsonl, Map};
use harbor::middleware::{self, Spec};
use harbor::scheduler::{Init, Scheduler};
use harbor::{Error as HarborError, Spider, SpiderFactory, Tx, engine};

#[derive(Clone, Debug)]
pub struct Config {
    pub concurrency: usize,
    pub claim_limit: usize,
    pub store_dir: PathBuf,
    pub browser: BrowserConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency: 5,
            claim_limit: 5,
            store_dir: PathBuf::from("./data/items"),
            browser: BrowserConfig::default(),
        }
    }
}

impl Config {
    /// Loads only Worker-local settings. Scheduler identity and backend settings belong to the Scheduler.
    pub fn from_env() -> Result<Self, Error> {
        Ok(Self {
            concurrency: parse_positive("HARBOR_WORKER_CONCURRENCY", 5)?,
            claim_limit: parse_positive("HARBOR_WORKER_LIMIT", 5)?,
            store_dir: optional("HARBOR_ITEM_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("./data/items")),
            browser: BrowserConfig::from_env()?,
        })
    }
}

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

pub struct Builder<S = harbor::scheduler::Memory, D = Downloader, F = RulesWorkerFactory, O = Jsonl>
{
    config: Config,
    engine: engine::Builder<S, D, F, O>,
}

impl Builder<harbor::scheduler::Memory, Downloader, RulesWorkerFactory, Jsonl> {
    pub fn new(config: Config) -> Self {
        Self {
            config: config.clone(),
            engine: engine::Builder::new()
                .with_downloader(default_downloader(&config))
                .with_store(Jsonl::with_dir(&config.store_dir))
                .with_spider(RulesWorkerFactory),
        }
    }
}

impl<S, D, F, O> Builder<S, D, F, O> {
    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, F, O> {
        Builder {
            config: self.config,
            engine: self.engine.with_scheduler(scheduler),
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, F, O> {
        Builder {
            config: self.config,
            engine: self.engine.with_downloader(downloader),
        }
    }

    pub fn with_spider<NextF>(self, spider: NextF) -> Builder<S, D, NextF, O> {
        Builder {
            config: self.config,
            engine: self.engine.with_spider(spider),
        }
    }

    pub fn with_store<NextO>(self, store: NextO) -> Builder<S, D, F, NextO> {
        Builder {
            config: self.config,
            engine: self.engine.with_store(store),
        }
    }

    pub fn with_middleware<M>(self, name: impl Into<String>, middleware: M) -> Self
    where
        M: middleware::Middleware + 'static,
    {
        Builder {
            config: self.config,
            engine: self.engine.with_middleware(name, middleware),
        }
    }

    pub fn with_attachment_downloader<A>(self, downloader: A) -> Self
    where
        A: harbor::item::attachment::AttachmentDownloader + 'static,
    {
        Builder {
            config: self.config,
            engine: self.engine.with_attachment_downloader(downloader),
        }
    }

    pub fn with_spider_middleware(self, spec: Spec) -> Self {
        Builder {
            config: self.config,
            engine: self.engine.with_spider_middleware(spec),
        }
    }
}

impl<S, D, F, O> Builder<S, D, F, O>
where
    S: Scheduler + Init + 'static,
    D: harbor::downloader::Download + 'static,
    F: SpiderFactory,
    F::Spider: Any + Spider + 'static,
    O: harbor::item::Store + 'static,
{
    pub async fn run(self) -> Result<(), Error> {
        let mut runtime = self
            .engine
            .build()
            .with_concurrency(self.config.concurrency)
            .with_claim_limit(self.config.claim_limit);
        runtime.start().await.map_err(Error::Runtime)
    }
}

pub async fn run(config: Config) -> Result<(), Error> {
    Builder::new(config).run().await
}

fn default_downloader(config: &Config) -> Downloader {
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

#[derive(Clone)]
pub struct RulesWorker {
    tx: Tx,
}

impl Spider for RulesWorker {
    type Item = Map;

    fn name(&self) -> &str {
        "halo-harbor-worker"
    }

    fn tx(&self) -> &Tx {
        &self.tx
    }

    async fn index(&self, _response: harbor::net::Response) -> Result<(), HarborError> {
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
    Runtime(HarborError),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_boolean_defaults() {
        assert!(parse_bool("MISSING_HARBOR_FLAG", true).unwrap());
    }

    #[test]
    fn rejects_zero_limits() {
        assert!(parse_positive("HARBOR_TEST_LIMIT", 0).is_err());
    }
}
