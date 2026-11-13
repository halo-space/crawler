use std::any::Any;

use harbor::downloader::{self, Downloader};
use harbor::item::Jsonl;
use harbor::middleware::{self, Spec};
use harbor::scheduler::{Init, Scheduler};
use harbor::{Spider, Tx, engine};

use crate::config::Config;
use crate::rules::{RulesSpider, rules};

pub struct Engine<S = harbor::scheduler::Memory, D = Downloader, P = RulesSpider, O = Jsonl> {
    config: Config,
    engine: engine::Engine<S, D, P, O>,
}

impl Engine<harbor::scheduler::Memory, Downloader, RulesSpider, Jsonl> {
    pub fn new(config: Config) -> Self {
        Self {
            config: config.clone(),
            engine: engine::Engine::new()
                .with_downloader(default_downloader(&config))
                .with_store(Jsonl::with_dir(&config.store_dir))
                .with_spider(rules),
        }
    }
}

impl<S, D, P, O> Engine<S, D, P, O> {
    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Engine<NextS, D, P, O> {
        Engine {
            config: self.config,
            engine: self.engine.with_scheduler(scheduler),
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Engine<S, NextD, P, O> {
        Engine {
            config: self.config,
            engine: self.engine.with_downloader(downloader),
        }
    }

    pub fn with_load<L>(self, load: L) -> Self
    where
        L: harbor::load::Load + 'static,
    {
        Engine {
            config: self.config,
            engine: self.engine.with_load(load),
        }
    }

    pub fn with_spider<NextP, F>(self, spider: F) -> Engine<S, D, NextP, O>
    where
        F: FnOnce(Tx) -> NextP + Send + 'static,
    {
        Engine {
            config: self.config,
            engine: self.engine.with_spider(spider),
        }
    }

    pub fn with_store<NextO>(self, store: NextO) -> Engine<S, D, P, NextO> {
        Engine {
            config: self.config,
            engine: self.engine.with_store(store),
        }
    }

    pub fn with_middleware<M>(self, name: impl Into<String>, middleware: M) -> Self
    where
        M: middleware::Middleware + 'static,
    {
        Engine {
            config: self.config,
            engine: self.engine.with_middleware(name, middleware),
        }
    }

    pub fn with_attachment_downloader<A>(self, downloader: A) -> Self
    where
        A: harbor::item::attachment::AttachmentDownloader + 'static,
    {
        Engine {
            config: self.config,
            engine: self.engine.with_attachment_downloader(downloader),
        }
    }

    pub fn with_middleware_spec(self, spec: Spec) -> Self {
        Engine {
            config: self.config,
            engine: self.engine.with_spider_middleware(spec),
        }
    }
}

impl<S, D, P, O> Engine<S, D, P, O>
where
    S: Scheduler + Init + 'static,
    D: harbor::downloader::Download + 'static,
    P: Any + Spider + 'static,
    O: harbor::item::Store + 'static,
{
    pub async fn run(self) -> Result<(), harbor::Error> {
        let mut runtime = self
            .engine
            .build()
            .with_concurrency(self.config.concurrency)
            .with_claim_limit(self.config.claim_limit);
        runtime.start().await
    }
}

pub async fn run(config: Config) -> Result<(), harbor::Error> {
    Engine::new(config).run().await
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
