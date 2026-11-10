use std::any::Any;

use harbor::downloader::{self, Downloader};
use harbor::item::Jsonl;
use harbor::middleware::{self, Spec};
use harbor::scheduler::{Init, Scheduler};
use harbor::{Spider, SpiderFactory, engine};

use crate::config::Config;
use crate::error::Error;
use crate::rules::RulesSpiderFactory;

pub struct Builder<S = harbor::scheduler::Memory, D = Downloader, F = RulesSpiderFactory, O = Jsonl>
{
    config: Config,
    engine: engine::Builder<S, D, F, O>,
}

impl Builder<harbor::scheduler::Memory, Downloader, RulesSpiderFactory, Jsonl> {
    pub fn new(config: Config) -> Self {
        Self {
            config: config.clone(),
            engine: engine::Builder::new()
                .with_downloader(default_downloader(&config))
                .with_store(Jsonl::with_dir(&config.store_dir))
                .with_spider(RulesSpiderFactory),
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
