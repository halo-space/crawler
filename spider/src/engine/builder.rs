use std::any::Any;
use std::sync::Arc;

use super::runtime::{DEFAULT_WORKER_ID, MAX_EVENTS, Runtime};
use crate::spider::Spider as _;
use crate::spider::tx;
use crate::{config, downloader, engine, middleware, scheduler, spider};

pub struct Builder<S = scheduler::Memory, D = downloader::Downloader, F = ()> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider_factory: F,
    pub(super) registry: middleware::Registry,
    pub(super) middlewares: Vec<middleware::Spec>,
}

impl Builder<scheduler::Memory, downloader::Downloader, ()> {
    pub fn new() -> Self {
        Self {
            scheduler: scheduler::Memory::new(DEFAULT_WORKER_ID),
            downloader: downloader::Downloader::new(),
            spider_factory: (),
            registry: middleware::Registry::new(),
            middlewares: Vec::new(),
        }
    }
}

impl Default for Builder<scheduler::Memory, downloader::Downloader, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, D, F> Builder<S, D, F> {
    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, F> {
        Builder {
            scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            registry: self.registry,
            middlewares: self.middlewares,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, F> {
        Builder {
            scheduler: self.scheduler,
            downloader,
            spider_factory: self.spider_factory,
            registry: self.registry,
            middlewares: self.middlewares,
        }
    }

    pub fn with_spider<NextF>(self, spider_factory: NextF) -> Builder<S, D, NextF> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory,
            registry: self.registry,
            middlewares: self.middlewares,
        }
    }

    pub fn with_rules(self, config: config::Config) -> super::rules::Builder<S, D, F> {
        super::rules::Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            config,
            registry: self.registry,
            middlewares: self.middlewares,
        }
    }

    pub fn with_middleware<M>(self, name: impl Into<String>, value: M) -> Self
    where
        M: middleware::Middleware + 'static,
    {
        self.registry.register(name, value);
        self
    }

    pub fn with_spider_middleware(mut self, spec: middleware::Spec) -> Self {
        self.middlewares.push(spec);
        self
    }
}

impl<S, D, F> Builder<S, D, F>
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
    D: downloader::Download + 'static,
    F: spider::SpiderFactory,
    F::Spider: Any + spider::Spider + 'static,
{
    pub fn build(self) -> Runtime<S, D, engine::executor::Executor<F::Spider>, engine::code::Init> {
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = self.spider_factory.build(tx);
        let seed = self.scheduler.initializes_run().then(|| {
            let task_id = spider.name().to_string();
            let trace_id = crate::trace::next_id(&task_id);
            spider.tx().set_trace(task_id.clone(), trace_id.clone());
            (task_id, trace_id)
        });
        let schemas = self.registry.schemas();
        let executor = engine::executor::Executor::new(Arc::new(spider), schemas);

        Runtime::new(
            self.scheduler,
            self.downloader,
            executor,
            events,
            self.registry,
            self.middlewares,
        )
        .with_init(engine::code::Init::new(seed))
    }
}
