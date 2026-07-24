use std::any::Any;
use std::sync::Arc;

use super::runtime::{MAX_EVENTS, Runtime};
use super::worker::Worker;
use crate::spider::Spider as _;
use crate::spider::tx;
use crate::{config, downloader, engine, middleware, net, scheduler, spider};

pub struct Builder<S = scheduler::Memory, D = downloader::Downloader, F = ()> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider_factory: F,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) ai: Option<Arc<crate::ai::OpenAI>>,
    pub(super) middlewares: Vec<middleware::Spec>,
    pub(super) worker: Worker,
}

impl Builder<scheduler::Memory, downloader::Downloader, ()> {
    pub fn new() -> Self {
        let schemas = Arc::new(crate::item::schema::Store::new());
        Self {
            scheduler: scheduler::Memory::new(),
            downloader: downloader::Downloader::new(),
            spider_factory: (),
            registry: middleware::Registry::with_schemas(schemas.clone()),
            schemas,
            ai: None,
            middlewares: Vec::new(),
            worker: Worker::default(),
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
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            worker: self.worker,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, F> {
        Builder {
            scheduler: self.scheduler,
            downloader,
            spider_factory: self.spider_factory,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            worker: self.worker,
        }
    }

    pub fn with_spider<NextF>(self, spider_factory: NextF) -> Builder<S, D, NextF> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            worker: self.worker,
        }
    }

    pub fn with_rules(self, config: config::Config) -> super::rules::Builder<S, D, F> {
        super::rules::Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            worker: self.worker,
        }
    }

    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker.set_id(worker_id);
        self
    }

    /// Selects the Worker-local OpenAI provider used by every parsed Response.
    pub fn with_ai(mut self, openai: crate::ai::OpenAI) -> Self {
        self.ai = Some(Arc::new(openai));
        self
    }

    pub fn with_modes(mut self, modes: impl IntoIterator<Item = net::Mode>) -> Self {
        self.worker.set_modes(modes);
        self
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
        let executor = engine::executor::Executor::new(Arc::new(spider), self.schemas, self.ai);

        Runtime::new(
            self.scheduler,
            self.downloader,
            executor,
            events,
            self.registry,
            self.middlewares,
            self.worker,
        )
        .with_init(engine::code::Init::new(seed))
    }
}
