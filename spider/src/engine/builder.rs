use std::any::Any;
use std::sync::Arc;

use super::runtime::{MAX_EVENTS, Runtime};
use crate::spider::Spider as _;
use crate::spider::tx;
use crate::{config, downloader, engine, middleware, scheduler, spider};

pub struct Builder<
    S = scheduler::Memory,
    D = downloader::Downloader,
    F = (),
    O = crate::item::Jsonl,
> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider_factory: F,
    pub(super) store: O,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) ai: Option<Arc<crate::ai::OpenAI>>,
    pub(super) middlewares: Vec<middleware::Spec>,
}

impl Builder<scheduler::Memory, downloader::Downloader, (), crate::item::Jsonl> {
    pub fn new() -> Self {
        let schemas = Arc::new(crate::item::schema::Store::new());
        Self {
            scheduler: scheduler::Memory::new(),
            downloader: downloader::Downloader::new(),
            spider_factory: (),
            store: crate::item::Jsonl::new(),
            registry: middleware::Registry::with_schemas(schemas.clone()),
            schemas,
            ai: None,
            middlewares: Vec::new(),
        }
    }
}

impl Default for Builder<scheduler::Memory, downloader::Downloader, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, D, F, O> Builder<S, D, F, O> {
    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, F, O> {
        Builder {
            scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, F, O> {
        Builder {
            scheduler: self.scheduler,
            downloader,
            spider_factory: self.spider_factory,
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_spider<NextF>(self, spider_factory: NextF) -> Builder<S, D, NextF, O> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory,
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_rules(self, config: config::Config) -> super::rules::Builder<S, D, F, O> {
        super::rules::Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            store: self.store,
            config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_store<NextO>(self, store: NextO) -> Builder<S, D, F, NextO> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    /// Selects the Worker-local OpenAI provider used by every parsed Response.
    pub fn with_ai(mut self, openai: crate::ai::OpenAI) -> Self {
        self.ai = Some(Arc::new(openai));
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

impl<S, D, F, O> Builder<S, D, F, O>
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
    D: downloader::Download + 'static,
    F: spider::SpiderFactory,
    F::Spider: Any + spider::Spider + 'static,
    O: crate::item::Store + 'static,
{
    pub fn build(
        self,
    ) -> Runtime<S, D, engine::executor::Executor<F::Spider>, engine::code::Init, O> {
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = self.spider_factory.build(tx);
        let seed = self.scheduler.initializes_run().then(|| {
            let task_id = spider.name().to_string();
            let trace_id = crate::trace::next_id();
            spider.tx().set_trace(task_id.clone(), trace_id.clone());
            (task_id, trace_id)
        });
        let executor = engine::executor::Executor::new(Arc::new(spider), self.schemas, self.ai);

        Runtime::new(super::runtime::Setup {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor,
            store: self.store,
            events,
            registry: self.registry,
            middlewares: self.middlewares,
        })
        .with_init(engine::code::Init::new(seed))
    }
}
