use std::any::Any;
use std::sync::Arc;

use super::runtime::{MAX_EVENTS, Runtime};
use crate::harbor::tx;
use crate::{config, downloader, engine, harbor, middleware, scheduler};

pub struct Builder<
    S = scheduler::Memory,
    D = downloader::Downloader,
    P = (),
    O = crate::item::Jsonl,
> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider: Box<dyn FnOnce(tx::Tx) -> P + Send>,
    pub(super) store: O,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) ai: Option<Arc<crate::ai::OpenAI>>,
    pub(super) middlewares: Vec<middleware::Spec>,
    pub(super) attachment: Option<crate::item::attachment::Config>,
}

impl Builder<scheduler::Memory, downloader::Downloader, (), crate::item::Jsonl> {
    pub fn new() -> Self {
        let schemas = Arc::new(crate::item::schema::Store::new());
        Self {
            scheduler: scheduler::Memory::new(),
            downloader: downloader::Downloader::new(),
            spider: Box::new(|_| ()),
            store: crate::item::Jsonl::new(),
            registry: middleware::Registry::with_schemas(schemas.clone()),
            schemas,
            ai: None,
            middlewares: Vec::new(),
            attachment: None,
        }
    }
}

impl Default for Builder<scheduler::Memory, downloader::Downloader, ()> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, D, P, O> Builder<S, D, P, O> {
    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, P, O> {
        Builder {
            scheduler,
            downloader: self.downloader,
            spider: self.spider,
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            attachment: self.attachment,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, P, O> {
        Builder {
            scheduler: self.scheduler,
            downloader,
            spider: self.spider,
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            attachment: self.attachment,
        }
    }

    pub fn with_spider<NextP, F>(self, spider: F) -> Builder<S, D, NextP, O>
    where
        F: FnOnce(tx::Tx) -> NextP + Send + 'static,
    {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider: Box::new(spider),
            store: self.store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            attachment: self.attachment,
        }
    }

    pub fn with_rules(self, config: config::Config) -> super::rules::Builder<S, D, P, O> {
        super::rules::Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider: self.spider,
            store: self.store,
            config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_store<NextO>(self, store: NextO) -> Builder<S, D, P, NextO> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider: self.spider,
            store,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            attachment: self.attachment,
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

    pub fn with_attachment_downloader<A>(self, downloader: A) -> Self
    where
        A: crate::item::attachment::AttachmentDownloader + 'static,
    {
        self.registry.register_attachment(downloader);
        self
    }

    pub fn with_attachment_config(mut self, config: crate::item::attachment::Config) -> Self {
        self.attachment = Some(config);
        self
    }

    pub fn with_spider_middleware(mut self, spec: middleware::Spec) -> Self {
        self.middlewares.push(spec);
        self
    }
}

impl<S, D, P, O> Builder<S, D, P, O>
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
    D: downloader::Download + 'static,
    P: Any + harbor::Spider + 'static,
    O: crate::item::Store + 'static,
{
    pub fn build(self) -> Runtime<S, D, engine::executor::Executor<P>, engine::code::Init, O> {
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = (self.spider)(tx);
        let seed = self.scheduler.initializes_run().then(|| {
            let task_id = spider.name().to_string();
            let trace_id = crate::trace::next_id();
            spider.tx().set_trace(task_id.clone(), trace_id.clone());
            (task_id, trace_id, self.attachment.clone())
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
