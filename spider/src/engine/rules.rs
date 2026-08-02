use std::sync::Arc;

use super::runtime::MAX_EVENTS;
use crate::spider::tx;
use crate::{config, downloader, middleware, scheduler, spider};

/// Rules 模式装配完成后的 Engine 类型。
pub type Runtime<S, D, F, O = crate::item::Jsonl> = super::runtime::Runtime<
    S,
    D,
    super::executor::Executor<<F as spider::SpiderFactory>::Spider>,
    Init,
    O,
>;

#[doc(hidden)]
pub mod executor;

pub struct Builder<S, D, F, O = crate::item::Jsonl> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider_factory: F,
    pub(super) store: O,
    pub(super) config: config::Config,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) ai: Option<Arc<crate::ai::OpenAI>>,
    pub(super) middlewares: Vec<middleware::Spec>,
}

impl<S, D, F, O> Builder<S, D, F, O> {
    pub fn with_spider<NextF>(self, spider_factory: NextF) -> Builder<S, D, NextF, O> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory,
            store: self.store,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
        }
    }

    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, F, O> {
        Builder {
            scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            store: self.store,
            config: self.config,
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
            config: self.config,
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
            config: self.config,
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
    F::Spider: std::any::Any + spider::Spider + 'static,
    O: crate::item::Store + 'static,
{
    pub fn build(self) -> Runtime<S, D, F, O> {
        validate_ai(&self.config, self.ai.as_deref());
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = self.spider_factory.build(tx);
        validate_item_functions(&spider, &self.config);
        let config = Arc::new(self.config);
        let schemas = self.schemas;
        let executor = super::executor::Executor::new(Arc::new(spider), schemas.clone(), self.ai);
        let trace_id = config.next_trace_id();

        super::runtime::Runtime::new(super::runtime::Setup {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor,
            store: self.store,
            events,
            registry: self.registry,
            middlewares: self.middlewares,
        })
        .with_init(Init::new(config, trace_id, schemas))
    }
}

fn validate_ai(config: &config::Config, openai: Option<&crate::ai::OpenAI>) {
    let uses_ai = config.graph.nodes.values().any(|node| {
        node.parse.fields.values().any(|field| {
            field
                .extractors
                .iter()
                .any(|extractor| matches!(extractor, crate::graph::rules::Extractor::Ai { .. }))
        })
    });
    assert!(
        !uses_ai || openai.is_some(),
        "Rules config uses an AI extractor but Engine has no AI provider"
    );
}

fn validate_item_functions<P>(spider: &P, config: &config::Config)
where
    P: spider::Spider,
{
    for edge in &config.graph.edges {
        if !matches!(edge.kind, crate::graph::edge::Kind::Item) {
            continue;
        }
        let name = edge.function.as_deref().unwrap_or("item");
        assert!(
            spider.item_fn(name).is_some(),
            "Rules item function is not registered: spider={}, node={}, fn={name}",
            spider.name(),
            edge.from,
        );
    }
}

#[doc(hidden)]
pub struct Init {
    config: Arc<config::Config>,
    trace_id: String,
    schemas: Arc<crate::item::schema::Store>,
}

impl Init {
    fn new(
        config: Arc<config::Config>,
        trace_id: String,
        schemas: Arc<crate::item::schema::Store>,
    ) -> Self {
        Self {
            config,
            trace_id,
            schemas,
        }
    }
}

impl<S> super::init::Init<S> for Init
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
{
    async fn init(&self, scheduler: Arc<S>) -> Result<super::init::Output, crate::Error> {
        if let Some(item) = &self.config.item {
            self.schemas
                .register(&item.schema)
                .map_err(crate::Error::Item)?;
        }
        let snapshot = crate::trace::Snapshot::rules(
            self.config.spider.name.clone(),
            self.config.as_ref().clone(),
        );
        let requests = self
            .config
            .initial_requests(
                self.config.spider.name.clone(),
                self.trace_id.clone(),
                Default::default(),
            )
            .map_err(crate::Error::Config)?;
        scheduler
            .init(self.trace_id.clone(), snapshot, requests)
            .await
            .map_err(crate::Error::Scheduler)?;
        Ok(super::init::Output::Consume)
    }
}
