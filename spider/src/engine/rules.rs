use std::sync::Arc;

use super::runtime::MAX_EVENTS;
use crate::spider::tx;
use crate::{config, downloader, middleware, scheduler, spider};

/// Rules 模式装配完成后的 Engine 类型。
pub type Runtime<S, D, F> = super::runtime::Runtime<
    S,
    D,
    super::executor::Executor<<F as spider::SpiderFactory>::Spider>,
    Init,
>;

#[doc(hidden)]
pub mod executor;

pub struct Builder<S, D, F> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider_factory: F,
    pub(super) config: config::Config,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) middlewares: Vec<middleware::Spec>,
}

impl<S, D, F> Builder<S, D, F> {
    pub fn with_spider<NextF>(self, spider_factory: NextF) -> Builder<S, D, NextF> {
        Builder {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider_factory,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            middlewares: self.middlewares,
        }
    }

    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Builder<NextS, D, F> {
        Builder {
            scheduler,
            downloader: self.downloader,
            spider_factory: self.spider_factory,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            middlewares: self.middlewares,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Builder<S, NextD, F> {
        Builder {
            scheduler: self.scheduler,
            downloader,
            spider_factory: self.spider_factory,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
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
    F::Spider: std::any::Any + spider::Spider + 'static,
{
    pub fn build(self) -> Runtime<S, D, F> {
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = self.spider_factory.build(tx);
        validate_item_functions(&spider, &self.config);
        let config = Arc::new(self.config);
        let schemas = self.schemas;
        let executor = super::executor::Executor::new(Arc::new(spider), schemas.clone());
        let trace_id = config.next_trace_id();

        super::runtime::Runtime::new(
            self.scheduler,
            self.downloader,
            executor,
            events,
            self.registry,
            self.middlewares,
        )
        .with_init(Init::new(config, trace_id, schemas))
    }
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
    async fn init(
        &self,
        scheduler: Arc<S>,
        registry: Arc<middleware::Registry>,
    ) -> Result<super::init::Output, crate::Error> {
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
        let requests = super::admission::apply(requests, None, registry.as_ref()).await?;

        scheduler
            .init(self.trace_id.clone(), snapshot, requests)
            .await
            .map_err(crate::Error::Scheduler)?;
        Ok(super::init::Output::Consume)
    }
}
