use std::sync::Arc;

use super::runtime::MAX_EVENTS;
use crate::harbor::tx;
use crate::{config, downloader, harbor, middleware, scheduler};

/// Rules 模式装配完成后的 Engine 类型。
pub type Runtime<S, D, P, O = crate::item::Jsonl> =
    super::runtime::Runtime<S, D, super::executor::Executor<P>, Init, O>;

#[doc(hidden)]
pub mod executor;

pub struct Rules<S, D, P, O = crate::item::Jsonl> {
    pub(super) scheduler: S,
    pub(super) downloader: D,
    pub(super) spider: Box<dyn FnOnce(tx::Tx) -> P + Send>,
    pub(super) store: O,
    pub(super) config: config::Config,
    pub(super) registry: middleware::Registry,
    pub(super) schemas: Arc<crate::item::schema::Store>,
    pub(super) ai: Option<Arc<crate::ai::OpenAI>>,
    pub(super) middlewares: Vec<middleware::Spec>,
    pub(super) load: Option<Arc<dyn crate::load::Load>>,
}

impl<S, D, P, O> Rules<S, D, P, O> {
    pub fn with_spider<NextP, F>(self, spider: F) -> Rules<S, D, NextP, O>
    where
        F: FnOnce(tx::Tx) -> NextP + Send + 'static,
    {
        Rules {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider: Box::new(spider),
            store: self.store,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            load: self.load,
        }
    }

    pub fn with_scheduler<NextS>(self, scheduler: NextS) -> Rules<NextS, D, P, O> {
        Rules {
            scheduler,
            downloader: self.downloader,
            spider: self.spider,
            store: self.store,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            load: self.load,
        }
    }

    pub fn with_downloader<NextD>(self, downloader: NextD) -> Rules<S, NextD, P, O> {
        Rules {
            scheduler: self.scheduler,
            downloader,
            spider: self.spider,
            store: self.store,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            load: self.load,
        }
    }

    pub fn with_store<NextO>(self, store: NextO) -> Rules<S, D, P, NextO> {
        Rules {
            scheduler: self.scheduler,
            downloader: self.downloader,
            spider: self.spider,
            store,
            config: self.config,
            registry: self.registry,
            schemas: self.schemas,
            ai: self.ai,
            middlewares: self.middlewares,
            load: self.load,
        }
    }

    /// Selects the Worker-local OpenAI provider used by every parsed Response.
    pub fn with_ai(mut self, openai: crate::ai::OpenAI) -> Self {
        self.ai = Some(Arc::new(openai));
        self
    }

    pub fn with_load<L>(mut self, load: L) -> Self
    where
        L: crate::load::Load + 'static,
    {
        self.load = Some(Arc::new(load));
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

    pub fn with_spider_middleware(mut self, spec: middleware::Spec) -> Self {
        self.middlewares.push(spec);
        self
    }
}

impl<S, D, P, O> Rules<S, D, P, O>
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
    D: downloader::Download + 'static,
    P: std::any::Any + harbor::Spider + 'static,
    O: crate::item::Store + 'static,
{
    pub fn build(self) -> Runtime<S, D, P, O> {
        let (tx, events) = tx::channel(MAX_EVENTS);
        let spider = Arc::new((self.spider)(tx));
        let validation_error = self.load.is_none().then(|| {
            validate_ai(&self.config, self.ai.as_deref())
                .or_else(|| validate_item_functions(spider.as_ref(), &self.config))
        });
        let validation_error = validation_error.flatten();
        let config = Arc::new(self.config);
        let schemas = self.schemas;
        let executor =
            super::executor::Executor::new(Arc::clone(&spider), schemas.clone(), self.ai.clone());
        let trace_id = config.next_trace_id();
        let load_enabled = self.load.is_some();
        let task_initializer = self.load.as_ref().map(|_| {
            Arc::new(super::runtime::task_initializer::<S>) as super::runtime::TaskInitializer<S>
        });
        let load_validator = self.load.as_ref().map(|_| {
            let spider = Arc::clone(&spider);
            let schemas = Arc::clone(&schemas);
            let ai = self.ai.clone();
            Arc::new(move |message: &crate::load::Message| {
                validate_loaded_message(message, spider.as_ref(), ai.as_deref(), schemas.as_ref())
            }) as super::runtime::TaskValidator
        });

        super::runtime::Runtime::new(super::runtime::Setup {
            scheduler: self.scheduler,
            downloader: self.downloader,
            executor,
            store: self.store,
            events,
            registry: self.registry,
            middlewares: self.middlewares,
            load: self.load,
            load_validator,
            task_initializer,
        })
        .with_init(Init::new(
            config,
            trace_id,
            schemas,
            validation_error,
            load_enabled,
        ))
    }
}

fn validate_ai(config: &config::Config, openai: Option<&crate::ai::OpenAI>) -> Option<String> {
    let uses_ai = config.graph.nodes.values().any(|node| {
        node.fields.values().any(|field| {
            field
                .extractors
                .iter()
                .any(|extractor| matches!(extractor, crate::graph::rules::Extractor::Ai { .. }))
        })
    });
    (uses_ai && openai.is_none())
        .then_some("Rules config uses an AI extractor but Engine has no AI provider".to_string())
}

fn validate_item_functions<P>(spider: &P, config: &config::Config) -> Option<String>
where
    P: harbor::Spider,
{
    for edge in &config.graph.edges {
        if !matches!(edge.kind, crate::graph::edge::Kind::Item) {
            continue;
        }
        let name = edge.function.as_deref().unwrap_or("item");
        if spider.item_fn(name).is_none() {
            return Some(format!(
                "Rules item function is not registered: spider={}, node={}, fn={name}",
                spider.name(),
                edge.from,
            ));
        }
    }
    None
}

fn validate_loaded_message<P>(
    message: &crate::load::Message,
    spider: &P,
    ai: Option<&crate::ai::OpenAI>,
    schemas: &crate::item::schema::Store,
) -> Result<(), crate::Error>
where
    P: harbor::Spider,
{
    message
        .validate()
        .map_err(|error| crate::Error::message(error.to_string()))?;
    let config = message
        .snapshot
        .dsl
        .as_ref()
        .ok_or_else(|| crate::Error::message("loaded task configuration must contain DSL"))?;
    if let Some(error) = validate_ai(config, ai) {
        return Err(crate::Error::message(error));
    }
    if let Some(error) = validate_item_functions(spider, config) {
        return Err(crate::Error::message(error));
    }
    if let Some(item) = &config.item {
        schemas.register(&item.schema).map_err(crate::Error::Item)?;
    }
    Ok(())
}

#[doc(hidden)]
pub struct Init {
    config: Arc<config::Config>,
    trace_id: String,
    schemas: Arc<crate::item::schema::Store>,
    validation_error: Option<String>,
    load_enabled: bool,
}

impl Init {
    fn new(
        config: Arc<config::Config>,
        trace_id: String,
        schemas: Arc<crate::item::schema::Store>,
        validation_error: Option<String>,
        load_enabled: bool,
    ) -> Self {
        Self {
            config,
            trace_id,
            schemas,
            validation_error,
            load_enabled,
        }
    }
}

impl<S> super::init::Init<S> for Init
where
    S: scheduler::Scheduler + scheduler::Init + 'static,
{
    async fn init(&self, scheduler: Arc<S>) -> Result<super::init::Output, crate::Error> {
        if let Some(error) = &self.validation_error {
            return Err(crate::Error::message(error.clone()));
        }
        if self.load_enabled {
            return Ok(super::init::Output::Consume);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net;

    struct ValidationSpider {
        tx: crate::harbor::tx::Tx,
    }

    impl harbor::Spider for ValidationSpider {
        type Item = crate::item::Map;

        fn name(&self) -> &str {
            "validation"
        }

        fn tx(&self) -> &crate::harbor::tx::Tx {
            &self.tx
        }

        async fn index(&self, _response: net::Response) -> Result<(), crate::Error> {
            Ok(())
        }
    }

    fn validation_spider() -> ValidationSpider {
        let (tx, _) = crate::harbor::tx::channel(super::super::runtime::MAX_EVENTS);
        ValidationSpider { tx }
    }

    #[tokio::test]
    async fn dynamic_rules_init_does_not_seed_the_static_configuration() {
        let config = Arc::new(
            config::Config::from_yaml(
                r#"
spider:
  name: books
  start: [{node: index, url: https://example.com/books}]
graph:
  nodes:
    index: {}
  edges: []
"#,
            )
            .unwrap(),
        );
        let init = Init::new(
            config,
            "trace-static".to_string(),
            Arc::new(crate::item::schema::Store::new()),
            None,
            true,
        );
        let scheduler = Arc::new(scheduler::Memory::new());

        let output = <Init as crate::engine::init::Init<scheduler::Memory>>::init(
            &init,
            Arc::clone(&scheduler),
        )
        .await
        .unwrap();

        assert_eq!(output, crate::engine::init::Output::Consume);
        assert_eq!(scheduler.trace_len(), 0);
        assert_eq!(scheduler.queued_len(), 0);
    }

    #[test]
    fn loaded_rules_require_the_worker_ai_provider_before_scheduler_init() {
        let config = config::Config::from_yaml(
            r#"
spider:
  name: ai-task
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index:
      fields:
        title:
          extractors:
            - {kind: ai, expr: "extract title"}
  edges: []
"#,
        )
        .unwrap();
        let message = crate::load::Message::new(
            "ai-task",
            "trace-ai",
            crate::trace::Snapshot::rules("ai-task", config),
        );

        let error = validate_loaded_message(
            &message,
            &validation_spider(),
            None,
            &crate::item::schema::Store::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("no AI provider"));
    }

    #[test]
    fn loaded_rules_require_registered_item_functions_before_scheduler_init() {
        let config = config::Config::from_yaml(
            r#"
spider:
  name: item-task
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges:
    - from: index
      kind: item
      fn: persist
item:
  schema:
    fields:
      title: {type: string}
"#,
        )
        .unwrap();
        let message = crate::load::Message::new(
            "item-task",
            "trace-item",
            crate::trace::Snapshot::rules("item-task", config),
        );

        let error = validate_loaded_message(
            &message,
            &validation_spider(),
            None,
            &crate::item::schema::Store::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("not registered"));
    }
}
