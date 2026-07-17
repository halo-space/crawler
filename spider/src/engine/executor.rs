use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use crate::engine::contract::Execute;
use crate::{net, spider};

/// The single runtime executor for both code and Rules Traces.
pub struct Executor<P> {
    spider: Arc<P>,
    schemas: Arc<crate::item::schema::Store>,
}

impl<P> Executor<P> {
    pub(crate) fn new(spider: Arc<P>, schemas: Arc<crate::item::schema::Store>) -> Self {
        Self { spider, schemas }
    }
}

impl<P> Execute for Executor<P>
where
    P: Any + spider::Spider + Send + Sync + 'static,
{
    #[allow(clippy::manual_async_fn)]
    fn start(&self) -> impl Future<Output = Result<(), crate::Error>> + Send {
        async move { self.spider.start().await }
    }

    async fn allowed_domains(&self, request: &net::Request) -> Vec<String> {
        let Some(snapshot) = request.snapshot() else {
            return self.spider.allowed_domains().await;
        };
        let Some(config) = snapshot.dsl.as_ref() else {
            return self.spider.allowed_domains().await;
        };
        let Some(node) = config.graph.nodes.get(request.node_key()) else {
            return config.spider.allowed_domains.clone();
        };
        node.allowed_domains
            .clone()
            .unwrap_or_else(|| config.spider.allowed_domains.clone())
    }

    fn validate(&self, request: &net::Request) -> Result<(), crate::Error> {
        let Some(snapshot) = request.snapshot() else {
            return self.validate_code(request);
        };
        if let Some(config) = snapshot.dsl.as_ref() {
            if !config.graph.nodes.contains_key(request.node_key()) {
                return Err(crate::Error::message(format!(
                    "rules node does not exist: {}",
                    request.node_key()
                )));
            }
            Ok(())
        } else {
            self.validate_code(request)
        }
    }

    fn parse(
        &self,
        request: net::Request,
        response: net::Response,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send {
        let spider = self.spider.clone();
        let schemas = self.schemas.clone();
        async move {
            let Some(snapshot) = request.snapshot().cloned() else {
                return Self::parse_code(spider.as_ref(), &request, response).await;
            };
            let Some(config) = snapshot.dsl.as_ref() else {
                return Self::parse_code(spider.as_ref(), &request, response).await;
            };

            // Rules Requests always pass through the same Rust business entry
            // before the declarative node is interpreted.
            spider.index(response.clone()).await?;
            crate::engine::rules::executor::execute(
                spider.as_ref(),
                schemas,
                config,
                &request,
                &response,
            )
            .await
        }
    }
}

impl<P> Executor<P>
where
    P: Any + spider::Spider + Send + Sync + 'static,
{
    fn validate_code(&self, request: &net::Request) -> Result<(), crate::Error> {
        if request.node_key() != "index" && self.spider.handler(request.node_key()).is_none() {
            return Err(crate::Error::message(format!(
                "code node is not registered by current spider: {}",
                request.node_key()
            )));
        }
        Ok(())
    }

    async fn parse_code(
        spider: &P,
        request: &net::Request,
        response: net::Response,
    ) -> Result<(), crate::Error> {
        if let Some(handler) = spider.handler(request.node_key()) {
            handler.call(spider, response).await
        } else if request.node_key() == "index" {
            spider.index(response).await
        } else {
            Err(crate::Error::message(format!(
                "code node is not registered by current spider: {}",
                request.node_key()
            )))
        }
    }
}
