use std::future::Future;

use super::tx::Tx;
use crate::{Error, net};

pub trait Spider: Send + Sync {
    type Item: crate::item::Item + 'static;

    fn name(&self) -> &str;

    fn tx(&self) -> &Tx;

    #[doc(hidden)]
    fn handler(&self, _node: &str) -> Option<net::Handler> {
        None
    }

    fn allowed_domains(&self) -> impl Future<Output = Vec<String>> + Send {
        async { Vec::new() }
    }

    fn start_urls(&self) -> impl Future<Output = Vec<String>> + Send {
        async { Vec::new() }
    }

    fn start(&self) -> impl Future<Output = Result<(), Error>> + Send {
        async move {
            let mut requests = Vec::new();

            for url in self.start_urls().await {
                let request =
                    net::Request::follow(url).map_err(|error| Error::message(error.to_string()))?;
                requests.push(request);
            }

            self.tx().request(requests).await
        }
    }

    fn index(&self, response: net::Response) -> impl Future<Output = Result<(), Error>> + Send;

    fn item(&self, item: Self::Item) -> impl Future<Output = Result<(), Error>> + Send {
        async move { self.tx().item(vec![item]).await }
    }

    #[doc(hidden)]
    fn item_fn(&self, name: &str) -> Option<crate::item::Function<Self>>
    where
        Self: Sized,
    {
        if name == "item" {
            Some(crate::item::Function::new("item", call_item::<Self>))
        } else {
            None
        }
    }
}

fn call_item<'a, S>(spider: &'a S, item: S::Item) -> net::BoxFuture<'a>
where
    S: Spider,
{
    Box::pin(spider.item(item))
}

pub trait SpiderFactory {
    type Spider: Spider + 'static;

    fn build(self, tx: Tx) -> Self::Spider;
}
