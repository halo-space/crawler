use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::{Error, net};

pub type BoxFuture<'a> = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>;
pub type HandlerFn<S> = for<'a> fn(&'a S, net::Response) -> BoxFuture<'a>;
type Invoke =
    dyn for<'a> Fn(&'a (dyn Any + Send + Sync), net::Response) -> BoxFuture<'a> + Send + Sync;

#[derive(Clone)]
pub struct Handler {
    name: &'static str,
    invoke: Arc<Invoke>,
}

impl Handler {
    pub fn new<S>(name: &'static str, function: HandlerFn<S>) -> Self
    where
        S: Any + Send + Sync + 'static,
    {
        Self {
            name,
            invoke: erase(name, function),
        }
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub async fn call(
        &self,
        spider: &(dyn Any + Send + Sync),
        response: net::Response,
    ) -> Result<(), Error> {
        (self.invoke)(spider, response).await
    }
}

fn erase<S>(name: &'static str, function: HandlerFn<S>) -> Arc<Invoke>
where
    S: Any + Send + Sync + 'static,
{
    Arc::new(move |spider, response| call(name, function, spider, response))
}

fn call<'a, S>(
    name: &'static str,
    function: HandlerFn<S>,
    spider: &'a (dyn Any + Send + Sync),
    response: net::Response,
) -> BoxFuture<'a>
where
    S: Any + Send + Sync + 'static,
{
    let Some(spider) = spider.downcast_ref::<S>() else {
        return Box::pin(async move {
            Err(Error::message(format!(
                "handler {name} does not belong to current spider"
            )))
        });
    };

    function(spider, response)
}
