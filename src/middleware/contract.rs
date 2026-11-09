use std::future::Future;
use std::pin::Pin;

use crate::{item, middleware::Spec, net};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Next<T> {
    Continue(T),
    Skip,
}

pub type BoxFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, crate::middleware::Error>> + Send + 'a>>;

pub trait Middleware: Send + Sync {
    fn order(&self, _hook: &str) -> i32 {
        0
    }

    fn before_spider<'a>(&'a self, _spec: &'a Spec) -> BoxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn after_spider<'a>(&'a self, _spec: &'a Spec) -> BoxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn before_scheduler<'a>(
        &'a self,
        request: net::Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Request>> {
        Box::pin(async { Ok(Next::Continue(request)) })
    }

    fn before_download<'a>(
        &'a self,
        request: net::Request,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Request>> {
        Box::pin(async { Ok(Next::Continue(request)) })
    }

    fn after_download<'a>(
        &'a self,
        response: net::Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Response>> {
        Box::pin(async { Ok(Next::Continue(response)) })
    }

    fn before_parse<'a>(
        &'a self,
        response: net::Response,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<net::Response>> {
        Box::pin(async { Ok(Next::Continue(response)) })
    }

    fn before_item<'a>(
        &'a self,
        item: Box<dyn item::Item>,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, Next<Box<dyn item::Item>>> {
        Box::pin(async { Ok(Next::Continue(item)) })
    }

    fn error_download<'a>(
        &'a self,
        _request: &'a net::Request,
        _error: &'a str,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn error_parse<'a>(
        &'a self,
        _response: &'a net::Response,
        _error: &'a str,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn error_item<'a>(
        &'a self,
        _item: &'a dyn item::Item,
        _error: &'a str,
        _spec: &'a Spec,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}
