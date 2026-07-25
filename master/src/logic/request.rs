use crate::Error;
use crate::svc::Context;
use crate::types::{Page, request};

pub(crate) async fn list(
    context: &Context,
    query: &request::List,
) -> Result<Page<request::Summary>, Error> {
    context
        .store
        .requests(context.config.namespace(), query)
        .await
}

pub(crate) async fn detail(context: &Context, id: &str) -> Result<Option<request::Detail>, Error> {
    context
        .store
        .request_detail(context.config.namespace(), id)
        .await
}

pub(crate) async fn push(context: &Context, body: &request::Push) -> Result<(), Error> {
    context.store.push(context.config.namespace(), body).await
}

pub(crate) async fn claim(
    context: &Context,
    operation: &str,
    body: &request::Claim,
) -> Result<request::Claims, Error> {
    context
        .store
        .claim(context.config.namespace(), operation, body)
        .await
}

pub(crate) async fn pending(
    context: &Context,
    body: &crate::types::worker::Worker,
) -> Result<bool, Error> {
    context
        .store
        .pending(context.config.namespace(), body)
        .await
}

pub(crate) async fn ack(context: &Context, identity: &request::Identity) -> Result<(), Error> {
    context
        .store
        .ack(context.config.namespace(), identity)
        .await
}

pub(crate) async fn release(
    context: &Context,
    operation: &str,
    identity: &request::Identity,
) -> Result<(), Error> {
    context
        .store
        .release(context.config.namespace(), operation, identity)
        .await
}

pub(crate) async fn refresh(context: &Context, identity: &request::Identity) -> Result<(), Error> {
    context
        .store
        .refresh(context.config.namespace(), identity)
        .await
}

pub(crate) async fn success(context: &Context, body: &request::Completion) -> Result<(), Error> {
    context
        .store
        .success(context.config.namespace(), body)
        .await
}

pub(crate) async fn failure(context: &Context, body: &request::Completion) -> Result<(), Error> {
    context
        .store
        .failure(context.config.namespace(), body)
        .await
}
