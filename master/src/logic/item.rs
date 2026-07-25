use crate::Error;
use crate::svc::Context;
use crate::types::{Page, item};

pub(crate) async fn list(
    context: &Context,
    query: &item::List,
) -> Result<Page<item::Summary>, Error> {
    context
        .store
        .item_list(context.config.namespace(), query)
        .await
}

pub(crate) async fn detail(context: &Context, id: &str) -> Result<Option<item::Detail>, Error> {
    context
        .store
        .item_detail(context.config.namespace(), id)
        .await
}

pub(crate) async fn submit(
    context: &Context,
    operation: &str,
    body: &item::Items,
) -> Result<(), Error> {
    context
        .store
        .items(context.config.namespace(), operation, body)
        .await
}
