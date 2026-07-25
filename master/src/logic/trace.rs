use crate::Error;
use crate::svc::Context;
use crate::types::{Page, trace};

pub(crate) async fn list(
    context: &Context,
    query: &trace::List,
) -> Result<Page<trace::Summary>, Error> {
    context
        .store
        .traces(context.config.namespace(), query)
        .await
}

pub(crate) async fn detail(context: &Context, id: &str) -> Result<Option<trace::Detail>, Error> {
    context
        .store
        .trace_detail(context.config.namespace(), id)
        .await
}

pub(crate) async fn snapshot(
    context: &Context,
    id: &str,
) -> Result<Option<spider::trace::Snapshot>, Error> {
    if id.is_empty() || id.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "trace_id must not be empty or contain control characters".to_string(),
        ));
    }
    context.store.trace(context.config.namespace(), id).await
}
