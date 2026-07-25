use crate::Error;
use crate::svc::Context;
use crate::types::{Page, task};

pub(crate) async fn put(context: &Context, path_id: &str, value: task::Task) -> Result<(), Error> {
    if path_id != value.id {
        return Err(Error::Invalid(
            "Task path id must match the request body id".to_string(),
        ));
    }
    context
        .store
        .upsert_task(context.config.namespace(), &value)
        .await
}

pub(crate) async fn list(
    context: &Context,
    query: &task::List,
) -> Result<Page<task::Summary>, Error> {
    context.store.tasks(context.config.namespace(), query).await
}

pub(crate) async fn detail(context: &Context, id: &str) -> Result<Option<task::Detail>, Error> {
    context.store.task(context.config.namespace(), id).await
}
