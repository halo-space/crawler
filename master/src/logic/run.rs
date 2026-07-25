use crate::Error;
use crate::svc::Context;
use crate::types::run;

pub(crate) async fn init(
    context: &Context,
    operation: &str,
    body: &run::Init,
) -> Result<(), Error> {
    context
        .store
        .init(context.config.namespace(), operation, body)
        .await
}
