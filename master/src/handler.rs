use axum::Router;

use crate::svc::Context;

mod access;
mod extract;
mod item;
mod request;
mod response;
mod run;
mod task;
mod trace;
mod worker;

#[cfg(test)]
#[path = "handler/control_tests.rs"]
mod control_tests;

#[cfg(test)]
#[path = "handler/tests.rs"]
mod tests;

pub(crate) fn build(config: crate::Config, store: crate::store::MySql) -> Router {
    let max_size = config.api().max_size;
    let context = Context::new(config, store);
    Router::new()
        .merge(router())
        .fallback(response::not_found)
        .method_not_allowed_fallback(response::method_not_allowed)
        .layer(axum::extract::DefaultBodyLimit::max(max_size))
        .with_state(context)
}

fn router() -> Router<Context> {
    Router::new()
        .merge(run::router())
        .merge(task::router())
        .merge(trace::router())
        .merge(request::router())
        .merge(worker::router())
        .merge(item::router())
}
