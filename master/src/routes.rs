use std::sync::Arc;

use axum::Router;
use axum::extract::DefaultBodyLimit;

use crate::Config;
use crate::store::MySql;

mod access;
mod control;
mod extract;
mod response;
mod worker;

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct App {
    config: Arc<Config>,
    store: MySql,
}

pub(crate) fn build(config: Config, store: MySql) -> Router {
    let max_api_bytes = config.max_api_bytes();
    let app = App {
        config: Arc::new(config),
        store,
    };
    Router::new()
        .merge(worker::router())
        .merge(control::router())
        .fallback(response::not_found)
        .method_not_allowed_fallback(response::method_not_allowed)
        .layer(DefaultBodyLimit::max(max_api_bytes))
        .with_state(app)
}
