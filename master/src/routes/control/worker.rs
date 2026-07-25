use axum::extract::{Query, State, rejection::QueryRejection};
use axum::routing::get;
use axum::{Json, Router};

use super::super::App;
use crate::Error;
use crate::control::{Page, worker};

pub(super) fn router() -> Router<App> {
    Router::new().route("/v1/control/workers", get(list))
}

async fn list(
    State(app): State<App>,
    _access: super::super::access::Control,
    query: Result<Query<worker::List>, QueryRejection>,
) -> Result<Json<Page<worker::Summary>>, Error> {
    let namespace = app.config.namespace();
    let query = super::decode_query(query)?;
    super::bounded(
        app.store.workers(namespace, &query).await?,
        app.config.max_api_bytes(),
    )
}
