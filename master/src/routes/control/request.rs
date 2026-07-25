use axum::extract::{
    Path, Query, State,
    rejection::{PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use super::super::App;
use crate::Error;
use crate::control::{Page, request};

pub(super) fn router() -> Router<App> {
    Router::new()
        .route("/v1/control/requests", get(list))
        .route("/v1/control/requests/{request_id}", get(detail))
}

async fn list(
    State(app): State<App>,
    _access: super::super::access::Control,
    query: Result<Query<request::List>, QueryRejection>,
) -> Result<Json<Page<request::Summary>>, Error> {
    let namespace = app.config.namespace();
    let query = super::decode_query(query)?;
    super::bounded(
        app.store.requests(namespace, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<App>,
    _access: super::super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<request::Detail>, Error> {
    let namespace = app.config.namespace();
    let id = super::super::extract::path(path)?;
    let request = app
        .store
        .request_detail(namespace, &id)
        .await?
        .ok_or_else(|| Error::RequestNotFound(id))?;
    super::bounded(request, app.config.max_api_bytes())
}
