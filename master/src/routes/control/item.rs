use axum::extract::{
    Path, Query, State,
    rejection::{PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use super::super::App;
use crate::Error;
use crate::control::{Page, item};

pub(super) fn router() -> Router<App> {
    Router::new()
        .route("/v1/control/items", get(list))
        .route("/v1/control/items/{row_id}", get(detail))
}

async fn list(
    State(app): State<App>,
    _access: super::super::access::Control,
    query: Result<Query<item::List>, QueryRejection>,
) -> Result<Json<Page<item::Summary>>, Error> {
    let namespace = app.config.namespace();
    let query = super::decode_query(query)?;
    super::bounded(
        app.store.item_list(namespace, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<App>,
    _access: super::super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<item::Detail>, Error> {
    let namespace = app.config.namespace();
    let id = super::super::extract::path(path)?;
    let item = super::found(app.store.item_detail(namespace, &id).await?, "Item", id)?;
    super::bounded(item, app.config.max_api_bytes())
}
