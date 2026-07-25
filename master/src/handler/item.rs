use axum::extract::{
    Path, Query, State,
    rejection::{JsonRejection, PathRejection, QueryRejection},
};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;

use crate::Error;
use crate::logic;
use crate::svc::Context;
use crate::types::{Page, item};

pub(super) fn router() -> Router<Context> {
    Router::new()
        .route("/v1/control/items", get(list))
        .route("/v1/control/items/{row_id}", get(detail))
        .route("/v1/worker/items", post(submit))
}

async fn submit(
    State(app): State<Context>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<item::Items>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::item::submit(&app, super::extract::operation(&headers)?, &body).await?;
    Ok(super::response::empty())
}

async fn list(
    State(app): State<Context>,
    _access: super::access::Control,
    query: Result<Query<item::List>, QueryRejection>,
) -> Result<Json<Page<item::Summary>>, Error> {
    let query = super::extract::query(query)?;
    super::response::bounded(
        logic::item::list(&app, &query).await?,
        app.config.api().max_size,
    )
}

async fn detail(
    State(app): State<Context>,
    _access: super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<item::Detail>, Error> {
    let id = super::extract::path(path)?;
    let item = super::response::found(logic::item::detail(&app, &id).await?, "Item", id)?;
    super::response::bounded(item, app.config.api().max_size)
}
