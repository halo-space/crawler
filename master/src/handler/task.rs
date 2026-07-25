use axum::extract::{
    Path, Query, State,
    rejection::{JsonRejection, PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use crate::Error;
use crate::logic;
use crate::svc::Context;
use crate::types::{Page, task};

pub(super) fn router() -> Router<Context> {
    Router::new()
        .route("/v1/control/tasks", get(list))
        .route("/v1/control/tasks/{task_id}", get(detail).put(put))
}

async fn put(
    State(app): State<Context>,
    _access: super::access::Control,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<task::Task>, JsonRejection>,
) -> Result<Json<serde_json::Value>, Error> {
    let task_id = super::extract::path(path)?;
    let task = super::extract::json(body)?;
    logic::task::put(&app, &task_id, task).await?;
    Ok(super::response::empty())
}

async fn list(
    State(app): State<Context>,
    _access: super::access::Control,
    query: Result<Query<task::List>, QueryRejection>,
) -> Result<Json<Page<task::Summary>>, Error> {
    let query = super::extract::query(query)?;
    super::response::bounded(
        logic::task::list(&app, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<Context>,
    _access: super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<task::Detail>, Error> {
    let id = super::extract::path(path)?;
    let task = super::response::found(logic::task::detail(&app, &id).await?, "Task", id)?;
    super::response::bounded(task, app.config.max_api_bytes())
}
