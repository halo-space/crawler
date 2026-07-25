use axum::extract::{
    Path, Query, State,
    rejection::{JsonRejection, PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use super::super::App;
use crate::Error;
use crate::control::{Page, task};

pub(super) fn router() -> Router<App> {
    Router::new()
        .route("/v1/control/tasks", get(list))
        .route("/v1/control/tasks/{task_id}", get(detail).put(put))
}

async fn put(
    State(app): State<App>,
    _access: super::super::access::Control,
    path: Result<Path<String>, PathRejection>,
    body: Result<Json<crate::store::Task>, JsonRejection>,
) -> Result<Json<serde_json::Value>, Error> {
    let namespace = app.config.namespace();
    let task_id = super::super::extract::path(path)?;
    let task = super::super::extract::json(body)?;
    if task_id != task.id {
        return Err(Error::Invalid(
            "Task path id must match the request body id".to_string(),
        ));
    }
    app.store.upsert_task(namespace, &task).await?;
    Ok(super::super::response::empty())
}

async fn list(
    State(app): State<App>,
    _access: super::super::access::Control,
    query: Result<Query<task::List>, QueryRejection>,
) -> Result<Json<Page<task::Summary>>, Error> {
    let namespace = app.config.namespace();
    let query = super::decode_query(query)?;
    super::bounded(
        app.store.tasks(namespace, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<App>,
    _access: super::super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<task::Detail>, Error> {
    let namespace = app.config.namespace();
    let id = super::super::extract::path(path)?;
    let task = super::found(app.store.task(namespace, &id).await?, "Task", id)?;
    super::bounded(task, app.config.max_api_bytes())
}
