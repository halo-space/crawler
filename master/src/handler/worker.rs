use axum::extract::{Query, State, rejection::JsonRejection, rejection::QueryRejection};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;

use crate::Error;
use crate::logic;
use crate::svc::Context;
use crate::types::{Page, worker};

pub(super) fn router() -> Router<Context> {
    Router::new()
        .route("/v1/control/workers", get(list))
        .route("/v1/worker/policy", get(policy))
        .route("/v1/worker/heartbeat", post(heartbeat))
}

async fn list(
    State(app): State<Context>,
    _access: super::access::Control,
    query: Result<Query<worker::List>, QueryRejection>,
) -> Result<Json<Page<worker::Summary>>, Error> {
    let query = super::extract::query(query)?;
    super::response::bounded(
        logic::worker::list(&app, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn policy(
    State(app): State<Context>,
    _access: super::access::Worker,
) -> Result<Json<worker::Policy>, Error> {
    Ok(Json(logic::worker::policy(&app)))
}

async fn heartbeat(
    State(app): State<Context>,
    _access: super::access::Worker,
    body: Result<Json<worker::Heartbeat>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::worker::heartbeat(&app, &body).await?;
    Ok(super::response::empty())
}
