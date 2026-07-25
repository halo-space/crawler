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
use crate::types::{Page, request, worker};

pub(super) fn router() -> Router<Context> {
    Router::new()
        .route("/v1/control/requests", get(list))
        .route("/v1/control/requests/{request_id}", get(detail))
        .route("/v1/worker/requests/push", post(push))
        .route("/v1/worker/requests/claim", post(claim))
        .route("/v1/worker/requests/pending", post(pending))
        .route("/v1/worker/requests/ack", post(ack))
        .route("/v1/worker/requests/release", post(release))
        .route("/v1/worker/requests/refresh", post(refresh))
        .route("/v1/worker/requests/success", post(success))
        .route("/v1/worker/requests/failure", post(failure))
}

async fn push(
    State(app): State<Context>,
    _access: super::access::Worker,
    body: Result<Json<request::Push>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::request::push(&app, &body).await?;
    Ok(super::response::empty())
}

async fn claim(
    State(app): State<Context>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<request::Claim>, JsonRejection>,
) -> Result<Json<request::Claims>, Error> {
    let body = super::extract::json(body)?;
    Ok(Json(
        logic::request::claim(&app, super::extract::operation(&headers)?, &body).await?,
    ))
}

async fn pending(
    State(app): State<Context>,
    _access: super::access::Worker,
    body: Result<Json<worker::Worker>, JsonRejection>,
) -> Result<Json<request::Pending>, Error> {
    let body = super::extract::json(body)?;
    Ok(Json(request::Pending {
        pending: logic::request::pending(&app, &body).await?,
    }))
}

async fn ack(
    State(app): State<Context>,
    _access: super::access::Worker,
    identity: Result<Json<request::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let identity = super::extract::json(identity)?;
    logic::request::ack(&app, &identity).await?;
    Ok(super::response::empty())
}

async fn release(
    State(app): State<Context>,
    _access: super::access::Worker,
    headers: HeaderMap,
    identity: Result<Json<request::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let identity = super::extract::json(identity)?;
    logic::request::release(&app, super::extract::operation(&headers)?, &identity).await?;
    Ok(super::response::empty())
}

async fn refresh(
    State(app): State<Context>,
    _access: super::access::Worker,
    identity: Result<Json<request::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let identity = super::extract::json(identity)?;
    logic::request::refresh(&app, &identity).await?;
    Ok(super::response::empty())
}

async fn success(
    State(app): State<Context>,
    _access: super::access::Worker,
    body: Result<Json<request::Completion>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::request::success(&app, &body).await?;
    Ok(super::response::empty())
}

async fn failure(
    State(app): State<Context>,
    _access: super::access::Worker,
    body: Result<Json<request::Completion>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::request::failure(&app, &body).await?;
    Ok(super::response::empty())
}

async fn list(
    State(app): State<Context>,
    _access: super::access::Control,
    query: Result<Query<request::List>, QueryRejection>,
) -> Result<Json<Page<request::Summary>>, Error> {
    let query = super::extract::query(query)?;
    super::response::bounded(
        logic::request::list(&app, &query).await?,
        app.config.api().max_size,
    )
}

async fn detail(
    State(app): State<Context>,
    _access: super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<request::Detail>, Error> {
    let id = super::extract::path(path)?;
    let request = logic::request::detail(&app, &id)
        .await?
        .ok_or_else(|| Error::RequestNotFound(id))?;
    super::response::bounded(request, app.config.api().max_size)
}
