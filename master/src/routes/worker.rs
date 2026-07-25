use axum::extract::{
    Path, State,
    rejection::{JsonRejection, PathRejection},
};
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;

use super::App;
use crate::{Error, wire};

pub(super) fn router() -> Router<App> {
    Router::new()
        .route("/v1/worker/policy", get(policy))
        .route("/v1/worker/runs/init", post(init))
        .route("/v1/worker/requests/push", post(push))
        .route("/v1/worker/items", post(items))
        .route("/v1/worker/traces/{trace_id}", get(trace))
        .route("/v1/worker/requests/claim", post(claim))
        .route("/v1/worker/requests/pending", post(pending))
        .route("/v1/worker/requests/ack", post(ack))
        .route("/v1/worker/requests/release", post(release))
        .route("/v1/worker/requests/refresh", post(refresh))
        .route("/v1/worker/requests/success", post(success))
        .route("/v1/worker/requests/failure", post(failure))
        .route("/v1/worker/heartbeat", post(heartbeat))
}

async fn policy(
    State(app): State<App>,
    _access: super::access::Worker,
) -> Result<Json<wire::Policy>, Error> {
    let policy = app.config.policy();
    Ok(Json(wire::Policy {
        lease_timeout_ms: policy.lease_timeout_ms,
        lease_interval_ms: policy.lease_interval_ms,
        heartbeat_interval_ms: policy.heartbeat_interval_ms,
        max_response_bytes: app.config.max_api_bytes() as u64,
    }))
}

async fn init(
    State(app): State<App>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<wire::Init>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store
        .init(namespace, super::extract::operation(&headers)?, &body)
        .await?;
    Ok(super::response::empty())
}

async fn push(
    State(app): State<App>,
    _access: super::access::Worker,
    body: Result<Json<wire::Push>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store.push(namespace, &body).await?;
    Ok(super::response::empty())
}

async fn items(
    State(app): State<App>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<wire::Items>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store
        .items(namespace, super::extract::operation(&headers)?, &body)
        .await?;
    Ok(super::response::empty())
}

async fn trace(
    State(app): State<App>,
    _access: super::access::Worker,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<Option<spider::trace::Snapshot>>, Error> {
    let namespace = app.config.namespace();
    let trace_id = super::extract::path(path)?;
    if trace_id.is_empty() || trace_id.chars().any(char::is_control) {
        return Err(Error::Invalid(
            "trace_id must not be empty or contain control characters".to_string(),
        ));
    }
    Ok(Json(app.store.trace(namespace, &trace_id).await?))
}

async fn claim(
    State(app): State<App>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<wire::Claim>, JsonRejection>,
) -> Result<Json<wire::Claims>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    Ok(Json(
        app.store
            .claim(namespace, super::extract::operation(&headers)?, &body)
            .await?,
    ))
}

async fn pending(
    State(app): State<App>,
    _access: super::access::Worker,
    body: Result<Json<wire::Worker>, JsonRejection>,
) -> Result<Json<wire::Pending>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    Ok(Json(wire::Pending {
        pending: app.store.pending(namespace, &body).await?,
    }))
}

async fn ack(
    State(app): State<App>,
    _access: super::access::Worker,
    identity: Result<Json<wire::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let identity = super::extract::json(identity)?;
    app.store.ack(namespace, &identity).await?;
    Ok(super::response::empty())
}

async fn release(
    State(app): State<App>,
    _access: super::access::Worker,
    headers: HeaderMap,
    identity: Result<Json<wire::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let identity = super::extract::json(identity)?;
    app.store
        .release(namespace, super::extract::operation(&headers)?, &identity)
        .await?;
    Ok(super::response::empty())
}

async fn refresh(
    State(app): State<App>,
    _access: super::access::Worker,
    identity: Result<Json<wire::Identity>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let identity = super::extract::json(identity)?;
    app.store.refresh(namespace, &identity).await?;
    Ok(super::response::empty())
}

async fn success(
    State(app): State<App>,
    _access: super::access::Worker,
    body: Result<Json<wire::Completion>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store.success(namespace, &body).await?;
    Ok(super::response::empty())
}

async fn failure(
    State(app): State<App>,
    _access: super::access::Worker,
    body: Result<Json<wire::Completion>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store.failure(namespace, &body).await?;
    Ok(super::response::empty())
}

async fn heartbeat(
    State(app): State<App>,
    _access: super::access::Worker,
    body: Result<Json<wire::Heartbeat>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let namespace = app.config.namespace();
    let body = super::extract::json(body)?;
    app.store.heartbeat(namespace, &body).await?;
    Ok(super::response::empty())
}
