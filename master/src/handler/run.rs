use axum::extract::{State, rejection::JsonRejection};
use axum::http::HeaderMap;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value;

use crate::Error;
use crate::logic;
use crate::svc::Context;
use crate::types::run;

pub(super) fn router() -> Router<Context> {
    Router::new().route("/v1/worker/runs/init", post(init))
}

async fn init(
    State(app): State<Context>,
    _access: super::access::Worker,
    headers: HeaderMap,
    body: Result<Json<run::Init>, JsonRejection>,
) -> Result<Json<Value>, Error> {
    let body = super::extract::json(body)?;
    logic::run::init(&app, super::extract::operation(&headers)?, &body).await?;
    Ok(super::response::empty())
}
