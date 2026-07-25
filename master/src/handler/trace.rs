use axum::extract::{
    Path, Query, State,
    rejection::{PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use crate::Error;
use crate::logic;
use crate::svc::Context;
use crate::types::{Page, trace};

pub(super) fn router() -> Router<Context> {
    Router::new()
        .route("/v1/control/traces", get(list))
        .route("/v1/control/traces/{trace_id}", get(detail))
        .route("/v1/worker/traces/{trace_id}", get(snapshot))
}

async fn snapshot(
    State(app): State<Context>,
    _access: super::access::Worker,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<Option<spider::trace::Snapshot>>, Error> {
    let id = super::extract::path(path)?;
    Ok(Json(logic::trace::snapshot(&app, &id).await?))
}

async fn list(
    State(app): State<Context>,
    _access: super::access::Control,
    query: Result<Query<trace::List>, QueryRejection>,
) -> Result<Json<Page<trace::Summary>>, Error> {
    let query = super::extract::query(query)?;
    super::response::bounded(
        logic::trace::list(&app, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<Context>,
    _access: super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<trace::Detail>, Error> {
    let id = super::extract::path(path)?;
    let trace = logic::trace::detail(&app, &id)
        .await?
        .ok_or_else(|| Error::TraceNotFound(id))?;
    super::response::bounded(trace, app.config.max_api_bytes())
}
