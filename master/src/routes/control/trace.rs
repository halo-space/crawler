use axum::extract::{
    Path, Query, State,
    rejection::{PathRejection, QueryRejection},
};
use axum::routing::get;
use axum::{Json, Router};

use super::super::App;
use crate::Error;
use crate::control::{Page, trace};

pub(super) fn router() -> Router<App> {
    Router::new()
        .route("/v1/control/traces", get(list))
        .route("/v1/control/traces/{trace_id}", get(detail))
}

async fn list(
    State(app): State<App>,
    _access: super::super::access::Control,
    query: Result<Query<trace::List>, QueryRejection>,
) -> Result<Json<Page<trace::Summary>>, Error> {
    let namespace = app.config.namespace();
    let query = super::decode_query(query)?;
    super::bounded(
        app.store.traces(namespace, &query).await?,
        app.config.max_api_bytes(),
    )
}

async fn detail(
    State(app): State<App>,
    _access: super::super::access::Control,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<trace::Detail>, Error> {
    let namespace = app.config.namespace();
    let id = super::super::extract::path(path)?;
    let trace = app
        .store
        .trace_detail(namespace, &id)
        .await?
        .ok_or_else(|| Error::TraceNotFound(id))?;
    super::bounded(trace, app.config.max_api_bytes())
}
