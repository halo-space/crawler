use axum::Json;
use serde_json::{Value, json};

use crate::Error;

pub(super) fn empty() -> Json<Value> {
    Json(json!({}))
}

pub(super) async fn not_found() -> Error {
    Error::NotFound
}

pub(super) async fn method_not_allowed() -> Error {
    Error::MethodNotAllowed
}
