use axum::Json;
use serde::Serialize;
use serde_json::{Value, json};

use crate::Error;

pub(super) fn empty() -> Json<Value> {
    Json(json!({}))
}

pub(super) fn found<T>(value: Option<T>, kind: &'static str, id: String) -> Result<T, Error> {
    value.ok_or(Error::Missing { kind, id })
}

pub(super) fn bounded<T>(value: T, max: usize) -> Result<Json<T>, Error>
where
    T: Serialize,
{
    if serde_json::to_vec(&value)?.len() > max {
        return Err(Error::ResponseTooLarge { max });
    }
    Ok(Json(value))
}

pub(super) async fn not_found() -> Error {
    Error::NotFound
}

pub(super) async fn method_not_allowed() -> Error {
    Error::MethodNotAllowed
}
