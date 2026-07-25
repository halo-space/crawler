use axum::Json;
use axum::extract::{Path, rejection::JsonRejection, rejection::PathRejection};
use axum::http::HeaderMap;

use crate::Error;

pub(super) fn json<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, Error> {
    body.map(|Json(value)| value).map_err(Error::json)
}

pub(super) fn path(path: Result<Path<String>, PathRejection>) -> Result<String, Error> {
    path.map(|Path(value)| value).map_err(Error::path)
}

pub(super) fn operation(headers: &HeaderMap) -> Result<&str, Error> {
    headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::Invalid("Idempotency-Key is required".to_string()))
}
