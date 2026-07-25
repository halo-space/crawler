use axum::{
    extract::rejection::{JsonRejection, PathRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("invalid request: {message}")]
    Rejected { status: StatusCode, message: String },
    #[error("request body exceeds the configured limit")]
    PayloadTooLarge,
    #[error("response exceeds the configured {max} byte limit")]
    ResponseTooLarge { max: usize },
    #[error("invalid Trace Snapshot {id}: {message}")]
    InvalidTrace { id: String, message: String },
    #[error("unauthorized")]
    Unauthorized,
    #[error("route not found")]
    NotFound,
    #[error("{kind} not found: {id}")]
    Missing { kind: &'static str, id: String },
    #[error("method not allowed")]
    MethodNotAllowed,
    #[error("request not found: {0}")]
    RequestNotFound(String),
    #[error("Trace Snapshot not found: {0}")]
    TraceNotFound(String),
    #[error("operation conflict: {0}")]
    Conflict(String),
    #[error("identity mismatch for request {id}: {field}")]
    Identity { id: String, field: &'static str },
    #[error("lease mismatch: {0}")]
    Lease(String),
    #[error("lease expired: {0}")]
    LeaseExpired(String),
    #[error("not acknowledged: {0}")]
    NotAcknowledged(String),
    #[error("version mismatch: {0}")]
    Version(String),
    #[error("state mismatch: {0}")]
    State(String),
    #[error("unavailable: {0}")]
    Unavailable(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct ErrorDetail {
    pub code: String,
    pub id: Option<String>,
    pub field: Option<String>,
    pub message: String,
}

impl Error {
    pub(crate) fn json(rejection: JsonRejection) -> Self {
        if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::PayloadTooLarge
        } else {
            Self::Rejected {
                status: rejection.status(),
                message: rejection.body_text(),
            }
        }
    }

    pub(crate) fn path(rejection: PathRejection) -> Self {
        Self::Rejected {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }

    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Config(_) => "message",
            Self::Database(_) | Self::Migration(_) => "unavailable",
            Self::Serialization(_)
            | Self::Invalid(_)
            | Self::Rejected { .. }
            | Self::PayloadTooLarge => "invalid_request",
            Self::ResponseTooLarge { .. } => "response_too_large",
            Self::InvalidTrace { .. } => "invalid_trace",
            Self::Unauthorized => "unauthorized",
            Self::NotFound => "route_not_found",
            Self::Missing { .. } => "not_found",
            Self::MethodNotAllowed => "message",
            Self::RequestNotFound(_) => "request_not_found",
            Self::TraceNotFound(_) => "trace_not_found",
            Self::Conflict(_) => "operation_conflict",
            Self::Identity { .. } => "identity_mismatch",
            Self::Lease(_) => "lease_mismatch",
            Self::LeaseExpired(_) => "lease_expired",
            Self::NotAcknowledged(_) => "not_acknowledged",
            Self::Version(_) => "version_mismatch",
            Self::State(_) => "state_mismatch",
            Self::Unavailable(_) => "unavailable",
        }
    }

    pub(crate) fn status(&self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound
            | Self::Missing { .. }
            | Self::RequestNotFound(_)
            | Self::TraceNotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Database(_) | Self::Migration(_) | Self::Unavailable(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Config(_)
            | Self::Serialization(_)
            | Self::Invalid(_)
            | Self::InvalidTrace { .. } => StatusCode::BAD_REQUEST,
            Self::Rejected { status, .. } => *status,
            Self::PayloadTooLarge | Self::ResponseTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::Identity { .. }
            | Self::Lease(_)
            | Self::LeaseExpired(_)
            | Self::NotAcknowledged(_)
            | Self::Version(_)
            | Self::State(_) => StatusCode::CONFLICT,
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Database(_) | Self::Migration(_) | Self::Unavailable(_) => {
                "Master is temporarily unavailable".to_string()
            }
            Self::Serialization(_) => "invalid request payload".to_string(),
            Self::PayloadTooLarge => "request body exceeds the configured limit".to_string(),
            Self::ResponseTooLarge { max } => {
                format!("response exceeds the configured {max} byte limit")
            }
            Self::InvalidTrace { message, .. } => message.clone(),
            Self::Rejected { message, .. }
            | Self::Config(message)
            | Self::Invalid(message)
            | Self::Conflict(message) => message.clone(),
            Self::Unauthorized => "unauthorized".to_string(),
            Self::NotFound => "route not found".to_string(),
            Self::Missing { kind, .. } => format!("{kind} not found"),
            Self::MethodNotAllowed => "method not allowed".to_string(),
            Self::RequestNotFound(_) => "request not found".to_string(),
            Self::TraceNotFound(_) => "Trace Snapshot not found".to_string(),
            Self::Identity { .. } => {
                "request identity does not match the claimed execution".to_string()
            }
            Self::Lease(_) => "request lease does not match the claimed execution".to_string(),
            Self::LeaseExpired(_) => "request lease has expired".to_string(),
            Self::NotAcknowledged(_) => "request execution has not been acknowledged".to_string(),
            Self::Version(_) => "request version does not match the claimed execution".to_string(),
            Self::State(_) => "request state does not allow this operation".to_string(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = axum::Json(ErrorBody {
            error: ErrorDetail {
                code: self.code().to_string(),
                id: error_id(&self),
                field: error_field(&self).map(str::to_string),
                message: self.message(),
            },
        });
        (status, body).into_response()
    }
}

fn error_id(error: &Error) -> Option<String> {
    match error {
        Error::RequestNotFound(id)
        | Error::TraceNotFound(id)
        | Error::Lease(id)
        | Error::LeaseExpired(id)
        | Error::NotAcknowledged(id)
        | Error::Version(id)
        | Error::State(id) => Some(id.clone()),
        Error::InvalidTrace { id, .. } => Some(id.clone()),
        Error::Identity { id, .. } => Some(id.clone()),
        Error::Missing { id, .. } => Some(id.clone()),
        _ => None,
    }
}

fn error_field(error: &Error) -> Option<&'static str> {
    match error {
        Error::Identity { field, .. } => Some(field),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;

    use super::*;

    #[tokio::test]
    async fn envelope_uses_machine_fields_without_leaking_database_errors() {
        let response = Error::Identity {
            id: "request-1".to_string(),
            field: "task_id",
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::CONFLICT);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body.error.code, "identity_mismatch");
        assert_eq!(body.error.id.as_deref(), Some("request-1"));
        assert_eq!(body.error.field.as_deref(), Some("task_id"));
        assert_eq!(
            body.error.message,
            "request identity does not match the claimed execution"
        );
    }

    #[tokio::test]
    async fn unavailable_errors_do_not_expose_storage_details() {
        let response = Error::Unavailable("mysql password=secret".to_string()).into_response();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body.error.code, "unavailable");
        assert!(!body.error.message.contains("secret"));
    }

    #[tokio::test]
    async fn invalid_trace_keeps_its_trace_identity() {
        let response = Error::InvalidTrace {
            id: "trace-1".to_string(),
            message: "snapshot is malformed".to_string(),
        }
        .into_response();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(body.error.code, "invalid_trace");
        assert_eq!(body.error.id.as_deref(), Some("trace-1"));
        assert_eq!(body.error.message, "snapshot is malformed");
    }

    #[tokio::test]
    async fn resource_and_route_absence_have_distinct_codes() {
        let response = Error::Missing {
            kind: "Task",
            id: "task-1".to_string(),
        }
        .into_response();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.error.code, "not_found");
        assert_eq!(body.error.id.as_deref(), Some("task-1"));
        assert_eq!(body.error.message, "Task not found");

        let response = Error::NotFound.into_response();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.error.code, "route_not_found");
        assert_eq!(body.error.id, None);
    }

    #[tokio::test]
    async fn response_limit_has_its_own_machine_code() {
        let response = Error::ResponseTooLarge { max: 64 }.into_response();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body.error.code, "response_too_large");
        assert_eq!(
            body.error.message,
            "response exceeds the configured 64 byte limit"
        );
    }
}
