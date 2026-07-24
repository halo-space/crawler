use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("request {field} mismatch: {id}")]
    IdentityMismatch { id: String, field: &'static str },

    #[error("request is not leased by worker: {0}")]
    LeaseMismatch(String),

    #[error("request lease expired: {0}")]
    LeaseExpired(String),

    #[error("request was not acknowledged: {0}")]
    NotAcknowledged(String),

    #[error("request is not processing: {0}")]
    StateMismatch(String),

    #[error("request version mismatch: {0}")]
    VersionMismatch(String),

    #[error("request not found: {0}")]
    RequestNotFound(String),

    #[error("Trace Snapshot not found: {0}")]
    TraceNotFound(String),

    #[error("invalid Trace Snapshot {id}: {message}")]
    InvalidTrace { id: String, message: String },

    #[error("invalid Request Snapshot {id}: {message}")]
    InvalidRequest { id: String, message: String },

    #[error("scheduler temporarily unavailable: {0}")]
    Unavailable(String),

    #[error("scheduler error: {0}")]
    Message(String),
}

impl Error {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }

    pub fn is_ownership_loss(&self) -> bool {
        matches!(
            self,
            Self::IdentityMismatch { .. }
                | Self::LeaseMismatch(_)
                | Self::LeaseExpired(_)
                | Self::NotAcknowledged(_)
                | Self::StateMismatch(_)
                | Self::VersionMismatch(_)
                | Self::RequestNotFound(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classes_are_stable() {
        let unavailable = Error::Unavailable("offline".to_string());
        assert!(unavailable.is_transient());
        assert!(!unavailable.is_ownership_loss());

        for error in [
            Error::IdentityMismatch {
                id: "request".to_string(),
                field: "task_id",
            },
            Error::LeaseMismatch("request".to_string()),
            Error::LeaseExpired("request".to_string()),
            Error::NotAcknowledged("request".to_string()),
            Error::StateMismatch("request".to_string()),
            Error::VersionMismatch("request".to_string()),
            Error::RequestNotFound("request".to_string()),
        ] {
            assert!(!error.is_transient());
            assert!(error.is_ownership_loss());
        }

        for error in [
            Error::TraceNotFound("trace".to_string()),
            Error::InvalidTrace {
                id: "trace".to_string(),
                message: "invalid".to_string(),
            },
            Error::InvalidRequest {
                id: "request".to_string(),
                message: "invalid".to_string(),
            },
            Error::Message("invalid".to_string()),
        ] {
            assert!(!error.is_transient());
            assert!(!error.is_ownership_loss());
        }
    }
}
