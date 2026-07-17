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
