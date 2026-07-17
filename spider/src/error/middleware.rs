use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("middleware is not registered: {0}")]
    NotRegistered(String),

    #[error("middleware error: {0}")]
    Message(String),

    #[error("invalid middleware configuration for {name}: {message}")]
    InvalidConfig { name: String, message: String },
}
