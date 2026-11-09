use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("item I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("item serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("item deserialization failed: {0}")]
    Deserialize(#[source] serde_json::Error),

    #[error("item error: {0}")]
    Message(String),
}
