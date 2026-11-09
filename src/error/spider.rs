use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("engine stopped before accepting spider output")]
    EngineStopped,

    #[error("spider error: {0}")]
    Message(String),

    #[error("request rejected: {0}")]
    RequestRejected(String),

    #[error("item rejected: {0}")]
    ItemRejected(String),
}
