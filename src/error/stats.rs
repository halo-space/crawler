use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("stats error: {0}")]
    Message(String),
}
