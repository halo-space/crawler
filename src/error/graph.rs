use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("graph error: {0}")]
    Message(String),
}
