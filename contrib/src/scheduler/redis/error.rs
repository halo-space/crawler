use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("redis scheduler is outside the current implementation scope")]
    Unsupported,
}
