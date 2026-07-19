use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("api scheduler is outside the current implementation scope")]
    Unsupported,
}
