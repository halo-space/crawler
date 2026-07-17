use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("api scheduler is outside v1 implementation scope")]
    Unsupported,
}
