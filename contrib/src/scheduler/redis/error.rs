use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("redis scheduler is outside v1 implementation scope")]
    Unsupported,
}
