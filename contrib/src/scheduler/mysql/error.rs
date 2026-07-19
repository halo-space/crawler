use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("mysql scheduler is outside the current implementation scope")]
    Unsupported,
}
