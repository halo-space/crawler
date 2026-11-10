use harbor::Error as HarborError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("worker configuration error: {0}")]
    Config(String),
    #[error("worker runtime failed: {0}")]
    Runtime(HarborError),
}
