use thiserror::Error;

/// Errors raised while constructing an API Scheduler client.
///
/// Operations performed after construction use `spider::scheduler::Error` so the
/// Engine sees the same error classes for every Scheduler implementation.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid API Scheduler URL: {0}")]
    Url(#[from] url::ParseError),

    #[error("invalid API Scheduler configuration: {0}")]
    Config(String),

    #[error("failed to build API Scheduler HTTP client: {0}")]
    Client(#[from] reqwest::Error),
}
