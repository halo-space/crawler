use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unsupported download mode: {0}")]
    UnsupportedMode(String),

    #[error("http request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("invalid header name: {0}")]
    InvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),

    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("redirect target is outside allowed domains: {0}")]
    DisallowedRedirect(String),

    #[error("invalid redirect target: {0}")]
    InvalidRedirect(String),

    #[error("too many redirects")]
    TooManyRedirects,
}
