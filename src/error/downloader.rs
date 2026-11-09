use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("unsupported download mode: {0}")]
    UnsupportedMode(String),

    #[error("http request failed: {0}")]
    Http(#[source] reqwest::Error),

    #[error("invalid HTTP Downloader configuration: {0}")]
    InvalidConfig(String),

    #[error("HTTP download timed out")]
    Timeout,

    #[error("decoded response body exceeds the {limit}-byte limit")]
    BodyTooLarge { limit: u64 },

    #[error("invalid header value: {0}")]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[error("redirect target is outside allowed domains: {0}")]
    DisallowedRedirect(String),

    #[error("invalid redirect target: {0}")]
    InvalidRedirect(String),

    #[error("too many redirects")]
    TooManyRedirects,
}

impl From<reqwest::Error> for Error {
    fn from(error: reqwest::Error) -> Self {
        Self::Http(error.without_url())
    }
}
