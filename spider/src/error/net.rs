use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("url is not absolute: {0}")]
    UrlNotAbsolute(String),

    #[error("unsupported protocol for url: {0}")]
    UnsupportedProtocol(String),

    #[error("url parse failed: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),
}
