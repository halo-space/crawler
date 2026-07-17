use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("url is not absolute: {0}")]
    UrlNotAbsolute(String),

    #[error("unsupported protocol for url: {0}")]
    UnsupportedProtocol(String),

    #[error("url parse failed: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("response body is not valid utf-8")]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),
}
