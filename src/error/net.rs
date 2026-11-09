use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("url must be absolute and contain a host")]
    UrlNotAbsolute,

    #[error("unsupported protocol for url: {0}")]
    UnsupportedProtocol(String),

    #[error("url parse failed: {0}")]
    UrlParse(#[from] url::ParseError),

    #[error("json decode failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Header(#[from] crate::net::headers::Error),

    #[error(transparent)]
    Cookie(#[from] crate::net::cookies::Error),

    #[error("Cookie header is reserved; use Request cookies instead")]
    CookieHeader,
}
