use thiserror::Error;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum Error {
    #[error("selector error: {0}")]
    Message(String),

    #[error("invalid css selector: {0}")]
    Css(String),

    #[error("ai selector error: {0}")]
    Ai(String),
}
