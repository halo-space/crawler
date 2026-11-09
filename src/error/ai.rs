#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct Error(String);

impl Error {
    pub(crate) fn message(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
