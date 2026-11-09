use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("config I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("config YAML parse failed: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("config error: {0}")]
    Message(String),
}
