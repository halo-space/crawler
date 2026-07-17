pub mod config;
pub mod downloader;
pub mod graph;
pub mod item;
pub mod middleware;
pub mod net;
pub mod scheduler;
pub mod selector;
pub mod spider;
pub mod stats;

/// Unified error returned by the framework runtime.
///
/// Replaceable component traits retain their own error types. Their errors
/// are converted into this type when they enter the shared runtime flow.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("download failed: {0}")]
    Download(#[from] downloader::Error),

    #[error("config failed: {0}")]
    Config(#[from] config::Error),

    #[error("item failed: {0}")]
    Item(#[from] item::Error),

    #[error("net failed: {0}")]
    Net(#[from] net::Error),

    #[error("middleware failed: {0}")]
    Middleware(#[from] middleware::Error),

    #[error("scheduler failed: {0}")]
    Scheduler(#[from] scheduler::Error),

    #[error("spider failed: {0}")]
    Spider(#[from] spider::Error),

    #[error("selector failed: {0}")]
    Selector(#[from] selector::Error),

    #[error("graph failed: {0}")]
    Graph(#[from] graph::Error),

    #[error("stats failed: {0}")]
    Stats(#[from] stats::Error),

    #[error("engine error: {0}")]
    Message(String),
}

impl Error {
    /// 构造不属于具体组件类型的运行期错误。
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}
