use std::future::Future;

use crate::net;

/// Engine 调用代码模式或 Rules 模式执行器的统一契约。
#[doc(hidden)]
pub trait Execute: Send + Sync {
    /// 产生当前模式的初始输出；Rules 模式已由 Init 准备时可保持空实现。
    #[allow(clippy::manual_async_fn)]
    fn start(&self) -> impl Future<Output = Result<(), crate::Error>> + Send {
        async { Ok(()) }
    }

    /// 返回当前 Request 所属爬虫允许访问的域名。
    fn allowed_domains(&self, request: &net::Request) -> impl Future<Output = Vec<String>> + Send;

    /// 在下载前校验当前 Request 是否能由本执行器处理。
    fn validate(&self, request: &net::Request) -> Result<(), crate::Error>;

    /// 执行代码 handler 或 Rules node 的响应解析逻辑。
    fn parse(
        &self,
        request: net::Request,
        response: net::Response,
    ) -> impl Future<Output = Result<(), crate::Error>> + Send;
}
