use std::future::Future;
use std::path::Path;

use crate::{net, payload, trace};

pub trait Scheduler: Send + Sync {
    /// Worker 本地运行目录；未配置本地文件能力的实现返回 `None`。
    fn dir(&self) -> Option<&Path> {
        None
    }

    /// 当前实现的租约恢复和续租策略；不需要租约的实现返回 `None`。
    fn lease(&self) -> Option<crate::scheduler::Lease> {
        None
    }

    /// 打开调度器持有的连接、存储或运行期资源。
    fn open(&self) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 关闭调度器资源；不改变已经结算的 Request 语义。
    fn close(&self) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 提交本轮解析产生的 Request，只消费 `payload.requests`。
    /// 相同 ID 与初始 Snapshot 的重放是 no-op；任一 Snapshot 冲突必须整批失败。
    fn push(
        &self,
        payload: payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 提交本轮解析产生的 Item 集合，只消费 `payload.items`。
    fn push_items(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 读取当前运行不可变的 Trace Snapshot；不存在时返回 `None`。
    fn trace(
        &self,
        trace_id: &str,
    ) -> impl Future<Output = Result<Option<trace::Snapshot>, crate::scheduler::Error>> + Send;

    /// 按当前 Worker 身份和能力领取并恢复下一批可执行 Request，`limit` 是本次最多领取条数。
    fn next_requests(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> impl Future<Output = Result<Vec<net::Request>, crate::scheduler::Error>> + Send;

    /// 判断当前 Worker 能力范围内是否仍有排队中或执行中的 Request。
    /// `modes` 定义能力范围；执行中的 Request 按 mode 全局统计，不按 `leased_by` 过滤。
    fn has_pending_requests(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> impl Future<Output = Result<bool, crate::scheduler::Error>> + Send;

    /// 确认 Engine 已接受当前领取的 Request。
    fn ack(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 主动归还当前 Request 执行权，不消费队列层重试次数。
    fn release(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 续租已经确认且仍由当前 Worker 持有的 Request。
    fn refresh_lease(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 成功结算当前 Request 和统计。
    fn success(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 失败结算当前 Request、统计和队列层重试。
    fn failure(
        &self,
        payload: &payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;
}
