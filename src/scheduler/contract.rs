use std::future::Future;

use crate::{payload, trace};

pub trait Scheduler: Send + Sync {
    /// 当前实现的租约恢复和续租策略；不需要租约的实现返回 `None`。
    fn lease(&self) -> Option<crate::scheduler::Lease> {
        None
    }

    /// 打开调度器持有的连接、存储或运行期资源。
    /// `concurrency` 是本次 Engine 运行冻结后的 Request 并发数。
    fn open(
        &self,
        concurrency: usize,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 关闭调度器资源；不改变已经结算的 Request 语义。
    fn close(&self) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 提交本轮解析产生的 Request，只消费 `payload.requests`。
    /// 每个 Request 必须已经绑定非空的 `task_id` 和 `trace_id`。
    /// 相同 ID 与初始 Snapshot 的重放是 no-op；任一 Snapshot 冲突必须整批失败。
    fn push(
        &self,
        payload: payload::Payload,
    ) -> impl Future<Output = Result<(), crate::scheduler::Error>> + Send;

    /// 读取当前运行不可变的 Trace Snapshot；不存在时返回 `None`。
    fn trace(
        &self,
        trace_id: &str,
    ) -> impl Future<Output = Result<Option<trace::Snapshot>, crate::scheduler::Error>> + Send;

    /// 按 Scheduler 自己持有的 Worker 身份和能力领取并恢复下一批可执行 Request。
    /// `limit` 是本次最多领取条数。
    fn next_requests(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<crate::net::Request>, crate::scheduler::Error>> + Send;

    /// 判断 Scheduler 自己持有的 Worker 能力范围内是否仍有排队中或执行中的 Request。
    fn has_pending_requests(
        &self,
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
