use std::future::Future;

use crate::{net, scheduler, trace};

pub trait Init: scheduler::Scheduler {
    /// 当前 Engine 启动是否负责创建本地运行种子。
    fn initializes_run(&self) -> bool {
        false
    }

    /// 原子保存一份 Trace Snapshot 及调用方提供的初始 Request 批次。
    /// 空集合仍是一轮有效运行，必须同时保存 Trace Snapshot。
    fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> impl Future<Output = Result<(), scheduler::Error>> + Send;
}
