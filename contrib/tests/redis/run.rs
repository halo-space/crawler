use contrib::scheduler::redis::Redis;
use spider::scheduler::Init;
use spider::{Scheduler, trace};

pub(super) const TASK_ID: &str = "redis-test-task";
pub(super) const TRACE_ID: &str = "redis-test-trace";

pub(super) async fn init(scheduler: &Redis) {
    match scheduler.trace(TRACE_ID).await.unwrap() {
        Some(snapshot) => assert_eq!(snapshot.task_id, TASK_ID),
        None => scheduler
            .init(
                TRACE_ID.to_string(),
                trace::Snapshot::code(TASK_ID),
                Vec::new(),
            )
            .await
            .unwrap(),
    }
}
