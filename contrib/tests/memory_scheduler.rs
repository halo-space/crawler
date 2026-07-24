#[path = "support/scheduler/conformance.rs"]
mod conformance;

use spider::Memory;
use spider::scheduler::Lease;
use std::sync::atomic::{AtomicU64, Ordering};

static FIXTURE_ID: AtomicU64 = AtomicU64::new(1);

fn memory() -> Memory {
    let dir = std::env::temp_dir().join(format!(
        "crawler-scheduler-conformance-{}-{}",
        std::process::id(),
        FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    Memory::new().with_dir(dir)
}

fn memory_with_short_lease() -> Memory {
    let (timeout, refresh) = conformance::Timing::memory().lease();
    let lease = Lease::new(timeout, refresh).unwrap();
    memory().with_lease(lease)
}

#[tokio::test]
async fn conforms_to_the_scheduler_contract() {
    conformance::run(memory, true, conformance::Timing::memory()).await;
    conformance::lease(memory_with_short_lease(), conformance::Timing::memory()).await;
}
