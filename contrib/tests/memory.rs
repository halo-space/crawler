#[path = "support/scheduler/conformance.rs"]
mod conformance;

use spider::Memory;
use spider::scheduler::Lease;
use std::sync::atomic::{AtomicU64, Ordering};

static DIRECTORY_ID: AtomicU64 = AtomicU64::new(1);

fn new() -> Memory {
    let dir = std::env::temp_dir().join(format!(
        "crawler-scheduler-conformance-{}-{}",
        std::process::id(),
        DIRECTORY_ID.fetch_add(1, Ordering::Relaxed)
    ));
    Memory::new().with_dir(dir)
}

fn short_lease() -> Memory {
    let (timeout, refresh) = conformance::Timing::memory().lease();
    let lease = Lease::new(timeout, refresh).unwrap();
    new().with_lease(lease)
}

#[tokio::test]
async fn conforms_to_the_contract() {
    conformance::run(new, true, conformance::Timing::memory()).await;
    conformance::lease(short_lease(), conformance::Timing::memory()).await;
}
