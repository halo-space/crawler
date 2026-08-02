#[path = "support/scheduler/conformance.rs"]
mod conformance;

use spider::Memory;
use spider::scheduler::Lease;

fn new() -> Memory {
    Memory::new().with_modes([spider::net::Mode::Http, spider::net::Mode::Browser])
}

fn short_lease() -> Memory {
    let (timeout, refresh) = conformance::Timing::memory().lease();
    let lease = Lease::new(timeout, refresh).unwrap();
    new().with_lease(lease)
}

#[tokio::test]
async fn conforms_to_the_contract() {
    conformance::run(new, true, conformance::Timing::memory()).await;
    conformance::lease(short_lease(), true, conformance::Timing::memory()).await;
}
