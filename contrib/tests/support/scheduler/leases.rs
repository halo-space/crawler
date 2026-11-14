use std::time::Duration;

use harbor::scheduler::Init;
use harbor::{Scheduler, payload};

use super::{
    fixture::{Timing, close, open_run},
    payload::{processing, request, success},
};

pub(super) async fn contract<S>(scheduler: S, timing: Timing)
where
    S: Scheduler + Init,
{
    let lease = scheduler
        .lease()
        .expect("conformance fixture must expose a lease policy");
    open_run(&scheduler).await;
    ack_does_not_extend_the_lease(&scheduler, lease.timeout(), timing).await;
    refresh_extends_the_active_lease(&scheduler, lease.timeout(), timing).await;
    unacknowledged_expiry_preserves_queue_retry(&scheduler, lease.timeout(), timing).await;
    close(&scheduler).await;
}

async fn ack_does_not_extend_the_lease<S>(scheduler: &S, timeout: Duration, timing: Timing)
where
    S: Scheduler,
{
    let mut original = request("lease-ack", "https://example.com/lease/ack");
    original.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    tokio::time::sleep(timing.after_refresh(timeout)).await;
    let processing = processing(&first);
    scheduler.ack(&processing).await.unwrap();
    scheduler.ack(&processing).await.unwrap();
    tokio::time::sleep(timing.after_refresh(timeout)).await;

    let recovered = scheduler.next_requests(1).await.unwrap();
    assert!(
        recovered.is_empty(),
        "an expired lease must not reclaim an online Worker's Request"
    );
    assert!(
        scheduler
            .success(&success(&first))
            .await
            .unwrap_err()
            .is_ownership_loss(),
        "ack must not extend the active lease"
    );
}

async fn refresh_extends_the_active_lease<S>(scheduler: &S, timeout: Duration, timing: Timing)
where
    S: Scheduler,
{
    let mut original = request("lease-refresh", "https://example.com/lease/refresh");
    original.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let processing = processing(&first);
    scheduler.ack(&processing).await.unwrap();

    tokio::time::sleep(timing.after_refresh(timeout)).await;
    scheduler.refresh_lease(&processing).await.unwrap();
    tokio::time::sleep(timing.after_refresh(timeout)).await;
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    tokio::time::sleep(timing.after_refresh(timeout)).await;
    let recovered = scheduler.next_requests(1).await.unwrap();
    assert!(recovered.is_empty());
    assert!(
        scheduler
            .success(&success(&first))
            .await
            .unwrap_err()
            .is_ownership_loss()
    );
}

async fn unacknowledged_expiry_preserves_queue_retry<S>(
    scheduler: &S,
    timeout: Duration,
    timing: Timing,
) where
    S: Scheduler,
{
    let mut original = request(
        "lease-unacknowledged",
        "https://example.com/lease/unacknowledged",
    );
    original.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let _first = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    tokio::time::sleep(timing.after_expiry(timeout)).await;
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
}
