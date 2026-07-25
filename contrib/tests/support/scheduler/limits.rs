use spider::{Scheduler, payload};

use super::{
    fixture::{HTTP, WORKER_A, close, open},
    payload::request,
    settlement::succeed,
};

pub(super) async fn request_retry_limit_is_enforced<S>(scheduler: S)
where
    S: Scheduler,
{
    open(&scheduler).await;

    let mut accepted = request("retry-limit", "https://example.com/retry-limit");
    accepted.max_retry_count = spider::net::request::MAX_RETRY_COUNT;
    scheduler
        .push(payload::Payload::new().requests(vec![accepted]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        claimed.max_retry_count,
        spider::net::request::MAX_RETRY_COUNT
    );
    succeed(&scheduler, &claimed).await;

    let mut rejected = request("retry-overflow", "https://example.com/retry-overflow");
    rejected.max_retry_count = spider::net::request::MAX_RETRY_COUNT + 1;
    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![rejected]))
            .await
            .is_err()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    close(&scheduler).await;
}
