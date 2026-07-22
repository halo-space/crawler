use super::*;

#[tokio::test]
async fn ack_is_idempotent_for_the_same_execution() {
    let scheduler = Memory::new();
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();
    let lease_time = crate::utils::time::now_millis().saturating_sub(1);
    scheduler
        .state()
        .processing
        .get_mut(&claimed[0].id)
        .unwrap()
        .lease_time = lease_time;
    let mut first = payload::Payload::for_request(&claimed[0], "worker-1");
    first.state = net::State::Processing;
    scheduler.ack(&first).await.unwrap();

    let mut duplicate = payload::Payload::for_request(&claimed[0], "worker-1");
    duplicate.state = net::State::Processing;
    scheduler.ack(&duplicate).await.unwrap();
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&claimed[0].id].lease_time,
        lease_time
    );
}

#[tokio::test]
async fn refresh_lease_updates_an_acknowledged_lease() {
    let scheduler = Memory::new();
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut lease = payload::Payload::for_request(&claimed, "worker-1");
    lease.state = net::State::Processing;
    scheduler.ack(&lease).await.unwrap();
    let before = crate::utils::time::now_millis().saturating_sub(1);
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .lease_time = before;

    scheduler.refresh_lease(&lease).await.unwrap();

    assert!(scheduler.state().processing[&claimed.id].lease_time >= before);
    assert_eq!(scheduler.processing_len(), 1);
}

#[tokio::test]
async fn lease_refresh_prevents_reclaim_until_it_stops() {
    let policy = scheduler::Lease::new(
        std::time::Duration::from_millis(40),
        std::time::Duration::from_millis(10),
    )
    .unwrap();
    let scheduler = Memory::new().with_lease(policy);
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut lease = payload::Payload::for_request(&claimed, "worker-1");
    lease.state = net::State::Processing;
    scheduler.ack(&lease).await.unwrap();

    for _ in 0..3 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        scheduler.refresh_lease(&lease).await.unwrap();
        assert!(
            scheduler
                .next_requests(1, WORKER, HTTP)
                .await
                .unwrap()
                .is_empty()
        );
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let reclaimed = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(reclaimed.id, claimed.id);
    assert_eq!(reclaimed.version, claimed.version + 1);
    assert_eq!(reclaimed.failed_workers, ["worker-1"]);
}
