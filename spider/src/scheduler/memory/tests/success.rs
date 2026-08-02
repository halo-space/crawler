use super::*;

#[tokio::test]
async fn repeated_success_is_idempotent() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, WORKER);
    success.start_time = Some(1);
    success.end_time = Some(2);
    success.stats.insert(
        "index".to_string(),
        serde_json::to_value(stats::Counter {
            total: 1,
            ..stats::Counter::default()
        })
        .unwrap(),
    );

    scheduler.success(&success).await.unwrap();
    scheduler.success(&success).await.unwrap();

    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.trace_stats(TRACE)["index"].total, 1);
}

#[tokio::test]
async fn success_rejects_negative_stats_without_settling() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, WORKER);
    success.start_time = Some(1);
    success.end_time = Some(2);
    success.stats.insert(
        "index".to_string(),
        serde_json::to_value(stats::Counter {
            total: -1,
            ..stats::Counter::default()
        })
        .unwrap(),
    );

    let error = scheduler.success(&success).await.unwrap_err();

    assert!(error.to_string().contains("must be non-negative"));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
    assert!(scheduler.trace_stats(TRACE).is_empty());
    let state = scheduler.state();
    assert!(
        state
            .acknowledged
            .contains(&(claimed.id.clone(), claimed.version))
    );
    assert!(state.completed.is_empty());
}

#[tokio::test]
async fn accepted_failure_remains_idempotent_after_a_later_success() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&first, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut failure = payload::Payload::for_request(&first, WORKER).failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();

    let second = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&second, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&second, WORKER);
    success.start_time = Some(3);
    success.end_time = Some(4);
    scheduler.success(&success).await.unwrap();

    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert!(scheduler.errors().is_empty());
}

#[tokio::test]
async fn success_rejects_stale_version() {
    let scheduler = memory();
    let request = request("https://example.com");

    scheduler
        .push(payload::Payload::for_request(&request, WORKER).requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], WORKER);
    payload.version += 1;
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    let error = scheduler.success(&payload).await.unwrap_err();

    assert!(matches!(error, scheduler::Error::VersionMismatch(_)));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
}

#[tokio::test]
async fn success_records_the_current_lease_owner_instead_of_payload_worker() {
    let scheduler = memory();
    let request = request("https://example.com");

    scheduler
        .push(payload::Payload::for_request(&request, WORKER).requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut ack = payload::Payload::for_request(&claimed[0], "reported-worker");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-2");
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    scheduler.success(&payload).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(
        scheduler.state().completed[&(claimed[0].id.clone(), claimed[0].version)].worker_id,
        WORKER
    );
}

#[tokio::test]
async fn success_rejects_processing_payload_state() {
    let scheduler = memory();
    let request = request("https://example.com");

    scheduler
        .push(payload::Payload::for_request(&request, WORKER).requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], WORKER);
    payload.state = net::State::Processing;
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    let error = scheduler.success(&payload).await.unwrap_err();

    assert!(matches!(error, scheduler::Error::Message(_)));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
}
