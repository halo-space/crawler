use super::*;

#[tokio::test]
async fn failure_rejects_stats_overflow_without_partial_settlement() {
    let scheduler = memory();
    let mut request = request("https://example.com");
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
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    scheduler.state().trace_stats.insert(
        TRACE.to_string(),
        HashMap::from([(
            "overflow".to_string(),
            stats::Counter {
                total: i64::MAX,
                ..stats::Counter::default()
            },
        )]),
    );
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    for name in ["new", "overflow"] {
        failure.stats.insert(
            name.to_string(),
            serde_json::to_value(stats::Counter {
                total: 1,
                ..stats::Counter::default()
            })
            .unwrap(),
        );
    }

    let error = scheduler.failure(&failure).await.unwrap_err();

    assert!(error.to_string().contains("stats counter overflow"));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
    let stats = scheduler.trace_stats(TRACE);
    assert_eq!(stats.len(), 1);
    assert_eq!(stats["overflow"].total, i64::MAX);
    let state = scheduler.state();
    assert!(
        state
            .acknowledged
            .contains(&(claimed.id.clone(), claimed.version))
    );
    assert!(state.completed.is_empty());
    assert_eq!(state.processing[&claimed.id].retry_count, 0);
}

#[tokio::test]
async fn repeated_retryable_failure_does_not_duplicate_the_queue() {
    let scheduler = memory();
    let mut request = request("https://example.com");
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
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);

    scheduler.failure(&failure).await.unwrap();
    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
}

#[tokio::test]
async fn failure_requeues_when_retry_budget_remains() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 2;

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();
    let mut ack = payload::Payload::for_request(&claimed[0], "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1").failed("boom");
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    scheduler.failure(&payload).await.unwrap();

    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
    let retried = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(retried.failed_workers, ["worker-1"]);
}

#[tokio::test]
async fn repeated_failures_do_not_duplicate_the_worker_history() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    for _ in 0..2 {
        let claimed = scheduler
            .next_requests(1, WORKER, HTTP)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mut ack = payload::Payload::for_request(&claimed, "worker-1");
        ack.state = net::State::Processing;
        scheduler.ack(&ack).await.unwrap();
        let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
        failure.start_time = Some(1);
        failure.end_time = Some(2);
        scheduler.failure(&failure).await.unwrap();
    }

    let retried = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(retried.failed_workers, ["worker-1"]);
    assert_eq!(retried.retry_count, 2);
}

#[tokio::test]
async fn failure_moves_to_failed_when_retry_budget_is_exhausted() {
    let scheduler = memory();
    let request = request("https://example.com");

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();
    let mut ack = payload::Payload::for_request(&claimed[0], "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1").failed("boom");
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    scheduler.failure(&payload).await.unwrap();

    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
}

#[tokio::test]
async fn retry_counter_overflow_still_reaches_a_terminal_completion() {
    let scheduler = memory();
    let request = request("https://example.com");
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
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .retry_count = i32::MAX;
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("overflow");
    failure.start_time = Some(1);
    failure.end_time = Some(2);

    scheduler.failure(&failure).await.unwrap();
    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    let errors = scheduler.errors();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("overflow"));
    assert!(errors[0].contains("request retry overflow"));
}

#[tokio::test]
async fn failure_retry_preserves_browser_mode_eligibility() {
    let scheduler = memory();
    let mut browser = request("https://example.com/browser").mode(net::Mode::Browser);
    browser.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();
    let first = scheduler
        .next_requests(1, BROWSER_WORKER, BROWSER)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut ack = payload::Payload::for_request(&first, "browser-worker");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut failure = payload::Payload::for_request(&first, "browser-worker").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();

    let retried = scheduler
        .next_requests(1, BROWSER_WORKER, BROWSER)
        .await
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(retried.id, first.id);
    assert_eq!(retried.mode, net::Mode::Browser);
    assert_eq!(retried.version, first.version + 1);
    assert_eq!(retried.retry_count, 1);
}
