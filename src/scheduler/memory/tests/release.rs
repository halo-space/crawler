use super::*;

#[tokio::test]
async fn expired_acknowledged_request_is_not_reclaimed_while_worker_is_online() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    {
        let mut state = scheduler.state();
        state.processing.get_mut(&claimed.id).unwrap().lease_time = 1;
    }

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&claimed.id].version,
        claimed.version
    );
}

#[tokio::test]
async fn expired_unacknowledged_claim_does_not_consume_an_attempt() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .lease_time = 1;

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&claimed.id].version,
        claimed.version
    );
}

#[tokio::test]
async fn release_requeues_without_consuming_retry_budget() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut release = payload::Payload::for_request(&claimed, WORKER);
    release.state = net::State::Processing;

    scheduler.release(&release).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(reclaimed.id, claimed.id);
    assert_eq!(reclaimed.retry_count, 0);
    assert_eq!(reclaimed.version, claimed.version + 1);
    assert!(scheduler.ack(&release).await.is_err());
}

#[tokio::test]
async fn release_defers_version_advance_until_the_next_claim() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    {
        let mut state = scheduler.state();
        let mut snapshot = state
            .take(crate::utils::time::now_millis(), 1, HTTP)
            .pop()
            .unwrap();
        snapshot.version = i64::MAX - 1;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(claimed.version, i64::MAX);
    let mut release = payload::Payload::for_request(&claimed, WORKER);
    release.state = net::State::Processing;

    scheduler.release(&release).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("version overflow while claiming"))
    );
}

#[tokio::test]
async fn release_conversion_failure_records_a_terminal_error() {
    let scheduler = memory();
    let config = rules_config("broken", "index");
    let request = config
        .initial_requests("broken", "trace-broken", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-broken".to_string(),
            trace::Snapshot::rules("broken", config),
            vec![request],
        )
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .middlewares
        .push(crate::middleware::Spec::new("retry").args(serde_json::json!({"count": "invalid"})));
    let mut release = payload::Payload::for_request(&claimed, WORKER);
    release.state = net::State::Processing;

    let error = scheduler.release(&release).await.unwrap_err();

    assert!(error.to_string().contains("invalid middleware"));
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("invalid middleware"))
    );
}

#[tokio::test]
async fn expired_browser_lease_does_not_reclaim_an_online_request() {
    let scheduler = browser_memory();
    let mut browser = request("https://example.com/browser").mode(net::Mode::Browser);
    browser.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&first, WORKER);
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&first.id)
        .unwrap()
        .lease_time = 1;

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&first.id].mode,
        net::Mode::Browser
    );
}

#[tokio::test]
async fn release_preserves_browser_mode_and_pending_state() {
    let scheduler = browser_memory();
    let browser = request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut release = payload::Payload::for_request(&first, WORKER);
    release.state = net::State::Processing;

    scheduler.release(&release).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    assert!(scheduler.has_pending_requests().await.unwrap());
    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(reclaimed.id, first.id);
    assert_eq!(reclaimed.mode, net::Mode::Browser);
    assert_eq!(reclaimed.version, first.version + 1);
    assert_eq!(reclaimed.retry_count, 0);
}

#[tokio::test]
async fn expired_unacknowledged_browser_claim_stays_processing() {
    let scheduler = browser_memory();
    let browser = request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&first.id)
        .unwrap()
        .lease_time = 1;

    assert!(scheduler.next_requests(0).await.unwrap().is_empty());

    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert!(scheduler.has_pending_requests().await.unwrap());
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
}
