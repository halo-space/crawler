use super::*;

#[tokio::test]
async fn claim_rejects_an_empty_worker_identity() {
    let result = memory().next_requests(1, "  ", HTTP).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn claim_rejects_an_empty_worker_capability_set() {
    let result = memory().next_requests(1, WORKER, &[]).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn broken_trace_fails_only_its_request_in_the_same_claim() {
    let scheduler = memory();
    let good_config = rules_config("good", "index");
    let good = good_config
        .initial_requests("good", "trace-good", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-good".to_string(),
            trace::Snapshot::rules("good", good_config),
            vec![good],
        )
        .await
        .unwrap();

    let broken_config = rules_config("broken", "index");
    let broken = broken_config
        .initial_requests("broken", "trace-broken", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-broken".to_string(),
            trace::Snapshot::rules("broken", broken_config),
            vec![broken],
        )
        .await
        .unwrap();
    scheduler.state().trace_snapshots.remove("trace-broken");

    let claimed = scheduler.next_requests(2, WORKER, HTTP).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].trace_id, "trace-good");
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("Trace Snapshot not found"))
    );
}

#[tokio::test]
async fn invalid_queued_snapshot_records_a_terminal_error() {
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
    {
        let mut state = scheduler.state();
        let mut snapshot = state
            .take(crate::utils::time::now_millis(), 1, HTTP)
            .pop()
            .unwrap();
        snapshot.state = net::State::Processing;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }

    assert!(
        scheduler
            .next_requests(1, WORKER, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("state must be pending"))
    );
}

#[tokio::test]
async fn invalid_snapshot_is_retried_at_most_once_per_claim() {
    let scheduler = memory();
    let mut request = request("https://example.com");
    request.max_retry_count = 3;
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
        snapshot.state = net::State::Processing;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }

    assert!(
        scheduler
            .next_requests(1, WORKER, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler.failed_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    let retried = scheduler
        .state()
        .take(crate::utils::time::now_millis(), 1, HTTP)
        .pop()
        .unwrap();
    assert_eq!(retried.retry_count, 1);
}

#[tokio::test]
async fn claim_version_overflow_records_a_terminal_error() {
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
        snapshot.version = i64::MAX;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }

    assert!(
        scheduler
            .next_requests(1, WORKER, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("version overflow while claiming"))
    );
}

#[tokio::test]
async fn claim_prefers_higher_priority_and_preserves_fifo_for_ties() {
    let scheduler = memory();
    let mut low = request("https://example.com/low");
    low.priority = 1;
    let low_id = low.id.clone();
    let mut high_first = request("https://example.com/high-first");
    high_first.priority = 10;
    let high_first_id = high_first.id.clone();
    let mut high_second = request("https://example.com/high-second");
    high_second.priority = 10;
    let high_second_id = high_second.id.clone();
    scheduler
        .push(payload::Payload::new().requests(vec![low, high_first, high_second]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(3, WORKER, HTTP).await.unwrap();

    assert_eq!(
        claimed
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        [
            high_first_id.as_str(),
            high_second_id.as_str(),
            low_id.as_str()
        ]
    );
}

#[tokio::test]
async fn claim_leaves_future_requests_pending() {
    let scheduler = memory();
    let mut delayed = request("https://example.com/delayed");
    delayed.next_time = crate::utils::time::now_millis() + 60_000;
    scheduler
        .push(payload::Payload::new().requests(vec![delayed]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();

    assert!(claimed.is_empty());
    assert_eq!(scheduler.queued_len(), 1);
    assert!(scheduler.has_pending_requests(WORKER, HTTP).await.unwrap());
}

#[tokio::test]
async fn claim_returns_empty_without_mutating_incompatible_work() {
    let scheduler = memory();
    let browser = request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();

    assert!(claimed.is_empty());
    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn pending_check_ignores_globally_pending_incompatible_work() {
    let scheduler = memory();
    let browser = request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![browser]))
        .await
        .unwrap();

    assert_eq!(scheduler.queued_len(), 1);
    assert!(!scheduler.has_pending_requests(WORKER, HTTP).await.unwrap());
}

#[tokio::test]
async fn pending_check_includes_compatible_work_leased_by_another_worker() {
    let scheduler = memory();
    let request = request("https://example.com/http");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    scheduler.next_requests(1, "worker-a", HTTP).await.unwrap();

    assert!(
        scheduler
            .has_pending_requests("worker-b", HTTP)
            .await
            .unwrap()
    );
    assert!(
        !scheduler
            .has_pending_requests(BROWSER_WORKER, BROWSER)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn claim_uses_the_worker_identity_and_modes_of_each_call() {
    let scheduler = memory();
    let http = request("https://example.com/http");
    let browser = request("https://example.com/browser").mode(net::Mode::Browser);
    scheduler
        .push(payload::Payload::new().requests(vec![http, browser]))
        .await
        .unwrap();

    let browser = scheduler
        .next_requests(1, BROWSER_WORKER, BROWSER)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let http = scheduler
        .next_requests(1, "http-worker", HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();

    assert_eq!(browser.mode, net::Mode::Browser);
    assert_eq!(browser.leased_by, BROWSER_WORKER);
    assert_eq!(http.mode, net::Mode::Http);
    assert_eq!(http.leased_by, "http-worker");
}

#[test]
fn concurrent_claims_only_take_compatible_requests_once() {
    const THREADS: usize = 8;
    const HTTP_REQUESTS: usize = 24;
    const BROWSER_REQUESTS: usize = 8;

    let scheduler = Arc::new(memory());
    let mut requests = Vec::new();
    for index in 0..HTTP_REQUESTS {
        if index < BROWSER_REQUESTS {
            let mut browser =
                request(format!("https://example.com/browser/{index}")).mode(net::Mode::Browser);
            browser.priority = 10;
            requests.push(browser);
        }
        let mut http = request(format!("https://example.com/http/{index}"));
        http.priority = 1;
        requests.push(http);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime
        .block_on(scheduler.push(payload::Payload::new().requests(requests)))
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    let handles = (0..THREADS)
        .map(|_| {
            let scheduler = scheduler.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                barrier.wait();
                runtime
                    .block_on(scheduler.next_requests(4, WORKER, HTTP))
                    .unwrap()
            })
        })
        .collect::<Vec<_>>();
    let claimed = handles
        .into_iter()
        .flat_map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    let ids = claimed
        .iter()
        .map(|request| request.id.as_str())
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(claimed.len(), HTTP_REQUESTS);
    assert_eq!(ids.len(), HTTP_REQUESTS);
    assert!(
        claimed
            .iter()
            .all(|request| request.mode == net::Mode::Http)
    );
    assert_eq!(scheduler.processing_len(), HTTP_REQUESTS);
    assert_eq!(scheduler.queued_len(), BROWSER_REQUESTS);
}
