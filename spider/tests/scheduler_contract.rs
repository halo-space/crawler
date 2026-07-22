use indexmap::IndexMap;
use spider::scheduler::Init;
use spider::{Memory, Scheduler, item, net, payload};

const HTTP: &[net::Mode] = &[net::Mode::Http];
const BROWSER: &[net::Mode] = &[net::Mode::Browser];
const ALL: &[net::Mode] = &[net::Mode::Http, net::Mode::Browser];

async fn complete<S>(scheduler: &S, request: &net::Request)
where
    S: Scheduler,
{
    let mut ack = payload::Payload::for_request(request, request.leased_by.clone());
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(request, request.leased_by.clone());
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
}

async fn request_contract<S>(scheduler: S, worker_id: &str, modes: &[net::Mode])
where
    S: Scheduler,
{
    scheduler.open().await.unwrap();
    let request = net::Request::follow("https://example.com/contract").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, worker_id, modes).await.unwrap();
    assert_eq!(claimed.len(), 1);
    let claimed = &claimed[0];
    let mut mismatched_ack = payload::Payload::for_request(claimed, claimed.leased_by.clone());
    mismatched_ack.state = net::State::Processing;
    mismatched_ack.task_id = "other-task".to_string();
    assert!(scheduler.ack(&mismatched_ack).await.is_err());

    let mut ack = payload::Payload::for_request(claimed, claimed.leased_by.clone());
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();

    let mut mismatched_refresh = payload::Payload::for_request(claimed, "other-worker");
    mismatched_refresh.state = net::State::Processing;
    assert!(scheduler.refresh_lease(&mismatched_refresh).await.is_err());
    scheduler.refresh_lease(&ack).await.unwrap();

    let mut mismatched_success = payload::Payload::for_request(claimed, claimed.leased_by.clone());
    mismatched_success.trace_id = "other-trace".to_string();
    mismatched_success.start_time = Some(1);
    mismatched_success.end_time = Some(2);
    assert!(scheduler.success(&mismatched_success).await.is_err());

    let mut success = payload::Payload::for_request(claimed, claimed.leased_by.clone());
    success.start_time = Some(1);
    success.end_time = Some(2);
    let stale = payload::Payload::for_request(claimed, claimed.leased_by.clone());
    scheduler.success(&success).await.unwrap();
    assert!(scheduler.success(&stale).await.is_err());

    let mut retry = net::Request::follow("https://example.com/retry").unwrap();
    retry.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![retry]))
        .await
        .unwrap();
    let first = scheduler
        .next_requests(1, worker_id, modes)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut ack = payload::Payload::for_request(&first, first.leased_by.clone());
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut mismatched_failure =
        payload::Payload::for_request(&first, first.leased_by.clone()).failed("boom");
    mismatched_failure.node = "other-node".to_string();
    mismatched_failure.start_time = Some(1);
    mismatched_failure.end_time = Some(2);
    assert!(scheduler.failure(&mismatched_failure).await.is_err());

    let mut failure = payload::Payload::for_request(&first, first.leased_by.clone()).failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();
    let second = scheduler
        .next_requests(1, worker_id, modes)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.id, first.id);
    assert!(second.version > first.version);
    let mut ack = payload::Payload::for_request(&second, second.leased_by.clone());
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&second, second.leased_by.clone());
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
    assert!(
        !scheduler
            .has_pending_requests(worker_id, modes)
            .await
            .unwrap()
    );

    let item = item::Map::new(IndexMap::from([(
        "value".to_string(),
        serde_json::Value::from("contract"),
    )]));
    let mut items = payload::Payload::new().items(vec![Box::new(item)]);
    items.task_id = "contract-task".to_string();
    scheduler.push_items(&items).await.unwrap();
    scheduler.close().await.unwrap();
}

async fn capability_claim_contract<S>(
    scheduler: S,
    worker_id: &str,
    modes: &[net::Mode],
    expected_urls: &[&str],
) where
    S: Scheduler,
{
    scheduler.open().await.unwrap();
    let mut http = net::Request::follow("https://example.com/http").unwrap();
    http.priority = 1;
    let mut browser = net::Request::follow("https://example.com/browser")
        .unwrap()
        .mode(net::Mode::Browser);
    browser.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![http, browser]))
        .await
        .unwrap();

    let mut claimed = scheduler.next_requests(2, worker_id, modes).await.unwrap();

    assert_eq!(claimed.len(), expected_urls.len());
    assert_eq!(
        claimed
            .iter()
            .map(|request| request.url.as_str())
            .collect::<Vec<_>>(),
        expected_urls
    );
    assert!(claimed.iter().all(|request| modes.contains(&request.mode)));
    for request in &claimed {
        assert_eq!(request.state, net::State::Processing);
        assert_eq!(request.version, 1);
        assert_eq!(request.leased_by, worker_id);
        assert!(request.lease_time > 0);
    }

    let remaining_modes = ALL
        .iter()
        .filter(|mode| !modes.contains(mode))
        .cloned()
        .collect::<Vec<_>>();
    if !remaining_modes.is_empty() {
        let remaining = scheduler
            .next_requests(2, "remaining-worker", &remaining_modes)
            .await
            .unwrap();
        let expected = [
            (net::Mode::Http, "https://example.com/http"),
            (net::Mode::Browser, "https://example.com/browser"),
        ]
        .into_iter()
        .filter(|(mode, _)| remaining_modes.contains(mode))
        .map(|(_, url)| url)
        .collect::<Vec<_>>();
        assert_eq!(
            remaining
                .iter()
                .map(|request| request.url.as_str())
                .collect::<Vec<_>>(),
            expected
        );
        for request in &remaining {
            assert_eq!(request.state, net::State::Processing);
            assert_eq!(request.version, 1);
            assert_eq!(request.leased_by, "remaining-worker");
            assert!(request.lease_time > 0);
        }
        claimed.extend(remaining);
    }

    for request in &claimed {
        complete(&scheduler, request).await;
    }
    assert!(
        !scheduler
            .has_pending_requests(worker_id, ALL)
            .await
            .unwrap()
    );
    scheduler.close().await.unwrap();
}

#[tokio::test]
async fn memory_implements_scheduler_request_contract() {
    let dir = std::env::temp_dir().join(format!("crawler-contract-{}", uuid::Uuid::now_v7()));
    request_contract(Memory::new().with_dir(&dir), "contract-worker", HTTP).await;
    let _ = tokio::fs::remove_dir_all(dir).await;
}

#[tokio::test]
async fn memory_init_accepts_an_empty_rules_request_collection() {
    let scheduler = Memory::new();
    let config = spider::config::Config::from_yaml(
        r#"
spider:
  name: empty-rules-run
  start: [{node: index, url: https://example.com}]
graph:
  nodes:
    index: {}
  edges: []
"#,
    )
    .unwrap();

    scheduler
        .init(
            "trace-empty".to_string(),
            spider::trace::Snapshot::rules("empty-rules-run", config),
            Vec::new(),
        )
        .await
        .unwrap();

    assert!(scheduler.trace("trace-empty").await.unwrap().is_some());
    assert!(
        !scheduler
            .has_pending_requests("contract-worker", HTTP)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn memory_http_only_capability_claim_contract() {
    capability_claim_contract(
        Memory::new(),
        "http-worker",
        HTTP,
        &["https://example.com/http"],
    )
    .await;
}

#[tokio::test]
async fn memory_browser_only_capability_claim_contract() {
    capability_claim_contract(
        Memory::new(),
        "browser-worker",
        BROWSER,
        &["https://example.com/browser"],
    )
    .await;
}

#[tokio::test]
async fn memory_multi_mode_capability_claim_contract() {
    capability_claim_contract(
        Memory::new(),
        "mixed-worker",
        ALL,
        &["https://example.com/browser", "https://example.com/http"],
    )
    .await;
}

#[tokio::test]
async fn memory_concurrent_workers_claim_only_compatible_requests() {
    let scheduler = std::sync::Arc::new(Memory::new());
    scheduler.open().await.unwrap();
    let mut requests = Vec::new();
    for index in 0..16 {
        requests.push(net::Request::follow(format!("https://example.com/http/{index}")).unwrap());
        requests.push(
            net::Request::follow(format!("https://example.com/browser/{index}"))
                .unwrap()
                .mode(net::Mode::Browser),
        );
    }
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();

    let (http, browser) = tokio::join!(
        scheduler.next_requests(16, "http-worker", HTTP),
        scheduler.next_requests(16, "browser-worker", BROWSER),
    );
    let http = http.unwrap();
    let browser = browser.unwrap();

    assert_eq!(http.len(), 16);
    assert_eq!(browser.len(), 16);
    assert!(
        http.iter().all(|request| {
            request.mode == net::Mode::Http && request.leased_by == "http-worker"
        })
    );
    assert!(browser.iter().all(|request| {
        request.mode == net::Mode::Browser && request.leased_by == "browser-worker"
    }));
    let ids = http
        .iter()
        .chain(&browser)
        .map(|request| request.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids.len(), 32);

    for request in http.iter().chain(&browser) {
        complete(scheduler.as_ref(), request).await;
    }
    scheduler.close().await.unwrap();
}
