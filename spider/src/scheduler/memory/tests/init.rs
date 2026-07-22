use super::*;

#[tokio::test]
async fn claim_uses_trace_snapshot_from_memory_domain() {
    let scheduler = Memory::new();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            Vec::new(),
        )
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].trace_id, "trace-1");
}

#[tokio::test]
async fn init_atomically_stores_trace_and_initial_requests() {
    let scheduler = Memory::new();
    let mut first = net::Request::follow("https://example.com/one").unwrap();
    first.task_id = "task-1".to_string();
    first.trace_id = "trace-1".to_string();
    let mut second = net::Request::follow("https://example.com/two").unwrap();
    second.task_id = "task-1".to_string();
    second.trace_id = "trace-1".to_string();

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            vec![first, second],
        )
        .await
        .unwrap();

    assert!(scheduler.trace("trace-1").await.unwrap().is_some());
    assert_eq!(scheduler.queued_len(), 2);
    assert_eq!(
        scheduler
            .next_requests(2, WORKER, HTTP)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn init_rejects_invalid_later_request_without_mutation() {
    let scheduler = Memory::new();
    let mut first = net::Request::follow("https://example.com/one").unwrap();
    first.task_id = "task-1".to_string();
    first.trace_id = "trace-1".to_string();
    let first_id = first.id.clone();
    let mut invalid = net::Request::follow("https://example.com/two").unwrap();
    invalid.task_id = "task-1".to_string();
    invalid.trace_id = "trace-1".to_string();
    invalid.state = net::State::Processing;
    let invalid_id = invalid.id.clone();

    let result = scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            vec![first, invalid],
        )
        .await;

    assert!(result.is_err());
    assert!(scheduler.trace("trace-1").await.unwrap().is_none());
    assert_eq!(scheduler.queued_len(), 0);
    let state = scheduler.state();
    assert!(!state.contains(&first_id));
    assert!(!state.contains(&invalid_id));
}

#[tokio::test]
async fn init_stores_a_rules_trace_with_no_accepted_requests() {
    let scheduler = Memory::new();
    let config = rules_config("books", "detail");

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::rules("books", config),
            Vec::new(),
        )
        .await
        .unwrap();

    assert!(scheduler.trace("trace-1").await.unwrap().is_some());
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
}

#[tokio::test]
async fn claim_restores_rules_requests_and_shares_trace_config() {
    let scheduler = Memory::new();
    let config = rules_config("books", "detail");
    let mut requests = config
        .initial_requests("task-1", "trace-1", HashMap::new())
        .unwrap();
    let mut second = requests[0].clone();
    second.id = "req-second".to_string();
    second.url = "https://example.com/two".to_string();
    requests.push(second);

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::rules("task-1", config),
            requests,
        )
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2, WORKER, HTTP).await.unwrap();

    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].node_key(), "detail");
    let first = claimed[0].snapshot().unwrap();
    let second = claimed[1].snapshot().unwrap();
    assert!(Arc::ptr_eq(first, second));
}

#[tokio::test]
async fn trace_round_trip_preserves_spider_metadata() {
    let scheduler = Memory::new();
    let mut config = rules_config("books", "detail");
    config.spider.version = Some("2026.07".to_string());
    config.spider.timezone = Some("Asia/Shanghai".to_string());

    let requests = config
        .initial_requests("task-1", "trace-1", HashMap::new())
        .unwrap();
    let snapshot = trace::Snapshot::rules("task-1", config);
    let snapshot =
        serde_json::from_value::<trace::Snapshot>(serde_json::to_value(snapshot).unwrap()).unwrap();

    scheduler
        .init("trace-1".to_string(), snapshot, requests)
        .await
        .unwrap();

    let stored = scheduler.trace("trace-1").await.unwrap().unwrap();
    let dsl = stored.dsl.unwrap();
    assert_eq!(dsl.spider.version.as_deref(), Some("2026.07"));
    assert_eq!(dsl.spider.timezone.as_deref(), Some("Asia/Shanghai"));
}

#[tokio::test]
async fn init_rejects_partial_trace_mismatch_without_mutation() {
    let scheduler = Memory::new();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-2".to_string();

    let result = scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            vec![request],
        )
        .await;

    assert!(result.is_err());
    assert!(scheduler.trace("trace-1").await.unwrap().is_none());
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn init_rejects_trace_overwrite_without_mutation() {
    let scheduler = Memory::new();
    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut replacement = trace::Snapshot::code("task-1");
    replacement.priority = 99;

    let result = scheduler
        .init("trace-1".to_string(), replacement, Vec::new())
        .await;

    assert!(result.unwrap_err().to_string().contains("already exists"));
    assert_eq!(
        scheduler.trace("trace-1").await.unwrap().unwrap().priority,
        0
    );
}
