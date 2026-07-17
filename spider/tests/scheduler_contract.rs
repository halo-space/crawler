use indexmap::IndexMap;
use spider::{Memory, Scheduler, item, net, payload};

async fn request_contract<S>(scheduler: S)
where
    S: Scheduler,
{
    scheduler.open().await.unwrap();
    let request = net::Request::follow("https://example.com/contract").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![request]))
            .await
            .is_err()
    );

    let claimed = scheduler.next_requests(1).await.unwrap();
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
    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
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
    let second = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(second.id, first.id);
    assert!(second.version > first.version);
    let mut ack = payload::Payload::for_request(&second, second.leased_by.clone());
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&second, second.leased_by.clone());
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
    assert!(!scheduler.has_pending_requests().await.unwrap());

    let item = item::Map::new(IndexMap::from([(
        "value".to_string(),
        serde_json::Value::from("contract"),
    )]));
    let mut items = payload::Payload::new().items(vec![Box::new(item)]);
    items.task_id = "contract-task".to_string();
    scheduler.push_items(&items).await.unwrap();
    scheduler.close().await.unwrap();
}

#[tokio::test]
async fn memory_implements_scheduler_request_contract() {
    let dir = std::env::temp_dir().join(format!("crawler-contract-{}", uuid::Uuid::now_v7()));
    request_contract(Memory::new("contract-worker").with_dir(&dir)).await;
    let _ = tokio::fs::remove_dir_all(dir).await;
}
