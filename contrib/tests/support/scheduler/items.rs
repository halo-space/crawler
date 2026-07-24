use spider::{Scheduler, payload};

use super::{
    fixture::{HTTP, WORKER_A, close, open},
    payload::{item, request},
    settlement::succeed,
};

pub(super) async fn submission_is_isolated<S>(scheduler: S)
where
    S: Scheduler,
{
    open(&scheduler).await;
    let item_request = request("item-request", "https://example.com/item-request");
    scheduler
        .push(payload::Payload::new().requests(vec![item_request]))
        .await
        .unwrap();

    let mut payload = payload::Payload::new().items(vec![item("first")]);
    payload.task_id = "task-items".to_string();
    scheduler.push_items(&payload).await.unwrap();
    scheduler.push_items(&payload).await.unwrap();
    assert!(
        scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    let empty = payload::Payload::new();
    scheduler.push_items(&empty).await.unwrap();

    let mixed_items = payload::Payload::new()
        .requests(vec![request("mixed-request", "https://example.com/mixed")])
        .items(vec![item("mixed")]);
    assert!(scheduler.push_items(&mixed_items).await.is_err());
    assert!(scheduler.push(mixed_items).await.is_err());

    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    succeed(&scheduler, &claimed[0]).await;
    close(&scheduler).await;
}
