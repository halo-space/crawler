use spider::scheduler::Init;
use spider::{Scheduler, payload, trace};

use super::common::{
    HTTP, WORKER_A, WORKER_B, namespace, owned_request, request, scheduler, succeed,
};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn close_preserves_data_for_a_new_scheduler_instance() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("restart");
    let first = scheduler(&fixture, &namespace);
    first.open().await.unwrap();

    let initial = owned_request(
        "restart-request",
        "https://example.com/restart",
        "restart-task",
        "restart-trace",
    );
    first
        .init(
            "restart-trace".to_string(),
            trace::Snapshot::code("restart-task"),
            vec![initial],
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let closed_error = first.trace("restart-trace").await.unwrap_err();
    assert!(
        !closed_error.is_transient(),
        "a closed local handle is a deterministic lifecycle error: {closed_error}"
    );

    let second = scheduler(&fixture, &namespace);
    second.open().await.unwrap();
    assert_eq!(
        second
            .trace("restart-trace")
            .await
            .unwrap()
            .unwrap()
            .task_id,
        "restart-task"
    );
    let claimed = second
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "restart-request");
    succeed(&second, &claimed).await;
    second.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn namespaces_isolate_identical_request_ids() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let left_namespace = namespace("left");
    let right_namespace = namespace("right");
    let left = scheduler(&fixture, &left_namespace);
    let right = scheduler(&fixture, &right_namespace);
    left.open().await.unwrap();
    right.open().await.unwrap();

    left.push(
        payload::Payload::new()
            .requests(vec![request("shared-id", "https://left.example.com/value")]),
    )
    .await
    .unwrap();
    assert!(!right.has_pending_requests(WORKER_B, HTTP).await.unwrap());
    right
        .push(payload::Payload::new().requests(vec![request(
            "shared-id",
            "https://right.example.com/value",
        )]))
        .await
        .unwrap();

    let left_request = left
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let right_request = right
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(left_request.url, "https://left.example.com/value");
    assert_eq!(right_request.url, "https://right.example.com/value");
    succeed(&left, &left_request).await;
    succeed(&right, &right_request).await;

    left.close().await.unwrap();
    right.close().await.unwrap();
    fixture.clear(&left_namespace).await;
    fixture.clear(&right_namespace).await;
}
