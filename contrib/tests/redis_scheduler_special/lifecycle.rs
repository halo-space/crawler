use spider::scheduler::Init;
use spider::{Scheduler, payload, trace};

use super::common::{
    HTTP, WORKER_A, WORKER_B, namespace, owned_request, processing_payload, request, request_key,
    scheduler, succeed,
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

#[tokio::test]
async fn terminal_settlement_clears_lease_and_queue_fields() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("terminal-fields");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "terminal-success",
            "https://example.com/terminal-success",
        )]))
        .await
        .unwrap();
    let succeeded = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    succeed(&scheduler, &succeeded).await;

    let mut failed_request = request("terminal-failure", "https://example.com/terminal-failure");
    failed_request.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![failed_request]))
        .await
        .unwrap();
    let failed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing_payload(&failed)).await.unwrap();
    let mut failure =
        payload::Payload::for_request(&failed, failed.leased_by.clone()).failed("failed");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();

    let mut connection = fixture.connection().await;
    for (id, state) in [("terminal-success", "done"), ("terminal-failure", "failed")] {
        let key = request_key(&namespace, id);
        for (field, expected) in [
            ("state", state),
            ("leased_by", ""),
            ("lease_time", "0"),
            ("ack_version", ""),
            ("queue_kind", ""),
            ("queue_member", ""),
        ] {
            let actual = redis::cmd("HGET")
                .arg(&key)
                .arg(field)
                .query_async::<String>(&mut connection)
                .await
                .unwrap();
            assert_eq!(actual, expected, "unexpected {field} for {id}");
        }
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}
