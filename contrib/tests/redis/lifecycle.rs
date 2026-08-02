use spider::scheduler::Init;
use spider::{Scheduler, payload, trace};

use super::{key, request, server, settlement};

#[tokio::test]
async fn close_preserves_data_for_a_new_scheduler_instance() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("restart");
    let first = server.redis(&namespace);
    server::open(&first).await;
    super::run::init(&first).await;

    let initial = request::for_trace(
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

    let second = server.redis(&namespace);
    server::open(&second).await;
    super::run::init(&second).await;
    assert_eq!(
        second
            .trace("restart-trace")
            .await
            .unwrap()
            .unwrap()
            .task_id,
        "restart-task"
    );
    let claimed = second.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(claimed.id, "restart-request");
    settlement::succeed(&second, &claimed).await;
    second.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn namespaces_isolate_identical_request_ids() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let left_namespace = server::namespace("left");
    let right_namespace = server::namespace("right");
    let left = server.redis(&left_namespace);
    let right = server.redis(&right_namespace);
    server::open(&left).await;
    super::run::init(&left).await;
    server::open(&right).await;
    super::run::init(&right).await;

    left.push(payload::Payload::new().requests(vec![request::new(
        "shared-id",
        "https://left.example.com/value",
    )]))
    .await
    .unwrap();
    assert!(!right.has_pending_requests().await.unwrap());
    right
        .push(payload::Payload::new().requests(vec![request::new(
            "shared-id",
            "https://right.example.com/value",
        )]))
        .await
        .unwrap();

    let left_request = left.next_requests(1).await.unwrap().pop().unwrap();
    let right_request = right.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(left_request.url, "https://left.example.com/value");
    assert_eq!(right_request.url, "https://right.example.com/value");
    settlement::succeed(&left, &left_request).await;
    settlement::succeed(&right, &right_request).await;

    left.close().await.unwrap();
    right.close().await.unwrap();
    server.clear(&left_namespace).await;
    server.clear(&right_namespace).await;
}

#[tokio::test]
async fn terminal_settlement_clears_lease_and_queue_fields() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("terminal-fields");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "terminal-success",
            "https://example.com/terminal-success",
        )]))
        .await
        .unwrap();
    let succeeded = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    settlement::succeed(&scheduler, &succeeded).await;

    let mut failed_request =
        request::new("terminal-failure", "https://example.com/terminal-failure");
    failed_request.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![failed_request]))
        .await
        .unwrap();
    let failed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .ack(&settlement::processing(&failed))
        .await
        .unwrap();
    let mut failure =
        payload::Payload::for_request(&failed, failed.leased_by.clone()).failed("failed");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();

    let mut connection = server.connection().await;
    for (id, state) in [("terminal-success", "done"), ("terminal-failure", "failed")] {
        let key = key::request(&namespace, id);
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
    server.clear(&namespace).await;
}
