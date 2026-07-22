use contrib::scheduler::redis::Redis;
use spider::scheduler::Init;
use spider::{Scheduler, payload, trace};

use super::common::{
    HTTP, WORKER_A, namespace, owned_request, request, request_key, scheduler, succeed,
};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn redis_errors_distinguish_lifecycle_data_and_availability_failures() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let error_namespace = namespace("errors");
    let scheduler = scheduler(&fixture, &error_namespace);

    let unopened = scheduler.trace("trace").await.unwrap_err();
    assert!(!unopened.is_transient());
    scheduler.open().await.unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("SET")
        .arg(format!("{error_namespace}:traces"))
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    let wrong_type = scheduler.trace("trace").await.unwrap_err();
    assert!(
        !wrong_type.is_transient(),
        "WRONGTYPE is a deterministic backend data error: {wrong_type}"
    );
    scheduler.close().await.unwrap();

    let unavailable = Redis::new("redis://127.0.0.1:1")
        .unwrap()
        .with_namespace(namespace("unavailable"))
        .unwrap()
        .open()
        .await
        .unwrap_err();
    assert!(unavailable.is_transient());

    fixture.clear(&error_namespace).await;
}

#[tokio::test]
async fn malformed_records_do_not_discard_valid_claims_from_the_same_call() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("malformed");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut broken_trace = owned_request(
        "broken-trace",
        "https://example.com/broken-trace",
        "broken-task",
        "broken-trace-id",
    );
    broken_trace.priority = 30;
    broken_trace.max_retry_count = 1;
    scheduler
        .init(
            "broken-trace-id".to_string(),
            trace::Snapshot::code("broken-task"),
            vec![broken_trace],
        )
        .await
        .unwrap();
    let mut broken_request = request("broken-request", "https://example.com/broken-request");
    broken_request.priority = 20;
    broken_request.max_retry_count = 1;
    let mut valid = request("valid-request", "https://example.com/valid-request");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![broken_request, valid]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(format!("{namespace}:traces"))
        .arg("broken-trace-id")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(request_key(&namespace, "broken-request"))
        .arg("snapshot")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(3, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-request");
    succeed(&scheduler, &claimed[0]).await;
    for id in ["broken-trace", "broken-request"] {
        let state = redis::cmd("HGET")
            .arg(request_key(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, "failed");
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn push_rejects_a_trace_whose_snapshot_was_removed() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("missing-trace");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();
    scheduler
        .init(
            "missing-trace".to_string(),
            trace::Snapshot::code("missing-task"),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HDEL")
        .arg(format!("{namespace}:traces"))
        .arg("missing-trace")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let request = owned_request(
        "missing-trace-request",
        "https://example.com/missing-trace",
        "missing-task",
        "missing-trace",
    );
    let error = scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap_err();
    assert!(matches!(error, spider::scheduler::Error::TraceNotFound(id) if id == "missing-trace"));
    let exists = redis::cmd("EXISTS")
        .arg(request_key(&namespace, "missing-trace-request"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}
