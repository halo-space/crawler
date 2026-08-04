use spider::{Scheduler, payload};

use super::{key, request, run, server, settlement, worker};

async fn replace_snapshot_url(
    connection: &mut redis::aio::MultiplexedConnection,
    namespace: &str,
    id: &str,
    url: &str,
) {
    let key = key::request(namespace, id);
    let original: String = redis::cmd("HGET")
        .arg(&key)
        .arg("snapshot")
        .query_async(connection)
        .await
        .unwrap();
    let snapshot_hash: String = redis::cmd("HGET")
        .arg(&key)
        .arg("snapshot_hash")
        .query_async(&mut *connection)
        .await
        .unwrap();

    let mut snapshot: serde_json::Value = serde_json::from_str(&original).unwrap();
    snapshot["url"] = serde_json::Value::String(url.to_string());
    let tampered = serde_json::to_string(&snapshot).unwrap();
    redis::cmd("HSET")
        .arg(&key)
        .arg("snapshot")
        .arg(tampered)
        .query_async::<usize>(&mut *connection)
        .await
        .unwrap();

    let unchanged: String = redis::cmd("HGET")
        .arg(&key)
        .arg("snapshot_hash")
        .query_async(&mut *connection)
        .await
        .unwrap();
    assert_eq!(
        unchanged, snapshot_hash,
        "the tamper must not update the stored Snapshot hash"
    );
}

#[tokio::test]
async fn tampered_snapshot_is_quarantined_without_discarding_a_valid_claim() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("snapshot-integrity-terminal");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut tampered = request::new("tampered-terminal", "https://example.com/original");
    tampered.priority = 20;
    tampered.max_retry_count = 1;
    let mut valid = request::new("valid-alongside-tampered", "https://example.com/valid");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![tampered, valid]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    replace_snapshot_url(
        &mut connection,
        &namespace,
        "tampered-terminal",
        "https://example.com/changed-but-valid",
    )
    .await;

    let claimed = scheduler.next_requests(2).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-alongside-tampered");
    settlement::succeed(&scheduler, &claimed[0]).await;

    let tampered_key = key::request(&namespace, "tampered-terminal");
    let state: String = redis::cmd("HGET")
        .arg(&tampered_key)
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let retry_count: String = redis::cmd("HGET")
        .arg(&tampered_key)
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(retry_count, "0");
    let failed_workers: i64 = redis::cmd("LLEN")
        .arg(key::failed_workers(&namespace, "tampered-terminal"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(failed_workers, 0);
    let error: String = redis::cmd("HGET")
        .arg(key::completion(&namespace, "tampered-terminal", 1))
        .arg("error")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(error.contains("hash does not match its content"));
    assert!(!scheduler.has_pending_requests().await.unwrap());

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn tampered_snapshot_cannot_fall_back_to_the_stored_retry_limit() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("snapshot-integrity-retry");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut tampered = request::new("tampered-retry", "https://example.com/original");
    tampered.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![tampered]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    replace_snapshot_url(
        &mut connection,
        &namespace,
        "tampered-retry",
        "https://example.com/changed-but-valid",
    )
    .await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "tampered-retry"))
        .arg("retry_limit")
        .arg(128)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    let key = key::request(&namespace, "tampered-retry");
    let state: String = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let retry_count: String = redis::cmd("HGET")
        .arg(&key)
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(retry_count, "0");
    let failed_workers: i64 = redis::cmd("LLEN")
        .arg(key::failed_workers(&namespace, "tampered-retry"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(failed_workers, 0);
    assert!(!scheduler.has_pending_requests().await.unwrap());

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn mutable_hash_cannot_override_the_snapshot_retry_limit() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("snapshot-retry-limit");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut request = request::new("retry-limit", "https://example.com/retry-limit");
    request.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "retry-limit"))
        .arg("max_retry_count")
        .arg(2)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .ack(&settlement::processing(&claimed))
        .await
        .unwrap();
    let mut failed =
        payload::Payload::for_request(&claimed, claimed.leased_by.clone()).failed("boom");
    failed.start_time = Some(1);
    failed.end_time = Some(2);
    scheduler.failure(&failed).await.unwrap();

    let key = key::request(&namespace, "retry-limit");
    let (state, retry_count, retry_limit, max_retry_count): (String, i32, i32, i32) =
        redis::cmd("HMGET")
            .arg(&key)
            .arg("state")
            .arg("retry_count")
            .arg("retry_limit")
            .arg("max_retry_count")
            .query_async(&mut connection)
            .await
            .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(retry_count, 1);
    assert_eq!(retry_limit, 1);
    assert_eq!(max_retry_count, 1);
    assert!(!scheduler.has_pending_requests().await.unwrap());

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn retry_limit_tampering_cannot_change_an_active_request_budget() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("retry-limit-integrity");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut request = request::new(
        "retry-limit-tamper",
        "https://example.com/retry-limit-tamper",
    );
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .ack(&settlement::processing(&claimed))
        .await
        .unwrap();

    let request_key = key::request(&namespace, &claimed.id);
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(&request_key)
        .arg("retry_limit")
        .arg(128)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let mut failed =
        payload::Payload::for_request(&claimed, claimed.leased_by.clone()).failed("boom");
    failed.start_time = Some(1);
    failed.end_time = Some(2);
    let error = scheduler.failure(&failed).await.unwrap_err();
    assert!(error.to_string().contains("CORRUPT_REQUEST_RETRY"));

    redis::cmd("HSET")
        .arg(&request_key)
        .arg("lease_time")
        .arg(0)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(key::processing(&namespace, "http"))
        .arg(0)
        .arg(key::segment(&claimed.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    let (state, retry_count): (String, i32) = redis::cmd("HMGET")
        .arg(&request_key)
        .arg("state")
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(retry_count, 0);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn expired_lease_cannot_recover_without_a_snapshot_retry_limit() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("expired-snapshot-retry-limit");
    let scheduler = server.redis(&namespace);
    let worker_b = server.redis_as(&namespace, worker::B);
    server::open(&scheduler).await;
    server::open(&worker_b).await;
    super::run::init(&scheduler).await;

    let mut request = request::new(
        "expired-snapshot-retry-limit",
        "https://example.com/expired-snapshot-retry-limit",
    );
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .ack(&settlement::processing(&claimed))
        .await
        .unwrap();

    let request_key = key::request(&namespace, &claimed.id);
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(&request_key)
        .arg("snapshot")
        .arg("{}")
        .arg("retry_limit")
        .arg(128)
        .arg("lease_time")
        .arg(0)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(key::processing(&namespace, "http"))
        .arg(0)
        .arg(key::segment(&claimed.id))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(worker_b.next_requests(1).await.unwrap().is_empty());
    let (state, retry_count): (String, i32) = redis::cmd("HMGET")
        .arg(&request_key)
        .arg("state")
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(retry_count, 0);
    let failed_workers: i64 = redis::cmd("LLEN")
        .arg(key::failed_workers(&namespace, &claimed.id))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(failed_workers, 0);
    assert!(!worker_b.has_pending_requests().await.unwrap());

    scheduler.close().await.unwrap();
    worker_b.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn another_request_snapshot_cannot_override_the_retry_limit() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("cross-request-retry-limit");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut target = request::new("retry-target", "https://example.com/retry-target");
    target.priority = 20;
    target.max_retry_count = 1;
    let mut source = request::new("retry-source", "https://example.com/retry-source");
    source.priority = 10;
    source.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![target, source]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let source_key = key::request(&namespace, "retry-source");
    let (snapshot, snapshot_hash): (String, String) = redis::cmd("HMGET")
        .arg(&source_key)
        .arg("snapshot")
        .arg("snapshot_hash")
        .query_async(&mut connection)
        .await
        .unwrap();
    let target_key = key::request(&namespace, "retry-target");
    redis::cmd("HSET")
        .arg(&target_key)
        .arg("snapshot")
        .arg(snapshot)
        .arg("snapshot_hash")
        .arg(snapshot_hash)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    let (state, retry_limit, max_retry_count): (String, i32, i32) = redis::cmd("HMGET")
        .arg(&target_key)
        .arg("state")
        .arg("retry_limit")
        .arg("max_retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(retry_limit, 1);
    assert_eq!(max_retry_count, 1);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn replay_rejects_a_removed_trace_without_mutating_requests() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("missing-trace-replay");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    run::init(&scheduler).await;

    let replay = request::new(
        "missing-trace-replay-request",
        "https://example.com/missing-trace-replay",
    );
    scheduler
        .push(payload::Payload::new().requests(vec![replay.clone()]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let request_key = key::request(&namespace, "missing-trace-replay-request");
    let before = redis::cmd("HGETALL")
        .arg(&request_key)
        .query_async::<std::collections::HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HDEL")
        .arg(format!("{namespace}:traces"))
        .arg(run::TRACE_ID)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let missing = request::for_trace(
        "missing-before-conflict",
        "https://example.com/missing-before-conflict",
        run::TASK_ID,
        "missing-before-conflict-trace",
    );
    let conflict = request::new(
        "missing-trace-replay-request",
        "https://example.com/conflicting-snapshot",
    );
    let error = scheduler
        .push(payload::Payload::new().requests(vec![missing, conflict]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conflicts with existing Snapshot")
    );
    let missing_exists = redis::cmd("EXISTS")
        .arg(key::request(&namespace, "missing-before-conflict"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(missing_exists, 0);

    let new = request::new(
        "missing-trace-replay-new",
        "https://example.com/missing-trace-replay/new",
    );
    let error = scheduler
        .push(payload::Payload::new().requests(vec![replay, new]))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        spider::scheduler::Error::TraceNotFound(id) if id == run::TRACE_ID
    ));
    let after = redis::cmd("HGETALL")
        .arg(&request_key)
        .query_async::<std::collections::HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(after, before);
    let new_exists = redis::cmd("EXISTS")
        .arg(key::request(&namespace, "missing-trace-replay-new"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(new_exists, 0);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn replay_rejects_a_missing_trace_task_owner() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("missing-trace-task-owner");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    run::init(&scheduler).await;
    let replay = request::new(
        "missing-trace-task-owner-request",
        "https://example.com/missing-trace-task-owner",
    );
    scheduler
        .push(payload::Payload::new().requests(vec![replay.clone()]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HDEL")
        .arg(format!("{namespace}:trace_tasks"))
        .arg(run::TRACE_ID)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    let error = scheduler
        .push(payload::Payload::new().requests(vec![replay]))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        spider::scheduler::Error::TraceNotFound(id) if id == run::TRACE_ID
    ));

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn replay_rejects_a_mismatched_trace_task_owner() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("mismatched-trace-task-owner");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    run::init(&scheduler).await;
    let replay = request::new(
        "mismatched-trace-task-owner-request",
        "https://example.com/mismatched-trace-task-owner",
    );
    scheduler
        .push(payload::Payload::new().requests(vec![replay.clone()]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(format!("{namespace}:trace_tasks"))
        .arg(run::TRACE_ID)
        .arg("other-task")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    let error = scheduler
        .push(payload::Payload::new().requests(vec![replay]))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        spider::scheduler::Error::IdentityMismatch { id, field }
            if id == "mismatched-trace-task-owner-request" && field == "task_id"
    ));

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}
