use spider::{Scheduler, payload};

use super::{key, request, server, settlement, worker};

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
    let digest: String = redis::cmd("HGET")
        .arg(&key)
        .arg("digest")
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
        .arg("digest")
        .query_async(&mut *connection)
        .await
        .unwrap();
    assert_eq!(
        unchanged, digest,
        "the tamper must not update the stored digest"
    );
}

#[tokio::test]
async fn tampered_snapshot_is_recovered_without_discarding_a_valid_claim() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("snapshot-integrity-terminal");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    let mut tampered = request::new("tampered-terminal", "https://example.com/original");
    tampered.priority = 20;
    tampered.max_retry_count = 1;
    let mut valid = request::new("valid-same-batch", "https://example.com/valid");
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

    let claimed = scheduler
        .next_requests(2, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-same-batch");
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
    assert_eq!(retry_count, "1");
    let error: String = redis::cmd("HGET")
        .arg(key::completion(&namespace, "tampered-terminal", 1))
        .arg("error")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(error.contains("digest does not match its content"));
    assert!(
        !scheduler
            .has_pending_requests(worker::A, worker::HTTP)
            .await
            .unwrap()
    );

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn tampered_snapshot_retries_then_reaches_the_retry_terminal_state() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("snapshot-integrity-retry");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

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

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let key = key::request(&namespace, "tampered-retry");
    let state: String = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "pending");
    let retry_count: String = redis::cmd("HGET")
        .arg(&key)
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(retry_count, "1");
    assert!(
        scheduler
            .has_pending_requests(worker::B, worker::HTTP)
            .await
            .unwrap()
    );

    assert!(
        scheduler
            .next_requests(1, worker::B, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let terminal_state: String = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(terminal_state, "failed");
    let terminal_retry: String = redis::cmd("HGET")
        .arg(&key)
        .arg("retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(terminal_retry, "2");
    assert!(
        !scheduler
            .has_pending_requests(worker::A, worker::HTTP)
            .await
            .unwrap()
    );

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
    scheduler.open().await.unwrap();

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
        .arg(i32::MAX)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let key = key::request(&namespace, "retry-limit");
    let state: String = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let max_retry_count: i32 = redis::cmd("HGET")
        .arg(&key)
        .arg("max_retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(max_retry_count, 1);
    let error: String = redis::cmd("HGET")
        .arg(key::completion(&namespace, "retry-limit", 1))
        .arg("error")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(error.contains("max_retry_count"));

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn another_request_snapshot_cannot_override_the_retry_limit() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("cross-request-retry-limit");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

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
    let (snapshot, digest): (String, String) = redis::cmd("HMGET")
        .arg(&source_key)
        .arg("snapshot")
        .arg("digest")
        .query_async(&mut connection)
        .await
        .unwrap();
    let target_key = key::request(&namespace, "retry-target");
    redis::cmd("HSET")
        .arg(&target_key)
        .arg("snapshot")
        .arg(snapshot)
        .arg("digest")
        .arg(digest)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let (state, max_retry_count): (String, i32) = redis::cmd("HMGET")
        .arg(&target_key)
        .arg("state")
        .arg("max_retry_count")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(max_retry_count, 1);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}
