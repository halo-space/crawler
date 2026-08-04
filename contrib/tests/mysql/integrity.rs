use std::time::Duration;

use spider::scheduler::Init as _;
use spider::{Scheduler as _, payload, trace};
use sqlx::Row as _;

use super::{fixture, server};

#[tokio::test]
async fn invalid_initial_queue_state_records_a_version_zero_completion() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("queue-integrity").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    let request = fixture::request("invalid-queue");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    sqlx::query("UPDATE queues SET priority = priority + 1 WHERE request_id = ?")
        .bind("invalid-queue")
        .execute(database.pool())
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    let row = sqlx::query(
        "SELECT r.state, c.version, c.state AS completion_state, c.error \
         FROM requests r \
         INNER JOIN completions c ON c.request_id = r.id AND c.version = 0 \
         WHERE r.id = ?",
    )
    .bind("invalid-queue")
    .fetch_one(database.pool())
    .await
    .unwrap();
    assert_eq!(utf8(&row, "state"), "failed");
    assert_eq!(row.try_get::<i64, _>("version").unwrap(), 0);
    assert_eq!(utf8(&row, "completion_state"), "failed");
    assert!(utf8(&row, "error").contains("queue does not match"));

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn request_ids_that_differ_by_trailing_spaces_remain_distinct() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("identity-collation").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![
            fixture::request("exact-id"),
            fixture::request("exact-id "),
        ]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(
        claimed
            .iter()
            .map(|request| request.id.as_str())
            .collect::<std::collections::HashSet<_>>(),
        ["exact-id", "exact-id "].into_iter().collect()
    );

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn missing_trace_is_released_without_consuming_a_retry() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("missing-trace-release").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![fixture::request("missing-trace")]))
        .await
        .unwrap();

    sqlx::query("DELETE FROM traces WHERE id = ?")
        .bind(fixture::TRACE_ID)
        .execute(database.pool())
        .await
        .unwrap();

    let claimed = tokio::time::timeout(Duration::from_secs(1), scheduler.next_requests(1))
        .await
        .expect("claim repeatedly selected the released Request")
        .unwrap();
    assert!(claimed.is_empty());

    let row =
        sqlx::query("SELECT state, retry_count, leased_by, lease_time FROM requests WHERE id = ?")
            .bind("missing-trace")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(utf8(&row, "state"), "pending");
    assert_eq!(row.try_get::<i32, _>("retry_count").unwrap(), 0);
    assert_eq!(utf8(&row, "leased_by"), "");
    assert_eq!(row.try_get::<i64, _>("lease_time").unwrap(), 0);
    let failed_workers =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM failed_workers WHERE request_id = ?")
            .bind("missing-trace")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(failed_workers, 0);

    scheduler
        .init(
            fixture::TRACE_ID.to_string(),
            trace::Snapshot::code(fixture::TASK_ID),
            Vec::new(),
        )
        .await
        .unwrap();
    let repaired = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(repaired.id, "missing-trace");
    assert_eq!(repaired.retry_count, 0);
    scheduler
        .ack(&fixture::processing(&repaired))
        .await
        .unwrap();
    scheduler
        .success(&fixture::success(&repaired))
        .await
        .unwrap();

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn immutable_snapshot_controls_the_retry_limit() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("immutable-retry-limit").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    let mut request = fixture::request("retry-limit");
    request.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    sqlx::query("UPDATE requests SET max_retry_count = 2 WHERE id = ?")
        .bind("retry-limit")
        .execute(database.pool())
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    let row = sqlx::query("SELECT state, retry_count, max_retry_count FROM requests WHERE id = ?")
        .bind("retry-limit")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(utf8(&row, "state"), "failed");
    assert_eq!(row.try_get::<i32, _>("retry_count").unwrap(), 1);
    assert_eq!(row.try_get::<i32, _>("max_retry_count").unwrap(), 1);

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn failure_uses_the_snapshot_retry_limit_after_a_claim() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("settlement-retry-limit").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;

    let mut request = fixture::request("settlement-retry-limit");
    request.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler.ack(&fixture::processing(&claimed)).await.unwrap();

    sqlx::query("UPDATE requests SET max_retry_count = 2 WHERE id = ?")
        .bind(&claimed.id)
        .execute(database.pool())
        .await
        .unwrap();

    scheduler
        .failure(&fixture::failure(&claimed, "boom"))
        .await
        .unwrap();
    let row = sqlx::query("SELECT state, retry_count, max_retry_count FROM requests WHERE id = ?")
        .bind(&claimed.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(utf8(&row, "state"), "failed");
    assert_eq!(row.try_get::<i32, _>("retry_count").unwrap(), 1);
    assert_eq!(row.try_get::<i32, _>("max_retry_count").unwrap(), 1);

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn damaged_snapshot_is_quarantined_without_trusting_mutable_retry_fields() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("damaged-snapshot-quarantine").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    let mut request = fixture::request("damaged-snapshot");
    request.priority = 20;
    request.max_retry_count = 2;
    let mut valid = fixture::request("valid-after-damaged-snapshot");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![request, valid]))
        .await
        .unwrap();

    sqlx::query("UPDATE requests SET snapshot = JSON_SET(snapshot, '$.url', ?) WHERE id = ?")
        .bind("https://example.com/tampered")
        .bind("damaged-snapshot")
        .execute(database.pool())
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-after-damaged-snapshot");
    scheduler
        .ack(&fixture::processing(&claimed[0]))
        .await
        .unwrap();
    scheduler
        .success(&fixture::success(&claimed[0]))
        .await
        .unwrap();
    let row = sqlx::query("SELECT state, retry_count FROM requests WHERE id = ?")
        .bind("damaged-snapshot")
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert_eq!(utf8(&row, "state"), "failed");
    assert_eq!(row.try_get::<i32, _>("retry_count").unwrap(), 0);
    let failed_workers =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM failed_workers WHERE request_id = ?")
            .bind("damaged-snapshot")
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(failed_workers, 0);

    scheduler.close().await.unwrap();
    database.remove().await;
}

fn utf8(row: &sqlx::mysql::MySqlRow, column: &str) -> String {
    String::from_utf8(row.try_get::<Vec<u8>, _>(column).unwrap()).unwrap()
}
