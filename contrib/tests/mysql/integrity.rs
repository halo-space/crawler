use spider::{Scheduler as _, payload};
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

fn utf8(row: &sqlx::mysql::MySqlRow, column: &str) -> String {
    String::from_utf8(row.try_get::<Vec<u8>, _>(column).unwrap()).unwrap()
}
