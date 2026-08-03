use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use spider::scheduler::Init as _;
use spider::{Scheduler as _, payload, trace};
use sqlx::Row as _;

use super::{fixture, server};

#[tokio::test]
async fn different_workers_claim_disjoint_requests() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-claims").await;
    let first = Arc::new(fixture::scheduler(database.url(), "worker-a"));
    let second = Arc::new(fixture::scheduler(database.url(), "worker-b"));
    fixture::open(&first).await;
    fixture::open(&second).await;
    fixture::init(&first).await;

    first
        .push(
            payload::Payload::new().requests(
                (0..32)
                    .map(|index| fixture::request(&format!("claim-{index}")))
                    .collect(),
            ),
        )
        .await
        .unwrap();
    let ready = Arc::new(tokio::sync::Barrier::new(3));
    let left = tokio::spawn({
        let scheduler = first.clone();
        let ready = ready.clone();
        async move {
            ready.wait().await;
            scheduler.next_requests(16).await
        }
    });
    let right = tokio::spawn({
        let scheduler = second.clone();
        let ready = ready.clone();
        async move {
            ready.wait().await;
            scheduler.next_requests(16).await
        }
    });
    ready.wait().await;
    let (left, right) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(left, right) })
            .await
            .expect("concurrent MySQL claims timed out");
    let left = left.unwrap().unwrap();
    let right = right.unwrap().unwrap();
    assert_eq!(left.len(), 16);
    assert_eq!(right.len(), 16);
    let ids = left
        .iter()
        .chain(&right)
        .map(|request| request.id.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(ids.len(), 32);
    assert!(left.iter().all(|request| request.leased_by == "worker-a"));
    assert!(right.iter().all(|request| request.leased_by == "worker-b"));

    for request in &left {
        first.ack(&fixture::processing(request)).await.unwrap();
        first.success(&fixture::success(request)).await.unwrap();
    }
    for request in &right {
        second.ack(&fixture::processing(request)).await.unwrap();
        second.success(&fixture::success(request)).await.unwrap();
    }
    first.close().await.unwrap();
    second.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn claim_skips_a_request_locked_by_replay_and_fills_the_limit() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("claim-past-replay").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![
            fixture::request("locked-prefix"),
            fixture::request("available-after-prefix"),
        ]))
        .await
        .unwrap();

    // Request replay locks the authoritative Request row before it checks the
    // immutable Snapshot hash. Claim must skip that row instead of waiting.
    let mut replay = database.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM requests WHERE id = ? FOR UPDATE")
        .bind("locked-prefix")
        .fetch_one(&mut *replay)
        .await
        .unwrap();

    let claimed = tokio::time::timeout(Duration::from_secs(1), scheduler.next_requests(1))
        .await
        .expect("claim waited for a replay-locked Request")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "available-after-prefix");
    replay.commit().await.unwrap();

    scheduler.ack(&fixture::processing(&claimed)).await.unwrap();
    scheduler
        .success(&fixture::success(&claimed))
        .await
        .unwrap();
    let prefix = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(prefix.id, "locked-prefix");
    scheduler.ack(&fixture::processing(&prefix)).await.unwrap();
    scheduler.success(&fixture::success(&prefix)).await.unwrap();

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn a_failed_worker_is_excluded_but_another_worker_can_retry() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-retry").await;
    let first = fixture::scheduler(database.url(), "worker-a");
    let second = fixture::scheduler(database.url(), "worker-b");
    fixture::open(&first).await;
    fixture::open(&second).await;
    first
        .init(
            fixture::TRACE_ID.to_string(),
            trace::Snapshot::code(fixture::TASK_ID),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut request = fixture::request("retry-on-another-worker");
    request.max_retry_count = 3;
    first
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let failed = first.next_requests(1).await.unwrap().pop().unwrap();
    first.ack(&fixture::processing(&failed)).await.unwrap();
    first
        .failure(&fixture::failure(&failed, "download failed"))
        .await
        .unwrap();

    assert!(first.next_requests(1).await.unwrap().is_empty());
    assert!(!first.has_pending_requests().await.unwrap());
    let retried = second.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(retried.id, failed.id);
    assert_eq!(retried.version, failed.version + 1);
    assert_eq!(retried.retry_count, failed.retry_count + 1);
    assert_eq!(retried.failed_workers, ["worker-a"]);
    second.ack(&fixture::processing(&retried)).await.unwrap();
    second.success(&fixture::success(&retried)).await.unwrap();

    first.close().await.unwrap();
    second.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn claimed_requests_receive_a_lease_time_near_transaction_commit() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("claim-lease-time").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![fixture::request("claim-lease-time-request")]))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE claim_probe (stamp BIGINT NOT NULL)")
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO claim_probe (stamp) VALUES (0)")
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::raw_sql(
        "CREATE TRIGGER slow_request_claim BEFORE UPDATE ON requests FOR EACH ROW \
         BEGIN \
             IF OLD.state = 'pending' AND NEW.state = 'processing' THEN \
                 DO SLEEP(0.2); \
                 UPDATE claim_probe SET stamp = \
                     CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED); \
             END IF; \
         END",
    )
    .execute(database.pool())
    .await
    .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let row = sqlx::query(
        "SELECT r.lease_time, p.stamp \
         FROM requests r CROSS JOIN claim_probe p WHERE r.id = ?",
    )
    .bind(&claimed.id)
    .fetch_one(database.pool())
    .await
    .unwrap();
    let stored = row.try_get::<i64, _>("lease_time").unwrap();
    let probe = row.try_get::<i64, _>("stamp").unwrap();
    assert_eq!(claimed.lease_time, stored);
    assert!(
        stored >= probe,
        "returned lease_time predates the final claim update: probe={probe}, lease_time={stored}"
    );

    scheduler.ack(&fixture::processing(&claimed)).await.unwrap();
    scheduler
        .success(&fixture::success(&claimed))
        .await
        .unwrap();
    scheduler.close().await.unwrap();
    database.remove().await;
}
