use std::sync::Arc;
use std::time::Duration;

use spider::{Scheduler as _, payload};
use sqlx::Row as _;

use super::{fixture, server};

#[tokio::test]
async fn duplicate_worker_id_is_rejected_until_the_active_worker_closes() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-conflict").await;
    let first = fixture::scheduler(database.url(), "worker-a");
    let second = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&first).await;

    let error = second.open(16).await.unwrap_err();
    assert!(error.to_string().contains("code 100"));
    first.close().await.unwrap();

    second.open(16).await.unwrap();
    second.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn concurrent_first_registration_reports_the_worker_conflict_contract() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-concurrent-conflict").await;
    let first = Arc::new(fixture::scheduler(database.url(), "worker-a"));
    let second = Arc::new(fixture::scheduler(database.url(), "worker-a"));
    let ready = Arc::new(tokio::sync::Barrier::new(3));

    let left = tokio::spawn({
        let scheduler = first.clone();
        let ready = ready.clone();
        async move {
            ready.wait().await;
            scheduler.open(16).await
        }
    });
    let right = tokio::spawn({
        let scheduler = second.clone();
        let ready = ready.clone();
        async move {
            ready.wait().await;
            scheduler.open(16).await
        }
    });
    ready.wait().await;
    let (left, right) =
        tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(left, right) })
            .await
            .expect("concurrent MySQL Worker registration timed out");
    let left = left.unwrap();
    let right = right.unwrap();

    match (left, right) {
        (Ok(()), Err(error)) => {
            assert!(error.to_string().contains("code 100"), "{error}");
            first.close().await.unwrap();
        }
        (Err(error), Ok(())) => {
            assert!(error.to_string().contains("code 100"), "{error}");
            second.close().await.unwrap();
        }
        (left, right) => {
            panic!("expected one registration and one conflict, got {left:?} and {right:?}")
        }
    }

    database.remove().await;
}

#[tokio::test]
async fn a_crashed_worker_id_can_register_after_its_heartbeat_timeout() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-timeout").await;
    let interval = Duration::from_millis(50);
    let timeout = Duration::from_millis(200);
    let first = fixture::scheduler_with_heartbeat(database.url(), "worker-a", interval, timeout);
    fixture::open(&first).await;
    drop(first);

    tokio::time::sleep(timeout + Duration::from_millis(100)).await;
    let replacement =
        fixture::scheduler_with_heartbeat(database.url(), "worker-a", interval, timeout);
    replacement.open(16).await.unwrap();
    replacement.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn offline_or_expired_workers_do_not_claim_requests() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-online-state").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![fixture::request("worker-state-request")]))
        .await
        .unwrap();

    sqlx::query(
        "UPDATE workers SET offline_time = \
             CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED) \
         WHERE worker_id = ?",
    )
    .bind("worker-a")
    .execute(database.pool())
    .await
    .unwrap();
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    sqlx::query("UPDATE workers SET offline_time = NULL, last_heartbeat = 0 WHERE worker_id = ?")
        .bind("worker-a")
        .execute(database.pool())
        .await
        .unwrap();
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    sqlx::query(
        "UPDATE workers SET last_heartbeat = \
             CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED) \
         WHERE worker_id = ?",
    )
    .bind("worker-a")
    .execute(database.pool())
    .await
    .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "worker-state-request");

    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn claim_reads_database_time_after_locking_the_worker() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-time-lock").await;
    let scheduler = Arc::new(fixture::scheduler(database.url(), "worker-a"));
    fixture::open(scheduler.as_ref()).await;
    fixture::init(scheduler.as_ref()).await;
    scheduler
        .push(payload::Payload::new().requests(vec![fixture::request("worker-time-lock-request")]))
        .await
        .unwrap();

    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT worker_id FROM workers WHERE worker_id = ? FOR UPDATE")
        .bind("worker-a")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let claim = tokio::spawn({
        let scheduler = scheduler.clone();
        async move { scheduler.next_requests(1).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    sqlx::query(
        "UPDATE workers SET \
             last_heartbeat = CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED), \
             updated_time = CURRENT_TIMESTAMP(3) \
         WHERE worker_id = ?",
    )
    .bind("worker-a")
    .execute(&mut *blocker)
    .await
    .unwrap();
    blocker.commit().await.unwrap();

    let claimed = claim.await.unwrap().unwrap().pop().unwrap();
    scheduler.ack(&fixture::processing(&claimed)).await.unwrap();
    scheduler
        .success(&fixture::success(&claimed))
        .await
        .unwrap();
    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn registration_reads_database_time_after_locking_the_worker() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("worker-register-time-lock").await;
    let interval = Duration::from_millis(150);
    let timeout = Duration::from_millis(200);
    let original = fixture::scheduler_with_heartbeat(database.url(), "worker-a", interval, timeout);
    fixture::open(&original).await;
    original.close().await.unwrap();

    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT worker_id FROM workers WHERE worker_id = ? FOR UPDATE")
        .bind("worker-a")
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let replacement = Arc::new(fixture::scheduler_with_heartbeat(
        database.url(),
        "worker-a",
        interval,
        timeout,
    ));
    let registration = tokio::spawn({
        let scheduler = replacement.clone();
        async move { scheduler.open(16).await }
    });
    tokio::time::sleep(timeout + Duration::from_millis(100)).await;
    blocker.commit().await.unwrap();
    registration.await.unwrap().unwrap();

    let row = sqlx::query(
        "SELECT last_heartbeat, \
                CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED) AS now \
         FROM workers WHERE worker_id = ?",
    )
    .bind("worker-a")
    .fetch_one(database.pool())
    .await
    .unwrap();
    let last_heartbeat = row.try_get::<i64, _>("last_heartbeat").unwrap();
    let now = row.try_get::<i64, _>("now").unwrap();
    assert!(
        now.saturating_sub(last_heartbeat) < timeout.as_millis() as i64,
        "registration stored an already expired heartbeat: now={now}, last_heartbeat={last_heartbeat}"
    );

    replacement.close().await.unwrap();
    database.remove().await;
}
