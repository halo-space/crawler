use std::sync::Arc;
use std::time::Duration;

use spider::stats::Counter;
use spider::{Scheduler as _, payload};
use sqlx::Row as _;

use super::{fixture, server};

#[tokio::test]
async fn settlement_rejects_a_scheduler_that_does_not_own_the_lease() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("settlement-lease-owner").await;
    let owner = fixture::scheduler(database.url(), "worker-a");
    let other = fixture::scheduler(database.url(), "worker-b");
    fixture::open(&owner).await;
    fixture::open(&other).await;
    fixture::init(&owner).await;

    let mut failed = fixture::request("lease-owner-failure");
    failed.max_retry_count = 1;
    owner
        .push(payload::Payload::new().requests(vec![
            fixture::request("lease-owner-ack"),
            fixture::request("lease-owner-release"),
            fixture::request("lease-owner-refresh"),
            fixture::request("lease-owner-success"),
            failed,
        ]))
        .await
        .unwrap();
    let mut claimed = owner.next_requests(5).await.unwrap();
    assert_eq!(claimed.len(), 5);

    let acked = take(&mut claimed, "lease-owner-ack");
    let ack = fixture::processing(&acked);
    assert_lease_mismatch(other.ack(&ack).await.unwrap_err(), &acked.id);
    let mut owner_ack = fixture::processing(&acked);
    owner_ack.worker_id = "worker-b".to_string();
    owner.ack(&owner_ack).await.unwrap();
    owner.success(&fixture::success(&acked)).await.unwrap();

    let released = take(&mut claimed, "lease-owner-release");
    let release = fixture::processing(&released);
    assert_lease_mismatch(other.release(&release).await.unwrap_err(), &released.id);
    let mut owner_release = fixture::processing(&released);
    owner_release.worker_id = "worker-b".to_string();
    owner.release(&owner_release).await.unwrap();

    let refreshed = take(&mut claimed, "lease-owner-refresh");
    owner.ack(&fixture::processing(&refreshed)).await.unwrap();
    let refresh = fixture::processing(&refreshed);
    assert_lease_mismatch(
        other.refresh_lease(&refresh).await.unwrap_err(),
        &refreshed.id,
    );
    let mut owner_refresh = fixture::processing(&refreshed);
    owner_refresh.worker_id = "worker-b".to_string();
    owner.refresh_lease(&owner_refresh).await.unwrap();
    owner.success(&fixture::success(&refreshed)).await.unwrap();

    let succeeded = take(&mut claimed, "lease-owner-success");
    owner.ack(&fixture::processing(&succeeded)).await.unwrap();
    let success = fixture::success(&succeeded);
    assert_lease_mismatch(other.success(&success).await.unwrap_err(), &succeeded.id);
    let mut owner_success = fixture::success(&succeeded);
    owner_success.worker_id = "worker-b".to_string();
    owner.success(&owner_success).await.unwrap();
    owner.success(&owner_success).await.unwrap();
    assert_lease_mismatch(
        other.success(&owner_success).await.unwrap_err(),
        &succeeded.id,
    );

    let failed = take(&mut claimed, "lease-owner-failure");
    owner.ack(&fixture::processing(&failed)).await.unwrap();
    let failure = fixture::failure(&failed, "boom");
    assert_lease_mismatch(other.failure(&failure).await.unwrap_err(), &failed.id);
    let mut owner_failure = fixture::failure(&failed, "boom");
    owner_failure.worker_id = "worker-b".to_string();
    owner.failure(&owner_failure).await.unwrap();
    owner.failure(&owner_failure).await.unwrap();
    assert_lease_mismatch(other.failure(&owner_failure).await.unwrap_err(), &failed.id);

    assert!(claimed.is_empty());
    owner.close().await.unwrap();
    other.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn stats_merge_and_overflow_are_transactional() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("stats").await;
    let scheduler = fixture::scheduler(database.url(), "worker-a");
    fixture::open(&scheduler).await;
    fixture::init(&scheduler).await;

    scheduler
        .push(payload::Payload::new().requests(vec![
            fixture::request("stats-first"),
            fixture::request("stats-second"),
            fixture::request("stats-overflow"),
        ]))
        .await
        .unwrap();
    for total in [2, 3] {
        let request = scheduler.next_requests(1).await.unwrap().pop().unwrap();
        scheduler.ack(&fixture::processing(&request)).await.unwrap();
        let mut success = fixture::success(&request);
        success.stats.insert(
            "requests".to_string(),
            serde_json::to_value(Counter {
                total,
                done: 1,
                ..Counter::default()
            })
            .unwrap(),
        );
        scheduler.success(&success).await.unwrap();
    }

    let row =
        sqlx::query("SELECT total, done FROM trace_stats WHERE trace_id = ? AND name = 'requests'")
            .bind(fixture::TRACE_ID)
            .fetch_one(database.pool())
            .await
            .unwrap();
    assert_eq!(row.get::<i64, _>("total"), 5);
    assert_eq!(row.get::<i64, _>("done"), 2);

    sqlx::query("UPDATE trace_stats SET total = ? WHERE trace_id = ? AND name = 'requests'")
        .bind(i64::MAX)
        .bind(fixture::TRACE_ID)
        .execute(database.pool())
        .await
        .unwrap();
    let request = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler.ack(&fixture::processing(&request)).await.unwrap();
    let mut overflow = fixture::success(&request);
    overflow.stats.insert(
        "requests".to_string(),
        serde_json::to_value(Counter {
            total: 1,
            ..Counter::default()
        })
        .unwrap(),
    );
    let error = scheduler.success(&overflow).await.unwrap_err();
    assert!(error.to_string().contains("stats counter overflow"));

    scheduler
        .success(&fixture::success(&request))
        .await
        .unwrap();
    assert!(!scheduler.has_pending_requests().await.unwrap());
    scheduler.close().await.unwrap();
    database.remove().await;
}

#[tokio::test]
async fn refresh_reads_database_time_after_locking_the_request() {
    let Some(server) = server::Server::connect().await else {
        return;
    };
    let database = server.database("refresh-time-lock").await;
    let scheduler = Arc::new(fixture::scheduler(database.url(), "worker-a"));
    fixture::open(scheduler.as_ref()).await;
    fixture::init(scheduler.as_ref()).await;
    scheduler
        .push(payload::Payload::new().requests(vec![fixture::request("refresh-time-lock-request")]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler.ack(&fixture::processing(&claimed)).await.unwrap();

    let mut blocker = database.pool().begin().await.unwrap();
    sqlx::query("SELECT id FROM requests WHERE id = ? FOR UPDATE")
        .bind(&claimed.id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();
    let refresh = tokio::spawn({
        let scheduler = scheduler.clone();
        let payload = fixture::processing(&claimed);
        async move { scheduler.refresh_lease(&payload).await }
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    let unlocked_at = sqlx::query_scalar::<_, i64>(
        "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS SIGNED)",
    )
    .fetch_one(&mut *blocker)
    .await
    .unwrap();
    blocker.commit().await.unwrap();
    refresh.await.unwrap().unwrap();

    let lease_time = sqlx::query_scalar::<_, i64>("SELECT lease_time FROM requests WHERE id = ?")
        .bind(&claimed.id)
        .fetch_one(database.pool())
        .await
        .unwrap();
    assert!(
        lease_time >= unlocked_at,
        "refresh used time from before the Request lock: unlocked_at={unlocked_at}, lease_time={lease_time}"
    );

    scheduler
        .success(&fixture::success(&claimed))
        .await
        .unwrap();
    scheduler.close().await.unwrap();
    database.remove().await;
}

fn take(requests: &mut Vec<spider::net::Request>, id: &str) -> spider::net::Request {
    let index = requests
        .iter()
        .position(|request| request.id == id)
        .unwrap();
    requests.swap_remove(index)
}

fn assert_lease_mismatch(error: spider::scheduler::Error, id: &str) {
    assert!(matches!(
        error,
        spider::scheduler::Error::LeaseMismatch(request_id) if request_id == id
    ));
}
