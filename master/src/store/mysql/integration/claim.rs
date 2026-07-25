use sqlx::Row;
use sqlx::types::Json;

use super::super::time::now_millis;
use super::{Database, Result, claim, init, init_body, require, snapshot};
use crate::types;

#[tokio::test]
async fn claim_starts_the_lease_after_recovery_waits() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
}

#[tokio::test]
async fn claim_scans_multiple_storage_pages_in_queue_order() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_paged_claim(&database).await;
    database.teardown(result).await
}

#[tokio::test]
async fn claim_bounds_invalid_cleanup_and_resumes_later() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_invalid_budget(&database).await;
    database.teardown(result).await
}

#[tokio::test]
async fn claim_normalizes_retry_limit_when_quarantining_a_corrupt_projection() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_retry_limit_repair(&database).await;
    database.teardown(result).await
}

async fn exercise_retry_limit_repair(database: &Database) -> Result<()> {
    init(
        database,
        "retry-limit-init",
        "retry-limit-task",
        "retry-limit-trace",
        &[
            "corrupt-retry-limit",
            "corrupt-retry-state",
            "valid-retry-limit",
        ],
        3,
    )
    .await?;
    sqlx::query(
        "UPDATE requests SET max_retry_count = ? WHERE namespace = ? AND id = 'corrupt-retry-limit'",
    )
    .bind(spider::net::request::MAX_RETRY_COUNT + 1)
    .bind(&database.namespace)
    .execute(&database.store.pool)
    .await?;
    let failed_workers = (0..spider::net::request::MAX_RETRY_COUNT + 2)
        .map(|index| format!("failed-worker-{index}"))
        .collect::<Vec<_>>();
    sqlx::query(
        "UPDATE requests SET retry_count = ?, failed_workers = ? \
         WHERE namespace = ? AND id = 'corrupt-retry-state'",
    )
    .bind(spider::net::request::MAX_RETRY_COUNT + 1)
    .bind(Json(&failed_workers))
    .bind(&database.namespace)
    .execute(&database.store.pool)
    .await?;

    let claimed = claim(database, "retry-limit-claim", "retry-limit-worker", 3).await?;
    require(
        claimed.len() == 1 && claimed[0].snapshot.id == "valid-retry-limit",
        "corrupt retry projection prevented the valid Request from being claimed",
    )?;
    let row = sqlx::query(
        "SELECT state, retry_count, max_retry_count FROM requests \
         WHERE namespace = ? AND id = 'corrupt-retry-limit'",
    )
    .bind(&database.namespace)
    .fetch_one(&database.store.pool)
    .await?;
    require(
        row.try_get::<i8, _>("state")? == 3
            && row.try_get::<i32, _>("retry_count")? == 3
            && row.try_get::<i32, _>("max_retry_count")? == 3,
        "terminal quarantine did not restore the immutable Snapshot retry limit",
    )?;
    let row = sqlx::query(
        "SELECT state, retry_count, max_retry_count, JSON_LENGTH(failed_workers) AS failed_count \
         FROM requests WHERE namespace = ? AND id = 'corrupt-retry-state'",
    )
    .bind(&database.namespace)
    .fetch_one(&database.store.pool)
    .await?;
    require(
        row.try_get::<i8, _>("state")? == 3
            && row.try_get::<i32, _>("retry_count")? == spider::net::request::MAX_RETRY_COUNT
            && row.try_get::<i32, _>("max_retry_count")? == spider::net::request::MAX_RETRY_COUNT
            && row.try_get::<i64, _>("failed_count")?
                == i64::from(spider::net::request::MAX_RETRY_COUNT),
        "terminal quarantine did not bound corrupt retry state consistently",
    )
}

async fn exercise_invalid_budget(database: &Database) -> Result<()> {
    let ids = (0..130)
        .map(|index| format!("invalid-budget-{index:03}"))
        .collect::<Vec<_>>();
    let borrowed = ids.iter().map(String::as_str).collect::<Vec<_>>();
    init(
        database,
        "invalid-budget-init",
        "invalid-budget-task",
        "invalid-budget-trace",
        &borrowed,
        1,
    )
    .await?;
    sqlx::query(
        "UPDATE requests SET snapshot = JSON_OBJECT('broken', true) \
         WHERE namespace = ? AND id <> ?",
    )
    .bind(&database.namespace)
    .bind(ids.last().unwrap())
    .execute(&database.store.pool)
    .await?;

    let first = claim(database, "invalid-budget-first", "invalid-budget-worker", 1).await?;
    require(
        first.is_empty(),
        "claim crossed its invalid-record maintenance budget",
    )?;
    require(
        failed(database).await? == 128,
        "claim did not apply the exact invalid-record maintenance budget",
    )?;

    let replay = claim(database, "invalid-budget-first", "invalid-budget-worker", 1).await?;
    require(
        replay.is_empty() && failed(database).await? == 128,
        "claim replay performed additional invalid-record maintenance",
    )?;

    let second = claim(
        database,
        "invalid-budget-second",
        "invalid-budget-worker",
        1,
    )
    .await?;
    require(
        second.len() == 1 && second[0].snapshot.id == *ids.last().unwrap(),
        "later claim did not resume after the invalid-record maintenance budget",
    )?;
    require(
        failed(database).await? == 129,
        "later claim did not quarantine the remaining invalid Request",
    )
}

async fn failed(database: &Database) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM requests WHERE namespace = ? AND state = 3")
            .bind(&database.namespace)
            .fetch_one(&database.store.pool)
            .await?,
    )
}

async fn exercise_paged_claim(database: &Database) -> Result<()> {
    let ids = (0..129)
        .map(|index| format!("paged-request-{index:03}"))
        .collect::<Vec<_>>();
    let mut requests = ids
        .iter()
        .map(|id| snapshot(id, "paged-claim-task", "paged-claim-trace", 1))
        .collect::<Vec<_>>();
    requests.last_mut().unwrap().priority = 1;
    database
        .store
        .init(
            &database.namespace,
            "paged-claim-init",
            &init_body("paged-claim-task", "paged-claim-trace", requests),
        )
        .await?;

    let claimed = claim(
        database,
        "paged-claim-operation",
        "paged-claim-worker",
        ids.len(),
    )
    .await?;
    let lease_time = claimed.first().unwrap().execution.lease_time;
    require(
        claimed
            .iter()
            .all(|request| request.execution.lease_time == lease_time),
        "paged claim returned different lease start times",
    )?;
    let claimed_ids = claimed
        .into_iter()
        .map(|request| request.snapshot.id)
        .collect::<Vec<_>>();
    let expected = std::iter::once(ids.last().unwrap().clone())
        .chain(ids[..ids.len() - 1].iter().cloned())
        .collect::<Vec<_>>();
    require(
        claimed_ids == expected,
        "paged claim changed the global priority/sequence order",
    )?;

    let replay = claim(
        database,
        "paged-claim-operation",
        "paged-claim-worker",
        ids.len(),
    )
    .await?;
    require(
        replay
            .iter()
            .all(|request| request.execution.lease_time == lease_time),
        "paged claim replay changed its lease start",
    )?;
    let replay = replay
        .into_iter()
        .map(|request| request.snapshot.id)
        .collect::<Vec<_>>();
    require(replay == expected, "paged claim replay changed its result")
}

async fn exercise(database: &Database) -> Result<()> {
    init(
        database,
        "fresh-lease-init",
        "fresh-lease-task",
        "fresh-lease-trace",
        &["expired-request", "ready-request"],
        2,
    )
    .await?;
    let expired = claim(database, "initial-claim", "expired-worker", 1).await?;
    require(
        expired.len() == 1,
        "initial claim did not lease one Request",
    )?;
    sqlx::query("UPDATE requests SET lease_time = 1 WHERE namespace = ? AND id = ?")
        .bind(&database.namespace)
        .bind(&expired[0].snapshot.id)
        .execute(&database.store.pool)
        .await?;

    let mut blocker = database.store.pool.begin().await?;
    let _: u64 = sqlx::query("SELECT value FROM queue_sequences WHERE namespace = ? FOR UPDATE")
        .bind(&database.namespace)
        .fetch_one(&mut *blocker)
        .await?
        .try_get("value")?;

    let store = database.store.clone();
    let namespace = database.namespace.clone();
    let mut waiting = tokio::spawn(async move {
        store
            .claim(
                &namespace,
                "waiting-claim",
                &types::Claim {
                    limit: 2,
                    worker_id: "ready-worker".to_string(),
                    modes: vec![spider::net::Mode::Http],
                },
            )
            .await
    });
    tokio::task::yield_now().await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let released_at = now_millis();
    blocker.commit().await?;

    let claimed = match tokio::time::timeout(std::time::Duration::from_secs(10), &mut waiting).await
    {
        Ok(result) => result??,
        Err(error) => {
            waiting.abort();
            let _ = waiting.await;
            return Err(error.into());
        }
    };
    require(
        claimed.requests.len() == 2,
        "claim did not return both available Requests",
    )?;
    require(
        claimed
            .requests
            .iter()
            .all(|request| request.execution.lease_time >= released_at),
        "claim lease started before recovery finished waiting",
    )?;
    let lease_time = claimed.requests[0].execution.lease_time;
    require(
        claimed
            .requests
            .iter()
            .all(|request| request.execution.lease_time == lease_time),
        "one claim returned different lease times",
    )?;
    require(
        claimed
            .requests
            .iter()
            .filter(|request| request.trace.is_some())
            .count()
            == 1,
        "one claim inlined the same Trace more than once",
    )
}
