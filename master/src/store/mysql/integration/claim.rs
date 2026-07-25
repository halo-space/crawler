use sqlx::Row;

use super::super::time::now_millis;
use super::{Database, Result, claim, init, require};
use crate::types;

#[tokio::test]
async fn claim_starts_the_lease_after_recovery_waits() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
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
