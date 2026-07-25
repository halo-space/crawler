use sqlx::Row as _;

use super::{Database, Result, require};
use crate::wire;

#[tokio::test]
async fn repeated_polling_does_not_rewrite_a_fresh_worker() -> Result<()> {
    let Some(database) = Database::connect(8).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
}

async fn exercise(database: &Database) -> Result<()> {
    let worker = wire::Worker {
        worker_id: "worker-1".to_string(),
        modes: vec![spider::net::Mode::Http],
    };
    require(
        !database.store.pending(&database.namespace, &worker).await?,
        "empty Scheduler must not report pending work",
    )?;
    let first = timestamps(database).await?;

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    require(
        !database.store.pending(&database.namespace, &worker).await?,
        "repeated polling must remain empty",
    )?;
    let second = timestamps(database).await?;

    require(
        first == second,
        format!("fresh Worker timestamps changed during polling: {first:?} -> {second:?}"),
    )
}

async fn timestamps(database: &Database) -> Result<(i64, i64)> {
    let row = sqlx::query(
        "SELECT last_heartbeat, updated_time FROM workers WHERE namespace = ? AND id = ?",
    )
    .bind(&database.namespace)
    .bind("worker-1")
    .fetch_one(&database.store.pool)
    .await?;
    Ok((row.try_get("last_heartbeat")?, row.try_get("updated_time")?))
}
