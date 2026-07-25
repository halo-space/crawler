use std::collections::HashMap;

use sqlx::Row;

use super::{Database, Result, claim, completion, identity, init, require};

#[tokio::test]
async fn requeued_requests_move_behind_untouched_work() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_requeue(&database).await;
    database.teardown(result).await
}

async fn exercise_requeue(database: &Database) -> Result<()> {
    init(
        database,
        "release-order-init",
        "order-task",
        "release-order-trace",
        &["release-first", "release-next"],
        2,
    )
    .await?;
    let released = claim(database, "release-order-claim", "release-worker", 1).await?;
    require(
        released[0].snapshot.id == "release-first",
        "release fixture lost FIFO",
    )?;
    database
        .store
        .release(
            &database.namespace,
            "release-order",
            &identity(&released[0], "release-worker"),
        )
        .await?;
    require(
        sequence(database, "release-first").await? > sequence(database, "release-next").await?,
        "released Request did not move behind untouched work",
    )?;
    delete_requests(database, "release-order-trace").await?;

    init(
        database,
        "failure-order-init",
        "order-task",
        "failure-order-trace",
        &["failure-first", "failure-next"],
        2,
    )
    .await?;
    let failed = claim(database, "failure-order-claim", "failure-worker", 1).await?;
    require(
        failed[0].snapshot.id == "failure-first",
        "failure fixture lost FIFO",
    )?;
    let identity = identity(&failed[0], "failure-worker");
    database.store.ack(&database.namespace, &identity).await?;
    database
        .store
        .failure(
            &database.namespace,
            &completion(identity, HashMap::new(), Some("retry")),
        )
        .await?;
    require(
        sequence(database, "failure-first").await? > sequence(database, "failure-next").await?,
        "failed Request retry did not move behind untouched work",
    )?;
    delete_requests(database, "failure-order-trace").await?;

    init(
        database,
        "recovery-order-init",
        "order-task",
        "recovery-order-trace",
        &["recovery-first", "recovery-next"],
        2,
    )
    .await?;
    let recovered = claim(database, "recovery-order-claim", "recovery-worker", 1).await?;
    require(
        recovered[0].snapshot.id == "recovery-first",
        "recovery fixture lost FIFO",
    )?;
    sqlx::query("UPDATE requests SET lease_time = 1 WHERE namespace = ? AND id = 'recovery-first'")
        .bind(&database.namespace)
        .execute(&database.store.pool)
        .await?;
    let report = database.store.recover(&database.namespace, 100_000).await?;
    require(
        report.pending == 1 && report.failed == 0,
        "recovery did not return the expired unacknowledged Request",
    )?;
    require(
        sequence(database, "recovery-first").await? > sequence(database, "recovery-next").await?,
        "recovered Request did not move behind untouched work",
    )
}

#[tokio::test]
async fn claim_quarantines_version_overflow_and_continues() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_overflow(&database).await;
    database.teardown(result).await
}

async fn exercise_overflow(database: &Database) -> Result<()> {
    init(
        database,
        "overflow-init",
        "overflow-task",
        "overflow-trace",
        &["overflow-request", "valid-request"],
        1,
    )
    .await?;
    sqlx::query("UPDATE requests SET version = ? WHERE namespace = ? AND id = 'overflow-request'")
        .bind(i64::MAX)
        .bind(&database.namespace)
        .execute(&database.store.pool)
        .await?;

    let claimed = claim(database, "overflow-claim", "overflow-worker", 2).await?;
    require(
        claimed.len() == 1 && claimed[0].snapshot.id == "valid-request",
        "version overflow prevented the later valid Request from being claimed",
    )?;
    require(
        state(database, "overflow-request").await? == 3,
        "version overflow was not terminally quarantined",
    )?;
    let completion = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM request_completions \
         WHERE namespace = ? AND request_id = 'overflow-request' AND version = ?",
    )
    .bind(&database.namespace)
    .bind(i64::MAX)
    .fetch_one(&database.store.pool)
    .await?;
    require(
        completion == 1,
        "version overflow did not record a completion",
    )
}

#[tokio::test]
async fn recovery_is_stable_and_bounded() -> Result<()> {
    let Some(database) = Database::connect(2).await? else {
        return Ok(());
    };
    let result = exercise_recovery_limit(&database).await;
    database.teardown(result).await
}

async fn exercise_recovery_limit(database: &Database) -> Result<()> {
    init(
        database,
        "bounded-recovery-init",
        "bounded-task",
        "bounded-trace",
        &["recover-c", "recover-a", "recover-b"],
        2,
    )
    .await?;
    let claimed = claim(database, "bounded-recovery-claim", "bounded-worker", 3).await?;
    require(
        claimed.len() == 3,
        "bounded recovery fixture did not claim all work",
    )?;
    sqlx::query(
        "UPDATE requests SET lease_time = 100 WHERE namespace = ? AND trace_id = 'bounded-trace'",
    )
    .bind(&database.namespace)
    .execute(&database.store.pool)
    .await?;

    let first = database.store.recover(&database.namespace, 100_000).await?;
    require(
        first.pending == 2 && first.failed == 0,
        "first bounded recovery did not process exactly two Requests",
    )?;
    require(
        states(database).await?
            == vec![
                ("recover-a".to_string(), 0),
                ("recover-b".to_string(), 0),
                ("recover-c".to_string(), 1),
            ],
        "bounded recovery did not use stable Request ID ordering",
    )?;

    let second = database.store.recover(&database.namespace, 100_000).await?;
    require(
        second.pending == 1 && second.failed == 0,
        "second bounded recovery did not process the remaining Request",
    )?;
    require(
        states(database).await?.iter().all(|(_, state)| *state == 0),
        "bounded recovery left an expired Request processing",
    )
}

async fn sequence(database: &Database, id: &str) -> Result<u64> {
    Ok(
        sqlx::query_scalar("SELECT sequence FROM requests WHERE namespace = ? AND id = ?")
            .bind(&database.namespace)
            .bind(id)
            .fetch_one(&database.store.pool)
            .await?,
    )
}

async fn state(database: &Database, id: &str) -> Result<i8> {
    Ok(
        sqlx::query_scalar("SELECT state FROM requests WHERE namespace = ? AND id = ?")
            .bind(&database.namespace)
            .bind(id)
            .fetch_one(&database.store.pool)
            .await?,
    )
}

async fn states(database: &Database) -> Result<Vec<(String, i8)>> {
    let rows = sqlx::query(
        "SELECT id, state FROM requests WHERE namespace = ? AND trace_id = 'bounded-trace' \
         ORDER BY id",
    )
    .bind(&database.namespace)
    .fetch_all(&database.store.pool)
    .await?;
    rows.into_iter()
        .map(|row| Ok((row.try_get("id")?, row.try_get("state")?)))
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
        .map_err(Into::into)
}

async fn delete_requests(database: &Database, trace_id: &str) -> Result<()> {
    sqlx::query("DELETE FROM requests WHERE namespace = ? AND trace_id = ?")
        .bind(&database.namespace)
        .bind(trace_id)
        .execute(&database.store.pool)
        .await?;
    Ok(())
}
