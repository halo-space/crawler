use std::collections::HashMap;
use std::time::Duration;

use serde_json::{Value, to_value};
use sqlx::Row;

use super::{Database, Result, claim, completion, identity, init, require};

#[tokio::test]
async fn concurrent_completions_lock_stats_in_one_order() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
}

async fn exercise(database: &Database) -> Result<()> {
    init(
        database,
        "stats-init",
        "stats-task",
        "stats-trace",
        &["stats-left", "stats-right"],
        1,
    )
    .await?;
    let claimed = claim(database, "stats-claim", "stats-worker", 2).await?;
    require(
        claimed.len() == 2,
        "stats fixture did not claim two Requests",
    )?;
    let left_identity = identity(&claimed[0], "stats-worker");
    let right_identity = identity(&claimed[1], "stats-worker");
    database
        .store
        .ack(&database.namespace, &left_identity)
        .await?;
    database
        .store
        .ack(&database.namespace, &right_identity)
        .await?;

    let (left_stats, right_stats) = opposite_stats()?;
    let left = completion(left_identity, left_stats, None);
    let right = completion(right_identity, right_stats, None);
    let (left_result, right_result) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            database.store.success(&database.namespace, &left),
            database.store.success(&database.namespace, &right),
        )
    })
    .await?;
    left_result?;
    right_result?;

    let rows = sqlx::query(
        "SELECT name, total, done FROM trace_stats \
         WHERE namespace = ? AND trace_id = 'stats-trace' ORDER BY name",
    )
    .bind(&database.namespace)
    .fetch_all(&database.store.pool)
    .await?;
    require(rows.len() == 2, "stats merge lost a counter row")?;
    for row in rows {
        let name: String = row.try_get("name")?;
        let total: i64 = row.try_get("total")?;
        let done: i64 = row.try_get("done")?;
        require(
            total == 2 && done == 2,
            format!("stats counter {name} did not merge both completions"),
        )?;
    }
    Ok(())
}

fn opposite_stats() -> Result<(HashMap<String, Value>, HashMap<String, Value>)> {
    let left = counters()?;
    let mut reversed = left.keys().cloned().collect::<Vec<_>>();
    reversed.reverse();
    for _ in 0..1_024 {
        let right = counters()?;
        if right.keys().eq(reversed.iter()) {
            return Ok((left, right));
        }
    }
    Err(std::io::Error::other("could not construct opposite HashMap iteration order").into())
}

fn counters() -> Result<HashMap<String, Value>> {
    let counter = spider::stats::Counter {
        total: 1,
        done: 1,
        ..Default::default()
    };
    Ok(HashMap::from([
        ("alpha".to_string(), to_value(&counter)?),
        ("beta".to_string(), to_value(&counter)?),
    ]))
}
