use std::time::Duration;

use serde_json::json;
use sqlx::AssertSqlSafe;

use super::{
    Database, Result, claim, completion, identity, init, init_body, require, require_one_conflict,
    snapshot,
};
use crate::types;

const TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn concurrent_init_is_atomic_and_idempotent() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_init(&database).await;
    database.teardown(result).await
}

async fn exercise_init(database: &Database) -> Result<()> {
    let body = init_body(
        "init-task",
        "init-trace",
        vec![snapshot("init-request", "init-task", "init-trace", 1)],
    );
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database.store.init(&database.namespace, "same-init", &body),
            database.store.init(&database.namespace, "same-init", &body),
        )
    })
    .await?;
    left?;
    right?;
    require(
        count(database, "traces", "id = 'init-trace'").await? == 1,
        "concurrent init created more than one Trace",
    )?;
    require(
        count(database, "requests", "id = 'init-request'").await? == 1,
        "concurrent init created more than one Request",
    )?;

    let left_body = init_body("init-task", "changed-left", Vec::new());
    let right_body = init_body("init-task", "changed-right", Vec::new());
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .init(&database.namespace, "changed-init", &left_body),
            database
                .store
                .init(&database.namespace, "changed-init", &right_body),
        )
    })
    .await?;
    require_one_conflict(&left, &right)?;
    require(
        count(
            database,
            "traces",
            "id IN ('changed-left', 'changed-right')",
        )
        .await?
            == 1,
        "different init bodies both mutated storage",
    )?;

    let body = init_body("init-task", "shared-trace", Vec::new());
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .init(&database.namespace, "trace-key-left", &body),
            database
                .store
                .init(&database.namespace, "trace-key-right", &body),
        )
    })
    .await?;
    require_one_conflict(&left, &right)?;
    require(
        count(database, "traces", "id = 'shared-trace'").await? == 1,
        "different operation keys created the same Trace twice",
    )
}

#[tokio::test]
async fn concurrent_items_are_written_once() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_items(&database).await;
    database.teardown(result).await
}

async fn exercise_items(database: &Database) -> Result<()> {
    init(database, "items-init", "items-task", "items-trace", &[], 1).await?;
    let context = types::Identity {
        id: String::new(),
        task_id: "items-task".to_string(),
        trace_id: "items-trace".to_string(),
        version: 0,
        worker_id: String::new(),
        node: String::new(),
    };
    let body = types::Items {
        context: context.clone(),
        items: vec![types::item::Item {
            id: "item-once".to_string(),
            data: json!({"title": "same"}),
        }],
    };
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .items(&database.namespace, "same-items", &body),
            database
                .store
                .items(&database.namespace, "same-items", &body),
        )
    })
    .await?;
    left?;
    right?;
    require(
        count(database, "items", "item_id = 'item-once'").await? == 1,
        "concurrent item replay wrote the Item twice",
    )?;

    let left_body = types::Items {
        context: context.clone(),
        items: vec![types::item::Item {
            id: "item-left".to_string(),
            data: json!({"title": "left"}),
        }],
    };
    let right_body = types::Items {
        context,
        items: vec![types::item::Item {
            id: "item-right".to_string(),
            data: json!({"title": "right"}),
        }],
    };
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .items(&database.namespace, "changed-items", &left_body),
            database
                .store
                .items(&database.namespace, "changed-items", &right_body),
        )
    })
    .await?;
    require_one_conflict(&left, &right)?;
    require(
        count(database, "items", "item_id IN ('item-left', 'item-right')").await? == 1,
        "different Item bodies both mutated storage",
    )
}

#[tokio::test]
async fn concurrent_claim_replays_one_assignment() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_claim(&database).await;
    database.teardown(result).await
}

async fn exercise_claim(database: &Database) -> Result<()> {
    init(
        database,
        "claim-init",
        "claim-task",
        "claim-trace",
        &["claim-a", "claim-b", "claim-c"],
        1,
    )
    .await?;
    let body = types::Claim {
        limit: 1,
        worker_id: "claim-worker".to_string(),
        modes: vec![spider::net::Mode::Http],
    };
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .claim(&database.namespace, "same-claim", &body),
            database
                .store
                .claim(&database.namespace, "same-claim", &body),
        )
    })
    .await?;
    let left = left?;
    let right = right?;
    require(
        left.requests.len() == 1
            && right.requests.len() == 1
            && left.requests[0].snapshot.id == right.requests[0].snapshot.id
            && left.requests[0].execution.version == right.requests[0].execution.version,
        "claim replay returned different assignments",
    )?;
    require(
        count(database, "requests", "state = 1").await? == 1,
        "claim replay leased more than one Request",
    )?;

    let left_body = types::Claim {
        limit: 1,
        worker_id: "claim-left".to_string(),
        modes: vec![spider::net::Mode::Http],
    };
    let right_body = types::Claim {
        limit: 1,
        worker_id: "claim-right".to_string(),
        modes: vec![spider::net::Mode::Http],
    };
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .claim(&database.namespace, "changed-claim", &left_body),
            database
                .store
                .claim(&database.namespace, "changed-claim", &right_body),
        )
    })
    .await?;
    require_one_conflict(&left, &right)?;
    require(
        count(database, "requests", "state = 1").await? == 2,
        "different claim bodies both leased Requests",
    )
}

#[tokio::test]
async fn concurrent_release_replays_one_transition() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_release(&database).await;
    database.teardown(result).await
}

async fn exercise_release(database: &Database) -> Result<()> {
    init(
        database,
        "release-init",
        "release-task",
        "release-trace",
        &["release-a", "release-b", "release-c"],
        1,
    )
    .await?;
    let requests = claim(database, "release-claim", "release-worker", 3).await?;
    require(
        requests.len() == 3,
        "release fixture did not claim three Requests",
    )?;
    let first = identity(&requests[0], "release-worker");
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .release(&database.namespace, "same-release", &first),
            database
                .store
                .release(&database.namespace, "same-release", &first),
        )
    })
    .await?;
    left?;
    right?;
    require(
        count(database, "requests", "state = 0").await? == 1,
        "release replay transitioned more than one Request",
    )?;

    let second = identity(&requests[1], "release-worker");
    let third = identity(&requests[2], "release-worker");
    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database
                .store
                .release(&database.namespace, "changed-release", &second),
            database
                .store
                .release(&database.namespace, "changed-release", &third),
        )
    })
    .await?;
    require_one_conflict(&left, &right)?;
    require(
        count(database, "requests", "state = 0").await? == 2
            && count(database, "requests", "state = 1").await? == 1,
        "different release bodies both transitioned Requests",
    )
}

#[tokio::test]
async fn completion_replay_survives_request_cleanup() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_completion_replay(&database).await;
    database.teardown(result).await
}

async fn exercise_completion_replay(database: &Database) -> Result<()> {
    init(
        database,
        "completion-init",
        "completion-task",
        "completion-trace",
        &["completion-request"],
        1,
    )
    .await?;
    let request = claim(database, "completion-claim", "completion-worker", 1)
        .await?
        .pop()
        .ok_or_else(|| std::io::Error::other("completion fixture did not claim a Request"))?;
    let identity = identity(&request, "completion-worker");
    database.store.ack(&database.namespace, &identity).await?;
    let body = completion(identity, Default::default(), None);
    database.store.success(&database.namespace, &body).await?;

    sqlx::query("DELETE FROM requests WHERE namespace = ? AND id = ?")
        .bind(&database.namespace)
        .bind(&body.identity.id)
        .execute(&database.store.pool)
        .await?;

    database.store.success(&database.namespace, &body).await?;
    Ok(())
}

#[tokio::test]
async fn concurrent_completion_replay_settles_once() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_concurrent_completion_replay(&database).await;
    database.teardown(result).await
}

async fn exercise_concurrent_completion_replay(database: &Database) -> Result<()> {
    init(
        database,
        "concurrent-completion-init",
        "concurrent-completion-task",
        "concurrent-completion-trace",
        &["concurrent-completion-request"],
        1,
    )
    .await?;
    let request = claim(
        database,
        "concurrent-completion-claim",
        "concurrent-completion-worker",
        1,
    )
    .await?
    .pop()
    .ok_or_else(|| std::io::Error::other("completion fixture did not claim a Request"))?;
    let identity = identity(&request, "concurrent-completion-worker");
    database.store.ack(&database.namespace, &identity).await?;
    let body = completion(identity, Default::default(), None);

    let (left, right) = tokio::time::timeout(TIMEOUT, async {
        tokio::join!(
            database.store.success(&database.namespace, &body),
            database.store.success(&database.namespace, &body),
        )
    })
    .await?;
    left?;
    right?;
    require(
        count(
            database,
            "request_completions",
            "request_id = 'concurrent-completion-request'",
        )
        .await?
            == 1,
        "concurrent completion replay created more than one completion",
    )?;
    require(
        count(
            database,
            "requests",
            "id = 'concurrent-completion-request' AND state = 2",
        )
        .await?
            == 1,
        "concurrent completion replay did not leave the Request completed",
    )
}

async fn count(database: &Database, table: &'static str, predicate: &'static str) -> Result<i64> {
    let query = format!("SELECT COUNT(*) FROM {table} WHERE namespace = ? AND {predicate}");
    Ok(sqlx::query_scalar(AssertSqlSafe(query))
        .bind(&database.namespace)
        .fetch_one(&database.store.pool)
        .await?)
}
