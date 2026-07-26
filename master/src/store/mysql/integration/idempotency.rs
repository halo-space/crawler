use std::time::Duration;

use serde_json::json;
use sqlx::AssertSqlSafe;

use super::{
    Database, Result, claim, completion, identity, init, init_body, require, require_one_conflict,
    snapshot,
};
use crate::{Error, types};

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
    let context = types::Identity {
        id: "items-request".to_string(),
        task_id: "items-task".to_string(),
        trace_id: "items-trace".to_string(),
        version: 1,
        worker_id: "items-worker".to_string(),
        node: "detail".to_string(),
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
async fn invalid_item_id_rejects_the_entire_submission() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_invalid_item_id(&database).await;
    database.teardown(result).await
}

async fn exercise_invalid_item_id(database: &Database) -> Result<()> {
    let body = types::Items {
        context: types::Identity {
            id: String::new(),
            task_id: "invalid-items-task".to_string(),
            trace_id: "invalid-items-trace".to_string(),
            version: 0,
            worker_id: String::new(),
            node: String::new(),
        },
        items: vec![
            types::item::Item {
                id: "valid-item".to_string(),
                data: json!({"title": "valid"}),
            },
            types::item::Item {
                id: String::new(),
                data: json!({"title": "invalid"}),
            },
        ],
    };

    require(
        matches!(
            database
                .store
                .items(&database.namespace, "invalid-items", &body)
                .await,
            Err(crate::Error::Invalid(message))
                if message == "every Item requires a non-empty framework Item ID"
        ),
        "empty framework Item ID did not return the store contract error",
    )?;
    require(
        count(database, "items", "1 = 1").await? == 0,
        "invalid Item submission partially mutated storage",
    )?;
    require(
        count(
            database,
            "operations",
            "kind = 'items' AND operation_key = 'invalid-items'",
        )
        .await?
            == 0,
        "invalid Item submission reserved an idempotency operation",
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
async fn claim_replay_respects_the_current_response_limit() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_claim_replay_limit(&database).await;
    database.teardown(result).await
}

async fn exercise_claim_replay_limit(database: &Database) -> Result<()> {
    let mut request = snapshot(
        "claim-replay-limit-request",
        "claim-replay-limit-task",
        "claim-replay-limit-trace",
        1,
    );
    request.vals.insert(
        "content".to_string(),
        serde_json::Value::String("x".repeat(2048)),
    );
    database
        .store
        .init(
            &database.namespace,
            "claim-replay-limit-init",
            &init_body(
                "claim-replay-limit-task",
                "claim-replay-limit-trace",
                vec![request],
            ),
        )
        .await?;
    let body = types::Claim {
        limit: 1,
        worker_id: "claim-replay-limit-worker".to_string(),
        modes: vec![spider::net::Mode::Http],
    };
    let first = database
        .store
        .claim(&database.namespace, "claim-replay-limit", &body)
        .await?;
    let bytes = serde_json::to_vec(&first)?.len();
    require(
        bytes > 1024,
        "claim replay fixture did not exceed the minimum API limit",
    )?;

    let mut restricted = database.store.clone();
    restricted.max_response_bytes = bytes - 1;
    let replay = restricted
        .claim(&database.namespace, "claim-replay-limit", &body)
        .await;
    require(
        matches!(
            replay,
            Err(Error::ResponseTooLarge { max }) if max == bytes - 1
        ),
        "claim replay bypassed the current API response limit",
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
async fn request_push_replay_survives_parent_completion() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_request_push_replay(&database).await;
    database.teardown(result).await
}

async fn exercise_request_push_replay(database: &Database) -> Result<()> {
    init(
        database,
        "push-replay-init",
        "push-replay-task",
        "push-replay-trace",
        &["push-replay-parent"],
        1,
    )
    .await?;
    let parent = claim(database, "push-replay-claim", "push-replay-worker", 1)
        .await?
        .pop()
        .ok_or_else(|| std::io::Error::other("push replay fixture did not claim its parent"))?;
    let identity = identity(&parent, "push-replay-worker");
    let child = snapshot(
        "push-replay-child",
        "push-replay-task",
        "push-replay-trace",
        1,
    );
    let body = types::Push {
        context: identity.clone(),
        requests: vec![child.clone()],
    };

    require(
        matches!(
            database.store.push(&database.namespace, &body).await,
            Err(Error::NotAcknowledged(id)) if id == identity.id
        ),
        "an unacknowledged parent published a child Request",
    )?;
    require(
        count(database, "requests", "id = 'push-replay-child'").await? == 0,
        "failed child publication partially mutated storage",
    )?;

    database.store.ack(&database.namespace, &identity).await?;
    database.store.push(&database.namespace, &body).await?;
    database
        .store
        .success(
            &database.namespace,
            &completion(identity, Default::default(), None),
        )
        .await?;

    database.store.push(&database.namespace, &body).await?;
    require(
        count(database, "requests", "id = 'push-replay-child'").await? == 1,
        "push replay duplicated the child Request",
    )?;

    let mixed = types::Push {
        context: body.context.clone(),
        requests: vec![
            child,
            snapshot(
                "push-replay-new-child",
                "push-replay-task",
                "push-replay-trace",
                1,
            ),
        ],
    };
    require(
        matches!(
            database.store.push(&database.namespace, &mixed).await,
            Err(Error::Lease(id)) if id == body.context.id
        ),
        "a completed parent did not return the expected lease error for a new child Request",
    )?;
    require(
        count(database, "requests", "id = 'push-replay-new-child'").await? == 0,
        "mixed replay partially inserted its new child Request",
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
