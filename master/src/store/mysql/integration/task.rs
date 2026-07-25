use std::collections::HashMap;

use serde_json::{Value, json};
use sqlx::Row;
use sqlx::types::Json;

use super::super::{CodeSeed, Task};
use super::{Database, Result, require, require_one_conflict};

#[tokio::test]
async fn task_name_conflict_preserves_the_existing_owner() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
}

async fn exercise(database: &Database) -> Result<()> {
    let original = task("task-a", "shared-name", "original");
    database
        .store
        .upsert_task(&database.namespace, &original)
        .await?;

    let conflict = task("task-b", "shared-name", "replacement");
    require(
        matches!(
            database
                .store
                .upsert_task(&database.namespace, &conflict)
                .await,
            Err(crate::Error::Conflict(_))
        ),
        "Task name conflict did not return Conflict",
    )?;
    assert_owner(database, "shared-name", "task-a", "original").await?;

    let left = task("task-c", "concurrent-name", "left");
    let right = task("task-d", "concurrent-name", "right");
    let (left_result, right_result) =
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(
                database.store.upsert_task(&database.namespace, &left),
                database.store.upsert_task(&database.namespace, &right),
            )
        })
        .await?;
    require_one_conflict(&left_result, &right_result)?;
    let (id, marker) = if left_result.is_ok() {
        ("task-c", "left")
    } else {
        ("task-d", "right")
    };
    assert_owner(database, "concurrent-name", id, marker).await
}

async fn assert_owner(
    database: &Database,
    name: &str,
    expected_id: &str,
    expected_marker: &str,
) -> Result<()> {
    let rows = sqlx::query("SELECT id, params FROM tasks WHERE namespace = ? AND name = ?")
        .bind(&database.namespace)
        .bind(name)
        .fetch_all(&database.store.pool)
        .await?;
    require(rows.len() == 1, "Task name does not have exactly one owner")?;
    let id: String = rows[0].try_get("id")?;
    let params: Json<Value> = rows[0].try_get("params")?;
    require(id == expected_id, "Task name owner changed unexpectedly")?;
    require(
        params.0.get("marker").and_then(Value::as_str) == Some(expected_marker),
        "Task conflict overwrote the existing configuration",
    )
}

fn task(id: &str, name: &str, marker: &str) -> Task {
    Task {
        id: id.to_string(),
        name: name.to_string(),
        periodic: false,
        interval_ms: 0,
        priority: 0,
        params: HashMap::from([("marker".to_string(), json!(marker))]),
        dsl: None,
        seeds: vec![CodeSeed {
            node: "index".to_string(),
            url: format!("https://example.com/{marker}"),
            method: Default::default(),
            headers: Default::default(),
            body: Default::default(),
            cookies: Default::default(),
            vals: HashMap::new(),
            kwargs: HashMap::new(),
            priority: 0,
            dont_filter: false,
            mode: Default::default(),
            timeout: None,
            max_body_bytes: None,
            proxy: None,
            tls: None,
            middlewares: Vec::new(),
            max_retry_count: 1,
        }],
        persister_id: None,
        attachment: None,
        next_time: 0,
    }
}
