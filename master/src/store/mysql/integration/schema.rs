use std::collections::BTreeMap;

use sqlx::Row;

use super::{Database, Result, clear, init_body, require, snapshot};
use crate::types;

#[tokio::test]
async fn control_queries_have_keyset_indexes() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise(&database).await;
    database.teardown(result).await
}

async fn exercise(database: &Database) -> Result<()> {
    let rows = sqlx::query(
        "SELECT table_name, index_name, column_name, seq_in_index \
         FROM information_schema.statistics \
         WHERE table_schema = DATABASE() \
         AND table_name IN ('tasks', 'traces', 'requests', 'items') \
         ORDER BY table_name, index_name, seq_in_index",
    )
    .fetch_all(&database.store.pool)
    .await?;
    let mut indexes = BTreeMap::<String, Vec<(u64, String)>>::new();
    for row in rows {
        let table: String = row.try_get(0)?;
        let index: String = row.try_get(1)?;
        let column: String = row.try_get(2)?;
        let position: u64 = row.try_get(3)?;
        indexes
            .entry(format!("{table}.{index}"))
            .or_default()
            .push((position, column));
    }

    for (name, expected) in [
        ("tasks.tasks_history", "namespace,updated_time,id"),
        ("traces.traces_history", "namespace,created_time,id"),
        ("requests.requests_history", "namespace,created_time,id"),
        (
            "requests.requests_trace_history",
            "namespace,trace_id,created_time,id",
        ),
        (
            "requests.requests_state_history",
            "namespace,state,created_time,id",
        ),
        (
            "requests.requests_worker_history",
            "namespace,leased_by,created_time,id",
        ),
        ("items.items_history", "namespace,created_time,id"),
    ] {
        let actual = indexes.get(name).map(|columns| {
            columns
                .iter()
                .map(|(_, column)| column.as_str())
                .collect::<Vec<_>>()
                .join(",")
        });
        require(
            actual.as_deref() == Some(expected),
            format!("missing or invalid control index {name}: {actual:?}"),
        )?;
    }
    assert_binary_keys(database).await?;
    Ok(())
}

async fn assert_binary_keys(database: &Database) -> Result<()> {
    let rows = sqlx::query(
        "SELECT table_name, column_name, collation_name, data_type \
         FROM information_schema.columns WHERE table_schema = DATABASE()",
    )
    .fetch_all(&database.store.pool)
    .await?;
    let mut columns = BTreeMap::<String, (Option<String>, String)>::new();
    for row in rows {
        columns.insert(
            format!(
                "{}.{}",
                row.try_get::<String, _>(0)?,
                row.try_get::<String, _>(1)?
            ),
            (row.try_get(2)?, row.try_get(3)?),
        );
    }
    for name in [
        "tasks.namespace",
        "tasks.id",
        "tasks.name",
        "tasks.persister_id",
        "traces.namespace",
        "traces.id",
        "traces.task_id",
        "requests.namespace",
        "requests.id",
        "requests.task_id",
        "requests.trace_id",
        "requests.node",
        "requests.mode",
        "requests.snapshot_digest",
        "requests.leased_by",
        "request_completions.namespace",
        "request_completions.request_id",
        "request_completions.task_id",
        "request_completions.trace_id",
        "request_completions.node",
        "request_completions.worker_id",
        "request_completions.payload_digest",
        "operations.namespace",
        "operations.kind",
        "operations.operation_key",
        "operations.request_digest",
        "workers.namespace",
        "workers.id",
        "items.namespace",
        "items.id",
        "items.item_id",
        "items.task_id",
        "items.trace_id",
        "items.request_id",
        "trace_stats.namespace",
        "trace_stats.trace_id",
        "trace_stats.name",
        "queue_sequences.namespace",
    ] {
        let actual = columns
            .get(name)
            .and_then(|(collation, _)| collation.as_deref());
        require(
            actual == Some("utf8mb4_0900_bin"),
            format!("identity column {name} has non-binary collation {actual:?}"),
        )?;
    }
    for name in [
        "items.persister_id",
        "items.config_version",
        "items.timezone",
    ] {
        require(
            !columns.contains_key(name),
            format!("removed Item metadata column still exists: {name}"),
        )?;
    }
    let error_type = columns
        .get("request_completions.error")
        .map(|(_, data_type)| data_type.as_str());
    require(
        error_type == Some("longtext"),
        format!("completion error column is not LONGTEXT: {error_type:?}"),
    )?;
    Ok(())
}

#[tokio::test]
async fn namespaces_are_case_sensitive_storage_boundaries() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let suffix = uuid::Uuid::now_v7().simple();
    let upper = format!("Case-{suffix}");
    let lower = upper.to_ascii_lowercase();
    let result = exercise_namespaces(&database, &upper, &lower).await;
    let cleanup = async {
        clear(&database.store, &upper).await?;
        clear(&database.store, &lower).await?;
        Ok(())
    }
    .await;
    database.teardown(result.and(cleanup)).await
}

#[tokio::test]
async fn failure_completion_keeps_a_long_error() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_long_error(&database).await;
    database.teardown(result).await
}

async fn exercise_long_error(database: &Database) -> Result<()> {
    let request = snapshot("error-request", "error-task", "error-trace", 1);
    database
        .store
        .init(
            &database.namespace,
            "error-init",
            &init_body("error-task", "error-trace", vec![request]),
        )
        .await?;
    let claimed = database
        .store
        .claim(
            &database.namespace,
            "error-claim",
            &types::Claim {
                limit: 1,
                worker_id: "error-worker".to_string(),
                modes: vec![spider::net::Mode::Http],
            },
        )
        .await?
        .requests
        .pop()
        .ok_or_else(|| std::io::Error::other("long error fixture did not claim a Request"))?;
    let identity = types::Identity {
        id: claimed.snapshot.id,
        task_id: claimed.snapshot.task_id,
        trace_id: claimed.snapshot.trace_id,
        version: claimed.execution.version,
        worker_id: "error-worker".to_string(),
        node: claimed.snapshot.node,
    };
    database.store.ack(&database.namespace, &identity).await?;
    let error = "x".repeat(70_000);
    database
        .store
        .failure(
            &database.namespace,
            &types::Completion {
                identity: identity.clone(),
                stats: Default::default(),
                start_time: 1,
                end_time: 2,
                error: Some(error),
            },
        )
        .await?;
    let bytes = sqlx::query_scalar::<_, Option<i64>>(
        "SELECT OCTET_LENGTH(error) FROM request_completions \
         WHERE namespace = ? AND request_id = ? AND version = ?",
    )
    .bind(&database.namespace)
    .bind(&identity.id)
    .bind(identity.version)
    .fetch_one(&database.store.pool)
    .await?;
    require(
        bytes == Some(70_000),
        format!("LONGTEXT completion error was truncated: {bytes:?}"),
    )
}

async fn exercise_namespaces(database: &Database, upper: &str, lower: &str) -> Result<()> {
    require(upper != lower, "namespace fixture must differ by case")?;
    let request = snapshot("case-request", "case-task", "case-trace", 1);
    let body = init_body("case-task", "case-trace", vec![request]);
    database.store.init(upper, "case-init", &body).await?;
    database.store.init(lower, "case-init", &body).await?;
    let rows = sqlx::query_scalar::<_, String>(
        "SELECT namespace FROM traces WHERE namespace IN (?, ?) AND id = ? ORDER BY namespace",
    )
    .bind(upper)
    .bind(lower)
    .bind("case-trace")
    .fetch_all(&database.store.pool)
    .await?;
    require(
        rows.len() == 2
            && rows.iter().any(|value| value == upper)
            && rows.iter().any(|value| value == lower),
        format!("case-distinct namespaces were not isolated: {rows:?}"),
    )
}
