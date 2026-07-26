use super::{Database, Result, init_body, require, snapshot};
use crate::{Error, types};

#[tokio::test]
async fn trace_reads_reject_a_mismatched_task_projection() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_trace_read(&database).await;
    database.teardown(result).await
}

async fn exercise_trace_read(database: &Database) -> Result<()> {
    database
        .store
        .init(
            &database.namespace,
            "trace-projection-read-init",
            &init_body("trace-task", "trace-projection-read", Vec::new()),
        )
        .await?;
    corrupt_task_projection(database, "trace-projection-read").await?;

    require(
        matches!(
            database
                .store
                .trace(&database.namespace, "trace-projection-read")
                .await,
            Err(Error::InvalidTrace { id, message })
                if id == "trace-projection-read" && message.contains("stored task_id")
        ),
        "Trace query accepted a task_id projection that disagreed with its Snapshot",
    )
}

#[tokio::test]
async fn request_push_rejects_a_mismatched_trace_task_projection() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_request_push(&database).await;
    database.teardown(result).await
}

async fn exercise_request_push(database: &Database) -> Result<()> {
    database
        .store
        .init(
            &database.namespace,
            "trace-projection-push-init",
            &init_body("trace-task", "trace-projection-push", Vec::new()),
        )
        .await?;
    corrupt_task_projection(database, "trace-projection-push").await?;

    let body = types::Push {
        context: types::Identity {
            id: String::new(),
            task_id: String::new(),
            trace_id: String::new(),
            version: 0,
            worker_id: String::new(),
            node: String::new(),
        },
        requests: vec![snapshot(
            "trace-projection-request",
            "trace-task",
            "trace-projection-push",
            1,
        )],
    };
    require(
        matches!(
            database.store.push(&database.namespace, &body).await,
            Err(Error::InvalidTrace { id, message })
                if id == "trace-projection-push" && message.contains("stored task_id")
        ),
        "Request push accepted a Trace whose task_id projection was corrupt",
    )?;
    let stored = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM requests WHERE namespace = ? AND id = ?",
    )
    .bind(&database.namespace)
    .bind("trace-projection-request")
    .fetch_one(&database.store.pool)
    .await?;
    require(stored == 0, "rejected Request push mutated storage")
}

#[tokio::test]
async fn request_conflict_precedes_a_missing_trace() -> Result<()> {
    let Some(database) = Database::connect(128).await? else {
        return Ok(());
    };
    let result = exercise_conflict_priority(&database).await;
    database.teardown(result).await
}

async fn exercise_conflict_priority(database: &Database) -> Result<()> {
    let trace_id = "conflict-priority-trace";
    let existing = snapshot(
        "z-conflicting-request",
        "conflict-priority-task",
        trace_id,
        1,
    );
    database
        .store
        .init(
            &database.namespace,
            "conflict-priority-init",
            &init_body("conflict-priority-task", trace_id, vec![existing.clone()]),
        )
        .await?;
    sqlx::query("DELETE FROM traces WHERE namespace = ? AND id = ?")
        .bind(&database.namespace)
        .bind(trace_id)
        .execute(&database.store.pool)
        .await?;

    let missing = snapshot(
        "a-missing-trace-request",
        "conflict-priority-task",
        trace_id,
        1,
    );
    let mut conflict = existing;
    conflict.priority = 1;
    let body = types::Push {
        context: types::Identity {
            id: String::new(),
            task_id: String::new(),
            trace_id: String::new(),
            version: 0,
            worker_id: String::new(),
            node: String::new(),
        },
        requests: vec![missing, conflict],
    };
    require(
        matches!(
            database.store.push(&database.namespace, &body).await,
            Err(Error::Conflict(message)) if message.contains("z-conflicting-request")
        ),
        "missing Trace was reported before the batch Snapshot conflict",
    )?;
    let stored = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM requests WHERE namespace = ? AND id = ?",
    )
    .bind(&database.namespace)
    .bind("a-missing-trace-request")
    .fetch_one(&database.store.pool)
    .await?;
    require(
        stored == 0,
        "conflicting Request push partially mutated storage",
    )
}

async fn corrupt_task_projection(database: &Database, trace_id: &str) -> Result<()> {
    sqlx::query("UPDATE traces SET task_id = ? WHERE namespace = ? AND id = ?")
        .bind("corrupt-task")
        .bind(&database.namespace)
        .bind(trace_id)
        .execute(&database.store.pool)
        .await?;
    Ok(())
}
