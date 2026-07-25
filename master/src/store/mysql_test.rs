use std::collections::HashMap;
use std::error::Error as StdError;
use std::time::Duration;

use super::MySql;
use crate::types::task::{CodeSeed, Task};
use crate::{Config, types};
use sqlx::{MySql as SqlxMySql, Pool, Row};

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn dispatch_persists_a_trace_and_its_seed_requests() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let namespace = format!("master-test-{}", uuid::Uuid::now_v7().simple());
    let task_id = format!("task-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "integration-test-worker-token",
        "integration-test-control-token",
    )?;
    let store = MySql::connect(&config).await?;

    clear(store.pool(), &namespace).await?;
    let result = exercise(&store, &namespace, &task_id).await;
    let cleanup = clear(store.pool(), &namespace).await;
    store.pool().close().await;

    result?;
    cleanup?;
    Ok(())
}

fn database_url() -> Option<String> {
    std::env::var("CRAWLER_MYSQL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

#[tokio::test]
async fn cleanup_removes_only_expired_history() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let namespace = format!("master-test-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "integration-test-worker-token",
        "integration-test-control-token",
    )?;
    let store = MySql::connect(&config).await?;

    clear(store.pool(), &namespace).await?;
    let result = exercise_cleanup(&store, &namespace).await;
    let cleanup = clear(store.pool(), &namespace).await;
    store.pool().close().await;

    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn claim_quarantines_invalid_data_and_preserves_fifo() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let namespace = format!("master-test-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "integration-test-worker-token",
        "integration-test-control-token",
    )?;
    let store = MySql::connect(&config).await?;

    clear(store.pool(), &namespace).await?;
    let result = exercise_claim(&store, &namespace).await;
    let cleanup = clear(store.pool(), &namespace).await;
    store.pool().close().await;

    result?;
    cleanup?;
    Ok(())
}

#[tokio::test]
async fn dispatch_quarantines_invalid_task_and_continues() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let namespace = format!("master-test-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "integration-test-worker-token",
        "integration-test-control-token",
    )?;
    let store = MySql::connect(&config).await?;

    clear(store.pool(), &namespace).await?;
    let result = exercise_invalid_task_dispatch(&store, &namespace).await;
    let cleanup = clear(store.pool(), &namespace).await;
    store.pool().close().await;

    result?;
    cleanup?;
    Ok(())
}

async fn exercise(store: &MySql, namespace: &str, task_id: &str) -> Result<()> {
    let task = Task {
        id: task_id.to_string(),
        name: format!("integration-{task_id}"),
        periodic: true,
        interval_ms: 1,
        priority: 7,
        params: HashMap::new(),
        dsl: None,
        seeds: vec![seed()],
        persister_id: None,
        attachment: None,
        next_time: 0,
    };
    store.upsert_task(namespace, &task).await?;
    let stored_name =
        sqlx::query_scalar::<_, String>("SELECT name FROM tasks WHERE namespace = ? AND id = ?")
            .bind(namespace)
            .bind(task_id)
            .fetch_one(store.pool())
            .await?;
    require(
        stored_name == task.name,
        "Task upsert did not persist the Task",
    )?;

    let now = now_millis();
    let first = store.dispatch_due(namespace, now, 1).await?;
    let second = store.dispatch_due(namespace, now + 1, 1).await?;

    require(
        first.len() == 1,
        "first dispatch did not create exactly one Trace",
    )?;
    require(
        second.len() == 1,
        "second dispatch did not create exactly one Trace",
    )?;
    require(first[0] != second[0], "periodic dispatch reused a Trace ID")?;

    for trace_id in first.iter().chain(&second) {
        let trace_task_id = sqlx::query_scalar::<_, String>(
            "SELECT task_id FROM traces WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(trace_id)
        .fetch_one(store.pool())
        .await?;
        require(
            trace_task_id == task_id,
            "persisted Trace belongs to the wrong Task",
        )?;

        let request = sqlx::query(
            "SELECT task_id, trace_id, node, mode, state FROM requests WHERE namespace = ? AND trace_id = ?",
        )
        .bind(namespace)
        .bind(trace_id)
        .fetch_one(store.pool())
        .await?;
        let request_task_id: String = request.try_get("task_id")?;
        let request_trace_id: String = request.try_get("trace_id")?;
        let node: String = request.try_get("node")?;
        let mode: String = request.try_get("mode")?;
        let state: i8 = request.try_get("state")?;

        require(
            request_task_id == task_id,
            "seed Request belongs to the wrong Task",
        )?;
        require(
            request_trace_id == *trace_id,
            "seed Request belongs to the wrong Trace",
        )?;
        require(node == "index", "seed Request node was not preserved")?;
        require(mode == "http", "seed Request mode was not preserved")?;
        require(state == 0, "seed Request is not pending")?;
    }

    let trace_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM traces WHERE namespace = ? AND task_id = ?",
    )
    .bind(namespace)
    .bind(task_id)
    .fetch_one(store.pool())
    .await?;
    let request_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM requests WHERE namespace = ? AND task_id = ?",
    )
    .bind(namespace)
    .bind(task_id)
    .fetch_one(store.pool())
    .await?;
    require(
        trace_count == 2,
        "unexpected Trace count after periodic dispatch",
    )?;
    require(
        request_count == 2,
        "unexpected seed Request count after periodic dispatch",
    )?;
    Ok(())
}

async fn exercise_cleanup(store: &MySql, namespace: &str) -> Result<()> {
    const NOW: i64 = 10_000;
    const RETENTION: Duration = Duration::from_millis(1_000);
    const EXPIRED: i64 = NOW - 1_001;
    const BOUNDARY: i64 = NOW - 1_000;

    for (id, state, updated_time, sequence) in [
        ("expired-request", 2, EXPIRED, 1_u64),
        ("boundary-request", 2, BOUNDARY, 2),
        ("pending-request", 0, EXPIRED, 3),
    ] {
        insert_request(store.pool(), namespace, id, state, updated_time, sequence).await?;
    }
    for (request_id, created_time) in [
        ("expired-completion", EXPIRED),
        ("boundary-completion", BOUNDARY),
    ] {
        insert_completion(store.pool(), namespace, request_id, created_time).await?;
    }
    for (key, updated_time) in [
        ("expired-operation", EXPIRED),
        ("boundary-operation", BOUNDARY),
    ] {
        insert_operation(store.pool(), namespace, key, updated_time).await?;
    }
    let report = store.cleanup(namespace, NOW, RETENTION, 16).await?;
    require(
        report.requests == 1,
        "cleanup did not remove one terminal Request",
    )?;
    require(
        report.completions == 1,
        "cleanup did not remove one Request completion",
    )?;
    require(
        report.operations == 1,
        "cleanup did not remove one idempotency operation",
    )?;

    require(
        count(
            store.pool(),
            "SELECT COUNT(*) FROM requests WHERE namespace = ? AND id = 'expired-request'",
            namespace,
        )
        .await?
            == 0,
        "cleanup retained an expired terminal Request",
    )?;
    require(
        count(
            store.pool(),
            "SELECT COUNT(*) FROM requests WHERE namespace = ? AND id = 'boundary-request'",
            namespace,
        )
        .await?
            == 1,
        "cleanup removed a Request at the strict retention boundary",
    )?;
    require(
        count(
            store.pool(),
            "SELECT COUNT(*) FROM requests WHERE namespace = ? AND id = 'pending-request'",
            namespace,
        )
        .await?
            == 1,
        "cleanup removed an expired pending Request",
    )?;
    require(
        count(
            store.pool(),
            "SELECT COUNT(*) FROM request_completions WHERE namespace = ? AND request_id = 'boundary-completion'",
            namespace,
        )
        .await?
            == 1,
        "cleanup removed a completion at the strict retention boundary",
    )?;
    require(
        count(
            store.pool(),
            "SELECT COUNT(*) FROM operations WHERE namespace = ? AND operation_key = 'boundary-operation'",
            namespace,
        )
        .await?
            == 1,
        "cleanup removed an operation at the strict retention boundary",
    )?;
    Ok(())
}

async fn exercise_claim(store: &MySql, namespace: &str) -> Result<()> {
    let task_id = "claim-task";
    let trace_id = "claim-trace";
    let snapshots = ["z-first", "a-broken", "m-second"]
        .into_iter()
        .map(|id| {
            let mut request = spider::net::Request::follow(format!("https://example.com/{id}"))
                .unwrap()
                .node("index");
            request.id = id.to_string();
            request.task_id = task_id.to_string();
            request.trace_id = trace_id.to_string();
            spider::net::request::Snapshot::try_from(request).unwrap()
        })
        .collect::<Vec<_>>();
    store
        .init(
            namespace,
            "claim-init",
            &types::Init {
                trace_id: trace_id.to_string(),
                trace: spider::trace::Snapshot::code(task_id),
                requests: snapshots,
            },
        )
        .await?;
    sqlx::query(
        "UPDATE requests SET snapshot = JSON_OBJECT('broken', true) \
         WHERE namespace = ? AND id = 'a-broken'",
    )
    .bind(namespace)
    .execute(store.pool())
    .await?;

    let claims = store
        .claim(
            namespace,
            "claim-operation",
            &types::Claim {
                limit: 3,
                worker_id: "worker-1".to_string(),
                modes: vec![spider::net::Mode::Http],
            },
        )
        .await?;
    let ids = claims
        .requests
        .iter()
        .map(|request| request.snapshot.id.as_str())
        .collect::<Vec<_>>();
    require(
        ids == ["z-first", "m-second"],
        "claim did not preserve FIFO around a damaged Request",
    )?;

    let state = sqlx::query_scalar::<_, i8>(
        "SELECT state FROM requests WHERE namespace = ? AND id = 'a-broken'",
    )
    .bind(namespace)
    .fetch_one(store.pool())
    .await?;
    require(state == 3, "damaged Request did not enter terminal failure")?;
    let completion = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM request_completions \
         WHERE namespace = ? AND request_id = 'a-broken' AND version = 0",
    )
    .bind(namespace)
    .fetch_one(store.pool())
    .await?;
    require(
        completion == 1,
        "damaged Request did not record a terminal completion",
    )?;
    Ok(())
}

async fn exercise_invalid_task_dispatch(store: &MySql, namespace: &str) -> Result<()> {
    let invalid = task("invalid-dispatch-task", 100);
    let healthy = task("healthy-dispatch-task", 1);
    store.upsert_task(namespace, &invalid).await?;
    store.upsert_task(namespace, &healthy).await?;
    sqlx::query("UPDATE tasks SET seed_specs = NULL WHERE namespace = ? AND id = ?")
        .bind(namespace)
        .bind(&invalid.id)
        .execute(store.pool())
        .await?;

    let traces = store.dispatch_due(namespace, now_millis(), 2).await?;
    require(
        traces.len() == 1,
        "an invalid Task prevented a healthy Task from dispatching",
    )?;
    let dispatched_task = sqlx::query_scalar::<_, String>(
        "SELECT task_id FROM traces WHERE namespace = ? AND id = ?",
    )
    .bind(namespace)
    .bind(&traces[0])
    .fetch_one(store.pool())
    .await?;
    require(
        dispatched_task == healthy.id,
        "dispatch did not continue with the healthy Task",
    )?;

    let row = sqlx::query("SELECT state, error FROM tasks WHERE namespace = ? AND id = ?")
        .bind(namespace)
        .bind(&invalid.id)
        .fetch_one(store.pool())
        .await?;
    require(
        row.try_get::<i8, _>("state")? == 4,
        "invalid Task was not quarantined",
    )?;
    let error: Option<String> = row.try_get("error")?;
    require(
        error.is_some_and(|message| message.contains("seed_specs must not be null")),
        "invalid Task did not record its deterministic dispatch error",
    )?;

    let repeated = store.dispatch_due(namespace, now_millis(), 1).await?;
    require(
        repeated.is_empty(),
        "quarantined Task was selected for dispatch again",
    )?;
    let invalid_traces = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM traces WHERE namespace = ? AND task_id = ?",
    )
    .bind(namespace)
    .bind(&invalid.id)
    .fetch_one(store.pool())
    .await?;
    require(
        invalid_traces == 0,
        "invalid Task left a partial Trace behind",
    )?;

    Ok(())
}

async fn insert_request(
    pool: &Pool<SqlxMySql>,
    namespace: &str,
    id: &str,
    state: i8,
    updated_time: i64,
    sequence: u64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO requests (namespace, id, task_id, trace_id, node, mode, state, version, priority, snapshot, snapshot_digest, next_time, leased_by, lease_time, retry_count, max_retry_count, failed_workers, ack_version, created_time, updated_time, sequence) VALUES (?, ?, 'task', 'trace', 'index', 'http', ?, 1, 0, JSON_OBJECT(), ?, 0, '', 0, 0, 1, JSON_ARRAY(), NULL, ?, ?, ?)",
    )
    .bind(namespace)
    .bind(id)
    .bind(state)
    .bind("0".repeat(64))
    .bind(updated_time)
    .bind(updated_time)
    .bind(sequence)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_completion(
    pool: &Pool<SqlxMySql>,
    namespace: &str,
    request_id: &str,
    created_time: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO request_completions (namespace, request_id, version, task_id, trace_id, node, worker_id, state, error, payload_digest, created_time) VALUES (?, ?, 1, 'task', 'trace', 'index', 'worker', 2, NULL, ?, ?)",
    )
    .bind(namespace)
    .bind(request_id)
    .bind("0".repeat(64))
    .bind(created_time)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_operation(
    pool: &Pool<SqlxMySql>,
    namespace: &str,
    key: &str,
    updated_time: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO operations (namespace, kind, operation_key, request_digest, result, created_time, updated_time) VALUES (?, 'test', ?, ?, JSON_OBJECT(), ?, ?)",
    )
    .bind(namespace)
    .bind(key)
    .bind("0".repeat(64))
    .bind(updated_time)
    .bind(updated_time)
    .execute(pool)
    .await?;
    Ok(())
}

async fn count(pool: &Pool<SqlxMySql>, query: &'static str, namespace: &str) -> Result<i64> {
    Ok(sqlx::query_scalar(query)
        .bind(namespace)
        .fetch_one(pool)
        .await?)
}

fn seed() -> CodeSeed {
    CodeSeed {
        node: "index".to_string(),
        url: "https://example.com/".to_string(),
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
    }
}

fn task(id: &str, priority: i32) -> Task {
    Task {
        id: id.to_string(),
        name: format!("integration-{id}"),
        periodic: false,
        interval_ms: 0,
        priority,
        params: HashMap::new(),
        dsl: None,
        seeds: vec![seed()],
        persister_id: None,
        attachment: None,
        next_time: 0,
    }
}

async fn clear(pool: &Pool<SqlxMySql>, namespace: &str) -> Result<()> {
    for statement in [
        "DELETE FROM trace_stats WHERE namespace = ?",
        "DELETE FROM items WHERE namespace = ?",
        "DELETE FROM request_completions WHERE namespace = ?",
        "DELETE FROM requests WHERE namespace = ?",
        "DELETE FROM traces WHERE namespace = ?",
        "DELETE FROM workers WHERE namespace = ?",
        "DELETE FROM operations WHERE namespace = ?",
        "DELETE FROM tasks WHERE namespace = ?",
        "DELETE FROM queue_sequences WHERE namespace = ?",
    ] {
        sqlx::query(statement).bind(namespace).execute(pool).await?;
    }
    Ok(())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn require(condition: bool, message: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
