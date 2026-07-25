use std::error::Error as StdError;

use super::super::MySql;
use crate::control::{item, request, task, trace, worker};
use crate::{Config, wire};
use serde_json::json;

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn control_queries_return_bounded_domains_and_stable_pages() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };
    let namespace = format!("master-observe-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "observe-worker-token",
        "observe-control-token",
    )?;
    let store = MySql::connect(&config).await?;

    clear(&store, &namespace).await?;
    let result = exercise(&store, &namespace).await;
    let cleanup = clear(&store, &namespace).await;
    store.pool.close().await;

    result?;
    cleanup?;
    Ok(())
}

async fn exercise(store: &MySql, namespace: &str) -> Result<()> {
    let now = super::super::time::now_millis();
    insert_task(store, namespace, "task-old", "Old", 3, 0, now - 1).await?;
    insert_task(store, namespace, "task-new", "New", 1, 1, now).await?;

    let first = store
        .tasks(
            namespace,
            &task::List {
                limit: Some(1),
                cursor: None,
                state: None,
            },
        )
        .await?;
    require(first.items.len() == 1, "Task first page has the wrong size")?;
    require(first.items[0].id == "task-new", "Task ordering is unstable")?;
    let wrong_endpoint = store
        .requests(
            namespace,
            &request::List {
                limit: None,
                cursor: first.next_cursor.clone(),
                trace_id: None,
                state: None,
                worker_id: None,
            },
        )
        .await;
    require(
        wrong_endpoint.is_err(),
        "Task cursor was accepted by the Request endpoint",
    )?;
    let second = store
        .tasks(
            namespace,
            &task::List {
                limit: Some(1),
                cursor: first.next_cursor.clone(),
                state: None,
            },
        )
        .await?;
    require(
        second.items.len() == 1 && second.items[0].id == "task-old",
        "Task cursor skipped or repeated a row",
    )?;
    let changed_filter = store
        .tasks(
            namespace,
            &task::List {
                limit: Some(1),
                cursor: first.next_cursor,
                state: Some(task::State::Scheduled),
            },
        )
        .await;
    require(
        changed_filter.is_err(),
        "Task cursor was accepted after changing filters",
    )?;
    let task = store
        .task(namespace, "task-new")
        .await?
        .ok_or("Task detail is missing")?;
    require(task.summary.periodic, "Task run mode was not exposed")?;
    require(
        task.params.get("source") == Some(&json!("integration")),
        "Task params were not exposed by detail",
    )?;

    let trace_id = "observe-trace";
    let request_id = "observe-request";
    let mut queued = spider::net::Request::follow("https://example.com/detail")?.node("detail");
    queued.id = request_id.to_string();
    queued.task_id = "task-new".to_string();
    queued.trace_id = trace_id.to_string();
    queued.priority = 7;
    let snapshot = spider::net::request::Snapshot::try_from(queued)?;
    let mut trace_snapshot = spider::trace::Snapshot::code("task-new");
    trace_snapshot.priority = 7;
    store
        .init(
            namespace,
            "observe-init",
            &wire::Init {
                trace_id: trace_id.to_string(),
                trace: trace_snapshot,
                requests: vec![snapshot],
            },
        )
        .await?;
    sqlx::query(
        "UPDATE requests SET retry_count = 1, failed_workers = JSON_ARRAY('worker-old') \
         WHERE namespace = ? AND id = ?",
    )
    .bind(namespace)
    .bind(request_id)
    .execute(&store.pool)
    .await?;
    sqlx::query(
        "INSERT INTO request_completions (namespace, request_id, version, task_id, trace_id, \
         node, worker_id, state, error, payload_digest, created_time) \
         VALUES (?, ?, 0, 'task-new', ?, 'detail', 'worker-old', 3, 'previous failure', ?, ?)",
    )
    .bind(namespace)
    .bind(request_id)
    .bind(trace_id)
    .bind("0".repeat(64))
    .bind(now)
    .execute(&store.pool)
    .await?;
    sqlx::query(
        "INSERT INTO trace_stats (namespace, trace_id, name, total, done, filter_count, dedup, \
         validate_count, download, created_time, updated_time) \
         VALUES (?, ?, 'detail', 2, 1, 0, 0, 1, 1, ?, ?)",
    )
    .bind(namespace)
    .bind(trace_id)
    .bind(now)
    .bind(now)
    .execute(&store.pool)
    .await?;

    let traces = store
        .traces(
            namespace,
            &trace::List {
                limit: None,
                cursor: None,
                task_id: Some("task-new".to_string()),
            },
        )
        .await?;
    require(
        traces.items.len() == 1 && traces.items[0].priority == 7,
        "Trace summary did not expose priority",
    )?;
    let trace = store
        .trace_detail(namespace, trace_id)
        .await?
        .ok_or("Trace detail is missing")?;
    require(
        trace.requests.pending == 1,
        "Trace Request counts are incorrect",
    )?;
    require(
        trace.stats.get("detail").map(|value| value.total) == Some(2),
        "Trace stats are missing",
    )?;

    let requests = store
        .requests(
            namespace,
            &request::List {
                limit: None,
                cursor: None,
                trace_id: Some(trace_id.to_string()),
                state: Some(spider::net::State::Pending),
                worker_id: None,
            },
        )
        .await?;
    require(
        requests.items.len() == 1 && requests.items[0].id == request_id,
        "Request filters did not return the expected row",
    )?;
    let request = store
        .request_detail(namespace, request_id)
        .await?
        .ok_or("Request detail is missing")?;
    require(
        request.failed_workers.len() == 1 && request.failed_workers[0] == "worker-old",
        "Request failed Workers are missing",
    )?;
    require(
        request
            .completion
            .as_ref()
            .and_then(|value| value.error.as_deref())
            == Some("previous failure"),
        "Request latest completion is missing",
    )?;

    store
        .heartbeat(
            namespace,
            &wire::Heartbeat {
                worker_id: "worker-online".to_string(),
                modes: vec![spider::net::Mode::Http],
            },
        )
        .await?;
    sqlx::query(
        "INSERT INTO workers (namespace, id, modes, last_heartbeat, created_time, updated_time) \
         VALUES (?, 'worker-offline', JSON_ARRAY('browser'), 0, 0, 0)",
    )
    .bind(namespace)
    .execute(&store.pool)
    .await?;
    let workers = store
        .workers(
            namespace,
            &worker::List {
                limit: None,
                cursor: None,
                mode: Some(spider::net::Mode::Http),
                online: Some(true),
            },
        )
        .await?;
    require(
        workers.items.len() == 1
            && workers.items[0].id == "worker-online"
            && workers.items[0].online,
        "Worker filters or online state are incorrect",
    )?;

    for (id, item_id, created_time) in [
        ("00000000-0000-7000-8000-000000000001", "item-old", now - 1),
        ("00000000-0000-7000-8000-000000000002", "item-new", now),
    ] {
        sqlx::query(
            "INSERT INTO items (namespace, id, item_id, task_id, trace_id, request_id, \
             persister_id, config_version, timezone, data, created_time, updated_time) \
             VALUES (?, ?, ?, 'task-new', ?, ?, 'jsonl', 'v1', 'Asia/Shanghai', \
             JSON_OBJECT('title', ?), ?, ?)",
        )
        .bind(namespace)
        .bind(id)
        .bind(item_id)
        .bind(trace_id)
        .bind(request_id)
        .bind(item_id)
        .bind(created_time)
        .bind(created_time)
        .execute(&store.pool)
        .await?;
    }
    let first = store
        .item_list(
            namespace,
            &item::List {
                limit: Some(1),
                cursor: None,
                trace_id: Some(trace_id.to_string()),
                request_id: None,
            },
        )
        .await?;
    require(
        first.items.len() == 1 && first.items[0].item_id == "item-new",
        "Item ordering is incorrect",
    )?;
    let second = store
        .item_list(
            namespace,
            &item::List {
                limit: Some(1),
                cursor: first.next_cursor,
                trace_id: Some(trace_id.to_string()),
                request_id: None,
            },
        )
        .await?;
    require(
        second.items.len() == 1 && second.items[0].item_id == "item-old",
        "Item cursor skipped or repeated a row",
    )?;
    let item = store
        .item_detail(namespace, "00000000-0000-7000-8000-000000000002")
        .await?
        .ok_or("Item detail is missing")?;
    require(
        item.data.get("title") == Some(&json!("item-new")),
        "Item data is missing from detail",
    )?;
    Ok(())
}

async fn insert_task(
    store: &MySql,
    namespace: &str,
    id: &str,
    name: &str,
    state: i8,
    run_mode: i8,
    time: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO tasks (namespace, id, name, state, run_mode, interval_ms, priority, params, \
         dsl, seed_specs, persister_id, attachment, next_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, 1000, 7, JSON_OBJECT('source', 'integration'), NULL, \
         JSON_ARRAY(), NULL, NULL, 0, ?, ?)",
    )
    .bind(namespace)
    .bind(id)
    .bind(name)
    .bind(state)
    .bind(run_mode)
    .bind(time)
    .bind(time)
    .execute(&store.pool)
    .await?;
    Ok(())
}

async fn clear(store: &MySql, namespace: &str) -> Result<()> {
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
        match sqlx::query(statement)
            .bind(namespace)
            .execute(&store.pool)
            .await
        {
            Ok(_) => {}
            Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("1146") => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn database_url() -> Option<String> {
    std::env::var("CRAWLER_MYSQL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn require(value: bool, message: &str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}
