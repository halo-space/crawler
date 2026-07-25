use std::collections::HashMap;
use std::error::Error as StdError;

use serde_json::Value;

use super::MySql;
use crate::{Config, Error, wire};

mod claim;
mod idempotency;
mod queue;
mod schema;
mod stats;
mod task;
mod worker;

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

struct Database {
    store: MySql,
    namespace: String,
}

impl Database {
    async fn connect(recovery_limit: usize) -> Result<Option<Self>> {
        let Some(database_url) = database_url() else {
            eprintln!("skipping MySQL integration test: CRAWLER_MYSQL_URL is not set");
            return Ok(None);
        };
        let namespace = format!("master-consistency-{}", uuid::Uuid::now_v7().simple());
        let config = Config::new(
            "127.0.0.1:0".parse()?,
            database_url,
            &namespace,
            "integration-worker-token",
            "integration-control-token",
        )?
        .with_recovery_limit(recovery_limit)?;
        let store = MySql::connect(&config).await?;
        clear(&store, &namespace).await?;
        Ok(Some(Self { store, namespace }))
    }

    async fn teardown(self, result: Result<()>) -> Result<()> {
        let cleanup = clear(&self.store, &self.namespace).await;
        self.store.pool.close().await;
        result?;
        cleanup?;
        Ok(())
    }
}

fn database_url() -> Option<String> {
    std::env::var("CRAWLER_MYSQL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
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
        sqlx::query(statement)
            .bind(namespace)
            .execute(&store.pool)
            .await?;
    }
    Ok(())
}

fn snapshot(
    id: &str,
    task_id: &str,
    trace_id: &str,
    max_retry_count: i32,
) -> spider::net::request::Snapshot {
    let mut request = spider::net::Request::follow(format!("https://example.com/{id}"))
        .unwrap()
        .with_id(id)
        .node("index");
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request.max_retry_count = max_retry_count;
    spider::net::request::Snapshot::try_from(request).unwrap()
}

fn init_body(
    task_id: &str,
    trace_id: &str,
    requests: Vec<spider::net::request::Snapshot>,
) -> wire::Init {
    wire::Init {
        trace_id: trace_id.to_string(),
        trace: spider::trace::Snapshot::code(task_id),
        requests,
    }
}

async fn init(
    database: &Database,
    key: &str,
    task_id: &str,
    trace_id: &str,
    ids: &[&str],
    max_retry_count: i32,
) -> Result<()> {
    let requests = ids
        .iter()
        .map(|id| snapshot(id, task_id, trace_id, max_retry_count))
        .collect();
    database
        .store
        .init(
            &database.namespace,
            key,
            &init_body(task_id, trace_id, requests),
        )
        .await?;
    Ok(())
}

async fn claim(
    database: &Database,
    key: &str,
    worker_id: &str,
    limit: usize,
) -> Result<Vec<wire::Claimed>> {
    Ok(database
        .store
        .claim(
            &database.namespace,
            key,
            &wire::Claim {
                limit,
                worker_id: worker_id.to_string(),
                modes: vec![spider::net::Mode::Http],
            },
        )
        .await?
        .requests)
}

fn identity(request: &wire::Claimed, worker_id: &str) -> wire::Identity {
    wire::Identity {
        id: request.snapshot.id.clone(),
        task_id: request.snapshot.task_id.clone(),
        trace_id: request.snapshot.trace_id.clone(),
        version: request.execution.version,
        worker_id: worker_id.to_string(),
        node: request.snapshot.node.clone(),
    }
}

fn completion(
    identity: wire::Identity,
    stats: HashMap<String, Value>,
    error: Option<&str>,
) -> wire::Completion {
    wire::Completion {
        identity,
        stats,
        start_time: 1,
        end_time: 2,
        error: error.map(str::to_string),
    }
}

fn require_one_conflict<T>(
    left: &std::result::Result<T, Error>,
    right: &std::result::Result<T, Error>,
) -> Result<()> {
    let successes = usize::from(left.is_ok()) + usize::from(right.is_ok());
    let conflicts = [left, right]
        .into_iter()
        .filter(|result| matches!(result, Err(Error::Conflict(_))))
        .count();
    if successes == 1 && conflicts == 1 {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "expected one success and one conflict, got success={successes}, conflict={conflicts}, \
             left={:?}, right={:?}",
            left.as_ref().err().map(ToString::to_string),
            right.as_ref().err().map(ToString::to_string),
        ))
        .into())
    }
}

fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(std::io::Error::other(message.into()).into())
    }
}
