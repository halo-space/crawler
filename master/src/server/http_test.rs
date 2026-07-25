use std::error::Error as StdError;
use std::time::Duration;

use reqwest::{Client, Response};
use serde_json::json;
use sqlx::{MySql as SqlxMySql, Pool};
use tokio::sync::oneshot;

use super::*;

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

#[tokio::test]
async fn http_dispatches_and_settles_a_code_task() -> Result<()> {
    let Some(database_url) = database_url() else {
        eprintln!("skipping Master HTTP integration test: CRAWLER_MYSQL_URL is not set");
        return Ok(());
    };

    let namespace = format!("master-http-{}", uuid::Uuid::now_v7().simple());
    let task_id = format!("task-{}", uuid::Uuid::now_v7().simple());
    let config = Config::new(
        "127.0.0.1:0".parse()?,
        database_url,
        &namespace,
        "http-integration-worker-token",
        "http-integration-control-token",
    )?
    .with_cron_interval(Duration::from_millis(10))?;
    let server = Server::from_config(config).await?;
    let pool = server.store.pool().clone();

    clear(&pool, &namespace).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (stop, shutdown) = oneshot::channel();
    let serving = tokio::spawn(server.serve_listener(listener, async move {
        let _ = shutdown.await;
    }));

    let client = Client::builder().no_proxy().build()?;
    let result = exercise(&client, address, &namespace, &task_id).await;
    let _ = stop.send(());
    let stopped = async {
        tokio::time::timeout(Duration::from_secs(5), serving)
            .await
            .map_err(|_| std::io::Error::other("Master server did not stop"))???;
        Ok::<(), Box<dyn StdError>>(())
    }
    .await;
    let cleanup = clear(&pool, &namespace).await;
    pool.close().await;

    result?;
    stopped?;
    cleanup?;
    Ok(())
}

async fn exercise(
    client: &Client,
    address: std::net::SocketAddr,
    namespace: &str,
    task_id: &str,
) -> Result<()> {
    let base = format!("http://{address}");
    let control_token = "http-integration-control-token";
    let worker_token = "http-integration-worker-token";
    let worker_id = "http-integration-worker";

    let response = client
        .put(format!("{base}/v1/control/tasks/{task_id}"))
        .bearer_auth(control_token)
        .header("X-Crawler-Namespace", namespace)
        .json(&json!({
            "id": task_id,
            "name": format!("http-{task_id}"),
            "seeds": [{
                "node": "index",
                "url": "https://example.com/"
            }],
            "next_time": 0
        }))
        .send()
        .await?;
    ensure_success(response, "publish Task").await?;

    let claimed = claim(client, &base, namespace, worker_token, worker_id).await?;
    let identity = crate::wire::Identity {
        id: claimed.snapshot.id,
        task_id: claimed.snapshot.task_id,
        trace_id: claimed.snapshot.trace_id,
        version: claimed.execution.version,
        worker_id: worker_id.to_string(),
        node: claimed.snapshot.node,
    };

    let response = client
        .post(format!("{base}/v1/worker/requests/ack"))
        .bearer_auth(worker_token)
        .header("X-Crawler-Namespace", namespace)
        .json(&identity)
        .send()
        .await?;
    ensure_success(response, "acknowledge Request").await?;

    let now = now_millis();
    let response = client
        .post(format!("{base}/v1/worker/requests/success"))
        .bearer_auth(worker_token)
        .header("X-Crawler-Namespace", namespace)
        .json(&json!({
            "identity": identity,
            "stats": {},
            "start_time": now,
            "end_time": now
        }))
        .send()
        .await?;
    ensure_success(response, "settle Request").await?;

    let response = client
        .post(format!("{base}/v1/worker/requests/pending"))
        .bearer_auth(worker_token)
        .header("X-Crawler-Namespace", namespace)
        .json(&json!({
            "worker_id": worker_id,
            "modes": ["http"]
        }))
        .send()
        .await?;
    let response = ensure_success(response, "check pending Requests").await?;
    let pending: crate::wire::Pending = response.json().await?;
    if pending.pending {
        return Err(std::io::Error::other("Master still reports a pending Request").into());
    }
    Ok(())
}

async fn claim(
    client: &Client,
    base: &str,
    namespace: &str,
    token: &str,
    worker_id: &str,
) -> Result<crate::wire::Claimed> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut attempt = 0_u32;
    loop {
        let response = client
            .post(format!("{base}/v1/worker/requests/claim"))
            .bearer_auth(token)
            .header("X-Crawler-Namespace", namespace)
            .header(
                "Idempotency-Key",
                format!("claim-{}-{attempt}", uuid::Uuid::now_v7().simple()),
            )
            .json(&json!({
                "limit": 1,
                "worker_id": worker_id,
                "modes": ["http"]
            }))
            .send()
            .await?;
        let response = ensure_success(response, "claim Request").await?;
        let claims: crate::wire::Claims = response.json().await?;
        if claims.requests.len() > 1 {
            return Err(std::io::Error::other("Master returned more Requests than claimed").into());
        }
        if let Some(claimed) = claims.requests.into_iter().next() {
            return Ok(claimed);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::other("Cron did not dispatch the code Task").into());
        }
        attempt = attempt.saturating_add(1);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn ensure_success(response: Response, operation: &str) -> Result<Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    Err(std::io::Error::other(format!("Master failed to {operation}: {status} {body}")).into())
}

fn database_url() -> Option<String> {
    std::env::var("CRAWLER_MYSQL_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
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
