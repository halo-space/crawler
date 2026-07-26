use std::collections::HashMap;
use std::time::{Duration, Instant};

use base64::Engine as _;
use contrib::middleware::rate_limit::Redis;
use spider::middleware::{Middleware as _, Next, Spec};
use spider::net::Request;

#[allow(dead_code)]
#[path = "redis/server.rs"]
mod server;

#[tokio::test]
async fn workers_share_one_group_schedule_across_tasks() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let group = server::namespace("rate-limit-shared");
    let first = Redis::new(server.url()).unwrap();
    let second = Redis::new(server.url()).unwrap();
    let spec = spec(&group, 10.0);
    let mut one = Request::follow("https://example.com/one").unwrap();
    one.task_id = "task-one".to_string();
    let mut two = Request::follow("https://example.com/two").unwrap();
    two.task_id = "task-two".to_string();
    let started = Instant::now();

    let (one, two) = tokio::join!(
        first.before_download(one, &spec),
        second.before_download(two, &spec)
    );

    assert!(matches!(one.unwrap(), Next::Continue(_)));
    assert!(matches!(two.unwrap(), Next::Continue(_)));
    assert!(started.elapsed() >= Duration::from_millis(80));
    remove(&server, &group).await;
}

#[tokio::test]
async fn different_groups_reserve_independent_schedules() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let first_group = server::namespace("rate-limit-first-group");
    let second_group = server::namespace("rate-limit-second-group");
    let limiter = Redis::new(server.url()).unwrap();
    let first_spec = spec(&first_group, 0.5);
    let second_spec = spec(&second_group, 0.5);
    let first = limiter.before_download(
        Request::follow("https://example.com/one").unwrap(),
        &first_spec,
    );
    let second = limiter.before_download(
        Request::follow("https://example.com/two").unwrap(),
        &second_spec,
    );

    let (first, second) = tokio::time::timeout(Duration::from_millis(500), async {
        tokio::join!(first, second)
    })
    .await
    .expect("independent groups must not wait behind each other");

    assert!(matches!(first.unwrap(), Next::Continue(_)));
    assert!(matches!(second.unwrap(), Next::Continue(_)));
    remove(&server, &first_group).await;
    remove(&server, &second_group).await;
}

#[tokio::test]
async fn active_qps_conflict_does_not_change_the_schedule() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let group = server::namespace("rate-limit-conflict");
    let limiter = Redis::new(server.url()).unwrap();
    let request = || Request::follow("https://example.com/data").unwrap();

    limiter
        .before_download(request(), &spec(&group, 2.0))
        .await
        .unwrap();
    let before = state(&server, &group).await;
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        limiter.before_download(request(), &spec(&group, 4.0)),
    )
    .await
    .expect("a conflicting qps must fail without waiting");

    assert!(matches!(
        result,
        Err(spider::middleware::Error::InvalidConfig { name, message })
            if name == "rate_limit" && message.contains("different qps")
    ));
    assert_eq!(state(&server, &group).await, before);

    tokio::time::sleep(Duration::from_millis(550)).await;
    limiter
        .before_download(request(), &spec(&group, 4.0))
        .await
        .unwrap();
    assert_eq!(state(&server, &group).await["interval"], "250000");
    remove(&server, &group).await;
}

#[tokio::test]
async fn group_state_uses_a_bounded_idle_expiration() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let group = server::namespace("rate-limit-expiration");
    let limiter = Redis::new(server.url()).unwrap();

    limiter
        .before_download(
            Request::follow("https://example.com/data").unwrap(),
            &spec(&group, 10.0),
        )
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let ttl = redis::cmd("PTTL")
        .arg(key(&group))
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    assert!((59_000..=61_000).contains(&ttl), "unexpected TTL: {ttl}");
    remove(&server, &group).await;
}

#[tokio::test]
async fn corrupt_group_state_is_rejected_without_mutation() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let group = server::namespace("rate-limit-corrupt");
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key(&group))
        .arg("interval")
        .arg("9007199254740992")
        .arg("next")
        .arg("0")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    let before = state(&server, &group).await;
    let limiter = Redis::new(server.url()).unwrap();

    let error = limiter
        .before_download(
            Request::follow("https://example.com/data").unwrap(),
            &spec(&group, 1.0),
        )
        .await
        .unwrap_err();

    assert!(error.to_string().contains("state is invalid"));
    assert_eq!(state(&server, &group).await, before);
    remove(&server, &group).await;
}

#[test]
fn invalid_redis_url_fails_during_construction() {
    let error = match Redis::new("not a redis URL") {
        Ok(_) => panic!("an invalid Redis URL must fail construction"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Redis RateLimit operation failed")
    );
}

fn spec(group: &str, qps: f64) -> Spec {
    Spec::new("rate_limit")
        .hook("before_download")
        .args(serde_json::json!({"group": group, "qps": qps}))
}

fn key(group: &str) -> String {
    let group = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(group.as_bytes());
    format!("rate_limit:{group}")
}

async fn state(server: &server::Handle, group: &str) -> HashMap<String, String> {
    let mut connection = server.connection().await;
    redis::cmd("HGETALL")
        .arg(key(group))
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn remove(server: &server::Handle, group: &str) {
    let mut connection = server.connection().await;
    redis::cmd("DEL")
        .arg(key(group))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
}
