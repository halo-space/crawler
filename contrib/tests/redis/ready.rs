use contrib::scheduler::redis::Redis;
use spider::{Scheduler, net, payload};

use super::{key, request, server, settlement, worker};

#[tokio::test]
async fn ready_events_follow_release_retry_and_terminal_settlement() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("ready-lifecycle");
    let scheduler = server.redis_as(&namespace, worker::A);
    let backup = server.redis_as(&namespace, worker::B);
    server::open(&scheduler).await;
    server::open(&backup).await;
    super::run::init(&scheduler).await;

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "released",
            "https://example.com/released",
        )]))
        .await
        .unwrap();
    assert_ready(&server, &namespace, "released").await;
    let released = claim(&scheduler).await;
    assert_clear(&server, &namespace, "released").await;
    scheduler
        .release(&settlement::processing(&released))
        .await
        .unwrap();
    assert_ready(&server, &namespace, "released").await;
    let released = claim(&scheduler).await;
    settlement::succeed(&scheduler, &released).await;
    assert_clear(&server, &namespace, "released").await;

    let mut retried = request::new("retried", "https://example.com/retried");
    retried.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![retried]))
        .await
        .unwrap();
    let retried = claim(&scheduler).await;
    scheduler
        .ack(&settlement::processing(&retried))
        .await
        .unwrap();
    scheduler.failure(&failed(&retried, "retry")).await.unwrap();
    assert_ready(&server, &namespace, "retried").await;
    let retried = claim(&backup).await;
    settlement::succeed(&backup, &retried).await;
    assert_clear(&server, &namespace, "retried").await;

    let mut terminal = request::new("terminal", "https://example.com/terminal");
    terminal.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![terminal]))
        .await
        .unwrap();
    let terminal = claim(&scheduler).await;
    scheduler
        .ack(&settlement::processing(&terminal))
        .await
        .unwrap();
    scheduler
        .failure(&failed(&terminal, "terminal"))
        .await
        .unwrap();
    assert_clear(&server, &namespace, "terminal").await;

    scheduler.close().await.unwrap();
    backup.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn missing_request_removes_its_ready_member_and_event() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("missing-ready-request");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    scheduler
        .push(
            payload::Payload::new()
                .requests(vec![request::new("missing", "https://example.com/missing")]),
        )
        .await
        .unwrap();
    assert_ready(&server, &namespace, "missing").await;

    let mut connection = server.connection().await;
    redis::cmd("DEL")
        .arg(key::request(&namespace, "missing"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(cardinality(&server, ready(&namespace)).await, 0);
    assert_eq!(cardinality(&server, events(&namespace)).await, 0);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn malformed_ready_member_clears_its_referenced_event() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("malformed-ready-member");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("damaged", "https://example.com/damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![
            damaged,
            request::new("valid", "https://example.com/valid"),
        ]))
        .await
        .unwrap();

    let request_key = key::request(&namespace, "damaged");
    let malformed = format!("not-a-revision|{}", key::segment("damaged"));
    let mut connection = server.connection().await;
    let stored = redis::cmd("HGET")
        .arg(&request_key)
        .arg("queue_member")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::pipe()
        .atomic()
        .cmd("ZREM")
        .arg(ready(&namespace))
        .arg(stored)
        .ignore()
        .cmd("HSET")
        .arg(&request_key)
        .arg("queue_member")
        .arg(&malformed)
        .ignore()
        .cmd("ZADD")
        .arg(ready(&namespace))
        .arg(-20)
        .arg(malformed)
        .ignore()
        .query_async::<()>(&mut connection)
        .await
        .unwrap();

    let valid = claim(&scheduler).await;
    assert_eq!(valid.id, "valid");
    settlement::succeed(&scheduler, &valid).await;
    let state = redis::cmd("HGET")
        .arg(request_key)
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    assert_eq!(cardinality(&server, ready(&namespace)).await, 0);
    assert_eq!(cardinality(&server, events(&namespace)).await, 0);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn dangling_terminal_member_clears_exclusions_before_pending_is_reused() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("terminal-ready-exclusion");
    let scheduler = server.redis(&namespace);
    let worker_b = server.redis_as(&namespace, worker::B);
    server::open(&scheduler).await;
    server::open(&worker_b).await;
    super::run::init(&scheduler).await;

    let mut stale = request::new("stale", "https://example.com/stale");
    stale.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![stale]))
        .await
        .unwrap();
    let stale = claim(&scheduler).await;
    scheduler
        .ack(&settlement::processing(&stale))
        .await
        .unwrap();
    scheduler.failure(&failed(&stale, "retry")).await.unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "stale"))
        .arg("state")
        .arg("done")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert!(worker_b.next_requests(1).await.unwrap().is_empty());
    assert_eq!(cardinality(&server, ready(&namespace)).await, 0);
    assert_eq!(cardinality(&server, events(&namespace)).await, 0);
    assert_eq!(cardinality(&server, exclusions(&namespace)).await, 0);

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "eligible",
            "https://example.com/eligible",
        )]))
        .await
        .unwrap();
    assert!(scheduler.has_pending_requests().await.unwrap());
    let eligible = claim(&scheduler).await;
    assert_eq!(eligible.id, "eligible");
    settlement::succeed(&scheduler, &eligible).await;

    scheduler.close().await.unwrap();
    worker_b.close().await.unwrap();
    server.clear(&namespace).await;
}

async fn claim(scheduler: &Redis) -> net::Request {
    scheduler
        .next_requests(1)
        .await
        .unwrap()
        .pop()
        .expect("expected one ready Request")
}

fn failed(request: &net::Request, message: &str) -> payload::Payload {
    let mut payload =
        payload::Payload::for_request(request, request.leased_by.clone()).failed(message);
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

async fn assert_ready(server: &server::Handle, namespace: &str, id: &str) {
    let mut connection = server.connection().await;
    let request = key::request(namespace, id);
    let member = redis::cmd("HGET")
        .arg(&request)
        .arg("queue_member")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    let event = redis::cmd("HGET")
        .arg(request)
        .arg("ready_event")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert!(!member.is_empty());
    assert!(event.ends_with(&member));
    assert!(
        redis::cmd("ZSCORE")
            .arg(events(namespace))
            .arg(event)
            .query_async::<Option<f64>>(&mut connection)
            .await
            .unwrap()
            .is_some()
    );
}

async fn assert_clear(server: &server::Handle, namespace: &str, id: &str) {
    let mut connection = server.connection().await;
    let event = redis::cmd("HGET")
        .arg(key::request(namespace, id))
        .arg("ready_event")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert!(event.is_empty());
    assert_eq!(cardinality(server, events(namespace)).await, 0);
}

async fn cardinality(server: &server::Handle, key: String) -> usize {
    redis::cmd("ZCARD")
        .arg(key)
        .query_async(&mut server.connection().await)
        .await
        .unwrap()
}

fn ready(namespace: &str) -> String {
    format!("{namespace}:queue:http:ready")
}

fn events(namespace: &str) -> String {
    format!("{namespace}:ready_events:http")
}

fn exclusions(namespace: &str) -> String {
    format!("{namespace}:pending_exclusions:http")
}
