use std::collections::HashMap;
use std::time::Duration;

use spider::{Scheduler, net, payload};

use super::{key, request, run, server, settlement};

pub(super) const A: &str = "worker-a";
pub(super) const B: &str = "worker-b";
#[tokio::test]
async fn registration_stores_worker_metadata_and_close_marks_it_offline() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-metadata");
    let scheduler = server
        .redis_as(&namespace, A)
        .with_worker_host("crawler-node-01")
        .unwrap()
        .with_worker_version("1.0.0")
        .unwrap()
        .with_modes([net::Mode::Browser, net::Mode::Http])
        .unwrap()
        .with_heartbeat(Duration::from_millis(50), Duration::from_millis(200))
        .unwrap();

    scheduler.open(7).await.unwrap();
    let worker_key = key::worker(&namespace, A);
    let mut connection = server.connection().await;
    let registered = redis::cmd("HGETALL")
        .arg(&worker_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(registered["worker_id"], A);
    assert_eq!(registered["host"], "crawler-node-01");
    assert_eq!(registered["version"], "1.0.0");
    assert_eq!(registered["modes"], r#"["http","browser"]"#);
    assert_eq!(registered["concurrency"], "7");
    assert_eq!(registered["heartbeat_timeout"], "200");
    assert_eq!(registered["offline_time"], "");
    assert_eq!(registered.len(), 10);
    let created_time = registered["created_time"].parse::<i64>().unwrap();
    assert!(registered["last_heartbeat"].parse::<i64>().unwrap() >= created_time);
    assert!(!registered["token"].is_empty());
    assert!(!registered.contains_key("ip"));

    let token = registered["token"].clone();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let last_heartbeat = redis::cmd("HGET")
                .arg(&worker_key)
                .arg("last_heartbeat")
                .query_async::<i64>(&mut connection)
                .await
                .unwrap();
            if last_heartbeat > created_time {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Redis Worker heartbeat did not update its record");
    assert_eq!(
        redis::cmd("HGET")
            .arg(&worker_key)
            .arg("token")
            .query_async::<String>(&mut connection)
            .await
            .unwrap(),
        token
    );

    scheduler.close().await.unwrap();
    let offline = redis::cmd("HGETALL")
        .arg(&worker_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert!(!offline["offline_time"].is_empty());
    assert_eq!(offline["token"], token);
    server.clear(&namespace).await;
}

#[tokio::test]
async fn open_rejects_a_different_concurrency_until_close() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-open-concurrency");
    let scheduler = server.redis_as(&namespace, A);

    scheduler.open(4).await.unwrap();
    scheduler.open(4).await.unwrap();
    let error = scheduler.open(5).await.unwrap_err();
    assert!(error.to_string().contains("already open with concurrency"));
    scheduler.close().await.unwrap();

    scheduler.open(5).await.unwrap();
    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn replaying_one_registration_uses_the_same_token_without_extra_worker_fields() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-register-replay");
    let worker_key = key::worker(&namespace, A);
    let mut connection = server.connection().await;
    let script = redis::Script::new(include_str!(
        "../../src/scheduler/redis/scripts/register.lua"
    ));

    let first = script
        .prepare_invoke()
        .key(&worker_key)
        .arg(A)
        .arg("crawler-node-01")
        .arg("1.0.0")
        .arg(r#"["http"]"#)
        .arg(4)
        .arg(30_000)
        .arg("open-attempt-1")
        .invoke_async::<(i64, String, String)>(&mut connection)
        .await
        .unwrap();
    assert_eq!(first.0, 200);
    let created_time = redis::cmd("HGET")
        .arg(&worker_key)
        .arg("created_time")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&worker_key)
        .arg("open_key")
        .arg("legacy")
        .arg("state")
        .arg("online")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let replay = script
        .prepare_invoke()
        .key(&worker_key)
        .arg(A)
        .arg("crawler-node-01")
        .arg("1.0.0")
        .arg(r#"["http"]"#)
        .arg(4)
        .arg(30_000)
        .arg("open-attempt-1")
        .invoke_async::<(i64, String, String)>(&mut connection)
        .await
        .unwrap();
    assert_eq!(replay.0, 200);
    assert_eq!(replay.2, first.2);
    let worker = redis::cmd("HGETALL")
        .arg(&worker_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(worker["created_time"], created_time);
    assert_eq!(worker.len(), 10);
    assert!(!worker.contains_key("open_key"));
    assert!(!worker.contains_key("state"));

    let conflict = script
        .prepare_invoke()
        .key(&worker_key)
        .arg(A)
        .arg("crawler-node-01")
        .arg("1.0.0")
        .arg(r#"["http"]"#)
        .arg(4)
        .arg(30_000)
        .arg("open-attempt-2")
        .invoke_async::<(i64, String, String)>(&mut connection)
        .await
        .unwrap();
    assert_eq!(conflict.0, 100);
    server.clear(&namespace).await;
}

#[tokio::test]
async fn an_online_worker_id_conflicts_and_offline_allows_registration() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-conflict");
    let first = server.redis_as(&namespace, A);
    let second = server.redis_as(&namespace, A);

    first.open(4).await.unwrap();
    let error = second.open(4).await.unwrap_err();
    assert!(error.to_string().contains("code 100"), "{error}");

    first.close().await.unwrap();
    second.open(4).await.unwrap();
    second.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn a_new_worker_cannot_shorten_the_existing_workers_online_window() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-timeout-policy");
    let first = server
        .redis_as(&namespace, A)
        .with_heartbeat(Duration::from_secs(1), Duration::from_secs(10))
        .unwrap();
    let second = server
        .redis_as(&namespace, A)
        .with_heartbeat(Duration::from_millis(1), Duration::from_millis(2))
        .unwrap();

    first.open(4).await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    let error = second.open(4).await.unwrap_err();
    assert!(error.to_string().contains("code 100"), "{error}");

    first.close().await.unwrap();
    second.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn a_timed_out_worker_id_can_register_again() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-timeout");
    let heartbeat = (
        Duration::from_secs(60 * 60),
        Duration::from_secs(2 * 60 * 60),
    );
    let first = server
        .redis_as(&namespace, A)
        .with_heartbeat(heartbeat.0, heartbeat.1)
        .unwrap();
    let second = server
        .redis_as(&namespace, A)
        .with_heartbeat(heartbeat.0, heartbeat.1)
        .unwrap();

    first.open(4).await.unwrap();
    let mut connection = server.connection().await;
    let worker_key = key::worker(&namespace, A);
    let first_token = redis::cmd("HGET")
        .arg(&worker_key)
        .arg("token")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&worker_key)
        .arg("last_heartbeat")
        .arg("0")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    second.open(4).await.unwrap();
    let registration = redis::cmd("HGETALL")
        .arg(&worker_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_ne!(registration["token"], first_token);
    assert_eq!(registration["offline_time"], "");

    let stale_heartbeat = redis::Script::new(include_str!(
        "../../src/scheduler/redis/scripts/heartbeat.lua"
    ))
    .prepare_invoke()
    .key(&worker_key)
    .arg(A)
    .arg(&first_token)
    .invoke_async::<String>(&mut connection)
    .await
    .unwrap();
    assert_eq!(stale_heartbeat, "WORKER_TOKEN_MISMATCH");

    first.close().await.unwrap();
    let after_stale_close = redis::cmd("HGETALL")
        .arg(&worker_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(after_stale_close["token"], registration["token"]);
    assert_eq!(after_stale_close["offline_time"], "");

    second.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn heartbeat_failure_pauses_new_claims_until_it_recovers() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-heartbeat");
    let scheduler = server
        .redis_as(&namespace, A)
        .with_heartbeat(Duration::from_millis(50), Duration::from_millis(200))
        .unwrap();
    scheduler.open(4).await.unwrap();
    run::init(&scheduler).await;

    let worker_key = key::worker(&namespace, A);
    let mut connection = server.connection().await;
    let token = redis::cmd("HGET")
        .arg(&worker_key)
        .arg("token")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("DEL")
        .arg(&worker_key)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&worker_key)
        .arg("invalid")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "heartbeat-recovery",
            "https://example.com/heartbeat-recovery",
        )]))
        .await
        .unwrap();
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    redis::cmd("DEL")
        .arg(&worker_key)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&worker_key)
        .arg("worker_id")
        .arg(A)
        .arg("host")
        .arg("crawler-node-01")
        .arg("version")
        .arg("1.0.0")
        .arg("modes")
        .arg(r#"["http"]"#)
        .arg("concurrency")
        .arg(4)
        .arg("token")
        .arg(token)
        .arg("heartbeat_timeout")
        .arg(200)
        .arg("offline_time")
        .arg("")
        .arg("created_time")
        .arg(1)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Some(request) = scheduler.next_requests(1).await.unwrap().pop() {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("heartbeat did not recover");
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn timed_out_registration_cannot_claim_new_requests() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-timeout-claim");
    let scheduler = server
        .redis_as(&namespace, A)
        .with_heartbeat(Duration::from_secs(60), Duration::from_secs(120))
        .unwrap();
    scheduler.open(4).await.unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::worker(&namespace, A))
        .arg("last_heartbeat")
        .arg(0)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn corrupted_registration_is_not_reported_as_an_empty_claim() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };

    for (label, command, expected) in [
        ("missing", Corruption::Delete, "WORKER_NOT_FOUND"),
        ("type", Corruption::WrongType, "CORRUPT_WORKER_TYPE"),
        (
            "identity",
            Corruption::Set("worker_id", "other-worker"),
            "WORKER_ID_MISMATCH",
        ),
        (
            "missing-heartbeat",
            Corruption::Remove("last_heartbeat"),
            "CORRUPT_WORKER_HEARTBEAT",
        ),
        (
            "invalid-timeout",
            Corruption::Set("heartbeat_timeout", "0"),
            "CORRUPT_WORKER_HEARTBEAT",
        ),
        (
            "future-heartbeat",
            Corruption::Set("last_heartbeat", "9223372036854775807"),
            "CORRUPT_WORKER_HEARTBEAT",
        ),
        (
            "missing-offline",
            Corruption::Remove("offline_time"),
            "CORRUPT_WORKER_OFFLINE_TIME",
        ),
        (
            "invalid-offline",
            Corruption::Set("offline_time", "invalid"),
            "CORRUPT_WORKER_OFFLINE_TIME",
        ),
        (
            "missing-host",
            Corruption::Remove("host"),
            "CORRUPT_WORKER_METADATA",
        ),
        (
            "missing-version",
            Corruption::Remove("version"),
            "CORRUPT_WORKER_METADATA",
        ),
        (
            "missing-modes",
            Corruption::Remove("modes"),
            "CORRUPT_WORKER_METADATA",
        ),
        (
            "missing-concurrency",
            Corruption::Remove("concurrency"),
            "CORRUPT_WORKER_METADATA",
        ),
        (
            "missing-token",
            Corruption::Remove("token"),
            "CORRUPT_WORKER_METADATA",
        ),
        (
            "missing-created",
            Corruption::Remove("created_time"),
            "CORRUPT_WORKER_METADATA",
        ),
    ] {
        let namespace = server::namespace(label);
        let scheduler = server
            .redis_as(&namespace, A)
            .with_heartbeat(Duration::from_secs(60), Duration::from_secs(120))
            .unwrap();
        scheduler.open(4).await.unwrap();
        let worker_key = key::worker(&namespace, A);
        let mut connection = server.connection().await;
        command.apply(&mut connection, &worker_key).await;

        let error = scheduler.next_requests(1).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{label}: {error}");

        drop(scheduler);
        server.clear(&namespace).await;
    }
}

enum Corruption {
    Delete,
    WrongType,
    Remove(&'static str),
    Set(&'static str, &'static str),
}

impl Corruption {
    async fn apply(&self, connection: &mut redis::aio::MultiplexedConnection, key: &str) {
        match self {
            Self::Delete => {
                redis::cmd("DEL")
                    .arg(key)
                    .query_async::<usize>(connection)
                    .await
                    .unwrap();
            }
            Self::WrongType => {
                redis::cmd("DEL")
                    .arg(key)
                    .query_async::<usize>(&mut *connection)
                    .await
                    .unwrap();
                redis::cmd("SET")
                    .arg(key)
                    .arg("invalid")
                    .query_async::<String>(connection)
                    .await
                    .unwrap();
            }
            Self::Remove(field) => {
                redis::cmd("HDEL")
                    .arg(key)
                    .arg(field)
                    .query_async::<usize>(connection)
                    .await
                    .unwrap();
            }
            Self::Set(field, value) => {
                redis::cmd("HSET")
                    .arg(key)
                    .arg(field)
                    .arg(value)
                    .query_async::<usize>(connection)
                    .await
                    .unwrap();
            }
        }
    }
}

#[tokio::test]
async fn offline_registration_cannot_claim_new_requests() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("worker-offline-claim");
    let scheduler = server
        .redis_as(&namespace, A)
        .with_heartbeat(Duration::from_secs(60), Duration::from_secs(120))
        .unwrap();
    scheduler.open(4).await.unwrap();
    run::init(&scheduler).await;
    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "offline-claim",
            "https://example.com/offline-claim",
        )]))
        .await
        .unwrap();

    let worker_key = key::worker(&namespace, A);
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(&worker_key)
        .arg("offline_time")
        .arg("1")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());

    redis::cmd("HSET")
        .arg(&worker_key)
        .arg("offline_time")
        .arg("")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}
