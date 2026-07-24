use std::collections::HashMap;

use spider::scheduler::Init;
use spider::{Scheduler, payload, stats, trace};

use super::{key, request, server, settlement, worker};

const EXACT_INTEGER: i64 = 9_007_199_254_740_993;

#[tokio::test]
async fn versions_and_stats_remain_exact_above_lua_safe_integers() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("integers");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();

    scheduler
        .init(
            "integer-trace".to_string(),
            trace::Snapshot::code("integer-task"),
            vec![request::for_trace(
                "integer-request",
                "https://example.com/integer",
                "integer-task",
                "integer-trace",
            )],
        )
        .await
        .unwrap();
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "integer-request"))
        .arg("version")
        .arg(EXACT_INTEGER - 1)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.version, EXACT_INTEGER);
    scheduler
        .ack(&settlement::processing(&claimed))
        .await
        .unwrap();

    let mut success = settlement::success(&claimed);
    let counter = stats::Counter {
        total: EXACT_INTEGER,
        done: EXACT_INTEGER + 1,
        filter: EXACT_INTEGER + 2,
        dedup: EXACT_INTEGER + 3,
        validate: EXACT_INTEGER + 4,
        download: EXACT_INTEGER + 5,
    };
    success.stats.insert(
        "requests".to_string(),
        serde_json::to_value(counter).unwrap(),
    );
    scheduler.success(&success).await.unwrap();

    let stored = redis::cmd("HGETALL")
        .arg(key::stats(&namespace, "integer-trace"))
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    for (field, expected) in [
        ("total", EXACT_INTEGER),
        ("done", EXACT_INTEGER + 1),
        ("filter", EXACT_INTEGER + 2),
        ("dedup", EXACT_INTEGER + 3),
        ("validate", EXACT_INTEGER + 4),
        ("download", EXACT_INTEGER + 5),
    ] {
        assert_eq!(stored[&format!("requests.{field}")], expected.to_string());
    }
    let completion_exists = redis::cmd("EXISTS")
        .arg(key::completion(
            &namespace,
            "integer-request",
            EXACT_INTEGER,
        ))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(completion_exists, 1);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn fifo_sequence_remains_exact_above_lua_safe_integers() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("sequence");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();
    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(format!("{namespace}:meta"))
        .arg("enqueue_sequence")
        .arg(EXACT_INTEGER - 1)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    for (id, url) in [
        ("z", "https://example.com/sequence/first"),
        ("a", "https://example.com/sequence/second"),
    ] {
        scheduler
            .push(payload::Payload::new().requests(vec![request::new(id, url)]))
            .await
            .unwrap();
    }
    for (id, expected) in [("z", EXACT_INTEGER), ("a", EXACT_INTEGER + 1)] {
        let member = redis::cmd("HGET")
            .arg(key::request(&namespace, id))
            .arg("queue_member")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(member, format!("{expected:032}|{}", key::token(id)));
    }
    let claimed = scheduler
        .next_requests(2, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
    for request in &claimed {
        settlement::succeed(&scheduler, request).await;
    }

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn stats_overflow_fails_without_partial_settlement() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("stats-overflow");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();
    scheduler
        .init(
            "overflow-trace".to_string(),
            trace::Snapshot::code("overflow-task"),
            vec![request::for_trace(
                "overflow-request",
                "https://example.com/overflow",
                "overflow-task",
                "overflow-trace",
            )],
        )
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler
        .ack(&settlement::processing(&claimed))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let stats_key = key::stats(&namespace, "overflow-trace");
    redis::cmd("HSET")
        .arg(&stats_key)
        .arg("parse.total")
        .arg(41)
        .arg("parse.done")
        .arg(i64::MAX)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    let before = redis::cmd("HGETALL")
        .arg(&stats_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();

    let mut overflow = settlement::success(&claimed);
    overflow.stats.insert(
        "parse".to_string(),
        serde_json::to_value(stats::Counter {
            total: 3,
            done: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let error = scheduler.success(&overflow).await.unwrap_err();
    assert!(!error.is_transient());

    let after = redis::cmd("HGETALL")
        .arg(&stats_key)
        .query_async::<HashMap<String, String>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(after, before);
    let state = redis::cmd("HGET")
        .arg(key::request(&namespace, "overflow-request"))
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let completion_exists = redis::cmd("EXISTS")
        .arg(key::completion(
            &namespace,
            "overflow-request",
            claimed.version,
        ))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(completion_exists, 0);

    scheduler
        .success(&settlement::success(&claimed))
        .await
        .unwrap();
    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn request_version_overflow_records_a_terminal_failure() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("version-overflow");
    let scheduler = server.redis(&namespace);
    scheduler.open().await.unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "version-overflow-request",
            "https://example.com/version-overflow",
        )]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "version-overflow-request"))
        .arg("version")
        .arg(i64::MAX)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(1, worker::A, worker::HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let state = redis::cmd("HGET")
        .arg(key::request(&namespace, "version-overflow-request"))
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let error = redis::cmd("HGET")
        .arg(key::completion(
            &namespace,
            "version-overflow-request",
            i64::MAX,
        ))
        .arg("error")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(error, "request version overflow while claiming");

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}
