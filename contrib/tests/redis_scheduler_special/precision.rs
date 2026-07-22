use std::collections::HashMap;

use spider::scheduler::Init;
use spider::{Scheduler, payload, stats, trace};

use super::common::{
    EXACT_INTEGER, HTTP, WORKER_A, completion_key, namespace, owned_request, processing_payload,
    request, request_key, scheduler, stats_key, succeed, success_payload, token,
};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn versions_and_stats_remain_exact_above_lua_safe_integers() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("integers");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .init(
            "integer-trace".to_string(),
            trace::Snapshot::code("integer-task"),
            vec![owned_request(
                "integer-request",
                "https://example.com/integer",
                "integer-task",
                "integer-trace",
            )],
        )
        .await
        .unwrap();
    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(request_key(&namespace, "integer-request"))
        .arg("version")
        .arg(EXACT_INTEGER - 1)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.version, EXACT_INTEGER);
    scheduler.ack(&processing_payload(&claimed)).await.unwrap();

    let mut success = success_payload(&claimed);
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
        .arg(stats_key(&namespace, "integer-trace"))
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
        .arg(completion_key(&namespace, "integer-request", EXACT_INTEGER))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(completion_exists, 1);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn fifo_sequence_remains_exact_above_lua_safe_integers() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("sequence");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();
    let mut connection = fixture.connection().await;
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
            .push(payload::Payload::new().requests(vec![request(id, url)]))
            .await
            .unwrap();
    }
    for (id, expected) in [("z", EXACT_INTEGER), ("a", EXACT_INTEGER + 1)] {
        let member = redis::cmd("HGET")
            .arg(request_key(&namespace, id))
            .arg("queue_member")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(member, format!("{expected:032}|{}", token(id)));
    }
    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(
        claimed
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        ["z", "a"]
    );
    for request in &claimed {
        succeed(&scheduler, request).await;
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn stats_overflow_fails_without_partial_settlement() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("stats-overflow");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();
    scheduler
        .init(
            "overflow-trace".to_string(),
            trace::Snapshot::code("overflow-task"),
            vec![owned_request(
                "overflow-request",
                "https://example.com/overflow",
                "overflow-task",
                "overflow-trace",
            )],
        )
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing_payload(&claimed)).await.unwrap();

    let mut connection = fixture.connection().await;
    let stats_key = stats_key(&namespace, "overflow-trace");
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

    let mut overflow = success_payload(&claimed);
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
        .arg(request_key(&namespace, "overflow-request"))
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let completion_exists = redis::cmd("EXISTS")
        .arg(completion_key(
            &namespace,
            "overflow-request",
            claimed.version,
        ))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(completion_exists, 0);

    scheduler.success(&success_payload(&claimed)).await.unwrap();
    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn request_version_overflow_records_a_terminal_failure() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("version-overflow");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "version-overflow-request",
            "https://example.com/version-overflow",
        )]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(request_key(&namespace, "version-overflow-request"))
        .arg("version")
        .arg(i64::MAX)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    let state = redis::cmd("HGET")
        .arg(request_key(&namespace, "version-overflow-request"))
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let error = redis::cmd("HGET")
        .arg(completion_key(
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
    fixture.clear(&namespace).await;
}
