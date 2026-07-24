use contrib::scheduler::redis::Redis;
use spider::scheduler::Init;
use spider::{Scheduler, payload, stats, trace};

use super::common::{
    HTTP, WORKER_A, WORKER_B, completion_key, namespace, owned_request, processing_payload,
    request, request_key, scheduler, stats_key, succeed, success_payload, token,
};
use crate::redis_fixture::Fixture;

#[tokio::test]
async fn redis_errors_distinguish_lifecycle_data_and_availability_failures() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let error_namespace = namespace("errors");
    let scheduler = scheduler(&fixture, &error_namespace);

    let unopened = scheduler.trace("trace").await.unwrap_err();
    assert!(!unopened.is_transient());
    scheduler.open().await.unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("SET")
        .arg(format!("{error_namespace}:traces"))
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    let wrong_type = scheduler.trace("trace").await.unwrap_err();
    assert!(
        !wrong_type.is_transient(),
        "WRONGTYPE is a deterministic backend data error: {wrong_type}"
    );
    scheduler.close().await.unwrap();

    let unavailable = Redis::new("redis://127.0.0.1:1")
        .unwrap()
        .with_namespace(namespace("unavailable"))
        .unwrap()
        .open()
        .await
        .unwrap_err();
    assert!(unavailable.is_transient());

    fixture.clear(&error_namespace).await;
}

#[tokio::test]
async fn malformed_records_do_not_discard_valid_claims_from_the_same_call() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("malformed");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut broken_trace = owned_request(
        "broken-trace",
        "https://example.com/broken-trace",
        "broken-task",
        "broken-trace-id",
    );
    broken_trace.priority = 30;
    broken_trace.max_retry_count = 1;
    scheduler
        .init(
            "broken-trace-id".to_string(),
            trace::Snapshot::code("broken-task"),
            vec![broken_trace],
        )
        .await
        .unwrap();
    let mut broken_request = request("broken-request", "https://example.com/broken-request");
    broken_request.priority = 20;
    broken_request.max_retry_count = 1;
    let mut broken_state = request("broken-state", "https://example.com/broken-state");
    broken_state.priority = 25;
    broken_state.max_retry_count = 1;
    let mut valid = request("valid-request", "https://example.com/valid-request");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![broken_state, broken_request, valid]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(format!("{namespace}:traces"))
        .arg("broken-trace-id")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(request_key(&namespace, "broken-request"))
        .arg("snapshot")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(request_key(&namespace, "broken-state"))
        .arg("state")
        .arg("unknown")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(4, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-request");
    succeed(&scheduler, &claimed[0]).await;
    for id in ["broken-trace", "broken-state", "broken-request"] {
        let state = redis::cmd("HGET")
            .arg(request_key(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, "failed");
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn failed_recovery_does_not_withhold_a_valid_claim() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("failed-recovery-valid-claim");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("failed-recovery", "https://example.com/failed-recovery");
    damaged.priority = 20;
    damaged.max_retry_count = 2;
    let mut valid = request(
        "valid-after-recovery",
        "https://example.com/valid-after-recovery",
    );
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(request_key(&namespace, "failed-recovery"))
        .arg("snapshot")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(format!("{namespace}:meta"))
        .arg("enqueue_sequence")
        .arg("99999999999999999999999999999999")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-after-recovery");
    succeed(&scheduler, &claimed[0]).await;

    let damaged_state: String = redis::cmd("HGET")
        .arg(request_key(&namespace, "failed-recovery"))
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(damaged_state, "processing");

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn push_rejects_a_trace_whose_snapshot_was_removed() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("missing-trace");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();
    scheduler
        .init(
            "missing-trace".to_string(),
            trace::Snapshot::code("missing-task"),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    redis::cmd("HDEL")
        .arg(format!("{namespace}:traces"))
        .arg("missing-trace")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let request = owned_request(
        "missing-trace-request",
        "https://example.com/missing-trace",
        "missing-task",
        "missing-trace",
    );
    let error = scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap_err();
    assert!(matches!(error, spider::scheduler::Error::TraceNotFound(id) if id == "missing-trace"));
    let exists = redis::cmd("EXISTS")
        .arg(request_key(&namespace, "missing-trace-request"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn wrong_type_settlement_indices_do_not_partially_settle() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("settlement-types");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "wrong-type-success",
            "https://example.com/wrong-type-success",
        )]))
        .await
        .unwrap();
    let succeeded = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler
        .ack(&processing_payload(&succeeded))
        .await
        .unwrap();
    let mut success = success_payload(&succeeded);
    success.stats.insert(
        "parse".to_string(),
        serde_json::to_value(stats::Counter {
            total: 1,
            ..Default::default()
        })
        .unwrap(),
    );

    let processing = super::common::processing_key(&namespace, "http");
    let mut connection = fixture.connection().await;
    redis::cmd("DEL")
        .arg(&processing)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&processing)
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.success(&success).await.is_err());
    assert_settlement_is_unchanged(
        &mut connection,
        &namespace,
        &succeeded,
        &stats_key(&namespace, ""),
    )
    .await;
    let kind = redis::cmd("TYPE")
        .arg(&processing)
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(kind, "string");

    redis::cmd("DEL")
        .arg(&processing)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    scheduler.success(&success).await.unwrap();

    let mut retry = request(
        "wrong-type-failure",
        "https://example.com/wrong-type-failure",
    );
    retry.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![retry]))
        .await
        .unwrap();
    let failed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing_payload(&failed)).await.unwrap();
    let mut failure =
        payload::Payload::for_request(&failed, failed.leased_by.clone()).failed("failed");
    failure.start_time = Some(1);
    failure.end_time = Some(2);

    let ready = format!("{namespace}:queue:http:ready");
    redis::cmd("DEL")
        .arg(&ready)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(&ready)
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    assert!(scheduler.failure(&failure).await.is_err());
    assert_settlement_is_unchanged(&mut connection, &namespace, &failed, "").await;
    let active = redis::cmd("ZSCORE")
        .arg(&processing)
        .arg(token(&failed.id))
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(active.is_some());
    let failed_workers = format!("{namespace}:request:{}:failed_workers", token(&failed.id));
    let failed_workers_exists = redis::cmd("EXISTS")
        .arg(&failed_workers)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(failed_workers_exists, 0);

    redis::cmd("DEL")
        .arg(&ready)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    scheduler.failure(&failure).await.unwrap();
    let retried = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    succeed(&scheduler, &retried).await;

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn claim_recovery_uses_the_original_queue_token_not_the_stored_id() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("claim-token");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("token-a", "https://example.com/token-a");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let mut valid = request("token-b", "https://example.com/token-b");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    let replacement = redis::cmd("HGET")
        .arg(request_key(&namespace, "token-b"))
        .arg("snapshot")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(request_key(&namespace, "token-a"))
        .arg("id")
        .arg("token-b")
        .arg("snapshot")
        .arg(replacement)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(completion_key(&namespace, "token-a", 1))
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "token-b");
    succeed(&scheduler, &claimed[0]).await;

    for (id, expected) in [("token-a", "failed"), ("token-b", "done")] {
        let state = redis::cmd("HGET")
            .arg(request_key(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, expected);
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_expired_lease_does_not_block_valid_claims() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("corrupt-expired-lease");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("expired-damaged", "https://example.com/expired-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request("expired-valid", "https://example.com/expired-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "expired-damaged");

    let mut connection = fixture.connection().await;
    redis::cmd("HSET")
        .arg(request_key(&namespace, "expired-damaged"))
        .arg("priority")
        .arg("not-an-integer")
        .arg("lease_time")
        .arg("0")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(super::common::processing_key(&namespace, "http"))
        .arg(0)
        .arg(token("expired-damaged"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let valid = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(valid.id, "expired-valid");
    succeed(&scheduler, &valid).await;

    let key = request_key(&namespace, "expired-damaged");
    for (field, expected) in [("state", "failed"), ("leased_by", ""), ("lease_time", "0")] {
        let value = redis::cmd("HGET")
            .arg(&key)
            .arg(field)
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(value, expected, "unexpected {field}");
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_ready_members_are_quarantined_without_blocking_valid_claims() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("corrupt-ready-member");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("ready-damaged", "https://example.com/ready-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request("ready-valid", "https://example.com/ready-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let ready = format!("{namespace}:queue:http:ready");
    let key = request_key(&namespace, "ready-damaged");
    let malformed = format!("not-a-sequence|{}", token("ready-damaged"));
    let mut connection = fixture.connection().await;
    let stored = redis::cmd("HGET")
        .arg(&key)
        .arg("queue_member")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZREM")
        .arg(&ready)
        .arg(stored)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(&key)
        .arg("queue_member")
        .arg(&malformed)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&ready)
        .arg(-20)
        .arg(&malformed)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "ready-valid");
    succeed(&scheduler, &claimed).await;

    let state = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "failed");
    let exists = redis::cmd("ZSCORE")
        .arg(&ready)
        .arg(malformed)
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(exists.is_none());

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn stray_delayed_members_preserve_their_valid_ready_request() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("stray-delayed-member");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![request(
            "stray-delayed",
            "https://example.com/stray-delayed",
        )]))
        .await
        .unwrap();

    let ready = format!("{namespace}:queue:http:ready");
    let delayed = format!("{namespace}:queue:http:delayed");
    let member = format!("9999999999999999999|{:032}|{}", 1, token("stray-delayed"));
    let mut connection = fixture.connection().await;
    redis::cmd("ZADD")
        .arg(&delayed)
        .arg(0)
        .arg(&member)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "stray-delayed");
    succeed(&scheduler, &claimed).await;
    let stale = redis::cmd("ZSCORE")
        .arg(&delayed)
        .arg(member)
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(stale.is_none());
    let ready_count = redis::cmd("ZCARD")
        .arg(ready)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(ready_count, 0);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_queue_pointers_cannot_remove_another_request() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("corrupt-queue-pointer");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("pointer-a", "https://example.com/pointer-a");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request("pointer-b", "https://example.com/pointer-b");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = fixture.connection().await;
    let valid_member = redis::cmd("HGET")
        .arg(request_key(&namespace, "pointer-b"))
        .arg("queue_member")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(request_key(&namespace, "pointer-a"))
        .arg("queue_member")
        .arg(valid_member)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "pointer-b");
    succeed(&scheduler, &claimed[0]).await;

    for (id, expected) in [("pointer-a", "failed"), ("pointer-b", "done")] {
        let state = redis::cmd("HGET")
            .arg(request_key(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, expected);
    }

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn mismatched_processing_scores_are_repaired_without_blocking_work() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("mismatched-lease-score");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let mut damaged = request("lease-damaged", "https://example.com/lease-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request("lease-valid", "https://example.com/lease-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "lease-damaged");

    let key = request_key(&namespace, "lease-damaged");
    let processing = super::common::processing_key(&namespace, "http");
    let mut connection = fixture.connection().await;
    let lease_time = redis::cmd("HGET")
        .arg(&key)
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&processing)
        .arg(9_007_199_254_740_000_i64)
        .arg(token("lease-damaged"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let valid = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(valid.id, "lease-valid");
    succeed(&scheduler, &valid).await;

    let state = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let score = redis::cmd("ZSCORE")
        .arg(&processing)
        .arg(token("lease-damaged"))
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(score.map(|score| score as i64), Some(lease_time));
    succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn malformed_future_delayed_members_do_not_keep_the_scheduler_pending() {
    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("malformed-future-delayed");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let delayed = format!("{namespace}:queue:http:delayed");
    let malformed = "zzzz-not-a-delayed-member";
    let mut connection = fixture.connection().await;
    redis::cmd("ZADD")
        .arg(&delayed)
        .arg(0)
        .arg(malformed)
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
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    let remaining = redis::cmd("ZCARD")
        .arg(&delayed)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(remaining, 0);

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

#[tokio::test]
async fn claim_bounds_expired_lease_recovery() {
    const RECOVERY_LIMIT: usize = 64;

    let Some(fixture) = Fixture::connect().await else {
        return;
    };
    let namespace = namespace("lease-recovery-bound");
    let scheduler = scheduler(&fixture, &namespace);
    scheduler.open().await.unwrap();

    let requests = (0..=RECOVERY_LIMIT)
        .map(|index| request(&format!("expired-{index}"), "https://example.com/expired"))
        .collect::<Vec<_>>();
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(RECOVERY_LIMIT + 1, WORKER_A, HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), RECOVERY_LIMIT + 1);

    let mut connection = fixture.connection().await;
    let processing = super::common::processing_key(&namespace, "http");
    for request in &claimed {
        redis::cmd("HSET")
            .arg(request_key(&namespace, &request.id))
            .arg("lease_time")
            .arg("0")
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
        redis::cmd("ZADD")
            .arg(&processing)
            .arg(0)
            .arg(token(&request.id))
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
    }

    let next = scheduler.next_requests(1, WORKER_B, HTTP).await.unwrap();
    assert_eq!(next.len(), 1);
    let remaining = redis::cmd("ZCARD")
        .arg(&processing)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        remaining, 2,
        "one old lease and one newly claimed lease remain"
    );

    scheduler.close().await.unwrap();
    fixture.clear(&namespace).await;
}

async fn assert_settlement_is_unchanged(
    connection: &mut redis::aio::MultiplexedConnection,
    namespace: &str,
    request: &spider::net::Request,
    stats: &str,
) {
    let state = redis::cmd("HGET")
        .arg(request_key(namespace, &request.id))
        .arg("state")
        .query_async::<String>(&mut *connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let completion_exists = redis::cmd("EXISTS")
        .arg(completion_key(namespace, &request.id, request.version))
        .query_async::<usize>(&mut *connection)
        .await
        .unwrap();
    assert_eq!(completion_exists, 0);
    if !stats.is_empty() {
        let stats_exists = redis::cmd("EXISTS")
            .arg(stats)
            .query_async::<usize>(&mut *connection)
            .await
            .unwrap();
        assert_eq!(stats_exists, 0);
    }
}
