use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use contrib::scheduler::redis::Redis;
use spider::scheduler::Init;
use spider::{Scheduler, payload, stats, trace};
use tracing::instrument::WithSubscriber;

use super::{key, request, server, settlement, worker};

#[derive(Clone)]
struct Events {
    values: Arc<Mutex<Vec<String>>>,
    next_span: Arc<AtomicU64>,
}

impl Events {
    fn new(values: Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            values,
            next_span: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl tracing::Subscriber for Events {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(self.next_span.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        struct Visitor(String);

        impl tracing::field::Visit for Visitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                use std::fmt::Write;
                let _ = write!(&mut self.0, " {}={value:?}", field.name());
            }
        }

        let mut visitor = Visitor(String::new());
        event.record(&mut visitor);
        self.values.lock().unwrap().push(visitor.0);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test]
async fn lifecycle_data_and_availability_failures_are_distinct() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let error_namespace = server::namespace("errors");
    let scheduler = server.redis(&error_namespace);

    let unopened = scheduler.trace("trace").await.unwrap_err();
    assert!(!unopened.is_transient());
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut connection = server.connection().await;
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
        .with_namespace(server::namespace("unavailable"))
        .unwrap()
        .open()
        .await
        .unwrap_err();
    assert!(unavailable.is_transient());

    server.clear(&error_namespace).await;
}

#[tokio::test]
async fn malformed_records_do_not_discard_valid_claims_from_the_same_call() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("malformed");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut broken_trace = request::for_trace(
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
    let mut broken_request = request::new("broken-request", "https://example.com/broken-request");
    broken_request.priority = 20;
    broken_request.max_retry_count = 1;
    let mut broken_state = request::new("broken-state", "https://example.com/broken-state");
    broken_state.priority = 25;
    broken_state.max_retry_count = 1;
    let mut valid = request::new("valid-request", "https://example.com/valid-request");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![broken_state, broken_request, valid]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(format!("{namespace}:traces"))
        .arg("broken-trace-id")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(key::request(&namespace, "broken-request"))
        .arg("snapshot")
        .arg("{")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(key::request(&namespace, "broken-state"))
        .arg("state")
        .arg("unknown")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(4, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-request");
    settlement::succeed(&scheduler, &claimed[0]).await;
    for id in ["broken-trace", "broken-state", "broken-request"] {
        let state = redis::cmd("HGET")
            .arg(key::request(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, "failed");
    }

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn failed_recovery_does_not_withhold_a_valid_claim() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("failed-recovery-valid-claim");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("failed-recovery", "https://example.com/failed-recovery");
    damaged.priority = 20;
    damaged.max_retry_count = 2;
    let mut valid = request::new(
        "valid-after-recovery",
        "https://example.com/valid-after-recovery",
    );
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "failed-recovery"))
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

    let events = Arc::new(Mutex::new(Vec::new()));
    let claimed = scheduler
        .next_requests(2, worker::A, worker::HTTP)
        .with_subscriber(Events::new(Arc::clone(&events)))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "valid-after-recovery");
    settlement::succeed(&scheduler, &claimed[0]).await;

    {
        let events = events.lock().unwrap();
        let warning = events
            .iter()
            .find(|event| event.contains("failed to recover damaged Redis Request"))
            .expect("failed recovery must be observable");
        for value in [
            "request_id",
            "failed-recovery",
            "token",
            &key::token("failed-recovery"),
            "version",
            "worker_id",
            worker::A,
            "error",
        ] {
            assert!(warning.contains(value), "missing {value} in {warning}");
        }
    }

    let damaged_state: String = redis::cmd("HGET")
        .arg(key::request(&namespace, "failed-recovery"))
        .arg("state")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(damaged_state, "processing");

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn push_rejects_a_trace_whose_snapshot_was_removed() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("missing-trace");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;
    scheduler
        .init(
            "missing-trace".to_string(),
            trace::Snapshot::code("missing-task"),
            Vec::new(),
        )
        .await
        .unwrap();

    let mut connection = server.connection().await;
    redis::cmd("HDEL")
        .arg(format!("{namespace}:traces"))
        .arg("missing-trace")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let request = request::for_trace(
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
        .arg(key::request(&namespace, "missing-trace-request"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    assert_eq!(exists, 0);

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn wrong_type_settlement_indices_do_not_partially_settle() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("settlement-types");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "wrong-type-success",
            "https://example.com/wrong-type-success",
        )]))
        .await
        .unwrap();
    let succeeded = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler
        .ack(&settlement::processing(&succeeded))
        .await
        .unwrap();
    let mut success = settlement::success(&succeeded);
    success.stats.insert(
        "parse".to_string(),
        serde_json::to_value(stats::Counter {
            total: 1,
            ..Default::default()
        })
        .unwrap(),
    );

    let processing = key::processing(&namespace, "http");
    let mut connection = server.connection().await;
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
        &key::stats(&namespace, ""),
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

    let mut retry = request::new(
        "wrong-type-failure",
        "https://example.com/wrong-type-failure",
    );
    retry.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![retry]))
        .await
        .unwrap();
    let failed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler
        .ack(&settlement::processing(&failed))
        .await
        .unwrap();
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
        .arg(key::token(&failed.id))
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert!(active.is_some());
    let failed_workers = format!(
        "{namespace}:request:{}:failed_workers",
        key::token(&failed.id)
    );
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
        .next_requests(1, worker::B, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    settlement::succeed(&scheduler, &retried).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn claim_recovery_uses_the_original_queue_token_not_the_stored_id() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("claim-token");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("token-a", "https://example.com/token-a");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let mut valid = request::new("token-b", "https://example.com/token-b");
    valid.priority = 10;
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let replacement = redis::cmd("HGET")
        .arg(key::request(&namespace, "token-b"))
        .arg("snapshot")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(key::request(&namespace, "token-a"))
        .arg("id")
        .arg("token-b")
        .arg("snapshot")
        .arg(replacement)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("SET")
        .arg(key::completion(&namespace, "token-a", 1))
        .arg("wrong-type")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(2, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "token-b");
    settlement::succeed(&scheduler, &claimed[0]).await;

    for (id, expected) in [("token-a", "failed"), ("token-b", "done")] {
        let state = redis::cmd("HGET")
            .arg(key::request(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, expected);
    }

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_expired_lease_does_not_block_valid_claims() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("corrupt-expired-lease");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("expired-damaged", "https://example.com/expired-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request::new("expired-valid", "https://example.com/expired-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "expired-damaged");

    let mut connection = server.connection().await;
    redis::cmd("HSET")
        .arg(key::request(&namespace, "expired-damaged"))
        .arg("priority")
        .arg("not-an-integer")
        .arg("lease_time")
        .arg("0")
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(key::processing(&namespace, "http"))
        .arg(0)
        .arg(key::token("expired-damaged"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let valid = scheduler
        .next_requests(1, worker::B, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(valid.id, "expired-valid");
    settlement::succeed(&scheduler, &valid).await;

    let key = key::request(&namespace, "expired-damaged");
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
    server.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_ready_members_are_quarantined_without_blocking_valid_claims() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("corrupt-ready-member");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("ready-damaged", "https://example.com/ready-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request::new("ready-valid", "https://example.com/ready-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let ready = format!("{namespace}:queue:http:ready");
    let key = key::request(&namespace, "ready-damaged");
    let malformed = format!("not-a-sequence|{}", key::token("ready-damaged"));
    let mut connection = server.connection().await;
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
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "ready-valid");
    settlement::succeed(&scheduler, &claimed).await;

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
    server.clear(&namespace).await;
}

#[tokio::test]
async fn stray_delayed_members_preserve_their_valid_ready_request() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("stray-delayed-member");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    scheduler
        .push(payload::Payload::new().requests(vec![request::new(
            "stray-delayed",
            "https://example.com/stray-delayed",
        )]))
        .await
        .unwrap();

    let ready = format!("{namespace}:queue:http:ready");
    let delayed = format!("{namespace}:queue:http:delayed");
    let member = format!(
        "9999999999999999999|{:032}|{}",
        1,
        key::token("stray-delayed")
    );
    let mut connection = server.connection().await;
    redis::cmd("ZADD")
        .arg(&delayed)
        .arg(0)
        .arg(&member)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "stray-delayed");
    settlement::succeed(&scheduler, &claimed).await;
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
    server.clear(&namespace).await;
}

#[tokio::test]
async fn corrupt_queue_pointers_cannot_remove_another_request() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("corrupt-queue-pointer");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("pointer-a", "https://example.com/pointer-a");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request::new("pointer-b", "https://example.com/pointer-b");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();

    let mut connection = server.connection().await;
    let valid_member = redis::cmd("HGET")
        .arg(key::request(&namespace, "pointer-b"))
        .arg("queue_member")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    redis::cmd("HSET")
        .arg(key::request(&namespace, "pointer-a"))
        .arg("queue_member")
        .arg(valid_member)
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let claimed = scheduler
        .next_requests(2, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, "pointer-b");
    settlement::succeed(&scheduler, &claimed[0]).await;

    for (id, expected) in [("pointer-a", "failed"), ("pointer-b", "done")] {
        let state = redis::cmd("HGET")
            .arg(key::request(&namespace, id))
            .arg("state")
            .query_async::<String>(&mut connection)
            .await
            .unwrap();
        assert_eq!(state, expected);
    }

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn mismatched_processing_scores_are_repaired_without_blocking_work() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("mismatched-lease-score");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let mut damaged = request::new("lease-damaged", "https://example.com/lease-damaged");
    damaged.priority = 20;
    damaged.max_retry_count = 1;
    let valid = request::new("lease-valid", "https://example.com/lease-valid");
    scheduler
        .push(payload::Payload::new().requests(vec![damaged, valid]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, worker::A, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(claimed.id, "lease-damaged");

    let key = key::request(&namespace, "lease-damaged");
    let processing = key::processing(&namespace, "http");
    let mut connection = server.connection().await;
    let lease_time = redis::cmd("HGET")
        .arg(&key)
        .arg("lease_time")
        .query_async::<i64>(&mut connection)
        .await
        .unwrap();
    redis::cmd("ZADD")
        .arg(&processing)
        .arg(9_007_199_254_740_000_i64)
        .arg(key::token("lease-damaged"))
        .query_async::<usize>(&mut connection)
        .await
        .unwrap();

    let valid = scheduler
        .next_requests(1, worker::B, worker::HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(valid.id, "lease-valid");
    settlement::succeed(&scheduler, &valid).await;

    let state = redis::cmd("HGET")
        .arg(&key)
        .arg("state")
        .query_async::<String>(&mut connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let score = redis::cmd("ZSCORE")
        .arg(&processing)
        .arg(key::token("lease-damaged"))
        .query_async::<Option<f64>>(&mut connection)
        .await
        .unwrap();
    assert_eq!(score.map(|score| score as i64), Some(lease_time));
    settlement::succeed(&scheduler, &claimed).await;

    scheduler.close().await.unwrap();
    server.clear(&namespace).await;
}

#[tokio::test]
async fn malformed_future_delayed_members_do_not_keep_the_scheduler_pending() {
    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("malformed-future-delayed");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let delayed = format!("{namespace}:queue:http:delayed");
    let malformed = "zzzz-not-a-delayed-member";
    let mut connection = server.connection().await;
    redis::cmd("ZADD")
        .arg(&delayed)
        .arg(0)
        .arg(malformed)
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
    assert!(
        !scheduler
            .has_pending_requests(worker::A, worker::HTTP)
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
    server.clear(&namespace).await;
}

#[tokio::test]
async fn claim_bounds_expired_lease_recovery() {
    const RECOVERY_LIMIT: usize = 64;

    let Some(server) = server::Handle::connect().await else {
        return;
    };
    let namespace = server::namespace("lease-recovery-bound");
    let scheduler = server.redis(&namespace);
    server::open(&scheduler).await;
    super::run::init(&scheduler).await;

    let requests = (0..=RECOVERY_LIMIT)
        .map(|index| request::new(&format!("expired-{index}"), "https://example.com/expired"))
        .collect::<Vec<_>>();
    scheduler
        .push(payload::Payload::new().requests(requests))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(RECOVERY_LIMIT + 1, worker::A, worker::HTTP)
        .await
        .unwrap();
    assert_eq!(claimed.len(), RECOVERY_LIMIT + 1);

    let mut connection = server.connection().await;
    let processing = key::processing(&namespace, "http");
    for request in &claimed {
        redis::cmd("HSET")
            .arg(key::request(&namespace, &request.id))
            .arg("lease_time")
            .arg("0")
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
        redis::cmd("ZADD")
            .arg(&processing)
            .arg(0)
            .arg(key::token(&request.id))
            .query_async::<usize>(&mut connection)
            .await
            .unwrap();
    }

    let next = scheduler
        .next_requests(1, worker::B, worker::HTTP)
        .await
        .unwrap();
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
    server.clear(&namespace).await;
}

async fn assert_settlement_is_unchanged(
    connection: &mut redis::aio::MultiplexedConnection,
    namespace: &str,
    request: &spider::net::Request,
    stats: &str,
) {
    let state = redis::cmd("HGET")
        .arg(key::request(namespace, &request.id))
        .arg("state")
        .query_async::<String>(&mut *connection)
        .await
        .unwrap();
    assert_eq!(state, "processing");
    let completion_exists = redis::cmd("EXISTS")
        .arg(key::completion(namespace, &request.id, request.version))
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
