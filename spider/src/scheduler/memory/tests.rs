use std::sync::Arc;

use super::*;
use crate::scheduler::{Init, Scheduler};

#[test]
#[should_panic(expected = "Memory worker_id must not be empty")]
fn empty_worker_identity_is_rejected_during_construction() {
    let _scheduler = Memory::new("   ");
}

fn rules_config(id: &str, node: &str) -> crate::config::Config {
    crate::config::Config::from_yaml(&format!(
        r#"
spider:
  name: {id}
  start:
    - node: {node}
      url: https://example.com
graph:
  nodes:
    {node}: {{}}
  edges: []
"#
    ))
    .unwrap()
}

#[tokio::test]
async fn push_and_claim_moves_request_to_processing() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    let payload =
        payload::Payload::for_request(&request, "worker-1").requests(vec![request.clone()]);

    scheduler.push(payload).await.unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(claimed[0].leased_by, "worker-1");
}

#[tokio::test]
async fn push_rejects_invalid_collection_without_partial_enqueue() {
    let scheduler = Memory::new("worker-1");
    let mut valid = net::Request::follow("https://example.com/valid").unwrap();
    valid.task_id = "task-1".to_string();
    let mut invalid = net::Request::follow("https://example.com/invalid").unwrap();
    invalid.task_id = "task-2".to_string();

    let result = scheduler
        .push({
            let mut payload = payload::Payload::new().requests(vec![valid, invalid]);
            payload.task_id = "task-1".to_string();
            payload
        })
        .await;

    assert!(result.is_err());
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn push_rejects_duplicate_request_ids_atomically() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    let result = scheduler
        .push(payload::Payload::new().requests(vec![request.clone(), request]))
        .await;
    assert!(result.is_err());
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn push_rejects_request_id_already_queued() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    let result = scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await;
    assert!(result.is_err());
    assert_eq!(scheduler.queued_len(), 1);
}

#[test]
fn concurrent_push_accepts_one_request_per_id() {
    const THREADS: usize = 32;

    let scheduler = Arc::new(Memory::new("worker-1"));
    let request = net::Request::follow("https://example.com/concurrent").unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(THREADS));
    let handles = (0..THREADS)
        .map(|_| {
            let scheduler = scheduler.clone();
            let request = request.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                barrier.wait();
                runtime.block_on(scheduler.push(payload::Payload::new().requests(vec![request])))
            })
        })
        .collect::<Vec<_>>();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert!(
        results
            .iter()
            .filter(|result| result.is_err())
            .all(|error| {
                error
                    .as_ref()
                    .is_err_and(|error| error.to_string().contains("request id already exists"))
            })
    );
    assert_eq!(scheduler.queued_len(), 1);
}

#[tokio::test]
async fn push_rejects_request_id_while_processing_or_terminal() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let processing_result = scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await;
    assert!(processing_result.is_err());

    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
    let terminal_result = scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await;
    assert!(terminal_result.is_err());
}

#[tokio::test]
async fn expired_acknowledged_request_is_reclaimed_with_a_new_version() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    {
        let mut state = scheduler.state();
        state.processing.get_mut(&claimed.id).unwrap().lease_time = 1;
    }

    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(reclaimed.id, claimed.id);
    assert_eq!(reclaimed.version, claimed.version + 1);
    assert_eq!(reclaimed.retry_count, 1);
    assert_eq!(reclaimed.failed_workers, ["worker-1"]);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&claimed.id].version,
        reclaimed.version
    );

    let mut stale = payload::Payload::for_request(&claimed, "worker-1");
    stale.start_time = Some(1);
    stale.end_time = Some(2);
    assert!(scheduler.success(&stale).await.is_err());

    let mut ack = payload::Payload::for_request(&reclaimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&reclaimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
}

#[tokio::test]
async fn expired_unacknowledged_claim_does_not_consume_an_attempt() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .lease_time = 1;

    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    assert_eq!(reclaimed.retry_count, 0);
    assert!(reclaimed.failed_workers.is_empty());
    assert_eq!(reclaimed.version, claimed.version + 1);
}

#[tokio::test]
async fn execution_operations_reject_identity_mismatch_without_mutation() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    for field in ["task_id", "trace_id", "node", "worker_id", "version"] {
        let mut ack = payload::Payload::for_request(&claimed, "worker-1");
        ack.state = net::State::Processing;
        match field {
            "task_id" => ack.task_id = "other-task".to_string(),
            "trace_id" => ack.trace_id = "other-trace".to_string(),
            "node" => ack.node = "other-node".to_string(),
            "worker_id" => ack.worker_id = "other-worker".to_string(),
            "version" => ack.version += 1,
            _ => unreachable!(),
        }
        assert!(scheduler.ack(&ack).await.is_err(), "field: {field}");
    }
    assert_eq!(scheduler.processing_len(), 1);

    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();

    for field in ["task_id", "trace_id", "node", "worker_id", "version"] {
        let mut refresh = payload::Payload::for_request(&claimed, "worker-1");
        refresh.state = net::State::Processing;
        match field {
            "task_id" => refresh.task_id = "other-task".to_string(),
            "trace_id" => refresh.trace_id = "other-trace".to_string(),
            "node" => refresh.node = "other-node".to_string(),
            "worker_id" => refresh.worker_id = "other-worker".to_string(),
            "version" => refresh.version += 1,
            _ => unreachable!(),
        }
        assert!(
            scheduler.refresh_lease(&refresh).await.is_err(),
            "field: {field}"
        );
    }

    for field in ["task_id", "trace_id", "node", "worker_id", "version"] {
        let mut success = payload::Payload::for_request(&claimed, "worker-1");
        success.start_time = Some(1);
        success.end_time = Some(2);
        match field {
            "task_id" => success.task_id = "other-task".to_string(),
            "trace_id" => success.trace_id = "other-trace".to_string(),
            "node" => success.node = "other-node".to_string(),
            "worker_id" => success.worker_id = "other-worker".to_string(),
            "version" => success.version += 1,
            _ => unreachable!(),
        }
        assert!(scheduler.success(&success).await.is_err(), "field: {field}");
    }

    for field in ["task_id", "trace_id", "node", "worker_id", "version"] {
        let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("failed");
        failure.start_time = Some(1);
        failure.end_time = Some(2);
        match field {
            "task_id" => failure.task_id = "other-task".to_string(),
            "trace_id" => failure.trace_id = "other-trace".to_string(),
            "node" => failure.node = "other-node".to_string(),
            "worker_id" => failure.worker_id = "other-worker".to_string(),
            "version" => failure.version += 1,
            _ => unreachable!(),
        }
        assert!(scheduler.failure(&failure).await.is_err(), "field: {field}");
    }

    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .state = net::State::Pending;
    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    assert!(matches!(
        scheduler.success(&success).await,
        Err(scheduler::Error::StateMismatch(_))
    ));
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .state = net::State::Processing;

    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);

    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
}

#[tokio::test]
async fn ack_is_idempotent_for_the_same_execution() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap();
    let lease_time = crate::utils::time::now_millis().saturating_sub(1);
    scheduler
        .state()
        .processing
        .get_mut(&claimed[0].id)
        .unwrap()
        .lease_time = lease_time;
    let mut first = payload::Payload::for_request(&claimed[0], "worker-1");
    first.state = net::State::Processing;
    scheduler.ack(&first).await.unwrap();

    let mut duplicate = payload::Payload::for_request(&claimed[0], "worker-1");
    duplicate.state = net::State::Processing;
    scheduler.ack(&duplicate).await.unwrap();
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(
        scheduler.state().processing[&claimed[0].id].lease_time,
        lease_time
    );
}

#[tokio::test]
async fn refresh_lease_updates_an_acknowledged_lease() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut lease = payload::Payload::for_request(&claimed, "worker-1");
    lease.state = net::State::Processing;
    scheduler.ack(&lease).await.unwrap();
    let before = crate::utils::time::now_millis().saturating_sub(1);
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .lease_time = before;

    scheduler.refresh_lease(&lease).await.unwrap();

    assert!(scheduler.state().processing[&claimed.id].lease_time >= before);
    assert_eq!(scheduler.processing_len(), 1);
}

#[tokio::test]
async fn lease_refresh_prevents_reclaim_until_it_stops() {
    let policy = scheduler::Lease::new(
        std::time::Duration::from_millis(40),
        std::time::Duration::from_millis(10),
    )
    .unwrap();
    let scheduler = Memory::new("worker-1").with_lease(policy);
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut lease = payload::Payload::for_request(&claimed, "worker-1");
    lease.state = net::State::Processing;
    scheduler.ack(&lease).await.unwrap();

    for _ in 0..3 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        scheduler.refresh_lease(&lease).await.unwrap();
        assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    }

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(reclaimed.id, claimed.id);
    assert_eq!(reclaimed.version, claimed.version + 1);
    assert_eq!(reclaimed.failed_workers, ["worker-1"]);
}

#[tokio::test]
async fn repeated_success_is_idempotent() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    success.stats.insert(
        "index".to_string(),
        serde_json::to_value(stats::Counter {
            total: 1,
            ..stats::Counter::default()
        })
        .unwrap(),
    );

    scheduler.success(&success).await.unwrap();
    scheduler.success(&success).await.unwrap();

    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.trace_stats("")["index"].total, 1);
}

#[tokio::test]
async fn success_rejects_negative_stats_without_settling() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    success.stats.insert(
        "index".to_string(),
        serde_json::to_value(stats::Counter {
            total: -1,
            ..stats::Counter::default()
        })
        .unwrap(),
    );

    let error = scheduler.success(&success).await.unwrap_err();

    assert!(error.to_string().contains("must be non-negative"));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
    assert!(scheduler.trace_stats("").is_empty());
    let state = scheduler.state();
    assert!(
        state
            .acknowledged
            .contains(&(claimed.id.clone(), claimed.version))
    );
    assert!(state.completed.is_empty());
}

#[tokio::test]
async fn failure_rejects_stats_overflow_without_partial_settlement() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    scheduler.state().trace_stats.insert(
        String::new(),
        HashMap::from([(
            "overflow".to_string(),
            stats::Counter {
                total: i64::MAX,
                ..stats::Counter::default()
            },
        )]),
    );
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    for name in ["new", "overflow"] {
        failure.stats.insert(
            name.to_string(),
            serde_json::to_value(stats::Counter {
                total: 1,
                ..stats::Counter::default()
            })
            .unwrap(),
        );
    }

    let error = scheduler.failure(&failure).await.unwrap_err();

    assert!(error.to_string().contains("stats counter overflow"));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
    let stats = scheduler.trace_stats("");
    assert_eq!(stats.len(), 1);
    assert_eq!(stats["overflow"].total, i64::MAX);
    let state = scheduler.state();
    assert!(
        state
            .acknowledged
            .contains(&(claimed.id.clone(), claimed.version))
    );
    assert!(state.completed.is_empty());
    assert_eq!(state.processing[&claimed.id].retry_count, 0);
}

#[tokio::test]
async fn repeated_retryable_failure_does_not_duplicate_the_queue() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);

    scheduler.failure(&failure).await.unwrap();
    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
}

#[tokio::test]
async fn accepted_failure_remains_idempotent_after_a_later_success() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let first = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&first, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut failure = payload::Payload::for_request(&first, "worker-1").failed("boom");
    failure.start_time = Some(1);
    failure.end_time = Some(2);
    scheduler.failure(&failure).await.unwrap();

    let second = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&second, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&second, "worker-1");
    success.start_time = Some(3);
    success.end_time = Some(4);
    scheduler.success(&success).await.unwrap();

    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert!(scheduler.errors().is_empty());
}

#[tokio::test]
async fn failure_requeues_when_retry_budget_remains() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 2;

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut ack = payload::Payload::for_request(&claimed[0], "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1").failed("boom");
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    scheduler.failure(&payload).await.unwrap();

    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.failed_len(), 0);
    let retried = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(retried.failed_workers, ["worker-1"]);
}

#[tokio::test]
async fn repeated_failures_do_not_duplicate_the_worker_history() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    for _ in 0..2 {
        let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
        let mut ack = payload::Payload::for_request(&claimed, "worker-1");
        ack.state = net::State::Processing;
        scheduler.ack(&ack).await.unwrap();
        let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("boom");
        failure.start_time = Some(1);
        failure.end_time = Some(2);
        scheduler.failure(&failure).await.unwrap();
    }

    let retried = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(retried.failed_workers, ["worker-1"]);
    assert_eq!(retried.retry_count, 2);
}

#[tokio::test]
async fn failure_moves_to_failed_when_retry_budget_is_exhausted() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut ack = payload::Payload::for_request(&claimed[0], "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1").failed("boom");
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    scheduler.failure(&payload).await.unwrap();

    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
}

#[tokio::test]
async fn retry_counter_overflow_still_reaches_a_terminal_completion() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .retry_count = i32::MAX;
    let mut failure = payload::Payload::for_request(&claimed, "worker-1").failed("overflow");
    failure.start_time = Some(1);
    failure.end_time = Some(2);

    scheduler.failure(&failure).await.unwrap();
    scheduler.failure(&failure).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    let errors = scheduler.errors();
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("overflow"));
    assert!(errors[0].contains("request retry overflow"));
}

#[tokio::test]
async fn push_rejects_missing_trace_snapshot_atomically() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();

    let result = scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await;

    assert!(matches!(result, Err(scheduler::Error::TraceNotFound(_))));
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
}

#[tokio::test]
async fn claim_uses_trace_snapshot_from_memory_domain() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            Vec::new(),
        )
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].trace_id, "trace-1");
}

#[tokio::test]
async fn init_atomically_stores_trace_and_initial_requests() {
    let scheduler = Memory::new("worker-1");
    let mut first = net::Request::follow("https://example.com/one").unwrap();
    first.task_id = "task-1".to_string();
    first.trace_id = "trace-1".to_string();
    let mut second = net::Request::follow("https://example.com/two").unwrap();
    second.task_id = "task-1".to_string();
    second.trace_id = "trace-1".to_string();

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            vec![first, second],
        )
        .await
        .unwrap();

    assert!(scheduler.trace("trace-1").await.unwrap().is_some());
    assert_eq!(scheduler.queued_len(), 2);
    assert_eq!(scheduler.next_requests(2).await.unwrap().len(), 2);
}

#[tokio::test]
async fn claim_restores_rules_requests_and_shares_trace_config() {
    let scheduler = Memory::new("worker-1");
    let config = rules_config("books", "detail");
    let mut requests = config
        .initial_requests("task-1", "trace-1", HashMap::new())
        .unwrap();
    let mut second = requests[0].clone();
    second.id = "req-second".to_string();
    second.url = "https://example.com/two".to_string();
    requests.push(second);

    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::rules("task-1", config),
            requests,
        )
        .await
        .unwrap();

    let claimed = scheduler.next_requests(2).await.unwrap();

    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].node_key(), "detail");
    let first = claimed[0].snapshot().unwrap();
    let second = claimed[1].snapshot().unwrap();
    assert!(Arc::ptr_eq(first, second));
}

#[tokio::test]
async fn trace_round_trip_preserves_spider_metadata() {
    let scheduler = Memory::new("worker-1");
    let mut config = rules_config("books", "detail");
    config.spider.version = Some("2026.07".to_string());
    config.spider.timezone = Some("Asia/Shanghai".to_string());

    let requests = config
        .initial_requests("task-1", "trace-1", HashMap::new())
        .unwrap();
    let snapshot = trace::Snapshot::rules("task-1", config);
    let snapshot =
        serde_json::from_value::<trace::Snapshot>(serde_json::to_value(snapshot).unwrap()).unwrap();

    scheduler
        .init("trace-1".to_string(), snapshot, requests)
        .await
        .unwrap();

    let stored = scheduler.trace("trace-1").await.unwrap().unwrap();
    let dsl = stored.dsl.unwrap();
    assert_eq!(dsl.spider.version.as_deref(), Some("2026.07"));
    assert_eq!(dsl.spider.timezone.as_deref(), Some("Asia/Shanghai"));
}

#[tokio::test]
async fn broken_trace_fails_only_its_request_in_the_same_claim() {
    let scheduler = Memory::new("worker-1");
    let good_config = rules_config("good", "index");
    let good = good_config
        .initial_requests("good", "trace-good", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-good".to_string(),
            trace::Snapshot::rules("good", good_config),
            vec![good],
        )
        .await
        .unwrap();

    let broken_config = rules_config("broken", "index");
    let broken = broken_config
        .initial_requests("broken", "trace-broken", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-broken".to_string(),
            trace::Snapshot::rules("broken", broken_config),
            vec![broken],
        )
        .await
        .unwrap();
    scheduler.state().trace_snapshots.remove("trace-broken");

    let claimed = scheduler.next_requests(2).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].trace_id, "trace-good");
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("Trace Snapshot not found"))
    );
}

#[tokio::test]
async fn invalid_queued_snapshot_records_a_terminal_error() {
    let scheduler = Memory::new("worker-1");
    let config = rules_config("broken", "index");
    let request = config
        .initial_requests("broken", "trace-broken", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-broken".to_string(),
            trace::Snapshot::rules("broken", config),
            vec![request],
        )
        .await
        .unwrap();
    {
        let mut state = scheduler.state();
        let mut snapshot = state.pop(crate::utils::time::now_millis()).unwrap();
        snapshot.state = net::State::Processing;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("state must be pending"))
    );
}

#[tokio::test]
async fn claim_version_overflow_records_a_terminal_error() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    {
        let mut state = scheduler.state();
        let mut snapshot = state.pop(crate::utils::time::now_millis()).unwrap();
        snapshot.version = i64::MAX;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }

    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("version overflow while claiming"))
    );
}

#[tokio::test]
async fn init_rejects_partial_trace_mismatch_without_mutation() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-2".to_string();

    let result = scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            vec![request],
        )
        .await;

    assert!(result.is_err());
    assert!(scheduler.trace("trace-1").await.unwrap().is_none());
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn success_rejects_stale_version() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1");
    payload.version += 1;
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    let error = scheduler.success(&payload).await.unwrap_err();

    assert!(matches!(error, scheduler::Error::VersionMismatch(_)));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
}

#[tokio::test]
async fn release_requeues_without_consuming_retry_budget() {
    let scheduler = Memory::new("worker-1");
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.max_retry_count = 3;
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    let mut release = payload::Payload::for_request(&claimed, "worker-1");
    release.state = net::State::Processing;

    scheduler.release(&release).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    let reclaimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(reclaimed.id, claimed.id);
    assert_eq!(reclaimed.retry_count, 0);
    assert_eq!(reclaimed.version, claimed.version + 1);
    assert!(scheduler.ack(&release).await.is_err());
}

#[tokio::test]
async fn release_defers_version_advance_until_the_next_claim() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    {
        let mut state = scheduler.state();
        let mut snapshot = state.pop(crate::utils::time::now_millis()).unwrap();
        snapshot.version = i64::MAX - 1;
        state.enqueue(snapshot, crate::utils::time::now_millis());
    }
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    assert_eq!(claimed.version, i64::MAX);
    let mut release = payload::Payload::for_request(&claimed, "worker-1");
    release.state = net::State::Processing;

    scheduler.release(&release).await.unwrap();

    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 1);
    assert!(scheduler.next_requests(1).await.unwrap().is_empty());
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("version overflow while claiming"))
    );
}

#[tokio::test]
async fn release_conversion_failure_records_a_terminal_error() {
    let scheduler = Memory::new("worker-1");
    let config = rules_config("broken", "index");
    let request = config
        .initial_requests("broken", "trace-broken", HashMap::new())
        .unwrap()
        .remove(0);
    scheduler
        .init(
            "trace-broken".to_string(),
            trace::Snapshot::rules("broken", config),
            vec![request],
        )
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler
        .state()
        .processing
        .get_mut(&claimed.id)
        .unwrap()
        .middlewares
        .push(crate::middleware::Spec::new("retry").args(serde_json::json!({"count": "invalid"})));
    let mut release = payload::Payload::for_request(&claimed, "worker-1");
    release.state = net::State::Processing;

    let error = scheduler.release(&release).await.unwrap_err();

    assert!(error.to_string().contains("invalid middleware"));
    assert_eq!(scheduler.processing_len(), 0);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.failed_len(), 1);
    assert!(
        scheduler
            .errors()
            .iter()
            .any(|error| error.contains("invalid middleware"))
    );
}

#[tokio::test]
async fn init_rejects_trace_overwrite_without_mutation() {
    let scheduler = Memory::new("worker-1");
    scheduler
        .init(
            "trace-1".to_string(),
            trace::Snapshot::code("task-1"),
            Vec::new(),
        )
        .await
        .unwrap();
    let mut replacement = trace::Snapshot::code("task-1");
    replacement.priority = 99;

    let result = scheduler
        .init("trace-1".to_string(), replacement, Vec::new())
        .await;

    assert!(result.unwrap_err().to_string().contains("already exists"));
    assert_eq!(
        scheduler.trace("trace-1").await.unwrap().unwrap().priority,
        0
    );
}

#[tokio::test]
async fn success_rejects_wrong_worker() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-2");
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    let error = scheduler.success(&payload).await.unwrap_err();

    assert!(matches!(error, scheduler::Error::LeaseMismatch(_)));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
}

#[tokio::test]
async fn success_rejects_processing_payload_state() {
    let scheduler = Memory::new("worker-1");
    let request = net::Request::follow("https://example.com").unwrap();

    scheduler
        .push(payload::Payload::for_request(&request, "worker-1").requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();
    let mut payload = payload::Payload::for_request(&claimed[0], "worker-1");
    payload.state = net::State::Processing;
    payload.start_time = Some(1);
    payload.end_time = Some(2);

    let error = scheduler.success(&payload).await.unwrap_err();

    assert!(matches!(error, scheduler::Error::Message(_)));
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.done_len(), 0);
    assert_eq!(scheduler.failed_len(), 0);
}

#[tokio::test]
async fn claim_prefers_higher_priority_and_preserves_fifo_for_ties() {
    let scheduler = Memory::new("worker-1");
    let mut low = net::Request::follow("https://example.com/low").unwrap();
    low.priority = 1;
    let low_id = low.id.clone();
    let mut high_first = net::Request::follow("https://example.com/high-first").unwrap();
    high_first.priority = 10;
    let high_first_id = high_first.id.clone();
    let mut high_second = net::Request::follow("https://example.com/high-second").unwrap();
    high_second.priority = 10;
    let high_second_id = high_second.id.clone();
    scheduler
        .push(payload::Payload::new().requests(vec![low, high_first, high_second]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(3).await.unwrap();

    assert_eq!(
        claimed
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        [
            high_first_id.as_str(),
            high_second_id.as_str(),
            low_id.as_str()
        ]
    );
}

#[tokio::test]
async fn claim_leaves_future_requests_pending() {
    let scheduler = Memory::new("worker-1");
    let mut delayed = net::Request::follow("https://example.com/delayed").unwrap();
    delayed.next_time = crate::utils::time::now_millis() + 60_000;
    scheduler
        .push(payload::Payload::new().requests(vec![delayed]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap();

    assert!(claimed.is_empty());
    assert_eq!(scheduler.queued_len(), 1);
    assert!(scheduler.has_pending_requests().await.unwrap());
}
