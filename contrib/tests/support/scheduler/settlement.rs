use spider::scheduler::{Error, Init};
use spider::{Scheduler, net, payload, trace};

use super::{
    fixture::{HTTP, WORKER_A, WORKER_B, close, open},
    payload::{failure_payload, owned_request, processing_payload, request, success_payload},
};

pub(super) async fn succeed<S>(scheduler: &S, request: &net::Request)
where
    S: Scheduler,
{
    scheduler.ack(&processing_payload(request)).await.unwrap();
    scheduler.success(&success_payload(request)).await.unwrap();
}

pub(super) async fn execution_identity_is_enforced<S>(scheduler: S)
where
    S: Scheduler + Init,
{
    open(&scheduler).await;
    scheduler
        .init(
            "trace-settlement".to_string(),
            trace::Snapshot::code("task-settlement"),
            Vec::new(),
        )
        .await
        .unwrap();
    let original = owned_request(
        "settlement",
        "https://example.com/settlement",
        "task-settlement",
        "trace-settlement",
    );
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let first = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();

    for payload in processing_mismatches(&first) {
        let error = scheduler.ack(&payload).await.unwrap_err();
        assert!(!error.is_transient());
    }
    let mut wrong_worker = processing_payload(&first);
    wrong_worker.worker_id = WORKER_B.to_string();
    assert!(matches!(
        scheduler.ack(&wrong_worker).await.unwrap_err(),
        Error::LeaseMismatch(_)
    ));

    let first_active = processing_payload(&first);
    scheduler.ack(&first_active).await.unwrap();
    scheduler.ack(&first_active).await.unwrap();
    for payload in processing_mismatches(&first) {
        assert!(scheduler.refresh_lease(&payload).await.is_err());
    }
    scheduler.refresh_lease(&first_active).await.unwrap();
    for payload in processing_mismatches(&first) {
        assert!(scheduler.release(&payload).await.is_err());
    }
    scheduler.release(&first_active).await.unwrap();
    assert!(scheduler.release(&first_active).await.is_err());

    let second = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.id, first.id);
    assert_eq!(second.version, first.version + 1);
    assert_eq!(second.retry_count, first.retry_count);

    let second_active = processing_payload(&second);
    assert!(matches!(
        scheduler.refresh_lease(&second_active).await.unwrap_err(),
        Error::NotAcknowledged(_)
    ));
    scheduler.ack(&second_active).await.unwrap();
    for payload in success_mismatches(&second) {
        assert!(scheduler.success(&payload).await.is_err());
    }

    let success = success_payload(&second);
    scheduler.success(&success).await.unwrap();
    scheduler.success(&success).await.unwrap();
    let stale = success_payload(&first);
    assert!(
        scheduler
            .success(&stale)
            .await
            .unwrap_err()
            .is_ownership_loss()
    );
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    close(&scheduler).await;
}

pub(super) async fn failure_owns_queue_retry<S>(scheduler: S)
where
    S: Scheduler,
{
    open(&scheduler).await;
    let mut original = request("failure", "https://example.com/failure");
    original.max_retry_count = 2;
    scheduler
        .push(payload::Payload::new().requests(vec![original.clone()]))
        .await
        .unwrap();

    let first = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing_payload(&first)).await.unwrap();
    for payload in failure_mismatches(&first) {
        assert!(scheduler.failure(&payload).await.is_err());
    }
    let first_failure = failure_payload(&first, "first failure");
    scheduler.failure(&first_failure).await.unwrap();
    scheduler.failure(&first_failure).await.unwrap();

    let second = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.id, first.id);
    assert_eq!(second.version, first.version + 1);
    assert_eq!(second.retry_count, 1);
    assert_eq!(second.failed_workers, [WORKER_A]);
    scheduler.ack(&processing_payload(&second)).await.unwrap();
    let second_failure = failure_payload(&second, "second failure");
    scheduler.failure(&second_failure).await.unwrap();
    scheduler.failure(&second_failure).await.unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );
    close(&scheduler).await;
}

pub(super) async fn release_before_ack_preserves_queue_retry<S>(scheduler: S)
where
    S: Scheduler,
{
    open(&scheduler).await;
    let original = request(
        "release-before-ack",
        "https://example.com/release-before-ack",
    );
    scheduler
        .push(payload::Payload::new().requests(vec![original]))
        .await
        .unwrap();
    let first = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();

    scheduler
        .release(&processing_payload(&first))
        .await
        .unwrap();

    let returned = scheduler
        .next_requests(1, WORKER_B, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(returned.id, first.id);
    assert_eq!(returned.version, first.version + 1);
    assert_eq!(returned.retry_count, first.retry_count);
    assert!(returned.failed_workers.is_empty());
    succeed(&scheduler, &returned).await;
    close(&scheduler).await;
}

fn processing_mismatches(request: &net::Request) -> Vec<payload::Payload> {
    let mut id = processing_payload(request);
    id.id = "other-request".to_string();
    let mut task_id = processing_payload(request);
    task_id.task_id = "other-task".to_string();
    let mut trace_id = processing_payload(request);
    trace_id.trace_id = "other-trace".to_string();
    let mut node = processing_payload(request);
    node.node = "other-node".to_string();
    let mut worker_id = processing_payload(request);
    worker_id.worker_id = "other-worker".to_string();
    let mut version = processing_payload(request);
    version.version += 1;
    let mut state = processing_payload(request);
    state.state = net::State::Done;
    vec![id, task_id, trace_id, node, worker_id, version, state]
}

fn success_mismatches(request: &net::Request) -> Vec<payload::Payload> {
    let mut id = success_payload(request);
    id.id = "other-request".to_string();
    let mut task_id = success_payload(request);
    task_id.task_id = "other-task".to_string();
    let mut trace_id = success_payload(request);
    trace_id.trace_id = "other-trace".to_string();
    let mut node = success_payload(request);
    node.node = "other-node".to_string();
    let mut worker_id = success_payload(request);
    worker_id.worker_id = "other-worker".to_string();
    let mut version = success_payload(request);
    version.version += 1;
    let state = failure_payload(request, "unexpected failure state");
    vec![id, task_id, trace_id, node, worker_id, version, state]
}

fn failure_mismatches(request: &net::Request) -> Vec<payload::Payload> {
    let mut id = failure_payload(request, "failure");
    id.id = "other-request".to_string();
    let mut task_id = failure_payload(request, "failure");
    task_id.task_id = "other-task".to_string();
    let mut trace_id = failure_payload(request, "failure");
    trace_id.trace_id = "other-trace".to_string();
    let mut node = failure_payload(request, "failure");
    node.node = "other-node".to_string();
    let mut worker_id = failure_payload(request, "failure");
    worker_id.worker_id = "other-worker".to_string();
    let mut version = failure_payload(request, "failure");
    version.version += 1;
    let state = success_payload(request);
    vec![id, task_id, trace_id, node, worker_id, version, state]
}
