use spider::scheduler::{Error, Init};
use spider::{Scheduler, net, payload, trace};

use super::{
    fixture::{HTTP, WORKER_A, WORKER_B, close, open},
    payload::{failure, owned_request, processing, request, success},
};

pub(super) async fn succeed<S>(scheduler: &S, request: &net::Request)
where
    S: Scheduler,
{
    scheduler.ack(&processing(request)).await.unwrap();
    scheduler.success(&success(request)).await.unwrap();
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
    let mut wrong_worker = processing(&first);
    wrong_worker.worker_id = WORKER_B.to_string();
    assert!(matches!(
        scheduler.ack(&wrong_worker).await.unwrap_err(),
        Error::LeaseMismatch(_)
    ));

    let first_active = processing(&first);
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

    let second_active = processing(&second);
    assert!(matches!(
        scheduler.refresh_lease(&second_active).await.unwrap_err(),
        Error::NotAcknowledged(_)
    ));
    scheduler.ack(&second_active).await.unwrap();
    for payload in success_mismatches(&second) {
        assert!(scheduler.success(&payload).await.is_err());
    }

    let completed = success(&second);
    scheduler.success(&completed).await.unwrap();
    scheduler.success(&completed).await.unwrap();
    let stale = success(&first);
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
    scheduler.ack(&processing(&first)).await.unwrap();
    for payload in failure_mismatches(&first) {
        assert!(scheduler.failure(&payload).await.is_err());
    }
    let first_failure = failure(&first, "first failure");
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
    scheduler.ack(&processing(&second)).await.unwrap();
    let second_failure = failure(&second, "second failure");
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

    scheduler.release(&processing(&first)).await.unwrap();

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
    let mut id = processing(request);
    id.id = "other-request".to_string();
    let mut task_id = processing(request);
    task_id.task_id = "other-task".to_string();
    let mut trace_id = processing(request);
    trace_id.trace_id = "other-trace".to_string();
    let mut node = processing(request);
    node.node = "other-node".to_string();
    let mut worker_id = processing(request);
    worker_id.worker_id = "other-worker".to_string();
    let mut version = processing(request);
    version.version += 1;
    let mut state = processing(request);
    state.state = net::State::Done;
    vec![id, task_id, trace_id, node, worker_id, version, state]
}

fn success_mismatches(request: &net::Request) -> Vec<payload::Payload> {
    let mut id = success(request);
    id.id = "other-request".to_string();
    let mut task_id = success(request);
    task_id.task_id = "other-task".to_string();
    let mut trace_id = success(request);
    trace_id.trace_id = "other-trace".to_string();
    let mut node = success(request);
    node.node = "other-node".to_string();
    let mut worker_id = success(request);
    worker_id.worker_id = "other-worker".to_string();
    let mut version = success(request);
    version.version += 1;
    let state = failure(request, "unexpected failure state");
    vec![id, task_id, trace_id, node, worker_id, version, state]
}

fn failure_mismatches(request: &net::Request) -> Vec<payload::Payload> {
    let mut id = failure(request, "failure");
    id.id = "other-request".to_string();
    let mut task_id = failure(request, "failure");
    task_id.task_id = "other-task".to_string();
    let mut trace_id = failure(request, "failure");
    trace_id.trace_id = "other-trace".to_string();
    let mut node = failure(request, "failure");
    node.node = "other-node".to_string();
    let mut worker_id = failure(request, "failure");
    worker_id.worker_id = "other-worker".to_string();
    let mut version = failure(request, "failure");
    version.version += 1;
    let state = success(request);
    vec![id, task_id, trace_id, node, worker_id, version, state]
}
