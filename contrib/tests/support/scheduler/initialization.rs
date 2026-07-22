use spider::scheduler::{Error, Init};
use spider::{Scheduler, net, payload, trace};

use super::{
    fixture::{HTTP, WORKER_A, close, open},
    payload::owned_request,
    settlement::succeed,
};

pub(super) async fn lifecycle_and_trace<S>(scheduler: S, initializes_run: bool)
where
    S: Scheduler + Init,
{
    open(&scheduler).await;
    assert_eq!(scheduler.initializes_run(), initializes_run);

    let mut snapshot = trace::Snapshot::code("task-trace");
    snapshot.priority = 7;
    snapshot.params.insert("source".to_string(), "test".into());
    scheduler
        .init("trace-empty".to_string(), snapshot, Vec::new())
        .await
        .unwrap();

    let mut first = scheduler.trace("trace-empty").await.unwrap().unwrap();
    assert_eq!(first.task_id, "task-trace");
    assert_eq!(first.priority, 7);
    assert_eq!(first.params["source"], "test");
    first.priority = 99;
    assert_eq!(
        scheduler
            .trace("trace-empty")
            .await
            .unwrap()
            .unwrap()
            .priority,
        7
    );
    assert!(scheduler.trace("missing").await.unwrap().is_none());
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    let replacement = trace::Snapshot::code("task-trace");
    assert!(
        scheduler
            .init("trace-empty".to_string(), replacement, Vec::new())
            .await
            .is_err()
    );
    assert_eq!(
        scheduler
            .trace("trace-empty")
            .await
            .unwrap()
            .unwrap()
            .priority,
        7
    );

    let rules = spider::config::Config::from_yaml(
        r#"
spider:
  name: rules-trace
  start:
    - {node: index, url: https://example.com/first}
    - {node: index, url: https://example.com/second}
graph:
  nodes:
    index: {}
  edges: []
"#,
    )
    .unwrap();
    let initial = rules
        .initial_requests("task-rules", "trace-rules", Default::default())
        .unwrap();
    assert_eq!(initial.len(), 2);
    scheduler
        .init(
            "trace-rules".to_string(),
            trace::Snapshot::rules("task-rules", rules),
            initial,
        )
        .await
        .unwrap();
    let stored = scheduler.trace("trace-rules").await.unwrap().unwrap();
    assert_eq!(stored.dsl.unwrap().spider.name, "rules-trace");
    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert!(claimed.iter().all(|request| {
        request.task_id == "task-rules"
            && request.trace_id == "trace-rules"
            && request.node_key() == "index"
    }));
    let mut urls = claimed
        .iter()
        .map(|request| request.url.as_str())
        .collect::<Vec<_>>();
    urls.sort_unstable();
    assert_eq!(
        urls,
        ["https://example.com/first", "https://example.com/second"]
    );
    for request in &claimed {
        succeed(&scheduler, request).await;
    }
    close(&scheduler).await;
}

pub(super) async fn initialization_is_atomic<S>(scheduler: S)
where
    S: Scheduler + Init,
{
    open(&scheduler).await;
    let first = owned_request(
        "init-first",
        "https://example.com/init/first",
        "task-init",
        "trace-init",
    );
    let mut invalid = owned_request(
        "init-invalid",
        "https://example.com/init/invalid",
        "task-init",
        "trace-init",
    );
    invalid.state = net::State::Processing;

    assert!(
        scheduler
            .init(
                "trace-init".to_string(),
                trace::Snapshot::code("task-init"),
                vec![first, invalid],
            )
            .await
            .is_err()
    );
    assert!(scheduler.trace("trace-init").await.unwrap().is_none());
    assert!(
        scheduler
            .next_requests(2, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    close(&scheduler).await;
}

pub(super) async fn trace_ownership_is_atomic<S>(scheduler: S)
where
    S: Scheduler + Init,
{
    open(&scheduler).await;
    scheduler
        .init(
            "trace-owner".to_string(),
            trace::Snapshot::code("task-owner"),
            Vec::new(),
        )
        .await
        .unwrap();

    let valid = owned_request(
        "owner-valid",
        "https://example.com/owner/valid",
        "task-owner",
        "trace-owner",
    );
    let mismatch = owned_request(
        "owner-mismatch",
        "https://example.com/owner/mismatch",
        "other-task",
        "trace-owner",
    );
    let error = scheduler
        .push(payload::Payload::new().requests(vec![valid, mismatch]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::IdentityMismatch { .. }));
    assert!(
        scheduler
            .next_requests(2, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let missing = owned_request(
        "owner-missing",
        "https://example.com/owner/missing",
        "task-owner",
        "trace-missing",
    );
    assert!(matches!(
        scheduler
            .push(payload::Payload::new().requests(vec![missing]))
            .await
            .unwrap_err(),
        Error::TraceNotFound(_)
    ));
    close(&scheduler).await;
}
