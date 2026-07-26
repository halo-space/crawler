use std::sync::Arc;

use spider::{Scheduler, payload};

use super::{
    fixture::{HTTP, WORKER_A, close, open, race},
    payload::{failure, processing, request},
    settlement::succeed,
};

pub(super) async fn initial_request_validation_is_atomic<S>(scheduler: S)
where
    S: Scheduler,
{
    open(&scheduler).await;
    let valid = request(
        "initial-validation-valid",
        "https://example.com/initial-validation/valid",
    );
    let mut invalid = request(
        "initial-validation-invalid",
        "https://example.com/initial-validation/invalid",
    );
    invalid.version = 1;

    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![valid, invalid]))
            .await
            .is_err()
    );
    assert!(
        scheduler
            .next_requests(2, WORKER_A, HTTP)
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
    close(&scheduler).await;
}

pub(super) async fn request_replay_is_atomic<S>(scheduler: S)
where
    S: Scheduler + 'static,
{
    open(&scheduler).await;
    let original = request("replay", "https://example.com/replay");
    let first = payload::Payload::new().requests(vec![original.clone()]);
    let second = payload::Payload::new().requests(vec![original.clone()]);
    let scheduler = Arc::new(scheduler);
    let (first, second) = race(
        scheduler.clone(),
        move |scheduler| async move { scheduler.push(first).await },
        move |scheduler| async move { scheduler.push(second).await },
    )
    .await;
    first.unwrap();
    second.unwrap();

    let added = request("replay-added", "https://example.com/replay/added");
    scheduler
        .push(payload::Payload::new().requests(vec![original.clone(), added.clone()]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(claimed.len(), 2);
    let ids = claimed
        .iter()
        .map(|request| request.id.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(ids, ["replay", "replay-added"].into_iter().collect());
    scheduler
        .push(payload::Payload::new().requests(vec![original.clone()]))
        .await
        .unwrap();
    for request in &claimed {
        succeed(scheduler.as_ref(), request).await;
    }

    scheduler
        .push(payload::Payload::new().requests(vec![original.clone()]))
        .await
        .unwrap();
    assert!(
        !scheduler
            .has_pending_requests(WORKER_A, HTTP)
            .await
            .unwrap()
    );

    let mut first_map = request("replay-map", "https://example.com/replay/map");
    first_map.vals.insert("first".to_string(), "one".into());
    first_map.vals.insert("second".to_string(), "two".into());
    first_map.kwargs.insert("first".to_string(), "one".into());
    first_map.kwargs.insert("second".to_string(), "two".into());
    let mut second_map = request("replay-map", "https://example.com/replay/map");
    second_map.vals.insert("second".to_string(), "two".into());
    second_map.vals.insert("first".to_string(), "one".into());
    second_map.kwargs.insert("second".to_string(), "two".into());
    second_map.kwargs.insert("first".to_string(), "one".into());
    scheduler
        .push(payload::Payload::new().requests(vec![first_map]))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![second_map]))
        .await
        .unwrap();
    let replayed = scheduler.next_requests(2, WORKER_A, HTTP).await.unwrap();
    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].id, "replay-map");
    succeed(scheduler.as_ref(), &replayed[0]).await;

    let mut failed = request("replay-failed", "https://example.com/replay/failed");
    failed.max_retry_count = 1;
    scheduler
        .push(payload::Payload::new().requests(vec![failed.clone()]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER_A, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler.ack(&processing(&claimed)).await.unwrap();
    scheduler
        .failure(&failure(&claimed, "terminal replay"))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![failed]))
        .await
        .unwrap();
    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let conflict = request("replay", "https://example.com/conflict");
    let new = request("new-after-conflict", "https://example.com/new");
    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![conflict, new]))
            .await
            .is_err()
    );
    assert!(
        scheduler
            .next_requests(2, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let duplicate = request("duplicate", "https://example.com/duplicate");
    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![duplicate.clone(), duplicate]))
            .await
            .is_err()
    );
    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );

    let valid = request("invalid-collection-valid", "https://example.com/valid");
    let mut invalid = request("invalid-collection-invalid", "https://example.com/invalid");
    invalid.state = spider::net::State::Processing;
    assert!(
        scheduler
            .push(payload::Payload::new().requests(vec![valid, invalid]))
            .await
            .is_err()
    );
    assert!(
        scheduler
            .next_requests(1, WORKER_A, HTTP)
            .await
            .unwrap()
            .is_empty()
    );
    close(scheduler.as_ref()).await;
}
