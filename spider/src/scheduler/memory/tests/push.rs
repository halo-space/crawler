use super::*;

#[tokio::test]
async fn push_and_claim_moves_request_to_processing() {
    let scheduler = memory();
    let request = request("https://example.com");
    let payload =
        payload::Payload::for_request(&request, "worker-1").requests(vec![request.clone()]);

    scheduler.push(payload).await.unwrap();
    let claimed = scheduler.next_requests(1, WORKER, HTTP).await.unwrap();

    assert_eq!(claimed.len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(claimed[0].leased_by, "worker-1");
}

#[tokio::test]
async fn push_rejects_an_unbound_follow_request() {
    let scheduler = Memory::new();
    let request = net::Request::follow("https://example.com").unwrap();

    let error = scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("new Request task_id must not be empty")
    );
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn push_rejects_invalid_collection_without_partial_enqueue() {
    let scheduler = memory();
    let mut valid = request("https://example.com/valid");
    valid.task_id = "task-1".to_string();
    let mut invalid = request("https://example.com/invalid");
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
    let scheduler = memory();
    let request = request("https://example.com");
    let result = scheduler
        .push(payload::Payload::new().requests(vec![request.clone(), request]))
        .await;
    assert!(result.is_err());
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn push_is_idempotent_for_a_request_already_queued() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    assert_eq!(scheduler.queued_len(), 1);
}

#[tokio::test]
async fn push_compares_structured_snapshots_independent_of_map_insertion_order() {
    let scheduler = memory();
    let mut request = request("https://example.com/ordered");
    request
        .vals
        .insert("first".to_string(), serde_json::json!(1));
    request
        .vals
        .insert("second".to_string(), serde_json::json!(2));
    let mut replay = request.clone();
    replay.vals.clear();
    replay
        .vals
        .insert("second".to_string(), serde_json::json!(2));
    replay
        .vals
        .insert("first".to_string(), serde_json::json!(1));

    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![replay]))
        .await
        .unwrap();

    assert_eq!(scheduler.queued_len(), 1);
}

#[test]
fn concurrent_identical_pushes_are_idempotent() {
    const THREADS: usize = 32;

    let scheduler = Arc::new(memory());
    let request = request("https://example.com/concurrent");
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

    assert!(results.iter().all(Result::is_ok));
    assert_eq!(scheduler.queued_len(), 1);
}

#[test]
fn concurrent_conflict_does_not_partially_insert_missing_requests() {
    let scheduler = Arc::new(memory());
    let existing = request("https://example.com/existing");
    let mut conflict = existing.clone();
    conflict.priority = 10;
    let rejected = request("https://example.com/rejected");
    let accepted = request("https://example.com/accepted");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime
        .block_on(scheduler.push(payload::Payload::new().requests(vec![existing.clone()])))
        .unwrap();

    let barrier = Arc::new(std::sync::Barrier::new(2));
    let conflicting = {
        let scheduler = scheduler.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            barrier.wait();
            runtime.block_on(
                scheduler.push(payload::Payload::new().requests(vec![conflict, rejected])),
            )
        })
    };
    let independent = {
        let scheduler = scheduler.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap();
            barrier.wait();
            runtime.block_on(scheduler.push(payload::Payload::new().requests(vec![accepted])))
        })
    };

    assert!(conflicting.join().unwrap().is_err());
    assert!(independent.join().unwrap().is_ok());
    assert_eq!(scheduler.queued_len(), 2);
    let claimed = runtime
        .block_on(scheduler.next_requests(3, WORKER, HTTP))
        .unwrap();
    let urls = claimed
        .iter()
        .map(|request| request.url.as_str())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        urls,
        std::collections::HashSet::from([
            "https://example.com/existing",
            "https://example.com/accepted",
        ])
    );
}

#[tokio::test]
async fn replay_remains_idempotent_after_a_cookie_expires() {
    let scheduler = memory();
    let mut request = request("https://example.com/cookies");
    let origin = url::Url::parse(&request.url).unwrap();
    let mut headers = net::Headers::new();
    headers
        .try_append("set-cookie", "sid=one; Max-Age=1; Path=/")
        .unwrap();
    request.cookies.store_response(&origin, &headers);
    assert_eq!(request.cookies.len(), 1);

    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    assert!(request.cookies.is_empty());
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    assert_eq!(scheduler.queued_len(), 1);
}

#[tokio::test]
async fn push_is_idempotent_while_a_request_is_processing_or_terminal() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    let claimed = scheduler
        .next_requests(1, WORKER, HTTP)
        .await
        .unwrap()
        .pop()
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request.clone()]))
        .await
        .unwrap();
    assert_eq!(scheduler.processing_len(), 1);

    let mut ack = payload::Payload::for_request(&claimed, "worker-1");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, "worker-1");
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.queued_len(), 0);
}

#[tokio::test]
async fn push_atomically_adds_missing_requests_when_existing_snapshots_match() {
    let scheduler = memory();
    let existing = request("https://example.com/existing");
    let missing = request("https://example.com/missing");
    scheduler
        .push(payload::Payload::new().requests(vec![existing.clone()]))
        .await
        .unwrap();

    scheduler
        .push(payload::Payload::new().requests(vec![existing.clone(), missing.clone()]))
        .await
        .unwrap();
    scheduler
        .push(payload::Payload::new().requests(vec![existing, missing]))
        .await
        .unwrap();

    assert_eq!(scheduler.queued_len(), 2);
}

#[tokio::test]
async fn push_rejects_a_conflicting_snapshot_without_adding_missing_requests() {
    let scheduler = memory();
    let existing = request("https://example.com/existing");
    let mut conflict = existing.clone();
    conflict.priority = 10;
    let missing = request("https://example.com/missing");
    scheduler
        .push(payload::Payload::new().requests(vec![existing]))
        .await
        .unwrap();

    let error = scheduler
        .push(payload::Payload::new().requests(vec![conflict, missing]))
        .await
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("conflicts with existing snapshot")
    );
    assert_eq!(scheduler.queued_len(), 1);
}

#[tokio::test]
async fn push_rejects_missing_trace_snapshot_atomically() {
    let scheduler = Memory::new();
    let mut request = request("https://example.com");
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
