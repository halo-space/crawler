use super::*;

#[tokio::test]
async fn cleanup_removes_old_terminal_history_and_trace_data() {
    let scheduler = memory();
    let request = request("https://example.com/cleanup");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();

    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();
    scheduler.ack(&processing(&claimed)).await.unwrap();
    let mut success = payload::Payload::for_request(&claimed, WORKER);
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();

    let mut state = scheduler.state();
    state
        .completed
        .get_mut(&(claimed.id.clone(), claimed.version))
        .unwrap()
        .completed_at = 1;
    state.trace_activity.insert(TRACE.to_string(), 1);
    drop(state);

    assert_eq!(scheduler.cleanup(2), 1);
    assert_eq!(scheduler.done_len(), 1);
    assert_eq!(scheduler.trace_len(), 0);
    assert!(scheduler.trace_stats(TRACE).is_empty());
    assert!(scheduler.state().completed.is_empty());
    assert!(scheduler.state().request_hashes.is_empty());
}

#[tokio::test]
async fn cleanup_preserves_queued_and_processing_requests() {
    let scheduler = memory();
    let queued = request("https://example.com/queued");
    scheduler
        .push(payload::Payload::new().requests(vec![queued]))
        .await
        .unwrap();
    let processing_request = request("https://example.com/processing");
    scheduler
        .push(payload::Payload::new().requests(vec![processing_request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    assert_eq!(scheduler.cleanup(i64::MAX), 0);
    assert_eq!(scheduler.queued_len(), 1);
    assert_eq!(scheduler.processing_len(), 1);
    assert_eq!(scheduler.trace_len(), 1);
    assert!(scheduler.state().request_hashes.contains_key(&claimed.id));
}

fn processing(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, WORKER);
    payload.state = net::State::Processing;
    payload
}
