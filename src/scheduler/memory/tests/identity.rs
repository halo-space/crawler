use super::*;

#[tokio::test]
async fn execution_operations_reject_identity_mismatch_without_mutation() {
    let scheduler = memory();
    let request = request("https://example.com");
    scheduler
        .push(payload::Payload::new().requests(vec![request]))
        .await
        .unwrap();
    let claimed = scheduler.next_requests(1).await.unwrap().pop().unwrap();

    for field in ["task_id", "trace_id", "node", "version"] {
        let mut ack = payload::Payload::for_request(&claimed, WORKER);
        ack.state = net::State::Processing;
        match field {
            "task_id" => ack.task_id = "other-task".to_string(),
            "trace_id" => ack.trace_id = "other-trace".to_string(),
            "node" => ack.node = "other-node".to_string(),
            "version" => ack.version += 1,
            _ => unreachable!(),
        }
        assert!(scheduler.ack(&ack).await.is_err(), "field: {field}");
    }
    assert_eq!(scheduler.processing_len(), 1);

    let mut ack = payload::Payload::for_request(&claimed, "reported-worker");
    ack.state = net::State::Processing;
    scheduler.ack(&ack).await.unwrap();

    let mut refresh = payload::Payload::for_request(&claimed, "reported-worker");
    refresh.state = net::State::Processing;
    scheduler.refresh_lease(&refresh).await.unwrap();

    for field in ["task_id", "trace_id", "node", "version"] {
        let mut refresh = payload::Payload::for_request(&claimed, WORKER);
        refresh.state = net::State::Processing;
        match field {
            "task_id" => refresh.task_id = "other-task".to_string(),
            "trace_id" => refresh.trace_id = "other-trace".to_string(),
            "node" => refresh.node = "other-node".to_string(),
            "version" => refresh.version += 1,
            _ => unreachable!(),
        }
        assert!(
            scheduler.refresh_lease(&refresh).await.is_err(),
            "field: {field}"
        );
    }

    for field in ["task_id", "trace_id", "node", "version"] {
        let mut success = payload::Payload::for_request(&claimed, WORKER);
        success.start_time = Some(1);
        success.end_time = Some(2);
        match field {
            "task_id" => success.task_id = "other-task".to_string(),
            "trace_id" => success.trace_id = "other-trace".to_string(),
            "node" => success.node = "other-node".to_string(),
            "version" => success.version += 1,
            _ => unreachable!(),
        }
        assert!(scheduler.success(&success).await.is_err(), "field: {field}");
    }

    for field in ["task_id", "trace_id", "node", "version"] {
        let mut failure = payload::Payload::for_request(&claimed, WORKER).failed("failed");
        failure.start_time = Some(1);
        failure.end_time = Some(2);
        match field {
            "task_id" => failure.task_id = "other-task".to_string(),
            "trace_id" => failure.trace_id = "other-trace".to_string(),
            "node" => failure.node = "other-node".to_string(),
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
    let mut success = payload::Payload::for_request(&claimed, "reported-worker");
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

    let mut success = payload::Payload::for_request(&claimed, WORKER);
    success.start_time = Some(1);
    success.end_time = Some(2);
    scheduler.success(&success).await.unwrap();
    assert_eq!(
        scheduler.state().completed[&(claimed.id, claimed.version)].worker_id,
        WORKER
    );
}
