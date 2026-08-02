use super::*;

#[tokio::test]
async fn damaged_snapshots_are_failed_without_withholding_valid_claims() {
    let mut valid = bound_request("https://example.com/valid");
    valid.id = "valid-request".to_string();
    let valid = claimed(net::request::Snapshot::try_from(valid).unwrap());

    let mut damaged_request = bound_request("https://example.com/damaged-request");
    damaged_request.id = "damaged-request".to_string();
    let mut damaged_request = claimed(net::request::Snapshot::try_from(damaged_request).unwrap());
    damaged_request["snapshot"] = json!({"id": 1});

    let mut damaged_trace = bound_request("https://example.com/damaged-trace");
    damaged_trace.id = "damaged-trace".to_string();
    let mut damaged_trace = claimed(net::request::Snapshot::try_from(damaged_trace).unwrap());
    damaged_trace["trace"] = json!({"task_id": 1});

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json(
            "200 OK",
            json!({"requests": [valid, damaged_request, damaged_trace]}),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let requests = api.next_requests(3).await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, "valid-request");
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 1);
    assert_eq!(count(&requests, "/requests/release"), 1);
    let failed = requests
        .iter()
        .filter(|request| request.path.ends_with("/requests/failure"))
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(failed[0]["identity"]["id"], "damaged-request");
    assert_eq!(failed[0]["identity"]["version"], 1);
    assert!(failed[0]["identity"].get("worker_id").is_none());
}

#[tokio::test]
async fn a_duplicate_claimed_request_is_rejected_and_released_once() {
    let mut request = bound_request("https://example.com/valid");
    request.id = "request-1".to_string();
    let snapshot = net::request::Snapshot::try_from(request).unwrap();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(snapshot.clone()), claimed(snapshot)]}),
        ),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(2).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::InvalidRequest { .. }));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/release"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 0);
}

#[tokio::test]
async fn an_over_limit_claim_is_fully_released_before_the_protocol_error() {
    let mut first = bound_request("https://example.com/first");
    first.id = "request-1".to_string();
    let mut second = bound_request("https://example.com/second");
    second.id = "request-2".to_string();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json(
            "200 OK",
            json!({"requests": [
                claimed(net::request::Snapshot::try_from(first).unwrap()),
                claimed(net::request::Snapshot::try_from(second).unwrap())
            ]}),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Message(message) if message.contains("claim limit 1"))
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/release"), 2);
}

#[tokio::test]
async fn recovery_settlement_retries_are_bounded_without_withholding_a_valid_claim() {
    let mut valid = bound_request("https://example.com/valid");
    valid.id = "valid-request".to_string();
    let valid = claimed(net::request::Snapshot::try_from(valid).unwrap());
    let mut damaged = bound_request("https://example.com/damaged");
    damaged.id = "damaged-request".to_string();
    let mut damaged = claimed(net::request::Snapshot::try_from(damaged).unwrap());
    damaged["snapshot"] = json!({});

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [valid, damaged]})),
        Response::empty("204 No Content"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let requests = api.next_requests(2).await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, "valid-request");
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 3);
    assert_eq!(count(&requests, "/requests/release"), 0);
}

#[tokio::test]
async fn a_recovery_settlement_error_is_returned_when_nothing_is_executable() {
    let mut damaged = bound_request("https://example.com/damaged");
    damaged.id = "damaged-request".to_string();
    let mut damaged = claimed(net::request::Snapshot::try_from(damaged).unwrap());
    damaged["snapshot"] = json!({});
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [damaged]})),
        Response::empty("204 No Content"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Unavailable(message) if message.contains("failed to settle its recovery"))
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 3);
}

#[tokio::test]
async fn a_temporary_trace_read_releases_without_consuming_request_retry() {
    let mut request = bound_request("https://example.com/temporary-trace");
    request.id = "temporary-trace".to_string();
    let mut request = claimed(net::request::Snapshot::try_from(request).unwrap());
    request["trace"] = serde_json::Value::Null;

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [request]})),
        unavailable("Trace store unavailable"),
        unavailable("Trace store unavailable"),
        unavailable("Trace store unavailable"),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::Unavailable(_)));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 0);
    assert_eq!(count(&requests, "/requests/failure"), 0);
    assert_eq!(count(&requests, "/requests/release"), 1);
    assert_eq!(count(&requests, "/v1/worker/traces/trace-1"), 3);
}

#[tokio::test]
async fn a_failed_temporary_trace_release_still_does_not_fail_the_request() {
    let mut request = bound_request("https://example.com/temporary-trace");
    request.id = "temporary-trace".to_string();
    let mut request = claimed(net::request::Snapshot::try_from(request).unwrap());
    request["trace"] = serde_json::Value::Null;

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [request]})),
        unavailable("Trace store unavailable"),
        unavailable("Trace store unavailable"),
        unavailable("Trace store unavailable"),
        unavailable("release unavailable"),
        unavailable("release unavailable"),
        unavailable("release unavailable"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::Unavailable(_)));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 0);
    assert_eq!(count(&requests, "/requests/failure"), 0);
    assert_eq!(count(&requests, "/requests/release"), 3);
}

#[tokio::test]
async fn a_missing_trace_is_released_without_failing_the_request() {
    let mut request = bound_request("https://example.com/missing-trace");
    request.id = "missing-trace".to_string();
    let mut request = claimed(net::request::Snapshot::try_from(request).unwrap());
    request["trace"] = serde_json::Value::Null;

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [request]})),
        Response::json("200 OK", serde_json::Value::Null),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::Message(_)));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 0);
    assert_eq!(count(&requests, "/requests/failure"), 0);
    assert_eq!(count(&requests, "/requests/release"), 1);
}

#[tokio::test]
async fn an_incompatible_claim_is_released_without_failing_the_request() {
    let mut request = bound_request("https://example.com/browser-only");
    request.id = "browser-only".to_string();
    request.mode = net::Mode::Browser;
    let request = claimed(net::request::Snapshot::try_from(request).unwrap());

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [request]})),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let error = api.next_requests(1).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::Message(_)));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 0);
    assert_eq!(count(&requests, "/requests/failure"), 0);
    assert_eq!(count(&requests, "/requests/release"), 1);
}

#[tokio::test]
async fn requests_with_the_same_cold_trace_share_one_read() {
    let mut first = bound_request("https://example.com/first");
    first.id = "request-1".to_string();
    let mut second = bound_request("https://example.com/second");
    second.id = "request-2".to_string();
    let mut first = claimed(net::request::Snapshot::try_from(first).unwrap());
    let mut second = claimed(net::request::Snapshot::try_from(second).unwrap());
    first["trace"] = serde_json::Value::Null;
    second["trace"] = serde_json::Value::Null;

    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": [first, second]})),
        Response::json(
            "200 OK",
            serde_json::to_value(Some(trace::Snapshot::code("task-1"))).unwrap(),
        ),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let requests = api.next_requests(2).await.unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        ["request-1", "request-2"]
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/v1/worker/traces/trace-1"), 1);
}

#[tokio::test]
async fn different_cold_traces_load_concurrently_within_the_handoff_budget() {
    let lease = scheduler::Lease::new(Duration::from_secs(3), Duration::from_secs(1)).unwrap();
    let mut first = bound_request("https://example.com/first");
    first.id = "request-1".to_string();
    first.trace_id = "trace-1".to_string();
    let mut second = bound_request("https://example.com/second");
    second.id = "request-2".to_string();
    second.trace_id = "trace-2".to_string();
    let mut first = claimed(net::request::Snapshot::try_from(first).unwrap());
    let mut second = claimed(net::request::Snapshot::try_from(second).unwrap());
    first["trace"] = serde_json::Value::Null;
    second["trace"] = serde_json::Value::Null;

    let (first_reached_tx, first_reached_rx) = std::sync::mpsc::channel();
    let (first_resume_tx, first_resume_rx) = std::sync::mpsc::channel();
    let (second_reached_tx, second_reached_rx) = std::sync::mpsc::channel();
    let (second_resume_tx, second_resume_rx) = std::sync::mpsc::channel();
    let custom_policy = Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 3000,
            "lease_interval_ms": 1000,
            "heartbeat_interval_ms": 10000,
            "max_request_bytes": 67108864
        }),
    );
    let trace = serde_json::to_value(Some(trace::Snapshot::code("task-1"))).unwrap();
    let (base_url, received, server) = concurrent_server(vec![
        custom_policy,
        registered(),
        Response::json("200 OK", json!({"requests": [first, second]})),
        Response::held_json("200 OK", trace.clone(), first_reached_tx, first_resume_rx),
        Response::held_json("200 OK", trace, second_reached_tx, second_resume_rx),
        offline(),
    ]);
    let api = api(base_url, "token").with_lease(lease).unwrap();
    api.open(CONCURRENCY).await.unwrap();

    let claim = tokio::spawn(async move {
        let requests = api.next_requests(2).await;
        (api, requests)
    });
    wait_for_request(first_reached_rx).await;
    wait_for_request(second_reached_rx).await;
    first_resume_tx.send(()).unwrap();
    second_resume_tx.send(()).unwrap();
    let (api, requests) = claim.await.unwrap();
    let requests = requests.unwrap();
    assert_eq!(
        requests
            .iter()
            .map(|request| request.id.as_str())
            .collect::<Vec<_>>(),
        ["request-1", "request-2"]
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/v1/worker/traces/trace-1"), 1);
    assert_eq!(count(&requests, "/v1/worker/traces/trace-2"), 1);
}

#[tokio::test]
async fn a_slow_recovery_failure_does_not_withhold_a_valid_peer() {
    let lease =
        scheduler::Lease::new(Duration::from_millis(300), Duration::from_millis(200)).unwrap();
    let mut valid = bound_request("https://example.com/valid");
    valid.id = "valid-request".to_string();
    let valid = claimed(net::request::Snapshot::try_from(valid).unwrap());
    let mut damaged = bound_request("https://example.com/damaged");
    damaged.id = "damaged-request".to_string();
    let mut damaged = claimed(net::request::Snapshot::try_from(damaged).unwrap());
    damaged["snapshot"] = json!({});

    let (failure_reached_tx, failure_reached_rx) = std::sync::mpsc::channel();
    let (failure_resume_tx, failure_resume_rx) = std::sync::mpsc::channel();
    let custom_policy = Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 300,
            "lease_interval_ms": 200,
            "heartbeat_interval_ms": 10000,
            "max_request_bytes": 67108864
        }),
    );
    let (base_url, received, server) = concurrent_server(vec![
        custom_policy,
        registered(),
        Response::json("200 OK", json!({"requests": [valid, damaged]})),
        Response::empty("204 No Content"),
        Response::held_json(
            "204 No Content",
            serde_json::Value::Null,
            failure_reached_tx,
            failure_resume_rx,
        ),
        offline(),
    ]);
    let api = api(base_url, "token").with_lease(lease).unwrap();
    api.open(CONCURRENCY).await.unwrap();

    let mut claim = tokio::spawn(async move {
        let requests = api.next_requests(2).await;
        (api, requests)
    });
    wait_for_request(failure_reached_rx).await;
    let completed = tokio::time::timeout(Duration::from_millis(160), &mut claim).await;
    let completed_in_budget = completed.is_ok();
    failure_resume_tx.send(()).unwrap();
    let (api, requests) = match completed {
        Ok(result) => result.unwrap(),
        Err(_) => claim.await.unwrap(),
    };
    assert!(
        completed_in_budget,
        "a recovery settlement consumed the Engine handoff budget"
    );
    let requests = requests.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, "valid-request");
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 1);
}

fn count(requests: &[Request], suffix: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.path.ends_with(suffix))
        .count()
}
