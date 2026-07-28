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
        Response::json("200 OK", json!({})),
        Response::json(
            "200 OK",
            json!({"requests": [valid, damaged_request, damaged_trace]}),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let requests = api
        .next_requests(3, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].id, "valid-request");
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 2);
    assert_eq!(count(&requests, "/requests/failure"), 2);
    assert_eq!(count(&requests, "/requests/release"), 0);
    let failed = requests
        .iter()
        .filter(|request| request.path.ends_with("/requests/failure"))
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(failed[0]["identity"]["id"], "damaged-request");
    assert_eq!(failed[1]["identity"]["id"], "damaged-trace");
    assert_eq!(failed[0]["identity"]["version"], 1);
    assert_eq!(failed[0]["identity"]["worker_id"], "worker-1");
}

#[tokio::test]
async fn a_duplicate_claimed_request_is_rejected_and_released_once() {
    let mut request = bound_request("https://example.com/valid");
    request.id = "request-1".to_string();
    let snapshot = net::request::Snapshot::try_from(request).unwrap();
    let (base_url, received, server) = server(vec![
        policy(),
        Response::json("200 OK", json!({})),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(snapshot.clone()), claimed(snapshot)]}),
        ),
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let error = api
        .next_requests(2, "worker-1", &[net::Mode::Http])
        .await
        .unwrap_err();
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
        Response::json("200 OK", json!({})),
        Response::json(
            "200 OK",
            json!({"requests": [
                claimed(net::request::Snapshot::try_from(first).unwrap()),
                claimed(net::request::Snapshot::try_from(second).unwrap())
            ]}),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let error = api
        .next_requests(1, "worker-1", &[net::Mode::Http])
        .await
        .unwrap_err();
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
        Response::json("200 OK", json!({})),
        Response::json("200 OK", json!({"requests": [valid, damaged]})),
        Response::empty("204 No Content"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let requests = api
        .next_requests(2, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
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
        Response::json("200 OK", json!({})),
        Response::json("200 OK", json!({"requests": [damaged]})),
        Response::empty("204 No Content"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
        unavailable("failure unavailable"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let error = api
        .next_requests(1, "worker-1", &[net::Mode::Http])
        .await
        .unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Unavailable(message) if message.contains("failed to settle its recovery"))
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(count(&requests, "/requests/ack"), 1);
    assert_eq!(count(&requests, "/requests/failure"), 3);
}

fn policy() -> Response {
    Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
            "max_response_bytes": 67108864
        }),
    )
}

fn count(requests: &[Request], suffix: &str) -> usize {
    requests
        .iter()
        .filter(|request| request.path.ends_with(suffix))
        .count()
}
