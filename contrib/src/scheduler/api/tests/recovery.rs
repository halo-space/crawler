use super::*;

#[tokio::test]
async fn a_failed_restore_releases_the_entire_claimed_collection() {
    let mut request = bound_request("https://example.com/valid");
    request.id = "valid-request".to_string();
    let valid = net::request::Snapshot::try_from(request).unwrap();
    let mut invalid = valid.clone();
    invalid.id.clear();
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
            "max_response_bytes": 67108864
            }),
        ),
        Response::json("200 OK", json!({})),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(valid), claimed(invalid)]}),
        ),
        Response::empty("204 No Content"),
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

    let received = received.recv().unwrap();
    server.join().unwrap();
    let releases = received
        .iter()
        .filter(|request| request.path.ends_with("/requests/release"))
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 2);
    assert_ne!(
        releases[0].headers.get("idempotency-key"),
        releases[1].headers.get("idempotency-key")
    );
}
#[tokio::test]
async fn a_duplicate_claimed_request_is_rejected_and_released_once() {
    let mut request = bound_request("https://example.com/valid");
    request.id = "request-1".to_string();
    let snapshot = net::request::Snapshot::try_from(request).unwrap();
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
                "max_response_bytes": 67108864
            }),
        ),
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
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/requests/release"))
            .count(),
        1
    );
}

#[tokio::test]
async fn a_release_failure_reports_both_errors_and_reuses_only_its_retry_key() {
    let mut request = bound_request("https://example.com/valid");
    request.id = "valid-request".to_string();
    let valid = net::request::Snapshot::try_from(request).unwrap();
    let mut invalid = valid.clone();
    invalid.id.clear();
    let unavailable = || {
        Response::json(
            "503 Service Unavailable",
            json!({"error": {
                "code": "unavailable",
                "id": null,
                "field": null,
                "message": "release unavailable"
            }}),
        )
    };
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
                "max_response_bytes": 67108864
            }),
        ),
        Response::json("200 OK", json!({})),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(valid), claimed(invalid)]}),
        ),
        Response::empty("204 No Content"),
        unavailable(),
        unavailable(),
        unavailable(),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let error = api
        .next_requests(2, "worker-1", &[net::Mode::Http])
        .await
        .unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Unavailable(message) if message.contains("failed to restore claimed Request collection") && message.contains("failed to release the collection"))
    );
    api.close().await.unwrap();

    let received = received.recv().unwrap();
    server.join().unwrap();
    let releases = received
        .iter()
        .filter(|request| request.path.ends_with("/requests/release"))
        .collect::<Vec<_>>();
    assert_eq!(releases.len(), 4);
    let first = releases[0].headers.get("idempotency-key").unwrap();
    let second = releases[1].headers.get("idempotency-key").unwrap();
    assert_ne!(first, second);
    assert!(
        releases[1..]
            .iter()
            .all(|request| request.headers.get("idempotency-key").unwrap() == second)
    );
}
