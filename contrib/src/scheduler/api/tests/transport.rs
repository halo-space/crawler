use super::*;

#[test]
fn missing_error_id_is_a_protocol_error_not_a_fake_request_id() {
    let error = client::map_error(
        reqwest::StatusCode::CONFLICT,
        br#"{"error":{"code":"version_mismatch","id":null,"field":null,"message":"stale"}}"#,
    );
    assert!(
        matches!(error, scheduler::Error::Message(message) if message.contains("omitted a required request id"))
    );
}
#[test]
fn machine_error_code_keeps_the_master_request_id() {
    let error = client::map_error(
        reqwest::StatusCode::CONFLICT,
        br#"{"error":{"code":"version_mismatch","id":"request-1","field":null,"message":"stale"}}"#,
    );
    assert!(matches!(error, scheduler::Error::VersionMismatch(id) if id == "request-1"));
}

#[test]
fn generic_invalid_request_is_not_a_protocol_error() {
    let error = client::map_error(
        reqwest::StatusCode::BAD_REQUEST,
        br#"{"error":{"code":"invalid_request","id":null,"field":null,"message":"request payload is invalid"}}"#,
    );

    assert!(
        matches!(error, scheduler::Error::Message(message) if message == "request payload is invalid")
    );
}

#[tokio::test]
async fn open_preserves_the_base_path_and_sends_required_headers() {
    let (base_url, received, server) = server(vec![Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
        "max_response_bytes": 67108864
        }),
    )]);
    let api = Api::new(format!("{base_url}/control"), "secret-token")
        .unwrap()
        .with_namespace("crawler")
        .unwrap();

    api.open().await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/control/v1/worker/policy");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer secret-token")
    );
    assert_eq!(
        requests[0]
            .headers
            .get("x-crawler-namespace")
            .map(String::as_str),
        Some("crawler")
    );
}

#[tokio::test]
async fn open_rejects_a_master_response_limit_above_the_client_capacity() {
    let (base_url, received, server) = server(vec![Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
            "max_response_bytes": 1025
        }),
    )]);
    let api = Api::new(base_url, "token")
        .unwrap()
        .with_max_response_bytes(1024)
        .unwrap();

    let error = api.open().await.unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Message(message) if message.contains("exceeds API Scheduler capacity"))
    );

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn master_message_limit_is_applied_before_sending_requests() {
    let (base_url, received, server) = server(vec![Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
            "max_response_bytes": 256
        }),
    )]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();
    let request = bound_request(format!("https://example.com/{}", "segment".repeat(128)));

    let result = api
        .push(payload::Payload::new().requests(vec![request]))
        .await;

    assert!(
        matches!(result, Err(scheduler::Error::Message(message)) if message.contains("request exceeds"))
    );
    api.close().await.unwrap();
    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn trace_id_uses_one_escaped_path_segment() {
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
        Response::json("200 OK", json!(null)),
    ]);
    let api = Api::new(base_url, "token").unwrap();

    api.open().await.unwrap();
    assert!(api.trace("trace/part name?query").await.unwrap().is_none());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].path,
        "/v1/worker/traces/trace%2Fpart%20name%3Fquery"
    );
}

#[tokio::test]
async fn empty_mutation_response_is_accepted() {
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
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();
    assert!(
        api.next_requests(0, "worker-1", &[net::Mode::Http])
            .await
            .unwrap()
            .is_empty()
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(requests[1].path.ends_with("/worker/heartbeat"));
}

#[tokio::test]
async fn worker_registration_is_sent_only_for_first_use_and_mode_changes() {
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
        Response::json("200 OK", json!({})),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    api.next_requests(0, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    api.next_requests(0, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    api.next_requests(0, "worker-1", &[net::Mode::Browser])
        .await
        .unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.path.ends_with("/worker/heartbeat"))
    );
}
