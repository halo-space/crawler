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

#[tokio::test]
async fn mode_updates_wait_for_an_older_heartbeat_before_advertising_new_modes() {
    let (heartbeat_reached, heartbeat_started) = std::sync::mpsc::channel();
    let (resume_heartbeat, heartbeat_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = concurrent_server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 100,
                "max_response_bytes": 67108864
            }),
        ),
        Response::empty("204 No Content"),
        Response::held_json("200 OK", json!({}), heartbeat_reached, heartbeat_continued),
        Response::empty("204 No Content"),
    ]);
    let api = Arc::new(Api::new(base_url, "token").unwrap());
    api.open().await.unwrap();
    api.next_requests(0, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    wait_for_request(heartbeat_started).await;

    let updating = {
        let api = api.clone();
        tokio::spawn(async move {
            api.next_requests(0, "worker-1", &[net::Mode::Browser])
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!updating.is_finished());

    resume_heartbeat.send(()).unwrap();
    updating.await.unwrap().unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 4);
    let heartbeat = serde_json::from_slice::<serde_json::Value>(&requests[2].body).unwrap();
    let update = serde_json::from_slice::<serde_json::Value>(&requests[3].body).unwrap();
    assert_eq!(heartbeat["modes"], json!(["http"]));
    assert_eq!(update["modes"], json!(["browser"]));
}

#[tokio::test]
async fn a_failed_heartbeat_forces_registration_before_the_next_claim() {
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 100,
                "max_response_bytes": 67108864
            }),
        ),
        Response::empty("204 No Content"),
        unavailable("heartbeat unavailable"),
        unavailable("heartbeat unavailable"),
        unavailable("heartbeat unavailable"),
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();
    api.next_requests(0, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    let registration = api
        .runtime
        .workers
        .lock()
        .await
        .get("worker-1")
        .unwrap()
        .registration
        .clone();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if !registration.lock().await.confirmed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    api.next_requests(0, "worker-1", &[net::Mode::Http])
        .await
        .unwrap();
    assert!(registration.lock().await.confirmed);
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 6);
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.path.ends_with("/worker/heartbeat"))
    );
}
