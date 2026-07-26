use super::*;

#[tokio::test]
async fn empty_request_submission_is_a_local_no_op() {
    let (base_url, received, server) = server(vec![Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
            "max_response_bytes": 67108864
        }),
    )]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();
    api.push(payload::Payload::new()).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 1);
}
#[tokio::test]
async fn claim_reuses_a_key_only_for_automatic_http_retries() {
    let mut responses = vec![
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
    ];
    responses.extend((0..3).map(|_| {
        Response::json(
            "503 Service Unavailable",
            json!({"error": {
                "code": "unavailable",
                "id": null,
                "field": null,
                "message": "offline"
            }}),
        )
    }));
    responses.push(Response::json("200 OK", json!({"requests": []})));
    let (base_url, received, server) = server(responses);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

    let first = api.next_requests(1, "worker-1", &[net::Mode::Http]).await;
    assert!(matches!(first, Err(scheduler::Error::Unavailable(_))));
    assert!(
        api.next_requests(1, "worker-1", &[net::Mode::Http])
            .await
            .unwrap()
            .is_empty()
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let claim_keys = requests
        .iter()
        .filter(|request| request.path.ends_with("/requests/claim"))
        .map(|request| {
            request
                .headers
                .get("idempotency-key")
                .expect("claim request must carry an idempotency key")
                .clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(claim_keys.len(), 4);
    assert!(claim_keys[..3].iter().all(|key| key == &claim_keys[0]));
    assert_ne!(claim_keys[3], claim_keys[0]);
}

#[tokio::test]
async fn init_retry_reuses_the_unresolved_logical_operation_key() {
    let mut responses = vec![Response::json(
        "200 OK",
        json!({
            "lease_timeout_ms": 30000,
            "lease_interval_ms": 10000,
            "heartbeat_interval_ms": 10000,
            "max_response_bytes": 67108864
        }),
    )];
    responses.extend((0..3).map(|_| unavailable("init unavailable")));
    responses.push(Response::empty("204 No Content"));
    let (base_url, received, server) = server(responses);
    let api = Arc::new(Api::new(base_url, "token").unwrap());
    api.open().await.unwrap();

    let retrying = {
        let api = api.clone();
        tokio::spawn(async move {
            let first = api
                .init(
                    "trace-1".to_string(),
                    trace::Snapshot::code("task-1"),
                    Vec::new(),
                )
                .await;
            assert!(matches!(first, Err(scheduler::Error::Unavailable(_))));
            api.init(
                "trace-1".to_string(),
                trace::Snapshot::code("task-1"),
                Vec::new(),
            )
            .await
            .unwrap();
        })
    };
    retrying.await.unwrap();
    api.close().await.unwrap();

    let received = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&received, "/v1/worker/runs/init");
    assert_eq!(keys.len(), 4);
    assert!(keys.iter().all(|key| key == &keys[0]));
}

#[tokio::test]
async fn independent_release_invocations_use_fresh_keys() {
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
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.id = "request-1".to_string();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();
    request.version = 1;
    let mut payload = payload::Payload::for_request(&request, "worker-1");
    payload.state = net::State::Processing;

    api.release(&payload).await.unwrap();
    api.release(&payload).await.unwrap();
    api.close().await.unwrap();

    let received = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&received, "/v1/worker/requests/release");
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
}
