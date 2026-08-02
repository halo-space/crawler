use super::*;

#[tokio::test]
async fn cancelled_open_reuses_the_pending_registration_key() {
    let (reached, registration_started) = std::sync::mpsc::channel();
    let (resume, registration_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        policy(),
        Response::held_json(
            "200 OK",
            json!({"code": 200, "message": "success", "data": "worker-token"}),
            reached,
            registration_continued,
        ),
        policy(),
        registered(),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));

    let opening = {
        let api = api.clone();
        tokio::spawn(async move { api.open(CONCURRENCY).await })
    };
    wait_for_request(registration_started).await;
    opening.abort();
    assert!(opening.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();
    let error = api.open(CONCURRENCY + 1).await.unwrap_err();
    assert!(error.to_string().contains("frozen with concurrency"));

    api.open(CONCURRENCY).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/register");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}

#[tokio::test]
async fn empty_request_submission_is_a_local_no_op() {
    let (base_url, received, server) = server(vec![policy(), registered(), offline()]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();
    api.push(payload::Payload::new()).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 3);
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
            }),
        ),
        registered(),
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
    responses.push(offline());
    let (base_url, received, server) = server(responses);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let first = api.next_requests(1).await;
    assert!(matches!(first, Err(scheduler::Error::Unavailable(_))));
    assert!(api.next_requests(1).await.unwrap().is_empty());
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
async fn init_retry_reuses_the_deterministic_body_key() {
    let mut responses = vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
            }),
        ),
        registered(),
    ];
    responses.extend((0..3).map(|_| unavailable("init unavailable")));
    responses.push(Response::empty("204 No Content"));
    responses.push(offline());
    let (base_url, received, server) = server(responses);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

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
            }),
        ),
        registered(),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();
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
