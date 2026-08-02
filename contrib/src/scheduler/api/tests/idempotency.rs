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

#[test]
fn pending_claim_keeps_its_original_key_and_start_time() {
    let runtime = super::super::state::Runtime::new(1, 1024);
    let claim = wire::Claim {
        limit: 1,
        worker_id: WORKER_ID.to_string(),
        modes: vec![net::Mode::Http],
    };

    let (first_key, first_started) = runtime.claim_operation(&claim).unwrap();
    let (retry_key, retry_started) = runtime.claim_operation(&claim).unwrap();
    assert_eq!(retry_key, first_key);
    assert_eq!(retry_started, first_started);

    runtime.confirm_claim(&first_key);
    let (next_key, next_started) = runtime.claim_operation(&claim).unwrap();
    assert_ne!(next_key, first_key);
    assert!(next_started >= first_started);
}

#[tokio::test]
async fn expired_release_key_is_not_retained() {
    let runtime = super::super::state::Runtime::new(1, 1024);
    let lease = wire::Lease {
        id: "request-1".to_string(),
        task_id: "task-1".to_string(),
        trace_id: "trace-1".to_string(),
        version: 1,
        node: "index".to_string(),
    };
    let expires = tokio::time::Instant::now() + Duration::from_millis(1);
    let first = runtime.release_key(&lease, expires);
    assert_eq!(runtime.release_key(&lease, expires), first);

    tokio::time::sleep(Duration::from_millis(2)).await;
    let next = runtime.release_key(&lease, tokio::time::Instant::now() + Duration::from_secs(1));
    assert_ne!(next, first);
}

#[tokio::test]
async fn claim_reuses_a_key_until_a_response_is_confirmed() {
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
    assert!(claim_keys.iter().all(|key| key == &claim_keys[0]));
}

#[tokio::test]
async fn invalid_claim_response_reuses_the_key_until_the_response_is_decoded() {
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response {
            status: "200 OK",
            body: b"not-json".to_vec(),
            wait: None,
        },
        Response::json("200 OK", json!({"requests": []})),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    assert!(matches!(
        api.next_requests(1).await,
        Err(scheduler::Error::Unavailable(_))
    ));
    assert!(api.next_requests(1).await.unwrap().is_empty());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/requests/claim");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}

#[tokio::test]
async fn cancelled_claim_reuses_the_key_on_the_next_call() {
    let (reached, claim_started) = std::sync::mpsc::channel();
    let (resume, claim_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::held_json("200 OK", json!({"requests": []}), reached, claim_continued),
        Response::json("200 OK", json!({"requests": []})),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

    let claiming = {
        let api = api.clone();
        tokio::spawn(async move { api.next_requests(1).await })
    };
    wait_for_request(claim_started).await;
    claiming.abort();
    assert!(claiming.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();

    assert!(api.next_requests(1).await.unwrap().is_empty());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/requests/claim");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}

#[tokio::test]
async fn concurrent_claims_complete_in_order_with_independent_keys() {
    let mut first = bound_request("https://example.com/first");
    first.id = "request-1".to_string();
    let mut second = bound_request("https://example.com/second");
    second.id = "request-2".to_string();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(net::request::Snapshot::try_from(first).unwrap())]}),
        ),
        Response::json(
            "200 OK",
            json!({"requests": [claimed(net::request::Snapshot::try_from(second).unwrap())]}),
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

    let (left, right) = tokio::join!(api.next_requests(1), api.next_requests(1));
    let ids = left
        .unwrap()
        .into_iter()
        .chain(right.unwrap())
        .map(|request| request.id)
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        ids,
        ["request-1".to_string(), "request-2".to_string()].into()
    );
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/requests/claim");
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
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

#[tokio::test]
async fn ambiguous_release_reuses_its_key_until_the_result_is_confirmed() {
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response {
            status: "200 OK",
            body: b"not-json".to_vec(),
            wait: None,
        },
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

    assert!(matches!(
        api.release(&payload).await,
        Err(scheduler::Error::Unavailable(_))
    ));
    api.release(&payload).await.unwrap();
    api.release(&payload).await.unwrap();
    api.close().await.unwrap();

    let received = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&received, "/v1/worker/requests/release");
    assert_eq!(keys.len(), 3);
    assert_eq!(keys[0], keys[1]);
    assert_ne!(keys[1], keys[2]);
}

#[tokio::test]
async fn cancelled_release_reuses_the_key_on_the_next_call() {
    let (reached, release_started) = std::sync::mpsc::channel();
    let (resume, release_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::held_json(
            "204 No Content",
            serde_json::Value::Null,
            reached,
            release_continued,
        ),
        Response::empty("204 No Content"),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();
    let mut request = net::Request::follow("https://example.com").unwrap();
    request.id = "request-1".to_string();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();
    request.version = 1;
    let mut payload = payload::Payload::for_request(&request, "worker-1");
    payload.state = net::State::Processing;
    let payload = Arc::new(payload);

    let releasing = {
        let api = api.clone();
        let payload = payload.clone();
        tokio::spawn(async move { api.release(&payload).await })
    };
    wait_for_request(release_started).await;
    releasing.abort();
    assert!(releasing.await.unwrap_err().is_cancelled());
    resume.send(()).unwrap();

    api.release(&payload).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/requests/release");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}
