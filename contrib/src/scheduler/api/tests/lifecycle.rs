use super::*;

#[tokio::test]
async fn open_is_idempotent_and_close_requires_a_new_open() {
    let policy = || {
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
            "max_response_bytes": 67108864
            }),
        )
    };
    let (base_url, received, server) = server(vec![policy(), policy()]);
    let api = Api::new(base_url, "token").unwrap();

    api.open().await.unwrap();
    api.open().await.unwrap();
    api.close().await.unwrap();
    assert!(
        api.next_requests(0, "worker-1", &[net::Mode::Http])
            .await
            .is_err()
    );

    api.open().await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
}
#[tokio::test]
async fn close_waits_for_an_in_flight_trace_and_clears_its_cache() {
    let (reached, request_started) = std::sync::mpsc::channel();
    let (resume, continued) = std::sync::mpsc::channel();
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
        Response::held_json(
            "200 OK",
            serde_json::to_value(trace::Snapshot::code("task-1")).unwrap(),
            reached,
            continued,
        ),
    ]);
    let api = Arc::new(Api::new(base_url, "token").unwrap());
    api.open().await.unwrap();

    let reading = {
        let api = api.clone();
        tokio::spawn(async move { api.trace("trace-1").await })
    };
    wait_for_request(request_started).await;
    let closing = {
        let api = api.clone();
        tokio::spawn(async move { api.close().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!closing.is_finished());

    resume.send(()).unwrap();
    assert!(reading.await.unwrap().unwrap().is_some());
    closing.await.unwrap().unwrap();

    assert!(api.runtime.traces.lock().await.get("trace-1").is_none());
    assert!(api.trace("trace-1").await.is_err());
    assert!(api.runtime.traces.lock().await.get("trace-1").is_none());

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn close_waits_for_an_in_flight_claim_and_blocks_later_claims() {
    let mut request = bound_request("https://example.com/article");
    request.id = "request-1".to_string();
    let snapshot = net::request::Snapshot::try_from(request).unwrap();
    let (reached, request_started) = std::sync::mpsc::channel();
    let (resume, continued) = std::sync::mpsc::channel();
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
        Response::held_json(
            "200 OK",
            json!({"requests": [claimed(snapshot)]}),
            reached,
            continued,
        ),
    ]);
    let api = Arc::new(Api::new(base_url, "token").unwrap());
    api.open().await.unwrap();

    let claiming = {
        let api = api.clone();
        tokio::spawn(async move { api.next_requests(1, "worker-1", &[net::Mode::Http]).await })
    };
    wait_for_request(request_started).await;
    let closing = {
        let api = api.clone();
        tokio::spawn(async move { api.close().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!closing.is_finished());

    resume.send(()).unwrap();
    assert_eq!(claiming.await.unwrap().unwrap().len(), 1);
    closing.await.unwrap().unwrap();
    assert!(
        api.next_requests(1, "worker-1", &[net::Mode::Http])
            .await
            .is_err()
    );

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 3);
}
