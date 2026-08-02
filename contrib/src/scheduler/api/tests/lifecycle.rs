use super::*;

#[tokio::test]
async fn open_is_idempotent_and_close_requires_a_new_open() {
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        offline(),
        policy(),
        worker_ok(json!("worker-token-2")),
        offline(),
    ]);
    let api = api(base_url, "token");

    api.open(CONCURRENCY).await.unwrap();
    api.open(CONCURRENCY).await.unwrap();
    api.close().await.unwrap();
    assert!(api.next_requests(0).await.is_err());

    api.open(CONCURRENCY).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 6);
    let offline = requests
        .iter()
        .filter(|request| request.path.ends_with("/worker/offline"))
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(offline[0]["token"], "worker-token");
    assert_eq!(offline[1]["token"], "worker-token-2");
}

#[tokio::test]
async fn open_rejects_a_different_concurrency_until_close() {
    let (base_url, received, server) = server(vec![policy(), registered(), offline()]);
    let api = api(base_url, "token");

    api.open(CONCURRENCY).await.unwrap();
    let error = api.open(CONCURRENCY + 1).await.unwrap_err();
    assert!(error.to_string().contains("already open with concurrency"));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 3);
}

#[tokio::test]
async fn drop_freezes_claims_and_aborts_heartbeat_without_offline() {
    let (base_url, received, server) = server(vec![policy(), registered()]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    let runtime = api.runtime.clone();
    let epoch = runtime.epoch.load(std::sync::atomic::Ordering::Acquire);
    let heartbeat = runtime
        .heartbeat
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .task
        .abort_handle();

    drop(api);

    assert!(!runtime.opened.load(std::sync::atomic::Ordering::Acquire));
    assert!(!runtime.can_claim.load(std::sync::atomic::Ordering::Acquire));
    assert_ne!(
        runtime.epoch.load(std::sync::atomic::Ordering::Acquire),
        epoch
    );
    assert!(runtime.heartbeat.lock().unwrap().is_none());
    assert!(runtime.token.lock().unwrap().is_none());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped API Scheduler heartbeat task did not stop");

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| !request.path.ends_with("/worker/offline"))
    );
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
            }),
        ),
        registered(),
        Response::held_json(
            "200 OK",
            serde_json::to_value(trace::Snapshot::code("task-1")).unwrap(),
            reached,
            continued,
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

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
    assert_eq!(requests.len(), 4);
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
            }),
        ),
        registered(),
        Response::held_json(
            "200 OK",
            json!({"requests": [claimed(snapshot)]}),
            reached,
            continued,
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

    let claiming = {
        let api = api.clone();
        tokio::spawn(async move { api.next_requests(1).await })
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
    assert!(api.next_requests(1).await.is_err());

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 4);
}

#[tokio::test]
async fn close_waits_for_an_in_flight_heartbeat_before_offline() {
    let (reached, heartbeat_started) = std::sync::mpsc::channel();
    let (resume, heartbeat_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 50,
            }),
        ),
        registered(),
        Response::held_json(
            "200 OK",
            json!({"code": 200, "message": "success", "data": null}),
            reached,
            heartbeat_continued,
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();
    wait_for_request(heartbeat_started).await;

    let closing = {
        let api = api.clone();
        tokio::spawn(async move { api.close().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!closing.is_finished());

    resume.send(()).unwrap();
    closing.await.unwrap().unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].path.ends_with("/worker/heartbeat"));
    assert!(requests[3].path.ends_with("/worker/offline"));
}

#[tokio::test]
async fn cancelled_close_keeps_the_token_for_another_offline_attempt() {
    let (reached, offline_started) = std::sync::mpsc::channel();
    let (resume, offline_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::held_json(
            "200 OK",
            json!({"code": 200, "message": "success", "data": null}),
            reached,
            offline_continued,
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

    let closing = {
        let api = api.clone();
        tokio::spawn(async move { api.close().await })
    };
    wait_for_request(offline_started).await;
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    assert_eq!(api.runtime.token(), Some("worker-token".to_string()));
    resume.send(()).unwrap();

    api.close().await.unwrap();
    assert!(api.runtime.token().is_none());

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let offline = requests
        .iter()
        .filter(|request| request.path.ends_with("/worker/offline"))
        .map(|request| serde_json::from_slice::<serde_json::Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(offline.len(), 2);
    assert_eq!(offline[0]["token"], "worker-token");
    assert_eq!(offline[1]["token"], "worker-token");
}

#[tokio::test]
async fn cancelled_close_during_heartbeat_cannot_reenable_claims() {
    let (reached, heartbeat_started) = std::sync::mpsc::channel();
    let (resume, heartbeat_continued) = std::sync::mpsc::channel();
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 50,
            }),
        ),
        registered(),
        Response::held_json(
            "200 OK",
            json!({"code": 200, "message": "success", "data": null}),
            reached,
            heartbeat_continued,
        ),
        offline(),
    ]);
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();
    wait_for_request(heartbeat_started).await;
    let heartbeat = api
        .runtime
        .heartbeat
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .task
        .abort_handle();

    let closing = {
        let api = api.clone();
        tokio::spawn(async move { api.close().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    assert!(
        !api.runtime
            .can_claim
            .load(std::sync::atomic::Ordering::Acquire)
    );

    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stale heartbeat task did not stop after close cancellation");
    assert!(
        !api.runtime
            .can_claim
            .load(std::sync::atomic::Ordering::Acquire)
    );

    api.close().await.unwrap();
    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert!(requests[2].path.ends_with("/worker/heartbeat"));
    assert!(requests[3].path.ends_with("/worker/offline"));
}
