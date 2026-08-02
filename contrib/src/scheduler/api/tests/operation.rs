use super::*;

#[tokio::test]
async fn identical_init_bodies_share_a_key_across_tasks() {
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
    let api = Arc::new(api(base_url, "token"));
    api.open(CONCURRENCY).await.unwrap();

    let first = {
        let api = api.clone();
        tokio::spawn(async move {
            api.init(
                "trace-1".to_string(),
                trace::Snapshot::code("task-1"),
                Vec::new(),
            )
            .await
        })
    };
    let second = {
        let api = api.clone();
        tokio::spawn(async move {
            api.init(
                "trace-1".to_string(),
                trace::Snapshot::code("task-1"),
                Vec::new(),
            )
            .await
        })
    };
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/runs/init");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    assert!(keys[0].starts_with("init-"));
}

#[tokio::test]
async fn different_init_bodies_use_different_keys() {
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

    api.init(
        "trace-1".to_string(),
        trace::Snapshot::code("task-1"),
        Vec::new(),
    )
    .await
    .unwrap();
    api.init(
        "trace-2".to_string(),
        trace::Snapshot::code("task-1"),
        Vec::new(),
    )
    .await
    .unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/runs/init");
    assert_eq!(keys.len(), 2);
    assert_ne!(keys[0], keys[1]);
}
