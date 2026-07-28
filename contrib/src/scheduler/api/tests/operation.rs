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
                "max_response_bytes": 67108864
            }),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
    ]);
    let api = Arc::new(Api::new(base_url, "token").unwrap());
    api.open().await.unwrap();

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
                "max_response_bytes": 67108864
            }),
        ),
        Response::empty("204 No Content"),
        Response::empty("204 No Content"),
    ]);
    let api = Api::new(base_url, "token").unwrap();
    api.open().await.unwrap();

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

#[test]
fn invocation_keys_are_always_fresh() {
    assert_ne!(Api::invocation_key(), Api::invocation_key());
}
