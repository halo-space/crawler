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
    let (base_url, received, server) = server(vec![policy(), registered(), offline()]);
    let api = api(format!("{base_url}/control"), "secret-token")
        .with_namespace("crawler")
        .unwrap();

    api.open(CONCURRENCY).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].path, "/control/v1/worker/policy");
    assert_eq!(requests[1].path, "/control/v1/worker/register");
    assert_eq!(requests[2].path, "/control/v1/worker/offline");
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
    assert!(!requests[0].headers.contains_key("traceparent"));
}

#[cfg(feature = "runtime-tracing")]
#[tokio::test]
async fn propagates_one_traceparent_across_api_retries() {
    use fastrace::future::FutureExt as _;
    use fastrace::prelude::SpanContext;

    let (base_url, received, server) = server(vec![
        unavailable("retry"),
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 10000,
            }),
        ),
        registered(),
        Response::json("200 OK", json!(null)),
        offline(),
    ]);
    let api = api(base_url, "token");
    let root = fastrace::Span::root("test.api", SpanContext::random());
    async {
        api.open(CONCURRENCY).await.unwrap();
    }
    .in_span(fastrace::Span::enter_with_parent("test.open", &root))
    .await;
    async {
        api.trace("trace-1").await.unwrap();
        api.close().await.unwrap();
    }
    .in_span(fastrace::Span::enter_with_parent("test.trace", &root))
    .await;

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 5);
    let first = requests[0].headers.get("traceparent").unwrap();
    let second = requests[1].headers.get("traceparent").unwrap();
    assert_eq!(first, second);
    let register = requests[2].headers.get("traceparent").unwrap();
    let trace = requests[3].headers.get("traceparent").unwrap();
    let offline = requests[4].headers.get("traceparent").unwrap();
    let first = SpanContext::decode_w3c_traceparent(first).unwrap();
    let second = SpanContext::decode_w3c_traceparent(second).unwrap();
    let register = SpanContext::decode_w3c_traceparent(register).unwrap();
    let trace = SpanContext::decode_w3c_traceparent(trace).unwrap();
    let offline = SpanContext::decode_w3c_traceparent(offline).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.trace_id, register.trace_id);
    assert_eq!(first.trace_id, trace.trace_id);
    assert_eq!(first.trace_id, offline.trace_id);
    assert_ne!(first.span_id, trace.span_id);
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
            }),
        ),
        registered(),
        Response::json("200 OK", json!(null)),
        offline(),
    ]);
    let api = api(base_url, "token");

    api.open(CONCURRENCY).await.unwrap();
    assert!(api.trace("trace/part name?query").await.unwrap().is_none());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(
        requests[2].path,
        "/v1/worker/traces/trace%2Fpart%20name%3Fquery"
    );
}

#[tokio::test]
async fn open_registers_once_and_requests_use_the_frozen_worker() {
    let (base_url, received, server) = server(vec![
        policy(),
        registered(),
        Response::json("200 OK", json!({"requests": []})),
        Response::json("200 OK", json!({"pending": true})),
        offline(),
    ]);
    let api = api(base_url, "token");

    api.open(CONCURRENCY).await.unwrap();
    api.open(CONCURRENCY).await.unwrap();
    assert!(api.next_requests(1).await.unwrap().is_empty());
    assert!(api.has_pending_requests().await.unwrap());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[1].path, "/v1/worker/register");
    assert_eq!(requests[2].path, "/v1/worker/requests/claim");
    assert_eq!(requests[3].path, "/v1/worker/requests/pending");
    assert_eq!(requests[4].path, "/v1/worker/offline");

    let registration = serde_json::from_slice::<serde_json::Value>(&requests[1].body).unwrap();
    assert_eq!(
        registration,
        json!({
            "worker_id": WORKER_ID,
            "host": WORKER_HOST,
            "version": WORKER_VERSION,
            "modes": ["http"],
            "concurrency": CONCURRENCY
        })
    );
    let claim = serde_json::from_slice::<serde_json::Value>(&requests[2].body).unwrap();
    assert_eq!(
        claim,
        json!({"limit": 1, "worker_id": WORKER_ID, "modes": ["http"]})
    );
    let pending = serde_json::from_slice::<serde_json::Value>(&requests[3].body).unwrap();
    assert_eq!(pending, json!({"worker_id": WORKER_ID, "modes": ["http"]}));
    let offline = serde_json::from_slice::<serde_json::Value>(&requests[4].body).unwrap();
    assert_eq!(
        offline,
        json!({"worker_id": WORKER_ID, "token": "worker-token"})
    );
}

#[tokio::test]
async fn worker_conflict_fails_open_without_starting_a_lifecycle() {
    let (base_url, received, server) = server(vec![
        policy(),
        Response::json(
            "200 OK",
            json!({"code": 100, "message": "worker_id is already online", "data": null}),
        ),
    ]);
    let api = api(base_url, "token");

    let error = api.open(CONCURRENCY).await.unwrap_err();
    assert!(matches!(error, scheduler::Error::Message(message) if message.contains("code 100")));
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        !api.runtime
            .opened
            .load(std::sync::atomic::Ordering::Acquire)
    );
    assert!(api.runtime.heartbeat.lock().unwrap().is_none());
    assert!(api.runtime.token.lock().unwrap().is_none());
}

#[tokio::test]
async fn register_requires_a_non_empty_server_token() {
    let (base_url, received, server) = server(vec![policy(), worker_ok(json!(null))]);
    let api = api(base_url, "token");

    let error = api.open(CONCURRENCY).await.unwrap_err();
    assert!(
        matches!(error, scheduler::Error::Message(message) if message.contains("non-empty token"))
    );

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(requests.len(), 2);
}

#[tokio::test]
async fn register_retries_reuse_one_idempotency_key() {
    let (base_url, received, server) = server(vec![
        policy(),
        unavailable("register unavailable"),
        registered(),
        offline(),
    ]);
    let api = api(base_url, "token");

    api.open(CONCURRENCY).await.unwrap();
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    let keys = operation_keys(&requests, "/v1/worker/register");
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
}

#[tokio::test]
async fn heartbeat_failure_pauses_claim_and_recovery_resumes_it() {
    let (base_url, received, server) = server(vec![
        Response::json(
            "200 OK",
            json!({
                "lease_timeout_ms": 30000,
                "lease_interval_ms": 10000,
                "heartbeat_interval_ms": 200,
            }),
        ),
        registered(),
        unavailable("heartbeat unavailable"),
        worker_ok(json!(null)),
        Response::json("200 OK", json!({"requests": []})),
        offline(),
    ]);
    let api = api(base_url, "token");
    api.open(CONCURRENCY).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        while api
            .runtime
            .can_claim
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(api.next_requests(1).await.unwrap().is_empty());

    tokio::time::timeout(Duration::from_secs(2), async {
        while !api
            .runtime
            .can_claim
            .load(std::sync::atomic::Ordering::Acquire)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(api.next_requests(1).await.unwrap().is_empty());
    api.close().await.unwrap();

    let requests = received.recv().unwrap();
    server.join().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/worker/register"))
            .count(),
        1
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/worker/heartbeat"))
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path.ends_with("/requests/claim"))
            .count(),
        1
    );
    let heartbeat = requests
        .iter()
        .find(|request| request.path.ends_with("/worker/heartbeat"))
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&heartbeat.body).unwrap(),
        json!({"worker_id": WORKER_ID, "token": "worker-token"})
    );
}
