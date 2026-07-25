use super::*;

#[test]
fn construction_exposes_the_lease_before_open() {
    let lease = scheduler::Lease::new(Duration::from_secs(12), Duration::from_secs(3)).unwrap();
    let api = Api::new("https://master.example.com/control", "token")
        .unwrap()
        .with_namespace("crawler")
        .unwrap()
        .with_lease(lease)
        .unwrap()
        .with_max_response_bytes(1024)
        .unwrap();

    assert_eq!(api.lease(), Some(lease));
}
#[test]
fn construction_rejects_invalid_transport_configuration() {
    assert!(Api::new("file:///tmp/master", "token").is_err());
    assert!(Api::new("https://master.example.com", " ").is_err());
    assert!(Api::new("https://master.example.com", "token\ninjected").is_err());
    assert!(Api::new("https://master.example.com", "令牌").is_err());
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_namespace("bad namespace")
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_namespace("爬虫")
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_namespace("x".repeat(129))
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_max_response_bytes(0)
            .is_err()
    );
}

#[test]
fn execution_identity_is_the_direct_ack_release_and_refresh_body() {
    let mut request = net::Request::follow("https://example.com")
        .unwrap()
        .node("detail");
    request.id = "request-1".to_string();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();
    request.version = 2;
    request.leased_by = "worker-1".to_string();
    let identity =
        wire::Identity::from_payload(&payload::Payload::for_request(&request, "worker-1"));

    assert_eq!(
        serde_json::to_value(identity).unwrap(),
        json!({
            "id": "request-1",
            "task_id": "task-1",
            "trace_id": "trace-1",
            "version": 2,
            "worker_id": "worker-1",
            "node": "detail"
        })
    );
}

#[test]
fn modes_are_canonical_and_worker_validation_is_stable() {
    assert_eq!(
        canonical_modes(
            "worker-1",
            &[net::Mode::Browser, net::Mode::Http, net::Mode::Browser]
        )
        .unwrap(),
        [net::Mode::Http, net::Mode::Browser]
    );
    assert!(canonical_modes(" ", &[net::Mode::Http]).is_err());
    assert!(canonical_modes("worker-1", &[]).is_err());
}

#[test]
fn trace_cache_is_bounded_and_rejects_mutation() {
    let mut cache = TraceCache::new(1, usize::MAX);
    cache
        .insert("trace-1".to_string(), trace::Snapshot::code("task-1"))
        .unwrap();
    cache
        .insert("trace-2".to_string(), trace::Snapshot::code("task-2"))
        .unwrap();
    assert!(cache.get("trace-1").is_none());
    assert!(cache.get("trace-2").is_some());

    assert!(
        cache
            .insert("trace-2".to_string(), trace::Snapshot::code("changed"))
            .is_err()
    );
}

#[test]
fn trace_cache_evicts_by_serialized_byte_budget() {
    let first = trace::Snapshot::code("task-1");
    let second = trace::Snapshot::code("task-2");
    let third = trace::Snapshot::code("task-3");
    let first_bytes = wire::canonical_fingerprint(&first).unwrap().1;
    let second_bytes = wire::canonical_fingerprint(&second).unwrap().1;
    let mut cache = TraceCache::new(3, first_bytes + second_bytes);

    cache.insert("trace-1".to_string(), first).unwrap();
    cache.insert("trace-2".to_string(), second).unwrap();
    assert!(cache.get("trace-1").is_some());
    cache.insert("trace-3".to_string(), third).unwrap();

    assert!(cache.get("trace-1").is_some());
    assert!(cache.get("trace-2").is_none());
    assert!(cache.get("trace-3").is_some());
}

#[test]
fn configuration_is_frozen_while_the_scheduler_is_open() {
    let lease = scheduler::Lease::new(Duration::from_secs(12), Duration::from_secs(3)).unwrap();

    let namespace = Api::new("https://master.example.com", "token").unwrap();
    namespace
        .runtime
        .opened
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(namespace.with_namespace("other").is_err());

    let configured_lease = Api::new("https://master.example.com", "token").unwrap();
    configured_lease
        .runtime
        .opened
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(configured_lease.with_lease(lease).is_err());

    let response_limit = Api::new("https://master.example.com", "token").unwrap();
    response_limit
        .runtime
        .opened
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(response_limit.with_max_response_bytes(1024).is_err());
}
