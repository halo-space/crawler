use super::*;

#[test]
fn construction_exposes_the_lease_before_open() {
    let lease = scheduler::Lease::new(Duration::from_secs(12), Duration::from_secs(3)).unwrap();
    let api = api("https://master.example.com/control", "token")
        .with_namespace("crawler")
        .unwrap()
        .with_lease(lease)
        .unwrap();

    assert_eq!(api.lease(), Some(lease));
    assert!(api.worker.validate(CONCURRENCY).is_ok());
}
#[test]
fn construction_rejects_invalid_transport_configuration() {
    assert!(Api::new("file:///tmp/master", "token").is_err());
    assert!(Api::new("https://user@master.example.com", "token").is_err());
    assert!(Api::new("https://user:password@master.example.com", "token").is_err());
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
}

#[test]
fn execution_lease_is_the_direct_ack_release_and_refresh_body() {
    let mut request = net::Request::follow("https://example.com")
        .unwrap()
        .node("detail");
    request.id = "request-1".to_string();
    request.task_id = "task-1".to_string();
    request.trace_id = "trace-1".to_string();
    request.version = 2;
    request.leased_by = "worker-1".to_string();
    let identity =
        wire::Lease::from_payload(&payload::Payload::for_request(&request, "reported-worker"));

    assert_eq!(
        serde_json::to_value(identity).unwrap(),
        json!({
            "id": "request-1",
            "task_id": "task-1",
            "trace_id": "trace-1",
            "version": 2,
            "node": "detail"
        })
    );
}

#[test]
fn worker_builders_validate_and_canonicalize_configuration() {
    let api = Api::new("https://master.example.com", "token")
        .unwrap()
        .with_worker_id(WORKER_ID)
        .unwrap()
        .with_worker_host(WORKER_HOST)
        .unwrap()
        .with_worker_version(WORKER_VERSION)
        .unwrap()
        .with_modes([net::Mode::Browser, net::Mode::Http, net::Mode::Browser])
        .unwrap();

    assert!(api.worker.validate(CONCURRENCY).is_ok());
    assert_eq!(api.worker.modes(), [net::Mode::Http, net::Mode::Browser]);
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_worker_id(" ")
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_worker_host(" ")
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_worker_version(" ")
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .with_modes([])
            .is_err()
    );
    assert!(
        Api::new("https://master.example.com", "token")
            .unwrap()
            .worker
            .validate(CONCURRENCY)
            .is_err()
    );
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

    let worker = Api::new("https://master.example.com", "token").unwrap();
    worker
        .runtime
        .opened
        .store(true, std::sync::atomic::Ordering::Release);
    assert!(worker.with_worker_id(WORKER_ID).is_err());

    let pending_registration = Api::new("https://master.example.com", "token").unwrap();
    pending_registration.runtime.open_key(CONCURRENCY).unwrap();
    assert!(pending_registration.with_namespace("other").is_err());

    let pending_offline = Api::new("https://master.example.com", "token").unwrap();
    pending_offline
        .runtime
        .set_token("worker-token".to_string());
    assert!(pending_offline.with_worker_id("other-worker").is_err());
}
