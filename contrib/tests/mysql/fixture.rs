use std::time::Duration;

use contrib::scheduler::mysql::MySql;
use spider::scheduler::Init as _;
use spider::{Scheduler as _, net, payload, trace};

pub(super) const TASK_ID: &str = "mysql-test-task";
pub(super) const TRACE_ID: &str = "mysql-test-trace";

pub(super) fn scheduler(url: &str, worker_id: &str) -> MySql {
    MySql::new(url)
        .unwrap()
        .with_worker_id(worker_id)
        .unwrap()
        .with_worker_host("mysql-test-host")
        .unwrap()
        .with_worker_version("test")
        .unwrap()
        .with_modes([net::Mode::Http, net::Mode::Browser])
        .unwrap()
}

pub(super) fn scheduler_with_heartbeat(
    url: &str,
    worker_id: &str,
    interval: Duration,
    timeout: Duration,
) -> MySql {
    scheduler(url, worker_id)
        .with_heartbeat(interval, timeout)
        .unwrap()
}

pub(super) async fn open(scheduler: &MySql) {
    scheduler.open(16).await.unwrap();
}

pub(super) async fn init(scheduler: &MySql) {
    scheduler
        .init(
            TRACE_ID.to_string(),
            trace::Snapshot::code(TASK_ID),
            Vec::new(),
        )
        .await
        .unwrap();
}

pub(super) fn request(id: &str) -> net::Request {
    let mut request = net::Request::follow(format!("https://example.com/{id}"))
        .unwrap()
        .with_id(id);
    request.task_id = TASK_ID.to_string();
    request.trace_id = TRACE_ID.to_string();
    request
}

pub(super) fn processing(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    payload
}

pub(super) fn success(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

pub(super) fn failure(request: &net::Request, error: &str) -> payload::Payload {
    let mut payload =
        payload::Payload::for_request(request, request.leased_by.clone()).failed(error);
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}
