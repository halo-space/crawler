use base64::Engine as _;
use contrib::scheduler::redis::Redis;
use redis::streams::StreamRangeReply;
use spider::{Scheduler, item, net, payload};

use crate::redis_fixture::Fixture;

pub(super) const EXACT_INTEGER: i64 = 9_007_199_254_740_993;
pub(super) const HTTP: &[net::Mode] = &[net::Mode::Http];
pub(super) const WORKER_A: &str = "worker-a";
pub(super) const WORKER_B: &str = "worker-b";

pub(super) fn scheduler(fixture: &Fixture, namespace: &str) -> Redis {
    Redis::new(fixture.url())
        .unwrap()
        .with_namespace(namespace)
        .unwrap()
}

pub(super) fn request(id: &str, url: &str) -> net::Request {
    net::Request::follow(url).unwrap().with_id(id)
}

pub(super) fn owned_request(id: &str, url: &str, task_id: &str, trace_id: &str) -> net::Request {
    let mut request = request(id, url);
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request
}

pub(super) fn processing_payload(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    payload
}

pub(super) fn success_payload(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

pub(super) fn namespace(label: &str) -> String {
    format!(
        "crawler-test-redis-{label}-{}",
        uuid::Uuid::now_v7().simple()
    )
}

pub(super) fn token(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

pub(super) fn request_key(namespace: &str, id: &str) -> String {
    format!("{namespace}:request:{}", token(id))
}

pub(super) fn processing_key(namespace: &str, mode: &str) -> String {
    format!("{namespace}:processing:{mode}")
}

pub(super) fn completion_key(namespace: &str, id: &str, version: i64) -> String {
    format!("{}:completion:{version}", request_key(namespace, id))
}

pub(super) fn stats_key(namespace: &str, trace_id: &str) -> String {
    format!("{namespace}:trace:{}:stats", token(trace_id))
}

pub(super) fn item(value: &str) -> Box<dyn item::Item> {
    let mut item = item::Map::new(item::Values::from([("value".to_string(), value.into())]));
    *item::Item::state_mut(&mut item).id_mut() = format!("item-{value}");
    Box::new(item)
}

pub(super) async fn stream_payloads(fixture: &Fixture, namespace: &str) -> Vec<serde_json::Value> {
    let mut connection = fixture.connection().await;
    redis::cmd("XRANGE")
        .arg(format!("{namespace}:items"))
        .arg("-")
        .arg("+")
        .query_async::<StreamRangeReply>(&mut connection)
        .await
        .unwrap()
        .ids
        .into_iter()
        .map(|record| {
            let payload = record
                .get::<String>("payload")
                .expect("Item Stream entry must contain a payload field");
            serde_json::from_str(&payload).expect("Item Stream payload must be valid JSON")
        })
        .collect()
}

pub(super) async fn succeed(scheduler: &Redis, request: &net::Request) {
    scheduler.ack(&processing_payload(request)).await.unwrap();
    scheduler.success(&success_payload(request)).await.unwrap();
}
