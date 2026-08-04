use base64::Engine as _;

pub(super) fn segment(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

pub(super) fn request(namespace: &str, id: &str) -> String {
    format!("{namespace}:request:{}", segment(id))
}

pub(super) fn processing(namespace: &str, mode: &str) -> String {
    format!("{namespace}:processing:{mode}")
}

pub(super) fn completion(namespace: &str, id: &str, version: i64) -> String {
    format!("{}:completion:{version}", request(namespace, id))
}

pub(super) fn failed_workers(namespace: &str, id: &str) -> String {
    format!("{}:failed_workers", request(namespace, id))
}

pub(super) fn stats(namespace: &str, trace_id: &str) -> String {
    format!("{namespace}:trace:{}:stats", segment(trace_id))
}

pub(super) fn worker(namespace: &str, worker_id: &str) -> String {
    format!("{namespace}:worker:{}", segment(worker_id))
}
