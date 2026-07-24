use base64::Engine as _;

pub(super) fn token(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

pub(super) fn request(namespace: &str, id: &str) -> String {
    format!("{namespace}:request:{}", token(id))
}

pub(super) fn processing(namespace: &str, mode: &str) -> String {
    format!("{namespace}:processing:{mode}")
}

pub(super) fn completion(namespace: &str, id: &str, version: i64) -> String {
    format!("{}:completion:{version}", request(namespace, id))
}

pub(super) fn stats(namespace: &str, trace_id: &str) -> String {
    format!("{namespace}:trace:{}:stats", token(trace_id))
}
