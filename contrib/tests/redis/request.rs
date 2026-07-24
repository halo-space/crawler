use spider::net;

pub(super) fn new(id: &str, url: &str) -> net::Request {
    net::Request::follow(url).unwrap().with_id(id)
}

pub(super) fn for_trace(id: &str, url: &str, task_id: &str, trace_id: &str) -> net::Request {
    let mut request = new(id, url);
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request
}
