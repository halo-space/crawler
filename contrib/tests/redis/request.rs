use spider::net;

use super::run::{TASK_ID, TRACE_ID};

pub(super) fn new(id: &str, url: &str) -> net::Request {
    for_trace(id, url, TASK_ID, TRACE_ID)
}

pub(super) fn for_trace(id: &str, url: &str, task_id: &str, trace_id: &str) -> net::Request {
    let mut request = net::Request::follow(url).unwrap().with_id(id);
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request
}
