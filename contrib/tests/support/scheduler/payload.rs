use spider::{net, payload};

pub(super) const TASK_ID: &str = "conformance-task";
pub(super) const TRACE_ID: &str = "conformance-trace";

pub(crate) fn request(id: &str, url: &str) -> net::Request {
    owned_request(id, url, TASK_ID, TRACE_ID)
}

pub(crate) fn owned_request(id: &str, url: &str, task_id: &str, trace_id: &str) -> net::Request {
    let mut request = net::Request::follow(url).unwrap().with_id(id);
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request
}

pub(crate) fn processing(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    payload
}

pub(crate) fn success(request: &net::Request) -> payload::Payload {
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
