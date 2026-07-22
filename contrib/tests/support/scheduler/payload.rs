use spider::{item, net, payload};

pub(crate) fn request(id: &str, url: &str) -> net::Request {
    net::Request::follow(url).unwrap().with_id(id)
}

pub(crate) fn owned_request(id: &str, url: &str, task_id: &str, trace_id: &str) -> net::Request {
    let mut request = request(id, url);
    request.task_id = task_id.to_string();
    request.trace_id = trace_id.to_string();
    request
}

pub(crate) fn processing_payload(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.state = net::State::Processing;
    payload
}

pub(crate) fn success_payload(request: &net::Request) -> payload::Payload {
    let mut payload = payload::Payload::for_request(request, request.leased_by.clone());
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

pub(super) fn failure_payload(request: &net::Request, error: &str) -> payload::Payload {
    let mut payload =
        payload::Payload::for_request(request, request.leased_by.clone()).failed(error);
    payload.start_time = Some(1);
    payload.end_time = Some(2);
    payload
}

pub(super) fn item(value: &str) -> Box<dyn item::Item> {
    Box::new(item::Map::new(item::Values::from([(
        "value".to_string(),
        value.into(),
    )])))
}
