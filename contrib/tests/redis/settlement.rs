use contrib::scheduler::redis::Redis;
use spider::{Scheduler, net, payload};

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

pub(super) async fn succeed(scheduler: &Redis, request: &net::Request) {
    scheduler.ack(&processing(request)).await.unwrap();
    scheduler.success(&success(request)).await.unwrap();
}
