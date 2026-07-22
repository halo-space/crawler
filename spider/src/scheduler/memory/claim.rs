use super::state::State;
use super::{reclaim, restore};
use crate::{net, scheduler};

pub(super) fn next(
    state: &mut State,
    lease: scheduler::Lease,
    limit: usize,
    worker_id: &str,
    modes: &[net::Mode],
) -> Vec<net::Request> {
    reclaim::expired(state, lease);
    let now = crate::utils::time::now_millis();
    let queued = state.take(now, limit, modes);
    let mut requests = Vec::with_capacity(queued.len());

    for queued in queued {
        let Some(mut request) = restore::request(state, queued, worker_id) else {
            continue;
        };
        request.state = net::State::Processing;
        request.leased_by = worker_id.to_string();
        request.lease_time = now;
        let Some(version) = request.version.checked_add(1) else {
            state.fail_request(
                &request,
                format!("request version overflow while claiming: {}", request.id),
            );
            continue;
        };
        request.version = version;

        state.processing.insert(request.id.clone(), request.clone());
        requests.push(request);
    }

    requests
}
