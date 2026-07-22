use super::queue;
use super::settle;
use super::state::{Completion, State};
use crate::{net, scheduler};

pub(super) fn expired(state: &mut State, lease: scheduler::Lease) {
    let now = crate::utils::time::now_millis();
    let expired = state
        .processing
        .values()
        .filter(|request| now.saturating_sub(request.lease_time) >= lease.timeout_millis())
        .map(|request| request.id.clone())
        .collect::<Vec<_>>();
    for id in expired {
        let Some(mut request) = state.processing.remove(&id) else {
            continue;
        };
        let acknowledged = state
            .acknowledged
            .remove(&(request.id.clone(), request.version));
        if acknowledged {
            let worker_id = request.leased_by.clone();
            let id = request.id.clone();
            let mut completion = failure(&request, &worker_id, "acknowledged lease expired");
            if let Some(error) = settle::retry(state, request, &worker_id) {
                completion.error = Some(format!("acknowledged lease expired; {error}"));
            }
            state.completed.insert((id, completion.version), completion);
            continue;
        }

        request.state = net::State::Pending;
        request.leased_by.clear();
        request.lease_time = 0;
        request.next_time = 0;
        match queue::snapshot(request.clone()) {
            Ok(snapshot) => state.enqueue(snapshot, now),
            Err(error) => state.fail_request(&request, error.to_string()),
        }
    }
}

fn failure(request: &net::Request, worker_id: &str, error: impl Into<String>) -> Completion {
    Completion::failed(
        request.task_id.clone(),
        request.trace_id.clone(),
        request.version,
        worker_id.to_string(),
        request.node_key().to_string(),
        error,
    )
}
