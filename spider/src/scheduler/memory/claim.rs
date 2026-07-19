use super::queue;
use super::settle;
use super::state::{Completion, State};
use crate::{net, scheduler};

pub(super) fn validate(request: &net::Request) -> Result<(), scheduler::Error> {
    if request.id.is_empty() {
        return Err(scheduler::Error::Message(
            "new Request id must not be empty".to_string(),
        ));
    }
    if request.task_id.is_empty() != request.trace_id.is_empty() {
        return Err(scheduler::Error::Message(
            "new Request task_id and trace_id must both be set or both be empty".to_string(),
        ));
    }
    if request.version != 0 {
        return Err(scheduler::Error::Message(
            "new Request version must be 0".to_string(),
        ));
    }
    if request.state != net::State::Pending {
        return Err(scheduler::Error::Message(
            "new Request state must be pending".to_string(),
        ));
    }
    if !request.leased_by.is_empty() || request.lease_time != 0 {
        return Err(scheduler::Error::Message(
            "new Request must not have a lease".to_string(),
        ));
    }
    if !request.failed_workers.is_empty() {
        return Err(scheduler::Error::Message(
            "new Request failed_workers must be empty".to_string(),
        ));
    }
    if request.next_time < 0 {
        return Err(scheduler::Error::Message(
            "new Request next_time must not be negative".to_string(),
        ));
    }
    if request.retry_count != 0 || request.max_retry_count <= 0 {
        return Err(scheduler::Error::Message(
            "new Request requires retry_count 0 and a positive max_retry_count".to_string(),
        ));
    }
    for middleware in &request.middlewares {
        crate::middleware::check(middleware).map_err(|error| {
            scheduler::Error::Message(format!("new Request has invalid middleware: {error}"))
        })?;
    }
    Ok(())
}

pub(super) fn validate_trace(
    state: &State,
    snapshots: &[net::request::Snapshot],
) -> Result<(), scheduler::Error> {
    for snapshot in snapshots {
        let request_id = snapshot.id.clone();
        let (task_id, trace_id) = (&snapshot.task_id, &snapshot.trace_id);
        if trace_id.is_empty() {
            continue;
        }
        let trace = state
            .trace_snapshots
            .get(trace_id)
            .ok_or_else(|| scheduler::Error::TraceNotFound(trace_id.clone()))?;
        if trace.task_id != *task_id {
            return Err(scheduler::Error::IdentityMismatch {
                id: request_id,
                field: "task_id",
            });
        }
    }
    Ok(())
}

pub(super) fn reclaim(state: &mut State, lease: scheduler::Lease) {
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
            let mut completion = failed_request(&request, &worker_id, "acknowledged lease expired");
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

pub(super) fn restore(
    state: &mut State,
    snapshot: net::request::Snapshot,
    worker_id: &str,
) -> Option<net::Request> {
    if let Err(error) = snapshot.validate() {
        retry_snapshot(state, snapshot, worker_id, error);
        return None;
    }
    let trace = if snapshot.trace_id.is_empty() {
        None
    } else {
        let Some(trace) = state.trace_snapshots.get(&snapshot.trace_id) else {
            retry_snapshot(state, snapshot, worker_id, "Trace Snapshot not found");
            return None;
        };
        if trace.task_id != snapshot.task_id {
            retry_snapshot(
                state,
                snapshot,
                worker_id,
                "Request Snapshot task_id does not match Trace Snapshot",
            );
            return None;
        }
        Some(trace.clone())
    };

    match snapshot.clone().restore(trace) {
        Ok(request) => Some(request),
        Err(error) => {
            retry_snapshot(state, snapshot, worker_id, error);
            None
        }
    }
}

fn retry_snapshot(
    state: &mut State,
    mut snapshot: net::request::Snapshot,
    worker_id: &str,
    error: impl Into<String>,
) {
    let error = error.into();
    if !snapshot
        .failed_workers
        .iter()
        .any(|worker| worker == worker_id)
    {
        snapshot.failed_workers.push(worker_id.to_string());
    }
    let Some(retry_count) = snapshot.retry_count.checked_add(1) else {
        state.fail_snapshot(
            &snapshot,
            worker_id,
            format!("{error}; request retry overflow"),
        );
        return;
    };
    snapshot.retry_count = retry_count;

    if retry_count < snapshot.max_retry_count {
        snapshot.state = net::State::Pending;
        snapshot.leased_by.clear();
        snapshot.lease_time = 0;
        snapshot.next_time = 0;
        state.completed.insert(
            (snapshot.id.clone(), snapshot.version),
            Completion::failed(
                snapshot.task_id.clone(),
                snapshot.trace_id.clone(),
                snapshot.version,
                worker_id.to_string(),
                snapshot.node.clone(),
                error,
            ),
        );
        state.enqueue(snapshot, crate::utils::time::now_millis());
    } else {
        state.fail_snapshot(&snapshot, worker_id, error);
    }
}

fn failed_request(request: &net::Request, worker_id: &str, error: impl Into<String>) -> Completion {
    Completion::failed(
        request.task_id.clone(),
        request.trace_id.clone(),
        request.version,
        worker_id.to_string(),
        request.node_key().to_string(),
        error,
    )
}
