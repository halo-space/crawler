use super::state::{Completion, State};
use crate::net;

pub(super) fn request(
    state: &mut State,
    snapshot: net::request::Snapshot,
    worker_id: &str,
) -> Option<net::Request> {
    if let Err(error) = snapshot.validate() {
        retry(state, snapshot, worker_id, error);
        return None;
    }
    let trace = if snapshot.trace_id.is_empty() {
        None
    } else {
        let Some(trace) = state.trace_snapshots.get(&snapshot.trace_id) else {
            retry(state, snapshot, worker_id, "Trace Snapshot not found");
            return None;
        };
        if trace.task_id != snapshot.task_id {
            retry(
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
            retry(state, snapshot, worker_id, error);
            None
        }
    }
}

fn retry(
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
