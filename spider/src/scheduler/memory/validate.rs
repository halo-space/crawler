use super::state::State;
use crate::{net, scheduler};

pub(super) fn request(request: &net::Request) -> Result<(), scheduler::Error> {
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

pub(super) fn ownership<'a>(
    state: &State,
    snapshots: impl IntoIterator<Item = &'a net::request::Snapshot>,
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
