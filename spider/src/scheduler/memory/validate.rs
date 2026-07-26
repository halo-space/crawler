use super::state::State;
use crate::{net, scheduler};

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
