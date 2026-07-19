use super::queue;
use super::state::{Completion, State};
use crate::{net, payload, scheduler, stats};

pub(super) fn validate_lease(
    state: &State,
    payload: &payload::Payload,
    lease: scheduler::Lease,
) -> Result<(), scheduler::Error> {
    let Some(request) = state.processing.get(&payload.id) else {
        return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
    };
    for (field, matches) in [
        ("task_id", request.task_id == payload.task_id),
        ("trace_id", request.trace_id == payload.trace_id),
        ("node", request.node_key() == payload.node),
    ] {
        if !matches {
            return Err(scheduler::Error::IdentityMismatch {
                id: request.id.clone(),
                field,
            });
        }
    }
    if request.leased_by != payload.worker_id {
        return Err(scheduler::Error::LeaseMismatch(request.id.clone()));
    }
    if request.version != payload.version {
        return Err(scheduler::Error::VersionMismatch(request.id.clone()));
    }
    if request.state != net::State::Processing {
        return Err(scheduler::Error::StateMismatch(request.id.clone()));
    }
    if crate::utils::time::now_millis().saturating_sub(request.lease_time) >= lease.timeout_millis()
    {
        return Err(scheduler::Error::LeaseExpired(request.id.clone()));
    }
    Ok(())
}

pub(super) fn is_duplicate(
    state: &State,
    payload: &payload::Payload,
) -> Result<bool, scheduler::Error> {
    let key = (payload.id.clone(), payload.version);
    let Some(completed) = state.completed.get(&key) else {
        if !state.processing.contains_key(&payload.id)
            && state.completed.keys().any(|(id, _)| id == &payload.id)
        {
            return Err(scheduler::Error::VersionMismatch(payload.id.clone()));
        }
        return Ok(false);
    };
    if completed.version != payload.version {
        return Err(scheduler::Error::VersionMismatch(payload.id.clone()));
    }
    for (field, matches) in [
        ("task_id", completed.task_id == payload.task_id),
        ("trace_id", completed.trace_id == payload.trace_id),
        ("node", completed.node == payload.node),
    ] {
        if !matches {
            return Err(scheduler::Error::IdentityMismatch {
                id: payload.id.clone(),
                field,
            });
        }
    }
    if completed.worker_id != payload.worker_id {
        return Err(scheduler::Error::LeaseMismatch(payload.id.clone()));
    }
    if completed.state != payload.state {
        return Err(scheduler::Error::StateMismatch(payload.id.clone()));
    }
    Ok(true)
}

pub(super) fn require_ack(
    state: &State,
    payload: &payload::Payload,
) -> Result<(), scheduler::Error> {
    if state
        .acknowledged
        .contains(&(payload.id.clone(), payload.version))
    {
        Ok(())
    } else {
        Err(scheduler::Error::NotAcknowledged(payload.id.clone()))
    }
}

pub(super) fn counters(
    payload: &payload::Payload,
) -> Result<Vec<(String, stats::Counter)>, scheduler::Error> {
    payload
        .stats
        .iter()
        .map(|(name, value)| {
            serde_json::from_value::<stats::Counter>(value.clone())
                .map_err(|error| {
                    scheduler::Error::Message(format!("invalid stats counter {name}: {error}"))
                })
                .and_then(|counter| {
                    if counter.is_non_negative() {
                        Ok((name.clone(), counter))
                    } else {
                        Err(scheduler::Error::Message(format!(
                            "invalid stats counter {name}: values must be non-negative"
                        )))
                    }
                })
        })
        .collect()
}

pub(super) fn merge_stats(
    state: &State,
    trace_id: &str,
    counters: Vec<(String, stats::Counter)>,
) -> Result<Vec<(String, stats::Counter)>, scheduler::Error> {
    let trace_stats = state.trace_stats.get(trace_id);
    counters
        .into_iter()
        .map(|(name, counter)| {
            let current = trace_stats
                .and_then(|counters| counters.get(&name))
                .cloned()
                .unwrap_or_default();
            current
                .checked_add(&counter)
                .map(|merged| (name.clone(), merged))
                .ok_or_else(|| scheduler::Error::Message(format!("stats counter overflow: {name}")))
        })
        .collect()
}

pub(super) fn apply_stats(
    state: &mut State,
    trace_id: String,
    counters: Vec<(String, stats::Counter)>,
) {
    let trace_stats = state.trace_stats.entry(trace_id).or_default();
    for (name, counter) in counters {
        trace_stats.insert(name, counter);
    }
}

pub(super) fn retry(
    state: &mut State,
    mut request: net::Request,
    worker_id: &str,
) -> Option<String> {
    if !request
        .failed_workers
        .iter()
        .any(|worker| worker == worker_id)
    {
        request.failed_workers.push(worker_id.to_string());
    }
    let Some(retry_count) = request.retry_count.checked_add(1) else {
        state.failed += 1;
        return Some(format!("request retry overflow: {}", request.id));
    };
    request.retry_count = retry_count;

    if request.retry_count < request.max_retry_count {
        request.state = net::State::Pending;
        request.leased_by.clear();
        request.lease_time = 0;
        request.next_time = 0;
        match queue::snapshot(request.clone()) {
            Ok(snapshot) => state.enqueue(snapshot, crate::utils::time::now_millis()),
            Err(error) => {
                request.state = net::State::Failed;
                request.lease_time = crate::utils::time::now_millis();
                request.next_time = 0;
                state.failed += 1;
                return Some(format!("Request cannot be queued: {error}"));
            }
        }
    } else {
        request.state = net::State::Failed;
        request.lease_time = crate::utils::time::now_millis();
        request.next_time = 0;
        state.failed += 1;
    }
    None
}

pub(super) fn record(state: &mut State, payload: &payload::Payload) {
    state.completed.insert(
        (payload.id.clone(), payload.version),
        Completion::new(payload),
    );
}

pub(super) fn record_error(
    state: &mut State,
    payload: &payload::Payload,
    error: impl Into<String>,
) {
    let mut completion = Completion::new(payload);
    completion.state = net::State::Failed;
    let transition = error.into();
    completion.error = Some(
        payload
            .error
            .as_deref()
            .map_or(transition.clone(), |error| format!("{error}; {transition}")),
    );
    state
        .completed
        .insert((payload.id.clone(), payload.version), completion);
}
