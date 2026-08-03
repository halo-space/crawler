use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{net, payload, scheduler, stats, trace};

mod claim;
mod queue;
mod reclaim;
mod restore;
mod settle;
mod state;
mod validate;

use state::State;

const LOCAL_WORKER: &str = "local";

#[derive(Debug)]
pub struct Memory {
    lease: scheduler::Lease,
    modes: Vec<net::Mode>,
    state: Mutex<State>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            lease: scheduler::Lease::default(),
            modes: vec![net::Mode::Http],
            state: Mutex::new(State::default()),
        }
    }

    pub fn with_lease(mut self, lease: scheduler::Lease) -> Self {
        self.lease = lease;
        self
    }

    pub fn with_modes(mut self, modes: impl IntoIterator<Item = net::Mode>) -> Self {
        self.modes.clear();
        for mode in modes {
            if !self.modes.contains(&mode) {
                self.modes.push(mode);
            }
        }
        self
    }

    pub fn queued_len(&self) -> usize {
        self.state().queued_len()
    }

    pub fn processing_len(&self) -> usize {
        self.state().processing.len()
    }

    pub fn done_len(&self) -> usize {
        self.state().done
    }

    pub fn failed_len(&self) -> usize {
        self.state().failed
    }

    pub fn trace_len(&self) -> usize {
        self.state().trace_snapshots.len()
    }

    pub fn trace_ids(&self) -> Vec<String> {
        let mut ids = self
            .state()
            .trace_snapshots
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    pub fn trace_stats(&self, trace_id: &str) -> HashMap<String, stats::Counter> {
        self.state()
            .trace_stats
            .get(trace_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn errors(&self) -> Vec<String> {
        let state = self.state();
        let mut latest = HashMap::<&str, &state::Completion>::new();
        for ((id, _), completed) in &state.completed {
            latest
                .entry(id)
                .and_modify(|current| {
                    if completed.version > current.version {
                        *current = completed;
                    }
                })
                .or_insert(completed);
        }
        latest
            .values()
            .filter_map(|completed| completed.error.clone())
            .collect()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

impl scheduler::Scheduler for Memory {
    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self, _concurrency: usize) -> Result<(), scheduler::Error> {
        validate_worker(LOCAL_WORKER, &self.modes)?;
        Ok(())
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        Ok(())
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let mut queued = Vec::with_capacity(payload.requests.len());
        for request in payload.requests {
            let snapshot = queue::snapshot(request)?;
            let snapshot_hash = snapshot
                .hash()
                .map_err(|error| scheduler::Error::Message(error.to_string()))?;
            queued.push((snapshot, snapshot_hash));
        }

        let mut state = self.state();
        if let Some(id) = queued
            .iter()
            .find(|(snapshot, snapshot_hash)| {
                state
                    .request_hashes
                    .get(&snapshot.id)
                    .is_some_and(|current| current != snapshot_hash)
            })
            .map(|(snapshot, _)| snapshot.id.as_str())
        {
            return Err(scheduler::Error::Message(format!(
                "request id conflicts with existing snapshot: {id}"
            )));
        }
        validate::ownership(&state, queued.iter().map(|(snapshot, _)| snapshot))?;
        let now = crate::utils::time::now_millis();
        for (snapshot, snapshot_hash) in queued {
            if state.contains(&snapshot.id) {
                continue;
            }
            state
                .request_hashes
                .insert(snapshot.id.clone(), snapshot_hash);
            state.enqueue(snapshot, now);
        }
        Ok(())
    }

    async fn trace(&self, trace_id: &str) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        Ok(self
            .state()
            .trace_snapshots
            .get(trace_id)
            .map(|snapshot| snapshot.as_ref().clone()))
    }

    async fn next_requests(&self, limit: usize) -> Result<Vec<net::Request>, scheduler::Error> {
        let mut state = self.state();
        Ok(claim::next(
            &mut state,
            self.lease,
            limit,
            LOCAL_WORKER,
            &self.modes,
        ))
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        let state = self.state();
        Ok(state.has_pending_requests(&self.modes))
    }

    async fn ack(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_ack()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        settle::validate_lease(&state, payload, self.lease)?;
        state
            .acknowledged
            .insert((payload.id.clone(), payload.version));
        Ok(())
    }

    async fn release(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_release()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        settle::validate_lease(&state, payload, self.lease)?;
        let Some(request) = state.processing.get(&payload.id).cloned() else {
            return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
        };
        let mut released = request.clone();
        released.state = net::State::Pending;
        released.leased_by.clear();
        released.lease_time = 0;
        let snapshot = match queue::snapshot(released) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                state.processing.remove(&payload.id);
                state
                    .acknowledged
                    .remove(&(payload.id.clone(), payload.version));
                state.fail_request(&request, error.to_string());
                return Err(error);
            }
        };

        state.processing.remove(&payload.id);
        state
            .acknowledged
            .remove(&(payload.id.clone(), payload.version));
        state.enqueue(snapshot, crate::utils::time::now_millis());
        Ok(())
    }

    async fn refresh_lease(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_refresh_lease()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        settle::validate_lease(&state, payload, self.lease)?;
        settle::require_ack(&state, payload)?;
        if let Some(request) = state.processing.get_mut(&payload.id) {
            request.lease_time = crate::utils::time::now_millis();
        }
        Ok(())
    }

    async fn success(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_success()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        if settle::is_duplicate(&state, payload)? {
            return Ok(());
        }
        settle::validate_lease(&state, payload, self.lease)?;
        settle::require_ack(&state, payload)?;
        let counters = settle::counters(payload)?;
        let counters = settle::merge_stats(&state, &payload.trace_id, counters)?;
        let worker_id = state
            .processing
            .get(&payload.id)
            .map(|request| request.leased_by.clone())
            .ok_or_else(|| scheduler::Error::RequestNotFound(payload.id.clone()))?;
        if state.processing.remove(&payload.id).is_none() {
            return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
        }
        state
            .acknowledged
            .remove(&(payload.id.clone(), payload.version));
        state.done += 1;
        settle::record(&mut state, payload, &worker_id);
        settle::apply_stats(&mut state, payload.trace_id.clone(), counters);
        Ok(())
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_failure()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        if settle::is_duplicate(&state, payload)? {
            return Ok(());
        }
        settle::validate_lease(&state, payload, self.lease)?;
        settle::require_ack(&state, payload)?;
        let counters = settle::counters(payload)?;
        let counters = settle::merge_stats(&state, &payload.trace_id, counters)?;
        let Some(request) = state.processing.remove(&payload.id) else {
            return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
        };
        state
            .acknowledged
            .remove(&(payload.id.clone(), payload.version));
        let worker_id = request.leased_by.clone();
        if let Some(error) = settle::retry(&mut state, request, &worker_id) {
            settle::record_error(&mut state, payload, &worker_id, error);
        } else {
            settle::record(&mut state, payload, &worker_id);
        }
        settle::apply_stats(&mut state, payload.trace_id.clone(), counters);
        Ok(())
    }
}

fn validate_worker(worker_id: &str, modes: &[net::Mode]) -> Result<(), scheduler::Error> {
    if worker_id.trim().is_empty() {
        return Err(scheduler::Error::Message(
            "worker_id must not be empty".to_string(),
        ));
    }
    if modes.is_empty() {
        return Err(scheduler::Error::Message(
            "worker modes must not be empty".to_string(),
        ));
    }
    Ok(())
}

impl scheduler::Init for Memory {
    fn initializes_run(&self) -> bool {
        true
    }

    async fn init(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        snapshot
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id.clone(),
                message,
            })?;
        if requests.iter().any(|request| request.trace_id != trace_id) {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the initialized trace_id".to_string(),
            ));
        }
        if requests
            .iter()
            .any(|request| request.task_id != snapshot.task_id)
        {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the Trace Snapshot task_id".to_string(),
            ));
        }
        let payload = payload::Payload::new().requests(requests);
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let requests = payload.requests;

        let mut queue = Vec::with_capacity(requests.len());
        for request in requests {
            let snapshot = queue::snapshot(request)?;
            let snapshot_hash = snapshot
                .hash()
                .map_err(|error| scheduler::Error::Message(error.to_string()))?;
            queue.push((snapshot, snapshot_hash));
        }

        let mut state = self.state();
        if state.trace_snapshots.contains_key(&trace_id) {
            return Err(scheduler::Error::Message(format!(
                "trace already exists: {trace_id}"
            )));
        }
        if let Some((snapshot, _)) = queue
            .iter()
            .find(|(snapshot, _)| state.contains(&snapshot.id))
        {
            return Err(scheduler::Error::Message(format!(
                "initial request id already exists: {}",
                snapshot.id
            )));
        }

        state.trace_snapshots.insert(trace_id, Arc::new(snapshot));
        let now = crate::utils::time::now_millis();
        for (snapshot, snapshot_hash) in queue {
            state
                .request_hashes
                .insert(snapshot.id.clone(), snapshot_hash);
            state.enqueue(snapshot, now);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
