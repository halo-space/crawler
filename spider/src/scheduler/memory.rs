use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{item, net, payload, scheduler, stats, trace};

mod claim;
mod queue;
mod settle;
mod state;

use state::State;

#[derive(Debug)]
pub struct Memory {
    worker_id: String,
    lease: scheduler::Lease,
    state: Mutex<State>,
    writer: item::local::Writer,
}

impl Memory {
    pub fn new(worker_id: impl Into<String>) -> Self {
        let worker_id = worker_id.into();
        assert!(
            !worker_id.trim().is_empty(),
            "Memory worker_id must not be empty"
        );
        Self {
            worker_id,
            lease: scheduler::Lease::default(),
            state: Mutex::new(State::default()),
            writer: item::local::Writer::default(),
        }
    }

    pub fn with_dir(mut self, dir: impl Into<std::path::PathBuf>) -> Self {
        self.writer = item::local::Writer::new(dir);
        self
    }

    pub fn with_lease(mut self, lease: scheduler::Lease) -> Self {
        self.lease = lease;
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

impl scheduler::Scheduler for Memory {
    fn dir(&self) -> Option<&std::path::Path> {
        Some(self.writer.dir())
    }

    fn lease(&self) -> Option<scheduler::Lease> {
        Some(self.lease)
    }

    async fn open(&self) -> Result<(), scheduler::Error> {
        self.writer
            .open()
            .await
            .map_err(|error| scheduler::Error::Message(error.to_string()))
    }

    async fn close(&self) -> Result<(), scheduler::Error> {
        self.writer
            .close()
            .await
            .map_err(|error| scheduler::Error::Message(error.to_string()))
    }

    async fn push(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_push()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut request_ids = HashSet::with_capacity(payload.requests.len());
        let mut queued = Vec::with_capacity(payload.requests.len());
        for request in payload.requests {
            if !request_ids.insert(request.id.clone()) {
                return Err(scheduler::Error::Message(format!(
                    "duplicate request id in payload: {}",
                    request.id
                )));
            }
            claim::validate(&request)?;
            queued.push(queue::snapshot(request)?);
        }

        let mut state = self.state();
        if let Some(id) = queued
            .iter()
            .map(|snapshot| snapshot.id.as_str())
            .find(|id| state.contains(id))
        {
            return Err(scheduler::Error::Message(format!(
                "request id already exists: {id}"
            )));
        }
        claim::validate_trace(&state, &queued)?;
        let now = crate::utils::time::now_millis();
        for snapshot in queued {
            state.known.insert(snapshot.id.clone());
            state.enqueue(snapshot, now);
        }
        Ok(())
    }

    async fn push_items(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_items()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        if payload.items.is_empty() {
            return Ok(());
        }
        self.writer
            .write(payload)
            .await
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
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
        claim::reclaim(&mut state, self.lease);
        let mut requests = Vec::new();
        let mut remaining = state.queued_len();

        while requests.len() < limit && remaining != 0 {
            remaining -= 1;
            let now = crate::utils::time::now_millis();
            let Some(queued) = state.pop(now) else {
                break;
            };

            let Some(mut request) = claim::restore(&mut state, queued, &self.worker_id) else {
                continue;
            };
            request.state = net::State::Processing;
            request.leased_by.clone_from(&self.worker_id);
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

        Ok(requests)
    }

    async fn has_pending_requests(&self) -> Result<bool, scheduler::Error> {
        let state = self.state();
        Ok(state.queued_len() != 0 || !state.processing.is_empty())
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
        if settle::repeat(&state, payload)? {
            return Ok(());
        }
        settle::validate_lease(&state, payload, self.lease)?;
        settle::require_ack(&state, payload)?;
        let counters = settle::counters(payload)?;
        if state.processing.remove(&payload.id).is_none() {
            return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
        }
        state
            .acknowledged
            .remove(&(payload.id.clone(), payload.version));
        state.done += 1;
        settle::record(&mut state, payload);
        settle::apply(&mut state, payload.trace_id.clone(), counters);
        Ok(())
    }

    async fn failure(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_failure()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let mut state = self.state();
        if settle::repeat(&state, payload)? {
            return Ok(());
        }
        settle::validate_lease(&state, payload, self.lease)?;
        settle::require_ack(&state, payload)?;
        let counters = settle::counters(payload)?;
        let Some(request) = state.processing.remove(&payload.id) else {
            return Err(scheduler::Error::RequestNotFound(payload.id.clone()));
        };
        state
            .acknowledged
            .remove(&(payload.id.clone(), payload.version));
        let worker_id = request.leased_by.clone();
        if let Some(error) = settle::retry(&mut state, request, &worker_id) {
            settle::record_error(&mut state, payload, error);
        } else {
            settle::record(&mut state, payload);
        }
        settle::apply(&mut state, payload.trace_id.clone(), counters);
        Ok(())
    }
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
        if requests.is_empty() && snapshot.dsl.is_some() {
            return Err(scheduler::Error::Message(
                "Rules initial requests must not be empty".to_string(),
            ));
        }
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

        let mut state = self.state();
        if state.trace_snapshots.contains_key(&trace_id) {
            return Err(scheduler::Error::Message(format!(
                "trace already exists: {trace_id}"
            )));
        }

        let existing_id = requests.iter().any(|request| state.contains(&request.id));
        let mut request_ids = HashSet::with_capacity(requests.len());
        let duplicate_id = requests
            .iter()
            .any(|request| !request_ids.insert(request.id.clone()));
        if existing_id || duplicate_id {
            return Err(scheduler::Error::Message(
                "initial request id already exists".to_string(),
            ));
        }

        let mut queue = Vec::with_capacity(requests.len());
        for request in requests {
            claim::validate(&request)?;
            queue.push(queue::snapshot(request)?);
        }

        state.trace_snapshots.insert(trace_id, Arc::new(snapshot));
        let now = crate::utils::time::now_millis();
        for snapshot in queue {
            state.known.insert(snapshot.id.clone());
            state.enqueue(snapshot, now);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
