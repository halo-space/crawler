use super::queue::Queue;
use crate::{net, payload, stats, trace};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Debug, Default)]
pub(super) struct State {
    pub(super) trace_snapshots: HashMap<String, Arc<trace::Snapshot>>,
    pub(super) request_hashes: HashMap<String, [u8; 32]>,
    pub(super) queue: Queue,
    pub(super) processing: HashMap<String, net::Request>,
    pub(super) acknowledged: HashSet<(String, i64)>,
    pub(super) completed: HashMap<(String, i64), Completion>,
    pub(super) done: usize,
    pub(super) failed: usize,
    pub(super) trace_stats: HashMap<String, HashMap<String, stats::Counter>>,
    pub(super) trace_activity: HashMap<String, i64>,
}

impl State {
    pub(super) fn contains(&self, id: &str) -> bool {
        self.request_hashes.contains_key(id)
    }

    pub(super) fn queued_len(&self) -> usize {
        self.queue.len()
    }

    pub(super) fn enqueue(&mut self, snapshot: net::request::Snapshot, now: i64) {
        self.queue.push(snapshot, now);
    }

    pub(super) fn take(
        &mut self,
        now: i64,
        limit: usize,
        supported_modes: &[net::Mode],
    ) -> Vec<net::request::Snapshot> {
        self.queue.take(now, limit, supported_modes)
    }

    pub(super) fn has_pending_requests(&self, supported_modes: &[net::Mode]) -> bool {
        self.queue.contains_supported(supported_modes)
            || self
                .processing
                .values()
                .any(|request| supported_modes.contains(&request.mode))
    }

    pub(super) fn fail_request(&mut self, request: &net::Request, error: impl Into<String>) {
        self.failed += 1;
        self.touch_trace(&request.trace_id);
        self.completed.insert(
            (request.id.clone(), request.version),
            Completion::failed(
                request.task_id.clone(),
                request.trace_id.clone(),
                request.version,
                request.leased_by.clone(),
                request.node_key().to_string(),
                error,
            ),
        );
    }

    pub(super) fn fail_snapshot(
        &mut self,
        snapshot: &net::request::Snapshot,
        worker_id: &str,
        error: impl Into<String>,
    ) {
        self.failed += 1;
        self.touch_trace(&snapshot.trace_id);
        self.completed.insert(
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
    }

    pub(super) fn touch_trace(&mut self, trace_id: &str) {
        self.trace_activity
            .insert(trace_id.to_string(), crate::utils::time::now_millis());
    }

    pub(super) fn cleanup(&mut self, before: i64) -> usize {
        let mut active_ids = self.queue.request_ids();
        active_ids.extend(self.processing.keys().cloned());

        let removable_ids = self
            .request_hashes
            .keys()
            .filter(|id| !active_ids.contains(*id))
            .filter(|id| {
                let mut entries = self
                    .completed
                    .iter()
                    .filter(|((request_id, _), _)| request_id == *id);
                entries.next().is_some()
                    && entries.all(|(_, completion)| completion.completed_at < before)
            })
            .cloned()
            .collect::<HashSet<_>>();

        self.completed
            .retain(|(request_id, _), _| !removable_ids.contains(request_id));
        self.request_hashes
            .retain(|request_id, _| !removable_ids.contains(request_id));
        self.acknowledged
            .retain(|(request_id, _)| !removable_ids.contains(request_id));

        let mut active_traces = self.queue.trace_ids();
        active_traces.extend(
            self.processing
                .values()
                .map(|request| request.trace_id.clone()),
        );
        let retained_traces = self
            .completed
            .values()
            .map(|completion| completion.trace_id.as_str())
            .collect::<HashSet<_>>();
        let removable_traces = self
            .trace_activity
            .iter()
            .filter(|(trace_id, updated_at)| {
                **updated_at < before
                    && !active_traces.contains(*trace_id)
                    && !retained_traces.contains(trace_id.as_str())
            })
            .map(|(trace_id, _)| trace_id.clone())
            .collect::<Vec<_>>();

        for trace_id in removable_traces {
            self.trace_activity.remove(&trace_id);
            self.trace_snapshots.remove(&trace_id);
            self.trace_stats.remove(&trace_id);
        }

        removable_ids.len()
    }
}

#[derive(Debug)]
pub(super) struct Completion {
    pub(super) task_id: String,
    pub(super) trace_id: String,
    pub(super) version: i64,
    pub(super) worker_id: String,
    pub(super) node: String,
    pub(super) state: net::State,
    pub(super) error: Option<String>,
    pub(super) completed_at: i64,
}

impl Completion {
    pub(super) fn new(payload: &payload::Payload, worker_id: &str) -> Self {
        Self {
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            version: payload.version,
            worker_id: worker_id.to_string(),
            node: payload.node.clone(),
            state: payload.state,
            error: payload.error.clone(),
            completed_at: crate::utils::time::now_millis(),
        }
    }

    pub(super) fn failed(
        task_id: String,
        trace_id: String,
        version: i64,
        worker_id: String,
        node: String,
        error: impl Into<String>,
    ) -> Self {
        Self {
            task_id,
            trace_id,
            version,
            worker_id,
            node,
            state: net::State::Failed,
            error: Some(error.into()),
            completed_at: crate::utils::time::now_millis(),
        }
    }
}
