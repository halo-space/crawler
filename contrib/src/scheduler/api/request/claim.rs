use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use spider::{net, scheduler, trace};

use super::super::{Api, wire};

impl Api {
    pub(in crate::scheduler::api) async fn claim(
        &self,
        limit: usize,
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let claim_started = tokio::time::Instant::now();
        let lease = scheduler::Scheduler::lease(self)
            .expect("API Scheduler always defines a Request lease");
        let (lease_deadline, handoff_deadline) = claim_deadlines(claim_started, lease)?;
        let worker_id = self.worker.id()?;
        let body = wire::Claim {
            limit,
            worker_id: worker_id.to_string(),
            modes: self.worker.modes().to_vec(),
        };
        let key = Self::invocation_key();
        let response = self
            .client
            .post::<_, wire::ClaimResponse>("v1/worker/requests/claim", &body, Some(&key))
            .await?;
        if response.requests.len() > limit {
            let error = scheduler::Error::Message(format!(
                "Master returned {} Requests for claim limit {limit}",
                response.requests.len()
            ));
            return self
                .release_after_protocol_error(&response.requests, error, lease_deadline)
                .await;
        }

        let mut ids = HashSet::with_capacity(response.requests.len());
        for claimed in &response.requests {
            if !ids.insert(claimed.identity.id.as_str()) {
                let error = scheduler::Error::InvalidRequest {
                    id: claimed.identity.id.clone(),
                    message: "Master returned a duplicate Request in one claim".to_string(),
                };
                return self
                    .release_after_protocol_error(&response.requests, error, lease_deadline)
                    .await;
            }
        }

        let traces = self
            .prepare_traces(&response.requests, handoff_deadline)
            .await;
        let mut requests = Vec::with_capacity(response.requests.len());
        let mut recoveries = Vec::new();
        for claimed in &response.requests {
            let restored = self
                .restore_prepared(claimed, worker_id, &body.modes, &traces)
                .await;
            match restored {
                Ok(request) if tokio::time::Instant::now() < handoff_deadline => {
                    requests.push(request)
                }
                Ok(_) => recoveries.push((
                    claimed,
                    RestoreError::Trace(scheduler::Error::Unavailable(format!(
                        "Request {} recovery exhausted its lease handoff budget",
                        claimed.identity.id
                    ))),
                )),
                Err(error) => recoveries.push((claimed, error)),
            }
        }

        let settlement_deadline = if requests.is_empty() {
            lease_deadline
        } else {
            handoff_deadline
        };
        let recovery_results = futures_util::future::join_all(recoveries.into_iter().map(
            |(claimed, restore)| async move {
                let (restore, recovery) = restore.into_recovery();
                let settlement = tokio::time::timeout_at(settlement_deadline, async {
                    match recovery {
                        Recovery::Release => self.release_restore(claimed).await,
                        Recovery::Failure => self.fail_restore(claimed, &restore).await,
                    }
                })
                .await
                .unwrap_or_else(|_| {
                    Err(scheduler::Error::Unavailable(format!(
                        "Request {} recovery settlement exceeded its deadline",
                        claimed.identity.id
                    )))
                });
                (claimed.identity.clone(), restore, recovery, settlement)
            },
        ))
        .await;
        let issues = recovery_results
            .into_iter()
            .filter(|(_, restore, recovery, settlement)| {
                *recovery == Recovery::Release || restore.is_transient() || settlement.is_err()
            })
            .collect::<Vec<_>>();

        if requests.is_empty() && !issues.is_empty() {
            let transient = issues.iter().any(|(_, restore, _, settlement)| {
                restore.is_transient()
                    || settlement
                        .as_ref()
                        .is_err_and(scheduler::Error::is_transient)
            });
            let message = issues
                .into_iter()
                .map(recovery_message)
                .collect::<Vec<_>>()
                .join("; ");
            return if transient {
                Err(scheduler::Error::Unavailable(message))
            } else {
                Err(scheduler::Error::Message(message))
            };
        }
        for (identity, restore, recovery, settlement) in issues {
            tracing::warn!(
                request_id = %identity.id,
                version = identity.version,
                worker_id = %identity.worker_id,
                restore_error = %restore,
                settlement = recovery.as_str(),
                settlement_error = settlement.as_ref().err().map(ToString::to_string),
                "API Scheduler could not execute a restored claim; valid peers remain executable"
            );
        }

        Ok(requests)
    }

    async fn release_after_protocol_error<T>(
        &self,
        requests: &[wire::Claimed],
        error: scheduler::Error,
        deadline: tokio::time::Instant,
    ) -> Result<T, scheduler::Error> {
        match self.release_claim(requests, deadline).await {
            Ok(()) => Err(error),
            Err(release) => {
                let message = format!(
                    "invalid claimed Request collection: {error}; failed to release the collection: {release}"
                );
                if release.is_transient() {
                    Err(scheduler::Error::Unavailable(message))
                } else {
                    Err(scheduler::Error::Message(message))
                }
            }
        }
    }

    async fn release_claim(
        &self,
        requests: &[wire::Claimed],
        deadline: tokio::time::Instant,
    ) -> Result<(), scheduler::Error> {
        let mut released = HashSet::with_capacity(requests.len());
        let mut claims = Vec::with_capacity(requests.len());
        for (index, claimed) in requests.iter().enumerate() {
            let key = (claimed.identity.id.clone(), claimed.identity.version);
            if released.insert(key) {
                claims.push((index, claimed));
            }
        }

        let results =
            futures_util::future::join_all(claims.into_iter().map(|(index, claimed)| async move {
                let identity = wire::Lease::from_claim(&claimed.identity);
                let key = Self::invocation_key();
                let result = tokio::time::timeout_at(
                    deadline,
                    self.client
                        .post_empty("v1/worker/requests/release", &identity, Some(&key)),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(scheduler::Error::Unavailable(format!(
                        "claim index {index} release exceeded its lease deadline"
                    )))
                });
                (index, result)
            }))
            .await;

        let mut failures = Vec::new();
        let mut transient = false;
        for (index, result) in results {
            if let Err(error) = result {
                transient |= error.is_transient();
                failures.push(format!("claim index {index}: {error}"));
            }
        }

        if failures.is_empty() {
            return Ok(());
        }

        let message = failures.join("; ");
        if transient {
            Err(scheduler::Error::Unavailable(message))
        } else {
            Err(scheduler::Error::Message(message))
        }
    }

    async fn release_restore(&self, claimed: &wire::Claimed) -> Result<(), scheduler::Error> {
        let key = Self::invocation_key();
        self.client
            .post_empty(
                "v1/worker/requests/release",
                &wire::Lease::from_claim(&claimed.identity),
                Some(&key),
            )
            .await
    }

    async fn fail_restore(
        &self,
        claimed: &wire::Claimed,
        error: &scheduler::Error,
    ) -> Result<(), scheduler::Error> {
        let identity = wire::Lease::from_claim(&claimed.identity);
        self.client
            .post_empty("v1/worker/requests/ack", &identity, None)
            .await?;
        let time = now_millis();
        let body = wire::Failure {
            identity,
            error: format!("failed to restore claimed Request: {error}"),
            stats: std::collections::HashMap::new(),
            start_time: time,
            end_time: time,
        };
        self.client
            .post_empty("v1/worker/requests/failure", &body, None)
            .await
    }

    pub(in crate::scheduler::api) async fn pending(&self) -> Result<bool, scheduler::Error> {
        let body = wire::Pending {
            worker_id: self.worker.id()?.to_string(),
            modes: self.worker.modes().to_vec(),
        };
        self.client
            .post::<_, wire::PendingResponse>("v1/worker/requests/pending", &body, None)
            .await
            .map(|response| response.pending)
    }

    #[cfg(test)]
    async fn restore(
        &self,
        claimed: &wire::Claimed,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<net::Request, RestoreError> {
        self.restore_prepared(claimed, worker_id, modes, &HashMap::new())
            .await
    }

    async fn restore_prepared(
        &self,
        claimed: &wire::Claimed,
        worker_id: &str,
        modes: &[net::Mode],
        traces: &HashMap<String, Result<Arc<trace::Snapshot>, TraceError>>,
    ) -> Result<net::Request, RestoreError> {
        validate_identity(&claimed.identity).map_err(RestoreError::Claim)?;
        let id = claimed.identity.id.clone();
        let snapshot = serde_json::from_value::<net::request::Snapshot>(claimed.snapshot.clone())
            .map_err(|error| {
            RestoreError::Request(scheduler::Error::InvalidRequest {
                id: id.clone(),
                message: format!("claimed Request Snapshot cannot be decoded: {error}"),
            })
        })?;
        for (field, matches) in [
            ("id", snapshot.id == claimed.identity.id),
            ("task_id", snapshot.task_id == claimed.identity.task_id),
            ("trace_id", snapshot.trace_id == claimed.identity.trace_id),
            ("node", snapshot.node == claimed.identity.node),
        ] {
            if !matches {
                return Err(RestoreError::Request(scheduler::Error::InvalidRequest {
                    id,
                    message: format!("claimed Request {field} does not match its identity"),
                }));
            }
        }
        if !modes.contains(&snapshot.mode) {
            return Err(RestoreError::Claim(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request mode is not supported by the claiming Worker".to_string(),
            }));
        }
        validate_execution(&claimed.execution, &snapshot, &claimed.identity, worker_id)
            .map_err(RestoreError::Claim)?;
        if claimed.identity.worker_id != worker_id {
            return Err(RestoreError::Claim(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request worker_id does not match the claiming Worker".to_string(),
            }));
        }

        let trace_id = claimed.identity.trace_id.clone();
        let trace = if let Some(value) = claimed.trace.clone() {
            let snapshot = serde_json::from_value::<trace::Snapshot>(value).map_err(|error| {
                RestoreError::Trace(scheduler::Error::InvalidTrace {
                    id: trace_id.clone(),
                    message: format!("claimed Trace Snapshot cannot be decoded: {error}"),
                })
            })?;
            snapshot.validate().map_err(|message| {
                RestoreError::Trace(scheduler::Error::InvalidTrace {
                    id: trace_id.clone(),
                    message,
                })
            })?;
            if snapshot.task_id != claimed.identity.task_id {
                return Err(RestoreError::Trace(scheduler::Error::InvalidTrace {
                    id: trace_id,
                    message: "Trace Snapshot task_id does not match its claimed Request"
                        .to_string(),
                }));
            }
            self.cache_trace(trace_id.clone(), snapshot)
                .await
                .map_err(RestoreError::Trace)?
        } else if let Some(snapshot) = self.cached_trace(&trace_id).await {
            snapshot
        } else if let Some(prepared) = traces.get(&trace_id) {
            prepared
                .as_ref()
                .map(Arc::clone)
                .map_err(|error| RestoreError::Trace(error.scheduler_error()))?
        } else {
            self.load_trace(&trace_id)
                .await
                .map_err(RestoreError::Trace)?
                .map(Arc::new)
                .ok_or_else(|| {
                    RestoreError::Trace(scheduler::Error::TraceNotFound(trace_id.clone()))
                })?
        };
        if trace.task_id != snapshot.task_id {
            return Err(RestoreError::Trace(scheduler::Error::InvalidTrace {
                id: trace_id,
                message: "Trace Snapshot task_id does not match its claimed Request".to_string(),
            }));
        }
        if let Some(config) = trace.dsl.as_ref()
            && !config.graph.nodes.contains_key(&snapshot.node)
        {
            return Err(RestoreError::Trace(scheduler::Error::InvalidTrace {
                id: trace_id,
                message: format!(
                    "Trace Snapshot does not define claimed Request node {}",
                    snapshot.node
                ),
            }));
        }

        let mut request = snapshot.restore(Some(trace)).map_err(|message| {
            RestoreError::Request(scheduler::Error::InvalidRequest {
                id: claimed.identity.id.clone(),
                message,
            })
        })?;
        request.state = net::State::Processing;
        request.version = claimed.identity.version;
        request.next_time = claimed.execution.next_time;
        request.leased_by = claimed.identity.worker_id.clone();
        request.lease_time = claimed.execution.lease_time;
        request.retry_count = claimed.execution.retry_count;
        request.failed_workers = claimed.execution.failed_workers.clone();
        Ok(request)
    }

    async fn prepare_traces(
        &self,
        claimed: &[wire::Claimed],
        deadline: tokio::time::Instant,
    ) -> HashMap<String, Result<Arc<trace::Snapshot>, TraceError>> {
        let mut ids = HashSet::with_capacity(claimed.len());
        for request in claimed {
            if request.trace.is_none() {
                ids.insert(request.identity.trace_id.clone());
            }
        }

        futures_util::future::join_all(ids.into_iter().map(|id| async move {
            let loaded = tokio::time::timeout_at(deadline, self.load_trace(&id))
                .await
                .map_err(|_| {
                    TraceError::Unavailable(format!(
                        "Trace Snapshot {id} recovery exceeded the lease handoff budget"
                    ))
                })
                .and_then(|result| result.map_err(TraceError::from))
                .and_then(|snapshot| {
                    snapshot
                        .map(Arc::new)
                        .ok_or_else(|| TraceError::NotFound(id.clone()))
                });
            (id, loaded)
        }))
        .await
        .into_iter()
        .collect()
    }
}

enum RestoreError {
    Request(scheduler::Error),
    Claim(scheduler::Error),
    Trace(scheduler::Error),
}

impl RestoreError {
    fn into_recovery(self) -> (scheduler::Error, Recovery) {
        match self {
            Self::Request(error) => (error, Recovery::Failure),
            Self::Claim(error) | Self::Trace(error) => (error, Recovery::Release),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Recovery {
    Release,
    Failure,
}

impl Recovery {
    fn as_str(self) -> &'static str {
        match self {
            Self::Release => "release",
            Self::Failure => "failure",
        }
    }
}

#[derive(Clone)]
enum TraceError {
    NotFound(String),
    Invalid { id: String, message: String },
    Unavailable(String),
    Message(String),
}

impl TraceError {
    fn scheduler_error(&self) -> scheduler::Error {
        match self {
            Self::NotFound(id) => scheduler::Error::TraceNotFound(id.clone()),
            Self::Invalid { id, message } => scheduler::Error::InvalidTrace {
                id: id.clone(),
                message: message.clone(),
            },
            Self::Unavailable(message) => scheduler::Error::Unavailable(message.clone()),
            Self::Message(message) => scheduler::Error::Message(message.clone()),
        }
    }
}

impl From<scheduler::Error> for TraceError {
    fn from(error: scheduler::Error) -> Self {
        match error {
            scheduler::Error::TraceNotFound(id) => Self::NotFound(id),
            scheduler::Error::InvalidTrace { id, message } => Self::Invalid { id, message },
            scheduler::Error::Unavailable(message) => Self::Unavailable(message),
            error => Self::Message(error.to_string()),
        }
    }
}

fn claim_deadlines(
    started: tokio::time::Instant,
    lease: scheduler::Lease,
) -> Result<(tokio::time::Instant, tokio::time::Instant), scheduler::Error> {
    let deadline = started.checked_add(lease.timeout()).ok_or_else(|| {
        scheduler::Error::Message("API claim lease deadline exceeds the runtime clock".to_string())
    })?;
    let handoff = deadline.checked_sub(lease.interval()).ok_or_else(|| {
        scheduler::Error::Message(
            "API claim handoff deadline exceeds the runtime clock".to_string(),
        )
    })?;
    Ok((deadline, handoff))
}

fn recovery_message(
    (identity, restore, recovery, settlement): (
        wire::Identity,
        scheduler::Error,
        Recovery,
        Result<(), scheduler::Error>,
    ),
) -> String {
    let action = recovery.as_str();
    match settlement {
        Ok(()) => format!(
            "failed to restore Request {}: {restore}; recovery {action} completed",
            identity.id
        ),
        Err(settlement) => format!(
            "failed to restore Request {}: {restore}; failed to settle its recovery with {action}: {settlement}",
            identity.id
        ),
    }
}

fn validate_execution(
    execution: &wire::Execution,
    snapshot: &net::request::Snapshot,
    identity: &wire::Identity,
    worker_id: &str,
) -> Result<(), scheduler::Error> {
    let id = identity.id.clone();
    if execution.version != identity.version {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request execution version does not match its identity".to_string(),
        });
    }
    if execution.next_time < 0 || execution.lease_time <= 0 {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message:
                "claimed Request next_time must not be negative and lease_time must be positive"
                    .to_string(),
        });
    }
    if execution.leased_by.trim().is_empty() {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request leased_by must not be empty".to_string(),
        });
    }
    if execution.leased_by != identity.worker_id {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request execution owner does not match its identity".to_string(),
        });
    }
    if execution.retry_count < 0 || execution.retry_count >= snapshot.max_retry_count {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request retry_count is invalid".to_string(),
        });
    }
    let mut workers = std::collections::HashSet::with_capacity(execution.failed_workers.len());
    if execution
        .failed_workers
        .iter()
        .any(|worker| worker.is_empty() || !workers.insert(worker))
    {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request failed_workers are invalid".to_string(),
        });
    }
    if execution.failed_workers.len() > execution.retry_count as usize {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request failed_workers exceed retry_count".to_string(),
        });
    }
    if execution
        .failed_workers
        .iter()
        .any(|failed| failed == worker_id)
    {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request was previously failed by the claiming Worker".to_string(),
        });
    }
    Ok(())
}

fn validate_identity(identity: &wire::Identity) -> Result<(), scheduler::Error> {
    if identity.id.is_empty()
        || identity.task_id.is_empty()
        || identity.trace_id.is_empty()
        || identity.worker_id.trim().is_empty()
        || identity.node.is_empty()
    {
        return Err(scheduler::Error::InvalidRequest {
            id: identity.id.clone(),
            message: "claimed Request identity contains an empty field".to_string(),
        });
    }
    if identity.version <= 0 {
        return Err(scheduler::Error::InvalidRequest {
            id: identity.id.clone(),
            message: "claimed Request identity version must be positive".to_string(),
        });
    }
    Ok(())
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use spider::trace;

    use super::*;

    fn claimed(mode: net::Mode) -> wire::Claimed {
        let mut request = net::Request::follow("https://example.com").unwrap();
        request.id = "request-1".to_string();
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.mode = mode;
        let snapshot = net::request::Snapshot::try_from(request).unwrap();
        wire::Claimed {
            identity: wire::Identity {
                id: snapshot.id.clone(),
                task_id: snapshot.task_id.clone(),
                trace_id: snapshot.trace_id.clone(),
                version: 1,
                worker_id: "worker-1".to_string(),
                node: snapshot.node.clone(),
            },
            snapshot: serde_json::to_value(snapshot).unwrap(),
            execution: wire::Execution {
                version: 1,
                next_time: 0,
                leased_by: "worker-1".to_string(),
                lease_time: 1,
                retry_count: 0,
                failed_workers: Vec::new(),
            },
            trace: Some(serde_json::to_value(trace::Snapshot::code("task-1")).unwrap()),
        }
    }

    #[tokio::test]
    async fn restore_rejects_a_mode_outside_the_worker_capabilities() {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let result = api
            .restore(&claimed(net::Mode::Browser), "worker-1", &[net::Mode::Http])
            .await;

        assert!(matches!(
            result,
            Err(RestoreError::Claim(scheduler::Error::InvalidRequest { .. }))
        ));
    }

    #[tokio::test]
    async fn restore_rejects_an_empty_lease_and_the_same_failed_worker() {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let mut empty_lease = claimed(net::Mode::Http);
        empty_lease.execution.lease_time = 0;
        assert!(matches!(
            api.restore(&empty_lease, "worker-1", &[net::Mode::Http])
                .await,
            Err(RestoreError::Claim(_))
        ));

        let mut failed = claimed(net::Mode::Http);
        failed.execution.retry_count = 1;
        failed.execution.failed_workers.push("worker-1".to_string());
        assert!(matches!(
            api.restore(&failed, "worker-1", &[net::Mode::Http]).await,
            Err(RestoreError::Claim(_))
        ));
    }

    #[tokio::test]
    async fn restore_does_not_cache_a_trace_with_the_wrong_task() {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let mut claimed = claimed(net::Mode::Http);
        claimed.identity.task_id = "task-1".to_string();
        claimed.identity.trace_id = "trace-1".to_string();
        claimed.trace = Some(serde_json::to_value(trace::Snapshot::code("task-2")).unwrap());

        assert!(
            api.restore(&claimed, "worker-1", &[net::Mode::Http])
                .await
                .is_err()
        );
        assert!(api.cached_trace("trace-1").await.is_none());
    }

    #[tokio::test]
    async fn empty_trace_is_rejected_before_caching() {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let mut claimed = claimed(net::Mode::Http);
        claimed.identity.trace_id.clear();

        assert!(
            api.restore(&claimed, "worker-1", &[net::Mode::Http])
                .await
                .is_err()
        );
        assert!(api.cached_trace("").await.is_none());
    }

    #[test]
    fn claim_recovery_reserves_one_refresh_interval_for_engine_handoff() {
        let lease = scheduler::Lease::new(
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(10),
        )
        .unwrap();
        let started = tokio::time::Instant::now();
        let (deadline, handoff) = claim_deadlines(started, lease).unwrap();

        assert_eq!(deadline.duration_since(started), lease.timeout());
        assert_eq!(
            handoff.duration_since(started),
            lease.timeout() - lease.interval()
        );
        assert_eq!(deadline.duration_since(handoff), lease.interval());
    }
}
