use std::sync::Arc;

use spider::{net, scheduler};

use super::super::{Api, wire};

impl Api {
    pub(in crate::scheduler::api) async fn claim(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        let modes = self.register(worker_id, modes).await?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let body = wire::Claim {
            limit,
            worker_id: worker_id.to_string(),
            modes,
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
                .release_after_protocol_error(&response.requests, error)
                .await;
        }

        let mut ids = std::collections::HashSet::with_capacity(response.requests.len());
        for claimed in &response.requests {
            if !ids.insert(claimed.identity.id.as_str()) {
                let error = scheduler::Error::InvalidRequest {
                    id: claimed.identity.id.clone(),
                    message: "Master returned a duplicate Request in one claim".to_string(),
                };
                return self
                    .release_after_protocol_error(&response.requests, error)
                    .await;
            }
        }

        let mut requests = Vec::with_capacity(response.requests.len());
        let mut recovery_errors = Vec::new();
        for claimed in &response.requests {
            match self.restore(claimed, worker_id, &body.modes).await {
                Ok(request) => requests.push(request),
                Err(restore) => {
                    if let Err(settlement) = self.fail_restore(claimed, &restore).await {
                        recovery_errors.push((claimed.identity.clone(), restore, settlement));
                    }
                }
            }
        }

        if requests.is_empty() && !recovery_errors.is_empty() {
            let transient = recovery_errors
                .iter()
                .any(|(_, _, settlement)| settlement.is_transient());
            let message = recovery_errors
                .into_iter()
                .map(|(identity, restore, settlement)| {
                    format!(
                        "failed to restore Request {}: {restore}; failed to settle its recovery: {settlement}",
                        identity.id
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            return if transient {
                Err(scheduler::Error::Unavailable(message))
            } else {
                Err(scheduler::Error::Message(message))
            };
        }
        for (identity, restore, settlement) in recovery_errors {
            tracing::warn!(
                request_id = %identity.id,
                version = identity.version,
                worker_id = %identity.worker_id,
                restore_error = %restore,
                settlement_error = %settlement,
                "failed to settle a damaged API Scheduler claim; valid peers remain executable"
            );
        }

        Ok(requests)
    }

    async fn release_after_protocol_error<T>(
        &self,
        requests: &[wire::Claimed],
        error: scheduler::Error,
    ) -> Result<T, scheduler::Error> {
        match self.release_claim(requests).await {
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

    async fn release_claim(&self, requests: &[wire::Claimed]) -> Result<(), scheduler::Error> {
        let mut released = std::collections::HashSet::with_capacity(requests.len());
        let mut failures = Vec::new();
        let mut transient = false;

        for (index, claimed) in requests.iter().enumerate() {
            if !released.insert(claimed.identity.clone()) {
                continue;
            }
            let identity = claimed.identity.clone();
            let key = Self::invocation_key();
            if let Err(error) = self
                .client
                .post_empty("v1/worker/requests/release", &identity, Some(&key))
                .await
            {
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

    async fn fail_restore(
        &self,
        claimed: &wire::Claimed,
        error: &scheduler::Error,
    ) -> Result<(), scheduler::Error> {
        let identity = claimed.identity.clone();
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

    pub(in crate::scheduler::api) async fn pending(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        let modes = self.register(worker_id, modes).await?;
        let body = wire::Pending {
            worker_id: worker_id.to_string(),
            modes,
        };
        self.client
            .post::<_, wire::PendingResponse>("v1/worker/requests/pending", &body, None)
            .await
            .map(|response| response.pending)
    }

    async fn restore(
        &self,
        claimed: &wire::Claimed,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<net::Request, scheduler::Error> {
        validate_identity(&claimed.identity)?;
        let id = claimed.identity.id.clone();
        let snapshot = serde_json::from_value::<net::request::Snapshot>(claimed.snapshot.clone())
            .map_err(|error| scheduler::Error::InvalidRequest {
            id: id.clone(),
            message: format!("claimed Request Snapshot cannot be decoded: {error}"),
        })?;
        for (field, matches) in [
            ("id", snapshot.id == claimed.identity.id),
            ("task_id", snapshot.task_id == claimed.identity.task_id),
            ("trace_id", snapshot.trace_id == claimed.identity.trace_id),
            ("node", snapshot.node == claimed.identity.node),
        ] {
            if !matches {
                return Err(scheduler::Error::InvalidRequest {
                    id,
                    message: format!("claimed Request {field} does not match its identity"),
                });
            }
        }
        if !modes.contains(&snapshot.mode) {
            return Err(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request mode is not supported by the claiming Worker".to_string(),
            });
        }
        validate_execution(&claimed.execution, &snapshot, &claimed.identity, worker_id)?;
        if claimed.identity.worker_id != worker_id {
            return Err(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request worker_id does not match the claiming Worker".to_string(),
            });
        }

        let trace_id = claimed.identity.trace_id.clone();
        let trace = if let Some(value) = claimed.trace.clone() {
            let snapshot =
                serde_json::from_value::<spider::trace::Snapshot>(value).map_err(|error| {
                    scheduler::Error::InvalidTrace {
                        id: trace_id.clone(),
                        message: format!("claimed Trace Snapshot cannot be decoded: {error}"),
                    }
                })?;
            snapshot
                .validate()
                .map_err(|message| scheduler::Error::InvalidTrace {
                    id: trace_id.clone(),
                    message,
                })?;
            if snapshot.task_id != claimed.identity.task_id {
                return Err(scheduler::Error::InvalidRequest {
                    id,
                    message: "claimed Request task_id does not match its Trace Snapshot"
                        .to_string(),
                });
            }
            self.cache_trace(trace_id.clone(), snapshot).await?
        } else if let Some(snapshot) = self.cached_trace(&trace_id).await {
            snapshot
        } else {
            self.load_trace(&trace_id)
                .await?
                .map(Arc::new)
                .ok_or_else(|| scheduler::Error::TraceNotFound(trace_id.clone()))?
        };

        let mut request =
            snapshot
                .restore(Some(trace))
                .map_err(|message| scheduler::Error::InvalidRequest {
                    id: claimed.identity.id.clone(),
                    message,
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
            Err(scheduler::Error::InvalidRequest { .. })
        ));
    }

    #[tokio::test]
    async fn restore_rejects_an_empty_lease_and_the_same_failed_worker() {
        let api = Api::new("https://master.example.com", "token").unwrap();
        let mut empty_lease = claimed(net::Mode::Http);
        empty_lease.execution.lease_time = 0;
        assert!(
            api.restore(&empty_lease, "worker-1", &[net::Mode::Http])
                .await
                .is_err()
        );

        let mut failed = claimed(net::Mode::Http);
        failed.execution.retry_count = 1;
        failed.execution.failed_workers.push("worker-1".to_string());
        assert!(
            api.restore(&failed, "worker-1", &[net::Mode::Http])
                .await
                .is_err()
        );
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
}
