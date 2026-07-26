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
            return Err(scheduler::Error::Message(format!(
                "Master returned {} Requests for claim limit {limit}",
                response.requests.len()
            )));
        }

        let mut ids = std::collections::HashSet::with_capacity(response.requests.len());
        let mut requests = Vec::with_capacity(response.requests.len());
        let mut restore_error = None;
        for claimed in &response.requests {
            if !ids.insert(claimed.snapshot.id.as_str()) {
                restore_error = Some(scheduler::Error::InvalidRequest {
                    id: claimed.snapshot.id.clone(),
                    message: "Master returned a duplicate Request in one claim".to_string(),
                });
                break;
            }
            match self.restore(claimed, worker_id, &body.modes).await {
                Ok(request) => requests.push(request),
                Err(error) => {
                    restore_error = Some(error);
                    break;
                }
            }
        }
        if let Some(error) = restore_error {
            match self.release_claim(&response.requests).await {
                Ok(()) => return Err(error),
                Err(release) => {
                    let combined = if release.is_transient() {
                        scheduler::Error::Unavailable(format!(
                            "failed to restore claimed Request collection: {error}; failed to release the collection: {release}"
                        ))
                    } else {
                        scheduler::Error::Message(format!(
                            "failed to restore claimed Request collection: {error}; failed to release the collection: {release}"
                        ))
                    };
                    return Err(combined);
                }
            }
        }

        Ok(requests)
    }

    async fn release_claim(&self, requests: &[wire::Claimed]) -> Result<(), scheduler::Error> {
        let mut released = std::collections::HashSet::with_capacity(requests.len());
        let mut failures = Vec::new();
        let mut transient = false;

        for (index, claimed) in requests.iter().enumerate() {
            let marker = (
                claimed.snapshot.id.as_str(),
                claimed.snapshot.task_id.as_str(),
                claimed.snapshot.trace_id.as_str(),
                claimed.execution.version,
                claimed.execution.leased_by.as_str(),
                claimed.snapshot.node.as_str(),
            );
            if !released.insert(marker) {
                continue;
            }
            let identity = wire::Identity::from_claimed(claimed);
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
        let id = claimed.snapshot.id.clone();
        if !modes.contains(&claimed.snapshot.mode) {
            return Err(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request mode is not supported by the claiming Worker".to_string(),
            });
        }
        validate_execution(&claimed.execution, &claimed.snapshot, worker_id)?;
        if claimed.execution.leased_by != worker_id {
            return Err(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request leased_by does not match the claiming Worker".to_string(),
            });
        }

        let trace_id = claimed.snapshot.trace_id.clone();
        if trace_id.is_empty() {
            return Err(scheduler::Error::InvalidRequest {
                id,
                message: "claimed Request trace_id must not be empty".to_string(),
            });
        }
        let trace = if let Some(snapshot) = claimed.trace.clone() {
            snapshot
                .validate()
                .map_err(|message| scheduler::Error::InvalidTrace {
                    id: trace_id.clone(),
                    message,
                })?;
            if snapshot.task_id != claimed.snapshot.task_id {
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

        let mut request = claimed
            .snapshot
            .clone()
            .restore(Some(trace))
            .map_err(|message| scheduler::Error::InvalidRequest {
                id: claimed.snapshot.id.clone(),
                message,
            })?;
        request.state = net::State::Processing;
        request.version = claimed.execution.version;
        request.next_time = claimed.execution.next_time;
        request.leased_by = claimed.execution.leased_by.clone();
        request.lease_time = claimed.execution.lease_time;
        request.retry_count = claimed.execution.retry_count;
        request.failed_workers = claimed.execution.failed_workers.clone();
        Ok(request)
    }
}

fn validate_execution(
    execution: &wire::Execution,
    snapshot: &net::request::Snapshot,
    worker_id: &str,
) -> Result<(), scheduler::Error> {
    let id = snapshot.id.clone();
    if execution.version <= 0 {
        return Err(scheduler::Error::InvalidRequest {
            id,
            message: "claimed Request version must be positive".to_string(),
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
        wire::Claimed {
            snapshot: net::request::Snapshot::try_from(request).unwrap(),
            execution: wire::Execution {
                version: 1,
                next_time: 0,
                leased_by: "worker-1".to_string(),
                lease_time: 1,
                retry_count: 0,
                failed_workers: Vec::new(),
            },
            trace: Some(trace::Snapshot::code("task-1")),
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
        claimed.snapshot.task_id = "task-1".to_string();
        claimed.snapshot.trace_id = "trace-1".to_string();
        claimed.trace = Some(trace::Snapshot::code("task-2"));

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
        claimed.snapshot.trace_id.clear();

        assert!(
            api.restore(&claimed, "worker-1", &[net::Mode::Http])
                .await
                .is_err()
        );
        assert!(api.cached_trace("").await.is_none());
    }
}
