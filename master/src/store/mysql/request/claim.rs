use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use sqlx::AssertSqlSafe;

use super::super::MySql;
use super::super::operation;
use super::super::time::now_millis;
use super::super::trace;
use super::super::validate::{namespace as validate_namespace, worker_id as validate_worker_id};
use super::super::worker::canonical_modes;
use super::{State, parse_stored, recover};
use crate::{Error, wire};

const EMPTY_RESPONSE_BYTES: usize = b"{\"requests\":[]}".len();

struct Candidate {
    request: State,
    version: i64,
    trace: Option<Arc<spider::trace::Snapshot>>,
}

impl Candidate {
    fn claimed(&self, worker_id: &str, lease_time: i64, include_trace: bool) -> wire::Claimed {
        wire::Claimed {
            snapshot: self.request.snapshot.clone(),
            execution: wire::Execution {
                version: self.version,
                next_time: self.request.next_time,
                leased_by: worker_id.to_string(),
                lease_time,
                retry_count: self.request.retry_count,
                failed_workers: self.request.failed_workers.clone(),
            },
            trace: if include_trace {
                self.trace.as_deref().cloned()
            } else {
                None
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Capacity {
    Accepted,
    Full,
    Oversized,
}

struct Size {
    bytes: usize,
    count: usize,
    max: usize,
}

impl Size {
    fn new(max: usize) -> Result<Self, Error> {
        if EMPTY_RESPONSE_BYTES > max {
            return Err(Error::ResponseTooLarge { max });
        }
        Ok(Self {
            bytes: EMPTY_RESPONSE_BYTES,
            count: 0,
            max,
        })
    }

    fn include(&mut self, request: usize) -> Result<Capacity, Error> {
        let single = EMPTY_RESPONSE_BYTES
            .checked_add(request)
            .ok_or_else(|| Error::Invalid("claim response size overflow".to_string()))?;
        if single > self.max {
            return Ok(Capacity::Oversized);
        }
        let separator = usize::from(self.count > 0);
        let next = self
            .bytes
            .checked_add(separator)
            .and_then(|bytes| bytes.checked_add(request))
            .ok_or_else(|| Error::Invalid("claim response size overflow".to_string()))?;
        if next > self.max {
            return Ok(Capacity::Full);
        }
        self.bytes = next;
        self.count += 1;
        Ok(Capacity::Accepted)
    }
}

struct Selection {
    size: Size,
    traces: HashSet<String>,
}

impl Selection {
    fn new(max: usize) -> Result<Self, Error> {
        Ok(Self {
            size: Size::new(max)?,
            traces: HashSet::new(),
        })
    }

    fn needs_trace(&self, trace_id: &str) -> bool {
        !trace_id.is_empty() && !self.traces.contains(trace_id)
    }

    fn include(&mut self, mut request: wire::Claimed) -> Result<(Capacity, wire::Claimed), Error> {
        if self.traces.contains(&request.snapshot.trace_id) {
            request.trace = None;
        }
        let mut capacity = self.measure(&request)?;
        if capacity != Capacity::Accepted && request.trace.is_some() {
            request.trace = None;
            capacity = self.measure(&request)?;
        }
        if capacity == Capacity::Accepted && !request.snapshot.trace_id.is_empty() {
            self.traces.insert(request.snapshot.trace_id.clone());
        }
        Ok((capacity, request))
    }

    fn measure(&mut self, request: &wire::Claimed) -> Result<Capacity, Error> {
        self.size.include(serde_json::to_vec(request)?.len())
    }
}

impl MySql {
    pub(crate) async fn claim(
        &self,
        namespace: &str,
        key: &str,
        body: &wire::Claim,
    ) -> Result<wire::Claims, Error> {
        validate_namespace(namespace)?;
        if body.limit == 0 || body.limit > 1024 {
            return Err(Error::Invalid(
                "claim limit must be between 1 and 1024".to_string(),
            ));
        }
        validate_worker_id(&body.worker_id)?;
        let modes = canonical_modes(&body.modes)?;
        let digest = operation::digest(&(body.limit, &body.worker_id, &modes))?;
        let mut tx = self.pool.begin().await?;
        if let Some(previous) =
            operation::reserve::<wire::Claims>(&mut tx, namespace, "claim", key, &digest).await?
        {
            self.ensure_worker(&mut tx, namespace, &body.worker_id, &modes)
                .await?;
            tx.commit().await?;
            return Ok(previous);
        }
        self.ensure_worker(&mut tx, namespace, &body.worker_id, &modes)
            .await?;
        let observed_at = now_millis();
        recover::expired(
            &mut tx,
            namespace,
            self.lease_timeout_ms,
            self.worker_timeout_ms,
            observed_at,
            self.recovery_limit,
        )
        .await?;
        let placeholders = std::iter::repeat_n("?", modes.len())
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "SELECT id, task_id, trace_id, node, mode, state, version, priority, snapshot, \
             snapshot_digest, next_time, leased_by, lease_time, retry_count, max_retry_count, \
             failed_workers, ack_version FROM requests \
             WHERE namespace = ? AND state = 0 AND next_time <= ? AND mode IN ({placeholders}) \
             AND JSON_CONTAINS(failed_workers, JSON_QUOTE(?)) = 0 \
             ORDER BY priority DESC, sequence ASC LIMIT ? FOR UPDATE SKIP LOCKED"
        );
        let mut request = sqlx::query(AssertSqlSafe(query))
            .bind(namespace)
            .bind(observed_at);
        for mode in &modes {
            request = request.bind(mode);
        }
        let rows = request
            .bind(&body.worker_id)
            .bind(body.limit as u64)
            .fetch_all(&mut *tx)
            .await?;
        let mut traces = HashMap::<String, Option<Arc<spider::trace::Snapshot>>>::new();
        let mut candidates = Vec::with_capacity(rows.len());
        for row in rows {
            let stored = parse_stored(row)?;
            let parsed = match stored.pending() {
                Ok(request) => request,
                Err(error) => {
                    recover::reject(
                        &mut tx,
                        namespace,
                        &stored,
                        observed_at,
                        &format!("invalid pending Request: {error}"),
                    )
                    .await?;
                    continue;
                }
            };
            let Some(version) = parsed.version.checked_add(1) else {
                recover::reject(
                    &mut tx,
                    namespace,
                    &stored,
                    observed_at,
                    "invalid pending Request: version overflow",
                )
                .await?;
                continue;
            };
            let trace = if let Some(trace) = traces.get(&parsed.trace_id) {
                trace.clone()
            } else {
                match trace::load(&mut tx, namespace, &parsed.trace_id).await {
                    Ok(trace) => {
                        let trace = trace.map(Arc::new);
                        traces.insert(parsed.trace_id.clone(), trace.clone());
                        trace
                    }
                    Err(error) if recover::is_stored_data_error(&error) => {
                        recover::reject(
                            &mut tx,
                            namespace,
                            &stored,
                            observed_at,
                            &format!("invalid pending Request: {error}"),
                        )
                        .await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            };
            if let Err(error) = trace::validate_snapshot(&parsed.snapshot, trace.as_deref()) {
                recover::reject(
                    &mut tx,
                    namespace,
                    &stored,
                    observed_at,
                    &format!("invalid pending Request: {error}"),
                )
                .await?;
                continue;
            }
            candidates.push(Candidate {
                request: parsed,
                version,
                trace,
            });
        }

        let lease_time = now_millis();
        let mut selection = Selection::new(self.max_response_bytes)?;
        let mut claimed = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let include_trace = selection.needs_trace(&candidate.request.trace_id);
            let request = candidate.claimed(&body.worker_id, lease_time, include_trace);
            let (capacity, request) = selection.include(request)?;
            match capacity {
                Capacity::Oversized => {
                    recover::reject_state(
                        &mut tx,
                        namespace,
                        &candidate.request,
                        lease_time,
                        &format!(
                            "claimed Request exceeds the configured {} byte API response limit",
                            self.max_response_bytes
                        ),
                    )
                    .await?;
                    continue;
                }
                Capacity::Full => break,
                Capacity::Accepted => {}
            }
            let updated = sqlx::query(
                "UPDATE requests SET state = 1, leased_by = ?, lease_time = ?, version = ?, \
                 ack_version = NULL, updated_time = ? WHERE namespace = ? AND id = ? AND state = 0",
            )
            .bind(&body.worker_id)
            .bind(lease_time)
            .bind(candidate.version)
            .bind(lease_time)
            .bind(namespace)
            .bind(&candidate.request.id)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(Error::Unavailable(format!(
                    "claim lost Request state transition: {}",
                    candidate.request.id
                )));
            }
            claimed.push(request);
        }
        let result = wire::Claims { requests: claimed };
        operation::record(&mut tx, namespace, "claim", key, &digest, &result).await?;
        tx.commit().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_uses_the_exact_wire_size() {
        let request = spider::net::Request::follow("https://example.com/article")
            .unwrap()
            .node("detail");
        let claimed = wire::Claimed {
            snapshot: spider::net::request::Snapshot::try_from(request).unwrap(),
            execution: wire::Execution {
                version: 1,
                next_time: 0,
                leased_by: "worker-1".to_string(),
                lease_time: 1,
                retry_count: 0,
                failed_workers: Vec::new(),
            },
            trace: None,
        };
        let bytes = serde_json::to_vec(&wire::Claims {
            requests: vec![claimed.clone()],
        })
        .unwrap()
        .len();

        let request_bytes = serde_json::to_vec(&claimed).unwrap().len();
        let mut exact = Size::new(bytes).unwrap();
        assert_eq!(exact.include(request_bytes).unwrap(), Capacity::Accepted);

        let mut short = Size::new(bytes - 1).unwrap();
        assert_eq!(short.include(request_bytes).unwrap(), Capacity::Oversized);

        let mut full = Size::new(bytes).unwrap();
        assert_eq!(full.include(request_bytes).unwrap(), Capacity::Accepted);
        assert_eq!(full.include(request_bytes).unwrap(), Capacity::Full);
    }

    #[test]
    fn selection_omits_trace_before_rejecting_a_valid_request() {
        let mut request = spider::net::Request::follow("https://example.com/article")
            .unwrap()
            .with_id("request-1")
            .node("detail");
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        let mut trace = spider::trace::Snapshot::code("task-1");
        trace.attachment = Some(serde_json::Value::String("x".repeat(2048)));
        let first = wire::Claimed {
            snapshot: spider::net::request::Snapshot::try_from(request).unwrap(),
            execution: wire::Execution {
                version: 1,
                next_time: 0,
                leased_by: "worker-1".to_string(),
                lease_time: 1,
                retry_count: 0,
                failed_workers: Vec::new(),
            },
            trace: Some(trace),
        };
        let mut first_without_trace = first.clone();
        first_without_trace.trace = None;
        let mut second = first_without_trace.clone();
        second.snapshot.id = "request-2".to_string();
        let requests_bytes = serde_json::to_vec(&wire::Claims {
            requests: vec![first_without_trace, second.clone()],
        })
        .unwrap()
        .len();
        let trace_bytes = serde_json::to_vec(&first.trace).unwrap().len();
        let max = requests_bytes.max(trace_bytes);
        assert!(trace_bytes <= max);
        assert!(serde_json::to_vec(&second).unwrap().len() + EMPTY_RESPONSE_BYTES <= max);
        assert!(
            serde_json::to_vec(&wire::Claims {
                requests: vec![first.clone()],
            })
            .unwrap()
            .len()
                > max
        );

        let mut selection = Selection::new(max).unwrap();
        let (capacity, first) = selection.include(first).unwrap();
        assert_eq!(capacity, Capacity::Accepted);
        assert!(first.trace.is_none());
        assert!(!selection.needs_trace("trace-1"));

        second.trace = Some(spider::trace::Snapshot::code("task-1"));
        let (capacity, second) = selection.include(second).unwrap();
        assert_eq!(capacity, Capacity::Accepted);
        assert!(second.trace.is_none());
    }
}
