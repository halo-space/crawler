use std::collections::{HashMap, HashSet};

use serde_json::Value;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Transaction};

use super::super::MySql;
use super::super::operation;
use super::super::trace;
use super::super::validate::namespace as validate_namespace;
use super::{FAILED, State, Stored, parse_stored, queue, settle};
use crate::{Error, types};

struct Failure<'a> {
    id: &'a str,
    task_id: &'a str,
    trace_id: &'a str,
    node: &'a str,
    version: i64,
    state: i8,
    leased_by: &'a str,
    retry_count: i32,
    max_retry_count: i32,
    failed_workers: Vec<String>,
}

impl<'a> Failure<'a> {
    fn stored(request: &'a Stored) -> Self {
        Self {
            id: &request.id,
            task_id: &request.task_id,
            trace_id: &request.trace_id,
            node: &request.node,
            version: request.version,
            state: request.state,
            leased_by: &request.leased_by,
            retry_count: request.retry_count,
            max_retry_count: retry_limit(&request.snapshot, request.max_retry_count),
            failed_workers: request.failed_workers(),
        }
    }

    fn state(request: &'a State) -> Self {
        Self {
            id: &request.id,
            task_id: &request.task_id,
            trace_id: &request.trace_id,
            node: &request.node,
            version: request.version,
            state: request.state,
            leased_by: &request.leased_by,
            retry_count: request.retry_count,
            max_retry_count: request.snapshot.max_retry_count,
            failed_workers: request.failed_workers.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Report {
    pub pending: u64,
    pub failed: u64,
}

impl MySql {
    pub async fn recover(&self, namespace: &str, now: i64) -> Result<Report, Error> {
        validate_namespace(namespace)?;
        let mut tx = self.pool.begin().await?;
        let report = expired(
            &mut tx,
            namespace,
            self.lease_timeout_ms,
            self.worker_timeout_ms,
            now,
            self.recovery_limit,
        )
        .await?;
        tx.commit().await?;
        Ok(report)
    }
}

pub(super) async fn expired(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    lease_timeout_ms: i64,
    worker_timeout_ms: i64,
    now: i64,
    limit: usize,
) -> Result<Report, Error> {
    let lease_deadline = now.saturating_sub(lease_timeout_ms);
    let worker_deadline = now.saturating_sub(worker_timeout_ms);
    let limit = u64::try_from(limit)
        .map_err(|_| Error::Invalid("recovery limit exceeds u64".to_string()))?;
    let rows = sqlx::query(
        "SELECT r.id, r.task_id, r.trace_id, r.node, r.mode, r.state, r.version, r.priority, \
         r.snapshot, r.snapshot_digest, r.next_time, r.leased_by, r.lease_time, r.retry_count, \
         r.max_retry_count, r.failed_workers, r.ack_version FROM requests AS r \
         WHERE r.namespace = ? AND r.state = 1 AND (r.lease_time <= ? \
         OR (r.leased_by <> '' AND NOT EXISTS (SELECT 1 FROM workers AS w \
         WHERE w.namespace = r.namespace AND w.id = r.leased_by AND w.last_heartbeat > ?))) \
         ORDER BY r.lease_time ASC, r.id ASC LIMIT ? FOR UPDATE SKIP LOCKED",
    )
    .bind(namespace)
    .bind(lease_deadline)
    .bind(worker_deadline)
    .bind(limit)
    .fetch_all(&mut **tx)
    .await?;
    let mut report = Report::default();
    for row in rows {
        let stored = parse_stored(row)?;
        let reason = if stored.lease_time <= lease_deadline {
            "lease expired"
        } else {
            "worker heartbeat expired"
        };
        let request = match stored.processing() {
            Ok(request) => request,
            Err(error) => {
                reject(tx, namespace, &stored, now, &format!("{reason}: {error}")).await?;
                report.failed += 1;
                continue;
            }
        };
        let trace = match trace::load(tx, namespace, &request.trace_id).await {
            Ok(trace) => trace,
            Err(error) if is_stored_data_error(&error) => {
                reject(tx, namespace, &stored, now, &format!("{reason}: {error}")).await?;
                report.failed += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        if let Err(error) = trace::validate_snapshot(&request.snapshot, trace.as_ref()) {
            reject(tx, namespace, &stored, now, &format!("{reason}: {error}")).await?;
            report.failed += 1;
            continue;
        }
        if request.ack_version != Some(request.version) {
            return_pending(tx, namespace, &request.id, now).await?;
            report.pending += 1;
            continue;
        }
        let Some(retry) = request.retry_count.checked_add(1) else {
            reject(
                tx,
                namespace,
                &stored,
                now,
                &format!("{reason}: request retry overflow"),
            )
            .await?;
            report.failed += 1;
            continue;
        };
        let mut failed_workers = request.failed_workers.clone();
        settle::append_worker(&mut failed_workers, &request.leased_by);
        let terminal = retry >= request.max_retry_count;
        settle::update_retry(
            tx,
            namespace,
            &request,
            retry,
            &failed_workers,
            terminal,
            now,
        )
        .await?;
        let completion = types::Completion {
            identity: types::Identity {
                id: request.id.clone(),
                task_id: request.task_id.clone(),
                trace_id: request.trace_id.clone(),
                version: request.version,
                worker_id: request.leased_by.clone(),
                node: request.node.clone(),
            },
            stats: HashMap::new(),
            start_time: now,
            end_time: now,
            error: Some(reason.to_string()),
        };
        let completion_digest = operation::digest(&completion)?;
        settle::insert_completion(tx, namespace, &completion, &completion_digest, false).await?;
        if terminal {
            report.failed += 1;
        } else {
            report.pending += 1;
        }
    }
    Ok(report)
}

pub(super) fn is_stored_data_error(error: &Error) -> bool {
    !matches!(
        error,
        Error::Database(_) | Error::Migration(_) | Error::Unavailable(_)
    )
}

pub(super) async fn reject(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    request: &Stored,
    now: i64,
    error: &str,
) -> Result<(), Error> {
    fail(tx, namespace, Failure::stored(request), now, error).await
}

pub(super) async fn reject_state(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    request: &State,
    now: i64,
    error: &str,
) -> Result<(), Error> {
    fail(tx, namespace, Failure::state(request), now, error).await
}

async fn fail(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    request: Failure<'_>,
    now: i64,
    error: &str,
) -> Result<(), Error> {
    let failed_workers = bounded_workers(request.failed_workers, request.leased_by);
    let maximum = spider::net::request::MAX_RETRY_COUNT;
    let retry_count = request
        .max_retry_count
        .clamp(1, maximum)
        .max(request.retry_count)
        .clamp(1, maximum)
        .max(failed_workers.len() as i32)
        .max(1);
    let updated = sqlx::query(
        "UPDATE requests SET state = ?, retry_count = ?, max_retry_count = ?, leased_by = ?, \
         lease_time = ?, ack_version = NULL, next_time = 0, failed_workers = ?, updated_time = ? \
         WHERE namespace = ? AND id = ? AND version = ? AND state = ?",
    )
    .bind(FAILED)
    .bind(retry_count)
    .bind(retry_count)
    .bind(request.leased_by)
    .bind(now)
    .bind(Json(&failed_workers))
    .bind(now)
    .bind(namespace)
    .bind(request.id)
    .bind(request.version)
    .bind(request.state)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unavailable(format!(
            "failed to reject Request state transition: {}",
            request.id
        )));
    }
    let completion = types::Completion {
        identity: types::Identity {
            id: request.id.to_string(),
            task_id: request.task_id.to_string(),
            trace_id: request.trace_id.to_string(),
            version: request.version,
            worker_id: request.leased_by.to_string(),
            node: request.node.to_string(),
        },
        stats: HashMap::new(),
        start_time: now,
        end_time: now,
        error: Some(error.to_string()),
    };
    let digest = operation::digest(&completion)?;
    sqlx::query(
        "INSERT IGNORE INTO request_completions (namespace, request_id, version, task_id, trace_id, \
         node, worker_id, state, error, payload_digest, created_time) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(namespace)
    .bind(&completion.identity.id)
    .bind(completion.identity.version)
    .bind(&completion.identity.task_id)
    .bind(&completion.identity.trace_id)
    .bind(&completion.identity.node)
    .bind(&completion.identity.worker_id)
    .bind(FAILED)
    .bind(&completion.error)
    .bind(digest)
    .bind(now)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn retry_limit(snapshot: &Value, projected: i32) -> i32 {
    let maximum = spider::net::request::MAX_RETRY_COUNT;
    snapshot
        .get("max_retry_count")
        .and_then(Value::as_i64)
        .map(|value| value.clamp(1, i64::from(maximum)) as i32)
        .unwrap_or_else(|| projected.clamp(1, maximum))
}

fn bounded_workers(values: Vec<String>, current: &str) -> Vec<String> {
    let limit = spider::net::request::MAX_RETRY_COUNT as usize;
    let history_limit = limit.saturating_sub(usize::from(!current.is_empty()));
    let mut seen = HashSet::with_capacity(values.len().min(limit));
    let mut workers = values
        .into_iter()
        .filter(|worker| !worker.is_empty() && seen.insert(worker.clone()))
        .take(history_limit)
        .collect::<Vec<_>>();
    if !current.is_empty() && seen.insert(current.to_string()) {
        workers.push(current.to_string());
    }
    workers
}

async fn return_pending(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    id: &str,
    now: i64,
) -> Result<(), Error> {
    let sequence = queue::next(tx, namespace).await?;
    let updated = sqlx::query(
        "UPDATE requests SET state = 0, leased_by = '', lease_time = 0, ack_version = NULL, \
         next_time = 0, sequence = ?, updated_time = ? \
         WHERE namespace = ? AND id = ? AND state = 1",
    )
    .bind(sequence)
    .bind(now)
    .bind(namespace)
    .bind(id)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unavailable(format!(
            "recovery lost Request state transition: {id}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn retry_limit_prefers_the_snapshot_and_bounds_corrupt_values() {
        assert_eq!(retry_limit(&json!({ "max_retry_count": 3 }), 99), 3);
        assert_eq!(retry_limit(&json!({ "max_retry_count": 0 }), 99), 1);
        assert_eq!(
            retry_limit(&json!({ "broken": true }), i32::MAX),
            spider::net::request::MAX_RETRY_COUNT
        );
    }
}
