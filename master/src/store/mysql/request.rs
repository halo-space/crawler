use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Row, Transaction};

use super::MySql;
use super::operation;
use super::time::now_millis;
use super::trace;
use super::validate::{identifier, identity as validate_identity, namespace as validate_namespace};
use crate::{Error, types};

mod claim;
mod lease;
mod queue;
mod recover;
mod settle;

pub(super) const PENDING: i8 = 0;
pub(super) const PROCESSING: i8 = 1;
pub(super) const DONE: i8 = 2;
pub(super) const FAILED: i8 = 3;

#[derive(Debug)]
pub(super) struct State {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    state: i8,
    version: i64,
    priority: i32,
    snapshot: spider::net::request::Snapshot,
    snapshot_digest: String,
    next_time: i64,
    leased_by: String,
    lease_time: i64,
    retry_count: i32,
    max_retry_count: i32,
    failed_workers: Vec<String>,
    ack_version: Option<i64>,
}

#[derive(Debug)]
struct Stored {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    state: i8,
    version: i64,
    priority: i32,
    snapshot: Value,
    snapshot_digest: String,
    next_time: i64,
    leased_by: String,
    lease_time: i64,
    retry_count: i32,
    max_retry_count: i32,
    failed_workers: Value,
    ack_version: Option<i64>,
}

impl MySql {
    pub(crate) async fn push(&self, namespace: &str, body: &types::Push) -> Result<(), Error> {
        validate_namespace(namespace)?;
        for request in &body.requests {
            if (!body.context.task_id.is_empty() && request.task_id != body.context.task_id)
                || (!body.context.trace_id.is_empty() && request.trace_id != body.context.trace_id)
            {
                return Err(Error::Invalid(format!(
                    "child request {} does not match parent context",
                    request.id
                )));
            }
        }
        let mut tx = self.pool.begin().await?;
        let requests = plan(&mut tx, namespace, &body.requests).await?;
        if requests.is_empty() {
            tx.commit().await?;
            return Ok(());
        }
        let parent = if body.context.id.is_empty() {
            None
        } else {
            identifier(&body.context.id, "request id")?;
            Some(load(&mut tx, namespace, &body.context.id).await?)
        };
        let requests = recheck(&mut tx, namespace, requests).await?;
        if requests.is_empty() {
            tx.commit().await?;
            return Ok(());
        }
        if let Some(parent) = parent {
            validate_identity(&body.context)?;
            verify_ownership(&parent, &body.context)?;
            validate_processing(&parent)?;
            verify_lease(&parent, self.lease_timeout_ms)?;
            if parent.ack_version != Some(body.context.version) {
                return Err(Error::NotAcknowledged(body.context.id.clone()));
            }
        }
        write(&mut tx, namespace, requests).await?;
        tx.commit().await?;
        Ok(())
    }
}

pub(super) async fn insert_values(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    requests: &[spider::net::Request],
) -> Result<(), Error> {
    let snapshots = requests
        .iter()
        .cloned()
        .map(spider::net::request::Snapshot::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| Error::Invalid(format!("invalid Request Snapshot: {message}")))?;
    insert(tx, namespace, &snapshots).await
}

pub(super) async fn insert(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    requests: &[spider::net::request::Snapshot],
) -> Result<(), Error> {
    let requests = plan(tx, namespace, requests).await?;
    let requests = recheck(tx, namespace, requests).await?;
    write(tx, namespace, requests).await
}

type Candidate<'a> = (usize, &'a spider::net::request::Snapshot, String);

async fn plan<'a>(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    requests: &'a [spider::net::request::Snapshot],
) -> Result<Vec<Candidate<'a>>, Error> {
    // Validate Request IDs in a stable order. The eventual writes return to the caller's order,
    // which defines FIFO within one submission.
    let mut ordered = requests.iter().enumerate().collect::<Vec<_>>();
    ordered.sort_unstable_by(|(_, left), (_, right)| left.id.cmp(&right.id));
    let mut seen = HashSet::with_capacity(requests.len());
    let mut checked = Vec::with_capacity(requests.len());
    for (position, snapshot) in ordered {
        if !seen.insert(snapshot.id.clone()) {
            return Err(Error::Invalid(format!(
                "duplicate Request Snapshot id: {}",
                snapshot.id
            )));
        }
        let snapshot_digest = operation::hex(
            &snapshot
                .digest()
                .map_err(|error| Error::Invalid(error.to_string()))?,
        );
        let existing = sqlx::query_scalar::<_, String>(
            "SELECT snapshot_digest FROM requests WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(&snapshot.id)
        .fetch_optional(&mut **tx)
        .await?;
        if existing
            .as_ref()
            .is_some_and(|existing| existing != &snapshot_digest)
        {
            return Err(Error::Conflict(format!(
                "Request Snapshot conflicts with existing request {}",
                snapshot.id
            )));
        }
        checked.push((position, snapshot, snapshot_digest, existing.is_none()));
    }

    let mut traces = HashMap::<String, Arc<spider::trace::Snapshot>>::new();
    for (_, snapshot, _, _) in &checked {
        let trace = if let Some(trace) = traces.get(&snapshot.trace_id) {
            Arc::clone(trace)
        } else {
            let trace = trace::load(tx, namespace, &snapshot.trace_id).await?;
            let trace = Arc::new(trace);
            traces.insert(snapshot.trace_id.clone(), Arc::clone(&trace));
            trace
        };
        if trace.task_id != snapshot.task_id {
            return Err(Error::Identity {
                id: snapshot.id.clone(),
                field: "task_id",
            });
        }
        validate_new_snapshot(snapshot, &trace)?;
    }

    Ok(checked
        .into_iter()
        .filter_map(|(position, snapshot, digest, missing)| {
            missing.then_some((position, snapshot, digest))
        })
        .collect())
}

async fn recheck<'a>(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    requests: Vec<Candidate<'a>>,
) -> Result<Vec<Candidate<'a>>, Error> {
    if requests.is_empty() {
        return Ok(requests);
    }
    // The per-namespace counter serializes the current-read recheck with all framework Request
    // inserts. This avoids missing-row gap locks while preserving atomic conflict detection.
    queue::lock(tx, namespace).await?;
    let mut ready = Vec::with_capacity(requests.len());
    for candidate in requests {
        let (_, snapshot, snapshot_digest) = &candidate;
        let existing = sqlx::query_scalar::<_, String>(
            "SELECT snapshot_digest FROM requests WHERE namespace = ? AND id = ? FOR UPDATE",
        )
        .bind(namespace)
        .bind(&snapshot.id)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(existing) = existing {
            if existing.as_str() != snapshot_digest.as_str() {
                return Err(Error::Conflict(format!(
                    "Request Snapshot conflicts with existing request {}",
                    snapshot.id
                )));
            }
        } else {
            ready.push(candidate);
        }
    }
    Ok(ready)
}

async fn write(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    mut requests: Vec<Candidate<'_>>,
) -> Result<(), Error> {
    requests.sort_unstable_by_key(|(position, _, _)| *position);
    let sequences = queue::allocate(tx, namespace, requests.len()).await?;
    let now = now_millis();
    for ((_, snapshot, snapshot_digest), sequence) in requests.into_iter().zip(sequences) {
        sqlx::query(
            r#"INSERT INTO requests (
                namespace, id, task_id, trace_id, node, mode, state, version, priority,
                snapshot, snapshot_digest, next_time, leased_by, lease_time, retry_count,
                max_retry_count, failed_workers, ack_version, created_time, updated_time, sequence
            ) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, '', 0, ?, ?, ?, NULL, ?, ?, ?)"#,
        )
        .bind(namespace)
        .bind(&snapshot.id)
        .bind(&snapshot.task_id)
        .bind(&snapshot.trace_id)
        .bind(&snapshot.node)
        .bind(mode_name(&snapshot.mode))
        .bind(snapshot.version)
        .bind(snapshot.priority)
        .bind(Json(snapshot))
        .bind(snapshot_digest)
        .bind(snapshot.next_time)
        .bind(snapshot.retry_count)
        .bind(snapshot.max_retry_count)
        .bind(Json(&snapshot.failed_workers))
        .bind(now)
        .bind(now)
        .bind(sequence)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

pub(super) async fn load(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    id: &str,
) -> Result<State, Error> {
    let row = sqlx::query(
        "SELECT id, task_id, trace_id, node, mode, state, version, priority, snapshot, \
         snapshot_digest, next_time, leased_by, lease_time, retry_count, max_retry_count, \
         failed_workers, ack_version FROM requests \
         WHERE namespace = ? AND id = ? FOR UPDATE",
    )
    .bind(namespace)
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| Error::RequestNotFound(id.to_string()))?;
    let request = parse_stored(row)?.row()?;
    validate_stored(&request)?;
    Ok(request)
}

pub(super) fn mode_name(mode: &spider::net::Mode) -> &'static str {
    match mode {
        spider::net::Mode::Http => "http",
        spider::net::Mode::Browser => "browser",
    }
}

pub(super) fn validate_new_snapshot(
    snapshot: &spider::net::request::Snapshot,
    trace: &spider::trace::Snapshot,
) -> Result<(), Error> {
    trace::validate_snapshot(snapshot, trace)?;
    identifier(&snapshot.id, "Request Snapshot id")?;
    identifier(&snapshot.node, "Request Snapshot node")?;
    identifier(&snapshot.task_id, "Request Snapshot task_id")?;
    identifier(&snapshot.trace_id, "Request Snapshot trace_id")?;
    if snapshot.version != 0 {
        return Err(Error::Invalid(
            "new Request Snapshot version must be 0".to_string(),
        ));
    }
    if snapshot.retry_count != 0 || !snapshot.failed_workers.is_empty() {
        return Err(Error::Invalid(
            "new Request Snapshot retry state must be empty".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn verify_ownership(request: &State, identity: &types::Identity) -> Result<(), Error> {
    if request.id != identity.id {
        return Err(Error::RequestNotFound(identity.id.clone()));
    }
    for (field, matches) in [
        ("task_id", request.task_id == identity.task_id),
        ("trace_id", request.trace_id == identity.trace_id),
        ("node", request.node == identity.node),
    ] {
        if !matches {
            return Err(Error::Identity {
                id: identity.id.clone(),
                field,
            });
        }
    }
    if request.version != identity.version {
        return Err(Error::Version(identity.id.clone()));
    }
    if request.leased_by != identity.worker_id {
        return Err(Error::Lease(identity.id.clone()));
    }
    if request.state != PROCESSING {
        return Err(Error::State(identity.id.clone()));
    }
    Ok(())
}

pub(super) fn verify_lease(request: &State, timeout_ms: i64) -> Result<(), Error> {
    if now_millis().saturating_sub(request.lease_time) >= timeout_ms {
        return Err(Error::LeaseExpired(request.id.clone()));
    }
    Ok(())
}

fn parse_stored(row: MySqlRow) -> Result<Stored, Error> {
    let snapshot: Json<Value> = row.try_get("snapshot")?;
    let failed_workers: Json<Value> = row.try_get("failed_workers")?;
    Ok(Stored {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        trace_id: row.try_get("trace_id")?,
        node: row.try_get("node")?,
        mode: row.try_get("mode")?,
        state: row.try_get("state")?,
        version: row.try_get("version")?,
        priority: row.try_get("priority")?,
        snapshot: snapshot.0,
        snapshot_digest: row.try_get("snapshot_digest")?,
        next_time: row.try_get("next_time")?,
        leased_by: row.try_get("leased_by")?,
        lease_time: row.try_get("lease_time")?,
        retry_count: row.try_get("retry_count")?,
        max_retry_count: row.try_get("max_retry_count")?,
        failed_workers: failed_workers.0,
        ack_version: row.try_get("ack_version")?,
    })
}

impl Stored {
    fn processing(&self) -> Result<State, Error> {
        let request = self.row()?;
        validate_stored(&request)?;
        validate_processing(&request)?;
        Ok(request)
    }

    fn pending(&self) -> Result<State, Error> {
        let request = self.row()?;
        validate_stored(&request)?;
        validate_pending(&request)?;
        Ok(request)
    }

    fn row(&self) -> Result<State, Error> {
        let request = State {
            id: self.id.clone(),
            task_id: self.task_id.clone(),
            trace_id: self.trace_id.clone(),
            node: self.node.clone(),
            mode: self.mode.clone(),
            state: self.state,
            version: self.version,
            priority: self.priority,
            snapshot: serde_json::from_value(self.snapshot.clone())?,
            snapshot_digest: self.snapshot_digest.clone(),
            next_time: self.next_time,
            leased_by: self.leased_by.clone(),
            lease_time: self.lease_time,
            retry_count: self.retry_count,
            max_retry_count: self.max_retry_count,
            failed_workers: serde_json::from_value(self.failed_workers.clone())?,
            ack_version: self.ack_version,
        };
        Ok(request)
    }

    fn failed_workers(&self) -> Vec<String> {
        serde_json::from_value::<Vec<String>>(self.failed_workers.clone()).unwrap_or_default()
    }
}

fn validate_stored(request: &State) -> Result<(), Error> {
    if !matches!(request.state, PENDING | PROCESSING | DONE | FAILED)
        || request.id != request.snapshot.id
        || request.task_id != request.snapshot.task_id
        || request.trace_id != request.snapshot.trace_id
        || request.node != request.snapshot.node
        || request.mode != mode_name(&request.snapshot.mode)
        || request.priority != request.snapshot.priority
        || request.max_retry_count != request.snapshot.max_retry_count
        || !(1..=spider::net::request::MAX_RETRY_COUNT).contains(&request.snapshot.max_retry_count)
        || request.snapshot_digest
            != operation::hex(
                &request
                    .snapshot
                    .digest()
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            )
    {
        return Err(Error::Invalid(format!(
            "Request storage projections are invalid: {}",
            request.id
        )));
    }
    Ok(())
}

pub(super) fn validate_processing(request: &State) -> Result<(), Error> {
    if request.state != PROCESSING
        || request.version <= 0
        || request.next_time < 0
        || request.lease_time < 0
        || request.leased_by.is_empty()
        || request.retry_count < 0
        || request.max_retry_count <= 0
        || request.max_retry_count > spider::net::request::MAX_RETRY_COUNT
        || request.retry_count >= request.max_retry_count
    {
        return Err(Error::Invalid(format!(
            "Request execution state is invalid: {}",
            request.id
        )));
    }
    let mut workers = HashSet::with_capacity(request.failed_workers.len());
    if request
        .failed_workers
        .iter()
        .any(|worker| worker.is_empty() || !workers.insert(worker))
        || request.failed_workers.len() > request.retry_count as usize
    {
        return Err(Error::Invalid(format!(
            "Request failed_workers are invalid: {}",
            request.id
        )));
    }
    Ok(())
}

fn validate_pending(request: &State) -> Result<(), Error> {
    if request.state != PENDING
        || request.version < 0
        || request.next_time < 0
        || !request.leased_by.is_empty()
        || request.lease_time != 0
        || request.ack_version.is_some()
        || request.retry_count < 0
        || request.max_retry_count <= 0
        || request.max_retry_count > spider::net::request::MAX_RETRY_COUNT
        || request.retry_count >= request.max_retry_count
    {
        return Err(Error::Invalid(format!(
            "Request pending state is invalid: {}",
            request.id
        )));
    }
    let mut workers = HashSet::with_capacity(request.failed_workers.len());
    if request
        .failed_workers
        .iter()
        .any(|worker| worker.is_empty() || !workers.insert(worker))
        || request.failed_workers.len() > request.retry_count as usize
    {
        return Err(Error::Invalid(format!(
            "Request failed_workers are invalid: {}",
            request.id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn processing_rejects_worker_history_beyond_retry_count() {
        let valid = processing(1, vec!["worker-a"]);
        assert!(valid.processing().is_ok());

        let invalid = processing(1, vec!["worker-a", "worker-b"]);
        let error = invalid.processing().unwrap_err();

        assert!(error.to_string().contains("failed_workers"));
    }

    #[test]
    fn stale_execution_is_classified_by_version_before_terminal_state() {
        let stored = processing(0, Vec::new());
        let mut request = stored.row().unwrap();
        request.state = DONE;
        request.version = 2;
        request.leased_by.clear();
        request.lease_time = 0;
        let identity = types::Identity {
            id: request.id.clone(),
            task_id: request.task_id.clone(),
            trace_id: request.trace_id.clone(),
            version: 1,
            worker_id: "worker-previous".to_string(),
            node: request.node.clone(),
        };

        assert!(matches!(
            verify_ownership(&request, &identity),
            Err(Error::Version(id)) if id == request.id
        ));
    }

    #[test]
    fn unknown_state_is_rejected_as_corrupt_storage() {
        let stored = processing(0, Vec::new());
        let mut request = stored.row().unwrap();
        request.state = i8::MAX;

        assert!(matches!(validate_stored(&request), Err(Error::Invalid(_))));
    }

    fn processing(retry_count: i32, failed_workers: Vec<&str>) -> Stored {
        let mut request = spider::net::Request::follow("https://example.com")
            .unwrap()
            .with_id("request-1")
            .node("index");
        request.task_id = "task-1".to_string();
        request.trace_id = "trace-1".to_string();
        request.max_retry_count = 3;
        let snapshot = spider::net::request::Snapshot::try_from(request).unwrap();
        let snapshot_digest = operation::hex(&snapshot.digest().unwrap());

        Stored {
            id: snapshot.id.clone(),
            task_id: snapshot.task_id.clone(),
            trace_id: snapshot.trace_id.clone(),
            node: snapshot.node.clone(),
            mode: mode_name(&snapshot.mode).to_string(),
            state: PROCESSING,
            version: 1,
            priority: snapshot.priority,
            snapshot: serde_json::to_value(&snapshot).unwrap(),
            snapshot_digest,
            next_time: 0,
            leased_by: "worker-current".to_string(),
            lease_time: 1,
            retry_count,
            max_retry_count: snapshot.max_retry_count,
            failed_workers: serde_json::json!(failed_workers),
            ack_version: None,
        }
    }
}
