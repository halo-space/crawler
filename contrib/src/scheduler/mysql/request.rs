use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use spider::{net, payload, scheduler, trace};
use sqlx::mysql::MySqlRow;
use sqlx::{MySql as SqlMySql, Row as _, Transaction};

use super::MySql;
use super::decode;
use super::error::sqlx as sql_error;
use super::worker::database_time;

const HTTP: &str = "http";
const BROWSER: &str = "browser";
const MAX_RECOVERY: u64 = 128;

pub(super) struct Stored {
    snapshot: net::request::Snapshot,
    encoded: Value,
    hash: [u8; 32],
}

struct Claimed {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    priority: i32,
    snapshot: Value,
    snapshot_hash: Vec<u8>,
    version: i64,
    next_time: i64,
    retry_count: i32,
    max_retry_count: i32,
    leased_by: String,
    lease_time: i64,
    trace: Option<Value>,
    failed_workers: Vec<String>,
}

struct Expired {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    priority: i32,
    snapshot: Value,
    snapshot_hash: Vec<u8>,
    version: i64,
    retry_count: i32,
    leased_by: String,
    acknowledged: bool,
}

#[derive(Clone, Copy)]
struct Candidate {
    priority: i32,
    sequence: u64,
}

impl MySql {
    pub(super) fn prepare_requests(
        requests: Vec<net::Request>,
    ) -> Result<Vec<Stored>, scheduler::Error> {
        requests
            .into_iter()
            .map(|request| {
                let snapshot =
                    net::request::Snapshot::try_from(request).map_err(scheduler::Error::Message)?;
                let hash = snapshot
                    .hash()
                    .map_err(|error| scheduler::Error::Message(error.to_string()))?;
                let encoded = serde_json::to_value(&snapshot)
                    .map_err(|error| scheduler::Error::Message(error.to_string()))?;
                Ok(Stored {
                    snapshot,
                    encoded,
                    hash,
                })
            })
            .collect()
    }

    pub(super) async fn insert_initial_request(
        transaction: &mut Transaction<'_, SqlMySql>,
        stored: &Stored,
    ) -> Result<(), scheduler::Error> {
        let snapshot = &stored.snapshot;
        let result = sqlx::query(
            "INSERT INTO requests \
             (id, task_id, trace_id, node, mode, priority, snapshot, snapshot_hash, state, \
              version, next_time, leased_by, lease_time, retry_count, max_retry_count, \
              ack_version, created_time, updated_time) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, '', 0, ?, ?, NULL, \
                     CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
        )
        .bind(&snapshot.id)
        .bind(&snapshot.task_id)
        .bind(&snapshot.trace_id)
        .bind(&snapshot.node)
        .bind(mode(&snapshot.mode))
        .bind(snapshot.priority)
        .bind(&stored.encoded)
        .bind(stored.hash.as_slice())
        .bind(snapshot.version)
        .bind(snapshot.next_time)
        .bind(snapshot.retry_count)
        .bind(snapshot.max_retry_count)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if super::error::database_number(&error) == Some(1062) {
                return Err(scheduler::Error::Message(format!(
                    "initial Request id already exists: {}",
                    snapshot.id
                )));
            }
            return Err(sql_error(error));
        }
        enqueue(transaction, snapshot).await
    }

    pub(super) async fn enqueue(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let stored = Self::prepare_requests(payload.requests)?;
        if stored.is_empty() {
            return Ok(());
        }

        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        validate_trace_ownership(&mut transaction, &stored).await?;

        let mut locks = stored.iter().collect::<Vec<_>>();
        locks.sort_unstable_by(|left, right| left.snapshot.id.cmp(&right.snapshot.id));
        for request in locks {
            replay_or_insert_request(&mut transaction, request).await?;
        }
        for request in &stored {
            enqueue_initial_request(&mut transaction, request).await?;
        }
        transaction.commit().await.map_err(sql_error)
    }

    pub(super) async fn claim(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        validate_worker(worker_id, modes)?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let (online, now) = self.worker.online(&mut transaction).await?;
        if !online {
            transaction.commit().await.map_err(sql_error)?;
            return Ok(Vec::new());
        }
        recover_expired(
            &mut transaction,
            now,
            self.lease.timeout().as_millis() as i64,
        )
        .await?;
        if limit == 0 {
            transaction.commit().await.map_err(sql_error)?;
            return Ok(Vec::new());
        }

        let (http, browser) = supported_modes(modes);
        let mut requests = Vec::with_capacity(limit);
        let mut traces = HashMap::<String, Arc<trace::Snapshot>>::new();
        let mut trace_releases = Vec::new();
        // Advance after every inspected priority/FIFO position so an exact
        // Request lock can skip contention without revisiting the same prefix.
        let mut cursor = None;
        while requests.len() < limit {
            let candidates = scan_candidates(
                &mut transaction,
                now,
                worker_id,
                http,
                browser,
                cursor,
                limit - requests.len(),
            )
            .await?;
            if candidates.is_empty() {
                break;
            }

            for candidate in candidates {
                cursor = Some(candidate);
                let Some(row) = lock_candidate(
                    &mut transaction,
                    now,
                    worker_id,
                    http,
                    browser,
                    candidate.sequence,
                )
                .await?
                else {
                    continue;
                };
                let id = decode::string(&row, "id")?;
                let version = row.try_get::<i64, _>("version").map_err(sql_error)?;
                let Some(next_version) = version.checked_add(1) else {
                    fail_unclaimable(
                        &mut transaction,
                        &id,
                        version,
                        "request version overflow while claiming",
                    )
                    .await?;
                    continue;
                };

                let queue_mode = decode::string(&row, "queue_mode")?;
                let queue_priority = row.try_get::<i32, _>("queue_priority").map_err(sql_error)?;
                let queue_next_time = row
                    .try_get::<i64, _>("queue_next_time")
                    .map_err(sql_error)?;
                let stored_mode = decode::string(&row, "mode")?;
                let priority = row.try_get::<i32, _>("priority").map_err(sql_error)?;
                let next_time = row.try_get::<i64, _>("next_time").map_err(sql_error)?;
                if queue_mode != stored_mode
                    || queue_priority != priority
                    || queue_next_time != next_time
                {
                    fail_unclaimable(
                        &mut transaction,
                        &id,
                        version,
                        "stored Request queue does not match its fields",
                    )
                    .await?;
                    continue;
                }

                let result = sqlx::query(
                    "UPDATE requests SET state = 'processing', version = ?, leased_by = ?, \
                        lease_time = ?, ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
                 WHERE id = ? AND state = 'pending' AND version = ?",
                )
                .bind(next_version)
                .bind(worker_id)
                .bind(now)
                .bind(&id)
                .bind(version)
                .execute(&mut *transaction)
                .await
                .map_err(sql_error)?;
                if result.rows_affected() != 1 {
                    return Err(scheduler::Error::StateMismatch(id));
                }
                let deleted =
                    sqlx::query("DELETE FROM queues WHERE sequence = ? AND request_id = ?")
                        .bind(candidate.sequence)
                        .bind(&id)
                        .execute(&mut *transaction)
                        .await
                        .map_err(sql_error)?;
                if deleted.rows_affected() != 1 {
                    return Err(scheduler::Error::Message(format!(
                        "MySQL Request queue membership changed while claiming: {id}"
                    )));
                }

                let failed_workers = failed_workers(&mut transaction, &id).await?;
                let claimed = Claimed {
                    id: id.clone(),
                    task_id: decode::string(&row, "task_id")?,
                    trace_id: decode::string(&row, "trace_id")?,
                    node: decode::string(&row, "node")?,
                    mode: stored_mode,
                    priority,
                    snapshot: row.try_get("snapshot").map_err(sql_error)?,
                    snapshot_hash: row.try_get("snapshot_hash").map_err(sql_error)?,
                    version: next_version,
                    next_time,
                    retry_count: row.try_get("retry_count").map_err(sql_error)?,
                    max_retry_count: row.try_get("max_retry_count").map_err(sql_error)?,
                    leased_by: worker_id.to_string(),
                    lease_time: now,
                    trace: row.try_get("trace_snapshot").map_err(sql_error)?,
                    failed_workers,
                };
                match restore(&claimed, &mut traces) {
                    Ok(request) => requests.push(request),
                    Err(error) if is_trace_error(&error) => {
                        trace_releases.push((claimed, error));
                    }
                    Err(error) => {
                        let retry_limit = snapshot_retry_limit(&claimed);
                        recover_claim(&mut transaction, &claimed, error.to_string(), retry_limit)
                            .await?;
                    }
                }
            }
        }

        for (claimed, _) in &trace_releases {
            release_claim(&mut transaction, claimed).await?;
        }
        refresh_claimed_leases(&mut transaction, &mut requests, worker_id).await?;
        transaction.commit().await.map_err(sql_error)?;
        for (claimed, error) in trace_releases {
            tracing::warn!(
                request_id = %claimed.id,
                task_id = %claimed.task_id,
                trace_id = %claimed.trace_id,
                version = claimed.version,
                worker_id = %claimed.leased_by,
                error = %error,
                "MySQL Request Trace restoration failed; Request returned to queue"
            );
        }
        Ok(requests)
    }

    pub(super) async fn pending(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        validate_worker(worker_id, modes)?;
        let (http, browser) = supported_modes(modes);
        let pool = self.pool().await?;
        let pending = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS( \
                 SELECT 1 FROM requests r \
                 WHERE r.state = 'processing' \
                   AND ((? AND r.mode = 'http') OR (? AND r.mode = 'browser')) \
                 UNION ALL \
                 SELECT 1 FROM queues q \
                 INNER JOIN requests r ON r.id = q.request_id AND r.state = 'pending' \
                 WHERE ((? AND q.mode = 'http') OR (? AND q.mode = 'browser')) \
                   AND NOT EXISTS ( \
                       SELECT 1 FROM failed_workers f \
                       WHERE f.request_id = r.id AND f.worker_id = ? \
                   ) \
             )",
        )
        .bind(http)
        .bind(browser)
        .bind(http)
        .bind(browser)
        .bind(worker_id)
        .fetch_one(&pool)
        .await
        .map_err(sql_error)?;
        Ok(pending != 0)
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_candidates(
    transaction: &mut Transaction<'_, SqlMySql>,
    now: i64,
    worker_id: &str,
    http: bool,
    browser: bool,
    cursor: Option<Candidate>,
    limit: usize,
) -> Result<Vec<Candidate>, scheduler::Error> {
    let rows = if let Some(cursor) = cursor {
        sqlx::query(
            "SELECT q.priority, q.sequence \
             FROM queues q \
             INNER JOIN requests r ON r.id = q.request_id \
             WHERE r.state = 'pending' AND q.next_time <= ? \
               AND ((? AND q.mode = 'http') OR (? AND q.mode = 'browser')) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM failed_workers f \
                   WHERE f.request_id = r.id AND f.worker_id = ? \
               ) \
               AND (q.priority < ? OR (q.priority = ? AND q.sequence > ?)) \
             ORDER BY q.priority DESC, q.sequence ASC \
             LIMIT ?",
        )
        .bind(now)
        .bind(http)
        .bind(browser)
        .bind(worker_id)
        .bind(cursor.priority)
        .bind(cursor.priority)
        .bind(cursor.sequence)
        .bind(u64::try_from(limit).unwrap_or(u64::MAX))
        .fetch_all(&mut **transaction)
        .await
        .map_err(sql_error)?
    } else {
        sqlx::query(
            "SELECT q.priority, q.sequence \
             FROM queues q \
             INNER JOIN requests r ON r.id = q.request_id \
             WHERE r.state = 'pending' AND q.next_time <= ? \
               AND ((? AND q.mode = 'http') OR (? AND q.mode = 'browser')) \
               AND NOT EXISTS ( \
                   SELECT 1 FROM failed_workers f \
                   WHERE f.request_id = r.id AND f.worker_id = ? \
               ) \
             ORDER BY q.priority DESC, q.sequence ASC \
             LIMIT ?",
        )
        .bind(now)
        .bind(http)
        .bind(browser)
        .bind(worker_id)
        .bind(u64::try_from(limit).unwrap_or(u64::MAX))
        .fetch_all(&mut **transaction)
        .await
        .map_err(sql_error)?
    };
    rows.into_iter()
        .map(|row| {
            Ok(Candidate {
                priority: row.try_get("priority").map_err(sql_error)?,
                sequence: row.try_get("sequence").map_err(sql_error)?,
            })
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn lock_candidate(
    transaction: &mut Transaction<'_, SqlMySql>,
    now: i64,
    worker_id: &str,
    http: bool,
    browser: bool,
    sequence: u64,
) -> Result<Option<MySqlRow>, scheduler::Error> {
    // The Request row is authoritative. Locking only it keeps the ordered
    // queue scan non-blocking while replay and concurrent claims coordinate.
    sqlx::query(
        "SELECT q.mode AS queue_mode, q.priority AS queue_priority, \
                q.next_time AS queue_next_time, \
                r.id, r.task_id, r.trace_id, r.node, r.mode, r.priority, r.snapshot, \
                r.snapshot_hash, r.version, r.next_time, r.retry_count, r.max_retry_count, \
                t.snapshot AS trace_snapshot \
         FROM queues q \
         INNER JOIN requests r ON r.id = q.request_id \
         LEFT JOIN traces t ON t.id = r.trace_id \
         WHERE q.sequence = ? AND r.state = 'pending' AND q.next_time <= ? \
           AND ((? AND q.mode = 'http') OR (? AND q.mode = 'browser')) \
           AND NOT EXISTS ( \
               SELECT 1 FROM failed_workers f \
               WHERE f.request_id = r.id AND f.worker_id = ? \
           ) \
         FOR UPDATE OF r SKIP LOCKED",
    )
    .bind(sequence)
    .bind(now)
    .bind(http)
    .bind(browser)
    .bind(worker_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(sql_error)
}

async fn refresh_claimed_leases(
    transaction: &mut Transaction<'_, SqlMySql>,
    requests: &mut [net::Request],
    worker_id: &str,
) -> Result<(), scheduler::Error> {
    if requests.is_empty() {
        return Ok(());
    }
    for request in requests {
        let lease_time = database_time(transaction).await?;
        let result = sqlx::query(
            "UPDATE requests SET lease_time = ?, updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
        )
        .bind(lease_time)
        .bind(&request.id)
        .bind(request.version)
        .bind(worker_id)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
        if result.rows_affected() != 1 {
            return Err(scheduler::Error::StateMismatch(request.id.clone()));
        }
        request.lease_time = lease_time;
    }
    Ok(())
}

fn mode(value: &net::Mode) -> &'static str {
    match value {
        net::Mode::Http => HTTP,
        net::Mode::Browser => BROWSER,
    }
}

fn parse_mode(value: &str, id: &str) -> Result<net::Mode, scheduler::Error> {
    match value {
        HTTP => Ok(net::Mode::Http),
        BROWSER => Ok(net::Mode::Browser),
        value => Err(scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: format!("stored Request has invalid mode: {value}"),
        }),
    }
}

fn supported_modes(modes: &[net::Mode]) -> (bool, bool) {
    (
        modes.contains(&net::Mode::Http),
        modes.contains(&net::Mode::Browser),
    )
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

async fn validate_trace_ownership(
    transaction: &mut Transaction<'_, SqlMySql>,
    requests: &[Stored],
) -> Result<(), scheduler::Error> {
    let mut traces = HashMap::<&str, String>::new();
    for request in requests {
        let snapshot = &request.snapshot;
        let owner = if let Some(owner) = traces.get(snapshot.trace_id.as_str()) {
            owner
        } else {
            let row = sqlx::query("SELECT task_id FROM traces WHERE id = ? FOR SHARE")
                .bind(&snapshot.trace_id)
                .fetch_optional(&mut **transaction)
                .await
                .map_err(sql_error)?
                .ok_or_else(|| scheduler::Error::TraceNotFound(snapshot.trace_id.clone()))?;
            let owner = decode::string(&row, "task_id")?;
            traces.insert(snapshot.trace_id.as_str(), owner);
            traces
                .get(snapshot.trace_id.as_str())
                .expect("inserted Trace owner must exist")
        };
        if owner != &snapshot.task_id {
            return Err(scheduler::Error::IdentityMismatch {
                id: snapshot.id.clone(),
                field: "task_id",
            });
        }
    }
    Ok(())
}

async fn replay_or_insert_request(
    transaction: &mut Transaction<'_, SqlMySql>,
    stored: &Stored,
) -> Result<(), scheduler::Error> {
    let snapshot = &stored.snapshot;
    sqlx::query(
        "INSERT INTO requests \
         (id, task_id, trace_id, node, mode, priority, snapshot, snapshot_hash, state, \
          version, next_time, leased_by, lease_time, retry_count, max_retry_count, \
          ack_version, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, '', 0, 0, ?, NULL, \
                 CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)) \
         ON DUPLICATE KEY UPDATE id = requests.id",
    )
    .bind(&snapshot.id)
    .bind(&snapshot.task_id)
    .bind(&snapshot.trace_id)
    .bind(&snapshot.node)
    .bind(mode(&snapshot.mode))
    .bind(snapshot.priority)
    .bind(&stored.encoded)
    .bind(stored.hash.as_slice())
    .bind(snapshot.next_time)
    .bind(snapshot.max_retry_count)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;

    let current = sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT snapshot_hash FROM requests WHERE id = ? FOR UPDATE",
    )
    .bind(&snapshot.id)
    .fetch_one(&mut **transaction)
    .await
    .map_err(sql_error)?;
    if current.as_slice() != stored.hash {
        return Err(scheduler::Error::Message(format!(
            "Request id conflicts with existing Snapshot: {}",
            snapshot.id
        )));
    }

    Ok(())
}

async fn enqueue_initial_request(
    transaction: &mut Transaction<'_, SqlMySql>,
    stored: &Stored,
) -> Result<(), scheduler::Error> {
    // Only a newly inserted immutable generation still has the initial mutable
    // fields. Replays of processing, retried, or terminal Requests stay no-op.
    sqlx::query(
        "INSERT INTO queues \
         (request_id, mode, priority, next_time, created_time, updated_time) \
         SELECT id, mode, priority, next_time, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3) \
         FROM requests \
         WHERE id = ? AND state = 'pending' AND version = 0 AND retry_count = 0 \
           AND leased_by = '' AND lease_time = 0 \
         ON DUPLICATE KEY UPDATE request_id = queues.request_id",
    )
    .bind(&stored.snapshot.id)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

async fn enqueue(
    transaction: &mut Transaction<'_, SqlMySql>,
    snapshot: &net::request::Snapshot,
) -> Result<(), scheduler::Error> {
    sqlx::query(
        "INSERT INTO queues \
         (request_id, mode, priority, next_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
    )
    .bind(&snapshot.id)
    .bind(mode(&snapshot.mode))
    .bind(snapshot.priority)
    .bind(snapshot.next_time)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

async fn recover_expired(
    transaction: &mut Transaction<'_, SqlMySql>,
    now: i64,
    lease_timeout: i64,
) -> Result<(), scheduler::Error> {
    let expired_before = now.saturating_sub(lease_timeout);
    let rows = sqlx::query(
        "SELECT id, task_id, trace_id, node, mode, priority, snapshot, snapshot_hash, \
                version, retry_count, leased_by, ack_version \
         FROM requests \
         WHERE state = 'processing' AND lease_time <= ? \
         ORDER BY lease_time ASC, id ASC \
         LIMIT ? FOR UPDATE SKIP LOCKED",
    )
    .bind(expired_before)
    .bind(MAX_RECOVERY)
    .fetch_all(&mut **transaction)
    .await
    .map_err(sql_error)?;

    for row in rows {
        let version = row.try_get::<i64, _>("version").map_err(sql_error)?;
        let expired = Expired {
            id: decode::string(&row, "id")?,
            task_id: decode::string(&row, "task_id")?,
            trace_id: decode::string(&row, "trace_id")?,
            node: decode::string(&row, "node")?,
            mode: decode::string(&row, "mode")?,
            priority: row.try_get("priority").map_err(sql_error)?,
            snapshot: row.try_get("snapshot").map_err(sql_error)?,
            snapshot_hash: row.try_get("snapshot_hash").map_err(sql_error)?,
            version,
            retry_count: row.try_get("retry_count").map_err(sql_error)?,
            leased_by: decode::string(&row, "leased_by")?,
            acknowledged: row
                .try_get::<Option<i64>, _>("ack_version")
                .map_err(sql_error)?
                == Some(version),
        };
        recover_expired_request(transaction, &expired).await?;
    }
    Ok(())
}

async fn recover_expired_request(
    transaction: &mut Transaction<'_, SqlMySql>,
    request: &Expired,
) -> Result<(), scheduler::Error> {
    sqlx::query("DELETE FROM queues WHERE request_id = ?")
        .bind(&request.id)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;

    if !request.acknowledged {
        sqlx::query(
            "UPDATE requests SET state = 'pending', next_time = 0, leased_by = '', \
                    lease_time = 0, ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ?",
        )
        .bind(&request.id)
        .bind(request.version)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
        enqueue_values(transaction, &request.id, &request.mode, request.priority, 0).await?;
        return Ok(());
    }

    let Some(max_retry_count) = trusted_retry_limit(
        &request.snapshot,
        &request.snapshot_hash,
        &request.id,
        &request.task_id,
        &request.trace_id,
        &request.node,
        &request.mode,
        request.priority,
    ) else {
        return quarantine_expired(
            transaction,
            request,
            "acknowledged lease expired but its immutable Request Snapshot is invalid",
        )
        .await;
    };

    let retry =
        request
            .retry_count
            .checked_add(1)
            .ok_or_else(|| scheduler::Error::InvalidRequest {
                id: request.id.clone(),
                message: "request retry overflow while recovering expired lease".to_string(),
            })?;
    record_failed_worker(transaction, &request.id, &request.leased_by).await?;
    record_failure(
        transaction,
        &request.id,
        request.version,
        &request.task_id,
        &request.trace_id,
        &request.node,
        &request.leased_by,
        "acknowledged lease expired",
    )
    .await?;

    if retry < max_retry_count {
        sqlx::query(
            "UPDATE requests SET state = 'pending', retry_count = ?, max_retry_count = ?, \
                    next_time = 0, \
                    leased_by = '', lease_time = 0, ack_version = NULL, \
                    updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ?",
        )
        .bind(retry)
        .bind(max_retry_count)
        .bind(&request.id)
        .bind(request.version)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
        enqueue_values(transaction, &request.id, &request.mode, request.priority, 0).await?;
    } else {
        sqlx::query(
            "UPDATE requests SET state = 'failed', retry_count = ?, max_retry_count = ?, \
                    next_time = 0, \
                    leased_by = '', lease_time = 0, ack_version = NULL, \
                    updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ?",
        )
        .bind(retry)
        .bind(max_retry_count)
        .bind(&request.id)
        .bind(request.version)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
    }
    Ok(())
}

async fn failed_workers(
    transaction: &mut Transaction<'_, SqlMySql>,
    request_id: &str,
) -> Result<Vec<String>, scheduler::Error> {
    let rows = sqlx::query(
        "SELECT worker_id FROM failed_workers \
         WHERE request_id = ? ORDER BY position ASC",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(sql_error)?;
    rows.into_iter()
        .map(|row| decode::string(&row, "worker_id"))
        .collect()
}

fn restore(
    claimed: &Claimed,
    traces: &mut HashMap<String, Arc<trace::Snapshot>>,
) -> Result<net::Request, scheduler::Error> {
    let mut snapshot = serde_json::from_value::<net::request::Snapshot>(claimed.snapshot.clone())
        .map_err(|error| scheduler::Error::InvalidRequest {
        id: claimed.id.clone(),
        message: error.to_string(),
    })?;
    let actual = snapshot
        .hash()
        .map_err(|error| scheduler::Error::InvalidRequest {
            id: claimed.id.clone(),
            message: error.to_string(),
        })?;
    if claimed.snapshot_hash.as_slice() != actual {
        return Err(scheduler::Error::InvalidRequest {
            id: claimed.id.clone(),
            message: "stored Request Snapshot hash does not match".to_string(),
        });
    }
    let stored_mode = parse_mode(&claimed.mode, &claimed.id)?;
    for (field, matches) in [
        ("id", snapshot.id == claimed.id),
        ("task_id", snapshot.task_id == claimed.task_id),
        ("trace_id", snapshot.trace_id == claimed.trace_id),
        ("node", snapshot.node == claimed.node),
        ("mode", snapshot.mode == stored_mode),
        ("priority", snapshot.priority == claimed.priority),
        (
            "max_retry_count",
            snapshot.max_retry_count == claimed.max_retry_count,
        ),
    ] {
        if !matches {
            return Err(scheduler::Error::InvalidRequest {
                id: claimed.id.clone(),
                message: format!("stored Request {field} does not match its Snapshot"),
            });
        }
    }

    snapshot.version = claimed.version;
    snapshot.next_time = claimed.next_time;
    snapshot.retry_count = claimed.retry_count;
    snapshot.max_retry_count = claimed.max_retry_count;
    snapshot.failed_workers = claimed.failed_workers.clone();
    snapshot.state = net::State::Pending;
    snapshot.leased_by.clear();
    snapshot.lease_time = 0;

    let trace = if let Some(trace) = traces.get(&claimed.trace_id) {
        trace.clone()
    } else {
        let encoded = claimed
            .trace
            .clone()
            .ok_or_else(|| scheduler::Error::TraceNotFound(claimed.trace_id.clone()))?;
        let trace = serde_json::from_value::<trace::Snapshot>(encoded).map_err(|error| {
            scheduler::Error::InvalidTrace {
                id: claimed.trace_id.clone(),
                message: error.to_string(),
            }
        })?;
        trace
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: claimed.trace_id.clone(),
                message,
            })?;
        let trace = Arc::new(trace);
        traces.insert(claimed.trace_id.clone(), trace.clone());
        trace
    };
    validate_trace_binding(&snapshot, trace.as_ref(), &claimed.trace_id)?;
    let mut request =
        snapshot
            .restore(Some(trace))
            .map_err(|message| scheduler::Error::InvalidRequest {
                id: claimed.id.clone(),
                message,
            })?;
    request.state = net::State::Processing;
    request.version = claimed.version;
    request.leased_by = claimed.leased_by.clone();
    request.lease_time = claimed.lease_time;
    request.mode = stored_mode;
    request.retry_count = claimed.retry_count;
    request.max_retry_count = claimed.max_retry_count;
    request.failed_workers = claimed.failed_workers.clone();
    Ok(request)
}

fn is_trace_error(error: &scheduler::Error) -> bool {
    matches!(
        error,
        scheduler::Error::TraceNotFound(_) | scheduler::Error::InvalidTrace { .. }
    )
}

fn validate_trace_binding(
    snapshot: &net::request::Snapshot,
    trace: &trace::Snapshot,
    trace_id: &str,
) -> Result<(), scheduler::Error> {
    if trace.task_id != snapshot.task_id {
        return Err(scheduler::Error::InvalidTrace {
            id: trace_id.to_string(),
            message: "Request Snapshot task_id does not match Trace Snapshot".to_string(),
        });
    }
    if let Some(config) = trace.dsl.as_ref()
        && !config.graph.nodes.contains_key(&snapshot.node)
    {
        return Err(scheduler::Error::InvalidTrace {
            id: trace_id.to_string(),
            message: format!(
                "Request Snapshot node does not exist in Trace Snapshot: {}",
                snapshot.node
            ),
        });
    }
    Ok(())
}

async fn release_claim(
    transaction: &mut Transaction<'_, SqlMySql>,
    claimed: &Claimed,
) -> Result<(), scheduler::Error> {
    let released = sqlx::query(
        "UPDATE requests SET state = 'pending', next_time = 0, leased_by = '', lease_time = 0, \
                ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
         WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
    )
    .bind(&claimed.id)
    .bind(claimed.version)
    .bind(&claimed.leased_by)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    if released.rows_affected() != 1 {
        return Err(scheduler::Error::StateMismatch(claimed.id.clone()));
    }
    enqueue_values(transaction, &claimed.id, &claimed.mode, claimed.priority, 0).await
}

async fn recover_claim(
    transaction: &mut Transaction<'_, SqlMySql>,
    claimed: &Claimed,
    reason: String,
    max_retry_count: Option<i32>,
) -> Result<(), scheduler::Error> {
    let Some(max_retry_count) = max_retry_count else {
        return quarantine_claim(transaction, claimed, &reason).await;
    };
    let retry =
        claimed
            .retry_count
            .checked_add(1)
            .ok_or_else(|| scheduler::Error::InvalidRequest {
                id: claimed.id.clone(),
                message: format!("{reason}; request retry overflow"),
            })?;
    record_failed_worker(transaction, &claimed.id, &claimed.leased_by).await?;
    record_failure(
        transaction,
        &claimed.id,
        claimed.version,
        &claimed.task_id,
        &claimed.trace_id,
        &claimed.node,
        &claimed.leased_by,
        &reason,
    )
    .await?;

    if retry < max_retry_count {
        sqlx::query(
            "UPDATE requests SET state = 'pending', retry_count = ?, max_retry_count = ?, \
                    next_time = 0, \
                    leased_by = '', lease_time = 0, ack_version = NULL, \
                    updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
        )
        .bind(retry)
        .bind(max_retry_count)
        .bind(&claimed.id)
        .bind(claimed.version)
        .bind(&claimed.leased_by)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
        enqueue_values(transaction, &claimed.id, &claimed.mode, claimed.priority, 0).await?;
    } else {
        sqlx::query(
            "UPDATE requests SET state = 'failed', retry_count = ?, max_retry_count = ?, \
                    next_time = 0, \
                    leased_by = '', lease_time = 0, ack_version = NULL, \
                    updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
        )
        .bind(retry)
        .bind(max_retry_count)
        .bind(&claimed.id)
        .bind(claimed.version)
        .bind(&claimed.leased_by)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
    }
    Ok(())
}

fn snapshot_retry_limit(claimed: &Claimed) -> Option<i32> {
    trusted_retry_limit(
        &claimed.snapshot,
        &claimed.snapshot_hash,
        &claimed.id,
        &claimed.task_id,
        &claimed.trace_id,
        &claimed.node,
        &claimed.mode,
        claimed.priority,
    )
}

#[allow(clippy::too_many_arguments)]
fn trusted_retry_limit(
    encoded: &Value,
    expected_hash: &[u8],
    id: &str,
    task_id: &str,
    trace_id: &str,
    node: &str,
    stored_mode: &str,
    priority: i32,
) -> Option<i32> {
    let snapshot = serde_json::from_value::<net::request::Snapshot>(encoded.clone()).ok()?;
    let actual = snapshot.hash().ok()?;
    let matches = expected_hash == actual
        && snapshot.id == id
        && snapshot.task_id == task_id
        && snapshot.trace_id == trace_id
        && snapshot.node == node
        && mode(&snapshot.mode) == stored_mode
        && snapshot.priority == priority
        && (1..=net::request::MAX_RETRY_COUNT).contains(&snapshot.max_retry_count);
    matches.then_some(snapshot.max_retry_count)
}

async fn quarantine_claim(
    transaction: &mut Transaction<'_, SqlMySql>,
    claimed: &Claimed,
    reason: &str,
) -> Result<(), scheduler::Error> {
    record_failure(
        transaction,
        &claimed.id,
        claimed.version,
        &claimed.task_id,
        &claimed.trace_id,
        &claimed.node,
        &claimed.leased_by,
        reason,
    )
    .await?;
    let updated = sqlx::query(
        "UPDATE requests SET state = 'failed', next_time = 0, leased_by = '', lease_time = 0, \
                ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
         WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
    )
    .bind(&claimed.id)
    .bind(claimed.version)
    .bind(&claimed.leased_by)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    if updated.rows_affected() != 1 {
        return Err(scheduler::Error::StateMismatch(claimed.id.clone()));
    }
    Ok(())
}

async fn quarantine_expired(
    transaction: &mut Transaction<'_, SqlMySql>,
    request: &Expired,
    reason: &str,
) -> Result<(), scheduler::Error> {
    record_failure(
        transaction,
        &request.id,
        request.version,
        &request.task_id,
        &request.trace_id,
        &request.node,
        &request.leased_by,
        reason,
    )
    .await?;
    let updated = sqlx::query(
        "UPDATE requests SET state = 'failed', next_time = 0, leased_by = '', lease_time = 0, \
                ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
         WHERE id = ? AND state = 'processing' AND version = ? AND leased_by = ?",
    )
    .bind(&request.id)
    .bind(request.version)
    .bind(&request.leased_by)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    if updated.rows_affected() != 1 {
        return Err(scheduler::Error::StateMismatch(request.id.clone()));
    }
    Ok(())
}

async fn fail_unclaimable(
    transaction: &mut Transaction<'_, SqlMySql>,
    id: &str,
    version: i64,
    reason: &str,
) -> Result<(), scheduler::Error> {
    let row = sqlx::query("SELECT task_id, trace_id, node FROM requests WHERE id = ? FOR UPDATE")
        .bind(id)
        .fetch_optional(&mut **transaction)
        .await
        .map_err(sql_error)?
        .ok_or_else(|| scheduler::Error::RequestNotFound(id.to_string()))?;
    sqlx::query("DELETE FROM queues WHERE request_id = ?")
        .bind(id)
        .execute(&mut **transaction)
        .await
        .map_err(sql_error)?;
    record_failure(
        transaction,
        id,
        version,
        &decode::string(&row, "task_id")?,
        &decode::string(&row, "trace_id")?,
        &decode::string(&row, "node")?,
        "",
        reason,
    )
    .await?;
    sqlx::query(
        "UPDATE requests SET state = 'failed', leased_by = '', lease_time = 0, \
                ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) WHERE id = ?",
    )
    .bind(id)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

async fn record_failed_worker(
    transaction: &mut Transaction<'_, SqlMySql>,
    request_id: &str,
    worker_id: &str,
) -> Result<(), scheduler::Error> {
    if worker_id.is_empty() {
        return Ok(());
    }
    let positions = sqlx::query(
        "SELECT worker_id, position FROM failed_workers \
         WHERE request_id = ? ORDER BY position FOR UPDATE",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(sql_error)?;
    for row in &positions {
        if decode::string(row, "worker_id")? == worker_id {
            return Ok(());
        }
    }
    let position = positions
        .last()
        .map(|row| row.get::<i32, _>("position"))
        .unwrap_or_default()
        .checked_add(1)
        .ok_or_else(|| scheduler::Error::Message("failed Worker position overflow".to_string()))?;
    sqlx::query(
        "INSERT INTO failed_workers \
         (request_id, worker_id, position, created_time, updated_time) \
         VALUES (?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
    )
    .bind(request_id)
    .bind(worker_id)
    .bind(position)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn record_failure(
    transaction: &mut Transaction<'_, SqlMySql>,
    id: &str,
    version: i64,
    task_id: &str,
    trace_id: &str,
    node: &str,
    worker_id: &str,
    reason: &str,
) -> Result<(), scheduler::Error> {
    sqlx::query(
        "INSERT INTO completions \
         (request_id, version, task_id, trace_id, node, worker_id, state, error, \
          start_time, end_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, ?, 'failed', ?, NULL, NULL, \
                 CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)) \
         ON DUPLICATE KEY UPDATE request_id = completions.request_id",
    )
    .bind(id)
    .bind(version)
    .bind(task_id)
    .bind(trace_id)
    .bind(node)
    .bind(worker_id)
    .bind(reason)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

async fn enqueue_values(
    transaction: &mut Transaction<'_, SqlMySql>,
    id: &str,
    mode: &str,
    priority: i32,
    next_time: i64,
) -> Result<(), scheduler::Error> {
    sqlx::query(
        "INSERT INTO queues \
         (request_id, mode, priority, next_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
    )
    .bind(id)
    .bind(mode)
    .bind(priority)
    .bind(next_time)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}
