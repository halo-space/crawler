use serde_json::Value;
use spider::{net, payload, scheduler, stats};
use sqlx::{MySql, Row as _, Transaction};

use super::MySql as Scheduler;
use super::decode;
use super::error::{database_number, sqlx as sql_error};
use super::worker::database_time;

struct Request {
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    priority: i32,
    state: String,
    version: i64,
    leased_by: String,
    lease_time: i64,
    retry_count: i32,
    ack_version: Option<i64>,
    snapshot: Value,
    snapshot_hash: Vec<u8>,
}

impl Scheduler {
    pub(super) async fn acknowledge(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_ack()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let request = lock_request(&mut transaction, &payload.id).await?;
        let now = database_time(&mut transaction).await?;
        validate_execution(&request, payload, worker_id, now, self.lease, false)?;
        sqlx::query(
            "UPDATE requests SET ack_version = version, updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ?",
        )
        .bind(&payload.id)
        .execute(&mut *transaction)
        .await
        .map_err(sql_error)?;
        transaction.commit().await.map_err(sql_error)
    }

    pub(super) async fn return_to_queue(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_release()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let request = lock_request(&mut transaction, &payload.id).await?;
        let now = database_time(&mut transaction).await?;
        validate_execution(&request, payload, worker_id, now, self.lease, false)?;

        sqlx::query(
            "INSERT INTO queues \
             (request_id, mode, priority, next_time, created_time, updated_time) \
             VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
        )
        .bind(&payload.id)
        .bind(&request.mode)
        .bind(request.priority)
        .execute(&mut *transaction)
        .await
        .map_err(sql_error)?;
        clear_execution(&mut transaction, &payload.id, "pending").await?;
        transaction.commit().await.map_err(sql_error)
    }

    pub(super) async fn refresh(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_refresh_lease()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let request = lock_request(&mut transaction, &payload.id).await?;
        let now = database_time(&mut transaction).await?;
        validate_execution(&request, payload, worker_id, now, self.lease, true)?;
        sqlx::query(
            "UPDATE requests SET lease_time = ?, updated_time = CURRENT_TIMESTAMP(3) \
             WHERE id = ?",
        )
        .bind(now)
        .bind(&payload.id)
        .execute(&mut *transaction)
        .await
        .map_err(sql_error)?;
        transaction.commit().await.map_err(sql_error)
    }

    pub(super) async fn succeed(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_success()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let counters = counters(payload)?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let request = lock_request(&mut transaction, &payload.id).await?;
        if completion_matches(&mut transaction, payload, worker_id).await? {
            transaction.commit().await.map_err(sql_error)?;
            return Ok(());
        }
        let now = database_time(&mut transaction).await?;
        validate_execution(&request, payload, worker_id, now, self.lease, true)?;

        merge_stats(&mut transaction, &payload.trace_id, counters).await?;
        insert_completion(&mut transaction, payload, &request.leased_by).await?;
        clear_execution(&mut transaction, &payload.id, "done").await?;
        transaction.commit().await.map_err(sql_error)
    }

    pub(super) async fn fail(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_failure()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let counters = counters(payload)?;
        let pool = self.pool().await?;
        let mut transaction = pool.begin().await.map_err(sql_error)?;
        let request = lock_request(&mut transaction, &payload.id).await?;
        if completion_matches(&mut transaction, payload, worker_id).await? {
            transaction.commit().await.map_err(sql_error)?;
            return Ok(());
        }
        let now = database_time(&mut transaction).await?;
        validate_execution(&request, payload, worker_id, now, self.lease, true)?;
        let retry_limit = trusted_retry_limit(&request, &payload.id)?;
        let retry_count = request.retry_count.checked_add(1).ok_or_else(|| {
            scheduler::Error::Message(format!("request retry overflow: {}", payload.id))
        })?;
        let failed_workers = failed_workers(&mut transaction, &payload.id).await?;
        if failed_workers.len() > request.retry_count as usize {
            return Err(scheduler::Error::InvalidRequest {
                id: payload.id.clone(),
                message: "stored failed Workers exceed retry_count".to_string(),
            });
        }
        if !failed_workers
            .iter()
            .any(|worker| worker == &request.leased_by)
        {
            let position = i32::try_from(failed_workers.len() + 1).map_err(|_| {
                scheduler::Error::Message("failed Worker position overflow".to_string())
            })?;
            sqlx::query(
                "INSERT INTO failed_workers \
                 (request_id, worker_id, position, created_time, updated_time) \
                 VALUES (?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
            )
            .bind(&payload.id)
            .bind(&request.leased_by)
            .bind(position)
            .execute(&mut *transaction)
            .await
            .map_err(sql_error)?;
        }

        merge_stats(&mut transaction, &payload.trace_id, counters).await?;
        insert_completion(&mut transaction, payload, &request.leased_by).await?;
        if retry_count < retry_limit {
            sqlx::query(
                "INSERT INTO queues \
                 (request_id, mode, priority, next_time, created_time, updated_time) \
                 VALUES (?, ?, ?, 0, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
            )
            .bind(&payload.id)
            .bind(&request.mode)
            .bind(request.priority)
            .execute(&mut *transaction)
            .await
            .map_err(sql_error)?;
            sqlx::query(
                "UPDATE requests SET state = 'pending', retry_count = ?, max_retry_count = ?, next_time = 0, \
                        leased_by = '', lease_time = 0, ack_version = NULL, \
                        updated_time = CURRENT_TIMESTAMP(3) \
                 WHERE id = ?",
            )
            .bind(retry_count)
            .bind(retry_limit)
            .bind(&payload.id)
            .execute(&mut *transaction)
            .await
            .map_err(sql_error)?;
        } else {
            sqlx::query(
                "UPDATE requests SET state = 'failed', retry_count = ?, max_retry_count = ?, next_time = 0, \
                        leased_by = '', lease_time = 0, ack_version = NULL, \
                        updated_time = CURRENT_TIMESTAMP(3) \
                 WHERE id = ?",
            )
            .bind(retry_count)
            .bind(retry_limit)
            .bind(&payload.id)
            .execute(&mut *transaction)
            .await
            .map_err(sql_error)?;
        }
        transaction.commit().await.map_err(sql_error)
    }
}

async fn lock_request(
    transaction: &mut Transaction<'_, MySql>,
    id: &str,
) -> Result<Request, scheduler::Error> {
    let row = sqlx::query(
        "SELECT task_id, trace_id, node, mode, priority, state, version, leased_by, \
                lease_time, retry_count, ack_version, snapshot, snapshot_hash \
         FROM requests WHERE id = ? FOR UPDATE",
    )
    .bind(id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(sql_error)?
    .ok_or_else(|| scheduler::Error::RequestNotFound(id.to_string()))?;
    Ok(Request {
        task_id: decode::string(&row, "task_id")?,
        trace_id: decode::string(&row, "trace_id")?,
        node: decode::string(&row, "node")?,
        mode: decode::string(&row, "mode")?,
        priority: row.try_get("priority").map_err(sql_error)?,
        state: decode::string(&row, "state")?,
        version: row.try_get("version").map_err(sql_error)?,
        leased_by: decode::string(&row, "leased_by")?,
        lease_time: row.try_get("lease_time").map_err(sql_error)?,
        retry_count: row.try_get("retry_count").map_err(sql_error)?,
        ack_version: row.try_get("ack_version").map_err(sql_error)?,
        snapshot: row.try_get("snapshot").map_err(sql_error)?,
        snapshot_hash: row.try_get("snapshot_hash").map_err(sql_error)?,
    })
}

fn trusted_retry_limit(request: &Request, id: &str) -> Result<i32, scheduler::Error> {
    let snapshot = serde_json::from_value::<net::request::Snapshot>(request.snapshot.clone())
        .map_err(|error| scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: format!("stored Request Snapshot cannot be decoded: {error}"),
        })?;
    let actual = snapshot
        .hash()
        .map_err(|error| scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: format!("stored Request Snapshot hash cannot be computed: {error}"),
        })?;
    if request.snapshot_hash.as_slice() != actual
        || snapshot.task_id != request.task_id
        || snapshot.trace_id != request.trace_id
        || snapshot.node != request.node
        || snapshot_mode(&snapshot.mode) != request.mode
        || snapshot.priority != request.priority
        || !(1..=net::request::MAX_RETRY_COUNT).contains(&snapshot.max_retry_count)
    {
        return Err(scheduler::Error::InvalidRequest {
            id: id.to_string(),
            message: "stored Request Snapshot hash or identity does not match its row".to_string(),
        });
    }
    Ok(snapshot.max_retry_count)
}

fn snapshot_mode(mode: &net::Mode) -> &'static str {
    match mode {
        net::Mode::Http => "http",
        net::Mode::Browser => "browser",
    }
}

fn validate_execution(
    request: &Request,
    payload: &payload::Payload,
    worker_id: &str,
    now: i64,
    lease: scheduler::Lease,
    require_ack: bool,
) -> Result<(), scheduler::Error> {
    for (field, matches) in [
        ("task_id", request.task_id == payload.task_id),
        ("trace_id", request.trace_id == payload.trace_id),
        ("node", request.node == payload.node),
    ] {
        if !matches {
            return Err(scheduler::Error::IdentityMismatch {
                id: payload.id.clone(),
                field,
            });
        }
    }
    if request.version != payload.version {
        return Err(scheduler::Error::VersionMismatch(payload.id.clone()));
    }
    if request.state != "processing" {
        return Err(scheduler::Error::StateMismatch(payload.id.clone()));
    }
    if request.leased_by != worker_id || request.lease_time <= 0 {
        return Err(scheduler::Error::LeaseMismatch(payload.id.clone()));
    }
    if now.saturating_sub(request.lease_time) >= lease.timeout().as_millis() as i64 {
        return Err(scheduler::Error::LeaseExpired(payload.id.clone()));
    }
    if require_ack && request.ack_version != Some(payload.version) {
        return Err(scheduler::Error::NotAcknowledged(payload.id.clone()));
    }
    Ok(())
}

async fn completion_matches(
    transaction: &mut Transaction<'_, MySql>,
    payload: &payload::Payload,
    worker_id: &str,
) -> Result<bool, scheduler::Error> {
    let row = sqlx::query(
        "SELECT task_id, trace_id, node, worker_id, state \
         FROM completions WHERE request_id = ? AND version = ? FOR UPDATE",
    )
    .bind(&payload.id)
    .bind(payload.version)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(sql_error)?;
    let Some(row) = row else {
        return Ok(false);
    };
    for (field, matches) in [
        (
            "task_id",
            decode::string(&row, "task_id")? == payload.task_id,
        ),
        (
            "trace_id",
            decode::string(&row, "trace_id")? == payload.trace_id,
        ),
        ("node", decode::string(&row, "node")? == payload.node),
    ] {
        if !matches {
            return Err(scheduler::Error::IdentityMismatch {
                id: payload.id.clone(),
                field,
            });
        }
    }
    if decode::string(&row, "state")? != state(payload.state) {
        return Err(scheduler::Error::StateMismatch(payload.id.clone()));
    }
    let completed_by = decode::string(&row, "worker_id")?;
    if completed_by.is_empty() {
        return Err(scheduler::Error::Message(
            "completion is missing its observed execution Worker".to_string(),
        ));
    }
    if completed_by != worker_id {
        return Err(scheduler::Error::LeaseMismatch(payload.id.clone()));
    }
    Ok(true)
}

async fn clear_execution(
    transaction: &mut Transaction<'_, MySql>,
    id: &str,
    state: &str,
) -> Result<(), scheduler::Error> {
    sqlx::query(
        "UPDATE requests SET state = ?, next_time = 0, leased_by = '', lease_time = 0, \
                ack_version = NULL, updated_time = CURRENT_TIMESTAMP(3) \
         WHERE id = ?",
    )
    .bind(state)
    .bind(id)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

fn counters(payload: &payload::Payload) -> Result<Vec<(String, stats::Counter)>, scheduler::Error> {
    let mut counters = payload
        .stats
        .iter()
        .map(|(name, value)| {
            serde_json::from_value::<stats::Counter>(value.clone())
                .map_err(|error| {
                    scheduler::Error::Message(format!("invalid stats counter {name}: {error}"))
                })
                .and_then(|counter| {
                    if counter.total >= 0
                        && counter.done >= 0
                        && counter.filter >= 0
                        && counter.dedup >= 0
                        && counter.validate >= 0
                        && counter.download >= 0
                    {
                        Ok((name.clone(), counter))
                    } else {
                        Err(scheduler::Error::Message(format!(
                            "invalid stats counter {name}: values must be non-negative"
                        )))
                    }
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    counters.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    Ok(counters)
}

async fn merge_stats(
    transaction: &mut Transaction<'_, MySql>,
    trace_id: &str,
    counters: Vec<(String, stats::Counter)>,
) -> Result<(), scheduler::Error> {
    for (name, counter) in counters {
        let result = sqlx::query(
            "INSERT INTO trace_stats \
             (trace_id, name, total, done, `filter`, dedup, `validate`, download, \
              created_time, updated_time) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3)) \
             AS incoming \
             ON DUPLICATE KEY UPDATE \
                 total = trace_stats.total + incoming.total, \
                 done = trace_stats.done + incoming.done, \
                 `filter` = trace_stats.`filter` + incoming.`filter`, \
                 dedup = trace_stats.dedup + incoming.dedup, \
                 `validate` = trace_stats.`validate` + incoming.`validate`, \
                 download = trace_stats.download + incoming.download, \
                 updated_time = CURRENT_TIMESTAMP(3)",
        )
        .bind(trace_id)
        .bind(&name)
        .bind(counter.total)
        .bind(counter.done)
        .bind(counter.filter)
        .bind(counter.dedup)
        .bind(counter.validate)
        .bind(counter.download)
        .execute(&mut **transaction)
        .await;
        if let Err(error) = result {
            if database_number(&error) == Some(1690) {
                return Err(scheduler::Error::Message(format!(
                    "stats counter overflow: {name}"
                )));
            }
            return Err(sql_error(error));
        }
    }
    Ok(())
}

async fn insert_completion(
    transaction: &mut Transaction<'_, MySql>,
    payload: &payload::Payload,
    worker_id: &str,
) -> Result<(), scheduler::Error> {
    sqlx::query(
        "INSERT INTO completions \
         (request_id, version, task_id, trace_id, node, worker_id, state, error, \
          start_time, end_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(3), CURRENT_TIMESTAMP(3))",
    )
    .bind(&payload.id)
    .bind(payload.version)
    .bind(&payload.task_id)
    .bind(&payload.trace_id)
    .bind(&payload.node)
    .bind(worker_id)
    .bind(state(payload.state))
    .bind(&payload.error)
    .bind(payload.start_time)
    .bind(payload.end_time)
    .execute(&mut **transaction)
    .await
    .map_err(sql_error)?;
    Ok(())
}

async fn failed_workers(
    transaction: &mut Transaction<'_, MySql>,
    request_id: &str,
) -> Result<Vec<String>, scheduler::Error> {
    let rows = sqlx::query(
        "SELECT worker_id, position FROM failed_workers \
         WHERE request_id = ? ORDER BY position FOR UPDATE",
    )
    .bind(request_id)
    .fetch_all(&mut **transaction)
    .await
    .map_err(sql_error)?;
    let mut workers = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let position = row.try_get::<i32, _>("position").map_err(sql_error)?;
        if position != i32::try_from(index + 1).unwrap_or(i32::MAX) {
            return Err(scheduler::Error::InvalidRequest {
                id: request_id.to_string(),
                message: "stored failed Worker positions are invalid".to_string(),
            });
        }
        workers.push(decode::string(&row, "worker_id")?);
    }
    Ok(workers)
}

fn state(value: net::State) -> &'static str {
    match value {
        net::State::Pending => "pending",
        net::State::Processing => "processing",
        net::State::Done => "done",
        net::State::Failed => "failed",
    }
}
