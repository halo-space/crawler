use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Row, Transaction};

use super::super::MySql;
use super::super::operation;
use super::super::time::now_millis;
use super::super::trace;
use super::super::validate::{identity as validate_identity, namespace as validate_namespace};
use super::{DONE, FAILED, PENDING, State, load, queue, verify_identity, verify_lease};
use crate::{Error, types};

impl MySql {
    pub(crate) async fn success(
        &self,
        namespace: &str,
        body: &types::Completion,
    ) -> Result<(), Error> {
        if body.error.is_some() {
            return Err(Error::Invalid(
                "success must not contain an error".to_string(),
            ));
        }
        self.settle(namespace, body, true).await
    }

    pub(crate) async fn failure(
        &self,
        namespace: &str,
        body: &types::Completion,
    ) -> Result<(), Error> {
        validate_error(body.error.as_deref(), self.max_response_bytes)?;
        self.settle(namespace, body, false).await
    }

    async fn settle(
        &self,
        namespace: &str,
        body: &types::Completion,
        success: bool,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate_identity(&body.identity)?;
        if body.start_time < 0 || body.end_time < body.start_time {
            return Err(Error::Invalid("invalid completion timestamps".to_string()));
        }
        trace::validate_stats(&body.stats)?;
        let payload_digest = operation::digest(body)?;
        let mut tx = self.pool.begin().await?;
        if completion_replay(&mut tx, namespace, body, &payload_digest, success).await? {
            tx.commit().await?;
            return Ok(());
        }
        let request = load(&mut tx, namespace, &body.identity.id).await?;
        // A concurrent settlement can commit after the first lookup and before this Request lock.
        // Recheck while holding the Request lock so only one settlement changes its state and stats.
        if completion_replay(&mut tx, namespace, body, &payload_digest, success).await? {
            tx.commit().await?;
            return Ok(());
        }
        verify_identity(&request, &body.identity)?;
        verify_lease(&request, self.lease_timeout_ms)?;
        if request.ack_version != Some(body.identity.version) {
            return Err(Error::NotAcknowledged(body.identity.id.clone()));
        }
        let now = now_millis();
        trace::apply_stats(
            &mut tx,
            namespace,
            &body.identity.trace_id,
            &body.stats,
            now,
        )
        .await?;
        if success {
            let updated = sqlx::query(
                "UPDATE requests SET state = 2, leased_by = '', lease_time = 0, ack_version = NULL, \
                 updated_time = ? WHERE namespace = ? AND id = ? AND version = ? AND state = 1",
            )
            .bind(now)
            .bind(namespace)
            .bind(&body.identity.id)
            .bind(body.identity.version)
            .execute(&mut *tx)
            .await?;
            if updated.rows_affected() != 1 {
                return Err(Error::Unavailable(format!(
                    "success lost Request state transition: {}",
                    body.identity.id
                )));
            }
        } else {
            let retry = request
                .retry_count
                .checked_add(1)
                .ok_or_else(|| Error::Invalid(format!("retry overflow: {}", request.id)))?;
            let mut failed_workers = request.failed_workers.clone();
            append_worker(&mut failed_workers, &body.identity.worker_id);
            let terminal = retry >= request.max_retry_count;
            update_retry(
                &mut tx,
                namespace,
                &request,
                retry,
                &failed_workers,
                terminal,
                now,
            )
            .await?;
        }
        insert_completion(&mut tx, namespace, body, &payload_digest, success).await?;
        tx.commit().await?;
        Ok(())
    }
}

fn validate_error(error: Option<&str>, max_bytes: usize) -> Result<(), Error> {
    let Some(error) = error.filter(|value| !value.is_empty()) else {
        return Err(Error::Invalid(
            "failure requires a non-empty error".to_string(),
        ));
    };
    if error.len() > max_bytes {
        return Err(Error::Invalid(format!(
            "failure error exceeds the configured {max_bytes} byte API limit"
        )));
    }
    Ok(())
}

pub(super) async fn update_retry(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    request: &State,
    retry: i32,
    failed_workers: &[String],
    terminal: bool,
    now: i64,
) -> Result<(), Error> {
    let sequence = if terminal {
        None
    } else {
        Some(queue::next(tx, namespace).await?)
    };
    let updated = sqlx::query(
        "UPDATE requests SET state = ?, retry_count = ?, leased_by = ?, lease_time = ?, \
         ack_version = NULL, next_time = 0, sequence = COALESCE(?, sequence), \
         failed_workers = ?, updated_time = ? \
         WHERE namespace = ? AND id = ? AND version = ? AND state = 1",
    )
    .bind(if terminal { FAILED } else { PENDING })
    .bind(retry)
    .bind(if terminal {
        request.leased_by.clone()
    } else {
        String::new()
    })
    .bind(if terminal { now } else { 0 })
    .bind(sequence)
    .bind(Json(failed_workers))
    .bind(now)
    .bind(namespace)
    .bind(&request.id)
    .bind(request.version)
    .execute(&mut **tx)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(Error::Unavailable(format!(
            "failure lost Request state transition: {}",
            request.id
        )));
    }
    Ok(())
}

pub(super) fn append_worker(workers: &mut Vec<String>, worker: &str) {
    if !worker.is_empty() && !workers.iter().any(|value| value == worker) {
        workers.push(worker.to_string());
    }
}

async fn completion_replay(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    body: &types::Completion,
    payload_digest: &str,
    success: bool,
) -> Result<bool, Error> {
    let row = sqlx::query(
        "SELECT task_id, trace_id, node, worker_id, state, payload_digest FROM request_completions \
         WHERE namespace = ? AND request_id = ? AND version = ? FOR UPDATE",
    )
    .bind(namespace)
    .bind(&body.identity.id)
    .bind(body.identity.version)
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Ok(false);
    };
    for (field, matches) in [
        (
            "task_id",
            row.try_get::<String, _>("task_id")? == body.identity.task_id,
        ),
        (
            "trace_id",
            row.try_get::<String, _>("trace_id")? == body.identity.trace_id,
        ),
        (
            "node",
            row.try_get::<String, _>("node")? == body.identity.node,
        ),
    ] {
        if !matches {
            return Err(Error::Identity {
                id: body.identity.id.clone(),
                field,
            });
        }
    }
    if row.try_get::<String, _>("worker_id")? != body.identity.worker_id {
        return Err(Error::Lease(body.identity.id.clone()));
    }
    if row.try_get::<i8, _>("state")? != if success { DONE } else { FAILED } {
        return Err(Error::State(body.identity.id.clone()));
    }
    if row.try_get::<String, _>("payload_digest")? != payload_digest {
        return Err(Error::Conflict(format!(
            "completion body conflicts with existing completion for request {}",
            body.identity.id
        )));
    }
    Ok(true)
}

pub(super) async fn insert_completion(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    body: &types::Completion,
    payload_digest: &str,
    success: bool,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT INTO request_completions (namespace, request_id, version, task_id, trace_id, node, \
         worker_id, state, error, payload_digest, created_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(namespace)
    .bind(&body.identity.id)
    .bind(body.identity.version)
    .bind(&body.identity.task_id)
    .bind(&body.identity.trace_id)
    .bind(&body.identity.node)
    .bind(&body.identity.worker_id)
    .bind(if success { DONE } else { FAILED })
    .bind(&body.error)
    .bind(payload_digest)
    .bind(now_millis())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_error;

    #[test]
    fn failure_error_uses_the_configured_utf8_byte_limit() {
        assert!(validate_error(Some("error"), 5).is_ok());
        assert!(validate_error(Some("error"), 4).is_err());
        assert!(validate_error(Some("\u{e9}\u{e9}"), 4).is_ok());
        assert!(validate_error(Some("\u{e9}\u{e9}x"), 4).is_err());
        assert!(validate_error(None, 5).is_err());
        assert!(validate_error(Some(""), 5).is_err());
    }
}
