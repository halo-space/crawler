use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Row, Transaction};

use super::operation;
use super::request::insert as insert_requests;
use super::time::now_millis;
use super::validate::{identifier, namespace as validate_namespace};
use super::{MySql, duplicate};
use crate::{Error, types};

impl MySql {
    pub(crate) async fn init(
        &self,
        namespace: &str,
        key: &str,
        body: &types::Init,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        body.trace
            .validate()
            .map_err(|message| Error::InvalidTrace {
                id: body.trace_id.clone(),
                message,
            })?;
        identifier(&body.trace_id, "trace_id")?;
        identifier(&body.trace.task_id, "trace.task_id")?;
        let digest = operation::digest(body)?;
        let mut tx = self.pool.begin().await?;
        if operation::reserve::<Value>(&mut tx, namespace, "init", key, &digest)
            .await?
            .is_some()
        {
            tx.commit().await?;
            return Ok(());
        }
        create(
            &mut tx,
            namespace,
            &body.trace_id,
            &body.trace,
            self.max_response_bytes,
        )
        .await?;
        if body.requests.iter().any(|request| {
            request.task_id != body.trace.task_id || request.trace_id != body.trace_id
        }) {
            return Err(Error::Invalid(
                "initial requests must belong to the initialized trace".to_string(),
            ));
        }
        insert_requests(&mut tx, namespace, &body.requests).await?;
        operation::record(
            &mut tx,
            namespace,
            "init",
            key,
            &digest,
            &Value::Object(Default::default()),
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn trace(
        &self,
        namespace: &str,
        trace_id: &str,
    ) -> Result<Option<spider::trace::Snapshot>, Error> {
        validate_namespace(namespace)?;
        identifier(trace_id, "trace_id")?;
        let stored = sqlx::query_as::<_, (String, Json<Value>)>(
            "SELECT task_id, snapshot FROM traces WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(trace_id)
        .fetch_optional(&self.pool)
        .await?;
        stored
            .map(|(task_id, Json(value))| {
                let snapshot = decode(trace_id, &task_id, value)?;
                if !fits(&snapshot, self.max_response_bytes)? {
                    return Err(Error::Invalid(format!(
                        "Trace Snapshot exceeds the configured {} byte API response limit",
                        self.max_response_bytes
                    )));
                }
                Ok(snapshot)
            })
            .transpose()
    }
}

pub(super) async fn create(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    trace_id: &str,
    snapshot: &spider::trace::Snapshot,
    max_response_bytes: usize,
) -> Result<(), Error> {
    identifier(trace_id, "trace_id")?;
    identifier(&snapshot.task_id, "trace.task_id")?;
    if !fits(snapshot, max_response_bytes)? {
        return Err(Error::Invalid(format!(
            "Trace Snapshot exceeds the configured {max_response_bytes} byte API response limit"
        )));
    }
    let now = now_millis();
    let result = sqlx::query(
        "INSERT INTO traces \
         (namespace, id, task_id, snapshot, start_time, created_time, updated_time) \
         VALUES (?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(namespace)
    .bind(trace_id)
    .bind(&snapshot.task_id)
    .bind(Json(snapshot))
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await;
    if let Err(error) = result {
        if duplicate(&error) {
            return Err(Error::Conflict(format!("Trace already exists: {trace_id}")));
        }
        return Err(error.into());
    }
    Ok(())
}

fn fits(snapshot: &spider::trace::Snapshot, max_response_bytes: usize) -> Result<bool, Error> {
    Ok(serde_json::to_vec(&Some(snapshot))?.len() <= max_response_bytes)
}

pub(super) async fn load(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    trace_id: &str,
) -> Result<spider::trace::Snapshot, Error> {
    identifier(trace_id, "trace_id")?;
    let (task_id, Json(value)) = sqlx::query_as::<_, (String, Json<Value>)>(
        "SELECT task_id, snapshot FROM traces WHERE namespace = ? AND id = ?",
    )
    .bind(namespace)
    .bind(trace_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| Error::TraceNotFound(trace_id.to_string()))?;
    decode(trace_id, &task_id, value)
}

fn decode(trace_id: &str, task_id: &str, value: Value) -> Result<spider::trace::Snapshot, Error> {
    let trace: spider::trace::Snapshot =
        serde_json::from_value(value).map_err(|error| Error::InvalidTrace {
            id: trace_id.to_string(),
            message: error.to_string(),
        })?;
    trace.validate().map_err(|message| Error::InvalidTrace {
        id: trace_id.to_string(),
        message,
    })?;
    if trace.task_id != task_id {
        return Err(Error::InvalidTrace {
            id: trace_id.to_string(),
            message: format!(
                "stored task_id {task_id:?} does not match Trace Snapshot task_id {:?}",
                trace.task_id
            ),
        });
    }
    Ok(trace)
}

pub(super) fn validate_snapshot(
    snapshot: &spider::net::request::Snapshot,
    trace: &spider::trace::Snapshot,
) -> Result<(), Error> {
    snapshot
        .clone()
        .restore(Some(Arc::new(trace.clone())))
        .map(|_| ())
        .map_err(|message| Error::Invalid(format!("invalid Request Snapshot: {message}")))
}

pub(super) fn validate_stats(stats: &HashMap<String, Value>) -> Result<(), Error> {
    for (name, value) in stats {
        identifier(name, "stats name")?;
        let counter: spider::stats::Counter = serde_json::from_value(value.clone())?;
        if [
            counter.total,
            counter.done,
            counter.filter,
            counter.dedup,
            counter.validate,
            counter.download,
        ]
        .into_iter()
        .any(|value| value < 0)
        {
            return Err(Error::Invalid(format!(
                "stats counter must be non-negative: {name}"
            )));
        }
    }
    Ok(())
}

pub(super) async fn apply_stats(
    tx: &mut Transaction<'_, SqlxMySql>,
    namespace: &str,
    trace_id: &str,
    stats: &HashMap<String, Value>,
    now: i64,
) -> Result<(), Error> {
    let mut counters = stats.iter().collect::<Vec<_>>();
    counters.sort_unstable_by_key(|(name, _)| name.as_str());
    for (name, value) in counters {
        let delta: spider::stats::Counter = serde_json::from_value(value.clone())?;
        sqlx::query(
            "INSERT IGNORE INTO trace_stats \
             (namespace, trace_id, name, total, done, filter_count, dedup, validate_count, \
              download, created_time, updated_time) \
             VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, ?, ?)",
        )
        .bind(namespace)
        .bind(trace_id)
        .bind(name)
        .bind(now)
        .bind(now)
        .execute(&mut **tx)
        .await?;
        let row = sqlx::query(
            "SELECT total, done, filter_count, dedup, validate_count, download FROM trace_stats \
             WHERE namespace = ? AND trace_id = ? AND name = ? FOR UPDATE",
        )
        .bind(namespace)
        .bind(trace_id)
        .bind(name)
        .fetch_one(&mut **tx)
        .await?;
        let current = spider::stats::Counter {
            total: row.try_get("total")?,
            done: row.try_get("done")?,
            filter: row.try_get("filter_count")?,
            dedup: row.try_get("dedup")?,
            validate: row.try_get("validate_count")?,
            download: row.try_get("download")?,
        };
        let merged = spider::stats::Counter {
            total: current
                .total
                .checked_add(delta.total)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
            done: current
                .done
                .checked_add(delta.done)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
            filter: current
                .filter
                .checked_add(delta.filter)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
            dedup: current
                .dedup
                .checked_add(delta.dedup)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
            validate: current
                .validate
                .checked_add(delta.validate)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
            download: current
                .download
                .checked_add(delta.download)
                .ok_or_else(|| Error::Invalid(format!("stats overflow: {name}")))?,
        };
        sqlx::query(
            "UPDATE trace_stats SET total = ?, done = ?, filter_count = ?, dedup = ?, \
             validate_count = ?, download = ?, updated_time = ? \
             WHERE namespace = ? AND trace_id = ? AND name = ?",
        )
        .bind(merged.total)
        .bind(merged.done)
        .bind(merged.filter)
        .bind(merged.dedup)
        .bind(merged.validate)
        .bind(merged.download)
        .bind(now)
        .bind(namespace)
        .bind(trace_id)
        .bind(name)
        .execute(&mut **tx)
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::to_value;

    use super::*;

    #[test]
    fn stats_names_use_identifier_bounds() {
        let value = to_value(spider::stats::Counter::default()).unwrap();

        assert!(validate_stats(&HashMap::from([("valid".to_string(), value.clone())])).is_ok());
        assert!(validate_stats(&HashMap::from([("x".repeat(192), value.clone())])).is_err());
        assert!(validate_stats(&HashMap::from([("bad\nname".to_string(), value)])).is_err());
    }
}
