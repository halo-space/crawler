use std::collections::HashMap;

use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, Row, Transaction};

use super::super::MySql;
use super::super::request::insert_values;
use super::super::trace::create;
use super::super::validate::namespace as validate_namespace;
use super::{CodeSeed, materialize};
use crate::Error;

#[derive(Clone, PartialEq)]
struct Stored {
    id: String,
    run_mode: i8,
    interval_ms: i64,
    priority: i32,
    params: Value,
    dsl: Option<Value>,
    seeds: Option<Value>,
    persister_id: Option<String>,
    attachment: Option<Value>,
    next_time: i64,
}

impl MySql {
    pub async fn dispatch_due(
        &self,
        namespace: &str,
        now: i64,
        limit: usize,
    ) -> Result<Vec<String>, Error> {
        validate_namespace(namespace)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut traces = Vec::with_capacity(limit);
        for _ in 0..limit {
            let mut tx = self.pool.begin().await?;
            let row = sqlx::query(
                "SELECT id, run_mode, interval_ms, priority, params, dsl, seed_specs, \
                 persister_id, attachment, next_time FROM tasks WHERE namespace = ? AND state = 1 \
                 AND next_time <= ? ORDER BY priority DESC, next_time ASC, id ASC \
                 LIMIT 1 FOR UPDATE SKIP LOCKED",
            )
            .bind(namespace)
            .bind(now)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(row) = row else {
                tx.commit().await?;
                break;
            };
            let task = Stored::from_row(row)?;
            match self.dispatch(&mut tx, namespace, &task, now).await {
                Ok(trace_id) => {
                    tx.commit().await?;
                    traces.push(trace_id);
                }
                Err(error) if is_data_error(&error) => {
                    tx.rollback().await?;
                    if self.quarantine(namespace, &task, now, &error).await? {
                        tracing::error!(task_id = task.id, %error, "quarantined invalid Task");
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Ok(traces)
    }

    async fn dispatch(
        &self,
        tx: &mut Transaction<'_, SqlxMySql>,
        namespace: &str,
        task: &Stored,
        now: i64,
    ) -> Result<String, Error> {
        let periodic = match task.run_mode {
            0 => false,
            1 => true,
            _ => {
                return Err(Error::Invalid(format!(
                    "invalid stored Task run_mode: {}",
                    task.run_mode
                )));
            }
        };
        if periodic && task.interval_ms <= 0 {
            return Err(Error::Invalid(
                "periodic Task interval_ms must be positive".to_string(),
            ));
        }
        if !periodic && task.interval_ms != 0 {
            return Err(Error::Invalid(
                "one-shot Task interval_ms must be zero".to_string(),
            ));
        }
        let params: HashMap<String, Value> = serde_json::from_value(task.params.clone())?;
        let seeds: Vec<CodeSeed> =
            serde_json::from_value(task.seeds.clone().ok_or_else(|| {
                Error::Invalid("stored Task seed_specs must not be null".to_string())
            })?)?;
        let trace_id = spider::trace::next_id();
        let snapshot = spider::trace::Snapshot {
            task_id: task.id.clone(),
            params,
            attachment: task.attachment.clone(),
            persister_id: task.persister_id.clone(),
            priority: task.priority,
            dsl: task.dsl.clone().map(serde_json::from_value).transpose()?,
        };
        snapshot
            .validate()
            .map_err(|message| Error::Invalid(format!("invalid Trace Snapshot: {message}")))?;
        create(tx, namespace, &trace_id, &snapshot, self.max_response_bytes).await?;

        let requests = if let Some(config) = snapshot.dsl.as_ref() {
            config
                .initial_requests(task.id.clone(), trace_id.clone(), snapshot.params.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?
        } else {
            materialize(&task.id, &trace_id, &snapshot.params, &seeds)?
        };
        if snapshot.dsl.is_some() && !seeds.is_empty() {
            return Err(Error::Invalid(
                "Task must define either Rules DSL or Code seeds, not both".to_string(),
            ));
        }
        if snapshot.dsl.is_none() && seeds.is_empty() {
            return Err(Error::Invalid(
                "Code Task must contain at least one serialized seed".to_string(),
            ));
        }
        insert_values(tx, namespace, &requests).await?;

        let next_time = if periodic {
            next_period(now, task.interval_ms)
        } else {
            0
        };
        let state = if periodic { 1_i8 } else { 3_i8 };
        sqlx::query(
            "UPDATE tasks SET state = ?, error = NULL, next_time = ?, updated_time = ? \
             WHERE namespace = ? AND id = ?",
        )
        .bind(state)
        .bind(next_time)
        .bind(now)
        .bind(namespace)
        .bind(&task.id)
        .execute(&mut **tx)
        .await?;
        Ok(trace_id)
    }

    async fn quarantine(
        &self,
        namespace: &str,
        failed: &Stored,
        now: i64,
        error: &Error,
    ) -> Result<bool, Error> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT id, run_mode, interval_ms, priority, params, dsl, seed_specs, \
             persister_id, attachment, next_time FROM tasks \
             WHERE namespace = ? AND id = ? AND state = 1 FOR UPDATE",
        )
        .bind(namespace)
        .bind(&failed.id)
        .fetch_optional(&mut *tx)
        .await?;
        let unchanged = row.map(Stored::from_row).transpose()?.as_ref() == Some(failed);
        if unchanged {
            sqlx::query(
                "UPDATE tasks SET state = 4, error = ?, updated_time = ? \
                 WHERE namespace = ? AND id = ? AND state = 1",
            )
            .bind(error.to_string())
            .bind(now)
            .bind(namespace)
            .bind(&failed.id)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(unchanged)
    }
}

impl Stored {
    fn from_row(row: MySqlRow) -> Result<Self, Error> {
        let params: Json<Value> = row.try_get("params")?;
        let dsl: Option<Json<Value>> = row.try_get("dsl")?;
        let seeds: Option<Json<Value>> = row.try_get("seed_specs")?;
        let attachment: Option<Json<Value>> = row.try_get("attachment")?;
        Ok(Self {
            id: row.try_get("id")?,
            run_mode: row.try_get("run_mode")?,
            interval_ms: row.try_get("interval_ms")?,
            priority: row.try_get("priority")?,
            params: params.0,
            dsl: dsl.map(|Json(value)| value),
            seeds: seeds.map(|Json(value)| value),
            persister_id: row.try_get("persister_id")?,
            attachment: attachment.map(|Json(value)| value),
            next_time: row.try_get("next_time")?,
        })
    }
}

fn is_data_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Serialization(_) | Error::Invalid(_) | Error::InvalidTrace { .. }
    )
}

pub(in crate::store::mysql) fn next_period(now: i64, interval: i64) -> i64 {
    let quotient = now.div_euclid(interval);
    quotient.saturating_add(1).saturating_mul(interval)
}
