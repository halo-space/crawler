use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, QueryBuilder, Row};

use super::super::MySql;
use super::super::validate::{identifier, namespace as validate_namespace, worker_id};
use crate::Error;
use crate::types::{Page, cursor, request};

const ENDPOINT: &str = "requests";
const LIST: &str = "SELECT r.id, r.task_id, r.trace_id, r.node, r.mode, r.state, r.version, \
    r.priority, r.next_time, r.leased_by, r.lease_time, r.retry_count, r.max_retry_count, \
    r.created_time, r.updated_time FROM requests AS r WHERE r.namespace = ";
const DETAIL: &str = "SELECT r.id, r.task_id, r.trace_id, r.node, r.mode, r.state, r.version, \
    r.priority, r.next_time, r.leased_by, r.lease_time, r.retry_count, r.max_retry_count, \
    r.created_time, r.updated_time, r.snapshot, r.failed_workers, r.ack_version \
    FROM requests AS r WHERE r.namespace = ? AND r.id = ?";

impl MySql {
    pub(crate) async fn requests(
        &self,
        namespace: &str,
        list: &request::List,
    ) -> Result<Page<request::Summary>, Error> {
        validate_namespace(namespace)?;
        if let Some(trace_id) = list.trace_id.as_deref() {
            identifier(trace_id, "trace_id")?;
        }
        if let Some(id) = list.worker_id.as_deref() {
            worker_id(id)?;
        }
        let limit = list.limit()?;
        let filter = list.filter();
        let cursor = super::timed(list.cursor.as_deref(), namespace, ENDPOINT, &filter)?;
        let mut query = QueryBuilder::<SqlxMySql>::new(LIST);
        query.push_bind(namespace);
        if let Some(trace_id) = list.trace_id.as_deref() {
            query.push(" AND r.trace_id = ").push_bind(trace_id);
        }
        if let Some(state) = list.state {
            query
                .push(" AND r.state = ")
                .push_bind(request::state_code(state));
        }
        if let Some(worker_id) = list.worker_id.as_deref() {
            query.push(" AND r.leased_by = ").push_bind(worker_id);
        }
        if let Some((time, id)) = cursor {
            query
                .push(" AND (r.created_time < ")
                .push_bind(time)
                .push(" OR (r.created_time = ")
                .push_bind(time)
                .push(" AND r.id < ")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY r.created_time DESC, r.id DESC LIMIT ")
            .push_bind((limit + 1) as u64);
        let rows = query.build().fetch_all(&self.pool).await?;
        let items = rows
            .into_iter()
            .map(|row| summary(&row))
            .collect::<Result<Vec<_>, _>>()?;
        super::page(items, limit, namespace, ENDPOINT, &filter, |item| {
            cursor::Key::Timed {
                time: item.created_time,
                id: item.id.clone(),
            }
        })
    }

    pub(crate) async fn request_detail(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<request::Detail>, Error> {
        validate_namespace(namespace)?;
        identifier(id, "request id")?;
        let row = sqlx::query(DETAIL)
            .bind(namespace)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let snapshot: Json<Value> = row.try_get("snapshot")?;
        let failed_workers: Json<Vec<String>> = row.try_get("failed_workers")?;
        let completion = sqlx::query(
            "SELECT version, worker_id, state, error, created_time FROM request_completions \
             WHERE namespace = ? AND request_id = ? ORDER BY version DESC LIMIT 1",
        )
        .bind(namespace)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .map(completion)
        .transpose()?;
        Ok(Some(request::Detail {
            summary: summary(&row)?,
            snapshot: snapshot.0,
            failed_workers: failed_workers.0,
            ack_version: row.try_get("ack_version")?,
            completion,
        }))
    }
}

fn summary(row: &MySqlRow) -> Result<request::Summary, Error> {
    let mode: String = row.try_get("mode")?;
    Ok(request::Summary {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        trace_id: row.try_get("trace_id")?,
        node: row.try_get("node")?,
        mode: request::mode(&mode)?,
        state: request::state(row.try_get("state")?)?,
        version: row.try_get("version")?,
        priority: row.try_get("priority")?,
        next_time: row.try_get("next_time")?,
        leased_by: row.try_get("leased_by")?,
        lease_time: row.try_get("lease_time")?,
        retry_count: row.try_get("retry_count")?,
        max_retry_count: row.try_get("max_retry_count")?,
        created_time: row.try_get("created_time")?,
        updated_time: row.try_get("updated_time")?,
    })
}

fn completion(row: MySqlRow) -> Result<request::CompletionInfo, Error> {
    Ok(request::CompletionInfo {
        version: row.try_get("version")?,
        worker_id: row.try_get("worker_id")?,
        state: request::state(row.try_get("state")?)?,
        error: row.try_get("error")?,
        created_time: row.try_get("created_time")?,
    })
}
