use std::collections::HashMap;

use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, QueryBuilder, Row};

use super::super::MySql;
use super::super::validate::{identifier, namespace as validate_namespace};
use crate::Error;
use crate::control::{Page, cursor, request, trace};

const ENDPOINT: &str = "traces";

impl MySql {
    pub(crate) async fn traces(
        &self,
        namespace: &str,
        list: &trace::List,
    ) -> Result<Page<trace::Summary>, Error> {
        validate_namespace(namespace)?;
        if let Some(task_id) = list.task_id.as_deref() {
            identifier(task_id, "task_id")?;
        }
        let limit = list.limit()?;
        let filter = list.filter();
        let cursor = super::timed(list.cursor.as_deref(), namespace, ENDPOINT, &filter)?;
        let mut query = QueryBuilder::<SqlxMySql>::new(
            "SELECT id, task_id, CAST(JSON_UNQUOTE(JSON_EXTRACT(snapshot, '$.priority')) AS \
             SIGNED) AS priority, start_time, created_time FROM traces WHERE namespace = ",
        );
        query.push_bind(namespace);
        if let Some(task_id) = list.task_id.as_deref() {
            query.push(" AND task_id = ").push_bind(task_id);
        }
        if let Some((time, id)) = cursor {
            query
                .push(" AND (created_time < ")
                .push_bind(time)
                .push(" OR (created_time = ")
                .push_bind(time)
                .push(" AND id < ")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY created_time DESC, id DESC LIMIT ")
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

    pub(crate) async fn trace_detail(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<trace::Detail>, Error> {
        validate_namespace(namespace)?;
        identifier(id, "trace_id")?;
        let row = sqlx::query(
            "SELECT id, task_id, CAST(JSON_UNQUOTE(JSON_EXTRACT(snapshot, '$.priority')) AS \
             SIGNED) AS priority, start_time, created_time, snapshot \
             FROM traces WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let snapshot: Json<Value> = row.try_get("snapshot")?;
        let stats = sqlx::query(
            "SELECT name, total, done, filter_count, dedup, validate_count, download \
             FROM trace_stats WHERE namespace = ? AND trace_id = ? ORDER BY name ASC",
        )
        .bind(namespace)
        .bind(id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            Ok((
                row.try_get("name")?,
                spider::stats::Counter {
                    total: row.try_get("total")?,
                    done: row.try_get("done")?,
                    filter: row.try_get("filter_count")?,
                    dedup: row.try_get("dedup")?,
                    validate: row.try_get("validate_count")?,
                    download: row.try_get("download")?,
                },
            ))
        })
        .collect::<Result<HashMap<_, _>, Error>>()?;
        let mut counts = trace::Counts::default();
        for row in sqlx::query(
            "SELECT state, COUNT(*) AS count FROM requests \
             WHERE namespace = ? AND trace_id = ? GROUP BY state",
        )
        .bind(namespace)
        .bind(id)
        .fetch_all(&self.pool)
        .await?
        {
            let count: i64 = row.try_get("count")?;
            let count = u64::try_from(count)
                .map_err(|_| Error::Invalid("invalid stored Request count".to_string()))?;
            match request::state(row.try_get("state")?)? {
                spider::net::State::Pending => counts.pending = count,
                spider::net::State::Processing => counts.processing = count,
                spider::net::State::Done => counts.done = count,
                spider::net::State::Failed => counts.failed = count,
            }
        }
        Ok(Some(trace::Detail {
            summary: summary(&row)?,
            snapshot: snapshot.0,
            stats,
            requests: counts,
        }))
    }
}

fn summary(row: &MySqlRow) -> Result<trace::Summary, Error> {
    let priority: i64 = row.try_get("priority")?;
    Ok(trace::Summary {
        id: row.try_get("id")?,
        task_id: row.try_get("task_id")?,
        priority: i32::try_from(priority)
            .map_err(|_| Error::Invalid("invalid stored Trace priority".to_string()))?,
        start_time: row.try_get("start_time")?,
        created_time: row.try_get("created_time")?,
    })
}
