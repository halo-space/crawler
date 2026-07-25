use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, QueryBuilder, Row};

use super::super::MySql;
use super::super::validate::{identifier, namespace as validate_namespace};
use crate::Error;
use crate::control::{Page, cursor, item};

const ENDPOINT: &str = "items";

impl MySql {
    pub(crate) async fn item_list(
        &self,
        namespace: &str,
        list: &item::List,
    ) -> Result<Page<item::Summary>, Error> {
        validate_namespace(namespace)?;
        if let Some(trace_id) = list.trace_id.as_deref() {
            identifier(trace_id, "trace_id")?;
        }
        if let Some(request_id) = list.request_id.as_deref() {
            identifier(request_id, "request_id")?;
        }
        let limit = list.limit()?;
        let filter = list.filter()?;
        let cursor = super::timed(list.cursor.as_deref(), namespace, ENDPOINT, &filter)?;
        let mut query = QueryBuilder::<SqlxMySql>::new(
            "SELECT id, item_id, task_id, trace_id, request_id, persister_id, config_version, \
             timezone, created_time FROM items WHERE namespace = ",
        );
        query.push_bind(namespace);
        if let Some(trace_id) = list.trace_id.as_deref() {
            query.push(" AND trace_id = ").push_bind(trace_id);
        }
        if let Some(request_id) = list.request_id.as_deref() {
            query.push(" AND request_id = ").push_bind(request_id);
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

    pub(crate) async fn item_detail(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<item::Detail>, Error> {
        validate_namespace(namespace)?;
        identifier(id, "Item row id")?;
        let row = sqlx::query(
            "SELECT id, item_id, task_id, trace_id, request_id, persister_id, config_version, \
             timezone, created_time, data, updated_time FROM items \
             WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let data: Json<Value> = row.try_get("data")?;
            Ok(item::Detail {
                summary: summary(&row)?,
                data: data.0,
                updated_time: row.try_get("updated_time")?,
            })
        })
        .transpose()
    }
}

fn summary(row: &MySqlRow) -> Result<item::Summary, Error> {
    Ok(item::Summary {
        id: row.try_get("id")?,
        item_id: row.try_get("item_id")?,
        task_id: row.try_get("task_id")?,
        trace_id: row.try_get("trace_id")?,
        request_id: row.try_get("request_id")?,
        persister_id: row.try_get("persister_id")?,
        config_version: row.try_get("config_version")?,
        timezone: row.try_get("timezone")?,
        created_time: row.try_get("created_time")?,
    })
}
