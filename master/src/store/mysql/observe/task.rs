use std::collections::HashMap;

use serde_json::Value;
use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, QueryBuilder, Row};

use super::super::MySql;
use super::super::validate::{identifier, namespace as validate_namespace};
use crate::Error;
use crate::types::task::CodeSeed;
use crate::types::{Page, cursor, task};

const ENDPOINT: &str = "tasks";

impl MySql {
    pub(crate) async fn tasks(
        &self,
        namespace: &str,
        list: &task::List,
    ) -> Result<Page<task::Summary>, Error> {
        validate_namespace(namespace)?;
        let limit = list.limit()?;
        let filter = list.filter();
        let cursor = super::timed(list.cursor.as_deref(), namespace, ENDPOINT, &filter)?;
        let mut query = QueryBuilder::<SqlxMySql>::new(
            "SELECT id, name, state, run_mode, interval_ms, priority, next_time, created_time, \
             updated_time FROM tasks WHERE namespace = ",
        );
        query.push_bind(namespace);
        if let Some(state) = list.state {
            query.push(" AND state = ").push_bind(state.code());
        }
        if let Some((time, id)) = cursor {
            query
                .push(" AND (updated_time < ")
                .push_bind(time)
                .push(" OR (updated_time = ")
                .push_bind(time)
                .push(" AND id < ")
                .push_bind(id)
                .push("))");
        }
        query
            .push(" ORDER BY updated_time DESC, id DESC LIMIT ")
            .push_bind((limit + 1) as u64);
        let rows = query.build().fetch_all(&self.pool).await?;
        let items = rows
            .into_iter()
            .map(|row| summary(&row))
            .collect::<Result<Vec<_>, _>>()?;
        super::page(items, limit, namespace, ENDPOINT, &filter, |item| {
            cursor::Key::Timed {
                time: item.updated_time,
                id: item.id.clone(),
            }
        })
    }

    pub(crate) async fn task(
        &self,
        namespace: &str,
        id: &str,
    ) -> Result<Option<task::Detail>, Error> {
        validate_namespace(namespace)?;
        identifier(id, "Task id")?;
        let row = sqlx::query(
            "SELECT id, name, state, run_mode, interval_ms, priority, params, dsl, seed_specs, \
             persister_id, attachment, error, next_time, created_time, updated_time \
             FROM tasks WHERE namespace = ? AND id = ?",
        )
        .bind(namespace)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        row.map(detail).transpose()
    }
}

fn summary(row: &MySqlRow) -> Result<task::Summary, Error> {
    let run_mode: i8 = row.try_get("run_mode")?;
    let periodic = match run_mode {
        0 => false,
        1 => true,
        _ => {
            return Err(Error::Invalid(format!(
                "invalid stored Task run mode: {run_mode}"
            )));
        }
    };
    Ok(task::Summary {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        state: task::State::from_code(row.try_get("state")?)?,
        periodic,
        interval_ms: row.try_get("interval_ms")?,
        priority: row.try_get("priority")?,
        next_time: row.try_get("next_time")?,
        created_time: row.try_get("created_time")?,
        updated_time: row.try_get("updated_time")?,
    })
}

fn detail(row: MySqlRow) -> Result<task::Detail, Error> {
    let params: Json<HashMap<String, Value>> = row.try_get("params")?;
    let dsl: Option<Json<Value>> = row.try_get("dsl")?;
    let seeds: Option<Json<Vec<CodeSeed>>> = row.try_get("seed_specs")?;
    let attachment: Option<Json<Value>> = row.try_get("attachment")?;
    Ok(task::Detail {
        summary: summary(&row)?,
        params: params.0,
        dsl: dsl
            .map(|Json(value)| serde_json::from_value(value))
            .transpose()?,
        seeds: seeds.map_or_else(Vec::new, |Json(value)| value),
        persister_id: row.try_get("persister_id")?,
        attachment: attachment.map(|Json(value)| value),
        error: row.try_get("error")?,
    })
}
