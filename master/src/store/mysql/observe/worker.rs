use sqlx::mysql::MySqlRow;
use sqlx::types::Json;
use sqlx::{MySql as SqlxMySql, QueryBuilder, Row};

use super::super::MySql;
use super::super::request::mode_name;
use super::super::time::now_millis;
use super::super::validate::namespace as validate_namespace;
use crate::Error;
use crate::control::{Page, cursor, request, worker};

const ENDPOINT: &str = "workers";

impl MySql {
    pub(crate) async fn workers(
        &self,
        namespace: &str,
        list: &worker::List,
    ) -> Result<Page<worker::Summary>, Error> {
        validate_namespace(namespace)?;
        let limit = list.limit()?;
        let filter = list.filter();
        let cursor = super::id(list.cursor.as_deref(), namespace, ENDPOINT, &filter)?;
        let deadline = now_millis().saturating_sub(self.worker_timeout_ms);
        let mut query = QueryBuilder::<SqlxMySql>::new(
            "SELECT id, modes, last_heartbeat, created_time, updated_time \
             FROM workers WHERE namespace = ",
        );
        query.push_bind(namespace);
        if let Some(mode) = list.mode.as_ref() {
            query
                .push(" AND JSON_CONTAINS(modes, JSON_QUOTE(")
                .push_bind(mode_name(mode))
                .push("))");
        }
        if let Some(online) = list.online {
            query.push(if online {
                " AND last_heartbeat > "
            } else {
                " AND last_heartbeat <= "
            });
            query.push_bind(deadline);
        }
        if let Some(id) = cursor {
            query.push(" AND id > ").push_bind(id);
        }
        query
            .push(" ORDER BY id ASC LIMIT ")
            .push_bind((limit + 1) as u64);
        let rows = query.build().fetch_all(&self.pool).await?;
        let items = rows
            .into_iter()
            .map(|row| summary(row, deadline))
            .collect::<Result<Vec<_>, _>>()?;
        super::page(items, limit, namespace, ENDPOINT, &filter, |item| {
            cursor::Key::Id {
                id: item.id.clone(),
            }
        })
    }
}

fn summary(row: MySqlRow, deadline: i64) -> Result<worker::Summary, Error> {
    let modes: Json<Vec<String>> = row.try_get("modes")?;
    let modes = modes
        .0
        .into_iter()
        .map(|mode| request::mode(&mode))
        .collect::<Result<Vec<_>, _>>()?;
    let last_heartbeat = row.try_get("last_heartbeat")?;
    Ok(worker::Summary {
        id: row.try_get("id")?,
        modes,
        last_heartbeat,
        online: last_heartbeat > deadline,
        created_time: row.try_get("created_time")?,
        updated_time: row.try_get("updated_time")?,
    })
}
