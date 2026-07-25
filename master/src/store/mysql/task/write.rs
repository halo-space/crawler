use sqlx::Row;
use sqlx::types::Json;

use super::super::time::now_millis;
use super::super::validate::namespace as validate_namespace;
use super::super::{MySql, duplicate};
use super::{Task, validate};
use crate::Error;

impl MySql {
    pub(crate) async fn upsert_task(&self, namespace: &str, task: &Task) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate(task)?;
        let now = now_millis();
        let dsl = task.dsl.as_ref().map(serde_json::to_value).transpose()?;
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query(
            "SELECT id, name FROM tasks WHERE namespace = ? AND (id = ? OR name = ?) \
             ORDER BY id FOR UPDATE",
        )
        .bind(namespace)
        .bind(&task.id)
        .bind(&task.name)
        .fetch_all(&mut *tx)
        .await?;
        let existing = rows
            .into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<String, _>("id")?,
                    row.try_get::<String, _>("name")?,
                ))
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()?;
        if existing
            .iter()
            .any(|(id, name)| name == &task.name && id != &task.id)
        {
            return Err(Error::Conflict(format!(
                "Task name already belongs to another Task: {}",
                task.name
            )));
        }
        let exists = existing.iter().any(|(id, _)| id == &task.id);
        let query = if exists {
            sqlx::query(
                "UPDATE tasks SET name = ?, state = 1, error = NULL, run_mode = ?, \
                 interval_ms = ?, priority = ?, params = ?, dsl = ?, seed_specs = ?, \
                 persister_id = ?, attachment = ?, next_time = ?, updated_time = ? \
                 WHERE namespace = ? AND id = ?",
            )
            .bind(&task.name)
            .bind(i8::from(task.periodic))
            .bind(task.interval_ms)
            .bind(task.priority)
            .bind(Json(&task.params))
            .bind(dsl.map(Json))
            .bind(Json(&task.seeds))
            .bind(&task.persister_id)
            .bind(task.attachment.as_ref().map(Json))
            .bind(task.next_time)
            .bind(now)
            .bind(namespace)
            .bind(&task.id)
        } else {
            sqlx::query(
                r#"INSERT INTO tasks (
                    namespace, id, name, state, run_mode, interval_ms, priority,
                    params, dsl, seed_specs, persister_id, attachment, next_time, created_time,
                    updated_time
                ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
            )
            .bind(namespace)
            .bind(&task.id)
            .bind(&task.name)
            .bind(i8::from(task.periodic))
            .bind(task.interval_ms)
            .bind(task.priority)
            .bind(Json(&task.params))
            .bind(dsl.map(Json))
            .bind(Json(&task.seeds))
            .bind(&task.persister_id)
            .bind(task.attachment.as_ref().map(Json))
            .bind(task.next_time)
            .bind(now)
            .bind(now)
        };
        if let Err(error) = query.execute(&mut *tx).await {
            if duplicate(&error) {
                return Err(Error::Conflict(format!(
                    "Task id or name already exists: {}/{}",
                    task.id, task.name
                )));
            }
            return Err(error.into());
        }
        tx.commit().await?;
        Ok(())
    }
}
