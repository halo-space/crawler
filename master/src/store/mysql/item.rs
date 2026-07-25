use sqlx::types::Json;

use super::MySql;
use super::operation;
use super::request::{load, verify_identity, verify_lease};
use super::time::now_millis;
use super::trace;
use super::validate::{identifier, identity, namespace as validate_namespace};
use crate::{Error, types};

impl MySql {
    pub(crate) async fn items(
        &self,
        namespace: &str,
        key: &str,
        body: &types::Items,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        let digest = operation::digest(body)?;
        let mut tx = self.pool.begin().await?;
        if operation::reserve::<()>(&mut tx, namespace, "items", key, &digest)
            .await?
            .is_some()
        {
            tx.commit().await?;
            return Ok(());
        }
        if body.items.is_empty() {
            operation::record(&mut tx, namespace, "items", key, &digest, &()).await?;
            tx.commit().await?;
            return Ok(());
        }
        if body.context.task_id.trim().is_empty() {
            return Err(Error::Invalid(
                "item submission requires task_id".to_string(),
            ));
        }
        if !body.context.id.is_empty() {
            identity(&body.context)?;
            let parent = load(&mut tx, namespace, &body.context.id).await?;
            verify_identity(&parent, &body.context)?;
            verify_lease(&parent, self.lease_timeout_ms)?;
        }
        let trace = if body.context.trace_id.is_empty() {
            None
        } else {
            let trace = trace::load(&mut tx, namespace, &body.context.trace_id)
                .await?
                .ok_or_else(|| Error::TraceNotFound(body.context.trace_id.clone()))?;
            if trace.task_id != body.context.task_id {
                return Err(Error::Invalid(
                    "task_id mismatch for item Trace Snapshot".to_string(),
                ));
            }
            Some(trace)
        };
        let (config_version, timezone) = trace.as_ref().map_or((None, None), metadata);
        let persister_id = trace.as_ref().and_then(|trace| trace.persister_id.clone());
        let now = now_millis();
        for item in &body.items {
            if !item.id.is_empty() {
                identifier(&item.id, "item id")?;
            }
            if !item.data.is_object() {
                return Err(Error::Invalid("item data must be an object".to_string()));
            }
            let item_id = if item.id.is_empty() {
                uuid::Uuid::now_v7().to_string()
            } else {
                item.id.clone()
            };
            sqlx::query(
                "INSERT INTO items \
                 (namespace, id, item_id, task_id, trace_id, request_id, persister_id, config_version, \
                  timezone, data, created_time, updated_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(namespace)
            .bind(uuid::Uuid::now_v7().to_string())
            .bind(item_id)
            .bind(&body.context.task_id)
            .bind(&body.context.trace_id)
            .bind(&body.context.id)
            .bind(&persister_id)
            .bind(&config_version)
            .bind(&timezone)
            .bind(Json(&item.data))
            .bind(now)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        }
        operation::record(&mut tx, namespace, "items", key, &digest, &()).await?;
        tx.commit().await?;
        Ok(())
    }
}

fn metadata(trace: &spider::trace::Snapshot) -> (Option<String>, Option<String>) {
    trace.dsl.as_ref().map_or((None, None), |config| {
        (
            config.spider.version.clone(),
            config.spider.timezone.clone(),
        )
    })
}
