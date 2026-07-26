use sqlx::types::Json;

use super::MySql;
use super::operation;
use super::time::now_millis;
use super::validate::{identifier, namespace as validate_namespace};
use crate::{Error, types};

impl MySql {
    pub(crate) async fn items(
        &self,
        namespace: &str,
        key: &str,
        body: &types::Items,
    ) -> Result<(), Error> {
        validate_namespace(namespace)?;
        validate(body)?;
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
        let now = now_millis();
        for item in &body.items {
            sqlx::query(
                "INSERT INTO items \
                 (namespace, id, item_id, task_id, trace_id, request_id, data, created_time, updated_time) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(namespace)
            .bind(uuid::Uuid::now_v7().to_string())
            .bind(&item.id)
            .bind(&body.context.task_id)
            .bind(&body.context.trace_id)
            .bind(&body.context.id)
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

pub(super) fn validate(body: &types::Items) -> Result<(), Error> {
    body.validate_store()?;
    if body.items.is_empty() {
        return Ok(());
    }
    identifier(&body.context.task_id, "item payload task_id")?;
    identifier(&body.context.trace_id, "item payload trace_id")?;
    if !body.context.id.is_empty() {
        identifier(&body.context.id, "item payload request_id")?;
    }
    for item in &body.items {
        identifier(&item.id, "item id")?;
        if !item.data.is_object() {
            return Err(Error::Invalid("item data must be an object".to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::Config;

    fn submission() -> types::Items {
        types::Items {
            context: types::Identity {
                id: "request-1".to_string(),
                task_id: "task-1".to_string(),
                trace_id: "trace-1".to_string(),
                version: 0,
                worker_id: String::new(),
                node: String::new(),
            },
            items: vec![types::item::Item {
                id: "item-1".to_string(),
                data: json!({"title": "valid"}),
            }],
        }
    }

    #[tokio::test]
    async fn submission_is_validated_before_database_access() {
        let config = Config::new(
            "127.0.0.1:0".parse().unwrap(),
            "mysql://crawler",
            "crawler",
            "worker-secret",
            "control-secret",
        )
        .unwrap();
        let store = MySql::disconnected(&config);
        let mut body = submission();
        body.items.push(types::item::Item {
            id: String::new(),
            data: json!({"title": "invalid"}),
        });

        assert!(matches!(
            store.items("crawler", "items-1", &body).await,
            Err(Error::Invalid(message))
                if message == "every Item requires a non-empty framework Item ID"
        ));
    }

    #[test]
    fn context_validates_only_persisted_identifiers() {
        let body = submission();
        validate(&body).unwrap();

        let mut invalid_task = body.clone();
        invalid_task.context.task_id = "task\n".to_string();
        assert!(validate(&invalid_task).is_err());

        let mut invalid_trace = body.clone();
        invalid_trace.context.trace_id = "trace\n".to_string();
        assert!(validate(&invalid_trace).is_err());

        let mut missing_trace = body.clone();
        missing_trace.context.trace_id.clear();
        assert!(matches!(
            validate(&missing_trace),
            Err(Error::Invalid(message)) if message == "item payload requires trace id"
        ));

        let mut invalid_request = body;
        invalid_request.context.id = "request\n".to_string();
        assert!(validate(&invalid_request).is_err());
    }
}
