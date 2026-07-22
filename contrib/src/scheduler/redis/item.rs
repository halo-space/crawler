use spider::{payload, scheduler};

use super::Redis;
use super::error::{message, redis as redis_error};
use super::model::{Item, Items};

impl Redis {
    pub(super) async fn write_items(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_items()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        if payload.items.is_empty() {
            return Ok(());
        }

        let records = payload
            .items
            .iter()
            .map(|item| {
                serde_json::to_value(item.as_ref())
                    .map(|data| Item {
                        id: item.id().to_string(),
                        data,
                    })
                    .map_err(message)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let (config_version, timezone) = self.item_metadata(&payload.trace_id).await?;
        let record = Items {
            id: payload.id.clone(),
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            version: payload.version,
            worker_id: payload.worker_id.clone(),
            node: payload.node.clone(),
            config_version,
            timezone,
            records,
        };
        let encoded = Self::encode(&record)?;
        let mut connection = self.connection().await?;
        let _: String = self
            .scripts
            .items
            .prepare_invoke()
            .key(self.keys.items())
            .arg(encoded)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(())
    }

    async fn item_metadata(
        &self,
        trace_id: &str,
    ) -> Result<(Option<String>, Option<String>), scheduler::Error> {
        if trace_id.is_empty() {
            return Ok((None, None));
        }
        let snapshot = self
            .load_trace(trace_id)
            .await?
            .ok_or_else(|| scheduler::Error::TraceNotFound(trace_id.to_string()))?;
        let Some(dsl) = snapshot.dsl else {
            return Ok((None, None));
        };
        Ok((dsl.spider.version, dsl.spider.timezone))
    }
}
