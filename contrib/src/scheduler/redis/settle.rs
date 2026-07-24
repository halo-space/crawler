use spider::{payload, scheduler, stats};

use super::Redis;
use super::error::redis as redis_error;
use super::key;
use super::model::{self, Execution, Stat};
use super::validate;

impl Execution {
    fn new(payload: &payload::Payload) -> Result<Self, scheduler::Error> {
        let stats = payload
            .stats
            .iter()
            .map(|(name, value)| {
                serde_json::from_value::<stats::Counter>(value.clone())
                    .map_err(|error| {
                        scheduler::Error::Message(format!("invalid stats counter {name}: {error}"))
                    })
                    .and_then(|counter| {
                        if validate::counter(&counter) {
                            Ok(Stat::new(name.clone(), counter))
                        } else {
                            Err(scheduler::Error::Message(format!(
                                "invalid stats counter {name}: values must be non-negative"
                            )))
                        }
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            id: payload.id.clone(),
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            node: payload.node.clone(),
            worker_id: payload.worker_id.clone(),
            version: payload.version.to_string(),
            state: model::state(payload.state).to_string(),
            error: payload.error.clone(),
            stats,
        })
    }
}

impl Redis {
    pub(super) async fn acknowledge(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_ack()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let encoded = Self::encode(&Execution::new(payload)?)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .ack
            .prepare_invoke()
            .key(self.keys.request(&payload.id))
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn return_to_queue(
        &self,
        payload: &payload::Payload,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_release()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let encoded = Self::encode(&Execution::new(payload)?)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .release
            .prepare_invoke()
            .key(self.keys.request(&payload.id))
            .key(self.keys.meta())
            .arg(self.keys.prefix())
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .arg(key::token(&payload.id))
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn refresh(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_refresh_lease()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let encoded = Self::encode(&Execution::new(payload)?)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .refresh
            .prepare_invoke()
            .key(self.keys.request(&payload.id))
            .arg(self.keys.prefix())
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .arg(key::token(&payload.id))
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn succeed(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_success()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let encoded = Self::encode(&Execution::new(payload)?)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .success
            .prepare_invoke()
            .key(self.keys.request(&payload.id))
            .key(self.keys.completion(&payload.id, payload.version))
            .key(self.keys.stats(&payload.trace_id))
            .arg(self.keys.prefix())
            .arg(key::token(&payload.id))
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn fail(&self, payload: &payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_failure()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let encoded = Self::encode(&Execution::new(payload)?)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .failure
            .prepare_invoke()
            .key(self.keys.request(&payload.id))
            .key(self.keys.completion(&payload.id, payload.version))
            .key(self.keys.stats(&payload.trace_id))
            .key(self.keys.meta())
            .arg(self.keys.prefix())
            .arg(key::token(&payload.id))
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }
}
