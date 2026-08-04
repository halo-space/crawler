use serde::Serialize;
use spider::{net, payload, scheduler, stats};

use super::Redis;
use super::error::redis as redis_error;
use super::key;
use super::validate;

fn state(value: spider::net::State) -> &'static str {
    match value {
        spider::net::State::Pending => "pending",
        spider::net::State::Processing => "processing",
        spider::net::State::Done => "done",
        spider::net::State::Failed => "failed",
    }
}

#[derive(Serialize)]
struct Execution {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    version: String,
    state: String,
    error: Option<String>,
    stats: Vec<Stat>,
}

#[derive(Serialize)]
struct Stat {
    name: String,
    total: String,
    done: String,
    filter: String,
    dedup: String,
    validate: String,
    download: String,
}

impl Stat {
    fn new(name: String, value: stats::Counter) -> Self {
        Self {
            name,
            total: value.total.to_string(),
            done: value.done.to_string(),
            filter: value.filter.to_string(),
            dedup: value.dedup.to_string(),
            validate: value.validate.to_string(),
            download: value.download.to_string(),
        }
    }
}

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
            version: payload.version.to_string(),
            state: state(payload.state).to_string(),
            error: payload.error.clone(),
            stats,
        })
    }
}

impl Redis {
    pub(super) async fn acknowledge(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
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
            .arg(worker_id)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn return_to_queue(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
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
            .arg(key::segment(&payload.id))
            .arg(worker_id)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn refresh(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
        payload
            .validate_refresh_lease()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        self.refresh_execution(payload, worker_id, true)
            .await
            .map(|_| ())
    }

    pub(super) async fn refresh_claimed_lease(
        &self,
        request: &net::Request,
        worker_id: &str,
    ) -> Result<i64, scheduler::Error> {
        let mut payload = payload::Payload::for_request(request, worker_id);
        payload.state = net::State::Processing;
        self.refresh_execution(&payload, worker_id, false).await
    }

    async fn refresh_execution(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
        require_ack: bool,
    ) -> Result<i64, scheduler::Error> {
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
            .arg(key::segment(&payload.id))
            .arg(worker_id)
            .arg(if require_ack { 1 } else { 0 })
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        if result == "OK" {
            return Ok(0);
        }
        if let Some(lease_time) = result.strip_prefix("OK:") {
            return lease_time.parse::<i64>().map_err(|error| {
                scheduler::Error::Message(format!(
                    "Redis refresh returned an invalid lease_time for {}: {error}",
                    payload.id
                ))
            });
        }
        Self::result(result, &payload.id).map(|_| 0)
    }

    pub(super) async fn succeed(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
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
            .arg(key::segment(&payload.id))
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .arg(worker_id)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }

    pub(super) async fn fail(
        &self,
        payload: &payload::Payload,
        worker_id: &str,
    ) -> Result<(), scheduler::Error> {
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
            .arg(key::segment(&payload.id))
            .arg(encoded)
            .arg(self.lease.timeout().as_millis() as i64)
            .arg(worker_id)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &payload.id)
    }
}
