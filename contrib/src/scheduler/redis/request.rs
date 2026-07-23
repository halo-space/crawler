use std::sync::Arc;

use redis::AsyncCommands as _;
use serde_json::Value;
use spider::{net, payload, scheduler, trace};

use super::Redis;
use super::error::{message, redis as redis_error};
use super::key;
use super::model::{self, Claimed, Queued};
use super::validate;

impl Redis {
    pub(super) fn stored(request: net::Request) -> Result<Queued, scheduler::Error> {
        validate::request(&request)?;
        let snapshot =
            net::request::Snapshot::try_from(request).map_err(scheduler::Error::Message)?;
        let digest = snapshot
            .digest()
            .map_err(|error| scheduler::Error::Message(error.to_string()))?;
        let id = snapshot.id.clone();
        Ok(Queued {
            token: key::token(&id),
            id,
            task_id: snapshot.task_id.clone(),
            trace_id: snapshot.trace_id.clone(),
            node: snapshot.node.clone(),
            mode: model::mode(&snapshot.mode).to_string(),
            priority: snapshot.priority,
            next_time: snapshot.next_time.to_string(),
            version: snapshot.version.to_string(),
            retry_count: snapshot.retry_count,
            max_retry_count: snapshot.max_retry_count,
            snapshot: serde_json::to_string(&snapshot).map_err(message)?,
            digest: validate::hex(&digest),
        })
    }

    pub(super) async fn enqueue(&self, payload: payload::Payload) -> Result<(), scheduler::Error> {
        payload
            .validate_push()
            .map_err(|message| scheduler::Error::Message(message.to_string()))?;
        let requests = payload
            .requests
            .into_iter()
            .map(Self::stored)
            .collect::<Result<Vec<_>, _>>()?;
        let encoded = Self::encode(&requests)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .push
            .prepare_invoke()
            .key(self.keys.meta())
            .key(self.keys.traces())
            .key(self.keys.trace_tasks())
            .arg(self.keys.prefix())
            .arg(encoded)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, "")
    }

    pub(super) async fn load_trace(
        &self,
        trace_id: &str,
    ) -> Result<Option<trace::Snapshot>, scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        let mut connection = self.connection().await?;
        let stored: Option<String> = connection
            .hget(self.keys.traces(), trace_id)
            .await
            .map_err(redis_error)?;
        stored
            .map(|encoded| {
                serde_json::from_str::<trace::Snapshot>(&encoded)
                    .map_err(|error| scheduler::Error::InvalidTrace {
                        id: trace_id.to_string(),
                        message: error.to_string(),
                    })
                    .and_then(|snapshot| {
                        snapshot
                            .validate()
                            .map_err(|message| scheduler::Error::InvalidTrace {
                                id: trace_id.to_string(),
                                message,
                            })?;
                        Ok(snapshot)
                    })
            })
            .transpose()
    }

    pub(super) async fn claim(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        validate::worker(worker_id, modes)?;
        let modes = modes.iter().map(model::mode).collect::<Vec<_>>();
        let modes = Self::encode(&modes)?;
        let mut connection = self.connection().await?;
        let encoded: Vec<String> = self
            .scripts
            .claim
            .prepare_invoke()
            .key(self.keys.meta())
            .key(self.keys.leases())
            .key(self.keys.traces())
            .arg(self.keys.prefix())
            .arg(limit)
            .arg(worker_id)
            .arg(self.lease.timeout().as_millis() as i64)
            .arg(modes)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        drop(connection);

        let mut requests = Vec::with_capacity(encoded.len());
        for encoded in encoded {
            let value = serde_json::from_str::<Value>(&encoded).map_err(|error| {
                scheduler::Error::InvalidRequest {
                    id: "unknown".to_string(),
                    message: format!("claimed Redis Request cannot be decoded: {error}"),
                }
            })?;
            let token = claim_field(&value, "token")?;
            let version = claim_field(&value, "version")?;
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            match serde_json::from_value::<Claimed>(value) {
                Ok(claimed) if key::token(&claimed.id) == token => match Self::restore(&claimed) {
                    Ok(request) => requests.push(request),
                    Err(error) => {
                        self.recover(&token, worker_id, &version, &id, &error.to_string())
                            .await?
                    }
                },
                Ok(_) => {
                    let error = scheduler::Error::InvalidRequest {
                        id: id.clone(),
                        message: "claimed Redis Request id does not match its queue token"
                            .to_string(),
                    };
                    self.recover(&token, worker_id, &version, &id, &error.to_string())
                        .await?;
                }
                Err(error) => {
                    let error = scheduler::Error::InvalidRequest {
                        id: id.clone(),
                        message: format!("claimed Redis Request cannot be decoded: {error}"),
                    };
                    self.recover(&token, worker_id, &version, &id, &error.to_string())
                        .await?;
                }
            }
        }
        Ok(requests)
    }

    pub(super) async fn pending(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        validate::worker(worker_id, modes)?;
        let modes = modes.iter().map(model::mode).collect::<Vec<_>>();
        let modes = Self::encode(&modes)?;
        let mut connection = self.connection().await?;
        let pending: i64 = self
            .scripts
            .pending
            .prepare_invoke()
            .arg(self.keys.prefix())
            .arg(modes)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Ok(pending != 0)
    }

    pub(super) async fn initialize(
        &self,
        trace_id: String,
        snapshot: trace::Snapshot,
        requests: Vec<net::Request>,
    ) -> Result<(), scheduler::Error> {
        if trace_id.is_empty() {
            return Err(scheduler::Error::Message(
                "trace_id must not be empty".to_string(),
            ));
        }
        snapshot
            .validate()
            .map_err(|message| scheduler::Error::InvalidTrace {
                id: trace_id.clone(),
                message,
            })?;
        if requests.iter().any(|request| request.trace_id != trace_id) {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the initialized trace_id".to_string(),
            ));
        }
        if requests
            .iter()
            .any(|request| request.task_id != snapshot.task_id)
        {
            return Err(scheduler::Error::Message(
                "all initial requests must reference the Trace Snapshot task_id".to_string(),
            ));
        }

        let requests = requests
            .into_iter()
            .map(Self::stored)
            .collect::<Result<Vec<_>, _>>()?;
        let encoded_trace = Self::encode(&snapshot)?;
        let encoded_requests = Self::encode(&requests)?;
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .init
            .prepare_invoke()
            .key(self.keys.meta())
            .key(self.keys.traces())
            .key(self.keys.trace_tasks())
            .arg(self.keys.prefix())
            .arg(&trace_id)
            .arg(&snapshot.task_id)
            .arg(encoded_trace)
            .arg(encoded_requests)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, &trace_id)
    }

    fn restore(claimed: &Claimed) -> Result<net::Request, scheduler::Error> {
        let version = validate::integer(&claimed.version, &claimed.id, "version")?;
        let next_time = validate::integer(&claimed.next_time, &claimed.id, "next_time")?;
        let lease_time = validate::integer(&claimed.lease_time, &claimed.id, "lease_time")?;
        let mut snapshot = serde_json::from_str::<net::request::Snapshot>(&claimed.snapshot)
            .map_err(|error| scheduler::Error::InvalidRequest {
                id: claimed.id.clone(),
                message: error.to_string(),
            })?;
        let mode = model::parse_mode(&claimed.mode)?;
        for (field, matches) in [
            ("id", snapshot.id == claimed.id),
            ("task_id", snapshot.task_id == claimed.task_id),
            ("trace_id", snapshot.trace_id == claimed.trace_id),
            ("node", snapshot.node == claimed.node),
            ("mode", snapshot.mode == mode),
            ("priority", snapshot.priority == claimed.priority),
        ] {
            if !matches {
                return Err(scheduler::Error::InvalidRequest {
                    id: claimed.id.clone(),
                    message: format!("claimed Redis Request {field} does not match its Snapshot"),
                });
            }
        }
        snapshot.version = version;
        snapshot.next_time = next_time;
        snapshot.retry_count = claimed.retry_count;
        snapshot.max_retry_count = claimed.max_retry_count;
        snapshot.failed_workers = claimed.failed_workers.clone();
        snapshot.state = net::State::Pending;
        snapshot.leased_by.clear();
        snapshot.lease_time = 0;

        let trace = claimed
            .trace
            .as_deref()
            .map(|encoded| {
                serde_json::from_str::<trace::Snapshot>(encoded)
                    .map_err(|error| scheduler::Error::InvalidTrace {
                        id: claimed.trace_id.clone(),
                        message: error.to_string(),
                    })
                    .and_then(|snapshot| {
                        snapshot
                            .validate()
                            .map_err(|message| scheduler::Error::InvalidTrace {
                                id: claimed.trace_id.clone(),
                                message,
                            })?;
                        Ok(Arc::new(snapshot))
                    })
            })
            .transpose()?;
        let mut request =
            snapshot
                .restore(trace)
                .map_err(|message| scheduler::Error::InvalidRequest {
                    id: claimed.id.clone(),
                    message,
                })?;
        request.state = net::State::Processing;
        request.leased_by = claimed.leased_by.clone();
        request.lease_time = lease_time;
        request.version = version;
        request.mode = mode;
        request.retry_count = claimed.retry_count;
        request.max_retry_count = claimed.max_retry_count;
        request.failed_workers = claimed.failed_workers.clone();
        Ok(request)
    }

    async fn recover(
        &self,
        token: &str,
        worker_id: &str,
        version: &str,
        id: &str,
        reason: &str,
    ) -> Result<(), scheduler::Error> {
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .recover
            .prepare_invoke()
            .key(self.keys.request_token(token))
            .key(self.keys.leases())
            .key(self.keys.meta())
            .arg(self.keys.prefix())
            .arg(token)
            .arg(worker_id)
            .arg(version)
            .arg(reason)
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, id)
    }
}

fn claim_field(value: &Value, field: &str) -> Result<String, scheduler::Error> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .ok_or_else(|| scheduler::Error::InvalidRequest {
            id: "unknown".to_string(),
            message: format!("claimed Redis Request has no valid {field}"),
        })
}
