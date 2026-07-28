use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use spider::{net, payload, scheduler, trace};

use super::Redis;
use super::error::{message, redis as redis_error};
use super::key;
use super::validate;

const HTTP: &str = "http";
const BROWSER: &str = "browser";

#[derive(Clone, Default, Eq, PartialEq)]
struct Position {
    revision: String,
    member: String,
}

#[derive(Clone, Default, Eq, PartialEq)]
pub(super) struct Cursor {
    http: Position,
    browser: Position,
}

impl Cursor {
    fn has_member(&self) -> bool {
        !self.http.member.is_empty() || !self.browser.member.is_empty()
    }
}

fn mode(value: &net::Mode) -> &'static str {
    match value {
        net::Mode::Http => HTTP,
        net::Mode::Browser => BROWSER,
    }
}

fn parse_mode(value: &str) -> Result<net::Mode, scheduler::Error> {
    match value {
        HTTP => Ok(net::Mode::Http),
        BROWSER => Ok(net::Mode::Browser),
        value => Err(scheduler::Error::Message(format!(
            "stored Request has invalid mode: {value}"
        ))),
    }
}

#[derive(Serialize)]
struct Queued {
    token: String,
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    priority: i32,
    next_time: String,
    version: String,
    retry_count: i32,
    max_retry_count: i32,
    snapshot: String,
    digest: String,
}

#[derive(Deserialize)]
struct Claimed {
    id: String,
    task_id: String,
    trace_id: String,
    node: String,
    mode: String,
    priority: i32,
    next_time: String,
    version: String,
    retry_count: i32,
    max_retry_count: i32,
    leased_by: String,
    lease_time: String,
    snapshot: String,
    digest: String,
    trace: Option<String>,
    #[serde(default)]
    failed_workers: Vec<String>,
}

struct Recovery {
    token: String,
    version: String,
    id: String,
    message: String,
    max_retry_count: Option<i32>,
}

impl Redis {
    fn stored(request: net::Request) -> Result<Queued, scheduler::Error> {
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
            mode: mode(&snapshot.mode).to_string(),
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
        payload.validate_push().map_err(scheduler::Error::Message)?;
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

    pub(super) async fn claim(
        &self,
        limit: usize,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<Vec<net::Request>, scheduler::Error> {
        validate::worker(worker_id, modes)?;
        let modes = modes.iter().map(mode).collect::<Vec<_>>();
        let modes = Self::encode(&modes)?;
        let mut connection = self.connection().await?;
        // These cursors bound excluded-Request scans without writing Worker-local state to Redis.
        let mut cursors = self.claim_cursors.lock().await;
        let mut position = cursors.get(worker_id).cloned().unwrap_or_default();
        let encoded = loop {
            let (
                http_revision,
                http_member,
                browser_revision,
                browser_member,
                scan_complete,
                encoded,
            ): (String, String, String, String, i64, Vec<String>) = self
                .scripts
                .claim
                .prepare_invoke()
                .key(self.keys.meta())
                .key(self.keys.traces())
                .arg(self.keys.prefix())
                .arg(limit)
                .arg(worker_id)
                .arg(self.lease.timeout().as_millis() as i64)
                .arg(&modes)
                .arg(&position.http.revision)
                .arg(&position.http.member)
                .arg(&position.browser.revision)
                .arg(&position.browser.member)
                .invoke_async(&mut connection)
                .await
                .map_err(redis_error)?;
            let next = Cursor {
                http: Position {
                    revision: http_revision,
                    member: http_member,
                },
                browser: Position {
                    revision: browser_revision,
                    member: browser_member,
                },
            };
            if scan_complete != 0 || !encoded.is_empty() {
                position = next;
                break encoded;
            }
            if next == position {
                return Err(scheduler::Error::Message(
                    "Redis ready-event cursor did not advance".to_string(),
                ));
            }
            position = next;
        };
        if position.has_member() {
            cursors.insert(worker_id.to_string(), position);
        } else {
            cursors.remove(worker_id);
        }
        drop(cursors);
        drop(connection);

        let mut requests = Vec::with_capacity(encoded.len());
        let mut recoveries = Vec::new();
        let mut recovery_error = None;
        for encoded in encoded {
            let value = match serde_json::from_str::<Value>(&encoded) {
                Ok(value) => value,
                Err(error) => {
                    let error = scheduler::Error::InvalidRequest {
                        id: "unknown".to_string(),
                        message: format!("claimed Redis Request cannot be decoded: {error}"),
                    };
                    warn_recovery(worker_id, "unknown", "unknown", "unknown", &error);
                    recovery_error.get_or_insert(error);
                    continue;
                }
            };
            let id = value
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .unwrap_or("unknown")
                .to_string();
            let token = match claim_field(&value, "token") {
                Ok(token) => token,
                Err(error) => {
                    let version = value
                        .get("version")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    warn_recovery(worker_id, &id, "unknown", version, &error);
                    recovery_error.get_or_insert(error);
                    continue;
                }
            };
            let version = match claim_field(&value, "version") {
                Ok(version) => version,
                Err(error) => {
                    warn_recovery(worker_id, &id, &token, "unknown", &error);
                    recovery_error.get_or_insert(error);
                    continue;
                }
            };
            match serde_json::from_value::<Claimed>(value) {
                Ok(claimed) if key::token(&claimed.id) == token => match Self::restore(&claimed) {
                    Ok(request) => requests.push(request),
                    Err(error) => recoveries.push(Recovery {
                        token,
                        version,
                        id,
                        message: error.to_string(),
                        max_retry_count: snapshot_retry_limit(&claimed),
                    }),
                },
                Ok(_) => recoveries.push(Recovery {
                    token,
                    version,
                    id: id.clone(),
                    message: scheduler::Error::InvalidRequest {
                        id,
                        message: "claimed Redis Request id does not match its queue token"
                            .to_string(),
                    }
                    .to_string(),
                    max_retry_count: None,
                }),
                Err(error) => recoveries.push(Recovery {
                    token,
                    version,
                    id: id.clone(),
                    message: scheduler::Error::InvalidRequest {
                        id,
                        message: format!("claimed Redis Request cannot be decoded: {error}"),
                    }
                    .to_string(),
                    max_retry_count: None,
                }),
            }
        }

        for recovery in recoveries {
            if let Err(error) = self
                .recover(
                    &recovery.token,
                    worker_id,
                    &recovery.version,
                    &recovery.id,
                    &recovery.message,
                    recovery.max_retry_count,
                )
                .await
            {
                warn_recovery(
                    worker_id,
                    &recovery.id,
                    &recovery.token,
                    &recovery.version,
                    &error,
                );
                recovery_error.get_or_insert(error);
            }
        }

        // Valid Requests from the same atomic claim remain executable even when
        // a damaged peer cannot be recovered. Its lease remains visible for the
        // normal timeout path.
        if requests.is_empty()
            && let Some(error) = recovery_error
        {
            return Err(error);
        }
        Ok(requests)
    }

    pub(super) async fn pending(
        &self,
        worker_id: &str,
        modes: &[net::Mode],
    ) -> Result<bool, scheduler::Error> {
        validate::worker(worker_id, modes)?;
        let modes = modes.iter().map(mode).collect::<Vec<_>>();
        let modes = Self::encode(&modes)?;
        let mut connection = self.connection().await?;
        let pending: i64 = self
            .scripts
            .pending
            .prepare_invoke()
            .arg(self.keys.prefix())
            .arg(worker_id)
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
        let payload = payload::Payload::new().requests(requests);
        payload.validate_push().map_err(scheduler::Error::Message)?;
        let requests = payload.requests;

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
        validate::snapshot_digest(&snapshot, &claimed.digest, &claimed.id)?;
        let mode = parse_mode(&claimed.mode)?;
        for (field, matches) in [
            ("id", snapshot.id == claimed.id),
            ("task_id", snapshot.task_id == claimed.task_id),
            ("trace_id", snapshot.trace_id == claimed.trace_id),
            ("node", snapshot.node == claimed.node),
            ("mode", snapshot.mode == mode),
            ("priority", snapshot.priority == claimed.priority),
            (
                "max_retry_count",
                snapshot.max_retry_count == claimed.max_retry_count,
            ),
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
        max_retry_count: Option<i32>,
    ) -> Result<(), scheduler::Error> {
        let mut connection = self.connection().await?;
        let result: String = self
            .scripts
            .recover
            .prepare_invoke()
            .key(self.keys.request_token(token))
            .key(self.keys.meta())
            .arg(self.keys.prefix())
            .arg(token)
            .arg(worker_id)
            .arg(version)
            .arg(reason)
            .arg(
                max_retry_count
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
            )
            .invoke_async(&mut connection)
            .await
            .map_err(redis_error)?;
        Self::result(result, id)
    }
}

fn snapshot_retry_limit(claimed: &Claimed) -> Option<i32> {
    let snapshot = serde_json::from_str::<net::request::Snapshot>(&claimed.snapshot).ok()?;
    let actual = snapshot.digest().ok()?;
    let matches = validate::hex(&actual) == claimed.digest
        && snapshot.id == claimed.id
        && snapshot.task_id == claimed.task_id
        && snapshot.trace_id == claimed.trace_id
        && snapshot.node == claimed.node
        && mode(&snapshot.mode) == claimed.mode
        && snapshot.priority == claimed.priority
        && (1..=net::request::MAX_RETRY_COUNT).contains(&snapshot.max_retry_count);
    matches.then_some(snapshot.max_retry_count)
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

fn warn_recovery(
    worker_id: &str,
    id: &str,
    token: &str,
    version: &str,
    error: &dyn std::fmt::Display,
) {
    tracing::warn!(
        request_id = %id,
        token = %token,
        version = %version,
        worker_id = %worker_id,
        error = %error,
        "failed to recover damaged Redis Request"
    );
}
