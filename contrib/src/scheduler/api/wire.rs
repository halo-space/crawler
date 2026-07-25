use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use spider::{net, trace};

#[derive(Clone, Debug, Serialize)]
pub(super) struct Context {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub version: i64,
    pub worker_id: String,
    pub node: String,
}

impl Context {
    pub(super) fn from_payload(payload: &spider::payload::Payload) -> Self {
        Self {
            id: payload.id.clone(),
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            version: payload.version,
            worker_id: payload.worker_id.clone(),
            node: payload.node.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct Init {
    pub trace_id: String,
    pub trace: trace::Snapshot,
    pub requests: Vec<net::request::Snapshot>,
}

#[derive(Debug, Serialize)]
pub(super) struct Push {
    pub context: Context,
    pub requests: Vec<net::request::Snapshot>,
}

#[derive(Debug, Serialize)]
pub(super) struct Items {
    pub context: Context,
    pub items: Vec<Record>,
}

#[derive(Debug, Serialize)]
pub(super) struct Record {
    pub id: String,
    pub data: Value,
}

#[derive(Debug, Serialize)]
pub(super) struct Worker {
    pub worker_id: String,
    pub modes: Vec<net::Mode>,
}

#[derive(Debug, Serialize)]
pub(super) struct Claim {
    pub limit: usize,
    pub worker_id: String,
    pub modes: Vec<net::Mode>,
}

#[derive(Debug, Serialize)]
pub(super) struct Pending {
    pub worker_id: String,
    pub modes: Vec<net::Mode>,
}

#[derive(Debug, Serialize)]
pub(super) struct Identity {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub version: i64,
    pub worker_id: String,
    pub node: String,
}

impl Identity {
    pub(super) fn from_payload(payload: &spider::payload::Payload) -> Self {
        Self {
            id: payload.id.clone(),
            task_id: payload.task_id.clone(),
            trace_id: payload.trace_id.clone(),
            version: payload.version,
            worker_id: payload.worker_id.clone(),
            node: payload.node.clone(),
        }
    }

    pub(super) fn from_claimed(claimed: &Claimed) -> Self {
        Self {
            id: claimed.snapshot.id.clone(),
            task_id: claimed.snapshot.task_id.clone(),
            trace_id: claimed.snapshot.trace_id.clone(),
            version: claimed.execution.version,
            worker_id: claimed.execution.leased_by.clone(),
            node: claimed.snapshot.node.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(super) struct Success {
    pub identity: Identity,
    pub stats: HashMap<String, Value>,
    pub start_time: i64,
    pub end_time: i64,
}

#[derive(Debug, Serialize)]
pub(super) struct Failure {
    pub identity: Identity,
    pub error: String,
    pub stats: HashMap<String, Value>,
    pub start_time: i64,
    pub end_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Policy {
    pub lease_timeout_ms: u64,
    pub lease_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_response_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ClaimResponse {
    pub requests: Vec<Claimed>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Claimed {
    pub snapshot: net::request::Snapshot,
    pub execution: Execution,
    pub trace: Option<trace::Snapshot>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Execution {
    pub version: i64,
    pub next_time: i64,
    pub leased_by: String,
    pub lease_time: i64,
    pub retry_count: i32,
    pub failed_workers: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PendingResponse {
    pub pending: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ErrorEnvelope {
    pub error: ErrorBody,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ErrorBody {
    pub code: String,
    pub id: Option<String>,
    pub field: Option<String>,
    pub message: String,
}

pub(super) fn canonical_digest<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    canonical_fingerprint(value).map(|(digest, _)| digest)
}

pub(super) fn canonical_fingerprint<T: Serialize>(
    value: &T,
) -> Result<(String, usize), serde_json::Error> {
    let mut value = serde_json::to_value(value)?;
    canonicalize(&mut value);
    let bytes = serde_json::to_vec(&value)?;
    let size = bytes.len();
    use sha2::{Digest, Sha256};
    Ok((hex(&Sha256::digest(bytes)), size))
}

fn canonicalize(value: &mut Value) {
    match value {
        Value::Object(values) => {
            for value in values.values_mut() {
                canonicalize(value);
            }
            let mut fields = std::mem::take(values).into_iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            values.extend(fields);
        }
        Value::Array(values) => values.iter_mut().for_each(canonicalize),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn hex(bytes: &[u8]) -> String {
    const TABLE: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(TABLE[(byte >> 4) as usize] as char);
        value.push(TABLE[(byte & 0x0f) as usize] as char);
    }
    value
}
