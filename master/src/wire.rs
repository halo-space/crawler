use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub version: i64,
    pub worker_id: String,
    pub node: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Init {
    pub trace_id: String,
    pub trace: spider::trace::Snapshot,
    pub requests: Vec<spider::net::request::Snapshot>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Push {
    pub context: Identity,
    pub requests: Vec<spider::net::request::Snapshot>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Items {
    pub context: Identity,
    pub items: Vec<Item>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Item {
    pub id: String,
    pub data: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Worker {
    pub worker_id: String,
    pub modes: Vec<spider::net::Mode>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claim {
    pub limit: usize,
    pub worker_id: String,
    pub modes: Vec<spider::net::Mode>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claims {
    pub requests: Vec<Claimed>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claimed {
    pub snapshot: spider::net::request::Snapshot,
    pub execution: Execution,
    pub trace: Option<spider::trace::Snapshot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Execution {
    pub version: i64,
    pub next_time: i64,
    pub leased_by: String,
    pub lease_time: i64,
    pub retry_count: i32,
    pub failed_workers: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Pending {
    pub pending: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Completion {
    pub identity: Identity,
    #[serde(default)]
    pub stats: HashMap<String, Value>,
    pub start_time: i64,
    pub end_time: i64,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Heartbeat {
    pub worker_id: String,
    pub modes: Vec<spider::net::Mode>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Policy {
    pub lease_timeout_ms: i64,
    pub lease_interval_ms: i64,
    pub heartbeat_interval_ms: i64,
    pub max_response_bytes: u64,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn identity_round_trips_with_the_fixed_wire_shape() {
        let identity = Identity {
            id: "request-1".to_string(),
            task_id: "task-1".to_string(),
            trace_id: "trace-1".to_string(),
            version: 2,
            worker_id: "worker-1".to_string(),
            node: "detail".to_string(),
        };

        let value = serde_json::to_value(&identity).unwrap();
        assert_eq!(
            value,
            json!({
                "id": "request-1",
                "task_id": "task-1",
                "trace_id": "trace-1",
                "version": 2,
                "worker_id": "worker-1",
                "node": "detail"
            })
        );
        assert_eq!(serde_json::from_value::<Identity>(value).unwrap(), identity);
    }

    #[test]
    fn wire_records_reject_unknown_fields() {
        let result = serde_json::from_value::<Worker>(json!({
            "worker_id": "worker-1",
            "modes": ["http"],
            "unknown": true
        }));

        assert!(result.is_err());
    }
}
