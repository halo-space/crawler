use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub id: String,
    pub task_id: String,
    pub trace_id: String,
    pub node: String,
    pub mode: spider::net::Mode,
    pub state: spider::net::State,
    pub version: i64,
    pub priority: i32,
    pub next_time: i64,
    pub leased_by: String,
    pub lease_time: i64,
    pub retry_count: i32,
    pub max_retry_count: i32,
    pub created_time: i64,
    pub updated_time: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct CompletionInfo {
    pub version: i64,
    pub worker_id: String,
    pub state: spider::net::State,
    pub error: Option<String>,
    pub created_time: i64,
}

#[derive(Serialize)]
pub(crate) struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    pub snapshot: Value,
    pub failed_workers: Vec<String>,
    pub ack_version: Option<i64>,
    pub completion: Option<CompletionInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct List {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub trace_id: Option<String>,
    pub state: Option<spider::net::State>,
    pub worker_id: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Filter<'a> {
    trace_id: Option<&'a str>,
    state: Option<spider::net::State>,
    worker_id: Option<&'a str>,
}

impl List {
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        super::limit(self.limit)
    }

    pub(crate) fn filter(&self) -> Filter<'_> {
        Filter {
            trace_id: self.trace_id.as_deref(),
            state: self.state,
            worker_id: self.worker_id.as_deref(),
        }
    }
}

pub(crate) fn state(value: i8) -> Result<spider::net::State, Error> {
    match value {
        0 => Ok(spider::net::State::Pending),
        1 => Ok(spider::net::State::Processing),
        2 => Ok(spider::net::State::Done),
        3 => Ok(spider::net::State::Failed),
        _ => Err(Error::Invalid(format!(
            "invalid stored Request state: {value}"
        ))),
    }
}

pub(crate) fn state_code(value: spider::net::State) -> i8 {
    match value {
        spider::net::State::Pending => 0,
        spider::net::State::Processing => 1,
        spider::net::State::Done => 2,
        spider::net::State::Failed => 3,
    }
}

pub(crate) fn mode(value: &str) -> Result<spider::net::Mode, Error> {
    match value {
        "http" => Ok(spider::net::Mode::Http),
        "browser" => Ok(spider::net::Mode::Browser),
        _ => Err(Error::Invalid(format!(
            "invalid stored Request mode: {value}"
        ))),
    }
}

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
pub(crate) struct Push {
    pub context: Identity,
    pub requests: Vec<spider::net::request::Snapshot>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
#[serde(deny_unknown_fields)]
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

#[cfg(test)]
mod transport_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn identity_round_trips_with_the_fixed_transport_shape() {
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
}
