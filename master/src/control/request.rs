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
pub(crate) struct Completion {
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
    pub completion: Option<Completion>,
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
