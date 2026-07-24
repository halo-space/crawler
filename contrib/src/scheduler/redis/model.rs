use serde::{Deserialize, Serialize};

use spider::{net, stats};

pub(super) const HTTP: &str = "http";
pub(super) const BROWSER: &str = "browser";

pub(super) fn mode(value: &net::Mode) -> &'static str {
    match value {
        net::Mode::Http => HTTP,
        net::Mode::Browser => BROWSER,
    }
}

pub(super) fn parse_mode(value: &str) -> Result<net::Mode, spider::scheduler::Error> {
    match value {
        HTTP => Ok(net::Mode::Http),
        BROWSER => Ok(net::Mode::Browser),
        value => Err(spider::scheduler::Error::Message(format!(
            "stored Request has invalid mode: {value}"
        ))),
    }
}

pub(super) fn state(value: net::State) -> &'static str {
    match value {
        net::State::Pending => "pending",
        net::State::Processing => "processing",
        net::State::Done => "done",
        net::State::Failed => "failed",
    }
}

#[derive(Serialize)]
pub(super) struct Queued {
    pub(super) token: String,
    pub(super) id: String,
    pub(super) task_id: String,
    pub(super) trace_id: String,
    pub(super) node: String,
    pub(super) mode: String,
    pub(super) priority: i32,
    pub(super) next_time: String,
    pub(super) version: String,
    pub(super) retry_count: i32,
    pub(super) max_retry_count: i32,
    pub(super) snapshot: String,
    pub(super) digest: String,
}

#[derive(Deserialize)]
pub(super) struct Claimed {
    pub(super) id: String,
    pub(super) task_id: String,
    pub(super) trace_id: String,
    pub(super) node: String,
    pub(super) mode: String,
    pub(super) priority: i32,
    pub(super) next_time: String,
    pub(super) version: String,
    pub(super) retry_count: i32,
    pub(super) max_retry_count: i32,
    pub(super) leased_by: String,
    pub(super) lease_time: String,
    pub(super) snapshot: String,
    pub(super) digest: String,
    pub(super) trace: Option<String>,
    #[serde(default)]
    pub(super) failed_workers: Vec<String>,
}

#[derive(Serialize)]
pub(super) struct Execution {
    pub(super) id: String,
    pub(super) task_id: String,
    pub(super) trace_id: String,
    pub(super) node: String,
    pub(super) worker_id: String,
    pub(super) version: String,
    pub(super) state: String,
    pub(super) error: Option<String>,
    pub(super) stats: Vec<Stat>,
}

#[derive(Serialize)]
pub(super) struct Stat {
    pub(super) name: String,
    pub(super) total: String,
    pub(super) done: String,
    pub(super) filter: String,
    pub(super) dedup: String,
    pub(super) validate: String,
    pub(super) download: String,
}

impl Stat {
    pub(super) fn new(name: String, value: stats::Counter) -> Self {
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

#[derive(Serialize)]
pub(super) struct Items {
    pub(super) id: String,
    pub(super) task_id: String,
    pub(super) trace_id: String,
    pub(super) version: i64,
    pub(super) worker_id: String,
    pub(super) node: String,
    pub(super) config_version: Option<String>,
    pub(super) timezone: Option<String>,
    pub(super) records: Vec<Item>,
}

#[derive(Serialize)]
pub(super) struct Item {
    pub(super) id: String,
    pub(super) data: serde_json::Value,
}
