use serde::{Deserialize, Serialize};

use crate::Error;

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub id: String,
    pub modes: Vec<spider::net::Mode>,
    pub last_heartbeat: i64,
    pub online: bool,
    pub created_time: i64,
    pub updated_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct List {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub mode: Option<spider::net::Mode>,
    pub online: Option<bool>,
}

#[derive(Serialize)]
pub(crate) struct Filter {
    mode: Option<spider::net::Mode>,
    online: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Worker {
    pub worker_id: String,
    pub modes: Vec<spider::net::Mode>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Heartbeat {
    pub worker_id: String,
    pub modes: Vec<spider::net::Mode>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Policy {
    pub lease_timeout_ms: i64,
    pub lease_interval_ms: i64,
    pub heartbeat_interval_ms: i64,
    pub max_response_bytes: u64,
}

impl List {
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        super::limit(self.limit)
    }

    pub(crate) fn filter(&self) -> Filter {
        Filter {
            mode: self.mode.clone(),
            online: self.online,
        }
    }
}
