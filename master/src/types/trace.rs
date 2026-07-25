use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub id: String,
    pub task_id: String,
    pub priority: i32,
    pub start_time: Option<i64>,
    pub created_time: i64,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct Counts {
    pub pending: u64,
    pub processing: u64,
    pub done: u64,
    pub failed: u64,
}

#[derive(Serialize)]
pub(crate) struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    pub snapshot: Value,
    pub stats: HashMap<String, spider::stats::Counter>,
    pub requests: Counts,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct List {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub task_id: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Filter<'a> {
    task_id: Option<&'a str>,
}

impl List {
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        super::limit(self.limit)
    }

    pub(crate) fn filter(&self) -> Filter<'_> {
        Filter {
            task_id: self.task_id.as_deref(),
        }
    }
}
