use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub id: String,
    pub item_id: String,
    pub task_id: String,
    pub trace_id: String,
    pub request_id: String,
    pub persister_id: Option<String>,
    pub config_version: Option<String>,
    pub timezone: Option<String>,
    pub created_time: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    pub data: Value,
    pub updated_time: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct List {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub trace_id: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Filter<'a> {
    trace_id: Option<&'a str>,
    request_id: Option<&'a str>,
}

impl List {
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        super::limit(self.limit)
    }

    pub(crate) fn filter(&self) -> Result<Filter<'_>, Error> {
        if self.trace_id.is_some() && self.request_id.is_some() {
            return Err(Error::Invalid(
                "items accepts either trace_id or request_id, not both".to_string(),
            ));
        }
        Ok(Filter {
            trace_id: self.trace_id.as_deref(),
            request_id: self.request_id.as_deref(),
        })
    }
}
