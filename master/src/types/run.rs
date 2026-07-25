use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Init {
    pub trace_id: String,
    pub trace: spider::trace::Snapshot,
    pub requests: Vec<spider::net::request::Snapshot>,
}
