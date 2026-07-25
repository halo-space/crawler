use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Error;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum State {
    Scheduled,
    Done,
    Failed,
}

impl State {
    pub(crate) fn code(self) -> i8 {
        match self {
            Self::Scheduled => 1,
            Self::Done => 3,
            Self::Failed => 4,
        }
    }

    pub(crate) fn from_code(value: i8) -> Result<Self, Error> {
        match value {
            1 => Ok(Self::Scheduled),
            3 => Ok(Self::Done),
            4 => Ok(Self::Failed),
            _ => Err(Error::Invalid(format!(
                "invalid stored Task state: {value}"
            ))),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Summary {
    pub id: String,
    pub name: String,
    pub state: State,
    pub periodic: bool,
    pub interval_ms: i64,
    pub priority: i32,
    pub next_time: i64,
    pub created_time: i64,
    pub updated_time: i64,
}

#[derive(Serialize)]
pub(crate) struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    pub params: HashMap<String, Value>,
    pub dsl: Option<spider::config::Config>,
    pub seeds: Vec<CodeSeed>,
    pub persister_id: Option<String>,
    pub attachment: Option<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CodeSeed {
    pub node: String,
    pub url: String,
    #[serde(default)]
    pub method: spider::net::Method,
    #[serde(default)]
    pub headers: spider::net::Headers,
    #[serde(default)]
    pub body: spider::net::Body,
    #[serde(default)]
    pub cookies: spider::net::Cookies,
    #[serde(default)]
    pub vals: HashMap<String, Value>,
    #[serde(default)]
    pub kwargs: HashMap<String, Value>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub dont_filter: bool,
    #[serde(default)]
    pub mode: spider::net::Mode,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub max_body_bytes: Option<u64>,
    #[serde(default)]
    pub proxy: Option<spider::net::ProxyConfig>,
    #[serde(default)]
    pub tls: Option<spider::net::TlsConfig>,
    #[serde(default)]
    pub middlewares: Vec<spider::middleware::Spec>,
    #[serde(default = "default_retry_count")]
    pub max_retry_count: i32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Task {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub periodic: bool,
    #[serde(default)]
    pub interval_ms: i64,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub dsl: Option<spider::config::Config>,
    #[serde(default)]
    pub seeds: Vec<CodeSeed>,
    #[serde(default)]
    pub persister_id: Option<String>,
    #[serde(default)]
    pub attachment: Option<Value>,
    #[serde(default)]
    pub next_time: i64,
}

fn default_retry_count() -> i32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct List {
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub state: Option<State>,
}

#[derive(Serialize)]
pub(crate) struct Filter {
    state: Option<State>,
}

impl List {
    pub(crate) fn limit(&self) -> Result<usize, Error> {
        super::limit(self.limit)
    }

    pub(crate) fn filter(&self) -> Filter {
        Filter { state: self.state }
    }
}
