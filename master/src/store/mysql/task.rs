use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::validate::identifier;
use crate::Error;

mod dispatch;
mod write;

#[cfg(test)]
pub(super) use dispatch::next_period;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodeSeed {
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
pub struct Task {
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

fn validate(task: &Task) -> Result<(), Error> {
    identifier(&task.id, "Task id")?;
    identifier(&task.name, "Task name")?;
    if task.next_time < 0 {
        return Err(Error::Invalid(
            "Task next_time must be non-negative".to_string(),
        ));
    }
    if task.periodic && task.interval_ms <= 0 {
        return Err(Error::Invalid(
            "periodic Task interval_ms must be positive".to_string(),
        ));
    }
    if !task.periodic && task.interval_ms != 0 {
        return Err(Error::Invalid(
            "one-shot Task interval_ms must be zero".to_string(),
        ));
    }
    if task.dsl.is_some() && !task.seeds.is_empty() {
        return Err(Error::Invalid(
            "Task must define either Rules DSL or Code seeds, not both".to_string(),
        ));
    }
    if let Some(config) = &task.dsl {
        config
            .validate()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let requests = config
            .initial_requests(task.id.clone(), "validation", task.params.clone())
            .map_err(|error| Error::Invalid(format!("invalid Rules Task seed: {error}")))?;
        for request in requests {
            spider::net::request::Snapshot::try_from(request)
                .map_err(|error| Error::Invalid(format!("invalid Rules Task seed: {error}")))?;
        }
    } else {
        if task.seeds.is_empty() {
            return Err(Error::Invalid(
                "Code Task must contain at least one serialized seed".to_string(),
            ));
        }
        for seed in &task.seeds {
            if seed.url.trim().is_empty() {
                return Err(Error::Invalid("seed URL must not be empty".to_string()));
            }
            identifier(&seed.node, "seed node")?;
            if seed.max_retry_count <= 0 {
                return Err(Error::Invalid(
                    "seed max_retry_count must be positive".to_string(),
                ));
            }
        }
        for request in materialize(&task.id, "validation", &task.params, &task.seeds)? {
            spider::net::request::Snapshot::try_from(request)
                .map_err(|error| Error::Invalid(format!("invalid Code Task seed: {error}")))?;
        }
    }
    Ok(())
}

pub(super) fn materialize(
    task_id: &str,
    trace_id: &str,
    params: &HashMap<String, Value>,
    seeds: &[CodeSeed],
) -> Result<Vec<spider::net::Request>, Error> {
    seeds
        .iter()
        .map(|seed| {
            let mut request = spider::net::Request::follow(seed.url.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?
                .node(seed.node.clone());
            request.task_id = task_id.to_string();
            request.trace_id = trace_id.to_string();
            request.method = seed.method.clone();
            request.headers = seed.headers.clone();
            request.body = seed.body.clone();
            request.cookies = seed.cookies.clone();
            request.vals = params.clone();
            request.vals.extend(seed.vals.clone());
            request.kwargs = seed.kwargs.clone();
            request.priority = seed.priority;
            request.dont_filter = seed.dont_filter;
            request.mode = seed.mode.clone();
            request.timeout = seed.timeout;
            request.max_body_bytes = seed.max_body_bytes;
            request.proxy = seed.proxy.clone();
            request.tls = seed.tls.clone();
            request.middlewares = seed.middlewares.clone();
            request.max_retry_count = seed.max_retry_count;
            Ok(request)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_shot_task_rejects_an_unused_interval() {
        let mut task = code_task();
        task.interval_ms = 1;

        assert!(validate(&task).is_err());
        task.interval_ms = 0;
        assert!(validate(&task).is_ok());
    }

    fn code_task() -> Task {
        Task {
            id: "task".to_string(),
            name: "task".to_string(),
            periodic: false,
            interval_ms: 0,
            priority: 0,
            params: HashMap::new(),
            dsl: None,
            seeds: vec![CodeSeed {
                node: "index".to_string(),
                url: "https://example.com".to_string(),
                method: Default::default(),
                headers: Default::default(),
                body: Default::default(),
                cookies: Default::default(),
                vals: HashMap::new(),
                kwargs: HashMap::new(),
                priority: 0,
                dont_filter: false,
                mode: Default::default(),
                timeout: None,
                max_body_bytes: None,
                proxy: None,
                tls: None,
                middlewares: Vec::new(),
                max_retry_count: 1,
            }],
            persister_id: None,
            attachment: None,
            next_time: 0,
        }
    }
}
