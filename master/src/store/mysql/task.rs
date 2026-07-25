use std::collections::HashMap;

use serde_json::Value;

mod dispatch;
mod write;

use super::validate::identifier;
use crate::Error;
pub(super) use crate::types::task::{CodeSeed, Task};

pub(super) fn validate(task: &Task) -> Result<(), Error> {
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
pub(super) use dispatch::next_period;
