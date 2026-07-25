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
            if !(1..=spider::net::request::MAX_RETRY_COUNT).contains(&seed.max_retry_count) {
                return Err(Error::Invalid(format!(
                    "seed max_retry_count must be between 1 and {}",
                    spider::net::request::MAX_RETRY_COUNT
                )));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_seed_enforces_the_retry_limit_boundary() {
        let mut accepted = task(spider::net::request::MAX_RETRY_COUNT);
        assert!(validate(&accepted).is_ok());

        accepted.seeds[0].max_retry_count = spider::net::request::MAX_RETRY_COUNT + 1;
        let error = validate(&accepted).unwrap_err();

        assert!(error.to_string().contains("max_retry_count"));
    }

    fn task(max_retry_count: i32) -> Task {
        Task {
            id: "task-1".to_string(),
            name: "task-1".to_string(),
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
                max_retry_count,
            }],
            persister_id: None,
            attachment: None,
            next_time: 0,
        }
    }
}
