use std::sync::Arc;

use super::*;
use crate::scheduler::{Init, Scheduler};

const WORKER: &str = "worker-1";
const HTTP: &[net::Mode] = &[net::Mode::Http];
const BROWSER_WORKER: &str = "browser-worker";
const BROWSER: &[net::Mode] = &[net::Mode::Browser];
const TASK: &str = "task-1";
const TRACE: &str = "trace-1";

mod claim;
mod failure;
mod identity;
mod init;
mod lease;
mod push;
mod release;
mod success;

fn request(url: impl Into<String>) -> net::Request {
    let mut request = net::Request::follow(url).unwrap();
    request.task_id = TASK.to_string();
    request.trace_id = TRACE.to_string();
    request
}

fn memory() -> Memory {
    let scheduler = Memory::new();
    add_trace(&scheduler);
    scheduler
}

fn memory_with_lease(lease: scheduler::Lease) -> Memory {
    let scheduler = Memory::new().with_lease(lease);
    add_trace(&scheduler);
    scheduler
}

fn add_trace(scheduler: &Memory) {
    scheduler
        .state()
        .trace_snapshots
        .insert(TRACE.to_string(), Arc::new(trace::Snapshot::code(TASK)));
}

fn rules_config(id: &str, node: &str) -> crate::config::Config {
    crate::config::Config::from_yaml(&format!(
        r#"
spider:
  name: {id}
  start:
    - node: {node}
      url: https://example.com
graph:
  nodes:
    {node}: {{}}
  edges: []
"#
    ))
    .unwrap()
}
