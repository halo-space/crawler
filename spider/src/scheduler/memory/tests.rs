use std::sync::Arc;

use super::*;
use crate::scheduler::{Init, Scheduler};

const WORKER: &str = "worker-1";
const HTTP: &[net::Mode] = &[net::Mode::Http];
const BROWSER_WORKER: &str = "browser-worker";
const BROWSER: &[net::Mode] = &[net::Mode::Browser];

mod claim;
mod failure;
mod identity;
mod init;
mod lease;
mod push;
mod release;
mod success;

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
