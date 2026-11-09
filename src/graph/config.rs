use std::collections::HashMap;

use crate::graph;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub nodes: HashMap<String, graph::node::Config>,
    #[serde(default)]
    pub edges: Vec<graph::edge::Spec>,
}
