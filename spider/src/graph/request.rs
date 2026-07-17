use indexmap::IndexMap;

use crate::graph::rules::ValueRef;

/// A complete declarative Request description.
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub node: String,
    pub url: ValueRef,
    #[serde(default)]
    pub priority: Option<i32>,
    #[serde(flatten)]
    pub transport: crate::net::request::Config,
    #[serde(default)]
    pub vals: IndexMap<String, ValueRef>,
}
