use indexmap::IndexMap;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    pub parse: crate::graph::rules::Parse,
    #[serde(default)]
    pub bind: IndexMap<String, crate::graph::rules::Bind>,
}
