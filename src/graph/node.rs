use indexmap::IndexMap;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub allowed_domains: Option<Vec<String>>,
    #[serde(default)]
    pub fields: IndexMap<String, crate::graph::rules::Field>,
    #[serde(default)]
    pub binds: IndexMap<String, crate::graph::rules::Bind>,
    #[serde(default)]
    pub middlewares: Vec<crate::middleware::Spec>,
}
