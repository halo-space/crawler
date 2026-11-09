use indexmap::IndexMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Request,
    Item,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub from: String,
    pub kind: Kind,
    #[serde(default)]
    pub request: Option<crate::graph::request::Spec>,
    #[serde(default)]
    pub vals: IndexMap<String, crate::graph::rules::ValueRef>,
    #[serde(default)]
    pub when: Option<String>,
    #[serde(default, rename = "fn")]
    pub function: Option<String>,
}
