use indexmap::IndexMap;

use crate::graph::rules::ValueRef;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Request,
    Item,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOp {
    Present,
    Absent,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Predicate {
    pub from: String,
    pub op: ConditionOp,
    #[serde(default)]
    pub value: Option<ValueRef>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct All {
    pub all: Vec<Condition>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Any {
    pub any: Vec<Condition>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Condition {
    Predicate(Predicate),
    All(All),
    Any(Any),
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub from: String,
    pub kind: Kind,
    #[serde(default)]
    pub each: Option<String>,
    #[serde(default)]
    pub request: Option<crate::graph::request::Spec>,
    #[serde(default)]
    pub vals: IndexMap<String, crate::graph::rules::ValueRef>,
    #[serde(default)]
    pub when: Option<Condition>,
    #[serde(default, rename = "fn")]
    pub function: Option<String>,
}
