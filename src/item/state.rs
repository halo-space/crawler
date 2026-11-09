use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::middleware::Spec;

#[derive(Clone, Debug, Default)]
pub struct State {
    id: String,
    schema: Option<crate::item::SchemaKey>,
    vals: HashMap<String, Value>,
    download_fields: HashSet<String>,
    middlewares: Vec<Spec>,
    task_id: String,
    node: String,
}

impl State {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn id_mut(&mut self) -> &mut String {
        &mut self.id
    }

    pub fn schema(&self) -> Option<crate::item::SchemaKey> {
        self.schema
    }

    pub fn set_schema(&mut self, schema: Option<crate::item::SchemaKey>) {
        self.schema = schema;
    }

    pub fn vals(&self) -> &HashMap<String, Value> {
        &self.vals
    }

    pub fn vals_mut(&mut self) -> &mut HashMap<String, Value> {
        &mut self.vals
    }

    pub fn set_download_fields<I, S>(&mut self, fields: I)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.download_fields = fields.into_iter().map(Into::into).collect();
    }

    pub fn download_fields(&self) -> &HashSet<String> {
        &self.download_fields
    }

    pub fn middlewares(&self) -> &[Spec] {
        &self.middlewares
    }

    pub fn middlewares_mut(&mut self) -> &mut Vec<Spec> {
        &mut self.middlewares
    }

    pub(crate) fn set_scope(&mut self, task_id: impl Into<String>, node: impl Into<String>) {
        self.task_id = task_id.into();
        self.node = node.into();
    }

    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }

    pub(crate) fn node(&self) -> &str {
        &self.node
    }
}
