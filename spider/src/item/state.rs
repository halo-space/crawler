use std::collections::HashMap;

use serde_json::Value;

#[derive(Clone, Debug, Default)]
pub struct State {
    id: String,
    schema: Option<crate::item::SchemaKey>,
    vals: HashMap<String, Value>,
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
}
