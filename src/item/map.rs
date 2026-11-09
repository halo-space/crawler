use indexmap::IndexMap;
use serde::Serialize;
use serde_json::Value;

use crate::item;

pub struct Map {
    state: item::State,
    fields: IndexMap<String, Value>,
}

impl Map {
    pub fn new(fields: IndexMap<String, Value>) -> Self {
        Self {
            state: item::State::default(),
            fields,
        }
    }

    pub fn fields(&self) -> &IndexMap<String, Value> {
        &self.fields
    }
}

impl Serialize for Map {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.fields.serialize(serializer)
    }
}

impl item::Item for Map {
    fn from_values(values: item::Values) -> Result<Self, crate::item::Error> {
        Ok(Self::new(values))
    }

    fn state(&self) -> &item::State {
        &self.state
    }

    fn state_mut(&mut self) -> &mut item::State {
        &mut self.state
    }

    fn values(&self) -> Result<item::Values, crate::item::Error> {
        Ok(self.fields.clone())
    }

    fn replace_values(&mut self, values: item::Values) -> Result<(), crate::item::Error> {
        self.fields = values;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_map_carries_item_config_to_before_item_validate() {
        let schema = serde_json::json!({
            "fields": {
                "title": {
                    "type": "string",
                    "rules": ["required", {"min": 2}]
                }
            }
        });

        let store = crate::item::schema::Store::new();
        let key = store.register(&schema).unwrap();
        let mut item = Map::new(IndexMap::from([("title".to_string(), Value::from("Rust"))]));
        item::Item::state_mut(&mut item).set_schema(Some(key));

        assert_eq!(crate::item::Item::schema(&item), Some(key));
        assert!(crate::item::Item::state(&item).middlewares().is_empty());
    }
}
