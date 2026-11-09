use serde_json::{Map, Value};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: Value,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Text,
    Image,
    Video,
    Audio,
}

impl Config {
    pub(crate) fn schema_fields(&self) -> Result<&Map<String, Value>, crate::item::Error> {
        self.schema
            .get("fields")
            .and_then(Value::as_object)
            .ok_or_else(|| crate::item::Error::Message("item schema requires fields".to_string()))
    }
}
