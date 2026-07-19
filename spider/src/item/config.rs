use indexmap::IndexMap;
use serde_json::{Map, Value};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub fields: IndexMap<String, Field>,
    pub schema: Value,
}

/// Describes crawler-side processing for an Item field.
///
/// Currently only uses `kind`. The dedicated structure leaves room for later
/// processing metadata without mixing crawler concerns into validator Schema.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub kind: Kind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
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

    pub(crate) fn kind(&self, name: &str) -> Kind {
        self.fields
            .get(name)
            .map(|field| field.kind)
            .unwrap_or(Kind::Text)
    }
}
