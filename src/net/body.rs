use bytes::Bytes;
use serde_json::Value;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub enum Body {
    #[default]
    Empty,
    Bytes(Bytes),
    Text(String),
    Json(Value),
}

impl Body {
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}
