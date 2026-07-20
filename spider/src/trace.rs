use std::collections::HashMap;
use std::fmt;

use serde_json::Value;

use crate::config;

pub fn next_id(spider_name: &str) -> String {
    format!("trace_{spider_name}_{}", uuid::Uuid::now_v7())
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub task_id: String,
    #[serde(default)]
    pub params: HashMap<String, Value>,
    #[serde(default)]
    pub attachment: Option<Value>,
    #[serde(default)]
    pub persister_id: Option<String>,
    pub priority: i32,
    #[serde(default)]
    pub dsl: Option<config::Config>,
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Snapshot")
            .field("task_id", &self.task_id)
            .field("params_len", &self.params.len())
            .field("has_attachment", &self.attachment.is_some())
            .field("has_persister_id", &self.persister_id.is_some())
            .field("priority", &self.priority)
            .field("has_dsl", &self.dsl.is_some())
            .finish()
    }
}

impl Snapshot {
    pub fn rules(task_id: impl Into<String>, dsl: config::Config) -> Self {
        let priority = dsl.spider.priority.unwrap_or_default();
        Self {
            task_id: task_id.into(),
            params: HashMap::new(),
            attachment: None,
            persister_id: None,
            priority,
            dsl: Some(dsl),
        }
    }

    pub fn code(task_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            params: HashMap::new(),
            attachment: None,
            persister_id: None,
            priority: 0,
            dsl: None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.task_id.is_empty() {
            return Err("Trace Snapshot task_id must not be empty".to_string());
        }
        if let Some(dsl) = &self.dsl {
            dsl.validate().map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_snapshot_has_no_dsl() {
        let snapshot = Snapshot::code("task-1");

        assert!(snapshot.dsl.is_none());
        snapshot.validate().unwrap();
    }

    #[test]
    fn final_shape_omits_speculative_metadata() {
        let snapshot = Snapshot::code("task-1");
        let encoded = serde_json::to_value(snapshot).unwrap();

        assert!(encoded.get("schema_version").is_none());
        assert!(encoded.get("task_version").is_none());
        assert!(encoded.get("download_mode").is_none());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let mut encoded = serde_json::to_value(Snapshot::code("task-1")).unwrap();
        encoded["schema_version"] = Value::from(1);

        assert!(serde_json::from_value::<Snapshot>(encoded).is_err());
    }

    #[test]
    fn debug_redacts_params_attachments_and_dsl() {
        let config = config::Config::from_yaml(
            r#"
spider:
  name: books
  start:
    - node: index
      url: https://example.com
      headers:
        Authorization: dsl-header-secret
      cookies:
        session: dsl-cookie-secret
graph:
  nodes:
    index: {}
  edges: []
"#,
        )
        .unwrap();
        let mut snapshot = Snapshot::rules("task-1", config);
        snapshot
            .params
            .insert("token".to_string(), Value::from("params-secret"));
        snapshot.attachment = Some(serde_json::json!({"token": "attachment-secret"}));
        snapshot.persister_id = Some("persister-secret".to_string());

        let debug = format!("{snapshot:?}");

        assert!(!debug.contains("params-secret"));
        assert!(!debug.contains("attachment-secret"));
        assert!(!debug.contains("persister-secret"));
        assert!(!debug.contains("dsl-header-secret"));
        assert!(!debug.contains("dsl-cookie-secret"));
        assert!(debug.contains("params_len: 1"));
        assert!(debug.contains("has_attachment: true"));
    }
}
