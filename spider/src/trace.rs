use std::collections::HashMap;

use serde_json::Value;

use crate::config;

pub fn next_id(spider_name: &str) -> String {
    format!("trace_{spider_name}_{}", uuid::Uuid::now_v7())
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
}
